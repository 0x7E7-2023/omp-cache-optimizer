/**
 * Archive snapshot for closed past-day stats shards (issue #28, phase 2).
 *
 * Every Pi process writes one shard per local day. Shards are kept for
 * SHARD_RETENTION_MS so session-scoped footer totals survive long
 * conversations, which left ~1,500 files to stat and parse on every refresh.
 *
 * Past-day shards that are closed (or abandoned by a dead process) never
 * change again, so maintenance folds them into one archive file and deletes
 * the originals. The directory then only holds today's shards.
 *
 * Correctness rules:
 * - Buckets keep the global and model epochs they were written under. Readers
 *   filter them with the same epoch checks as raw shards, so a reset hides
 *   archived counters at once, before the next compaction drops them.
 * - The archive records every instanceId it absorbed. Readers ignore raw
 *   shards (and in-memory retired shards) with an absorbed id, so a crash
 *   between publishing the archive and deleting the originals, or two
 *   processes compacting at once, never double counts.
 * - Archived data only feeds the all-days (session-scoped) aggregate. Its
 *   buckets keep their original day, so the current-day aggregate filters
 *   them out exactly as it filters the raw past-day shards they replace.
 *   Per-instance counters in the all-days aggregate count archive buckets,
 *   not processes; only `bySession` and `lastRoutedModelBySession` are read
 *   from it.
 */
import { randomUUID } from "node:crypto";
import { lstat, mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { LOG_PREFIX, asRecord, getErrorCode, isNonEmptyString, isProcessAlive } from "./common.ts";
import {
  type CacheStats,
  type PersistedRoutedModelRef,
  type PersistedStatsShardV7,
  SHARD_CLEANUP_MARKER_PATH,
  SHARD_FILES_DIR,
  SHARD_IO_CONCURRENCY,
  SHARD_MAINTENANCE_DIR,
  SHARD_RETENTION_MS,
  SHARD_STATE_DIR,
  SHARD_TEMP_RETENTION_MS,
  acquireStatsMaintenanceLock,
  cleanupStatsShardsV7,
  cloneCacheStats,
  currentLocalDay,
  isMaintenanceMarkerDue,
  mapWithConcurrency,
  mergeStatsAcrossDays,
  parseCacheStats,
  parsePersistedRoutedModelRef,
  holdsStatsMaintenanceLock,
  readGlobalStatsEpochStrict,
  readModelStatsEpochStrict,
  readValidStatsShardsV7,
  releaseStatsMaintenanceLock,
  renameWithRetry,
  writeMaintenanceMarker,
} from "./stats-store.ts";

export const SHARD_ARCHIVE_DIR = join(SHARD_STATE_DIR, "archive");
export const SHARD_ARCHIVE_PATH = join(SHARD_ARCHIVE_DIR, "archive.json");
export const SHARD_COMPACTION_MARKER_PATH = join(SHARD_MAINTENANCE_DIR, "last-compaction");

/**
 * An active shard is only treated as abandoned once its process is gone and
 * it has not been touched for this long, so a recycled Windows PID or a
 * transient process-check failure cannot fold a live shard away.
 */
export const ARCHIVE_ABANDONED_ACTIVE_MS = 24 * 60 * 60 * 1000;

export type StatsArchiveBucketV1 = {
  globalEpoch: string;
  sessionHash: string;
  modelKey: string;
  modelEpoch: string;
  /** Day of the shards folded into this bucket. */
  day: string;
  provider: string;
  modelId: string;
  modelName?: string;
  api?: string;
  /** Latest lifecycle.updatedAt among the folded shards. */
  updatedAt: number;
  stats: CacheStats;
};

export type StatsArchiveRoutedV1 = {
  globalEpoch: string;
  sessionHash: string;
  day: string;
  updatedAt: number;
  model: PersistedRoutedModelRef;
};

export type StatsArchiveV1 = {
  version: 1;
  kind: "pi-cache-optimizer-archive";
  updatedAt: number;
  /** instanceId -> time it was folded in. */
  absorbed: Record<string, number>;
  buckets: StatsArchiveBucketV1[];
  routed: StatsArchiveRoutedV1[];
};

const DAY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export function emptyStatsArchive(now = Date.now()): StatsArchiveV1 {
  return { version: 1, kind: "pi-cache-optimizer-archive", updatedAt: now, absorbed: {}, buckets: [], routed: [] };
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/** Strict parse: returns undefined when the document is not a v1 archive. Invalid entries are dropped. */
export function parseStatsArchive(value: unknown): StatsArchiveV1 | undefined {
  const record = asRecord(value);
  if (!record || record.version !== 1 || record.kind !== "pi-cache-optimizer-archive") return undefined;
  const rawAbsorbed = asRecord(record.absorbed);
  if (!rawAbsorbed || !Array.isArray(record.buckets) || !Array.isArray(record.routed)) return undefined;

  const absorbed: Record<string, number> = {};
  for (const [id, at] of Object.entries(rawAbsorbed)) {
    if (isNonEmptyString(id) && isFiniteNumber(at)) absorbed[id] = at;
  }

  const buckets: StatsArchiveBucketV1[] = [];
  for (const raw of record.buckets) {
    const entry = asRecord(raw);
    const stats = parseCacheStats(entry?.stats);
    if (
      !entry || !stats ||
      !isNonEmptyString(entry.globalEpoch) || !isNonEmptyString(entry.sessionHash) ||
      !isNonEmptyString(entry.modelKey) || !isNonEmptyString(entry.modelEpoch) ||
      !isNonEmptyString(entry.provider) || !isNonEmptyString(entry.modelId) ||
      typeof entry.day !== "string" || !DAY_PATTERN.test(entry.day) ||
      !isFiniteNumber(entry.updatedAt) ||
      entry.modelKey !== `${entry.provider}/${entry.modelId}`
    ) continue;
    buckets.push({
      globalEpoch: entry.globalEpoch,
      sessionHash: entry.sessionHash,
      modelKey: entry.modelKey,
      modelEpoch: entry.modelEpoch,
      day: entry.day,
      provider: entry.provider,
      modelId: entry.modelId,
      ...(isNonEmptyString(entry.modelName) ? { modelName: entry.modelName } : {}),
      ...(isNonEmptyString(entry.api) ? { api: entry.api } : {}),
      updatedAt: entry.updatedAt,
      stats,
    });
  }

  const routed: StatsArchiveRoutedV1[] = [];
  for (const raw of record.routed) {
    const entry = asRecord(raw);
    const model = parsePersistedRoutedModelRef(entry?.model);
    if (
      !entry || !model ||
      !isNonEmptyString(entry.globalEpoch) || !isNonEmptyString(entry.sessionHash) ||
      typeof entry.day !== "string" || !DAY_PATTERN.test(entry.day) ||
      !isFiniteNumber(entry.updatedAt)
    ) continue;
    routed.push({ globalEpoch: entry.globalEpoch, sessionHash: entry.sessionHash, day: entry.day, updatedAt: entry.updatedAt, model });
  }

  return {
    version: 1,
    kind: "pi-cache-optimizer-archive",
    updatedAt: isFiniteNumber(record.updatedAt) ? record.updatedAt : 0,
    absorbed,
    buckets,
    routed,
  };
}

export type LoadedStatsArchive = {
  /** False when no archive file exists; true for a parsed archive. */
  present: boolean;
  /** True when a file exists but cannot be parsed; compaction then refuses to overwrite it. */
  corrupt: boolean;
  archive: StatsArchiveV1;
  absorbedIds: ReadonlySet<string>;
  /** Archive buckets/routed entries as synthetic shards for aggregateStatsShardsV7. Do not mutate. */
  shards: readonly PersistedStatsShardV7[];
};

type CachedArchive = { ino: number | bigint; size: number; mtimeMs: number; loaded: LoadedStatsArchive };
const archiveCache = new Map<string, CachedArchive>();

export function clearStatsArchiveCache(): void {
  archiveCache.clear();
}

/** Convert archive entries into synthetic closed shards so the existing aggregation applies unchanged. */
export function archiveToSyntheticShards(archive: StatsArchiveV1): PersistedStatsShardV7[] {
  const shards: PersistedStatsShardV7[] = [];
  const base = (id: string, sessionHash: string, day: string, globalEpoch: string, updatedAt: number): PersistedStatsShardV7 => ({
    version: 7,
    kind: "pi-cache-optimizer-shard",
    instanceId: id,
    sessionHash,
    process: { pid: 0, ppid: 0, instanceStartedAt: 0 },
    lifecycle: { state: "closed", createdAt: updatedAt, updatedAt, closedAt: updatedAt },
    day,
    globalEpoch,
    models: {},
  });
  archive.buckets.forEach((bucket, index) => {
    const shard = base(`archive-bucket-${index}`, bucket.sessionHash, bucket.day, bucket.globalEpoch, bucket.updatedAt);
    shard.models[bucket.modelKey] = {
      modelEpoch: bucket.modelEpoch,
      provider: bucket.provider,
      modelId: bucket.modelId,
      ...(bucket.modelName ? { modelName: bucket.modelName } : {}),
      ...(bucket.api ? { api: bucket.api } : {}),
      stats: cloneCacheStats(bucket.stats),
    };
    shards.push(shard);
  });
  archive.routed.forEach((entry, index) => {
    const shard = base(`archive-routed-${index}`, entry.sessionHash, entry.day, entry.globalEpoch, entry.updatedAt);
    shard.lastRoutedModel = { ...entry.model };
    shards.push(shard);
  });
  return shards;
}

function buildLoaded(archive: StatsArchiveV1, present: boolean, corrupt: boolean): LoadedStatsArchive {
  return {
    present,
    corrupt,
    archive,
    absorbedIds: new Set(Object.keys(archive.absorbed)),
    shards: archiveToSyntheticShards(archive),
  };
}

/**
 * Read the archive, re-parsing only when inode/size/mtime changed. A missing
 * file is an empty archive; an invalid one is reported as corrupt and treated
 * as empty, so raw shards stay the source of truth. A transient I/O failure
 * (Windows EPERM/EBUSY while another process or a scanner holds the file) is
 * never cached: the last good parse is returned when there is one, otherwise
 * an uncached corrupt result, so the next refresh retries.
 */
export async function readStatsArchive(path: string = SHARD_ARCHIVE_PATH): Promise<LoadedStatsArchive> {
  const transientFailure = (): LoadedStatsArchive => {
    const previous = archiveCache.get(path);
    return previous && !previous.loaded.corrupt ? previous.loaded : buildLoaded(emptyStatsArchive(0), true, true);
  };
  let info: import("node:fs").Stats;
  try {
    info = await lstat(path);
  } catch (error) {
    if (getErrorCode(error) === "ENOENT") {
      archiveCache.delete(path);
      return buildLoaded(emptyStatsArchive(0), false, false);
    }
    return transientFailure();
  }
  if (!info.isFile() || info.isSymbolicLink()) {
    archiveCache.delete(path);
    return buildLoaded(emptyStatsArchive(0), true, true);
  }
  const cached = archiveCache.get(path);
  if (cached && cached.ino === info.ino && cached.size === info.size && cached.mtimeMs === info.mtimeMs) return cached.loaded;
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    // A file renamed away between lstat and read is simply gone.
    if (getErrorCode(error) === "ENOENT") {
      archiveCache.delete(path);
      return buildLoaded(emptyStatsArchive(0), false, false);
    }
    return transientFailure();
  }
  let loaded: LoadedStatsArchive;
  try {
    const parsed = parseStatsArchive(JSON.parse(text));
    loaded = parsed ? buildLoaded(parsed, true, false) : buildLoaded(emptyStatsArchive(0), true, true);
  } catch {
    loaded = buildLoaded(emptyStatsArchive(0), true, true);
  }
  archiveCache.set(path, { ino: info.ino, size: info.size, mtimeMs: info.mtimeMs, loaded });
  return loaded;
}

type ArchiveFileIdentity = { ino: number | bigint; size: number; mtimeMs: number } | undefined;

async function archiveFileIdentity(path: string): Promise<ArchiveFileIdentity> {
  try {
    const info = await lstat(path);
    return { ino: info.ino, size: info.size, mtimeMs: info.mtimeMs };
  } catch (error) {
    if (getErrorCode(error) === "ENOENT") return undefined;
    throw error;
  }
}

function sameArchiveFile(a: ArchiveFileIdentity, b: ArchiveFileIdentity): boolean {
  if (!a || !b) return a === b;
  return a.ino === b.ino && a.size === b.size && a.mtimeMs === b.mtimeMs;
}

/** Remove archive temp files a crash left behind (shard cleanup only covers shards/). */
async function cleanupStaleArchiveTemps(archivePath: string, now: number): Promise<void> {
  const directory = dirname(archivePath);
  let names: string[];
  try {
    names = await readdir(directory);
  } catch {
    return;
  }
  for (const name of names) {
    if (!name.endsWith(".tmp")) continue;
    const path = join(directory, name);
    try {
      const info = await lstat(path);
      if (info.isFile() && !info.isSymbolicLink() && now - info.mtimeMs >= SHARD_TEMP_RETENTION_MS) await unlink(path);
    } catch {}
  }
}

async function writeStatsArchive(path: string, archive: StatsArchiveV1): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tempPath = `${path}.${process.pid}.${Date.now()}.${randomUUID()}.tmp`;
  try {
    await writeFile(tempPath, JSON.stringify(archive) + "\n", "utf8");
    await renameWithRetry(tempPath, path);
  } catch (error) {
    await unlink(tempPath).catch(() => undefined);
    throw error;
  }
  archiveCache.delete(path);
}

/** A shard maintenance may fold away: from a past day and no longer written to. */
export function isArchivableShard(shard: PersistedStatsShardV7, now: number, today: string): boolean {
  if (shard.day >= today) return false;
  if (now - shard.lifecycle.updatedAt >= SHARD_RETENTION_MS) return false; // retention deletes it instead
  if (shard.lifecycle.state === "closed") return true;
  return now - shard.lifecycle.updatedAt >= ARCHIVE_ABANDONED_ACTIVE_MS && !isProcessAlive(shard.process.pid);
}

export type CompactionResult = {
  /** Raw shards newly folded into the archive. */
  absorbed: number;
  /** Raw shard files deleted (newly or previously absorbed). */
  removed: number;
  /** Archive entries dropped for an old epoch or past retention. */
  dropped: number;
  /** True when an unreadable archive blocked compaction. */
  skippedCorrupt: boolean;
  /**
   * True when the archive, an epoch, or lock ownership changed while this
   * pass ran (e.g. a stale-lock takeover or a concurrent reset). Nothing was
   * published or deleted; the next pass retries.
   */
  skippedConcurrent: boolean;
};

export type CompactionOptions = {
  /** Owner token from acquireStatsMaintenanceLock; ownership is re-checked before publishing. */
  lockToken?: string;
  /** Test hook: runs after the plan is built, right before the publish checks. */
  beforePublish?: () => Promise<void>;
};

const bucketId = (b: Pick<StatsArchiveBucketV1, "globalEpoch" | "sessionHash" | "modelKey" | "modelEpoch" | "day">): string =>
  JSON.stringify([b.globalEpoch, b.sessionHash, b.modelKey, b.modelEpoch, b.day]);
const routedId = (r: Pick<StatsArchiveRoutedV1, "globalEpoch" | "sessionHash">): string =>
  JSON.stringify([r.globalEpoch, r.sessionHash]);

/**
 * Fold archivable shards into the archive, then delete them. Call only while
 * holding the maintenance lock. Order matters for crash safety: the archive
 * (with the absorbed ids) is published before any raw shard is deleted.
 */
export async function compactStatsShardsV7(
  now = Date.now(),
  directory: string = SHARD_FILES_DIR,
  archivePath: string = SHARD_ARCHIVE_PATH,
  options: CompactionOptions = {},
): Promise<CompactionResult> {
  const result: CompactionResult = { absorbed: 0, removed: 0, dropped: 0, skippedCorrupt: false, skippedConcurrent: false };
  await cleanupStaleArchiveTemps(archivePath, now);
  clearStatsArchiveCache();
  const identityAtRead = await archiveFileIdentity(archivePath);
  const loaded = await readStatsArchive(archivePath);
  if (loaded.corrupt) {
    result.skippedCorrupt = true;
    return result;
  }
  const previous = loaded.archive;
  const today = currentLocalDay();
  // Strict reads: compaction permanently drops counters whose epoch differs,
  // so an unreadable epoch file must abort instead of falling back to the
  // initial epoch (which would discard every archived and absorbed counter).
  const globalEpoch = await readGlobalStatsEpochStrict();
  const modelEpochs = new Map<string, string>();
  const currentModelEpoch = async (key: string): Promise<string> => {
    let epoch = modelEpochs.get(key);
    if (!epoch) {
      epoch = await readModelStatsEpochStrict(key);
      modelEpochs.set(key, epoch);
    }
    return epoch;
  };

  // Carry forward live entries; drop ones a reset or retention invalidated.
  const buckets = new Map<string, StatsArchiveBucketV1>();
  for (const bucket of previous.buckets) {
    if (
      bucket.globalEpoch !== globalEpoch ||
      bucket.modelEpoch !== await currentModelEpoch(bucket.modelKey) ||
      now - bucket.updatedAt >= SHARD_RETENTION_MS
    ) {
      result.dropped++;
      continue;
    }
    buckets.set(bucketId(bucket), { ...bucket, stats: cloneCacheStats(bucket.stats) });
  }
  const routed = new Map<string, StatsArchiveRoutedV1>();
  for (const entry of previous.routed) {
    if (entry.globalEpoch !== globalEpoch || now - entry.updatedAt >= SHARD_RETENTION_MS) {
      result.dropped++;
      continue;
    }
    routed.set(routedId(entry), { ...entry, model: { ...entry.model } });
  }

  const shards = await readValidStatsShardsV7(directory);
  const presentIds = new Set(shards.map((shard) => shard.instanceId));
  const absorbed: Record<string, number> = {};
  // Keep ids whose raw file still exists (a crash or a failed delete left it).
  for (const [id, at] of Object.entries(previous.absorbed)) if (presentIds.has(id)) absorbed[id] = at;

  for (const shard of shards) {
    if (shard.instanceId in absorbed || !isArchivableShard(shard, now, today)) continue;
    absorbed[shard.instanceId] = now;
    result.absorbed++;
    if (shard.globalEpoch !== globalEpoch) continue; // reset away: nothing to keep
    if (shard.lastRoutedModel) {
      const id = routedId({ globalEpoch, sessionHash: shard.sessionHash });
      const existing = routed.get(id);
      if (!existing || existing.updatedAt < shard.lifecycle.updatedAt) {
        routed.set(id, { globalEpoch, sessionHash: shard.sessionHash, day: shard.day, updatedAt: shard.lifecycle.updatedAt, model: { ...shard.lastRoutedModel } });
      }
    }
    for (const [key, entry] of Object.entries(shard.models)) {
      if (entry.modelEpoch !== await currentModelEpoch(key)) continue;
      const id = bucketId({ globalEpoch, sessionHash: shard.sessionHash, modelKey: key, modelEpoch: entry.modelEpoch, day: shard.day });
      const existing = buckets.get(id);
      const newer = !existing || existing.updatedAt <= shard.lifecycle.updatedAt;
      buckets.set(id, {
        globalEpoch,
        sessionHash: shard.sessionHash,
        modelKey: key,
        modelEpoch: entry.modelEpoch,
        day: shard.day,
        provider: newer ? entry.provider : existing.provider,
        modelId: newer ? entry.modelId : existing.modelId,
        ...((newer ? entry.modelName : existing.modelName) ? { modelName: newer ? entry.modelName : existing.modelName } : {}),
        ...((newer ? entry.api : existing.api) ? { api: newer ? entry.api : existing.api } : {}),
        updatedAt: Math.max(existing?.updatedAt ?? 0, shard.lifecycle.updatedAt),
        stats: mergeStatsAcrossDays(existing?.stats, entry.stats, null),
      });
    }
  }

  const absorbedChanged =
    Object.keys(absorbed).length !== Object.keys(previous.absorbed).length ||
    Object.keys(absorbed).some((id) => !(id in previous.absorbed));
  if (result.absorbed > 0 || result.dropped > 0 || absorbedChanged) {
    await options.beforePublish?.();
    // Publish only if nothing this plan depends on moved underneath it: a
    // reset during the pass (counters written under the new epoch would be
    // dropped as stale), or another compactor after a stale-lock takeover
    // (overwriting its archive would lose shards it already deleted).
    let concurrent =
      !(await holdsStatsMaintenanceLock(options.lockToken)) ||
      !sameArchiveFile(identityAtRead, await archiveFileIdentity(archivePath)) ||
      (await readGlobalStatsEpochStrict()) !== globalEpoch;
    for (const [key, epoch] of modelEpochs) {
      if (concurrent) break;
      if ((await readModelStatsEpochStrict(key)) !== epoch) concurrent = true;
    }
    if (concurrent) {
      result.skippedConcurrent = true;
      result.absorbed = 0;
      result.dropped = 0;
      return result;
    }
    await writeStatsArchive(archivePath, {
      version: 1,
      kind: "pi-cache-optimizer-archive",
      updatedAt: now,
      absorbed,
      buckets: Array.from(buckets.values()),
      routed: Array.from(routed.values()),
    });
  }

  // Only now that the archive covering them is durable, delete the originals.
  const toDelete = Object.keys(absorbed);
  const outcomes = await mapWithConcurrency(toDelete, SHARD_IO_CONCURRENCY, async (id): Promise<number> => {
    try {
      await unlink(join(directory, `${id}.json`));
      return 1;
    } catch {
      // ENOENT: already gone. Anything else: the id stays absorbed (so the
      // file is ignored by readers) and the next compaction retries.
      return 0;
    }
  });
  result.removed = outcomes.reduce((sum, value) => sum + value, 0);
  return result;
}

export type StatsMaintenanceResult = { cleanupRemoved: number; compaction?: CompactionResult };

/**
 * Due retention cleanup and archive compaction under one lock. Each has its
 * own marker so the first run after upgrading compacts immediately even when
 * cleanup ran recently.
 */
export async function maybeRunStatsMaintenance(now = Date.now()): Promise<StatsMaintenanceResult> {
  const result: StatsMaintenanceResult = { cleanupRemoved: 0 };
  await mkdir(SHARD_MAINTENANCE_DIR, { recursive: true });
  const cleanupDue = await isMaintenanceMarkerDue(SHARD_CLEANUP_MARKER_PATH, now);
  const compactionDue = await isMaintenanceMarkerDue(SHARD_COMPACTION_MARKER_PATH, now);
  if (!cleanupDue && !compactionDue) return result;
  const lockToken = await acquireStatsMaintenanceLock(now);
  if (!lockToken) return result;
  try {
    if (cleanupDue) {
      result.cleanupRemoved = await cleanupStatsShardsV7(now);
      await writeMaintenanceMarker(SHARD_CLEANUP_MARKER_PATH, now);
    }
    if (compactionDue) {
      try {
        result.compaction = await compactStatsShardsV7(now, SHARD_FILES_DIR, SHARD_ARCHIVE_PATH, { lockToken });
        if (result.compaction.skippedCorrupt) {
          console.warn(`${LOG_PREFIX}: stats archive ${SHARD_ARCHIVE_PATH} is unreadable; keeping raw shards`);
        }
      } catch (error) {
        console.warn(`${LOG_PREFIX}: stats archive compaction failed`, error);
      }
      // A pass that lost a race published nothing; let the next startup retry
      // instead of waiting out the full interval.
      if (!result.compaction?.skippedConcurrent) await writeMaintenanceMarker(SHARD_COMPACTION_MARKER_PATH, now);
    }
  } finally {
    await releaseStatsMaintenanceLock(lockToken);
  }
  return result;
}

/**
 * Archive snapshot for closed past-day stats shards (issue #28, phase 2).
 *
 * Every Pi process writes one shard per local day. Shards are kept for
 * SHARD_RETENTION_MS so session-scoped footer totals survive long
 * conversations, which left ~1,500 files to stat and parse on every refresh.
 *
 * Past-day shards that are closed (or abandoned by a dead process) never
 * change again, so maintenance folds them into one archive snapshot and
 * deletes the originals. The directory then only holds today's shards.
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
 *
 * Storage (generations): each snapshot is an immutable file
 * `archive/archive.<generation>.json`. A compactor that read generation N
 * publishes N+1 with link(temp, target), which fails with EEXIST when anyone
 * else already published N+1. That makes publishing a compare-and-swap on
 * Windows and POSIX alike: a holder that stalled past the stale-lock window
 * (laptop sleep) and lost its lock can never replace a newer snapshot, and it
 * deletes no raw shard unless its own snapshot is the newest one. Readers use
 * the highest generation; old generations are removed one generation late
 * and readers retry when the file they listed is gone.
 *
 * 2.9.0 compatibility: 2.9.0 kept a single `archive/archive.json`. It is still
 * read while no generation exists and migrated by the next compaction, which
 * then replaces it with a directory of the same name. 2.9.0 treats that as an
 * unreadable archive: it never compacts (so it cannot overwrite or delete
 * anything) and its footer shows only history that is not archived, exactly
 * like the versions before 2.9.0. A file that 2.9.0 still manages to write
 * there afterwards (only if that directory is removed) is never counted: it is
 * set aside as `archive.legacy-<time>.json`, trading the counters it absorbed
 * for a guarantee against double counting.
 */
import { createHash, randomUUID } from "node:crypto";
import { link, lstat, mkdir, open, readFile, readdir, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { LOG_PREFIX, asRecord, getErrorCode, isNonEmptyString, isProcessAlive } from "./common.ts";
import {
  type CacheStats,
  type PersistedRoutedModelRef,
  type PersistedStatsShardV7,
  type ShardAggregate,
  type ShardAggregator,
  type ShardContribution,
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
  createShardAggregatorV7,
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
  touchStatsMaintenanceLock,
  writeMaintenanceMarker,
} from "./stats-store.ts";

export const SHARD_ARCHIVE_DIR = join(SHARD_STATE_DIR, "archive");
/**
 * 2.9.0's single-file archive. Read (and migrated) while no generation file
 * exists; afterwards a directory blocks 2.9.0 from writing it. The other
 * archive functions take this path and keep generations next to it.
 */
export const SHARD_ARCHIVE_PATH = join(SHARD_ARCHIVE_DIR, "archive.json");
export const SHARD_COMPACTION_MARKER_PATH = join(SHARD_MAINTENANCE_DIR, "last-compaction");

/**
 * An active shard is only treated as abandoned once its process is gone and
 * it has not been touched for this long, so a recycled Windows PID or a
 * transient process-check failure cannot fold a live shard away.
 */
export const ARCHIVE_ABANDONED_ACTIVE_MS = 24 * 60 * 60 * 1000;

/** How often a running maintenance pass refreshes the lock's mtime. */
export const STATS_MAINTENANCE_HEARTBEAT_MS = 5 * 60 * 1000;

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

/** In-memory archive contents (and 2.9.0's on-disk format). */
export type StatsArchiveV1 = {
  version: 1;
  kind: "pi-cache-optimizer-archive";
  updatedAt: number;
  /** instanceId -> time it was folded in. */
  absorbed: Record<string, number>;
  buckets: StatsArchiveBucketV1[];
  routed: StatsArchiveRoutedV1[];
};

/**
 * On-disk generation format: the same entries as v1 with every string
 * interned in one table, so a bucket is one short row of numbers.
 * models rows: [provider, modelId, modelName | -1, api | -1] (string indexes).
 * buckets rows: [globalEpoch, sessionHash, model, modelEpoch, day, updatedAt,
 *   stats.day, totalRequests, hitRequests, cachedInputTokens,
 *   cacheWriteInputTokens, totalInputTokens] (strings and model are indexes).
 * routed rows: [globalEpoch, sessionHash, day, updatedAt, provider, id, name | -1].
 */
export type StatsArchiveV2Document = {
  version: 2;
  kind: "pi-cache-optimizer-archive";
  generation: number;
  updatedAt: number;
  /** sha256 of the 2.9.0 archive.json this archive was migrated from. */
  legacySha256?: string;
  absorbed: Record<string, number>;
  strings: string[];
  models: number[][];
  buckets: number[][];
  routed: number[][];
};

const DAY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const GENERATION_FILE_PATTERN = /^archive\.(\d{1,15})\.json$/;

export function emptyStatsArchive(now = Date.now()): StatsArchiveV1 {
  return { version: 1, kind: "pi-cache-optimizer-archive", updatedAt: now, absorbed: {}, buckets: [], routed: [] };
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function parseArchiveBucket(raw: unknown): StatsArchiveBucketV1 | undefined {
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
  ) return undefined;
  return {
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
  };
}

function parseArchiveRouted(raw: unknown): StatsArchiveRoutedV1 | undefined {
  const entry = asRecord(raw);
  const model = parsePersistedRoutedModelRef(entry?.model);
  if (
    !entry || !model ||
    !isNonEmptyString(entry.globalEpoch) || !isNonEmptyString(entry.sessionHash) ||
    typeof entry.day !== "string" || !DAY_PATTERN.test(entry.day) ||
    !isFiniteNumber(entry.updatedAt)
  ) return undefined;
  return { globalEpoch: entry.globalEpoch, sessionHash: entry.sessionHash, day: entry.day, updatedAt: entry.updatedAt, model };
}

function parseAbsorbed(value: unknown): Record<string, number> | undefined {
  const rawAbsorbed = asRecord(value);
  if (!rawAbsorbed) return undefined;
  const absorbed: Record<string, number> = {};
  for (const [id, at] of Object.entries(rawAbsorbed)) {
    if (isNonEmptyString(id) && isFiniteNumber(at)) absorbed[id] = at;
  }
  return absorbed;
}

/** Strict parse of 2.9.0's format: undefined when the document is not a v1 archive. Invalid entries are dropped. */
export function parseStatsArchive(value: unknown): StatsArchiveV1 | undefined {
  const record = asRecord(value);
  if (!record || record.version !== 1 || record.kind !== "pi-cache-optimizer-archive") return undefined;
  const absorbed = parseAbsorbed(record.absorbed);
  if (!absorbed || !Array.isArray(record.buckets) || !Array.isArray(record.routed)) return undefined;
  const buckets: StatsArchiveBucketV1[] = [];
  for (const raw of record.buckets) {
    const bucket = parseArchiveBucket(raw);
    if (bucket) buckets.push(bucket);
  }
  const routed: StatsArchiveRoutedV1[] = [];
  for (const raw of record.routed) {
    const entry = parseArchiveRouted(raw);
    if (entry) routed.push(entry);
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

/** Encode an archive in the compact generation format. */
export function encodeStatsArchiveV2(archive: StatsArchiveV1, generation: number, legacySha256?: string): StatsArchiveV2Document {
  const strings: string[] = [];
  const stringIndex = new Map<string, number>();
  const str = (value: string): number => {
    let index = stringIndex.get(value);
    if (index === undefined) {
      index = strings.length;
      strings.push(value);
      stringIndex.set(value, index);
    }
    return index;
  };
  const optional = (value: string | undefined): number => (value === undefined ? -1 : str(value));
  const models: number[][] = [];
  const modelIndex = new Map<string, number>();
  const model = (bucket: StatsArchiveBucketV1): number => {
    const row = [str(bucket.provider), str(bucket.modelId), optional(bucket.modelName), optional(bucket.api)];
    const id = row.join(",");
    let index = modelIndex.get(id);
    if (index === undefined) {
      index = models.length;
      models.push(row);
      modelIndex.set(id, index);
    }
    return index;
  };
  const buckets = archive.buckets.map((bucket) => [
    str(bucket.globalEpoch), str(bucket.sessionHash), model(bucket), str(bucket.modelEpoch), str(bucket.day), bucket.updatedAt,
    str(bucket.stats.day), bucket.stats.totalRequests, bucket.stats.hitRequests, bucket.stats.cachedInputTokens,
    bucket.stats.cacheWriteInputTokens, bucket.stats.totalInputTokens,
  ]);
  const routed = archive.routed.map((entry) => [
    str(entry.globalEpoch), str(entry.sessionHash), str(entry.day), entry.updatedAt,
    str(entry.model.provider), str(entry.model.id), optional(entry.model.name),
  ]);
  return {
    version: 2,
    kind: "pi-cache-optimizer-archive",
    generation,
    updatedAt: archive.updatedAt,
    ...(legacySha256 ? { legacySha256 } : {}),
    absorbed: archive.absorbed,
    strings,
    models,
    buckets,
    routed,
  };
}

/**
 * Strict parse of a generation file: undefined when the document itself is
 * not a v2 archive. Rows with bad indexes or values are dropped, applying the
 * same per-entry validation as v1.
 */
export function parseStatsArchiveV2(value: unknown): { archive: StatsArchiveV1; generation: number; legacySha256?: string } | undefined {
  const record = asRecord(value);
  if (!record || record.version !== 2 || record.kind !== "pi-cache-optimizer-archive") return undefined;
  if (!Number.isSafeInteger(record.generation) || (record.generation as number) < 1) return undefined;
  const absorbed = parseAbsorbed(record.absorbed);
  const { strings, models, buckets: rawBuckets, routed: rawRouted } = record;
  if (
    !absorbed || !Array.isArray(strings) || !Array.isArray(models) || !Array.isArray(rawBuckets) || !Array.isArray(rawRouted) ||
    !strings.every((entry) => typeof entry === "string")
  ) return undefined;
  const str = (index: unknown): string | undefined =>
    Number.isInteger(index) && (index as number) >= 0 && (index as number) < strings.length ? strings[index as number] as string : undefined;
  const row = (value: unknown, length: number): unknown[] | undefined => (Array.isArray(value) && value.length === length ? value : undefined);

  const modelTable = models.map((raw) => {
    const entry = row(raw, 4);
    return entry ? { provider: str(entry[0]), modelId: str(entry[1]), modelName: str(entry[2]), api: str(entry[3]) } : undefined;
  });
  const buckets: StatsArchiveBucketV1[] = [];
  for (const raw of rawBuckets) {
    const entry = row(raw, 12);
    if (!entry) continue;
    const modelEntry = Number.isInteger(entry[2]) ? modelTable[entry[2] as number] : undefined;
    if (!modelEntry) continue;
    const bucket = parseArchiveBucket({
      globalEpoch: str(entry[0]),
      sessionHash: str(entry[1]),
      modelKey: `${modelEntry.provider}/${modelEntry.modelId}`,
      modelEpoch: str(entry[3]),
      day: str(entry[4]),
      provider: modelEntry.provider,
      modelId: modelEntry.modelId,
      modelName: modelEntry.modelName,
      api: modelEntry.api,
      updatedAt: entry[5],
      stats: {
        day: str(entry[6]), totalRequests: entry[7], hitRequests: entry[8], cachedInputTokens: entry[9],
        cacheWriteInputTokens: entry[10], totalInputTokens: entry[11],
      },
    });
    if (bucket) buckets.push(bucket);
  }
  const routed: StatsArchiveRoutedV1[] = [];
  for (const raw of rawRouted) {
    const entry = row(raw, 7);
    if (!entry) continue;
    const parsed = parseArchiveRouted({
      globalEpoch: str(entry[0]),
      sessionHash: str(entry[1]),
      day: str(entry[2]),
      updatedAt: entry[3],
      model: { provider: str(entry[4]), id: str(entry[5]), name: str(entry[6]) },
    });
    if (parsed) routed.push(parsed);
  }
  return {
    archive: {
      version: 1,
      kind: "pi-cache-optimizer-archive",
      updatedAt: isFiniteNumber(record.updatedAt) ? record.updatedAt : 0,
      absorbed,
      buckets,
      routed,
    },
    generation: record.generation as number,
    ...(isNonEmptyString(record.legacySha256) ? { legacySha256: record.legacySha256 } : {}),
  };
}

export function statsArchiveGenerationPath(archivePath: string, generation: number): string {
  return join(dirname(archivePath), `archive.${String(generation).padStart(12, "0")}.json`);
}

/** Published generations next to `archivePath`, ascending. */
export async function listStatsArchiveGenerations(archivePath: string = SHARD_ARCHIVE_PATH): Promise<number[]> {
  let names: string[];
  try {
    names = await readdir(dirname(archivePath));
  } catch (error) {
    if (getErrorCode(error) === "ENOENT") return [];
    throw error;
  }
  const generations: number[] = [];
  for (const name of names) {
    const match = GENERATION_FILE_PATTERN.exec(name);
    if (match) generations.push(Number(match[1]));
  }
  return generations.sort((a, b) => a - b);
}

function bucketToSyntheticShard(bucket: StatsArchiveBucketV1, index: number): PersistedStatsShardV7 {
  const shard = syntheticShardBase(`archive-bucket-${index}`, bucket.sessionHash, bucket.day, bucket.globalEpoch, bucket.updatedAt);
  shard.models[bucket.modelKey] = {
    modelEpoch: bucket.modelEpoch,
    provider: bucket.provider,
    modelId: bucket.modelId,
    ...(bucket.modelName ? { modelName: bucket.modelName } : {}),
    ...(bucket.api ? { api: bucket.api } : {}),
    stats: cloneCacheStats(bucket.stats),
  };
  return shard;
}

function routedToSyntheticShard(entry: StatsArchiveRoutedV1, index: number): PersistedStatsShardV7 {
  const shard = syntheticShardBase(`archive-routed-${index}`, entry.sessionHash, entry.day, entry.globalEpoch, entry.updatedAt);
  shard.lastRoutedModel = { ...entry.model };
  return shard;
}

function syntheticShardBase(id: string, sessionHash: string, day: string, globalEpoch: string, updatedAt: number): PersistedStatsShardV7 {
  return {
    version: 7,
    kind: "pi-cache-optimizer-shard",
    instanceId: id,
    sessionHash,
    process: { pid: 0, ppid: 0, instanceStartedAt: 0 },
    lifecycle: { state: "closed", createdAt: updatedAt, updatedAt, closedAt: updatedAt },
    day,
    globalEpoch,
    models: {},
  };
}

/**
 * Archive entries as synthetic closed shards: the reference semantics.
 * Aggregating these (after the raw shards) is what the archive must be
 * equivalent to; readers use StatsArchiveView's reduced forms instead.
 */
export function archiveToSyntheticShards(archive: StatsArchiveV1): PersistedStatsShardV7[] {
  return [
    ...archive.buckets.map(bucketToSyntheticShard),
    ...archive.routed.map(routedToSyntheticShard),
  ];
}

export type LoadedStatsArchive = {
  /** False when no archive exists; true for a parsed (or unreadable) one. */
  present: boolean;
  /** True when an archive exists but cannot be parsed; compaction then refuses to overwrite it. */
  corrupt: boolean;
  /** Generation this snapshot came from; 0 for 2.9.0's archive.json or no archive. */
  generation: number;
  archive: StatsArchiveV1;
  absorbedIds: ReadonlySet<string>;
  /** Every archive entry as a synthetic shard (reference form, built on first access). Do not mutate. */
  readonly shards: readonly PersistedStatsShardV7[];
};

type BucketGroup = {
  /** Position in ReducedArchive.groups. */
  order: number;
  globalEpoch: string;
  sessionHash: string;
  key: string;
  modelEpoch: string;
  /** The single bucket's own stats until a second bucket is merged (then a private copy). */
  stats: CacheStats;
  count: number;
  /** First bucket (in archive order) with the group's newest updatedAt. */
  bestUpdatedAt: number;
  bestIndex: number;
  /** Index of the first bucket an aggregation could record a ref from (updatedAt > -1); Infinity if none. */
  firstEligibleIndex: number;
};

type RoutedGroup = {
  globalEpoch: string;
  sessionHash: string;
  bestUpdatedAt: number;
  model: PersistedRoutedModelRef;
  /** Index of the first entry an aggregation could pick (updatedAt > -1); Infinity if none. */
  firstEligibleIndex: number;
};

type ReducedArchive = {
  /** Buckets folded per (globalEpoch, session, model, modelEpoch), in order of first appearance. */
  groups: BucketGroup[];
  routed: RoutedGroup[];
  /** The same groups per session (each list in archive order). */
  groupsBySession: Map<string, BucketGroup[]>;
  routedBySession: Map<string, RoutedGroup[]>;
  /** Model keys with buckets under each global epoch, sorted. */
  keysByGlobalEpoch: Map<string, string[]>;
};

/**
 * One parsed archive snapshot, cached per file identity. Besides the raw
 * entries it lazily builds what a refresh needs: the per-day synthetic shards
 * for the current-day view (archived days are past days, so normally none)
 * and, per set of current epochs, the all-days aggregate pre-reduced to one
 * entry per (session, model).
 */
export class StatsArchiveView implements LoadedStatsArchive {
  readonly absorbedIds: ReadonlySet<string>;
  private syntheticShards?: PersistedStatsShardV7[];
  private dayIndex?: Map<string, { buckets: number[]; routed: number[] }>;
  private dayShards?: { day: string; shards: readonly PersistedStatsShardV7[] };
  private reduced?: ReducedArchive;
  private contributionCache?: { fingerprint: string; contribution: ShardContribution };

  constructor(
    readonly archive: StatsArchiveV1,
    readonly present: boolean,
    readonly corrupt: boolean,
    readonly generation: number,
    /** For 2.9.0's archive.json: its sha256; for a generation: the legacy file it was migrated from. */
    readonly legacySha256?: string,
  ) {
    this.absorbedIds = new Set(Object.keys(archive.absorbed));
  }

  get shards(): readonly PersistedStatsShardV7[] {
    return this.syntheticShards ??= archiveToSyntheticShards(this.archive);
  }

  /** The synthetic shards of `day`, in archive order. Do not mutate. */
  shardsForDay(day: string): readonly PersistedStatsShardV7[] {
    if (this.dayShards?.day === day) return this.dayShards.shards;
    if (!this.dayIndex) {
      this.dayIndex = new Map();
      const slot = (entryDay: string) => {
        let entry = this.dayIndex!.get(entryDay);
        if (!entry) {
          entry = { buckets: [], routed: [] };
          this.dayIndex!.set(entryDay, entry);
        }
        return entry;
      };
      this.archive.buckets.forEach((bucket, index) => slot(bucket.day).buckets.push(index));
      this.archive.routed.forEach((entry, index) => slot(entry.day).routed.push(index));
    }
    const indexes = this.dayIndex.get(day);
    const shards = indexes
      ? [
        ...indexes.buckets.map((index) => bucketToSyntheticShard(this.archive.buckets[index]!, index)),
        ...indexes.routed.map((index) => routedToSyntheticShard(this.archive.routed[index]!, index)),
      ]
      : [];
    this.dayShards = { day, shards };
    return shards;
  }

  private reduce(): ReducedArchive {
    if (this.reduced) return this.reduced;
    const groups = new Map<string, BucketGroup>();
    const keys = new Map<string, Set<string>>();
    this.archive.buckets.forEach((bucket, index) => {
      const id = JSON.stringify([bucket.globalEpoch, bucket.sessionHash, bucket.modelKey, bucket.modelEpoch]);
      const group = groups.get(id);
      if (!group) {
        groups.set(id, {
          order: groups.size,
          globalEpoch: bucket.globalEpoch,
          sessionHash: bucket.sessionHash,
          key: bucket.modelKey,
          modelEpoch: bucket.modelEpoch,
          stats: bucket.stats,
          count: 1,
          bestUpdatedAt: bucket.updatedAt,
          bestIndex: index,
          firstEligibleIndex: bucket.updatedAt > -1 ? index : Infinity,
        });
        let set = keys.get(bucket.globalEpoch);
        if (!set) keys.set(bucket.globalEpoch, set = new Set());
        set.add(bucket.modelKey);
        return;
      }
      if (group.count === 1) group.stats = cloneCacheStats(group.stats);
      mergeStatsAcrossDays(group.stats, bucket.stats, null);
      group.count++;
      if (group.firstEligibleIndex === Infinity && bucket.updatedAt > -1) group.firstEligibleIndex = index;
      if (bucket.updatedAt > group.bestUpdatedAt) {
        group.bestUpdatedAt = bucket.updatedAt;
        group.bestIndex = index;
      }
    });
    const routed = new Map<string, RoutedGroup>();
    this.archive.routed.forEach((entry, index) => {
      const id = JSON.stringify([entry.globalEpoch, entry.sessionHash]);
      const group = routed.get(id);
      const eligible = entry.updatedAt > -1 ? index : Infinity;
      if (!group) {
        routed.set(id, { globalEpoch: entry.globalEpoch, sessionHash: entry.sessionHash, bestUpdatedAt: entry.updatedAt, model: entry.model, firstEligibleIndex: eligible });
        return;
      }
      if (group.firstEligibleIndex === Infinity) group.firstEligibleIndex = eligible;
      if (entry.updatedAt > group.bestUpdatedAt) {
        group.bestUpdatedAt = entry.updatedAt;
        group.model = entry.model;
      }
    });
    const bySession = <T extends { sessionHash: string }>(items: T[]): Map<string, T[]> => {
      const map = new Map<string, T[]>();
      for (const item of items) {
        let list = map.get(item.sessionHash);
        if (!list) map.set(item.sessionHash, list = []);
        list.push(item);
      }
      return map;
    };
    const groupList = Array.from(groups.values());
    const routedList = Array.from(routed.values());
    this.reduced = {
      groups: groupList,
      routed: routedList,
      groupsBySession: bySession(groupList),
      routedBySession: bySession(routedList),
      keysByGlobalEpoch: new Map(Array.from(keys, ([epoch, set]) => [epoch, Array.from(set).sort()])),
    };
    return this.reduced;
  }

  /**
   * The archive's share of an all-days aggregate under the aggregator's
   * epochs. Equivalent to adding `shards` (only those of `sessions`, when
   * given) at that point of the sequence; cached until an epoch, the
   * session selection, or the archive changes.
   */
  async contribution(aggregator: Pick<ShardAggregator, "globalEpoch" | "modelEpoch">, only?: ReadonlySet<string>): Promise<ShardContribution> {
    const reduced = this.reduce();
    const globalEpoch = aggregator.globalEpoch;
    let groupList = reduced.groups;
    let routedList = reduced.routed;
    let keys = reduced.keysByGlobalEpoch.get(globalEpoch) ?? [];
    if (only) {
      groupList = Array.from(only, (session) => reduced.groupsBySession.get(session) ?? []).flat()
        .sort((a, b) => a.order - b.order);
      routedList = Array.from(only, (session) => reduced.routedBySession.get(session) ?? []).flat();
      keys = Array.from(new Set(groupList.filter((group) => group.globalEpoch === globalEpoch).map((group) => group.key))).sort();
    }
    const epochs = new Map<string, string>();
    for (const key of keys) epochs.set(key, await aggregator.modelEpoch(key));
    const fingerprint = JSON.stringify([globalEpoch, ...keys.flatMap((key) => [key, epochs.get(key)]), only ? Array.from(only).sort() : null]);
    if (this.contributionCache?.fingerprint === fingerprint) return this.contributionCache.contribution;

    type SessionOut = { sessionHash: string; instances: number; models: Array<{ key: string; stats: CacheStats; instances: number }> };
    type ModelOut = { key: string; stats: CacheStats; instances: number; sessions: string[]; refUpdatedAt: number; refIndex: number; firstEligibleIndex: number };
    const sessions = new Map<string, SessionOut>();
    const models = new Map<string, ModelOut>();
    for (const group of groupList) {
      if (group.globalEpoch !== globalEpoch || group.modelEpoch !== epochs.get(group.key)) continue;
      let session = sessions.get(group.sessionHash);
      if (!session) sessions.set(group.sessionHash, session = { sessionHash: group.sessionHash, instances: 0, models: [] });
      session.models.push({ key: group.key, stats: group.stats, instances: group.count });
      session.instances += group.count;
      const model = models.get(group.key);
      if (!model) {
        models.set(group.key, {
          key: group.key,
          stats: cloneCacheStats(group.stats),
          instances: group.count,
          sessions: [group.sessionHash],
          refUpdatedAt: group.bestUpdatedAt,
          refIndex: group.bestIndex,
          firstEligibleIndex: group.firstEligibleIndex,
        });
        continue;
      }
      model.firstEligibleIndex = Math.min(model.firstEligibleIndex, group.firstEligibleIndex);
      mergeStatsAcrossDays(model.stats, group.stats, null);
      model.instances += group.count;
      model.sessions.push(group.sessionHash);
      // The sequential aggregation keeps the first shard with the newest updatedAt.
      if (group.bestUpdatedAt > model.refUpdatedAt || (group.bestUpdatedAt === model.refUpdatedAt && group.bestIndex < model.refIndex)) {
        model.refUpdatedAt = group.bestUpdatedAt;
        model.refIndex = group.bestIndex;
      }
    }
    const routed = routedList
      .filter((group) => group.globalEpoch === globalEpoch && group.firstEligibleIndex !== Infinity)
      .sort((a, b) => a.firstEligibleIndex - b.firstEligibleIndex)
      .map((group) => ({ sessionHash: group.sessionHash, updatedAt: group.bestUpdatedAt, model: group.model }));
    const contribution: ShardContribution = {
      sessions: Array.from(sessions.values()),
      models: Array.from(models.values(), ({ key, stats, instances, sessions: list }) => ({ key, stats, instances, sessions: list })),
      refs: Array.from(models.values())
        .filter((model) => model.firstEligibleIndex !== Infinity)
        .sort((a, b) => a.firstEligibleIndex - b.firstEligibleIndex)
        .map((model) => {
          const bucket = this.archive.buckets[model.refIndex]!;
          return { key: model.key, updatedAt: model.refUpdatedAt, ref: { provider: bucket.provider, id: bucket.modelId, name: bucket.modelName ?? bucket.modelId } };
        }),
      routed,
    };
    this.contributionCache = { fingerprint, contribution };
    return contribution;
  }
}

/**
 * Aggregate `leading` shards, then the archive, then `trailing` shards: the
 * same result as aggregateStatsShardsV7([...leading, ...archive.shards,
 * ...trailing], day), without materializing the archive. With `sessions`,
 * only shards of those sessions take part. The current-day view only sees
 * archive entries of that day; the all-days view merges the archive's
 * pre-reduced contribution.
 */
export async function aggregateStatsWithArchiveV7(
  leading: readonly PersistedStatsShardV7[],
  archive: StatsArchiveView,
  trailing: readonly PersistedStatsShardV7[] = [],
  day: string | null = currentLocalDay(),
  sessions?: ReadonlySet<string>,
): Promise<ShardAggregate> {
  const aggregator = await createShardAggregatorV7(day);
  const include = (shard: PersistedStatsShardV7) => !sessions || sessions.has(shard.sessionHash);
  for (const shard of leading) if (include(shard)) await aggregator.add(shard);
  if (day === null) aggregator.addContribution(await archive.contribution(aggregator, sessions));
  else for (const shard of archive.shardsForDay(day)) if (include(shard)) await aggregator.add(shard);
  for (const shard of trailing) if (include(shard)) await aggregator.add(shard);
  return aggregator.finish();
}

type ArchiveSource =
  | { kind: "none" }
  | { kind: "invalid"; path: string }
  | { kind: "legacy"; path: string; info: import("node:fs").Stats }
  | { kind: "generation"; path: string; generation: number; info: import("node:fs").Stats };

const SOURCE_ATTEMPTS = 5;

/**
 * The newest snapshot: the highest generation, or 2.9.0's archive.json while
 * no generation exists. Retries when a listed generation disappears (it was
 * superseded and cleaned up meanwhile). Throws on other I/O errors.
 */
async function locateArchiveSource(archivePath: string): Promise<ArchiveSource> {
  for (let attempt = 0; attempt < SOURCE_ATTEMPTS; attempt++) {
    const generations = await listStatsArchiveGenerations(archivePath);
    const generation = generations.at(-1);
    if (generation !== undefined) {
      const path = statsArchiveGenerationPath(archivePath, generation);
      try {
        const info = await lstat(path);
        if (!info.isFile() || info.isSymbolicLink()) return { kind: "invalid", path };
        return { kind: "generation", path, generation, info };
      } catch (error) {
        if (getErrorCode(error) === "ENOENT") continue;
        throw error;
      }
    }
    try {
      const info = await lstat(archivePath);
      if (info.isFile() && !info.isSymbolicLink()) return { kind: "legacy", path: archivePath, info };
      // The migration tombstone is created only after a generation exists:
      // the listing raced a cleanup, or the generations were removed by hand.
      if (info.isDirectory()) continue;
      return { kind: "invalid", path: archivePath };
    } catch (error) {
      if (getErrorCode(error) === "ENOENT") return { kind: "none" };
      throw error;
    }
  }
  return { kind: "none" };
}

type CachedArchive = { path: string; ino: number | bigint; size: number; mtimeMs: number; view: StatsArchiveView };
const archiveCache = new Map<string, CachedArchive>();

export function clearStatsArchiveCache(): void {
  archiveCache.clear();
}

const emptyView = (): StatsArchiveView => new StatsArchiveView(emptyStatsArchive(0), false, false, 0);
const corruptView = (): StatsArchiveView => new StatsArchiveView(emptyStatsArchive(0), true, true, 0);

function sha256(text: string | Buffer): string {
  return createHash("sha256").update(text).digest("hex");
}

function parseArchiveFile(source: Extract<ArchiveSource, { kind: "legacy" | "generation" }>, bytes: Buffer): StatsArchiveView {
  let value: unknown;
  try {
    value = JSON.parse(bytes.toString("utf8"));
  } catch {
    return corruptView();
  }
  if (source.kind === "legacy") {
    const archive = parseStatsArchive(value);
    return archive ? new StatsArchiveView(archive, true, false, 0, sha256(bytes)) : corruptView();
  }
  const parsed = parseStatsArchiveV2(value);
  if (!parsed || parsed.generation !== source.generation) return corruptView();
  return new StatsArchiveView(parsed.archive, true, false, parsed.generation, parsed.legacySha256);
}

export type ReadStatsArchiveOptions = {
  /** Throw on a transient I/O failure instead of serving the last good snapshot (compaction). */
  strict?: boolean;
};

/**
 * Read the newest archive snapshot, re-parsing only when the file (path,
 * inode, size, mtime) changed. No archive is an empty one; an invalid one is
 * reported as corrupt and treated as empty, so raw shards stay the source of
 * truth. A transient I/O failure (Windows EPERM/EBUSY while another process
 * or a scanner holds the file) is never cached: the last good snapshot is
 * returned when there is one, otherwise an uncached corrupt result, so the
 * next refresh retries.
 */
export async function readStatsArchiveView(archivePath: string = SHARD_ARCHIVE_PATH, options: ReadStatsArchiveOptions = {}): Promise<StatsArchiveView> {
  const transientFailure = (error: unknown): StatsArchiveView => {
    if (options.strict) throw error;
    const previous = archiveCache.get(archivePath);
    return previous && !previous.view.corrupt ? previous.view : corruptView();
  };
  for (let attempt = 0; attempt < SOURCE_ATTEMPTS; attempt++) {
    let source: ArchiveSource;
    try {
      source = await locateArchiveSource(archivePath);
    } catch (error) {
      return transientFailure(error);
    }
    if (source.kind === "none") {
      archiveCache.delete(archivePath);
      return emptyView();
    }
    if (source.kind === "invalid") {
      archiveCache.delete(archivePath);
      return corruptView();
    }
    const { info } = source;
    const cached = archiveCache.get(archivePath);
    // Generations are immutable, so (path, identity) is a safe cache key. 2.9.0
    // rewrites archive.json in place of the old one, and on filesystems with
    // ino 0 and coarse mtimes (FAT/SMB on Windows) a rewrite of the same size
    // keeps the same identity. Compaction migrates from it and then deletes
    // shards, so it always re-reads that file instead of trusting the cache.
    const cacheable = !(options.strict && source.kind === "legacy");
    if (cacheable && cached && cached.path === source.path && cached.ino === info.ino && cached.size === info.size && cached.mtimeMs === info.mtimeMs) {
      return cached.view;
    }
    let bytes: Buffer;
    try {
      bytes = await readFile(source.path);
    } catch (error) {
      // Superseded and cleaned up (or migrated) between listing and reading.
      if (getErrorCode(error) === "ENOENT") continue;
      // On Windows a generation that is being deleted while someone holds it
      // open fails with EPERM instead; a newer one exists by then.
      if (source.kind === "generation") {
        const newest = await listStatsArchiveGenerations(archivePath).then((list) => list.at(-1), () => undefined);
        if (newest !== undefined && newest > source.generation) continue;
      }
      return transientFailure(error);
    }
    const view = parseArchiveFile(source, bytes);
    archiveCache.set(archivePath, { path: source.path, ino: info.ino, size: info.size, mtimeMs: info.mtimeMs, view });
    return view;
  }
  return transientFailure(new Error(`stats archive ${archivePath} kept changing while being read`));
}

/** Reference-form read (used by tests and tools): the snapshot with synthetic shards available. */
export async function readStatsArchive(archivePath: string = SHARD_ARCHIVE_PATH, options: ReadStatsArchiveOptions = {}): Promise<LoadedStatsArchive> {
  return readStatsArchiveView(archivePath, options);
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

/** Identity of whatever sits at 2.9.0's archive.json path (undefined when nothing does). */
async function legacyArchiveIdentity(archivePath: string): Promise<string | undefined> {
  try {
    const info = await lstat(archivePath);
    return JSON.stringify([String(info.ino), info.size, info.mtimeMs, info.isFile()]);
  } catch (error) {
    if (getErrorCode(error) === "ENOENT") return undefined;
    throw error;
  }
}

/** Test hooks for the publish protocol; production never sets them. */
export type ArchivePublishHooks = {
  /** Runs after every publish check passed, right before the new generation becomes visible. */
  beforeCommit?: () => Promise<void>;
};

/**
 * Publish `generation` atomically and exclusively: the snapshot is written
 * and flushed to a private temp file, then hard-linked to its final name.
 * link() fails with EEXIST when that generation already exists, on Windows
 * and POSIX alike, so of all writers that read generation N exactly one can
 * publish N+1. Resolves false when another writer won.
 */
async function commitStatsArchiveGeneration(archivePath: string, generation: number, text: string, hooks: ArchivePublishHooks): Promise<boolean> {
  await mkdir(dirname(archivePath), { recursive: true });
  const target = statsArchiveGenerationPath(archivePath, generation);
  const tempPath = `${target}.${process.pid}.${Date.now()}.${randomUUID()}.tmp`;
  try {
    const handle = await open(tempPath, "wx");
    try {
      await handle.writeFile(text, "utf8");
      await handle.sync();
    } finally {
      await handle.close();
    }
    await hooks.beforeCommit?.();
    try {
      await link(tempPath, target);
    } catch (error) {
      if (getErrorCode(error) === "EEXIST") return false;
      // A filesystem without hard links (FAT/exFAT) cannot publish safely;
      // the caller reports it and raw shards simply stay where they are.
      throw error;
    }
    return true;
  } finally {
    await unlink(tempPath).catch(() => undefined);
  }
}

/**
 * Drop generations older than the one before `current`. The previous one is
 * kept for readers that listed the directory just before `current` appeared;
 * a reader that still finds its file gone re-lists and reads a newer one.
 */
async function removeSupersededGenerations(archivePath: string, current: number): Promise<void> {
  for (const generation of await listStatsArchiveGenerations(archivePath).catch(() => [] as number[])) {
    if (generation >= current - 1) continue;
    await unlink(statsArchiveGenerationPath(archivePath, generation)).catch(() => undefined);
  }
}

const LEGACY_TOMBSTONE_README =
  "pi-cache-optimizer stores its stats archive in the archive.<generation>.json files next to this directory.\n" +
  "This directory replaces the single archive.json of version 2.9.0 so that 2.9.0 can no longer write it\n" +
  "(it would publish without a compare-and-swap). 2.9.0 reports the archive as unreadable and keeps raw shards.\n" +
  "Do not remove it while any Pi process might still run pi-cache-optimizer 2.9.0.\n";

/**
 * After migrating (or whenever generations exist), make sure 2.9.0 cannot
 * write archive.json: move a file there aside and put a directory in its
 * place. A file whose content is the one this archive was migrated from is
 * simply deleted. Anything else was written by 2.9.0 after the migration
 * (only possible if the directory was removed); it is never counted, because
 * merging it could double count, and is kept for inspection.
 */
async function ensureLegacyArchiveTombstone(archivePath: string, migratedFromSha256: string | undefined): Promise<void> {
  let info: import("node:fs").Stats | undefined;
  try {
    info = await lstat(archivePath);
  } catch (error) {
    if (getErrorCode(error) !== "ENOENT") return;
  }
  if (info?.isDirectory()) return;
  if (info) {
    const retired = `${archivePath}.${process.pid}.${randomUUID()}.retired.tmp`;
    try {
      await renameWithRetry(archivePath, retired);
    } catch {
      return; // retried by the next pass; readers ignore archive.json once a generation exists
    }
    let sha: string | undefined;
    try {
      sha = sha256(await readFile(retired));
    } catch {}
    if (sha !== undefined && sha === migratedFromSha256) {
      await unlink(retired).catch(() => undefined);
    } else {
      const orphan = join(dirname(archivePath), `archive.legacy-${Date.now()}.json`);
      await rename(retired, orphan).catch(() => undefined);
      console.warn(`${LOG_PREFIX}: ignored a stats archive written by an older pi-cache-optimizer after migration; kept as ${orphan}`);
    }
  }
  try {
    await mkdir(archivePath);
    await writeFile(join(archivePath, "README.txt"), LEGACY_TOMBSTONE_README, "utf8");
  } catch {}
}

/** Instance ids of every shard-named file in `directory`, readable or not. */
async function listShardInstanceIds(directory: string): Promise<Set<string>> {
  let names: string[];
  try {
    names = await readdir(directory);
  } catch (error) {
    if (getErrorCode(error) === "ENOENT") return new Set();
    throw error;
  }
  return new Set(names.filter((name) => /^[0-9a-f-]+\.json$/i.test(name)).map((name) => name.slice(0, -".json".length)));
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
   * pass ran (e.g. a stale-lock takeover or a concurrent reset), or another
   * compactor published a newer generation first. No raw shard was deleted
   * and no visible archive was replaced; the next pass retries.
   */
  skippedConcurrent: boolean;
  /** Generation this pass published, when it published one. */
  generation?: number;
};

export type CompactionOptions = {
  /** Owner token from acquireStatsMaintenanceLock; ownership is re-checked before publishing. */
  lockToken?: string;
  /** Test hook: runs after the plan is built, right before the publish checks. */
  beforePublish?: () => Promise<void>;
} & ArchivePublishHooks;

/**
 * Buckets expire by their newest folded shard. Shards of one day can be
 * closed much later (an idle process closes yesterday's shard at its next
 * rollover, possibly weeks later), so buckets are also split by the UTC day
 * of `updatedAt`. Otherwise the earliest shard would outlive its own 60-day
 * retention by as long as the latest one, and archived totals would exceed
 * what the raw shards would have shown. The remaining error is under a day.
 */
const ARCHIVE_EXPIRY_GRANULARITY_MS = 24 * 60 * 60 * 1000;
const bucketId = (b: Pick<StatsArchiveBucketV1, "globalEpoch" | "sessionHash" | "modelKey" | "modelEpoch" | "day" | "updatedAt">): string =>
  JSON.stringify([b.globalEpoch, b.sessionHash, b.modelKey, b.modelEpoch, b.day, Math.floor(b.updatedAt / ARCHIVE_EXPIRY_GRANULARITY_MS)]);
const routedId = (r: Pick<StatsArchiveRoutedV1, "globalEpoch" | "sessionHash">): string =>
  JSON.stringify([r.globalEpoch, r.sessionHash]);

/**
 * Fold archivable shards into the archive, then delete them. Call only while
 * holding the maintenance lock. Order matters for crash safety: the archive
 * (with the absorbed ids) is published before any raw shard is deleted, and
 * only by a pass whose generation is the newest one.
 */
export async function compactStatsShardsV7(
  now = Date.now(),
  directory: string = SHARD_FILES_DIR,
  archivePath: string = SHARD_ARCHIVE_PATH,
  options: CompactionOptions = {},
): Promise<CompactionResult> {
  const result: CompactionResult = { absorbed: 0, removed: 0, dropped: 0, skippedCorrupt: false, skippedConcurrent: false };
  await cleanupStaleArchiveTemps(archivePath, now);
  // Taken before the read so a 2.9.0 write racing it is noticed below.
  const legacyIdentityAtRead = await legacyArchiveIdentity(archivePath);
  // The snapshot this pass builds on. Generations are immutable, so the
  // readers' cached parse is safe to reuse; the publish below is a
  // compare-and-swap against exactly this generation.
  const base = await readStatsArchiveView(archivePath, { strict: true });
  if (base.corrupt) {
    result.skippedCorrupt = true;
    return result;
  }
  // 2.9.0's archive.json: publish it as the first generation even when
  // nothing else changed, so 2.9.0 can be locked out of writing it.
  const migrating = base.present && base.generation === 0;
  const previous = base.archive;
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
  // Keep ids whose raw file still exists (a crash or a failed delete left it).
  // Presence is decided by the directory listing, not by a successful parse:
  // a leftover that is only transiently unreadable (Windows EPERM/EBUSY while
  // a scanner or sync client holds it) must stay absorbed, or it would be
  // counted raw again once readable and then folded into the archive twice.
  const presentIds = await listShardInstanceIds(directory);
  for (const shard of shards) presentIds.add(shard.instanceId);
  const absorbed: Record<string, number> = {};
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
      const id = bucketId({ globalEpoch, sessionHash: shard.sessionHash, modelKey: key, modelEpoch: entry.modelEpoch, day: shard.day, updatedAt: shard.lifecycle.updatedAt });
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

  await touchStatsMaintenanceLock(options.lockToken);

  const absorbedChanged =
    Object.keys(absorbed).length !== Object.keys(previous.absorbed).length ||
    Object.keys(absorbed).some((id) => !(id in previous.absorbed));
  const skip = (): CompactionResult => {
    result.skippedConcurrent = true;
    result.absorbed = 0;
    result.dropped = 0;
    return result;
  };
  if (result.absorbed > 0 || result.dropped > 0 || absorbedChanged || migrating) {
    await options.beforePublish?.();
    // Cheap early exits: lost the lock, someone published meanwhile, or a
    // reset landed during the pass (counters written under the new epoch
    // would be dropped as stale). The compare-and-swap below is what makes
    // a stale holder safe; these only avoid wasted work.
    let concurrent =
      !(await holdsStatsMaintenanceLock(options.lockToken)) ||
      ((await listStatsArchiveGenerations(archivePath)).at(-1) ?? 0) !== base.generation ||
      (base.generation === 0 && (await legacyArchiveIdentity(archivePath)) !== legacyIdentityAtRead) ||
      (await readGlobalStatsEpochStrict()) !== globalEpoch;
    for (const [key, epoch] of modelEpochs) {
      if (concurrent) break;
      if ((await readModelStatsEpochStrict(key)) !== epoch) concurrent = true;
    }
    if (concurrent) return skip();
    const next: StatsArchiveV1 = {
      version: 1,
      kind: "pi-cache-optimizer-archive",
      updatedAt: now,
      absorbed,
      buckets: Array.from(buckets.values()),
      routed: Array.from(routed.values()),
    };
    const generation = base.generation + 1;
    const text = JSON.stringify(encodeStatsArchiveV2(next, generation, base.legacySha256)) + "\n";
    if (!(await commitStatsArchiveGeneration(archivePath, generation, text, options))) return skip();
    // A generation above ours means ours is invisible to readers (only
    // possible after our base was superseded and cleaned up while this pass
    // was suspended): it must not delete what its snapshot covers.
    if (((await listStatsArchiveGenerations(archivePath)).at(-1) ?? 0) !== generation) return skip();
    result.generation = generation;
    try {
      const info = await lstat(statsArchiveGenerationPath(archivePath, generation));
      archiveCache.set(archivePath, {
        path: statsArchiveGenerationPath(archivePath, generation),
        ino: info.ino, size: info.size, mtimeMs: info.mtimeMs,
        view: new StatsArchiveView(next, true, false, generation, base.legacySha256),
      });
    } catch {}
    await removeSupersededGenerations(archivePath, generation);
  }
  if (result.generation !== undefined || base.generation > 0) await ensureLegacyArchiveTombstone(archivePath, base.legacySha256);
  await touchStatsMaintenanceLock(options.lockToken);

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
  // Heartbeat so a long pass (a huge archive on a slow, scanned disk) is not
  // taken over as stale. It stops while the process is suspended; the
  // archive compare-and-swap covers that case.
  const heartbeat = setInterval(() => void touchStatsMaintenanceLock(lockToken), STATS_MAINTENANCE_HEARTBEAT_MS);
  heartbeat.unref?.();
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
    clearInterval(heartbeat);
    await releaseStatsMaintenanceLock(lockToken);
  }
  return result;
}

// Build a realistic stats directory: DAYS x PER_DAY shards per day, past days
// compacted into an archive generation, yesterday + today left raw (today has
// 10 active shards), ~2 days of raw shards in total.
// Usage: node build-fixture.mjs <agentDir> [sessionsPerDay=200] [days=60]
// Uses the checkout this script lives in (only its stable on-disk format).
import { mkdir, writeFile, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID, createHash } from "node:crypto";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const { createJiti } = await import(join(REPO, "node_modules", "jiti", "lib", "jiti.mjs"));
const [agentDir, perDayArg = "200", daysArg = "60"] = process.argv.slice(2);
if (!agentDir) throw new Error("usage: node build-fixture.mjs <agentDir> [sessionsPerDay] [days]");
const PER_DAY = Number(perDayArg), DAYS = Number(daysArg);
await rm(agentDir, { recursive: true, force: true });
await mkdir(agentDir, { recursive: true });
process.env.PI_CODING_AGENT_DIR = agentDir;
const jiti = createJiti(join(REPO, "bench.ts"), { interopDefault: false });
const store = await jiti.import(join(REPO, "src/stats-store.ts"));
const archive = await jiti.import(join(REPO, "src/stats-archive.ts"));

const MODELS = ["openai/gpt-5.5", "anthropic/claude-sonnet-5", "deepseek/deepseek-v4", "kimi/kimi-k3", "google/gemini-3-pro"];
let seed = 42;
const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
const dayStr = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

function shardFor(day, dayStart, i, state) {
  const sessionHash = createHash("sha256").update(`${day}-${i}`).digest("hex").slice(0, 16);
  const models = {};
  const n = rnd() < 0.3 ? 2 : 1;
  for (let m = 0; m < n; m++) {
    const key = MODELS[Math.floor(rnd() * MODELS.length)];
    const [provider, modelId] = key.split("/");
    const total = 1 + Math.floor(rnd() * 80);
    const hits = Math.floor(total * rnd());
    const input = total * 30000;
    models[key] = {
      modelEpoch: `initial:model:${key}`, provider, modelId, modelName: modelId, api: "openai-completions",
      stats: { day, totalRequests: total, hitRequests: hits, cachedInputTokens: Math.floor(input * 0.6), cacheWriteInputTokens: 0, totalInputTokens: input },
    };
  }
  const createdAt = dayStart + Math.floor(rnd() * 20 * 3600e3);
  const updatedAt = createdAt + 3600e3;
  const instanceId = randomUUID();
  return {
    version: 7, kind: "pi-cache-optimizer-shard", instanceId, sessionHash,
    process: { pid: 999999, ppid: 1, instanceStartedAt: createdAt },
    lifecycle: { state, createdAt, updatedAt, ...(state === "closed" ? { closedAt: updatedAt } : {}) },
    day, globalEpoch: "initial:global", models,
    lastRoutedModel: { provider: Object.values(models)[0].provider, id: Object.values(models)[0].modelId, name: Object.values(models)[0].modelId },
  };
}

await mkdir(store.SHARD_FILES_DIR, { recursive: true });
const now = Date.now();
const today = new Date(); today.setHours(0, 0, 0, 0);
async function writeDay(offset, stateFn) {
  const d = new Date(today); d.setDate(d.getDate() - offset);
  const day = dayStr(d);
  const shards = Array.from({ length: PER_DAY }, (_, i) => shardFor(day, d.getTime(), i, stateFn(i)));
  for (const s of shards) {
    // Keep today's closed shards out of the 15-minute hot window (a quiet afternoon).
    const latest = now - 20 * 60e3;
    if (s.lifecycle.updatedAt > latest) { s.lifecycle.updatedAt = latest; if (s.lifecycle.closedAt) s.lifecycle.closedAt = latest; }
    await writeFile(join(store.SHARD_FILES_DIR, `${s.instanceId}.json`), JSON.stringify(s, null, 2) + "\n");
  }
  return shards;
}
const t0 = Date.now();
for (let off = DAYS; off >= 2; off--) await writeDay(off, () => "closed");
const res = await archive.maybeRunStatsMaintenance(now);
console.log("compaction", JSON.stringify(res.compaction), "ms", Date.now() - t0);
await writeDay(1, () => "closed");
await writeDay(0, (i) => (i < 10 ? "active" : "closed"));
// Fresh markers: no maintenance pass during the benchmark.
await writeFile(join(store.SHARD_STATE_DIR, "maintenance", "last-cleanup"), `${now}\n`);
await writeFile(join(store.SHARD_STATE_DIR, "maintenance", "last-compaction"), `${now}\n`);
console.log("fixture ready", agentDir);

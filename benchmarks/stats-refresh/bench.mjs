// End-to-end footer refresh benchmark through the extension's own handlers,
// under a simulated slow filesystem. Compares checkouts (e.g. 2.9.3 vs the
// working tree) on the same fixture.
//
// Usage: node bench.mjs --repo <checkout> --fixture <agentDir> [--delay 0.2]
//          [--read-delay <ms>] [--iterations 40] [--label name] [--jitter 0.02:5]
//
// Scenarios (one JSON line each):
// - steady: awaited model_select (refresh + publish), nothing changed.
// - other-shard-rewritten: another process rewrote one hot (active) shard before each refresh.
// - pi-wait: how long tool_execution_end / agent_settled / message_end hold Pi
//   (2.9.3 awaits a refresh there; 2.10 returns at once).
// The extension runs in "json" mode (no watcher) with a fixed session.
import { counters, resetCounters, setDelay } from "./slowfs.mjs";
import { cp, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    repo: { type: "string" }, fixture: { type: "string" }, delay: { type: "string", default: "0.2" },
    "read-delay": { type: "string" }, iterations: { type: "string", default: "40" }, label: { type: "string" },
    jitter: { type: "string" },
  },
});
if (!values.repo || !values.fixture) throw new Error("usage: node bench.mjs --repo <checkout> --fixture <agentDir> [--delay ms]");
const REPO = resolve(values.repo);
const delay = Number(values.delay);
const readDelay = values["read-delay"] === undefined ? delay : Number(values["read-delay"]);
const ITER = Number(values.iterations);
const [jitterProbability, jitterMs] = (values.jitter ?? "0:0").split(":").map(Number);
const label = values.label ?? REPO;

const dir = `/tmp/pco-stats-bench-${process.pid}`;
await rm(dir, { recursive: true, force: true });
await cp(resolve(values.fixture), dir, { recursive: true });
process.env.PI_CODING_AGENT_DIR = dir;
process.env.PI_CACHE_OPTIMIZER_FOOTER_MODE = "session";
const { createJiti } = await import(join(REPO, "node_modules", "jiti", "lib", "jiti.mjs"));
const jiti = createJiti(join(REPO, "bench.ts"), { interopDefault: false });
const loaded = await jiti.import(join(REPO, "index.ts"));
const I = loaded.__internals_for_tests;
const handlers = new Map();
loaded.default({ on(name, handler) { handlers.set(name, handler); }, registerCommand() {} });
const statuses = [];
const model = {
  provider: "openai", id: "gpt-5.5", name: "gpt-5.5", api: "openai-completions", baseUrl: "https://api.openai.com/v1",
  compat: {}, reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 8192,
};
const ctx = {
  model, mode: "json", hasUI: false,
  sessionManager: { getSessionId: () => "stats-refresh-bench" },
  modelRegistry: { find: () => undefined, getAvailable: () => [], getAll: () => [] },
  ui: { notify() {}, setStatus(_key, value) { statuses.push(value); } },
};
const hr = () => Number(process.hrtime.bigint()) / 1e6;
const pct = (xs, p) => { const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
const round = (x) => +x.toFixed(2);
const settled = async () => { if (I.statsRefreshSettled) await I.statsRefreshSettled(); };

// Startup (restore) without the slow filesystem: only the handler's own time is reported.
const sessionStartAt = hr();
await handlers.get("session_start")({ reason: "startup" }, ctx);
const sessionStartMs = hr() - sessionStartAt;
await I.waitForStatsRestore();
console.log(JSON.stringify({ label, scenario: "session_start-handler", ms: +sessionStartMs.toFixed(3) }));
setDelay(delay, { readFileMs: readDelay, jitterProbability, jitterMs });
const report = (scenario, samples, calls, extra = {}) => console.log(JSON.stringify({
  label, scenario, delay, readDelay, median: round(pct(samples, 0.5)), p95: round(pct(samples, 0.95)), max: round(Math.max(...samples)), calls, ...extra,
}));

// Warm-up (the first tiered refresh after restore is already steady state).
for (let i = 0; i < 3; i++) await handlers.get("model_select")({ model }, ctx);

{
  const samples = [];
  let calls;
  for (let i = 0; i < ITER; i++) {
    resetCounters();
    const start = hr();
    await handlers.get("model_select")({ model }, ctx);
    samples.push(hr() - start);
    calls = { ...counters };
  }
  report("steady", samples, calls);
}

{
  const shardsDir = join(dir, "pi-cache-optimizer-stats.d", "shards");
  const names = [];
  for (const name of (await readdir(shardsDir)).filter((n) => n.endsWith(".json"))) {
    const shard = JSON.parse(await readFile(join(shardsDir, name), "utf8"));
    if (shard.lifecycle.state === "active" && shard.process.pid === 999999) names.push(name);
  }
  setDelay(0);
  const samples = [];
  let calls;
  for (let i = 0; i < ITER; i++) {
    const path = join(shardsDir, names[i % names.length]);
    const shard = JSON.parse(await readFile(path, "utf8"));
    shard.lifecycle.updatedAt = Date.now();
    const key = Object.keys(shard.models)[0];
    shard.models[key].stats.totalRequests += 1;
    shard.models[key].stats.totalInputTokens += 1000;
    await writeFile(`${path}.${i}.tmp`, JSON.stringify(shard, null, 2) + "\n");
    await rename(`${path}.${i}.tmp`, path);
    setDelay(delay, { readFileMs: readDelay, jitterProbability, jitterMs });
    resetCounters();
    const start = hr();
    await handlers.get("model_select")({ model }, ctx);
    samples.push(hr() - start);
    calls = { ...counters };
    setDelay(0);
  }
  setDelay(delay, { readFileMs: readDelay, jitterProbability, jitterMs });
  report("other-shard-rewritten", samples, calls);
}

{
  const message = { message: { role: "assistant", provider: "openai", model: "gpt-5.5", api: "openai-completions", stopReason: "stop", usage: { input: 1000, cacheRead: 500, cacheWrite: 0 } } };
  const waits = { tool_execution_end: [], agent_settled: [], message_end: [] };
  const totals = [];
  for (let i = 0; i < ITER; i++) {
    const start = hr();
    for (const [name, event] of [["message_end", message], ["tool_execution_end", { toolCallId: `t${i}` }], ["agent_settled", {}]]) {
      const t = hr();
      await handlers.get(name)(event, ctx);
      waits[name].push(hr() - t);
    }
    await settled();
    totals.push(hr() - start);
  }
  console.log(JSON.stringify({
    label, scenario: "pi-wait", delay, readDelay,
    handlerMedianMs: Object.fromEntries(Object.entries(waits).map(([k, v]) => [k, round(pct(v, 0.5))])),
    handlerP95Ms: Object.fromEntries(Object.entries(waits).map(([k, v]) => [k, round(pct(v, 0.95))])),
    batchIncludingBackgroundMedianMs: round(pct(totals, 0.5)),
  }));
}

{
  // Another process published a new archive generation (same content, next
  // number): the first refresh after it parses and reduces it.
  const archiveDir = join(dir, "pi-cache-optimizer-stats.d", "archive");
  const generations = (await readdir(archiveDir)).map((name) => /^archive\.(\d+)\.json$/.exec(name)?.[1]).filter(Boolean).map(Number).sort((a, b) => a - b);
  let generation = generations.at(-1);
  const doc = JSON.parse(await readFile(join(archiveDir, `archive.${String(generation).padStart(12, "0")}.json`), "utf8"));
  const samples = [];
  for (let i = 0; i < 5; i++) {
    generation++;
    doc.generation = generation;
    setDelay(0);
    await writeFile(join(archiveDir, `archive.${String(generation).padStart(12, "0")}.json`), JSON.stringify(doc) + "\n");
    setDelay(delay, { readFileMs: readDelay, jitterProbability, jitterMs });
    const start = hr();
    await handlers.get("model_select")({ model }, ctx);
    samples.push(hr() - start);
  }
  report("new-archive-generation", samples, undefined, { bytes: JSON.stringify(doc).length });
}

setDelay(0);
await handlers.get("session_shutdown")({}, ctx);
await rm(dir, { recursive: true, force: true });

// Simulated slow filesystem for Windows-like per-call cost (antivirus-scanned
// AppData). Wraps node:fs/promises so each call occupies one of POOL
// simulated libuv threadpool workers (UV_THREADPOOL_SIZE, default 4) for
// DELAY ms, resolving through a setImmediate spin so sub-millisecond delays
// work and the event loop keeps running. Counts calls per kind.
//
// Import this module FIRST (before the code under test): it patches both the
// fs/promises default export (looked up at call time by 2.10's read facade)
// and, through syncBuiltinESMExports, the named ESM bindings 2.9.3 imports.
import fsp from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";

export const counters = {};
export const delays = { lstat: 0, stat: 0, readdir: 0, readFile: 0, open: 0 };
const POOL = Number(process.env.SLOWFS_POOL ?? 4);
const workerFreeAt = new Array(POOL).fill(0);
const now = () => Number(process.hrtime.bigint()) / 1e6;
let jitter = { probability: 0, ms: 0 };
let seed = 1;
const random = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);

function waitUntil(t) {
  return new Promise((resolve) => {
    const spin = () => (now() >= t ? resolve() : setImmediate(spin));
    spin();
  });
}

for (const name of Object.keys(delays)) {
  const original = fsp[name];
  fsp[name] = async function (...args) {
    counters[name] = (counters[name] ?? 0) + 1;
    let d = delays[name];
    if (d > 0 && jitter.probability > 0 && random() < jitter.probability) d += jitter.ms;
    if (d > 0) {
      let k = 0;
      for (let i = 1; i < POOL; i++) if (workerFreeAt[i] < workerFreeAt[k]) k = i;
      const start = Math.max(now(), workerFreeAt[k]);
      workerFreeAt[k] = start + d;
      await waitUntil(start + d);
    }
    return original.apply(this, args);
  };
}
syncBuiltinESMExports();

/** Per-call delay in ms (readFile/open may differ); optional antivirus-like jitter (e.g. 2% of calls +5 ms). */
export function setDelay(ms, { readFileMs = ms, jitterProbability = 0, jitterMs = 0 } = {}) {
  delays.lstat = ms; delays.stat = ms; delays.readdir = ms; delays.open = readFileMs; delays.readFile = readFileMs;
  jitter = { probability: jitterProbability, ms: jitterMs };
}
export function resetCounters() { for (const k of Object.keys(counters)) delete counters[k]; }

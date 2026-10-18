# Footer stats refresh benchmark (2.10 incremental refresh)

Development-only. Measures one footer refresh end to end through the
extension's handlers on a realistic stats directory, under a simulated slow
filesystem (`slowfs.mjs`: every fs/promises call holds one of 4 simulated
libuv threads for `--delay` ms, the Windows/antivirus cost model; 0.2 ms per
call reproduced the reported 28 ms median of 2.9.3).

```sh
# 1. Fixture: 60 days x 200 sessions/day, past days compacted into one
#    archive generation, ~400 raw shards (yesterday + today, 10 active).
node benchmarks/stats-refresh/build-fixture.mjs /tmp/pco-fixture 200 60

# 2. A 2.9.3 checkout to compare with (no worktree needed):
mkdir -p /tmp/pco-293 && git archive 688878f | tar -x -C /tmp/pco-293 && ln -s "$PWD/node_modules" /tmp/pco-293/node_modules

# 3. Run both at 0.2 and 0.5 ms per call:
for d in 0.2 0.5; do
  node benchmarks/stats-refresh/bench.mjs --repo /tmp/pco-293 --fixture /tmp/pco-fixture --delay $d --label 2.9.3
  node benchmarks/stats-refresh/bench.mjs --repo . --fixture /tmp/pco-fixture --delay $d --label 2.10
  PI_CACHE_OPTIMIZER_STATS_INCREMENTAL=0 node benchmarks/stats-refresh/bench.mjs --repo . --fixture /tmp/pco-fixture --delay $d --label 2.10-killswitch
done
```

Scenarios: `steady` (awaited `model_select`, nothing changed), `other-shard-rewritten`
(another process rewrote one active shard before each refresh), `pi-wait`
(how long `message_end`, `tool_execution_end` and `agent_settled` hold Pi).
`--jitter 0.02:5` adds 5 ms to 2% of calls (antivirus tail).

CI asserts call counts (see `tests/stats-incremental-refresh.test.ts`), never timings.

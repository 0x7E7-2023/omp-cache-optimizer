# Upgrade verification

- Synced origin/master via fast-forward to af4e505 (23 commits).
- Upstream already declares pi-coding-agent ^1.3.0 and pi-server ^1.1.0 with matching lockfile. Ran root npm ci; local executable reports 1.3.0 and npm ls confirms pi-server 1.1.0. No manifest or runtime code changes required.
- .pi/npm only installs pi-subagents 0.24.3; npm ls finds no separate Pi/server installation there. Left this unrelated extension unchanged.
- Global installation untouched. No push or commit performed.
- Full npm run check passes: typecheck, 360/360 tests, diff whitespace, module graph (33 modules, no cycles), package checks (38 files).
- Reproduction command: `env -u PI_CACHE_RETENTION TMPDIR="$PWD/.trellis/.runtime/upgrade-tmp" npm run check` (create the directory first).
- Initial check inherited PI_CACHE_RETENTION=long from the running agent and failed four tests that assume an unset startup value. Unsetting it is limited to the check subprocess; production behavior was not changed.
- Second attempt hit ENOSPC on the full /tmp tmpfs. Used a project-local ignored runtime temporary directory on /home instead; did not delete any user files.

## Adjustment / optimization assessment

No necessary Pi compatibility changes found by installed-version typechecking and the host-contract/regression suite. Do not make speculative production changes solely for this upgrade.

Suggested separate improvement: isolate test startup environment explicitly, while preserving dedicated tests for user-requested long retention; document or configure a writable temporary directory for large Jiti-based test runs. These are test reproducibility improvements, not runtime upgrade blockers.

Spec review: existing quality guidelines already require testing against installed official Pi declarations and host-contract fixtures. No new runtime contract or coding convention requires a spec change for this install-only task.

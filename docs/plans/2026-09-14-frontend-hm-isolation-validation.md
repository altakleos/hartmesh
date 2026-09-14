# Frontend isolation validation

Date: 2026-09-14. Branch: `plan/frontend-hm-isolation`.
Status: implementation and validation complete; inherited performance-budget
failures remain explicitly recorded below.

## Source and integration evidence

- The pure-copy commit `367f321b` has `frontend-hm` tree
  `4b3ee6ca3b806b80026d42dc2f573ea619c7621e`, exactly the seed frontend tree
  at `f916f235beaeaf0f97fd1faf72e89b06643d04d8`.
- The restored reference tree equals
  `0f7d8709d3bbf0be26460b6277fbad9329302243:frontend`, including file modes
  and additions/deletions. Local ignored settings were preserved.
- Root commands, setup, pnpm selection, diagnostics, release version tooling,
  CI, hooks, and Docker builds now select `frontend-hm/`. Runtime names,
  `/app/frontend`, service ports, image repositories, and nginx routes stay
  unchanged. No image was published and no deployment or release pin changed.
- Synthetic Git tests exercise upstream edits/additions/deletions, confirm
  the Hartmesh subtree is unchanged by that merge, and require an explicit
  marker advance. Invalid pins, missing history, staged/worktree drift, mode
  changes, cross-app literal inputs, and escaping symlinks are rejected.
- New tooling regression tests were run failing before implementation.
  The existing pnpm runner's relative-PATH issue was also reproduced before
  fixing executable resolution ahead of the project-directory switch.

## Checks

Local frontend toolchain: Node `22.19.0`, pnpm `10.26.2`; backend Python
`3.12.11`. Docker/Compose and Helm were available. The migration does not
upgrade dependencies or relax test/performance thresholds.

Backend comparisons use disposable Git worktrees with lock-synchronized
environments, including the `opensandbox` extra. The baseline's completed
391-file prefix (9,290 passes, 57 runtime skips, no failures) is retained;
its remaining tests and the entire implementation suite use four
duration-aware shards each. Final offline runs place pytest temporary
databases on `/dev/shm` to avoid the slow disk-backed migration run. They
still exercise file-backed SQLite, not a replacement database implementation.
The blocking-I/O and focused tooling runs also ran with ordinary disk-backed
temporary files.

The final shard command uses `uv run --no-sync pytest` with explicit
`--splits 4 --group N --splitting-algorithm least_duration
--durations-path=.test_durations --basetemp <allocated-directory>` arguments
and the standard non-live/non-blocking test selection. It does **not** export
`UV_NO_SYNC` or `PYTEST_ADDOPTS` into test subprocesses. An earlier invocation
incorrectly exported `UV_NO_SYNC=1`, preventing the extension bootstrap test
from installing its test package; that invocation is not a clean acceptance
run. No product code was changed to accommodate the runner mistake.

| Check | Result |
| --- | --- |
| Seed frontend formatting, lint/type checking, unit tests | Passed; 1,217 unit tests |
| Hartmesh frozen-lockfile install, formatting, lint/type checking, unit tests | Passed; 1,217 unit tests |
| Hartmesh install, checks, unit tests, production build with `frontend/` unavailable | Passed in a disposable export; 1,217 unit tests |
| Mocked browser suite | 164 passed with retries disabled |
| Auth recovery browser suite | 5 passed |
| Real replay Gateway/browser suite | 3 passed |
| Real auth-enabled Gateway/browser suite | 1 passed with retries disabled |
| Focused backend tooling and isolation regressions | 378 passed |
| Additional packaging, Compose/Helm, and release contracts | 112 passed |
| Final boundary/guidance regressions and backend replay golden | 42 passed; replay golden 1 passed |
| Full offline backend baseline and final sweep | Baseline: 17,661 passed; implementation: 17,707 passed; no failures |
| Strict backend blocking-I/O suites | Baseline and implementation: 107 passed each on final runs |
| Backend Ruff lint/format and new root Python scripts | Passed |
| Agent guidance budgets | Passed; five existing soft-limit warnings, no errors |
| Upstream isolation and coordinated version checks | Passed |
| Actual dev/prod Compose config validation with disposable settings | Passed |
| Helm lint/template and deployment contract tests | Passed |
| Dev and production Docker builds | Passed |
| Dev image with Hartmesh source bind mounts and networking disabled | HTTP 200 |
| Production image, UID 1000, networking disabled, read-only root, writable cache tmpfs | HTTP 200; correct Hartmesh package/version |
| Route asset performance budgets | Existing baseline failure; exact same JS/CSS byte counts after the split |

The final backend sweep covers 17,838 selected test items: 17,707 passes and
131 existing runtime skips. The baseline covers 17,792 selected items:
17,661 passes and the same 131 runtime skips. Both also exclude eight
live-marked items and skip seven modules during collection. Deduplicating
those collection skips across the shards gives 138 skips per revision, not
the sum of the repeated per-shard collection counts. Skips include the
explicit PostgreSQL, Kubernetes, and live-provider qualification gates;
those gates remain unpassed, not newly qualified by this migration.

The first browser run exposed an existing intermittent Lark popup check:
the popup's initial external navigation was not mocked. A context-level route
now supplies that test response. The formerly intermittent case passed three
consecutive repetitions without retries, followed by the complete 164-test
suite without retries. No application behavior was changed for that fix.

The first baseline blocking-I/O run had 106 passes and one failure in
`test_abandoned_relay_records_are_drained_by_retried_stop_or_restart`: the
test checks persistence after a fixed 50 ms sleep. The unchanged test passed
on standalone rerun, and the subsequent complete baseline and implementation
blocking suites passed all 107 tests each. No channel code, blocking detector,
or timing threshold was changed. The initial non-isolated offline run also hit the unchanged
checkpointer concurrency test's three-second timeout; its seven-test class
passed in the clean validation checkout. These initial results are retained
in the raw logs, not counted as clean full-suite runs.

The new auth integration test uses an ephemeral, auth-enabled replay Gateway,
initializes its first administrator, registers a normal user, and exercises
browser login, HTTP-only session cookies, SSR cookie forwarding after reload,
CSRF rejection/acceptance, and logout through the frontend's same-origin
proxy. Unlike the existing auth recovery tests, it does not mock API responses.

## Inherited performance failure

`pnpm perf:check` was run on both the seed app in a detached baseline worktree
and the Hartmesh app with the same toolchain. Every measured JS and CSS total
is identical; the following existing budgets fail in both:

| Route / asset | Actual bytes | Existing limit |
| --- | ---: | ---: |
| `/login` JS | 851,870 | 850,000 |
| `/workspace/chats` CSS | 193,816 | 190,000 |
| Demo thread CSS | 193,816 | 190,000 |
| Demo thread JS | 4,104,420 | 4,100,000 |
| `/en/docs` CSS | 272,114 | 270,000 |
| `/blog/posts` CSS | 272,114 | 270,000 |

These are not migration regressions. Budget changes or bundle optimization
remain separate work; the migration leaves `performance-budgets.json`
unchanged. Static HTML lengths can differ with generated documentation
metadata; they are not part of these JS/CSS gates.

## Scope and artifacts

This validation makes no new live-provider, PostgreSQL/Redis HA,
`durable_two_gateway_v1`, Kubernetes deployment, or tenant rollout claim.
Standard suite exclusions and environment-dependent skips must remain
visible; offline/mocked checks do not qualify those production boundaries.

Local raw logs are under `/tmp/frontend-hm-validation.9nOiBJ/` (temporary,
not committed). This report records the durable summary. See the
[implementation plan](2026-09-14-frontend-hm-isolation.md) and
[ownership/sync guide](../FRONTEND_ISOLATION.md) for the maintained contract.

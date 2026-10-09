# AIO Sandbox

`LocalContainerBackend._start_container(start=False)` prepares a stopped Docker
environment with the existing builder. Only host storage admission may activate
it after durable exact-ID registration and mount/containment verification. This
adds no thread identity or lease-based writer proof; see `deerflow/spaces/AGENTS.md`.

Instance SDK clients may use host-owned immutable-container control transport and
explicit admitted download roots. Legacy clients retain their existing root guard.
Keep the same HTTP protocol and SDK; no IP fallback or requester mounts are allowed.

Prewarming admits at most one speculative create per provider, before executor
submission. Busy requests skip immediately; the dedicated one-worker executor
has no admission queue and consumes neither default nor real-acquire workers.
In-process and cross-process locks are nonblocking for speculation. Cancellation
signals the existing readiness stop and drains creation/cleanup; reset and
shutdown close admission and drain the worker before serializer/store teardown.
Capacity, ownership, failed-destroy quarantine and unclaimed reaping still use
the ordinary provider lifecycle. Regressions: `test_sandbox_prewarm.py`.

## Stdin contracts per transport

AIO has two distinct stdin contracts: the persistent `/v1/shell` transport is a
PTY, while `/v1/bash` uses a subprocess pipe that stays open for writes. Keep
persistent shell commands unchanged: the Lark broker shim already ignores TTY
input. Only fresh `_run_bash_exec` calls prefix the original command with
`exec < /dev/null` so default stdin gets immediate EOF; explicit
pipes/heredocs/files still override fd0. Those sessions are created and
released per invocation, so the prefix never closes a reusable terminal or
alters its state. Avoid brace-group/eval wrappers: they shift top-level
parsing (alias/extglob timing), disturb `PIPESTATUS` across calls, can fire an
`ERR` trap an extra time, or append a delimiter a trailing backslash consumes.

The broker shim forwards pipe/file input only after EOF: an idle pipe exits
124 without a broker request, and read errors fail rather than becoming empty
input. Window overrides must be finite positive seconds, at most 600. Broker
INFO logs contain argc/exit/elapsed only, never argument values.

Regressions: `tests/test_aio_sandbox.py`, `tests/test_lark_broker.py`.

## Persistent shell cancellation

Use an explicitly created default shell session from the first call; omitting
`id` creates a fresh AIO session and loses cwd, exports and cancellation targeting.
Only the first command exports a session token and records the shell PID plus
Linux start ticks in a private marker. Later commands stay unchanged. Abort
shortens the session idle wait, kills the matching parent shell before sweeping
its current descendants, then discards the session. Killing the foreground
child first allows trailing statements to run. Before each later call, snapshot existing token-bearing PID/start-tick
identities on a separate control shell. The abort sweep preserves those jobs
and their descendants, including jobs started just before the cancelled call.
Age rounding cannot distinguish adjacent calls. An aborted in-flight command fences recovery,
including missing-session 404s, and an already aborted call cannot dispatch.
Every abort RPC has a timeout and disables retries. Warm-pool release detaches
the drained default shell before closing host sockets; reclaim restores its
id and token from a process-local `SandboxInfo.default_shell_state` that never
appears in persisted metadata. Scoped shells still close with their execution.

Regression: `tests/test_sandbox_command_abort.py` uses real Bash processes for
persistence, trailing statements, old jobs and recovery; `test_aio_sandbox.py`
pins the session lifecycle and unchanged-command contract.

Shell exec is submitted asynchronously and polled within the command deadline;
the final console record supplies this command's output (view output includes
older commands). This lets a cancelled caller drain even when an image's hard
timeout ignores idle changes. The HartMesh image also makes the vendor executor
loop observe its closed flag, so deleted sessions release their server workers;
the real-image smoke check covers a pending command with a hard timeout.

Shell capacity reserves `2 * subagent_runtime.max_running + 3`: persistent
lead/subagent shells, their temporary snapshot controls, and an abort control.
Accepted asynchronous commands never replay on a polling error; missing-session
recovery applies only before dispatch was accepted.

Cancelled callers wait for the abort control's bounded completion before normal
session cleanup. Cleanup itself kills foreground processes and must not overtake
the parent-shell kill, or Bash can execute the rest of the cancelled command.

`execution_outcome_uncertain` is monotonic per SDK client. Lost command/mutating-file
replies and failed session cleanup prevent Work completion confirmation even when
socket close returns. `requires_container_recycle` also covers pending/ambiguous
session creation. Neither flag grants permission to retire a native storage writer.

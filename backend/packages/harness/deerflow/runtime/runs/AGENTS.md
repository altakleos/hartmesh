# AGENTS.md

Scoped guide for `runtime/runs/` — the run manager, the worker, the run and
event stores, and the delivery verdict they commit. Split out of
[`../AGENTS.md`](../AGENTS.md), which owns the rest of the runtime.

### Cancellations written by another process (`runs/manager.py`)

`RunManager.cancel` is the in-process path. A cancellation from outside the
process -- the deployer's `python -m app.gateway.auth.accounts disable`, which
holds the database and no run manager -- is a durable request on the row
(`request_cancel_compat`), and the worker that owns the run has to observe it.

With `run_ownership.heartbeat_enabled`, `_renew_leases` already reads
`cancel_action` on every renewal. Without it -- the single-Gateway deployment a
tenant runs -- nothing read the column at all and the request sat there until
the run ended by itself. `start_cancellation_watch` / `stop_cancellation_watch`
fill exactly that gap and are a no-op wherever the heartbeat runs, so one row
never has two observers. A tick costs one `list_inflight` query, and only while
this process owns an active run, so an idle Gateway asks the database nothing;
the interval is `OUT_OF_BAND_CANCELLATION_POLL_SECONDS` and sets the floor on
how fast the deployer's command can confirm a run stopped. Both paths end in
`_signal_local_cancel`, so the worker's own abort and terminal handling are
unchanged. The Gateway starts and stops the watch alongside the heartbeat in
`app/gateway/deps.py`.

### Cancellation reaches the command (`sandbox/lease.py` + `sandbox/sandbox.py`)

`asyncio.to_thread` cannot interrupt the worker blocked on a sandbox command,
and `run_sync_lifecycle_operation` waits for that worker by design, so a
cancelled run used to wait out whatever the command was doing -- measured on a
deployment: a `sleep 541` and its stream ran on for 537 s after the account was
turned off. Sandbox tool bodies now run through `run_sync_sandbox_command`,
which calls `Sandbox.abort_running_commands(call_id)` before that drain: the
drain still guarantees the worker thread has finished, but the thread returns
because its command is dead rather than because the command ran out its own
budget. The abort runs off the event loop -- an accepted session refuses a
synchronous call on its owner loop, and the call makes network requests.

The scope is the cancelled **call**, carried from the wrapper into the worker
thread in `SANDBOX_COMMAND_CALL` (a context variable, which `to_thread` copies).
One sandbox serves the lead agent and all its subagents, so a sandbox-wide abort
would make a subagent's own timeout kill its siblings' commands and a person's
Stop kill the server they backgrounded three turns ago; cancelling a run cancels
every tool call it has open, so a run still stops all of its own work. `None`
means every command in the sandbox.

Each command carries its own `ABORT_TOKEN_ENV` value, which a child inherits
even after `setsid` takes it out of the shell's process group. `LocalSandbox`
kills each tracked process group and then sweeps `/proc` for that call's tokens,
skipping its own pid. `AioSandbox` calls `shell.kill_process` on each session
running one of them and runs the same sweep in the container on a fresh session;
every abort request carries `_ABORT_REQUEST_TIMEOUT_SECONDS` rather than the
client's 600 s command budget, because the drain behind it cannot be
interrupted. The sweep is one command in a persistent session, so it must never
`exit`: that closed the session, the API never saw the command finish, and
every abort waited out its request timeout (21 s under runsc). Measured on the
released image's slim profile under runsc it returns in 3.2 s at one CPU and
1.3 s at two. Every path that executes a command counts it, `bash.exec` included
-- an uncounted command is one the abort reports nothing for and never reaches,
which is every skill carrying a request-scoped secret -- and a command whose
call was aborted is never rotated-and-retried, or the work just ended would
start again. A provider that cannot reach its commands returns 0 and keeps the
previous behaviour, so the kill is an attempt; only the run's cancellation is a
guarantee. The verb is declared in `sandbox/operations.py` like every other, so
the accepted facade fences it.

A run whose row carries a cancellation is never resumed by an execution
takeover: `GatewayExecutionRecoveryCoordinator` refuses with
`recovery_takeover_cancelled` at both its attachment and safe-point checks, and
the manager detaches the takeover without executing anything, so no effect is
replayed and no cancelled tool attempt is reattached.

### Run Delivery Receipts (`runtime/journal.py` + `runs/worker.py`)

`RunJournal` records each non-empty artifact update once per tool `Command` for
the terminal `run.delivery` event. When a command holds several messages, a
unique tool name resolved from matching `ToolMessage` entries supplies
attribution; additional command messages do not duplicate paths or counts, and
several resolved names leave one flat update counted but unattributed, because
the command carries no per-path mapping. `RunJournal` callbacks set
`run_inline=True`: only in-memory bookkeeping or scheduled async writes, and
staying on the run's event-loop thread serializes parallel tool callbacks
before terminal delivery recording and flushing. Each worker creates a separate journal per run before
cancellable/fallible preflight work, so checkpoint compatibility failures and
cancellation while waiting for prior finalization still emit a zero-delivery
receipt. The worker flushes ordinary journal events, idempotently persists the
run-scoped receipt, and only then persists the staged terminal run status. A
receipt failure is retried on a short bounded schedule while the owning worker
still knows the real outcome and holds the lease. Delivery candidates are every regular
file created or modified under `/mnt/user-data/outputs`, minus internal
process-feedback files (the scanner's `EXCLUDED_DIR_NAMES` plus the configured
`tool_output.storage_subdir`), so a run that only externalized oversized tool
outputs does not fail delivery. At least one candidate must be covered by a
path the run presented — a tool result tagged `presented_files`
(`runtime/presented_files.py`), which `present_files` and any tool that
presents the files it was asked to make both write (`bash` with a `present`
argument; `tools/presentation.py` validates each path against the filesystem
— inside this thread's outputs, a regular file, modified after the call
started — before recording it; a file a later command in the same run
deletes stays in `artifacts` and 404s, as it always did after
`present_files`). The journal records tagged paths as the receipt's
`presented_files`; `paths` is every `artifacts` update, side effects such as a
browser screenshot included, and is not a presentation; `by_tool` is
attribution only. No list of presenting tool names exists:
`delivery.py`'s `presented_paths()` reads `presented_files` (falling back to
`by_tool.present_files` only for receipts written before the tag existed).
Presenting only an unrelated pre-existing path does not satisfy delivery, so a
presentation that named nothing this run produced is `mismatched`. Readers of
the receipt (the fence, the archive route, the evidence bundle, IM channels)
go through `presented_paths()`. Such receipts add `produced_paths`,
`matched_paths`, `presented_by`, `verification`, `stage`, and `satisfied` to
the Slice 1 fact fields.

Since that repair the fence is an invariant, not the common failure
path: `RuntimeDeliveryMiddleware` hands over inside the graph whatever the
turn produced and nobody presented, so a turn that made a file the person
asked for ends `success`. It reaches the worker through
`runtime.context[RUNTIME_PRESENTED_FILES_CONTEXT_KEY]`, not the journal (which
records only tool-end presentations).
`_delivery_content_with_outputs` merges it into `presented_files` itself, the
field `presented_paths()` reads, splitting attribution under `presented_by`: a
second key would leave the archive route and evidence bundle on the narrower, so
a completed run would 409 on the download of the file it handed over. The fence
still fires when the runtime could not: a failed outputs scan, or a graph
interrupt before `after_agent` (unreached today). Missing a *matching* presentation is a run
error, as is a successful one whose receipt cannot be durably verified. Neither
publishes an `error` stream frame: the graph completed and the answer is
checkpointed, so the stream reaches its end marker normally and the verdict
rides `stop_reason` on the run record (`artifact_delivery_incomplete` /
`delivery_receipt_failed`, also the `run.terminal.v1` failure code) plus one
advisory `custom` frame for live clients — `artifact_delivery_incomplete`
naming the withheld paths (bounded at 20, exact count) or
`artifact_delivery_unverified` carrying only the run and the message. The
same advisory channel carries `turn_progress` frames (`runtime/turn_progress.py`:
`preparing` at admission, `workspace_starting` at sandbox create, `thinking`
at the first model request; each at most once per run and in that order — a
stage behind one already published is dropped — from a phase-journal observer
the worker closes when the run ends) so a client can name what the run is
doing instead of "Working…"; losing one costs a label. That
frame rides outside the negotiated stream modes, which is acceptable because
it is advisory: a consumer that never requested `custom` reads the verdict
from the record after the end marker, as `/wait` and the IM follow-up watcher
already do. `runs/delivery.py` owns the bound,
the stop reason, the produced-minus-presented rule, and the projection behind
`GET .../runs/{run_id}/delivery`, so frame and receipt cannot describe one run
differently; that route reads the receipt only when `stop_reason` says the
fence fired, and reports nothing rather than an empty correction when the
best-effort receipt is missing, duplicated, or holds no withheld path. Runs
without changed outputs keep ordinary chat behavior and the original receipt
shape. Orphan recovery first atomically claims an expired lease, then uses the
same singleton write to backfill a zero-delivery receipt — a stale recovery
scan cannot overwrite a live run's later detailed receipt, an event-store
outage does not undo the terminal takeover, and a detailed receipt written
before a crash is preserved. Event stores serialize `put_if_absent` with
ordinary thread writers: memory and JSONL give the documented single-process
guarantee, the DB store adds per-thread in-process locks plus PostgreSQL
advisory locks for cross-process writers. Constructing the journal ahead of preflight is
receipt-only on early failure paths: a separate boundary flag keeps the
previous completion-data semantics, so checkpoint incompatibility, or
cancellation while waiting for an older finalizing run, persists no empty
completion snapshot. Worker tests pin one accumulated receipt across several
goal-continuation `_stream_once` calls; journal tests drive LangChain's real
async callback dispatcher against one journal to pin serialized, deduplicated
parallel tool callbacks.

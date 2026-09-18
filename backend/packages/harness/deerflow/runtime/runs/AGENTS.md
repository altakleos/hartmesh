# AGENTS.md

Scoped guide for `runtime/runs/` — the run manager, the worker, the run and
event stores, and the delivery verdict they commit. Split out of
[`../AGENTS.md`](../AGENTS.md), which owns the rest of the runtime.

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
`by_tool.present_files` only for receipts written before the tag existed). Presenting
only an unrelated pre-existing path does not satisfy delivery, so a
presentation that named nothing this run produced is `mismatched`, not
satisfied. Readers of the receipt (the fence, the archive route, the evidence
bundle, IM channels) go through `presented_paths()`. Such receipts add `produced_paths`,
`matched_paths`, `presented_by`, `verification`, `stage`,
and `satisfied` to the Slice 1 fact fields.

Since hartmesh-tenancy/DF22 the fence is an invariant rather than the common
failure path: `RuntimeDeliveryMiddleware` (see
[`../../agents/middlewares/AGENTS.md`](../../agents/middlewares/AGENTS.md))
hands over inside the graph whatever the turn produced and nobody presented,
so a turn that made a file the person asked for ends `success` instead of
`artifact_delivery_incomplete`. It reaches the worker through
`runtime.context[RUNTIME_PRESENTED_FILES_CONTEXT_KEY]`
(`runtime/presented_files.py`) — not the journal, which records only
presentations it observes at tool end, and a runtime presentation is a state
update at the end of the agent. `_delivery_content_with_outputs` merges that
list into `presented_files` itself — the one field the receipt has always
meant by "what this run presented", and the field `presented_paths()` reads —
and records both sides under `presented_by` (`model` / `runtime`), so a
receipt says who handed each file over without holding the set twice. A
second key would have left the archive route and the evidence bundle on the
narrower one: a run the runtime completed would succeed and then answer 409
to the download of the file it had just handed over. The fence
still fires when the runtime could not: a failed outputs scan, or a turn that
interrupts before `after_agent` runs. Missing a *matching* presentation is a run
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

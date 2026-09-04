# Run Event Stream

The run event stream is DeerFlow's append-only record of what happened during
an agent run. Producers write through `RunEventStore`; history, debug, subtask,
memory-audit, and workspace-review consumers read projections of the same rows.

The machine-readable contract is
`contracts/run_event_stream_contract.json`. Canonical event names and
categories live in `deerflow.runtime.events.catalog`; conformance tests require
the runtime catalog and JSON contract to match exactly.

## Record Envelope

Every persisted event has these required fields:

| Field | Meaning |
| --- | --- |
| `thread_id` | Thread that owns the event. |
| `run_id` | Run that produced the event. |
| `seq` | Store-assigned sequence, strictly increasing within a thread. |
| `event_type` | Fixed event name or documented dynamic pattern. |
| `category` | Consumer-routing bucket. |
| `content` | Event payload, normally a string or JSON object. |
| `metadata` | Filterable or audit metadata. |
| `created_at` | Timezone-aware ISO-8601 timestamp. |

Backends may return additional fields. `DbRunEventStore`, for example, returns
`user_id` and may add serialization markers such as `content_is_json` to
metadata. Consumers must ignore unknown envelope and metadata fields.

`event_type` is limited to 32 characters and `category` to 16 characters by the
database schema. Catalog-backed definitions enforce the same limits before
writing so they cannot emit values that only the memory or JSONL store accepts.

`seq` is thread-global, not run-local. Memory and database stores assign it
monotonically for their supported deployment modes. JSONL only provides this
guarantee within one process; shared multi-process deployments must use the
database store.

## Authoritative Lifecycle Evidence

`RunEventStore` is the rich, callback-derived stream described by the rest of
this document. It is deliberately separate from the safe authoritative journal
in `run_lifecycle_events`:

| Lifecycle field | Meaning |
| --- | --- |
| `event_id` | Stable event UUID. |
| `cursor` | Globally ordered committed-transition cursor. |
| `run_id` / `thread_id` | Normal invocation and bound context identity. |
| `owner_scope` | Bounded, non-identifying access scope. |
| `lifecycle_type` | One of `accepted`, `started`, `cancellation_requested`, `cancelled`, `succeeded`, `failed`, `timed_out`, or `interrupted`. |
| `state_version` / `status` | The exact resulting authoritative `RunRow` version and status. |
| `payload_json` | Versioned bounded safe reason/evidence references only. |

A new normal `RunRow(operation_kind="run")` commits at `state_version=1`
with `accepted`. Each successful compare-and-set increments the row version
once and inserts one lifecycle row in the same transaction; a stale CAS changes
neither. `RunRow.status` remains authoritative, so this is not event sourcing.
Historical and auxiliary operation rows use version 0, and checkpoint/artifact
reservations never emit lifecycle rows.

Global cursor allocation is transaction ordered through the singleton
`run_lifecycle_cursor_state` row: PostgreSQL writers use `SELECT ... FOR
UPDATE`; SQLite writers acquire `BEGIN IMMEDIATE` before reading it. A missing
singleton may be repaired only while no lifecycle event exists. Lifecycle
events without that singleton are corrupt ordering state and fail Gateway
initialization.

Readiness never repairs state. In one database snapshot it performs exactly three bounded
`LIMIT 2` reads: singleton rows, the first retained event cursors, and the last retained event
cursors. A missing/duplicate singleton, invalid `0 <= pruned_through <= last_cursor`, retained
range mismatch, or discontinuity at either retained sequence edge makes readiness and new
admission fail closed. This deliberately validates maintained structural boundaries without
scanning conversation or lifecycle history.

The lifecycle payload never stores prompts, messages, reasoning, tool payloads,
credentials, artifact contents, or the rich bodies below. Reasons are selected
from host-owned safe codes; v1 evidence accepts only the cancellation `action`
reference (`interrupt` or `rollback`). Lifecycle type/resulting-status pairs are
validated before a row can change. A host-independent in-process API can query
this journal, and the Gateway exposes the same access-filtered query through
`GET /api/runtime/v1/invocations/{run_id}` and
`GET /api/runtime/v1/contexts/{thread_id}/invocations`. There is no broker.
Constraint-fence failures use the safe reason
`constraint_evidence_mismatch` or `constraint_expired_before_start` and map to
the ordinary `failed` lifecycle type; they do not add lifecycle vocabulary.

### Lifecycle paging and pruning

Invocation and context queries apply owner/admin visibility and optional current
`invocation:observe` authorization before reading a snapshot or event. Unknown,
auxiliary, and owner-invisible rows are indistinguishable. A context feed makes
one authorization decision for the bound context and returns only normal runs.
It may filter on the sealed source kind (`http`, `scheduled_task`,
`native_channel`, or `service`) without exposing another owner's records.

Opaque `deerflow.lifecycle.cursor/v1` tokens encode the last-seen global integer
cursor. One page captures `read_fence_cursor=last_cursor`, reads matching events
in `(C,F]` with `limit + 1`, and never returns an event above the fence. If a
matching event remains, `next_cursor` is the last returned event; otherwise it
advances to the fence, including for a filtered empty page. Repeating a page is
therefore at least once and harmless through stable event IDs/cursors without
skipping a later match.

PostgreSQL begins a read-only `REPEATABLE READ` transaction before the first
SELECT; SQLite uses one explicit read transaction. Cursor metadata, events,
and joined `InvocationSummaryV1` rows consequently represent one database
snapshot across process restarts. Summaries carry bounded accepted source and
digest evidence once; lifecycle events do not duplicate those static facts.
The portable page rejects mixed-context data: every snapshot/event must match
the observed thread, singular pages must match the observed run, snapshot and
summary run IDs are unique, and each summary must match one page snapshot's
identity and current state. Historical event states remain valid transition
evidence and need not equal the latest snapshot.
For context pages, the store fetches summary/snapshot rows only for distinct run
IDs in the returned event page—not every run in the thread. The page limit is
1–500, lifecycle payloads are limited to 4 KiB, summaries to 16 KiB, and the
portable observation to 12 MiB canonical JSON. Historical rows without sealed
Origin remain event/snapshot-readable and simply have no summary.
A malformed/wrong-version token is invalid, a token above the fence is ahead,
and a token below `pruned_through` reports a gap plus the minimum available
cursor. Equality with the minimum resumes at the first retained event.
`prune_through()` locks cursor metadata, deletes the eligible prefix, and moves
the marker monotonically but never beyond `last_cursor`. Pruning is operator-
initiated only; no retention timer or worker exists.

### Durable tool-attempt evidence

Accepted durable lead and subagent executions also write two rich run-event
types: `tool_receipt.started.v1` and `tool_receipt.outcome.v1`, both in
`category="tool"`. These are not invocation-state transitions and do not change
`RunRow.state_version`. The outer tool wrapper atomically reserves and commits
the start under the current `(owner_id, lease_epoch)` fence before calling any
authorization, guardrail, provider, or tool code. All terminal phases share one
idempotency slot, so an identical replay returns the first stored event and a
different terminal phase is an integrity failure. Store time is authoritative
and is retained on duplicate appends.

Receipt identity is `tr_` plus the full SHA-256 digest of version, run ID,
execution-task ID, model tool-call ID, and durable attempt number. Attempt
reservation belongs to the event store: an unfinished recovery reuses its
start, while a genuinely new dispatch after a terminal outcome increments the
number. A stable subagent execution-task digest and the accepted subagent
catalog/definition digests keep equal provider call IDs in different execution
scopes distinct. A crash after start writes no synthetic outcome; the lifecycle
projection reports that pair as `indeterminate`.

Receipt bodies are strict canonical JSON capped at 8 KiB. They store the safe
tool name, full projection digests, bounded server-derived policy references,
worker fence, and accepted assembly/revision/generation anchors. Request
projections retain field names and JSON shapes; credential-like values become a
secret-handle class, and unclassified strings become length/type markers rather
than raw hashes. Only a field explicitly marked evidence-safe in its
server-owned tool schema may contribute a bounded scalar value. Result digests
cover the exact sanitized and budgeted model-visible result plus result
type/status. Raw arguments, results, URLs with credentials, exception messages,
headers, and stack traces are forbidden.

A digest can compare a receipt with an independently available projection; it
does not make low-entropy content confidential and is not a truth assertion.
A durable receipt records HartMesh's observation of a tool attempt. It does not
guarantee an external side effect occurred exactly once or that the tool result
was correct.

Supported external retrieval adds `retrieval.observation.v1` beside the
terminal receipt. `append_retrieval_pair()` validates their receipt ID, attempt,
tenant, run, phase, and exact `result_projection_digest`, then writes both in
one fenced store operation. Recovery may complete a retained receipt-only pair
only while the original safe draft remains available; reservation replay never
accepts that receipt as a complete supported retrieval. An observation-only
state, conflicting duplicate, or stale fence is an integrity failure. The
observation is capped at 12 KiB and contains only safe constraints, normalized
source references, counts/status, accepted material references, and digests—no
query/query-derived identifier or result text. See
[EVIDENCE_BEARING_RETRIEVAL.md](EVIDENCE_BEARING_RETRIEVAL.md).

### Live rich-event write authority

After run admission, worker-owned journal, subagent, workspace, and delivery
writes pass through `FencedRunEventAppender`. Its capability fixes tenant,
thread, run, owner, and lifecycle epoch; callers cannot select a different
identity. The memory store validates and appends without yielding, the database
store locks and validates the `RunRow` in the same transaction as the event
insert, and JSONL holds the run-store execution fence until its off-thread
atomic rename has completed. Cancellation of the JSONL awaiter is delayed until
that rename finishes, so cancellation never releases the fence around a still-
running filesystem mutation.

Pre-admission failures and recovery have no live execution capability. They
must opt into the visibly separate `AdministrativeRunEventAppender`; that port
is for recovery, migration, and history seeding, not ordinary worker writes.
Process-local unqualified execution uses an explicit local authority validator;
it is not a multi-process durability claim.

## Categories

`category="message"` means an event is eligible for a message projection; it
does not guarantee that the row is visible in the UI. Thread-history APIs also
filter middleware model calls, subagent AI responses, and superseded regenerate
runs, and the frontend honors message-level visibility markers such as
`hide_from_ui`. Subagent events remain available through the run-events
endpoint; parent `task` ToolMessages remain in thread history so subtask cards
can restore their terminal status after reload.

All other categories are excluded from message projections and are available
through run-event or specialized APIs:

| Category | Purpose |
| --- | --- |
| `trace` | Execution evidence. |
| `outputs` | Root graph completion output. |
| `error` | Callback-observed failure evidence. |
| `middleware` | Middleware state-change audit evidence. |
| `context` | Effective hidden-context identity and bounded external-memory audit evidence. |
| `subagent` | Subagent lifecycle and step history. |
| `tool` | Bounded durable tool-attempt start/outcome evidence. |
| `policy` | Bounded accepted execution-policy warning/stop decisions. |
| `workspace` | Workspace/output file-change evidence. |

## Producers

`RunJournal` emits callback-derived events:

| Event type | Category | Producer |
| --- | --- | --- |
| `run.start` | `trace` | Root `on_chain_start()` |
| `run.end` | `outputs` | Root `on_chain_end()` |
| `run.error` | `error` | `on_chain_error()` |
| `run.terminal.v1` | `trace` | Worker finalization through `record_terminal_summary()` |
| `llm.human.input` | `message` | First persisted lead-agent human input |
| `llm.ai.response` | `message` | `on_llm_end()` |
| `llm.tool.result` | `message` | `on_tool_end()` |
| `llm.error` | `trace` | `on_llm_error()` |
| `context:memory` | `context` | `record_memory_context()` |
| `memory.observation.v1` | `context` | `persist_memory_observations()` for tenant-bound Honcho operations |
| `middleware:{tag}` | `middleware` | `record_middleware()` |
| `tool_receipt.started.v1` | `tool` | `RunEventToolReceiptSink.reserve_started()` |
| `tool_receipt.outcome.v1` | `tool` | `RunEventToolReceiptSink.record_outcome()` |
| `retrieval.observation.v1` | `tool` | `RunEventToolReceiptSink.record_with_receipt_outcome()` |
| `policy.decision.v1` | `policy` | Execution-policy row-backed outbox through `FencedRunEventAppender` |

`policy.decision.v1` contains only the decision (`warn` or `stop`), stable
reason and summary keys, safe current/limit counters, and the accepted budget
and compact-state digests. The compact run-row outbox is written with the
counter transition before publication; recovery republishes a missing event
under the current fence and clears the outbox only after the event is visible.
Tool arguments, result text, equivalence commitments, HMAC material, prompts,
and provider payloads are excluded.

Current middleware tags are `guardrail`, `loop_detection`, `mcp_preparation`,
`safety_termination`, `skill_activation`, and `skill_secrets`.
`mcp_preparation` contains only the pinned capability generation,
contribution IDs, and bounded persistable safe evidence references; transient
headers and MCP arguments are excluded. The pattern is intentionally open so
new middleware tags are additive. Because the full event type is limited to 32
characters and `middleware:` uses 11, a tag must contain 1-21 characters.

`middleware:loop_detection` records transitions into the warned state (first
per call hash or per tool-frequency burst) and each hard stop produced by
`LoopDetectionMiddleware` in lead-agent and ordinary task-tool subagent runs.
Task-tool subagents forward the append to the parent loop because `RunJournal`
and its event store must not cross the isolated-loop boundary. Durable batch
subagents have no parent run journal and do not emit these events. The event's
`action` is `warn` or
`hard_stop`. The `changes` object identifies the detection layer, affected tool
names, observed count, effective threshold, whether the producer was a
subagent, and its agent id when applicable. Tool arguments, prompts, message
content, tool results, and argument-derived hashes are not persisted in this
event.

### External memory observations

Accepted durable runs bind Honcho's portable operation callback to their
`RunJournal`. Each completed `get_context`, `search`, `get_memory`, or `add`
operation admitted through that trusted boundary emits one
`memory.observation.v1` event. The event contains the server-owned tenant
reference, a pseudonymous workspace reference, operation and bounded status,
an optional projection digest, optional item count, truncation flag, and
timestamp. It never contains raw workspace names, memory text, user IDs,
prompts, credentials, or provider errors. Write observations deliberately omit
the projection digest so low-entropy written content is not turned into a
guessable oracle.

Candidate middleware reads are not admitted operations yet: they stage their
observation in the offloaded worker and commit it only after the worker returns
before its timeout. The
journal flushes prior callback rows, then awaits the observation batch's real
event-store write before the candidate memory can enter model context. A late
worker completion after timeout is discarded and emits no observation.
Direct durable reads likewise wait for the acknowledged store write.

Retries are separate observations because the external memory is mutable.
Ordinary non-durable calls have no run journal and do not emit this event. An
observation persistence failure follows the configured Honcho failure policy:
the operation fails closed, or its otherwise successful result is discarded
and reported through the normal fail-open path.

### Opaque Run Outputs

`run.end.content` is the root graph output and is intentionally opaque. Its
nested representation is not currently identical across storage backends:

- `MemoryRunEventStore` retains the original Python container and nested
  values.
- `JsonlRunEventStore` and `DbRunEventStore` serialize through
  `json.dumps(default=str)`, so nested values that are not directly JSON
  serializable are read back as strings.

Consumers may use `run.end` as completion evidence, but must not depend on
backend-identical nested output values. Normalizing those values would be a
separate runtime compatibility change rather than part of this current-state
contract.

`run.terminal.v1` is the bounded companion for lifecycle consumers. It contains
only version, terminal status, optional safe stop reason, and optional V1
failure evidence (`code`, sanitized exception class, and opaque correlation
ID). It does not replace or reinterpret `run.end.content`. `run.error` and
`llm.error` use the same bounded failure shape; raw exception messages and
tracebacks are excluded. Intentional assistant and tool message content remains
unchanged.

`subagents/step_events.py::subagent_run_event()` maps streamed `task_*` chunks
to persisted events. The worker batches them through `put_batch()`:

| Event type | Source chunk | Required content |
| --- | --- | --- |
| `subagent.start` | `task_started` | `task_id`, `description` |
| `subagent.step` | `task_running` | `task_id`, `message_index`, `kind`, `text`, `truncated`; AI steps add `tool_calls`, tool steps add `tool_name` |
| `subagent.end` | terminal `task_*` | `task_id`, `status`; optional model, usage, result/error, and truncation fields |

Terminal subagent status is one of `completed`, `failed`, `cancelled`, or
`timed_out`.

Malformed lifecycle chunks are not persisted. Every chunk requires a non-empty
string `task_id`; `task_running` additionally requires a non-negative integer
`message_index` and a message object.

`workspace_changes.record_workspace_changes()` writes `workspace_changes` in
category `workspace` when a run changed files. Its string content is a summary;
the structured versioned summary, file list, and limits live in
`metadata.workspace_changes`.

The JSON contract defines required and optional payload fields using JSON
Schema. It is the authoritative field-level reference.

## Consumers

| Consumer | Read path and behavior |
| --- | --- |
| Frontend thread history | `GET /api/threads/{thread_id}/messages/page` scans `list_messages()`, removes middleware rows, subagent AI responses, and superseded regenerate runs, then applies frontend message visibility rules. |
| Per-run message clients | Thread-scoped and stateless run message endpoints call `list_messages_by_run()`. |
| Run debug/audit | `GET /api/threads/{thread_id}/runs/{run_id}/events` calls `list_events()` and supports `event_types`, `task_id`, `limit`, and `after_seq`. |
| Historical subtask cards | Fetch `subagent.step` through the run-events endpoint, filtered and paginated by `task_id`. |
| Memory audit | Filters run events to `context:memory` to compare the frozen hidden block's `content_sha256`, or to `memory.observation.v1` to audit bounded tenant-bound Honcho operation evidence; full memory text is not duplicated into the event store. |
| Workspace review | `GET /api/threads/{thread_id}/runs/{run_id}/workspace-changes` projects the latest `workspace_changes` payload. |
| Authorized durable receipt page | `GET /api/runtime/v1/invocations/{run_id}?include_tool_receipts=true` pairs starts/outcomes with an independently scoped cursor and a 100-item cap. |
| Authorized retrieval observations | `GET /api/threads/{thread_id}/runs/{run_id}/retrieval-observations` returns a closed safe projection with a 100-item page cap and `after_seq` cursor. |

Token and cost summaries are not reconstructed by reading event rows.
`RunJournal` accumulates usage while callbacks fire, and the worker writes the
aggregates to `RunRow`.

External Langfuse/LangSmith tracing is a parallel callback pipeline, not a
`RunEventStore` consumer. It is correlated through trace metadata rather than
being derived from these rows.

Evaluation consumers discussed in #4243 are planned rather than present in
this tree. They should read evidence through `list_events()` and treat the
compatibility and terminal-state limits below as part of that integration.

## Compatibility

The existing mixture of dot-separated, colon-separated, and bare-word names is
frozen. This contract documents current behavior; it does not normalize names.
A rename, removal, category change, required-field removal, or required-field
type change is breaking and needs an explicit versioned migration or dual-write
period.

Adding a new event type or optional field is additive. Consumers must ignore
unknown event types and unknown optional fields. Producers must add a catalog
entry, update the JSON contract and this document, and extend the conformance
tests in the same change.

`ai_message` is a read-only legacy alias for `llm.ai.response`. Current
producers never emit it. Category-based message projections and store queries
for the last visible AI message recognize previously persisted alias rows, so
the `/messages/page` endpoint also attaches feedback correctly. The legacy
`/messages` endpoint still returns those rows but only enriches feedback for the
canonical name. Legacy aliases live outside the canonical catalog and must not
be used by new producers.

## Known Gaps

- Model tool-call intent remains embedded in `llm.ai.response.content.tool_calls`;
  accepted durable executions additionally record an attempt start/outcome.
  Local/direct and historical pre-feature runs have display receipts only, and a
  started attempt interrupted by process loss deliberately has no outcome.
- `run.end.metadata.status` is only a root graph completion marker and is
  always `success`. Live workers also append bounded `run.terminal.v1`, but
  `RunRow.status` remains authoritative for lifecycle state.
  The separate lifecycle journal records authoritative worker-loss recovery as
  `failed` with reason `orphan_recovered`; this rich callback stream may still
  have no matching terminal `run.end`/`run.error`/`run.terminal.v1` event.
- Nested non-JSON values in `run.end.content` have backend-dependent
  representations: memory retains Python values, while JSONL and database
  stores read them back as strings.
- Durable batch subagent loop detection and deferred-tool promotion do not
  currently emit middleware events.
- Journal attribution, token accounting, and external tracing metadata still
  depend on manual instrumentation at several LLM call sites.

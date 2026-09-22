### Durable Invocation Boundary

Gateway-owned `InvocationRuntime` is the sole durable admission module. Its
accepted record binds actor/credential/Origin/context/constraints, pinned
agent/extensions, and immutable skills. Governed records additionally bind
base/overlay digests and generations, projection/effective digests, secret-safe
MCP structure/allowlists, integration IDs, MCP tool objects, and skill bytes;
recovery verifies and reuses these snapshots instead of rediscovery. Authorization
precedes admission; replay keys must match visible stored request evidence.
Lifecycle writes are transactional, cancellation is fenced, and recovery uses
the persisted policy plus a distinct audited executor. `terminalize_v1` remains
default; `exact_two_takeover_v1` is dormant and every takeover claim is rejected.
Activation requires the multi-Gateway guide's linearizable authority and fully
reconstructible state; markers and fault barriers alone are not support.

`subagent_snapshot.py` is the sole live-to-immutable seam for durable subagents.
Admission captures a bounded `ResolvedSubagentCatalogV1` plus a digest-scoped
`ResolvedSkillScopesV1` map for `lead` and every `subagent:<name>`. Both are part
of the agent revision digest and are embedded in `agent_revision_json`; even a
delegation-disabled revision carries their canonical empty forms. Worker recovery
validates internal catalog/scope digests and required accepted skill bytes, then
reuses the persisted objects without comparing current managed records. Legacy
terminal revisions without these fields remain readable; legacy nonterminal
execution stops with `subagent_catalog_unavailable`. Do not install an older
runtime that cannot validate the additive fields until all such nonterminal rows
are drained or terminalized.

Durable batches are subordinate to `InvocationRuntime`. The worker strips
caller batch context; `batch_task` requires the accepted parent and active
receipt. `AcceptedBatchV1` binds the assembly and execution snapshot without
sealing another revision. Recovery does not rediscover subagents or skills;
stable named tool adapters must match their accepted contract digests.

Durable sandbox operations use `AcceptedSandboxSession`; see
`backend/docs/ACCEPTED_SANDBOX_EXECUTION.md`. It composes run/material
authority and exposes no raw provider handle. A thread the coordinator holds
as clearing is freed only by a provider that proves the material gone:
gateway admission and the worker's claim wait each finish the pending clear
before refusing (`complete_pending_projection_clear`), and an unproven clear
must leave the fence exactly as it was. Never free one on elapsed time.

Accepted durable lead execution also binds `AssemblyEvidenceV1` to the running
owner/state-version fence. After accepted material is verified and the run starts,
the worker assembles under the frozen extension generation, validates the actual
model/prompt/tools/middleware/skills/policy descriptor against accepted anchors,
and atomically binds or compares it before constructing a checkpoint accessor or
calling `astream`. `assembly_evidence_unavailable` covers a missing descriptor;
`agent_assembly_drift` covers invalid, contradictory, or changed evidence. Bound
evidence is immutable and survives every terminal outcome. It proves only which
assembly the runtime admitted, not the integrity of the Python code that produced it.
The host reserves the fingerprinted effective-policy key
`hartmesh.tool_recovery.v1`; its value is a finite tool-name-to-recovery-kind
map. Absence means no reconciliation capability. Do not add this declaration to
the public extension-api `ToolDescriptor` wire shape or infer it from a tool
name. A started receipt may resume only when a trusted recovery coordinator
binds proof to that policy, receipt, dispatch generation, assembly digest, and
the current takeover fence.

`run_evidence.py` owns transport-neutral terminal snapshots and canonical V1
manifests. Keep FastAPI/ZIP/storage out; manifests omit raw payloads/IDs and
authenticity claims. Adapters revalidate fences and bind copied artifact bytes.

Live rich run-event writes use `events/appender.py`. A
`FencedRunEventAppender` binds tenant, thread, run, worker owner, and lifecycle
epoch for journal, subagent, workspace, and delivery writes; DB stores validate
the fence in the insert transaction. For DB runtime-event and durable-receipt
writes, every non-null lease expiry is compared with the shared database wall
clock sampled after the owner row lock; a worker process clock is never lease
authority, while a null expiry remains valid for heartbeat-disabled single-node
operation. JSONL retains the run-store fence until its off-thread rename
completes even under cancellation. Pre-admission and recovery code must opt into
`AdministrativeRunEventAppender`. Runtime exception messages and tracebacks
cross no persistence/SSE/log boundary: map them once via
`failure_evidence.py`, while leaving intentional `run.end` and conversation
content untouched. `run.terminal.v1` supplements opaque `run.end` with bounded
versioned terminal facts.

Run-state and delivery-event stores do not yet share a prepared-replacement
transaction. When a durable event store is configured, interrupt/rollback
admission therefore resolves existing replay identities read-only, then rejects
any novel candidate while a predecessor is active. The official run stores
repeat this through `require_predecessor_inactive` under their mutation lock;
clients cancel, await terminal `run.delivery`, and retry. Receiptless
compatibility configurations retain legacy atomic replacement. Exact-two rows
reject generic replacement unconditionally. Reopening durable replacement
requires an additive prepared intent that first reserves candidate identity and
predecessor epoch, not cross-store ordering in `RunManager`.

Runtime-owned progress snapshots and effective-model observations are also
owner/epoch writes. `RunManager` captures the current owner and state version,
waits for the store CAS before changing its local mirror, and requires a live
lease whenever the row has one. SQL validates that lease from the shared
database wall clock after locking the row. Omitting the fence remains compatible
only for a genuinely null-lease single-node row; it never authorizes an actively
leased row. A same-owner cancellation may advance the epoch before either write,
so a rejected observation first refreshes the durable cancellation and signals
the local abort rather than misclassifying it as a takeover. A tool call's
terminal receipt follows the cancellation too, through
`RunManager.adopt_cancellation_epoch`: the sink asks once, never for a
`started` receipt, and adopts only the epoch exactly one past the one it held,
on a row still running under the same owner with a cancellation recorded. So
the call a cancellation ended writes its terminal receipt instead of staying
indeterminate. The adoption moves the epoch and signals nothing -- the route,
heartbeat and out-of-band watch skip a run already signalled, so a quick
tool's receipt must not stop them cancelling the task a slow one runs in.

Qualified run ownership uses the versioned `database_v1` clock capability.
Admission, renewal, and takeover accept a duration; SQL mints the persisted
deadline from the same post-lock database-time sample used by its authority
predicate. The persisted timestamp remains evidence, while `RunManager` anchors
a conservative monotonic budget before the store call and arms a process-local
watchdog so late results or renewal errors cannot extend execution. The additive
absolute timestamp arguments and `process_v1` behavior remain compatibility
seams for non-qualified stores. `grace_seconds` delays recovery after database-
authoritative expiry and grants no additional execution authority.

Run-derived title/status writes use `ThreadMetaRunProjection`; the worker must
not call the human/admin thread-metadata update methods. The projection store
accepts only the latest normal run by its database-assigned monotonic
`admission_cursor` (never process timestamps or run IDs), fails closed if any
relevant cursor is missing, and holds that ordering authority through the
metadata write. A running projection requires the exact active owner and state
version (plus an unexpired lease when present); a terminal projection requires
the exact displaced owner, its last active state version, and the terminal state
version minted by the successful lifecycle CAS. Title and terminal status are
one conditional mutation. Explicit human/admin updates remain independent APIs,
and the memory adapter serializes them with projection writes so neither can
resurrect a deleted row or overwrite a disjoint concurrent update.

`runtime/tool_evidence.py` is the deep module for durable tool-attempt evidence.
It owns the V1 context/receipt schemas, canonical full SHA-256 identities and
projections, bounded redaction policy, state transitions, trusted runtime keys,
and the sink port/adapters. Attempt reservation belongs to `RunEventStore`, not
process memory: public LangGraph checkpoint/task data is only a local retry
observation. The store owns contiguous attempt numbering; a reconstructed
counter reuses the latest durable start and terminal, then a binding-local
offset maps its next live retry to the immediate successor. Conflicting immutable anchors fail
closed. `ToolReceiptMiddleware` is only the observation point; do not duplicate
receipt identity, redaction, transition, or event parsing logic in middleware,
subagents, or lifecycle consumers. The accepted lead binding is propagated to a
subagent with a stable server-derived execution-task ID and the immutable
catalog/definition digests. Caller-supplied `__tool_evidence_*` fields are always
stripped before the worker installs typed values.

The portable `deerflow-runtime-api` DTOs contain runtime operations only;
deployment provenance and qualification are administrator-only. Memory-backed
local development is explicitly unqualified. Real Postgres and opt-in
Kubernetes suites are release gates, and neither establishes multi-replica HA.
Invocation observation includes MCP children only when `include_mcp_tasks=true`
and payload-free batch lifecycle only when `include_subagent_batches=true`;
both use independent owner/tenant/parent-scoped cursor pages after parent
visibility. Parent cancellation cascades to neither MCP tasks nor batches.

### Turn phase timings

`turn_phases.py`: one in-memory journal per run, monotonic offsets, opened by `run_agent`, found by run id from the SSE consumer. Its module docstring is the contract -- what each phase means, what the counters claim, and what is deliberately not recorded -- and is the place to change it; this is orientation only. The launch's own steps (every entry point stamps the intent; `InvocationRuntime.launch` times them) ride the created record as transient `launch_timings`, which the worker hands to the journal. `observe()` hears each phase as it begins, on the opening thread; `turn_progress.py` turns three of them into one advisory `turn_progress` stream frame each (at most once per run, in order; `call_soon_threadsafe` to the run's loop; closed by the worker at run end; failures logged and dropped). A turn's wait is attributed end to end: four spans nest inside `skill_materialization` for the pre-model wait (`docs/ACCEPTED_SANDBOX_EXECUTION.md`, "Attributing the accepted preparation"), and `tool_execution` plus the `tools=`/`model=`/`busy=` counters cover what follows the first answer. Three things bite anyone extending it: `ignore_agent` gates every tool callback, so it is `False`; a subagent's own `callbacks` list replaces the inherited one, so its calls are not counted here; and an end event never delivered (a cancelled async tool) must stay `tools_open=`, never be folded into a measured figure.

### Stream Bridge Heartbeats

Memory and Redis bridges take their default idle heartbeat cadence from the startup-only `stream_bridge.heartbeat_interval_seconds` setting. Keep the default on the bridge instance so SSE, `/wait`, and internal subscribers stay aligned; an explicit `subscribe(..., heartbeat_interval=...)` remains a per-subscription override.

### Checkpoint Channel Modes (`full` / `delta`)

Checkpointer storage runs in one of two channel modes, selected by `checkpoint_channel_mode` in `config.yaml` (default `full`). `delta` adopts LangGraph 1.2's `DeltaChannel` for `messages`, so storage grows O(N) instead of O(N²) in turns. Every saver backend serves both modes unchanged — the semantics live in the compiled graph's channel table, not in the saver.

Rules you must not discover the hard way:

- **Mode and delta snapshot cadence are process-frozen and restart-required.** A second, different value in one process raises `CheckpointModeReconfigurationError`. The cadence must also match across every process sharing one checkpoint database, and — unlike the mode — it is deliberately not stamped into checkpoint metadata, so a cross-process mismatch is silent.
- **Compatibility is asymmetric and fail-closed.** Delta checkpoints carry a metadata marker; a full-mode process opening a delta thread raises `CheckpointModeMismatchError` (the threads router maps it to 409), never a silently empty `messages`. Delta reads full transparently, so full → delta is the smooth migration.
- **Never bypass `CheckpointStateAccessor` (`checkpoint_state.py`) for thread-state access.** It is the one choke point binding graph + checkpointer + mode, and raw reads of a delta blob see a sentinel.
- **Wholesale state replacement goes through `build_state_mutation_graph` with the thread's effective schema.** The base-`ThreadState` fallback silently discards middleware-contributed channels; never hand-write checkpoints via `checkpointer.aput`.
- **A delta-mode run cannot fork.** Resuming an older checkpoint would replay an abandoned sibling's pending writes, so `runs/worker.py` linearizes the resume onto the current head instead (#4458).

Full depth — freeze and compatibility internals, replay lineage,
rollback and the `Overwrite` mutation contract, message-feed seq stamping,
run-event attribution, terminal cleanup, where each piece lives, and both
benchmark harnesses — is in
[`backend/docs/CHECKPOINT_CHANNEL_MODES.md`](../../../../docs/CHECKPOINT_CHANNEL_MODES.md).

## Exact two-Gateway boundary

The only multi-pod runtime claim is
`durable_two_gateway_v1_postgres_redis_aio_rwx`. Runs, events, checkpoints, and
ownership remain PostgreSQL-authoritative; Redis is a tenant-scoped reconnect
transport and cache. Cross-pod callers must use stable external keys and current
owner/epoch fences, and reconnect must rebuild from durable history after Redis
loss. The qualification-only failure barriers in
`runtime/kubernetes_qualification.py` are inert unless both exact internal test
flags are present and a tenant-prefixed Redis arm is atomically consumed. Never
reuse them as a production coordination mechanism.

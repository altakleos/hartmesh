# Durable Invocation Runtime

`app.runtime.InvocationRuntime` is the single in-process application boundary for durable
HTTP, Scheduled Task, authenticated native-channel, and embedded-service launches. Scheduling and channel
delivery remain source-owned; normalization, accepted-fact sealing, durable admission, and
one worker attachment belong to the runtime. Checkpoint and artifact reservations are
auxiliary thread operations, not accepted invocations.

## Trust and sealing

Every source constructs an internal launch intent, but caller thread, assistant, agent,
body context, headers, queries, and metadata remain hints. Before admission the host:

1. authenticates the principal and, for channels, authenticates the provider event, looks
   up the connection, and revalidates its current owner;
2. resolves the thread, agent, source facts, normalized input, and execution-significant
   options;
3. creates a bounded base `InvocationOrigin` containing only independently authenticatable
   source/correlation evidence;
4. runs trusted Origin and run-context contributors; and
5. seals an immutable `AcceptedInvocation` and admits it with the normal active-thread
   conflict rule before attaching one worker.

The accepted object contains a minimal principal projection, sealed Origin, bound
thread/context references, resolved agent revision, normalized input/options, immutable
extension generation, and versioned principal, base-Origin, accepted-context,
runtime-identity, and contributor-execution digests. Contributors cannot replace host-owned
principal, thread, agent, source kind, or base-source fields.

## Contributor data and redaction

Public contributor contracts live in `deerflow-extension-api`, which remains independent
of the harness and Gateway. Contributor factories are startup-only, loader-attributed
capabilities. Calls are concurrent, each has a two-second timeout, and successful results
are composed in stable contribution-ID order.

Results contain only bounded namespaced scalar/list references. Persistable safe Origin
facts may enter `origin_json`; runtime-only values never do. Execution references and stable
secret-handle identifiers affect the accepted-context digest, correlation references do
not. Raw credentials, arbitrary objects, nested maps, and plugin exception text are never
persisted or reflected in diagnostics. Optional failures are omitted; a configured required
capability fails startup when absent/broken and fails an invocation closed when it times out
or returns invalid data.

## Persistence

Normal `runs` rows have nullable, backward-compatible columns for Origin and principal
projections and digests, accepted-context and agent-revision digests, safe agent revision
metadata, extension generation, and versioned empty decision evidence. Full resolved agent
material, credentials, and runtime-only contributor references are not serialized. A keyed
run additionally stores an internal request projection in its existing run kwargs so
retries can be compared against accepted routing and contributor evidence without resolving
today's mutable host state; this internal projector is not returned by the run API.
Historical null rows remain readable. Auxiliary checkpoint/artifact operation rows carry
none of these facts.

## Invocation-operation authorization

`authorization.invocation_operations` is an operator-only, startup-snapshotted opt-in.
Its `start_enabled`, `observe_enabled`, and `cancel_enabled` flags default to false, so
existing owner and route checks remain the complete behavior unless an operator enables a
control. Enabling any flag requires `authorization.enabled: true` and exactly one provider
from the Gateway's coherent `AuthorizationProviderResolver`; startup fails if that provider
is missing, ambiguous, or cannot initialize. Each enabled decision uses the provider's async
API under `timeout_seconds` (default 2.0). Denial is distinct from an indeterminate timeout,
exception, malformed response, or unavailable provider, and both fail closed.

Fresh starts authorize `resource="invocation"`, `action="start"`, and target the sealed
`agent:<id>@sha256:<revision>` after principal, Origin, thread/context, request digest, and
agent revision resolution but before `RunRow` admission. Only host-built safe facts reach the
provider. Allowed rows persist bounded authorization generation, policy ID, reason codes,
and an evidence digest in `decision_evidence_json`; provider metadata, messages, and claims
are never stored. A denial or indeterminate result creates no run or lifecycle event and
does no graph/model/tool work.

Observation first applies the existing owner/route visibility boundary. A visible run uses
`action="observe"`, target `run:<run-id>`; a visible thread feed makes one decision for
`context:<thread-id>`. Cancellation similarly applies visibility, then `action="cancel"`
for `run:<run-id>` before the atomic cancellation receipt/state mutation. Unknown or
owner-invisible rows do not call policy. Observe/cancel decisions are not cached.

For keyed replay, lookup and visibility happen first. A known row receives a fresh observe
decision and is compared with its stored accepted evidence without rerunning start policy or
contributors. A concurrent admission loser repeats visibility, observe authorization, and
digest comparison; only the creator attaches a worker. Invocation checks resolve through the
same provider snapshot used by route, resource, tool, model, skill, and agent assembly paths.

## Restrictive invocation constraints

`deerflow-extension-api` 0.4.0 defines one optional, singular
`InvocationConstraintsProvider`. Gateway invokes it only for a genuinely absent invocation,
after invocation-start authorization allows and before atomic acceptance. The request binds
the canonical request digest and pinned agent-revision digest. The strict v1 projection may
only provide a positive `max_total_subagents`, short-lived evidence timestamps/revision, and
safe evidence ID/digest. Authorization remains the sole binary permission authority.

The Capability Host runs the provider directly—not through observational fail-open
middleware—with its own two-second timeout and timezone-aware clock. It rejects unknown
fields, malformed/binding evidence, future skew beyond 30 seconds, expired or over-15-minute
projections, and ceilings the active runtime cannot enforce. A provider rejection is denied;
timeout, exception, malformed output, or uncertainty is indeterminate. Either outcome stops
before row creation and graph/model/tool work. Optional provider absence preserves existing
behavior. Operators can make the provider startup-required only with
`required_capabilities: [invocation_constraints.v1]` in `config.yaml`.

An allowed projection is intersected with the static host ceiling and only that normalized
effective projection is persisted in `decision_evidence_json`. A visible matching known
invocation bypasses projection and reuses the stored result, even after it expires; expiry
never creates a second execution under the same key. The worker checks the stored request,
revision, and evidence binding plus freshness before graph construction, then checks
freshness again immediately before the first graph `astream`. Non-expiry evidence failures
end the run with `constraint_evidence_mismatch`; queue/pre-stream expiry uses
`constraint_expired_before_start`; both occur with zero graph/model work.

For `max_total_subagents`, the worker installs one invocation-scoped, concurrency-safe
reservation counter into the lead runtime. The task tool reserves a stable tool-call ID
immediately before each dispatch, shares that same object with delegated subagents, rejects
the dispatch that would exceed the limit, and does not double-count a retry of an already
reserved ID. The existing token-budget and delegation-ledger middleware remain useful
observational/post-response guards, but neither is advertised as this exact boundary.

## Atomic idempotent admission

A normal `RunRow` is both the accepted invocation and the executable run; auxiliary thread
operation rows are never invocations. Keyed admission stores `external_scope`,
`external_key`, and the `sha256-canonical-json-v1` request digest atomically with the pending
row. A partial unique index over scope/key arbitrates concurrent processes. The store returns
only `created`, `known_same`, or `key_conflict`, and `InvocationRuntime` attaches a worker only
for `created`.

HTTP create/stream/wait routes accept `Idempotency-Key`. Their scope is tied to the
authenticated server subject; auth-disabled mode uses the configured default user and an
ownerless keyed request is rejected. Native channels use only a verified stable provider
event/message ID, scoped by provider, connection, workspace, and chat. Scheduled Tasks use
their persisted `task_run_id`, scoped by persisted owner and task. Short keys are stored with
an explicit `raw:` prefix; values longer than 255 UTF-8 bytes use `sha256:utf8:`. A missing
channel provider ID leaves that launch unkeyed—content hashes are never substitutes.

The request digest covers accepted principal/base-Origin/context evidence, bound thread,
pinned agent revision, extension generation, normalized input/command and attachments,
checkpoint selection, multitask/interrupt settings, effective execution context, and the
clamped recursion limit. Stream mode, route choice, disconnect/delivery preferences,
callbacks, trace data, temporary credentials, and temporary attachment URLs are excluded.
Known visible rows are checked against stored accepted facts without rerunning contributors
or current alias/plugin resolution. Equal requests return the same row in active or terminal
state; a changed binding, authenticated source fact, input, or execution option conflicts.
A different key still follows the independent active-thread rule (`reject` is thread-busy;
`interrupt`/`rollback` supersede atomically). The replay guarantee ends when the retained row
is deleted.

## Embedded runtime API and lifecycle observation

`deerflow-runtime-api==0.1.0` owns the frozen, strict, standard-library-only
`deerflow.runtime/v1` records. `app.runtime.api.build_in_process_runtime_api()`
binds those records to the Gateway application and one already-authenticated
service ID. The adapter derives the service principal, Origin, and canonical
hashed scope; a caller can provide only an external key, thread ID, optional
agent hint, strict graph/resume input, and the finite v1 execution options.

The API offers `ensure`, invocation/context `observe`, fenced cancellation via
`control`, and truthful capabilities. It reuses the same idempotency,
authorization, constraints, admission, worker attachment, visibility, and
lifecycle stores as HTTP/channel/scheduler launches. Known requests return the
retained normal run, while conflicts, thread-busy, denial, indeterminate policy,
and safe request failures remain finite outcomes. Auxiliary operation rows are
never visible.

The Gateway publishes the same adapter at `/api/runtime/v1`: capabilities,
ensure, one-invocation observation, one-context observation, and fenced control.
Authentication supplies the principal and external-key scope; HTTP bodies cannot
provide either. The capabilities route is administrator-only, observation and
control retain owner/admin visibility, and every non-success response uses the
bounded versioned `runtime.error` envelope. The existing LangGraph-compatible
create/stream/wait routes remain unchanged.

Lifecycle observations return a fixed safe snapshot projection plus authoritative
events, opaque `next_cursor`, the `minimum_available_cursor`, and a captured
`read_fence_cursor`. Visibility and optional observe authorization run before
the store query. PostgreSQL pages use one read-only repeatable-read snapshot and
SQLite pages one explicit read transaction; filtered empty pages advance to the
global fence without skipping a future match. Administrative pruning is explicit
and monotonic, and stale/ahead cursor conditions are typed. Reads are at least
once, so consumers deduplicate with stable event IDs/cursors.

## Pinned agent construction

Acceptance resolves `ResolvedAgentMaterialV1` once. Its versioned projector covers agent
storage source/version, validated agent configuration, SOUL bytes, resolved non-secret model
execution settings and opaque secret-handle IDs, effective tool groups/tools, enabled skill
manifest/content digests, and thinking/reasoning/planning/subagent defaults. Guard tests make
new graph-factory configuration fields choose explicit inclusion or exclusion.

The accepting worker receives and uses that exact captured object; lead and subagents inherit
its revision digest and extension generation for audit. After restart, the worker resolves
current material once. Only an equal digest allows that exact newly resolved object to become
the pinned factory input. A mismatch sets terminal error state with
`stop_reason=agent_revision_drift` immediately before construction, so no graph or model work
occurs. The runtime never reconstructs historical material or performs a compare followed by
a mutable-state reread.

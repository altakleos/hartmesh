# Durable Invocation Runtime

`app.runtime.InvocationRuntime` is the single in-process application boundary for durable
HTTP, Scheduled Task, authenticated native-channel, and embedded-service launches. Scheduling and channel
delivery remain source-owned; normalization, accepted-fact sealing, durable admission, and
one worker attachment belong to the runtime. Checkpoint and artifact reservations are
auxiliary thread operations, not accepted invocations. Each invocation is one normal
`RunRow(operation_kind="run")`; there is no separate invocation row or table.

## Trust and sealing

Every source constructs an internal launch intent, but caller thread, assistant, agent,
body context, headers, queries, and metadata remain hints. Before admission the host:

1. authenticates the effective subject and optional acting service and, for channels,
   authenticates the provider event, looks
   up the connection, and revalidates its current owner;
2. resolves the thread, agent, source facts, normalized input, and execution-significant
   options;
3. creates a bounded base `InvocationOrigin` containing only independently authenticatable
   source/correlation evidence;
4. runs trusted Origin and run-context contributors; and
5. seals an immutable `AcceptedInvocation` and admits it with the normal active-thread
   conflict rule before attaching one worker.

The accepted object contains a minimal split identity projection, sealed Origin, bound
thread/context references, resolved agent revision, normalized input/options, immutable
extension generation, and versioned principal, base-Origin, accepted-context,
runtime-identity, and contributor-execution digests. Contributors cannot replace host-owned
principal, thread, agent, source kind, or base-source fields.

After contributor validation the Gateway also seals one `TrustedRunContextV1`: split
identity, final Origin including contributor references, thread/external-key binding,
agent/profile revisions, extension generation/manifest digest, approved persistable
references, runtime-only execution references, and stable secret handles. Its digest is part
of the committed evidence. A separate execution digest feeds accepted-context identity and
excludes contributor correlation, so audit correlation is bound without changing execution
semantics. The accepting worker binds the run ID
and installs this exact immutable object at the lead execution seam; authorization,
constraints, MCP, and subagents consume it rather than assembling parallel dictionaries.

Identity has three non-interchangeable parts. `EffectiveSubjectV1` is the human or service
whose authority the invocation exercises. `ActingServiceV1` is optional and identifies the
authenticated/delegating service representing that subject. `InvocationOrigin` contains
only trusted source/transport evidence. Direct humans have no acting service; a channel user
and user-owned schedule remain human with `channel:<provider>` or `scheduler` as actor; a
system-owned schedule has service subject `scheduler` and no invented human; and the
embedded adapter has its authenticated service ID as the service subject. Provider,
connection, chat, event, and task facts never confer subject authority.

Gateway code builds all three parts after authentication and removes caller-supplied
identity, actor, Origin, and legacy internal flags. The same immutable identity and final
Origin flow to Gateway route and start/observe/cancel policy, contributors, constraints,
tool/MCP policy and preparation, and lead/nested subagents. Their principal and Origin digests are accepted
evidence persisted in the same admission transaction.

## Contributor data and redaction

Public contributor contracts live in `deerflow-extension-api`, which remains independent
of the harness and Gateway. Contributor factories are startup-only, loader-attributed
capabilities. Calls are concurrent, each has a two-second timeout, and successful results
are composed in stable contribution-ID order.

Results contain only bounded namespaced scalar/list references. The 32-reference and 8 KiB
canonical limits apply both per contributor result and across the combined Origin/run-context
products; fully qualified keys must be unique. Approved persistable references and
persistable stable-handle identifiers enter accepted evidence. Runtime-only execution values
stay only in the accepting process, while their digest/count remains bound to acceptance.
Storage labels are contributor requests, not authority: the host applies this finite policy
and rejects runtime-only correlation because it has no approved consumer.
Correlation references are retained for audit but do not change execution identity. Raw
credentials, arbitrary objects, nested maps, and plugin exception text are never persisted
or reflected in diagnostics; a diagnostic contains only stable code, exception class,
contribution ID, and correlation ID. Optional failures are omitted; a configured required
capability fails startup when absent/broken and fails an invocation closed when it times out
or returns invalid data.

The worker reserves `__deerflow_trusted_run_context` for the host-owned record and removes
caller values plus the old free-form `authz_attributes` map. Compatibility policy receives
only a read-only namespaced view derived from accepted execution references. Stable secret
handles are not credentials or authority; the already-authorized narrow MCP/operation
boundary may resolve one transiently, and the resolved value never enters generic run kwargs,
rows, checkpoints, lifecycle/rich events, public responses, or logs. A known-key replay
reuses accepted evidence without rerunning contributors. Under the supported single-process
recovery contract, a restart that loses required ephemeral values ends the retained run with
`trusted_context_unavailable` before graph construction rather than reconstructing them.
The trusted-context evidence and execution digests are calculated from persisted-safe
projections plus the retained runtime-only digest/count, so both remain stable after
reconstruction even though ephemeral values are absent.

## Persistence

Normal `runs` rows have nullable, backward-compatible columns for Origin and principal
projections and digests, accepted-context and agent-revision digests, safe agent revision
metadata, extension generation, and versioned empty decision evidence. Full resolved agent
material, credentials, and runtime-only contributor references are not serialized. A keyed
run additionally stores an internal request projection in its existing run kwargs so
retries can be compared against accepted routing and contributor evidence without resolving
today's mutable host state; this internal projector is not returned by the run API.
The versioned `decision_evidence_json` also carries the safe persisted trusted-context
projection; older rows without it remain readable through the conservative legacy path.
Historical null rows remain readable. Auxiliary checkpoint/artifact operation rows carry
none of these facts.

Principal projection v2 stores the nested v1 split identity. Historical v1 projections
remain readable, but their legacy `is_internal` flag is treated conservatively: an
attributed channel user or a principal whose role is not `service`/`internal` is never
promoted. New providers should use `Principal.identity`; older providers continue to receive
the flattened fields and a compatibility boolean that can remove privilege but cannot grant
service authority to a represented human.

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

`deerflow-extension-api` 0.9.0 defines one optional, singular constraints provider with
separate v1 and v2 contracts. Gateway invokes it only for a genuinely absent invocation,
after invocation-start authorization allows and before atomic acceptance. V2 receives only
the sealed split identity and final Origin, bounded namespaced correlation lookup references,
thread/external-key binding, pinned agent/profile revisions, request/trusted-context/manifest
digests, extension generation, and the host's enforceable subagent ceiling. It receives no
content, credential, arbitrary kwargs, or opaque host object. Authorization remains the sole
binary permission authority, and dynamic effects remain subject to operation-time
authorization and MCP preparation.

The Capability Host runs the provider directly—not through observational fail-open
middleware—with its own two-second timeout and timezone-aware clock. V2 binds the exact
request, trusted context, thread, agent/profile revisions, manifest, and extension generation
to its projection and evidence. It rejects unknown fields, malformed/binding evidence,
future skew beyond 30 seconds, expired or over-15-minute projections, unsupported mandatory
obligations, and ceilings the active runtime cannot enforce. A provider rejection is denied;
timeout, exception, malformed output, or uncertainty is indeterminate. Either outcome stops
before row creation and graph/model/tool work. Optional provider absence preserves existing
behavior. Operators make v2 startup-required only with
`required_capabilities: [invocation_constraints.v2]` in `config.yaml`.
For that required path, a genuinely absent invocation also requires a fresh healthy snapshot
for the exact v2 capability before projection; missing, stale, unknown, or unhealthy health
is indeterminate. Matching keyed replay remains pinned to accepted evidence and bypasses both
the live health check and provider.

An allowed projection is intersected with the static host ceiling and only that normalized
effective projection, its mandatory-obligation list, and a canonical projection digest are
persisted in `decision_evidence_json` in the same admission transaction. A visible matching known
invocation bypasses projection and reuses the stored result, even after it expires; expiry
never creates a second execution under the same key. The worker checks the stored request,
request/thread/revisions/trusted-context/manifest/generation and evidence binding plus
freshness before graph construction, then checks the same projection, supported obligations,
and freshness again immediately before the first graph `astream`. Non-expiry evidence failures
end the run with `constraint_evidence_mismatch`; queue/pre-stream expiry uses
`constraint_expired_before_start`; both occur with zero graph/model work.

For `max_total_subagents`, the worker installs one invocation-scoped, concurrency-safe
reservation counter into the lead runtime. The task tool reserves a stable tool-call ID
immediately before each dispatch, shares that same object with delegated subagents, rejects
the dispatch that would exceed the limit, and does not double-count a retry of an already
reserved ID. The existing token-budget and delegation-ledger middleware remain useful
observational/post-response guards, but neither is advertised as this exact boundary;
exact token limits are deferred.

V1 remains readable and usable only through its explicit `invocation_constraints.v1`
registration. It retains its original positive-only, subagent-ceiling semantics and cannot
satisfy a v2 operator requirement or be advertised as full v2 policy context.

## Atomic idempotent admission

A normal `RunRow` is both the accepted invocation and the executable run; auxiliary thread
operation rows are never invocations. Keyed admission stores `external_scope`,
`external_key`, a `caller-intent-canonical-json-v1` projection/digest, and the separate
`sha256-canonical-json-v1` accepted effective-execution digest atomically with the pending row.
The caller-intent projection records only the execution semantics supplied through the
caller-facing contract. The effective projection records server-resolved defaults and pinned
principal, Origin, agent-revision, extension, contributor, thread, and execution facts used by
authorization, constraints, and the worker. A partial unique index over scope/key arbitrates
concurrent processes. The store returns only `created`, `known_same`, or `key_conflict`, and
`InvocationRuntime` attaches a worker only for `created`.

HTTP create/stream/wait routes accept `Idempotency-Key`. Their scope is tied to the
authenticated server subject; auth-disabled mode uses the configured default user and an
ownerless keyed request is rejected. Native channels use only a verified stable provider
event/message ID, scoped by provider, connection, workspace, and chat. Scheduled Tasks use
their persisted `task_run_id`, scoped by persisted owner and task. Short keys are stored with
an explicit `raw:` prefix; values longer than 255 UTF-8 bytes use `sha256:utf8:`. A missing
channel provider ID leaves that launch unkeyed—content hashes are never substitutes.

Caller-intent equality is exact after these contract-defined normalizations:

- Graph input and resume input are distinct; mapping key order is irrelevant, while array,
  message, content-block, and attachment order remains significant.
- An explicit thread and the stateless server-assigned-thread selection are distinct. Repeating
  the stateless selection is equal; copying the generated thread into a retry is not.
- The default agent selector and explicit default aliases normalize together. Bootstrap routing
  includes its selected agent name. A different agent selector conflicts even if it would
  currently resolve to the same material.
- Nullable model/thinking/reasoning/planning/subagent context options, checkpoint selectors,
  and interrupt selectors define null as absence. Removing a non-null value or adding one to a
  previously absent value conflicts. Explicit `multitask_strategy="reject"` equals its
  contract default.
- A missing or null recursion limit selects the Gateway default. Every non-null supplied limit
  is retained as caller intent before server validation/clamping, so removing or changing it
  conflicts even when two values would clamp to the same effective limit.
- Gateway config context takes precedence over configurable context, and body context fills only
  values not supplied there. Trusted source-only context remains host-built.

Stream mode, subgraph streaming, stream/wait route choice, disconnect/delivery preferences,
callbacks, trace data, temporary credentials, and temporary attachment URLs are transient and
excluded from caller equality. Keyed request metadata or unclassified config/context remains
invalid rather than being silently omitted.

A known visible row is first checked against current authenticated principal/base-Origin and
explicit binding evidence, then only the fresh canonical caller intent is compared. Equality
returns the same row in active or terminal state and reuses its accepted effective projection
and lifecycle without rerunning contributors, start authorization, constraint projection,
default or alias resolution, or graph execution. Replay never starts with accepted effective
values and overwrites only fields present on the retry. Rows written before caller-intent
evidence exists remain readable, but a keyed replay conflicts because equality cannot be
proven. A different key still follows the independent active-thread rule (`reject` is
thread-busy; `interrupt`/`rollback` supersede atomically). The replay guarantee ends when the
retained row is deleted.

## Embedded runtime API and lifecycle observation

`deerflow-runtime-api==0.1.0` owns the strict, standard-library-only
`deerflow.runtime/v1` records and the `DurableInvocationPort` Protocol. The
Protocol is the complete transport-neutral seam: `ensure`, invocation/context
`observe`, fenced `control`, and `capabilities`; it exposes no application,
storage, worker, graph, session, or framework type. All public records are
transitively immutable snapshots. Parsing freezes nested JSON mappings and
sequences, while every `to_dict()` call returns a fresh mutable wire copy.

`app.runtime.api.build_in_process_runtime_api()` binds that Protocol to the
Gateway application and one already-authenticated service ID. The HTTP facade
depends on the same Protocol, and the shared conformance suite exercises both
adapters. The adapter derives one service effective subject (used consistently for
start, observe, and cancel), Origin, and canonical
hashed scope; a caller can provide only an external key, thread ID, optional
agent hint, strict graph/resume input, and the finite v1 execution options.

The host-internal trust seam applies the same defensive rule to launch values:
`InternalLaunchIntent` and `PreparedLaunch` recursively snapshot mappings,
sequences, sets, checkpoint/config/context values, and callback collections.
Only fresh mutable container copies cross into Gateway normalization,
RunManager persistence, and graph execution. Opaque callback objects retain
their operational identity; their caller-owned containing collections do not.

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

`runtime.capabilities` is one strict portable record and is byte-shape identical
across in-process and HTTP Adapters. It never contains deployment state. The
administrator-only `GET /api/runtime/v1/deployment` uses a separate
`deerflow.deployment/v1` Interface for extension manifest/health, bounded
provenance, persistence tier, and explicit qualification status. Persistence
atomicity is independent from restart/pod-loss durability: memory is
`process_local`, SQLite is `node_durable`, and PostgreSQL is `shared_durable`.
The `durable_production` deployment profile fails startup/readiness with
process-local state; `local_development` remains an explicit convenience profile.
The report also carries the latest safe admission-readiness status, reason codes,
and correlation identifier. Raw provider/database exception text remains internal.
Unexpected Adapter exceptions become bounded indeterminate failures with a
correlation ID matching a safe internal diagnostic; exception text is never public.

Lifecycle observations return authoritative events plus bounded
`InvocationSummaryV1` records joined from accepted normal `RunRow` rows. A
summary is the single public representation of static source/evidence facts:
current state/version, sealed source kind, bounded safe Origin correlation
references, agent revision, extension generation/manifest, and caller-intent,
accepted-context, authorization, and constraint evidence digests. Static facts
are not copied into each event. Historical rows without a sealed Origin retain
legacy event/snapshot readability but cannot prove a summary.

Visibility and optional observe authorization run before the store query.
Context observation can filter the strict source kinds `http`,
`scheduled_task`, `native_channel`, and `service`; filtering never weakens owner
scope. PostgreSQL pages use one read-only repeatable-read snapshot and SQLite
pages one explicit read transaction for cursor metadata, events, and summaries.
Only distinct run IDs present in the bounded event page are materialized, so a
long thread cannot turn one page into an all-run scan. Limits are 1–500 events,
4 KiB per lifecycle payload, 16 KiB per summary, and 12 MiB per portable
observation. Opaque `next_cursor`, `minimum_available_cursor`, and
`read_fence_cursor` retain the global pruning-aware semantics; filtered empty
pages advance to the fence without skipping a future match. Reads are at least
once, so consumers deduplicate with stable event IDs/cursors.

## Authoritative lifecycle and failure recovery

Normal admissions commit at `state_version=1` with `accepted`. Each successful start,
first cancellation request, terminal completion, supersession, attachment failure, or
orphan recovery increments `state_version` once and writes exactly one matching safe
lifecycle row in the same transaction. The complete v1 vocabulary is `accepted`, `started`,
`cancellation_requested`, `cancelled`, `succeeded`, `failed`, `timed_out`, and
`interrupted`; `RunRow.status` remains authoritative. A stale compare-and-set changes
neither row nor journal. Interrupt/rollback replacement commits every predecessor
transition and the replacement acceptance as one ordered batch.

Process death after acceptance is recovered by the current lease/orphan scan as `failed`
with `orphan_recovered`; attachment failure is `worker_attachment_failed`. These guarantees
make retained keyed retries converge, but do not provide scheduler HA or a multi-replica
Gateway ownership design beyond the explicitly configured PostgreSQL lease and stream
primitives. Kubernetes pod-termination qualification remains a deployment release gate.

## Graceful Gateway shutdown

`app.gateway.shutdown.GracefulShutdownCoordinator` is the sole production owner
of shutdown ordering and deadline accounting. It atomically closes the admission
permit seam, stops channel and scheduler producers, requests interruption and
bounded drain of locally active runs, flushes memory only after admission and run
writers are quiescent, then closes retrieval, memory, browser, OIDC, stream, and
database resources. Concurrent or repeated shutdown calls share one result and do
not emit duplicate terminal transitions.

`deployment.shutdown` supplies the admission, channel, scheduler, run, and final
dependency sub-budgets. The memory phase uses
`memory.shutdown_flush_timeout_seconds`; their sum is the absolute application
deadline. A timed-out subsystem records only a stable code, error class, phase,
and correlation ID while later phases continue within the remaining deadline. If
admission or run quiescence cannot be proven, the coordinator deliberately skips
memory flush/close rather than racing a late writer. Active locally owned runs are
requested to interrupt; any run still unsettled is left to the existing durable
orphan recovery on restart. Normal acceptance holds the admission permit through
worker attachment, so graceful shutdown cannot enter between those operations;
process loss after committed acceptance but before attachment remains an
`orphan_recovered` case. Pending/running local tasks receive `interrupt`, a real
terminal commit that wins the race is preserved, and already-terminal rows are
untouched. These guarantees cover the supported one-replica
topology and repository process-loss simulations only, not live Kubernetes pod
termination qualification.

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

## Capability health and required MCP preparation

The Capability Host publishes a restart-only immutable manifest, generation, and digest.
New accepted invocations pin that generation/digest, while live health is a separate mutable
snapshot and cannot change it. Every successful health observation records its generation and
timestamp. `GET /health` remains minimal process liveness during recoverable dependency
outages; `GET /ready` fails closed for missing, failed, stale, unhealthy, or
generation-mismatched operator-required authorization,
contributors, invocation constraints, or required MCP preparation, and for corrupt lifecycle
cursor ordering and an unsatisfied durable deployment profile. The administrator-only
deployment report exposes the safe manifest, separately labelled health snapshots,
persistence truth, optional artifact provenance, and qualification state; the portable
runtime capabilities record deliberately does not.

The same coordinator fences every genuinely absent HTTP, scheduled-task, native-channel, and
embedded-service invocation before normalization and durable acceptance. It runs required
health probes concurrently with bounded lifecycle integrity under the configured overall
timeout. The authoritative `authorize`, `contribute`, or `project` call then remains the final
proof and fails closed independently. Known keyed replay is resolved before this fence and
reuses its accepted evidence without rerunning health or authority code. Defaults and bounds
live under startup-only `deployment.readiness`: 10-second cache/admission health, 30-second
diagnostic staleness, two-second per-probe timeout, five-second overall timeout, and a required
failure threshold fixed to one.

Required MCP interceptors run only after the coherent authorization provider allows. At the
final network fence, the host verifies the pinned generation and fresh required health, then
composes bounded preparation in contribution-ID order. Failure or conflicting transient
headers call the handler zero times. Preparation cannot grant permission, and transient
credentials never enter run rows, checkpoints, lifecycle/rich events, manifests, or logs.
The API-writable legacy interceptor path remains optional warning-and-skip compatibility and
cannot satisfy an operator requirement.

## Compatibility and deferred scope

Existing LangGraph SDK and DeerFlow REST create/stream/wait response contracts remain
compatible facades over the durable Gateway path. The synchronous `DeerFlowClient` remains a
documented non-durable local graph path; applications that need durable embedded parity use
the supported asynchronous `deerflow-runtime-api` adapter instead.

The following are deferred and are not implemented or promised by this contract: context
export or retirement, group/dynamic outbound governance, scheduler HA, a general
multi-replica Gateway ownership model, an artifact catalogue, a full profile registry, an
event sink or broker, and durable embedded parity for synchronous `DeerFlowClient`. Exact
token constraints are also deferred; v1 enforces only the exact total-subagent count ceiling
described above.

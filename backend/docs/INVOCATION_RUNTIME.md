# Durable Invocation Runtime

`app.runtime.InvocationRuntime` is the single in-process application boundary for durable
HTTP, Scheduled Task, authenticated native-channel, and embedded-service launches. Scheduling and channel
delivery remain source-owned; normalization, accepted-fact sealing, durable admission, and
one worker attachment belong to the runtime. Checkpoint and artifact reservations are
auxiliary thread operations, not accepted invocations. Each invocation is one normal
`RunRow(operation_kind="run")`; there is no separate invocation row or table.

## Identifier domains

Admission validates policy-visible identities before sealing. Agent input matches
`[A-Za-z0-9][A-Za-z0-9-]{0,127}`; the existing case-insensitive agent store makes
lowercase the explicit canonical agent identity returned by host and portable records.
`lead_agent` remains the reserved built-in runtime identity and cannot be created as a
custom agent.
Thread IDs preserve exact case and match `[A-Za-z0-9_-]{1,64}` everywhere, including
constraints and MCP projections. Model-profile IDs preserve exact case/Unicode, reject
ASCII controls, and are limited to 128 UTF-8 bytes in configuration, accepted evidence,
portable options, and run persistence. Existing over-bound profile names require an
operator rename of `models[].name` and every agent/request reference before startup;
Hartmesh does not truncate or create a hidden alias.

MCP server configuration keys preserve exact case/Unicode under their own non-control
128-byte bound. MCP callable tool names preserve exact case and match
`[A-Za-z0-9_-]{1,128}`. A server key outside the callable-tool grammar must explicitly
disable `tool_name_prefix`; a prefix-enabled key is limited to 126 characters so the
separator and a non-empty tool name fit the callable bound. Server keys and tool names are
not contributor or agent IDs.

## Concern-to-evidence closure matrix

This matrix is the release closure record for the current one-replica durable invocation
contract. “Implemented” means the invariant has executable repository evidence. A deployment
qualification is named separately where storage or infrastructure behavior cannot be proven by
an ordinary unit test.

| Concern | Implemented invariant | Implementation | Named evidence | Deployment evidence | Status |
|---|---|---|---|---|---|
| Application Module and portable Adapters | One application-layer invocation Module owns ensure/observe/control; the in-process and HTTP Adapters implement one host-independent Protocol. | `backend/app/runtime/invocation.py`; `backend/app/runtime/api.py`; `backend/packages/runtime-api/deerflow_runtime_api/__init__.py` | `test_runtime_transport_conformance` | Strict HTTP/in-process conformance | Implemented |
| All durable launch sources | HTTP create/stream/wait, Scheduled Tasks, native channels, and embedded services enter the same durable admission boundary. | `backend/app/gateway/services.py`; `backend/app/scheduler/service.py`; `backend/app/channels/manager.py`; `backend/app/runtime/api.py` | `test_gateway_create_stream_wait_routes_share_durable_admission`; `test_scheduled_occurrence_enters_runtime_with_typed_execution_facts`; `test_authenticated_channel_launch_enters_runtime_with_typed_source_facts`; `test_ensure_builds_a_host_trusted_service_launch_intent` | Offline launch-source characterization | Implemented |
| Keyed native ingress receipts | Signed GitHub acknowledges only after an atomic bounded receipt batch; leased/fenced recovery composes with stable-key invocation replay, and busy work stays durable FIFO. | `backend/app/channels/inbound_receipts.py`; `backend/app/channels/service.py`; `backend/app/gateway/github/dispatcher.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0015_inbound_receipts.py` | `test_received_payload_survives_process_loss_before_claim`; `test_response_loss_after_admission_replays_known_run_before_binding`; `test_signed_route_reaches_real_runtime_and_redelivery_replays` | PostgreSQL receipt concurrency/migration qualification | Implemented for signed GitHub with PostgreSQL; other/local paths report best-effort |
| Canonical keyed replay | Atomic external-key arbitration compares caller intent, retains accepted effective execution, and attaches only one worker. | `backend/app/runtime/idempotency.py`; `backend/packages/harness/deerflow/persistence/run/sql.py` | `test_http_replay_conflicts_when_original_execution_field_is_removed` | PostgreSQL equal/unequal race qualification | Implemented; PostgreSQL locking is a gated qualification |
| Split identity and sealed Origin | Effective subject, optional acting service, and trusted source evidence are non-interchangeable and caller-forgery resistant. | `backend/packages/extension-api/deerflow_extension_api/authorization.py`; `backend/app/gateway/services.py`; `backend/app/runtime/authorization.py` | `test_channel_human_cannot_be_promoted_by_internal_transport` | Source-specific launch tests | Implemented |
| Trusted contributor context | One immutable trusted context carries validated persistable evidence, runtime-only values, and stable handles without secret persistence. | `backend/packages/extension-api/deerflow_extension_api/contributors.py`; `backend/packages/harness/deerflow/extensions/contributors.py`; `backend/packages/harness/deerflow/runtime/accepted_invocation.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/packages/harness/deerflow/subagents/executor.py`; `backend/packages/harness/deerflow/mcp/tools.py` | `test_worker_carries_one_trusted_context_without_parallel_attributes_path` | Store/event/response redaction tests | Implemented |
| Restrictive authorization and constraints | Authority failures fail closed; v2 constraints bind accepted material and can only narrow the exactly enforced subagent ceiling. | `backend/app/runtime/authorization.py`; `backend/app/runtime/constraints.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/packages/harness/deerflow/subagents/executor.py` | `test_v2_host_rejects_projection_for_different_bound_material`; `test_worker_enforces_zero_ceiling_before_any_subagent_dispatch` | Required-capability health/readiness tests | Implemented |
| Pinned agent and extension material | Accepted agent material and extension generation remain pinned; every effective skill package is copied and hashed from one bounded immutable snapshot shared by lead, slash/deferred discovery, policy, sandbox, and subagent consumers; drift fails before graph/model work. | `backend/packages/harness/deerflow/runtime/accepted_invocation.py`; `backend/packages/harness/deerflow/runtime/agent_revision.py`; `backend/packages/harness/deerflow/runtime/skill_snapshot.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; skill middlewares and sandbox providers | `test_restart_drift_fails_before_graph_construction_or_model_work`; `test_same_process_live_edit_cannot_replace_accepted_slash_skill`; `test_deleting_live_tree_cannot_remove_accepted_supporting_material`; `test_live_allowed_tools_edit_cannot_widen_accepted_policy`; `test_snapshot_drift_fails_before_graph_construction` | Process-reconstruction tests; skill bytes are process-local and lost workers terminalize rather than resume | Implemented for the supported one-replica process contract |
| Transactional lifecycle evidence | Every normal-run state change increments one state version and commits its safe lifecycle event atomically. | `backend/packages/harness/deerflow/persistence/run/sql.py`; `backend/packages/harness/deerflow/runtime/runs/store/base.py`; `backend/packages/harness/deerflow/runtime/runs/store/memory.py` | `test_sql_row_and_lifecycle_event_commit_together` | PostgreSQL CAS/cursor qualification | Implemented; PostgreSQL atomicity is a gated qualification |
| Polling observation and bounded summaries | Authorized observation reads a pruning-aware bounded page and its source-aware summaries from one database snapshot. | `backend/packages/harness/deerflow/runtime/runs/lifecycle_query.py`; `backend/packages/harness/deerflow/persistence/run/sql.py`; `backend/app/runtime/api.py` | `test_context_query_excludes_other_owners_and_auxiliary_rows`; `test_malformed_ahead_and_pruned_cursors_are_typed`; `test_sql_context_page_loads_summary_rows_only_for_bounded_page_ids`; `test_postgres_query_uses_one_repeatable_read_snapshot` | Repeatable-read PostgreSQL query qualification | Implemented; PostgreSQL snapshot isolation is a gated qualification |
| Scoped service observation | An authenticated service is owner-scoped unless an operator grants a finite run/thread/owner/source search scope; the current coherent authorization provider still makes the final observe decision. | `backend/app/runtime/visibility.py`; `backend/app/runtime/invocation.py`; `backend/packages/harness/deerflow/persistence/run/sql.py` | `test_ordinary_service_cannot_observe_another_owner_or_trigger_policy`; `test_current_authorization_denial_overrides_a_valid_visibility_grant`; `test_context_pagination_stays_inside_the_finite_owner_scope` | Memory and SQL bounded-query tests; no external service | Implemented |
| Clarification continuation | A clarification ends the current invocation successfully; the answer is a distinct invocation on the same thread. | `backend/packages/harness/deerflow/agents/middlewares/clarification_middleware.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/app/channels/manager.py` | `test_clarification_completes_then_answer_starts_new_same_thread_invocation`; `test_native_channel_revalidates_owner_dedupes_and_continues_clarification` | Native-channel characterization | Implemented; same-invocation suspension is not claimed |
| Graceful shutdown and process recovery | One deadline coordinator freezes admission, stops producers, drains runs, flushes memory, then closes dependencies; unsettled durable runs use orphan recovery. | `backend/app/gateway/shutdown.py`; `backend/packages/harness/deerflow/runtime/runs/manager.py` | `test_shutdown_orders_producers_runs_memory_and_dependencies`; `test_orphan_recovery_records_failed_with_stable_reason` | Process-loss simulations; live pod evidence is separate | Implemented for one replica |
| PostgreSQL schema and arbitration | Real Alembic predecessor data upgrades through accepted/idempotency/lifecycle/receipt evidence and remains repository-readable after the promised downgrade/re-upgrade path. | `backend/packages/harness/deerflow/persistence/migrations/versions/0011_accepted_invocation.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0012_invocation_idempotency.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0013_invocation_lifecycle.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0014_canonical_caller_intent.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0015_inbound_receipts.py`; `backend/packages/harness/deerflow/persistence/run/sql.py` | `test_pre_feature_postgres_upgrade_downgrade_reupgrade_and_runtime_io`; `test_postgres_inbound_receipt_acquisition_and_claim_are_atomic` | `postgres_contract` with `DEERFLOW_TEST_POSTGRES_URL` | Qualified only when the mandatory PostgreSQL gate passes |
| One-replica deployment truth | Production requires one Gateway, shared durable PostgreSQL, compatible readiness/shutdown timing, and digest-pinned Gateway plus enabled provisioner execution artifacts; process-local mode makes no durability claim. | `backend/app/runtime/deployment.py`; `deploy/helm/deer-flow/values.yaml`; `deploy/helm/deer-flow/templates/gateway-deployment.yaml`; `deploy/helm/deer-flow/templates/provisioner-deployment.yaml` | `test_production_mode_requires_pinned_runtime_images_and_one_replica`; `test_production_mode_rejects_process_local_storage` | Helm lint/render and storage-profile checks | Implemented; not a high-availability claim |
| Live Kubernetes pod recovery | The opt-in harness emits one strict canonical artifact; the report exposes only an operator assertion, while the offline verifier independently matches its digest, run/namespace, image, chart, config, schema, scope, and complete scenario set. | `backend/packages/harness/deerflow/qualification_evidence.py`; `backend/tests/support/kubernetes_qualification.py`; `backend/app/runtime/deployment.py`; `backend/scripts/verify_qualification_evidence.py` | `test_exact_external_evidence_verifies_against_declared_reference_and_subjects`; `test_real_one_replica_pod_recovery_contract` | `kubernetes_contract` artifact plus a successful exact-subject offline verification | Unqualified when absent; operator-asserted when declared; externally verified only after the offline verifier succeeds |
| Legacy compatibility and native execution | Existing LangGraph/REST facades retain their responses and native lead-agent, skill, memory, subagent, sandbox, and thread behavior. | `backend/app/gateway/routers/runs.py`; `backend/app/gateway/routers/thread_runs.py`; `backend/packages/harness/deerflow/agents/lead_agent/agent.py`; `backend/packages/harness/deerflow/agents/memory`; `backend/packages/harness/deerflow/subagents/executor.py`; `backend/packages/harness/deerflow/sandbox/middleware.py` | `test_gateway_mounts_runtime_routes_without_replacing_legacy_runs`; `test_full_chain_order`; `test_make_lead_agent_custom_skill_allowlist_does_not_activate_tool_policy`; `test_after_agent_queues_memory_under_runtime_user`; `test_aexecute_propagates_one_trusted_run_context_without_free_form_attributes`; `test_sandbox_middleware_state_matches_thread_state_sandbox_field` | Full offline compatibility suite | Implemented; synchronous client durability remains deferred |

## Trust and sealing

Every source constructs an internal launch intent, but caller thread, assistant, agent,
body context, headers, queries, and metadata remain hints. Before admission the host:

1. authenticates the effective subject and optional acting service and, for channels,
   authenticates the provider event, then either revalidates the current interactive
   connection owner or resolves a signed webhook's trusted route binding;
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

## Keyed native ingress receipts

Invocation admission is durable only after the runtime sees a launch, so a durable
native ingress source also needs a pre-admission record. Signed GitHub supplies that
record when the application uses PostgreSQL receipt storage. After HMAC verification
and trusted registry resolution, the dispatcher builds every fan-out envelope and
commits the whole batch before returning success. A row is uniquely identified by
provider, verified binding kind/reference, and provider delivery ID. `MessageBus`
carries only receipt-ID wake-ups; duplicate or lost wake-ups do not change the ledger.

The receipt lifecycle is `received -> claimed -> admitted(run_id) -> completed`, with
`deferred` for a bounded retry and `dead_letter` for exhausted or permanently invalid
input. A claim has an owner, expiry, attempt counter, and monotonically increasing
fencing token. Only that token may bind, defer, complete, or dead-letter. Recovery reads
bounded due pages, reclaims expired claims, and admits only the earliest unfinished row
for a thread. A crash after invocation acceptance but before receipt binding replays the
same external key and binds the known-equal run; contributors, policy, graph, and model
do not run a second time. Commands and rejections complete with a bounded outcome and no
invented run ID.

Persisted envelopes contain only the finite normalized text/type, owner/agent/thread
route, verified binding, safe provider correlation, and supported stable attachment
handles. The current signed GitHub path rejects transient attachments rather than
persisting their bytes. Webhook secrets, access tokens, raw provider payloads, resolved
credentials, and arbitrary metadata never enter the receipt. The administrator
deployment report exposes `native_ingress.v1` per enabled source as `durable` or
`best_effort`; portable runtime capabilities do not. SQLite/memory and explicitly
memory-backed receipt configuration remain local convenience and cannot satisfy the
durable production profile.

`InboundReceiptProcessor` owns both recovery and in-flight claim tasks. Channel
shutdown stops producers, cancels and awaits those tasks, and leaves an interrupted
claim fenced until its lease expires; a later process reclaims it from PostgreSQL.

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

Observation first applies owner/admin visibility. An authenticated service remains owner-scoped
unless operator `authorization.service_observation_grants` supplies a current finite search
scope for that exact service. A grant may select bounded run, thread/context, owner, or sealed
source-kind identities; selectors are OR-composed, resolved before SQL, and never come from a
request role, transport location, source facts, or `visibility_prevalidated`. Grants require
enabled invocation observe authorization. They only make a row eligible for lookup: the same
coherent provider must then allow `resource="invocation"`, `action="observe"`, target
`run:<run-id>` or `context:<thread-id>` before anything is returned. Scope resolution and the
authorization decision are re-evaluated on every request, so revocation applies to the next
poll without replacing the authorization provider or its generation. Cancellation does not use
observation grants and remains owner/admin-scoped before
`action="cancel"`. Unknown and grant-invisible rows do not call policy and remain
indistinguishable. Observe/cancel decisions are not cached.

For keyed replay, lookup and visibility happen first. A known row receives a fresh observe
decision and is compared with its stored accepted evidence without rerunning start policy or
contributors. A concurrent admission loser repeats visibility, observe authorization, and
digest comparison; only the creator attaches a worker. Invocation checks resolve through the
same provider snapshot used by route, resource, tool, model, skill, and agent assembly paths.

## Restrictive invocation constraints

`deerflow-extension-api` 0.10.0 defines one optional, singular constraints provider with
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
Gateway partitions the complete required-capability list once during application
construction. Both `invocation_constraints.v1` and `invocation_constraints.v2` are routed
only to `InvocationConstraintsHost`; contributor and MCP hosts receive only their own
publicly classified IDs. Unknown/duplicate requirements, duplicate singular registrations,
and a provider whose declared version cannot satisfy the selected requirement fail closed
before admission. This keeps a future constraints version from depending on an unrelated
host's hard-coded pass-through list.
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
event/message ID, scoped by provider, verified binding kind/reference, workspace, and chat.
Interactive channels mint a `connection` binding only after repository owner revalidation
(normally in the manager, or in Buzz's signed adapter through the same lookup helper);
signed webhooks mint a distinct `webhook_route` binding only after request authentication and
trusted route lookup. The historical connection tuple is unchanged for retained-row replay.
The safe binding evidence is sealed into Origin and trusted run context; messages without a
verified binding stay unkeyed even if they carry a stable-looking ID. Scheduled Tasks use
their persisted `task_run_id`, scoped by persisted owner and task. Short keys are stored with
an explicit `raw:` prefix; values longer than 255 UTF-8 bytes use `sha256:utf8:`. A missing
channel provider ID leaves that launch unkeyed—content hashes are never substitutes.
For a historical connection-backed row whose Origin predates the redundant binding fields,
replay compares the original connection-based Origin digest; this compatibility rule never
applies to webhook-route bindings or partially populated binding evidence.

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

`deerflow-runtime-api==0.2.0` owns the strict, standard-library-only
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
The deployment process may supply bounded image/source provenance and a qualification
artifact reference through trusted environment fields. The v1 report retains
`status="qualified"` for wire compatibility but pairs it with
`trust="operator_asserted"`; this states only that the operator declared a bounded
reference. With no reference it returns `status="unqualified"` and
`trust="none_declared"`. The Gateway does not fetch or attest the artifact. Exact digest,
deployment subjects, and scenario coverage become
`trust="external_evidence_verified"` only in the separate offline verifier result. Invalid
declarations degrade to explicit unqualified state; neither deployment facts nor their
configuration enter the portable runtime Interface.

Lifecycle observations return authoritative events plus bounded
`InvocationSummaryV1` records joined from accepted normal `RunRow` rows. A
summary is the single public representation of static source/evidence facts:
current state/version, sealed source kind, bounded safe Origin correlation
references, agent revision, extension generation/manifest, and caller-intent,
accepted-context, authorization, and constraint evidence digests. Static facts
are not copied into each event. Historical rows without a sealed Origin retain
legacy event/snapshot readability but cannot prove a summary.

The portable observation validates the complete page relationship on direct
construction and wire parsing. All snapshots and events belong to the observed
thread; a singular query contains only its requested run. Snapshot and summary
run IDs are unique, every summary joins to one materialized snapshot and agrees
on run, thread, current status, and state version, and a singular snapshot agrees
with the top-level current state. Historical lifecycle events intentionally keep
their transition-time state rather than being rewritten to the latest state.

Visibility and observe authorization run before serialization. The default query is
owner/admin-scoped. For an operator-granted service, the host projects a maximum of 128 finite
run/thread/owner/source selectors into an immutable grant that lives at most 30 seconds and into
the exact SQL or memory-store predicate before paging;
the current authorization decision follows the visibility match. Context observation can also
apply the caller's strict source-kind filter `http`, `scheduled_task`, `native_channel`, or
`service`; that filter only narrows the resolved scope. PostgreSQL pages use one read-only repeatable-read snapshot and SQLite
pages one explicit read transaction for cursor metadata, events, and summaries.
Only distinct run IDs present in the bounded event page are materialized, so a
long thread cannot turn one page into an all-run scan. Limits are 1–500 events,
4 KiB per lifecycle payload, 16 KiB per summary, and 12 MiB per portable
observation. Opaque `next_cursor`, `minimum_available_cursor`, and
`read_fence_cursor` retain the global pruning-aware semantics; filtered empty
pages advance to the fence without skipping a future match. Reads are at least
once, so consumers deduplicate with stable event IDs/cursors.

Polling `DurableInvocationPort.observe` is the supported durable evidence path.
No event sink or broker is required for correctness: observation reads the authoritative run
state and its transactional lifecycle journal directly. A push sink or event broker may be
added only as a separately reviewed delivery mechanism and cannot become an undeclared
correctness or release criterion.

### Clarification is continuation, not suspension

A clarification request completes its current invocation successfully with the structured
request for more information as its result.
The user's answer starts a new invocation on the same thread, so ordinary idempotency,
acceptance, and lifecycle rules apply to that answer.
`input_required` is not a v1 lifecycle state. If a future contract supports true suspension
and resumption of the same invocation, it may add an explicit nonterminal event only through a
separately reviewed, versioned lifecycle change.

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
primitives. Live pod termination is qualified only by the separate opt-in
`kubernetes_contract` suite; its default skip remains an unpassed release gate.

Repository process-loss simulation is not Kubernetes pod-recovery qualification. The
process-loss simulation, image construction, and Helm rendering are separate evidence; none
becomes live pod evidence by implication or aggregation.

The PostgreSQL migration qualification starts both from an empty schema and from
the real main-line predecessor `0010_run_cancel_request` with representative
normal and auxiliary rows. It applies 0011–0015 individually, verifies the
accepted/idempotency/caller-intent columns, checks and partial indexes, validates
the lifecycle singleton/journal/index contract and inbound receipt arbitration, and then uses `RunRepository` for
replay, cancellation, orphan recovery, lifecycle, and summary reads/writes. The
same CI marker suite retains the independent-session equal/unequal admission,
lifecycle CAS/cursor ordering, and repeatable-read query races. With
`DEERFLOW_TEST_POSTGRES_URL` configured, any marked skip fails the session;
SQLite remains a fast compatibility tier rather than PostgreSQL evidence.

Alembic can structurally downgrade this feature tail to 0010 and re-upgrade it,
but the older schema cannot represent accepted evidence, external retry keys,
canonical caller intent, state versions, lifecycle events, or inbound receipts. Downgrade therefore
preserves the base `runs` rows while deliberately dropping those feature fields
and journal rows; re-upgrade reads them conservatively as legacy version-zero
rows and does not invent lost evidence. Operators must quiesce writers and back
up PostgreSQL before rollback.

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
termination qualification by themselves.

## Opt-in Kubernetes recovery evidence

`tests/kubernetes/test_durable_invocation_pod_recovery.py` drives the actual Helm
chart and pinned Gateway image in a disposable, explicitly confirmed Kubernetes
context. The runner creates one isolated `hartmesh-qualification-*` namespace,
keeps PostgreSQL and Redis outside the killed Gateway pod, and exercises accepted
commit before response, accepted before worker attachment, active execution,
terminal-before-lifecycle-commit, graceful rollout termination, and forced kill
after the graceful deadline. Each restart retries the same external key and
requires one run identity, coherent lifecycle, no duplicate terminal evidence,
no duplicate graph/model start, and preserved observe/cancel visibility.

The `kubernetes_contract` marker is skipped by default with a precise opt-in
message. Exact `DEERFLOW_TEST_KUBERNETES=1` turns every missing prerequisite,
scenario skip, timeout, unreached barrier, or evidence-write failure into a
failed qualification. Runtime barriers and the deterministic no-network model
exist only behind the test-only `DEERFLOW_TEST_KUBERNETES_RUNTIME=1` environment
injected by the qualification ConfigMap; there is no diagnostic HTTP endpoint.
Passing machine-readable evidence binds the image digest, chart version/digest,
safe configuration digest, Alembic head, exact database/cache pod, volume, and
image identities plus their versions, confirmed context, operator-reported
driver, scenario outcomes, and timestamp. Only after all scenarios pass does the harness
feed the bounded identifier, scope, artifact digest, and pass state into the administrator
deployment report. That report is still an operator assertion. A release or deployment
controller independently supplies the artifact and expected subjects to
`backend/scripts/verify_qualification_evidence.py`; only a zero exit with
`status="verified"` proves that exact artifact match. Collection, skip, or a declared JSON
reference alone never produces verified evidence. This is one-replica pod recovery evidence,
not scheduler HA, failover, active-active execution, or zero-downtime rollout.

The supported production topology has one Gateway replica and shared PostgreSQL for durable
state. When configured, shared Redis provides the bounded transient stream bridge used for
SSE reconnect; Redis is not the authoritative invocation or lifecycle store, and its delivery
semantics do not replace polling observation.
`process_local` survives neither process restart nor pod loss. An operator may declare live
recovery evidence, but exact matching of the deployed image, chart, configuration, schema,
run/namespace, scope, and required scenarios is an offline release/deployment gate. The
default skip and an unverified declaration are both unpassed release gates.

## Pinned agent construction

Acceptance resolves `ResolvedAgentMaterialV1` once. Its versioned projector covers agent
storage source/version, validated agent configuration, SOUL bytes, resolved non-secret model
execution settings and opaque secret-handle IDs, effective tool groups/tools, enabled skill
manifest/content digests, and thinking/reasoning/planning/subagent defaults. Guard tests make
new graph-factory configuration fields choose explicit inclusion or exclusion.

Skill evidence is calculated from `AcceptedSkillSnapshot`, not from mutable live paths. The
Gateway copies every non-symlink regular file in each effective skill package—including
`SKILL.md`, supporting resources, scripts, and frontmatter used by tool/secret policy—into a
content-addressed process-local tree before admission. It re-reads the source before atomic
publication and rejects unreadable, changing, symlinked, special-file, escaping, duplicate, or
over-limit material. Bounds are 64 skills, 256 files and 8 MiB per skill, 2,048 files and
32 MiB per invocation, 2 MiB per file, and 512 UTF-8 bytes per relative path. Published files
are read-only; accepted executable files retain only their read/execute bits.

The snapshot-backed `Skill` records are the only durable-run inputs to prompt/slash activation,
deferred discovery, skill-derived allowed-tool/secret policy, lead and subagent construction,
and sandbox paths under `/mnt/skills/.accepted/<snapshot-digest>/...`. A later install, edit,
delete, enable, or disable changes only a later invocation and revision. Empty accepted skill
sets also remain empty and never fall back to the live registry. The ledger stores only stable
skill identities, digests, counts, and bounded metadata—never bodies or host paths.

The accepting worker receives and uses that exact captured object; lead and subagents inherit
its revision digest and extension generation for audit. After restart, the worker resolves
current material once. Only an equal digest allows that exact newly resolved object to become
the pinned factory input. A mismatch sets terminal error state with
`stop_reason=agent_revision_drift` immediately before construction, so no graph or model work
occurs. The runtime never reconstructs historical material or performs a compare followed by
a mutable-state reread.

Snapshots use process-local reference-counted leases so equal concurrent revisions and active
background subagents may share one tree without racing cleanup. A child takes an independent
lease before dispatch. An owning lease is released after success, failure, cancellation, task
completion, or failed attachment; startup removes abandoned trees from an earlier process while
preserving active leases. A process-lost worker is terminalized by the existing orphan contract.
Historical skill bodies are not reconstructed and this mechanism is not a distributed artifact
service.

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

The following are deliberate non-goals, not latent requirements or defects:

- multi-replica Gateway coordination;
- scheduler high availability;
- an event broker or push sink;
- a general channel extension contract;
- context export and retirement;
- synchronous-client durability;
- speculative budget, deadline, resource, or effect ceilings; and
- a PodDisruptionBudget and topology spread before a real multi-replica design.

Group/dynamic outbound governance, an artifact catalogue, and a full profile registry are
likewise deferred. Exact token ceilings are not advertised; the current contract enforces only
the exact total-subagent count ceiling described above.

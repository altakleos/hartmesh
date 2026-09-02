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
| Keyed native ingress receipts | Signed GitHub acknowledges only after an atomic bounded receipt batch; exact verified-request evidence decides replay for each currently resolved binding-and-delivery receipt key while the first accepted launch envelope is retained; no-candidate events have no delivery-wide ledger; owner-scoped v2 conversation identity prevents cross-owner thread collisions; leased/fenced recovery composes with stable-key invocation replay; `thread_busy` preserves durable FIFO without consuming the poison budget. | `backend/app/channels/inbound_receipts.py`; `backend/app/channels/service.py`; `backend/app/gateway/github/dispatcher.py`; `backend/app/gateway/github/identity.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0015_inbound_receipts.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0018_inbound_receipt_failures.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0019_inbound_event_identity.py` | `test_received_payload_survives_process_loss_before_claim`; `test_equal_provider_event_reuses_first_accepted_envelope_after_policy_change`; `test_ping_returns_no_target_without_creating_delivery_wide_identity`; `test_verified_dispatch_scopes_conversation_to_each_owner_binding`; `test_thread_contention_outlives_poison_budget_and_preserves_fifo`; `test_poison_receipt_dead_letters_without_exposing_exception_text`; `test_response_loss_after_admission_replays_known_run_before_binding`; `test_signed_route_reaches_real_runtime_and_redelivery_replays` | PostgreSQL receipt concurrency/migration qualification | Implemented per verified binding for signed GitHub with PostgreSQL; legacy receipts have no invented event proof; other/local paths report best-effort |
| Canonical keyed replay | Atomic external-key arbitration compares caller intent, retains accepted effective execution, and attaches only one worker. A request cancellation cannot interrupt the store between commit and returned decision: the manager drains that decision, retains the one actionable replacement predecessor, closes only an unseen newly created run, and leaves a retained known replay untouched. Registration after a possible commit is total: contradictory evidence enters a readiness/shutdown quarantine rather than abandoning the candidate. Checkpoint/artifact release uses the same supervisor with an exact owner-fenced store operation. | `backend/app/runtime/idempotency.py`; `backend/packages/harness/deerflow/runtime/runs/manager.py`; `backend/packages/harness/deerflow/runtime/runs/store/base.py`; `backend/packages/harness/deerflow/persistence/run/sql.py` | `test_cancelled_atomic_admission_reconciles_commit_before_return`; `test_cancelled_atomic_unique_failure_does_not_retry_admission`; `test_cancelled_known_replay_does_not_close_retained_run`; `test_cancelled_reservation_releases_commit_before_return`; `test_many_finalizers_do_not_overflow_lost_replacement_compensation`; `test_exact_candidate_proof_does_not_depend_on_historical_finalizer_reads`; `test_multiple_actionable_predecessors_fail_before_store_admission`; `test_manager_release_outage_blocks_shutdown_then_recovers_exact_row`; `test_malformed_custom_release_result_preserves_body_and_stays_supervised`; `test_shutdown_waits_for_terminal_finalizer_task`; `test_shutdown_does_not_cancel_running_status_finalizer` | PostgreSQL equal/unequal arbitration, auxiliary release/takeover, and post-lock lease-expiry qualification | Implemented; PostgreSQL locking is a gated qualification |
| Split identity and sealed Origin | Effective subject, optional acting service, and trusted source evidence are non-interchangeable and caller-forgery resistant; an owned worker cannot start until thread metadata proves the same owner, and conflict, store failure, or timeout fails closed with bounded diagnostics and a terminal stream. A previously committed cancellation or replacement wins that startup race. | `backend/packages/extension-api/deerflow_extension_api/authorization.py`; `backend/app/gateway/services.py`; `backend/app/runtime/authorization.py`; `backend/packages/harness/deerflow/persistence/thread_meta` | `test_channel_human_cannot_be_promoted_by_internal_transport`; `test_rival_owner_and_metadata_failure_cannot_reach_graph`; `test_owned_thread_metadata_failure_closes_attached_stream_consumers`; `test_fenced_cancel_wins_metadata_failure_before_graph_start`; `test_replacement_closes_pending_metadata_stream_without_graph_preflight` | Source-specific launch and persistence tests | Implemented |
| Trusted contributor and hydrated evidence | One immutable trusted context carries validated persistable evidence, runtime-only values, and stable handles without secret persistence; reconstruction recomputes every derivable digest and refuses contradictory retained authority before replay/observe/cancel/recovery. | `backend/packages/extension-api/deerflow_extension_api/contributors.py`; `backend/packages/harness/deerflow/extensions/contributors.py`; `backend/packages/harness/deerflow/runtime/accepted_invocation.py`; `backend/packages/harness/deerflow/runtime/runs/manager.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/packages/harness/deerflow/subagents/executor.py`; `backend/packages/harness/deerflow/mcp/tools.py` | `test_worker_carries_one_trusted_context_without_parallel_attributes_path`; `test_hydration_recomputes_bound_effective_and_trusted_evidence`; `test_store_reconstruction_rejects_corrupt_accepted_evidence_before_recovery`; `test_external_replay_lookup_rejects_corrupt_accepted_evidence_with_stable_error` | Store/event/response redaction and reconstruction tests | Implemented; wholly absent legacy evidence uses the non-privileged compatibility path |
| Restrictive authorization and constraints | Authority failures fail closed; required async operations are validated at startup; v2 constraints bind accepted material and can only narrow a dispatch ledger that gives equal retries one physical start. | `backend/app/runtime/authorization.py`; `backend/app/runtime/constraints.py`; `backend/packages/harness/deerflow/diagnostics.py`; `backend/packages/harness/deerflow/runtime/constraints.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/packages/harness/deerflow/tools/builtins/task_tool.py` | `test_v2_host_rejects_projection_for_different_bound_material`; `test_worker_enforces_zero_ceiling_before_any_subagent_dispatch`; `test_task_dispatch_inflight_equal_replay_waits_for_one_physical_start`; `test_create_app_fails_closed_for_malformed_required_v2_constraints_provider` | Required-capability health/readiness tests | Implemented |
| Pinned agent and extension material | Accepted agent material, installed extension artifact, deployment configuration, capability manifest, and extension generation remain pinned; every effective skill package is copied and hashed from one bounded immutable snapshot shared by lead, slash/deferred discovery, policy, sandbox, and subagent consumers; recovery revalidates the extension tuple before graph/model/tool/external work, and remote v2 execution binds and revalidates the admitted isolation tuple before graph/model work. | `backend/packages/harness/deerflow/runtime/accepted_invocation.py`; `backend/packages/harness/deerflow/extensions/artifacts.py`; `backend/packages/harness/deerflow/runtime/agent_revision.py`; `backend/packages/harness/deerflow/runtime/skill_snapshot.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/packages/harness/deerflow/community/aio_sandbox/remote_backend.py`; `docker/provisioner/app.py` | `test_restart_drift_fails_before_graph_construction_or_model_work`; `test_same_process_live_edit_cannot_replace_accepted_slash_skill`; `test_live_allowed_tools_edit_cannot_widen_accepted_policy`; `test_remote_v1_material_receipt_is_compatibility_only`; `test_accepted_pod_isolation_digest_binds_every_pod_security_field`; `test_v2_execution_fence_rereads_every_supporting_resource` | Fake-Kubernetes and Helm-render evidence only; live cross-node exact-artifact qualification is separate | Implemented offline; live cross-node execution remains unqualified until the opt-in gate passes |
| Bound actual agent assembly | Every accepted durable lead run validates the finished descriptor against accepted anchors and atomically binds one immutable V1 fingerprint under the running owner/state-version fence before checkpoint or graph execution; recovery must match it. | `backend/packages/harness/deerflow/runtime/assembly_evidence.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/packages/harness/deerflow/persistence/run/sql.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0023_agent_assembly_evidence.py` | `test_accepted_durable_evidence_is_bound_before_checkpoint_access_and_astream`; `test_recovered_assembly_must_match_original_before_astream`; `test_ownership_loss_during_evidence_bind_does_not_terminalize_new_owner`; `test_lifecycle_summary_does_not_verify_evidence_from_another_accepted_run` | PostgreSQL first/repeat/stale/concurrent bind qualification | Implemented; evidence is an execution record, not code attestation |
| Durable tool-attempt receipts | Every accepted lead/subagent tool attempt reserves one fenced stable start before any inner policy/provider/tool code and appends at most one idempotent terminal outcome; store-owned contiguous attempt history suppresses completed recovery replay under any writer fence, crash gaps remain indeterminate, and raw arguments/results never enter evidence. | `backend/packages/harness/deerflow/runtime/tool_evidence.py`; `backend/packages/harness/deerflow/agents/middlewares/tool_receipt_middleware.py`; `backend/packages/harness/deerflow/runtime/events/store/`; `backend/packages/harness/deerflow/persistence/migrations/versions/0024_tool_receipt_idempotency.py` | `test_start_is_acknowledged_before_tool_side_effect_and_success_is_terminal`; `test_completed_attempt_replay_under_same_fence_does_not_reserve_again`; `test_jsonl_reopen_reuses_unfinished_attempt_reservation`; `test_start_then_terminal_are_monotonic_and_terminals_conflict`; `test_pairs_start_and_outcome_and_keeps_crash_gap_indeterminate` | PostgreSQL receipt reservation/idempotency qualification | Implemented offline; external effects are not exactly-once |
| Transactional lifecycle evidence | Every normal-run state change increments one state version and commits its safe lifecycle event atomically; maintained retained-cardinality plus bounded edge reads detects interior deletion. | `backend/packages/harness/deerflow/persistence/run/sql.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py`; `backend/packages/harness/deerflow/runtime/runs/store/base.py`; `backend/packages/harness/deerflow/runtime/runs/store/memory.py` | `test_sql_row_and_lifecycle_event_commit_together`; `test_lifecycle_readiness_rejects_deleted_interior_event_without_scanning` | PostgreSQL CAS/cursor qualification | Implemented; PostgreSQL atomicity is a gated qualification |
| Polling observation and bounded summaries | Authorized observation reads a pruning-aware bounded page and its source-aware summaries from one database snapshot. | `backend/packages/harness/deerflow/runtime/runs/lifecycle_query.py`; `backend/packages/harness/deerflow/persistence/run/sql.py`; `backend/app/runtime/api.py` | `test_context_query_excludes_other_owners_and_auxiliary_rows`; `test_malformed_ahead_and_pruned_cursors_are_typed`; `test_sql_context_page_loads_summary_rows_only_for_bounded_page_ids`; `test_postgres_query_uses_one_repeatable_read_snapshot` | Repeatable-read PostgreSQL query qualification | Implemented; PostgreSQL snapshot isolation is a gated qualification |
| Scoped service observation | An authenticated service is owner-scoped unless an operator grants a finite run/thread/owner/source search scope; the current coherent authorization provider still makes the final observe decision. | `backend/app/runtime/visibility.py`; `backend/app/runtime/invocation.py`; `backend/packages/harness/deerflow/persistence/run/sql.py` | `test_ordinary_service_cannot_observe_another_owner_or_trigger_policy`; `test_current_authorization_denial_overrides_a_valid_visibility_grant`; `test_context_pagination_stays_inside_the_finite_owner_scope` | Memory and SQL bounded-query tests; no external service | Implemented |
| Clarification continuation | A clarification ends the current invocation successfully; the answer is a distinct invocation on the same thread. | `backend/packages/harness/deerflow/agents/middlewares/clarification_middleware.py`; `backend/packages/harness/deerflow/runtime/runs/worker.py`; `backend/app/channels/manager.py` | `test_clarification_completes_then_answer_starts_new_same_thread_invocation`; `test_native_channel_revalidates_owner_dedupes_and_continues_clarification` | Native-channel characterization | Implemented; same-invocation suspension is not claimed |
| Graceful shutdown and process recovery | One deadline coordinator freezes admission, stops producers, drains runs, flushes memory, then closes dependencies; unsettled durable runs use orphan recovery. | `backend/app/gateway/shutdown.py`; `backend/packages/harness/deerflow/runtime/runs/manager.py` | `test_shutdown_orders_producers_runs_memory_and_dependencies`; `test_orphan_recovery_records_failed_with_stable_reason` | Process-loss simulations; live pod evidence is separate | Implemented for one replica |
| PostgreSQL schema and arbitration | The real `0011_mcp_tasks` predecessor (including MCP-task data) upgrades through both the invocation and MCP-result branches to one merge head; supported rollback stops at that predecessor, and re-upgrade preserves core unrelated state. | `backend/packages/harness/deerflow/persistence/migrations/versions/0011_mcp_tasks.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0011_accepted_invocation.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0012_mcp_task_results.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0019_inbound_event_identity.py`; `backend/packages/harness/deerflow/persistence/migrations/versions/0020_merge_mcp_task_results.py`; `backend/packages/harness/deerflow/persistence/run/sql.py` | `test_pre_feature_postgres_upgrade_downgrade_reupgrade_and_runtime_io`; `test_postgres_inbound_receipt_acquisition_and_claim_are_atomic` | `postgres_contract` with `DEERFLOW_TEST_POSTGRES_URL` | Qualified only when the mandatory PostgreSQL gate passes; SQLite is not PostgreSQL evidence |
| One-replica deployment truth | Production requires one Gateway, `Recreate` rollout ordering, shared durable PostgreSQL, compatible readiness/shutdown timing, and digest-pinned Gateway plus enabled provisioner execution artifacts; process-local mode makes no durability claim. | `backend/app/runtime/deployment.py`; `deploy/helm/deer-flow/values.yaml`; `deploy/helm/deer-flow/templates/gateway-deployment.yaml`; `deploy/helm/deer-flow/templates/provisioner-deployment.yaml` | `test_gateway_rollouts_preserve_single_process_ownership`; `test_production_mode_requires_pinned_runtime_images_and_one_replica`; `test_production_mode_rejects_process_local_storage` | Helm lint/render; opt-in real Deployment rollout | Implemented; replacement causes downtime and is not a high-availability claim |
| Live Kubernetes pod recovery | The opt-in harness emits one strict canonical artifact; the report exposes only an operator assertion, while the offline verifier independently matches its digest, run/namespace, image, chart, config, schema, scope, and complete scenario set. | `backend/packages/harness/deerflow/qualification_evidence.py`; `backend/tests/support/kubernetes_qualification.py`; `backend/app/runtime/deployment.py`; `backend/scripts/verify_qualification_evidence.py` | `test_exact_external_evidence_verifies_against_declared_reference_and_subjects`; `test_real_one_replica_pod_recovery_contract` | `kubernetes_contract` artifact plus a successful exact-subject offline verification | Unqualified when absent; operator-asserted when declared; externally verified only after the offline verifier succeeds |
| Legacy compatibility and native execution | Existing LangGraph/REST facades retain their responses and native lead-agent, skill, memory, subagent, sandbox, and thread behavior. | `backend/app/gateway/routers/runs.py`; `backend/app/gateway/routers/thread_runs.py`; `backend/packages/harness/deerflow/agents/lead_agent/agent.py`; `backend/packages/harness/deerflow/agents/memory`; `backend/packages/harness/deerflow/subagents/executor.py`; `backend/packages/harness/deerflow/sandbox/middleware.py` | `test_gateway_mounts_runtime_routes_without_replacing_legacy_runs`; `test_full_chain_order`; `test_make_lead_agent_custom_skill_allowlist_does_not_activate_tool_policy`; `test_after_agent_queues_memory_under_runtime_user`; `test_aexecute_propagates_one_trusted_run_context_without_free_form_attributes`; `test_sandbox_middleware_state_matches_thread_state_sandbox_field` | Full offline compatibility suite | Implemented; synchronous client durability remains deferred |

## Server-owned tenant boundary

Tenant identity is selected by the operator at service startup. It cannot be selected by an API caller and does not replace per-user authorization.

Application construction resolves one immutable `TenantIdentityV1`; admission,
stores, recovery, extensions, and deployment reporting reuse the same object.
All ingress paths inherit it after caller context is scrubbed. Newly accepted
records bind only the pseudonymous `TenantReferenceV1` into their canonical
accepted/trusted-context digests, assembly evidence, and durable tool receipts.
The tenant digest also scopes external-key admission identity and every
run/event/lifecycle read or mutation.

One `hartmesh_deployment_identity` singleton binds the configured database
schema. Empty schemas bind atomically; legacy nonempty schemas require explicit
operator migration; a different binding fails before scheduler/workers start.
This is one tenant per database schema/release, not row-level shared-schema
multi-tenancy. Recovery compares accepted and process identity before ownership
acquisition and stops with `tenant_identity_mismatch` before graph/model/tool
work on disagreement.

Extension contributor requests receive the same immutable safe reference and
cannot replace it. Provider-specific consumers accept a typed
`TenantNamespaceV1`; they never derive tenancy from user, thread, request,
release name, namespace, or a free-form prefix. Configuration, migration,
Redis ACL, and rollback details live in
[TENANT_IDENTITY.md](TENANT_IDENTITY.md).

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

The accepted object contains a minimal split identity projection, safe tenant reference, sealed Origin, bound
thread/context references, resolved agent revision, normalized input/options, immutable
extension generation, and versioned principal, base-Origin, accepted-context,
runtime-identity, and contributor-execution digests. Contributors cannot replace host-owned
principal, thread, agent, source kind, or base-source fields.

After contributor validation the Gateway also seals one `TrustedRunContextV1`: split
identity, safe tenant reference, final Origin including contributor references, thread/external-key binding,
agent/profile revisions, extension generation/manifest digest, approved persistable
references, runtime-only execution references, and stable secret handles. Its digest is part
of the committed evidence. A separate execution digest feeds accepted-context identity and
excludes contributor correlation, so audit correlation is bound without changing execution
semantics. The accepting worker binds the run ID
and installs this exact immutable object at the lead execution seam; authorization,
constraints, MCP, and subagents consume it rather than assembling parallel dictionaries.

Store hydration is an integrity fence, not deserialization by trust. It validates digest
format and revision identity, recomputes the principal, base and finalized Origin,
caller/effective request, accepted-context, and runtime-identity bindings wherever their
persisted facts make that proof possible, and reapplies fresh-seal cross-field invariants.
Two present representations that disagree fail before replay authorization, graph
construction, cancel mutation, or orphan takeover and surface only
`accepted_evidence_invalid`. A row with the complete legacy evidence family absent remains
readable through a non-privileged compatibility path; Hartmesh never guesses equality or
authority from partial contradictory evidence.

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
input. A claim has an owner, expiry, total attempt counter, poison-only failure counter, and monotonically increasing
fencing token. Only that token may bind, defer, complete, or dead-letter. Recovery reads
bounded due pages, reclaims expired claims, and admits only the earliest unfinished row
for a thread. `thread_busy` is schedulable contention, not poison: it defers the unchanged row
on a fixed non-tight cadence, preserves FIFO through restart, and never increments the failure
counter, even beyond the normal poison exhaustion horizon. Malformed or failing processing uses
bounded exponential retry and eventually dead-letters; completed-row retention is explicit and
bounded. A crash after invocation acceptance but before receipt binding replays the
same external key and binds the known-equal run; contributors, policy, graph, and model
do not run a second time. Commands and rejections complete with a bounded outcome and no
invented run ID.

Unresolved dead letters are not removed by automatic retention. Administrators have one
narrow operations surface under `/api/channels/inbound-receipts`: for each finite state,
a summary reads at most the requested cap plus one indexed receipt identity (maximum cap 1,000),
reports capped counts, and reports oldest due age only for the `received` and `deferred`
states where `next_attempt_at` has that meaning; exact-ID inspection returns only bounded state/digest/counter/timestamp
evidence and never the retained envelope, text, binding reference, or provider delivery ID.
Exact-ID requeue is a single compare-and-set requiring `dead_letter`, `run_id IS NULL`,
the expected fencing token, payload digest, and exact provider-event digest (or explicit
`null` only for a legacy row that has no event proof). A winner moves the row to `deferred`,
increments fencing,
preserves the envelope, verified provider-event evidence, and total attempt count, resets
only the poison failure budget, clears terminal/lease fields, and publishes a receipt-ID
wake-up after the transaction commits. Concurrent or stale attempts return a bounded
conflict. A permanently invalid row can instead be logically discarded through a second
exact compare-and-set with the same fences. Discard requires `run_id IS NULL`, moves the
row to `completed` with `outcome_code=operator_discarded`, increments fencing, clears lease
and retry ownership, and deliberately publishes no processing wake-up. The retained
envelope remains available only for the ordinary completed-row forensic window and is then
removed by the existing bounded retention cleanup. The POSTs use normal administrator
authentication and CSRF protection. Omitting provider-event evidence is rejected; an
explicit `null` matches only a legacy SQL `NULL`. There is no receipt enumeration, bulk
requeue, bulk discard, or bulk delete API. Automatic cleanup removes only completed rows
and repeats the state/cutoff predicates on deletion so a stale candidate read cannot delete
a row whose state changed.
Dead-letter creation, operator requeue, and operator discard emit structured stable codes with receipt and
correlation/fencing evidence. Requeue audit records contain a domain-separated pseudonymous
reference derived from the authenticated administrator, never the raw user identifier.
Unexpected operator-store failures return one bounded correlated `503`; exception text,
message content, and retained envelopes are not logged or returned.

Persisted envelopes contain only the finite normalized text/type, owner/agent/thread
route, verified binding, safe provider correlation, and supported stable attachment
handles. The current signed GitHub path rejects transient attachments rather than
persisting their bytes. Webhook secrets, access tokens, raw provider payloads, resolved
credentials, and arbitrary metadata never enter the receipt. The administrator
deployment report exposes `native_ingress.v1` per enabled source as `durable` or
`best_effort`; portable runtime capabilities do not. `durable` is the conjunction of
current HMAC-authenticated webhook mode and PostgreSQL receipt storage. The explicit
unverified webhook opt-in is available only in `local_development` and remains
`best_effort` even when PostgreSQL is configured. A durable process whose secret is
removed becomes not-ready and rejects requests rather than falling through to the
development bus path. SQLite/memory and explicitly memory-backed receipt configuration
remain local convenience and cannot satisfy the durable production profile.
Verified-ingress eligibility is startup-frozen. A secret added after Gateway composition
does not mount the absent route or upgrade its deployment report until restart; rotating
one configured nonblank secret to another remains request-time behavior.

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

The safe agent revision metadata does embed the versioned effective subagent
catalog and its per-agent skill-content scope map. Their full prompts and
execution settings are authoritative input material, not public lifecycle data.
New revisions always write both fields, including canonical empty values. A
pre-feature terminal row without them remains readable; a pre-feature
nonterminal row cannot execute through live resolution and fails closed. Because
the fields are additive, rollback to code that cannot validate them is safe only
after rows written by the newer runtime have been drained or terminalized.

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

`deerflow-extension-api` 0.13.0 defines one optional, singular constraints provider with
separate v1 and v2 contracts. Gateway invokes it only for a genuinely absent invocation,
after invocation-start authorization allows and before atomic acceptance. V2 receives only
the sealed split identity and final Origin, bounded namespaced correlation lookup references,
thread/external-key binding, pinned agent/profile revisions, request/trusted-context/manifest
digests, extension generation, paired extension artifact/configuration digests, and the
host's enforceable subagent ceiling. It receives no
content, credential, arbitrary kwargs, or opaque host object. Authorization remains the sole
binary permission authority, and dynamic effects remain subject to operation-time
authorization and MCP preparation.

The Capability Host runs the provider directly—not through observational fail-open
middleware—with its own two-second timeout and timezone-aware clock. Startup validates the
declared authoritative operation itself: required authorization `aauthorize`, contributor
`contribute`, constraints `project`, and MCP `prepare_call` methods must be async functions;
an ordinary synchronous method is malformed even if its return value happens to be awaitable.
Optional malformed contributions retain only their documented fail-open behavior, and health
probe sync/async compatibility remains a separate contract. V2 binds the exact
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
dispatch ledger into the lead runtime and shares it with delegated subagents. The task tool
hashes the prompt, resolved subagent/options/tools, accepted revision/generation/constraint,
and skill snapshot immediately before dispatch. A new ID/intent is `NEW` and consumes one
physical-start slot; an equal in-flight or completed retry is `REPLAY` and awaits/reuses the
same defensively copied result. Changed intent under one ID is `CONFLICT`; a new ID after the
ceiling is `EXHAUSTED`. Zero therefore starts nothing, and cancellation closes the ledger and
wakes equal waiters without a second executor. The existing token-budget and delegation-ledger middleware remain useful
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
The two named projections have separate jobs. **Canonical caller intent** records only the
execution semantics supplied through the caller-facing contract and decides equal replay versus
conflict. **Accepted effective execution** records server-resolved defaults, resolved
agent/profile material, the accepted constraints projection, extension generation, and pinned
principal, Origin, contributor, thread, and execution facts used by authorization and the
worker. A partial unique index over scope/key arbitrates concurrent processes. The store
returns only `created`, `known_same`, or `key_conflict`, and `InvocationRuntime` attaches a
worker only for `created`.

The transaction may commit before the SQL coroutine has materialized and returned its result.
`RunManager` therefore shields and drains the complete atomic decision when the request is
cancelled in that window. It first synchronizes the committed predecessor and replacement rows
into local state, then terminalizes an unseen newly created run (or exactly releases an unseen
auxiliary thread-operation reservation) before propagating cancellation. Auxiliary release is
fenced by run, thread, owner, operation kind, worker, and—when enabled—an unexpired lease. If
release cannot be proven, the immutable obligation remains supervised through the same readiness
and shutdown fence until the row is released, absent, or already inactive; it never emits a
normal-run lifecycle event. Cancelling a `known_same` or
`key_conflict` lookup never cancels or rewrites the retained invocation. Cancellation state is
retained even when the atomic decision raises: in particular, a retryable uniqueness race is
not allowed to start a second admission attempt after the caller has gone away.

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
and lifecycle. An equal retry does not rerun contributors, authorization, constraints, default
resolution, agent/profile routing, or model execution. Here authorization means start/admission
authorization; current observe authorization still applies before a retained row is revealed.
An explicitly caller-requested revision remains part of caller intent; host-resolved revision
and profile material remain accepted effective execution. Replay never starts with accepted
effective values and overwrites only fields present on the retry. Rows written before
caller-intent evidence exists remain readable, but a keyed replay conflicts because equality
cannot be proven. A different key still follows the independent active-thread rule (`reject`
is thread-busy; with durable run events, `interrupt`/`rollback` also fail closed until the
predecessor is explicitly cancelled and its terminal delivery receipt is observable).
Receiptless compatibility stores retain atomic supersession. The replay guarantee ends when
the retained row is deleted.

## Durability boundaries

Hartmesh separates the boundaries instead of treating “durable invocation” as an end-to-end
exactly-once claim:

- **Ingress receipt boundary** — an HMAC-verified GitHub delivery using PostgreSQL receipt storage is
  acknowledged only after the bounded verified-source envelope is committed. Unsupported native
  sources, unverified development webhooks, and local memory/SQLite receipt modes are
  `best_effort`; provider delivery and `MessageBus` notification before a durable receipt remain
  outside the guarantee.
- **Admission boundary** — invocation durability begins when the normal `RunRow`, acceptance
  evidence, external-key arbitration, and `accepted` lifecycle row commit. Before that commit
  there is no retained invocation. After it, an equal key/intent converges on that row while it is
  retained, including after response loss. Each attempt proposes one candidate run UUID; finding
  that exact UUID after a lost store response proves that this attempt owns worker attachment,
  while the same external key bound to another UUID is a peer outcome—either an equal replay or a
  changed-intent conflict—and never creator ownership. Exact candidate proof never depends on
  rereading historical finalizers; their prior replacements already fenced them, so unrelated
  cleanup-store availability cannot revoke creator ownership. The application keeps one admission
  supervisor and readiness permit until the creator has exactly one attached worker or one
  intended authoritative terminal state: cancellation or `worker_attachment_failed`. It never
  abandons a still-resolving admission task on an elapsed request timeout. If both the commit response and the
  exact candidate read are temporarily unavailable, a process-local post-commit supervisor keeps
  readiness closed and retains only bounded candidate, intended terminal disposition, and the one
  local predecessor that has not already been abort-fenced. Already-finalizing predecessors remain
  worker/shutdown fences but are not copied into every later ambiguity record. More than one
  actionable predecessor is an integrity failure rejected before the store mutation. Registrations
  merge monotonically; a contradiction after a possible commit enters read-only integrity
  quarantine instead of throwing away the retained candidate. Quarantine consults only the
  store's explicit privileged primary-key lookup: an active row remains quarantined regardless
  of owner visibility, global absence evicts a fenced local phantom, and conflicting terminal
  truth never copies cross-owner fields. Candidate UUID reuse is a bounded integrity failure that
  loses before predecessor, index, or lifecycle mutation; it is never reported as thread
  contention. The supervisor uses independent
  capped backoff per obligation rather than a tight poll or allowing new work to reset a poisoned
  item. Once the exact candidate proves a replacement committed, compensation fences the named
  actionable predecessor and terminalizes the unattached candidate; it never starts model work. A known-created
  row whose cancellation or attachment-failure write becomes uncertain stays non-attachable and
  supervised, and local state does not advance beyond durable truth. Readiness and shutdown remain
  fenced until exact-row compensation proves absence or one durable intended terminal state.
- **Auxiliary reservation boundary** — checkpoint and artifact mutations use short-lived
  non-invocation rows under the same active-thread uniqueness rule. Successful, failing, and
  cancelled bodies all preserve their caller-visible result while exact release is attempted.
  PostgreSQL checks lease freshness against statement wall time after its row lock, and malformed
  custom-store release results remain supervised rather than escaping cleanup or masking the body.
  At body exit the immutable release obligation is retained and the exact caller task is detached
  synchronously before cleanup can await. A detached auxiliary row is not selected for another
  lease renewal; a renewal already in flight may finish durably, but cannot restore local
  authority or create further renewals. The post-commit supervisor alone owns any remaining
  readiness and shutdown fence.
  Release uncertainty is a post-commit obligation, not best-effort cleanup: same-process recovery
  retries it, lease takeover may terminalize the row without invocation lifecycle evidence, and
  later thread work remains fail closed until ownership is durably settled.
  A per-UUID registration token fences contradictory admission/release evidence across store
  awaits. A collision observed before mutation dispatch prevents the write. A store mutation
  already dispatched linearizes first, but its response cannot clear local supervision after a
  later collision; authoritative primary-key truth must settle both retained obligations. An
  active contradictory row remains quarantined even when an intermediate cancellation request
  committed first; that event does not authorize another terminal mutation. Readiness and
  shutdown remain closed until external/orphan recovery establishes terminal truth.
- **Execution boundary** — only the admission creator attaches a worker, and accepted agent,
  constraint, trusted-context, and extension material is pinned for that worker. Hartmesh does
  not promise exactly-once model execution, resumable model execution after process loss, or
  rollback of already-committed external tool effects. A process may die before execution,
  during a model/tool call, or after an external side effect.
  Before checkpoint access or graph invocation, an accepted durable lead run also binds the
  actual assembly's V1 fingerprint under its owner/state-version fence. A bare third-party
  graph remains compatible outside this boundary but is insufficient here.
- **Observation boundary** — Cursor polling of transactional lifecycle rows is the
  authoritative v1 evidence path. The run row and lifecycle journal, read through
  `DurableInvocationPort.observe`, establish current state; a push sink is optional
  at-least-once acceleration and never a synchronous completion dependency.
- **Outbound delivery boundary** — SSE/stream retention, native-channel replies, provider
  acknowledgements after receipt admission, and other outbound delivery are separate from
  invocation/lifecycle durability. Delivery to an external provider and external side effects
  are not made exactly once by invocation idempotency.

The supported topology has one Gateway replica. Multi-replica execution ownership, scheduler
high availability, unsupported source receipts, broker delivery, and provider-side outbound
delivery remain outside this contract.

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
process-local invocation state, any `run_events.backend` other than `db`, or an
unbounded PostgreSQL command timeout. Database-backed run events are required
so accepted tool attempts can honor fenced reservation and storage-level
idempotency; `local_development` remains an explicit convenience profile where
memory/JSONL events and disabling that timeout are permitted without a durable
claim.
The report also carries the latest safe admission-readiness status, reason codes,
and correlation identifier. Its optional `post_commit_obligations` field is a
versioned, process-local operational snapshot: it reports saturated pending counts
for admission and auxiliary-release obligations, the overlapping count of quarantined
identities, and compensator-proven resolved-since-process-start counts by obligation
type. Quarantine is
not a third workload bucket and must not be added to the two pending type counts.
These counters reset on restart and are neither durable lifecycle evidence nor a
cross-replica total. Backlog transition logs contain only stable codes, type, and
count; bounded integrity diagnostics may retain an opaque run ID but never owner,
thread, prompt, envelope, or retry content. The serialized v1 readiness reason
`admission_compensation_pending` remains
for compatibility and now means that any post-commit ownership obligation is pending.
Provider/database/Adapter messages and tracebacks are neither
public nor retained in ordinary diagnostics or logs. Every portable HTTP operation uses one
failure wrapper: an unexpected exception becomes the strict versioned `runtime.error`
envelope with HTTP 503 `indeterminate`, while the matching internal diagnostic contains only
the stable code, exception class, bounded operation/capability or contribution identifier,
and correlation ID.
Portable operation failures retain the distinct `runtime.failure` kind; `runtime.error` is
only the Gateway HTTP transport envelope used when no normal portable result can be returned.
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

The summary's `assembly_evidence` is a further bounded projection containing
only V1, the effective model, and full overall/prompt/toolset/middleware/skillset/
policy digests. The stored V1 object is strictly parsed and its canonical digest
recomputed before `assembly_evidence_status="verified"` is returned. An active
row with no bound record is `pending`; a terminal pre-feature row or any partial,
malformed, or digest-mismatched pair is null with `legacy_unavailable`. Stored
content is never reflected on failure. This says which assembly HartMesh admitted
to execute; it is not a cryptographic attestation of source code or behavior.

The summary's independently validated `subagent_catalog` projection contains
only V1, its full digest, entry count, and exact sorted allowed names, paired with
`verified` or `legacy_unavailable`. It never exposes descriptions, prompts,
models/settings, tools, skills, limits, source records, or policy material.

One-invocation observation has an independent, opt-in durable tool-receipt page.
Set `include_tool_receipts=true`; `tool_receipt_limit` defaults to 100 and is
bounded to 1–100, and `tool_receipt_cursor` pages only that exact authorized run.
The page pairs `tool_receipt.started.v1` and `tool_receipt.outcome.v1` records and
returns stable receipt/task identity, lead/subagent attribution, attempt and
status, store timestamps, full projection/accepted-anchor digests, and bounded
authorization/guardrail references. A start without a matching terminal record
is `indeterminate`. Pre-feature runs report `evidence_status="legacy_unavailable"`;
their event tails are never projected. Malformed evidence is contained, omitted, and counted with
`evidence_status="invalid"` rather than reflected. The page also returns its own
`next_cursor`, nullable `pruned_before`, and `invalid_event_count`. Context-wide
observation cannot request receipts, so callers must first select and authorize
one exact run. Receipt cursors are checksummed and bound to both run and thread.

The receipt request digest commits to a bounded safe projection of argument
names/types plus only server-declared evidence-safe scalar fields. Credential-like
fields are classified, and unclassified strings contribute shape/length rather
than their raw value or raw-value hash. The result digest commits to the exact
sanitized and budgeted model-visible result plus type/status. These SHA-256
digests are equality evidence, not a confidentiality mechanism; low-entropy
values may still be guessable when a projection is independently available.
Each start and terminal carries the same validated dispatch-generation digest,
derived from the store-owned run/task/tool-call/attempt identity. Public
checkpoint namespace/ID, task ID, and node-attempt fields form only a local
dispatch observation because the framework counter may reset on reconstruction.
The store reconciles that observation with contiguous durable history: recovery
reuses the latest start and terminal regardless of writer-fence changes, then a
binding-local offset maps the next live retry to the immediate successor.
A durable receipt records HartMesh's observation of a tool attempt. It does not
guarantee an external side effect occurred exactly once or that the tool result
was correct.

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

Polling `DurableInvocationPort.observe` is the supported durable evidence path. Cursor polling
of transactional lifecycle rows is the authoritative v1 evidence path.
No event sink or broker is required for correctness: observation reads the authoritative run
state and its transactional lifecycle journal directly. A push sink or event broker is optional
at-least-once acceleration only; it is never a synchronous model-completion dependency and
cannot become an undeclared correctness or release criterion.

### Clarification continues the thread, not the invocation

A clarification request completes its current invocation successfully with the structured
request for more information as its result.
The user's answer starts a new invocation on the same DeerFlow thread, so ordinary idempotency,
acceptance, and lifecycle rules apply to that answer. That new invocation reuses the same
DeerFlow thread, checkpoints, memory, workspace, and conversation context; it does not resume
the completed graph execution.
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

Assembly binding is not another lifecycle transition and does not increment the
state version. After `started`, `bind_assembly_evidence(run_id, owner_id,
lease_epoch, ...)` atomically fills the both-null pair or compares the already
bound valid record while checking that exact running fence. `bound` and
`already_matching` proceed; `mismatch` ends the attempt as
`agent_assembly_drift`; missing evidence ends it as
`assembly_evidence_unavailable`; and ownership loss leaves terminalization to the
current owner. The pair is retained through success, failure, cancellation, and
timeout.

An active invocation lost with its process is terminalized as failed with
`stop_reason=orphan_recovered`; it does not resume model execution. A product retry is a new
invocation under the new process generation, while a replay of the lost invocation's retained
external key only returns that terminal row.
Attachment failure is `worker_attachment_failed`. A pre-graph ownership-metadata failure
publishes a bounded startup error and terminates both attached and late stream consumers.
If a cancellation was committed first, cancellation evidence and status take precedence and
the stream terminates without entering graph preflight. These guarantees
make retained keyed retries converge, but do not provide scheduler HA or a multi-replica
Gateway ownership design beyond the explicitly configured PostgreSQL lease and stream
primitives. Live pod termination is qualified only by the separate opt-in
`kubernetes_contract` suite; its default skip remains an unpassed release gate.

Repository process-loss simulation is not Kubernetes pod-recovery qualification. The
process-loss simulation, image construction, and Helm rendering are separate evidence; none
becomes live pod evidence by implication or aggregation.

The PostgreSQL migration qualification starts both from an empty schema and from
the real durable-invocation predecessor `0011_mcp_tasks` with representative
normal, auxiliary, and MCP-task rows. It applies every invocation revision through
`0019_inbound_event_identity`, joins the result, managed-subagent, and scheduled-enqueue
branches through `0022_merge_scheduled_enqueue`, and verifies the single
`0026_mcp_task_lineage` head plus accepted/idempotency/caller-intent/assembly/tenant
and MCP-task lineage columns, the schema-identity singleton, the nullable run-event
receipt idempotency key/partial unique index, and tenant-scoped MCP-task lineage,
parent, notification, and due-work indexes and checks, validates lifecycle
singleton/journal/index/retained-cardinality
and inbound receipt arbitration, and then uses `RunRepository` for
replay, cancellation, orphan recovery, lifecycle, and summary reads/writes. The
same CI marker suite retains the independent-session equal/unequal admission,
assembly bind, lifecycle CAS/cursor ordering, and repeatable-read query races. With
`DEERFLOW_TEST_POSTGRES_URL` configured, any marked skip fails the session;
SQLite remains a fast compatibility tier rather than PostgreSQL evidence.

The supported feature-tail downgrade stops at `0011_mcp_tasks` and re-upgrade proves the
representative core MCP task survives; bounded result columns from the sibling branch are
also outside that predecessor. It cannot represent accepted evidence,
external retry keys, canonical caller intent, state versions, lifecycle events, inbound
receipts, execution evidence, or assembly evidence, so those feature facts are deliberately lost and are not
invented on re-upgrade. A further technical downgrade to `0010_run_cancel_request` executes
the unrelated MCP-task downgrade and destroys MCP-task state; it is not the supported
invocation rollback. Operators must quiesce writers and back up PostgreSQL before rollback.

## Graceful Gateway shutdown

`app.gateway.shutdown.GracefulShutdownCoordinator` is the sole production owner
of shutdown ordering and deadline accounting. It atomically closes the admission
permit seam, stops channel and scheduler producers, requests interruption and
bounded drain of locally active runs, flushes memory only after channel, scheduler,
admission, and run writers are quiescent, then closes retrieval, memory, browser, OIDC, stream, and
database resources. Concurrent or repeated shutdown calls share one result and do
not emit duplicate terminal transitions.

`deployment.shutdown` supplies the admission, channel, scheduler, run, and final
dependency sub-budgets. The memory phase uses
`memory.shutdown_flush_timeout_seconds`; their sum is the absolute application
deadline. A timed-out subsystem records only a stable code, error class, phase,
and correlation ID while later phases continue within the remaining deadline. If
any writer quiescence cannot be proven, the coordinator deliberately skips
memory flush/close and runtime dependency close rather than racing a late writer.
Runtime callbacks are detached from the surrounding context manager before shutdown,
so an unsafe explicit-close skip is not undone by implicit stack cleanup. Active locally owned runs are
requested to interrupt; any run still unsettled is left to the existing durable
orphan recovery on restart. Durable PostgreSQL deployments require a finite driver
command timeout so an unresolved admission cannot outlive every shutdown budget.
Normal acceptance holds the admission permit through
the supervised worker attachment or terminal compensation, so graceful shutdown
cannot enter between those operations. Request cancellation before attachment
terminalizes the creator; shutdown also terminalizes any locally supervised taskless row before
reporting run quiescence. An unresolved candidate compensation keeps readiness and shutdown
non-quiescent until storage proves absence or terminal state. Actual process loss after committed acceptance remains an
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
The graceful scenario issues a real Deployment rollout against the rendered
`Recreate` strategy and fails if old and replacement Gateway Pod UIDs overlap;
it is not simulated by deleting a Pod directly.

The `kubernetes_contract` marker is skipped by default with a precise opt-in
message. Exact `DEERFLOW_TEST_KUBERNETES=1` turns every missing prerequisite,
scenario skip, timeout, unreached barrier, or evidence-write failure into a
failed qualification. Runtime barriers and the deterministic no-network model
exist only behind the test-only `DEERFLOW_TEST_KUBERNETES_RUNTIME=1` environment
injected by the qualification ConfigMap; there is no diagnostic HTTP endpoint.
These qualification hooks are opt-in deterministic fault injection, are disabled in ordinary
processes, and are not part of the runtime API.
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

Admission also resolves every reachable effective subagent once through
`runtime/subagent_snapshot.py`. Registry precedence remains built-in, then
`config.yaml` custom, then enabled managed, with per-name overrides applied to
the winner. The canonical V1 definition captures source version, description,
effective prompt/model settings, tools, skills, limits, and construction policy;
the catalog binds exact allowed names and is capped at 64 entries and 256 KiB.
Named missing or invalid definitions fail admission instead of being omitted.
Managed subagent changes apply to invocations accepted after the edit; an
in-flight or recovered invocation uses its accepted snapshot.

Admission snapshots the transitive union of lead and allowed-subagent skill
packages once, while a content-digest scope map limits prompt/discovery/tool
loading to `lead` or the relevant `subagent:<name>`. A shared accepted sandbox
tree may contain that full union; scope is not a filesystem-confidentiality
claim. Recovery validates catalog/scope digests and required accepted package
material but never compares the catalog with current managed state.

After the worker restores that accepted material, it installs an opaque
server-owned descriptor-required sentinel, starts under the execution fence, and
assembles under the accepted extension generation. `AssemblyEvidenceV1`
fingerprints the actual effective model, prompt hash, authorized tool/deferred-tool
set, ordered middleware chain, enabled skill names plus immutable skill catalog,
and complete effective policy projection. Admission-comparable policy anchors are
the bootstrap, non-interactive, plan, recursion, and full subagent release-policy
fields, including the catalog digest, exact allowed names, and frozen limits.
Subagent descriptors likewise record their accepted definition digest and parent
catalog digest. Model, skill, or policy contradictions fail before binding; any later
fingerprint change fails comparison without replacing the original. Project 02's
managed-subagent material feeds this canonical subagent/skill projection without
changing the V1 evidence envelope.

Skill evidence is calculated from `AcceptedSkillSnapshot`, not from mutable live paths. The
Gateway copies every non-symlink regular file in each effective skill package—including
`SKILL.md`, supporting resources, scripts, and frontmatter used by tool/secret policy—into a
content-addressed process-local tree before admission. It re-reads the source before atomic
publication and rejects unreadable, changing, symlinked, special-file, escaping, duplicate, or
over-limit material. Bounds are 64 skills, 256 files and 8 MiB per skill, 2,048 files and
32 MiB per invocation, 2 MiB per file, and 512 UTF-8 bytes per relative path. Published host
files are write-protected; accepted executable files retain only their read/execute bits. That
host mode is evidence integrity, not an OS boundary against a same-identity sandbox process.

The snapshot-backed `Skill` records are the only durable-run inputs to prompt/slash activation,
deferred discovery, skill-derived allowed-tool/secret policy, lead and subagent construction,
and sandbox paths under `/mnt/skills/.accepted/<snapshot-digest>/...`. Durable sandbox
acquisition omits every mutable live skill mount or upload; legacy non-durable execution
retains those live views. File, list, search, and shell paths are also restricted to the
exact accepted subtree before sandbox I/O. Before the first model
call, the sandbox provider binds exactly that accepted digest to a per-user, per-thread active
view; sibling snapshots are not mounted. An accepted empty skill set binds an empty view and
never falls back to the live registry. Local Docker-backed AIO mounts nonempty accepted material
through an OS-enforced read-only boundary. Kubernetes/provisioner AIO advertises the same
`immutable_read_only` capability only after an `rwx_verified_copy_v2` Pod returns a complete
receipt and becomes reachable through its per-attempt capability
gate. Its init container traverses and copies the exact content-addressed RWX snapshot into a
private per-Pod `emptyDir`, recomputes the canonical digest twice, and the sandbox mounts only
that completed private copy read-only. The source PVC is never mounted into the sandbox and no
live skill projection is present. A source mutation after verification therefore cannot replace
the execution bytes. A v1 receipt remains parseable for backward readability but is always
`empty_only` and cannot authorize nonempty material. The v2 receipt binds the accepted
run/snapshot/generation, Pod UID and canonical admitted isolation digest, Lease UID/attempt,
exact NetworkPolicy UID/spec digest, immutable evidence/capability Secret UIDs and digests,
pinned sandbox/verifier images plus observed image-ID digest, verifier receipt, and final
materialization digest. The profile requires actual `ReadWriteMany` storage, pinned sandbox and
verifier/gate images, and cross-node Pod networking; it deliberately uses no same-node affinity.
A writable configured host mount that overlaps accepted material is rejected. Local, E2B,
unqualified remote AIO, and custom providers remain explicitly empty-only until they prove an
equivalent boundary; a nonempty snapshot fails before graph/model execution.

For remote AIO, materialization completes before the authoritative pending-to-running
transition and before graph construction. One native Kubernetes Lease owns the Pod, immutable
evidence/capability Secrets, and NetworkPolicy. An AlreadyExists response is reusable only when
owner identity and canonical spec match exactly. Response-loss replay, active/warm reuse, and
Lease renewal re-read the complete v2 tuple; the owning process renews only after the
authoritative RunRow worker lease has renewed successfully. The worker revalidates the same
tuple before graph construction and again after all other pre-stream awaits, immediately before `astream`; any deletion, replacement, unsupported version, or drift fails closed. Expired Leases are reclaimed in bounded
pages and Kubernetes garbage collection removes their children. A response-loss retry reuses
only the same attempt identity and in-memory capability. The full bounded v2 receipt is
written to the authoritative `RunRow` in the same transaction that changes it to `running`; the
`started` lifecycle payload contains only its digest. A process restart cannot recover the
ephemeral capability and therefore follows the documented orphan-terminalization behavior.

Provisioner management is separate from the per-attempt data-plane capability. The Helm profile
projects a rotating ServiceAccount token into the Gateway, scopes it to the provisioner audience,
and has the provisioner verify its exact namespace and ServiceAccount through TokenReview. Gateway
readiness authenticates to `/api/capabilities` and requires `rwx_verified_copy_v2`; a profile,
token, PVC, image, or networking failure blocks new admission while liveness remains independent.
Repository fake-Kubernetes and Helm-render tests cover every bound field and drift fence, but do
not qualify live cross-node CNI/RWX execution. That claim remains absent until an artifact-bound
opt-in Kubernetes qualification passes against the deployed image/chart/config/schema subjects.
The live nonempty-skill gate is the separately versioned
`deerflow.kubernetes-accepted-skill-qualification/v2` artifact with scope
`durable_one_replica_rwx_verified_copy_v2_nonempty_skill`. The existing marked
runner selects it only through `DEERFLOW_TEST_KUBERNETES_SCOPE` and then requires
exact Gateway, provisioner/verifier, and sandbox digests, an explicit RWX storage
class, two Ready schedulable nodes, distinct admitted Gateway/sandbox nodes,
deterministic nonempty bytes and allowed-tool metadata, a materialized verifier
receipt bound to RunRow execution evidence, an observed Lease renewal, and
Gateway replacement plus cleanup after exact Lease owner loss. Lease owner loss
does not rehydrate the same run's sandbox: the per-attempt capability is
intentionally non-recoverable, so that path fails closed and proves cleanup only.
The renewal proof requires the same Lease UID, accepted-attempt holder, and qualified
duration plus a strictly advancing bounded RFC3339 `renewTime`; a `resourceVersion`
change alone is not renewal evidence. The verifier and materialization gate are shipped
by the provisioner artifact, so v2 evidence and expectations require their exact pinned
image reference and digest to equal the provisioner subject while still reporting both roles.
The v1 pod recovery artifact remains readable and is not upgraded into this stronger claim.
Both scopes remain skipped and unpassed by default; neither offline/fake coverage
nor a declared reference qualifies live cross-node execution.

Admission reserves one process-local projection owner inside the thread admission lock. Atomic
creation promotes that owner to the committed run; equal known-key replay bypasses the busy
check and unequal/new work remains thread-busy. The coordinator issues exact
run/generation/consumer tokens. Lead and background subagent consumers hold independent tokens,
and only the last exact release may begin provider cleanup. Ownership remains busy in a
two-phase clearing state until the provider proves the old bytes cleared, never published, or
quarantined and an exact CAS finalizes removal. Failed clears retry with the same proof; an
unproven custom-provider failure remains busy and cannot recycle its sandbox. Stale or
wrong-run cleanup cannot remove a later invocation's view. Once material clear succeeds, later resource-parking
failure does not strand ownership because it cannot make the removed bytes reachable again.
Local LRU and Docker/AIO idle cleanup skip invocation-owned sandboxes; explicit process shutdown
may tear them down under the existing orphan-terminalization contract. A later install, edit,
delete, enable, or disable changes only a later invocation and revision. The ledger stores only
stable skill identities, digests, counts, and bounded metadata—never bodies or host paths.

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
cursor ordering, pruning bounds, retained cardinality, or event edges and an unsatisfied durable deployment profile. Lifecycle inserts and pruning update an independent retained count in the same transaction; readiness compares it with the cursor range and first/last retained rows using bounded reads, so an interior deletion cannot remain healthy. The administrator-only
deployment report exposes the safe manifest, separately labelled health snapshots,
persistence truth, optional artifact provenance, and qualification state; the portable
runtime capabilities record deliberately does not.

One immutable extension generation means one startup-frozen process generation in the
supported one-replica profile. A restart constructs the next generation only in the new
process; Hartmesh does not coordinate simultaneous generations or rolling replicas. An
already accepted invocation stays bound to its process generation until it finishes or is
terminalized by process-loss recovery.

The same coordinator fences every genuinely absent HTTP, scheduled-task, native-channel, and
embedded-service invocation before normalization and durable acceptance. It runs required
health probes concurrently with bounded lifecycle integrity under the configured overall
timeout. The authoritative `authorize`, `contribute`, or `project` call then remains the final
proof and fails closed independently. Known keyed replay is resolved before this fence and
reuses its accepted evidence without rerunning health or authority code. Defaults and bounds
live under startup-only `deployment.readiness`: 10-second cache/admission health, 30-second
diagnostic staleness, two-second per-probe timeout, five-second overall timeout, and a required
failure threshold fixed to one.

Required authoritative operations are validated at host construction: authorization,
contributor, constraint, and MCP preparation methods declared async by their public contract
must be async functions. A synchronous implementation fails required startup with one bounded
attributed diagnostic; optional contributions preserve only their documented fail-open path.
Provider exceptions record only stable code, class, bounded operation/contribution identity,
and correlation ID—never the message, traceback, configuration, request, or secret.

Required MCP interceptors run only after the coherent authorization provider allows. At the
final network fence, the host verifies the pinned generation and fresh required health, then
composes bounded preparation in contribution-ID order. Failure or conflicting transient
headers call the handler zero times. Preparation cannot grant permission, and transient
credentials never enter run rows, checkpoints, lifecycle/rich events, manifests, or logs.
The API-writable legacy interceptor path remains optional warning-and-skip compatibility and
cannot satisfy an operator requirement.

## Compatibility and deferred scope

Existing LangGraph SDK and DeerFlow REST create/stream/wait response contracts remain
compatible facades over the durable Gateway path. The application-hosted in-process
`InvocationRuntime` Adapter is the durable embedded surface and implements
`DurableInvocationPort` over the same Gateway-owned admission, lifecycle, and policy machinery.
The local, non-durable embedded `DeerFlowClient` is a direct synchronous graph client and does
not enter `InvocationRuntime`; applications that need durable embedded parity use the supported
asynchronous `deerflow-runtime-api` adapter instead.

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

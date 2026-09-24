# Runtime invariants

The rules the backend runtime holds itself to, in full. The one-line summary of
each lives in [`backend/AGENTS.md`](../AGENTS.md) under **Runtime**, which is
the guidance every backend chain inherits; the mechanism is here so that chain
stays readable.

Paths in the prose below are relative to `backend/`, as in the guide they came
from, so `docs/X.md` is this directory and `../docs/X.md` is the repository
root's. Markdown links resolve from this file.

- Tenant and credential evidence are server-owned at durable boundaries; see
  [tenant](TENANT_IDENTITY.md) and
  [actor evidence](../../docs/AUDITABLE_AUTOMATION_IDENTITIES.md). Honcho receives
  tenant context, never durable authority (see the memory guide).
- Every durable launch (HTTP, Scheduled Tasks, signed native channels, embedded
  services) enters the application-owned `InvocationRuntime`, whose admission
  seals identity, Origin, trusted context, constraints, agent revision,
  extension generation, and effective skill material before worker/model work;
  keyed replay, lifecycle observation, and fenced cancellation use that accepted
  record. Durable runs then verify material, fence start, and bind
  `AssemblyEvidenceV1` before checkpoint/graph/model/tool work (missing evidence
  fails `assembly_evidence_unavailable`, drift fails `agent_assembly_drift`,
  stale owners cannot finalize). Fail closed rather than falling back to a less
  durable path; see the runtime guide and `docs/INVOCATION_RUNTIME.md`.
  Durability is the deployment profile's promise, not a run record's:
  `local_development` runs a nonempty accepted skill snapshot through the
  accepted-skills projection; durable profiles refuse without a qualified
  materializer (`docs/ACCEPTED_SANDBOX_EXECUTION.md`).
- Every sandbox is a session of a declared Kind, ordinary or accepted,
  dispatched by the session provider from `sandbox/session.py`; see
  `docs/ACCEPTED_SANDBOX_EXECUTION.md`.
  `sandbox/accepted_material.py` owns accepted material's V1/V2 request,
  lease/evidence, capability profiles, and `AcceptedSandboxSession`, which
  composes the existing run or batch-item fence, exposes no raw provider handle,
  and blocks calls/publication after observed loss. Provider extras are
  `sandbox/capabilities.py` contracts negotiated through
  `SandboxProvider.capability`; the required surface is acquire/get/release. AIO
  keeps `rwx_verified_copy_v2` but declares atomic fencing, process-loss lookup,
  and exact-two false. OpenSandbox ordinary execution stays separate; its
  accepted nonempty material fails closed (no ownership CAS or resolved-image
  digest readback; candidate surfaces remain live-unqualified; Phase 0 no-go
  evidence in `docs/OPENSANDBOX_ACCEPTED_MATERIAL_FEASIBILITY.md`). No
  production profile until every live qualification scenario passes.
- After assembly bind, a trusted fenced sink commits `started` before policy or
  tool code and one terminal afterward; gaps are `indeterminate`. Receipts bind
  accepted anchors and graph dispatches, omit raw arguments/results/errors, and
  record HartMesh's observation of a tool attempt, never an exactly-once
  external effect or a correct result. Production requires
  `run_events.backend: db`.
- Batch acceptance is parent/tenant-bound and database-time fenced; production
  stays disabled (`../docs/DURABLE_SUBAGENT_BATCHES.md`). Terminal evidence export
  uses a runtime snapshot fence and Gateway exact-byte archive; required
  missing, pruned, or legacy material fails closed and bundles are unsigned
  (`../docs/RUN_EVIDENCE_BUNDLES.md`). Admission seals `ExecutionBudgetV1` and
  the run-bound `EgressAllowanceV1` the accepted Material renders; the
  worker advances `ExecutionPolicyStateV1` with an owner/epoch/lease-fenced CAS,
  fails closed on missing keys, drift, or a stale writer, and only safe
  projections leave the runtime (`../docs/EXECUTION_POLICY_AND_EVIDENCE_UI.md`).
- Startup and background paths carry no request, so every user-scoped
  repository call they make passes an explicit `user_id` -- the row's owner,
  or `None` where the lookup is not user-scoped -- and never the `AUTO`
  sentinel, which resolves the caller from a per-request contextvar and
  raises when there is none. `reconcile_orphaned_inflight_runs` terminalizes
  what it finds, never resumes it and never retries its tool calls, and
  accounts for each run at INFO (run id, previous status, new status,
  reason); its takeover claim commits before the record is returned, so a
  raise between the two leaves a terminal row nobody is told about. The suite
  binds a user contextvar for every test by default, so a test covering
  either rule needs `@pytest.mark.no_auto_user` or it cannot fail:
  `tests/test_startup_recovery_without_user_context.py`.
- Live journal, subagent, workspace, and delivery event writes are
  authority-bound to tenant/run/owner/epoch; recovery uses a separate explicit
  administrative appender. Runtime failures become bounded correlated V1
  evidence once (run rows, SSE, logs, `run.error`, `llm.error`);
  `run.terminal.v1` adds terminal facts without changing opaque authorized
  `run.end` or conversation content.
- `packages/runtime-api/` is the stdlib-only portable contract; Gateway HTTP and
  in-process adapters must stay behaviorally identical. The synchronous
  `DeerFlowClient` is a legacy local graph client and does not enter `InvocationRuntime`;
  it makes no durability claim.
- Artifact provenance proves which extension bytes/configuration HartMesh
  admitted within one startup-frozen process generation; extensions still run
  with Gateway privileges and must come from a trusted operator source.
- `durable_two_gateway_v1` is an exact-two, evidence-gated boundary, not
  arbitrary scaling or IM/upgrade HA (`../docs/MULTI_GATEWAY_QUALIFICATION.md`).
- Provisioner-created Kubernetes sandbox Pods take their optional RuntimeClass
  from `SANDBOX_RUNTIME_CLASS`, apply the restricted baseline (no privilege
  escalation, all capabilities dropped, RuntimeDefault seccomp) to sandbox,
  init, and sidecar containers, and fix `runAsUser`/`runAsGroup`/`fsGroup` at
  1000 non-root, so every companion image must tolerate that identity. Mounts
  must avoid the entrypoint's ownership paths (`/home/gem` and its `Downloads`,
  `/var/log/gem`, `/var/lib/aio-sandbox`, `/opt/gem`, `/opt/jupyter`).
  `SANDBOX_VOLUME_MODE` is resolved once at provisioner startup: explicit `pvc`
  requires both PVC names, explicit `hostpath` keeps the legacy layout, and
  inference accepts only both claim names set or both unset. Startup and
  liveness probes are values-driven: the 200-second startup default leaves 66
  seconds beyond a measured 134-second three-way concurrent gVisor start, and
  the liveness default keeps the 40-second refused-connection budget while
  allowing 61 seconds for a wedged listener (three 10-second probes).
- The local Docker sandbox's cold-start budget is `sandbox.ready_timeout`
  (default 60 s, validated, never disableable): both acquisition paths wait
  that long on a monotonic clock, then destroy under the ownership fences. A
  new container is owned and marked starting *before* the wait, so
  reconciliation and renewal treat it as this instance's; a cancelled async
  wait rolls back under the same fences. Live regression for the released
  Compose limits under runsc, with the template's `sandbox.environment`
  applied and as many concurrent starts as `sandbox.replicas`:
  `tests/test_restricted_runsc_readiness_live.py`.
- `sandbox.replicas` is a **hard budget** on that provider, counted per
  Gateway process, not a soft cap. One admission decision (`_admit_create` /
  `_admit_create_async`) serves both create paths: the slot is reserved in the
  same critical section that reads the count, before any container work, and
  is released only when the container, its sidecar and its networks are
  confirmed absent or the reservation is abandoned. Active, parked,
  quarantined and reserved sets all count; reuse of a set that is already
  counted never spends a second slot. A full budget evicts the oldest parked
  container first and never a live turn, then waits up to
  `sandbox.capacity_wait_timeout` (default 5 s, 0 refuses at once, bounded at
  300; Stop ends it on every path a run awaits: the async create path cancels its awaited
  wait, and a worker-thread acquisition, such as the accepted projection's,
  leaves it through a cancel signal the caller sets, before reserving a slot;
  the budget still bounds work already past the wait and a purely
  synchronous caller, which no Stop reaches) and refuses with
  `SandboxCapacityExceededError` — a typed retryable outcome whose
  `tool_error_type` tells the model to summarize rather than retry, read off
  the exception rather than matched in its text. The wait is one
  `TurnPhase.SANDBOX_CAPACITY_WAIT` span per turn, beside the journal's
  `capacity_waits` / `capacity_refusals` counters.
  `tests/test_sandbox_capacity_budget.py`.
- `make dev`, Docker dev, and production all run the agent runtime in Gateway
  via `RunManager` + `run_agent()` + `StreamBridge`
  (`packages/harness/deerflow/runtime/`); Nginx exposes it at
  `/api/langgraph/*`, rewritten to native `/api/*`.
- Gateway batches `write_file`/`str_replace` argument deltas for multi-mode
  `messages-tuple` consumers (single-mode consumers keep the per-chunk
  contract); non-message frames flush pending batches, and `values` stays an
  optional snapshot, not a batching prerequisite. With `stream_subgraphs`,
  subgraph frames keep their namespace in the SSE event name (`values|<ns>`)
  instead of impersonating root frames, because a delegated subagent's bare
  `values` would replace the whole thread view (#4399); root-only consumers
  ignore namespaced frames, and the web frontend rides root-namespace `task_*`
  events instead of subgraph streaming.
- Background subagent identity is split: the provider `tool_call_id` stays the
  correlation key for `ToolMessage`, `task_*` events, persisted lifecycle
  events, frontend cards, and `ExtensionData.scope_id`
  (`SubagentResult.external_task_id`), while `SubagentExecutor.execute_async()`
  mints a server-side `execution_id` for `SubagentResult.task_id`, the registry,
  polling, cancellation, timeouts, and cleanup. Provider IDs are not unique
  across parent runs and must never become registry ownership keys; scheduler
  closures keep their own `SubagentResult`. Terminal subagent token usage
  travels in the run's `ToolMessage.additional_kwargs`, never a process-global
  provider-ID cache.
- Scheduled executions dispatch through the same Gateway run path
  (`launch_scheduled_thread_run`; `scheduler.recursion_limit` default 1000,
  clamped by `max_recursion_limit`, read from `get_app_config()` at dispatch);
  the scheduler only decides when. It is single-instance by default:
  `scheduler.multi_instance=true` requires shared Postgres,
  `run_ownership.heartbeat_enabled=true`, and `run_events.backend=db`
  (startup rejects anything else), preserves live runs when a peer starts,
  returns expired launch claims to the queue, terminalizes expired
  `terminalize_v1` leases atomically, keeps exact-two rows fail-closed, fences
  stale launch writes by lease ownership, and makes `max_concurrent_runs` a
  shared advisory-locked cap over `launching`/`running` rows.
  `uq_scheduled_task_run_active` allows one `queued`/`launching`/`running`
  occurrence per task: queued work is durable and consumes no concurrency,
  only a short lease-fenced `launching` row invokes the Gateway path under a
  stable admission key so recovery reuses the run, thread conflicts requeue,
  other launch errors fail, repeated triggers coalesce, same-thread FIFO spans
  every active state, definition mutations lock the parent first (pause/delete
  interrupt queued work but reject launched/running work; PATCH/resume reject
  all active states), recovery locks task/run pairs in deterministic order and
  reconstructs live fields before releasing a claim, launch/failure/timeout
  update parent and occurrence atomically (timeout also advances cadence), and
  repositories coerce serialized timestamps before SQL binding.
- Durable MCP tasks (`McpTaskService`, `mcp_tasks`; details in the MCP guide):
  submission persists the remote handle before returning a local ID, the
  database stays authoritative with only a bounded `ThreadState` projection,
  Agent submissions require accepted run facts and the active `started` receipt
  while standalone API submissions ignore client provenance, replay equality
  uses the startup-frozen dedicated HMAC keyring (`replay_commitment.py`; any
  rotation is a quiesced restart, missing historical keys fail replay closed
  without disabling polling/cancellation), public lineage stays redacted and
  parent evidence needs independent parent-run authorization, notification
  failures back off to `dead_letter` after five attempts, cancellation is
  acknowledged only after the durable fence and only while the loop runs, and
  nothing guarantees exactly-once remote execution.
- Retrieval evidence: `docs/EVIDENCE_BEARING_RETRIEVAL.md`.
- `packages/harness/deerflow/tool_plane/` is the only skill/MCP writer under
  default governance (secret-safe base/user revisions, validation, SQL
  generations/attestations, locked projection, bootstrap, drift,
  reconciliation); legacy routes require opt-out, exact-two mounts only
  governed reads, and admission pins the coherent effective revision with its
  captured skill/MCP material. Never bypass it from routers, clients, tools, or
  UI; see `../docs/GOVERNED_TOOL_PLANE.md` and `test_tool_plane_*`.

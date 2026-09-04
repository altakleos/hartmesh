# Exact two-Gateway qualification

## Status and claim boundary

`durable_two_gateway_v1` is qualified only for the exact two-replica PostgreSQL + Redis + AIO/RWX profile and artifact. It does not claim arbitrary scaling, IM connector HA, cross-region operation, or zero-downtime upgrades.

This repository contains the profile contract, implementation, offline verifier,
and opt-in Kubernetes harness. It does not contain a passing live artifact for
this checkout. Until an operator runs the exact live lane and independently
verifies its artifact, the scope is an unpassed release gate. Even after the
harness produces an externally verified artifact, this checkout does not accept
operator-supplied metadata as release authority: production startup and render
remain blocked until a future reviewed release bundles authority for that exact
artifact. A Helm render,
test collection, skipped test, candidate deployment, or
`deployment.qualificationEvidence` reference alone is not qualification.

External retrieval provider qualification is a separate, deployment-specific
gate. The DuckDuckGo, Serply, Tencent WSA, and RAGFlow adapters being present in
the exact-two application does not qualify their network behavior or credentials;
each selected provider must pass the opt-in lane documented in
[Evidence-Bearing External Retrieval](../backend/docs/EVIDENCE_BEARING_RETRIEVAL.md).
A skipped lane or missing credential/network/dataset remains unqualified and
cannot strengthen the `durable_two_gateway_v1` claim.

The one supported scope is:

```text
durable_two_gateway_v1_postgres_redis_aio_rwx
```

The profile is deliberately narrow:

| Surface | Exact supported value |
| --- | --- |
| Gateway | Two replicas; HPA disabled; `Recreate` strategy |
| Tenant | One server-owned tenant identity per release |
| Durable state | One tenant-bound PostgreSQL schema at the exact Alembic head |
| Run ownership clock | `lease_clock=database_v1`; PostgreSQL time owns lease deadlines, renewals, and expired-run scans |
| Redis | Shared, tenant-ACL-scoped stream/cache/ownership/notification keys and channels |
| Scheduler | Enabled on both replicas; database-time leases and global capacity |
| MCP tasks | Enabled, PostgreSQL-fenced, and exercised against the deterministic qualification service |
| Sandbox | In-cluster AIO provisioner and `rwx_verified_copy_v2` only |
| Volumes | Existing home and skills claims with `ReadWriteMany` |
| Images | Gateway, frontend, nginx, provisioner, sandbox, PostgreSQL, and Redis pinned by digest |
| Extensions | Empty canonical set or the exact qualified first-party governance artifact tuple |
| Channels | Long-lived IM connectors and webhook channel ingress disabled |
| Excluded | OpenSandbox, arbitrary extensions, three or more replicas, HPA, cross-region operation, rolling zero downtime |

The checked-in dependency inventory is
[`contracts/deployment/durable_two_gateway_v1.topology.json`](../contracts/deployment/durable_two_gateway_v1.topology.json).
Its completeness tests force every startup config section and every entry in
the Gateway's canonical construction registry used by this profile to be
classified. Startup compares that registry with the inventory exactly, so an
unclassified service or a stale inventory entry rejects the candidate.

## Runtime authority and readiness

PostgreSQL owns admission, run/event state, ownership epochs, scheduled-task
definitions and occurrences, the shared scheduler capacity budget, MCP task
lineage, metadata, dedupe, and topology registration. Redis owns only the
required shared transport/cache/ownership surfaces under the frozen tenant
prefix; durable run history remains reconstructible from PostgreSQL after a
Redis interruption. Accepted skill bytes remain immutable under the AIO/RWX
materialization contract, but exact-two resource recovery is not qualified.

That material guarantee is not an atomic sandbox-operation guarantee. The AIO
capability profile declares `atomic_provider_operation_fencing=false` and
`recoverable_resource_lookup=false`; its one-replica live lane therefore proves
the narrower check-then-call behavior (one validation-gap call may start, later
calls and stale terminal success are refused). Consequently AIO declares
`exact_two=false`, and no one-replica artifact can unlock the exact-two profile.
See
[tenant-bound accepted sandbox execution](../backend/docs/ACCEPTED_SANDBOX_EXECUTION.md).

The exact-two startup inventory and readiness probe inspect the constructed
run store, not only its configured backend. They require the versioned
`database_v1` lease-clock authority after store initialization and before run
heartbeat or recovery begins. A missing or process-clock adapter fails closed.
Other deployment profiles retain their compatible process-clock stores.

Every Gateway registers a redacted topology fingerprint in PostgreSQL. It
contains digests and safe references, never credentials, raw configuration, or
the tenant ID. The fingerprint includes a versioned non-secret confirmation of
the startup-frozen MCP replay keyring: all retained key IDs and bytes plus the
active ID affect it, but no key material is serialized. A different live
fingerprint is rejected. Readiness requires the pod's own compatible
registration and healthy shared dependencies; it does not wait for its peer.
Therefore a surviving pod remains ready after peer loss while the administrator
deployment report truthfully shows one compatible replica and
`degraded_replicas=1`. Adding, removing, or switching replay keys changes the
fingerprint and must use the same quiesced stop/restart procedure as any other
exact-two topology change.

### Recovery is an admission-time, reversible policy

Run recovery is deliberately narrower than generic retry. Every newly accepted
run stores one server-selected policy. Ordinary and historical rows use
`terminalize_v1`, preserving the existing orphan-terminalization behavior.
Only an exact-two qualification candidate may stamp
`exact_two_takeover_v1`, and a client cannot request or change that value.
Changing the deployment profile affects later admissions only; it never
retrofits an active or historical row.

`HARTMESH_EXECUTION_RECOVERY_CLAIMS_ENABLED` is a reversible, process-local
precondition and defaults to `false`. It is not an enablement switch in this
checkout: the exact-two eligibility gate rejects every execution-takeover claim
before any PostgreSQL owner/epoch compare-and-set, even when the variable is
`true`. An expired `exact_two_takeover_v1` row therefore remains fail-closed
and is not attached to a replacement worker or silently routed through legacy
terminalization. Ordinary `terminalize_v1` behavior is unchanged.

This is deliberate candidate-unavailable behavior. Absence of accepted-skill
evidence does not prove that an ordinary AIO sandbox was unused, and projected
Secret rotation is eventual rather than a linearizable revocation point. Safe
enablement requires a per-request database-authoritative owner/epoch check,
owner-fenced destruction, and durable reconstruction of checkpoint-aligned
events, delivery state, output baseline, and cumulative usage before the first
replacement side effect. The dormant coordinator/schema remains an additive
two-way-door seam; it is not a supported capability claim.

Tool recovery has two deliberately separate outcomes. An ordinary tool whose
receipt is started without an outcome terminates with the bounded reason
`recovery_tool_attempt_indeterminate`; HartMesh neither guesses whether its
external side effect happened nor executes it again. A tool accepted with the
fingerprinted `receipt_idempotent_reconcile_v1` contract may resume only after a
trusted coordinator proves the exact open receipt, dispatch generation,
accepted assembly digest, and current takeover epoch. The qualification tool
uses that receipt as the idempotency key for a detached sandbox operation: the
replacement wrapper reattaches to its durable result, while the external
operation body remains exactly once. A tool name or mutable runtime metadata is
never sufficient reconciliation proof. The host seals this finite map under
the existing fingerprinted effective-policy key
`hartmesh.tool_recovery.v1`; it does not change the public extension-api 0.13
`ToolDescriptor` wire shape.

This is a two-way operational door. Returning to `durable_one_replica` changes
the policy stamped on future admissions back to `terminalize_v1`; the additive
schema and existing evidence remain readable. Operators must drain or explicitly
terminalize active exact-two-policy rows before the maintenance rollback. No
down-migration, policy rewrite, or claim inheritance is required or permitted.

The administrator report at `GET /api/runtime/v1/deployment` exposes the safe
replica ID, topology digest, live/degraded counts, exact qualification scope and
artifact reference, plus process-local scheduler/service health. It never
asserts general HA or zero downtime. Support bundles retain only this redacted
topology summary.

## Live release gate

Use only a disposable cluster context and a fresh namespace beginning
`hartmesh-qualification-`. The manual workflow
[`multi-gateway-qualification.yml`](../.github/workflows/multi-gateway-qualification.yml)
requires the confirmed context, namespace, qualification ID, and one bounded
`qualification_subjects_json` object containing the RWX StorageClass, database
schema reference, extension tuple, and repository plus digest for all seven
qualified images, a digest-pinned compatible predecessor Gateway used for the
maintenance-upgrade proof, and a different digest-pinned deliberately
incompatible Gateway binary. Target, predecessor, and incompatible Gateway
digests must all differ. The workflow rejects missing, extra, malformed, or
same-digest controls before touching the cluster. These are immutable operator
subjects. Kubernetes Service/Pod/PVC/Lease UIDs, topology registrations, Redis
ACL denials, and scenario counters are collected from the live cluster and are
never accepted from this input object. Its
kubeconfig comes from `QUALIFICATION_KUBECONFIG_B64`.

The equivalent local entrypoint is:

```bash
export DEERFLOW_TEST_KUBERNETES=1
export DEERFLOW_TEST_KUBERNETES_RUNTIME=1
export DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION=1
export DEERFLOW_TEST_KUBERNETES_SCOPE=durable_two_gateway_v1_postgres_redis_aio_rwx
export KUBECONFIG=/absolute/path/to/disposable-kubeconfig
export DEERFLOW_TEST_KUBERNETES_CONTEXT=<context>
export DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT="$DEERFLOW_TEST_KUBERNETES_CONTEXT"
export DEERFLOW_TEST_KUBERNETES_NAMESPACE=hartmesh-qualification-<run>
export DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID=<safe-run-id>
export DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS=<rwx-class>
export DEERFLOW_TEST_KUBERNETES_EVIDENCE="$PWD/artifacts/multi-gateway-qualification.json"
# Also set the seven qualified DEERFLOW_TEST_*_IMAGE_REPOSITORY / *_IMAGE_DIGEST pairs,
# DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_{REPOSITORY,DIGEST} to a real,
# compatible, digest-pinned predecessor distinct from the target,
# DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_{REPOSITORY,DIGEST} to a real
# third digest that can reach topology startup but is topology-incompatible,
# DEERFLOW_TEST_EXTENSION_{ARTIFACT,CONFIGURATION}_DIGEST,
# DEERFLOW_TEST_CAPABILITY_MANIFEST_DIGEST, and
# DEERFLOW_TEST_DATABASE_SCHEMA_REF exactly as listed by the workflow.
cd backend
PYTHONPATH=. uv run pytest -m kubernetes_contract -v -s \
  tests/kubernetes/test_multi_gateway_qualification.py
```

Missing inputs, tools, real PostgreSQL/Redis/Kubernetes routing, RWX behavior,
or any scenario fail the enabled lane. They are never converted to a skip. The
exact-two test file is itself scope-specific: once Kubernetes qualification is
explicitly enabled, an absent or different `DEERFLOW_TEST_KUBERNETES_SCOPE`
fails the entrypoint instead of reporting a passing no-op. Use the separate
one-replica entrypoint for either one-replica scope. The
harness exercises two directly addressable Gateway pods and their Service,
deletes owners, interrupts dependencies, introduces a mismatched pod, and runs a
second isolated tenant release. It must pass these exact scenarios in order:

1. topology identity;
2. concurrent admission;
3. execution ownership;
4. owner SIGKILL at pre-materialization, post-materialization, post-checkpoint,
   post-dispatch-marker/pre-graph, model, long sandbox-tool, and
   pre-terminal-commit windows, including checkpoint/event alignment and stale
   owner rejection;
5. SSE reconnect;
6. scheduler occurrence/global cap;
7. both scheduler owner-loss windows;
8. nonempty accepted-skill sandbox recovery with exact resource identity,
   linearizable old-owner revocation, byte-identical accepted evidence,
   revalidation, one durable tool outcome, and exactly one receipt-keyed
   external operation body across takeover;
9. MCP poller takeover/result/notification lineage;
10. cross-pod cancel/fail/succeed finalization races, terminal owner/lease
    release, and accepted-material Lease/Pod cleanup;
11. an actual SSE request to both pods returning retryable `503` plus
    `Retry-After` during Redis loss, followed by cursor replay, durable-history
    reconstruction, and ACL denial after recovery;
12. an owner-only PostgreSQL network partition, peer takeover, and a verified
    stop/continue of the original `uvicorn` process, with its real checkpoint,
    receipt, accepted-material renewal, and terminal mutations all rejected;
    its controlled tool window also reconciles the same receipt without
    repeating the external operation body;
13. configuration and real incompatible-binary skew rejection;
14. tenant separation across restricted database roles (including cross-database
    connection denial), Redis ACLs, Kubernetes, and HTTP;
15. every unsupported chart/startup surface;
16. mixed-version rejection and a stop/migrate/start transition from the
    pinned compatible predecessor to the target while preserving durable rows.

Separate deterministic contract tests cover the negative half of the tool
policy: a generic started-without-outcome receipt has no accepted reconciler,
terminalizes with `recovery_tool_attempt_indeterminate`, keeps starts at one and
outcomes at zero, and never attaches a replacement tool attempt. That negative
does not substitute for the positive controlled live tool-recovery scenarios.

The success file is published atomically only after the canonical artifact
passes the offline verifier against independently supplied subjects. Failure
writes a bounded failure record and redacted cluster diagnostics. The verifier
checks the exact schema/scope, digest, freshness, seven images, chart/config/head,
two distinct compatible replicas, environment subjects, all 16 scenarios, and
their supporting counters. Never derive expected subjects or the scenario list
from the artifact being verified.

The Helm `deployment.qualificationCandidate` switch is test-only. It is accepted
only in a disposable qualification namespace and only when the Gateway has the
internal live-harness runtime flag. It cannot carry passing evidence and must
never be used for production traffic.

Scenarios 4 and 8 are mandatory and cannot pass with the current global
takeover gate. Their negative result is not converted into passing evidence;
consequently this checkout cannot emit a passing artifact or render the profile
as production-supported.

## Future adoption contract: stop, migrate, start

These steps apply only after a later release explicitly enables the exact
verified artifact. They are not executable with this candidate-only checkout.
Use a maintenance window. Mixed binaries/configuration are intentionally rejected.

1. Qualify the target image/chart/configuration tuple in staging and retain the
   canonical artifact plus its declared digest.
2. Run `backend/scripts/verify_qualification_evidence.py` offline with expected
   subjects supplied by the deployment controller. Require its bounded
   `status: verified` result, then land a reviewed release change that binds the
   exact artifact and subjects; a Helm evidence reference alone cannot do so.
3. Back up PostgreSQL and the RWX volumes. Disable upstream traffic and new
   admission. Pause operator-created schedules that must not enqueue during the
   window.
4. Wait until application `runs` have no `pending` or `running` rows,
   `scheduled_task_runs` have no `queued`, `launching`, or `running` rows, and `mcp_tasks`
   have no `submitted`, `working`, or `input_required` rows. Treat nonzero rows,
   an unready dependency, or an incomplete graceful-shutdown report as a stop.
5. Stop the old Gateway deployment and confirm no Gateway pod remains:

   ```bash
   kubectl -n <namespace> scale deployment/<release>-deer-flow-gateway --replicas=0
   kubectl -n <namespace> wait --for=delete pod \
     -l app.kubernetes.io/component=gateway --timeout=5m
   ```

6. Install/upgrade the exact qualified values. Helm's pre-install/pre-upgrade
   migration Job uses the exact Gateway digest and a PostgreSQL advisory lock.
   Gateway pods run `uv run --no-sync`, verify the head, and never race an
   application-start migration.

7. Require two distinct compatible registrations, both readiness probes green,
   no degraded replica, and the exact artifact digest in the administrator
   report. Run bounded admission/SSE/scheduler smoke checks before restoring
   traffic and resuming schedules.

## Rollback

Rollback is another maintenance window, not a rolling downgrade.

1. Disable traffic and pause new schedule admission; wait for the same ownership
   conditions above. Confirm there are no active rows stamped
   `exact_two_takeover_v1`; rows already terminal remain immutable history.
2. Scale the two-Gateway deployment to zero and wait for graceful termination.
3. Restore `durable_one_replica` with one of the identical compatible
   image/config replicas and `gateway.replicas=1`.
4. Do not down-migrate the shared schema. Preserve topology and qualification
   evidence records, including each run's recovery policy. If the previous
   one-replica binary cannot read the current additive schema, restore a tested
   forward-compatible image instead of forcing a database downgrade.
5. Verify one-replica readiness and durable history before restoring traffic.

## Failure codes and troubleshooting

Startup and render checks use stable public reasons including
`topology_profile_unsupported`, `topology_fingerprint_mismatch`,
`topology_replica_count_invalid`, `topology_dependency_not_shared`,
`topology_extension_not_replica_safe`, `topology_channel_not_replica_safe`, and
`topology_qualification_missing`. Inspect the authenticated deployment report,
the migration Job, readiness reason codes, and the redacted support-bundle
topology summary. Do not paste Secrets, DSNs, raw config, task inputs/results, or
tenant IDs into diagnostics.

Logs and evidence use the tenant public reference, safe replica/run/schedule/task
IDs, ownership epoch, takeover/stale-rejection counts, and dependency status.
Sticky sessions may improve transport locality, but they are never required for
correctness.

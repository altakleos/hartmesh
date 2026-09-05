# Tenant-bound accepted sandbox execution

## Status and guarantee

Durable accepted sandbox work is authorized by the existing accepted-material
tuple. HartMesh does not add another execution lease, epoch, table, or heartbeat:

| Component | Responsibility | Not authority for |
| --- | --- | --- |
| `AcceptedMaterialExecutionClaimV1` | Current tenant/run/worker/state fence from the durable run store | Provider cleanup or immutable evidence |
| `AcceptedMaterialLeaseV1` | Provider resource, provider ownership epoch, expiry, and private renewal handle | Durable run ownership |
| `AcceptedExecutionEvidenceV1` / `V2` | Immutable material, image, provider, epoch, qualification, and isolation proof | Current execution or cleanup permission |
| `AcceptedMaterializer` | Provider acquisition, validation, renewal, and release | Durable run-state transitions |
| `AcceptedSandboxSession` | Composes the run fence and materializer tuple before each operation | New authority of its own |
| Cleanup ownership | Reaps provider resources and reconciles orphans | Sandbox execution |

The session exposes operations, never its backing provider sandbox. Every
public `Sandbox` operation crosses the same facade. The set is declared once in
`sandbox/operations.py` (ten today: command execution, scoped command
execution, scope release, full/ranged read, download, directory list, text
write, glob, grep, and binary update); the facade's methods are generated from
those declarations, and the module refuses to import if a `Sandbox` method has
no declaration or the facade would inherit one as an unfenced passthrough.
Upstream's scoped-shell hooks are declared already; providers keep the base
class's pass-through defaults until they implement scoping, so a scoped call on
the facade is fenced and recorded even where it is not yet isolated. Normal
sandbox tools, sandbox middleware, output externalization, lead agents, inherited
subagents, and durable batch children resolve that facade through one resolver,
`declared_sandbox()`, which answers from the executing context's session
declaration; nothing in a runtime context dict can stand in for it. A batch
child declares a separate session over its existing SQL item-attempt fence; it
does not borrow the parent run's mutable authority. The
child's canonical request/evidence pair and initial `acquired` observation are
atomically attached to that attempt after the provider call and a fresh fence
sample, before the executor may start. Later bounded lifecycle observations are
appended through the retained attempt/evidence/worker binding even after terminal
publication; they cannot authorize another operation.

Before every provider call the session:

1. samples the current durable run or batch-item attempt fence;
2. calls `AcceptedMaterializer.validate(lease, evidence)`;
3. performs a final process-local open/lease identity check; and
4. delegates the closed operation envelope to the private sandbox object.

Renewal reuses the worker's supervised run heartbeat. Run-fence loss, provider
validation or renewal loss, cancellation, and close invalidate the session for
later calls. Close refuses new calls immediately, waits for an already-delegated
call, then releases through the materializer. Terminal publication independently
revalidates the tuple and remains fenced by the durable run/item store.

### Session provider

Every path that resolves a sandbox handle, including Gateway routes, channels,
and upstream's own middleware and tools, goes through the configured provider's
`acquire`, `get`, and `release`. HartMesh installs a session provider
(`sandbox/session.py`) in front of whatever `sandbox.use` resolves, exactly once
per process, and dispatches those three verbs by the executing session's
declaration:

- an execution that declared an accepted session acquires that session's
  public ref and never provisions; if the session is no longer open the acquire
  fails with `sandbox_session_conflict` rather than falling back to an ordinary
  thread sandbox;
- a public ref resolves to the fenced facade only for the execution that
  declared it, so a fork, a Gateway request, or a channel with no declaration
  gets nothing back and takes its own ordinary path;
- an ordinary acquire for a user and thread held by an open accepted session is
  refused with `sandbox_session_conflict`, which is what makes "destroy while
  another holder is attached" impossible rather than merely unlikely; and
- releasing a public ref retires the session, once, and only from the declaring
  execution.

The declaration travels with the execution as a context variable that child
tasks and worker threads inherit, and it is the only carrier: no runtime
context key holds a session, so a caller-supplied context cannot plant one.
Provisioning always precedes declaring. The durable worker materializes the
session, calls `declare_accepted_sandbox_session` with the run's
`(user_id, thread_id)` mount scope, binds the declaration to the run's task so
every task and thread the run spawns inherits it, and withdraws it after the
session closes. An in-run subagent borrows its parent's declaration: the task
tool hands the current declaration to the executor, which binds it for exactly
the child's execution. A durable batch child is the same kind under its own
attempt key: the batch service declares the child's session with no mount
scope, because no ordinary acquire is keyed by an attempt and so none can
collide with it, and the executor binds that declaration on the isolated
subagent loop for the child alone; the service loop is never bound. Closing a
session ends its declaration; withdrawing is idempotent and never disturbs a
later declaration of the same public ref. A session that is already declared
and still open cannot be declared again.

Every other provider method and attribute is forwarded unchanged, so ordinary
sessions behave exactly as the backing provider does, and `isinstance` checks
against the configured provider class keep working. Accepted-suffixed
containers found by the AIO startup reconciler are destroyed once this instance
can claim them, never adopted into the warm pool.

This is a check-then-call guarantee, not distributed atomicity. A call accepted
before loss may finish. For a provider without atomic operation fencing, one call
may also enter the provider when takeover/loss occurs after both checks but before
provider acceptance. Once loss is observed, every later call is refused, and the
stale worker cannot publish accepted terminal success. A provider may set
`atomic_provider_operation_fencing=true` only when the expected epoch travels in
the operation request and is checked atomically with starting that operation.

Upstream's execution leases (`sandbox/lease.py`) sit beside this, not inside
it. The lease manager only ever calls the provider's `acquire`, `get`, and
`release`, and HartMesh keys every manager by the installed session provider
(`lifecycle_sandbox_provider`), so a caller holding the backing provider and a
caller holding the wrapper share one manager whose calls go through the
declaration dispatch above. A declared execution never takes a lease: its
tools and middleware resolve the declared handle before any lease code runs,
and the declarer owns the terminal. Accepted-skill sandboxes (the projection
material, not an accepted session) are held under the execution lease as
borrowers, because the projection's consumer refcount is what parks them.

Provider hooks that are keyed by sandbox id, today the network policy hooks
(`consume`, `deny_pending`, `decide`), receive whatever id state carries. For a
declared session that is the public ref, so the session provider translates it
to the provider's own id, which the declaration carries as `provider_ref`, for
the declaring execution only; a stranger's call resolves to no events and no
decision, and the provider id never appears in state, logs, or evidence.

## V1 and V2 persistence boundary

V1 remains strictly decodable under its original guarantees. It is never silently
upgraded. V2 retains every V1 semantic field and adds the missing accepted
anchors:

| V1 meaning | V2 representation |
| --- | --- |
| Run, attempt, tenant, provider, ownership epoch | Retained |
| Runtime image, skill snapshot/scope, materialization, verifier, read-only proof, qualification scope | Retained |
| Raw `provider_instance_ref` | Replaced in portable evidence by a tenant-bound SHA-256 resource commitment |
| Accepted invocation | Pseudonymous reference plus immutable invocation digest |
| Governed tool plane | Deployment-base, user-overlay, projection, and effective digests |
| Batch child | Optional tenant-derived child-attempt reference |
| Provider guarantees | Capability-profile digest plus qualification-evidence digest |
| Isolation | Bounded restricted-non-root, read-only-material, no-privilege-escalation facts and optional runtime-class/network-policy digests |

Request and evidence decoders require exact field sets and canonical digests.
The lease's provider handle and renewal object stay process-local. Portable V2
evidence and lifecycle events contain no namespace, Pod/container/sandbox ID,
endpoint, credential, command, file content, or output. The safe handoff forms
are `accepted-execution-<evidence-digest>` and the per-call opaque
`accepted-operation-<uuid>` reference.

A persisted V1 row can be read and terminalized under V1. A durable V2 provider
selection must produce V2 evidence; a missing V2 invocation/tool-plane binding,
capability mismatch, or absent current qualification fails before model work.
AIO process takeover remains unavailable because the per-attempt capability is
not recoverable across processes.

## Provider capability and qualification matrix

Capability is an adapter declaration. Qualification is current, exact external
evidence for a configured deployment. Both are required for durable admission.

| Provider/profile | Ordinary use | Immutable accepted material | Ownership / shared expiry | Atomic operation fence | Resolved image and restricted isolation | Protected lookup after process loss | Durable one replica | Exact two |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Remote AIO/Kubernetes `rwx_verified_copy_v2` | Yes | Declared and verified per attempt | Declared atomic ownership and authoritative shared expiry | **No**; baseline check-then-call only | Declared; each attempt binds observed digests | **No** | Eligible only with a current pinned passing artifact | **No** |
| Local/container AIO | Yes | Existing local accepted-only behavior only; no durable V2 selection | No qualified durable profile | No | No live durable qualification | No | No | No |
| Local host, E2B, BoxLite, Tenki | Yes under their ordinary contracts | No durable accepted profile | Not claimed for this contract | No | Not qualified for this contract | Not claimed | No | No |
| OpenSandbox 0.1.14 / SDK 0.1.15 | Yes | `empty_only`; nonempty durable paths rejected | Required ownership CAS is absent | No | Resolved-image readback is absent | No | No | No |
| In-memory accepted adapter | Tests only | Contract fixture | Test state only | No production claim | No production claim | No | No production claim | No |

Remote AIO's capability profile deliberately records
`atomic_provider_operation_fencing=false`, `recoverable_resource_lookup=false`,
and `exact_two=false`. The repository does not ship a passing artifact. Missing
cluster infrastructure or an unrun lane is an unpassed gate, never a skip that
enables production.

## Qualification and configuration

For production durable admission, mount one canonical
`deerflow.accepted-sandbox-qualification/v1` companion read-only into the
Gateway and configure all three fields. It embeds and digest-binds the
independently verifiable Kubernetes accepted-skill v2 evidence:

```yaml
sandbox:
  use: deerflow.community.aio_sandbox:AioSandboxProvider
  provisioner_url: http://deer-flow-provisioner:8002
  accepted_skill_projection_profile: rwx_verified_copy_v2
  accepted_material_qualification_evidence: /var/run/hartmesh/qualification/evidence.json
  accepted_material_qualification_digest: sha256:<artifact-sha256>
  accepted_material_qualification_max_age_seconds: 2592000
```

The path and digest are an inseparable pair. Selection reads at most 64 KiB,
requires canonical strict companion JSON with `status: passed`, verifies the
pinned byte digest and freshness, and compares its AIO capability-profile and
portable topology-policy digests with a fresh authenticated provisioner sample.
The sample must resolve the current namespace UID, ServiceAccount, and each
bound PVC UID plus its `spec.volumeName`; it does not claim a PV UID. Those
deployment-specific values are excluded from the portable policy digest because
the qualification namespace is disposable. It records
the non-atomic fencing race, subsequent refusal, stale-terminal refusal, and
cleanup; the embedded v2 proof pins all image subjects. A standalone v2 artifact
cannot unlock execution. Any mismatch returns a safe
`sandbox_provider_unqualified` or `sandbox_image_unresolved` failure before
model/tool work. Qualification artifacts are administrator-controlled deployment
material, not API-writable configuration.

The live Kubernetes harness has a circular-bootstrap exception: a Helm
`deployment.qualificationCandidate` can create a short-lived `candidate`
qualification only when every internal test/fault flag is set, its ID matches the
harness qualification ID, and the namespace begins `hartmesh-qualification-`.
Candidate status is never current production evidence and must be explicitly
allowed by the worker. The harness runs a restricted non-root Pod, bounded work
through `AcceptedSandboxSession`, deletes the authoritative provider Lease after
both checks at a deterministic barrier, observes exactly one raced call for AIO's
non-atomic profile, proves the next call is refused, and proves stale terminal
success is rejected. Publishing disables candidate mode, mounts the companion,
then requires one fresh accepted invocation through the restarted Gateway before
the artifact is finalized.

Exact-two remains rejected. Its enablement additionally needs recoverable
protected resource lookup and live cross-worker atomic operation-fencing evidence;
projected Secret rotation is not linearizable revocation.

## Lifecycle and recovery

`sandbox.lifecycle.v1` is a bounded, non-authoritative trace event linked to the
accepted run/attempt and execution-evidence digest. Routine renewal success is
coalesced. A durable batch attempt stores at most eight distinct observations;
an overflow is rejected rather than truncating away acquisition or loss. Its
states are:

- `acquired`: a session was constructed from a validated tuple;
- `authority_lost`: run or provider authority failed validation/renewal;
- `released`: materializer release completed;
- `cleanup_pending`: release failed or was interrupted and cleanup ownership must reconcile;
- `orphaned`: durable run reconciliation won the expired-owner CAS for persisted accepted evidence.

The orphan event is written only after authoritative run takeover/terminalization
or a batch attempt's database-time lease-expiry transition.
It diagnoses the abandoned resource; it does not authorize a replacement worker
or cleanup. Run observations use the event store; batch-child observations remain
on the existing append-only attempt row and are exposed through the same
owner-scoped bounded lifecycle query. Observation failure does not undo the
terminal CAS. Logs and observations carry safe provider kind, qualification
scope, time, reason code, and evidence digest—not the raw resource reference.

Recovery outcomes follow the existing authorities:

| Failure point | Deterministic outcome |
| --- | --- |
| Resource created before materializer return | Adapter compensates with provider destroy; a failed compensation is `cleanup_pending` for existing reconciliation |
| Material placed before evidence joins `RunRow` | Pending run cannot execute; release/cleanup paths own the resource |
| Worker dies after evidence persistence, before first operation | Expired run owner is terminalized, an `orphaned` observation is emitted, and provider cleanup reconciliation proceeds |
| Process dies with an accepted container running | The AIO reconciler destroys the accepted-suffixed orphan instead of parking it in the warm pool; it never becomes an ordinary thread sandbox |
| Run/provider loss during an operation | Already-issued work may finish; later operations and stale terminal publication fail closed |
| Release succeeds but final observation fails | Resource remains released; diagnostics may be incomplete and never become authority |
| Release fails after local close | Session stays closed, records `cleanup_pending`, and existing cleanup ownership/reconciliation retries or reaps |

See [invocation runtime](INVOCATION_RUNTIME.md),
[OpenSandbox feasibility](OPENSANDBOX_ACCEPTED_MATERIAL_FEASIBILITY.md), and the
[Kubernetes/Helm guide](../../deploy/helm/deer-flow/README.md).

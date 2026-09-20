# Sandbox sessions and tenant-bound accepted execution

## Every sandbox is a session of a declared Kind

HartMesh runs every sandbox, ordinary or accepted, as one session under one
Kind. A Kind is four policies:

| Policy | Question it answers | Ordinary Kind | Accepted Kind |
| --- | --- | --- | --- |
| Material | What is the container made of? | The mutable thread workspace, plus the bound accepted-skills snapshot under an explicit Agent policy | Digest-bound admitted material placed by the qualified materializer before the run starts |
| Fence | What is checked before each operation? | Nothing beyond upstream's authorization gate and execution lease | The durable run or batch-item attempt fence, then the materializer's lease validation |
| Terminal | What happens when the last holder leaves? | Park: the provider keeps the container for the next turn | Retire: destroy once, from the declaring execution, never park |
| Observer | Where are operations recorded? | Thread-scoped diagnostics | The evidence ledger (the closed lifecycle set) plus run-bound diagnostics; the declaration's `observe` carries a fact observed outside the run onto the run's record |

Ordinary is the degenerate accepted session: no fence, no ledger, park instead
of destroy. The public surface is two constructors. `SandboxSessionKind.ORDINARY`
is the default and nothing declares it; `SandboxSessionKind.ACCEPTED` is
declared by the durable worker or the batch service after the material exists.
A third population is a code change, not a configuration.

### Concepts

| Concept | What it is | Module | Owner |
| --- | --- | --- | --- |
| Session | One container in use by one or more holders under one Kind | `sandbox/session.py` | HartMesh |
| Kind | Material, Fence, Terminal, and Observer, declared once per execution as a `SandboxSessionDeclaration` | `sandbox/session.py` | HartMesh |
| Resource key | The principal and resource the registry serializes on. Distinct from the mount scope, which is whose thread the material is projected from; a batch child shares its parent's mount scope and owns its resource key | providers | HartMesh derivation, upstream tuple shape |
| Public ref | The only identifier that leaves the provider: the provider's own id for an ordinary session, `accepted-execution-<evidence-digest>` for an accepted one. It is what state, logs, and evidence carry | `sandbox/accepted_material.py` | HartMesh |
| Operation | The closed set of `Sandbox` verbs, declared once; the declaration generates the fenced facade method, the envelope, and the evidence label | `sandbox/operations.py` | HartMesh |
| Registry | Holders, borrowers, per-key serialization, last-holder release | `sandbox/lease.py` | Upstream, verbatim |
| Session provider | Dispatches acquire, get, and release by the declared Kind, translates public refs, refuses conflicting admission, and runs the Kind's terminal | `sandbox/session.py` | HartMesh |
| Capability | Optional provider contracts negotiated at runtime; the required provider surface stays acquire, its async twin, get, and release | `sandbox/capabilities.py` | HartMesh, upstream-shaped |

### The populations, as one Kind four ways

| Population | Kind | Resource key | Material | Fence | Terminal | Handle |
| --- | --- | --- | --- | --- | --- | --- |
| Ordinary thread | ordinary | `(user, thread)` | mutable thread workspace | none | park | provider's own `Sandbox` |
| Accepted-skills projection | ordinary | `(user, thread)` | thread workspace plus the bound snapshot; the consumer-token coordinator lives inside the Material | none | park | provider's own `Sandbox` |
| Accepted durable lead | accepted | `(user ref, thread ref)` | digest-bound admitted material via the materializer | run claim against the SQL fence | retire | generated facade |
| Batch child | accepted | `(user ref, accepted-attempt digest)` | same | item-attempt fence | retire | generated facade |

### Rules

1. **Public refs resolve only for the declaring execution.** A fork, a Gateway
   request, or a channel with no declaration gets nothing back and falls
   through to its own ordinary acquire.
2. **Accepted material is provisioned before it is declared**, never lazily by
   the registry. A missing declaration is a typed failure, never an ordinary
   acquire, so the registry's synchronous acquire path never has to provision
   accepted material.
3. **Terminal is a property of the container, fixed at provisioning.** A
   retire-terminal container refuses a second mount-scope holder at admission.
4. **The real provider identifier never leaves the session provider.** Network
   hooks and scope release receive public refs and translate inside.
5. **Every operation crosses the declared facade.** Normal sandbox tools,
   sandbox middleware, output externalization, lead agents, inherited
   subagents, and durable batch children resolve their handle through one
   resolver, `declared_sandbox()`, which answers from the executing context's
   declaration; nothing in a runtime context dict can stand in for it. The
   raw provider verbs (`get`, `acquire`, `acquire_async`) are called only
   from the modules that own resolution; `tests/test_sandbox_handle_boundary.py`
   scans the harness and the Gateway and fails on any other caller, so the
   opt-out is one allowlist entry, visible in the diff.

## Session provider

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
  another holder is attached" impossible rather than merely unlikely. The
  Gateway's upload and artifact sync and the channel attachment copy turn that
  refusal into a skipped sync: the file lands in thread storage, the response
  says `sandbox_sync_skipped: sandbox_session_conflict` with a plain sentence
  in its message, and the refusal is recorded on the holding run as a
  `session.refused` diagnostic; and
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
the child's execution. A durable batch child is the same Kind under its own
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

Provider hooks that are keyed by sandbox id, today the network policy hooks
(`consume`, `deny_pending`, `decide`), receive whatever id state carries. For a
declared session that is the public ref, so the session provider translates it
to the provider's own id, which the declaration carries as `provider_ref`, for
the declaring execution only; a stranger's call resolves to no events and no
decision, and the provider id never appears in state, logs, or evidence.

### Which population a deployment profile runs

Every Gateway run is an accepted invocation with a run record, and a nonempty
effective-skill snapshot makes its materialization mandatory before the
authoritative running transition. The population that serves it is the
deployment profile's decision, not the record's:

| Profile | Nonempty snapshot | Empty snapshot |
| --- | --- | --- |
| `local_development` | Accepted-skills projection: the provider's own parked `(user, thread)` sandbox with `.accepted` as its only skills mount, bound before the model is called, released at the run's end with its verified read-only view retained (the next bind of the same digest verifies it in place, a different digest replaces it, teardown clears it) | Accepted-skills projection with no snapshot bound: no materialization before the model, the sandbox is provisioned at the first tool call, `.accepted` is empty and every skills path is refused |
| `durable_production`, `durable_two_gateway_v1` | A qualified materializer, or `sandbox_provider_unqualified` before any sandbox exists | Accepted-skills projection with no snapshot bound: no materialization before the model, the sandbox is provisioned at the first tool call, `.accepted` is empty and every skills path is refused |

The ordinary thread sandbox, which mounts the Gateway's `skills_view/public`
read-only at `/mnt/skills/public`, belongs to runs that carry no resolved
agent material; no Gateway turn is one. The profile decides only when the
provider offers no materializer; a provider that offers one whose
qualification is stale or whose capabilities fall short still refuses under
every profile, and the Kubernetes qualification candidate never takes the
projection.

`_durable_admission_required` in `runtime/runs/worker.py` reads the resolved
configuration's profile; a missing configuration or profile counts as
durable. From 2026-09-03 until
this rule the worker keyed the refusal on the record's presence instead, which
made the projection population unreachable from any real run: the first tenant
release that seeded a public library (v2.1.0+hartmesh.13) refused every chat
turn with `AcceptedSkillSandboxBindingError` (reason
`sandbox_provider_unqualified`) on a healthy stack, and no earlier release had
noticed because no tenant had a skill, so every snapshot was empty. Two
properties of the projection path matter when reading timings and paths: the
sandbox is provisioned before the model runs, so on a new thread the cold
start precedes first text; and the model-visible skill path is
`/mnt/skills/.accepted/<snapshot digest>/<category>/<name>`, the live
`/mnt/skills/public` mount is absent from such a sandbox, and the sandbox
tools refuse skill paths outside the snapshot. A provider that cannot declare
immutable accepted material refuses a nonempty snapshot under every profile
(`accepted_skill_snapshot_immutability_unsupported`): the host-local provider
answers `empty_only`. A stock `make dev` stack uses that provider and the
repository's own `skills/`, so any turn by a user with a skill enabled refuses
with that reason; a local stack that runs skills needs the AIO provider over
Docker. `tests/_seeded_skill_sandbox_provider.py` is the
test-only subclass that declares it for the scripted-Gateway regression
(`tests/test_seeded_skill_gateway_stream_e2e.py`);
`test_worker_materialization_follows_the_deployment_profile` pins the three
profiles. The boundary error stays opaque to callers; the worker logs the
reason code (`Accepted skill materialization failed ... reason=`).

### Park means reuse

Park is only worth its name if the next turn actually takes the container
back. The ordinary acquisition path has always done so at its warm-pool layer;
the accepted-skills projection path (an ordinary Kind: park terminal, thread
resource key) did not. It checked active tracking, found nothing -- release had
moved the entry to the warm pool -- and fell through to create, whose replica
enforcement evicts the oldest warm entry before the backend can observe that
the target already exists. On the released Compose profile that meant a
compatible follow-up turn stopped an unrelated thread's container and its relay
(about twenty seconds) and then rediscovered or rebuilt its own.

The repair is a reclaim step between the active check and create,
`AioSandboxProvider._reclaim_accepted_warm_sandbox`. It hands a parked
container back only when every one of these holds; what happens when one
does not is described after the table:

| Fact | Where it is proved |
| --- | --- |
| The id is one *this process* provisioned as an accepted-only projection | `_accepted_only_sandbox_ids`, under the provider lock |
| The create-time inputs still match: mount set, lark provisioning flags, config-mount exclusion root, backend class, skills root, and -- for the remote backend, which bakes them into the Pod -- binding identity, execution claim and egress allowance digest | `_accepted_reuse_fingerprint`, recorded at create and compared at reclaim; a digest of inputs this process computed, never of provider text |
| Same tenant and thread or attempt identity | `_assert_warm_identity_available_locked` |
| Not reserved for local teardown | `_being_torn_down_locally`, checked before and after the ownership round trip |
| Alive according to the backend | `_check_tracked_sandbox_alive` |
| Ownership published before the warm-to-active transition | `_publish_ownership`; a peer's `del:` refuses |

Anything the fingerprint does not cover is either identical by construction
or re-established after reuse: `bind_accepted_skill_snapshot` re-projects the
bound snapshot on every acquisition and still refuses a remote receipt that
does not match, which is the same contract the already-active branch relied
on. A deterministic name or an `-accepted` suffix alone proves nothing and is
never consulted as evidence.

Three properties keep the repair from becoming a different defect. Refusing to
reuse never destroys anything *unrelated*: a mismatch on someone else's entry
leaves it where it was. Eviction never targets the id it is making room for
(`_evict_oldest_warm(exclude=...)`), which would buy a cold start with a
teardown. And a genuinely new third resource at capacity still evicts an
unrelated warm entry and waits for it -- that is real work, and hiding it
behind an overlapping unbudgeted container is the separately tracked capacity
defect, not this repair.

### Prewarm: the first turn's container, built while the person types

Park-means-reuse makes the *second* turn fast. The first turn still paid the
container: on the released profile, create plus readiness measured 10 to 29
seconds of a pre-model block that is otherwise about two seconds, and every
second of it was spent while the person waited on a message the sandbox had
no part in. The fingerprint above already says why that wait is avoidable: on
a local backend the accepted container is shaped by `(user, thread)` and the
configured mounts, never by the binding, so it can be built before anyone
knows what the turn will say.

`WorkspacePrewarm` (`sandbox/capabilities.py`) is that contract, and
`AioSandboxProvider._prewarm_accepted_skills` implements it as the acquisition
minus the hand-out: the same preflight (`_accepted_projection_preflight`, one
statement for both callers, so a prewarm can never park what the turn would
have refused to build), the same deterministic name, the same fingerprint
recorded with `binding=None`, the same `_create_sandbox`, then `release`. The
turn's own `_reclaim_accepted_warm_sandbox` finds it; nothing about
acquisition changes, and no second equivalence rule exists. The Gateway
exposes it as `POST /api/threads/{id}/workspace/prewarm`, which the web client
calls the moment it mints a new thread id -- seconds before the first message.

Four rules keep a speculative build from ever slowing a real turn, and the
fourth is the honest exception:

| Rule | Where |
| --- | --- |
| A prewarm never evicts: it takes a free slot or builds nothing (`SandboxSlotsBusyError`), and containers still in their readiness wait count as taken, so prewarms started within seconds of each other stay inside the slot budget | `_create_sandbox(allow_eviction=False)` |
| A prewarm never stands in for an active sandbox, and never replaces a parked one -- a mismatch is the acquisition's to resolve under its own fences | `_prewarm_accepted_skills`, under the acquire serializer |
| The view publication runs **after** the acquire serializer is released, with the container already parked, so a turn arriving mid-publication takes it and goes. It published under the hold until the released `.26` measured what that cost: 3.7 s queued on every new chat and 16.3 s on the first after a boot, waiting for the head start it was being handed. It needs no hold — it fences itself under the views lock, where a generation-0 bind is refused on a view a run owns and the clear is a compare-and-pop that never touches another run's bytes. A right guess still repays in full (the turn verifies in tens of ms instead of staging ~2 s); a lost race costs exactly the staging `.25` paid on every first turn, and never a wait | `_publish_prewarmed_skill_view`, called outside `_acquire_serializer.hold` |
| An unclaimed prewarm is stopped once it has sat past `sandbox.prewarm_claim_timeout` (default 300 s), not the idle timeout -- the reaper looks every 30 s, so the container goes at up to 330 s; the mark is the parked container *object*, never its reused id, so a container rebuilt under the same id after an eviction or replacement is never mistaken for the prewarm, and the claim pops the mark, so a container a turn used is an ordinary parked sandbox again | `_reap_unclaimed_prewarms`, on its own reaper thread -- independent of `idle_timeout`, as lease renewal is, so `idle_timeout: 0` cannot silently disable it |

The remote backend refuses (`None`): there the binding is baked into the Pod
at creation, which the fingerprint records, so a container without one is not
the container the turn needs. The route answers `scheduled: false` for a
provider without the capability and 202 either way; a build failure is the
provider's log, never the client's error. The evidence contract is untouched:
materialization still completes before `try_start`, it just binds into a
container that is already ready.

**The view, not only the container.** On the released `.25` profile a tenant's
prewarmed container was reclaimed in 14 ms and the first turn still spent
2.251 s in `skill_materialization`: the container was parked, the accepted
*view* it mounts was not, so the first `bind_skill_snapshot_active_view`
staged a full fsync'd copy while every later bind verified in place. The
prewarm now publishes that view too. The route resolves
`likely_first_turn_skill_snapshot` (`runtime/agent_revision.py`) — the same
`snapshot_effective_skills` call the turn makes, over the enabled skills,
which is exactly what an ordinary default-agent turn with no subagents
brings — and hands it to the provider, which binds it under
`run_id="prewarm"` and **generation 0**, the provisional identity every real
run supersedes without a conflict.

It is a guess, and only the bytes ever authorize its reuse: the turn adopts
the published view when its own snapshot id matches and the tree re-digests
to the evidence, and otherwise re-stages exactly as it does today. The guess
is exact more often than "guess" suggests — an unnamed run's transitive set
is already every enabled skill, so a subagent-enabled run agrees, and a
named agent whose `AgentConfig.skills` is `None` takes the same branch. It
differs only for an agent carrying an explicit `skills:` list, a bootstrap
run (IM-only; the prewarm hook is browser-only), and a skill toggled between
opening the chat and sending. A view that cannot be published — a read-only
volume, a thread a run already bound — leaves the container parked and warns
with the reason, because the container is the prewarm's deliverable and the
view is a head start.

**The record is dropped, the bytes are not.** The publication is immediately
followed by a compare-and-pop of its own `(run_id, generation)`, leaving the
view in the module's documented retained-but-unowned state. The map's
records are the identities the coordinator issued, and a guess is not one; a
turn's exact compare-and-release only ever matches its own record, and a
turn whose own bind raises empties the view under the coordinator's fence
whatever the map holds (the rule below), so the pop is hygiene rather than
what keeps a full disk from wedging the thread. It can only ever remove the
prewarm's own record: a turn that bound first makes it a no-op.

### A release that cannot match its record empties the view under the coordinator's fence

This was recorded here as a known defect; for the providers whose material
lives on the host it is closed by a rule, not a special case. **What it took.** `_active_view_bindings[view]` held a
*foreign* `(run_id, generation)` — one that was not the identity being
unwound — at the moment a bind raised inside staging (ENOSPC or EIO on the
tenant data disk is the realistic cause): any earlier turn whose clean
release never ran, or a prewarm's guess caught between its publication and
its pop, which the publication running outside the acquire serializer makes
an ordinary interleaving. **What it did.** The exact compare-and-clear could
not match a foreign identity; the fallback refused any view that carried a
record at all; `accepted_projection.py` returned before `finalize_release`,
so `clearing` stayed set with nothing to sweep it. Every later
`reserve_admission`, `try_claim_committed_run`, `claim_committed_run` and
`fence_committed_owner` refused the thread — interrupt and rollback
included — and `provider.release` was never reached, so the sandbox was
never parked and held one of the tenant's two slots. Only a Gateway restart
recovered it, taking every other thread's sandbox with it.

**The rule.** Ownership of a thread's projection is the coordinator's, not
the view map's. The map's records exist for the bind's generation fence and
for the exact compare-and-release; they are never read as ownership at
release. While the coordinator holds a thread as clearing under a
`SkillProjectionClear`, no other run can be admitted to that thread and the
only sandbox that mounts its view is the one the clear names, so whatever
record the map carries at that moment is not a live owner: a coordinator
generation newer than the clearing run's cannot have been issued while it
holds the thread, an older one had to finalize before it could claim, and
generation 0 is a prewarm's guess or the field's default, which no run
executes under. So the recovery half of a release,
`empty_skill_snapshot_active_view(clear)`, empties the view and drops the
record on the coordinator's word — `SkillProjectionCoordinator.is_clearing(clear)`
must hold, checked under the views lock every bind takes, or it empties
nothing — and the thread's next turn stages once.
That fence check, not the record, is what keeps a mistaken caller away from
a view a live container is mounting; the old refusal protected the same
thing by refusing every recovery, the ones nothing needed protecting from
included. An emptied view whose record named another identity is logged at
info with both identities, because a stale record is a symptom worth a line.

**What it does not change.** Generation 0 is still never skipped or
special-cased: a real `AcceptedSkillSandboxBindingV1` can legally carry it,
and the release does not consult the generation at all. A provider whose
material lives inside the sandbox answers for that sandbox, not for a host
view: E2B clears the material in the live sandbox and quarantines the exact
sandbox when it cannot. The AIO remote backend does not clear in place: the accepted
tree is staged read-only by the provisioner, and the backend's own surface is
create, destroy, is_alive, discover, list_running and renew. So it takes the
contract's other answer and destroys the exact sandbox, which makes the
material absent by construction. Three things have to hold before it does:
the sandbox carries accepted isolation, `_identity_for_sandbox` proves it is
this thread's, and the coordinator still holds the thread as clearing under
*this* proof. The last is checked, not assumed, for the same reason
`empty_skill_snapshot_active_view` checks it: the coordinator hands the same
clear to two releasers, and once the first finalizes, the next turn can be
admitted and warm-reuse hands it the same sandbox id. A stale proof would then
pass the identity gate and destroy a container a live run is executing in.
Before the destroy existed that line was a harmless lookup. The prewarm's
compare-and-pop stays, as hygiene.

**What stays open.** Absence here means the provisioner accepted the delete
and the set is not in quarantine -- `RemoteSandboxBackend.destroy` says of
itself that acceptance is not verified absence, since Kubernetes removes pods
asynchronously. A destroy that cannot confirm quarantines the set and raises
`SandboxCleanupIncompleteError`; the branch catches it and answers False,
because two callers of `release_accepted_skill_consumer` catch nothing and the
declared contract is a bool. The refusal cannot be spelled by asking `get`
again: `get` answers None for a quarantined set and for one another reaper has
reserved, so absence is asked of a predicate that excludes both.

A refused release is retried once. `token_for_consumer` returns the retained
clearing token to the same consumer id, and the worker's terminal cleanup
re-obtains it and re-drives the release
(`runtime/runs/worker.py`; `tests/test_skill_projection_binding_recovery.py`
pins that contract). What no caller does is retry *after* that: nothing sweeps
threads left clearing, and five of the seven call sites discard the bool
rather than warn on it (`sandbox/middleware.py` twice, the worker's terminal
path, `subagents/executor.py`, `subagents/batch_service.py`). So a set whose
teardown never confirms still strands its thread. Closing that outright means
something asking again on its own; the coordinator's `clearing` state already
is that fact, so the derived form is a sweep that reads it rather than a
second ledger beside it. That is a change of its own and is not made here.

### Rediscovery is not creation

`create` is an attempt, not a result. `LocalContainerBackend.create` can answer
with a container that already existed under the deterministic name (the
restricted "compatible existing set" path and the open-mode name-conflict
path both discover and return it), and the provisioner's `POST /api/sandboxes`
is idempotent for an existing Pod. Until this repair the provider labelled
every such answer `created`, and that label chose destructive cleanup when the
caller was cancelled and counted a creation that never happened.

The backend now says what it did. `SandboxInfo.provenance` is `created` when
the backend started the resource in that call, `rediscovered` when it returned
one that already existed, and `unknown` when it did not say (a backend that
predates the field, or an older provisioner image: the Gateway maps the
provisioner's `provenance` word and treats its absence as unknown). Unknown is
never treated as proof of creation.

What the accepted path does with it:

| Origin | How it came about | On caller cancellation before hand-off | Reuse fingerprint |
| --- | --- | --- | --- |
| `active` | already tracked for this identity | left to its holders | unchanged |
| `reclaimed` | taken back from the warm pool | parked again | unchanged |
| `created` | backend confirms it started it | destroyed under the fences (the only rollback) | recorded from this request |
| `rediscovered` | create found an existing resource | parked again, owned and reapable | left as recorded, or absent |
| `unknown` | backend did not say | parked again, owned and reapable | not recorded |

A rediscovered or unknown container never has the *request's* fingerprint
written over it: new requested inputs do not prove its mounts or configuration
changed, so the next reclaim compares against what this process actually knows
(its true inputs, or nothing) and, on a mismatch, replaces it rather than
reuse it unverified. Two consequences follow and are deliberate. A
rediscovered accepted-only container is used once with unverified create-time
inputs (only `discover`'s mode, network-policy, sidecar and readiness checks,
as the ordinary path has always done) and is replaced on its next
acquisition, so a Gateway restart that leaves containers running costs each
accepted thread one cold start on its *second* turn. And a backend that never
reports provenance disables warm reuse of the containers it creates: no
fingerprint is ever recorded, so our own parked entry is replaced on every
follow-up turn; the provider warns once, and `unknown_create_results` in the
journal is the per-turn signal. Both in-tree backends report it.

**Version skew.** Until the provisioner image that reports `provenance` is
deployed, every remote create is `unknown`: remote accepted warm reclaim does
not happen (each turn is one `create_attempts`, the idempotent existing Pod
comes back as `UNKNOWN_PROVENANCE`) and a cancelled acquisition parks the Pod
(owned, reaped by the idle checker) instead of rolling it back. This is
Kubernetes/Helm only; the consumed Compose profile runs no provisioner. Roll
the provisioner and Gateway images together, as the chart pins them.

**Replacing our own drifted entry.** When the parked entry under this id is
one *this process* provisioned as accepted-only and its recorded create-time
inputs differ from the request (locally that is the Lark provisioning state),
the container is not silently adopted with the old mounts and no unrelated
container is stopped to reach a create. It is replaced under the ordinary
teardown fences (`_replace_parked_accepted_sandbox`, the same path as replica
eviction: local reservation, cross-instance claim, held lease, entry popped
only after the stop, and identity-fenced first: a parked entry of another
identity under a colliding id raises `SandboxIdentityCollisionError` as every
promote path does). Replacing our own entry frees our own slot, so at
capacity the unrelated thread's container stays parked. A refused replacement
(reclaimed meanwhile, peer-owned, store unavailable, failed stop) fails the
acquisition closed with `accepted_sandbox_inputs_changed`: create would find
the container under its name, take the lease over from the very peer or
reaper whose fence just refused the stop, and hand the turn a container whose
inputs are known to differ. The container stays running under whoever holds
it and the next turn retries once the lease or reservation resolves; a
peer-owned entry stays refused until this process's renewal reports it lost
and drops it, after which it is reached as a foreign rediscovery. On the
remote backend the provisioner validates an existing Pod against the request
itself, so no provider-side replacement is attempted.

**A foreign container at capacity.** A container this process does not track
that create rediscovers is a third tracked set once adopted, so the soft cap
evicts the oldest unrelated warm entry *before* create exactly as for a new
container, and that wait is the `sandbox_eviction` phase. Our own parked
entry never triggers this: it is replaced (its slot freed) or the acquisition
is refused before create is reached. There is no backend probe, no deferred
eviction and no overshoot window.

**What that wait costs, and why it is short.** The eviction is a person's
wait: it runs inside the acquisition, ahead of the turn's first model
request. A tenant-class upgrade measured it at 22.8 s of a cold report turn
that reached the model at 35.7 s. Almost all of it was the container
runtime's SIGTERM grace, paid twice. Neither member of the set honours
SIGTERM -- the sandbox's init is a bash script with no trap, the sidecar a
Python server that installs no handler -- so both have always died by
SIGKILL, and the default ten-second grace only decided how long the person
waited for it. `LocalContainerBackend._STOP_GRACE_SECONDS` asks Docker for one
second instead. Measured on the released images at the profile's limits:
10.94 s + 10.68 s by default, 1.74 s + 1.63 s with the flag, exit 137 in
every case. A second remains long enough for an image that later does handle
the signal, because `stop` returns as soon as the process exits and the grace
is only paid when it is ignored.

What the shorter grace does *not* do is make a teardown safe for something
still writing, because the longer one never did. A sandbox has four writable
host mounts under `/mnt/user-data` -- `workspace`, `uploads`, `outputs`, and
the person's own `files`, which is shared across all of their threads -- and
`destroy` is also the idle reaper's and the shutdown path's, not only
eviction's. But the ten seconds was never a shutdown window: PID 1 ignores
SIGTERM and does not forward it, so nothing inside the container is ever told
to finish, and a process mid-write is killed abruptly at ten seconds exactly
as it is at one. The window was nine extra seconds of unsignalled runtime --
a chance that a write happened to complete, not a guarantee that it could.
Making that teardown genuinely safe needs an image whose init handles the
signal; until then the honest statement is that neither value protects a
writer, and the one that does not also cost somebody their wait is the
shorter. The flag goes to Docker alone; Apple Container's spelling is
unverified here, so that runtime keeps its default.

### Destroy means absent

A stop or remove command that fails can still return normally: the local
backend's `docker stop` swallowed `CalledProcessError`, and its sidecar and
network removals only logged. Until this repair the provider took the return
as a confirmed teardown, forgot the parked entry, counted the set gone and --
on the input-drift path -- went on to create, rediscovered the container it
had just rejected and handed it back active with the old configuration.

`SandboxBackend.destroy` now returns a `DestroyOutcome`, and successful
destruction means the owned set is *confirmed absent*, not that commands were
attempted. `LocalContainerBackend.destroy` runs its commands, then inspects
every member -- the sandbox container, the network sidecar and both networks
(locally that is the counting unit) -- and answers `absent` only when each is
positively not found; `partial` when some remain, `failed` when the set is
intact, `unknown` when a member could not be observed (a daemon that does not
answer, or a timed-out command whose members are not afterwards all found
absent; a sandbox stop timeout still attempts the rest of the set, a sidecar
stop timeout does not). A failed command is never absence, and a not-found
answer is: a genuinely absent set is cleaned idempotently. The remote backend's
DELETE is its only observation: a 2xx is the provisioner accepting the
deletion (Kubernetes removes the Pod and Service asynchronously), answered as
`absent` and trusted as before, not verified. A backend that predates the
contract and returns `None` is trusted as before (normal return means gone, an
exception means not, classified `unknown`); any other non-outcome return is
`unknown`.

What the provider does with anything but `absent`, on every path that
consumes the contract (drifted replacement, replica eviction, idle reaping,
explicit destroy, cancellation rollback, unready rollback, reconciliation):

- The set stays tracked in the warm pool -- counted against the replica
  budget, renewed by the lease thread, ownership re-established at once (a
  lease a peer already holds is not taken back; the set stays tracked and
  pending, retries refuse, and the renewal thread drops the handle on its next
  tick) -- and is marked pending cleanup (`_cleanup_pending`). Quarantine is a
  lifecycle state, not a warm-pool detail: `get` answers `None` for a pending
  id whichever map names it, the in-process reuse and the accepted active
  shortcut refuse it, and the mark is discharged by confirmed absence, by a
  peer's takeover (its responsibility then) or by a registration under the id
  (the create path refuses a reserved id, so a registration means the set
  was confirmed absent first). A mark that no map names any longer has no
  set to retry and is dropped with an error log; no path produces that
  state. It is never handed out: the
  ordinary reclaim raises `SandboxBeingDestroyedError`, the accepted reclaim
  refuses with `accepted_sandbox_cleanup_pending`, and a drifted replacement
  that did not confirm refuses with `accepted_sandbox_inputs_changed` rather
  than proceed to create, adopt the rejected container or evict an unrelated
  one to escape the failure.
- Cleanup is retried under the same fences as every reap -- by the idle
  checker once per pass (the expiry reaper skips pending sets), by eviction (a
  pending set is the first candidate; a healthy unrelated set pays only when
  that retry fails again), and by the next acquisition under its id (a set
  destroyed on the reuse path with a stale identity is retried by the reaper
  and eviction only). A retry never stops a set another instance has since
  taken (the claim refuses), never touches a set reserved by a reaper in this
  process, and is bounded to one attempt per trigger.
- Recovery: once the fault clears, the next trigger finishes the cleanup, the
  set is counted absent exactly once, and the acquisition builds one correctly
  configured replacement.
- Reconciliation and acquisition arbitrate locally as well as through the
  store. The accepted-orphan branch observes an unowned id outside the lock;
  its teardown claim succeeds against this process's own lease, so it cannot
  see an acquisition that registered the id meanwhile. It therefore takes the
  local teardown reservation first (`_destroy_accepted_orphan`, the same
  shape as `_replace_incompatible_sandbox`), whose predicate re-validates in
  one critical section that the id is still untracked -- not active, not
  warm, not starting -- and holds it through the claim, the stop and the
  quarantine. If acquisition won, the obsolete decision is a counted refusal
  and the live holder is untouched. If teardown won, the create path refuses
  the id at `_mark_starting` and registration refuses it before and after
  publishing ownership (`SandboxBeingDestroyedError`), so nothing is handed
  out while the stop or an uncertain cleanup is in progress; the next
  acquisition builds one fresh generation. A container this process is still
  starting is deferred at the loop head before any branch runs (no attempt),
  and a create that marks the id starting after that check is refused by the
  reservation predicate (one counted refusal); the incompatible-policy
  replacement and the ordinary adoption check carry the same `_starting`
  term. The create path marks the id starting before replica enforcement
  and before the attempt is journaled, so a refused create evicts nothing
  and counts nothing. Every warm destroy re-validates the entry's identity
  inside its reservation, so a decision taken about one generation (a
  reaper's snapshot, a pending retry) never lands on a later generation
  parked under the same name.
- The accepted reclaim checks identity before it drives any cleanup: a known
  conflicting identity under the id raises `SandboxIdentityCollisionError`
  with no mutation, pending or not, and the owning identity (or background
  cleanup under its own authority) still finishes the set. An unknown
  identity is a separate state, never read as proof of the requester's.
- The active lookups respect teardown winning first, as the warm and create
  paths do. Idle destroy reserves an active id and claims ownership before it
  untracks (so a refused claim can recover), and in that interval `get` and
  the accepted active shortcut answer `None` / `SandboxBeingDestroyedError`,
  decided in the critical section that reads the reservation, refreshing no
  activity and taking no ownership on the refused lookup's behalf. A `get` or
  an accepted reuse that landed before the reservation counts as activity
  (the shortcut refreshes it in that same critical section) and is honoured
  by the reservation's own still-idle predicate (a counted refusal, nothing
  stopped); a refused claim releases the reservation and the handle answers
  again; a stop that does not confirm leaves the set quarantined as before,
  and the pass that quarantined it does not retry it (one attempt per
  trigger holds on the active-idle path too).
- An explicit `destroy()` raises `SandboxCleanupIncompleteError` after
  quarantining, so shutdown, the idle checker's active-idle path, cancellation
  rollback and the stale-entry destroy on reuse see the failure instead of a
  clean return; cancellation rollback stays one bounded attempt.
- The journal distinguishes `teardown_refusals` (a fence said no) from
  `teardown_failures` (the backend ran and the set is not confirmed absent);
  `resource_teardowns` is incremented once per set, on the retry that
  confirmed it.

Tests: `backend/tests/test_sandbox_cleanup_outcomes.py` composes the real
`LocalContainerBackend.destroy` control flow with a fake daemon whose
inventory is the independent record (each member refused alone, all together,
partial, timeout, daemon unavailable, genuinely absent, success, retry after
the fault clears, same-id replacement at two-slot capacity, a sidecar left
behind, ownership loss during a pending cleanup, reservation races, explicit
destroy, cancellation rollback, A/B/A untouched);
`test_aio_sandbox_local_backend.py` pins the outcome classification at the
backend seam. The reconciliation/acquisition boundary is pinned in the same
file with deterministic scheduling (the acquisition runs between the real
orphan observation and the teardown claim, between the reservation and the
claim, and during the stop) on the memory ownership store with its
cross-process flag raised solely so the real owner/grace decision runs; that
is a single-process model, not a Redis or multi-instance qualification.

**Counting.** The journal (`deerflow.runtime.turn_phases`) counts resource
*sets* -- container plus network sidecar and networks locally, Pod plus
Service remotely -- and distinguishes `create_attempts` from confirmed
`resource_creates`, `resource_rediscoveries` and `unknown_create_results`,
and `teardown_attempts` from confirmed `resource_teardowns`, ownership-fenced
`teardown_refusals` and `teardown_failures` (the backend ran and the set is not
confirmed absent). A teardown is confirmed only when the backend's destroy
returned `absent`; neither a refusal nor a failure is reported as a
disappearance.
The tests reconcile every journal value against the fake backend's own call
record. Tests: `backend/tests/test_sandbox_rediscovery_provenance.py`,
`test_sandbox_warm_reuse_latency.py`, `test_aio_sandbox_local_backend.py`
(provenance through the real local control flow),
`test_remote_sandbox_backend.py` and `test_provisioner_runtime_hardening.py`.

**Attributing the accepted preparation.** Tenant-class `.17` measured 5 to 6 s
before the first model request on a *warm* turn -- most of the budget for a
one-sentence revision -- inside a `skill_materialization` phase that reported
one figure and named nothing in it. Four spans nest inside it now:
`accepted_authorization` (authorizing, resolving the provider and choosing the
materializer -- on a durable profile that selection asks the sandbox backend
for its pinned runtime digest, so it is not local),
`accepted_material_verify` (durable profiles only: two re-digests of the
published snapshot and one file-manifest walk, three passes over the same
tree), `skill_projection` (the provider putting the material in a sandbox;
`sandbox_lookup`, and on a cold turn `sandbox_create` and `sandbox_readiness`,
nest inside it) and `skill_snapshot_bind`. On the released projection profile
what is left over is the binding lookup and the isolation assertions; a durable
profile also leaves `validate_accepted_materialization` and two execution-fence
round trips there.

Read `skill_snapshot_bind` knowing what it measures. On the released
local-Docker profile a provider that binds while it provisions -- the AIO
backend does -- has already published the snapshot inside `skill_projection`
(which also holds the sandbox lookup), so the worker's later bind and the
sandbox middleware's `sandbox_binding` are the second and third binds of one
identity before the first model request; each sandbox tool call binds again.
Until 2026-09-17 each bind captured the source tree three times and wrote a
full fsync'd staged copy *before* comparing identities: tenant-class `.18`
measured 1.2 to 2.1 s in `skill_snapshot_bind` and 1.3 to 1.9 s in
`sandbox_binding` for the tenant's seeded library (13 packages, 43 files,
413 kB); on a slow development disk the development checkout's whole
`skills/public` (24 packages, 112 files, 815 kB) bound in 5.5 to 9.3 s per
call, against a capture of about 20 ms and a digest of about 19 ms -- the
copy's per-file `fsync` was the whole cost (no-op'ing it leaves 95 to
140 ms). `bind_skill_snapshot_active_view` now verifies the view it already
holds before it captures anything: exactly one entry named for the snapshot;
no symlink, special file, empty directory or unlistable directory beneath it;
every directory exactly `0o500` and every file exactly `0o400` or `0o500`,
the modes it was published with (a setuid or world-readable variant is a
change); and the bytes re-digested against the evidence's snapshot id,
content digest, file count and byte count, with the regular-file count over
the whole tree equal to the evidence's. A tree that passes is adopted under
the new `(run_id, generation)` identity -- the generation rule is unchanged
and is checked first, so an identity can still refuse a bind; it never earns
one -- and a tree that fails for any reason, a tampered byte, a changed mode,
an extra or hidden file, an empty directory, a second entry or a different
snapshot, is re-staged from the source and replaced. The fast path is tried
before the source snapshot is consulted, so a verifiable view is bound even
when the lease behind it has gone. An exact-identity repeat is verified the
same way rather than returned unread, which it used to be. Measured on the
development checkout: the first bind of a turn still publishes at 6.5 to
8.6 s; the second reads a just-written tree back through a cold cache at
roughly 40 to 90 ms; the third and every repeat after it about 25 ms.
`test_accepted_skill_snapshots.py` pins each failure mode, the source-loss
case, the bind-level refusal of foreign evidence, and that the fast path
stages nothing; the span figures a live trace reports are the measurement.

**Material is retained across turns.** The fast path above was only ever
taken *within* a turn until 2026-09-17, because two run-end cleanups removed
what it would have verified: the run's last lease deleted the snapshot's
published digest from the subject scope, and the last consumer's release
cleared the thread view. Tenant-class `.19` therefore paid both copies again
on every warm turn — the launch re-staged the digest before the run row
existed (1.4 to 2.2 s there; 2.0 to 2.4 s and 45 `fsync`s on a development
host, for the seeded 13 packages: 43 files, 415,749 bytes) and the provider
re-staged the view inside `skill_projection` (1.2 to 1.8 s there; 3.2 s and
43 `fsync`s on that host), for material that verifies in 23 ms and binds in
13 with no `fsync` at all. The tenant-class figures are that class's own turn
lines; the repair has not been measured there.
Both are content-addressed, read-only and re-digested against server-owned
evidence before any use, so both are now kept: a scope retains its newest
two unleased digests (`MAX_RETAINED_SNAPSHOTS_PER_SCOPE`; publishing a third
removes the oldest, and a retained tree that fails verification at the next
admission is removed and staged again rather than trusted, while one that
fails under a live lease is still `skill_snapshot_drift`), and the thread
view's bytes outlive the run that bound them (`clear_skill_snapshot_active_view`
is compare-and-release: the exact `(run_id, generation)` fence still decides
it, the binding entry goes, the bytes stay). What removes them: a bind of a
different digest (under the views lock, as before); the container going away,
which is what bounds a view — `destroy`, an idle reap, a replica eviction and
shutdown all clear it (`force_clear_skill_snapshot_active_view`), and a
teardown that fails leaves the view alone because the container may still
have it mounted; a live verification that found drift (the tree leaves with
its last lease, or at once if it has none, and the path is forgotten so the
next clean publication of that digest is kept); the recovery half of a
release, which empties the view under the coordinator's fence whatever
record it carries (`empty_skill_snapshot_active_view`); and Gateway startup
(`cleanup_abandoned_skill_snapshots` removes every digest and view a prior
process left, so the first turn after a restart stages once per user). No
consumer executes between runs; nothing here authorizes a reuse, only the
bytes do, at bind. The bound is two *unleased* trees per user — a digest a
run holds is never evicted, and the bound is re-applied when that user next
publishes — plus one view per parked thread. Nothing expires on a timer, so
a tenant holds `2 × snapshot × users admitted since the last Gateway start`
until a restart reclaims it. The remote `rwx_verified_copy_v2` branch is unchanged.

**Model-to-stream timing, end to end.**
`backend/tests/test_turn_phase_gateway_stream_e2e.py` runs the real Gateway
under uvicorn on loopback, registers, creates a thread and drives the
authenticated `runs/stream` route with a deterministic streaming model loaded
through the ordinary `models[].use` path, so callback invocation and event
publication come from real execution. It asserts that the HTTP client sees the
first answer text well before completion (a buffered body would collapse the
timestamps), that hidden reasoning is not counted as text, that silent and
cancelled turns manufacture no text timestamps, and that the journal orders
model request, first provider text, first outgoing text, completion and
terminal on one clock with the injected delays between them. Its last test
drives the released `docker/nginx/nginx.conf` in `nginx:alpine` published on
`127.0.0.1` only, with a relay container aliased `gateway` forwarding to the
same Gateway on the Docker bridge's host address, and skips (saying so) without
Docker. Browser first paint, cross-process delivery and post-terminal replay
remain unobserved.

What the fingerprint can and cannot fence depends on the backend. On the
remote backend it is a real fence: the binding identity, execution claim and
egress allowance digest are baked into the Pod at creation, a mismatch falls
through to create, and the Pod that is built is a different one. On the local
container backend, for the same deterministic id, tenant and thread are equal
by construction, the skills root and projection state are already part of the
id itself, the backend class and skills root are startup constants, the thread
and active-view mounts are path-deterministic, and the bound snapshot is
re-verified on every acquisition (staged again only when that verification
fails) -- so the only create-time input that can differ is the Lark CLI provisioning state. A mismatch there is repaired by the
fenced replacement of our own parked entry described above, or refuses the
acquisition when the fences refuse the stop; the container is never adopted
with its old mounts. The label of every create result is the backend's own
provenance, pinned by
`test_local_backend_same_id_input_drift_replaces_our_own_parked_container`.

Cancellation follows the same rule as everything above: the async entry
records how the id was obtained and undoes only what this call did. A
container the backend confirms it started is destroyed; a reclaimed,
rediscovered or unknown-provenance one is parked again under the same
identity, because the caller never received it and nothing else would ever
release it; an already-active one is left to the holders that made it active.

The retire terminal is untouched. A declared accepted session never reaches
the warm pool, and reconciliation destroys a cross-process accepted orphan
rather than adopting it, so every accepted entry the reclaim can find was
parked by this process, for this identity, under this Kind.

### Execution leases beside sessions

Upstream's execution leases (`sandbox/lease.py`) sit beside the session
provider, not inside it. The lease manager only ever calls the provider's
`acquire`, `get`, and `release`, and HartMesh keys every manager by the
installed session provider (`lifecycle_sandbox_provider`), so a caller holding
the backing provider and a caller holding the wrapper share one manager whose
calls go through the declaration dispatch above. A declared execution never
takes a lease: its tools and middleware resolve the declared handle before any
lease code runs, and the declarer owns the terminal. Accepted-skill sandboxes
(the projection Material, not an accepted session) are held under the
execution lease as borrowers, because the projection's consumer refcount is
what parks them.

### Egress, per Kind

An ordinary interactive session is asked (the Human Input card) and both the
blocked request and the applied decision are recorded; a subagent or
non-interactive execution is denied unasked and recorded once. A grant made
that way lives in the container's sidecar, so it can never be the accepted
Kind's egress: the accepted session is held to a run, not to a container.

The accepted Kind declares its egress at admission instead, the way it
declares its execution budget (`sandbox/egress.py`, `EgressAllowanceV1`). The
operator's `execution_policy.accepted_egress` is the ceiling (public CIDR
rules with protocol and optional port, plus whether cluster DNS is allowed;
the default allows nothing); a caller may only narrow it through
`context.egress_allowance`; the canonical allowance and its digest are part of
the accepted invocation's runtime identity and of the V2 material request. The
Material renders it: the provisioner writes it into the accepted Pod's
NetworkPolicy (private, loopback, link-local, carrier-NAT, multicast,
documentation, and cloud-metadata ranges are carved out of any wide rule and
can never be allowed), binds its digest to the attempt Lease, and echoes the
digest; the remote backend destroys a Pod whose attestation is missing or
differs. Recovery re-provisions the same allowance because it is read from the
accepted row, and nothing mid-run can widen it. A request sealed before
allowances existed renders as deny-all, never as the cluster default. The
session records `egress.bound` (profile, rule count, DNS) once on its
diagnostic stream; a sidecar denial on an accepted session is still recorded,
never asked. Recording never changes the tool result the receipt layer
digests: the card is emitted after the fact is recorded and the sandbox
middleware stays inner of the receipt middleware.

## Operations and the facade

An accepted session exposes operations, never its backing provider sandbox.
Every public `Sandbox` operation crosses the same facade. The set is declared
once in `sandbox/operations.py` (ten today: command execution, scoped command
execution, scope release, full/ranged read, download, directory list, text
write, glob, grep, and binary update); the facade's methods are generated from
those declarations, and the module refuses to import if a `Sandbox` method has
no declaration or the facade would inherit one as an unfenced passthrough.
Upstream's scoped-shell hooks are declared already; providers keep the base
class's pass-through defaults until they implement scoping, so a scoped call on
the facade is fenced and recorded even where it is not yet isolated. A provider
reporting persistent shells as a class constant cannot upgrade an acceptance
check: the tests-passed degradation stays until the provider proves per-scope
shell freshness.

## Provider capabilities

The required provider surface is `acquire`, its async twin, `get`, and
`release`. Everything accepted execution needs beyond that is an optional
contract in `sandbox/capabilities.py`, offered through
`SandboxProvider.capability(protocol)` and discovered with `sandbox_capability`:

| Capability | Carries | Offered by |
| --- | --- | --- |
| `AcceptedSkillProjection` | `provision_accepted_skills` (the one provisioning verb), snapshot bind, isolation and immutability proof, exact compare-and-clear, native attempt evidence/validate/renew | Local host, AIO, E2B |
| `AcceptedMaterialization` | `accepted_materializer_selection`, the qualified provider-neutral adapter | Remote AIO `rwx_verified_copy_v2` |

A provider offers a contract by inheriting it, in which case negotiation
answers the provider itself, or by answering a companion object that inherits
it. Every member fails closed until implemented, so a partial provider refuses
accepted material with `accepted_skill_snapshot_projection_unsupported` rather
than executing it against live skill roots. OpenSandbox, BoxLite, and Tenki
offer neither. The session provider answers itself for contracts its backing
provider implements, so the raw provider object never leaves negotiation.

The accepted-skills projection Material (`sandbox/accepted_projection.py`)
composes the capability with the consumer-token coordinator: provisioning
precedes binding, the run's token is activated after provisioning, the
coordinator-issued snapshot is bound, and a failure unwinds the token and, when
no token ever owned the sandbox, the sandbox itself. The coordinator stays a
second refcount inside the Material because its membership (every lead and
child consumer of one projection) differs from the execution lease's; the lease
only ever borrows an accepted-skill sandbox.

## The accepted Kind

### Authority

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

A batch child declares a separate session over its existing SQL item-attempt
fence; it does not borrow the parent run's mutable authority. The child's
canonical request/evidence pair and initial `acquired` observation are
atomically attached to that attempt after the provider call and a fresh fence
sample, before the executor may start. Later bounded lifecycle observations are
appended through the retained attempt/evidence/worker binding even after
terminal publication; they cannot authorize another operation.

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

This is a check-then-call guarantee, not distributed atomicity. A call accepted
before loss may finish. For a provider without atomic operation fencing, one call
may also enter the provider when takeover/loss occurs after both checks but before
provider acceptance. Once loss is observed, every later call is refused, and the
stale worker cannot publish accepted terminal success. A provider may set
`atomic_provider_operation_fencing=true` only when the expected epoch travels in
the operation request and is checked atomically with starting that operation.

### V1 and V2 persistence boundary

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

### Provider capability and qualification matrix

Capability is an adapter declaration. Qualification is current, exact external
evidence for a configured deployment. Both are required for durable admission.

| Provider/profile | Ordinary use | Immutable accepted material | Ownership / shared expiry | Atomic operation fence | Resolved image and restricted isolation | Protected lookup after process loss | Durable one replica | Exact two |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Remote AIO/Kubernetes `rwx_verified_copy_v2` | Yes | Declared and verified per attempt | Declared atomic ownership and authoritative shared expiry | **No**; baseline check-then-call only | Declared; each attempt binds observed digests | **No** | Eligible only with a current pinned passing artifact | **No** |
| Local/container AIO | Yes | The accepted-skills projection only ("Which population a deployment profile runs"); no durable V2 selection | No qualified durable profile | No | No live durable qualification | No | No | No |
| Local host, E2B, BoxLite, Tenki | Yes under their ordinary contracts | No durable accepted profile | Not claimed for this contract | No | Not qualified for this contract | Not claimed | No | No |
| OpenSandbox 0.1.14 / SDK 0.1.15 | Yes | `empty_only`; nonempty durable paths rejected | Required ownership CAS is absent | No | Resolved-image readback is absent | No | No | No |
| In-memory accepted adapter | Tests only | Contract fixture | Test state only | No production claim | No production claim | No | No production claim | No |

Remote AIO's capability profile deliberately records
`atomic_provider_operation_fencing=false`, `recoverable_resource_lookup=false`,
and `exact_two=false`. The repository does not ship a passing artifact. Missing
cluster infrastructure or an unrun lane is an unpassed gate, never a skip that
enables production.

### Qualification and configuration

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

### Exact-two boundary

`durable_two_gateway_v1` admits only the AIO/Kubernetes provider with shared
tenant-prefixed Redis ownership, existing RWX home/skills claims, projected
ServiceAccount authentication, and `rwx_verified_copy_v2`. Execution takeover
is currently unavailable for all exact-two orphans. The dormant AIO recovery
seam keeps the immutable accepted resource tuple separate from mutable
execution authority: the capability Secret stays in the material receipt and a
second, non-evidence execution-claim Secret (Lease-anchored name/UID, credential
rotated under the tenant/run/owner/state/material CAS) is projected per
exact-two run. It is not recovery authority. Projected Secret rotation and
Redis adoption are not linearizable per-request execution revocation, so the
Gateway rejects every takeover claim before owner CAS. Never put renewable
timestamps, current Gateway owners, or rotating claim credentials into
immutable evidence. Future activation requires a database-authoritative request
gate, owner-fenced destruction, recoverable protected resource lookup, live
cross-worker atomic operation-fencing evidence, and fresh qualification.
Process-local warm pools are caches only. OpenSandbox and every other
materialization profile are excluded from this scope.

### Lifecycle: the closed authority set

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
scope, time, reason code, and evidence digest, never the raw resource reference.

### Diagnostics: the bounded stream for both Kinds

Those five states are the closed, authority-relevant set. Everything else worth
knowing about a sandbox session is a diagnostic (`sandbox/diagnostics.py`):
`sandbox.diagnostic.v1` events whose kind is open but namespaced
(`egress.blocked`, `egress.bound`, `egress.decided`, `egress.denied`,
`scope.opened`, `scope.released`, `session.refused`) and whose facts are a bounded mapping of
scalars. A fact observed outside the run, such as an upload refused because
the run holds the thread, reaches the run through the declaration's Observer
(`SandboxSessionRegistry.observe`), which the accepted bridge answers with the
same run-bound anchors. Both
session Kinds record into one per-run stream of 64 entries that drops oldest
rather than refusing a write; each published event carries its sequence and
the drop count, so a quiet run and a truncated one look different. Ordinary
sessions record thread-scoped facts under the provider's own sandbox id;
accepted sessions record run-bound facts under the public ref, the attempt,
and the execution evidence digest, and never the container id. The worker
publishes the stream at terminal cleanup and then forgets it. Diagnostics may
be incomplete and never become authority.

### Recovery outcomes

Recovery follows the existing authorities:

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

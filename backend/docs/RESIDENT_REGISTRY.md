# Resident registry and Gateway ownership

`deerflow.agent_instances.residents.ResidentRegistry` provides generic workspace
capacity and lifecycle bookkeeping. Managed Gateway startup installs it after
capturing the closed host capability and binding the exact provider/attachments.
With `AgentInstances.residents=None`, instance preparation and completion keep
their existing path.
The installation includes durable restoration, exact preparation correlation,
Work admission/replay, completion, maintenance and shutdown. Compose defaults
permit one AI assistant per person and ten company AI employees, with managed
storage enabled even when its seven planning keys are absent.
There is no database schema change in this integration.

Startup recovery freezes the retained original workspace graphs and rotates at
most four per pass under one absolute deadline. Only an exact supported AI agent
Home with missing or unchanged fenced attachment metadata is eligible. Home
exclusion, provider reservations and sorted backing locks cover fresh identity,
root, local activity and borrowed complete durable-run checks. Pending creates,
foreign inflight runs and ambiguous identities preserve the original evidence.
The original graph survives the eligibility commit. A second validation-only
parent transaction rechecks eligibility and fresh absence before durable journal
completion. Logs report host cleanup without inventing a human closure event.
Normal fenced retries retain their observation-only authority. Capacity opens
only after a later fresh complete native census; unresolved resources stay charged.

The additive foundation provides `SpaceAttachments.inspect_preparation` and
Work readiness/host-settlement APIs. Inspection
returns immutable SQL facts for the exact incarnation, actor, host and complete
mount scope. Missing intent differs from a fenced historical row; mismatched or
malformed facts and unavailable metadata raise errors. This read resolves no
current grants and performs no physical inspection or effects. Revocation must
not prevent cleanup correlation. It supplies no containment or absence authority.

Work readiness requires the current admitted Home generation, exact qualified
host and valid immutable container identity; it records no new Work revision.
Host settlement instead permits historical mount generations and recorded host
replacement, preserving human closure attribution. It locks Home, instance and
Work, then reloads the attempt before deciding. A supplied live-run snapshot is a
caller precondition; Gateway also supplies a memory-only live-attempt guard after
the Work lock and preserves current durable foreign owners. Settlement never promotes a candidate
or releases a preparation claim. It commits failed-attempt status and its single
host event before logging. `finish()` returns the resulting status. With a registry,
a lifecycle-stale attempt without a workspace remains unresolved for qualified
host settlement; an assignment-only change keeps cancellation.

## Optional preparation and worker hooks

`prepare_environment` passes through the existing path unless a registry is
explicitly supplied by the owning host. It captures configuration and public
skills and freshly qualifies the exact provider/attachments before taking the
Home lock. Under that lock, it rechecks authority and run admission, drains
resume verification, and admits capacity before creating a new attachment.
Authority is checked again after capacity waiting. Resume recovery uses a scoped
`unfenced` read requiring the exact actor, host and complete mount view; an
unrelated or ambiguous row cannot authorize adoption or containment.

An operator can close a workspace from a separate process while the Gateway
retains its old association. Before admission, preparation observes that exact
fenced attachment and lets canonical host containment finish the original native
journal and notify the registry with the stored closure reason. Its admission
callback borrows the parent transaction and rechecks the actor, incarnation,
host, container, fenced phase and complete mount scope. A missing resume alone
proves no closure. Unknown outcomes keep the old charge and refuse replacement;
closed runtime admission remains closed. The check also covers closure racing
with resume, and cancellation drains confirmation before returning.

A newly invoked start is recovered only by its exact preparation incarnation.
Unreadable or mismatched SQL retains the opaque claim and slot. Missing intent
releases the claim only after the operation drains and the qualified host is
freshly confirmed. A matching durable row transfers to its real attachment ID
even when it is unusable. Readiness and public package copying run outside the
Home lock. The owned preparation publishes the client into its trusted run
environment before cancellation can discard its return value.

The worker records Work readiness before any model/tool dispatch. Authority
middleware checks the run's captured attachment on the original authority loop,
including child loops and synchronous workers; a replacement is never substituted.
Error frames preserve refusal status, reason and cause, or the captured
attachment's historical closure reason. Diagnostics failures retain the original
error, and cancellation follows ordinary worker cancellation handling.

Registry-aware completion drains SDK closure, durable status lookup, Work
settlement and the mandatory `run_ended` notification as one owned operation.
Failures cannot skip the later mandatory hooks or replace the original worker
outcome. Fenced workspaces and uncertain cleanup cannot promote Work candidates.
These hooks do not install a registry themselves; the Gateway owns their coordinated
restoration, admission/replay, reaping and shutdown.

## Durable restoration

`SpaceAttachments.retained_claims()` supplies a complete metadata snapshot of
every non-fenced attachment, bounded globally at 4,096 rows. Overflow, missing
mounts, invalid identity/closure facts or SQL failure raises instead of returning
an incomplete successful snapshot. Each frozen claim carries the real attachment
ID, incarnation, actor, host, full typed mount scope, phase and optional immutable
container ID. It performs no grants, filesystem or provider operations.

The registry retains each durable attachment separately, including multiple
attachments on the same Home, unknown owners and multi-Space environments.
Accounting grants no workspace permission. A multi-Space attachment consumes one
environment; a Home reservation remains an additional charge until exact transfer.
`preparation_retained` transfers only its captured preparation token and matching
durable facts under the provider lock, with no count gap or temporary double count.
Pending transferred attachments still refuse admission.

Register `begin_restore()` before SQL reads. `finish_restore()` merges bounded
concurrent creation/phase updates and containment tombstones; stale snapshots
cannot resurrect contained claims or overwrite newer immutable identities.
Snapshot absence never proves physical containment. A malformed, conflicting or
overflowed snapshot keeps old accounting and leaves restoration incomplete.
Legacy creation without exact typed update facts during the scan invalidates its
publication. Coordinated installation must fence that creation window.

Untouched uninstalled registries preserve their existing optional behavior while
`restoration_complete` remains false. Once restoration begins, incomplete or failed
restoration blocks admission and capacity decisions before same-Home shortcuts.
Live identity collisions and bounded-index overflow also close readiness without
discarding known claims. Only an exact containment notification releases its
durable charge; later Work settlement failure cannot reverse that release.

An eligible Home association requires its lock and one exact claim for the
current actor, host, generation and complete Home mount. It preserves the capacity
charge atomically. It grants no physical liveness or execution proof; ordinary
resume and authority checks remain required. Gateway coordinates this restoration
without a database schema change.

## Owned startup coordinator

`ResidentBootstrapCoordinator` owns one exact registry, provider, attachment service
and lifecycle capability on the registry's loop. Its default wait uses the original
startup entry time plus 120 seconds. Timeout and caller cancellation detach only
the waiter; the same retained initializer and its result survive for the host's
later maintenance pass. Explicit retry waits have their own bounded timeout.
Successful repeated calls return the consumed result, without another restoration.
An explicit invalidation must first consume any owned initializer result.

Restoration begins before producer drainage and the complete SQL snapshot. Drain
already submitted producers before reading metadata, so their late commits appear
in that snapshot. Failed reads preserve previous claims and close readiness.
SQL alone never certifies the full sandbox budget. The host supplies a typed `NativeAccountingProof`
for its complete census, with a memory-only validator of the retained census and
drained producer identities. Capture performs no census by itself. The proof pins
the exact provider, closed-lifecycle permit, pool owner, native bridge, evidence
object and teardown revision; boolean success cannot replace it.

Final publication rechecks the original lifecycle revision and all runtime bindings
under one provider critical section, validates the retained proof and resident count,
then installs both callbacks and opens admission together. Registry restoration
methods acquire that lock independently and must not be nested inside it. Workspace
qualification is a separate result: complete accounting can permit ordinary mountless
chats while workspace qualification is unavailable. Failed publication retains all
charges and existing callbacks.

Close seals the exact captured capability before any await. One shared drain task
joins mandatory worker completion, initialization/reconciliation and registry effects,
even if an earlier shutdown hook fails. Concurrent or cancelled close callers retain
that same task. Callbacks remain installed during drainage. The installing host must
stop its separately owned maintenance/reaper producers and close provider, storage,
SQL and pool owner only after this boundary. Reconciliation must retain all durable
inflight rows and never infer foreign lease takeover from census membership.

`app.gateway.resident_runtime` supplies the complete collector and actual producer
drain. It reads every durable inflight Run, including unexpired leases; failed
inventory never becomes an empty success. Bounded all-state census includes native
chat and resident primaries plus relay/network resources independently of name
prefix. Exact SQL CID/host/full mounts cover residents; an unexplained primary or
secondary refuses publication. Preparations without an original CID remain charged
and cannot be contained merely from an empty listing. Strict durable actorless
resident orphan cleanup remains a separate follow-up before count activation.

Workspace readiness is current host state, separate from the cached initial
capacity result. Maintenance retries qualification on the same registry. Failed
inventory closes admission before requalification; an unfinished initializer must
be consumed before invalidation. Cached attachments/adapters never change on retry.
Reconciliation waits for a CID-less preparation under its Home lock and inherited
deadline, then re-reads exact durable correlation. Only pending-to-active completion
with its preparation cleared can supply the original CID; normal owner, run and
fresh graph checks still precede publication. An unknown start, changed scope or
expired deadline retains accounting and refuses readiness.
The startup recovered-run snapshot applies only to its original attachment IDs,
so later maintenance cannot close a newer workspace from historical recovery.

Work activation first performs a fully authorized replay-only durable preflight.
A new attempt reserves physical capacity outside SQL and binds its exact promise
before durable writes. A capacity refusal rechecks exact replay, and a concurrent
replay cancels only the caller's unused promise. Runless replay recovery holds the
Home lock through capacity awaiting, releasing it before run launch takes that lock.
Live launch owners prevent reservation expiry before commit/attachment. Confirmed
rollback may release; unknown commits remain charged until exact host settlement.

Gateway registers owned launch tasks before their first await. Shutdown seals
registration and drains those admissions before RunManager snapshots/cancels its
workers, then retains mandatory completion before provider/SQL closure. Periodic
orphan callbacks correlate exact Work/run facts and request a deferred recycle;
the existing maintenance task owns physical effects and settlement.
Storage maintenance drains through one shared task without withdrawing callbacks.
Resident and chat-host/provider drainage follows; final storage cleanup withdraws
the callbacks afterward, including when provider shutdown fails.

## Shared capacity

The existing AIO provider adds `external_slots()` exactly once to its reserved
count, alongside native starting, pending handoff and teardown reservations.
Callbacks default to zero slots and no external eviction. Ordinary admission
tries an idle chat first, then asks the host to contain an idle workspace.
Prewarm never evicts a workspace. External admission uses the same budget;
`wait=False` permits eviction but declines the bounded capacity wait.

`admit_external_async` checks an optional exact-Home `is_reserved` predicate,
capacity and the reservation callback under the provider metadata lock. Both
callbacks must be synchronous, memory-only and non-reentrant. Recheck them after
eviction and before a second lifecycle effect, including cancellation and shutdown.
Drain started async eviction before propagating cancellation. `external_changed`
runs one memory change under the lock and wakes waiters; becoming evictable is
also a wake-up event, even when no slot has been released.

## Owning-loop bookkeeping

All registry decisions, task snapshots, reservations, Home locks and containment
run on the creating event loop. Provider workers schedule the entire eviction
coroutine onto that loop; they never select a Home or inspect run state themselves.
Calling the eviction callback on that loop's thread refuses immediately.
Selection takes the oldest eligible resident and rechecks the exact attachment,
live runs, reservations and preparation after obtaining its Home lock.

The registry counts live local worker tasks, including staged terminal results
still finalizing. `ended` masks a worker only after its completion notification;
it is pruned from current task snapshots. Work reservations bridge activation to
that existing task state. Binding, cancellation and expiry use exact reservation
identity; expired unowned admission cannot publish a late slot. Bound attempts and
live launch promises do not expire before handoff or confirmed settlement. Workspace concurrency is
two runs; deployment Work capacity is `max(1, replicas - 1)`.

## Uncertain preparation and containment

Before awaiting capacity or attachment creation, a host caller holding the Home
lock creates one opaque `PreparationClaim` with its incarnation and expected
Home, generation and host. Reservations can expire while that claim exists;
the workspace's slot remains retained. A late completion must match the exact
claim object. Successful attachment publication transfers the claim to the real
attachment ID atomically. Never fabricate an attachment or container ID.

`preparation_absent` is a trusted bookkeeping seam, not physical proof. Its caller
must hold the Home lock and prove a pre-invocation failure, or drained preparation
plus fresh qualified absence/containment matching the exact durable incarnation
and mount scope. Failed or ambiguous inspection keeps the claim. The coordinated
integration must supply that inspection seam and restore uncertain claims before
installing capacity callbacks. An empty Docker census is insufficient.

Containment callbacks may run while the caller holds the Home lock, so they never
acquire it again. They clear only the matching attachment and leave preparation
claims and replacements intact. Recycling waits for other runs and reservations;
uncertain outcomes allow a five-minute grace before refusing new starts. Reaping
rechecks exact identity and eligibility after each wait. Uncorrelated retained
containment intents stay in `attachments.uncontained`; integration must restore
their exact Home scope before retrying them under the correct locks.

An eviction timeout grants no capacity credit and does not cancel physical work.
The scheduled task stays owned until it drains. `close()` fences outstanding slot
admission and drains owned eviction tasks. Completion drains across cancellation
and applies final eligible-slot bookkeeping even if settlement fails. Neither
SDK closure, worker status nor reservation expiry proves physical containment.

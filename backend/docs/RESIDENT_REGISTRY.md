# Resident registry foundation

`deerflow.agent_instances.residents.ResidentRegistry` provides generic workspace
capacity and lifecycle bookkeeping. It is not installed by Gateway startup yet.
The current instance preparation path and storage/agent defaults remain unchanged.
Gateway installation must include durable restart restoration, exact preparation
correlation, Work admission and replay, worker completion, reaping and shutdown.
There is no database schema change in this foundation.

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
identity; expired admission cannot publish a late slot. Workspace concurrency is
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

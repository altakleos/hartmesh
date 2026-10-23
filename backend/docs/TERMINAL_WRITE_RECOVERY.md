# Terminal write failure recovery

The SQL-backed runtime confirms a terminal run transition through
`RunRepository.finalize_owned`. This applies with run ownership heartbeats on
or off. The shipped Compose profile enables heartbeats so it can receive
account-disable cancellation requests from another process; keep that setting.
No database schema change is required.

## Failure classification and recovery

Terminal persistence logs the run ID, attempted status, stable
`terminal_write_failed` reason, attempt number and a fixed exception
classification. It never logs exception text, tracebacks, SQL, parameters,
messages or credentials. Progress and completion metadata failures use the same
fixed classifications with an operation name and `run_metadata_write_failed`.
Terminal outcomes distinguish:

- `validation_refused`: the payload was refused before opening a transaction.
- `rolled_back`: a pre-commit failure was followed by a confirmed rollback.
- `unknown`: commit was attempted, rollback could not be confirmed, or the
  store/read transport failed without a stronger phase guarantee.
- `stale_owner`, `stale_version`, `lease_expired` or `conditional_rejection`:
  the observed ownership/fence did not authorize the transition.

After a failure, recovery reads durable state before attempting another write.
An already committed terminal outcome is adopted without replaying the failed
transaction. If the original active row is still present, a separate recovery
transition preserves the worker's actual executed outcome. Only invalid metadata
shapes are sanitized: an unrepresentable reason becomes `terminal_metadata_recovered`
and an invalid error field becomes a fixed explanation (or no error for success).
That transition is conditional on the original observed `change_seq`, owning
worker, source status, cancellation action and current lease. An uncertain
original commit that finishes later must win this same row fence or lose to
the recovery transition; it cannot overwrite a newer row. Active version or
owner changes fence the old worker. A missing row is never recreated.
An already committed terminal row remains confirmable after its old lease expires;
lease expiry prevents active writes, not observation of a completed result.

A durable cancellation discovered during completion/recovery returns to the
worker's ordinary interrupt or checkpoint-rollback handling before terminal
settlement. It cannot be overwritten by recovery success. Recovery itself never
mutates checkpoints. Delivery receipt and checkpoint ordering remain in the worker.
For a live SQL-backed worker, the cancellation caller records the durable request
and signals execution; the worker alone persists its terminal outcome after
rollback/checkpoint/receipt drainage. The caller does not terminalize the active
row or overwrite a staged rollback result with an interrupted acknowledgement.
Completion metadata for this path
requires an already confirmed terminal row with the matching owner and status;
it cannot recreate the run or change its terminal state.

Recovery makes at most three attempts. Each read/write attempt has a five-second
timeout, with 100 ms between attempts: a nominal 15.2-second recovery window.
Cancellation drains the database session's rollback/close before releasing it;
driver cleanup can add time, and no transaction is detached to meet a deadline.
Staged finalization continues renewing its lease and participates in bounded
Gateway shutdown drainage until its terminal outcome is confirmed.
Recovery is independent of peer heartbeats. A returning database inside this
window permits settlement in the same Gateway process.

## Streams and operator action

A recovered result reports the confirmed execution outcome. If persistence
cannot be confirmed, the worker fences further durable finalization. Exhausted
persistence recovery emits an SSE `error` with reason `run_state_unconfirmed`
before the stream END marker while its last confirmed lease remains valid
(or heartbeats are off). A stale owner/version/lease, missing row, or generic
ownership-loss fence emits no new failure narrative onto a peer-owned stream.
Local error status does not
prove a durable terminal row. The active database row continues blocking a new
turn in that chat until recovery confirms settlement.

If the database remains unavailable beyond automatic recovery:

1. Restore database connectivity or repair the diagnosed persistence failure.
2. Restart the affected Gateway through the deployment's ordinary procedure.
   Confirm the old Gateway has stopped; never run a second heartbeat-off worker
   against the same database.
3. Check durable run status. Startup orphan recovery settles abandoned NULL-lease
   rows immediately in heartbeat-off mode. With heartbeats enabled, a live lease
   remains protected until expiry plus grace; periodic recovery then settles it.
   Its sweep runs every three heartbeat cycles, nominally once per lease period.
4. Retry the next chat turn after terminal confirmation. Re-run `accounts disable`
   when checking an account removal that previously reported unconfirmed runs.

`accounts disable` retains exit 2 and the exact `runs_unconfirmed` IDs while a
durable outcome remains unresolved. A local fence never converts that result
to exit 0. Persistent storage failure requires repair; restarting against an
unavailable database does not establish recovery.

## Scope and evidence

`test_terminal_write_recovery.py` drives the production manager, worker, stream
bridge and durable SQL repository with offline fault injection. The same cases
run on SQLite and, when `TEST_POSTGRES_URI` is supplied, isolated PostgreSQL
schemas. They cover validation before SQL, confirmed rollback, lost commit
acknowledgement, unavailable writes, recovery after availability returns,
owner/version/lease changes, cancellation races, SQL-close drainage, shutdown,
receipt ordering, successful edit preservation and same-chat admission.
The rollback and ambiguous-commit cases reproduce baseline defects;
capacity reasons and the existing cancellation/delivery behavior are preservation
controls. No provider call or additional sandbox is required.

Custom stores retain their existing API. A store must explicitly implement the
atomic `finalize_owned` contract before setting `supports_terminal_recovery`.
The legacy compatibility path does not provide SQL recovery guarantees.

This source evidence does not qualify a published release for tenant use.
Infra owns qualification on its separately leased fixture, including the fresh
twenty-person batch-removal proof requested by P-ba. That run is performed
outside this development session.

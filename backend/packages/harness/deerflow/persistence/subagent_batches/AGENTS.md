# Stored conversation jobs

An entry without a scope uses the ordinary batch worker. For a recognized agent
scope, the worker observes the source run and leases admission of a new run.
The admitted run owns helper claims, results and the single final turn. Keep the
typed private execution projection separate from owner-visible controls and
counts. Unknown scopes receive no worker or operator writes.

Submission locks the physical conversation before checking for an open job.
Admission locks the batch before its execution fence. The run insert and the
stored continuation run ID commit in the same transaction. Both the captured
session principal and thread incarnation must still match. The full Gateway
run store rotates each open job's private thread generation on an intervening
admission, across owners. Checkpoint reads use that generation so even a
completed and deleted competing operation invalidates an earlier decision. Cross-store writers
use PostgreSQL row locks or SQLite's immediate write transaction.

Use the last expired run lease to bound a replacement, falling back to the run
update time only where leases were disabled. Replacement requires unfinished
items, leaves terminal results intact and reuses the original item deadline.
Final model execution is claimed once in durable state. Monitor the admitted
run after its items end. Confirm completion from its latest persisted lead
reply, using the transcript visibility rules, before recording an outcome. Readers can reopen the
existing status columns across versions. Derive the deadline count and the
final lead's item positions from persisted cancelled checks with the fixed
`time_limit` reason. Web run cancellation records the role with the first
accepted action on the run row. Panel cancellation checks that durable decision
under the existing batch-before-run lock. Keep actor IDs out of the projection.

Run `tests/test_bound_batch_lifecycle.py` on SQLite and PostgreSQL for changes
to admission, cancellation, recovery or item ownership. Ordinary and previous
reader controls are in `tests/test_batch_scope_repository.py`.

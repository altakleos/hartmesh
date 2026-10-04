# Scheduled execution retirement

Timeout completion is an atomic active-to-failed transition with a non-expiring
`__execution_retirement_pending__` marker in the existing occurrence lease owner.
It records the first terminal history while continuing to hold execution capacity.
No database migration is needed. Never clear this marker based on elapsed grace,
a terminal runtime status, lease expiry, a missing record, or scheduler restart.

All admission, mutation, same-thread ordering and global SQL-budget checks must
include marked terminal rows. Queue claims retain the PostgreSQL advisory lock /
SQLite BEGIN IMMEDIATE boundary. A fenced occurrence-ID + run-ID confirmation
clears only the marker, preserving terminal history and parent projections.

The Gateway resolves actual owner-local worker tasks with store errors propagated.
Only task.done() / the actual worker's done callback confirms drain; completion
callbacks run before cleanup. Cache positive evidence through database failures
and runtime record eviction. Bound observation, Stop and confirmation calls;
track helper tasks and cancel/drain them on shutdown. Stop retries are four per
service object (initial attempt, then 5/15/30 seconds), not a cluster-global count.
Rotate bounded pages of pending holds so unknown owners cannot starve others.

An unconfirmable hold survives every startup. Operators may release its exact
marker only after verifying the old worker and sandbox have terminated, using the
fenced repository confirmation method; service-count configuration alone is no
proof. Do not add an HTTP route that accepts an unverified retirement assertion.

Tests use real SQLite/PostgreSQL repositories and actual asyncio tasks; cover
first-terminal races, cross-instance capacity, thread ordering, bounded retries,
unknown evidence, record eviction, confirmation failures and shutdown. See
backend/tests/test_scheduled_run_time_bound.py and test_scheduled_task_postgres.py.

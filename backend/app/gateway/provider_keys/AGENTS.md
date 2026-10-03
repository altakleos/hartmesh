# Provider key changes

`ProviderKeyService` applies runtime configuration before persisting a provider
key change. Under its mutation lock, drain installation, database commit, and
failure rollback as one operation with `await_drained`. Cancellation of the
caller (including repeated cancellation) propagates only after that operation
settles; a started successful mutation still commits. Never restore the previous
configuration just because the caller was cancelled: the database commit may
already have succeeded. Keep filesystem work and config reloads off the event
loop.

Log mutation and rollback failures inside the drained operation so a cancelled
caller cannot hide them. Log exception types only; exception text may contain
credentials. Provider-key tests use explicit worker and commit barriers against
the real renderer, config loader, and SQLite repository.

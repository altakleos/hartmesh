# Persistent agent instances

Definitions remain in `persistence/agents`; adopted documents, resident
identities and delegated access live in `persistence/agent_instances`. Never
substitute a resident principal for human `user_id`, create login users, or
inherit private creator memory/credentials as ambient agent context.

`InstanceDirectory` rechecks active nonhuman identities. Existence does not
authenticate caller-selected IDs. Active personal actors need a live custodian;
company actors do not depend on creator login. Only the exact host-owned
provisioning ContextVar validates a pending grant target; reset it on every exit.
Ordinary lookup rejects pending/suspended/archived/deleted identities.

Creation drains enrollment, atomic home binding, canonical membership grants
and activation across cancellation. Creation UUIDs bind creator plus exact
request. Pending retries retain home/revision identity; ready retries never
restore revoked grants or recreate backing. Use Storage Spaces' provisioning
transaction and membership API, never direct grant inserts or directory creation.
Unknown outcomes retain inspectable intent.

SQLite reserves its writer before instance reads; PostgreSQL locks the parent.
Resource locks precede instance locks during home binding/activation. Confirm
initial grants in the shared transaction before active publication. Host grant
admission also fences delayed initializers under resource-then-instance locks:
a ready peer must not cause a stale creator to restore later revoked grants.
This additional condition never replaces mandatory storage checks. Generation
checks fence rename. SQL filters current grants before pagination. Provider
route permissions are additional ceilings; optional authorization settings
never bypass mandatory instance/storage checks.

Hash exact captured config/SOUL/lookup namespace, without adding future defaults
to old revisions. Validate stored revision bytes on reads and retries. Business
configuration does not grant ADMIN, credentials, plugin trust or wider mounts.
Append migrations after `0034`; preserve published fixtures and register models
in `persistence/models/__init__.py`. No creator/user deletion cascade may erase
company identity or homes.

`test_agent_instances.py` covers both SQL backends and actual storage APIs with
injected directories/backings; these fixtures never qualify native quotas. The
mandatory Storage Spaces Linux Docker tier owns native filesystem proof.
Runtime binding, scoped memory and full lifecycle/product controls are later
stages consuming these identities and qualified attachments.

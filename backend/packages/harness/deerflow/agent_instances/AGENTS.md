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
`AgentConversations` owns durable bindings, retry keys and deleted-chat tombstones.
Never infer binding from JSON metadata or requester ownership. SQL audience filters
precede pagination; current Inspect or original requester Use admits reads. Manage
controls mutations, while Use controls execution and stopping one's own run.
Worker bookkeeping locks thread then instance, matching repository mutations.

`AgentExecution` is host-only and carries human requester separately from resident,
adopted revision, Home and thread incarnation. Reject client copies and never persist
the object. Inspection projections cannot execute. Validate on the original host
loop before model/tool calls, including isolated child loops and worker threads.

`runtime.prepare_environment` consumes exact admitted native attachments and the
existing AIO SDK. Address immutable Docker IDs, not container IPs. Drain preparation
and closure across cancellation; unknown outcomes retain native intents. SDK closure,
leases and ordinary completion never fence or replace physical writers. Children
inherit the full Home mount; tool filtering is not a narrower filesystem boundary.

Public skill scope is read-only and excludes private/legacy/integration storage,
including late activation and primed caches. Capture bounded public bytes and record
their native revision separately from live host activation. Home copies are mutable
data, not authority or immutable execution evidence. Instance DeerMem facts and
all summaries use SQL through the portable storage port, with explicit current
Inspect/Home READ and execution Use. Never substitute user IDs or disk indexes.
Resource→thread→instance locks admit snapshots/commits; release before extraction.
Capture epoch when queued and retain it across retries. Clear/import retire old
epochs atomically; lifecycle generations and conversation tombstones reject stale
writes. Use-only/disabled/unsupported scopes have no memory fallback. Queues stay
owned through completion and shared-budget shutdown; do not prune live run queues.
No scheduling, background tasks or separate storage/file-manager API is introduced.

# Persistent agent instances

A definition is reusable configuration. A persistent instance has a separate
non-login identity, personal/company custody, human supervisor, adopted
definition revision and a Storage Spaces home. Two instances using one
definition retain different principals and homes.

Management requires `agents_api.enabled: true`, a durable database, and enabled
Storage Spaces with qualified provider inventory. Existing default/custom chats
retain their behavior. Creation needs no conversation or running sandbox.

## Management API

`POST /api/agent-instances` requires provider permissions `agents:read` and
`agents:write`. Send a UUID-hex `creation_id`, display `name`, and caller-visible
custom `definition_name`. Optional `custody` is `personal` (default) or `company`;
`supervisor_id` defaults to the requester. Company creation requires current
host provisioning authority, and supervisors must be active humans.

The host loads the definition in the authenticated caller's namespace and
records its exact configuration and SOUL text under a SHA-256 revision. Requests
cannot provide an owner, principal, revision, home path or configuration snapshot.
The revision does not freeze remote models or future host behavior. Execution
must still honor current model/tool/resource/provider policy.

The server generates the instance ID and `agent:<id>` principal. Its stable
home is provisioned through Storage Spaces. Personal home custody stays with
the person; company custody has no personal owner. The resident receives
explicit READ/WRITE/EXPORT, without ADMIN. Creator and supervisor initially
receive Use, Inspect and Manage instance access; a different supervisor also
receives explicit home management access. Company custody confers no ambient
audience or creator-private credentials.

`GET /api/agent-instances` filters current grants in SQL before pagination.
`GET /api/agent-instances/{id}` exposes identity to current Use, Inspect or Manage;
`/definition` requires Inspect and exposes its
adopted configuration under that same check. `PATCH /api/agent-instances/{id}`
renames with the current `generation`, Manage, and provider `agents:write`.
Rename retains principal, custody, revision and home identity. Provider route
ceilings supplement mandatory grants. PAT instance/storage scopes are unsupported.

## Interrupted creation

Creation records a durable `provisioning` intent, atomically binds one qualified
home, and installs grants through the existing storage membership API. It
publishes `active` only after confirming those facts. Pending principals are
unavailable to ordinary storage callers; only the exact trusted provisioning
scope can validate a pending grant target.

Retry the exact request with the same `creation_id` after interruption. The
original identity, home and revision survive even if the source definition was
changed or removed. A different request returns 409. Lack of capacity leaves an
inspectable pending intent; it never substitutes an ordinary folder. Ready
retries do not recreate homes, restore revoked grants or reactivate suspended
identities. Cancellation drains owned persistent work before returning.

Company actors survive creator departure; personal actors require an active
custodian. Ordinary home browsing uses the existing Spaces resource page.

## Conversations and execution

`POST /api/agent-instances/{id}/conversations` requires `agents:read`,
`threads:write`, and current Use. Send only a UUID-hex `creation_id`; the server
mints the conversation ID. Exact retries return that chat, and deleted-chat
retries cannot recreate it or adopt old checkpoint data. Each conversation
keeps its human requester for attribution; a separate durable binding selects
the instance and adopted definition.

Inspect reads all of the instance's conversations and evidence. Use also reads
the caller's own instance conversations. Manage plus the relevant provider
ceiling controls conversation mutation and deletion. Use can stop the caller's
own run; stopping another requester or replacing active work requires Manage.
Current grants control lists before pagination, history, streams, exports and
run evidence. Revocation stops subsequent SSE frames. JSON metadata cannot bind
an ordinary chat or change an instance identity.

Instance execution currently requires the qualified direct Linux Docker AIO
storage adapter. Other transports refuse execution. The existing lead graph,
run worker, journals and checkpoint accessors remain in use. The environment
mounts only the instance Home at `/mnt/spaces/home`; its fixed workspace,
uploads and outputs aliases refer to that Home. SDK requests address the exact
registered container through Docker control, without a reusable container-IP
endpoint. A new chat or completed run reuses the admitted attachment. Lease
expiry, SDK closure and chat deletion do not retire a physical writer or Home.

Ordinary wiki, repository and database edits persist in Home without an artifact
manifest or automatic delivery requirement. Explicit output presentation uses
Home's `outputs/` and the existing Space download path. Checkpoint branches keep
the same instance and Home, instead of copying requester directories. Children
inherit that native filesystem authority; a smaller tool list does not narrow
it, and the child prompt states this scope.

The requester supplies attribution and current provider ceilings, rather than
My Files, private memory, private skills or personal credentials. Instance
definitions may reference currently enabled public skills. Private/legacy
packages, uploads tooling and MCP credentials have no instance adapter yet.
Public package bytes are captured with bounded readers, copied and verified in
a content-addressed Home cache; they count against Home quotas. Run evidence
records the admitted package revision separately from the current host skill
catalog. The native cache remains writable data; its hash/path does not attest
that later execution bytes are immutable or grant additional authority.

Requester-directory editors, ZIP artifact archives, browser sessions, legacy
Shared publishing and scheduling return unsupported for bound chats. Use the
existing Home Space browser for file edits and downloads. Legacy chat features
keep their existing behavior. Turning managed storage off makes persisted
bindings unavailable instead of restoring creator-owner access.

## Delivery stages and schema

Identity, creation, revisions, homes, rename, durable conversations, runtime,
instance memory and lifecycle/product controls are implemented. No separate
file manager, storage ACL engine or background scheduler is introduced.

Migration `0034_agent_instances` adds `agent_definition_revisions`,
`agent_instances`, and `agent_instance_grants` after release 43's `0033` head.
Existing application tables and configuration format are unchanged. Fresh
bootstrap and normal upgrades register the new tables. Downgrade refuses to
erase used identity/grant/revision facts; published ancestry stays immutable.

Migration `0035_agent_conversations` adds one binding table with the instance
reference, original requester and retry identity. It retains deleted-chat
tombstones and has no user/thread deletion cascade. Existing application tables
and configuration format are unchanged. Downgrade refuses to erase used bindings.

## Instance memory

Default DeerMem instances keep facts and every summary field in the application
database under stable instance identity. They start empty, including company
instances; no creator-private summary or user/global bucket is read. Legacy
chats keep their existing file-backed memory. General wiki/knowledge files still
belong to Home and keep their ordinary Storage Spaces behavior. SQL platform
memory checks Home audience/lifecycle through READ admission; it does not claim
an exclusive filesystem-edit window over a live native attachment.

Current Inspect and Home READ admit whole-instance memory. Native execution also
requires Use; Use-only execution disables injection, tools, capture and compaction
flush. Losing Inspect stops further model/tool dispatch for a run that already
injected instance memory. Definition `memory_enabled: false` and global memory
disabling prevent execution reads and capture. Manual management remains available
under Inspect, or Manage plus Inspect for edits, and provider memory/agent ceilings.
Conversation-derived memories are shared instance data for this explicit audience.

Memory execution also requires compatible conversation and writable Home readers,
including an original requester with Use who has no Home grant. Memory-bearing
history requires current Inspect for reads and execution. References and branches
check destination readers against source readers; copies across original requesters
retain this Inspect requirement across restarts and subsequent copies. Losing
Inspect cannot reopen protected history through the original-requester Use exception.
Inspect admits conversation/evidence reads without an additional Home READ grant.

Facts and summaries use DeerMem's existing extraction, safety filters, capacity
policy and lexical/relevance ranking. Derived access/eviction metadata stays in
SQL; instance data/retrieval uses no private file/index fallback. Normal
host singleton initialization for legacy memory remains unchanged. Instance search currently uses
bounded lexical/relevance ranking, rather than a disk FTS5 index. Remote backends,
custom storage providers and custom retrieval factories have no qualified instance
adapter: execution memory is disabled and management returns 501. Legacy usage
of those backends is unchanged.

`GET /api/agent-instances/{id}/memory` and `/memory/export` return the admitted
document. `/memory/facts` supports POST and PATCH/DELETE by fact ID. DELETE
`/memory` clears facts and summaries. POST `/memory/import` accepts an explicit
`document` replacement. Read needs `agents:read` and `memory:read`; mutation needs
`agents:write`, `memory:read` and `memory:write` in addition to mandatory grants.
Imports normalize/cap facts and replace only this instance's summaries.

Clear/import advance a SQL epoch in the same commit. Queued jobs retain their
original epoch across reload/CAS retries; generation, current grants, Home and
conversation incarnation are rechecked before commit. Extraction holds no SQL
locks during model calls. Shutdown drains the bounded queues while the original
host loop is live and retires scopes that exceed its shared flush budget.
Documents are limited to 4 MiB, 10,000 facts and 64 KiB per fact/summary.

Migration `0036_agent_instance_memory` adds `agent_instance_memory` for the
instance document/epoch and `agent_protected_contexts` for durable Inspect-required
conversation provenance after `0035`. No user-deletion cascade applies. Downgrade
refuses used memory or provenance; configuration format is unchanged.

## Lifecycle and product controls

`/workspace/instances` creates personal/company residents from caller-visible
definitions, shows custody/supervision/current access, and starts server-minted
conversations. The existing chat page resolves identity through
`GET /api/agent-instances/conversations/{thread}/instance`; client metadata cannot
bind an agent. Instance chats link to Home and withhold unsupported legacy upload,
browser, sidecar and scheduling controls. Existing-chat legacy capabilities wait
for successful feature discovery and a positive unbound result; lookup errors
keep those actions unavailable. Use Home for ordinary file uploads.
Home/Projects browsing and selected cross-space copies use `/workspace/spaces`;
the native execution view remains the qualified Home, without ambient requester
or Project mounts. Existing storage audience/copy checks remain authoritative.

`POST /api/agent-instances/{id}/lifecycle` accepts captured `generation`, UUID-hex
`operation_id`, and `action`: `suspend`, `archive`, `delete`, `restore`, `adopt`,
`supervise`, or `grant`. Adoption takes caller-visible `definition_name`, supervision
takes `supervisor_id`, and grants take `member_id` plus explicit permission bits
(Use1/Inspect2/Manage4; zero revokes). Agent access and storage membership are
separate; manage resource grants through Spaces. Domain Manage and current Home
ADMIN are mandatory, alongside provider `agents:read`/`agents:write` ceilings.

Suspension/archive/removal preserve principal, Home bytes, human storage grants
and independently owned Projects. Qualified Storage Spaces retirement records the
exact attachment scope and inactive agent generation atomically before physical
effects. HTTP202 means containment remains pending, never that the writer stopped.
GET `/lifecycle` supplies current operation status and captured retry requests.
Retries reuse the same identity/scope; completed retries cannot stop replacement
writers. Renaming cannot strand a pending intent. Owned mutations drain cancellation.

Restore requires a retained active, qualified Home, current resident read/write
grants, a live supervisor/custodian and a valid adopted definition. Conflicting or
unresolved native views prevent restoration. It restores no old membership and
does not reconstruct deleted Space backing. Use qualified Space recovery separately
when storage itself needs recovery. Generations reject older runs and memory jobs.
Company managers with current Home ADMIN may resolve the exact committed pending
request after creator departure; original actor remains recorded and `resolved_by`
identifies the current resolver. Personal intents remain actor-bound. Captured
adoption retries do not require the mutable source definition to survive.
`POST /api/agent-instances/{id}/lifecycle/abandon` takes the pending operation ID
and current instance generation. Current Manage/Home ADMIN may abandon obsolete
intents only after their exact captured attachment scope is fenced. It records
`phase=abandoned` and the resolver, keeps grants/request/actor and suspended status,
and performs no provider effects. Use normal supervision/restore afterward.
Abandoned retries are terminal and cannot replay old grants or stop replacements.

`POST /api/agent-instances/{personal-id}/company-copy` takes a creation identity,
captured personal generation, name and optional supervisor. The current personal
custodian needs Inspect/Manage and current company-provisioning authority. The
separate company target adopts an explicit copy of the captured business definition;
its Home and memory start empty. Select file transfers in Spaces under current
READ/EXPORT/WRITE and disclosure admission, and transfer memory only through explicit
export/import. Credentials, old principal, private user memory and legacy folders
are never transferred implicitly. The personal instance remains independent.

Migration `0037_agent_lifecycle` adds `agent_lifecycle_operations` after0036, with
exact request/Home/attachment identity, original actor, prior status, generation,
phase and resolver. Existing tables/configuration format are unchanged. Downgrade
refuses used intents. Historical migrations/ancestry fixtures remain immutable.

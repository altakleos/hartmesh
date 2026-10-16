# AI assistants and AI employees

An agent is a person’s own AI assistant or a company’s AI employee. API identifiers
such as `home`, `home_id`, `/home/` and `/mnt/spaces/home` retain their names; in this
document they refer to the agent’s Space and files.

A definition is reusable configuration. A persistent instance has a separate
non-login identity, personal/company custody, human supervisor, adopted
definition revision and a Storage Space. Two instances using one
definition retain different principals and Spaces.

Management requires `agents_api.enabled: true`, a durable database, and enabled
Storage Spaces with qualified provider inventory. Existing default/custom chats
retain their behavior. Creation needs no conversation or running sandbox.

## Management API

Managed-storage availability is separate from agent identity. `/api/features`
reports `agent_instances.execution` as `ready` or `unavailable` with a bounded
reason; `/health` maps this to state words `ready`, `none`, `held` or `unavailable`
(and `off` with storage disabled). Current reasons include `storage-unavailable`,
`agents-none`, `space-unverified`, `space-not-assigned`, `operator-hold` and
`host-replaced`. AI assistant/AI employee allowances expose
`allowed` and `in_use` counts, never bytes. An execution admission refusal carries
HTTP 409 with a public `detail`, `reason: execution-unavailable` and a separate
`execution_reason`. Feature file routes instead use a structured `detail` with
`code`, `reason` and a public message. The Compose renderer still writes both
explicit agent creation counts: one AI assistant per person and ten company AI
employees by default. Both can be zero while personal/company storage stays enabled.

Startup logs report storage and agent execution readiness separately. A prepared
storage pool can coexist with `agents: unavailable (storage-unavailable)` while
resident initialization is incomplete. Failed initialization reports bounded
error types and source locations plus qualification state; exception values,
tenant contents and source text are omitted.
Resident graph qualification excludes only Docker's observational `ExecIDs` and
`State.Health` fields from snapshot equality. Identity, process state, restart
count, complete configuration, mounts and network facts still have to match;
the paired graph is checked against the fresh primary inspection. A mismatch
logs only known top-level field names, with unknown names shown as `other`.
Mount list order is normalized using each complete record; all record fields
and duplicate multiplicity remain part of the comparison.
Workspace liveness accepts absence only with return code 1, empty or `[]` output
and an exact known error naming the original container ID, including Docker 29's
lowercase form. Confirmed loss enters original-resource containment; an outage
or ambiguous reply retains the charge and prevents replacement.
Routine reconciliation keeps the last confirmed readiness while healthy checks
are in flight. Initial qualification remains unavailable; pending findings,
failures, cancellation, accounting invalidation and shutdown clear readiness.
Refresh publication checks its captured revision and exact runtime bindings,
so a stale success cannot restore readiness or reopen provider admission.
Refusal logs distinguish unavailable storage, a missing runtime, pending
bootstrap and an unqualified workspace. Invalidation logs name fixed reasons,
and run-inventory logs count mismatches using fixed field labels. Attachment
diagnostics use booleans for captured state and preparation; none records
tenant identifiers, metadata keys or values, or file contents.
Reconciliation rechecks a captured attachment's actor, host, mount scope, identity
and phase under the Space lock before acting on it. An exactly fenced snapshot
permits one whole-pass rescan only after its retained charge, association,
preparation and uncertain-containment state are cleared. The new pass must
qualify the replacement through the normal resume path under the original
deadline. A filtered scan, a second change or uncertain identity remains pending;
inspection failures retain captured charges. A failed scan before claims are
processed closes accounting for restoration. Observation performs no cleanup.
The existing maintenance loop also wakes after expired-run recovery. A consumed
failed initializer starts one fresh owned pass in that invocation; an unfinished
initializer remains owned. Successful completion wakes that same loop to consume
the result after its maintenance owner drains; failed tasks retain the normal
retry interval. Stop and wake waiters drain with the same loop. This
changes scheduling only: peer lease takeover and current native proof still
precede recovery. When startup qualification finishes later, actual execution
readiness reports its confirmed containment counts once.

Creation, conversations, replies, activations and lifecycle changes check current
execution readiness. AI assistant creation can refuse `ai-assistant-limit` with
`{per_user, mine, allowed, in_use}` or `ai-assistant-none-free`. AI employee
creation can refuse `ai-employee-limit` with `{allowed, in_use}`; a caller without
current company provisioning authority gets 403. The instance list also returns
`ai_assistants` personal counts. Custody selects the kind: `personal` takes an
assistant slot and `company` an employee slot. Unfinished and inactive instances
count toward allowances; exact creation retries preserve admitted identities.


`POST /api/agent-instances` requires provider permissions `agents:read` and
`agents:write`. Send a UUID-hex `creation_id`, display `name`, and caller-visible
custom `definition_name`. Optional `custody` is `personal` (default) or `company`;
`supervisor_id` defaults to the requester. Company creation requires current
host provisioning authority, and supervisors must be active humans.

The host loads the definition in the authenticated caller's namespace and
records its exact configuration and SOUL text under a SHA-256 revision. Requests
cannot provide an owner, principal, revision, filesystem path or configuration snapshot.
The revision does not freeze remote models or future host behavior. Execution
must still honor current model/tool/resource/provider policy.

The server generates the instance ID and `agent:<id>` principal. Its stable
Space is provisioned through Storage Spaces. Personal custody stays with
the person; company custody has no personal owner. The resident receives
explicit READ/WRITE/EXPORT, without ADMIN. Creator and supervisor initially
receive Use, Inspect and Manage instance access; a different supervisor also
receives explicit Space management access. Company custody confers no ambient
audience or creator-private credentials.

`GET /api/agent-instances` filters current grants in SQL before pagination.
`GET /api/agent-instances/{id}` exposes identity to current Use, Inspect or Manage;
`/definition` requires Inspect and exposes its
adopted configuration under that same check. `PATCH /api/agent-instances/{id}`
renames with the current `generation`, Manage, and provider `agents:write`.
Rename retains principal, custody, revision and Space identity. Provider route
ceilings supplement mandatory grants. PAT instance/storage scopes are unsupported.

## Interrupted creation

Creation records a durable `provisioning` intent, atomically binds one qualified
Space, and installs grants through the existing storage membership API. It
publishes `active` only after confirming those facts. Pending principals are
unavailable to ordinary storage callers; only the exact trusted provisioning
scope can validate a pending grant target.

Retry the exact request with the same `creation_id` after interruption. The
original identity, Space and revision survive even if the source definition was
changed or removed. A different request returns 409. Lack of capacity leaves an
inspectable pending intent; it never substitutes an ordinary folder. Ready
retries do not recreate Spaces, restore revoked grants or reactivate suspended
identities. Cancellation drains owned persistent work before returning.

Company actors survive creator departure; personal actors require an active
custodian. The agent’s Files tab reads its Space through existing storage routes.

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

Instance execution currently requires the qualified Linux Docker AIO storage
adapter. Admission probes a fresh nonce through the daemon instead of requiring
the Gateway and Docker to share a mount namespace: a Gateway container may
qualify through its mounted local socket. Workspaces use the provider's restricted
network controller and exact read-only configured mounts; other transports refuse execution. The existing lead graph,
run worker, journals and checkpoint accessors remain in use. The environment
mounts instance Space at `/mnt/spaces/home` and the freshly qualified Company
`shared/` directory read-only at `/mnt/user-data/shared`; its fixed workspace,
uploads and outputs aliases refer to that Space. SDK requests address the exact
registered container through Docker control, without a reusable container-IP
endpoint. Neither the resident nor its relay publishes a host port. Shared binding
and directory revisions must still match before reuse. Company reclamation remains
unsupported; a configured Shared bind is not an ordinary attachment mount edge.
A new chat or completed run reuses the admitted attachment. Lease
expiry, SDK closure and chat deletion do not retire a physical writer or Space.

A workspace takes one of `sandbox.replicas` slots. At most two runs share it;
Work attempts are globally limited to `max(1, replicas - 1)`, including launch
reservations. Admission reports `busy`, `run-limit`, `work-limit`,
`restarting` or `starting-elsewhere` when those boundaries refuse work.
The host contains orphaned workspaces at startup, after 1800 idle seconds,
after a failed start or an uncertain end, with recorded attribution. A running
workspace with no run survives a Gateway restart. Closing a workspace under a
run stops it by its next model call with `ResidentInterrupted` and the recorded
reason. A busy Space refuses a new message with `storage-busy` and `cause`;
starting or closing workspaces retain their charge until confirmed containment.
Graceful Gateway shutdown records `gateway_stopped` on interrupted runs. This
managed resident lifecycle assumes one Gateway worker.

Ordinary wiki, repository and database edits persist in Space without an artifact
manifest or automatic delivery requirement. Explicit output presentation uses
Space's `outputs/` and the existing Space download path. Checkpoint branches keep
the same instance and Space, instead of copying requester directories. Children
inherit that native filesystem authority; a smaller tool list does not narrow
it, and the child prompt states this scope.

The requester supplies attribution and current provider ceilings, rather than
My Files, private memory, private skills or personal credentials. Instance
definitions may reference currently enabled public skills. Private/legacy
skill packages and MCP credentials have no instance adapter yet. Composer
uploads use the direct agent file routes below.
Public package bytes are captured with bounded readers, copied and verified in
a content-addressed Space cache; they count against Space quotas. Run evidence
records the admitted package revision separately from the current host skill
catalog. The native cache remains writable data; its hash/path does not attest
that later execution bytes are immutable or grant additional authority.

Requester-directory editors, ZIP artifact archives, browser sessions, legacy
Shared publishing and scheduling return unsupported for bound chats. Use the
agent’s Files tab for file edits and downloads. Legacy chat features
keep their existing behavior. Turning managed storage off makes persisted
bindings unavailable instead of restoring creator-owner access.

## Direct agent file writes

The following APIs write through the host's existing Space filesystem. They
require qualified execution, but do not start, stop or wait for a workspace;
ordinary filesystem concurrency applies while the agent is running.

| Endpoint | Current authority | Body and result |
| --- | --- | --- |
| `POST /api/agent-instances/{id}/conversations/{thread}/files?operation_id=…` | Use on this conversation, plus `threads:write` and `agents:read` ceilings | Multipart field `file`; returns 201 with `path`, `size`, `sha256`, `deduplicated` and `sandbox_path`. |
| `PUT /api/agent-instances/{id}/home/content?path=…&operation_id=…` | Space WRITE, plus `agents:read/write` ceilings | Raw bytes; returns `path`, `size`, `sha256` and `deduplicated`. Add `expected_sha256` to edit the exact loaded revision. |
| `POST /api/agent-instances/{id}/home/files` | Space WRITE, plus `agents:read/write` ceilings | JSON `operation_id`, `action` (`mkdir`, `rename`, `remove`), `path`, and `destination` only for rename; returns `complete` and `path`. |

Use a fresh 32-character lowercase UUID-hex operation ID per request. Exact
retries return the recorded result only after checking current access; changing
the request under that ID refuses. Uploads are limited to 64 MiB per file and
stream through bounded temporary spools. File contents never enter an SQL row.
The agent is the file-operation actor; `on_behalf_of` records the human requester.
Current human and conversation authority is rechecked under the mutation lock.

Conversation uploads land in Space's `uploads/`, creating it before the first run
when necessary. The returned sandbox path begins `/mnt/user-data/uploads/`.
An existing file with identical size and SHA-256 is reused; different content
uses the next free name, from `name (2).ext` through `name (99).ext`. Occupied
names are never overwritten. Names that cannot fit a suffix return a collision.
Edits require a matching SHA-256. Mutations refuse symlinked parents, protected
`.agent-skill-packages` paths, and inactive agents. Removal never recursively
deletes a directory. Before a write, quota checks require the body plus 1 MiB of
available bytes and at least two free inodes.

Failures use the existing refusal envelope, including `file-too-large` (413),
`files-full` (507), `file-changed`, `name-collision`, `reserved-path`,
`directory-not-empty` and `agent-not-active` (409), `invalid-name` (400),
`not-found` (404), and `file-write-failed` (503). Pending lifecycle or file work
uses `storage-busy` with its cause. Permission failures retain their access-denied
response. Cancellation drains all owned file workers before closing descriptors.
Upload context reads only confined, current regular-file metadata; it never
opens file content, follows upload links or reads requester directories.

These APIs add no database migration, native evidence format or configuration
version. The Files tab and conversation composer use these routes; the next
message carries saved references in `additional_kwargs.files`, using the returned
`sandbox_path`, filename and size. Use alone can attach input but does not permit
browsing or opening files; **Can see all work** (Inspect) is required for that.

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

Migration `0041_storage_host_replacements` follows `0040_work_execution`. It
adds the host-replacement table, nullable user seat-release marker, four storage
attachment fence-attribution columns and three Work-attempt containment/workspace
columns, and permits `host` as a Work-event actor. It ships with configuration
version 54. Normal bootstrap upgrades
SQLite/PostgreSQL; rollback to release 45 restores the pre-upgrade snapshot.

The Gateway now writes those existing host-attribution and Work-workspace fields
through coordinated restoration, exact preparation transfer and settlement.
Capacity publication requires a complete native census and drained producers;
workspace qualification remains a separate, retried execution condition. Work
launches retain exact reservations through commit and worker attachment, including
caller cancellation. The operator's `agents-hold` file refuses new agent execution
and closes current local workspaces through maintenance, without taking over a
live foreign lease.

This integration adds no database migration or configuration-version change.
Private native evidence adds explicit schema-3 zero-slot create and schema-4
zero-slot teardown records for managed chats without User storage. Existing
schema-1 creates and schema-2 teardowns keep their validation rules. Compose
agent counts now apply when explicitly set. Strict recovery uses retained original
container, network and backing identities, even when an attachment row is absent
or fenced. Live leases and unfinished create operations remain protected;
unexplained resources keep admission closed until their identity is resolved.

## Instance memory

Default DeerMem instances keep facts and every summary field in the application
database under stable instance identity. They start empty, including company
instances; no creator-private summary or user/global bucket is read. Legacy
chats keep their existing file-backed memory. General wiki/knowledge files still
belong to Space and keep their ordinary Storage Spaces behavior. SQL platform
memory checks Space audience/lifecycle through READ admission; it does not claim
an exclusive filesystem-edit window over a live native attachment.

Current Inspect and Space READ admit whole-instance memory. Native execution also
requires Use; Use-only execution disables injection, tools, capture and compaction
flush. Losing Inspect stops further model/tool dispatch for a run that already
injected instance memory. Definition `memory_enabled: false` and global memory
disabling prevent execution reads and capture. Manual management remains available
under Inspect, or Manage plus Inspect for edits, and provider memory/agent ceilings.
Conversation-derived memories are shared instance data for this explicit audience.

Memory execution also requires compatible conversation and writable Space readers,
including an original requester with Use who has no Space grant. Memory-bearing
history requires current Inspect for reads and execution. References and branches
check destination readers against source readers; copies across original requesters
retain this Inspect requirement across restarts and subsequent copies. Losing
Inspect cannot reopen protected history through the original-requester Use exception.
Inspect admits conversation/evidence reads without an additional Space READ grant.

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
original epoch across reload/CAS retries; generation, current grants, Space and
conversation incarnation are rechecked before commit. Extraction holds no SQL
locks during model calls. Shutdown drains the bounded queues while the original
host loop is live and retires scopes that exceed its shared flush budget.
Documents are limited to 4 MiB, 10,000 facts and 64 KiB per fact/summary.

Migration `0036_agent_instance_memory` adds `agent_instance_memory` for the
instance document/epoch and `agent_protected_contexts` for durable Inspect-required
conversation provenance after `0035`. No user-deletion cascade applies. Downgrade
refuses used memory or provenance; configuration format is unchanged.

## Lifecycle and product controls

**My assistants** (`/workspace/assistants`) lists personal AI assistants;
**AI employees** (`/workspace/employees`) lists company agents. Their page offers
Conversations, Files, Work, Memory, Access and Settings according to current
permissions. **Can work with**, **Can see all work** and **Can manage** correspond
to Use, Inspect and Manage. Manage includes Inspect in the product controls.

The existing chat page uses the canonical thread row’s `agent_instance` binding,
including `custody`. A failed or unknown binding keeps legacy actions and file
requests unavailable. Bound conversations do not prewarm a sandbox or expose
legacy browser, archive, sidecar or scheduling actions. Attachments use the agent
file routes. Files open only after current Inspect is verified. Generic Spaces
browsing and authorized copies retain current source/destination access checks;
execution mounts no ambient requester or Project directory.

`GET /api/v1/auth/users` supplies named human pickers. Instance detail includes
`activity`, `home` and `execution`; the list adds `unfinished` with
`include_provisioning=true`. `DELETE /api/agent-instances/{id}` abandons an
unfinished creation and requires its captured `creation_id`. The conversations
list is `GET /api/agent-instances/{id}/conversations`. `GET /api/agent-instances/roles`
reads role starting points from `ui.roles` or the live tenant `roles.json`; changing
one never changes an already adopted brief.

`POST /api/agent-instances/{id}/lifecycle` accepts captured `generation`, UUID-hex
`operation_id`, and `action`: `suspend`, `archive`, `delete`, `restore`, `adopt`,
`supervise`, or `grant`. Adoption takes caller-visible `definition_name`, supervision
takes `supervisor_id`, and grants take `member_id` plus explicit permission bits
(Use1/Inspect2/Manage4; zero revokes). Inspect grants Space READ/EXPORT; Inspect
plus Manage grants READ/WRITE/EXPORT/ADMIN. Membership without Inspect removes
the file grant. In the Compose bundle every generic Space grant route refuses `owned-space`.
Access to an agent’s files follows its instance membership: Inspect gives 17
(READ/EXPORT), Inspect plus Manage gives 27 (READ/WRITE/ADMIN/EXPORT), and a member
without Inspect has no human file grant. Abandoning a grant restores the currently
committed membership’s file grant.
Domain Manage and current Space
ADMIN are mandatory, alongside provider `agents:read`/`agents:write` ceilings.

Optional `only_if_idle: true` checks the owning registry's actual run IDs inside
withdrawal before changing state. Busy requests return HTTP409 `work-in-flight`
with `run_ids`. The flag is not stored, so the same operation may be retried
without it. File activity and capacity reservations are not this run snapshot.

Suspension/archive preserve principal, Space bytes, human storage grants
and independently owned Projects. Qualified Storage Spaces retirement records the
exact attachment scope and inactive agent generation atomically before physical
effects. HTTP202 means containment remains pending, never that the writer stopped.
GET `/lifecycle` supplies current operation status and captured retry requests.
Retries reuse the same identity/scope; completed retries cannot stop replacement
writers. Renaming cannot strand a pending intent. Owned mutations drain cancellation.

Delete commits the agent and Space tombstones together, advancing the Space
generation and recording `retained_backups: false` with cause `agent-deleted`.
The route then makes one guarded host release attempt. HTTP200 includes
`space_released`, false for incomplete or unverified cleanup; HTTP202 and abandoned
operations omit it. Maintenance retries bindings that remain. Uncertain release
proof is retained until physical reclamation can be confirmed.

Deleted agents cannot be restored. Restore requires a retained active, qualified Space, current resident read/write
grants, a live supervisor/custodian and a valid adopted definition. Conflicting or
unresolved native views prevent restoration. It restores no old membership and
does not reconstruct deleted Space backing. Use qualified Space recovery separately
when storage itself needs recovery. Generations reject older runs and memory jobs.
Company managers with current Space ADMIN may resolve the exact committed pending
request after creator departure; original actor remains recorded and `resolved_by`
identifies the current resolver. Personal intents remain actor-bound. Captured
adoption retries do not require the mutable source definition to survive.
`POST /api/agent-instances/{id}/lifecycle/abandon` takes the pending operation ID
and current instance generation. Current Manage/Space ADMIN may abandon obsolete
intents only after their exact captured attachment scope is fenced. It records
`phase=abandoned` and the resolver, retains the request/actor and suspended status,
and performs no provider effects. An unfinished grant first resets Space access
from current committed membership; a retired human receives no file grant.
Reset failure or a changed grant leaves the intent pending. The final transaction
rechecks both membership and file access. Use normal supervision/restore afterward.
Abandoned retries are terminal and cannot replay old grants or stop replacements.

`POST /api/agent-instances/{personal-id}/company-copy` takes a creation identity,
captured personal generation, name and optional supervisor. The current personal
custodian needs Inspect/Manage and current company-provisioning authority. The
separate company target adopts an explicit copy of the captured business definition;
its Space and memory start empty. Select file transfers in Spaces under current
READ/EXPORT/WRITE and disclosure admission, and transfer memory only through explicit
export/import. Credentials, old principal, private user memory and legacy folders
are never transferred implicitly. The personal instance remains independent.

Migration `0037_agent_lifecycle` adds `agent_lifecycle_operations` after0036, with
exact request/Space/attachment identity, original actor, prior status, generation,
phase and resolver. Existing tables/configuration format are unchanged. Downgrade
refuses used intents. Historical migrations/ancestry fixtures remain immutable.

## Network decisions

`direct_fetch` records the requester’s admitted web-fetch capability. Sandbox
egress uses the standing provider allowlist. A network card is available only
in an interactive conversation belonging to a current manager, never during a
Work attempt or for a Use-only requester. It offers Keep blocked or a temporary
allowance, which applies to that agent’s workspace across conversations and
assignments. Current authority is checked again on the response.

Every network request artifact carries `human_input.details` with `host`, `port`
and `ttl_seconds`, including ordinary chats. Agent tool messages record denied
connections in `sandbox_network_denied: {host, port}`. UI cards render from the
request on any tool message. Work runs in autonomous mode and asks its supervisor
through `report_work`/`request_input` instead of a conversational card.

## Operator tool

Run `python -m app.gateway.agent_instances` through the privilege-dropping `ai`
function in the [Compose runbook](../deploy/compose/README.md#commands). Its nine
verbs are `list`, `status`, `close-workspace`, `verify`, `export`, `supervise`,
`delete`, `adopt-host` and `settle`. Each invocation emits one JSON document;
exit 0 means complete, 1 refusal/failure, and 2 incomplete or unconfirmed effects.

`delete` removes only an agent no eligible customer human can manage: a personal
AI assistant whose owner is no longer live, or a company AI employee with no live
supervisor or eligible Manage holder. The command rechecks this before committing
both tombstones. It does not free the slot: the running Gateway’s host release
removes files/backups and returns it to the matching kind. `settle` reconciles
one interrupted file operation/publication; `--discard` records a host decision
without replaying filesystem effects. See the runbook for exact flags, eligibility,
uncertain outcomes and host replacement requirements.

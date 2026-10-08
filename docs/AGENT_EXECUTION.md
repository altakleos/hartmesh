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
`GET /api/agent-instances/{id}` requires Inspect; `/definition` exposes its
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

## Delivery stages and schema

Identity, creation, revisions, homes and rename are implemented. Conversation
and runtime binding, isolated instance memory, and lifecycle/product controls
are subsequent stages; instances are not yet selectable for chat. No separate
file manager, storage ACL engine or background scheduler is introduced.

Migration `0034_agent_instances` adds `agent_definition_revisions`,
`agent_instances`, and `agent_instance_grants` after release 43's `0033` head.
Existing application tables and configuration format are unchanged. Fresh
bootstrap and normal upgrades register the new tables. Downgrade refuses to
erase used identity/grant/revision facts; published ancestry stays immutable.

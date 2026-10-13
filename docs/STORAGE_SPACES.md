# Storage Spaces

## Host reclamation foundation

The private host recovery method reclaims already-deleted native Spaces only
when the latest delete event records `retained_backups` as JSON false and every
attachment is fenced. It does not resolve a principal or grant permissions.
Trusted domain callbacks recheck their own eligibility in borrowed SQL sessions;
callers own external containment through the entire operation. Account release,
Docker discovery and periodic scheduling join in the next Compose category.

Reclamation commits an immutable intent before filesystem effects. A private
sibling journal records exact replacement roots; cleanup exchanges both public
roots, purges the retired trees through descriptors checked against their durable
identity, then removes the retired directories. Old opened and unopened views
refuse the replacement. Full control storage can bootstrap under the resource
lock, retaining the binding marker and any existing workflow lock until ownership
is established. A held unlinked file can keep reclamation pending until it closes.

A filesystem-complete checkpoint commits while the original binding remains.
Journal removal precedes the final binding/domain transaction commit. A failed
commit or callback can retry that checkpoint even after journal removal, without
adopting another root. Unexpected journal paths, identities, contents, private
records or authority changes preserve pending state. An unwritable mounted parent
refuses before intent creation and requires operator repair. Read-only capacity
checks exclude unresolved sibling journals without requiring renewal permissions.

This corrects the design's CONTRACT 8.6 assumption that host release needs no
operation row: durable crash recovery uses the existing operation table's private,
versioned `host-release` JSON and the writer label `nonhuman/host:space-release`.
That label is provenance, never a directory identity. There is no database schema
change and storage defaults remain disabled. Native byte/inode/held-descriptor
qualification runs only in the mandatory Linux Docker CI tier.

## Extension storage contract

The dependency-free `deerflow-extension-api` 0.2.7 adds `StorageActor`,
`StorageCapabilities`, `StorageResource`, `ResourceReference`, `StorageController`,
`ResourceStorage` and `StorageProvider`. A `PluginContribution(api_version=4,
storage_api_version=1)` explicitly negotiates these additions. Old contributions
and positional action/tool contexts retain their behavior.

An action/tool receives optional `context.actor`, `context.storage` and a
host-resolved `context.resource` for a resource-bound browser action. A nonhuman
actor has a kind and subject ID and no human `principal`. Only trusted host
authentication/embedding code binds `storage_actor_scope`; claimed runtime or
payload identities never authenticate a caller. Storage calls accept no actor
parameter and recheck the live host binding, identity directory and current grants.
Personal access tokens have no storage scopes; the same denial follows inherited
tool/service work, and token runs receive no storage provider.
Trusted internal calls require an attributed owner; an ownerless internal or
scheduled request never becomes the synthetic default resource owner.
Services receive a lazy `deps.storage` provider: `capabilities.available=False`
and explicit unsupported errors distinguish a missing adapter from an empty root.
The public errors distinguish `StorageIdentityRequired`, `StorageAccessDenied`,
`StorageConflict`, `StorageOperationPending`, `StorageUnavailable` and
`StorageUnsupported`. Pending outcomes need explicit recovery rather than a
new automatic mutation. The facade advertises native attachments as unsupported;
qualified attachments currently use the separate trusted host consumer API.

Native operations expose resource discovery/provisioning/grants, confined file
reads/edits/transfers/export and qualified lifecycle/recovery. Cross-resource
copies retain source EXPORT and audience admission. A declared `StorageController`
binds mediated writes/removes/directory operations to an exact namespace, name and
metadata version under OPERATE. Fresh installed/enabled facts are checked inside
both durable admissions. Disable/remove/upgrade preserves resource facts and
readable data; generic native writes never replace the required controller.
Mediated lifecycle recovery remains unsupported without qualified feature recovery.

The existing browser module may add `resourceApiVersion: 1`. Its ordinary page
URL accepts `?space=<stable-id>`; the host resolves current access before mounting,
passes immutable resource facts and negotiated storage capabilities, and sends
`X-Deerflow-Resource` independently of action payloads. Backend actions recheck
that resource under the current actor. Account/unmount cancellation rejects late
metadata/mounts/actions. This extends the installed registry and assets lifecycle;
it introduces no second loader, policy language or per-customer browser build.

## Resource registry

Storage Spaces is HartMesh's persistent folder resource foundation. The resource and file
delivery adds identity, custody, mandatory access and a generic browser/API.
My Files, Shared and Projects keep their compatibility routes. In qualified
resource mode they use fresh resources through default first-party plugins.
Legacy folders are neither adopted nor copied. The
[consumer handoff](STORAGE_SPACES_CONSUMER.md) specifies the asynchronous API,
trusted nonhuman adapter, native workloads and supported deployment limits.

Each resource has a stable ID and opaque backing handle. Display names are
labels: renaming changes neither location, ownership nor permissions. Personal
custody names a typed principal; company custody survives a creator/member
leaving and does not imply company-wide visibility. Ordinary subdirectories
will remain ordinary folders without a resource record per directory.

Access uses explicit current grants, independently of optional authorization.
Human and nonhuman subjects have distinct typed keys, validated by trusted host
identity adapters. The registry is a host API: callers must bind authenticated
identity before invoking it. Unknown or retired actors receive no access.

Native mode permits ordinary writes once file/attachment adapters are
qualified. Native write grants include read authority and explicitly acknowledge
existing data. Mediated mode never grants native writes; an opaque installed
controller binding preserves its required behavior even when that feature is
unavailable. A name or file written inside a resource cannot install platform
code or grant access.

Resource generations and grants change in one database transaction, with a
typed authority event. Stale metadata mutations fail. Archive/deletion/restore
and writable attachment replacement require actual writer fencing; metadata
changes and expired leases are insufficient.

The additive `0030_storage_spaces` migration creates `storage_spaces`,
`storage_space_grants` and `storage_space_events`. Existing data is not moved or
reinterpreted. The migration verifies pre-existing table shape and refuses a
downgrade after these tables hold resource identity, grants or events. Normal
Gateway startup applies the migration through the existing bootstrap.

## Qualified Linux backing

See the [Compose storage runbook](../deploy/compose/README.md#storage-and-agents)
for the shared-mount preflight, seven planning keys, pool layout, operator verbs
and snapshot rollback. The model and agents remain inactive by default in this
release. The renderer and operator CLI both keep `ABSENT_MEANS_DEFAULT = False`.


The opt-in host filesystem adapter exposes a product file API and browser. It supports
ordinary dotfiles, relative internal links, binary files and SQLite through
Linux `openat2` confinement. Absolute links and nested mount transitions are
explicitly unsupported by the host browser adapter. Bounded reads can detect
concurrent changes; revision hashes require an admitted edit window and do not
serialize arbitrary native writers.

The initial backing adapter verifies separate, fixed ext4 filesystems prepared
by the provider on a private ext4 tenant data disk. Image extent inspection
requires complete, exclusive allocation, including reserved unwritten extents;
aggregate block counts alone do not qualify. Each mounted filesystem UUID must
match its opened image, and the loop device must cover that complete image.
Filesystem blocks and inodes bound native writes; visible data and private
staging/recovery data consume the same capacity. Private control data stays
outside the visible root. Displayed available capacity excludes filesystem
overhead. Image allocation leaves the configured platform byte/inode reserve;
ordinary resource writes cannot grow these fixed image files.

The explicit operator helper uses preinstalled tools and prepares only new
regular image files. It never installs dependencies, builds VM images or reads
application configuration. For a new pool on a supported Linux host:

```sh
sudo python3 scripts/storage_spaces_volume.py \
  --data-disk /mnt/tenant/storage-spaces --count 4 \
  --bytes 1073741824 --inodes 65536 \
  --reserve-bytes 1073741824 --reserve-inodes 1024 --data-uid 1000
```

The directory is provider-owned and private. `inventory.v1.json` is a trusted
startup input, never a browser registration surface. Provisioning and mount
availability are operator responsibilities; mounts must exist before the host
loads that inventory. A late durability failure after publishing the inventory
preserves prepared backings. Existing pools and data are not reformatted.

The Gateway pins the inventory at startup. Missing or malformed inventories and
individual slot failures retain durable resource and AI employee records; they
do not stop the Gateway. A durable database is required. Startup records separate
storage and execution status, including individual unverified slot identities.
Docker admission checks the local root-owned Unix socket and daemon identity,
then asks a UID-1000, read-only, network-isolated container to read a fresh nonce
file from the pool. Each bound native view gets the same check through its private
control directory before workspace creation. These probes establish path coherence;
the ext4 backing verifier separately establishes capacity and filesystem identity.
Probes bind one file, use the daemon's default runtime and clean their own markers.
Native writer tests optionally use `HARTMESH_NATIVE_WRITER_RUNTIME`, including
`runsc`; a directory or engine double never qualifies native limits.

Docker admission has a 30-second wait within the 120-second storage startup
budget. Timed-out verification and admission remain owned; the minute recheck
uses their outcome before starting another admission. Static provider failures
require configuration repair and a restart. Shutdown drains owned initialization
before database teardown, including when later service startup fails.
Startup orders the optional `mount-all` call, pinned inventory verification,
service construction, bound-resource metadata checks, then Docker admission
within one budget. `execution_state` and `allowance(kind)` use current inventory
and durable rows rather than a cached free count. Feature dispatch remains
lookup-only and returns bounded structured 503 reasons.
Successful pool qualification does not enable unsupported workspace topology.
Restricted networking and configured-mount workspace support remain a later
runtime change; preparation and resume currently retain that guard, while
discovery and fencing remain available.

When `HARTMESH_STORAGE_INIT_IMAGE` is configured, Gateway startup asks that
pinned backend image to run `mount-all` before inventory verification. The helper
runs without a network, with fixed memory/process limits and a 60-second deadline;
the Gateway cannot invoke preparation, resizing or other operator verbs. Outside
the Compose bundle, an unset image key skips this call.

Timeout cleanup targets only the helper's generated name. Unacknowledged creation
or removal blocks automatic mount retries, even if the helper later disappears:
an empty listing cannot prove that an in-flight creation will never finish.
The journal reports the owned name for operator review. Shutdown drains the call
and attempts retained cleanup before database teardown. A successful retry
reverifies the catalogue pinned at startup, clears recovered-slot errors and
refreshes binding facts before Docker admission. Missing/unreadable inventories
and insufficient disk retain their stronger failure reason.

Typed assignment selects only its kind or a legacy untyped slot, creates its
visible and private folder layout after SQL binding uniqueness is established,
and writes user/company role links in that transaction. Cancelled layout workers
finish before SQL ownership releases; uncertain folders remain unclaimed.
Unbound files and failed verification never become another resource's backing.
Legacy untyped inventories remain supported outside typed Compose pools.

Offline growth takes effect when the Gateway reads the new inventory after a
restart. The same filesystem UUID and data/control inodes must remain, and both
bounds must stay the same or grow. Verified larger bounds update the durable
binding; shrinking or reformatting remains unavailable. Private slot claims prevent delayed metadata or filesystem results from
overwriting a newer allocation. An allocation remains reserved through its
caller's commit or rollback, and failed refresh commits publish no cache facts.
Claims are operational bookkeeping; current resource, grant, root and private
marker verification remain mandatory. One allowance definition
uses prepared capacity, configured counts and existing use. Disabled human
accounts still hold seats until explicitly released; AI assistants and AI
employees count in every status except deleted.

`GET /api/features` reports the storage state/reason, the caller's own-file
readiness, account seats and AI assistant/AI employee allowances. Aggregate
`allowed` and `in_use` counts are global; an AI assistant's `mine` count is
caller-specific. Identity comes from an authenticated session, explicit
auth-disabled adapter or attributed internal owner. Unsupported credentials
receive no personal projection. The existing `agents_api.enabled` flag remains
independent so custom-agent clients retain their existing contract. Off deployments
report zero native counts; failed projection returns unavailable defaults with
HTTP 200. No byte capacities appear in this response.

Own files are `not-assigned` while an actual assignable user slot remains,
`no-seat` when none remains, and `unverified` when an assigned binding cannot
prove its filesystem. Account seat occupancy does not imply file assignment.
Status reads close SQL before filesystem verification and grant no admission;
each file operation still rechecks authority and provider facts. `/health` adds
only storage/agent state words, preserving its existing fields. Storage failure
precedes the `none` agent state. It exposes no counts, paths or exception text.

`storage_spaces.user_spaces: false` refuses generic API creation with HTTP 403.
It also refuses generic grant additions, changes and removals with HTTP 409
`owned-space`. Host provisioning, assignment and trusted grants remain available.
The User and Company links identify storage whose chat mounts are not attachment
records: their generic backup, restore, archive, delete and recovery-acceptance
requests return HTTP 409 `mounts-unrecorded`. Restore the data disk from an
operator snapshot instead. `GET /api/spaces/{id}/recovery` remains available.
These refusals use the central `{detail, reason}` response, after current ADMIN
grant and generation checks; unowned resources retain their generic lifecycle.

When backing verification fails, an exact ADMIN admission may inspect or change
metadata without probing its unavailable workflow lock or resolving pending file
operations. Current principal, grant, generation and attachment checks still
apply. Any request that also includes file permissions still requires physical
verification, and recovery verifies the volume before filesystem mutation.
Accepting uncertain state may quiesce a workspace before verification refuses.
Unverified geometry is never published by a metadata edit.
Feature and quota projections
use the `shared` editor label; native attachments remain separately advertised.

The backend image also includes `python -m app.gateway.storage_init` for typed
Compose pools. Its verbs are `prepare`, `mount-all`, `fstab-sync`, `verify`,
`resize`, `forget`, `quiesce` and `status`. It reads `HARTMESH_DATA_DIR` and the
seven storage/count environment keys, and requires a privileged Linux container
with the host PID namespace and a shared pool bind. It never reads app config or
the database. The Compose bundle runs it as the bounded privileged one-shot before Gateway
startup; see [the operator runbook](../deploy/compose/README.md#storage-and-agents).

Typed pools use `inventory.v2.json` with a kind on every slot and
`max_inodes = max_bytes // 8192`. Schema 1 remains supported by the standalone
helper. The pool is root:1000 0750; images root:1000 0640; inventory 1000:1000
0440; visible data 1000:1000 0770 and private control 1000:1000 0700.
The mounted filesystem parent is root:1000 0770, allowing host root renewal;
the underlying unmounted mount point remains root:1000 0750. This corrects
the Compose design's original mounted root:root 0755 requirement. Parent access
is host-only: sandbox binds contain data roots or admitted descendants.
`mount-all` repairs only this parent after exact allocated image, three-way UUID,
loop-source/geometry and writable mount proof. The same image/root descriptors
remain pinned through changes. Missing data/control roots stay missing during
startup; full layout repair belongs to preparation or explicit repair. A marked
fstab block orders mounts before Docker, and `.storage-init.lock` serializes
operator verbs. `host_image_path` stays unset when pool paths match inside and
outside the Gateway. Preparation checks
the whole missing plan before allocating anything, leaves the larger of 4 GiB
and 10% of the disk free, and preserves at least 1,024 host inodes. Each completed
slot is published atomically, so a repeat continues an interrupted plan. Counts
may rise or fall without removing existing slots. Growth preserves files and
filesystem identity; it requires the Gateway and other holders to be stopped.
`forget --discard` explicitly destroys a slot's contents. Every successful or
partial verb prints one sorted JSON document; a refusal carries its reason.

Managed preparation also publishes `.native-creates`, a private 1000:1000/0700
directory outside `.probe` and every reclaimable data/control root. Publication
uses a private staged directory, descriptor proof, no-replace rename and directory
fsync. Published ownership, permissions or identity conflicts are refused without
repair; interrupted staging directories and existing bytes are preserved.

Managed native chat creation uses version-1 bounded single-link 0600 records
containing immutable attempt, daemon, container name, owner/thread and exact slot
correlation facts; these facts grant no authority. Qualification restores retained
pending capacity before orphan reconciliation or native publication. The exact
journal stays owned through retry and worker drainage, alongside the pool lock.

Each primary request is recorded and fsynced before submission. Acknowledgment
requires actual same-daemon inspection of the full Docker ID, exact name, private
attempt label and all requested labels/slots. The provider retains verified
identity before acknowledgment or worker return, so cancellation and failed
acknowledgment cannot lose the container. Canonical fenced cleanup retries unknown
returned results from quarantine; identity-less requests consume capacity without
fabricated clients. Confirmed absence releases an acknowledged result's capacity,
but never settles an unacknowledged request.

Pending survives restart and is never erased because inventory is empty or time
has elapsed. The same owner or name cannot submit again while its request is
unresolved; unrelated owners remain admissible within the normal replica budget.
Partial record publication must restore accounting or disable native publication.
Malformed records, interrupted stages and limits refuse admission without eviction.
Preserve this directory with the pool; no browser/model force-clear API is provided.
Mountless chats and existing durable attachment preparation retain their own
lifecycles. Account-release containment follows separately. Defaults remain off.
There is no database schema change; private journal JSON remains version 1.

The private AIO native-retirement scope reserves an opaque owner and the exact
retiring physical slot before any worker starts. Its reservation lasts through
the host caller's filesystem work, final absence proof, publication and cancellation
drainage. The provider defines no released-account, role, retention or business
eligibility rules; the Gateway must supply that policy in the following stage.

Native create, speculative futures and cached/warm/discovered candidates retain
live admission references and register actual slots before publication. Shared
Company use and positively unrelated owners remain admissible. A copied expired
reference grants nothing. Unresolved creation evidence refuses containment when
the owner OR target slot matches, even with empty Docker inventory.

Containment reads all prefixes and running/stopped states by the exact slot label,
under a bounded same-daemon inspection budget. Unknown attribution, conflicting
tracking, foreign ownership and held executions refuse reclamation. Cleanup uses
the existing global holder reservation, provider teardown reservation, ownership
store and heartbeat. Qualified open-mode primaries are stopped and, if still
present, removed by their exact full Docker ID. The configured prefix/network mode
never supplies missing resource identity. Unsupported restricted sets refuse;
their sidecars and networks must be positively contained before release is enabled.

Stop alone is insufficient. Re-enumeration and the caller's drained final proof
must establish absence. Failed cleanup retains the exact result in quarantine;
an already-removed result can settle only after canonical fresh CID AND name
absence proof. Neither absence nor time clears an unacknowledged request. Correlation
facts stay process-local and are excluded from persisted sandbox metadata.

Managed-plan qualification requires one participating Gateway runtime per pool.
`prepare` and `mount-all` maintain `.gateway-owner.lock`, a stable empty regular
single-link file owned by 1000:1000 with mode 0600, outside temporary probes.
A valid file held by a Gateway is left untouched; repair requires exclusive
nonblocking ownership, and unknown bytes or inode types are refused. The Gateway
never creates, replaces or repairs it. Acquisition precedes baseline provider
construction and binds that exact provider to the owning Gateway lifetime.
Qualification retries retain the same file descriptor and provider. Closing
refuses new native admission and publication while owned teardown drains; final
close follows storage workers, provider shutdown and database disposal. Native
Docker commands inherit a duplicate of that descriptor, preserving exclusion
if their launcher exits. The lock does not prove container or daemon-request
absence; destructive cleanup still needs strict inventory and holder fencing.
Missing or busy ownership reports `host-unqualified` and retries every minute.
History and freshly prepared mountless chats stay available. Unqualified runtimes
cannot adopt, replace or remove native or unknown containers; mountless creations
use provider-specific names and never reuse cached native routing.

Host commands run through `nsenter -m -t 1`. Every verb holds the pool's exclusive
lock, inherited by host tools so it remains held if the CLI process dies during
offline repair or growth. `prepare --at-start` bounds its entire child process group at 600 seconds;
the process exits zero so startup can continue, while its JSON document retains
the actual return code and any partial outcome. No request provisions a slot.
After a forced interruption, the document lists persisted slots and their current
mount state; facts the supervisor cannot establish are explicitly `null`.
Storage remains off when `HARTMESH_USERS` is absent in this foundation change.

The mandatory Linux Docker job requires actual mounted backings and rejects
every skipped native case. This development host cannot mount the fixture; it
does not substitute ordinary directories or claim local native qualification.
Every category PR must pass that job at its final head, together with ordinary
SQLite/PostgreSQL Docker acceptance.

## Enable and use Spaces

After the provider has prepared and mounted the inventory, use a persistent
SQLite or PostgreSQL database and configure:

```yaml
storage_spaces:
  enabled: true
  inventory_path: /mnt/tenant/storage-spaces/inventory.v1.json
```

Restart the Gateway. Missing/unreadable inventory, invalid mounts, mismatched
identities or missing reserve report unavailable storage while preserving durable
records; they do not stop Gateway startup. A durable database remains required.
`/api/features` reports the service that actually started, rather than a later
config edit. The section is startup-only; configuration version 54 adds the plan
and `user_spaces` flag. The Compose renderer sets `user_spaces: false`, so its
generic creation route answers 403 while host assignment remains available.

The sidebar offers **Spaces** when supported. Create a personal space, or a
company space with host provisioning authority. A new resource claims one empty
prepared filesystem; there is no automatic pool growth or reused deleted root.
Browse nested folders and dotfiles, upload ordinary files, download binaries,
edit UTF-8 text, and rename/remove entries. Removing a nonempty folder requires
emptying it first. The browser accepts UTF-8 resource-relative names; absolute
links and nested mounts are unsupported. Individual upload/copy/edit requests
are limited to 64 MiB; the text editor opens at most 1 MiB. Descriptor downloads
stream larger regular files and support byte ranges.
READ-only members can preview bounded UTF-8 text without WRITE or EXPORT;
legitimate internal directory links are navigable. Host authority is checked
while opening each descriptor, then SQL locks release before its stream. An
already-admitted read retains its opened inode across atomic host replacement;
this is not a snapshot of arbitrary concurrent native/database writes.

HTML, SVG, XML and other active content are downloads. The editor displays
literal text and never executes file contents in the HartMesh origin. Access is
checked on the resource, including when optional authorization is off. Host
edits can coexist with active native attachments. The default Gateway has no native execution consumer; a trusted
host consumer installs the separately qualified attachment adapter.

Browser saves use the loaded SHA-256 and verify the original inode before replacement. A
stale file, generation or grant fails without silently overwriting or merging
the draft. This is not a general concurrent shell/SQLite transaction
protocol. Explicit cross-space copy requires source READ/EXPORT and destination
WRITE. A broader destination audience additionally requires source ADMIN and
explicit disclosure acknowledgement. A read grant alone cannot disclose bytes.

## Resource API

Trusted host integrations may use `registry.host_create` to allocate without a
human actor/grant/event, and `host_grant` for an atomic validated human grant:
its callback runs before no-op detection and every attachment must be fenced.
`host_locked` locks sorted distinct Space rows without assuming a human grant;
missing rows are skipped and all statuses remain eligible for host coordination.
These APIs confer no public HTTP authority.

`run_workflow(host_write=True)` admits a native destination under the member
request's exact READ | EXPORT, records `"by": "host"`, and rejects archived or
otherwise pending destinations. Source admission/disclosure and private receipts
remain enforced. The bundled Shared dispatcher uses this host-written protocol.
`run_read_workflow` takes shared host locks with `READ_LOCK_WAIT_SECONDS = 5`,
then rechecks authority and backing after the wait. `SpaceFull` identifies only
definite pre-publication byte/inode capacity failures; post-publication errors
stay uncertain for recovery.


All browser requests use the authenticated human. Attributed internal requests
use their validated actual owner; missing attribution never becomes `default`.
The explicit local auth-disabled adapter supports the one local development
identity. Nonhuman principal resolution is a trusted host integration, not an
HTTP registration API. PAT credentials do not yet carry storage scopes.

| Method and path | Operation |
| --- | --- |
| `GET/POST /api/spaces` | Discover or provision a native space |
| `GET/PATCH /api/spaces/{id}` | Current facts/quota or rename a label |
| `PUT /api/spaces/{id}/grants` | Set/revoke a validated typed subject grant |
| `GET /api/spaces/{id}/files?path=...` | Confined folder listing |
| `GET /api/spaces/{id}/content?path=...` | Stream file/ranges; `download=true` requires EXPORT |
| `GET /api/spaces/{id}/text?path=...` | Bounded UTF-8 text and SHA-256 revision |
| `PUT /api/spaces/{id}/content?...` | Import/create or compare-and-replace bytes |
| `POST /api/spaces/{id}/files` | Create folder, rename entry or remove file/empty folder |
| `POST /api/spaces/{id}/import` | Explicit audience-checked cross-space file copy |
| `GET /api/spaces/{id}/recovery` | ADMIN-only backup, operation and attachment facts |
| `POST /api/spaces/{id}/recovery` | Explicitly accept uncertain current file state |
| `POST /api/spaces/{id}/lifecycle` | ADMIN backup, restore, archive or delete |
| `POST /api/spaces/{id}/attachments/retire` | Retire the captured attachment IDs |

Mutations carry the current resource generation. File mutations also carry a
new UUID-hex `operation_id`; retrying a completed identical request returns its
recorded result, while another actor/payload/generation cannot reuse that ID.
File contents do not advance the metadata generation: native data is not a
per-file approval/event system. Source-chat deletion has no role in these URLs.

`0031_storage_files` adds durable backing bindings and file-operation intents
without changing `0030` ancestry. Used bindings and operation facts cannot be
downgraded away. Root inode identity is checked after service restarts. An intent
is durable before file publication; if publication or SQL completion is
uncertain, further WRITE/OPERATE/ADMIN requests fail visibly pending recovery.
Ordinary READ/EXPORT requests remain available beside pending file actions and
feature workflows. Pending copy, controller and lifecycle actions block all
admissions. A busy refusal carries `workflow` or `lifecycle` as its cause.
No automatic replay reports success or erases the evidence.

## Native attachment and containment

`SpaceAttachments` is a host-only API. A consumer supplies its validated actor,
a fresh execution-incarnation UUID, and typed resource IDs/current generations,
safe aliases and RO/RW modes. It does not supply authority through physical
paths or conversation IDs. Roots are mounted only under `/mnt/spaces/{alias}`.
Resource and alias duplication are refused. A request has at most 32 roots;
each resource admits at most 32 live/pending attachments. Joint views require
source EXPORT for cross-resource copying and the same audience-disclosure
admission as explicit transfers.

An attachment can span many executions and parallel application processes.
Multiple writable environment incarnations and Gateway file writes can share a
resource. Applications coordinate their own filesystem/database writes. The
Gateway stages and syncs new files before atomic no-replace publication, verifies
revision and inode before replacing an edit, never truncates in place, and retries
a changing bounded read once. Invalid persisted attachment phases refuse every
request; `pending` and `fence_pending` attachments refuse ADMIN only. Lifecycle
backup, restore, archive, deletion and acceptance of current state still require
all attachments fenced, even after the attachment adapter is lost. Generic
mediated RW attachment is always denied.

Host writing workflows take private exclusive locks without waiting. Reading
workflows take shared locks and wait at most five seconds across their requested
resources, outside SQL transactions; current grants, generation and backing
identity are checked again before the callback. Ordinary WRITE/READ/EXPORT
admission does not probe these locks; ADMIN/OPERATE does. Direct filesystem
writes provide no cross-container writer exclusion.

Capacity errors before publication raise `SpaceFull`, a definite file conflict,
and leave no public entry or stage file. Errors after publication, including
directory-sync failures, retain an uncertain pending intent for recovery.

The `DockerStorageAdapter` requires a local Linux Docker endpoint with a
root-owned canonical Unix socket. Existing `/run` and `/var/run` aliases must
identify the same socket, and replacement during qualification is refused. A
UID-1000 read-only, network-isolated helper reads an unpredictable nonce file
through Docker's bind mount to prove that the daemon sees the same pool; the
same test runs against each bound view before preparation. Exact engine identity
and endpoint are rechecked without repeating the nonce on lifecycle calls.
Remote Docker, Docker Desktop, alternate endpoints and writable configured
mounts remain unsupported. Qualification can run from a containerized Gateway;
matching pathname strings alone never prove matching bytes. Restricted-network
or read-only configured-mount execution is refused at prepare/start/resume until
its topology is integrated; discovery and fencing remain available.

The existing `LocalContainerBackend` builder can prepare a stopped container.
Its exact immutable ID commits before activation checks or start. Inspection
requires the admitted roots/modes, private bind propagation, attachment/host
labels, no privileges/devices/host PID or network namespace, dropped capabilities
except the existing small image-startup set, no privilege escalation and no
restart policy. Images, Gateway configuration, other homes and container-control
sockets are outside the view. A trusted caller cannot start a pending environment
independently of the attachment service.

Retirement durably marks containment pending, removes the exact owned container
and confirms absence, closing child processes and RO handles as well as writers.
An unknown daemon, missing preparation identity, wrong ownership or host change
leaves takeover and conflicting mutations unavailable. Lease expiry and a stopped
but restartable container are never retirement proof. Captured-ID retirement
retries cannot stop a newly attached replacement. Provider disappearance preserves
these facts and never restores a host edit window. Normal run completion does
not retire an attachment.

## Backups, restore and lifecycle

Native-space administrators can open **Backups and recovery** in the generic
browser. Each operation requires current authority and the resource generation.
Backup and lifecycle actions retire attached environments first. The filesystem
must be quiesced, including SQLite writers; a changing SQLite file is never
copied as a consistent database. Quiesced backups include database journals/WAL,
ordinary binary files, dotfiles, internal repository metadata and links. Special
device/FIFO/socket nodes and archives over 100,000 entries are unsupported.

Backups live in private control storage and are labelled `quiesced-filesystem`.
They, staged replacements and retained displaced trees all count against the
same fixed byte/inode capacity. Insufficient restore headroom fails without a
capacity bypass. A backup is file data, not a snapshot of memberships or platform
metadata. Backup artifact identity is recorded before filesystem work.
Lifecycle intent and its captured attachment set commit before any retirement;
concurrent duplicate requests cannot retire a newly attached replacement.

Restore verifies the selected resource's backup, stages and validates its tree,
preserves the resource/root identity, advances its lifecycle generation and uses
current grants. It never reinstates removed members. The quiesced host publication
is serialized but is not an atomic multi-file filesystem transaction. Uncertain
publication/SQL outcomes remain pending, with displaced bytes retained. Only after
confirmed SQL completion does separately recorded cleanup reclaim its private
stage; failed cleanup stays visible and consumes capacity.

Archive preserves readable data while removing effective WRITE/OPERATE. Current
administrators can still revoke archived memberships. Delete is separate: it
fences access, removes visible bytes and retains a permanent tombstone, used
backing identity and private retention data/backups. Deleted capacity is never
silently reused. Physical retention/purge remains an explicit operator policy;
delete does not promise secure erasure or automatic pool reclamation.

Pending file operations have an ADMIN recovery view even while file access is
paused. After inspecting bytes and provider evidence, the owner can explicitly
accept the current state. This marks the old request failed with an attributed
resolution; it neither replays it nor claims its intended change completed.
Partial lifecycle reconciliation advances the generation. Required mediated
recovery controllers cannot be bypassed by this native recovery surface.

The additive `0032_storage_lifecycle` migration adds attachment, mount and backup
facts after `0031`, preserving previous ancestry and all existing feature bytes.
Used containment/backup facts cannot be downgraded away. The mandatory native tier
also qualifies restricted nonhuman wiki access, actual Git operations and SQLite
transactions at capacity; see the [consumer evidence](STORAGE_SPACES_CONSUMER.md#qualification-evidence-and-feature-independence).


## Default first-party features

Enabling `storage_spaces` loads three ordinary extension entries by default:
`deerflow.features.plugins:install_my_files`, `:install_shared` and
`:install_projects`. Explicit entries in `plugins` override their defaults,
including `enabled: false` to omit a feature. Each supplies a resource
action and the existing app-scoped service lifecycle. Shared is host-written and
supplies neither a mediated controller nor a public recovery action. There is one loader and no feature installer.

My Files and Projects look up the same assigned user resource through
`("hm.user-space", "human:<id>")`; Shared looks up
`("hm.company-space", "company")`. There is one role link per resource,
persisted atomically with assignment; feature requests never allocate or replace
it. The table keeps its shape and unique resource constraint, with no migration.
Rename, restart and feature disable preserve IDs. Missing assignments answer 503
`storage-unavailable` with `space-not-assigned` and a public message; revoked or
deleted assigned resources retain the ordinary inaccessible outcome. Backing
verification failures answer 503 `space-unverified`, with no private host path.
My Files, Shared and Projects use `data/files`, `data/shared` and `data/projects`;
private state uses `control/my-files`, `control/shared`,
`control/project-staging/<id>` and `control/project-conversion`.
Projects remain owner-filtered conversation groups
and shelves: sharing a resource does not broaden project metadata audiences.
Instructions/indexes remain transient, untrusted and resolved at run admission.
Shelf content is hash checked in resource mode. Retention purges referenced trash
under its existing rules and collects private staging; it never deletes ordinary
native files as apparent feature orphans. Project archive/restore updates project
metadata without permanently archiving/tombstoning its backing resource.

Qualified Gateway startup creates the native Company Space with its company role,
without a human actor, grant or event. Creation is serialized and bounded by the
remaining startup budget; timed-out work is retained and drained on shutdown.
Later qualification rechecks retry failed company provisioning. An existing
inactive or incompatible role is retained and reported, never replaced.
New-account requests assign the personal user Space after committing the account,
then grant company READ/EXPORT independently. Setup-pending accounts qualify;
allocation or grant failures are logged and preserve successful account creation.
The assignment is idempotent and retains incompatible or inactive role links.
Startup and each 60-second pass repair offline-created accounts and failed
assignments, in account creation order with ID ties. Company grants repair even
when no User slot remains; released/refused accounts receive neither. Each owned
maintenance task survives a startup timeout and drains before database teardown.
Trusted hosts grant resolving, unreleased humans READ/EXPORT; setup-pending
accounts remain eligible. Company custody itself grants no access. Shared uses
those permissions for publication/removal, including READ/EXPORT on a My Files
source. Generic writes and writable resource attachments remain denied to those
members, including administrators.

Typed Compose plans enforce account seats in the repository's creation
transaction, including when the pool is unavailable. Disabled accounts still
occupy seats; released accounts do not. The allowance uses prepared user slots
and the configured count, retaining existing occupancy when the count is lowered.
An empty database can always create its first administrator even with no prepared
slots. Other creation requests receive `no_seats_left` at capacity: registration
returns 403, administrator creation 409, and SSO returns its refusal redirect.
The offline `python -m app.gateway.auth.add_user --email ADDRESS` command reads
the same allowance from inventory without starting Docker; refusal exits 1 and
reports `allowed` and `in_use`. Storage without a typed plan retains ordinary
account creation.

The private repository callback `seats_allowed(session)` borrows the already
locked creation transaction. It must not commit, close the session, or open a
second connection; the count and insertion share that transaction. Startup binds
the actual account-creation repository and clears only its owned callback.

The operator can disable local accounts as well as provider identities. Local
disablement binds the immutable account ID, so changing or reusing its email
does not transfer that fact. Local accounts do not acquire OAuth fields.

```bash
python -m app.gateway.auth.accounts disable --email ADDRESS
python -m app.gateway.auth.accounts release-seat --email ADDRESS
# Retry cleanup after the address has been released:
python -m app.gateway.auth.accounts release-seat --id ACCOUNT_UUID
```

Release requires current disablement and atomically records `released_at`, releases
the email, and marks the exact owned user Space deleted with one attributed host
event. Account rows and email provenance remain. Session and running-work cleanup
follows commit. Exit 2 with `cleanup_pending: true` reports committed release with
unconfirmed cleanup; retry by ID preserves the timestamp and event while retrying
cleanup. Released accounts cannot be enabled or reclaim their email through stale
credential writes. Enablement changes an entire provider identity: if any covered
account was released, it refuses before changing disablement or held work.
The existing `released` operator field still denotes email release; `released_at`
distinguishes a seat release. Physical volume emptying and AI-assistant release are
subsequent integration changes; a released slot is not yet recycled.
Privileged personal MCP discovery and cached calls also recheck current account
eligibility, so a retained administrator role cannot restore disabled access.

The kernel now supplies explicit host transactions and assignment helpers.
`host_locked` locks scoped parents in sorted order without resolving a human
grant or invoking the ordinary operation guard. Private actorless company
creation writes no human grant or event. `host_grant` requires a resolving human,
nonzero mode-compatible permissions and every attachment fenced; its optional
admission callback runs inside the transaction before writes or a no-op result.
The grant, generation and attributed event commit together.
Host-only provisioning admission runs in the allocation transaction before slot
selection or layout. App-owned assignment and release serialization prevents a
concurrent post-commit helper from assigning storage after a seat is released.

An explicit host-write workflow admits the human with READ/EXPORT and records
`by: host`, while requiring an active native destination and rejecting unresolved
destination intents. Only its exact reconciliation intent is exempt; source-only
reads remain available. This is a host API, not a browser/extension capability.
The bundled Shared dispatcher rechecks the exact native company-role assignment
in every admission, including completion after effects. It retains the original
human attribution together with `by: host`.

Compatibility Keep/Publish/Remove calls use the installed enabled service plus
current resource permissions. Publication from My Files also requires source
READ/EXPORT and disclosure authority. Publishing explicitly discloses that file
to the Shared resource audience. The original copy, live-byte deduplication,
conflict suffixes, provenance and publication-ID-fenced Undo are retained. Undo
removes only fresh publications from the operation; it is not file version rollback.

Copy stages, removal payloads, conversion temporary files and bounded host receipts
live under the qualified private control root, on the same byte/inode-limited
filesystem and outside every generic data view. A domain workflow commits its
intent before effects and holds private filesystem locks while its domain
repository uses independent SQL transactions. It drains cancellation. Reads do
not hold a SQL writer across domain callbacks or client streaming; the stream
opens one confined inode under fresh resource admission. Read-only requests never
reconcile pending removals.

A lost publication acknowledgement first checks its preassigned exact publication
ID. Committed records preserve their bytes. Unknown outcomes keep bytes and an
operation-bound private receipt. The private host-only
`settle_publication(intent, session=...)` helper requires the exact pending intent,
verified company-role volume, persisted receipt and fenced attachments under an
owned workflow lock. It never resolves or reinstates the original publisher.
Its publication repository borrows the caller's transaction and flushes without
committing or closing it. The caller completes the intent in that transaction and
removes the receipt only after confirmed commit. Rollback/cancellation retain the
pending intent and receipt; insertion rollback preserves published bytes. Removal
reconciliation finishes or restores staged bytes according to its already committed
publication record. The automatic caller arrives in a later change.
No public recovery action or generic native recovery downgrade bypasses this. Live host-operation locks
prevent recovery from declaring a still-running callback retired. Disabling or
removing the feature keeps resources, grants, rows and bytes; affected behavior
is unavailable, while settled generic reads/export obey the persisted core floor.

AIO thread mounts and local path mappings do not implement qualified resource
attachment admission/fencing and explicitly reject resource mode. They never
mount legacy files/shared directories as fallback. Trusted consumers may use the
qualified direct Docker attachment API described above; the neutral extension
facade continues to report native attachments unsupported.

The host path foundation now accepts a pure, opt-in `Paths.thread_root_resolver`.
It routes conversation directories below a selected data root, preserves the
existing leaf layout and uses `0770` for projected thread directories. It adds no
authority or backing qualification; creation pins the existing root and refuses
symlinked or foreign-filesystem descendants. A trusted integration must qualify
the backing and admit current authority
before supplying roots and ensure Docker sees the same source paths. The Gateway
does not install this resolver yet. Coordinated chat admission, mounts, reuse
validation and full-files refusals are the next integration change.

The additive `0033_storage_features` migration creates
`hm_storage_feature_links`. Used relationships cannot be downgraded away.
Configuration remains version 53; no legacy-directory adoption is implemented.


Host maintenance settles interrupted write/put/mkdir/rename/remove intents from
bounded confined proof, with every attachment fenced and exact current backings.
It never replays bytes or invents a human actor. Live direct writes and feature
workflows retain private locks through their final commit. Domain callbacks bind
after storage initialization even when extensions start first. Shared uses its
borrowed publication transaction; other installed feature callbacks confirm exact
private receipts. Missing receipts, unavailable callbacks and failed inspections
remain pending. Historical intent generations need not equal today's generation;
the intent and current binding snapshots must remain exact. Receipts and unused
generic stages are removed only after confirmed completion, through pinned control
descriptors, with unrelated stages/links/directories preserved. An unresolved
operation on the same Space preserves generic stages. Copy/lifecycle/unknown
intents require operator inspection. HTTP 409 `storage-busy` carries `cause` as
`workflow` or `lifecycle`; provenance never changes the actor or mandatory grants.

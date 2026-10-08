# Storage Spaces

## Resource registry

Storage Spaces is HartMesh's persistent folder resource foundation. The resource and file
delivery adds identity, custody, mandatory access and a generic browser/API.
Existing My Files, Shared and Projects continue through their current routes
and mounts. Native attachment and recovery adapters are described below;
feature extraction and the Agent Execution consumer qualification remain pending.

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

Restart the Gateway. Invalid mounts, mismatched identities, missing reserve or
memory database mode refuse startup. `/api/features` reports the service that
actually started, rather than a subsequently edited config. This startup-only
section is new in config version 53.

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
edits require an exclusive window: an existing native attachment must be
retired first. The default Gateway has no native execution consumer; a trusted
host consumer installs the separately qualified attachment adapter.

Browser saves use the loaded SHA-256 inside an exclusive host edit window. A
stale file, generation or grant fails without silently overwriting or merging
the draft. This window is not a general concurrent shell/SQLite transaction
protocol. Explicit cross-space copy requires source READ/EXPORT and destination
WRITE. A broader destination audience additionally requires source ADMIN and
explicit disclosure acknowledgement. A read grant alone cannot disclose bytes.

## Resource API

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
uncertain, subsequent access/metadata mutations fail visibly pending recovery.
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
There is one writable environment incarnation per resource, rather than one
task mutex. READ mounts can coexist; applications retain responsibility for
their own filesystem/database coordination. Host browser edits require all
attachments to be retired. Generic mediated RW attachment is always denied.

The initial `DockerStorageAdapter` supports only a direct Linux host whose local
Unix Docker endpoint and root-owned daemon PID establish the same mount namespace
as the Gateway. Kernel UNIX socket diagnostics bind the current canonical socket
inode/device to the listener held by that verified daemon, including inherited
systemd listeners; the `/var/run` alias must identify the same socket. It refuses
ambiguous/replaced listeners, alternate/proxied Unix sockets, remote Docker,
Docker Desktop, namespace-remapped Gateways and unrelated configured mounts.
Such deployments need a separately qualified daemon-side backing-identity adapter;
matching pathname strings do not prove matching bytes.
The local builder helper also rejects restricted-network providers; their
network controller must be integrated before attaching these resource views.

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
Used containment/backup facts cannot be downgraded away. Feature facade/plugin
extraction and final wiki/repository/nonhuman qualification are later stages.

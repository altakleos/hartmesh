# Storage Spaces consumer handoff

The consumer contract is extension storage API **1**, shipped by
`deerflow-extension-api` **0.2.7**. Persistent resource IDs, current typed
principals and explicit grants are independent of chats and optional feature
plugins. This contract supplies storage to a future execution system; it does
not define agent creation, memory, scheduling or service hosting.

## Bind identity at the trusted host boundary

`StorageProvider.current()` returns a `ResourceStorage` handle for the current
host actor. Actions, tools and services receive the existing host provider;
standalone embedding code supplies `HostStorageProvider`. Handles recheck their
current actor, active directory record, resource grants and generation. A
captured handle cannot move to another account or survive principal retirement.

For nonhuman access, the host must configure
`HostPrincipalResolver(human=human_lookup, nonhuman=nonhuman_lookup)` on its
`SpaceRegistry`. Each trusted lookup returns an active `ResolvedPrincipal`
whose reference exactly matches the requested typed subject, or rejects it.
`can_provision_company` is a host capability, not an inferred admin label.
Trusted authentication/embedding code binds the verified reference with
`storage_actor_scope`; request JSON, actor IDs and caller-selected user/thread
aliases do not authenticate it. No human user or AgentInstance row is required.

The Gateway configures human resolution and the persistent agent-instance
directory when Storage Spaces is enabled. Instance creation and management are
described in [Agent Execution](AGENT_EXECUTION.md); conversation execution binding
is a subsequent stage. Other embedding consumers supply their own nonhuman
directory and trusted authentication adapter. Advertised
`actor_kinds` includes `nonhuman` only when that resolver exists. First-party
My Files/Shared/Projects convenience features remain human workflows. PATs have
no storage scopes, and ownerless internal requests are rejected.

## Use the public asynchronous contract

Negotiate `PluginContribution(api_version=4, storage_api_version=1)` when
using storage in an installed plugin. Older plugin contracts retain their
existing behavior. Check `provider.capabilities.available` and the requested
capability; absence raises `StorageUnsupported` instead of returning empty data.
The [consumer example](../examples/storage-consumer/consumer.py) uses only the
public extension API and needs no scheduler:

```python
from uuid import uuid4

storage = await provider.current()  # The host already authenticated this caller.
resource = await storage.get(space_id=retained_space_id)
operation_id = uuid4().hex
# Retain resource.generation, this operation ID and its exact request with the intent.
reference = await create_file(
    provider,
    resource=resource,
    path="home.md",
    content=b"# Home\n",
    operation_id=operation_id,
)
```

The returned `ResourceReference(space_id, path, revision)` is resource relative.
Its optional SHA-256 captures those bytes; a later ordinary `read` returns live
data and does not implicitly assert that revision. Keep captured hashes when a
consumer needs provenance or compare-and-replace. Ordinary edits use
`write(..., expected_sha256=captured_sha256)` and preserve the same resource ID.
Subdirectories, dotfiles, internal links, binaries, Git repositories and SQLite
data require no Keep operation or result manifest.

| Surface | Supported contract |
| --- | --- |
| Discovery, provisioning, metadata | `list`, `get`, `provision`, current `StorageResource` facts |
| Ordinary files | `read`, `list_directory`, `write`, `mkdir`, `rename`, `remove`, `quota` |
| Disclosure | `export_file`, `copy` with typed source/destination references |
| Membership | `grant` with validated `StorageActor`, current generation and existing-data acknowledgement |
| Lifecycle | Native `backup`, `restore`, `archive`, `delete`, `recovery_status` |
| Controlled behavior | `mediated_*` requires OPERATE and the exact installed/enabled controller |
| Native environments | Separate trusted host `SpaceAttachments` API; facade `native_attachments=False` |

Permission bits are READ=1, WRITE=2, OPERATE=4, ADMIN=8 and EXPORT=16.
Native WRITE includes READ; mediated resources never accept WRITE. Company
custody grants no ambient audience and survives a creator/member leaving.
Copy requires source READ/EXPORT and destination WRITE; broadening destination
readers additionally requires source ADMIN and explicit disclosure acknowledgement.
Joint native views apply the same audience rule. An execution consumer must also
select memory/context that is compatible with the entire mounted audience.

## Outcomes and consistency

| Public outcome | Caller response |
| --- | --- |
| `StorageIdentityRequired` | Establish a currently validated host binding |
| `StorageAccessDenied` | Current grants, custody or required controller deny access |
| `StorageConflict` | Resolve stale generation/revision or conflicting resource state |
| `StorageOperationPending` | Inspect recovery; retain the exact intent and avoid a new automatic mutation |
| `StorageUnavailable` | Provider cannot establish backing facts; preserve uncertainty |
| `StorageUnsupported` | Capability or qualified adapter is absent |
| `FileNotFoundError`, `ValueError` | Ordinary missing file or invalid request shape |

File mutations retain durable operation IDs bound to actor, generation and exact
request. A completed identical retry returns its recorded result. An unknown
outcome stays pending; changing the operation ID does not authorize replay.
File contents do not advance metadata generation. Restore and lifecycle/grant
changes use current generations and do not restore obsolete memberships.

Host file edits need an exclusive window after native attachments are fenced.
SHA-256 save checks are valid inside that window; they do not coordinate arbitrary
concurrent shell writes. Database applications own transactions/locking. Reads
open confined descriptors under current admission and release SQL before streams;
an admitted inode read is not a snapshot of concurrent native/database writes.

## Native environments and recovery

`SpaceAttachments.attach` accepts a host-validated actor, execution-incarnation
UUID and `ResourceMount` entries with resource IDs/current generations, safe
aliases and explicit RO/RW modes. The consumer records returned immutable
attachment identity. It must not start prepared containers independently, create
another writer after an uncertain outcome or replace approved roots with paths.

The qualified `DockerStorageAdapter` uses a verified direct **single Linux
Docker host** and canonical local Unix daemon endpoint in the same mount namespace
as the Gateway. Remote/proxied Docker, Docker Desktop, namespace-remapped hosts,
unrelated mounts and restricted-network builders are unsupported. The trusted
builder creates a stopped container; exact identity commits before activation.
At most 32 roots belong to one attachment and 32 live/pending attachments to a
resource. One writable environment incarnation may use a resource across many
asynchronous calls and application processes. Ordinary task completion does not
retire the environment.

An ADMIN caller uses `retire` with captured attachment IDs. Exact container
removal and confirmed absence fence readers, writers and child processes. Lease
expiry, elapsed time and a stopped but restartable container are insufficient.
Unconfirmed containment blocks takeover, conflicting edits and completed write
revocation. No cross-host HA guarantee is provided.

Native backups are `quiesced-filesystem`: all admitted environments are fenced
before copying the ordinary tree, including SQLite WAL/journals. Application
SQLite backup APIs may produce their own ordinary files; the platform never
labels a live raw database copy consistent. Restore verifies/stages bytes,
preserves resource/root identity, advances generation and applies current grants.
Unknown publication/SQL outcomes keep displaced bytes and block further work.
Mediated recovery needs its installed compatible controller; generic native
recovery cannot bypass it.

## Backing and capacity limits

The supported backing is a provider-prepared fully allocated **fixed ext4
filesystem**, with verified image/loop/mount identity on the private ext4 data
disk. Configure its trusted inventory before Gateway startup. No ordinary-folder
or legacy-adoption fallback qualifies a resource. Provisioning consumes a new
empty prepared slot; deleted backing bindings and tombstones are not reused.

The filesystem bounds bytes and inodes for data, private staging, snapshots,
retained backups and displaced restore trees together. The provider retains a
separate platform byte/inode reserve; displayed capacity excludes filesystem
overhead. Growth, backups and atomic replacements can fail at capacity. A restore
needs headroom on that same filesystem. Physical purge and new inventory are
operator policy. Individual host write/import/copy requests are limited to
64 MiB; the text editor opens at most 1 MiB. Backups admit at most 100,000 entries.
Absolute links, escaping paths, special file nodes and nested mount transitions
are unsupported by host confinement. Relative links must stay inside their root.

## Qualification evidence and feature independence

| Requirement | Executable coverage |
| --- | --- |
| Async validated nonhuman actor, disclosure, stale calls, retirement and company custody | `test_storage_spaces_consumer.py` on SQLite/PostgreSQL; functional injected catalog |
| Restricted company wiki, Markdown/source refs, processing script and SQLite index across native replacement | `test_storage_spaces_volume_native.py::test_qualified_company_wiki_nonhuman_actor_survives_native_replacement` |
| Real Git init/commit/branches/checkout/fsck, native tests, binary and relative link | `test_storage_spaces_volume_native.py::test_real_git_branches_tests_and_internal_links_on_qualified_backing`; Git runs on the host, native edits/tests in containers |
| SQLite locking/rollback, committed-data integrity at real ENOSPC | `test_storage_spaces_volume_native.py::test_sqlite_transactions_and_committed_rows_survive_real_disk_exhaustion` |
| Actual source-chat deletion and zero-feature retained reads/edits | `test_storage_feature_routes.py::test_saved_bytes_survive_actual_thread_deletion_and_optional_feature_removal` on SQLite/PostgreSQL |
| Kernel byte/inode bounds, reserve, private growth, exact fencing and quiesced WAL restore | Existing cases in mandatory `test_storage_spaces_volume_native.py` |
| Test-fixture creation acknowledgement loss preserves its backing until exact absence | Native real-create-then-error case and `test_storage_spaces_native_fixture_ownership.py` |
| Missing controller, private state and current grants | Existing facade, workflow, feature-control/publication and attachment tests |

The mandatory Linux Docker merge job executes all 15 storage cases plus the
instance AIO persistence case (16 total) and rejects
skips. Ordinary directory/SQL fixtures prove functional contracts, not native
mounts or quotas. Unsupported contributor hosts skip only the separately
qualified tier and must not self-attest native readiness. The complete default
offline suite runs in four CI shards; strict blocking-I/O, frontend/isolation and
both production Docker acceptance configurations remain required gates.

Removing My Files or an optional wiki view preserves IDs, files and core grants.
Removing Shared's required controller preserves mediated/read-only enforcement
and denies its affected mutations/recovery. Source chat deletion removes neither
saved resources nor publication copies. Principal retirement rejects delayed
calls while other authorized company members retain their resource and bytes.
No agent runtime, scheduler or legacy-folder adoption is needed for this handoff.

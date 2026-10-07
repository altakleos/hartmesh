# Storage Spaces

## Resource registry

Storage Spaces is HartMesh's persistent folder resource foundation. The first
delivery adds host-side identity, custody and access contracts. Existing My
Files, Shared and Projects continue through their current routes and mounts.
Generic file APIs, browsing, hard quotas, qualified attachments and recovery,
and feature extraction are subsequent delivery stages; this first delivery
does not claim the Agent Execution consumer contract is ready.

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
and writable attachment replacement will additionally require actual writer
fencing; metadata changes and expired leases are insufficient.

The additive `0030_storage_spaces` migration creates `storage_spaces`,
`storage_space_grants` and `storage_space_events`. Existing data is not moved or
reinterpreted. The migration verifies pre-existing table shape and refuses a
downgrade after these tables hold resource identity, grants or events. Normal
Gateway startup applies the migration through the existing bootstrap.

## Linux backing development

The host filesystem adapter is available for provider integration. It does not
yet expose a product file API or writable runtime attachment. It supports
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

This development environment cannot mount the disposable fixture and therefore
does not qualify the backing. The dedicated Linux Docker merge gate requires
all native byte/inode, staging, backup, visibility and restart checks with zero
skips before support can be claimed. File/resource APIs, attachment fencing,
recovery and consumer readiness remain subsequent implementation work.

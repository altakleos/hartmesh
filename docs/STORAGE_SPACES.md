# Storage Spaces

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

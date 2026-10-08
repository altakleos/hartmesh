# Storage Spaces

`spaces/` owns generic resource identity and mandatory access, independent of
chats, agent definitions and feature installation. `storage/` remains the
separate content-addressed blob store. Product workflows do not belong here.

The registry is **host-only**. Bind its actor from authenticated host context
or a trusted nonhuman adapter; never deserialize an actor from a browser request
or impersonate an owner through `user_id`. `current_human_principal()` refuses
missing context, and `HostPrincipalResolver` rechecks active identities. Human
and nonhuman keys include their kind. A directory lookup validates existence;
it does not authenticate whoever supplied an ID.

Persisted facts live in `persistence/spaces/`: stable UUID resource/backing
handles, personal/company custody, mode, optional opaque controller binding,
generation, explicit grants and typed authority events. A company resource has
no creator/member foreign key and receives no ambient company-readable grant.
Personal custody may be provisioned only for the acting principal. Company
provisioning requires a trusted host capability, independent of admin labels.

READ, EXPORT, WRITE, OPERATE and ADMIN are explicit. Native WRITE requires READ
because a normal writable filesystem view can read existing bytes. Granting
new READ/EXPORT requires acknowledgement of existing data. ADMIN changes
resource facts; it does not grant native writes. Mediated resources require a
binding and never accept WRITE. A binding is opaque data, not evidence that a
controller is installed, compatible or authorized to mutate files.

Every metadata mutation reserves SQLite's writer before reading or locks the
Postgres parent row before grants. Revalidate actor/positive grant target inside
that boundary, validate persisted facts before using authority, compare the
expected generation, and commit the new generation with its event atomically.
Retired members remain revocable; retired administrators cannot satisfy the
last-active-administrator guard. No optional authorization setting bypasses
these checks. Unknown or malformed persisted facts fail closed.

`SpaceFiles` binds a qualified empty inventory slot atomically to resource
creation. Slot/UUID uniqueness prevents duplicate allocation; used bindings
are never recycled. Root inodes persist across Gateway restarts. `admitted()`
locks all scoped parents in sorted order and rechecks authority before I/O.
File intents commit before filesystem work; uncertain publication/SQL outcomes
block further operations and metadata changes. Completed IDs bind exact actor,
generation and request. Drain the entire owned mutation before cancellation.
Cross-space copy needs source READ/EXPORT, destination WRITE, and source ADMIN
plus explicit acknowledgement when destination readers broaden the audience.

Host edits require an exclusive window with no native attachments. The host-only
attachment service reserves durable intents, commits exact container identity
before activation, and fences by confirmed removal on one direct Linux Docker
host. Missing/foreign identity, unknown containment and adapter loss stay pending;
leases never prove retirement. Reject remote/remapped daemon namespaces and extra
mounts. Joint views require audience admission. No chat or task lifecycle owns it.

Native recovery quiesces all mounts, records backup identity before I/O and
validates archived trees before publication. Restore keeps root identity/current
grants and advances generation. Retain displaced bytes until SQL completion;
separately recorded cleanup failures consume quota. Archive is readable; delete
keeps tombstones/bindings/retention data. Owner-accepted uncertain state is failed,
not replayed or claimed complete. Mediated recovery requires its controller.

`filesystem.py` uses Linux openat2 BENEATH/NO_MAGICLINKS/NO_XDEV and owned
descriptors; ordinary dotfiles/internal relative links remain data. Private
same-filesystem staging is outside the view. Browser hashes require an admitted
edit window; this primitive grants no writer exclusion. `backings.py` verifies
operator-prepared fixed ext4 images/mounts and platform reserve, rejects directory
fallback, and compares opened root incarnations. Native disk-limit qualification
is a separate no-skip CI tier; unsupported hosts never self-attest readiness.

Gateway actors come from authenticated request identity or an actually
attributed trusted internal owner. PAT storage scopes remain unsupported;
auth-disabled development uses its explicit adapter, never absent context.
Generic mediated writes fail. HTTP ranges/MIME use owned confined descriptors
opened under current admission. Release SQL before streaming the owned inode;
slow clients must not reserve SQLite's global application writer. Open only
once and drain reads before close. Active content downloads; reads never cache.

Tests: `tests/test_storage_spaces_*.py`, migrations `0030`–`0032` and the
separate mandatory native-volume Docker tier. Append new Alembic revisions;
never change shipped ancestry or erase used custody/grant/event tables.

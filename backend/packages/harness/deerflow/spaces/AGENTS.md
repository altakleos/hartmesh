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

The initial registry is not a file/mount/lifecycle implementation. It exposes no
archive/delete/restore completion or writer-stop claim. Later adapters must
fence actual writers before completing those operations; lease expiry alone
never proves retirement. A metadata generation is a stale-request fence, not
a filesystem fence. Files, mounts, quotas and feature migration require their
own qualification before consumer readiness.

`filesystem.py` uses Linux openat2 BENEATH/NO_MAGICLINKS/NO_XDEV and owned
descriptors; ordinary dotfiles/internal relative links remain data. Private
same-filesystem staging is outside the view. Browser hashes require an admitted
edit window; this primitive grants no writer exclusion. `backings.py` verifies
operator-prepared fixed ext4 images/mounts and platform reserve, rejects directory
fallback, and compares opened root incarnations. Native disk-limit qualification
is a separate no-skip CI tier; unsupported hosts never self-attest readiness.

Tests: `tests/test_storage_spaces_contracts.py` and
`tests/test_migration_0030_storage_spaces.py`. Append new Alembic revisions;
never change shipped ancestry or erase used custody/grant/event tables.

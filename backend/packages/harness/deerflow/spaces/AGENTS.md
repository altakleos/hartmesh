# Storage Spaces

Ordinary chat routing uses private `chat_paths.py`, `chat_bridge.py` and
`chat_snapshots.py` adapters. `ChatStoragePaths` carries trusted role/root facts
and exact mount projections, never a cached grant. The Gateway owns async
qualification, closes SQL before filesystem verification and rechecks current
identity/roles/grants/bindings afterwards. No qualified User means no data/ACP
mount or legacy data-directory fallback. Company unavailability omits Shared
alone. Policy views and acquisition locks are operational state outside Spaces.
Host effects refresh admission and use pinned confined descriptors; ordinary
chat containers need no attachment row or per-chat probe. Do not infer permission
from a path projection, container name or cached client. Unsupported storage
credentials remain denied by the facade even when their active human identity
can run a text chat.

`provision.py` owns operator preparation and whole-plan byte/inode arithmetic.
The legacy script imports its unchanged entry point. Typed schema-2 inventories
carry user/assistant/employee/company kinds; schema-1 slots remain untyped.
Preparation never runs in a request. The Compose CLI validates confined paths,
holds one pool lock and publishes each completed slot atomically. Retained plus
missing slots must stay within `MAX_SPACES`; lowering counts never erases slots.

Typed `provisioning` keeps inventory order, skips unavailable/nonempty slots and
creates `LAYOUT` only after binding uniqueness flushes. Drain layout workers
before releasing allocation SQL ownership; `assign` drains the entire role-link
transaction. Bound/unclaimed sets retain uncertainty. Same-UUID growth requires
unchanged data/control inodes and nondecreasing bounds, recorded under resource
parent locks from the startup-pinned inventory. Prove bound volumes before taking
the short SQL publication lock, so stalled startup verification never retains
SQLite's application writer. Startup ownership and availability
are documented in [the guide](../../../../../docs/STORAGE_SPACES.md).

`binding_verified` projects facts after closing its SQL session; it grants no
admission. Assignable-slot snapshots exclude bound/unclaimed/unverified slots,
independently of account-seat occupancy. `user_spaces` gates `create` only;
host assignment remains ungated. Availability uses one shared allowance formula.
Host-only provisioning admission runs before slot selection/layout, borrowing the
allocation transaction. It never bypasses custody or principal validation. The
Gateway serializes role assignment with seat release; business rules stay in app.

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

`facade.py` binds the neutral extension API to current host actors. Its methods
take stable IDs/relative paths and never accept a caller-selected actor. Captured
handles cannot cross account context; every operation rechecks active identities
and current grants. Mediated primitives use OPERATE plus the installed controller
binding/settings in both intent and publication admissions. Provider capabilities
are explicit; absent storage/identity never returns an empty successful result.
Host credential admission is inherited by asynchronous work. PATs have no resource
scope: middleware denies facade actor projection and the Gateway omits their run
provider. Setting a typed actor cannot override a denied credential context.

Every metadata mutation reserves SQLite's writer before reading or locks the
Postgres parent row before grants. Revalidate actor/positive grant target inside
that boundary, validate persisted facts before using authority, compare the
expected generation, and commit the new generation with its event atomically.
Retired members remain revocable; retired administrators cannot satisfy the
last-active-administrator guard. No optional authorization setting bypasses
these checks. Unknown or malformed persisted facts fail closed.
Host-only grant admission callbacks add domain preconditions under the same
resource lock; they never bypass mandatory grants or reach browser/plugin input.
Consumers acquire their own SQL parent after the resource parent. A callback
rejection rolls back the grant, event and generation together.

`SpaceFiles` binds a qualified empty inventory slot atomically to resource
creation. Slot/UUID uniqueness prevents duplicate allocation; used bindings
are never recycled. Root inodes persist across Gateway restarts. `admitted()`
locks all scoped parents in sorted order and rechecks authority before I/O.
File intents commit before filesystem work; uncertain file/feature-workflow
outcomes block WRITE/OPERATE/ADMIN, while other pending actions block all access.
Busy refusals carry workflow/lifecycle cause. Completed IDs bind exact actor,
generation and request. Drain the entire owned mutation before cancellation.
Cross-space copy needs source READ/EXPORT, destination WRITE, and source ADMIN
plus explicit acknowledgement when destination readers broaden the audience.

Host edits and multiple native attachments share ordinary filesystem semantics.
Invalid attachment phases always refuse; pending/fence_pending refuse ADMIN only.
Lifecycle effects and accepted state explicitly require every attachment fenced
under the final parent lock, including adapter loss. The host-only
attachment service reserves durable intents, commits exact container identity
before activation, and fences by confirmed removal on one qualified Linux Docker
host. Missing/foreign identity, unknown containment and adapter loss stay pending;
leases never prove retirement. Local endpoint checks retain root socket/alias
identity; a UID-1000 nonce-file probe proves pool and bound-view path coherence.
Private probe directories serialize cleanup through pinned descriptors; only the
controlled first runtime qualification clears the pool. Repeated construction
still probes but preserves live markers. Unsupported workspace topology rejects
prepare and activation/resume while discovery/fencing remain available. Joint
views require audience admission. No chat or task lifecycle owns it.

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

Host cross-session workflows commit intents before domain effects and hold private
filesystem locks outside SQL while the feature repository commits. Every core
ADMIN/OPERATE admission probes those locks. Read workflows wait up to five seconds
for shared locks outside SQL, then revalidate grants, generations, backings and
controller admission; writing workflows use immediate exclusive locks.
Accepting a live callback as retired is forbidden. Direct writes do not flock.
Drain cancellation, preserve unknown results, and distinguish confirmed pre-effect
rejections. Bounded exact-ID receipts live in private control, never data mounts.
Default first-party contributions/compatibility/context adapters are under
`features/` and Gateway `storage_features.py`; see the [feature contract](../../../../../docs/STORAGE_SPACES.md#default-first-party-features).
Migration `0033` persists their opaque unique relationships; no legacy adoption.
Bundled features now look up role links: `hm.user-space/human:<id>` serves My
Files and Projects; `hm.company-space/company` serves Shared. They never call
`ensure`. Namespace folders are under data; staging remains under control.
Missing links have a distinct structured 503, separate from revoked access.
`host_locked` is explicit host authority: sorted parent locks, no principal,
grant or operation admission. Actorless company provisioning uses a private
boundary and writes no human grant/event. Ordinary creation still requires a
typed resolving principal. `host_grant` resolves a human, validates nonzero
mode-specific permissions, requires every attachment fenced and awaits its
callback under the same transaction, including no-op grants.
Host-written workflows require active native destinations and still reject
pending destination intents, despite admitting the human with READ/EXPORT.
Only the exact own reconciliation intent is exempt. The flag is host-only;
bundled Shared uses it without a mediated controller/public recover action.
`admit_in_session` borrows an active host writer without starting/committing or
closing SQL and retains the ordinary resource, generation and operation guards.
Host principal adapters explicitly support the borrowed session: human lookup
loads current account eligibility in it, and the instance directory checks its
current owner there. Never silently drop identity checks or perform another pool
checkout while the owning writer is held. Source-only User role verification
can require READ/EXPORT; ordinary chat projection keeps READ/WRITE.
`settle_publication(intent, session=...)` requires an owned workflow, exact pending
intent snapshot, verified company receipt and fenced attachments. Borrowed
publication repositories flush without committing/closing the caller; completion
and receipt cleanup belong to the caller after confirmed commit. Drain workers
before cancellation releases the host transaction. Periodic settlement is deferred.

Consumer readiness: [the handoff](../../../../../docs/STORAGE_SPACES_CONSUMER.md)
maps the public API to functional SQL and mandatory native evidence. The native
tier requires 16 cases without skips, including real AIO two-chat persistence;
directory fixtures never qualify quotas. `resume` rechecks exact mounts, current
generations, audience and immutable container identity without prepare/start.

`retire` accepts a host-only admission callback after locking scoped attachments
inside its first ADMIN transaction, before pending/effects. Domain conditions add
to mandatory resource checks. A rejected callback rolls back and propagates its
effect-free failure; uncertain provider/commit outcomes remain pending. Never pass
callbacks from HTTP/plugin data or convert an empty captured scope to all-current.

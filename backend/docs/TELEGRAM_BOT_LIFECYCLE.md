# Telegram bot lifecycle

The parts of `app/telegram_bots/` that run a bot rather than a chat: recovery, conversation
preparation, polling, the supervisor, binding, tokens and health, a bot's profile, and strangers. The module guide is
[`app/telegram_bots/AGENTS.md`](../app/telegram_bots/AGENTS.md).

## Recovery

`recovery.py` is the Supervisor-owned bounded scheduler over the publication
coordinator and the outbox sender. Periodic passes observe marked publications only; no
empty/404/absent-task fact authorizes dispatch. `recovery_page` keysets heads,
fetches every global-turn member with a sentinel and holds malformed, mixed,
oversized, legacy or unsupported groups. Limits bound returned groups/origins,
decoded-payload counts and retained operations, not bytes, SQL scan time or RAM.

Actual negative-copy startup takes one retained page before held/create/send
release. `recovery_authority.py` issues an opaque one-shot epoch for that fixed
cohort. Tickets are host capabilities, never UUID/boolean/JSON authority. They
bind original adapter/token/endpoint/current phase/deadline. Retire all tickets
synchronously before release; unseen/unarmed tail stays held. The distinct fixed
guard prepares current account/target outside SQL with no named private or
execution ceilings, then borrows the existing same-engine custody guard with a
last synchronous ticket check. `publication_recovery` is reserved bookkeeping;
exclude it from business input hashes but compare and validate the complete
stored fact. Atomic `plan_restart_before_dispatch` writes that fact, strict fixed
basis3 primary and all route seals together, preserving null run/assistant IDs.
Marker and closure winners serialize; uncertainty never resubmits a model.
Only the exact closed fact/primary receipt confirms a lost writer result.
Prepare fresh zero-ceiling read admission for that confirmation; recheck SQL
custody after decoding, then the borrowed human account and literal original
read fence. The service compares the actual closure's startup ID with its failed
attempt. That correlation is evidence, never a renewed write ticket. A retired
epoch must refuse each startup leaf before a metadata backend is called.
Known model declarations and malformed bases cannot gain fixed privilege.

Service slots include native/codec/response cleanup. An absolute pass deadline
shortens coordinator control before its child is queued. Logical expiry admits
no fresh work; a previously authorized COMMIT can settle late. The startup page
and native cleanup can outlast that deadline. Final stop drains the service
before coordinator/client teardown, retaining dependencies on failure.

## Conversation preparation

Conversation preparation reserves `route_basis.thread_preparation` on an actual
admitted, routed, unsealed input before HTTP. Public routing cannot supply this
server bookkeeping; business replay/group comparisons omit it and snapshots
retain it. Save one purpose, endpoint fingerprint, creation UUID and expected
mapping. The mapping and selected-thread outcome commit together under current
owner/account/link/connection/target/generation authority. Existing conversation
row IDs rotate on every replacement, including the generic connection writer,
so a frozen timestamp cannot hide an A-to-B-to-A mapping change. Expected absence
uses atomic insertion; it makes no claim about arbitrary absent-present-absent
history under unchanged authority.

For persistent targets the host creates the conversation through
`AgentConversations.create` as the bound person, retaining the reserved creation
UUID through cancellation and retry. Only channel provenance is written into its
metadata. Reuse requires the authoritative instance binding as well as a fresh
owner-scoped Gateway read; an unrelated stored conversation is replaced.

## Polling

`polling.py` owns retained suffixes and startup-held batches. A poll failure ends
in a retry while the database is down, a refusal with a reply, or a counted death;
it never just stops. An update still failing after `ADMISSION_GIVE_UP_SECONDS` is
let go through `refuse_update`: a brief fault clears within that minute, and no
list of errors could tell the two apart. `handle_update` raises the handler's
error so that clock sees it, and a plain False is an ordinary retry. Keep
`except Exception` at the death guards: a database going down raises raw driver
errors no list names. An uncertain `authority.target` never writes a health.
Serialize every
getUpdates operation, including probes, drops and shutdown confirmation. A
current-token 409 commits `record_conflict` before counters/callbacks/admission;
failed persistence owns that retry before another poll. Successful old-token
results also retire. Pass the complete absolute conflict transition through the
health callback; a timer alone cannot declare conflict. Held release transfers
ownership without waiting for database recovery.
The poller's complete current-link SQL guard uses `await_drained`: cancellation
must finish its cursor and transaction before retirement proceeds. A canceled
SQLite SELECT can retain a lock even after connection invalidation. Recheck
current poll/token identity after the owned guard; stale work still cannot send
or admit input. This guarantee covers that guard, not every driver operation.
Starting pending admission consumes its observed wake. A fresh wake arriving
during awaited admission remains available to interrupt the following wait;
a serviced held-release signal must not skip that failed suffix's backoff.
`defer_start_retry` transfers the first startup rate-limit observation before
any awaited status guard. Rate waits retain that absolute deadline across guards
and queued sleeper startup; ordinary timed waits retain their entry deadline.
Wake/cancel still joins both timer and wake children. Restore requests one discard
intent with
`require_drop_pending`; `complete_pending_drop` only joins that intent and cannot
schedule another negative-offset discard. Await completion outside the lifecycle
lock needed by health callbacks.

## The supervisor

`supervisor.py` owns the one Gateway-loop runtime. Await `check` before serving,
retain `start` as a task, and use the shared health barrier and ten-start permit
for scan/add/retry/replacement paths. A permit covers construction and the first
SDK start attempt; cancelled constructors keep their result owned until loop-side
cleanup. Captured channel and poll identities fence callbacks even when a retry
uses the same token revision. Preserve death history across poll recreation.
Freeze initial membership/conflict facts before routes are admitted; finalize
actual startup pollers only after every first attempt. One coordinator times its
sticky decision from the first actual poll dispatch. Health readiness and send
admission are separate. Defer notices before claiming metadata and gate control
acknowledgements, profile/takeover mutations and raw delivery. The mandatory
`TransportServices.send_allowed()` must return exactly True after the adapter's
last awaited authority/SQL guard. Retired startup buffers cannot retain Updates
after the decision; live failed admission suffixes remain owned and ordered.
`begin_shutdown` drains producers/pollers/workers while retaining the listener;
the Gateway drains existing channels before `stop` confirms safe offsets,
unsubscribes and closes clients. The Gateway owns the single five-second stop
grace. Only its expiration calls `drain_after_stop_timeout`: retire future latch
retries, wake/drain backoff, then join the active SQL attempt and the same owned
client cleanup before database teardown. Ordinary cancelled stop waiters retain
retry intent. A late successful commit remains durable; an unpersisted latch
gets one fixed diagnostic and keeps local refusal. Already-started work may
outlast grace; no new SQL retry survives the database owner.
Do not install future-stage no-op schedulers or trust manager text as a model
source.

## Instance conversations and policy

Instance conversation preparation uses the existing `AgentConversations` service
as the binding's human owner. Preserve the saved creation identity and drain its
writer before cancellation returns. Current Use precedes execution readiness and
resident admission; `work=False` reserves no workspace. Both the authoritative
Gateway projection and in-process binding must match the target before mapping.
Instance publication captures the original `AgentConversations` service, exact
thread binding and owning engine. Current Use precedes execution readiness and
non-reserving resident admission before enqueue/POST. Retained Telegram content
reads still require an active target, current Use and conversation audience,
but do not require execution readiness or a workspace. Paused/retired targets
retain only fixed status delivery; generic web Inspect reads are broader. The borrowed writer rechecks the
joined link, binding, owner, Use/Inspect provenance and policy/discard snapshot.
The HTTP binding projection must agree; metadata is never an instance identity.
The channel scope carries the instance ID separately from custom-agent names and
adds the owner's stored time zone to human-message context. The browser add route
still refuses instance targets until files and presentation paths are qualified.

`policy.py` stores employee Telegram settings in the existing policy table. Its
reads and upserts drain admitted SQL through cancellation. Browser policy routes
require a session, route ceilings and current Manage plus Inspect; personal
assistants have no policy. The upsert borrows its writer for a fresh human and
locked instance/grant admission on the authority's exact engine; no second
session or provider call belongs in that writer. Counts intersect Use grants with connected links.
List restrictions and privacy indicators include only Use-visible instances,
including those without bots. A view without Manage always has other readers;
for managers, inspect the current grants instead of inferring from custody.

Delivery checks current instance policy at claim, each part and raw transport.
Off skips pending delivery; protected strengthens saved parts before sending.
Instance raw-send fences preflight the live host authority, then read link,
instance, Use grant, custody, policy and discard intent in one final SQL snapshot;
no separate grant/policy refresh can publish an earlier protection decision.
Lifecycle notices retain exact destination/revision and optional expected policy
through SDK limiter waits. Live instance previews additionally need current Use;
protected policies admit no draft, including voice preparation. Optional fixed
lifecycle notices can still explain disabled access, with protection preserved.
Policy-off reconciliation stops polling after its one bounded notice and ends
pending work without consuming callbacks. Restoration drops buffered updates;
policy notice values and transition events prevent repeated announcements.
The policy writer records a unique `notices.policy_discard` intent on every
instance link in the same transaction as off. It locks links in ID order before
the policy row; discard acknowledgement uses link → policy and never takes an
instance/grant lock afterward. Restoration and startup retain that intent through
constructor/SQL failures, end pending work without consuming callbacks, and
confirm the negative-offset result with its positive successor before clearing
only the captured intent. The poll owner retains fetched/confirmed state through
SQL acknowledgement retries; a new intent retires an older poll before admission.

## Binding, codes, tokens and health

Binding saves account ownership, channel identity, generation, event, sanitized
Start identity and ordered welcome/privacy delivery in one unit. Start accepts
only update/message/chat IDs and an optional topic ID. Welcome input parts are
`{source_ref: {key, values}, silent?: bool}`: a target-specific hello first,
optional shared-reader text, then `hello.privacy`, always silent. The repository
allocates durable part identities and stores source references, never raw codes
or updates. Replay consults that saved Start before consumed-code/state checks;
an old binding generation cannot reconnect. Inbox identities include the chat:
`(link_id, message_id, chat_id)` from additive revision 0043. Every inbox lookup
must carry the captured chat; never delete older receipts to admit another
account's same message ID.

Codes store hashes with captured link/conflict epoch/token revision. Every
current-token conflict commits a monotonic epoch and invalidates waiting codes;
stale tokens cannot change it. Exhausted epochs fail closed. Generic `update`
cannot write binding authority, revisions, origin or health; use their dedicated
transactions. Cleanup invalidates delivery claim IDs and old-generation file/
topic metadata. Disconnect-all is idempotent and mints no new authority.

Token wrapping uses `ProviderKeyCipher` with the Telegram label and bot-bound
variable; its default label preserves existing provider-key bytes. Rewrap uses
`store_token(action="rewrap", expected_revision=..., expected_encrypted_token=...)`
and changes neither logical revision nor valid codes. A logical replacement
retires codes/transport claims while retaining saved same-binding deliveries.
Only a positive plaintext comparison with its captured revision may clear the
pause: `validated_bot.token_changed=True` requires `expected_token_revision`.
Ciphertext inequality proves nothing. Token-replacement receipts contain stable
IDs/digests and owed postcommit actions, never a token or connect code. Read an
exact owner-scoped receipt before old-link lookup or remote validation.

Health cause publication holds a per-link lock before SQL and through commit/
session cleanup. Install process-local causes only after success; token repair
uses that same order. `set_health(..., transition=...)` accepts complete absolute
conflict timestamps and an optional hostname atomically. The poller supplies its
full window on first active entry. Omitting transition metadata preserves the
window; explicit null clears the hostname. Ending conflict preserves the action
pause. `conflict`, `token_rejected`, `stopped` and a conflict's end each write a
link event.

## The profile

`profile.py` owns a bot's public name, descriptions, menu and picture. `ProfileSync`
borrows each running client and owns no worker: the supervisor runs `push_owed` at
start and hourly, `add` pushes a new bot, and a saved language marks the person's
links owed in its settings writer, taking links in ID order right after settings.
One keyed lock serializes pushes per link, and each waiter rereads what is owed.
`finish_profile` compares the saved language in the owner writer, so a language
chosen during a push keeps the link owed. The name goes last because Telegram
rations it. A client replaced mid-push is no failure; its successor still owes the
push. After `profile.retry_hours` of failures a link waits for the next start,
because `_failing_since` is process memory, unless its owner acts. `avatar.py` is a
pure drawing function, always called off-loop; it loads the two pinned `fonts/`
files with `Layout.BASIC`, and initials are drawn only when every character has a
glyph and is left to right. Keep the Python and web colour rule in step.

## Strangers

`strangers.py` keeps bounded unexpired reply charges. The supervisor owns one
ledger through adapter/token replacement and bot removal/re-add, and neither
replenishes the 24-hour pair, per-bot hourly or global per-sender bad-code
allowance. Overflow suppresses a notice instead of evicting a live charge; all
state resets on process restart.

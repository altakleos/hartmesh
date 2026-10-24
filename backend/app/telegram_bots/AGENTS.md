# Managed Telegram bots

`config.py` validates the optional `AppConfig.model_extra["telegram_bots"]`
section without starting services. Nested models forbid unknown keys and type
coercion; absence returns fresh defaults with `enabled=False`. Keep this separate
from the existing deployment-wide channel adapter.

Compose's renderer owns enabled/public URL/bot limits/voice provider and the
legacy-channel closure. Until stage one is complete, an absent environment key
omits the section and `show_web_page` tool. See `deploy/compose/README.md`.

Keep bot network, supervisor and router startup outside configuration loading.
Use socket-free transports and synthetic credentials in offline tests.

Product text lives in `texts/` in English and Chinese; keep stage-one keys equal
and copy English wording from the design. `render` escapes values; `render_plain`
preserves them for plain labels/alerts. `markdown` accepts only Telegram markup,
and `split` counts visible UTF-16 units while retaining balanced tags and complete
canonical content. Preview rendering is presentation only: durable receipts,
callback authority and execution gating belong to their owning writers.
Ambiguous details containers display literally. Impossible canonical
split budgets raise `ValueError`; disposable drafts may clip their first part.

`pages.page_address` uses only the configured base, stored target and supplied
identities. Restricted instance tabs need current grants; Memory also needs the
`memory:read` ceiling, and adding an AI employee needs administrator authority.
Web pages still enforce access. No handover is a sign-in capability.

`voice.resolve_transcriber` rereads configured model entries and never obtains
keys from environment variables. Capture one qualifying provider/key/model per
note; requests use fixed official endpoints with no provider fallback. Keep SDK
imports lazy and suppress SDK payload logging only inside the voice context.
The package emits fixed qualification reasons, never keys, audio or transcripts.

`strangers.py` separates touch-only LRU recency from bounded unexpired reply
charges. The supervisor owns one ledger through adapter/token replacement and
bot removal/re-add. Forgetting recency never replenishes the 24-hour pair,
per-bot hourly or global per-sender bad-code allowance. Overflow suppresses a
notice instead of evicting a live charge; all state resets on process restart.

`links.py` owns capacity and connection transactions. Capacity changes lock the
deployment singleton, then owner settings, then links in ID order. SQLite takes
`BEGIN IMMEDIATE` before any read. Retain the writer's initial deployment/owner
counts through nested remove/add helpers so net-zero replacements keep their
place after limits fall. First settings, link, event and code commit together.
Borrowed helpers accept only their owning writer session and never commit or
open another checkout. Connection-only writers never acquire capacity afterward.
The existing channel repository's committing/ownership-transfer helpers are
not suitable inside these units; use the local owner-filtered row helpers.
`BotLink.from_row` captures detached public facts without its credential; use
this factory instead of importing the link module's private snapshot helper.
The managed ingress path supplies the prepared live-owner checker to bind and
unblock. It rereads the shared account repository's derived disabled/released
state through the borrowed writer; config/provider preparation stays outside
that writer. Unblock compares the captured account/connection/token/generation,
records one transition and preserves generation, pause and unrelated deadlines.
Only deliberate wrong-bot consumption commits a refusal outside the bind writer;
ordinary binding failures still roll back code, account, welcome and inbox.

`inbox.py` owns immutable admission/routes, compatible grouping and publication
history. Match the captured chat and generation, and bind runs only to the current
publication. Shared `register_delivery`/`settle_deliveries` keep completion in one
place: a sealed route and every required delivery must be sent/skipped. An
auxiliary receipt never completes unfinished processing. Borrowed helpers share
the caller's writer/session and never own its commit or rollback.

`producer.py` owns reply plans and bounded claim workers. Raw delivery receives
link/outbox/part IDs plus both process/admission fences. Current owner/policy/
target eligibility is mandatory at claim and each part, outside SQL transactions.
Never reclaim a live worker by elapsed time. Already-dispatched transport may
finish during conflict; new parts wait. Saved acknowledgements and presentation
identities survive retries. `OutboxSender.retire_link` drains only that link's
owned workers, fencing claims before cancellation and retaining cleanup through
caller cancellation. Production uses the supervisor's single sender.
Whole-sender shutdown drains every captured worker even if a SQL fence fails,
then raises that original failure. A current in-flight acknowledgement may
settle, but shutdown cannot dispatch a next part. Owned drains can outlast the
caller's grace budget when transport itself suppresses cancellation.
A later tick also repairs finished current-process claims after failed cleanup
SQL, using the process-wide live-admission registry and both saved claim fences.

The adapter queues fixed product replies through public `plan_product`, whose
saved origin set and original source/destination basis cannot change on replay.
The Memory URL-only part freezes a closed page/address separately from callback
controls. Caller-supplied URL fallback receipts are removed from both the replay
basis and parts. A refused button journals the same part's text-address fallback
under current claim/source/scope fences; another bounded attempt sends it.
Current blocked-message 403 keeps its row pending/due without transient backoff;
the blocked state owns that wait, leaving other retry/pacing floors unchanged.
`capture_sending_part` returns only the first required sending part under the
current token, binding, presentation and both claim fences. Re-capture after
awaited source preparation, await the same mandatory current-authority
collaborator as the sender, then capture again before dispatch. A wait, refusal,
changed binding/token/adapter or newly required protection retires admission;
the next sender pass persists any strengthened protection before transport.
No ORM row or SQL transaction crosses transport.
`fallback_to_plain` preserves that part's identity and source and schedules one
later attempt. `DeliveryClaimRetired` exits normally and never becomes a payload
failure. Raw `send_part` returns acknowledgement IDs and calls no planner.
`defer_token_verification` journals only the closed classification continuation
of an observed failure. A server wait releases its claim; an unverified network
outcome leaves backoff to the worker. Resume matching-token verification before
another raw send, and consume the marker with the permanent disposition so
delivery-only Retry can make a repaired attempt. Fresh plans cannot supply this
transport receipt; no exception text or credential is retained.

`adapter.py` uses the actual pinned Application with no updater/job queue.
Its synchronous constructor resolves SDK paths, so the supervisor must construct
the channel with `await asyncio.to_thread(...)`; this creates objects only.
Initialize, start, poll, send and shutdown all use the one Gateway loop;
inherited thread/lifecycle and retry helpers are not used.
Its limiter preserves a monotonic short hold through overlapping requests,
cancellation and exhausted retries. A long 429 releases the worker through the
durable retry path. Capture destination/purpose/generation/token before a call;
only current bound-message Forbidden can block a link. Start/poll/send token
checks share one coalesced verifier; two getMe 401s separated by the recheck
interval are required. Ordered blocking handlers use a per-call acknowledgment:
the library's swallowed exception path never proves admission. Fixed input
routes and required reply plans commit before acknowledgment. Token-shaped
content is refused before hashes/inbox/task payloads; disposable tasks retain
safe identities only and drain before SDK teardown. Unfinished ordinary text,
skills, photo/document and voice consumers keep returning False.

`polling.py` owns retained suffixes and startup-held batches. Serialize every
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
any awaited status guard. Restore requests one discard intent with
`require_drop_pending`; `complete_pending_drop` only joins that intent and cannot
schedule another negative-offset discard. Await completion outside the lifecycle
lock needed by health callbacks.

Transport facts use narrow `update_if_current`; health callbacks supply
`expected_token_revision`. A no-facts status guard writes no timestamp. Cause
caches are revision-scoped and publish after commit; one live supervisor owns
ordinary health causes. SQL revision fences support rotation through another
repository, but do not merge independent hidden cause sets at the same revision.
`deployment.py` shares static refusal precedence and owns the persisted CopyGuard.
Host discovery uses only the forced-local Docker CLI off-loop, outside writers.
Guard writers share the capacity singleton lock. `peek` is SELECT-only and may
borrow an active read session; `resume` uses stored state without a host read.
The startup heuristic is conditional, never an ownership election: concurrent
copies must have Telegram off before first boot, including same-engine-ID copies.
`cli.py` prints one sorted JSON document for every invocation and starts no bot,
Gateway or provider service. Its existing-only engine path performs no bootstrap,
DDL or stamping; status is read-only and captures one real multi-query snapshot.
Project safe status fields explicitly and count only current-generation delivery.
Token opening returns classifications only, never credentials or rewrapped data.
Removal checks fresh derived account access inside the borrowed capacity writer;
refusal rolls back the entire unit. Preserve settings, acts and audit events.
`token_pattern.py` keeps repository imports independent of the Telegram SDK;
`adapter.TOKEN_BODY` remains a compatibility re-export. CLI voice qualification
passes rendered models to the shared resolver, including an explicit empty list.
`log_filter.py` sanitizes messages and exception text on root and
non-propagating handlers; installation and logger levels belong to the
supervisor constructor, never imports.
`api_logging.py` supplies closed error classifications and exact event fields.
Native call failures have one logging owner; a translated permanent failure
retains that fact so durable recovery does not log the same attempt twice.

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
source. This slice sends trusted fixed product/status sources only; model-final
recovery, content execution, callback actions, files, profiles and instance policy
have later owners.

`authority.py` rereads the live human account, strict route ceilings, exact
connection and supported owner-scoped target. Fixed product/status sources need
no private-read ceiling; withdrawn execution-only permissions do not suppress a
saved acknowledgement. Missing/disabled/released/inert accounts refuse delivery;
unreadable config/provider/target state waits. Instance policy is unavailable,
never implicitly unprotected. Disposable lifecycle notices have separate current
context/protection checks so `access.off` can follow account disablement.
Prepare live config off-loop, pass its mode to `account_refusal`, and call
`resolve_route_permissions(..., prepared_config=..., strict=True)` on the Gateway
loop. That strict seam shares the HTTP route cache but raises
`RouteAuthorityUnavailable` for read/provider/malformed-decision errors; HTTP's
configured fail-open behavior stays with HTTP. Discovery runs off-loop while
provider construction, evaluation and cache publication stay on the calling loop.
`hold_if_current` advances the binding generation and ends pending work;
`release_if_current` rejoins only an exact connection already restored externally.
Neither invents account authority. `mark_notice_if_current` claims closed notice
metadata under the token/generation/connection writer before bounded transport.
Held-input and reconcile paths share its atomic access-off claim. A real new
hold clears that category's prior claim; one disposable attempt serves the hold.

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
account's same message ID. Work baseline markers stay null
until the stage-three initializer joins this same session.

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
conflict timestamps and an optional hostname atomically. The future poller owns
the duration constant and must supply its full window on first active entry.
Omitting transition metadata preserves the window; explicit null clears the
hostname. Ending conflict preserves the action pause.

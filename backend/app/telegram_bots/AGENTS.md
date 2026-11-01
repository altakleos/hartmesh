# Managed Telegram bots

`config.py` validates the optional `AppConfig.model_extra["telegram_bots"]`
section without starting services. Nested models forbid unknown keys and type
coercion; absence returns fresh defaults with `enabled=False`. Keep this separate
from the existing deployment-wide channel adapter.

Compose's renderer owns enabled/public URL/bot limits/voice provider and the
legacy-channel closure; an absent environment key means on. See
`deploy/compose/README.md`.

Keep bot network, supervisor and router startup outside configuration loading.
Use socket-free transports and synthetic credentials in offline tests.

Product text lives in `texts/` in English and Chinese; keep stage-one keys equal
and copy English wording from the design. `render` escapes values; `render_plain`
preserves them for plain labels/alerts. `markdown` accepts only Telegram markup,
and `split` counts visible UTF-16 units while retaining balanced tags and complete
canonical content. Preview rendering is presentation only: durable receipts,
callback authority and execution gating belong to their owning writers.
`KEYBOARDS` supplies fixed, closed descriptions for current settings, help and
new-conversation controls. `callbacks.py` owns canonical layout, saved authority,
group adoption and prompt scope. Callback data carries only `h1:<random row id>`;
never derive an action or target from Telegram message text. Compare original
link/account/chat/topic/generation, anchor, presentation and the complete group.
Historical aliases route only to a stale guard, never to a new action. Clear
keyboards explicitly at raw delivery; the SDK omits empty object sequences.
One two-second logical press budget covers checks and acknowledgement; reserve
ack time and admit no fresh metadata/action leaf after retirement. The fast fixed
ack uses only captured transport state and a presentation-only locale cache.
An already-started SQL unit remains owned through cleanup beyond that budget.

`commands.py` handles fixed navigation and language settings only. Deliberate
navigation closes only that prompt scope. `acts.py` commits a saved settings
choice, its exact old/new preference, completed Act, consumed group and replacement
controls in one guarded writer. `SettingsRepository.update(session=...)` borrows
the owning engine/writer without committing. Future Act kinds have no consumer.
`plan_product_keyboard` freezes descriptions with the original product basis and
creates parent, group and controls atomically; exact replay reuses identities.
The supervisor constructs these services after resolving default repositories.

`human_input.py` composes finite clarification/form/network interactions with the
existing turn controller and sender; it owns no scheduler. `human_input_source.py`
qualifies one exact lead/tool request using the persisted document-2 output,
complete inventory and actual messages. Manager `turn_artifacts` are bounded
presentation hints only. V5 human-input plans freeze that source and their input
snapshots; every unsent private part rereads source and live scope, including
inside the SDK limiter. Recovery passes its original read guard into the same
planner. Fixed navigation can close scoped metadata without reading old private
content; any private keyboard cleanup still needs fresh source qualification.

`mixed_source.py` qualifies one supported human-input request plus ordered files
from the same complete, unmodified output projection. Request basis 2 and reply
basis 7 are closed variants; existing question-only V5 and file-only V6 identities
remain strict. Metadata uses the original source/read guard before entering the
existing primary/seal writer. Files precede the complete request UI without a
caption. Every part retains its own source, acknowledgement and control group.
Only owned request UI needs current question/form state; media, file warnings
and delivery settlement remain independent after an answer. File Retry never
stages another model input. HI views and cleanup select request-owned parts,
never the outbox's most recent media acknowledgement.

Mixed form/Other prompts are saved inactive in the same writer. The exact final
request part's acknowledgement activates its saved anchor and starts its TTL;
unseen prompts accept no field input. File retries cannot reanchor, reactivate
or extend them. Mixed V5 steps require the original V7 parent, identical request
and input snapshots, and its acknowledged request/prerequisite barrier. Media
relocation/replacement preserves the separate request lineage and old receipts.

`forms.py` stores bounded field values/revisions in existing callback rows. Saved
option identities, required-checkbox truth, field-by-field prompts and the final
review/Send determine the exact response; caller labels never select an action.
Prompt/group/plan changes share one guarded writer. Staging a response retains
the source control until actual enqueue/bind confirmation, so interrupted handoff
can repair delivery without applying a second gesture. A network Act records the
saved choice and completes only with that same confirmation; it grants no access
directly. The next run's existing network middleware applies policy.

Submission owner version 2 closes over the canonical response and original
thread; outer publication input version 1 still identifies the origin receipt.
Typed alternatives retain actual inbox origins, callback choices their saved
callback owner. Neither invents a human message row. Plain text after a free-text
question carries no response mark. Old quoted prompt input is held until explicit
reopen, send-as-new or discard, and is never silently applied to another field.

`turns.py` owns ordinary/custom chat-input admission and the ephemeral queue.
Save actual sanitized input/profile before effects; forwarded input cannot acquire
plain provenance. Closed V2 media/forward input keeps actual SDK descriptors;
V1 plain input and typed form/Other provenance stay strict. Persist complete
joined groups before thread preparation. Album, long-paste and forward timers
use the existing Application/turn lifetime and never occupy a manager worker.
`queue.py` never owns workers or authorization. A terminal durable plan releases
one logical turn; transport retries do not hold it. Only exact typed terminal
reasons or opaque host creating-refusals authorize capacity retry. Explicit retry
owns a new saved callback input identity. Stop binds the original publication,
including before a run exists, and never guesses a newest run.
Only an accepted cancellation or a proven undispatched input drops waiting input;
a refused Stop preserves it. A guarded read failure holds the same publication
and retries observation, never model dispatch. Callback-owned input retains its
actual original-message snapshots across inbox collection. Reservation and the
dispatch marker alone never consume the retry control.

`streaming.py` consumes only opaque qualified lead previews from `progress.py`.
Raw source offsets and UTF-16 chunk limits stay separate. Earlier acknowledged
edit parts remain fixed; the newest anchor may be replaced. Drafts coalesce,
keep alive and redisplay after chat sends. Thinking placeholders require an actual
live view at delivery, including inside limiter retries; old process rows cannot
revive a preview. Pause drains admitted display acknowledgements before Stop
freezes them. Final/stopped plans recheck exact source through `ModelSource`.
Each publication uses a fresh random draft ID to reject late Stop gestures from
an earlier attempt; the reply anchor remains the actual first human message.
Retiring the last auxiliary Thinking row also settles its original input in the
same writer, including when its primary reply was already acknowledged.
`ManagedExtBot` preserves empty draft strings across the pinned SDK defaults
conversion; keep the real SDK serialization check when changing that boundary.

Ambiguous details containers display literally. Impossible canonical
split budgets raise `ValueError`; disposable drafts may clip their first part.

A `show_web_page` result is a closed `page` entry in the run's inventory, set by
the harness capture; any other artifact stays opaque `structured` output.
`content_items` excludes pages, so every reply family keeps its proof of the whole
inventory unfiltered and gains no new version. `ModelSource._page_keys` derives the
offers from that saved proof for the last answer-bearing part of a primary, and
`send_part` builds the buttons with `page_buttons` at each attempt. Beside
ForceReply, or after Telegram refuses an address, the address becomes the
message's last line; a file reply with offers keeps its answer as a text part,
because one refused button would otherwise fail the whole upload.

`pages.page_address` uses only the configured base, stored target and supplied
identities. Restricted instance tabs need current grants; Memory also needs the
`memory:read` ceiling, and adding an AI employee needs administrator authority.
Web pages still enforce access. No handover is a sign-in capability.

`voice.resolve_transcriber` rereads configured model entries and never obtains
keys from environment variables. Capture one qualifying provider/key/model per
note; requests use fixed official endpoints with no provider fallback. Keep SDK
imports lazy and suppress SDK payload logging only inside the voice context.
The package emits fixed qualification reasons, never keys, audio or transcripts.
SDK imports, TLS client construction and sync closes run off-loop.

`VoiceNotes` treats a voice note as typed text that arrives late. The inbox row
is its only state: route `voice` with target `working` until it has an outcome,
or `asked` while its consent question waits. There are no other phases, no
worker and no provider retry. `complete_voice` rewrites the row as ordinary plain
or forwarded input in the writer that saves the `voice_note` audit and the
`voice.heard` echo, retaining the recording's descriptor in `voice_note` only to
compare a redelivered update. Everything after that is the typed path: prompts,
pending files, joins, queue, Stop, recovery and Try again need no voice branch.
The per-owner lock spans bots, covers checks through the audit commit, and is
released before the transcript is routed. Queued notes hold no audio; audio lives
only inside `_transcribe`, charged to the shared byte budget through the provider
call. `_download` is the one getFile/capped-stream leaf for model files and voice
audio; voice uses no `FileInputContext`, storage preflight or upload.

Late text is judged by when it was spoken: `_text_target` lets a prompt created
after `accepted_at` claim nothing, matching the typed rule that admitted input
stays ordinary. A consent press agrees only to its `offered_provider`, compared
under the lock; a mismatch edits the same message into a fresh question. Decline
never touches an existing agreement. The startup input page settles `working`
notes as `voice.failed` because their provider call may have happened; no live
note exists then, so no liveness registry is needed. `asked` notes stay out of
that page and wait for their press. In a live process `_give_up` answers any note
a step could not finish, once; a spent press is the only silent outcome. A spoken
leading slash is dropped, so a transcript can never select a skill.

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

`router.py` is the browser's surface; every route needs a session and acts on the
caller's own rows. Refusals go through `_RefusingRoute` and the closed `_REFUSALS`
table, as `{detail, reason}` plus only the keys their row names. Routes 2 and 3
read their own bodies, because a framework 422 would repeat the token. Routes that
need Telegram refuse with the deployment's reason; list, disconnect, settings and
remove are served whatever that reason is. Route 3 reads its receipt before it
looks for the old link, so a replay never swaps twice; only after commit does
`adopt_token` restart the client. `telegram_feature` fails closed to the model's
defaults. Later-stage routes are not declared, so they answer 404 or 405.

`retention.py` runs daily, first an hour after start. Each table is swept in its
own transaction. Outbox candidates are locked, skipping any row in use, before the
dependencies are read: a live callback's target or `anchor_outbox_id`, an
unfinished publication owner's `source_outbox_id`, `outbox_id` and `delivery_ids`
(it may sit on a consumed callback), an unfinished mark's deliveries, and the
`reply:<turn>` row of a live anchor's turn, which a form's later steps read by key.
A new kind of outbox reference must be added there. No extra clock starts when a
dependency ends. A mark is done only once its deliveries are sent or skipped, so
inbound retention needs only the open-act check. `retry_message` deliberately gets
no pin: its mark holds the person's text, so the seven days stand and a later press
refuses as `stale.not_waiting`. Stage one writes no
`stop_dispatch`, `continuation`, digest or attempt status message; a stage that
adds one states its dependency here and in `_pinned_deliveries`. Callbacks and
pending files sweep hourly on their own tasks, so one failing never skips the
other.

`skills.py` answers whether a slash word names a skill, from the owner's own storage
as the run reads it, off the loop. A skill that can run goes on through `_on_text`
as a chat message with its text unchanged, and the run's middleware activates it.
One that is off or that a custom agent does not list answers its own sentence; any
other word, and any word on an agent's bot, answers `cmd.unknown`.

A token is read once per process and per revision, because a secret changes only
with a restart. That one reading clears `secret_unreadable` when the token opens, and
moves a token that will not start, held or stopped, to the current secret, so
`tokens.previous` can reach 0. `limited_since` follows the first durable send that
gave up on a 429 once another gives up `LIMITED_AFTER_SECONDS` later, and the next
accepted send clears it; the bookkeeping never changes a send's own result.

A turn holds its chat only while its driver can still finish it. `_own` wraps
`_drive`, so every way out, by return, raise or running out of time, reaches `_close`
and then lets the next message run, even when no plan could be written. One clock,
`quiet_since` on `queue.max_wait_seconds`, bounds a turn that shows no life: a run first
seen or still working resets it, and a read that keeps failing and a reply that cannot be
planned run it out. Past it a successful run ends with `error.generic` under the closed
proof `undeliverable`, and a run interrupted without proof of this chat's Stop ends as
that interrupted run. The error plan is itself an attempt: it loses to a reply that did
commit, which the next `plan_final` returns.

A message waiting for its run is not on that clock. The worker reports through
`ChannelExecutionContext.progress`, so `worker_state` tells a wait for a worker, however
long, from a message nobody holds. Once a worker lets go a run shows within
`RUN_SHOWS_SECONDS` or never will, and it ends under the closed proof `no_run`. Stop
fences the message, retiring the admission a worker checks, so a message still `queued`
gets the same short limit. A fence cannot recall a POST already sent, so a message
`taken` is waited for even after Stop, and `observe(closing=)` answers pending, not
`no_run`, until its worker lets go. A chat starved of workers therefore has no limit:
`chat_turn_waiting` says `worker=queued`, and `/stop` ends it.

`observe(closing=)` is the durable twin: a marked publication with no live owner ends in
a plan. Only a caller that knows no turn owns it may pass `closing`; recovery asks
`owns_publication`, which compares the publication and not the link row, since a poll
rewrites that row's stamps, and which lets go of a turn that is closing. A publication
stays held only while its proof cannot be read, such as a conversation deleted under it.
`orphan_recovered` keeps the design's `refused.interrupted`; `gateway_stopped` is
`chat.restarted`.
`delivery_items` is what a reply must
carry: a tool's own artifact is not in it, since the model read that and its answer is
the reply, while `content_items` keeps it for human-input requests.

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

Managed publication custody uses frozen `PublicationRecord`/scope/origin DTOs.
Reserve a new model-turn identity and versioned execution receipt atomically;
never adopt a receiptless legacy publication as dispatch-not-started. Exact replay
compares complete saved origins, canonical input, target, thread and endpoint.
Dispatch marker/run ID are fresh observations, not immutable caller identity.
The marker is irreversible and permits one winner; ambiguous commit never
authorizes another dispatch. Managed explicit retry uses a newly saved callback input.
The mandatory original-attempt `PublicationGuard` borrows a same-engine writer,
checks current human/account/connection/link custody and requires literal True
after its final awaited authority read. Flush mutations before that final guard;
only commit follows. Keep link/custody -> primary outbox -> sorted inbox order.
Borrowed publication lookup returns a fresh record and detached complete rows.
Owned SQL units retain execute/fetch/commit/rollback/exit through cancellation.
`plan_publication_reply` and closed fixed notices share `reply:<turn_id>` and
atomic route sealing. Exact canonical proof/presentation replay retains actual
part acknowledgements; changed plans refuse. Frozen visible-text ranges use
codepoints, while Telegram chunk limits use UTF-16. These primitives install no
dispatcher/observer/model sender themselves; `turns.py` owns their composition.
Strict remote source and artifact/action eligibility stay mandatory. Legacy `plan_reply` keeps its
compatibility semantics. See [publication custody](../../docs/TELEGRAM_PUBLICATION_CUSTODY.md).

`publication.py` supplies finite callable dispatch/observe/read operations, not an
input handler or recovery scheduler. Accept only complete saved groups with a
closed trusted `input_profile`; unknown input refuses. Media and forwarded
material use their V2 projection.
An actual bus reservation precedes the irreversible marker. Only its winner may
commit synchronously after the original creating fence, with no intervening await.
Bounded progressing discovery must complete before adopting a unique exact run.
Root attempt thread/endpoint bookmarks supplement authoritative remote proof.
Owned reserve/marker/bind and verified-primary operations invoke optional public
`prepare_publication_guard` after codecs, immediately before opening their writer;
borrowed checks retain same-engine account/custody and no provider work. This is a
point-in-time policy fence, not cross-system policy atomicity. V2 primary bases
retain both proofs, inventory and frozen presentation; V1 is never adopted as V2.
The existing sender borrows `ModelSource` and rereads every unsent model part.
Part shape cannot turn a model basis into a fixed source. Unknown/nonempty output
stays held. Phase one closes creating/sending but retains authorized final reads
through manager drain; final stop drains coordinator cleanup before the same HTTP
owner. Failed cleanup retains that owner and listener for retry. Explicit document
2 uses generic live-host NoUser qualification; see [turn execution](../../docs/TELEGRAM_TURN_EXECUTION.md).

`recovery.py` is the Supervisor-owned bounded scheduler over that same callable
coordinator and sender. Periodic passes observe marked publications only; no
empty/404/absent-task fact authorizes dispatch. `recovery_page` keysets heads,
fetches every global-turn member with a sentinel and holds malformed, mixed,
oversized, legacy or unsupported groups. Limits bound returned groups/origins,
decoded-payload counts and retained operations, not bytes, SQL scan time or RAM.
The schema has no turn/head index; no migration is added for this slice.

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
before coordinator/client teardown, retaining dependencies on failure. No new
after-manager observation pass or grace is installed. Ordinary input activation,
queue/Stop/retry consumers belong to `turns.py`.

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

`gateway_client.py` owns one public SDK/HTTPX client for the independently resolved
LangGraph URL, prepared TLS, finite timeouts and no environment proxy/key discovery.
Its tracked JSON/SSE operations build fresh bound-human internal/CSRF headers,
validate closed relative paths and never follow a response-selected endpoint.
The generic `channels.execution_client.ChannelExecutionFacade` borrows this owner;
it validates the detached JSON snapshot actually encoded, original scope, strict
read acknowledgements and progressing run/event cursors. Neither metadata nor a
raw stream frame becomes a server receipt or proof of final output.
Creating waits/streams use explicit continue-on-disconnect and 5/300 timeouts;
reads retain 30 seconds. There is no automatic POST/reconnect after ambiguity.
The one-slot public-SSE pump and responses retain cancellation/failed-close
ownership. Post-response authorization runs inside that same retained operation
or pump; queued parts recheck current read authority immediately before handoff.
Stream close also drains its detached-snapshot preparation. Network cancellation
is prompt; codecs and admitted guards drain.
Require live `threads:read` before reads and `threads:write` before creation. GET
requests require authoritative binding availability and exact stored owner,
thread ID and lead/custom target; every create/replay is followed by that GET.
No local lookup substitutes for proof at a separately configured endpoint.
`threads.py` serializes ensure/new through the shared keyed lock and reserves
before network work. The stable intent excludes token revision: a new authorized
attempt may reuse it after same-bot rotation, while the old captured adapter/token
cannot upgrade itself. Writers check current attempt identity after the final
borrowed-owner await. Retain construction, full SDK/decode operations, lock waiters
and admitted SQL through cancellation/shutdown. Preparation stop closes only its
own admission and drains its work. Supervisor phase one also closes new model
dispatch, while the shared client/final reads survive the actual manager drain.
Already-dispatched waits/parts recheck current read authority, never new-create
ceilings. Supervisor final stop closes that client before persistence teardown;
failed response/client cleanup stays retryable. `turns.py` composes these callable
foundations for ordinary model input. `/new` reserves its original routed command and
mapping/creation bookmark without HTTP. A saved first-choice claim precedes
confirm's existing thread API; Keep never creates. Final guarded delivery planning
consumes the exact group and seals the original input. The bot message is never
fabricated into a human-origin receipt.

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
Fixed server statuses use a separate immutable `status_basis`, including exact
scope, origins, topic and completion intent. Strip supplied transport receipts
before both basis and parts; regenerate part identities and reject extra controls.
Only exact source/page pairs derive a server label: `web.memory`/`web.memory`
uses `label.open_memory`, and `custom_agent.deleted`/`web.telegram` uses
`label.open_telegram`, and `chat.restarted`/`web.conversation` uses
`label.open_conversation` with the original saved thread. Validate the pair again at raw delivery. Deleted-custom
repair follows authoritative owner-scoped config absence, uses a safe captured
label and an eligible configured public address; uncertainty never means deletion.
The fixed repair may send during `agent_unavailable`, with no model/file privilege.
URL-only parts freeze page/address separately from callback controls.
A refused button journals the same part's text-address fallback
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
safe identities only and drain before SDK teardown. Text/photos/documents use the
managed turn controller. Voice notes use `VoiceNotes`; potential skill references
stay held.

`files.py` owns one Supervisor byte budget and pending attachment descriptors.
Reserve whole batches before reads, shrink only, and retain each charge until
native writes and transport cleanup finish. `capped_download.py` reuses the
normal bot request's HTTPX client with raw bounded streaming and no redirects;
failed response closes retain their actual stream and capacity release for
shutdown retry. No download URL, token, Update or file bytes enter persisted input.
Pending add/take uses real admitted rows and the same guarded writer: original
input plus even an empty take is saved, so replay never adopts later attachments.
Source-bound file notices are auxiliary to the actual publication. File Retry
keeps the original thread and input snapshot across restart, /new and collection;
its control is consumed only after actual enqueue/bind confirmation. Storage
preflight uses the separately configured Gateway as the bound human; capacity
is discovered only from an actual write failure.

`file_source.py` qualifies V6 file replies from the same complete matched output
inventory. Metadata is presentation data, never read authority. The existing
artifact HEAD and explicit streaming GET retain owner/Space checks; the one
Gateway client owns their responses. `outbound.py` reserves the whole batch
before reading, records first-read SHA before upload and refuses changed retries.
Every SDK dispatch rechecks the artifact door after limiter waits; thread/run
rights alone do not cover independently revoked Space access. Classification and fallback replace saved unsent parts, preserving every ACK.
`outbound_upload.py` uses distinct multipart attachments with bounded memory-only
readers, emptied after SDK drain so retained request cycles keep no leased bytes.
It returns closed failures; drain native users before releasing `OutputBatch`.
Failed HTTP closes retain only member-close signals; the shared charge releases
after both transport cleanup and the batch owner settle. File Retry repairs
saved delivery in its original thread without publishing another model input.

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
Every turn line goes through `turns._chat_line`, which names the link, bot, turn
and run. A cause prints through `deerflow.runtime.runs.terminal.failure_chain`:
`raise ... from None` keeps `__context__`, so class, fixed slug and raising line
survive to the report while exception text stays out of the log.

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
source. The existing sender accepts fixed sources and strictly verified model
finals. Managed text, inbound files and saved controls use `turns.py`; outbound
files, profiles and instance policy have later owners.

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

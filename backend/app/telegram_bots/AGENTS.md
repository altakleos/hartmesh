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

Optional `work_text_inputs.py` routes plain text for shown Work prompts and,
through `work_card_text.py`, explicit card replies. Complete prompt previews
accept a reply to any part. Known spent/closed/input-owned Work presentations
retain input ownership for recovery; they cannot fall through to ordinary chat.
`work_card_inputs.py` owns explicit replies to complete Work cards: immutable
`work_input` version2 proof, consumed source group and one Respond Act. Comments
never decide or accept. Supervisor installation remains separate. Confirmation-card comments preserve the
original unsubmitted draft; closed inputs offer explicit recovery choices. Final writer proof binds the original input across its own writes. A card's
complete presentation must have been dispatched before the input was accepted;
a delayed host ACK does not invalidate an explicit reply.
`work_file_inputs.py` owns single-file answers (input/journal V4), immutable native
receipts and named-file ACKs. `work_file_handoff.py` transfers explicit Send-as-new
choices before chat publication. `work_file_albums.py` collects; `work_album_*`
owns group capture, receipts, recovery choices and whole ACK. Controllers remain dormant;
see [Work guidance](../../docs/TELEGRAM_WORK.md).
`work_untied_inputs.py` asks before saving unquoted text after the last acknowledged
Work question, after ordinary clarification routing. Its durable `work_answer`
route becomes version3 Work input only on the actual Yes callback; No owns one
ordinary callback publication. Replays reconcile receipts without resubmission.
`work_untied_delivery.py` joins current claim/input/source/choice facts to final
account and grant checks at the SDK fence. Keep JSON types and the original input
snapshot intact across awaited admission; all public operations drain cancellation.
`work_retained_drafts.py` settles a retained review note on Edit/Discard and a typed
decision on Back/Add note; it creates no Work action. Final input/action/source
checks include the transition's own settlement. Editing opens a separate prompt
against the complete displayed basis. Review-note controls inherit the original input's
24-hour age, including after repeated refreshes.
`work_input_refresh.py` preserves typed decisions and review notes across changed
confirmations. Only unreserved controls or an exact conclusive refusal may refresh;
unknown/missing journals stay held. A fresh full ACK and press own any new action.
`work_review_recovery.py` handles closed reviews for unsubmitted notes and exact
conclusive conflicts, including text whose review closes after admission. Unknown
or missing journals stay held; receipt reconciliation precedes recovery. Replacement lookup stays on the same Work; otherwise
control retirement, a fixed notice and input settlement share a guarded writer.
Admitted confirmations remain owned by receipt/conflict recovery.
Input admission stores its operation marker with the Act. Orphaned input markers
or V4 control reservations block every fresh preview/recovery entry point.
Capture precedes admission; stored-input resume never reconstructs an Update or
retries an unknown action. Typed decision confirmations own their original input
until outcome delivery. Supervisor installation and broader input routing remain deferred.

`work_comment_cards.py` supplies receipt-owned outcome V5: saved comments return
a full current decision/review with fresh controls, while legacy plain outcome
ACKs remain readable. Further replies own distinct Respond operations. Request
previews and full comment outcomes share `work_source_spaces.py` for final joined
Space READ checks; comment publication uses that proof too. Multipart recovery
preserves the original operation and ACKs. Supervisor installation remains pending.

The optional `work_callbacks.py` dispatcher composes existing Work owners; the
supervisor does not install it yet. Stale callbacks can refresh only unreserved
controls or exact conclusive conflicts; retained input has its own owner. Known
non-effect confirmation refusals use `work_refusal_recovery.py` before retry.
Routing refresh preserves unresolved and collected journals. Explicit Retry validates the whole shown
outcome and original Act body, then serializes one attempt for that displayed
attempt count. Delivery ACK and both kinds of shown controls share
`acknowledged_parts`; keep full typed receipts before any control authority.
Work outcomes fence superseded controls, receipts and current source/claims.
Its dormant receipt observer never calls the explicit action-retry path. Routing
results edit only their command’s exact ACKed selection; missing/incompatible
anchors rematerialize current receipt content without another command.
A receipt-owned V3 outcome can carry a manager’s wrong-person routing choices.
Admission rechecks its canonical response, full ACK and selected person in the
final writer; replay retains the separate original routing operation. V4 refreshes
retain a consumed choice as reply ownership and never reopen completed input.
Source drift holds that refresh for repair; retention preserves its exact evidence.
Routing V5/V6 has a dedicated recipient/account source and cannot use ordinary
question recovery. Source-aware routing refresh/Show/repair retain ACK history and
unresolved actions; fresh controls await full delivery. Recipient-choice admission atomically reserves the original
operation and group; a missing reserved journal never becomes a fresh action.
Actual Work effects retain instance → link → Act lock order and recheck joined
authority after provider awaits. Exact receipts precede fresh-effect admission.
Full ACK alone seals captured input and writes the Work-only continuation receipt;
keep retention and callback activation aligned with [Work guidance](../../docs/TELEGRAM_WORK.md).

Product text lives in `texts/` in English and Chinese; keep locale keys equal
and copy English wording from the design. `render` escapes values; `render_plain`
preserves them for plain labels/alerts. `markdown` accepts only Telegram markup,
and `split` counts visible UTF-16 units while retaining balanced tags and complete
canonical content.
`KEYBOARDS` supplies fixed, closed descriptions for current settings, help and
new-conversation controls. `callbacks.py` owns canonical layout, saved authority,
group adoption and prompt scope. Callback data carries only `h1:<random row id>`;
never derive an action or target from Telegram message text. Compare original
link/account/chat/topic/generation, anchor, presentation and the complete group.
Historical aliases route only to a stale guard, never to a new action. Clear
keyboards explicitly at raw delivery; the SDK omits empty object sequences.
One two-second press budget covers checks and ack; a slow ack costs no passed
press. Reserve ack time and admit no fresh metadata/action leaf after retirement.
The fast fixed ack uses only captured transport state and a presentation-only
locale cache. An already-started SQL unit remains owned through cleanup beyond
that budget.
Past the acknowledgement `_tell_press_failed` owns the outcome: a refusal's key or
`tap.failed`, as a reply. A spent row or a keyless refusal stays silent, having lost
to a press that worked or is working. `_source_for` reads the thread to tell a deleted
conversation from a failing Gateway. `keyboard_cleanup_row` takes spent buttons off
their message in the writer that spends them.

`agent_lists.py` projects at most 20 Use-qualified entries from the Gateway’s
500-entry page. `agent_list_source.py` owns drained HTTP/role/Work reads;
Inspect gates details and Attention owns recipient eligibility. Semantic lines
fit both locales and render in the current language. `agent_list_authority.py`
joins owner/role limits/binding/targets/Open-bots/claim in the final SDK check.
Stale lists retire without blocking later messages. Ordinary bots dispatch
`/assistants` and `/employees` to `handle_list`; their Work words only point to
those chats. Instance/custom bots do not acquire these ordinary-chat commands.

Work, notification, digest and interaction contracts are in
[the Work delivery guide](../../docs/TELEGRAM_WORK.md). Read it before changing
`attention*`, `notification*`, `digest*`, `work_*` or their sender/callback integration.

`commands.py` handles fixed navigation and language settings only. A command closes
only a typing prompt; `/new` and `/stop` also end the forms and cards in scope.

`acts.py` commits a saved settings choice, its exact old/new preference, completed
Act, consumed group and replacement controls in one guarded writer.
`SettingsRepository.update(session=...)` borrows the owning engine/writer without
committing.
`plan_product_keyboard` freezes descriptions with the original product basis and
creates parent, group and controls atomically; exact replay reuses identities.
The supervisor constructs these services after resolving default repositories.

`human_input.py` composes finite clarification/form/network interactions with the
existing turn controller and sender; it owns no scheduler. What would answer a card takes
`_waiting_source`: only Telegram input closes a card, so a newer run in the conversation
means it went on from the web, and the card refuses. Closing one needs no such check.
Instance network cards also require current Manage at planning, the SDK limiter
and the action writer; [network admission](../../docs/TELEGRAM_TURN_EXECUTION.md#instance-network-cards).
`network_denials.py` qualifies dropped denials from exact-run artifacts and complete
inventory. `network_notices.py` saves button-free V8 trailers with their primary;
delivery requires its acknowledgement and fresh scope/manager wording checks.
Terminal questions separate only pure host-proven denial artifacts; other structured
content stays accounted for. Their original source guard also owns the V8 writer.
`human_input_source.py`
qualifies one exact lead/tool request using the persisted document-2 output,
complete inventory and actual messages. V5 human-input plans freeze that source and their input
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
directly. The next run's existing network middleware applies policy. A tick keeps
its keyboard, because each redraw lands under the next press: a press on a list is
good at any revision while that list is the current field, a button remembers
whether it showed a tick so a repeat is not a change of mind, and leaving the list
consumes every keyboard it drew.

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
reasons or opaque host creating-refusals authorize capacity retry.
Instance outcomes and error frames retain `instance_id` through observation and saved proofs;
capacity notices never seal an outcome; startup hints use saved primaries. `instance_refusals.py` handles
the four send refusals and queued lifecycle closure; details: the lifecycle guide. Explicit retry
owns a new saved callback input identity. Stop binds the original publication,
including before a run exists, and never guesses a newest run.
Only an accepted cancellation or a proven undispatched input drops waiting input;
a refused Stop preserves it. A guarded read failure holds the same publication
and retries observation, never model dispatch. Callback-owned input retains its
actual original-message snapshots across inbox collection. Reservation and the
dispatch marker alone never consume the retry control.
Acknowledged creating refusals are paired with a drained recheck of the exact
conversation's execution authority and resident admission. Its opaque proof is
bound to the original refusal and publication; the saved closed outcome retains
that scope. Retry creation continues to use the original host refusal.

`streaming.py` consumes only opaque qualified lead previews from `progress.py`.
Exact instance `turn_progress` frames select fixed workspace-starting lines.
The 30-second timer belongs to the original observer; stage advance, Stop and
retirement suppress it, and timer plus stream cleanup drains as one unit.
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
`ManagedExtBot` preserves empty draft strings across the SDK defaults conversion;
a real SDK serialization check pins that.

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

`VoiceNotes` routes ordinary transcripts as typed text. Optional `work_voice_inputs.py`
captures Work ownership before transcription and requires explicit confirmation;
its source, retention and audit rules are in [Work guidance](../../docs/TELEGRAM_WORK.md).
The inbox row
is its only state: route `voice` with target `working` until it has an outcome,
or `asked` while its consent question waits. There are no other phases, no
worker and no provider retry. `complete_voice` retains captured Work context in
the same writer as its audit and consent plan. Ordinary transcripts become plain
or forwarded input with a `voice.heard` echo. The recording descriptor remains
in `voice_note` to compare redelivery. Ordinary prompts, pending files, joins,
queue, Stop, recovery and Try again continue through the typed path.
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

`router.py` is the browser's surface; every route needs a session and acts on the
caller's own rows. Refusals go through `_RefusingRoute` and the closed `_REFUSALS`
table, as `{detail, reason}` plus only the keys their row names. Routes 2 and 3
read their own bodies, because a framework 422 would repeat the token. Routes that
need Telegram refuse with the deployment's reason; list, disconnect, settings and
remove are served whatever that reason is. Route 3 reads its receipt before it
looks for the old link, so a replay never swaps twice; only after commit does
`adopt_token` restart the client. `telegram_feature` fails closed to the model's
defaults. Later-stage routes are not declared, so they answer 404 or 405.

`retention.py` runs daily, first an hour after start, whatever stops Telegram: the
sweeps stay out of `_background`, which a late refusal cancels. Outbox candidates are
locked, skipping any row in use, before the dependencies are read: a live callback's
target or `anchor_outbox_id`, an unfinished publication owner's `source_outbox_id`,
`outbox_id` and `delivery_ids` (it may sit on a consumed callback), an unfinished
mark's deliveries, and the `reply:<turn>` row of a live anchor's turn, which a form's
later steps read by key. A new kind of outbox reference must be added there. A mark
is done once its deliveries are sent or skipped, so inbound retention needs only the
open-act check. `retry_message` gets no pin: its mark holds the person's text, so
the seven days stand and a later press refuses as `stale.not_waiting`. Declare delivery pins here and in `_pinned_deliveries`. Validated done/refused file
journals expire after 400 days and pin no outbox. Callbacks and pending files sweep hourly
on their own tasks, so one failing never skips the other.

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

A turn holds its chat only while its driver can finish it. `_own` wraps `_drive`, so every
way out reaches `_close` and lets the next message run, even with no plan written. One
clock, `quiet_since` on `queue.max_wait_seconds`, bounds a turn that shows no life: a run
first seen or still working resets it, and a failing read or unplannable reply runs it out.
Past it a successful run ends under the closed proof `undeliverable`: `answer.on_web` with
a link and no Try again when its output is complete, otherwise `error.generic` with Try
again. A run interrupted without proof of this chat's Stop ends as that interrupted run.
The outcome plan is itself an attempt: it loses to a reply that did commit, which the next
`plan_final` returns.

A table's shape cannot stop an answer. It stays aligned while its line fits `PHONE_WIDTH`,
an estimate, and it holds no address, and otherwise becomes labelled lines. A final answer
is rendered once and then cut by `split_html`, so no block is broken at a raw offset; raw
slices remain for previews and acknowledged edit parts, so `streaming: edit` can still
show a torn block. Telegram ignores entities past 100 in a message, so nothing counts them
over a whole answer. A saved part is recomputed before each send, so a render change
strands unsent rows: `_saved_part` ends them as `SourceGone`.

A manager worker holds a message only until its run's first frame, because the run goes on
with `on_disconnect="continue"` and the driver reads it itself. Eight workers therefore
bound how many runs start at once, not how many answers run, and the end of a run reaches
the driver through its watcher. A message waiting for its run is not on that clock. The worker reports through
`ChannelExecutionContext.progress`, so `worker_state` tells a wait for a worker, however
long, from a message nobody holds. Once a worker lets go a run shows within
`RUN_SHOWS_SECONDS` or never will, and it ends under the closed proof `no_run` once a read
answers that there is none; a failing read gets the ordinary clock. Stop
fences the message, so one still `queued` gets the same short limit. A fence cannot
recall a POST already sent, so a `taken` message is waited for even after Stop, and
`observe(closing=)` answers pending until its worker lets go. A chat starved of workers
has no limit: `chat_turn_waiting` says `worker=queued`, and `/stop` ends it.
A run the provider refused for `quota` or `auth` carries `stop_reason` `model_refused`,
which is `error.model_refused` with Try again.

`observe(closing=)` is the durable twin: a marked publication with no live owner ends in
a plan. Only a caller that knows no turn owns it may pass `closing`; recovery asks
`owns_publication`, which compares the publication and not the link row, since a poll
rewrites that row's stamps, and which lets go of a turn that is closing. A publication
stays held only while its proof cannot be read. A conversation deleted under it ends under
the closed proof `conversation_deleted`, once the thread itself answers 404, whatever run
the row names: `chat.conversation_deleted` and no Try again, which would ask the same
conversation. A deleted conversation has no output either: `read_output` answers None
after the same 404, and every reader of it ends on that.
`orphan_recovered` keeps the design's `refused.interrupted`; `gateway_stopped` is
`chat.restarted`.
`delivery_items` is what a reply must
carry: a tool's own artifact is not in it, since the model read that and its answer is
the reply, while `content_items` keeps it for human-input requests.

A message saved but not yet admitted is not dropped by its replay. A failed attempt
leaves its update unacknowledged, and `ChatTurns.unfinished` decides: a row nothing holds,
no turn took and this process saved goes on where it stopped, a typed answer included. A
routed row from before this process is recovery's. A row with no route goes on in any
process, since recovery cannot read it. A channel going away also leaves its update
unacknowledged, so the same rule decides at the next channel, unless `recover_link` closed
the row first. A join's updates are acknowledged as they join, so a failed flush plans
`error.generic` with Try again itself.

Once the copy decision is made, `recover_link` closes what a late channel's link saved
before that channel polls, by startup's path. A publication reserved but never marked
waits for the next restart. `end_pending` closes every open input of an ended binding, a
Try again in line included; both an unbind and a hold end one. The startup read follows
`next_after`, and a late answer held for its person's choice is not on it.

`links.py` owns capacity and connection transactions. Capacity changes lock the
deployment singleton, optional target instance/grants, owner settings, then links
in ID order. Target admission precedes owner link locks to agree with policy
writers; optional final confirmation follows flush and capacity reads. SQLite takes
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
`plan_verified_publication_reply` and closed fixed notices share `reply:<turn_id>` and
atomic route sealing. Exact canonical proof/presentation replay retains actual
part acknowledgements; changed plans refuse. Frozen visible-text ranges use
codepoints, while Telegram chunk limits use UTF-16.
Strict remote source and artifact/action eligibility stay mandatory. See
[publication custody](../../docs/TELEGRAM_PUBLICATION_CUSTODY.md).

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
retain both proofs, inventory and frozen presentation; another saved basis is
never adopted.
The existing sender borrows `ModelSource` and rereads every unsent model part.
Part shape cannot turn a model basis into a fixed source. Unknown/nonempty output
stays held. Phase one closes creating/sending but retains authorized final reads
through manager drain; final stop drains coordinator cleanup before the same HTTP
owner. Failed cleanup retains that owner and listener for retry. Explicit document
2 uses generic live-host NoUser qualification; see [turn execution](../../docs/TELEGRAM_TURN_EXECUTION.md).

`gateway_client.py` owns one public SDK/HTTPX client for the independently resolved
LangGraph URL, prepared TLS, finite timeouts and no environment proxy/key discovery.
Its tracked JSON/SSE operations build fresh bound-human internal/CSRF headers,
validate closed relative paths and never follow a response-selected endpoint.
The generic `channels.execution_client.ChannelExecutionFacade` borrows this owner;
it validates the detached JSON snapshot actually encoded, original scope, strict
read acknowledgements and progressing run/event cursors. Neither metadata nor a
raw stream frame becomes a server receipt or proof of final output.
The creating stream uses explicit continue-on-disconnect and 5/300 timeouts;
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
`label.open_telegram`, and `chat.restarted`/`web.conversation` and
`answer.on_web`/`web.conversation` use `label.open_conversation` with the original saved
thread. Validate the pair again at raw delivery. Deleted-custom
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
later attempt. A plain `DeliveryClaimRetired` exits normally and never becomes a payload
failure. Two subclasses carry a model source read through the same fences to `_run`, which
gives them an outcome, because a row with none is claimed again every second ahead of the
rest of its bot's deliveries: `SourceGone` skips the row and ends what hung on it, its
Retry delivery and a prompt it would have shown, which settles its message, with
`delivery.gone` and no Retry, since no retry can send it, and `SourceNotRead` backs off.
Raw `send_part` returns acknowledgement IDs and calls no planner.
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
managed turn controller; voice notes use `VoiceNotes`.

`files.py`: byte budgets and pending files. File handling: [turn guide](../../docs/TELEGRAM_TURN_EXECUTION.md#instance-file-writes).
Reserve whole batches before reads, shrink only, and retain each charge until
native writes and transport cleanup finish. `capped_download.py` reuses the
normal bot request's HTTPX client with raw bounded streaming and no redirects;
failed response closes retain their actual stream and capacity release for
shutdown retry. No download URL, token, Update or file bytes enter persisted input.
Pending add/take uses real admitted rows and the same guarded writer: original
input plus even an empty take is saved, so replay never adopts later attachments.
Source-bound file notices are auxiliary to the actual publication. File Retry
keeps the original thread and input snapshot across restart, /new and collection;
its control is consumed only after actual enqueue/bind confirmation. Ordinary/custom files use the configured Gateway as the bound
human: `POST /api/threads/{id}/uploads`, the web's route.
Capacity is discovered only from that write: 507 is `file.own_full`, 503
`file.own_unavailable`, anything else `file.not_saved`, and the message goes on.
`_Turn.saved_files` gives each file one outcome across busy and capacity retries. A planned
notice is that outcome, and `_file_notice` records it, so no later attempt reads the file,
tells the person twice or makes the notice false. A read nobody was told about is no
outcome and runs again. Send again is a new turn, and a restart forgets it.

`file_source.py` qualifies V6 file replies from complete matched output inventories.
Metadata grants no authority. Ordinary artifact HEAD/GET retain owner/Space checks
and client response ownership. Instance planning and direct human exports follow
[the output contract](../../docs/TELEGRAM_TURN_EXECUTION.md#instance-output-delivery).
`outbound.py` reserves each full batch before reads, records first-read SHA before
upload and refuses changed retries. SDK dispatch reacquires current file rights
after limiter waits. Classification/fallback preserve every ACK.
`outbound_upload.py` owns distinct bounded memory-only readers through SDK drain;
failed HTTP cleanup retains its shared charge until owner and transport settle.
File Retry repairs saved delivery in its original thread without new model input.

Transport facts use narrow `update_if_current`; health callbacks supply
`expected_token_revision`. A no-facts status guard writes no timestamp.
`_note_person_active` stamps `last_update_at` at most once a minute; `_bind` clears it. Cause
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

`authority.py` rereads the live human account, strict route ceilings, exact
connection and supported owner-scoped target. Fixed product/status sources need
no private-read ceiling; withdrawn execution-only permissions do not suppress a
saved acknowledgement. Missing/disabled/released/inert accounts refuse delivery;
unreadable config/provider/target state waits. Instance policy comes from current
host custody and the stored employee setting; an unreadable policy waits rather
than selecting unprotected delivery. Disposable lifecycle notices have separate current
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

Recovery, conversation preparation, polling, the supervisor, binding, tokens, health,
profiles, Work-event cursors/readers and strangers are in [the bot lifecycle](../../docs/TELEGRAM_BOT_LIFECYCLE.md).
Read it before changing `recovery.py`, `recovery_authority.py`, `threads.py`, `polling.py`,
`supervisor.py`, `profile.py`, `avatar.py`, `strangers.py`, or those paths of `links.py`.

Instance conversation/publication and employee-policy authority use the same
host binding and current grant. Read [their lifecycle rules](../../docs/TELEGRAM_BOT_LIFECYCLE.md#instance-conversations-and-policy)
before changing `publication_authority.py`, `policy.py`, `presentation.py`,
`roles.py`, or instance delivery.
Lifecycle deletion requires a confirmed deleted row; missing service authority
keeps the link. Public profile target evidence lives in `notices.profile_target`.
Browser setup requires current Use, employee policy and execution readiness
before token inspection. It repeats preflight after the token roundtrip, then
borrows the capacity writer for locked instance/grant and policy checks on the
same service/engine. Final human/service authority follows every capacity read.
Same-bot token repair keeps current Use checks but remains available during
policy/execution holds or archival; a different-bot replacement uses new-setup
rules. Exact committed replacement receipts still settle without fresh mutation.
Suspended instances remain connectable;
provisioning, deleted and archived instances do not. No readiness/provider read
opens another session inside that writer. Profile version 2 adds the ordinary
bot’s assistant/employee menu entries; instance/custom menus retain their scope.

Managed turns run with `recursion_limit` 1000 (`EXECUTION_RUN_CONFIG` in `app/channels/manager.py`), matching web chats and scheduled runs; a configured channel session value overrides it and the Gateway clamps to `max_recursion_limit`.

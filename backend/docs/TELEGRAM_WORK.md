# Telegram Work and notification contracts

Read alongside [the managed bot guide](../app/telegram_bots/AGENTS.md).

`notifications.py` owns class preferences and optional-news budgets. The sender's
`proactive` switch remains false until notification entry points are complete.
Essential rows bypass discretionary budgets; edits require actual edit parts.
PostgreSQL serializes cross-bot daily admission with an owner advisory lock;
SQLite uses the existing writer. Immutable first-dispatch reservations retain a
logical notification through ambiguous retries; transport pacing is independent.
`notification_authority.py` checks settings, account, binding, claim and current
instance grant/policy in one final SQL snapshot at the SDK limiter boundary.
Model, callback and file notification plans remain refused until their own source
authority joins that snapshot. Local-day calculations run off-loop and preserve
daylight-saving boundaries.

`digests.py` reserves up to ten due unsolicited rows within one notification
category; `digest_threshold` accepts 1–10. Membership and original ages stay immutable through shared claims and
ambiguous retry; ACK alone marks members in-digest. Pre-ACK retirement consumes
controls before freeing owned members. Digest cards have one text part; invalid
historical partial ACKs retain members for reconciliation. Retention follows
transitive source edges. Reservations have no parts until source materialization;
source dispatch and item/reveal controllers are not installed by this primitive.

`attention.py` exhausts stable Work/request pages before ranking or applying
presentation caps. `attention_authority.py` prepares route decisions, then joins
current account/role limits/binding/connection/grants/policy in a final snapshot.
A changed principal, configuration or source authority retires the read; read-only
owners retain inspectable content without actionable requests. No chat list or
summary scheduler is installed by this collector.

`work_routing.py` reads owner-authenticated grants and active accounts before
capping six recipient buttons. Information needs Use+Inspect; decisions/reviews
need Manage+Inspect. The latest wrong recipient is excluded, but their follow-up ends when the request
or assignment revision changes; names, never IDs,
label choices. `Acts.route` serializes body capture and exact receipt recovery;
HumanInput's borrowed admission fences binding/account/policy at commit. Routing
grants no access and ignores the action pause. `work_routing_delivery.py` freezes initial recipient selection in a closed V5
request basis. Its dedicated source requires current manager authority and a
final joined snapshot of every candidate's grants and account state, including
the current recipient before wrong-person exclusion. No password hashes enter
that compact projection. Full ACK alone arms the final button group. Protected
notifications show only a teaser; ordinary question recovery cannot turn routing
into an answer prompt. Routing-specific recovery uses V6 and the same full-roster final snapshot for
refresh, acknowledged Show and failed-anchor repair. Generic recovery accepts
only ordinary V1/V2 questions; it cannot reinterpret interaction drafts.
Recovery preserves historical ACKs, holds unresolved operations and arms fresh
controls only after full delivery. Multipart Show clears its teaser first. Failed
routing anchors require guarded source-aware repair, which starts a new current
presentation, retaining a consumed Show as reply ownership. Explicit Show and
its later refreshes bypass proactive preferences; the final source read proves
the original Show receipt. Retain that exact consumed proof while its parent
exists, and require it under lock before recovery mutates controls. Complete ACK
validation precedes control activation,
and repair cannot consume another presentation’s controls. Controller installation remains a prerequisite for activation.
Presentation/handlers stay dormant.

`work_routing_admission.py` reserves acknowledged recipient choices in the same
writer as their immutable Work Act. The command retains the displayed request
revision and recipient, capturing the current general request CAS only once.
Replays use the saved body, including after the canonical routing event changes
the request. A reserved group without its journal cannot recreate an action.
The borrowed final snapshot checks manager/source and selected-account/grant
facts; candidate disablement is a recipient refusal, not an owner-account state.
Routing ignores the action pause and grants no access. The receipt worker owns
execution; this primitive installs no chat handler. Actual effects use the
separate final authority fence below; journal admission alone grants no effect.

`work_interactions.py` renders complete unsubmitted confirmation/prompt views.
Its closed JSON state retains normalized text and the displayed `seen` basis;
all comments precede separate final-part controls. A stale changes draft requires
a fresh complete review and explicit confirmation; rendering performs no command.
Decision/acceptance views grant no acknowledgement or execution permission.

`work_requests.py` reads every response before forming request controls and
retains the complete displayed basis. Review instructions, comments and response
dispositions are never clipped to a notification title. `work_request_delivery.py`
atomically freezes a materialized preview and its last-part callback group; a
retry reuses the original identities. This is a healthy-binding presentation
primitive, not event ingestion. Unrendered events must retain their intent while
transport is unavailable. The raw SDK requires an explicitly installed `WorkRequestSources`; supervisor
startup does not install it yet. Literal templates and misclassified replies
cannot bypass this boundary. `work_request_source.py` validates the entire frozen
part sequence and preceding ACKs, re-proves complete source/seen data and joins
claim/settings/Work/request/Space/callback facts to the final account/grant read.
Compact correlated arrays avoid multiplying the full outbox JSON. Privacy, locale
and Work-only revision drift defer until the optional recovery coordinator is
installed. `work_request_recovery.py` owns drained refresh and acknowledged Show:
prepare config and provider off-writer, borrow the final joined account/grant/source check,
retain uncertain Acts, consume old controls atomically, and clear old keyboards.
Compose that admission after the outer guard’s last await, then check lifetime synchronously.
Revealed protected content requires its consumed Show receipt at dispatch. Multipart
reveal first clears the teaser through a retained cleanup dependency, then sends
every full chunk as a new protected message; missing cleanup targets settle silently. Current
presentation versions require every preceding ACK; same-basis anchor repair keeps
control identities/expiry and moves anchors only after complete delivery. The
source family stays uninstalled until event and interaction integration.

`work_actions.py` adds optional Work service actions to `Acts`; supervisor startup
does not install them yet. Bound people own immutable request bodies and operation
ids. Exact service receipts are read without mutation before fresh action guards.
A generic borrowed-writer admission checks link/account/policy/pause and the exact
journal attempt before a fresh effect and again before commit. `work_authority.py`
prepares the journal attempt's route provider off-writer. Domain admission uses
`work_effect_authority.py`: after the service's instance/Work/request locks, it
retains link then Act locks through commit. Owner/role/connection/grant/policy,
exact original Act and selected routing-account/grant facts share the final SQL
snapshot after provider awaits. No nested account checkout is allowed. Current
manager access gates management effects; only decide/accept honor the action
pause. Recipient refusal never disables the actor's account. Canonical receipt
recovery happens before this fresh-effect gate and never repeats the command. Review commands
use the complete displayed basis and captured Work CAS. The bounded worker only
retries admitted service calls with no conclusive refusal, never settings, files,
network or transcription. No action here starts or stops an assignment.

`work_interaction_delivery.py` consumes only acknowledged canonical Work controls
and derives the first-tap menu/confirmation from their captured basis. It creates
a separate reply and a durable old-keyboard cleanup; no service action occurs.
The version-3 source basis retains the consumed origin group and its exact ACK.
The SDK joins those facts to current Work, account, claim, settings and grants,
including for reply-class delivery. All new controls remain pending until the
complete sequence is acknowledged. Every chunk checks the complete final control
group’s inherited lifetime, even when that chunk carries no markup. Pending/failed replies retain origin evidence
through both outbox and callback sweeps. No action, text or voice controller is installed
by this publication primitive; entry points remain disabled.
Queued interaction source/locale drift retires delivery without altering the
unsubmitted basis; generic question recovery must never rewrite it. Controllers
must recover retained drafts into a fresh complete preview in the current locale
before these entry points are enabled.

`work_typing.py` identifies deferred prompt ownership for `Prompts.attach` and
ACK. Answers, cannot/wrong reasons, own decisions, decision notes and changes
retain their complete source and seen basis. ForceReply appears only on the last
part; that ACK starts prompt visibility and expiry. SDK authority joins the
pending prompt and its source: closed/replaced prompts never publish or revive.
The origin control's age still bounds delayed publication. Prompt ownership is
not dispatch authority; the full source and current-authority fence remains
mandatory. Typing submission and stale-draft recovery controllers are not yet
installed.

`work_control_admission.py` reserves one acknowledged answer choice or second-tap
decision/acceptance with its immutable Acts operation in the same guarded writer.
The entire callback group records the selected operation; competing buttons
cannot create a second response. Admission performs no domain action. The existing
receipt worker can finish it after restart, using the original body and current
command authority. Saved bodies use typed JSON equality. Final source, access,
pause and outer-lifetime checks roll back both journal and reservation on drift.
First taps cannot acknowledge unchecked sources. Controls remain unconsumed until
an outcome controller settles them; result delivery and chat installation remain
outstanding.

`work_refusal_recovery.py` refreshes only a conclusively refused pending decision
or acceptance with reason `attempt-unresolved`. Unchanged seen data keeps the
original controls and operation. Changed data atomically fences the old operation
with cancellation/conflict, preserves its body/id and publishes a complete fresh
request view. The same bot/Act locks serialize against retry admission; unknown
pending calls remain untouched. Publication consumes the old group with
`work-refresh`, and final SDK proof requires that exact acknowledged origin plus
current source/access. A new first tap and confirmation follow full delivery.
The fresh preview is a direct reply even when proactive notifications are off.
Entry points and outcome delivery remain uninstalled.

`same_displayed_basis` is the common preview/confirmation comparison: complete
rendered source fields, all responses and people, typed seen/CAS data and protection.
Do not compare whole Work projections across reads: attempt workspace readiness
and execution availability can change without changing anything displayed or its
revision. Current permission and source admission still run independently; this
comparison never substitutes for their final joined SQL checks.

`work_input_capture.py` owns accepted plain text and its fully acknowledged Work
prompt in one guarded transaction. `Inbox.route` can borrow that active same-store
writer; it never commits it. Capture reads the stored accepted input, never a
caller's replacement text, retains original seen/interaction and a stable future
operation ID, attaches the sent source as a delivery dependency, and closes only
that prompt. Capture matches the shown message, part, version and expiry to the
final ACK. An exact stored reply may precede host ACK persistence; unquoted text
requires acceptance after that ACK and a later chat message ID. Earlier inputs
stay unrouted for separate untied-input handling. Replay preserves this route.
Pending input pins its prompt evidence
through callback retention; normal collection resumes after input settlement.
Capture requires current binding/account custody but does not read a new review,
submit a command or authorize its effect. Thus a changed or withdrawn review can
keep an unsubmitted note. Submission must separately recheck current permission,
request state and the captured basis; stale-note preview and outcome controllers
remain required before these entry points are installed. Files/voice and expired
or replaced prompt routing are separate controller paths.

`work_input_admission.py` admits captured answers/reasons and unchanged change
requests into the existing immutable Act journal. It re-proves the stored input
in the same final guarded writer; no caller text or fresh revision replaces the
captured body. A stale preview creates no Act, while a post-admission change is
rejected by the domain CAS. The receipt worker owns execution after commit.
Replay requires the original journal row to remain under lock; deletion cannot
convert replay into a fresh operation. Decision text remains unsubmitted until
a separate complete preview and explicit confirmation. Capture can borrow only
an active same-store writer. Result delivery and chat installation remain pending.

`work_input_previews.py` publishes complete input-owned decision confirmations and
fresh reviews for retained changes notes. Its V4 source ties every SDK send to the
original acknowledged typing prompt, accepted inbox text and reserved operation
identity. A new review preserves that text and gets fresh seen/CAS controls only
after full delivery. Unchanged changes notes keep the direct submission path.
Repeated refreshes retire old controls and queue keyboard removal; a claimed send
must settle first. Pending, uncertain or completed actions from the same input
prevent another intent. Captured inbox delivery dependencies retain these views.
Confirmation admission locks and rechecks the original input proof; restart or
caller text cannot replace it. Back restores the complete request and its choices
without applying an action. These components remain dormant until controller wiring.

`work_retained_drafts.py` owns explicit Edit note and Discard for a V4 review note.
Both recheck the whole acknowledged group and original input, refuse unresolved
actions or claimed deliveries, retire old controls and finish the old input in
one guarded writer. Edit publishes a new prompt against the displayed current
review; Discard queues only keyboard removal. Neither creates an Act or erases
the visible note. The draft's 24-hour deadline starts at original input acceptance
and survives preview refreshes. Stale note previews explain the change before
the complete current review. Closed/replaced-review recovery remains separate.

`work_input_refresh.py` preserves the original V4 input when its displayed review
changes before admission or the service's CAS rejects it. An exact pending
`attempt-unresolved` decision can refresh after its old operation is fenced with
cancellation/conflict; an unchanged preview retries the same saved body.
Unknown, missing, advanced or altered journals cannot authorize replacement.
The original prompt/input proof and complete current source share the guarded
writer with old-control retirement and new publication. Fresh controls await the
whole preview's ACK and a separate explicit press. Refreshed typed decisions
carry a closed flag derived from original seen data, so their stale notice is
revalidated at delivery without replacing the decision or note. No recovery path
submits a new action automatically; original Act bodies remain immutable.

`work_review_recovery.py` handles a captured changes note when its original review
has closed. A current pending review on the same Work can reuse the original note
only in a complete V4 replacement preview with a new explicit confirmation. The
replacement flag is derived from original and current request identities; the
original input time still fixes expiry. With no replacement, old controls retire
and a fixed closed/withdrawn notice settles the input without an action. Closure
state/revisions and the unchanged input share the final joined read, including
after settlement; drift rolls the entire transition back. Reserved confirmations
may enter only after an exact conclusive conflict: the full shown group, original
confirm control, Act scope and immutable command must agree. Unknown, pending,
missing or altered journals stay held. A known non-effect confirmation whose
review closed first checks its original operation and CAS; receipt reconciliation
still comes first. Direct text closure after admission uses the same recovery
only after its immutable operation conclusively conflicts.
Direct text admission records `route_basis.work_action_id` atomically with its
Act, preserving the captured `work_input`. Missing marked journals cannot be
reconstructed by input replay. Preview authority also checks orphaned V4 control
reservations, so restarting from the input cannot bypass a missing confirmation
journal. These existence probes use opaque text identities on both SQL dialects.

`work_outcomes.py` renders complete success/refusal/uncertain wording, without
issuing commands or installing controls. Unknown actions never become a failed
save. A known attempt-closing refusal keeps the original confirmation; a retained
uncertain operation offers only its original retry. Plain replies to decisions or
reviews are comments. The text renderer uses the plain wrong-recipient acknowledgement;
the outcome planner can replace it with current manager routing choices. Start/resume navigation is
web-only at this stage. The central `errors.text_key_for` reads typed refusal codes
and classes, never exception sentences; `key_for_reader` owns manager wording.

`work_outcome_reads.py` qualifies these views through the exact public service
receipt, current Work/request and a final joined account/grant/policy/event read.
It never calls an action or upgrades an uncertain journal to success. A receipt
found before journal reconciliation defers presentation until receipt-only journal
reconciliation; this does not authorize an automatic replay of uncertain actions.
The returned `receipt` is canonical; the journal's cached `act.result` is not a
source for success content. Delivery still needs its own claim/part fence and
current read immediately before each SDK call; these readers install no handlers.
The current attempt's status distinguishes running from starting/stopping/uncertain
wording; unsettled attempts offer no resume action. An activation whose captured
expected Work revision includes the canonical action event is a later continuation.
That comparison uses logical revisions, not wall-clock times. Delayed replies after
such a continuation confirm only the saved effect and make no claim that work has
not started. Human-input outcomes match both their request and Work event receipts.

`work_outcome_delivery.py` publishes receipt-owned result replies. Each send
re-reads the exact action and canonical receipt and fences current account,
reader grant, protection, destination, settings, input ownership and claim in one
final SQL snapshot. Complete escaped text spans ordered parts; only the last part
carries navigation or the original operation's Retry action. Completed actions
retire their original controls without repeating the command. Input completion
and a versioned Work-only delivery receipt in `Act.continuation` commit with the
complete Telegram ACK. Collected outbox history cannot republish an acknowledged
phase; pending deliveries retain their action through cleanup. Obsolete unsent
plans can be replaced while their captured input remains unfinished.

This planner and source remain uninstalled. Completed routing uses a closed V2
edit proof tied to the original command's acknowledged selection, message and
protection. The final SDK check and ACK settlement recheck that proof. Protection
upgrades use a new protected reply and clear the previous keyboard, including
when an earlier queued result already consumed its controls. Missing/incompatible
anchors hold with `work-outcome-refresh-required`; the next receipt observation
replans current content as a new message, retaining old ACKs and the original
input/action. It never repeats the command. Assignment topics and relocation
belong to the later topic feature.
`work_routing_followups.py` gives a completed wrong-person response one direct
routing reply through a closed V3 outcome basis. It uses the canonical response,
current manager/source/privacy and full recipient roster; others retain the plain
acknowledgement. Explicit replies bypass proactive preferences. Full ACK both
settles the input and enables recipient choices. Routing admission rechecks that
shown reply, original completed response and both domain events in its final
writer, alongside current manager and selected-account authority. The new choice
owns a separate immutable routing operation; its success edits the exact shown
selection. A damaged ACK cannot supply captured choices. Already acknowledged
outcomes remain deduplicated.

`work_followup_refresh.py` refreshes an acknowledged recipient list only through
its actual old choice. The V4 outcome carries that consumed refresh proof, while
keeping the original completed response and input immutable. Known recipient
refusals may refresh; unresolved routes retain their original operation and controls.
Fresh choices inherit the old lifetime and activate only after full ACK. Both the
writer and SDK reprove the consumed choice and original input after other awaits.
Source drift holds the refresh with `work-outcome-refresh-required`; explicit
replay can repair that saved intent into a new source-qualified delivery, preserving
old parts and never repeating the command. The input stays completed throughout.
Retention pins that exact input while current, unexpired recipient choices or
pending refreshes need it; pending delivery also retains the consumed proof and
its old parent. Ordinary age collection resumes when those dependencies end.
The full controller still owns fresh-request recovery, dispatching explicit action
retries and refreshes, and installation of the receipt observer.

`work_outcome_recovery.py` observes Work journals without issuing commands.
`Acts.reconcile_receipt` shares the original operation's local serialization and
settles only an exact canonical receipt under current reader/binding authority;
absence leaves the action unchanged. This differs from an explicit `Acts.check`,
which can repeat the original command. Observation never calls that retry path.
Each tick scans one bounded keyset page for one binding with a fixed maximum ID
for that pass; new records cannot postpone revisiting older deferred actions.
Acknowledged phases need no new observation. Per-link tasks share eight slots,
so a held link cannot drop another link's tick. The worker owns and drains all
tasks across stop/cancellation and fences publication after shutdown. These components remain uninstalled until the full controller wiring.

`work_callbacks.py` is an optional dispatcher for acknowledged previews, typing
prompts, confirmations, routing, Show and explicit Retry. The adapter consults it
only when a collaborator is installed; the supervisor does not install it yet.
It reserves the existing immutable operation before checking it, and lets canonical
outcome planning own result replies. A result-read failure after saving an action
is deferred to receipt observation, never reported as a failed save. Direct
wrong-person routing uses the reply-owned refresh coordinator when a selected
recipient loses access. General routing uses a locked acknowledged choice to
refresh the current roster, admitting only an unreserved choice or its exact
terminal recipient refusal. Missing journals and unresolved actions cannot be
replaced. The separate V6 `choice_origin` proves a consumed routing control and
its complete historical ACK, keeping explicit refreshes as direct replies even
when notifications are off. It survives refresh/repair and callback retention;
final SDK admission requires it independently of Show. Known attempt-unresolved
confirmations consult refusal recovery before
checking the same operation again. Input-owned V4 confirmations stay with their
retained-input owner and cannot enter generic refusal recovery.

`work_stale_recovery.py` publishes a complete current request with the fixed
changed-message notice after an unreserved stale control or its exact conclusive
conflict. It retains the original action and validates the consumed source group
through final SDK delivery. The closed `current` interaction view preserves all
review content and comments; fresh controls arm only after the complete ACK.
Retained-input drafts, closed/rerouted requests and routing revision conflicts
still need their remaining orchestration.

`work_action_retries.py` accepts only a complete ACK of an uncertain Work outcome
and the exact original operation/body. Its guarded writer rechecks shown receipts
after the outer guard awaits. `Acts.check` can compare the captured attempt count
under the existing operation lock; duplicate presses of one shown Retry return
the current record without another attempt. A late canonical receipt settles
without a command; retired callback lifetimes cannot start a new effect. This
retry primitive never creates an Act or substitutes a current revision. The
shared `acknowledged_parts` validates ordered unique typed ACKs for outcome
settlement, routing choices and Retry; source and domain authority remain separate.


`work_text_inputs.py` connects accepted plain text to its exact shown Work prompt
through the optional adapter collaborator. The prompt and original accepted text
are captured before admission. Answers, inability reasons and unchanged changes
notes submit once with their displayed basis; changed changes notes retain their
text and get a complete current review before a new confirmation. Own decisions
and decision notes always publish a complete input-owned confirmation first.

Duplicate accepted input reads the original receipt after its first attempt.
`resume` rereads stored input by identity, ignoring caller copies of text/basis;
it can settle a late canonical receipt without another command. An existing
ordinary input cannot switch into a new Work prompt. An admitted decision owns
recovery until its receipt outcome is delivered, so resuming its source input
cannot create another confirmation. No model run or Work activation is started.

This owner and its startup recovery are not installed by the supervisor yet.
Direct replies to question cards without a typing prompt, untied text, comments,
voice/files and expired or replaced prompts remain separate required input flows.


Input-owned navigation settles a V4 decision's original input on Back or Add a
note, just as review Edit/Discard settles a retained note. The old typed value
stays visible and immutable; a new prompt owns a separate input and any eventual
confirmation. The transition's final joined read rechecks original input and
blocked actions even after its own settlement. Back/Add note/Edit also recheck
the displayed source and current permissions. Discard does not require the old
review to remain pending. A changed unreserved decision keeps the original text
in a complete fresh preview instead of applying navigation to an unseen basis.
Back/Add note remain available while actions are paused, including their
unreserved stale refresh. An admitted unresolved confirmation continues to own
recovery.


`work_card_inputs.py` supplies direct-card capture/admission separately from
prompt capture. A plain explicit Telegram reply to any part of a complete,
acknowledged V1/V2/V3 request card owns `work_input` version2. It freezes the
shown source, exact quoted message, complete current control group, text and one
operation id before any domain admission. The old group retires with a durable
keyboard cleanup. Prompt and input-owned V4 views are excluded from this owner.

Admission creates only Respond: an information answer or a decision/review
comment. Current Use+Inspect and route write authority, original request and
assignment revisions, exact request instance, current request basis and unfinished
input gate new admission. Replays keep
the same operation and missing marked journals stay held. The final joined read
reproves complete source ACKs, canonical consumed controls, original input and
immutable Act after external owner preparation. Captures retain their immutable
input across routing and delivery bookkeeping instead of adopting late mutations.
The optional text controller uses this primitive for actual Telegram replies,
observes canonical outcomes and resumes stored inputs without resubmitting an
unknown operation. A reply predating a newer presentation's dispatch cannot be
reinterpreted against that presentation. An explicit reply received before the
host saves its ACK remains eligible once the full delivery is acknowledged.

Replies to any part of a complete prompt use the same prompt owner. Known
spent cards, closed prompts and input-owned presentations retain a durable
`work_card_hold` route, never ordinary-chat ownership. This hold is an internal
recovery foundation; its user choices and current-comment-card refresh remain
required before activation. The supervisor does not install these controllers.

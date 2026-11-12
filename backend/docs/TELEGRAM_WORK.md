# Telegram Work and notification contracts

Read alongside [the managed bot guide](../app/telegram_bots/AGENTS.md).

`work_file_exports.py` exports one indexed source of the current Work outcome
as the human recipient. Outcome, evidence and assignment revisions, source
identity, referenced Space generation and READ+EXPORT grants are checked in
the provider-final snapshot. It uses the referenced Space, which need not be
the agent's Home, without workspace or execution readiness. Callers can compose
their exact delivery projection into that same statement; `check_delivery`
may acquire the claimed link/outbox locks in the borrowed writer. An earlier
unlocked claim read is insufficient. Byte limits remain caller-bounded, and
protected dispatch requires the actual protection flag. Reads drain cancellation
and recheck lifetime after native I/O and transaction cleanup. This export
primitive does not install workers. `authorize_work_file_batch` admits all album
Spaces together before claim locks and the combined provider-final snapshot;
review delivery also requires the current `agents:write` route ceiling.

`work_result_delivery.py` provides dormant initial review-file plans. Its closed
V1 `result_files` capsule augments the existing request basis, with files before
the complete card and a filename trailer. Outcome indices preserve identical
paths in distinct Spaces. `work_result_ledger.py` qualifies the ordered parts,
explicit photo/document rewrites, first-byte hashes, per-member album receipts
and visible failure notices. Known file ACKs survive ordinary card transport
retries, and the full required sequence must finish before controls activate.
`work_result_upload.py` reserves the entire bounded batch before reads and uses
the existing SDK upload leaf. Every final upload snapshot combines canonical
outcome references, all Space operation guards/grants, request, claim and owner.
Generic text recovery cannot discard this media owner. `work_result_recovery.py`
owns the first protected Show: qualify the acknowledged teaser and its ledger,
consume its exact control, then clear that keyboard before any files or the new
card. A retry or missing teaser uses the durable cleanup receipt; full delivery
still gates the new controls. Refresh creates a new card version and uses
`work_result_history.py` to qualify the original media recipes, rewrite ancestry,
byte hashes and actual historical receipts. Carry events retire covered fresh
roots without inventing sent parts. Unfinished files retain their byte identity
and photo rejection fallback; the new card uses current redaction and locale.
Historical ACKs retain their actual protection flags and need no current Space
grant. An opaque canonical outcome/source fingerprint is captured from the raw
Work row and checked again in the provider-final recovery and SDK projections;
redaction cannot substitute another hidden source. History must cover actual
past receipts and remembered hashes, and every current plan must carry all known
ACKs exactly once. Compare historical JSON with strict Boolean/integer identity.
A protection upgrade creates a new teaser before any further content;
the generic sender must not overwrite a frozen result's protection recipe.
Explicit repair rebuilds only a held failed General result with all claim fields
cleared, under current source and owner guards. Non-General retained failures
stay held pending a separate source-aware destination transition. It preserves file ACKs and refuses
changed canonical outcomes. `work_result_navigation.py` binds failure links to
the outcome's exact attempt and rechecks its Work/thread in final joined reads.
The captured origin must still equal the prepared live configuration after every
provider await; coherent stored URL substitution cannot authorize another origin.
Unavailable-file warnings have no navigation; other file failures reuse durable
URL-button/plain-address fallback. Historical warning proofs retain their own
navigation snapshot when the current card changes.
These initial-delivery components must remain uninstalled until the remaining
notification paths and supervisor integration are complete.

`result_notice_sources.py` owns non-review `result.done` and
`result.waiting_review` facts. Its joined provider-final read binds the exact
settlement, outcome attempt and requester/creator eligibility to the current
Work, reviewer, agent name, account/grants and visible Space references. The
initial displayed outcome metadata must match the raw canonical outcome before
sealing its opaque signature. Suggestions retain their full canonical event and
exact recorded attempt provenance, including when the stored suggestion is from
an earlier attempt. Redaction never substitutes the raw source identity.

`result_notices.py` renders the full statement and optional suggestion, with
files before the card and no review or activation controls. Titles use the
existing bounded first-line rule. An unnamed reviewer uses the existing localized
colleague label; it does not suppress an otherwise valid unrouted-review result.
`result_notice_delivery.py` preserves the intent's delivery identity and qualifies
its own closed basis before sharing the result-media ledger and upload leaf.
The final SDK snapshot also binds the exact sending claim, preferences, quiet
hours, Show controls and protection. This read-only result path needs current
Use+Inspect and per-file READ+EXPORT; it does not require a review write ceiling.
Generic fixed text cannot impersonate this source family.

`result_notice_recovery.py` owns protected Show and source-aware refresh. It consumes the
actual acknowledged teaser, waits for durable keyboard cleanup, then sends files
and the complete card. Waiting-review notices retain their forced-silent flag.
Consumed Show proofs and unfinished cleanup dependencies survive retention.
Result preview cleanup hashes its parent/version/group identity so its
deduplication key fits the outbox's 96-character SQL limit on both dialects.
Locale, visible-source and policy changes rebuild the current presentation while
preserving actual media ACKs, initial byte hashes and photo-rejection fallback.
The originating event and raw outcome identity cannot change. Historical media
proof version 2 binds this family's result recipes; review proofs retain version
1 and cannot cross families. A protection upgrade requires a fresh Show before
the remaining content, without relabeling or repeating an earlier upload.
Explicit repair accepts only known held General failures with no claim fields;
active/uncertain sends and retained non-General destinations stay untouched.
These owners remain dormant pending the remaining notification integration.

`work_event_intents.py` projects current recipient/grant/preferences into empty,
content-free delivery intents in `WorkEventReader`'s pair writer. Ordinary
events omit their human actor; status, existing-question edits, routing to self
and the answerer's reminder have their explicit exceptions. Same-operation
HumanInput receipts identify responses; supplied management comments create no
answer notification. `work_event_facts.py` resolves exact recorded attempts or
the activation preceding the event, never the latest attempt. Request identity
uses canonical receipts or Work blocker/outcome basis, not wall-clock ordering.
Question edits retain their specific request/assignment revision, with a route's
old edit revision separate from its new question revision. Delivery still has
to prove a current exact acknowledged anchor before editing anything.

Review creation shares the Work settlement event in the current domain service.
That event can create both a manager routing question and a requester result;
class-specific event dedupe preserves both. Progress and edits retain their
dedicated keys. Questions record a bounded decision hold, reminders their exact
response and one-hour due time. No intent starts work or sends content. Empty
intents cannot be claimed as delivered, even when proactive sending is opted
in. Content materializers must still qualify recipient access, source freshness,
protection, complete previews/files, exact edits, quiet-hour reminder suppression
and decision-hold release before supervisor installation enables this path.

`work_event_delivery.py` materializes information and decision question intents
into the existing request source family without allocating a second delivery.
It preserves the exact request/assignment revision and causal attempt; decisions
wait for that attempt to settle or their original hold deadline. Provider awaits
are followed by a joined source/account/grant check in the borrowed writer.
`work_event_source.py` hashes canonical Work event, HumanInput receipt and saved
intent metadata. The final SDK snapshot rechecks those facts alongside the
current request, recipient and delivery claim. A stale unsolicited question
cannot refresh into a question from a later routing revision. Acknowledged Show
owns its separate explicit current-source proof. New controls stay pending
until the whole presentation is acknowledged. This materializer remains dormant;
edits, status and summaries require their own integration.

`work_review_event_delivery.py` connects review-ready settlement and routing
intents to the result media owner, preserving the original outbox identity.
Its final writer snapshot combines the canonical event/receipt with the raw
outcome, exact outcome attempt/thread, current request and recipient authority.
The event attempt must match the outcome attempt. Files precede the complete
review card; protected delivery uses the existing Show/keyboard cleanup flow.
The SDK rechecks the original event before any file read or card send. This
materializer is also dormant and does not enable notification workers.

`work_routing_event_delivery.py` borrows that materialization pipeline for manager
questions, including an unrouted review created by settlement. An exact unsupplied
response may justify routing even while its original recipient remains eligible.
The routing basis retains that response ID and complete explanation through
refresh, Show and repair; another response cannot substitute for it. The final
snapshot combines the canonical event proof with the entire current candidate
roster and manager grant. `work_routing_response.py` also joins the actual response,
its HumanInput receipt and Work event, comparing the full displayed explanation
with canonical command data. That proof remains required during explicit Show
and V6 recovery, after every provider await. The same bounded hold applies while the causal attempt
is unresolved. Existing explicit wrong-person follow-ups keep their own receipt
and source family. This event materializer is also uninstalled.

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
Reservations recognize canonical `event:` intents as well as retained `ev:` rows.
A digest is silent only if every member would be silent; quiet hours are still
applied at dispatch. Silent preview prefixes do not silence an audible final part.
`work_digest_text.py` supplies bounded navigation in both locales: at most ten
items, one 32-character item button per row, question/decision/review phrases and
product first lines for other news. Concealed navigation exposes only a count and
Show. These bounded titles fit one message; item activation must still deliver a
fresh complete member preview. This pure renderer supplies no source authority or
callback admission and remains uninstalled with the reservation primitive.

`work_digest_projection.py` composes up to ten source projections into one final
authority statement, preserving each member's route permissions and distinct
source roles. Each mapped entity occupies one JSON result column so complex
batches stay below PostgreSQL's result-column limit. Nested JSON keeps its exact
types; only SQL dates and booleans are normalized into read-only source facts.
Account and role joins occur once outside member subqueries. Missing members
fail qualification; compiling a dialect is not evidence of runtime execution.

`work_digest_members.py` derives navigation from current question, routing,
review, result and colleague-news owners. Saved sources must be untouched,
unsent presentations; a pending-preview validator never invents a dispatch.
Final projections retain source events, exact claims, referenced Spaces, routing
rosters, result identities and optional route permissions. Digest titles do not
read file bytes or grant action authority. Delivery and item controllers still
own whole-batch claims, acknowledgements and complete preview admission.

Digest ACKs must match the detached manifest admitted before the SDK await.
A conflicting receipt retains the original manifest, parts and returned message
IDs for reconciliation; it cannot settle a different membership or release the
original reservations. Retention keeps this evidence and its original members.
Parent silence is derived from the current member choices, not independently
trusted from the batch row.

`work_digest_navigation.py` reads current full sources behind an acknowledged
digest. Its final composed snapshot retains the exact batch receipt, every
reservation edge, the complete acknowledged callback group and the selected
member's authority. Reading a preview never consumes controls or changes the
original delivery receipt. This admission helper is not a controller and remains
uninstalled until Show and item presentation owners are integrated.

`work_digest_delivery.py` freezes one bounded navigation card and its complete
pending keyboard. The adapter requires this owner for digest delivery; raw class
labels and templates do not supply source authority. The final compound read
rechecks every member plus the parent's claim, callback group, preferences,
quiet hours and reminder due time. Changed source or presentation choices retire
the pre-ACK group and release its original members explicitly. Transient retries
keep membership reserved; typed whole-card ACKs settle members together. An
admitted digest SDK call drains during shutdown, preserving ambiguous receipts
and reservations. These plans and sources remain uninstalled; Show/item handling
and startup integration are still required.

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
Acknowledged phases need no new observation. A conclusive conflict or refusal
whose presentation moved to a fresh preview cannot publish an old outcome.
The source-qualified writer records a version-2 `work_outcome_transfer` on the
original Act, distinct from a version-1 acknowledged delivery receipt. It survives
callback/outbox cleanup, applies only to that conclusive phase and expires with
the settled journal. Transfer retires earlier outcome phases and their controls,
including failed deliveries, while preserving acknowledged history. An active
claim must drain before the successor can take ownership. Observation reconciles
canonical receipts first: later success still wins. The transfer proof shares the final source/authority check
for publication and SDK dispatch; queued obsolete replies retire without changing
the new preview or claiming delivery. Completed and uncertain operations retain
their receipt and retry handling.
Per-link tasks share eight slots,
so a held link cannot drop another link's tick. The worker owns and drains all
tasks across stop/cancellation and fences publication after shutdown. These components remain uninstalled until the full controller wiring.

`work_callbacks.py` is an optional dispatcher for acknowledged previews, typing
prompts, confirmations, routing, Show and explicit Retry. The adapter consults it
only when a collaborator is installed; the supervisor does not install it yet.
It reserves the existing immutable operation before checking it, and lets canonical
outcome planning own result replies. A result-read failure after saving an action
is deferred to receipt observation, never reported as a failed save. Preview-only
read failures report `tap.failed` while the original button remains unconsumed;
an ambiguous committed preview stays silent. Conclusive `storage-busy` refusals
also report `tap.failed` and wait for an explicit press. That press checks receipts
first and retains the same body, operation, first-attempt clock and all current
authority/revision guards. Its locked original acknowledged control must still
be usable, and a transferred presentation cannot reopen; the final effect read
also rejects retained transfer receipts. Timers, unknown outcomes, cancellation and other
refusals do not gain retry permission from this path. Direct
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
Untied last-question text and voice/files remain separate required input flows.


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
acknowledged V1/V2/V3/V4 request card owns `work_input` version2. It freezes the
shown source, exact quoted message, complete current control group, text and one
operation id before any domain admission. The old group retires with a durable
keyboard cleanup for ordinary cards. Prompt views use their own capture owner.
On an input-owned V4 confirmation, a comment leaves the original draft input
and controls intact. Its later confirmation uses the existing complete-preview
refresh to retain the unsubmitted note or decision alongside the new comment;
only another explicit press can submit it. Expired or already-reserved controls
cannot admit a fresh comment. Once captured, comment replay keeps its separate
operation and acknowledged source even after navigation closes the original
draft. An uncertain admitted comment reconciles its receipt without resubmission.

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
spent cards and closed prompts retain a durable
`work_card_hold` route, never ordinary-chat ownership. `work_closed_inputs.py`
offers explicit Reopen, Send as new message and Discard choices. Reopen verifies
the original target and current rights, settles the held input without submitting
it, and shows a complete fresh prompt. A spent decision/review card reopens a
comment prompt; an original typing prompt retains its purpose. An unavailable
target offers only Send as new message and Discard. The former uses the existing
stable callback publication owner; the latter publishes no human input.

Unquoted text after an expired unresolved prompt follows the same hold. Commands
and explicit navigation clear that implicit guard; a later explicit reply still
identifies the old prompt. Detached spent-card contexts never replace an active
typing prompt. Unsettled choices retain the original prompt context through
cleanup. Reopened V3 delivery rechecks the actual held input, consumed Reopen
choice, acknowledged notice, and current Work/Space authority at dispatch. The
supervisor does not install these controllers.

Completed decision/review comments can use outcome V5: the canonical response
receipt owns a complete current request and fresh controls in the same delivered
reply. Only a still-pending request for the current recipient with management
rights gets those controls. Other saved comments keep the plain acknowledgment.
The input settles after the entire card is acknowledged; each later reply owns
a new Respond operation, while a decision or acceptance still requires its own
preview and press. Existing V1 acknowledgments remain deliverable.

Both request previews and full comment outcomes use `work_source_spaces.py` to
join all referenced Space statuses and READ grants into the final SQL statement.
The comment planner checks the same facts before publication. No extra provider
read or prior successful read substitutes for that last dispatch check. A failed
multipart replacement preserves its ACK history and rebuilds from the original
response receipt; it never repeats the comment command.

`work_untied_source.py` scans the complete chat/topic/binding history for the last
acknowledged Work question before an unquoted plain-text input. A later received
message or bot message prevents the fallback. `work_untied_inputs.py` runs after
ordinary clarification routing, retaining the actual input until Save as answer
or It's a new message is pressed. It never fabricates a quote or typing prompt.
Save captures a Respond operation owned by that callback; review and decision
text remains a comment. New-message publication retains one callback owner across
replays. Sibling choices cannot win after either operation is reserved. The unfinished input
pins its original question as a delivery dependency until settlement. Changed
questions retain the text for existing closed-input recovery and a fresh choice.

The fixed consent notice has its own `work_untied_delivery.py` source authority.
It checks the exact sending claim, complete original question, retained input and
canonical choices in the final joined account/grant snapshot. Compact callback
facts avoid repeating full outbox JSON. Admission preserves the original input
snapshot through its own writes, rejects finished input and compares JSON types
strictly. Receipt-only replay does not repeat an unknown response. These optional
controllers remain uninstalled until notification and lifecycle integration.

`work_voice_inputs.py` captures an actual quoted card or shown typing prompt
before transcription. Ordinary clarification keeps priority over an unquoted
last-question fallback. Forwarded voice retains ordinary behavior. Source and
prompt context remain pinned while transcription or consent is unfinished;
completion refuses a changed captured context and never transcribes again.

The answer-consent capsule's V2 variant preserves the original recording
descriptor, prompt purpose and displayed revisions. It shows the full transcript
across ordered parts, with Save/Discard only on the last part. Save requires the
complete typed ACK and records one Respond; cannot/wrong preserve their
disposition. Spoken decisions, decision notes and review changes use their
existing specific confirmation owners. Even an unchanged spoken review note
needs a press. Ordinary text submission semantics stay unchanged.

Discard clears callback transcript payloads and classifies the same transcription
audit as discarded, preserving its duration, provider and budget charge. Known
completed responses clear their Save payload; receipt-only reconciliation still
uses the original immutable Act. Source retirement retains Work ownership and
offers existing closed-input choices. Overlong transcripts remain complete and
offer reentry or a new-message choice without weakening the Work text limit.
Those multipart recovery choices also require every transcript part's ACK.
Send as new reclassifies the existing voice audit as a message. These optional
controllers remain dormant until supervisor integration.

Consent and transcript recovery notices keep their original destination after
a missing-topic response. The ordinary failed-delivery journal and Retry delivery
control preserve acknowledged parts and retry only the unsent suffix, with no
new transcription or Work action. Generic presentation relocation must not
rewrite their version or move the input to another topic.


`work_file_inputs.py` captures an actual document/photo reply as Work input V4.
Its native FilePuts journal V4 retains the same original media, input digest,
request/source and reserved Respond operation. The native writer rechecks that
unsealed input after all provider awaits. The digest also binds response admission
to the original caption, so a later input snapshot cannot replace what was sent.
The saved path becomes response text; no fabricated Work source or additional
Space grant is introduced. No chat, Work run or pending ordinary attachment is
created by answering. The current Home/instance, native receipt and original input
share the final Respond transaction snapshot. Once Respond is committed, receipt
reconciliation uses the frozen admitted file intent despite Home changes or native
journal collection. Named-file acknowledgements hide internal paths and settle the
input only after the full delivery ACK, with a final joined immutable input check.

A closed question retains its media. `work_file_handoff.py` reserves the actual
Send-as-new choice as `work_file_new` before any missing file is saved. Native
admission rechecks the original input and selected callback; other recovery choices
cannot take that reservation. A terminal Work file journal transfers to ordinary
V3 ownership without changing its operation, body, target or result. Uncertain
files require the existing explicit File Retry; neither handoff nor restart
repeats their effect. A caption then enters the stable callback publication as ordinary chat; existing
saved files reach the actual manager as receipt-backed attachments. Without a
caption, the file stays pending for the next text and a saved-file question starts
nothing. The original callback and update IDs own interrupted handoffs; recovery
reconciles metadata and never downloads or repeats an uncertain native upload.

Metadata-only startup recovery recognizes V4 failed retries. Completed file retries
remain owned by Work-input recovery, which observes/admits the response and its
named-file outcome rather than an ordinary file-received question. Whole recovery
entry points drain repeated cancellation. Unfinished Work file inputs retain native
receipts beyond the ordinary terminal-journal age; historical JSON filters use text
comparisons before closed-shape validation, never PostgreSQL integer casts.

The optional album collector commits `work_album_pending` with the first actual
input, before the quiet-window join is acknowledged. It resolves the complete
stored cohort before ordinary file effects, including an unquoted member preceding
the quoted question. Exact scope includes link, chat, topic and binding generation;
prompt boundaries remain distinct. Ordinary decisions persist before admission,
which still partitions by each original reply/prompt scope. A Work cohort retains
all actual members and pins its sources without consuming controls or uploading.

The closed album manifest records member IDs, prior cohorts, sources and an actual
quoted root only for one unambiguous source. Conflicting explicit quotes have no
selected root. Later members retain the prior Work owner; a late Work quote cannot
repurpose an already ordinary cohort. Malformed or oversized groups stay held.
Classification failures expose no ordinary Retry button. Startup classifies pending
metadata before generic interrupted-input recovery, without downloading, writing
files or starting a model. Pending collection conservatively pins potential Work
source outboxes in the same scope until classification; source removal must not
change a Work reply into ordinary chat.

`work_album_inputs.py` captures a ready cohort in the same guarded writer as
source-control consumption and its sole Respond operation. The actual quoted root
keeps Work input V4 and its own single-file raw media; followers use
`work_album_member`, never a duplicate Work action marker. The closed `work_album`
capsule binds every immutable actual input, root reply basis and operation. Complete
captions plus bounded native fallback/collision name growth must fit Statement
before any file write. An oversized cohort stays retained without uploads.

Native journal V4 uses Work context V2 for a member, including its original input
digest and frozen group context. Final native admission checks the complete group,
including newly pending/late members and competing publication/action ownership.
`work_album_files.py` reads every completed native journal with full scope and target
proof before one Respond. Its final compact SQL packet revalidates the entire
journal after provider awaits. No ordinary pending-file context or chat run is
created. A failed member gets its own File Retry; recovery reconciles missing or
interrupted siblings as uncertain instead of downloading them automatically.

`work_album_answers.py` owns live submission and metadata recovery. Once a Respond
journal exists, replay reconciles its fixed body/receipt before any native file
work, including after Home changes or file-journal collection. Work file V2 freezes
all actual saved paths and captions. The existing outcome evidence column carries
root/member proofs without shifting the shared delivery projection. The outcome
records the original group context and frozen-file digest; a whole ACK must match
that identity even after SDK I/O. Every original member registers that outcome and
is sealed together after its final part. Unfinished followers retain their native
receipts, and root/member SDK redeliveries cannot enter ordinary chat.

Late or conflicting cohorts retain their own members without another response.
`work_album_recovery.py` gives a whole cohort explicit recovery choices. Only one
known current source offers Reopen; ambiguous sources cannot select a request.
Oversized answers explain the bound before upload. Discard settles every member;
Reopen shows a fresh question without submitting the retained files or captions.
Known-unavailable Reopen preserves the group for Send as new or Discard. An original
Respond marker or unresolved journal prevents any purpose transfer.

`work_album_handoff.py` transfers an explicit Send-as-new choice before ordinary
publication. Native IDs, bodies, targets, selected children and receipts stay fixed;
only their Work purpose metadata changes. Unstarted/interrupted siblings reconcile
as uncertain on restart. Captionless groups retain saved files and ask once without
starting a model. Captioned groups stage one callback publication with all actual
input snapshots; original raw quotes and captions remain unchanged. Ordinary input
scope rules stay strict, while Retry reconstructs authenticated callback-owned
origins. Native receipt retention follows exact pending callback snapshots, including
successors after original inbox rows are collected. A live Retry also retains its
exact predecessor callback group; expiry or completed transfer releases obsolete
owners and receipts.

The closed recovery capsule binds input timestamps, member order, original route
basis and operation IDs. `work_album_guard.py` joins complete group, choice and
original-action facts to the final account/grant/policy snapshot after provider
awaits. Metadata recovery requires Use; only Reopen additionally requires Inspect.
Native transfer also checks the whole acknowledged choice group and presentation.
Reopen's source proof ties all settled members to the original request and fresh
prompt; the collector still recognizes their retained Work ownership after any
choice. The single-file recovery path refuses group roots. All Work controllers
remain dormant; notification production and lifecycle installation remain pending.


Colleague news remains dormant. `work_news.py` renders complete escaped
statements and optional notes; its first line contains only product words and
bounded navigation names/titles. Protected teasers contain no assignment or
colleague details. Presentation alone grants no delivery authority: generic
product replies cannot send these source keys.

`work_news_sources.py` currently owns canonical reads for edit, cancel, reopen,
accept, changes-requested and decide notifications. The current recipient must
retain Use+Inspect. Final provider reads join the exact event, current Work,
original activation/attempt, actor name and delivery, including unclaimed-row
checks. Historical requester selection never substitutes a later activation.
The originating event signature stays separate from mutable display facts.
Work stores one decision statement; render it once as the decision rather than
inventing a separate choice or duplicating it as a note. `work_news_delivery.py` materializes these six command families on their
original rows. Its closed text recipes bind actual ordered ACKs, current locale,
quiet hours, notification preferences, live page origin and exact claims in the
final provider snapshot. Protected Show consumes its acknowledged teaser;
`work_news_recovery.py` edits a single-part notice in place or waits for keyboard
cleanup before sending a multipart notice. Original teaser and consumed Show
proofs remain qualified, and cleanup/Show retention follows the parent.
A missing anchor/topic is held as `work-news-refresh-required`; generic repair
cannot rewrite the source-owned plan. Source-aware refresh replaces locale,
protection and URL presentation under the exact claim, preserving the immutable
event and every historical ACK. V3 keeps a flat per-version list of qualified capsules and
parts; only unfinished parts retire, and repeated refreshes retain bounded JSON depth.
Recoverable presentation drift at final SDK admission defers delivery; immutable
event changes and withdrawn authority still retire it. Explicit repair accepts known General
failures after all claim fields clear. It never moves a topic or repeats a Work
operation. Refreshed teasers still require their own actual acknowledged Show;
a missing edit target is cleared before a complete replacement message. The
`work_answer_news.py` additionally joins the exact information-response receipt
and saved response at materialization and final SDK admission. It validates the
canonical response command, operation, actor, purpose and request/assignment
revision, including supplied disposition and every saved source. Management
comments and unsupplied responses cannot become answer news. The original
requester gets one silent notice; the responder is excluded. Open assignment
is a web link only when no attempt is unresolved, rechecked in the final snapshot. Other news kinds
and installation remain pending.


`work_stop_news.py` qualifies colleague Stop, edit and withdrawal notifications
against the settlement's explicit attempt/run and its original activation. Only
the latest intervening canonical command can supply the actor, and a person's
own command sends no colleague notice. The original requester receives it
silently; Stop/edit offer a web assignment link and withdrawal has no control.
Command and settlement bodies, revisions and the cause timestamp remain part
of immutable provenance even after a later attempt starts.

`settings.format_time` uses HH:MM on the recipient's current day and a localized
day label otherwise. `NewsClock` prepares ZoneInfo off-loop and reads its injected
clock at each source/provider snapshot, so crossing midnight or changing the
timezone defers the old presentation for refresh. Untimed edit/withdrawal notices
leave preference drift to the ordinary final settings fence. These sources remain
dormant pending installation; progress/status messages are separate.

`work_proposal_news.py` qualifies a derived assignment's exact source Work,
attempt and activation, including its original requester and agent principal.
It does not infer a parent report from timestamps. The child must remain open
and never have started; the complete current objective and success criteria are
rechecked at final delivery and refreshed when changed. The full proposal offers
Open assignment on the web and Not now. Protected proposals first offer Show.
Not now consumes only the exact fully acknowledged proposal group and queues
durable keyboard cleanup in the same guarded transaction. It does not modify,
withdraw or start Work, and it can clear an old proposal after Work changes.
An already consumed group cannot repeat dismissal. These owners remain dormant.

`work_resume_reminders.py` reuses the canonical information-answer receipt and
targets its answerer. It requires the same answered blocker and assignment with
no unresolved or later activation, stop or assignment change. The final SDK
snapshot checks the response's one-hour due time independently of queue timing.
Reminders remain essential, silent and governed by question preferences. Current
quiet hours plus an enabled summary retire unsolicited delivery as `quiet-hours`;
an explicit protected Show is still served. Resumed or changed Work retires as
`resumed`. The source-owned sender preserves these closed skip reasons under its
exact claim fence. Refresh/Show history keeps the original reminder class, and
the only navigation is a web assignment link. Installation remains pending.

`work_attempt_notices.py` qualifies failed, contained, service-restarted and
no-result settlements against their explicit attempt and activation. Current
run metadata, when retained, must match the Work, attempt, requester, thread and
idempotency key; failure wording uses only its closed `stop_reason` vocabulary.
Host containment uses the exact event/attempt reason and attachment. A restart
notice refuses an intervening stop/edit/cancel. Saved progress belongs to the
settlement snapshot, and conversation navigation keeps that attempt's thread
even after a later activation. Manager and administrator forms come from the
current joined grant and account/role limits. Files-full advice exposes Files
only to managers; containment reasons have their specified files handover.
`attempt_text.py` renders complete escaped progress and both locales, omitting
claims that work was kept for unstarted, restored, moved or unknown outcomes.
Protected Show and refresh reuse the source-owned ACK/history boundary while
retaining the interrupt class. Generic fixed replies cannot impersonate these
notices. `attempt_output.py` accepts only the exact successful run's capture
receipt and durable source event, including the original assistant identity,
event sequence and text digest. Capture/receipt identity is immutable after
planning; coherent replacement retires delivery instead of adopting new output.
It never reads the latest conversation message
or cached answer. Missing, changed or unsupported proof omits the excerpt.
The source and current conversation binding are joined into the final provider
snapshot. Current `threads:read` permits the original conversation link;
`runs:read` is additionally required for an excerpt. Deleted, rebound or recreated threads
lose both. The nonactionable excerpt is clipped to 600 characters at a word
boundary; protected previews conceal it until Show. Attention projections that
need optional route ceilings run their checks after provider resolution against
a fresh joined snapshot. Installation remains pending.

`work_question_updates.py` qualifies closure facts independently of request
actability. Answer updates require the exact supplied information response and
HumanInput receipt; routing/withdrawal require canonical command receipts;
assessment requires the nonhuman Work report and its exact HumanInput assessment.
Question, assignment and request revisions stay tied to the original delivery.
These silent edits bypass discretionary notification preferences, retaining all
current account, instance and referenced-Space READ checks.

`work_question_edits.py` waits for the original complete question ACK. It validates
the original control group before consuming it, removes the keyboard and updates
the closure footer. Full question text stays intact; an overflowing footer gets a
separate message. Unopened protected questions close with the existing generic
stale text. A protection upgrade clears the old keyboard before a protected
replacement; only complete, typed ACKs move the physical target. Later events
serialize behind an unresolved earlier update and cannot overwrite a newer
terminal footer. Missing edit targets retire instead of creating endless copies.
Display changes refresh a flat presentation history; canonical event changes
retire. Explicit repair accepts only a held known refresh failure with no claim.
Pending intents and updates retain their original and moved message evidence.
This owner remains uninstalled until notification integration is complete.

`work_status.py` reads one admitted attempt through its original activation,
canonical progress/question events and terminal settlement. Retained run metadata
must agree with that attempt. A broken blocker/request join cannot become a working
status. Provider-final snapshots retain current account, role, grant and policy
checks. `work_status_events.py` qualifies nonhuman reports and human creation
receipts; JSON revision joins use text comparisons before strict Python validation.

`work_status_delivery.py` composes these reads with the existing Work notice
plans, complete ACK validation, protected Show and display refresh. Notice delivery
accepts a source projection owner so status authority stays separate from colleague
events. Refresh may adopt later progress, but cannot rewrite an already used event.
Raw status templates require this source owner. Initial status sends are silent,
including when discretionary notices are disabled, and expose no execution controls.
`work_status_updates.py` coalesces newer progress into that attempt's fully
acknowledged presentation. Every edit is silent, with the 30-second interval
checked again immediately before dispatch. A missing message, changed protection
or different part count uses a complete replacement; only its complete typed
acknowledgement moves the target. Later updates require that retained target and
its monotonically advancing acknowledged revision. They cannot fall back to an
obsolete message. Ordinary transport retries retain acknowledged parts; display
refresh and known missing-target repair retain a flat presentation history.
Current authority and immutable source evidence are checked after provider waits.
A late locale change preserves delivery for refresh, while revoked access or
changed source retires it. Retention keeps original and moved presentations while
the attempt or an update remains unresolved. These owners remain dormant pending
supervisor installation.

`work_status_pins.py` owns a durable, serialized pin ledger for each status root.
Every operation binds an exact complete acknowledged presentation and its first
message ID; the final joined account/grant/source/claim read includes that retained
presentation. Pinning is silent. A moved message unpins the old exact target before
pinning the fully acknowledged replacement; a terminal attempt only unpins.
Possible dispatch and confirmed pin are distinct. A skipped attempt can be retried
on the same target; a known refused target waits for a new presentation. Failed
unpin repair admits only a claim-free cleanup of that same possibly pinned ID.
Pin failures produce no generic message-delivery retry card. Ordinary transport
retries retain their operation, and shutdown drains an admitted SDK call.
Bot-authored pin service messages are deleted through ingress only when the ledger
proves their pinned target, with fresh checks inside the SDK limiter. Retention
uses the same closed, typed ledger and presentation receipts; a row's `sent` state
alone never proves cleanup. Pin operations and all referenced presentations stay
retained until exact cleanup is acknowledged. Installation remains deferred.

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
need Manage+Inspect. The latest wrong recipient is excluded; names, never IDs,
label choices. `Acts.route` serializes body capture and exact receipt recovery;
HumanInput's borrowed admission fences binding/account/policy at commit. Routing
grants no access and ignores the action pause. `work_routing_delivery.py` freezes initial recipient selection in a closed V5
request basis. Its dedicated source requires current manager authority and a
final joined snapshot of every candidate's grants and account state, including
the current recipient before wrong-person exclusion. No password hashes enter
that compact projection. Full ACK alone arms the final button group. Protected
notifications show only a teaser; ordinary question recovery cannot turn routing
into an answer prompt. Routing-specific reveal/refresh, selected-recipient
admission and reply-owned follow-ups remain prerequisites for installation.
Presentation/handlers stay dormant.

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
prepares the route provider off-writer, then evaluates it against the borrowed
current account/role limit; no nested account checkout is allowed. Review commands
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

`work_outcomes.py` renders complete success/refusal/uncertain wording, without
issuing commands or installing controls. Unknown actions never become a failed
save. A known attempt-closing refusal keeps the original confirmation; a retained
uncertain operation offers only its original retry. Plain replies to decisions or
reviews are comments. Wrong-recipient acknowledgements always use the plain form;
current managers get the separate routing follow-up. Start/resume navigation is
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

This planner and source remain uninstalled. Source-aware protection repair is
an activation prerequisite; assignment topics and their relocation belong to
the later topic feature. For either replacement, generic replacement holds these frozen
plans with `work-outcome-refresh-required`, preserving ACKed parts, original
input and action identity. It cannot silently move them or replay the prefix.
The full controller still owns fresh-request and routing follow-ups, explicit
action retries and installation of the receipt observer.

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

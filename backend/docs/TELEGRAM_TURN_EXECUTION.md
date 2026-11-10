# Managed turn execution

A Telegram message to the ordinary chat or a custom agent runs through the existing
channel manager and sender. `ChatTurns` (`app/telegram_bots/turns.py`) owns it from
the accepted input to its last delivery, with the queue, Stop and Try again. Files,
voice notes, questions and slash skills join this path; the module guide covers
them. AI assistants, AI employees and Work are not on it. Schema stays
`0044_telegram_assistant_ids`, configuration stays 55.

## Submission and observation

`PublicationCoordinator.dispatch(link, origins, thread_id=...)` consumes complete
saved `InboxRow` snapshots. The original thread comes from `ThreadCoordinator`
and gets an authoritative owner/target read before custody. `ChatTurns` saves each
input's `input_profile` as a business-route fact: version 1 `plain_text`, or version 2
`chat_input` when it carries files or was forwarded. An input without that fact is
held and never treated as plain text. Actual content and accepted ordering are retained.

The shared message bus reserves capacity before the durable dispatch marker.
Only the marker winner can synchronously commit its private host-capability
message after the original creating fence, with no intervening await. Full/closed
capacity before the marker is no submission. Lost marker replies, cancellation,
retirement and enqueue failures never permit an automatic second handoff or POST.
The stable publication identity remains unresolved until proved otherwise.

`observe(record, current_link, max_pages=..., deadline=None)` is one explicit bounded pass:
1–8 pages, 100 rows per page, and a 30-second logical deadline. A candidate is
bound only after complete progressing traversal establishes its unique exact
owner/thread/publication/target identity. An empty page or acknowledged 404 does
not prove that a marked creating request cannot still commit. Observations return
`pending`, `planned`, `held` or `budget_exhausted`, with closed reasons.
An optional absolute owning-loop deadline can shorten the existing 30 seconds;
it is pinned before the child is queued and cannot extend that limit.

`read_output(record, current_link)` borrows the same retained HTTP owner and strict
capture-version-2 facade with explicit inventory document 2 and read protocol 2.
It creates nothing and has no latest-history or default
client fallback. A final synchronous original-attempt check follows the public
waiter's last await before a buffered result is handed back.

## Authority and the primary reply

An attempt captures its actual adapter, token, owner, connection, target, thread
and endpoint. Frozen thread/endpoint bookmarks prevent local scope substitution;
they do not replace remote proof. Creating and retained reads have separate guards.
Managed creation intentionally requires `threads:read`, `threads:write` and
`runs:create`; private run/source reads require `threads:read` and `runs:read`.
These app-consumer requirements can be stricter than an individual HTTP route.

An optional guard preparation hook refreshes exact permissions and target
availability inside each retained owned operation, after rendering/codec work
and immediately before its SQL writer opens. Provider work stays outside that
writer. Final borrowed checks retain current human/account/link/connection
custody through the same transaction. No second writer is opened inside it.
This is a point-in-time authority check, not atomicity between SQL and external
policy/configuration. Missing hooks preserve compatible legacy guard behavior;
malformed or failed present hooks refuse.

`Outbox.plan_verified_publication_reply` accepts only complete text with a matched,
complete inventory containing no file or structured-presentation items, and a
supported frozen renderer. Version-2 primary bases retain the full original scope,
text, both proofs, inventory and immutable presentation in the existing
`reply:<turn_id>` namespace. Exact replay preserves part identities and actual
acknowledgements. Version-1 primaries cannot be upgraded or adopted as proof.

The same `OutboxSender` and adapter borrow `ModelSource`. Every unsent model part
gets a fresh strict source read and compares the entire original evidence and
frozen presentation before transport. Current account/route/target eligibility,
part/process/claim/token/protection fences and final adapter admission still apply.
Fixed sources retain their separate policy; changing a part to look like a fixed
notice cannot bypass a model basis. Source uncertainty preserves pending delivery
and acknowledged prefixes, without a replacement answer or model reexecution.

An inventory holding files goes to the file planner, which delivers them in the same
conversation. A pending, unavailable, changed, unsupported or incomplete inventory
plans no reply. A live turn then ends all the same: a failed run tells the chat, and a
finished run that cannot be planned closes after `queue.max_wait_seconds`. Recovery
keeps such a group held.
No scan is fabricated. A chat intentionally admitted without personal files can
use the generic host-qualified `no_admitted_output_view` variant of document 2.
It still requires an active human owner, the original live worker scope and
unchanged managed routing, plus the separate actual assembly/presentation proof.
The host refreshes current owner eligibility and routing before sealing; a copied
projection, reason string, missing folder, raised or missing preparation result,
or retired scope cannot grant this qualification. A normal fail-closed host
projection may qualify its actual no-view routing after fresh confirmation.
It attests the managed output view only, not
filesystem emptiness or absence of other effects. Independent file, artifact and
structured presentation offers still prevent empty-inventory delivery.
Feature-off legacy paths retain real scans, and unknown or changed routing stays
held. This host adapter holds an unsupported provider and one replaced during the
run. A supported provider that starts during the run is no change: its first tool
call can start it. Model-only runs need no sandbox acquisition.
The same retained HTTP owner negotiates format 2; old servers can reject the
original creating POST before run/storage/model effects, without a fallback POST.
AI-employee Home qualification belongs to the later instance consumer. The V1 fixed
notice API remains compatible and is not used as an empty-inventory permit.

## Bounded recovery

The Supervisor owns one recovery service over the same coordinator, HTTP owner
and sender. Periodic passes process only marked managed publications. Discovery
uses the saved original thread/publication and the existing exact unique-run
proof. A missing run ID, empty page, 404, age or absent local task never permits
another dispatch or proves that an ambiguous creating request cannot commit.
Unavailable and unsupported inventories remain held.

Startup has a distinct fixed-notice path after the negative copy decision and
actual channel readiness, before held inputs and create/send admission release.
The retained startup task reads one page and captures at most 32 complete,
supported, unmarked publications. An opaque one-shot epoch/ticket binds that
cohort, original adapter/token/endpoint and a 30-second absolute deadline.
False marker alone supplies no restart authority. The epoch retires before
admission opens; unseen/unarmed tail remains held and periodic passes cannot
later call it restarted. The conditional copy heuristic remains no ownership
election: copies need Telegram off before first boot.

`plan_restart_before_dispatch` takes the actual ticket and distinct fixed guard.
It revalidates current account/link/connection/target plus the complete original
input/group, then commits a reserved `publication_recovery` fact, one version-3
fixed primary and full route closure together. The namespace stays
`reply:<turn_id>`. Run/assistant remain null. It needs no private thread/run or
execution ceiling, reads no model source and grants no model/file/repair power.
The same sender still checks current custody, account, target and protection.
Known or malformed model sources cannot acquire fixed privilege from a part.
The optional conversation button uses the stored original thread, with no
callback or retry action. Version-1 and version-2 contracts stay unchanged.

Closure and dispatch marker serialize under the same custody/group writer.
Closure wins: a delayed marker refuses and its bus reservation releases without
enqueue/POST. Marker wins or its result is ambiguous: no restart notice or model
resubmission. A lost closure result can be confirmed only by the exact persisted
closure/primary receipt from that same startup attempt, under freshly qualified
read-only account/link/connection custody. It does not renew a ticket or authorize
a writer retry. A different valid winning pair is preserved without adoption as
that attempt's confirmation.

The reader keysets group heads with a lookahead and fetches every row of each
selected global turn, including completed, foreign or inconsistent members.
More than 64 origins, malformed/legacy/unsupported input or mixed state is held,
never parsed as a valid prefix. Periodic passes admit at most two 32-head pages,
four retained operations globally and one per link. Active cleanup keeps its
slot. Cursor advancement includes held groups and never consumes the lookahead.

These are group/row/operation and decoded-payload-count bounds. The current
schema has no turn/head index, and stored scalars have no general byte cap;
this version does not guarantee bounded SQL scan time, decoded bytes or RAM.
The retained startup page and started native work can physically exceed the
logical deadline. No new leaf/ticket/final writer authorization is admitted
after it. A commit already issued after its valid final fence may settle later;
that exact operation remains owned through cleanup. Schema remains `0044`.

AI-employee Home/Work authority is never inferred from a human requester's access.

## Shutdown

Phase one closes creating, sending and new recovery passes while retaining authorized final reads and
planning through the actual channel-manager drain. Final stop retires read
admission, cancels prompt network waits, and drains admitted SQL/codec/response
and plan cleanup before closing the same HTTP owner and listener. Failed cleanup
retains the owner/listener for retry. The existing Gateway grace remains the only
outer grace; already-admitted native cleanup may outlast a logical deadline.
Final stop drains the recovery service before the coordinator and shared HTTP
owner. Failed recovery cleanup retains those dependencies and the listener for
retry. There is no additional after-manager observation pass or shutdown grace;
late durable results wait for the next startup. A worker lets go of a run at its first
frame, so a manager with an empty queue stops at once, and a run still going then is
recovery's to end.

## Instance file writes

`instance_file_exports.prepare_file_export` separately authorizes outbound files
as the bound human. Current Use and Inspect precede original Home READ/EXPORT;
exports never require execution readiness or consult a resident. A final joined
snapshot under the Space writer checks the binding, instance and Home generations,
current grants, policy and live account before and after native reads. The same
authorization can be repeated without reading bytes at the SDK dispatch boundary.
Native work drains through repeated cancellation. This bridge supplies read
authority only: the sender must still own its claimed original output source,
reserve its byte budget before export and retain it through transport cleanup.

`instance_file_authority.prepare_file_write` owns current Use before execution
readiness and Home diagnostics. It captures the installed same-engine instance
and file services, original instance/Home generations and Telegram binding.
Its direct `AgentFiles.apply` call neither creates a conversation nor consults
a resident. The agent principal writes the bytes; the bound human is recorded
as requester, without requiring that human's Inspect or Home Write grant.

The Space writer's controller admission locks the instance before Telegram
custody, then reads the current binding, Use, employee policy and human account
in one final joined statement. It checks the Home generation already admitted
by that writer and the original caller lifetime after the final await. The whole
write drains through cancellation so seekable content remains owned until the
file effect and its receipt settle. Native limits still require the storage CI
tier. The input journal, bounded transport and saved retry controls own operation
IDs and bytes outside this bridge; they must be integrated before bot activation.


`file_puts.FilePuts` journals an exact admitted SDK descriptor before downloading.
Its stable Act identity derives from the link, provider file identity and original
message; only file identifiers and metadata persist. Its closed target records
the original instance principal, generation, definition and Home ID/generation.
A replacement target refuses receipt reuse and retry instead of rebinding a path. Current custody guards each
journal writer, while the direct file boundary independently authorizes the actual
Home effect. The installed instance of this service must be shared by handlers:
its per-Act lock owns download, native write, bounded retry and lease cleanup.

Only `file-write-failed` repeats until the configured deadline; workflow file
maintenance repeats once; an invalid name gets one fallback under `rename`.
An actual adopted `file_retry` button targeting the Act allocates `retry:2`, then
`retry:3`, and so on, and consumes its group in the same writer. Redelivery reads
the saved result. An interrupted call becomes uncertain and waits for a new press;
startup recovery settles only metadata. Attempt/operation comparisons reject stale
outcomes, and a saved completion wins over a late error. The byte lease survives
native work and repeated cancellation. Callback IDs in the closed continuation
identify completed presses, not delivery dependencies: no extra outbox retention
pin is needed. The 400-day Act sweep deletes validated done/refused file journals,
including those whose link was removed; uncertain and malformed records stay.
The supervisor installs one shared journal for its handlers and pending readers.
The adapter routes incoming saves and renders their captionless/retry outcomes;
public activation also requires the remaining output and presentation paths.


`FileReceiptReader` prepares current file authority before entering a metadata
writer. Completed-file reads recheck the original descriptor, closed receipt and
Home target plus current Use, policy and account in that borrowed writer.
The reader composes these checks into the writer's final guard, then checks the
original caller lifetime synchronously after the last authority await. They
read no file bytes and repeat no file operation. `composer_files` projects those
receipts as `filename`, `size`, `/mnt/user-data/` path and `status: uploaded`.

`PendingFiles` can retain an instance receipt's actual `agent_file_path`; it never
accepts a caller-supplied path. Taking pending files validates each receipt before
attaching the original descriptors and deleting pending rows in the same writer.
A replay reuses that frozen attachment set. Reader preparation occurs outside a
writer, including a second guarded pass when candidates were found; an empty take
needs no file readiness. Borrowed writers must supply a prepared reader and use its `guard(base)` for
their outer transaction boundary too.

Instance publication requires an installed `FileReceiptReader` for every retained
file. After any ordered preflight omissions, it projects completed receipts into
`human_message_kwargs.files` and clears raw transport descriptors, so the channel
manager merges uploaded files into the HumanMessage without the legacy uploader.
The receipt guard wraps the original creating guard through the final dispatch
writer: changed Home, Use, policy or original lifetime rolls back the marker and
releases the bus reservation. The original admission also retains that reader
in process through queueing and the facade's actual creating HTTP fence. Every
later `run_create` check reauthorizes the captured target without reading bytes,
so an enqueued path cannot bind to a replacement Home. A refusal after enqueue
leaves its dispatch marker intact; it grants neither reupload nor redispatch.
Empty files need no receipt reader. The supervisor
shares its prepared-reader factory with publication and pending context.


`adapter.put_instance_file` owns the direct journal/download/write as one
Application task, also retained by the adapter's file-task drain. Repeated
cancellation cannot release a byte reservation before the admitted native write
and its journal writer settle. The existing `_download` leaf reserves bytes
before SDK getFile and rechecks authority inside limiter waits and capped reads.
`FileProgress` shows only fixed saving/waiting lines with the current instance
name. Protected policy uses a protected temporary message; other policy uses a
fresh draft ID. Cleanup is best effort and cannot replace a committed file
receipt. Captioned preflight and captionless/retry routing must qualify these
leaves before activation.


`file_outcomes.FileOutcomes` plans fixed save/refusal replies from an actual
journal outcome and its original admitted input. Native rename/deduplication
results supply their wording; retry controls target that exact file Act and keep
no bytes. A closed `file_put_basis` records the Act, operation and outcome in the
product reply. Replay returns the same frozen text and controls under current
authority. The operation's single hash is the dedupe suffix, avoiding accidental
token-shaped text in internal identifiers. Reply and callback creation borrow one
receipt-guarded writer; late Use/Home changes roll both back. Manager full-files
wording and Files links, including the size-limit link, recheck Manage/Inspect at
the SDK boundary. A size refusal links only to an already existing conversation
for other readers. Initial save notices are auxiliary; captionless questions,
explicit retry execution and lifecycle/status refusal routing remain the callers'
responsibility and must be qualified before activation.

Unconfirmed file writes and pending workflow writes use cannot-confirm wording: native bytes may already exist even when the completion receipt is unavailable. Their retry controls remain explicit and journal-fenced.

A file writer supplied the actual joined `origins` records a closed version-3
input capsule: captionlessness of the whole group, its topic and source message
IDs. It validates every original inbox snapshot and joining scope under the
journal writer. A caption on another album member therefore prevents an extra
pending attachment. Legacy version-1/2 journals remain readable and are never
upgraded by replay.

For a version-2/3 captionless input, completion and its pending attachment commit
in the same writer. The terminal journal prevents re-creating that context after
it is taken or swept; an explicit successful retry can create it even after the
original inbound mark has been collected. Pending consumption then validates the
retained source capsule and original Home receipt. A retry in another topic is
refused before its keyboard is consumed. Every file journal writer now retains
the original Home/Use/policy guard through its final commit check. Incoming
routing must supply the joined origins; this metadata support alone does not
activate instance file input.

The adapter's direct file method now supplies its actual input group to that
journal. An instance `file_retry` Act control takes the owned SDK/file task path,
never the ordinary message retry. Its descriptor and topic come from the current
original-target receipt; the saved callback group still authorizes the child
operation. A failed live owner can close only its already-recorded press as
uncertain, without a download or write. This is not an automatic effect retry.

Retry outcome delivery belongs to that recorded press, independently of the
original inbox route. It can therefore confirm a save or provide the next exact
retry control after the old route closes or its mark is collected. Frozen outcome
plans deduplicate simultaneous presses; replaying an older press cannot publish
a newer child's result. Successful captioned retries only confirm the file;
captionless completion retains the one-time pending context.

Incoming instance media now saves each actual source file before queue/thread
preparation. Files carried from an earlier captionless receipt are recognized by
their original message IDs and are never downloaded again. Captionless input
plans one receipt-qualified question for its saved members and starts no turn;
the question and route closure share the final receipt guard. Album members keep
their shared caption basis. The queued publication preflight reads only settled
original-target journals, omits refused/unconfirmed attachments, and includes the
instance ID in its execution-context comparison. Personal-storage preflight and
legacy upload are never used for this path. A failed initial file owner can close
only its own unconfirmed metadata and offer the exact file retry, without another
effect. Before admission, a paused or retired target answers file-specific
wording under the existing lifecycle notice fence; withdrawn Use still takes
precedence. A lifecycle change after inbox admission closes the entire original
input without publishing its caption. Execution-readiness refusals plan the
fixed unavailable reply and close the original group under current Use, target,
policy and custody checks in the final writer. These refusals invent no file
journal and need no Home or workspace.

`file_recovery.py` runs inside the existing bounded startup/link-start phases,
before polling is released. Its internal ticket binds the original phase, loop,
host, turn controller, adapter and complete link identity. It refuses live file
owners and active turns; every final writer keeps the ticket and original
Home/Use/policy/account checks. The ticket retires when the operation returns,
including cancellation. Recovery never downloads, resumes a native write or
publishes model input. It settles unfinished file journals as uncertain, plans
their exact file Retry, and restores a saved captionless question. A caption
keeps the ordinary explicit chat retry. Unknown groups and unavailable authority
remain held; a stale input does not block the next independent input.

Version 3 adds a closed delivery receipt for the latest operation. The reply and
receipt commit together; an ordinary successful initial save records that no
auxiliary reply is needed. Explicit presses clear the previous delivery receipt
when allocating their new operation. A simultaneous same-result planner may
observe only the receipt change, never a newer operation's result. Bounded Act
pages recover a recorded press's pending outcome even after input/control
collection, using that original journal's saved press. A retained delivery
receipt prevents recreation after outbox collection. Legacy terminal journals
without this receipt are not automatically republished or upgraded. No extra
outbox retention pin or schema change is required.

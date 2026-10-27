# Callable managed turn execution

The managed Telegram runtime has an internal callable path for ordinary and
custom-agent plain-text turns. It uses the existing channel manager and sender.
It does not install ordinary input handlers,
queue/Stop controls, file/voice consumers, instance/Work consumers or activation.
Schema stays `0044_telegram_assistant_ids`, configuration stays 55.

## Submission and observation

`PublicationCoordinator.dispatch(link, origins, thread_id=...)` consumes complete
saved `InboxRow` snapshots. The original thread comes from `ThreadCoordinator`
and gets an authoritative owner/target read before custody. Each input has exact
`type='text'` plus nonblank text, and a trusted persisted business-route fact:
`input_profile={"version":1,"kind":"plain_text","forwarded":false}`. The future
ingress classifier must establish that fact; missing or forwarded input is not
silently treated as plain text. Actual content and accepted ordering are retained.

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
capture-version-2 facade. It creates nothing and has no latest-history or default
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

Pending, unavailable, changed, unsupported, incomplete or nonempty inventories
remain held, including failed/cancelled runs whose inventory is unavailable.
No scan is fabricated. Chats without a qualified personal-file view require a
separate generic qualification before ordinary activation; AI-employee Home
qualification belongs to the later instance consumer. The existing V1 fixed
notice API remains compatible and is not used as an empty-inventory permit.

## Bounded recovery

The Supervisor owns one recovery service over the same coordinator, HTTP owner
and sender. Periodic passes process only marked managed publications. Discovery
uses the saved original thread/publication and the existing exact unique-run
proof. A missing run ID, empty page, 404, age or absent local task never permits
another dispatch or proves that an ambiguous creating request cannot commit.
Unavailable, unsupported and nonempty inventories remain held.

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

This is dormant recovery infrastructure, not completed ordinary-chat activation.
The trusted ingress classifier, generic NoUser/no-admitted-output qualification,
queue/Stop/explicit retry controls, drafts and other stage-one consumers remain
separate work. AI-employee Home/Work authority is never inferred from a human
requester's access.

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
late durable results wait for the next startup.

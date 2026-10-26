# Callable managed turn execution

The managed Telegram runtime has an internal callable path for ordinary and
custom-agent plain-text turns. It uses the existing channel manager and sender.
It does not install ordinary input handlers, periodic or restart observers,
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

`observe(record, current_link, max_pages=...)` is one explicit bounded pass:
1–8 pages, 100 rows per page, and a 30-second logical deadline. A candidate is
bound only after complete progressing traversal establishes its unique exact
owner/thread/publication/target identity. An empty page or acknowledged 404 does
not prove that a marked creating request cannot still commit. Observations return
`pending`, `planned`, `held` or `budget_exhausted`, with closed reasons.

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

## Shutdown

Phase one closes creating and sending while retaining authorized final reads and
planning through the actual channel-manager drain. Final stop retires read
admission, cancels prompt network waits, and drains admitted SQL/codec/response
and plan cleanup before closing the same HTTP owner and listener. Failed cleanup
retains the owner/listener for retry. The existing Gateway grace remains the only
outer grace; already-admitted native cleanup may outlast a logical deadline.

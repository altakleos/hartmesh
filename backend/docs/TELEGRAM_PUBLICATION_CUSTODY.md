# Managed Telegram publication custody

The publication APIs preserve the identity of an accepted input group and its
primary reply. They are the SQL primitives under the managed-bot dispatcher
(`app/telegram_bots/publication.py`), which every Telegram turn goes through.

`reserve_publication` creates the turn, publication and versioned execution
receipt in one transaction. It accepts saved model-turn routes and freezes the
human owner, bot, connection, target, original conversation, endpoint, complete
origins and canonical input digest. Exact replay returns the same identity.
Existing publications without an execution receipt are refused: a missing run
ID cannot establish that an earlier dispatch never started. An explicit retry
reserves a successor publication (`reserve_retry_publication`).

The version-one input digest includes each saved origin, accepted time, sanitized
input, route, ordered operation IDs and business route basis. Assembly follows
accepted time, update ID and message ID. Canonical JSON preserves Unicode,
whitespace, array order and JSON types; only server publication and thread
preparation bookkeeping is excluded. Per-origin preparation receipts remain
saved independently.

`mark_dispatch_started` has one successful winner. Its irreversible marker means
dispatch may have begun; it does not prove a bus commit or an HTTP request.
Cancellation, an ambiguous commit, token rotation or restart cannot clear it.
`bind_publication_run` binds the first exact run only after the saved marker, and
its callers supply real execution provenance.

Every managed write requires an original captured-attempt guard and current
durable human/account/connection custody. Owned transactions retain SQL and
cleanup through cancellation. Borrowed helpers use their caller's active
same-engine writer. Lock order is link and custody, primary outbox, then complete
sorted inbox rows; no writer crosses a network, bus or model call. The final
attempt check follows the last awaited human-authority read. SQL commit and an
external process fence are separate systems.

`plan_verified_publication_reply` joins the saved publication to a complete
output projection and its exact source receipt. It freezes the source digest and
a versioned HTML presentation, including contiguous codepoint ranges in the
rendered visible text. It retains the original source identity rather than
substituting a digest of rendered text. Closed fixed failure/cancellation
dispositions use the same primary identity, `reply:<turn_id>`, without inventing
an assistant-message ID.

Exact primary replay preserves saved part IDs and delivery acknowledgements.
Changed proof, source, presentation or completion intent refuses reuse. Primary
planning and complete-group route sealing commit together. Auxiliary notices
cannot consume the primary identity; outstanding required deliveries still keep
the input unresolved after its route is sealed.

The nullable, nonindexed `assistant_message_id` column uses Text so a legal
opaque SDK ID can survive worker sealing and Telegram planning unchanged.
Downgrade to the earlier 64-character column refuses longer retained IDs before
changing the schema. Other identity widths are unchanged.

The publication DTO and SQL guard do not themselves establish remote run or
source authority. The dispatcher obtains strict owner-scoped proofs, accounts for
artifacts and actions, and uses the existing sender. None of this makes an
exactly-once Telegram delivery guarantee.

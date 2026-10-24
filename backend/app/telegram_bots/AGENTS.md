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

`links.py` owns capacity and connection transactions. Capacity changes lock the
deployment singleton, then owner settings, then links in ID order. SQLite takes
`BEGIN IMMEDIATE` before any read. Retain the writer's initial deployment/owner
counts through nested remove/add helpers so net-zero replacements keep their
place after limits fall. First settings, link, event and code commit together.
Borrowed helpers accept only their owning writer session and never commit or
open another checkout. Connection-only writers never acquire capacity afterward.
The existing channel repository's committing/ownership-transfer helpers are
not suitable inside these units; use the local owner-filtered row helpers.

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

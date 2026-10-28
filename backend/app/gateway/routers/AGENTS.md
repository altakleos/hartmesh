# Gateway routers

Bootstrap counts are global except `mine`. Own-file status closes SQL before
kernel proof, uses assignable slots rather than seat vacancy, and projects failures
as unavailable. Public health exposes state words only; unsupported identities
never project a synthetic owner.

Artifact `preview=namespace/id` uses `_artifact_projection.py`: match installed,
enabled literal suffixes/compatibility queries after the read/owner fence.
`report_preview=true` uses that fence for output reports. Public descriptors expose
budgets/markers only. Capture one bounded no-follow snapshot; project/serialize
in a drained worker, with original-byte SHA-256 ETags. Source/response limits are
16/1 MiB. No cache, report mutation or ordinary/download projection. Unsupported
capabilities return 501 for bounded source fallback.
Managed chat files use `chat_files.py`/`ChatUploadSession`: fresh human User
admission, pinned descriptors and private quota-counted stages. Shared reads refresh
Company admission. Retain ranges, ZIP-member/projector budgets and private no-store
responses. Edits recheck admission/revision/parent; uploads qualify each phase/chunk's
limits/quota and commit after final admission. Drain all stages on failure/cancel,
including project attachments. `ChatFilesFull` uses the central refusal handler.
Ordinary upload/list/delete/edit refuses instance conversations. `agent_home.py`
owns conversation upload and `/home/content`, `/home/files`: direct writes recheck
human rights under mutation SQL and drain spool workers. Retain access-denied
responses. Declare `GET /roles` before `GET /{instance_id}`.

`chat_cleanup.py` admits purpose-bound deletion, including unsupported credentials,
without general User scope. Durable deletion and the owner/thread gate fence
creation through strict container absence, fresh admission, per-entry NO_XDEV purge
and logical cleanup. Private purge tickets allow retries.
`chat_branch.py` reserves targets through latest-turn quota-private copying,
publication and metadata/checkpoint/history writes. Clone workspace/uploads/outputs
only, with empty ACP; skip symlinks/upload stages. Confined reads/revisions detect
changes. Drain exact-owned files/stores rollback on failure/cancel. Historical/NoUser
copies history only; legacy/AgentHome stays compatible. Unsupported credentials
never gain general Space access.
Native adapters prepare `NativeFileSources` before intent: READ/EXPORT sources,
union destination rights, borrowed SQL guards, fresh pinned-root admission.
Stages/`NativeDirectoryChanges` remove owned files/empty dirs only. Shared receipts
pin inodes; project placement borrows its writer, with file-before-row outcomes.
Retain ZIP budgets/private-skill delegation. Browser REST/tools use the Gateway;
AI employees cannot use human User storage. Account exports select owned chats,
My Files/live shelf rows plus legacy skills; exclude Company/Homes/control/orphans.
Refresh exact job/authority before ready/headers; drain builders/download work.
NoUser has no legacy fallback.

Malformed/oversized/unsupported projections retain bounded source preview;
authority/path denials do not. Unsupported descriptor reads return 501.

Workspace prewarm checks `sandbox:execute` before provider lookup/scheduling,
using the shared request authorization helper off-loop. Denial returns 202
with `scheduled: false, reason: forbidden`; thread ownership still applies.

Thread `owner_user_id` comes only from stored `user_id` across create/replay/race,
GET/search/PATCH/move; checkpoint-only ownership stays unknown.
GET's opt-in bool `X-DeerFlow-Require-Thread-Binding` requires `agent_conversations`:
missing authority is fixed 503; SQL errors propagate. False/omitted preserves memory
compatibility. Create's default null binding proves nothing about actual binding.

`POST /api/threads/{id}/history` returns the newest checkpoint’s title/messages
and rendered `artifacts`, `todos` and non-null `goal`. Keep the API channel
serializer and explicit projection; expose no internal sandbox state. Empty lists
are meaningful; omit absent/null goals to preserve frontend local overrides.

Artifacts, Files and Shared stream MIME sniffing/metadata/ranges from one safely
opened descriptor. Preflight paths determine HTTP errors, never permission to
reopen through symlinked parents. Use `deerflow.files.store.open_regular_source`,
and retain ownership until every offloaded operation has drained on cancellation.
Conversation keep/publish sources remain lexical paths within uploads/outputs.
Artifact reads preserve links until no-follow open. Default GET captures small
editable files for SHA-256/ranges. HEAD and `stream=true` skip payload preparation;
raw archive/projection/instance sources refuse. Owner/Space checks still apply.
Skill archive detection/extraction shares one source descriptor and drains off-loop.
`SafeFileAccessUnavailable` means HTTP 501 for descriptor reads and archives;
never turn an unsupported host into a missing/invalid/changed file.
`files.store.copy_into` uses private hidden staging and descriptor-relative
exclusive hard links: no placeholders or overwrites of racing names. Verify
staging ownership/mode before writing; the worker owns cleanup. Unsupported
atomic publication fails closed.
Shared publication drains copying, its database record and failure rollback as
one operation under the deduplication lock before propagating cancellation.
Every Shared access enters `_shared_mutation`; its process mutex and private
filesystem advisory lock retain ownership through recovery/settlement.
Delete authorizes the current publication under that lock; optional
`expected_publication_id` adds stale-client intent fencing (empty means no record).
Lists/publications carry nullable `publication_id`; Files/Undo pass it back.
Removal journals stage bytes on the same filesystem before updating the immutable
publication ID. Recovery matches record ID and path before using saved outcomes;
ambiguity refuses access and preserves bytes. Drain removal/recovery workers before
either lock releases. Multi-Gateway filesystem locks need deployment qualification.

Work activation uses `start_run` with host-only attempt capability and stable run
idempotency. Require agents read/write, runs create, Use+Inspect and an existing
bound conversation; request bodies convey no authority.
Manager edits/cancellation of unresolved Work additionally require runs cancel and
retry the persisted exact stop intent. Human-input replies never invoke activation.

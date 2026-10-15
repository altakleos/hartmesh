# Gateway routers

Bootstrap counts are global except caller `mine`. Own-file status closes SQL
before kernel proof and uses assignable slots, not seat vacancy. Failures project
unavailable; public health exposes state words only. Unsupported identities never
project a synthetic owner.

Artifact `preview=namespace/id` uses `_artifact_projection.py`: match installed,
enabled literal suffixes/compatibility queries after the read/owner fence.
`report_preview=true` uses that fence for output reports. Public descriptors expose
budgets/markers only. Capture one bounded no-follow snapshot; project/serialize
in a drained worker, with original-byte SHA-256 ETags. Source/response limits are
16/1 MiB. No cache, report mutation or ordinary/download projection. Unsupported
capabilities return 501 for bounded source fallback.
Managed chat file effects use `chat_files.py`/`ChatUploadSession`: fresh human
User admission, pinned descriptors and private quota-counted stages; Shared reads
also refresh Company admission. Keep ranges, ZIP-member/projector budgets and
private no-store responses. Edits recheck admission, revision and parent before
replacement. Uploads qualify by phase, check every chunk's limits/quota, and commit
only after final admission. Failure/cancellation drains all stages, including
project attachments. `ChatFilesFull` uses the central refusal handler. Ordinary
upload/list/delete/edit refuses instance conversations. `agent_home.py` owns
direct Home writes: recheck human rights under mutation SQL; drain spool workers
before close. Permission errors retain their access-denied response.

`chat_cleanup.py` admits only purpose-bound deletion, including unsupported resource
credentials, without a general User scope. Durable deletion and the stable
owner/thread gate fence creation through strict container absence, fresh admission,
per-entry NO_XDEV purge and logical cleanup. Private purge tickets allow retries.
`chat_branch.py` reserves its target through latest-turn quota-private copying,
publication and metadata/checkpoint/history writes. Clone only workspace/uploads/
outputs, with empty ACP; skip symlinks/upload stages. Confined per-entry reads and
revisions detect source changes. Failure/cancellation drains rollback of exact-owned
files and target stores. Historical/NoUser copies history only; legacy/AgentHome
branching keeps its existing behavior. Unsupported credentials never gain general
Space access.
Native adapters prepare `NativeFileSources` before intent: READ/EXPORT sources,
union destination rights, borrowed SQL guards and fresh pinned-root admission.
Stages/`NativeDirectoryChanges` remove only owned files/empty dirs. Shared receipts
pin inodes; project placement borrows its writer with file-before-row outcomes.
Keep ZIP budgets/private-skill delegation. Browser REST/tools use the Gateway;
AI employees cannot use human User storage. Account exports select owned chats,
My Files/live shelf rows plus legacy skills; exclude Company/Homes/control/orphans.
Refresh exact job/authority before ready/headers; drain builders/download work.
NoUser has no legacy fallback.

Malformed/oversized/unsupported projections retain bounded UI source preview;
authority/path denials do not. Unsupported descriptor reads return 501 for fallback.

Workspace prewarm checks `sandbox:execute` before provider lookup/scheduling,
using the shared request authorization helper off-loop. Denial returns 202
with `scheduled: false, reason: forbidden`; thread ownership still applies.

`POST /api/threads/{id}/history` hydrates reopened chats with the rendered
`artifacts`, `todos`, and non-null `goal` channels in the newest returned
checkpoint alongside title and messages.
Keep that projection explicit and serialized through the API channel serializer;
do not expose internal sandbox state. Empty lists are meaningful, while absent
or null goals are omitted to preserve the frontend's local override semantics.

Artifacts, Files and Shared must stream from one safely opened descriptor, including MIME
sniffing, response metadata and range reads. Preflight paths only determine HTTP
errors; they must never become permission to reopen a path through symlinked
parents. Use `deerflow.files.store.open_regular_source` for source reads/copies,
and retain ownership until every offloaded operation has drained on cancellation.
Conversation keep/publish sources remain lexical paths within uploads/outputs.
Artifact reads use `resolve_thread_read_path`; never resolve away link segments
before no-follow opens. Small editable artifacts capture bounded immutable bytes
for SHA-256/ranges; large artifacts stay descriptor-streamed without content hashing.
Skill archive detection/extraction shares one source descriptor and drains off-loop.
`SafeFileAccessUnavailable` means HTTP 501 for descriptor reads and archives;
never turn an unsupported host into a missing/invalid/changed file.
`files.store.copy_into` uses private hidden staging and descriptor-relative
exclusive hard links: no placeholders or overwrites of racing names. Verify
staging ownership/mode before writing; the worker owns cleanup. Unsupported
atomic publication fails closed.
Shared publication drains copying, its database record and failure rollback as
one operation under the deduplication lock before propagating cancellation.
Every Shared access enters `_shared_mutation`: the process mutex and private
filesystem advisory lock retain ownership through recovery and settlement.
Delete authorizes the current publication under that lock; optional
`expected_publication_id` adds stale-client intent fencing (empty means no record).
Lists and publication responses carry nullable `publication_id`; Files and Undo
pass it back. Removal journals stage bytes on the same filesystem before updating
the immutable publication ID. Recovery must match both record ID and path before
using the persisted removal outcome; ambiguity refuses access and preserves bytes.
Drain every removal/recovery worker before releasing either lock. Filesystem lock
semantics on multi-Gateway shared storage still require deployment qualification.

Work activation uses the existing `start_run` adapter with a host-only attempt
capability and stable run idempotency key. Require agents read/write and runs create,
Use+Inspect, and an existing bound conversation; no request body conveys authority.
Manager edits/cancellation of unresolved Work additionally require runs cancel and
retry the persisted exact stop intent. Human-input replies never invoke activation.

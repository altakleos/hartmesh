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

Trash listing resolves the live retention configuration off-loop before its
best-effort sweep. Configuration failures skip automatic cleanup and preserve
the listing; never substitute a default policy for an unreadable one. Resolve
the policy before consuming the reconciliation throttle window.

HTTP creation holds `goal_thread_lock` from the idempotency lookup through the
initial checkpoint write. Same-id HTTP creates must return the existing row,
including in memory mode; retain the native memory store's overwrite contract
and SQL duplicate-insert recovery across workers. Release on failure/cancellation
without blocking different thread IDs. Tests: `test_thread_create_idempotency.py`.

`GET /api/plugins/diagnostics` uses `require_admin_user` before reading the
canonical runtime diagnostic snapshot. Return bounded entry-point identifiers,
known levels and fixed summaries; raw messages stay in logs. Never clear the
sink or expand PAT permissions.

Branch metadata inherits valid `assistant_id` values verbatim. Validate with
`custom_agent_name` and clear invalid legacy values to null in the new row;
leave the source record unchanged.

Console usage attributes legacy rows without a per-model usage map to
`model_name`, including their recorded `total_input_tokens`. Accumulate those
inputs with newer usage buckets for the same model; do not infer cache hits.
A counter that does not coerce to an integer contributes zero tokens instead
of aborting the request, and non-finite pricing leaves that model's cost
unreported; rows whose counters are all unusable still fall back to run-level
totals. Regression coverage lives in `tests/test_console_router.py`.

Shared scheduled-task update validation preserves an unchanged future `once`
instant on a live task inside `scheduler.min_once_delay_seconds`; new/changed
instants and terminal reactivation still enforce the minimum delay.

Scheduled-task admission rejects nonexistent naive `run_at` and `end_at`
wall times through the shared Gateway datetime resolver. Keep this check out
of runtime `next_run_at`: legacy tasks retain their accepted execution time,
including unchanged live once definitions. Explicit offsets retain their instant;
ambiguous naive times keep the existing fold choice.

The MCP task list accepts optional `TaskStatus` and `active_only` query filters.
Pass both to the repository before its limit; preserve `threads:read`, owner
checks, current-thread incarnation scoping, and the unfiltered array response.

Custom Agent packages are strict, versioned definition-only JSON documents.
Export explicitly projects the caller's portable `AgentConfig` fields and SOUL;
exclude `github` installation bindings, memory contents, conversations and
credentials. Import may rename locally but must remain owner-scoped and
create-only: collisions return 409, never overwrite or upsert. Ordinary creation
and import share `_persist_new_agent`, which uses `run_drained_write` so a
cancelled request still drains the owned persistence worker.

`GET /api/skills`, `/api/skills/custom` and `/api/skills/{name}` serve every
signed-in user, so their unexpected-failure 500s return a fixed detail. Load
errors (e.g. a malformed `extensions_config.json`) carry host paths and stay in
the server log.

Thread upload, upload delete and shelf attach-to-thread 500s name the file but
not the cause: staging and unlink `OSError`s carry host paths and stay in the
server log.

Skill mutation operation lists return `next_cursor: null` when `has_more` is false, including empty pages after a supplied `after_id`. The next cursor is the final returned operation only when a further page exists.

`GET /api/agent-tool-groups` projects only configured group names for the
custom-Agent settings allowlist. Never expose tool provider paths, credentials,
or raw tool configuration through this catalog.

Custom Agent update guards apply legacy shared-file checks only when the
resolved store is `FileAgentStore`, including the `update_agent` tool. DB
imports retain source files for rollback; those files cannot veto SQL updates.
Keep store resolution and filesystem checks off-loop in the HTTP route.
Agent PUT uses `preserve_non_managed_fields(..., new_tool_groups=...)` to clear
an inherited knowledge default when changed groups remove knowledge access.
Explicit scope/null updates still win; unchanged groups are not a migration.

Agent/profile and deployment/personal MCP mutation 500s plus the
scheduled-task `invalid_assistant` 422 use fixed text. Log exceptions
server-side; they can carry host paths.

Committed MCP reconciliation failures are logged once by
`fail_mcp_reconciliation`, with only the underlying exception type.
`_run_drained_mcp_apply` passes `_McpCommittedReconciliationError` as an expected
error to `run_drained_write` to avoid a duplicate diagnostic. It still drains
retired-pool cleanup outside the config locks and reports the fixed-detail 500.

Custom-skill edit, delete and rollback hold the owning root's process mutex and
`.custom.mutation.lock` sidecar across predecessor read, storage mutation and
history append. Peer Gateway processes sharing that root must use this same
critical section to preserve history ordering. Keep the sidecar distinct from
the nested projection lock, retain it across requests, and acquire/release both
off-loop inside the drained worker. Unrelated users retain independent locks;
external filesystem writers do not participate in this history guarantee.

Plugin actions stamp `ActionContext.interaction=session` only from authenticated
session provenance. Never accept it from payloads or propagate it to model tools;
extensions use it to distinguish owner review from model/tool output.

Skill archive upload copies (`_copy_uploaded_skill_archive`) report the original
size-limit or read error when temporary cleanup or its warning log fails.
Successful copies still return the uploaded bytes unchanged. The upload route's
`finally` discards that archive through the same helper before closing the
multipart form. Form close is suppressed, so a close, cleanup, or warning
failure cannot turn a successful install into a 500, replace an install
error, or skip the discard. Coverage: `tests/test_skill_archive_upload_cleanup.py`.

State updates synchronize titles into thread metadata only for the root checkpoint
namespace. Nonroot writes preserve the conversation title, branch-title sequence
and activity timestamp. `tests/test_threads_router.py` checks metadata/search
isolation and root title synchronization in full and delta modes.

Unauthenticated auth routes (`/register`, `/setup-status`, `/providers`,
`/oauth/{provider}`, `/callback/{provider}`, the `/login/local` throttle) read
`get_app_config()` through `asyncio.to_thread`: each read stats and hashes the
config files. Coverage: `tests/blocking_io/test_auth_router_config_reads.py`.

`product.py` exposes public product/provider snapshots through the exact auth
allowlisted `/api/v1/auth/product-identity`; legacy `/product` retains `{name}`.
Only explicitly public sanitized payload fields/assets are projected. Protected
tenant branding routes keep their existing authorization.

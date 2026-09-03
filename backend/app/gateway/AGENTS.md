### Gateway API (`app/gateway/`)

FastAPI listens on port 8001; health: `GET /health`. `GATEWAY_ENABLE_DOCS=false`
disables `/docs`, `/redoc`, and `/openapi.json`.

`/api/runtime/v1/*` and in-process adapters share one `InvocationRuntime` and
its accepted identity/material admission. `GracefulShutdownCoordinator` orders
shutdown; if quiescence is unproven, leave resources for process reclamation.

Durable MCP task notifications run as internal Agents. Keep trusted delivery
instructions outside user input, frame remote payloads as untrusted, and require
existing owned threads so late events dead-letter instead of recreating deleted chats.

CORS defaults to same-origin through nginx. Split-origin or port-forwarded
clients must set exact `GATEWAY_CORS_ORIGINS` for CORS/CSRF and expose
`Content-Location` via `CORS_EXPOSED_HEADERS`; the LangGraph SDK reads new run
IDs from that header to resolve placeholders and enable edit/regenerate/branch.

Browser auth sessions are owned by `app.gateway.auth.session_cookie`. Login accepts a `remember_me` form flag, but the Gateway never stores passwords. `SessionCookiePolicy` persists the `HttpOnly access_token` cookie only for HTTPS/trusted-forwarded HTTPS, direct-host localhost HTTP, or explicit operator opt-in for insecure persistence; public HTTP sandbox URLs degrade to session cookies. Session-creating handlers stamp the final `max_age` on `request.state`; CSRF cookie creation mirrors it so the double-submit pair expires together, including re-issue after password changes and OIDC callbacks. A small `HttpOnly` preference cookie preserves the remember choice across re-issues. Logout clears all auth cookies and suppresses CSRF re-issue on the logout response.

PATs (`Bearer dfp_...`) act as owners, never services; invalid Bearers get 401, never cookie fallback. `PAT_ROUTE_SCOPE_RULES` default-denies PAT routes by owner permission. UUID4 public refs are tenant-bound and token-free; `TrustedRunContextV1` v4 binds them. `require_audited_permission` rechecks authority, fails closed before mutation, and returns the actor. Other audit is best-effort; revocation blocks reuse; management is session-only. See [the contract](../../../docs/AUDITABLE_AUTOMATION_IDENTITIES.md).

Localhost persistence deliberately reads the direct request `Host` and ignores `Forwarded` / `X-Forwarded-Host`. Scheme and auth-origin reconstruction still consume forwarding headers. The bundled nginx sets `X-Forwarded-Proto`, but preserves an upstream HTTPS value and does not overwrite every forwarded header, so the outer trusted proxy must replace or strip client-supplied forwarding headers before traffic reaches DeerFlow.

Standalone LangGraph Studio is recognized only by the upstream
`Auth.types.StudioUser` principal type; older SDKs fall back to normal owner
scoping. Its assistant reads include genuine registered assistants plus its own,
while all other resources remain owner-scoped. Create/update makes `user_id` and
`created_by=user` server-owned. Before runtime 0.30.0 loads,
`langgraph_studio.py` uses the CLI graph registry to recreate genuine system
assistants and demote all other legacy `created_by=system` active/version rows.
This file loader does not pre-register the module in `sys.modules`, so keep
annotations eager and preserve the loader regression test. Missing persistence
is a no-op; parse/write errors fail startup, and missing expected registered rows
emit a drift warning.

**Routers**:

| Router | Endpoints |
|--------|-----------|
| **Models** (`/api/models`) | `GET /` - list models; `GET /{name}` - model details |
| **Features** (`/api/features`) | `GET /` - UI capabilities: hot-reloaded agents, guarded browser, startup MCP tasks, and separate batch repository/worker states so history stays readable without a worker |
| **Console** (`/api/console`) | Read-only cross-thread observability for the current user (the data layer for an operations dashboard or external monitoring): `GET /stats` - headline counters (runs/threads/agents/tokens/cost); `GET /runs` - paginated run history joined with thread titles (per-run cost); `GET /usage` - zero-filled daily token series + per-model breakdown with spend. Queries `runs`/`threads_meta` directly as a reporting layer (no new `RunStore` methods); requires a SQL database backend — returns 503 on `database.backend: memory`. Real-cost estimation reads optional `models[*].pricing` (`currency`, `input_per_million`, `output_per_million`, `input_cache_hit_per_million`; `ModelConfig` is `extra="allow"`, so no schema change) and prices each run from its `token_usage_by_model` input/output split. Pricing is **cache-aware**: `RunJournal` accumulates prompt-cache hits from `usage_metadata.input_token_details.cache_read` into a sparse `cache_read_tokens` bucket key (also threaded through `SubagentTokenCollector` → `record_external_llm_usage_records`), and cache-hit input tokens are billed at `input_cache_hit_per_million` (omitted → billed at the miss price, a conservative upper bound). All priced models must use one currency; mixed currencies disable cost reporting and leave cost/currency fields null instead of producing invalid aggregates. Legacy rows fall back to run-level totals at `model_name`; unpriced models yield `cost: null` and cost fields are null when no pricing is configured |
| **MCP** (`/api/mcp`) | `GET /config` - raw/masked config; `PUT /config` - bulk replace; `PATCH /config` - toggle one server; `POST /config/servers` - add; `PUT /config/server` - replace; `DELETE /config/servers/{server_name:path}` - bodyless delete. Writes validate expanded values, preserve raw configuration where applicable, reload config, and reset the process-local MCP cache. |
| **MCP Tasks** (`/api/threads/{id}/mcp-tasks`) | `GET /` - current user's durable tasks for one owned thread; `POST /` - standalone submission from authenticated server provenance (client provenance-shaped extras are ignored) with exact replay equality provided by a separate private versioned HMAC commitment; `GET /{task_id}` - bounded result/input/status-error/cancellation-error detail, including cancellation attempts and independently authorized parent execution/receipt/evidence fields, without remote task IDs, private commitments, or driver configuration; `POST /{task_id}/cancel` - persist the remote-task cancellation fence |
| **Subagent Batches** (`/api/threads/{id}/subagent-batches`) | Scoped status, controls, evidence, and protected results; model-only submission. |
| **Skills** (`/api/skills`) | `GET /` - list skills; `GET /{name}` - details; `PUT /{name}` - update enabled; `POST /install` - install a thread-local `.skill` archive with standard optional frontmatter; `POST /install/upload` - admin-only multipart upload, authorized before parsing and capped at a 100 MiB file plus 1 MiB framing; `POST /reload` - admin-only process-local prompt-cache invalidation after trusted external filesystem changes |
| **Subagents** (`/api/subagents`) | Admin managed-worker CRUD and listing. |
| **Integrations** (`/api/integrations`) | `GET /lark/status` - inspect managed Lark/Feishu CLI integration state, including `sandbox_runtime_mode` / `sandbox_runtime_ready` (whether `lark-cli` will actually be present in the sandbox at chat time); `POST /lark/install` - admin-only install of the official `lark-*` managed skill pack; `POST /lark/config/start` and `/lark/config/complete` - internal first-time Lark connection setup; `POST /lark/config/credentials` - atomically switch the caller's per-user Lark app after validating the new `app_id`/`app_secret` through the official CLI's live tenant-token probe, revoke/remove the previous OAuth tokens, and restore the prior credential tree if the switch fails; `POST /lark/auth/start` and `/lark/auth/complete` - browser device-flow user authorization without terminal access, with optional `domains` / exact `scope` for incremental permission grants. Config and auth flows carry a server-issued, per-user generation persisted under the credential lock; a rejected direct switch leaves the current generation unchanged, stale completions return 409, and browser re-registration uses the same token-clearing/revocation transaction as direct credential switches. |
| **Memory** (`/api/memory`) | `GET /` - memory data; `POST /reload` - force reload; `GET /config` - config; `GET /status` - config + data |
| **Uploads** (`/api/threads/{id}/uploads`) | `POST /` - upload files (auto-converts PDF/PPT/Excel/Word); `GET /list` - list; `DELETE /{filename}` - delete |
| **Threads** (`/api/threads/{id}`) | `DELETE /` - remove DeerFlow-managed local thread data after LangGraph thread deletion; `POST /branches` - branch a completed assistant turn with a replay checkpoint; inherited titles take next-free displayed sibling suffixes, including explicit/renamed ones, while explicit titles stay unchanged. Durable `branch` admission rejects races. Workspace files are not checkpointed, so the branch only best-effort copies the current workspace when branching from the **latest** turn (`workspace_clone_mode="current_thread_best_effort"`); branching from an older/historical turn skips the copy (`workspace_clone_mode="skipped_historical_turn"`) so the branch never inherits files that only exist in a later timeline. Thread-scoped runtime channels (`sandbox`, `thread_data`) are not copied onto the branch: the parent's `sandbox_id` binds path mappings and the release lifecycle to the parent's workspace, so the branch lazily acquires its own sandbox instead. Branch creation also seeds the new thread's run-event feed from the branch checkpoint's visible messages (`history_seed_mode` in the response): the thread feed reads run_events, not checkpoints, so without the seed the inherited history disappears from the UI after the branch's first run (#4380). Seeded rows are grouped into one synthetic run per inherited turn (`branch-seed-{thread_id}-{n}`, a new turn opening at every persisted human message, including an allowlisted hidden `ask_clarification` reply) because `run_id` is a turn identity to the feed's consumers, not a provenance tag: regenerating an inherited answer supersedes that row's whole `run_id` in `GET /messages/page`, so one shared id for the entire seed deleted the complete inherited history on a branch's first regenerate (#4458); `GET /goal`, `PUT /goal`, `DELETE /goal` - read, set, and clear the active thread goal; `POST /compact` - manually summarize older active context into `summary_text` and retain the recent message window, blocked while a run is in flight; unexpected failures are logged server-side and return a generic 500 detail |
| **Artifacts** (`/api/threads/{id}/artifacts`) | `GET /{path}` - stream regular text and binary artifacts with `FileResponse`, including byte-`Range` 206/416 behavior used by bounded text previews and media seeking; active content types (`text/html`, `application/xhtml+xml`, `image/svg+xml`) are always forced as download attachments to reduce XSS risk; `?download=true` still forces download for other file types. `PUT /{path}` atomically replaces an existing UTF-8 text file under `/mnt/user-data/outputs` when its expected SHA-256 still matches; active runs conflict, and non-mounted sandbox providers receive the same update explicitly. Atomic replacement applies the existing POSIX permission handling when descriptor-based APIs are available and otherwise keeps the platform-native temporary-file permissions (Windows). |
| **Suggestions** (`/api/suggestions`) | `GET /config` - returns global suggestions config boolean; `POST /threads/{id}/suggestions` - generate follow-up questions; rich list/block model content is normalized and inline reasoning (`<think>...</think>`, including unclosed/truncated blocks from reasoning models like MiniMax-M3) is stripped before JSON parsing |
| **Input Polish** (`/api/input-polish`) | `POST /` - rewrite a composer draft before it is sent. This is a short authenticated `runs:create` LLM request using `input_polish` config; it does not create a LangGraph run, persist a message, or modify thread state. Shares the non-graph one-shot LLM path (`deerflow.utils.oneshot_llm.run_oneshot_llm`) with the suggestions route so model build + Langfuse metadata + invoke stay in one place; validates the same stripped view of the draft it sends to the model, and preserves literal `<think>` substrings in the rewrite (`strip_think_blocks(truncate_unclosed=False)`) |
| **Thread Runs** (`/api/threads/{id}/runs`) | `POST /` - create background run; `POST /stream` - create + SSE stream; `POST /wait` - create + block. Before the first journaled run, seed an empty feed from a checkpoint so legacy checkpoint-only history keeps its order and visibility; skip absent checkpoints or populated feeds. `POST /regenerate/prepare` - prepare clean input + checkpoint metadata for regenerating the latest completed or interrupted assistant answer, carrying the latest non-empty thread title in graph input so resuming an older checkpoint cannot roll back a later manual rename (#4457); `POST /edit-regenerate/prepare` - prepare a checkpoint replay from the latest editable human turn with a replacement user message and edit replay metadata; it carries the current thread title the same way, but only when the replay base already has one — an untitled base belongs to a thread the title middleware has not named yet, so pinning the current title there would keep a name generated from the prompt the edit just replaced; `GET /` - list runs; `GET /{rid}` - run details; `POST /{rid}/cancel` - cancel; `GET /{rid}/join` - join SSE; `GET /{rid}/stream` hides action/wait; GET action 405 pre-owner; POST needs `runs:cancel`; `GET /{rid}/messages` - paginated per-run messages `{data, has_more}`; `GET /{rid}/events` - full event stream; `GET /{rid}/workspace-changes` - workspace/output file change summary and optional diffs; `GET/POST /{rid}/artifacts/archive` - receipt manifest / bounded ZIP; `GET /../messages` - legacy thread message array; `GET /../messages/page` - backward thread-global `seq` history page with middleware/subagent-AI/successful-regenerate/edit-replay filtering and page-run-scoped feedback enrichment; subagent AI callbacks remain available through run events while parent `task` ToolMessages stay visible for card restoration; `GET /../token-usage` - aggregate tokens plus an optional `context_usage` percentage. Context usage approximately counts messages from the latest materialized thread state through `build_thread_checkpoint_state_accessor`, so full and delta checkpoint modes expose the same input. The percentage uses the latest run's model and its `context_window`. |
| **Feedback** (`/api/threads/{id}/runs/{rid}/feedback`) | `PUT /` - upsert feedback; `DELETE /` - delete user feedback; `POST /` - create feedback; `GET /` - list feedback; `GET /stats` - aggregate stats; `DELETE /{fid}` - delete specific |
| **Runs** (`/api/runs`) | `POST /stream`, `/wait` - stateless runs requiring `runs:create`; optional body `thread_id` is owner-checked. Scheduled-task create/update/resume/trigger also require `threads:write` plus `runs:create`. `GET /{rid}/messages`, `/feedback` - run messages/feedback |
| **GitHub Webhooks** (`/api/webhooks/github`) | `POST /` - receive GitHub App / repo webhook deliveries. Verifies `X-Hub-Signature-256` against `GITHUB_WEBHOOK_SECRET`; exempt from auth + CSRF because authenticity is enforced by HMAC. The route is fail-closed: mounted only when `GITHUB_WEBHOOK_SECRET` is set, or when explicit dev opt-in `DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS=1` is set. Recognized events include `ping`, `issues`, `issue_comment`, `pull_request`, `pull_request_review`, and `pull_request_review_comment`; unknown events return 200 with `handled=false`. Fan-out runtime failures return 503, keeping the delivery recorded as failed for manual/API/scripted redelivery (GitHub does not automatically retry any failed delivery, 5xx included); permanent/non-retryable conditions such as `channels.github.enabled: false`, unknown events, malformed payloads, or unavailable channel service return 200 with a skipped/handled response. |
| **GitHub Event-Driven Agents** | Custom agents can declare a `github:` block in their `config.yaml` to bind to repos and event triggers. Webhook fan-out publishes one `InboundMessage` per matching binding to the channel bus; `GitHubChannel` routes those messages through `ChannelManager`. The response `dispatch` summarizes matched/fired/skipped agents. |

Thread identifiers use the shared `deerflow.utils.thread_id` contract
`^[A-Za-z0-9_-]{1,64}$`. Caller-provided opaque IDs remain supported; UUIDs
are generated only for `None`, while explicit empty strings fail validation.
Gateway creation and state-producing request boundaries, embedded-client
entry points, filesystem/upload/event-store consumers, scheduled launches,
and the standalone Provisioner enforce the same contract before persistence
or workspace initialization. Route-addressable legacy IDs remain accepted by
pure reads and cleanup/control endpoints; deleting one best-effort removes
metadata and checkpoints but skips local filesystem cleanup, so the raw value
is never interpolated into a host path. New runs, workspace/sandbox
operations, and other state-producing mutations remain blocked.

**Message feed seq** (#4666): streaming `values` frames, `GET
/threads/{id}/state`, and `POST /threads/{id}/history` stamp serialized
messages with `additional_kwargs.deerflow_seq` so clients can place
checkpoint-kept messages against the paged feed; the REST reads resolve the
store via `threads.py::_optional_run_event_store` (a feed-less deployment
still reads threads), and `services.py::normalize_input` strips the
server-owned key from client input (#4380). Mechanism and identity rule:
`packages/harness/deerflow/runtime/AGENTS.md`.

**Workspace change review**: `packages/harness/deerflow/workspace_changes/`
captures a pre-run and post-run snapshot of the thread-owned `workspace` and
`outputs` directories. `runtime/runs/worker.py` performs the filesystem scan via
`asyncio.to_thread` and writes a `workspace_changes` event with category
`workspace` when changes exist. Uploads are intentionally excluded. Text diffs
are size-limited; binary, large, and sensitive-looking paths are persisted as
metadata only. Internal process-feedback directories never count as changes:
the scanner's `EXCLUDED_DIR_NAMES` drops `BROWSER_FRAMES_DIRNAME` (transient
browser screenshots) and `TOOL_RESULTS_DIRNAME` (the tool-output budget
middleware's default externalization subdir, `constants.py` is the shared
source of truth for both writers and the scanner), and the worker threads the
configured `tool_output.storage_subdir` through the snapshot capture as an
extra excluded dir name so custom storage locations stay excluded too.

**Run delivery receipts**: `RunJournal` records each non-empty artifact update
once per tool `Command` for the terminal `run.delivery` event. When a command
contains multiple messages, a unique tool name resolved from matching
`ToolMessage` entries supplies attribution; additional command messages do not
duplicate artifact paths or counts. If multiple different tool names resolve
for one flat artifact update, the paths remain counted but unattributed because
the command does not carry a per-path mapping. `RunJournal` callbacks set
`run_inline=True`: they do only in-memory bookkeeping or schedule async writes,
and staying on the run's event-loop thread serializes parallel tool callbacks
before terminal delivery recording and flushing. Each worker creates a separate
journal per run before cancellable/fallible preflight work, so checkpoint
compatibility failures and cancellation while waiting for prior finalization
still emit a zero-delivery receipt. The worker flushes ordinary journal events,
idempotently persists the run-scoped receipt, and only then persists the staged
terminal run status. A receipt failure is retried on a short bounded schedule
while the owning worker still knows the real outcome and holds the lease. The
worker derives delivery requirements from the run's workspace snapshots rather
than a client request option: every regular file created or modified under
`/mnt/user-data/outputs` is a candidate produced artifact. Internal
process-feedback files are not candidates: the snapshot capture excludes the
scanner's `EXCLUDED_DIR_NAMES` (including the default tool-output
externalization subdir) plus the configured `tool_output.storage_subdir`, so a
run that only externalized oversized tool outputs does not fail delivery. At
least one candidate must be covered by a path attributed by the journal to
`present_files`; presenting only an unrelated pre-existing path does not
satisfy delivery.
Receipts for such runs add `produced_paths`, `presented_paths`, `matched_paths`,
`verification`, `stage`, and `satisfied` to the Slice 1 fact fields. Missing a
matching presentation becomes a run error; a successful presentation is also
downgraded to error if its receipt cannot be durably verified. Runs without
changed outputs preserve ordinary chat behavior and the original receipt shape.
Orphan recovery first
atomically claims an expired lease, then uses the same singleton write to
backfill a zero-delivery receipt. This ordering prevents a stale recovery scan
from overwriting a live run's later detailed receipt; an event-store outage
does not undo the terminal takeover. An existing detailed receipt is preserved
when a worker crashed after writing it. Event stores
serialize `put_if_absent` with ordinary thread writers: memory and JSONL provide
the documented single-process guarantee, while the DB store adds per-thread
in-process locks and PostgreSQL advisory locks for cross-process writers. Every
manager-authored receipt carries the authoritative run owner identity. The DB
singleton write repairs a legacy NULL identity under that same lock and rejects
a contradictory non-NULL identity; migration `0030_run_delivery_owner_backfill`
repairs terminal legacy rows that will never re-enter recovery. Taskless and
pre-graph compensation retains its exact admission obligation when this write
is unavailable, leaving the owner-fenced row active until receipt-first
terminalization can be retried.
Moving journal construction ahead of preflight is receipt-only on early failure
paths: a separate boundary flag preserves the previous completion-data
semantics, so checkpoint incompatibility or cancellation while waiting for an
older finalizing run does not persist an empty completion snapshot. Worker tests
pin one accumulated receipt across multiple goal-continuation `_stream_once`
calls; journal tests drive LangChain's real async callback dispatcher against a
single journal to pin serialized, deduplicated parallel tool callbacks.
Multi-worker deployments therefore require `run_events.backend: db` for shared,
ordered delivery events; the startup gate rejects process-local memory and
JSONL event stores when `GATEWAY_WORKERS > 1`.

**RunManager / RunStore contract**:
- The exact-two profile accepts only a live `RunStore` advertising
  `lease_clock_authority=database_v1`. Gateway checks this after lifecycle
  initialization and before run heartbeat/reconciliation, and readiness keeps
  checking the constructed adapter. Memory and other non-exact profiles retain
  `process_v1`; a configured PostgreSQL label alone is not sufficient evidence.
- LangGraph-compatible run requests validate their supported subset before creating a run. `runtime/stream_modes.py` is the shared backend contract for public stream modes and the worker's `graph.astream` mapping; the public `messages-tuple` mode maps to LangGraph's internal `messages` mode, while public `messages`, `events`, and other unsupported modes are rejected instead of being dropped or replaced with `values`. `app/gateway/run_models.py::RunCreateRequest` is shared by HTTP and internal scheduled launch paths, retains only truthful compatibility defaults for unimplemented options (`if_not_exists="create"` plus `None` placeholders), returns 422 for unsupported values including `on_completion="complete"`, `on_completion="continue"`, and `multitask_strategy="enqueue"`, and forbids undeclared SDK options so fields such as `checkpoint_during` and `durability` cannot be silently discarded. A placeholder must still accept the stock SDK's own default: `langgraph_sdk` drops only `None` from its run payload, so `stream_resumable=False` reaches every request and means "non-resumable", which is what DeerFlow serves — rejecting it 422'd every IM channel run (#4466). `tests/test_run_request_validation.py::test_gateway_accepts_langgraph_sdk_default_payload` pins the real SDK payload against this boundary; channel tests mock the SDK client and cannot catch this class of drift.
- `RunManager.get()` is async; direct callers must `await` it.
- The history batch helpers `list_successful_regenerate_sources()`, `list_edit_regenerate_runs()`, and `get_many_by_thread()` default to `user_id=AUTO`: they resolve the request user and fail closed when no user context exists. Migration/admin callers that intentionally need an unscoped read must pass `user_id=None` explicitly.
- Edit-and-rerun visibility is derived from edit replay runs (`metadata.replay_kind="edit"` plus `regenerate_from_run_id`) by `RunManager.list_edit_replay_visibility()`: the newest attempt for each source run is authoritative. Pending/running/success attempts hide the original source run; failed, timed-out, or interrupted attempts hide only the failed attempt so the original conversation reappears.
- When a persistent `RunStore` is configured, `get()` and `list_by_thread()` hydrate historical runs from the store. In-memory records win for the same `run_id` so task, abort, and stream-control state stays attached to active local runs.
- Thread metadata status switches to `running` only after `RunManager.try_start()` succeeds. Pending-cancelled runs therefore skip the projection, while clients may observe the prior thread status during the short worker-startup window. Every run-derived title/status write goes through `ThreadMetaStore.project_run()`: the store proves the run has the latest database-assigned `admission_cursor` and either the exact active owner/state epoch or the exact terminal state version. Terminal title and status are one conditional write; missing legacy cursors and malformed terminal owner/lease state fail closed. Human/admin metadata updates remain separate APIs.
- `cancel()` returns a :class:`~deerflow.runtime.CancelOutcome` enum: `cancelled` (local cancel), `requested` (the non-owning worker durably recorded the first cancellation action for the live owner), `taken_over` (non-owning worker claimed a `terminalize_v1` run because the owner's lease expired — marks it as `error`), `lease_valid_elsewhere` (legacy/custom store lacks the durable request primitive — caller retains the safe 409 + `Retry-After` fallback), `not_active_locally` (heartbeat disabled or an exact-two store-only row cannot use generic takeover, preserving the safe 409 path), `not_cancellable` (terminal state), or `unknown` (not found in memory or store). Receiptless compatibility configurations retain atomic `interrupt`/`rollback` replacement in the run store.
- Durable-event `interrupt`/`rollback` admission uses a stable process-local per-thread gate and resolves already-persisted external/idempotency replays with read-only lookups. A novel candidate facing any active predecessor returns `ConflictError` before local mutation; `require_predecessor_inactive` repeats that check under the official store's mutation lock to close cross-process races. Run and event stores have no composite replacement transaction: writing a zero receipt first could suppress a later detailed receipt if candidate admission fails, while terminalizing first can expose a receiptless run. Clients must explicitly cancel, await terminal `run.delivery`, then retry. The strategy names remain a reversible interface seam for a future additive prepared-replacement intent that reserves candidate identity and predecessor epoch before cancellation.
- Store-only hydrated runs are readable history. In multi-worker mode with heartbeat enabled, cancel on a store-only run records `runs.cancel_action` / `cancel_requested_at` while the owner's lease is live; the first action wins even if a retry later lands on the owner. `RunStore.request_cancel()` and owner completion through `finalize_if_not_cancelled()` are competing active-row CAS operations, so an accepted cancel cannot be overwritten by a later success. `RunStore.renew_lease()` renews and observes the request atomically in the SQL implementation. The owner then executes the normal process-local interrupt/rollback and terminal stream path without transferring the lease. An expired `terminalize_v1` owner is taken over and marked `error`; an exact-two store-only row remains active and returns the safe 409 path while execution takeover is unavailable. `wait=true` and cancel-then-stream use the shared bridge to observe owner finalization; a non-standard process-local bridge returns accepted 202 instead of subscribing to an unreachable stream. In single-worker mode (heartbeat off), store-only runs still return 409.
- Qualified stores mint lease deadlines from a duration and one post-lock PostgreSQL-time sample; pod clocks never interpret persisted `lease_expires_at` as authority. Before each store call the owner arms a monotonic safety deadline. A timeout, failed renewal, or exception sets `ownership_lost`, raises `abort_event`, and cancels the task. Compatibility stores retain absolute deadlines outside exact-two. Heartbeat gathers cancellation requests but signals local tasks only after all renewals finish; finalization owns status and rollback cleanup. Fenced workers stop journal, receipt, progress/completion/status, checkpoint/thread-metadata, and completion-hook writes. Terminal completion cannot replace another terminal status. `grace_seconds` is a database-clock recovery delay, and committed remote side effects remain outside this boundary.
- Startup/orphan reconciliation must claim stale active rows with `RunStore.claim_for_takeover()`, not a plain `update_status()`. The final claim re-checks `status` and lease expiry atomically, so a heartbeat renewal between the candidate scan and the recovery write keeps the run active.
- Exact-two candidates stamp immutable `exact_two_takeover_v1`; ordinary profiles retain `terminalize_v1`. Generic `claim_for_takeover()` and atomic interrupt/rollback replacement reject exact-two rows under their mutation locks, and every dedicated execution-takeover claim is currently rejected before owner CAS. `HARTMESH_EXECUTION_RECOVERY_CLAIMS_ENABLED` defaults false and cannot bypass any gate. Expired rows stay fail-closed instead of falling through to terminalization. The dormant coordinator/schema is a reversible seam only; see `docs/MULTI_GATEWAY_QUALIFICATION.md` for activation and qualification requirements.
- Run admission and independent writes are first-class thread operations. `runs.operation_kind` distinguishes `run` from `checkpoint_write`, `artifact_write`, `artifact_archive`, `branch`, and `delete`; every active kind shares the durable active-thread uniqueness constraint. New operation kinds must go through `RunStore.create_thread_operation_atomic()` and `RunManager.reserve_thread_operation()` rather than adding another lock or metadata marker. Live and lease-less reservations are non-interruptible; an expired leased reservation can be reclaimed immediately by interrupt/rollback admission without waiting for orphan reconciliation. Lease-less rows stay fail-closed because the store cannot distinguish a stale row from a live writer in another heartbeat-disabled worker; a rare failed delete therefore requires startup reconciliation, and heartbeat-disabled multi-worker deployment remains unsupported. Reservation bodies are attached to their caller task so loss detected by lease renewal cancels the writer before it can continue after takeover; the context manager translates that lease-loss cancellation to `ConflictError` after cleanup so Gateway mutation routes return a retryable 409 instead of dropping the HTTP request. The cleanup scope begins immediately after durable admission, including the await that attaches the caller task, so cancellation cannot strand a locally renewed pending reservation. A failed renewal is revalidated under the manager lock before cancellation; if the reservation completed and unregistered while the store update was in flight, its request task must not be cancelled after the write. Reservations are excluded from run history/reporting and from run-only helpers such as `list_by_thread()` and `has_inflight()`, release uses the captured owner rather than ambient user context, and local cleanup still runs when the best-effort store delete fails. `RunStore.create_run_atomic()` remains a deprecated compatibility shim for external stores that only admit normal runs; new stores must implement `create_thread_operation_atomic()` to support internal operation kinds.
- Gateway checkpoint mutations outside run execution must use `services.reserve_checkpoint_write()`, which composes the process-local thread lock with the durable `checkpoint_write` reservation. Manual compaction, `POST /threads/{id}/state`, and both goal mutation routes (`PUT` / `DELETE /threads/{id}/goal`, including creation of a missing goal checkpoint) use this boundary, so an existing run blocks the write and the reservation blocks new reject/interrupt/rollback runs across workers.
- `POST /wait` (both thread-scoped and `/api/runs/wait`) drains the stream bridge via `wait_for_run_completion()` instead of bare `await record.task`, so it honours the run's `on_disconnect` setting and cancels the background run on real client disconnect rather than returning a stale checkpoint (issue #3265).
- Memory and Redis `StreamBridge` retain at most `queue_maxsize` data events. A valid `Last-Event-ID` below the retained watermark, or a lagging live subscriber, receives `StreamGap` before partial replay; SSE maps it to id-less `stream_replay_gap` without stopping the run. `/wait` resumes from the latest retained ID. Redis atomically checks bounds and reads, using blocking `XREAD` only to wake and repeat that snapshot; a first no-cursor wake is provisional until retention is rechecked. Malformed cursors remain backend-specific. Memory conservatively gaps numeric cursors below its watermark and preserves replay-from-earliest for unknown IDs at or above it.
- Redis `StreamBridge` keys use a rolling retained-buffer TTL (`stream_bridge.stream_ttl_seconds`, refreshed on `publish()` / `publish_end()`) as a leak safety net, not as a run timeout. Startup and lease-driven periodic orphan recovery share one Gateway stream-terminalization path: after `RunManager` durably marks a run `error` with `stop_reason=orphan_recovered`, Gateway publishes `END_SENTINEL`, conditionally projects the recovered terminal status, and schedules stream cleanup. The same `ThreadMetaStore.project_run()` path is used at startup and during periodic recovery; its latest `admission_cursor` and exact terminal-version checks make concurrent newer admissions win atomically, while legacy NULL cursors fail closed. The periodic store scan, per-row status writes, and Gateway callback run as one supervised single-flight task, so a slow pass is skipped at the next interval instead of piling up or pausing the sole lease-renewal loop. Store retries have bounded attempts/backoff; an individual operation still relies on the database driver/pool timeout. `RunManager.shutdown()` gives active user runs priority within its shared deadline, then drains or cancels orphan recovery. Gateway tracks delayed recovered-stream cleanups and converts unfinished delays to immediate deletes before closing the bridge; the Redis TTL remains the outage safety net. Store-only SSE and `/wait` consumers wait for the bridge's real END marker after an ordinary durable terminal status, because status persistence can precede tail events. The explicit `orphan_recovered` signal is the only heartbeat fallback: its publisher is known to be gone, so it supplies the liveness boundary if END publication fails or the retained key expires. Malformed `Last-Event-ID` reconnect values live-tail new Redis events rather than replaying the retained buffer. Keep cross-component recovery orchestration in Gateway through the generic `RunManager.on_orphans_recovered` callback; do not introduce a harness-to-app dependency. Callback failure warnings include every recovered `run_id` so operators can identify rows whose Gateway-side terminalization needs inspection.
- Thread-scoped run creation accepts `checkpoint` / `checkpoint_id`; Gateway validates the checkpoint belongs to the request thread before writing `checkpoint_id` / `checkpoint_ns` into `config.configurable` for LangGraph branching. In `delta` checkpoint mode the worker rewrites that fork into a linear head write before the graph starts (see "A delta-mode run cannot fork" under Checkpoint Channel Modes), because delta state for a fork replays the abandoned sibling's writes.
- After each visible turn, `runtime/goal.py` evaluates active `ThreadState.goal` against visible evidence with one reused non-thinking model. Because the graph trace has closed, the evaluator attaches its own callbacks and Langfuse thread/user/trace metadata. Satisfaction clears the goal; other outcomes persist `last_evaluation`, and stopping outcomes add `stand_down_reason`. Only `goal_not_met_yet` streams a hidden continuation, requiring a durable assistant checkpoint, unchanged thread, no abort, and no-progress clearance. The hard cap is 8 (clamped in TUI/tools, 422 in HTTP); two continuations without new visible assistant evidence stop. Shared response-cleanup helpers live in `deerflow.utils.llm_text`.
- Run event stream changes must keep producer code, `deerflow/constants.py`, `runtime/events/catalog.py`, `contracts/run_event_stream_contract.json`, `backend/docs/RUN_EVENT_STREAM.md`, and `tests/test_run_event_stream_contract.py` in sync. The dependency-free constants module owns the persisted envelope limits (`event_type` 32 characters, `category` 16) and cross-layer workspace event identity; the catalog owns validated runtime definitions and categories. Dynamic middleware tags are limited to 21 characters after the `middleware:` prefix. The JSON contract owns payload schemas, backend-specific storage semantics, legacy aliases, and compatibility rules; conformance tests require both views and all producer groups to agree. `run.end.content` remains opaque and may retain nested Python values in memory while JSONL/database stores stringify non-JSON nested values, so consumers must not assume backend-identical nested output representations.

Proxied through nginx: `/api/langgraph/*` → Gateway LangGraph-compatible runtime, all other `/api/*` → Gateway REST APIs.

**Branch/regenerate checkpoint invariant**: `app/gateway/checkpoint_lineage.py`
walks `parent_config` rather than globally ordered checkpoint history so replay
anchors stay on the selected lineage after regenerations create sibling branches.
New conversation branches persist the pre-user replay anchor before their visible
head through the state mutation graph, which preserves materialized state in both
full and delta checkpoint modes. Only an explicitly absent legacy parent link may
use chronological compatibility lookup; cycles, dangling links, and depth-limit
exhaustion fail closed. Existing single-checkpoint branches are never repaired by
copying a raw checkpoint because delta state is not self-contained in one tuple.
Both lookups additionally require the replay base to be a **settled** checkpoint
(`has_pending_tasks` — no scheduled `next` tasks). A checkpoint with pending tasks
is a mid-run snapshot: resuming from it replays the writes of the node that was
about to run. Message ids alone cannot exclude those, because middleware may
rewrite a message's id inside the run that produced it — `DynamicContextMiddleware`
moves the first user turn to `{id}__user` and gives `{id}` to the injected
reminder, so every checkpoint written before it holds the same prompt under an
unmatched id. Selecting one of those re-added the original prompt *after* the
edited one, and the model answered the question the edit was replacing (#4531).
`next` is not derivable on the degraded raw-checkpoint read path, which reports no
tasks; absence of evidence stays permissive there rather than failing closed.
Edit replay resolves its base through the same lineage-first path as regenerate;
it must pass `head_checkpoint` or it silently degrades to the chronological scan
that cannot tell sibling branches apart.

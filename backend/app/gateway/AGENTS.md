### Gateway API (`app/gateway/`)

FastAPI listens on port 8001; health: `GET /health` (liveness), `GET /ready`
(HartMesh readiness incl. tenant identity and topology; the Helm probe),
`GET /health/ready` (upstream persistence readiness; the Compose healthcheck).
`GATEWAY_ENABLE_DOCS=false` disables `/docs`, `/redoc`, and `/openapi.json`.

`/api/runtime/v1/*` and in-process adapters share one `InvocationRuntime` and
its accepted identity/material admission. `GracefulShutdownCoordinator` orders shutdown; unproven quiescence leaves resources to process reclamation.

Durable MCP task notifications run as internal Agents: keep trusted delivery instructions outside user input, frame remote payloads as untrusted, and require existing owned threads so late events dead-letter rather than recreate deleted chats.

CORS defaults to same-origin through nginx. Split-origin or port-forwarded
clients must set exact `GATEWAY_CORS_ORIGINS` for CORS/CSRF and expose
`Content-Location` via `CORS_EXPOSED_HEADERS`; the LangGraph SDK reads new run
IDs from that header to resolve placeholders and enable edit/regenerate/branch.

Browser auth sessions are owned by `app.gateway.auth.session_cookie`. Login accepts a `remember_me` flag; the Gateway never stores passwords. `SessionCookiePolicy` persists the `HttpOnly access_token` cookie only for HTTPS/trusted-forwarded HTTPS, direct-host localhost HTTP, or explicit insecure opt-in; public HTTP sandbox URLs degrade to session cookies. Session-creating handlers stamp the final `max_age` on `request.state` and CSRF cookie creation mirrors it (incl. re-issue after password changes and OIDC callbacks); a small `HttpOnly` preference cookie keeps the remember choice across re-issues. Logout clears all auth cookies and suppresses CSRF re-issue.

Login lockout is keyed on the **account**, not the client address (`app.gateway.auth.login_throttle`), plus a loose per-source spray guard; a locked account answers exactly as a wrong password does. `auth.local.lockout_store` fails closed; admins unlock via `/api/v1/auth/lockouts`.

In local-password mode an administrator adds a person with `POST /api/v1/auth/users` (admin + interactive session, as the lockout routes; refused in sign-on-only mode) or the deployer with `python -m app.gateway.auth.add_user` -- one operation, `app.gateway.auth.local_accounts`: role `user`, a one-time password returned once and stored only as a hash, `needs_setup` set. Every session of a `needs_setup` account (this, and `reset_admin`) is refused with `403 setup_required` everywhere but `mode.SETUP_ROUTES` (`/me`, `/change-password`); the rule lives in `mode.require_live_account`, the session paths' one refusal (the middleware's cookie path, the WebSocket, the LangGraph hook). PATs and internal launches keep `mode.account_refusal` (disabled/inert only): an added account holds neither, and a reset exposes neither. `/health` carries `registration` (`open`/`closed`, `mode.registration_state`) beside `auth_mode`.

Provider keys in the product (`app/gateway/provider_keys/`, `routers/provider_keys.py`): only where `HARTMESH_PROFILE_DIR` names the compose profile directory, the Gateway has a session factory, and the profile is not `durable_two_gateway_v1` (`deps.py`; a write would reach only the replica that took it). `ProviderKeyService.start()` runs inside `langgraph_runtime` right after the tenant binding, before anything is built from the config: for each catalog variable it puts the stored key (unwrapped with `HARTMESH_PROVIDER_KEYS_SECRET`, Fernet) or the value the process started with into `os.environ`, renders with the profile's own `gateway/render_config.py` to `config.effective.yaml` beside the base file, points this process's `DEER_FLOW_CONFIG_PATH` at it and reloads; the lifespan then rebinds `startup_config`. Every write renders first (a refusal changes nothing), installs, then commits, so a change the Gateway could not apply is never recorded; a failed commit re-installs what is stored. The install sets new variables before the file swap and drops old ones after the reload, so a concurrent config read always resolves. No stored key: no render, no second file. A stored key that cannot be unwrapped leaves its provider with no key, never the environment's. `HARTMESH_MODELS_FILE` refuses writes and suppresses applying. The base `config.yaml` stays what the environment alone renders, so operator commands keep loading it. `python -m app.gateway.provider_keys.status` is the deployer's source signal.

PATs (`Bearer dfp_...`) act as owners, never services; invalid Bearers get 401, never cookie fallback. `PAT_ROUTE_SCOPE_RULES` default-denies PAT routes by owner permission. UUID4 public refs are tenant-bound and token-free; `TrustedRunContextV1` v4 binds them. `require_audited_permission` rechecks authority, fails closed before mutation, and returns the actor. Other audit is best-effort; revocation blocks reuse; management is session-only. See [the contract](../../../docs/AUDITABLE_AUTOMATION_IDENTITIES.md).

Localhost persistence deliberately reads the direct request `Host` and ignores `Forwarded` / `X-Forwarded-Host`. Scheme and auth-origin reconstruction still consume forwarding headers. The bundled nginx sets `X-Forwarded-Proto`, but preserves an upstream HTTPS value and does not overwrite every forwarded header, so the outer trusted proxy must replace or strip client-supplied forwarding headers before traffic reaches DeerFlow.

Standalone LangGraph Studio is recognized only by the upstream `Auth.types.StudioUser` principal type; older SDKs fall back to normal owner scoping. Its assistant reads cover registered assistants plus its own; other resources stay owner-scoped. Create/update makes `user_id` and
`created_by=user` server-owned. Before runtime 0.30.0 loads,
`langgraph_studio.py` uses the CLI graph registry to recreate genuine system
assistants and demote all other legacy `created_by=system` active/version rows.
The file loader does not pre-register the module in `sys.modules`, so keep annotations eager and preserve the loader regression test. Missing persistence is a no-op; parse/write errors fail startup, and missing registered rows warn of drift.

**Routers**:

| Router | Endpoints |
|--------|-----------|
| **Models** (`/api/models`) | `GET /` - list models; `GET /{name}` - model details |
| **Features** (`/api/features`) | `GET /` - UI capabilities: hot-reloaded agents, guarded browser, startup MCP tasks, batch repository/worker states, `ui`, and `branding` from the tenant bundle (`/api/branding/logo` serves its picture) |
| **Console** (`/api/console`) | Read-only cross-thread observability for the current user: `GET /stats` - headline counters (runs/threads/agents/tokens/cost); `GET /runs` - paginated run history joined with thread titles (per-run cost); `GET /usage` - zero-filled daily token series + per-model breakdown with spend. Queries `runs`/`threads_meta` directly (no new `RunStore` methods); 503 on `database.backend: memory`. Real-cost estimation reads optional `models[*].pricing` (`currency`, `input_per_million`, `output_per_million`, `input_cache_hit_per_million`; `ModelConfig` is `extra="allow"`) and prices each run from its `token_usage_by_model` input/output split. Pricing is **cache-aware**: `RunJournal` accumulates prompt-cache hits from `usage_metadata.input_token_details.cache_read` into a sparse `cache_read_tokens` bucket key (also via `SubagentTokenCollector`), and cache-hit input tokens are billed at `input_cache_hit_per_million` (omitted → the miss price). All priced models must use one currency; mixed currencies null every cost/currency field. Legacy rows fall back to run-level totals at `model_name`; unpriced models or deployments yield null cost fields |
| **MCP** (`/api/mcp`) | Read masked config; legacy-only replace/add/delete/toggle routes validate and atomically reload config, then reset the local cache. Mutations return 409 unless `tool_plane.enabled=false`. |
| **Tool Plane** (`/api/tool-plane`) | Status/history/inspection and stage → validate → promote/rollback for deployment-base/current-user revisions; `/admin/...` explicitly selects another opaque user scope. Exact-two mounts only reads, omitting mutation/bootstrap from routing and OpenAPI. |
| **MCP Tasks** (`/api/threads/{id}/mcp-tasks`) | `GET /` - current user's durable tasks for one owned thread; `POST /` - standalone submission from authenticated server provenance (client provenance-shaped extras are ignored) with exact replay equality provided by a separate private versioned HMAC commitment; `GET /{task_id}` - bounded result/input/error detail incl. cancellation attempts and independently authorized parent execution/receipt/evidence fields, never remote task IDs, private commitments, or driver configuration; `POST /{task_id}/cancel` - persist the remote-task cancellation fence |
| **Subagent Batches** (`/api/threads/{id}/subagent-batches`) | Scoped status, controls, evidence, and protected results; model-only submission. |
| **Skills** (`/api/skills`) | List/details plus admin cache reload. Archive upload is authorized before parsing and capped at 100 MiB + 1 MiB framing. Install/edit/delete/state/legacy-rollback routes are legacy-only and return 409 while governance is enabled. |
| **Subagents** (`/api/subagents`) | Admin managed-worker CRUD and listing. |
| **Integrations** (`/api/integrations`) | `GET /lark/status` - inspect managed Lark/Feishu CLI integration state, including `sandbox_runtime_mode` / `sandbox_runtime_ready` (whether `lark-cli` will actually be present in the sandbox at chat time); `POST /lark/install` - admin-only install of the official `lark-*` managed skill pack, legacy-only and rejected while governed revisions are enabled; `POST /lark/config/start` and `/lark/config/complete` - internal first-time Lark connection setup; `POST /lark/config/credentials` - atomically switch the caller's per-user Lark app after validating the new `app_id`/`app_secret` through the official CLI's live tenant-token probe, revoke/remove the previous OAuth tokens, and restore the prior credential tree if the switch fails; `POST /lark/auth/start` and `/lark/auth/complete` - browser device-flow user authorization without terminal access, with optional `domains` / exact `scope` for incremental permission grants. Config and auth flows carry a server-issued, per-user generation persisted under the credential lock; a rejected direct switch leaves the current generation unchanged, stale completions return 409, and browser re-registration uses the same token-clearing/revocation transaction as direct credential switches. |
| **Memory** (`/api/memory`) | `GET /` - memory data; `POST /reload` - force reload; `GET /config` - config; `GET /status` - config + data |
| **Uploads** (`/api/threads/{id}/uploads`) | `POST /` - upload files (auto-converts PDF/PPT/Excel/Word); non-mounted sandbox sync uses a non-releasing request lease; `GET /list` - list; `DELETE /{filename}` - delete |
| **Files** (`/api/files`, `/api/threads/{id}/files`) | The person's own files (`users/{user_id}/files`, mounted rw at `/mnt/user-data/files` in every sandbox of theirs): `GET /` lists (recursive; hidden, symlinked and unaddressable paths skipped; `truncated`), `GET /{path}` streams (active content downloads), `DELETE /{path}` removes a file, not a folder, `POST /api/threads/{id}/files` keeps an owned thread's upload or output into `folder` (keep-both `_N`). Per owner under `threads:*` — capped authority universe, so no new permission; internal owner header honoured; no PAT route. |
| **Shared** (`/api/shared`) | The company's Shared area, one per tenant at `/mnt/user-data/shared` in every sandbox; same shapes as Files, plus publisher, when, and per-caller `can_remove` on `GET /`. `POST /publish` copies one of the caller's own files or an owned conversation's upload/output; `DELETE /{path}` is the publisher's or an admin's and amends the never-deleted `shared_publications` row. 503 on memory backend. |
| **Threads** (`/api/threads/{id}`) | `DELETE /` - remove DeerFlow-managed local thread data after LangGraph thread deletion; `POST /branches` - branch a completed assistant turn with a replay checkpoint; inherited titles take next-free displayed sibling suffixes, including explicit/renamed ones, while explicit titles stay unchanged. Durable `branch` admission rejects races. Workspace files are not checkpointed, so the branch only best-effort copies the current workspace when branching from the **latest** turn (`workspace_clone_mode="current_thread_best_effort"`); branching from an older/historical turn skips the copy (`workspace_clone_mode="skipped_historical_turn"`) so the branch never inherits files that only exist in a later timeline. Thread-scoped runtime channels (`sandbox`, `thread_data`) are not copied onto the branch: the parent's `sandbox_id` binds path mappings and the release lifecycle to the parent's workspace, so the branch lazily acquires its own sandbox instead. Branch creation also seeds the new thread's run-event feed from the branch checkpoint's visible messages (`history_seed_mode` in the response), because the feed reads run_events, not checkpoints (#4380); seeded rows form one synthetic run per inherited turn (`branch-seed-{thread_id}-{n}`, a turn opening at every persisted human message, including an allowlisted hidden `ask_clarification` reply), because regenerating an inherited answer supersedes its whole `run_id` in `GET /messages/page` and one shared id would delete the entire inherited history (#4458); `GET /goal`, `PUT /goal`, `DELETE /goal` - read, set, and clear the active thread goal; `POST /compact` - manually summarize older active context into `summary_text` and retain the recent message window, blocked while a run is in flight; unexpected failures are logged server-side and return a generic 500 detail |
| **Artifacts** (`/api/threads/{id}/artifacts`) | `GET /{path}` - stream regular text and binary artifacts with `FileResponse`, including byte-`Range` 206/416 behavior used by bounded text previews and media seeking; active content types (`text/html`, `application/xhtml+xml`, `image/svg+xml`) are always forced as download attachments to reduce XSS risk; `?download=true` still forces download for other file types. `PUT /{path}` atomically replaces an existing UTF-8 text file under `/mnt/user-data/outputs` when its expected SHA-256 still matches; active runs conflict, and non-mounted sandbox providers receive the same update under a request lease. Atomic replacement applies the existing POSIX permission handling when descriptor-based APIs are available and otherwise keeps the platform-native temporary-file permissions (Windows). |
| **Suggestions** (`/api/suggestions`) | `GET /config` - returns global suggestions config boolean; `POST /threads/{id}/suggestions` - generate follow-up questions; rich list/block model content is normalized and inline reasoning (`<think>...</think>`, including unclosed/truncated blocks) is stripped before JSON parsing |
| **Input Polish** (`/api/input-polish`) | `POST /` - rewrite a composer draft before it is sent. This is a short authenticated `runs:create` LLM request using `input_polish` config; it does not create a LangGraph run, persist a message, or modify thread state. Shares the non-graph one-shot LLM path (`deerflow.utils.oneshot_llm.run_oneshot_llm`) with the suggestions route so model build + Langfuse metadata + invoke stay in one place; validates the same stripped view of the draft it sends to the model, and preserves literal `<think>` substrings in the rewrite (`strip_think_blocks(truncate_unclosed=False)`) |
| **Thread Runs** (`/api/threads/{id}/runs`) | `POST /` - create background run; `POST /stream` - create + SSE stream; `POST /wait` - create + block. Before the first journaled run, seed an empty feed from a checkpoint so legacy history keeps its order and visibility (skip absent checkpoints or populated feeds). `POST /regenerate/prepare` - prepare clean input + checkpoint metadata for regenerating the latest completed or interrupted assistant answer, carrying the latest non-empty thread title so resuming an older checkpoint cannot roll back a later rename (#4457); `POST /edit-regenerate/prepare` - prepare a checkpoint replay from the latest editable human turn with a replacement user message and edit replay metadata; it carries the current title the same way only when the replay base already has one, since an untitled base belongs to a thread the title middleware has not named yet and pinning a title there would keep a name generated from the replaced prompt; `GET /` - list runs; `GET /{rid}` - run details; `POST /{rid}/cancel` - cancel; `GET /{rid}/join` - join SSE; `GET /{rid}/stream` hides action/wait; GET action 405 pre-owner; POST needs `runs:cancel`; `GET /{rid}/messages` - paginated per-run messages `{data, has_more}`; `GET /{rid}/events` - full event stream; `GET /{rid}/workspace-changes` - workspace/output file change summary and optional diffs; `GET/POST /{rid}/artifacts/archive` - receipt manifest / bounded ZIP; `GET /../messages` - legacy thread message array; `GET /../messages/page` - backward thread-global `seq` history page with middleware/subagent-AI/successful-regenerate/edit-replay filtering and page-run-scoped feedback enrichment; subagent AI callbacks remain available through run events while parent `task` ToolMessages stay visible for card restoration; `GET /../token-usage` - aggregate tokens plus an optional `context_usage` percentage. Context usage counts messages from the latest materialized thread state via `build_thread_checkpoint_state_accessor`, so full and delta checkpoint modes expose the same input. The percentage uses the latest run's model and its `context_window`. |
| **Feedback** (`/api/threads/{id}/runs/{rid}/feedback`) | `PUT /` - upsert feedback; `DELETE /` - delete user feedback; `POST /` - create feedback; `GET /` - list feedback; `GET /stats` - aggregate stats; `DELETE /{fid}` - delete specific |
| **Runs** (`/api/runs`) | `POST /stream`, `/wait` - stateless runs requiring `runs:create`; optional body `thread_id` is owner-checked. Scheduled-task create/update/resume/trigger also require `threads:write` plus `runs:create`. `GET /{rid}/messages`, `/feedback` - run messages/feedback |
| **GitHub Webhooks** (`/api/webhooks/github`) | `POST /` - receive GitHub App / repo webhook deliveries. Verifies `X-Hub-Signature-256` against `GITHUB_WEBHOOK_SECRET`; exempt from auth + CSRF because authenticity is enforced by HMAC. The route is fail-closed: mounted only when `GITHUB_WEBHOOK_SECRET` is set, or when explicit dev opt-in `DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS=1` is set. Recognized events include `ping`, `issues`, `issue_comment`, `pull_request`, `pull_request_review`, and `pull_request_review_comment`; unknown events return 200 with `handled=false`. Fan-out runtime failures return 503, keeping the delivery recorded as failed for manual/API/scripted redelivery (GitHub does not automatically retry any failed delivery, 5xx included); permanent/non-retryable conditions such as `channels.github.enabled: false`, unknown events, malformed payloads, or unavailable channel service return 200 with a skipped/handled response. |
| **GitHub Event-Driven Agents** | Custom agents can declare a `github:` block in their `config.yaml` to bind to repos and event triggers. Webhook fan-out publishes one `InboundMessage` per matching binding to the channel bus; `GitHubChannel` routes those messages through `ChannelManager`. The response `dispatch` summarizes matched/fired/skipped agents. |

**Run evidence exports**: the evidence-bundle GET/POST is terminal-only,
owner-scoped `runs:read`, PAT-allowlisted, no-store, and distinct from ordinary
ZIPs. It reuses `artifact_archive`; `run_evidence.py` snapshots repositories and
the archive binds exact copied bytes. Snapshot coverage follows accepted
capabilities and terminal attempts; operations cancel on disconnect or the
60-second deadline. Return stable errors/public refs; bundles are unsigned.

Thread IDs use `deerflow.utils.thread_id` (`^[A-Za-z0-9_-]{1,64}$`); `None` generates a UUID and empty strings fail. Creation/state-producing boundaries validate before persistence or workspace initialization. Legacy IDs stay readable/controllable but cannot drive new runs or filesystem state; cleanup skips their host paths.

**Message feed seq** (#4666): streaming `values` frames, `GET
/threads/{id}/state`, and `POST /threads/{id}/history` stamp serialized
messages with `additional_kwargs.deerflow_seq` so clients can place
checkpoint-kept messages against the paged feed; the REST reads resolve the
store via `threads.py::_optional_run_event_store` (a feed-less deployment
still reads threads), and `services.py::normalize_input` strips the
server-owned key from client input (#4380). Mechanism and identity rule:
`backend/docs/CHECKPOINT_CHANNEL_MODES.md`.

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
once per tool `Command` for the terminal `run.delivery` event; a multi-message
command attributes paths through the one matching `ToolMessage` name without
duplicating counts, and ambiguous attribution stays counted but unattributed. `RunJournal` callbacks set
`run_inline=True`: they do only in-memory bookkeeping or schedule async writes,
and staying on the run's event-loop thread serializes parallel tool callbacks
before terminal delivery recording and flushing. Each worker creates a separate
journal per run before cancellable/fallible preflight work, so checkpoint
compatibility failures and cancellation while waiting for prior finalization
still emit a zero-delivery receipt. The worker flushes ordinary journal events,
idempotently persists the run-scoped receipt, and only then persists the staged
terminal run status. A receipt failure is retried on a short bounded schedule while the owner still holds the lease. The
worker derives delivery requirements from the run's workspace snapshots rather
than a client request option: every regular file created or modified under
`/mnt/user-data/outputs` is a candidate produced artifact. Internal
process-feedback files are not candidates: the snapshot capture excludes the
scanner's `EXCLUDED_DIR_NAMES` (including the default tool-output
externalization subdir) plus the configured `tool_output.storage_subdir`, so a
run that only externalized oversized tool outputs does not fail delivery. At
least one candidate must be covered by a path a tool result presented
(tagged `presented_files`; `runs/delivery.py`); a side-effect or unrelated
path does not satisfy delivery.
Receipts for such runs add `produced_paths`, `presented_paths`, `matched_paths`,
`verification`, `stage`, and `satisfied` to the Slice 1 fact fields. Missing a
matching presentation becomes a run error; a successful presentation is also
downgraded to error if its receipt cannot be durably verified. Runs without
changed outputs preserve ordinary chat behavior and the original receipt shape.
Orphan recovery first atomically claims an expired lease, then uses the same singleton write to
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
Moving journal construction ahead of preflight is receipt-only on early failure paths: a separate boundary flag preserves the previous completion-data semantics, so cancellation or checkpoint incompatibility while waiting for an older finalizing run persists no empty completion snapshot.
Multi-worker deployments therefore require `run_events.backend: db` for shared,
ordered delivery events; the startup gate rejects process-local memory and
JSONL event stores when `GATEWAY_WORKERS > 1`.

**RunManager / RunStore contract**: the rules a caller must not get wrong —
mechanism, recovery, and lease detail in
[`backend/docs/RUN_MANAGER_CONTRACT.md`](../../docs/RUN_MANAGER_CONTRACT.md).

- The exact-two profile accepts only a live `RunStore` advertising `lease_clock_authority=database_v1`; a configured PostgreSQL label alone is not evidence.
- `RunManager.get()` is async; direct callers must `await` it.
- The history batch helpers (`list_successful_regenerate_sources()`, `list_edit_regenerate_runs()`, `get_many_by_thread()`) default to `user_id=AUTO` and fail closed with no user context. A migration or admin caller that needs an unscoped read must pass `user_id=None` explicitly.
- Thread metadata status switches to `running` only after `RunManager.try_start()` succeeds, and every run-derived title/status write goes through `ThreadMetaStore.project_run()`. Missing legacy cursors and malformed terminal owner/lease state fail closed.
- `cancel()` returns a `CancelOutcome` enum, not a boolean; each variant implies a different safe response, including the 409 + `Retry-After` fallback.
- New thread operation kinds go through `RunStore.create_thread_operation_atomic()` and `RunManager.reserve_thread_operation()` — never another lock or metadata marker.
- Gateway checkpoint mutations outside run execution must use `services.reserve_checkpoint_write()`, so an existing run blocks the write and the reservation blocks new reject/interrupt/rollback runs across workers.
- Startup and orphan reconciliation must claim stale active rows with `RunStore.claim_for_takeover()`, never a plain `update_status()`.
- LangGraph run requests validate their supported subset before a run is created: unsupported stream modes and options are rejected 422 rather than dropped or silently replaced, and a placeholder must still accept the stock SDK's own default (`stream_resumable=False` reaches every request; rejecting it 422'd every IM channel run, #4466).
- A delta-mode run cannot fork: the worker rewrites a checkpoint resume into a linear head write before the graph starts.
- `POST /wait` must drain the bridge via `wait_for_run_completion()`, never `await record.task`, so it honours `on_disconnect` instead of returning a stale checkpoint (#3265).
- `HARTMESH_EXECUTION_RECOVERY_CLAIMS_ENABLED` defaults false and cannot bypass any gate; expired exact-two rows stay fail-closed rather than falling through to terminalization.
- Keep cross-component recovery orchestration in Gateway behind `RunManager.on_orphans_recovered`; do not introduce a harness-to-app dependency.
- The persisted envelope limits are hard: `event_type` 32 characters, `category` 16, and dynamic middleware tags 21 after the `middleware:` prefix. Consumers must not assume backend-identical nested output representations.
- Run event stream changes must keep producer code, `deerflow/constants.py`, `runtime/events/catalog.py`, `contracts/run_event_stream_contract.json`, `backend/docs/RUN_EVENT_STREAM.md`, and `tests/test_run_event_stream_contract.py` in sync.

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

# Harness guidance details

Detail moved out of `packages/harness/deerflow/AGENTS.md` and `packages/harness/deerflow/agents/AGENTS.md` so the harness guidance chains stay within the budgets in `scripts/check_agent_guidance.py`. These rules still apply; each guide links its sections.

## Request trace context

DeerFlow's request-level correlation id — the `X-Trace-Id` header and the `deerflow_trace_id` key. Not Langfuse's trace id, not `run_id`, not the short subagent `trace_id` log label.

**The ContextVar is the only source.** Every path that reaches a run binds one first; downstream treats the id as a plain `str`, no `if trace_id:` guards.

Entry points and binders: Gateway HTTP — `TraceMiddleware`; scheduled occurrence — `ScheduledTaskService._attempt_queued_run` → `launch_scheduled_thread_run`; MCP task notification — `launch_mcp_task_notification_run`; IM inbound — `ChannelManager._worker_loop`; embedded / TUI / CLI turn — `DeerFlowClient.stream()`.

Only the first is HTTP; the rest run outside ASGI, so the binding cannot live in middleware alone. Each scopes **one unit of work**, never a poller loop — a leaked binding on a reused worker task would tag later occurrences with the first id. `ensure_trace_context` inherits, keeping layered scheduled bindings and a manual trigger inside a Gateway request on one trace.

**Every other carrier is a derived output, never read back as an input.** `worker._bind_trace_id` stamps the runtime context and `config["metadata"]`; `services.start_run` stamps the run record; a caller-sent `deerflow_trace_id` (`body.metadata`, `body.config.context`) is replaced — honouring it would let the persisted run disagree with the header and the logs. `_SERVER_OWNED_RUNTIME_CONTEXT_KEYS` covers the embedded path and also rejects caller-supplied sandbox lease/scope identities, `redact_config_secrets` scrubs the kwargs echo (`runs.kwargs_json`), and `build_run_config` merges metadata onto a copy so the stamp cannot reach `body.config`. Callers pin an id with `X-Trace-Id`.

Accepted divergence: a crash-recovered scheduled launch reuses its run via the idempotency key without restamping — the record keeps the first attempt's id, the retry's logs a fresh one; restamping would rewrite an existing record. Not a bug. Thread metadata omits the key entirely — a thread spans many runs.

**Do not open-code fallback chains.** Two helpers own the resolution order:

- `resolve_trace_id(*carriers)` — first usable carrier, else ambient. For ids travelling as data in `runtime.context`; ContextVars do not survive a bare thread hop.
- `ensure_trace_context(trace_id)` — reuse the surrounding scope, else start a self-contained one. For boundary crossings (`SubagentExecutor._aexecute`, the memory `trace_context_manager` hook) and non-HTTP entry points; no argument mints a scoped id.

`request_trace_context` (HTTP) deliberately does **not** inherit: a crafted header must not fall back to the previous request's id.

`get_current_trace_id()` stays nullable only for the logging filter (pre-entry-point records render as `trace_id=-`); everything else uses `ensure_trace_id()`/`resolve_trace_id()`.

`DeerFlowClient.stream()` binds per `next()` step and around `inner.close()`, never across a `yield`: a sync generator shares the caller's context, so a scope held across yields would leak the id and break on cross-context GC finalization.

`logging.enhance.enabled` gates **log output only** (`trace_id` field presence and format) — not the id, the header, or the run metadata — so `TraceMiddleware` reads no `AppConfig`; `logging` stays restart-required (`STARTUP_ONLY_FIELDS["logging"]`). `X-Trace-Id` is in `CORS_EXPOSED_HEADERS` (not safelisted). Unhandled-exception 500s keep the header — `TraceMiddleware` sends its own plain 500 (CORS-opaque, see its docstring) before re-raising; mid-stream failures propagate unchanged.

Tests: the `tests/test_trace_*` and `tests/test_worker_trace_binding.py` suites, `test_gateway_services.py`, `test_run_metadata_secret_safety.py`, plus the Langfuse suites in `tracing/AGENTS.md`.

## Embedded client: Gateway equivalents and tests

**Gateway Equivalent Methods** (replaces Gateway API):

| Category | Methods | Return format |
|----------|---------|---------------|
| Models | `list_models()`, `get_model(name)` | `{"models": [...]}`, `{name, display_name, ...}` |
| MCP | `get_mcp_config()`, `update_mcp_config(servers)` | `{"mcp_servers": {...}}` |
| Skills | `list_skills()`, `get_skill(name)`, `update_skill(name, enabled)`, `install_skill(path)` | `{"skills": [...]}` |
| Goals | `get_goal(thread_id)`, `set_goal(thread_id, objective, max_continuations=8)`, `clear_goal(thread_id)` | `{"goal": {...}}` or `{"goal": None}` |
| Memory | `get_memory()`, `reload_memory()`, `get_memory_config()`, `get_memory_status()` | dict |
| Uploads | `upload_files(thread_id, files)`, `list_uploads(thread_id)`, `delete_upload(thread_id, filename)` | `{"success": true, "files": [...]}`, `{"files": [...], "count": N}` |
| Artifacts | `get_artifact(thread_id, path)` → `(bytes, mime_type)` | tuple |

**Gateway differences**: Upload takes local `Path`, not `UploadFile`, rejects directories before copying, and reuses one conversion worker inside an active event loop. Artifacts return `(bytes, mime_type)`, not HTTP Response. Gateway alone deletes `.deer-flow/threads/{thread_id}` after LangGraph thread deletion; the client has no equivalent. `update_mcp_config()` and `update_skill()` invalidate the cached agent.

**Tests**: `tests/test_client.py` is offline, including `TestGatewayConformance`.
`tests/test_client_live.py` requires root `config.yaml`, valid API credentials,
and opt-in via `make test-live` or `DEER_FLOW_RUN_LIVE_TESTS=1`. It calls real
APIs (possible costs) and may create local sandboxes, artifacts, and files.
Marked `live`, it is excluded from `make test` and skipped in default CI.

**Gateway Conformance Tests** (`TestGatewayConformance`): Parse every dict-returning client method's output through its Gateway Pydantic model so missing required fields raise `ValidationError` in CI. Covers: `ModelsListResponse`, `ModelResponse`, `SkillsListResponse`, `SkillResponse`, `SkillInstallResponse`, `McpConfigResponse`, `UploadResponse`, `MemoryConfigResponse`, `MemoryStatusResponse`.

## Agent runtime configuration

**Runtime Configuration** (via `config.configurable`):
- `thinking_enabled` - Enable model's extended thinking
- `model_name` - Select specific LLM model
- `is_plan_mode` - Enable TodoList middleware
- `subagent_enabled` - Enable task delegation tool
- `max_concurrent_subagents` - Per-response `task` call concurrency limit (clamped by `SubagentLimitMiddleware`)
- `max_total_subagents` - Per-run total delegation cap override (unset/`null` uses `subagents.max_total_per_run`, clamped to 1-50)
  Gateway and `DeerFlowClient.stream()` always provide the runtime `run_id`; custom
  graph integrations must do the same. If it is absent, enforcement deliberately
  counts the thread's full delegation ledger (fail-restrictive) and emits a warning.

## Direct subagent runtime

**Direct subagent runtime**: `create_deerflow_agent(..., subagent_runtime=runtime)`
accepts one caller-owned `deerflow.subagents.SubagentRuntime` per application capacity
boundary. Tools, limits, controller, durable submitter and feature policy share its
snapshot. For a task repository, `await runtime.start()` (or `async with`) before
building graphs and `stop()` at shutdown. The factory rejects a stopped worker;
already-bound task tools also fail unavailable, never falling back to a global
submitter. The factory creates no SQL infrastructure, renders no caller-owned
`system_prompt` and mounts no Gateway API/UI. Full middleware takeover cannot
combine with this runtime; direct callers/custom subagent middleware own prompt
call-policy wording.

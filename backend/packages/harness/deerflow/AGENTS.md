### Request Trace Context (`packages/harness/deerflow/trace_context.py`)

The `X-Trace-Id` / `deerflow_trace_id` correlation id has one source, its ContextVar:
each entry point binds it for one unit of work, every other carrier is a derived
output never read back, and `resolve_trace_id()` / `ensure_trace_context()` own all
fallback order. Read [the trace-context rules](../../../docs/harness-guidance-details.md#request-trace-context)
before changing an entry point, binder or carrier.

### Managed Lark CLI credentials (`integrations/lark_cli.py`)

Installed `lark-shared` guidance points to Capability Center > Plugins > Lark
(`/workspace/capabilities?tab=plugins&plugin=lark`). Guidance changes bump the
version marker; reinstalling the managed skill pack refreshes the stored text.

Managed skill-pack installation holds `_lark_install_lock` while preparing a
private tree; the final `staging_target.rename(target)` publishes the validated
skill bodies and manifest together, keeping earlier writes private. A write
failure leaves the previous pack unchanged and discards staging; replacement
moves the old target to a backup first and restores it on an ordinary
publication error. Atomic writes fsync only their temporary files, so
whole-tree durability after power loss is not guaranteed.
`_publish_bytes_atomically` removes a failed temporary through
`cleanup_failed_atomic_write`, so an unlink or warning-log failure cannot
replace the original publication error, including interruptions. Callers own
final file modes: runtime artifacts executable, skill bodies and manifests
read-only. Coverage: `tests/test_lark_cli_atomic_publish.py`.

App registration and direct app switching replace the per-user Lark credential
tree transactionally. Clear the old OAuth data before `lark-cli config init`:
on Linux it writes the new app secret into the file-backed keychain under the
data directory, so clearing afterward would leave `config.json` with a dangling
keychain reference. The snapshot still supplies the previous OAuth data for
logout and restores the complete old tree on any switch failure.

### DDG image search

Offload config/DDGS together. Only returned empties mean `No images found`;
the SDK sentinel can hide timeouts. See `tests/test_image_search_errors.py`.

### Browser Automation

Screenshot and session rules: [browser guidance](community/browser_automation/AGENTS.md).

### Embedded Client (`packages/harness/deerflow/client.py`)

`DeerFlowClient` embeds the harness without HTTP/FastAPI, sharing Gateway modules, config, data directories, and response schemas.

**Agent Conversation**:
- `chat(message, thread_id)` — synchronous; returns final AI text.
- The first `values` snapshot reconciles any suffix appended to already-streamed AI text by a model wrapper; unchanged text and non-prefix replacements are not re-emitted. Later snapshots retain the same append-only rule.
- `stream(message, thread_id)` — subscribes to LangGraph `stream_mode=["values", "messages", "custom"]` and yields `StreamEvent`:
  - `"values"` — state snapshot (title, messages, artifacts, summary_text). Always forward `summary_text` (current summary or `None`), including unchanged values/resets. Never re-emit AI text delivered via `messages`; serialized `ToolMessage` entries retain non-`None` native `artifact`
  - `"messages-tuple"` — current-turn AI text **deltas** by `id` and each tool call/result once; excludes resumed history and preserves result `artifact`
  - `"custom"` — forwarded from `StreamWriter`; DeerFlow-built-in custom events are dual-emitted through `deerflow.utils.custom_events`, so `astream_events(version="v2")` consumers also receive one `on_custom_event` with `name=payload["type"]` and the unchanged payload as `data`
  - `"end"` — turn `usage`: signed chunks + positive full-message remainders, deduped by id
- **Custom-event invariant** — use `emit_custom_event` / `aemit_custom_event`, never `StreamWriter` alone. Built-in payloads require a non-empty string `type`; typeless payloads stay writer-only, absent from `astream_events`. The writer runs first and is authoritative for Gateway/Web UI/embedded clients; best-effort callbacks must not break it. Async graph hooks must await the async helper, never dispatch synchronously on a running event loop.
- Lazy graph creation uses `create_agent()` + `build_middlewares()`.
- Cache graphs by storage `user_id` and the unordered set of named-agent `mcp_plugins`. `stream()` materializes `user_id` before worker/loop boundaries in every auth mode.
- Supports `checkpointer` parameter for state persistence across turns
- `reset_agent()` reloads AgentConfig and rebuilds the graph. Every run's metadata carries `mcp_plugins` for delegation, including cache hits.
- [Streaming design](../../../docs/STREAMING.md): Gateway/client parallel paths, LangGraph `stream_mode`, per-id deduplication, and regression tests

Gateway-equivalent client methods, their differences from the Gateway, and the
offline/live/conformance test split: [details](../../../docs/harness-guidance-details.md#embedded-client-gateway-equivalents-and-tests).

Upload capture, filename claims and publication: [guide](uploads/AGENTS.md).

### Tenki `sticky`

Env values stay strings; parse booleans before SDK calls (`bool("false")` is true).

### E2B Mount Uploads

Before changing creation uploads, policy projections, budgets, deadlines, or result
reporting, read [the E2B upload contract](community/e2b_sandbox/AGENTS.md#mount-uploads).

### Project Summary Worker Shutdown (`projects/summaries.py`)

`stop()` closes admission synchronously and shares one shielded `_stop_task`; `start()` reopens admission only after it completes and is cleared. Preserve queue-drop/sentinel/grace semantics. Regressions: `test_stop_drains_workers_before_propagating_repeated_cancellation` and `test_start_after_stop_restarts_worker_pool` in `backend/tests/test_project_summaries.py`.

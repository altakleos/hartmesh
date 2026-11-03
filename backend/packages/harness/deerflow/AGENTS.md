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

App registration and direct app switching replace the per-user Lark credential
tree transactionally. Clear the old OAuth data before running `lark-cli config
init`: on Linux that command writes the new app secret into the file-backed
keychain under the data directory, so clearing the directory afterward would
leave `config.json` with a dangling keychain reference. The transaction snapshot
still supplies the previous OAuth data for logout and restores the complete old
tree if any switch step fails.

### Browser Progress Screenshots (`community/browser_automation/`)

Hidden per-action browser progress frames use JPEG at quality 80 to keep their
storage and transfer cost bounded relative to lossless PNG. The explicit
`browser_screenshot` tool remains PNG because it creates a user-requested
artifact. New automatic capture entry points must reuse the shared progress
encoding definition in `tools.py` so the byte encoding and `.jpg` suffix cannot
drift.

### Embedded Client (`packages/harness/deerflow/client.py`)

`DeerFlowClient` embeds the harness without HTTP/FastAPI, sharing Gateway modules, config, data directories, and response schemas.

**Agent Conversation**:
- `chat(message, thread_id)` — synchronous; returns final AI text.
- `stream(message, thread_id)` — subscribes to LangGraph `stream_mode=["values", "messages", "custom"]` and yields `StreamEvent`:
  - `"values"` — state snapshot (title, messages, artifacts, summary_text). Always forward `summary_text` (current summary or `None`), including unchanged values/resets. Never re-emit AI text delivered via `messages`; serialized `ToolMessage` entries retain non-`None` native `artifact`
  - `"messages-tuple"` — current-turn AI text **deltas** by `id` and each tool call/result once; excludes resumed history and preserves result `artifact`
  - `"custom"` — forwarded from `StreamWriter`; DeerFlow-built-in custom events are dual-emitted through `deerflow.utils.custom_events`, so `astream_events(version="v2")` consumers also receive one `on_custom_event` with `name=payload["type"]` and the unchanged payload as `data`
  - `"end"` — current-turn cumulative `usage`, counted once per message id
- **Custom-event invariant** — use `emit_custom_event` / `aemit_custom_event`, never `StreamWriter` alone. Built-in payloads require a non-empty string `type`; typeless payloads stay writer-only, absent from `astream_events`. The writer runs first and is authoritative for Gateway/Web UI/embedded clients; best-effort callbacks must not break it. Async graph hooks must await the async helper, never dispatch synchronously on a running event loop.
- Lazy graph creation uses `create_agent()` + `build_middlewares()`.
- Cache graphs by storage `user_id` and the unordered set of named-agent `mcp_plugins`. `stream()` materializes `user_id` before worker/loop boundaries in every auth mode.
- Supports `checkpointer` parameter for state persistence across turns
- `reset_agent()` reloads AgentConfig and rebuilds the graph. Every run's metadata carries `mcp_plugins` for delegation, including cache hits.
- [Streaming design](../../../docs/STREAMING.md): Gateway/client parallel paths, LangGraph `stream_mode`, per-id deduplication, and regression tests

Gateway-equivalent client methods, their differences from the Gateway, and the
offline/live/conformance test split: [details](../../../docs/harness-guidance-details.md#embedded-client-gateway-equivalents-and-tests).

### Tenki `sticky`

Env values stay strings; parse booleans before SDK calls (`bool("false")` is true).

### E2B Mount Uploads

E2B uploads host mounts during sandbox creation using binary file objects.
Per-mount limits: 100 MiB/file, 512 MiB total, 2,000 files. The full creation
pass shares a 512 MiB / 2,000-file budget across skill projections and mounts.

The pass has a cooperative deadline controlled by
``mount_upload_deadline_seconds`` (default: 120 seconds). The provider checks it before
each mount, during directory preflight, and before each SDK write. The deadline
does not interrupt active filesystem or E2B SDK calls.

The provider checks mount limits before upload. It rechecks each opened file descriptor against its preflight size before SDK upload.

For policy-scoped turns, clearing the four managed remote skill categories and
uploading their prepared projection is one per-user/thread/skills-root critical
section, shared with acquire and release. The provider snapshots that canonical
root at startup and carries it through warm-pool identity and E2B metadata; a VM
from another root is never adopted. A second policy sync cannot reset the remote
tree until the first upload pass has completed.

An invalid mount does not block later mounts.

Each successful upload logs its source, destination, file count, byte count, and elapsed time.

A stopped pass logs its limit reason and elapsed time. It reports attempted and completed upload totals separately.

After creation, ``E2BSandbox.mount_upload_result`` holds a ``MountUploadResult``.
``result.truncated`` is true only for resource-limit stops (deadline, file count,
bytes), not logged mount failures (missing paths, SDK errors). A provider-level
map preserves creation results within the Gateway process; ``None`` on a
reclaimed sandbox means unavailable.


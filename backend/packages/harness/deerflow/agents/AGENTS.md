### Agent System

`AppConfig.lead_prompt_overlay` wraps the fully rendered lead system prompt with
literal operator prepend/append text. Do not template-format these extensions
or source them from run context. The graph and assembly descriptor must see the
same resulting prompt. Empty overlays preserve bytes.

**Lead Agent** (`packages/harness/deerflow/agents/lead_agent/agent.py`):
- `make_lead_agent(config: RunnableConfig)` is the published `langgraph.json`
  entry point; preserve its signature and bare-graph return type.
- Gateway `assemble_lead_agent(config, *, app_config=None)` returns
  `LeadAgentAssembly(graph, descriptor, effective_model, inventory_profile)`.
  `make_lead_agent` returns `.graph`; `runtime/runs/worker.py::_agent_graph` also
  accepts bare factories. `effective_model` stays available without observers.
  `assembly_descriptor.py::build_assembly_descriptor()` hashes effective model
  overrides, prompt, authorized tools and middleware order. The host-only
  `inventory_profile` binds actual normalized adapters without observer hashes;
  see [inventory](../../../../docs/RUN_OUTPUT_INVENTORY.md).
  The hashed skill catalog preserves `allowed_tools=None` (legacy allow-all),
  `()` (explicit allow-none for business tools), and non-empty allowlists as
  distinct policy states; allowlist ordering does not affect the fingerprint.
- Dynamic model selection via `create_chat_model()` with thinking/vision support
- Tools loaded via `get_available_tools()` - combines sandbox, built-in, MCP, community, and subagent tools
- Prompts: `apply_prompt_template()`; runtime scope: [guide](../agent_instances/AGENTS.md).
- Custom Agent `memory_enabled: false` disables memory reads, writes, tools, and compaction flushes while retaining date context; manual compaction trusts the state-producing checkpoint's agent binding, and global disable stays authoritative. Embedded clients cache the named policy by agent/user until `reset_agent()`; unreadable configs keep the legacy enabled default and log a warning.
- **Prompt trust**: framework authority uses the system channel; user/model-influenced text uses the sanitized `HumanMessage` data channel. Never interpolate untrusted values into system text—even escaped tags do not neutralize natural-language injection (PR #5090).
- Pass the same rendered prompt and middleware objects to the graph and assembly descriptor so observers describe the live graph, including Custom Agent `allowed_subagents` scope.

**ThreadState** (`packages/harness/deerflow/agents/thread_state.py`):
- Extends `AgentState` with: `sandbox`, `thread_data`, `title`, `artifacts`, `todos`, `uploaded_files`, `viewed_images`, `goal`, `promoted`, `delegations`, `skill_context`, `summary_text`
- Uses custom reducers: `merge_artifacts` (deduplicate), `merge_viewed_images` (merge/clear), `merge_goal` (preserve the active goal across ordinary state updates unless the goal writer replaces it), `merge_promoted` (catalog-hash-scoped deferred tool promotions), `merge_delegations` (append task delegation entries, same (run,id) wins, terminal status never downgraded, capped to the most recent entries), and `merge_skill_context` (dedupe active-skill references by path, keep the most recently read entries; entries store a name/path/description reference, not the SKILL.md body). `summary_text` is a LastValue channel updated by summarization and projected into model requests as durable context data instead of being stored as a `messages` item.
- Delta-mode `merge_message_writes` normalizes the current message state once,
  then folds normalized writes in order with message-ID position indexes and
  deferred tombstone compaction. It preserves public `add_messages` behavior,
  including duplicate IDs, replacement position, removal errors,
  `REMOVE_ALL_MESSAGES`, null-write errors, and missing-ID allocation order,
  without rescanning the accumulated state for every write. Keep this
  full-parity contract covered by differential tests: LangGraph's private
  `_messages_delta_reducer` is also linear, but intentionally omits some of
  those public `add_messages` semantics and cannot be substituted directly.

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

Work attempts use `autonomous` interaction mode, so `ask_clarification` is never
offered. `runtime_scope.py::ASSIGNMENT_QUESTION_RULE` directs `report_work` with
`action: request_input` before stopping with a question; prose alone is no blocker.

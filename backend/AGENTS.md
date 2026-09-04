# AGENTS.md

## Project Overview

DeerFlow is a LangGraph-based AI super agent system with a full-stack architecture. The backend provides a "super agent" with sandbox execution, persistent memory, subagent delegation, and extensible tool integration - all operating in per-thread isolated environments.

**Architecture**:
- **Gateway API** (port 8001): REST API plus embedded LangGraph-compatible agent runtime
- **Frontend** (port 3000): Next.js web interface
- **Nginx** (port 2026): Unified reverse proxy entry point
- **Provisioner** (port 8002, optional in Docker dev): Started only when sandbox is configured for provisioner/Kubernetes mode

**Runtime**:
- Tenant and credential evidence are server-owned at durable boundaries; see
  [tenant](docs/TENANT_IDENTITY.md) and
  [actor evidence](../docs/AUDITABLE_AUTOMATION_IDENTITIES.md).
- Honcho receives tenant context, never durable authority (see the memory guide).
- HartMesh durable launches from HTTP, Scheduled Tasks, signed native channels,
  and embedded services all enter the application-owned `InvocationRuntime`.
  Admission seals identity, Origin, trusted context, constraints, agent revision,
  extension generation, and effective skill material before worker/model work;
  keyed replay, lifecycle observation, and fenced cancellation use that same
  accepted record. Fail closed rather than falling back to a less durable path.
- Durable runs verify material, fence start, and bind
  `AssemblyEvidenceV1` before checkpoint/graph/model/tool work. Missing evidence
  fails `assembly_evidence_unavailable`, drift fails `agent_assembly_drift`, and
  stale owners cannot finalize.
- `sandbox/accepted_material.py` owns accepted material's bounded V1/V2 request,
  lease/evidence, capability profiles, and `AcceptedSandboxSession`. The session
  composes the existing run or batch-item fence, exposes no raw provider handle,
  and blocks calls/publication after observed loss. AIO keeps
  `rwx_verified_copy_v2` but declares atomic fencing, process-loss lookup, and
  exact-two false; see `docs/ACCEPTED_SANDBOX_EXECUTION.md`. OpenSandbox
  ordinary execution remains separate and accepted nonempty material fails
  closed: its pinned server/SDK surface has no ownership CAS or resolved-image
  digest readback; candidate trusted-setup surfaces remain live-unqualified. The committed Phase 0
  no-go evidence and upstream dependency are documented in
  `docs/OPENSANDBOX_ACCEPTED_MATERIAL_FEASIBILITY.md`; do not advertise or add a
  production profile until every live qualification scenario passes.
- After assembly bind, a trusted fenced sink commits `started` before policy or
  tool code and one terminal afterward; gaps are `indeterminate`. Stable IDs
  bind accepted anchors and graph dispatches; bodies omit raw arguments,
  results, and errors. A
  durable receipt records HartMesh's observation of a tool attempt. It
  does not guarantee an external side effect occurred exactly once or that the
  tool result was correct. Production requires `run_events.backend: db`.
- Batch acceptance is parent/tenant-bound and database-time fenced. Production
  stays disabled; see `docs/DURABLE_SUBAGENT_BATCHES.md`.
- Live journal, subagent, workspace, and delivery event writes are authority-
  bound to tenant/run/owner/epoch. Recovery uses a separate explicit
  administrative appender. Arbitrary runtime failures are converted once to
  bounded correlated V1 evidence for run rows, SSE, logs, `run.error`, and
  `llm.error`; `run.terminal.v1` adds terminal facts without changing opaque
  authorized `run.end` or conversation content.
- `packages/runtime-api/` is the stdlib-only portable contract; Gateway HTTP and
  in-process adapters must remain behaviorally identical. The synchronous
  `DeerFlowClient` is a legacy local graph client and does not enter `InvocationRuntime`;
  it makes no durability claim.
- Artifact provenance proves which extension bytes/configuration HartMesh
  admitted. Extensions still execute with Gateway privileges and must come from
  a trusted operator source. Provenance and enforcement remain host-owned within
  one startup-frozen process generation.
- `durable_two_gateway_v1` is an exact-two, evidence-gated boundary; it does not
  claim arbitrary scaling or IM/upgrade HA. See `docs/MULTI_GATEWAY_QUALIFICATION.md`.
- Provisioner-created Kubernetes sandbox Pods take their optional RuntimeClass
  from `SANDBOX_RUNTIME_CLASS` and apply the restricted container baseline
  (no privilege escalation, all capabilities dropped, RuntimeDefault seccomp)
  to the sandbox, init containers, and sidecars. The Pod-level context fixes
  `runAsUser`, `runAsGroup`, and `fsGroup` at 1000 and requires non-root, so
  every companion image must tolerate that identity. Sandbox mounts must stay
  away from the image entrypoint's ownership paths (`/home/gem`, its
  `Downloads`, `/var/log/gem`, `/var/lib/aio-sandbox`, `/opt/gem`, and
  `/opt/jupyter`). Sandbox volume mode is resolved once at provisioner startup:
  explicit `SANDBOX_VOLUME_MODE=pvc` requires both PVC names, explicit
  `hostpath` keeps the legacy layout, and inference accepts only both claim
  names set or both unset. Generated sandbox Pods use values-driven startup and
  liveness probes. The 200-second startup default leaves 66 seconds beyond a
  measured 134-second three-way concurrent gVisor start. The liveness default
  preserves the 40-second refused-connection budget while allowing up to 61
  seconds for a wedged listener whose three probes each consume the 10-second
  timeout.
- `make dev`, Docker dev, and production all run the agent runtime in Gateway via `RunManager` + `run_agent()` + `StreamBridge` (`packages/harness/deerflow/runtime/`). Nginx exposes that runtime at `/api/langgraph/*` and rewrites it to Gateway's native `/api/*` routers.
- Gateway streams `write_file` and `str_replace` argument deltas in bounded batches when clients also subscribe to `values`; messages-only consumers retain the original per-chunk contract, while `values` preserves the complete tool call.
- With `stream_subgraphs`, subgraph frames keep their namespace in the SSE event name (`values|<ns>`, LangGraph Platform style) instead of impersonating root frames — a delegated subagent inherits the parent checkpoint namespace, so publishing its `values` snapshot as bare `values` replaces the whole thread view in SDK clients (#4399). Root-only consumers (file-tool chunk batcher, subagent event persistence, LLM error-fallback detection) ignore namespaced frames. The web frontend does not request subgraph streaming; subtask progress rides root-namespace `task_*` custom events.
- Background subagent identity is deliberately split: the provider `tool_call_id` remains the correlation key for `ToolMessage`, `task_*` SSE events, persisted lifecycle events, frontend cards, and the public `ExtensionData.scope_id` contract (stored as `SubagentResult.external_task_id`), while `SubagentExecutor.execute_async()` generates a full server-side `execution_id` for `SubagentResult.task_id`, the process-wide registry, polling, cancellation, timeout handling, and cleanup. Provider IDs are not globally unique across parent runs, so they must never become registry ownership keys; scheduler closures retain their own `SubagentResult` rather than resolving ownership again through the mutable registry. Terminal subagent token usage travels in the current run's `ToolMessage.additional_kwargs` and is attributed from message state, never through a process-global provider-ID cache.
- Scheduled-task executions must reuse that same Gateway run lifecycle. The scheduler may decide *when* work runs, but it must dispatch through the existing run path rather than introducing a parallel execution stack. Scheduled launches pass `scheduler.recursion_limit` (default 1000, matching the web UI's `recursion_limit: 1000`, clamped by `max_recursion_limit`) via `launch_scheduled_thread_run`; the value is read from `get_app_config()` at dispatch.
- The background scheduler is single-instance by default. `scheduler.multi_instance=true` opts into lease-aware recovery across Gateway instances and requires shared Postgres, `run_ownership.heartbeat_enabled=true`, and `run_events.backend=db`; otherwise startup rejects the configuration. Live scheduled runs are preserved when a peer starts; expired launch claims return to the durable queue, expired `terminalize_v1` run leases are atomically terminalized, exact-two rows stay fail-closed while execution takeover is unavailable, stale launch writes are fenced by lease ownership, and the Postgres advisory-locked budget makes `max_concurrent_runs` a shared global cap for `launching`/`running` rows.
- Long-running MCP work uses a separate durable task runtime; submission persists the remote handle before returning a local ID. `McpTaskService` leases due rows and stores normalized snapshots in `mcp_tasks`. Shutdown closes submit admission, cancels/awaits admitted submits, and drains late remote compensation; start reopens admission. Expired leases recover after restart; stale or post-cancel results are discarded. Cancellation fences polling, batch failures stay task-local, and idempotent Agent runs deliver terminal/input-required events. The database remains authoritative; `ThreadState` gets only a bounded projection, and task-toolset config changes require restart. New tasks persist immutable lineage beside the handle; Agent submissions require accepted run facts and the active `started` receipt, while standalone API submissions ignore client provenance. Recovery revalidates tenant and credential commitments, links require independent authorization, and parent/task cancellation remain independent. MCP task lineage records who submitted a task and how its completion was correlated. It does not guarantee exactly-once execution by the remote MCP server.
- MCP replay equality is separate from public lineage. `replay_commitment.py` HMACs the canonical request with the startup-frozen dedicated keyring; SQL stores only version/key ID/HMAC, and startup fails if enabled without keys. Exact-two topology binds a non-secret confirmation of all key bytes/IDs and the active ID, so any rotation uses the quiesced restart procedure. Missing historical keys fail replay closed without disabling durable polling/cancellation. Parent evidence requires independent parent-run authorization.
- MCP notification failures use a consecutive counter separate from the idempotency-key `dispatch_attempt`, capped exponential backoff, latest-event rebuilding before a run launches, and a five-attempt budget before `dead_letter`. A permanently missing/mismatched target thread is dead-lettered immediately instead of being recreated or reclaimed. HTTP and Agent cancellation requests return after the durable cancel fence; the first request separately persists a pseudonymous actor and fixed source reason, while the background loop alone owns the potentially slow remote call and retry schedule. The HTTP cancel endpoint rejects requests with 503 when the loop is not running (`mcp_tasks_available` false, e.g. `mcp_tasks.enabled=false` with SQL persistence), so a cancellation is never acknowledged without a worker to perform it. The bounded notification error/count/status join poll and cancellation diagnostics in the task detail API and expanded card.
- Retrieval evidence: `docs/EVIDENCE_BEARING_RETRIEVAL.md`.
- `uq_scheduled_task_run_active` permits one `queued`/`launching`/`running` occurrence per task. Queued work is durable and consumes no concurrency; only a short lease-fenced `launching` row may invoke the normal Gateway path, using a stable admission key so recovery reuses the run. Thread conflicts requeue; other launch errors fail. Repeated triggers coalesce and same-thread FIFO includes every active state. Definition mutations lock the parent first: pause/delete interrupt queued work but reject launched/running work, while PATCH/resume reject all active states. Recovery locks task/run pairs in deterministic order and reconstructs live fields before releasing a claim. Launch, failure, and timeout update parent plus occurrence atomically; timeout also advances scheduled cadence. Repository boundaries coerce serialized timestamps before SQL binding.
- `packages/harness/deerflow/tool_plane/` owns canonical secret-safe base/user revisions, validation, SQL generations/attestations, locked projection, bootstrap, drift, and reconciliation. Default-enabled governance makes it the only skill/MCP writer; legacy routes require opt-out, and exact-two mounts only governed reads. Admission pins its coherent effective revision and captured skill/MCP material. Do not bypass the service from routers, clients, tools, or UI; see `docs/GOVERNED_TOOL_PLANE.md` and `test_tool_plane_*`.

**Backend map**: `app/` owns Gateway and channel application code;
`packages/harness/deerflow/` owns the agent framework; `packages/extension-api/`
and `packages/runtime-api/` are public contracts; `tests/` mirrors production
boundaries. Follow the nearest scoped `AGENTS.md` for subsystem detail. The root
guide owns the full repository map.

## Important Development Guidelines

### Documentation Update Policy
**CRITICAL: Always update README.md and AGENTS.md after every code change**

When making code changes, you MUST update the relevant documentation:
- Update `README.md` for user-facing changes (features, setup, usage instructions)
- Update `AGENTS.md` for development changes (architecture, commands, workflows, internal systems). `CLAUDE.md` imports it via `@AGENTS.md`, so editing `AGENTS.md` updates both.
- Keep documentation synchronized with the codebase at all times
- Ensure accuracy and timeliness of all documentation

### Backend Benchmarks

`scripts/benchmark/` contains standalone, reproducible measurements and
evaluations of production backend behavior. A benchmark may import the
production function it measures, but it must not duplicate or introduce an
alternative runtime implementation.

- Pin every external dataset by immutable revision and SHA-256. Callers provide
  the local dataset path; evaluation commands must not silently download data.
- Never commit upstream dataset text, credentials, complete provider requests,
  or response headers. Committed manifests may contain stable IDs and source
  locators. Synthetic cases must identify themselves as synthetic.
- Read provider credentials and endpoints from named environment variables.
  Version model IDs, inference parameters, prompts, retry rules, clocks, and
  random seeds in the evaluation config.
- Public raw results may contain case IDs, policy decisions, model hypotheses,
  grades, and non-secret response metadata. Keep dataset questions, reference
  answers, memory content, and full provider payloads in ignored local run
  directories.
- Use fixed clocks and deterministic ordering for offline selection. Results
  must record the config, manifest, prompt, dataset, and git revisions used.

`scripts/benchmark/deermem_eviction/` evaluates the production
`select_facts_for_capacity()` implementation used by DeerMem. It compares only
the historical `confidence` policy and PR #4789's opt-in `hybrid-v1`; do not add
another eviction strategy to this evaluation. Run its offline checks from
`backend/`:

```bash
PYTHONPATH=. uv run python -m scripts.benchmark.deermem_eviction validate-contracts
PYTHONPATH=. uv run python -m scripts.benchmark.deermem_eviction validate --dataset "$LONGMEMEVAL_ORACLE_PATH"
PYTHONPATH=. uv run python -m scripts.benchmark.deermem_eviction run-policy \
  --dataset "$LONGMEMEVAL_ORACLE_PATH" \
  --output-dir /tmp/deermem-eviction-policy-run
PYTHONPATH=. uv run pytest tests/test_bench_deermem_eviction_*.py -q
```

The offline test suite must not require network access, provider credentials,
or the LongMemEval dataset. Small LongMemEval-shaped fixtures must be synthetic
and generated by tests.

## Commands

**Root directory** (for full application):
```bash
make check      # Check system requirements
make install    # Install all dependencies (frontend + backend)
make extension-install SOURCE=...  # Install and enable a trusted Python extension
make extension-list                # List configured Python extensions
make extension-enable NAME=...     # Enable an installed extension
make extension-disable NAME=...    # Disable an extension without uninstalling it
make extension-remove NAME=...     # Remove a managed extension
make detect-thread-boundaries  # Inventory backend executor/thread/event-loop boundaries
make dev        # Start all services (Gateway + Frontend + Nginx), with config.yaml preflight
make start      # Start production services locally
make stop       # Stop all services
```

**Backend directory** (for backend development only):
```bash
make install            # Install backend dependencies
make dev                # Run Gateway API with runtime-safe reload (port 8001)
make gateway            # Run Gateway API only (port 8001)
make test               # Run offline backend tests with the lock-pinned OpenSandbox probe SDK (excludes live external-API tests)
make test-live          # Explicitly run live DeerFlowClient tests with real APIs
make test-blocking-io   # Run strict Blockbuster runtime gate on tests/blocking_io/
make test-shard SPLITS=4 GROUP=2  # Run one duration-aware test shard
make test-shard-durations  # Refresh the duration baseline
make lint               # Lint with ruff
make format             # Format code with ruff
make migrate-rev MSG="..."  # Autogenerate a new alembic revision (see Schema Migrations section)
```

The backend `make dev` target pre-creates and excludes `DEER_FLOW_HOME`
(default: `backend/.deer-flow`) and `backend/sandbox` from Uvicorn's reload
watcher. Do not replace it with a bare `uvicorn --reload`: agent tasks write
Python and other runtime files below `DEER_FLOW_HOME`, which would otherwise
restart the Gateway during an active run.

More specific `AGENTS.md` files in backend code directories contain the subsystem sections split from this file. Follow the nearest file in the directory tree.

## Architecture

### Harness / App Split

The backend is split into two layers with a strict dependency direction:

- **Harness** (`packages/harness/deerflow/`): Publishable agent framework package (`deerflow-harness`). Import prefix: `deerflow.*`. Contains agent orchestration, tools, sandbox, models, MCP, skills, config — everything needed to build and run agents.
- **App** (`app/`): Unpublished application code. Import prefix: `app.*`. Contains the FastAPI Gateway API and IM channel integrations (Feishu, Slack, Telegram, DingTalk).

**Dependency rule**: App imports deerflow, but deerflow never imports app. This boundary is enforced by `tests/test_harness_boundary.py` which runs in CI.

**Import conventions**:
```python
# Harness internal
from deerflow.agents import make_lead_agent
from deerflow.models import create_chat_model

# App internal
from app.gateway.app import app
from app.channels.service import start_channel_service

# App → Harness (allowed)
from deerflow.config import get_app_config

# Harness → App (FORBIDDEN — enforced by test_harness_boundary.py)
# from app.gateway.routers.uploads import ...  # ← will fail CI
```

Package import hygiene: the `deerflow.agents` and `deerflow.subagents` package
roots expose heavyweight graph/executor entrypoints lazily. The
`deerflow.agents:make_lead_agent` LangGraph Server entrypoint is a concrete thin
module-level function because the server resolves graph factories directly from
the module dictionary; the wrapper keeps the lead-agent and skill-cache imports
inside the function so importing the package remains lightweight. Internal
modules that only need lightweight types, config, or registries should import
the concrete submodule instead of adding eager package-root imports that pull in
the tool graph or subagent executor during state/schema imports.

`ThreadMetaStore.search()` keeps JSON filter semantics identical across memory,
SQLite, and PostgreSQL: missing differs from null, bool differs from int, and
float filters accept integer or real JSON numbers through `json_value_matches`.

## Development Workflow

### Test-Driven Development (TDD) — MANDATORY

**Every new feature or bug fix MUST be accompanied by unit tests. No exceptions.**

- Write tests in `backend/tests/` following the existing naming convention `test_<feature>.py`
- Run the offline and blocking-I/O suites before and after your change: `make test` and `make test-blocking-io`
- `make test` explicitly selects the lock-pinned `opensandbox` extra because the
  offline Phase 0 feasibility probe verifies the installed SDK bytes. The
  provider remains an optional production/harness dependency.
- Tests must pass before a feature is considered complete
- For lightweight config/utility modules, prefer pure unit tests with no external dependencies
- If a module causes circular import issues in tests, add a `sys.modules` mock in `tests/conftest.py` (see existing example for `deerflow.subagents.executor`)

```bash
# Run default offline tests
make test

# Run strict blocking-I/O tests
make test-blocking-io

# Explicit live integration tests (requires config.yaml and credentials;
# calls real APIs and may create local side effects)
make test-live

# Run a specific test file
PYTHONPATH=. uv run pytest tests/test_<feature>.py -v
```

Direct pytest collection or execution of `tests/test_client_live.py` remains
skipped unless `DEER_FLOW_RUN_LIVE_TESTS=1` is set. Do not add that opt-in to
default CI workflows.

Jina request-failure logging tests set a dummy API key so the separate once-per-process
missing-key warning cannot make assertions depend on test order or shard placement.
Missing-key behavior has its own tests in `tests/test_jina_client.py`.

### Running the Full Application

From the **project root** directory:
```bash
make dev
```

This starts all services and makes the application available at `http://localhost:2026`.

**All startup modes:**

| | **Local Foreground** | **Local Daemon** | **Docker Dev** | **Docker Prod** |
|---|---|---|---|---|
| **Dev** | `./scripts/serve.sh --dev`<br/>`make dev` | `./scripts/serve.sh --dev --daemon`<br/>`make dev-daemon` | `./scripts/docker.sh start`<br/>`make docker-start` | — |
| **Prod** | `./scripts/serve.sh --prod`<br/>`make start` | `./scripts/serve.sh --prod --daemon`<br/>`make start-daemon` | — | `./scripts/deploy.sh`<br/>`make up` |

| Action | Local | Docker Dev | Docker Prod |
|---|---|---|---|
| **Stop** | `./scripts/serve.sh --stop`<br/>`make stop` | `./scripts/docker.sh stop`<br/>`make docker-stop` | `./scripts/deploy.sh down`<br/>`make down` |
| **Restart** | `./scripts/serve.sh --restart [flags]` | `./scripts/docker.sh restart` | — |

**Nginx routing**:
- `/api/langgraph/*` → Gateway embedded runtime (8001), rewritten to `/api/*`
- `/api/*` (other) → Gateway API (8001)
- `/` (non-API) → Frontend (3000)

### Running Backend Services Separately

From the **backend** directory:

```bash
# Gateway API
make gateway
```

Direct access (without nginx):
- Gateway: `http://localhost:8001`

### Frontend Configuration

The frontend uses environment variables to connect to backend services:
- `NEXT_PUBLIC_LANGGRAPH_BASE_URL` - Defaults to `/api/langgraph` (through nginx)
- `NEXT_PUBLIC_BACKEND_BASE_URL` - Defaults to empty string (through nginx)

When using `make dev` from root, the frontend automatically connects through nginx.

Subsystem feature contracts live in the nearest scoped `AGENTS.md` and the
linked files under `docs/`; keep this backend guide as the orientation layer.

## Code Style

- Uses `ruff` for linting and formatting
- Line length: 240 characters
- Python 3.12+ with type hints
- Double quotes, space indentation

## Documentation

See `docs/` directory for detailed documentation:
- [CONFIGURATION.md](docs/CONFIGURATION.md) - Configuration options
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Architecture details
- [API.md](docs/API.md) - API reference
- [SETUP.md](docs/SETUP.md) - Setup guide
- [FILE_UPLOAD.md](docs/FILE_UPLOAD.md) - File upload feature
- [PATH_EXAMPLES.md](docs/PATH_EXAMPLES.md) - Path types and usage
- [summarization.md](docs/summarization.md) - Context summarization
- [plan_mode_usage.md](docs/plan_mode_usage.md) - Plan mode with TodoList

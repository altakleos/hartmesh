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
  [actor evidence](../docs/AUDITABLE_AUTOMATION_IDENTITIES.md). Honcho receives
  tenant context, never durable authority (see the memory guide).
- Every durable launch (HTTP, Scheduled Tasks, signed native channels, embedded
  services) enters the application-owned `InvocationRuntime`, whose admission
  seals identity, Origin, trusted context, constraints, agent revision,
  extension generation, and effective skill material before worker/model work;
  keyed replay, lifecycle observation, and fenced cancellation use that accepted
  record. Durable runs then verify material, fence start, and bind
  `AssemblyEvidenceV1` before checkpoint/graph/model/tool work (missing evidence
  fails `assembly_evidence_unavailable`, drift fails `agent_assembly_drift`,
  stale owners cannot finalize). Fail closed rather than falling back to a less
  durable path; see the runtime guide and `docs/INVOCATION_RUNTIME.md`.
- Every sandbox is a session of a declared Kind, ordinary or accepted,
  dispatched by the session provider from `sandbox/session.py`; see
  `docs/ACCEPTED_SANDBOX_EXECUTION.md`.
  `sandbox/accepted_material.py` owns accepted material's V1/V2 request,
  lease/evidence, capability profiles, and `AcceptedSandboxSession`, which
  composes the existing run or batch-item fence, exposes no raw provider handle,
  and blocks calls/publication after observed loss. Provider extras are
  `sandbox/capabilities.py` contracts negotiated through
  `SandboxProvider.capability`; the required surface is acquire/get/release. AIO
  keeps `rwx_verified_copy_v2` but declares atomic fencing, process-loss lookup,
  and exact-two false. OpenSandbox ordinary execution stays separate; its
  accepted nonempty material fails closed (no ownership CAS or resolved-image
  digest readback; candidate surfaces remain live-unqualified; Phase 0 no-go
  evidence in `docs/OPENSANDBOX_ACCEPTED_MATERIAL_FEASIBILITY.md`). No
  production profile until every live qualification scenario passes.
- After assembly bind, a trusted fenced sink commits `started` before policy or
  tool code and one terminal afterward; gaps are `indeterminate`. Receipts bind
  accepted anchors and graph dispatches, omit raw arguments/results/errors, and
  record HartMesh's observation of a tool attempt, never an exactly-once
  external effect or a correct result. Production requires
  `run_events.backend: db`.
- Batch acceptance is parent/tenant-bound and database-time fenced; production
  stays disabled (`docs/DURABLE_SUBAGENT_BATCHES.md`). Terminal evidence export
  uses a runtime snapshot fence and Gateway exact-byte archive; required
  missing, pruned, or legacy material fails closed and bundles are unsigned
  (`../docs/RUN_EVIDENCE_BUNDLES.md`). Admission seals `ExecutionBudgetV1` and
  the run-bound `EgressAllowanceV1` the accepted Material renders; the
  worker advances `ExecutionPolicyStateV1` with an owner/epoch/lease-fenced CAS,
  fails closed on missing keys, drift, or a stale writer, and only safe
  projections leave the runtime (`../docs/EXECUTION_POLICY_AND_EVIDENCE_UI.md`).
- Live journal, subagent, workspace, and delivery event writes are
  authority-bound to tenant/run/owner/epoch; recovery uses a separate explicit
  administrative appender. Runtime failures become bounded correlated V1
  evidence once (run rows, SSE, logs, `run.error`, `llm.error`);
  `run.terminal.v1` adds terminal facts without changing opaque authorized
  `run.end` or conversation content.
- `packages/runtime-api/` is the stdlib-only portable contract; Gateway HTTP and
  in-process adapters must stay behaviorally identical. The synchronous
  `DeerFlowClient` is a legacy local graph client and does not enter `InvocationRuntime`;
  it makes no durability claim.
- Artifact provenance proves which extension bytes/configuration HartMesh
  admitted within one startup-frozen process generation; extensions still run
  with Gateway privileges and must come from a trusted operator source.
- `durable_two_gateway_v1` is an exact-two, evidence-gated boundary, not
  arbitrary scaling or IM/upgrade HA (`docs/MULTI_GATEWAY_QUALIFICATION.md`).
- Provisioner-created Kubernetes sandbox Pods take their optional RuntimeClass
  from `SANDBOX_RUNTIME_CLASS`, apply the restricted baseline (no privilege
  escalation, all capabilities dropped, RuntimeDefault seccomp) to sandbox,
  init, and sidecar containers, and fix `runAsUser`/`runAsGroup`/`fsGroup` at
  1000 non-root, so every companion image must tolerate that identity. Mounts
  must avoid the entrypoint's ownership paths (`/home/gem` and its `Downloads`,
  `/var/log/gem`, `/var/lib/aio-sandbox`, `/opt/gem`, `/opt/jupyter`).
  `SANDBOX_VOLUME_MODE` is resolved once at provisioner startup: explicit `pvc`
  requires both PVC names, explicit `hostpath` keeps the legacy layout, and
  inference accepts only both claim names set or both unset. Startup and
  liveness probes are values-driven: the 200-second startup default leaves 66
  seconds beyond a measured 134-second three-way concurrent gVisor start, and
  the liveness default keeps the 40-second refused-connection budget while
  allowing 61 seconds for a wedged listener (three 10-second probes).
- `make dev`, Docker dev, and production all run the agent runtime in Gateway
  via `RunManager` + `run_agent()` + `StreamBridge`
  (`packages/harness/deerflow/runtime/`); Nginx exposes it at
  `/api/langgraph/*`, rewritten to native `/api/*`.
- Gateway batches `write_file`/`str_replace` argument deltas for multi-mode
  `messages-tuple` consumers (single-mode consumers keep the per-chunk
  contract); non-message frames flush pending batches, and `values` stays an
  optional snapshot, not a batching prerequisite. With `stream_subgraphs`,
  subgraph frames keep their namespace in the SSE event name (`values|<ns>`)
  instead of impersonating root frames, because a delegated subagent's bare
  `values` would replace the whole thread view (#4399); root-only consumers
  ignore namespaced frames, and the web frontend rides root-namespace `task_*`
  events instead of subgraph streaming.
- Background subagent identity is split: the provider `tool_call_id` stays the
  correlation key for `ToolMessage`, `task_*` events, persisted lifecycle
  events, frontend cards, and `ExtensionData.scope_id`
  (`SubagentResult.external_task_id`), while `SubagentExecutor.execute_async()`
  mints a server-side `execution_id` for `SubagentResult.task_id`, the registry,
  polling, cancellation, timeouts, and cleanup. Provider IDs are not unique
  across parent runs and must never become registry ownership keys; scheduler
  closures keep their own `SubagentResult`. Terminal subagent token usage
  travels in the run's `ToolMessage.additional_kwargs`, never a process-global
  provider-ID cache.
- Scheduled executions dispatch through the same Gateway run path
  (`launch_scheduled_thread_run`; `scheduler.recursion_limit` default 1000,
  clamped by `max_recursion_limit`, read from `get_app_config()` at dispatch);
  the scheduler only decides when. It is single-instance by default:
  `scheduler.multi_instance=true` requires shared Postgres,
  `run_ownership.heartbeat_enabled=true`, and `run_events.backend=db`
  (startup rejects anything else), preserves live runs when a peer starts,
  returns expired launch claims to the queue, terminalizes expired
  `terminalize_v1` leases atomically, keeps exact-two rows fail-closed, fences
  stale launch writes by lease ownership, and makes `max_concurrent_runs` a
  shared advisory-locked cap over `launching`/`running` rows.
  `uq_scheduled_task_run_active` allows one `queued`/`launching`/`running`
  occurrence per task: queued work is durable and consumes no concurrency,
  only a short lease-fenced `launching` row invokes the Gateway path under a
  stable admission key so recovery reuses the run, thread conflicts requeue,
  other launch errors fail, repeated triggers coalesce, same-thread FIFO spans
  every active state, definition mutations lock the parent first (pause/delete
  interrupt queued work but reject launched/running work; PATCH/resume reject
  all active states), recovery locks task/run pairs in deterministic order and
  reconstructs live fields before releasing a claim, launch/failure/timeout
  update parent and occurrence atomically (timeout also advances cadence), and
  repositories coerce serialized timestamps before SQL binding.
- Durable MCP tasks (`McpTaskService`, `mcp_tasks`; details in the MCP guide):
  submission persists the remote handle before returning a local ID, the
  database stays authoritative with only a bounded `ThreadState` projection,
  Agent submissions require accepted run facts and the active `started` receipt
  while standalone API submissions ignore client provenance, replay equality
  uses the startup-frozen dedicated HMAC keyring (`replay_commitment.py`; any
  rotation is a quiesced restart, missing historical keys fail replay closed
  without disabling polling/cancellation), public lineage stays redacted and
  parent evidence needs independent parent-run authorization, notification
  failures back off to `dead_letter` after five attempts, cancellation is
  acknowledged only after the durable fence and only while the loop runs, and
  nothing guarantees exactly-once remote execution.
- Retrieval evidence: `docs/EVIDENCE_BEARING_RETRIEVAL.md`.
- `packages/harness/deerflow/tool_plane/` is the only skill/MCP writer under
  default governance (secret-safe base/user revisions, validation, SQL
  generations/attestations, locked projection, bootstrap, drift,
  reconciliation); legacy routes require opt-out, exact-two mounts only
  governed reads, and admission pins the coherent effective revision with its
  captured skill/MCP material. Never bypass it from routers, clients, tools, or
  UI; see `docs/GOVERNED_TOOL_PLANE.md` and `test_tool_plane_*`.

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

`scripts/benchmark/concurrency/run_concurrency_bench.py` (run from `backend/`)
measures multi-process `users`-table contention for SQLite vs Postgres with N OS
processes on a READY/GO barrier; it seeds a disposable per-run Postgres schema
(`--pg-url`, never `public`), exits non-zero on any crash or `errors > 0`, and
is covered by `tests/test_bench_concurrency.py` / `tests/test_bench_worker.py`.

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

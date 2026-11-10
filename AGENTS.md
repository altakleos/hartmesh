# AGENTS.md

Coding-agent source of truth; `CLAUDE.md` imports it. Module guides own depth:

- **[backend/AGENTS.md](backend/AGENTS.md)** — backend depth: harness/app split, agent &
  middleware chain, sandbox, MCP, skills, memory, IM channels, persistence/migrations,
  config system, test layout.
- **[frontend-hm/AGENTS.md](frontend-hm/AGENTS.md)** — frontend depth: Next.js App Router layout,
  thread/streaming data flow, code style, commands.

## What is DeerFlow

HartMesh's product UI lives in `frontend-hm/`. `frontend/` is an exact upstream
snapshot pinned by `.github/upstream-frontend.json`; never edit it for HartMesh,
including version bumps, formatting, or guidance. `make check-frontend-isolation`
checks the snapshot and direct source references. Port upstream UI fixes into
`frontend-hm/` deliberately; see [the isolation and scope guide](docs/FRONTEND_ISOLATION.md).

DeerFlow combines LangGraph and Next.js with thread-isolated sandbox execution,
memory, subagents and extensible tools. IM channels connect through the Gateway.

## Service Topology

A single `make dev` / Docker stack runs four cooperating services:

| Service | Port | Role |
| --- | --- | --- |
| **Nginx** | `2026` | Unified reverse-proxy entry point — open this in the browser |
| **Gateway API** | `8001` | FastAPI REST API + embedded LangGraph-compatible agent runtime |
| **Frontend** | `3000` | Next.js web interface |
| **Provisioner** | `8002` | Optional — only when sandbox is configured for provisioner/K8s mode |

Nginx serves the frontend and proxies `/api/*` to Gateway, rewriting
`/api/langgraph/*` to native routes. See [backend/AGENTS.md](backend/AGENTS.md).
It compresses HTML and configured text assets. SSE, fonts, images, audio, and
video stay uncompressed.

Put the `/api/threads/{thread_id}/browser/stream` WebSocket regex before the
generic threads regex in Docker, local, and Helm configs.
`backend/tests/test_nginx_browser_websocket.py` pins upgrade headers and the
600s read timeout. Helm uses nginx defaults for Gateway connect/send timeouts.
Keep local temp paths checkout-relative and aligned with `serve.sh` and `nginx.sh`.

Both Compose files publish nginx at `"${BIND_HOST:-127.0.0.1}:${PORT:-2026}:2026"`.
Bare `"${PORT}:2026"` binds `0.0.0.0`. Require explicit bind addresses for new
published ports. `backend/tests/test_compose_default_bind_host.py` pins both files.
Root `PORT` controls Docker ingress only. Local Next.js stays on `3000` so
`.env` cannot redirect the dev port wait. Nginx's IPv4/IPv6 `default_server`
and Gateway's `0.0.0.0:8001` stay container-internal. Only published nginx is external.

Provisioner PVC mode declares `USERDATA_PVC_NAME` once as `user-data`; extra and
Lark broker credential mounts reuse it with their existing subPaths and read-only
flags. Keep credential mounts broker-only and keep distinct volumes in hostPath mode.

## Repository Map

```
deer-flow/
├── Makefile  # Root orchestration: drives the full stack (dev/start/stop, docker, setup)
├── config.example.yaml  # Template → copy to config.yaml (gitignored) at repo root
├── extensions_config.example.json  # Template → copy to extensions_config.json (gitignored): MCP servers + skills
├── backend/  # Python backend — see backend/AGENTS.md
│   ├── Makefile  # Per-module backend commands (dev, gateway, test, lint, migrate-rev)
│   ├── extensions/sources/  # Deployable snapshots of locally installed Python extensions
│   ├── packages/extension-api/  # deerflow-extension-api package (import: deerflow_extension_api.*) — public extension contract
│   ├── packages/harness/  # deerflow-harness package (import: deerflow.*) — agent framework
│   └── app/  # FastAPI Gateway + IM channels (import: app.*)
├── frontend-hm/  # HartMesh Next.js app — see frontend-hm/AGENTS.md
├── frontend/  # Pinned upstream reference; no HartMesh edits
├── deploy/  # Compose distribution and upstream Helm chart
├── docker/  # docker-compose files, nginx config, provisioner
├── skills/  # Agent skills: public/ (committed), custom/ (gitignored)
│  # Managed integration skill packs are global at .deer-flow/integrations/skills/{provider}/
│  # Integration credentials and enabled state remain per-user
├── contracts/  # Cross-component JSON contracts (e.g. subagent status, skill review)
├── examples/  # Extension examples: deerflow-extension-{example,bookmarks,agent-teams}
├── scripts/  # Root orchestration scripts invoked by the Makefile (check, configure, doctor, support_bundle, serve, nginx, docker, deploy, setup_wizard)
├── tests/  # Root-level tests (currently tests/skills/ — public skill tests)
└── docs/  # Cross-cutting docs, plans, and design notes
```

Third-party extensions come only from the operator-controlled `plugins:` list in
`config.yaml`, never `extensions_config.json`; they run trusted code with Gateway
privileges and need a restart. See [the extensions guide](backend/packages/harness/deerflow/extensions/AGENTS.md)
and [details](docs/repository-guidance-details.md#third-party-extensions).

**Extension-first evaluation** — before implementing a feature by editing core code,
evaluate whether it can ship as a packaged extension through the contribution points
above. Stay in core for bug fixes and for changes to the agent loop, memory, context
compaction, or authentication. If existing hooks cannot express the feature, add a
generic hook to the extension contract plus an extension that uses it — do not
hard-code business logic into core. Working examples:
`examples/deerflow-extension-{example,bookmarks,agent-teams}`.
[Workflow guide](examples/deerflow-extension-workflows/AGENTS.md): public
AgentRuns; POSIX, one process/store; approval and session-authorized recovery; uncertain admissions need reconciliation;
no multi-Gateway failover or hard currency budget.

Knowledge management/space policy: `examples/deerflow-extension-knowledge-{management,platform}/`
(scoped `AGENTS.md`, `CONTRACT.md`). Use existing plugin/router/principal contracts;
retrieval stays independent. Space policy intersects administrator/resource/operation
and upstream grants, never grants chat access. SQLite holds plugin-owned space policy.
Provider stages are not completion evidence; reconcile uncertain writes, never retry.

Root runtime configs: copy `config.example.yaml` → `config.yaml` and
`extensions_config.example.json` → `extensions_config.json` (MCP/skills).
Both are gitignored and Gateway-editable; schema/resolution:
[backend/AGENTS.md](backend/AGENTS.md).

Skill review (`skills/public/skill-reviewer/`) and its CI waivers
(`.github/skill-review-waivers.v1.json`: trusted base only, never for blockers):
[details](docs/repository-guidance-details.md#skill-review-and-ci-waivers).

Scheduled tasks use a persisted, lease-fenced queue and non-interactive runs.
Lifecycle, authorization and concurrency rules live in [backend/AGENTS.md](backend/AGENTS.md);
the optional schedule tools: [details](docs/repository-guidance-details.md#scheduled-task-notes).

## Commands: Root vs. Module

**Root stack commands** (from repo root):

```bash
make setup       # Interactive setup wizard (recommended for new users)
make doctor      # Check configuration and system requirements
make support-bundle  # Generate redacted troubleshooting summary, AI issue draft, and optional zip
make config      # Generate local config files from the examples
make check       # Check that required tools are installed
make install     # Install all dependencies (frontend + backend + pre-commit hooks)
make extension-install SOURCE=...  # Manage trusted extensions; every target: docs/repository-guidance-details.md
make dev         # Start all services with hot-reload (Gateway + Frontend + Nginx)
make start       # Start all services in production mode (local, optimized); SKIP_FRONTEND_BUILD=1 reuses the last frontend build
make stop        # Stop all running services
make up / down   # Build/stop the production Docker stack (browser at localhost:2026)
make docker-start / docker-stop / docker-logs   # Docker development environment
```

Production uses image Python (`uv run --no-sync`) and `exec` for SIGTERM;
Compose sets `init: true` (`backend/tests/test_gateway_sigterm_delivery.py`).
`make up` waits for Gateway `/health`; failures show Compose status and Gateway logs.

Docker logs/restarts resolve `DEER_FLOW_ROOT` from this checkout before Compose,
as start/stop do.

Full list: `make help`.

**Module commands** (from each module):

```bash
# Backend (see backend/AGENTS.md for the full set)
cd backend && make dev        # Gateway API with reload (port 8001)
cd backend && make test       # Four parallel shards; excludes live and blocking-I/O tests
cd backend && make test-blocking-io  # Strict blocking-I/O suite
cd backend && make lint       # ruff check
cd backend && make format     # ruff format

# Frontend (see frontend-hm/AGENTS.md for the full set)
cd frontend-hm && pnpm dev    # Dev server: Webpack by default (override with DEER_FLOW_DEV_BUNDLER=turbo)
cd frontend-hm && pnpm check  # Lint + type check (run before committing)
cd frontend-hm && pnpm test   # Unit tests
```

Rule of thumb: **root `make` = the full application**; **`backend/Makefile` and `frontend-hm/`
(`pnpm`) = per-module work.**

HartMesh host pnpm calls and `make frontend-config`: [details](docs/repository-guidance-details.md#hartmesh-pnpm-wrapper).

### Prerequisites before `make dev`

Run `make config`, then `make install`, then `make dev` on first setup.
`make dev` requires existing config; both runtime configs are API-editable and
gitignored. Never commit them.

### Run a single test

```bash
# Backend (pytest); run one file or one test function
cd backend && python -m pytest tests/test_compose_default_bind_host.py -q
cd backend && python -m pytest tests/path/to/test.py::test_func -q

# Frontend (rstest)
cd frontend-hm && pnpm rstest run <pattern>  # e.g. pnpm rstest run my-component
```

### Logs

- Docker stack: `make docker-logs` (or `docker compose -f docker/... logs -f <svc>`).
- Local `make dev`: Gateway and frontend output goes to
  `logs/gateway.log` and `logs/frontend.log` in the repository root.
  Run `tail -f logs/gateway.log logs/frontend.log` there to follow both.

## References

- Backend work → **[backend/AGENTS.md](backend/AGENTS.md)**
- Frontend work → **[frontend-hm/AGENTS.md](frontend-hm/AGENTS.md)**
- Setup & install → **[Install.md](Install.md)**, **[CONTRIBUTING.md](CONTRIBUTING.md)**
- Project overview & usage → **[README.md](README.md)** (translations: `README_zh.md`,
  `README_ja.md`, `README_fr.md`, `README_ru.md`, `README_pt.md`, `README_ko.md`)
- Security policy → **[SECURITY.md](SECURITY.md)**
- Changes → **[CHANGELOG.md](CHANGELOG.md)**
- Cutting a release → **[RELEASING.md](RELEASING.md)**

## Cross-Cutting Conventions

Repo-wide conventions:

- Update `README.md` for user-facing changes and the relevant `AGENTS.md` for
  development/architecture changes in the same change set.
- **Test-driven development** — features and bug fixes ship with tests. Backend tests live
  in `backend/tests/` (TDD is mandatory there; see [backend/AGENTS.md](backend/AGENTS.md));
  frontend tests live in `frontend-hm/tests/`. Skill-script and skill-creator test rules:
  [details](docs/repository-guidance-details.md#skill-script-tests).
- **Before pushing** — backend `make format`, frontend `pnpm check`.
  Backend CI enforces `ruff format --check`.
- **Skill text encoding** — use explicit UTF-8 for textual skill resources.
  Read `SKILL.md` with `encoding="utf-8-sig"` to strip a leading BOM; write with
  `encoding="utf-8"`. Never rely on the platform locale.
- **Version sources must stay in lockstep** — `backend/pyproject.toml`,
  `backend/packages/harness/pyproject.toml`, the `deer-flow` and
  `deerflow-harness` entries in `backend/uv.lock` (uv's normalized spelling),
  `frontend-hm/package.json`, and `deploy/helm/deer-flow/Chart.yaml` (`version`
  and `appVersion`) must agree. A `v*` tag triggers `scripts/verify_versions.sh`
  and blocks publishing on drift. Bump with `scripts/bump_version.sh <ver>`
  (requires uv), verify with `scripts/verify_versions.sh <ver>`; see
  [RELEASING.md](RELEASING.md).
- **Don't edit `CLAUDE.md`** — it only contains `@AGENTS.md`. All agent guidance changes
  belong here in `AGENTS.md`; `CLAUDE.md` is a thin import shim.

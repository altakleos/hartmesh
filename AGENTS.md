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

DeerFlow is a LangGraph-based AI super-agent system with a full-stack architecture. The
backend runs a "super agent" with sandboxed execution, persistent memory, subagent
delegation, and extensible tools (built-in, MCP, community), all per-thread isolated. The
frontend is a Next.js chat UI. External IM platforms (Feishu, Slack, Telegram, Discord,
DingTalk) bridge into the same agent through the Gateway.

## Service Topology

A single `make dev` / Docker stack runs four cooperating services:

| Service         | Port   | Role                                                                 |
| --------------- | ------ | ------------------------------------------------------------------- |
| **Nginx**       | `2026` | Unified reverse-proxy entry point — open this in the browser        |
| **Gateway API** | `8001` | FastAPI REST API + embedded LangGraph-compatible agent runtime      |
| **Frontend**    | `3000` | Next.js web interface                                               |
| **Provisioner** | `8002` | Optional — only when sandbox is configured for provisioner/K8s mode |

Nginx is the single public entry: it proxies `/api/*` to the Gateway, rewriting
`/api/langgraph/*` onto the Gateway's native routes, and serves the frontend — see
[backend/AGENTS.md](backend/AGENTS.md) for the runtime and router detail. It compresses
HTML and configured textual assets, deliberately leaving SSE, fonts, images, audio, and
video uncompressed at the proxy layer.

Both compose files publish that entry as `"${BIND_HOST:-127.0.0.1}:${PORT:-2026}:2026"`
— **loopback by default**, matching the README's documented deployment model; a bare
`"${PORT}:2026"` binds `0.0.0.0`, which does not. The root `PORT` value is Docker ingress
configuration only; local orchestration pins Next.js to `3000` so loading `.env` cannot
make `make dev` wait on the wrong port. Nginx listening `default_server` on IPv4+IPv6 and
the Gateway binding `0.0.0.0:8001` are container-internal on purpose: the published nginx
port is the entire external surface. Any new published port needs an explicit bind
address; `backend/tests/test_compose_default_bind_host.py` pins this for every service in
both compose files.

## Repository Map

```
deer-flow/
├── Makefile                        # Root orchestration: drives the full stack (dev/start/stop, docker, setup)
├── config.example.yaml             # Template → copy to config.yaml (gitignored) at repo root
├── extensions_config.example.json  # Template → copy to extensions_config.json (gitignored): MCP servers + skills
├── backend/                        # Python backend — see backend/AGENTS.md
│   ├── Makefile                    # Per-module backend commands (dev, gateway, test, lint, migrate-rev)
│   ├── extensions/sources/         # Deployable snapshots of locally installed Python extensions
│   ├── packages/extension-api/     # deerflow-extension-api package (import: deerflow_extension_api.*) — public extension contract
│   ├── packages/harness/           # deerflow-harness package (import: deerflow.*) — agent framework
│   └── app/                        # FastAPI Gateway + IM channels (import: app.*)
├── frontend-hm/                    # HartMesh Next.js app — see frontend-hm/AGENTS.md
├── frontend/                       # Pinned upstream reference; no HartMesh edits
├── deploy/                         # Compose distribution and upstream Helm chart
├── docker/                         # docker-compose files, nginx config, provisioner
├── skills/                         # Agent skills: public/ (committed), custom/ (gitignored)
│                                    # Managed integration skill packs are global at .deer-flow/integrations/skills/{provider}/
│                                    # Integration credentials and enabled state remain per-user
├── contracts/                      # Cross-component JSON contracts (e.g. subagent status, skill review)
├── examples/                       # Extension examples: deerflow-extension-{example,bookmarks}
├── scripts/                        # Root orchestration scripts invoked by the Makefile (check, configure, doctor, support_bundle, serve, nginx, docker, deploy, setup_wizard)
├── tests/                          # Root-level tests (currently tests/skills/ — public skill tests)
└── docs/                           # Cross-cutting docs, plans, and design notes
```

Third-party extensions come from the operator-controlled `plugins:` list in
`config.yaml`, never API-writable `extensions_config.json`: they import code
with Gateway privileges and must be trusted. They contribute middleware,
lifecycle observers, Gateway services, routers and experimental full-stack
plugins. Manage them with `deerflow extensions` or `make extension-*`; restart
required. See [the extensions guide](backend/packages/harness/deerflow/extensions/AGENTS.md).
The upstream reference manual is under `frontend/src/content/{en,zh}/harness/extensions/`.

Runtime config lives at the **repo root**: copy `config.example.yaml` → `config.yaml`
(main app config) and `extensions_config.example.json` → `extensions_config.json` (MCP
servers + skills). Both real files are gitignored and may be edited at runtime via the
Gateway API. Config schema and resolution order are documented in
[backend/AGENTS.md](backend/AGENTS.md).

Skill review: `skills/public/skill-reviewer/` is the read-only reviewer using
`review_skill_package` and `contracts/skill_review/`. Model-visible data is
compact and tag-neutralized; raw payloads stay in artifacts. See the backend
guide for activation and skill-creator boundaries.

CI waivers (`.github/skill-review-waivers.v1.json`, enforced by
`scripts/review_changed_public_skills.py`) come only from the trusted base
manifest, bind one error to its file SHA-256 and expiry, stay visible, and never
waive blockers. An entry may preapprove future full-file hashes, effective once
that manifest lands in the trusted base. Merge the waiver before changing the
skill, then promote the consumed hash to `file_sha256`.

Scheduled tasks use a persisted, lease-fenced queue and non-interactive runs.
Lifecycle, authorization and concurrency rules live in [backend/AGENTS.md](backend/AGENTS.md).

## Commands: Root vs. Module

**Root `make` targets drive the whole stack** (run from the repo root):

```bash
make setup       # Interactive setup wizard (recommended for new users)
make doctor      # Check configuration and system requirements
make support-bundle  # Generate redacted troubleshooting summary, AI issue draft, and optional zip
make config      # Generate local config files from the examples
make check       # Check that required tools are installed
make install     # Install all dependencies (frontend + backend + pre-commit hooks)
make extension-install SOURCE=...  # Install and enable a trusted Python extension
make extension-upgrade SOURCE=...  # Replace an installed extension and keep its config
make extension-list                # List configured Python extensions
make extension-enable NAME=...     # Enable an installed extension (restart required)
make extension-disable NAME=...    # Disable without uninstalling (restart required)
make extension-remove NAME=...     # Remove package and config entry (restart required)
make dev         # Start all services with hot-reload (Gateway + Frontend + Nginx)
make start       # Start all services in production mode (local, optimized); SKIP_FRONTEND_BUILD=1 reuses the last frontend build
make stop        # Stop all running services
make up / down   # Build/stop the production Docker stack (browser at localhost:2026)
make docker-start / docker-stop / docker-logs   # Docker development environment
```

Production startup uses the image's pre-built Python environment with `uv run
--no-sync`, gives the Gateway a real `/health` probe, and makes `make up` wait
for that probe before printing its success banner. A readiness failure must
surface Compose status and recent Gateway logs instead of claiming the stack is
running.

Docker log and restart commands resolve `DEER_FLOW_ROOT` from the current
checkout before invoking Compose, matching the start and stop commands.

Run `make help` for the full list.

**Per-module commands drive a single module** (run inside that module):

```bash
# Backend (see backend/AGENTS.md for the full set)
cd backend && make dev        # Gateway API with reload (port 8001)
cd backend && make test       # Default backend suite; excludes live and blocking-I/O tests
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

HartMesh host calls use `scripts/pnpm.py --project frontend-hm --`: it prefers
direct pnpm, falls back to Corepack, and resolves executable paths before
changing to that project. The no-selector form retains `frontend/` for upstream
compatibility. `make frontend-config` initializes the product's ignored `.env`,
preserving an existing destination or migrating the old app's settings.

### Prerequisites before `make dev`

`make dev` does **not** generate config files. First-time setup order:

```bash
make config      # copy config.example.yaml -> config.yaml and extensions_config.example.json -> extensions_config.json (both gitignored)
make install     # install frontend + backend deps and pre-commit hooks
make dev         # then start everything
```

Without `config.yaml` present, services fail to boot. `config.yaml` / `extensions_config.json`
may be edited at runtime via the Gateway API but are gitignored, so never commit them.

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
- Local `make dev`: each service logs to its own terminal pane. Frontend dev-server
  errors surface in the browser console at `localhost:3000`; backend tracebacks appear
  in the Gateway terminal.

## Where to Go Next

- Backend work → **[backend/AGENTS.md](backend/AGENTS.md)**
- Frontend work → **[frontend-hm/AGENTS.md](frontend-hm/AGENTS.md)**
- Setup & install → **[Install.md](Install.md)**, **[CONTRIBUTING.md](CONTRIBUTING.md)**
- Project overview & usage → **[README.md](README.md)** (translations: `README_zh.md`,
  `README_ja.md`, `README_fr.md`, `README_ru.md`)
- Security policy → **[SECURITY.md](SECURITY.md)**
- Changes → **[CHANGELOG.md](CHANGELOG.md)**
- Cutting a release → **[RELEASING.md](RELEASING.md)**

## Cross-Cutting Conventions

These apply repo-wide; module guides own the module-specific detail.

- **Documentation update policy** — keep docs in sync with code: update `README.md` for
  user-facing changes and the relevant `AGENTS.md` for development/architecture changes in
  the same change set.
- **Test-driven development** — features and bug fixes ship with tests. Backend tests live
  in `backend/tests/` (TDD is mandatory there; see [backend/AGENTS.md](backend/AGENTS.md));
  frontend tests live in `frontend-hm/tests/`.
- **Format before pushing** — run `make format` (backend) / `pnpm check` (frontend). Backend
  CI enforces `ruff format --check`, so formatting must be clean before a push.
- **Skill text encoding** — treat `SKILL.md` and other textual skill resources as UTF-8;
  Python utilities that read or write them must pass `encoding="utf-8"` rather than
  relying on the platform locale.
- **Version sources must stay in lockstep** — `backend/pyproject.toml`, the root
  `deer-flow` entry in `backend/uv.lock` (uv's normalized spelling),
  `frontend-hm/package.json`, and `deploy/helm/deer-flow/Chart.yaml` (`version`
  and `appVersion`) must agree. A `v*` tag triggers `scripts/verify_versions.sh`
  and blocks publishing on drift. Bump with `scripts/bump_version.sh <ver>`
  (requires uv), verify with `scripts/verify_versions.sh <ver>`; see
  [RELEASING.md](RELEASING.md).
- **Don't edit `CLAUDE.md`** — it only contains `@AGENTS.md`. All agent guidance changes
  belong here in `AGENTS.md`; `CLAUDE.md` is a thin import shim.

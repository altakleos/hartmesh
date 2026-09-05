# AGENTS.md

Coding-agent source of truth; `CLAUDE.md` imports it. Module guides own depth:

- **[backend/AGENTS.md](backend/AGENTS.md)** — backend depth: harness/app split, agent &
  middleware chain, sandbox, MCP, skills, memory, IM channels, persistence/migrations,
  config system, test layout.
- **[frontend/AGENTS.md](frontend/AGENTS.md)** — frontend depth: Next.js App Router layout,
  thread/streaming data flow, code style, commands.

## What is DeerFlow

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

Nginx is the single public entry: it serves the frontend, proxies
`/api/langgraph/*` to the Gateway's LangGraph runtime (rewritten to native
`/api/*`), and passes other `/api/*` to the Gateway REST routers; see
[backend/AGENTS.md](backend/AGENTS.md). One tenant is frozen per Gateway
([contract](backend/docs/TENANT_IDENTITY.md)). It compresses HTML and
configured textual assets but not SSE, fonts, images, audio, or video.

Both compose files publish that entry as
`"${BIND_HOST:-127.0.0.1}:${PORT:-2026}:2026"` — **loopback by default**; a
bare `"${PORT}:2026"` binds `0.0.0.0`. Root `PORT` is Docker ingress only;
local orchestration pins Next.js to `3000` so `.env` cannot make `make dev`
wait on the wrong port. Nginx listens `default_server` (IPv4+IPv6) and the
Gateway binds `0.0.0.0:8001` on purpose, both container-internal: the published
nginx port is the entire external surface and `8001` stays unpublished. Any new
published port needs an explicit bind address;
`backend/tests/test_compose_default_bind_host.py` pins this for every service.

`durable_two_gateway_v1` covers only its exact two-replica PostgreSQL + Redis +
AIO/RWX artifact, not arbitrary scaling, IM HA, cross-region operation, or
zero-downtime upgrades. No passing artifact exists here; operator claims cannot
unlock production rendering or startup. The chart's optional pre-created
`sandboxNamespace` split (`K8S_NAMESPACE` selects sandbox resources,
`PROVISIONER_GATEWAY_NAMESPACE` the release namespace for Gateway
ServiceAccount validation) is documented in the Helm README.

## Repository Map

```
deer-flow/
├── Makefile                        # Root orchestration: drives the full stack (dev/start/stop, docker, setup)
├── config.example.yaml             # Template → copy to config.yaml (gitignored) at repo root
├── extensions_config.example.json  # Template → copy to extensions_config.json (gitignored): MCP servers + skills
├── backend/                        # Python backend — see backend/AGENTS.md
│   ├── Makefile                    # Per-module backend commands (dev, gateway, test, lint, migrate-rev)
│   ├── extensions/sources/         # Deployable snapshots of locally installed Python extensions
│   ├── packages/extension-api/     # deerflow-extension-api package (import: deerflow_extension_api.*), the public contract
│   ├── packages/harness/           # deerflow-harness package (import: deerflow.*) — agent framework
│   ├── packages/runtime-api/       # deerflow-runtime-api — stdlib-only embedded durable runtime contracts
│   └── app/                        # FastAPI Gateway + IM channels (import: app.*)
├── frontend/                       # Next.js frontend (pnpm) — see frontend/AGENTS.md
├── docker/                         # docker-compose files, nginx config, provisioner
├── skills/                         # Agent skills: public/ (committed), custom/ (gitignored)
│                                    # Managed integration skill packs are global at .deer-flow/integrations/skills/{provider}/
│                                    # Integration credentials and enabled state remain per-user
├── contracts/                      # Cross-component JSON contracts (e.g. subagent status, skill review)
├── examples/deerflow-extension-example/ # Standalone package demonstrating all extension contribution kinds
├── scripts/                        # Root orchestration scripts invoked by the Makefile (check, configure, doctor, support_bundle, serve, nginx, docker, deploy, setup_wizard)
├── tests/                          # Root-level tests (currently tests/skills/ — public skill tests)
└── docs/                           # Cross-cutting docs, plans, and design notes
```

Third-party extensions come from the operator-controlled top-level `plugins:`
list in `config.yaml` (never API-writable `extensions_config.json`) because
they import code. They contribute middleware, lifecycle, model observers,
Gateway services, and routers ([reference extension](examples/deerflow-extension-example/));
manage via `deerflow extensions` / `make extension-*`, restart required.
Provenance records admitted bytes/config, but extensions run with Gateway
privileges and must be trusted. See the
[extensions guide](backend/packages/harness/deerflow/extensions/AGENTS.md) and
[provenance guide](docs/EXTENSION_ARTIFACT_PROVENANCE.md).

Runtime config lives at the **repo root**: copy `config.example.yaml` → `config.yaml`
(main app config) and `extensions_config.example.json` → `extensions_config.json` (MCP
servers + skills). Both real files are gitignored. The default governed tool plane
requires stage→validate→promote for writes; direct routes require
`tool_plane.enabled: false`. It separates deployment material from user overlays,
binds accepted runs, detects drift, and mounts no exact-two mutation/bootstrap
routes. See the
[operator guide](docs/GOVERNED_TOOL_PLANE.md).

Upstream offers: see [docs/UPSTREAM_OFFERS.md](docs/UPSTREAM_OFFERS.md).

Durable-runtime invariants (accepted admission, `deerflow-runtime-api`,
execution budgets and evidence projections, run evidence bundles, durable
batches, MCP tasks and replay keys, scheduled tasks, exact-two qualification)
live in [backend/AGENTS.md](backend/AGENTS.md) and the `docs/` files it links.
Missing qualification infrastructure is an unpassed gate, never a skip.

Skill review: `skills/public/skill-reviewer/` is the read-only reviewer using
the `review_skill_package` tool and `contracts/skill_review/`; model-visible
data is compact and tag-neutralized, raw payloads stay in tool artifacts.
Durable invocations snapshot effective skills before admission and execute only
accepted immutable material (nonempty packages require Docker/AIO); live edits
affect later invocations only. CI skill-review waivers
(`.github/skill-review-waivers.v1.json`, enforced by
`scripts/review_changed_public_skills.py`) come only from the trusted base
manifest, bind one error to its file SHA-256 and expiry, stay visible, and never
waive blockers; an entry may preapprove future full-file SHA-256 values,
effective once that manifest lands in the trusted base. Merge the waiver before
changing the skill, then promote the consumed hash to `file_sha256`.

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
make extension-list / extension-enable NAME=... / extension-disable NAME=... / extension-remove NAME=...  # restart required
make dev         # Start all services with hot-reload (Gateway + Frontend + Nginx)
make start       # Production mode locally; SKIP_FRONTEND_BUILD=1 reuses the last frontend build
make stop        # Stop all running services
make up / down   # Build/stop the production Docker stack (browser at localhost:2026)
make docker-start / docker-stop / docker-logs   # Docker development environment
```

Production startup uses the image's pre-built Python environment (`uv run
--no-sync`) and a real Gateway `/health` probe; `make up` waits for that probe
before its success banner, and a readiness failure must surface Compose status
and recent Gateway logs rather than claim the stack is running. Docker
log/restart commands resolve `DEER_FLOW_ROOT` from the current checkout,
matching start and stop.

Run `make help` for the full list.

**Per-module commands drive a single module** (run inside that module):

```bash
# Backend (see backend/AGENTS.md for the full set)
cd backend && make dev        # Gateway API with reload (port 8001)
cd backend && make test       # Default backend suite; excludes live and blocking-I/O tests
cd backend && make test-blocking-io  # Strict blocking-I/O suite
cd backend && make lint       # ruff check
cd backend && make format     # ruff format

# Frontend (see frontend/AGENTS.md for the full set)
cd frontend && pnpm dev       # Dev server: Webpack by default (override with DEER_FLOW_DEV_BUNDLER=turbo)
cd frontend && pnpm check     # Lint + type check (run before committing)
cd frontend && pnpm test      # Unit tests
```

Rule of thumb: **root `make` = the full application**; **`backend/Makefile` and `frontend/`
(`pnpm`) = per-module work.**

Host-side pnpm consumers (root/frontend Makefiles, diagnostic scripts) run through `scripts/pnpm.py`: it prefers a direct `pnpm`/`pnpm.cmd`, falls back to `corepack pnpm`, resolves absolute paths before changing directory, and runs from `frontend/` so Corepack honors that project's pinned package-manager version.

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
cd frontend && pnpm rstest run <pattern>     # e.g. pnpm rstest run my-component
```

### Logs

- Docker stack: `make docker-logs` (or `docker compose -f docker/... logs -f <svc>`).
- Local `make dev`: each service logs to its own terminal pane. Frontend dev-server
  errors surface in the browser console at `localhost:3000`; backend tracebacks appear
  in the Gateway terminal.

## Where to Go Next

- Backend work → **[backend/AGENTS.md](backend/AGENTS.md)**
- Frontend work → **[frontend/AGENTS.md](frontend/AGENTS.md)**
- Setup & install → **[Install.md](Install.md)**, **[CONTRIBUTING.md](CONTRIBUTING.md)**
- Project overview & usage → **[README.md](README.md)** (translations: `README_zh.md`,
  `README_ja.md`, `README_fr.md`, `README_ru.md`, `README_es.md`, `README_pt.md`,
  `README_de.md`)
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
  frontend tests live in `frontend/tests/`.
- **Format before pushing** — run `make format` (backend) / `pnpm check` (frontend). Backend
  CI enforces `ruff format --check`, so formatting must be clean before a push.
- **Skill text encoding** — treat `SKILL.md` and other textual skill resources as UTF-8;
  Python utilities that read or write them must pass `encoding="utf-8"` rather than
  relying on the platform locale.
- **Version sources must stay in lockstep** — `backend/pyproject.toml`, the
  root `deer-flow` entry in `backend/uv.lock`, `frontend/package.json`, and
  `deploy/helm/deer-flow/Chart.yaml` (`version` + `appVersion`) must match. A
  `v*` tag triggers `scripts/verify_versions.sh` in CI and **blocks all
  publishing** on drift. Bump with `scripts/bump_version.sh <ver>`, verify with
  `scripts/verify_versions.sh <ver>`. See [RELEASING.md](RELEASING.md).
- **Don't edit `CLAUDE.md`** — it only contains `@AGENTS.md`. All agent guidance changes
  belong here in `AGENTS.md`; `CLAUDE.md` is a thin import shim.

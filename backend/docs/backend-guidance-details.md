# Backend guidance details

Detail moved out of `backend/AGENTS.md` so the backend guidance chains stay within the budgets in `scripts/check_agent_guidance.py`. These rules still apply; `backend/AGENTS.md` links each section.

## Package import hygiene

Package import hygiene: the `deerflow.agents` and `deerflow.subagents` package
roots expose heavyweight graph/executor entrypoints lazily. The
`deerflow.agents:make_lead_agent` LangGraph Server entrypoint is a concrete thin
module-level function because the server resolves graph factories directly from
the module dictionary; the wrapper keeps the lead-agent and skill-cache imports
inside the function so importing the package remains lightweight. Internal
modules that only need lightweight types, config, or registries should import
the concrete submodule instead of adding eager package-root imports that pull in
the tool graph or subagent executor during state/schema imports.

## ThreadMetaStore JSON filters

`ThreadMetaStore.search()` keeps JSON filter semantics identical across memory,
SQLite, and PostgreSQL: missing differs from null, bool differs from int, and
float filters accept integer or real JSON numbers through `json_value_matches`.

## Running backend services separately

From the **backend** directory:

```bash
# Gateway API
make gateway
```

Direct access (without nginx):
- Gateway: `http://localhost:8001`

## Frontend configuration

The frontend uses environment variables to connect to backend services:
- `NEXT_PUBLIC_LANGGRAPH_BASE_URL` - Defaults to `/api/langgraph` (through nginx)
- `NEXT_PUBLIC_BACKEND_BASE_URL` - Defaults to empty string (through nginx)

When using `make dev` from root, the frontend automatically connects through nginx.

## Documentation

See `docs/` directory for detailed documentation:
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration options
- [ARCHITECTURE.md](ARCHITECTURE.md) - Architecture details
- [API.md](API.md) - API reference
- [SETUP.md](SETUP.md) - Setup guide
- [FILE_UPLOAD.md](FILE_UPLOAD.md) - File upload feature
- [PATH_EXAMPLES.md](PATH_EXAMPLES.md) - Path types and usage
- [summarization.md](summarization.md) - Context summarization
- [plan_mode_usage.md](plan_mode_usage.md) - Plan mode with TodoList

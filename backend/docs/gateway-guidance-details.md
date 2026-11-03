# Gateway guidance details

Detail moved out of `app/gateway/AGENTS.md` so the Gateway guidance chains stay within the budgets in `scripts/check_agent_guidance.py`. These rules still apply; the Gateway guide links each section.

## Conversation access

`conversation_access.py` grants the opt-in read-only tool only for explicit
`conversation_references` with effective `runs:read`; never infer grants from
messages or checkpoints. Pass the callback through `RunContext`, not serialized
config, and let the worker inject and clear it. `conversation_reader.py` shares
HTTP transcript visibility/pagination without a Request; callers own authorization.
Exclude non-text/internal content. `RunCreateRequest` lifts SDK-compatible
`context.conversation_references` into its top-level field before validation,
then removes it from context; supplying both is 422. This shares bounds/error
locations and keeps `merge_run_context_overrides` clear. Admission and
`/api/features` use `conversation_references_enabled()` as the sole configured
predicate.

## Health and readiness

Port 8001: `/health` is liveness; `/health/ready` checks
ORM, startup checkpointer/Store and required Redis. Failures return 503; memory
is `not_configured`. Redis probes reuse the startup bridge and live login
store, coalesce before off-thread config reads, cache for one second and bound
waiters to 2s. Drain workers before readmission; close
readiness before auth/runtime clients. Omit probe error details.
`GATEWAY_ENABLE_DOCS=false` hides `/docs`, `/redoc` and `/openapi.json`.

## Browser transport and credentials

CORS defaults to same-origin through nginx:2026. Split-origin/port-forwarded browsers require exact `GATEWAY_CORS_ORIGINS`, read by both `CORSMiddleware` and `CSRFMiddleware` to align CORS/auth checks. Expose `Content-Location` through `CORS_EXPOSED_HEADERS` (`csrf_middleware.py`): creating routes return the run ID there; it is not CORS-safelisted. The SDK resolves run metadata only from that header; hiding it breaks `useStream.onCreated` and thread-gated actions.

Browser sessions use `app.gateway.auth.session_cookie`. Persist `HttpOnly access_token` only for HTTPS/trusted-forwarded HTTPS, direct-host localhost HTTP or operator opt-in; public HTTP uses session cookies. A `HttpOnly` preference cookie preserves `remember_me` across reissues. Handlers stamp final `max_age` on `request.state`; CSRF mirrors it, including password/OIDC reissues. Logout clears all auth cookies and suppresses CSRF reissue. Never store passwords.

Personal Access Tokens (`app.gateway.auth.pat`, `Authorization: Bearer dfp_...`) run as their owning user: an invalid Bearer is a hard 401 with no cookie fallback, which keeps `CSRFMiddleware`'s Bearer skip safe (origin checks still run). Scopes narrow within the allowlisted threads/runs/projects routes (including `POST /api/threads/{id}/move`); every other authenticated route 403s PAT callers (admin included). PAT management and `/change-password` require session auth; only SHA-256 digests are stored (`0017`).

## Thread project membership

Thread→project membership is written by thread creation (`POST /api/threads` with
a validated `project_id`), branch creation (the new row inherits the source
thread's project; an archived/deleted project degrades the branch to unassigned
instead of failing), and explicit moves (`POST /api/threads/{id}/move`); run
admission never modifies membership; it only pins the resolved project context
read-only into the run context. The server-reserved `deerflow_project_id`
metadata key is a read-only exposure of the `threads_meta.project_id` column and
is stripped from client writes.

## Localhost persistence

Localhost persistence deliberately reads the direct request `Host` and ignores `Forwarded` / `X-Forwarded-Host`. Scheme and auth-origin reconstruction still consume forwarding headers. The bundled nginx sets `X-Forwarded-Proto`, but preserves an upstream HTTPS value and does not overwrite every forwarded header, so the outer trusted proxy must replace or strip client-supplied forwarding headers before traffic reaches DeerFlow.

## Standalone LangGraph Studio

Standalone local LangGraph Studio is recognized only through the upstream
`Auth.types.StudioUser` principal type, never by its reusable identity string.
The type is resolved once at import; an older SDK without it degrades to normal
owner scoping.
For that principal's assistant reads/searches, `langgraph_auth.add_owner_filter`
selects genuine server-registered assistants plus assistants owned by Studio;
all other resources remain owner-scoped. Assistant create/update handlers make
both `user_id` and `created_by=user` server-owned, because LangGraph gives
`created_by=system` privileged ownership semantics during run creation. The
custom application module in `langgraph_studio.py` is imported before the
locked in-memory runtime lifespan. At that pre-runtime boundary it derives
genuine system assistant IDs from the CLI-provided graph registry, removes
their persisted active/version rows so graph registration recreates them, and
demotes every other legacy `created_by=system` marker in both active assistants
and version history. This must happen before runtime 0.30.0 loads and purges
system-marked rows; a user application lifespan is too late. LangGraph executes
this file-backed custom app without first registering its module in
`sys.modules`; keep its annotations eager so dataclass processing remains
compatible with that loader, and preserve the direct file-loader regression
test. An empty graph registry or absent persistence file is a no-op, while
persistence parse/write errors fail startup closed. The harness requires
in-memory runtime 0.30.0 or newer, and a persisted store containing no
expected registered assistant row emits a drift warning so changes to
LangGraph's internal persistence contract are observable. With current
create/update writes and all legacy versions sanitized, ordinary
owner-scoped assistant version selection remains enabled.

## Thread identifiers

Thread identifiers use the shared `deerflow.utils.thread_id` contract
`^[A-Za-z0-9_-]{1,64}$`. Caller-provided opaque IDs remain supported; UUIDs
are generated only for `None`, while explicit empty strings fail validation.
Gateway creation and state-producing request boundaries, embedded-client
entry points, filesystem/upload/event-store consumers, scheduled launches,
and the standalone Provisioner enforce the same contract before persistence
or workspace initialization. Route-addressable legacy IDs remain accepted by
pure reads and cleanup/control endpoints; deleting one best-effort removes
metadata and checkpoints but skips local filesystem cleanup, so the raw value
is never interpolated into a host path. New runs, workspace/sandbox
operations, and other state-producing mutations remain blocked.

## Message feed seq

**Message feed seq** (#4666): streaming `values` frames, `GET
/threads/{id}/state`, and `POST /threads/{id}/history` stamp serialized
messages with `additional_kwargs.deerflow_seq` so clients can place
checkpoint-kept messages against the paged feed; the REST reads resolve the
store via `threads.py::_optional_run_event_store` (a feed-less deployment
still reads threads), and `services.py::normalize_input` strips the
server-owned key from client input (#4380). Mechanism and identity rule:
`packages/harness/deerflow/runtime/AGENTS.md`.

## Route and skill-listing authorization

`authz.authorize_model_use` shares the `model:use` decision between model details
and follow-up suggestions, including the factory's first configured model when
the caller omits `model_name`. Suggestions check admission before their
best-effort LLM error handler: explicit denies return 403 without invoking the
model, while provider failures honor `authorization.fail_closed`.

`authz.py::resolve_route_permissions()` serves HTTP middleware, decorators and Live Browser WebSocket admission. It evaluates registered permissions asynchronously as `resource="route"`, targeting full `resource:action` strings. `AuthContext` caches HTTP decisions; provider errors follow `authorization.fail_closed` per permission. Disabled authorization returns all permissions without a provider. Owner/admin checks remain independent. Live requires `threads:write` with `is_internal=False` after login/Origin checks but before acceptance, owner lookup, or session acquisition; denial closes 4403, unexpected setup errors close 4501, cancellation propagates. Checks are admission-only; restart Gateway to close old connections. Tests: `test_authorization_route_permissions.py`, `test_browser_readonly_security.py`, `test_auth.py`, `test_auth_middleware.py` in `tests/`.

Skill listing authorization mirrors the models pattern: `routers/skills.py` routes resolve `(provider, principal)` through `authz.py::resolve_skill_authorization()` — a thin sibling of `resolve_model_authorization`, both delegating to the shared `_resolve_route_scoped_authorization` core — and the shared `_filter_visible_skills` helper filters the user-scoped catalog (public + caller's custom skills) through `filter_resources(principal, "skill", ...)` by name. Three user-facing surfaces apply it: `GET /api/skills` (the frontend skill list / slash-command autocomplete), `GET /api/skills/custom`, and `GET /api/skills/{name}` — the detail endpoint returns the standard 404 for an invisible skill so it cannot become an existence oracle the filtered list closed (`get_model`'s 403 is an execution decision via `authorize("model", "use")`, which has no skills equivalent in this layer). Anonymous callers are unfiltered (mirroring `list_models`), and provider resolution/decision errors follow `authorization.fail_closed` (fail-closed → empty listing / 404, fail-open → full listing). Skill management endpoints (`install`/`upload`/`reload`/custom-skill CRUD) stay `require_admin_user`-gated, and runtime activation authorization is a separate layer. The built-in RBAC provider maps this to the per-role `skills` policy key. Tests: `tests/test_skills_listing_authorization.py`.

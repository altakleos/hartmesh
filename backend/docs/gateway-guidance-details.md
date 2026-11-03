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

FastAPI listens on 8001. `GET /health` is liveness; `GET /health/ready` concurrently
probes startup snapshots of ORM, checkpointer/Store and stream bridge
(`StreamBridge.ping()`) under one deadline and per-kind gates. Unreachable or
unresolved gating backends return 503; `memory` reports `not_configured`;
provisioner `/health` is report-only. See `health.py`. `GATEWAY_ENABLE_DOCS=false`
disables `/docs`, `/redoc` and `/openapi.json`.

## Browser transport and credentials

CORS defaults to same-origin behind nginx on 2026. Split-origin and port-forwarded
browser clients must set exact `GATEWAY_CORS_ORIGINS`, shared by `CORSMiddleware`
and `CSRFMiddleware`. Expose `Content-Location` through `CORS_EXPOSED_HEADERS`
(`csrf_middleware.py`): run-creating routes put the run ID there, and the LangGraph
SDK uses only that header for run metadata. Without exposure, JS cannot read it,
breaking `useStream.onCreated` and thread-gated actions.

Browser sessions belong to `app.gateway.auth.session_cookie`. Login's `remember_me`
flag never stores passwords. `SessionCookiePolicy` persists `HttpOnly access_token`
only for HTTPS/trusted-forwarded HTTPS, direct-host localhost HTTP, or explicit
operator opt-in; public HTTP otherwise uses session cookies. Session creators set
final `max_age` on `request.state`; CSRF mirrors it across login, password changes
and OIDC callbacks. A small `HttpOnly` preference cookie preserves the choice.
Logout clears auth cookies and suppresses CSRF re-issue.

OIDC redirects retain existing endpoint query bytes (RFC 6749 section 3.1).
Malformed IdP bodies raise `OIDCError` (502/`sso_failed`).
`tests/test_oidc_auth.py` pins query, state-cookie, nonce and PKCE agreement.

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

`authz.py::resolve_route_permissions()` is shared by HTTP, decorator-only auth, and Live Browser WebSocket admission. It evaluates full `resource:action` targets; provider errors follow `authorization.fail_closed`, while disabled authorization preserves legacy permissions. Owner/admin checks stay separate. Live requires `threads:write` with `is_internal=False` after login/Origin checks; denial closes 4403 and setup errors close 4501. Admission is connection-scoped. On disconnect, Live drains owned `stop_screencast()` cleanup before releasing the session lease. Tests: `test_authorization_route_permissions.py`, `test_browser_readonly_security.py`, `test_auth.py`, `test_auth_middleware.py`.

Skill listing authorization mirrors the models pattern: `routers/skills.py` routes resolve `(provider, principal)` through `authz.py::resolve_skill_authorization()` — a thin sibling of `resolve_model_authorization`, both delegating to the shared `_resolve_route_scoped_authorization` core — and the shared `_filter_visible_skills` helper filters the user-scoped catalog (public + caller's custom skills) through `filter_resources(principal, "skill", ...)` by name. Three user-facing surfaces apply it: `GET /api/skills` (the frontend skill list / slash-command autocomplete), `GET /api/skills/custom`, and `GET /api/skills/{name}` — the detail endpoint returns the standard 404 for an invisible skill so it cannot become an existence oracle the filtered list closed (`get_model`'s 403 is an execution decision via `authorize("model", "use")`, which has no skills equivalent in this layer). Anonymous callers are unfiltered (mirroring `list_models`), and provider resolution/decision errors follow `authorization.fail_closed` (fail-closed → empty listing / 404, fail-open → full listing). Skill management endpoints (`install`/`upload`/`reload`/custom-skill CRUD) stay `require_admin_user`-gated, and runtime activation authorization is a separate layer. The built-in RBAC provider maps this to the per-role `skills` policy key. Tests: `tests/test_skills_listing_authorization.py`.

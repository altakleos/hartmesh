# API Reference

This document provides a complete reference for the DeerFlow backend APIs.

## Overview

DeerFlow backend exposes two sets of APIs:

1. **LangGraph-compatible API** - Agent interactions, threads, and streaming (`/api/langgraph/*`)
2. **Gateway API** - Models, MCP, skills, uploads, and artifacts (`/api/*`)

All APIs are accessed through the Nginx reverse proxy at port 2026.

For agent conversations, clients can either pre-create a thread
(`POST /api/langgraph/threads`) or start immediately with the stateless stream
endpoint (`POST /api/langgraph/runs/stream`). The latter auto-creates a thread
and returns `thread_id` and `run_id` in the response `Content-Location` header.

## Authentication

Browser sessions authenticate with the `access_token` session cookie issued at
login. Programmatic clients can instead use a **personal access token (PAT)**
sent as a Bearer credential:

```http
POST /api/threads/search
Authorization: Bearer dfp_...
Content-Type: application/json

{}
```

PATs require a configured database backend (SQLite/PostgreSQL) — on the
memory-only backend, Bearer credentials are rejected and PAT management routes
return `503`.

### Personal Access Tokens

Base URL: `/api/v1/auth`

PAT management requires an **interactive session** (a PAT cannot manage PATs
or change passwords, so a leaked automation token cannot mint fresh
credentials). The raw token is returned **exactly once** at creation; only its
SHA-256 digest is stored server-side.

#### Create Token

```http
POST /api/v1/auth/pats
Content-Type: application/json
```

**Request Body:**
```json
{
  "name": "ci-runner",
  "scopes": ["threads:read", "runs:create", "runs:read"],
  "expires_in_days": 90
}
```

- `scopes` — subset of the route permissions: `threads:read`, `threads:write`,
  `threads:delete`, `runs:create`, `runs:read`, `runs:cancel`. A PAT can only
  *narrow* its owning user's permissions, never widen them.
- `expires_in_days` — optional (`1`–`365`); omitted means the token never expires.

**Response (`201`):**
```json
{
  "id": "0f0c6e6a-...",
  "name": "ci-runner",
  "scopes": ["runs:create", "runs:read", "threads:read"],
  "expires_at": "2026-11-25T10:30:00Z",
  "created_at": "2026-08-27T10:30:00Z",
  "token": "dfp_..."
}
```

Save `token` immediately — it cannot be retrieved again.

#### List Tokens

```http
GET /api/v1/auth/pats
```

Returns the caller's tokens with `last_used_at` / `revoked_at` audit fields;
never returns digests or raw tokens.

#### Revoke Token

```http
DELETE /api/v1/auth/pats/{pat_id}
```

Revocation commits a tenant-scoped tombstone. A request whose authentication
lookup completed before that commit may finish; every later lookup is denied.
Historical accepted-run evidence is retained but grants no new access.

#### Read Token Audit

```http
GET /api/v1/auth/pats/{pat_id}/audit?limit=50
```

Requires an interactive session and ownership of the tenant-bound PAT. `limit`
is `1`–`100`. The response contains bounded daily aggregates with the public
credential reference, pseudonymous actor digest, authentication method,
authority digest, coarse action/route/reason, timestamps, and count. It never
returns the PAT name, raw token, stored token digest, headers, request body, or
IP address. SQL audit retention defaults to 90 days.

### PAT Constraints

- A request carrying an `Authorization` header that fails validation gets a
  hard `401` — it never falls back to the session cookie.
- **Cancel capability requires `runs:cancel` on every request dimension that
  carries it**, not just the dedicated cancel route: `?action=interrupt|rollback`
  on `POST /api/threads/{thread_id}/runs/{run_id}/stream` (action-less joins
  stay at `runs:read`), and `multitask_strategy=interrupt|rollback` on run
  creation (the default `reject` stays at `runs:create`). Joining a run's
  stream is pure observation — an observer disconnecting never cancels the run.
- **Route-level default-deny:** PAT requests are admitted only to the
  thread/run lifecycle routes the v1 scopes govern — `POST /api/threads`
  (create), `POST /api/threads/search` (list), `GET/PATCH/DELETE
  /api/threads/{thread_id}`, the thread `goal`/`state`/`compact`/`history`/
  `branches` subroutes, and exactly the implemented `/runs` subroutes
  (`GET|POST /api/threads/{thread_id}/runs`, the POST-only `stream`, `wait`,
  `regenerate/prepare`, and `edit-regenerate/prepare` collection endpoints,
  `GET /api/threads/{thread_id}/runs/{run_id}` plus its `cancel` (POST),
  `join`/`messages`/`events`/`workspace-changes` (GET), and
  `GET|POST .../runs/{run_id}/stream`, plus
  `GET|POST .../runs/{run_id}/artifacts/archive`), plus `POST /api/runs/stream|wait` and
  `GET /api/runs/{run_id}/messages|feedback`. A route added under `/runs` is
  denied until explicitly added to the policy.
  Every other authenticated route — memory, agents, models, MCP/skills
  config, integrations, channels, uploads — answers `403` to PAT callers
  regardless of scopes. Scope enforcement alone only constrains
  permission-decorated routes, so the allowlist is the outer boundary;
  session-cookie callers are unaffected.
- PAT credentials never carry admin capability, even when the owning user is
  an admin. This includes extension-contributed admin routes: the extension
  principal projection suppresses every admin signal for PAT callers.
- Revoking or deleting the owning user invalidates their PATs on the next
  authentication lookup. Unknown, expired, revoked, cross-tenant, and malformed
  candidates share the same non-oracular `401` response.
- Every new durable invocation binds a server-created credential method,
  optional PAT UUID reference, and canonical effective-authority digest to its
  existing identity/Origin/tenant evidence. Required audit failure returns 503
  before durable admission or any cancel-capable control; ordinary use/failure
  audit refresh is best-effort. See the
  [full contract](../../docs/AUDITABLE_AUTOMATION_IDENTITIES.md).

## LangGraph-compatible API

Base URL: `/api/langgraph`

The public LangGraph-compatible API follows LangGraph SDK conventions. In the unified nginx deployment, Gateway owns `/api/langgraph/*` and translates those paths to its native `/api/*` run, thread, and streaming routers.

### Threads

#### Create Thread

```http
POST /api/langgraph/threads
Content-Type: application/json
```

**Request Body:**
```json
{
  "metadata": {}
}
```

**Response:**
```json
{
  "thread_id": "abc123",
  "created_at": "2024-01-15T10:30:00Z",
  "metadata": {}
}
```

#### Get Thread State

```http
GET /api/langgraph/threads/{thread_id}/state
```

**Response:**
```json
{
  "values": {
    "messages": [...],
    "sandbox": {...},
    "artifacts": [...],
    "thread_data": {...},
    "title": "Conversation Title"
  },
  "next": [],
  "config": {...}
}
```

### Runs

#### Create Run

Execute the agent with input.

```http
POST /api/langgraph/threads/{thread_id}/runs
Content-Type: application/json
```

**Request Body:**
```json
{
  "input": {
    "messages": [
      {
        "role": "user",
        "content": "Hello, can you help me?"
      }
    ]
  },
  "config": {
    "recursion_limit": 100,
    "configurable": {
      "model_name": "gpt-4",
      "thinking_enabled": false,
      "is_plan_mode": false
    }
  },
  "stream_mode": ["values", "messages-tuple", "custom"]
}
```

**Stream Mode Compatibility:**
- Use: `values`, `messages-tuple`, `custom`, `updates`, `debug`, `tasks`, `checkpoints`
- Unsupported modes, including `messages`, `events`, and `tools`, return `422` before a run is created. DeerFlow never substitutes `values` for an unsupported mode.

**Run Option Compatibility:**
- Supported concurrency strategies: `reject`, `rollback`, and `interrupt`
- With durable run events configured, `rollback` and `interrupt` return `409` without mutation while a predecessor is active. Cancel that run, wait for its terminal `run.delivery`, then retry. Receiptless compatibility deployments retain legacy atomic supersession; the durable path stays fail-closed until a prepared replacement transaction can bind candidate identity, predecessor epoch, and delivery evidence together.
- Compatibility default: `if_not_exists="create"`; this matches DeerFlow's current behavior
- Artifact delivery is enforced automatically when a run creates or modifies regular files under `/mnt/user-data/outputs`. `present_files` must present at least one path produced by the current run (or a directory containing it), and the terminal receipt must be persisted; presenting only an unrelated file does not satisfy delivery. Runs without changed outputs retain ordinary conversational behavior. `artifact_delivery` is not a client-settable run option.
- Unsupported options return `422`: `webhook`, `stream_resumable=true`, `after_seconds`, `feedback_keys`, any non-null `on_completion` value (including the SDK values `"complete"` and `"continue"`), `if_not_exists="reject"`, and `multitask_strategy="enqueue"`
- `stream_resumable=false` is accepted: it is the LangGraph SDK's default and requests the non-resumable stream DeerFlow already serves
- Undeclared SDK options, including `checkpoint_during` and `durability`, also return `422` instead of being silently discarded

**Idempotent creation:**

All thread-scoped create/stream/wait routes and stateless stream/wait routes accept an
optional `Idempotency-Key` header. The value must be a non-empty UTF-8 string without control
characters. It is scoped to the authenticated server-side user/service identity (auth-disabled
mode uses DeerFlow's configured default user), so clients cannot create a shared ownerless
key space.

An equal retry returns the original run, including after success, error, timeout, or
interruption. Only the request that creates the row attaches a worker. Equality compares a
persisted canonical projection of caller intent, never a partial merge into the accepted
effective execution values. Changing or removing the bound-thread selection, agent selector,
input/command, checkpoint, multitask/interrupt settings, recursion-limit selection, or a
non-null execution-context option returns `409`. Repeating an omitted stateless thread remains
equal and reuses the generated thread, but changing that selection to an explicit thread does
not. Mapping order is irrelevant; sequence order remains significant.

For nullable model/thinking/reasoning/planning/subagent, checkpoint, and interrupt fields,
explicit null means omission. For recursion limit, omitted or null means the Gateway default;
every non-null supplied value remains distinct before server clamping. Explicit
`multitask_strategy="reject"` equals its default. Changing stream/wait route, stream mode,
subgraph streaming, disconnect behavior, or other response-delivery preferences reuses the
run because those fields are transient transport choices. Keyed requests return `422` for
non-empty arbitrary metadata or config/context fields that the canonical projector cannot
classify, rather than silently ignoring them.

An equal replay reuses the original accepted effective projection and lifecycle without
rerunning defaults, alias resolution, contributors, authorization-start, constraints, or graph
execution. Historical keyed rows that predate canonical caller-intent evidence remain readable,
but replay returns `409` because equality cannot be proven safely.

Keys through 255 UTF-8 bytes are retained exactly behind a `raw:` form marker; longer values
are represented by a SHA-256 UTF-8 digest. Replay is guaranteed only while the original run
row is retained. A different key targeting a thread with an active run keeps the existing
multitask semantics: `reject` reports the thread as busy; with durable run events,
`interrupt` and `rollback` do the same without mutating the predecessor. A retry of an
already-persisted equal key remains a read-only replay and returns its original run.

When outputs changed during the run, `run.delivery` events retain the Slice 1
facts (`presented`, `paths`, and `by_tool`) and add `produced_paths`,
`presented_paths`, `matched_paths`, plus an explicit verdict: `verification`,
`stage` (`presented`, `mismatched`, or `not_started`), and `satisfied`. Receipts
for runs without changed outputs keep their existing shape.

**Recursion Limit:**

`config.recursion_limit` caps the number of graph steps LangGraph will execute
in a single run. The unified Gateway path defaults to `100` in
`build_run_config` (see `backend/app/gateway/services.py`), which is a safer
starting point for plan-mode or subagent-heavy runs. Clients can still set
`recursion_limit` explicitly in the request body; increase it if you run deeply
nested subagent graphs. Scheduled-task launches do not take a client body: they
use `scheduler.recursion_limit` from `config.yaml` (default `1000`, matching
the web UI). For safety, the Gateway clamps any supplied
value to a configurable server ceiling (`max_recursion_limit` in `config.yaml`,
default `1000`) so a single run cannot execute unbounded graph steps (runaway
LLM cost / DoS); invalid or non-positive values fall back to the `100` default.

**Configurable Options:**
- `model_name` (string): Override the default model
- `thinking_enabled` (boolean): Enable extended thinking for supported models
- `is_plan_mode` (boolean): Enable TodoList middleware for task tracking

**Response:** Server-Sent Events (SSE) stream

```
event: values
data: {"messages": [...], "title": "..."}

event: messages
data: {"content": "Hello! I'd be happy to help.", "role": "assistant"}

event: end
data: {}
```

#### Get Run History

```http
GET /api/langgraph/threads/{thread_id}/runs
```

**Response:**
```json
{
  "runs": [
    {
      "run_id": "run123",
      "status": "success",
      "created_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

#### Stream Run

Stream responses in real-time.

```http
POST /api/langgraph/threads/{thread_id}/runs/stream
Content-Type: application/json
```

Same request body as Create Run. Returns SSE stream.

#### Stateless Stream Run

Start a conversation without creating a thread first. Gateway auto-creates a
thread when `config.configurable.thread_id` is omitted, and returns both
identifiers in the response `Content-Location` header.

```http
POST /api/langgraph/runs/stream
Content-Type: application/json
Accept: text/event-stream
```

Through Nginx, `/api/langgraph/runs/stream` is rewritten to the native Gateway
path `POST /api/runs/stream`.

**Request Body:** Same as [Create Run](#create-run). Omit `thread_id` to start a
new conversation; include it to continue an existing one:

```json
{
  "input": {
    "messages": [
      {
        "role": "user",
        "content": "Hello, can you help me?"
      }
    ]
  },
  "config": {
    "recursion_limit": 100,
    "configurable": {
      "model_name": "gpt-4",
      "thinking_enabled": false,
      "is_plan_mode": false
    }
  },
  "stream_mode": ["values", "messages-tuple", "custom"]
}
```

**Response:** Server-Sent Events (SSE) stream with a `Content-Location` header:

```http
Content-Location: /api/threads/{thread_id}/runs/{run_id}
```

Clients should parse `thread_id` and `run_id` from this header (the path ends
with `/runs/{run_id}`). Persist `thread_id` and send it back on the next turn
via `config.configurable.thread_id` to keep conversation history.

**Continuing a conversation:**

```json
{
  "input": {
    "messages": [
      {
        "role": "user",
        "content": "What did I just ask?"
      }
    ]
  },
  "config": {
    "configurable": {
      "thread_id": "abc123",
      "model_name": "gpt-4"
    }
  },
  "stream_mode": ["values", "messages-tuple", "custom"]
}
```

---

## Gateway API

Base URL: `/api`

### Models

#### List Models

Get all available LLM models from configuration.

```http
GET /api/models
```

**Response:**
```json
{
  "models": [
    {
      "name": "gpt-4",
      "display_name": "GPT-4",
      "supports_thinking": false,
      "supports_vision": true
    },
    {
      "name": "claude-3-opus",
      "display_name": "Claude 3 Opus",
      "supports_thinking": false,
      "supports_vision": true
    },
    {
      "name": "deepseek-v3",
      "display_name": "DeepSeek V3",
      "supports_thinking": true,
      "supports_vision": false
    }
  ]
}
```

#### Get Model Details

```http
GET /api/models/{model_name}
```

**Response:**
```json
{
  "name": "gpt-4",
  "display_name": "GPT-4",
  "model": "gpt-4",
  "max_tokens": 4096,
  "supports_thinking": false,
  "supports_vision": true
}
```

### MCP Configuration

#### Get MCP Config

Get current MCP server configurations.

```http
GET /api/mcp/config
```

Requires an authenticated admin session. Sensitive env/header/OAuth secret
values are masked in the response. Environment placeholders outside secret
containers are returned in their raw form so editing cannot expose or persist
their expanded values. Invalid operator-authored JSON/config shapes return
`400` instead of being reported as a Gateway fault.

**Response:**
```json
{
  "mcp_servers": {
    "github": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": {
        "GITHUB_TOKEN": "***"
      },
      "description": "GitHub operations"
    }
  }
}
```

#### Update MCP Config

Update MCP server configurations.

```http
PUT /api/mcp/config
Content-Type: application/json
```

Requires an authenticated admin session. API-managed `stdio` MCP servers may
only use allowed executable names for `command` (default: `npx`, `uvx`). Set
`DEER_FLOW_MCP_STDIO_COMMAND_ALLOWLIST` to a comma-separated list when a
deployment needs additional trusted launchers.

**Request Body:**
```json
{
  "mcp_servers": {
    "github": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": {
        "GITHUB_TOKEN": "$GITHUB_TOKEN"
      },
      "description": "GitHub operations"
    }
  }
}
```

**Response:**
```json
{
  "mcp_servers": {
    "github": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": {
        "GITHUB_TOKEN": "***"
      },
      "description": "GitHub operations"
    }
  }
}
```

#### Update One MCP Server State

Enable or disable one configured MCP server without replacing the full
extensions configuration.

```http
PATCH /api/mcp/config
Content-Type: application/json
```

Requires an authenticated admin session. Enabling a `stdio` server validates
that server's `command` against the same allowlist used by the full `PUT`
endpoint. Disabling a server does not require its command to be allowlisted, and
invalid commands on other servers do not block the update. The endpoint
preserves secrets, environment-variable placeholders, skills, custom server
fields, and other top-level extensions config. SSE/HTTP targets may use either
DeerFlow's `type` field or the MCP-spec `transport` field.

**Request Body:**
```json
{
  "server_name": "semantic-scholar",
  "enabled": false
}
```

The response is the full masked MCP configuration, matching `GET` and `PUT`.
An unknown `server_name` returns `404`; attempting to enable a server with a
disallowed `stdio` command returns `400`.

#### Add MCP Servers

Add one or more servers without replacing existing entries. The Gateway
re-reads the file under the shared configuration lock, so concurrent sibling
changes are preserved. Existing names return `409`.

```http
POST /api/mcp/config/servers
Content-Type: application/json
```

The request body uses the same `mcp_servers` map as the full `PUT` endpoint.

#### Replace One MCP Server

Completely replace one existing server while preserving sibling entries.
Omitted ordinary fields are deleted or reset; explicit `***` placeholders
restore the corresponding stored secret.

A disabled `stdio` replacement may keep a syntactically valid command outside
the allowlist for offline editing. Command-shape and code-injecting environment
variable checks still run when saving; the allowlist and executable-argument
policy run when the server is enabled.

```http
PUT /api/mcp/config/server
Content-Type: application/json
```

```json
{
  "server_name": "github",
  "server": {
    "enabled": true,
    "command": "npx",
    "args": ["-y", "@modelcontextprotocol/server-github"],
    "env": {"GITHUB_TOKEN": "***"}
  }
}
```

#### Delete One MCP Server

Delete one server without replacing sibling entries. The server name is a
path parameter and the DELETE request has no body. Percent-encode names before
placing them in the URL; the path converter also keeps legacy empty and
slash-containing names addressable.

```http
DELETE /api/mcp/config/servers/{server_name}
```

All targeted mutations return the full masked MCP configuration. Before any
write, the Gateway resolves environment variables in a copy and validates the
same expanded document the runtime will load while persisting the original raw
placeholders.

#### Reset MCP Tools Cache

Clear cached MCP tools and persistent MCP sessions process-wide. This affects
all threads and users in the current Gateway process. Tools are loaded again
from configured MCP servers on the next agent run or tool lookup.

```http
POST /api/mcp/cache/reset
```

Requires an authenticated admin session.

**Response:**
```json
{
  "success": true,
  "message": "MCP tools cache reset. Tools will reload on next use."
}
```

### Skills

#### List Skills

Get all available skills.

```http
GET /api/skills
```

**Response:**
```json
{
  "skills": [
    {
      "name": "pdf-processing",
      "display_name": "PDF Processing",
      "description": "Handle PDF documents efficiently",
      "enabled": true,
      "license": "MIT",
      "path": "public/pdf-processing"
    },
    {
      "name": "frontend-design",
      "display_name": "Frontend Design",
      "description": "Design and build frontend interfaces",
      "enabled": false,
      "license": "MIT",
      "path": "public/frontend-design"
    }
  ]
}
```

#### Get Skill Details

```http
GET /api/skills/{skill_name}
```

**Response:**
```json
{
  "name": "pdf-processing",
  "display_name": "PDF Processing",
  "description": "Handle PDF documents efficiently",
  "enabled": true,
  "license": "MIT",
  "path": "public/pdf-processing",
  "allowed_tools": ["read_file", "write_file", "bash"],
  "content": "# PDF Processing\n\nInstructions for the agent..."
}
```

#### Enable Skill

```http
POST /api/skills/{skill_name}/enable
```

**Response:**
```json
{
  "success": true,
  "message": "Skill 'pdf-processing' enabled"
}
```

#### Disable Skill

```http
POST /api/skills/{skill_name}/disable
```

**Response:**
```json
{
  "success": true,
  "message": "Skill 'pdf-processing' disabled"
}
```

#### Install Skill

Install a skill from a `.skill` file.

```http
POST /api/skills/install
Content-Type: multipart/form-data
```

**Request Body:**
- `file`: The `.skill` file to install

**Response:**
```json
{
  "success": true,
  "message": "Skill 'my-skill' installed successfully",
  "skill": {
    "name": "my-skill",
    "display_name": "My Skill",
    "path": "custom/my-skill"
  }
}
```

#### Reload Skills

Invalidate the skill prompt caches for every user in the current Gateway
process. Subsequent runs rescan the configured public, custom, and legacy skill
directories; runs that have already started keep their existing skill snapshot.

```http
POST /api/skills/reload
```

The request has no body and requires an authenticated administrator. For a
cookie-authenticated request, send the CSRF cookie value in the matching header:

```bash
curl -X POST http://localhost:2026/api/skills/reload \
  -b cookies.txt \
  -H "X-CSRF-Token: <csrf_token-cookie-value>"
```

**Response:**

```json
{
  "success": true,
  "scope": "process",
  "message": "Skill caches invalidated; subsequent runs in this Gateway process will rescan the latest skills."
}
```

`success` confirms cache invalidation, not that every file on disk was valid:
malformed skills retain the existing parser behavior of being skipped and
logged. The endpoint returns `401` for unauthenticated callers, `403` for
non-admin users, and a generic `500` if the invalidation mechanism itself
fails or the process-local background scan does not finish within the cache
refresh timeout. A loader-level failure, such as an unavailable mounted root,
does not publish an empty catalog: the last successfully loaded process cache
remains available. A timed-out scan continues in its daemon worker and can
still populate the process cache when it finishes.

The scope is deliberately process-local. Each Uvicorn worker or Kubernetes Pod
must be called directly; repeated requests through a load-balanced Service do
not guarantee that every instance is reached. External MinIO/NFS/CSI writes
bypass the validation, SkillScan, and history used by the install/edit APIs, so
the mounted directory must be writable only by trusted operators.

### File Uploads

#### Upload Files

Upload one or more files to a thread.

```http
POST /api/threads/{thread_id}/uploads
Content-Type: multipart/form-data
```

**Request Body:**
- `files`: One or more files to upload

**Response:**
```json
{
  "success": true,
  "files": [
    {
      "filename": "document.pdf",
      "size": 1234567,
      "path": ".deer-flow/threads/abc123/user-data/uploads/document.pdf",
      "virtual_path": "/mnt/user-data/uploads/document.pdf",
      "artifact_url": "/api/threads/abc123/artifacts/mnt/user-data/uploads/document.pdf",
      "markdown_file": "document.md",
      "markdown_path": ".deer-flow/threads/abc123/user-data/uploads/document.md",
      "markdown_virtual_path": "/mnt/user-data/uploads/document.md",
      "markdown_artifact_url": "/api/threads/abc123/artifacts/mnt/user-data/uploads/document.md"
    }
  ],
  "message": "Successfully uploaded 1 file(s)"
}
```

**Supported Document Formats** (auto-converted to Markdown):
- PDF (`.pdf`)
- PowerPoint (`.ppt`, `.pptx`)
- Excel (`.xls`, `.xlsx`)
- Word (`.doc`, `.docx`)

#### List Uploaded Files

```http
GET /api/threads/{thread_id}/uploads/list
```

**Response:**
```json
{
  "files": [
    {
      "filename": "document.pdf",
      "size": 1234567,
      "path": ".deer-flow/threads/abc123/user-data/uploads/document.pdf",
      "virtual_path": "/mnt/user-data/uploads/document.pdf",
      "artifact_url": "/api/threads/abc123/artifacts/mnt/user-data/uploads/document.pdf",
      "extension": ".pdf",
      "modified": 1705997600.0
    }
  ],
  "count": 1
}
```

#### Delete File

```http
DELETE /api/threads/{thread_id}/uploads/{filename}
```

**Response:**
```json
{
  "success": true,
  "message": "Deleted document.pdf"
}
```

### Thread Cleanup

Remove DeerFlow-managed local thread files under `.deer-flow/threads/{thread_id}` after the LangGraph thread itself has been deleted.

```http
DELETE /api/threads/{thread_id}
```

**Response:**
```json
{
  "success": true,
  "message": "Deleted local thread data for abc123"
}
```

**Error behavior:**
- `422` for invalid thread IDs
- `500` returns a generic `{"detail": "Failed to delete local thread data."}` response while full exception details stay in server logs

### Artifacts

#### Get Artifact

Download or view an artifact generated by the agent.

```http
GET /api/threads/{thread_id}/artifacts/{path}
```

**Path Examples:**
- `/api/threads/abc123/artifacts/mnt/user-data/outputs/result.txt`
- `/api/threads/abc123/artifacts/mnt/user-data/uploads/document.pdf`

**Query Parameters:**
- `download` (boolean): If `true`, force download with Content-Disposition header

**Response:** File content with appropriate Content-Type

---

### Durable inbound receipt operations

These administrator-only routes expose bounded operations for PostgreSQL-backed
native ingress. Browser mutations use the normal CSRF protection.

| Route | Contract |
|---|---|
| `GET /api/channels/inbound-receipts/summary` | Capped counts by finite receipt state and indexed oldest-due age; never enumerates receipt envelopes. |
| `GET /api/channels/inbound-receipts/{receipt_id}` | Exact unresolved dead-letter evidence with bounded digests, counters, and timestamps; never returns message text, binding reference, or provider delivery ID. |
| `POST /api/channels/inbound-receipts/{receipt_id}/requeue` | Exact CAS from runless `dead_letter` to `deferred`, then a best-effort receipt-ID wake-up. |
| `POST /api/channels/inbound-receipts/{receipt_id}/discard` | Exact CAS from runless `dead_letter` to `completed` with `operator_discarded`; no processing wake-up is emitted. |

Both mutations require the expected fencing token, payload digest, and an
explicit provider-event digest. A JSON `null` is accepted only to match a legacy
row whose database event digest is SQL `NULL`; omitting the field is invalid.
Concurrent, stale, already-bound, or otherwise ineligible mutations return a
bounded conflict. Discard preserves the retained envelope only through the
ordinary completed-row forensic window, after which normal bounded retention may
remove it. There is no list, bulk-requeue, bulk-discard, or raw-payload route.

---

### Durable subagent batch operations

Base URL: `/api/threads/{thread_id}/subagent-batches`

These routes are available when `subagent_batches.enabled` starts a SQL-backed
worker. All lookups require the authenticated owner and server-owned tenant; an
invisible batch returns `404`. Batch creation is model-initiated through
`batch_task` inside an accepted durable parent tool attempt, not through an HTTP
create route.

| Route | Contract |
|---|---|
| `GET /api/threads/{thread_id}/subagent-batches` | List up to 100 owner-scoped batch projections. |
| `GET /api/threads/{thread_id}/subagent-batches/{batch_id}` | Return safe aggregate status, counts, immutable evidence anchors, and terminal code. |
| `GET /api/threads/{thread_id}/subagent-batches/{batch_id}/items` | Page item projections; optional finite `status` filter. Raw results are omitted. |
| `GET /api/threads/{thread_id}/subagent-batches/{batch_id}/attempts` | Return at most 100 payload-free attempt evidence records, optionally for one item. |
| `GET /api/threads/{thread_id}/subagent-batches/{batch_id}/observations` | Return at most 100 `batch.accepted`, item-attempt transition, and `batch.terminal` observations. |
| `POST /api/threads/{thread_id}/subagent-batches/{batch_id}/pause` | Stop new claims without revoking an active lease. |
| `POST /api/threads/{thread_id}/subagent-batches/{batch_id}/resume` | Make paused work claimable again. |
| `POST /api/threads/{thread_id}/subagent-batches/{batch_id}/cancel` | Persist cancellation, increment its fence, and reject stale completions; returns `503` if no worker is running. |
| `POST /api/threads/{thread_id}/subagent-batches/{batch_id}/items/{item_id}/retry` | Requeue an owner-scoped failed item without resetting its accepted attempt budget; returns `409` when the item is ineligible or the budget is exhausted. |
| `GET /api/threads/{thread_id}/subagent-batches/{batch_id}/results.jsonl` | Stream results through the protected owner-authorized channel. |

Attempt and lifecycle routes never return prompts, results, tool arguments,
exception text, credentials, worker names, or provider handles. Result export is
separate because model output is operational data, not lifecycle evidence.
Parent-run cancellation does not cascade into a batch in this release.

See [Evidence-bound durable subagent batches](../../docs/DURABLE_SUBAGENT_BATCHES.md)
for admission, retry, cancellation, limits, and qualification semantics.

---

## Durable Invocation Runtime API

Base URL: `/api/runtime/v1`

This transport publishes the strict `deerflow.runtime/v1` contract used by the
embedded runtime adapter. All routes use current Gateway authentication and
permissions; invocation and context reads retain owner/admin visibility, and
unknown and invisible resources both return `not_found_or_invisible`. Browser
`POST` requests also require the normal CSRF cookie/header pair.

The standard-library-only package exports `DurableInvocationPort`, the shared
Protocol implemented by the embedded and HTTP adapters. Its records are
transitively immutable defensive snapshots; parsing freezes every nested JSON
container, while each `to_dict()` call returns a new mutable JSON wire copy.

| Route | Contract |
|---|---|
| `GET /capabilities` | Administrator-only strict `runtime.capabilities`; reports only portable ensure, invocation/context observation, cancel control, and unsupported context export/retirement. |
| `GET /deployment` | Administrator-only `deerflow.deployment/v1` report with extension manifest/health, latest safe admission-readiness reason codes/correlation, bounded image/source provenance when supplied, persistence facts, and an operator-asserted qualification reference or explicit `unqualified` state. This is not part of `DurableInvocationPort` and is not remote attestation. |
| `POST /invocations/ensure` | Exact `invocation.ensure` body. `external_key` is required; the server derives its scope from the authenticated principal or service. |
| `GET /invocations/{run_id}` | Access-filtered authoritative snapshot and lifecycle page. Optional `cursor`; `limit` defaults to 100 and must be 1–500. Optional `include_tool_receipts=true`, `include_mcp_tasks=true`, and `include_subagent_batches=true` add independently paged bounded receipt, MCP-child, and payload-free batch-lifecycle projections. Each auxiliary limit is 1–100 and each cursor is scoped to the visible run plus its owner/tenant where applicable. |
| `GET /contexts/{thread_id}/invocations` | Access-filtered normal-run lifecycle page. Optional `cursor`; `limit` defaults to 100 and must be 1–500; optional `source_kind` is `http|scheduled_task|native_channel|service`. |
| `POST /invocations/{run_id}/control` | Exact `invocation.cancel` body with required `expected_state_version`; body and path run IDs must match. |

Every request record includes `api_version="deerflow.runtime/v1"` and its exact
`kind`; unknown versions, kinds, fields, or nested input/options are rejected.
Ensure accepts only an external key, thread ID, nullable agent hint, strict
graph/resume input, and finite invocation options. It does not accept an
external scope, principal, Origin, raw config/context/metadata, callbacks,
credentials, commands, or delivery/stream properties.

The exact ensure body is:

```json
{
  "api_version": "deerflow.runtime/v1",
  "kind": "invocation.ensure",
  "external_key": "source-delivery-id",
  "thread_id": "thread-123",
  "agent_hint": null,
  "input": {
    "api_version": "deerflow.runtime/v1",
    "kind": "invocation.input.graph",
    "value": {"messages": []}
  },
  "options": {
    "api_version": "deerflow.runtime/v1",
    "kind": "invocation.options",
    "model_name": null,
    "thinking_enabled": null,
    "multitask_strategy": "reject",
    "checkpoint_id": null,
    "interrupt_before": null,
    "interrupt_after": null
  }
}
```

`input.kind` is exactly `invocation.input.graph` with an object value or
`invocation.input.resume` with any finite JSON value. Multitask strategy is
`reject|rollback|interrupt`; interrupt selectors are a string list, `"*"`, or
null. Observation is represented by `invocation.query` (`run_id`) or
`context.invocations.query` (`thread_id`) plus nullable cursor, limit,
`include_snapshot`, and strict nullable `source_kind`. HTTP supplies the path
identity and fixes `include_snapshot=true`; invocation paging accepts `cursor`
and `limit`, plus additive `include_tool_receipts`, `tool_receipt_cursor`, and
`tool_receipt_limit` fields, additive `include_mcp_tasks`, `mcp_task_cursor`,
and `mcp_task_limit` fields, and additive `include_subagent_batches`,
`subagent_batch_cursor`, and `subagent_batch_limit` fields. HTTP accepts exact
lowercase `true|false`; an auxiliary cursor without its matching inclusion
flag, duplicate parameters, an empty cursor, or an auxiliary limit outside
1–100 is `422 invalid_request`.
Context paging additionally accepts `source_kind` but cannot request receipt,
MCP-child, or batch-child pages. Control accepts exactly:

```json
{
  "api_version": "deerflow.runtime/v1",
  "kind": "invocation.cancel",
  "run_id": "run-123",
  "expected_state_version": 4,
  "action": "interrupt"
}
```

`action` is `interrupt|rollback`, and the path/body run IDs must match.
Visible ensure/control receipts carry `run_id`, `thread_id`, `status`, and
`state_version`. Observations carry fixed snapshots/events, typed immutable
`invocation.summary.v1` records, and all three cursor values; auxiliary rows
never enter any collection. Each summary is joined from its accepted normal run
and contains only run/thread/current state, source kind, bounded safe Origin
correlation references, agent/extension identity, and acceptance evidence
digests. It excludes model input, secrets, secret handles, private policy
reasons, and unbounded context. A pre-Origin historical row remains readable
but has no summary.

When explicitly requested for one visible run, an observation also carries a
strict `tool_receipts` page. Each item includes its full `tr_<sha256>` identity,
stable lead/subagent task scope, subagent name when applicable, tool name,
attempt, `succeeded|failed|denied|cancelled|indeterminate` status, start/finish
store timestamps, request/result projection digests, safe result/error kind,
bounded policy decision references, and the accepted revision/assembly/
extension/catalog/definition anchors. The page contains at most 100 items and
has independent `next_cursor`, nullable `pruned_before`, `evidence_status`, and
`invalid_event_count` fields. Old runs return `legacy_unavailable`; malformed
receipt events are not reflected and make the page `invalid`. A start with no
terminal event is `indeterminate`, including a process-loss gap.

Receipt evidence never contains raw tool arguments, results, provider messages,
headers, stack traces, or credential-bearing URLs. Request digests use field
names/types, classified secret handles, length/type markers, and only bounded
server-declared evidence-safe scalar values. Result digests cover the exact
sanitized and output-budgeted model-visible result plus type/status. Digests are
comparison commitments, not encryption, confidentiality, or truth evidence.
A durable receipt records HartMesh's observation of a tool attempt. It does not
guarantee an external side effect occurred exactly once or that the tool result
was correct.

When explicitly requested for one visible invocation, `mcp_tasks` is a separate
page containing only task ID, lineage digest, submitting execution-task/receipt
IDs, safe server/tool names, status and safe terminal code, notification run ID,
timestamps, `next_cursor`, and `pruning_status`. The join is one bounded indexed
query after parent visibility and current observation authorization succeed; it
does not fetch one row per task. Cursors cannot be moved between tenants, owners,
or parent runs. Parent cancellation does not cancel these remote tasks.

When explicitly requested for one visible invocation, `subagent_batches` is a
separate parent-linked page. It carries immutable acceptance and tool-receipt
references, safe current status, timestamps, and bounded typed lifecycle
observations. It never carries prompts, model output, tool arguments,
credentials, worker identities, or provider handles. Its cursor is scoped to
the server tenant, owner, and accepted parent run.

The thread-scoped durable-task API is outside the `/api/runtime/v1` base:

| Route | Contract |
|---|---|
| `GET /api/threads/{thread_id}/mcp-tasks` | Owner-authorized bounded current-task list with a safe lineage summary. |
| `POST /api/threads/{thread_id}/mcp-tasks` | Owner-authorized standalone task creation. Accepts server/task-toolset names, arguments, and idempotency key; provenance-shaped extras are ignored and all lineage fields are server-derived. A private versioned HMAC commitment enforces exact replay equality without persisting raw arguments. |
| `GET /api/threads/{thread_id}/mcp-tasks/{task_id}` | Owner-authorized detail with bounded lineage and result fields. Parent execution/receipt/evidence fields and parent/notification links are included only after independent run authorization; private replay commitments are never returned. |
| `POST /api/threads/{thread_id}/mcp-tasks/{task_id}/cancel` | Durably requests remote cancellation; it does not modify immutable lineage, and the first request records separate pseudonymous actor attribution plus a fixed reason code. |

Agent-created lineage is classified `agent_tool` and binds the accepted parent
run/task/receipt/evidence anchors. HTTP-created lineage is `standalone_api` and
has no parent fields. Existing pre-lineage rows report `legacy_unavailable`.
Both submission paths use configured MCP call preparation; required preparation
that depends on accepted Agent invocation facts fails standalone submission closed
before network dispatch because standalone lineage deliberately has no parent run.
Unknown or unauthorized task/thread/tenant combinations use the existing
not-found behavior, and an unauthorized linked run is omitted rather than
reported as an error.

Accepted durable summaries include a nullable `assembly_evidence` object with
only `version`, `fingerprint`, `effective_model`, `prompt_digest`,
`toolset_digest`, `middleware_digest`, `skillset_digest`, and `policy_digest`.
`assembly_evidence_status` is `pending`, `verified`, or
`legacy_unavailable`. The server returns `verified` only after strict V1 parsing
and canonical digest revalidation; partial or corrupt storage is returned as
null/unavailable without reflecting stored content. The record identifies the
graph HartMesh assembled and admitted, not a cryptographic code attestation.

Ensure uses the existing durable idempotency boundary. Its strict v1 record always carries the
complete option record: null model/thinking/checkpoint/interrupt values mean omission, while
the serialized `multitask_strategy="reject"` is the defaulted caller intent. Object-key order
does not affect equality; array order does. A new accepted request returns `201 created`; an
equal retained caller intent returns `200 known` without a second worker; a changed or removed
intent field returns `409 conflict`; and an independently busy thread returns
`409 thread_busy`. Transport details outside the strict DTO do not participate. The accepted
effective execution projection is retained separately and reused on replay. Equal replay does
not rerun contributors, authorization, constraints, default resolution, agent/profile routing,
or model execution. This refers to start/admission authorization; current observe authorization
still applies before a retained row is revealed. The guarantee lasts while the retained normal
run row exists. Auxiliary operation rows are never visible.

Observation pages contain `next_cursor`, `minimum_available_cursor`, and
`read_fence_cursor`. Cursor tokens are opaque. Empty filtered pages advance to
the captured read fence; a pruned cursor returns `410 cursor_gap` with
`minimum_available_cursor`, while malformed and ahead cursors return `422`.
Reads are at least once, so consumers should deduplicate stable event IDs and
cursors. Cursor metadata, returned events, and their summaries share one SQL
snapshot. Context pages fetch summaries only for distinct run IDs in the
bounded event page, not every run in the thread. Limits are 500 events per page,
4 KiB canonical JSON per lifecycle payload, 16 KiB per summary, and 12 MiB for
the full portable observation; the independent tool-receipt page is capped at
100 items and each canonical receipt event body at 8 KiB.

Lifecycle event types are exactly `accepted`, `started`,
`cancellation_requested`, `cancelled`, `succeeded`, `failed`, `timed_out`, and
`interrupted`. Each successful mutation increments the normal run's
`state_version` and commits its matching safe event atomically; the run row,
not the journal, remains authoritative.

Polling either observation route is the supported durable evidence path; cursor polling of the
transactional lifecycle rows is authoritative, and a push sink is at most optional
at-least-once acceleration. A clarification request completes its current invocation
successfully, and the answer starts a new invocation on the same DeerFlow thread, reusing that
thread's checkpoints, memory, workspace, and conversation context. The v1 lifecycle does not
define `input_required` or same-invocation suspension/resumption.

Durability starts at committed invocation acceptance, or at the earlier PostgreSQL native
receipt commit for a source that explicitly reports durable ingress. It does not promise
exactly-once model execution, process-resumable execution, provider/bus delivery before a
durable receipt, outbound provider delivery, rollback of external side effects, or
multi-replica execution ownership. See `INVOCATION_RUNTIME.md` for the complete boundary table.

Success status mapping is `201` for `created`, `202` for cancellation
`requested`, and `200` for `known`, observations, capabilities,
`already_requested`, and `already_terminal`. Failures use `403 denied` only
after an authenticated visible-resource decision, `404
not_found_or_invisible`, `409 conflict|thread_busy|stale`, `410 cursor_gap`,
`422 invalid_request|cursor_ahead`, or `503 indeterminate`.

Unlike legacy Gateway endpoints, every non-2xx runtime response—including auth
and CSRF middleware rejection—uses only this envelope:

```json
{
  "api_version": "deerflow.runtime/v1",
  "kind": "runtime.error",
  "code": "invalid_request"
}
```

Cursor-gap/ahead responses may add their allowlisted cursor detail. An unexpected
Adapter failure may add only a bounded correlation identifier; the matching
internal log contains that identifier and safe operation context, never a public
exception message. The transport never returns policy objects, exception text,
private Origin data, secrets, or a free-form property bag.

Portable capabilities are transport-identical: HTTP emits the exact strict record
that the in-process Adapter returns. Deployment facts never appear in that record.
The separate `GET /deployment` report exposes the host-owned immutable capability
manifest/digest, separately labelled mutable health, optional bounded build/image
identifiers, and persistence/qualification truth. When the Gateway runtime supplies
it, the optional versioned `post_commit_obligations` object reports process-local
pending admission and auxiliary-release counts plus compensator-proven
resolved-since-start counts.
`quarantined_identities` overlaps those pending types and is not additive. Every
counter is saturated, resets on process restart, and is operational state rather than
durable lifecycle or multi-replica evidence. The v1 readiness reason
`admission_compensation_pending` is retained for compatibility and covers every
post-commit ownership obligation. `process_local` survives neither
restart nor pod loss; `node_durable` survives process restart on its node; and
`shared_durable` uses the configured shared PostgreSQL store. `atomic_lifecycle` is
reported independently because an in-memory store can be atomic without being
restart-durable. `deployment.profile: durable_production` refuses process-local
state and requires `run_events.backend: db` for fenced idempotent tool receipts
at startup and readiness; `local_development` permits memory/JSONL evidence
without claiming durability. Qualification remains `unqualified` with
`trust="none_declared"` unless a
reference is explicitly supplied. A supplied reference retains v1
`status="qualified"` but is labelled `trust="operator_asserted"`; it is not independently
verified by the Gateway. Live health never changes the manifest digest or an
invocation's accepted generation. There is no context export, context retirement,
event broker, or additional control in v1.

Trusted deployers may stamp `DEER_FLOW_IMAGE_REFERENCE`,
`DEER_FLOW_IMAGE_DIGEST`, `DEER_FLOW_SOURCE_REVISION`, and bounded
`DEER_FLOW_QUALIFICATION_EVIDENCE` JSON. Qualification evidence is a finite list
of exact ID, SHA-256 artifact digest, and RFC3339 completion-time records. New
scoped records additionally carry a bounded scope and exact `passed` state;
legacy three-field records remain readable as `legacy_unspecified`. The opt-in
real-pod suite uses scope `durable_one_replica_pod_recovery` and supplies it only
after all required scenarios pass. Collection or default skip cannot manufacture a
reference. Invalid input is ignored with a safe server diagnostic. The Helm chart validates
and supplies these fields from its non-secret deployment values; they never enter portable
capabilities. Exact verification is an offline operation: the operator supplies the artifact
independently and runs `backend/scripts/verify_qualification_evidence.py` with the report
digest plus expected qualification ID, image/chart/config/schema, namespace, scope, and
required scenarios. Its only successful trust state is
`external_evidence_verified`; it performs no network fetch.

`GET /health` is independent process liveness. Unauthenticated `GET /ready`
returns the overall `status`, the safe tenant-identity projection, and—when
selected—the safe optional contextual-memory projection. A degraded optional
Honcho backend is reported without changing overall readiness and is never
described as a durable dependency. The overall status uses the same bounded proof that fences
genuinely new invocation admission: current-generation fresh health for every required
authority capability, bounded lifecycle singleton/pruning/event-edge integrity, database
availability, and the configured persistence profile. Accepted keyed replay is resolved
before that fence and reuses its sealed evidence. Startup-only `deployment.readiness`
configures the cache/admission/staleness windows and per-probe/overall deadlines.

---

## Error Responses

Legacy APIs return errors in this format (the durable runtime namespace uses
the versioned `runtime.error` envelope documented above):

```json
{
  "detail": "Error message describing what went wrong"
}
```

**HTTP Status Codes:**
- `400` - Bad Request: Invalid input
- `404` - Not Found: Resource not found
- `422` - Validation Error: Request validation failed
- `500` - Internal Server Error: Server-side error

---

## Authentication

DeerFlow supports four HTTP identity sources. They share the same thread/run isolation rules but differ in whether a row is created in `users` and how external identities are mapped. See [AUTH_DESIGN.md](AUTH_DESIGN.md) for the full design.

| Model | Entry | `users` table | Isolation key |
|---|---|---|---|
| Browser session | `access_token` cookie after login/register | Yes | `users.id` |
| OIDC / SSO | OAuth callback → cookie | Yes | `users.id` (see [SSO.md](SSO.md)) |
| IM channel binding | Connect code + `channel_connections` | Bound to registered user | `channel_connections.owner_user_id` |
| **Internal Auth** | `X-DeerFlow-Internal-Token` + `X-DeerFlow-Owner-User-Id` | **No** | Owner string on `threads_meta.user_id` |

**IM channel binding** and **Internal Auth** are both *platform-trust* integrations: DeerFlow trusts the channel/platform to authenticate end users. IM bindings persist the mapping in `channel_connections` / `channel_conversations` and require a DeerFlow `users` row. Internal Auth lets a platform call the Gateway API directly with a deployment-shared token and a per-request owner header—no `users` row, but thread/run/checkpoint isolation works the same way.

### Browser session (default)

DeerFlow enforces authentication for all non-public HTTP routes. Public routes are limited to health/docs metadata and these public auth endpoints:

- `POST /api/v1/auth/initialize` creates the first admin account when no admin exists.
- `POST /api/v1/auth/login/local` logs in with email/password and sets an HttpOnly `access_token` cookie.
- `POST /api/v1/auth/register` creates a regular `user` account and sets the session cookie.
- `POST /api/v1/auth/logout` clears the session cookie.
- `GET /api/v1/auth/setup-status` reports whether the first admin still needs to be created.

The authenticated auth endpoints are:

- `GET /api/v1/auth/me` returns the current user.
- `POST /api/v1/auth/change-password` changes password, optionally changes email during setup, increments `token_version`, and reissues the cookie.

Protected state-changing requests also require the CSRF double-submit token: send the `csrf_token` cookie value as the `X-CSRF-Token` header. Login/register/initialize/logout are bootstrap auth endpoints: they are exempt from the double-submit token but still reject hostile browser `Origin` headers.

User isolation is enforced from the authenticated user context:

- Thread metadata is scoped by `threads_meta.user_id`; search/read/write/delete APIs only expose the current user's threads.
- Thread files live under `{base_dir}/users/{user_id}/threads/{thread_id}/user-data/` and are exposed inside the sandbox as `/mnt/user-data/`.
- Memory and custom agents are stored under `{base_dir}/users/{user_id}/...`.

Note: MCP outbound connections can still use OAuth for configured HTTP/SSE MCP servers; that is separate from DeerFlow API authentication.

### Internal Auth (platform HTTP integration)

For server-to-server integrations (e.g. a Feishu or WeCom/Enterprise WeChat bot backend), configure:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="<long-random-secret>"
```

| Header | Required | Description |
|---|---|---|
| `X-DeerFlow-Internal-Token` | Yes | Must match `DEER_FLOW_INTERNAL_AUTH_TOKEN`; missing/invalid → `401` |
| `X-DeerFlow-Owner-User-Id` | Yes for per-user isolation | Platform user id (e.g. `feishu_ou_alice`, `wecom_user_bob`); omit → `default` bucket |

Does **not** use browser cookies or CSRF tokens. Does **not** insert into `users`; sets `threads_meta.user_id` / `runs.user_id` from the owner header. DeerFlow validates only the platform token—not whether the owner id represents a real end user; user validity is entirely the platform's responsibility. See [AUTH_DESIGN.md — Internal Auth](AUTH_DESIGN.md#internal-auth-direct-http) for trust boundaries, persistence, and security notes.

Use the standard Gateway thread/run endpoints (`POST /api/threads`, `POST /api/threads/{thread_id}/runs/stream`, etc.) with the headers above on every request.

---

## Rate Limiting

No rate limiting is implemented by default. For production deployments, configure rate limiting in Nginx:

```nginx
limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;

location /api/ {
    limit_req zone=api burst=20 nodelay;
    proxy_pass http://backend;
}
```

---

## Streaming Support

Gateway's LangGraph-compatible API streams run events with Server-Sent Events (SSE).

**Thread-scoped streaming** (thread must exist):

```http
POST /api/langgraph/threads/{thread_id}/runs/stream
Accept: text/event-stream
```

**Stateless streaming** (no pre-created thread; Gateway auto-creates one):

```http
POST /api/langgraph/runs/stream
Accept: text/event-stream
```

Both endpoints return `Content-Location: /api/threads/{thread_id}/runs/{run_id}`.
The DeerFlow web UI and LangGraph SDK clients rely on this header to discover the
assigned `thread_id` and `run_id` on the first message of a new chat.

### SSE replay retention and gaps

Clients may reconnect to a run stream with `Last-Event-ID`. Replay history is
bounded by `stream_bridge.queue_maxsize` (default `256`) and, for Redis, by the
rolling `stream_ttl_seconds`. A retained cursor resumes after that event with no
additional control frame.

When a syntactically valid cursor is older than the retained watermark, the
server sends exactly one `gap` event before any retained data and closes that
subscription without an `end` event:

```text
event: gap
data: {"code":"stream_replay_gap","run_id":"run-123","requested_event_id":"1718000000000-1","earliest_available_event_id":"1718000000100-42","latest_available_event_id":"1718000000200-84","recovery":"reload_durable_state"}

```

The frame deliberately has no SSE `id:`. Both `earliest_available_event_id` and
`latest_available_event_id` are `string | null` (they are `null` when no events
are retained in the buffer). Consumers must reload durable thread state and
persisted run events/messages, then may reconnect from `latest_available_event_id`
to follow newer live events, or rejoin without a cursor when the buffer is empty
(`latest_available_event_id` is `null`). A gap does not cancel the active run.
The same signal applies when a no-cursor subscriber has already established an
empty-stream wait but the first Redis wake-up falls behind before delivery; in
that case `requested_event_id` is `null`. Malformed cursor handling is
backend-specific and is not the same as a valid cursor that was evicted.

---

## SDK Usage

### Python (LangGraph SDK)

```python
from langgraph_sdk import get_client

client = get_client(url="http://localhost:2026/api/langgraph")
run_meta: dict[str, str] = {}


def on_run_created(meta) -> None:
    # langgraph-sdk 0.3.x parses Content-Location only when this callback is set.
    if meta.thread_id:
        run_meta["thread_id"] = meta.thread_id
    run_meta["run_id"] = meta.run_id


# Option A: stateless stream — no thread pre-creation
# Gateway auto-creates a thread and returns thread_id/run_id in Content-Location.
async for event in client.runs.stream(
    None,
    "lead_agent",
    input={"messages": [{"role": "user", "content": "Hello"}]},
    config={"configurable": {"model_name": "gpt-4"}},
    stream_mode=["values", "messages-tuple", "custom"],
    on_run_created=on_run_created,
):
    print(event)

thread_id = run_meta["thread_id"]  # persist before the next turn

# Option A (continued): same thread on the next turn
async for event in client.runs.stream(
    None,
    "lead_agent",
    input={"messages": [{"role": "user", "content": "What did I just ask?"}]},
    config={"configurable": {"thread_id": thread_id, "model_name": "gpt-4"}},
    stream_mode=["values", "messages-tuple", "custom"],
    on_run_created=on_run_created,
):
    print(event)

# Option B: thread-scoped stream — create thread first, then stream
thread = await client.threads.create()
async for event in client.runs.stream(
    thread["thread_id"],
    "lead_agent",
    input={"messages": [{"role": "user", "content": "Hello"}]},
    config={"configurable": {"model_name": "gpt-4"}},
    stream_mode=["values", "messages-tuple", "custom"],
    on_run_created=on_run_created,
):
    print(event)
```

### JavaScript/TypeScript

```typescript
// Using fetch for Gateway API
const response = await fetch('/api/models');
const data = await response.json();
console.log(data.models);

function parseRunLocation(contentLocation: string | null) {
  if (!contentLocation) return null;
  const match = /\/threads\/([^/]+)\/runs\/([^/]+)/.exec(contentLocation);
  if (!match) return null;
  return { threadId: match[1], runId: match[2] };
}

// Option A: stateless stream — no thread pre-creation
let threadId: string | undefined;
const firstResponse = await fetch("/api/langgraph/runs/stream", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
  },
  body: JSON.stringify({
    input: { messages: [{ role: "user", content: "Hello" }] },
    stream_mode: ["values", "messages-tuple", "custom"],
  }),
});

const created = parseRunLocation(firstResponse.headers.get("Content-Location"));
threadId = created?.threadId;
console.log("thread_id:", created?.threadId, "run_id:", created?.runId);

// Option B: continue the same thread on the next turn
const followUpResponse = await fetch("/api/langgraph/runs/stream", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
  },
  body: JSON.stringify({
    input: { messages: [{ role: "user", content: "What did I just ask?" }] },
    config: { configurable: { thread_id: threadId } },
    stream_mode: ["values", "messages-tuple", "custom"],
  }),
});

// Option C: thread-scoped stream when you already have a thread_id
const streamResponse = await fetch(`/api/langgraph/threads/${threadId}/runs/stream`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
  },
  body: JSON.stringify({
    input: { messages: [{ role: "user", content: "Hello" }] },
    stream_mode: ["values", "messages-tuple", "custom"],
  }),
});

const reader = streamResponse.body?.getReader();
// Decode and parse SSE frames from reader in your client code.
```

### cURL Examples

```bash
# List models
curl http://localhost:2026/api/models

# Get MCP config
curl http://localhost:2026/api/mcp/config

# Upload file
curl -X POST http://localhost:2026/api/threads/abc123/uploads \
  -F "files=@document.pdf"

# Enable skill
curl -X POST http://localhost:2026/api/skills/pdf-processing/enable

# Stateless stream — no thread pre-creation
curl -s -D - -N -X POST http://localhost:2026/api/langgraph/runs/stream \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "input": {"messages": [{"role": "user", "content": "Hello"}]},
    "config": {
      "recursion_limit": 100,
      "configurable": {"model_name": "gpt-4"}
    },
    "stream_mode": ["values", "messages-tuple", "custom"]
  }'
# Read Content-Location: /api/threads/{thread_id}/runs/{run_id} from the headers.

# Continue the same thread on the next turn
curl -s -N -X POST http://localhost:2026/api/langgraph/runs/stream \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "input": {"messages": [{"role": "user", "content": "What did I just ask?"}]},
    "config": {
      "configurable": {"thread_id": "abc123", "model_name": "gpt-4"}
    },
    "stream_mode": ["values", "messages-tuple", "custom"]
  }'

# Thread-scoped flow — create thread first, then stream
curl -X POST http://localhost:2026/api/langgraph/threads \
  -H "Content-Type: application/json" \
  -d '{}'

curl -X POST http://localhost:2026/api/langgraph/threads/abc123/runs/stream \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "input": {"messages": [{"role": "user", "content": "Hello"}]},
    "config": {
      "recursion_limit": 100,
      "configurable": {"model_name": "gpt-4"}
    },
    "stream_mode": ["values", "messages-tuple", "custom"]
  }'
```

> The unified Gateway path defaults `config.recursion_limit` to 100 for
> plan-mode and subagent-heavy runs. Clients may still set
> `config.recursion_limit` explicitly — see the [Create Run](#create-run)
> section for details. Scheduled-task launches use
> `scheduler.recursion_limit` from `config.yaml` instead of a client body.

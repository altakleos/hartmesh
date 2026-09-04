# Honcho memory backend

Honcho is an optional remote user-model memory backend for DeerFlow. It stores
filtered conversation turns and uses Honcho's server-side deriver to build
cross-session working representations; DeerFlow makes no local LLM call for
that derivation.

Honcho supplies mutable contextual memory. It is tenant- and user-scoped, but it is not HartMesh's source of truth for admission, checkpoints, invocation status, authorization, or audit evidence.

## Configuration

```yaml
deployment:
  profile: durable_production
  tenant_id: customer-a

memory:
  enabled: true
  injection_enabled: true
  manager_class: honcho
  mode: middleware            # or tool for query-aware search
  backend_config:
    base_url: https://api.honcho.example
    api_key: $HONCHO_API_KEY
    assistant_peer: deerflow
    message_char_limit: 8000
    max_injection_chars: 6000
    timeout_seconds: 10
    connect_timeout_seconds: 3
    failure_policy:
      read: fail_open          # fail_open | fail_closed
```

The Gateway owns `_hartmesh_tenant` and injects it from its startup-frozen
`TenantIdentityV1`. File/API configuration cannot select or replace that key.
Do not add it to `config.yaml`. `durable_production` refuses Honcho when the
host projection is absent or malformed.

An API key requires HTTPS in production. Plain HTTP with a key requires
`allow_insecure_http: true` and is only appropriate for explicit local
development. URLs containing credentials, a query, or a fragment are rejected.
Connectivity is deliberately not probed during startup.

## Tenant and user isolation

For Gateway-created managers, the host derives the workspace namespace from
`TenantIdentityV1.namespace(HONCHO)` and appends `-`. The portable backend then
resolves:

```text
workspace = <tenant Honcho namespace>-<sanitized user>-<16 hex SHA-256 suffix>
user peer = hm-u-<12 tenant digest hex>-<stable user>
assistant = hm-a-<12 tenant digest hex>-<stable configured assistant name>
session   = hm-s-<12 tenant digest hex>-<stable thread>
```

Digest space is reserved before readable text is truncated, and every result
is checked against Honcho's 100-character identifier bound. Thus identical
users/threads in two tenants and distinct raw identifiers that sanitize the
same way remain disjoint. A missing or empty user ID fails closed before any
HTTP request: writes are no-ops and reads are empty.

The readable sanitized components are identifiers visible to the external
Honcho service. Do not use secrets as user IDs, thread IDs, assistant names, or
legacy workspace names. Honcho also receives the bounded message content it is
configured to remember; treat the provider and its retention controls as part
of the deployment's data-processing boundary.

### Legacy overrides

- `workspace_prefix` is deprecated. With a host tenant projection it is dropped
  with a warning and the derived namespace is used, so an inherited upstream
  value cannot block startup. Memory written under a previous prefix is not
  migrated and is not read; see
  [Existing-workspace migration](#existing-workspace-migration) to move it.
- In production, every `workspace_overrides[user]` value must exactly equal
  that user's derived workspace. Escaping or sharing a workspace is rejected.
- `user_peer_overrides` must be grammar-safe, bounded, unique for configured
  users, retain the `hm-u-<tenant-prefix>-` namespace, and never collide with
  the reserved assistant peer.
- Local shared workspaces require all three conditions: the
  `local_development` profile, an actual custom override, and
  `allow_local_shared_workspaces: true`. Startup logs a warning and health
  reports `local_explicit_shared`. There is no production escape hatch.

## Recall, observations, and failure behavior

Middleware recall uses `get_context`; tool mode adds workspace-scoped `search`
and retains passive writes because Honcho learns from `add`. Search projections
are capped at 100 items and all injected text is bounded by
`max_injection_chars`. Writes bound each message with `message_char_limit`.

For an accepted durable run, each read/write appends a
`memory.observation.v1` event containing only the safe tenant reference, a
hashed workspace reference, operation, status, digest of the exact bounded
read projection when one exists, item count, truncation flag, and timestamp.
Write observations carry no content digest. The event stores no memory
text, query, raw user/workspace ID, provider response, or exception message.
Separate retries produce separate observations because Honcho is mutable; the
digest is evidence of what HartMesh observed, not deterministic replay proof.
Legacy and ordinary non-durable calls have no trusted observation binding and
continue without claiming that memory was or was not used.

The durable path acknowledges the actual run-event-store write before using a
read projection. Middleware candidates are staged until the offloaded
injection finishes within its deadline; a timeout discards the stage, so a
late Honcho response cannot create evidence for content that was never
injected.

`failure_policy.read: fail_open` (default) records a safe failed-open status
when possible and continues with empty context. `fail_closed` raises the stable
`honcho_memory_recall_failed` error. If a successful durable read cannot append
its observation, fail-open discards the returned content and fail-closed
aborts; durable production never injects unobserved Honcho content. Provider
errors are reduced to stable codes without URL, headers, response body, query,
workspace, or API-key detail.

The client is synchronous for compatibility with `MemoryManager`, but every
async manager entrypoint runs it through `asyncio.to_thread`, preserving the
event-loop/blocking-I/O contract. Writes remain at-most-once and log-and-drop;
nothing is buffered for `shutdown_flush`.

Health, readiness, and deployment reports describe Honcho as optional mutable
contextual memory, never as a durable dependency. A degraded Honcho projection
does not make overall readiness fail. These surfaces expose only selection/init state,
safe tenant projection, isolation/transport/failure posture, a bounded
operational status, safe timestamps, and the last stable error code. They do
not contact Honcho or expose its host.

## Existing-workspace migration

Changing to the tenant-derived namespace makes legacy memory invisible. The
Gateway intentionally does not dual-read or copy workspaces. DeerFlow's Honcho
client has no qualified workspace-copy API, so copying requires supported
Honcho provider tooling.

Use this operator procedure:

1. Stop the Gateway and every writer, then retain a provider backup/export.
2. Build a UTF-8 JSON inventory mapping each raw user ID to its exact old
   workspace ID. If legacy `user_peer_overrides` or a non-default
   `assistant_peer` was used, make that value an object with `workspace`,
   `user_peer`, and `assistant_peer`; otherwise the string form reproduces the
   old default peer derivation. Do not include messages, API keys, or other
   provider data.
3. Choose and record the canonical deployment tenant ID.
4. From `backend/`, generate the first bounded plan page:

   ```bash
   uv run python scripts/plan_honcho_workspace_migration.py \
     --inventory /secure/path/honcho-workspaces.json \
     --tenant-id customer-a \
     --dry-run \
     --offset 0 \
     --limit 100
   ```

   The command emits pseudonymous user references, exact old/new workspace and
   user/assistant peer mappings, counts, and a digest. It accepts no provider
   credential, reads no content, performs zero writes, and limits each page to
   100 mappings. Repeat with the next `offset` while `has_more` is true.
5. Copy/export/import each exact mapping with Honcho-supported provider tooling.
   Preserve the legacy sessions and rewrite their peer associations to the
   emitted target peers; new tenant-scoped session IDs apply to future writes.
   Running the local utility without `--dry-run` fails with
   `honcho_provider_copy_required`; it never falls back to dual-read.
6. Validate provider counts/digests where available, deploy the tenant-derived
   configuration, and perform isolated reads for representative users.
7. Keep the backup and old workspaces untouched until the rollback window ends.
   Roll back by stopping writers and restoring the old configuration/data as a
   unit; never run old and new namespaces as an implicit merged view.

## Backend limitations

- Honcho does not implement DeerMem fact CRUD, import, or Settings-page fact
  editing. `get_memory` projects the working representation into a minimal
  DeerMem-shaped document with an empty fact list.
- `agent_name` is not mapped; Honcho models the user, not per-agent facts.
- There is no automatic DeerMem-to-Honcho or Honcho workspace migration.

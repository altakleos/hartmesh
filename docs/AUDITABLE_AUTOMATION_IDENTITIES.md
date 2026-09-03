# Auditable Automation Identities

HartMesh binds a server-created answer to every newly accepted durable
invocation: which credential acted, for which principal and tenant, with which
effective authority. The answer is durable evidence, not a capability token.
Every later submit, observe, control, or export request is authenticated and
authorized again against current state.

## Actor contract

The host-independent contract is exported by `deerflow_extension_api`:

```python
CredentialEvidenceV1(
    method=...,                       # session, personal_access_token,
                                      # internal_service, channel, or
                                      # development_bypass
    credential_ref=...,               # PAT UUID4; otherwise currently null
    effective_authority_digest=...,   # canonical authority commitment
    authority_categories=...,         # bounded coarse resource names
    issued_at=...,
    expires_at=...,
)

VerifiedActorContextV1(
    identity=InvocationIdentityV1(...),
    credential=CredentialEvidenceV1(...),
    tenant=TenantReferenceV1(...),
)
```

`InvocationIdentityV1` remains the principal contract. Its optional
`acting_service` exists only for genuine service delegation. A PAT is a user
credential and never creates an acting service. `SealedOriginV1` remains source
and transport evidence; source fields do not grant authority. The tenant stays
in `TenantReferenceV1`. No parallel identity hierarchy is introduced.

The Gateway creates `CredentialEvidenceV1` only from authenticated server
state. It removes credential-looking request context keys before admission.
Unsupported or anonymous durable adapters fail with
`credential_evidence_unavailable`; they do not invent a principal or method.

New `TrustedRunContextV1` records use serialization version 4 and bind the
credential projection into both the evidence digest and execution digest.
Versions 1 through 3 remain explicitly readable and have `credential=None`.
Credential evidence contains no bearer value, PAT digest, cookie/session value,
credential name, OAuth credential, service secret, request body, or IP address.

### What the reference proves

A PAT `credential_ref` identifies the server-created PAT row used at the
authentication boundary. HartMesh reuses that row's random canonical UUID4;
it is stable, log/URL safe, and unrelated to the raw `dfp_...` token or its
stored SHA-256 digest. It does not prove an external service honored policy,
that a downstream side effect occurred exactly once, or that the credential is
still active.

## Canonical authority

Authority canonicalization is version 1. It resolves checked-in aliases, rejects
unknown or malformed identifiers, deduplicates, and sorts lowercase
`resource:action` values. The digest is SHA-256 of compact, key-sorted UTF-8
JSON:

```json
{"authorities":["runs:create","threads:read"],"version":1}
```

At most 64 authorities and 16 coarse resource categories are accepted. The
durable projection stores the digest and categories, not the scope list. The
authorized PAT management response remains the only surface that returns the
bounded scope list. Authority evidence records the effective permission set at
admission; it is not inferred later from a route name.

## PAT route contract

PAT access is default-deny. The checked-in `PAT_ROUTE_SCOPE_RULES` table and its
router contract tests are the source of truth. Current mappings are:

| Method and path | Required scope |
| --- | --- |
| `POST /api/threads` | `threads:write` |
| `POST /api/threads/search` | `threads:read` |
| `GET /api/threads/{thread_id}` | `threads:read` |
| `PATCH /api/threads/{thread_id}` | `threads:write` |
| `DELETE /api/threads/{thread_id}` | `threads:delete` |
| `GET /api/threads/{thread_id}/goal` | `threads:read` |
| `PUT`, `DELETE /api/threads/{thread_id}/goal` | `threads:write` |
| `GET /api/threads/{thread_id}/state` | `threads:read` |
| `POST /api/threads/{thread_id}/state` | `threads:write` |
| `POST /api/threads/{thread_id}/compact` | `threads:write` |
| `POST /api/threads/{thread_id}/history` | `threads:read` |
| `POST /api/threads/{thread_id}/branches` | `threads:write` |
| `GET /api/threads/{thread_id}/runs` | `runs:read` |
| `POST /api/threads/{thread_id}/runs` | `runs:create` |
| `POST /api/threads/{thread_id}/runs/{stream,wait,regenerate/prepare,edit-regenerate/prepare}` | `runs:create` |
| `GET /api/threads/{thread_id}/runs/{run_id}` | `runs:read` |
| `POST /api/threads/{thread_id}/runs/{run_id}/cancel` | `runs:cancel` |
| `GET /api/threads/{thread_id}/runs/{run_id}/{join,messages,events,workspace-changes}` | `runs:read` |
| `GET`, `POST /api/threads/{thread_id}/runs/{run_id}/artifacts/archive` | `runs:read` |
| `GET`, `POST /api/threads/{thread_id}/runs/{run_id}/stream` | `runs:read` |
| `POST /api/runs/{stream,wait}` | `runs:create` |
| `GET /api/runs/{run_id}/{messages,feedback}` | `runs:read` |

The route scope is necessary but not sufficient: it is intersected with the
owning user's freshly resolved permissions. Cancel-capable query/body variants
also require `runs:cancel`: `action=interrupt|rollback` on the existing-run
stream and `multitask_strategy=interrupt|rollback` on run creation. Credential
management, deployment qualification, extension installation, administration,
and tool-plane promotion are not reachable with ordinary run scopes.

## Persistence and migration

Migration `0033_automation_identities` adds nullable legacy anchors
`personal_access_tokens.tenant_ref` and `.tenant_digest`, their both-or-neither
check, and tenant-first digest/user indexes. Every new row has both anchors.
Create, digest lookup, list, revoke, last-used, and audit operations compare the
repository's frozen tenant reference as well as user ownership where relevant.

The migration backfills only when the schema already has the bound
`hartmesh_deployment_identity` singleton. A populated unbound schema remains
unbound and therefore unreadable by the tenant-scoped repository. The explicit
offline `deerflow deployment bind-tenant --expected-nonempty-schema` flow also
backfills PAT rows in its singleton transaction. It never derives tenancy from
a request, user, host, release, or namespace. Existing PAT UUIDs are preserved.

The same migration creates `credential_audit_events`. Rows are tenant-bound,
validated, and aggregated by a daily canonical key over safe dimensions. The
operator response is capped at 100 aggregates. SQL retention defaults to 90
days and is enforced during writes; the local in-memory adapter holds at most
1,024 aggregates. Revocation retains the PAT row as a tombstone, and neither
audit expiry nor account lifecycle rewrites accepted invocation history.

The session-only management API is:

```text
GET /api/v1/auth/pats/{pat_id}/audit?limit=1..100
```

It applies tenant and owner checks and returns only credential reference,
pseudonymous actor digest, method, authority digest, coarse action/route/reason,
timestamps, and count. PAT name and token material are excluded. PAT scopes
cannot call PAT management APIs.

Creation and revocation audits share the PAT mutation transaction. Durable
admission and all cancel-capable controls require an audit write and fail 503
before the action when it is unavailable. Routine successful use, rejected
use, expiry observations, and last-used timestamps are best-effort so audit
refresh cannot become a general authentication availability dependency. Scope
mutation is not currently supported; if added, it must write the existing
`scope_changed` action in the same transaction as the mutation.

## Revocation, replay, and recovery

Revocation linearizes at the committed tenant-scoped PAT update. A request that
completed authentication before that commit may finish; every authentication
started after the commit is rejected with the same non-oracular `Invalid token`
response used for unknown, cross-tenant, expired, and malformed candidates.
The last-used update cannot reactivate a token. This rule is covered by an
ordered concurrency test.

Accepted v4 evidence is immutable. Revocation does not delete or edit it. An
equal client retry is still processed behind current authentication and route,
scope, user, and tenant authorization, so historical equality cannot submit,
observe, cancel, or export with a revoked credential.

An authorized recovery worker resumes the already accepted record under the
existing owner/lease/epoch recovery fences. Its service execution authority is
separate runtime evidence; it does not replace the original
`TrustedRunContextV1.identity` or `.credential`. Historical admission actor and
current recovery executor must be displayed as different facts.

## Handoff to governed tool-plane revisions

Project 03 should import `VerifiedActorContextV1`, `CredentialEvidenceV1`, and
the existing identity/tenant types from `deerflow_extension_api`; its deep
revision service must not import Gateway authentication modules. At an HTTP
boundary, `app.gateway.credential_evidence.verified_actor_context_for_request`
composes the current server-resolved actor. The Gateway's
`require_audited_cancel_permission_if` demonstrates the privileged-action
sequence: recheck a dedicated current permission, then call
`record_required_credential_action` before mutation, and pass the verified
actor into the deep service.

The authority digest is evidence, not an authorization evaluator. Tool-plane
staging/promotion needs a new explicit administrator permission and route
mapping; it must not infer that privilege from `runs:*`, a method/category, an
old accepted record, or the digest alone.

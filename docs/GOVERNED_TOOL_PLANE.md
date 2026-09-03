# Governed Skill and MCP Revisions

HartMesh treats deployment MCP configuration, public and managed-integration
skill packages, and each user's custom tool state as versioned tool-plane
material. With `tool_plane.enabled: true` (the default), the legacy skill, MCP,
and integration write routes cannot change active material. An authorized actor
must stage, validate, and promote a revision.

This workflow governs data-driven skills and MCP configuration. It does not
admit or sandbox Python plugins. Plugins remain trusted, restart-required
operator code under the separate extension provenance boundary.

## Scope model

There is one deployment-base scope per frozen Gateway tenant and at most one
active overlay for each verified user:

- The deployment base owns MCP server structure, public-skill packages and
  state, globally installed managed-integration package bytes, and validation
  policy identity.
- A user overlay owns that user's custom-skill packages and state, MCP and
  managed-integration enablement, and non-secret credential binding
  references/versions. Ordinary callers cannot supply another user's scope.
- An effective revision is the canonical composition of the active base and
  the verified user's active overlay. Users without nonempty state use the
  canonical empty-overlay marker; no synthetic user revision row is created.

Skill lookup keeps the established precedence: a managed integration shadows
a same-named public skill, and a user's custom skill shadows either base
variant. Validation permits those intentional seams while still rejecting
duplicate identifiers within one source.

Revision, content, base, overlay, projection, policy, report, and effective
digests are lowercase SHA-256 identities over canonical versioned projections.
The records and transition events are append-only. A rollback creates a new,
attributed promotion from previously validated immutable material; it never
edits history.

## Stage, validate, promote

The phases deliberately have different effects:

1. **Stage** stores a canonical candidate and any skill archive in protected,
   content-addressed storage. It does not install, enable, or project anything.
2. **Validate** binds a bounded report to the exact revision/content bytes and
   current validator-policy digest. Any byte or policy change requires a new
   validation.
3. **Promote** rechecks current authorization, parent/base generations, and
   validation; writes durable `prepared` state; projects under the established
   config/skill locks; verifies the observed digest; and only then marks the
   revision active.

The authenticated API is rooted at `/api/tool-plane`:

| Operation | Endpoint |
| --- | --- |
| Current-user or base status | `GET /status?scope_kind=user_overlay|deployment_base` |
| Current-user or base history | `GET /revisions?scope_kind=...` |
| Stage a revision | `POST /revisions` |
| Stage an inert `.skill` archive | `POST /skill-artifacts` |
| Inspect safe manifest/report | `GET /revisions/{revision_id}` |
| Inspect a bounded changed-field diff | `GET /revisions/{revision_id}/diff?against=...` |
| Validate, promote, or roll back | `POST /revisions/{revision_id}/{validate|promote|rollback}` |
| Explicit cross-user administrator access | `GET /admin/status`, `GET /admin/revisions`, and `/admin/revisions/{revision_id}...` |
| Capture an upgraded mutable installation | `POST /bootstrap/stage-current` |

Deployment-base operations require the dedicated audited administrator action.
User-overlay operations derive the opaque user reference from the
`VerifiedActorContextV1` established by authentication. An administrator acting
on another user's existing revision must use the `/admin/...` route, which is
audited separately. Personal access tokens use a default-deny route matrix and
cannot call tool-plane management endpoints.

The service independently verifies the credential's canonical authority digest
against the server-owned permission universe. A coarse `tool_plane` category or
an administrator role without `tool_plane:admin` is not sufficient for a base
operation; user writes require `tool_plane:mutate`.

A minimal base candidate looks like:

```json
{
  "scope_kind": "deployment_base",
  "candidate": {
    "version": 1,
    "validation_policy_digest": "<digest reported by GET /api/tool-plane/status>",
    "parent_revision_digest": null,
    "change_summary": "Add reviewed search tools",
    "mcp_servers": {
      "search": {
        "type": "http",
        "url": "https://mcp.example.com/api",
        "headers": {"Authorization": "$SEARCH_TOKEN"},
        "tools": {"lookup": {}}
      }
    },
    "public_skills": {},
    "managed_integrations": {}
  }
}
```

The keys of a nonempty MCP `tools` map are the approved raw-tool allowlist;
their values may contain the supported routing overrides. An empty map retains
the compatibility meaning “all valid tools advertised by this server.”

A user overlay names its validated base and only safe per-user choices:

```json
{
  "scope_kind": "user_overlay",
  "candidate": {
    "version": 1,
    "base_revision_digest": "<active deployment revision digest>",
    "mcp_enablement": {"search": true},
    "credential_selectors": {
      "search": {"binding_ref": "search:user-primary", "version": 3}
    },
    "custom_skills": {},
    "managed_integration_enablement": {},
    "skill_states": {}
  }
}
```

The archive staging response supplies the archive/tree/manifest digests,
declared frontmatter version (when present), and the exact `SKILL.md` entry
point needed by a skill entry. A revision must reproduce those artifact-bound
values; caller-asserted version or entry-point metadata is rejected. Declared
versions must conform to SemVer 2.0 in both staged artifacts and revision
manifests. Every
managed-integration entry additionally requires its provider identifier;
projection preserves the canonical
`<provider>/<skill>/` layout and provider-owned hidden manifests while replacing
all governed package directories. Archive staging and revision staging are both
inert; promotion copies only the exact validated artifact bytes.

## Secrets and credential selectors

Canonical material may contain selector identities, never credential values.
MCP header, environment, OAuth secret, refresh/access token, and equivalent
fields must use `$ENV_NAME` or `${ENV_NAME}`. They are canonicalized as
`env:ENV_NAME`. Request-scoped headers retain only a `context:<key>` selector.
User overlays retain a bounded credential binding reference and integer version.

Literal secrets, `user_auth` value maps, credentials embedded in URLs or
arguments, unknown secret-like fields, and existing per-user integration
credential files fail with `secret_value_present`. HartMesh does not hash those
values into evidence: low-entropy hashes would still disclose them.

Selectors resolve only while process-local runtime configuration is constructed
through the existing environment/request-context credential boundary. Resolved
values are not written to revisions, validation reports, events, diffs, API
responses, or accepted-invocation evidence. Rotating a binding in a way that
must invalidate durable MCP task recovery requires incrementing its configured
binding version.

## Validation and trust boundary

Validation covers:

- bounded safe archive extraction, including traversal, absolute-path,
  symlink/hard-link, device, duplicate/conflicting-path, file-count, and expanded
  size rejection;
- skill manifest/file integrity against staged digests;
- the built-in read-only SkillScan/review facts for the exact staged tree;
- the optional managed-integration provider allowlist and exact forbidden
  skill capabilities derived from `SKILL.md` (`unrestricted-tools`,
  `autonomous-secrets`, `declared-secrets`, and `tool:<name>`). Startup rejects
  unknown capability IDs and malformed tool names instead of accepting a
  no-op denial;
- MCP schema, transport, executable/argument, endpoint/SSRF, tool, selector,
  provider, count, and deployment-policy checks; and
- base/overlay composition and non-widening rules.

Warnings remain visible but do not require an override in policy version 1.
Errors reject the candidate. An unavailable or incomplete required validator is
`unqualified`/failed in durable operation; it is never treated as a successful
skip. Reports expose bounded safe codes, locations, counts, validator versions,
policy identity, and digests. Validator versions include SHA-256 identities for
the complete conservative source closure used by the canonicalizer, governed
validator, artifact verifier, endpoint and MCP launch policy, MCP schema,
SkillScan, and skill-review implementations, plus the review facts schema
version. The closure includes the MCP schema's imported identifier and bound
definitions. These identities are frozen at process startup so validation and
promotion perform no synchronous source-tree I/O on the event loop. A mismatch
makes a prior report stale at promotion. Raw unsafe scanner payloads and skill
source bodies do not enter general audit projections.

Validation means the candidate satisfied these deterministic checks. It does
not prove that a skill is correct, that a remote MCP server is trustworthy, that
an endpoint will preserve behavior, or that arbitrary Python code is safe.

## Base/overlay compatibility and locking

Before any base promotion or rollback, the service keyset-pages one snapshot of
all active nonempty overlays and its SQL-owned generation. Every distinct
overlay is revalidated against the candidate base, producing an immutable
compatibility attestation bound to both revision digests, the policy digest,
and report. The prepare transaction succeeds only if that generation is
unchanged and every active overlay has a matching successful attestation.

Overlay promotion similarly binds the one current base generation. The global
SQL lock order is deployment-base scope first, then at most one user-overlay
scope. Filesystem projection uses the established skill mutation lock,
`extensions_config_write_lock`, sidecar advisory locks, and atomic replacement.
A deployment promotion never holds every user's filesystem lock.

SQL and filesystem changes are not one atomic transaction. `prepared` is the
durable journal for that deliberate crash window:

1. SQL records the intended revision and projection digest.
2. A complete content-addressed projection is built and its active pointer is
   switched under the applicable locks.
3. The active projection is re-read and hashed.
4. SQL finalizes only on an exact match.

Startup reconciliation replays immutable prepared material when safe. A crash,
mismatch, conflicting active row, or failed restore leaves
`recovery_required`; readiness remains failed and the service never guesses
which state should be blessed.

## Upgrade bootstrap

Migration `0034_tool_plane_revisions` creates schema and governance metadata;
it never synthesizes a promoted revision from existing mutable bytes. If the
Gateway sees existing deployment or indexed user material without revisions,
status becomes `bootstrap_required`.

An administrator adopts it as follows:

1. Call `POST /api/tool-plane/bootstrap/stage-current`. Under the read/mutation
   locks, this captures the base plus every nonempty user store from the
   authoritative bounded user index and returns all staged revision IDs.
2. Validate and promote the returned base through the normal endpoints.
3. Validate and promote every returned overlay through the explicit
   `/api/tool-plane/admin/revisions/{revision_id}/...` endpoints.
4. The service rereads the inventory high-water digest, every active pointer,
   and every projection. Bootstrap clears only if all match the original
   capture.

Concurrent indexed/user changes return `bootstrap_inventory_changed`. Material
in an unindexed user bucket also fails closed; directory names are never parsed
as identities. Embedded secrets must be migrated to selectors and the capture
restarted.

For an indexed user still seeing the pre-user-isolation global custom-skill
fallback, bootstrap stages those exact visible bytes as that user's custom
package. Its effective enabled state includes the legacy global enablement
ceiling, so adopting a globally disabled package cannot silently enable it.
A protected source digest separately fences the raw user state through base
promotion; a concurrent edit still returns `bootstrap_inventory_changed` even
when it would produce the same effective disabled value. Overlay promotion then
moves the immutable package into the user scope.

## Drift, modes, and readiness

Direct edits are never retroactively converted into revision history. A live
config, package tree, user state file, subject binding, or active pointer that
does not match its promoted content produces the constant classification
`unmanaged_drift`; HartMesh does not digest possibly secret mismatching bytes.
The canonical empty-overlay marker is also an assertion about live absence:
custom/legacy packages, nonempty user state, credentials, or an unexpected
pointer—including a dangling pointer symlink—make an otherwise row-less user
overlay drifted and block durable admission/readiness. Empty overlay staging is
rejected: after a base is active, verified absence is itself reported as
governed without fabricating a per-user row. Administrative status resolves an
opaque user reference through the bounded authoritative user inventory before
performing that same live-absence check.

- In ordinary local/non-durable mode, bootstrap-required or drifted material
  remains usable for development. Status and the settings notice say
  `bootstrap_required`/`unmanaged`; admission makes no governed-revision claim.
- In durable production, bootstrap, drift, incoherent generations, prepared
  work, or recovery state fail readiness and new admission with a typed code.
  The authenticated management path stays available for repair.
- `durable_two_gateway_v1` is deployment-managed and immutable. The settings UI
  exposes no write controls, and the Gateway does not mount mutation or
  bootstrap routes in that profile. Service-level mutation still fails with
  `immutable_deployment` as defense in depth. The checked-in topology contract
  keeps governed tool-plane qualification unpassed until an exact external
  artifact and shared rows are supplied; this repository makes no
  production-ready claim.

## Accepted invocation pinning

New governed admissions bind the deployment-base revision digest, verified
user-overlay digest (or empty marker), both generations, observed projection
digest, safe effective MCP structure and allowlists, managed-integration IDs,
and the canonical effective digest. Admission captures skill bytes and MCP tool
objects, then rereads the effective revision; a concurrent promotion causes a
bounded retry rather than a mixed snapshot.

Lead agents and delegated subagents consume those captured objects. Later
promotion or rollback affects only later admissions. Process recovery rebuilds
MCP runtime configuration from the accepted secret-safe selector structure—not
the current mutable extensions file—and requires the rebuilt MCP tool contracts
and accepted agent revision to match before execution. Existing accepted skill
snapshot material follows the established accepted-skill recovery path.

Pre-governance accepted invocations retain their legacy accepted-material
adapter. They are not assigned synthetic tool-plane history; insufficient
legacy material fails with its existing typed material error.

## Operational failures

Common safe reason codes include `unsafe_archive`, `validation_failed`,
`validation_stale`, `secret_value_present`, `base_revision_changed`,
`overlay_preflight_failed`, `overlay_preflight_incomplete`,
`active_overlay_set_changed`, `projection_failed`,
`projection_digest_mismatch`, `tool_plane_bootstrap_required`,
`bootstrap_inventory_changed`, `recovery_required`, `unmanaged_drift`,
`promotion_not_authorized`, and `immutable_deployment`.

For `recovery_required`, keep writers quiesced, preserve SQL and the
content-addressed projection store, inspect the safe revision/events and
Gateway logs, and restart one Gateway to run reconciliation. Do not edit the
active pointer or mark a SQL row promoted manually. If reconciliation cannot
prove one state, restore the database and projection store together from the
same operator backup or stage a new attributed revision after resolving the
conflict.

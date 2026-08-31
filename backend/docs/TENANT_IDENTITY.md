# Server-Owned Tenant Identity

HartMesh runs one tenant per Gateway process and deployment release. The
Gateway resolves one immutable `TenantIdentityV1` during application
construction and passes that same object through admission, persistence,
runtime recovery, extension facts, Redis factories, and deployment reporting.

Tenant identity is selected by the operator at service startup. It cannot be selected by an API caller and does not replace per-user authorization.

This is a deployment isolation boundary, not a tenant directory or general
shared-schema multi-tenancy feature. Every tenant release needs its own
database or PostgreSQL schema, and normal user/principal authorization remains
mandatory inside that tenant.

## Configuration and projection

Set the lowercase DNS-label identifier with `DEER_FLOW_TENANT_ID` or
`config.yaml -> deployment.tenant_id`; the direct environment variable wins.
Values must match `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$` and are never
lowercased or repaired. `durable_production` requires an explicit value other
than `local`. Local development resolves `local` only when neither source is
present. Changing the value requires a Gateway restart.

The canonical identifier remains in trusted process configuration. Durable
rows, extension requests, lifecycle responses, health, and support diagnostics
receive only `TenantReferenceV1(version=1, public_ref, digest)`, where:

- `digest` is SHA-256 over canonical JSON
  `{"tenant_id":"<canonical-id>","version":1}`;
- `public_ref` is `tenant-` followed by the first 16 digest hex characters.

The reference is pseudonymous, not secret or resistant to guessing from a
small identifier space. HTTP and log output must never return the canonical
identifier. Readiness and deployment reports expose only the reference,
digest, identity version, and Redis prefix schema version.

Consumers request a typed namespace from
`TenantIdentityV1.namespace(TenantSubsystem)`. They must not derive a tenant
from a user, thread, request, header, Helm release name, Kubernetes namespace,
or unrelated free-form prefix. Later provider adapters for OpenSandbox,
Honcho, MCP tasks, and extension artifacts accept `TenantIdentityV1`, its safe
reference, or their typed `TenantNamespaceV1` projection.

## Admission, evidence, and recovery

Gateway input sanitization removes tenant-looking request and context aliases,
including `tenant`, `tenant_id`, `tenantId`, `tenant_ref`, `tenant_digest`, and
`X-Tenant-ID` spellings. Internal services, Scheduled Tasks, channel ingress,
and extension-created launches inherit the process identity; none supplies it.

Every newly accepted invocation includes the safe tenant reference in its
canonical accepted and trusted-context digests. Run rows, lifecycle events,
assembly evidence, durable tool receipts, and event stores carry the same
anchor. The host wraps its authenticated HTTP/channel/scheduler/service
idempotency scope in a tenant-digested admission scope, so otherwise identical
external keys in two deployments cannot share an admission namespace.

All run/event reads and mutations apply the process tenant digest in addition
to ordinary user authorization. Before acquiring recovered-run ownership, the
worker compares accepted and process identity. A disagreement stops with
`tenant_identity_mismatch` before graph construction, model work, or tool
execution.

## Database binding and migration

The application schema contains one `hartmesh_deployment_identity` singleton.
An empty schema is atomically bound on first startup. A schema already bound to
a different digest fails startup. A nonempty schema created before this
feature fails with `tenant_schema_unbound`; the first request never infers its
owner.

Use this migration sequence for an existing deployment:

1. Choose the canonical `tenant_id` and record it in the deployment's operator
   configuration.
2. Stop the Gateway, scheduler, workers, and every other writer using the
   schema.
3. Back up the database and Redis before changing either.
4. Decide whether to copy Redis data to the canonical projections (preferred)
   or retain exact old prefixes for this one-release compatibility window. If
   retaining them, pass the applicable `--legacy-*-prefix` flags shown below.
   From `backend/`, preview the explicit legacy binding:

   ```bash
   uv run deerflow deployment bind-tenant \
     --tenant-id <id> \
     --expected-nonempty-schema \
     --legacy-stream-bridge-prefix <old-stream-prefix> \
     --legacy-checkpoint-cache-prefix <old-checkpoint-prefix> \
     --legacy-sandbox-ownership-prefix <old-ownership-prefix> \
     --dry-run
   ```

   Omit all three legacy flags when copying to canonical projections; each flag
   is independently optional when that Redis subsystem was not enabled.
5. Review the safe public reference/digest and recorded component names, then
   repeat the exact command without `--dry-run`.
   The command records the singleton, backfills nullable tenant anchors, and
   wraps legacy keyed-run admission scopes in one database transaction. Any
   supplied Redis projections are stored in the same singleton as an immutable
   compatibility record. The wrapping preserves idempotent replay under the
   selected tenant; the command does not copy Redis data or rewrite pre-tenant
   canonical evidence.
6. Inventory the new Redis namespace without exposing keys or values:

   ```bash
   uv run python ../scripts/check_tenant_namespaces.py \
     --tenant-id <id> \
     --redis-url "$REDIS_URL" \
     --dry-run
   ```

7. Copy or rename retained Redis keys offline into the canonical names below,
   or configure exactly the recorded legacy projections. For Helm, copy each
   recorded value into both `tenant.legacyRedisPrefixes.<component>` and its
   matching `redis.keyPrefixes.<component>` selector. The chart checks the two
   declarations; Gateway startup independently checks the database record.
   Verify counts before removing old data. The Gateway deliberately performs
   no dual reads and does not silently migrate external state.
8. Deploy with the tenant setting, verify `/health`, `/ready`, and the
   deployment report show the expected safe reference and prefix schema, then
   resume traffic.

During the first feature release containing server-owned tenant identity, a
legacy Helm/config prefix must exactly equal either the canonical projection or
the corresponding projection recorded by `bind-tenant`. Merely setting Helm's
`tenant.legacyRedisPrefixes` is insufficient: runtime startup reads the schema
binding and fails an unrecorded selection with `tenant_namespace_conflict`,
naming the configured field. The following feature release removes this
compatibility path. Plan the Redis copy during this window.

Rollback after binding requires either code that understands the new nullable
tenant columns and singleton table or restoration of the database backup. Do
not delete the identity row to make older code start. Restore/copy Redis from
the pre-migration backup if the rollback expects legacy names.

## Redis namespaces and ACLs

For public reference `tenant-<digest-prefix>`, prefix schema v1 derives:

| Family | Prefix / key shape |
| --- | --- |
| Stream bridge | `hm:v1:tenant-<digest-prefix>:redis:deerflow:stream_bridge:<run-id>` |
| Checkpoint history cache | `hm:v1:tenant-<digest-prefix>:redis:ckpt-hist:v1:<cache-key>` |
| Sandbox ownership | `hm:v1:tenant-<digest-prefix>:redis:deerflow:sandbox:owner:<sandbox-id>` |
| E2B capacity ledger | `hm:v1:tenant-<digest-prefix>:redis:deerflow:sandbox:owner:e2b-capacity` |
| Opt-in qualification evidence | `hm:v1:tenant-<digest-prefix>:redis:qualification:<qualification-id>:...` |

Restrict the release's Redis principal with both key and pub/sub channel
patterns, even though the covered v1 factories currently emit keys and Redis
Streams rather than pub/sub messages:

```text
~hm:v1:tenant-<digest-prefix>:redis* &hm:v1:tenant-<digest-prefix>:redis*
```

Two tenants resolve different pseudonymous prefixes even when their run,
thread, user, and sandbox identifiers are identical. All new Redis consumers
must be added to the central `RedisTenantComponent` inventory and accept the
typed Redis `TenantNamespaceV1`; a factory-local prefix is not a tenancy
boundary.

## Stable failures

| Code | Meaning |
| --- | --- |
| `tenant_identity_invalid` | Operator identity violates the grammar or bounds. |
| `tenant_identity_required` | A durable production deployment omitted a nonlocal identity. |
| `tenant_identity_mismatch` | Process, schema, or accepted-run identity differs. |
| `tenant_schema_unbound` | A nonempty legacy schema needs the explicit binding command. |
| `tenant_namespace_conflict` | A compatibility prefix matches neither its canonical nor recorded legacy projection. |

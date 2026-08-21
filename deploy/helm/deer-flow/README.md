# DeerFlow Helm Chart

Deploys the full DeerFlow stack to Kubernetes: **gateway** (backend + embedded
LangGraph runtime), **frontend** (Next.js), **nginx** (internal reverse proxy
preserving the compose routing), and the **provisioner** (K8s-native sandbox
that spawns code-execution Pods on demand).

The default values describe an explicitly unqualified local evaluation profile,
but a bare install is intentionally refused: home persistence is enabled while
no skills claim is named, which would crashloop the provisioner. Supply the
recommended PVC values below or explicitly select the legacy local/hybrid
hostPath mode. The validated production mode is still exactly one Gateway
replica; it does not claim high availability, rolling zero downtime, or live
pod-loss qualification.

## Prerequisites

- A Kubernetes cluster (Docker Desktop K8s, OrbStack, kind, k3d, or a real cluster).
- `kubectl` + `helm` 3.8+ installed (OCI registry support stabilized in 3.8; earlier 3.x needs `HELM_EXPERIMENTAL_OCI=1`).
- The three DeerFlow images — either the published ones (see "Install the
  published chart" below) or built locally (see step 1).
- An Ingress controller (e.g. ingress-nginx) if you enable `ingress`.

## Install the published chart (GHCR)

The chart and all three images are published to GHCR on every `v*` release tag
(see `.github/workflows/container.yaml` and `chart.yaml`). Skip the build step
and install directly:

```bash
helm install deer-flow oci://ghcr.io/<owner>/charts/deer-flow \
  --version <version> \
  -n deer-flow --create-namespace \
  -f my-values.yaml
```

where `<owner>` is the GitHub owner the chart is published from and `<version>`
matches the release tag without the leading `v` (tag
`v2.1.0+hartmesh.1` → `--version 2.1.0+hartmesh.1`). Helm handles the chart's
underscore-normalized OCI storage tag internally.

> **Note:** the helm chart is new in 2.1.0 - no chart was published before it.
> It publishes to `oci://ghcr.io/<owner>/charts/deer-flow` (the `charts/` prefix
> keeps it distinct from the `deer-flow-{backend,frontend,provisioner}` image
> packages).

For local evaluation, the legacy shared tag values remain supported:

```yaml
image:
  registry: ghcr.io/<owner>
  tag: "vX.Y.Z-hartmesh.N"      # release-manifest.json -> images.*.tag
  gatewayImage: <repo>-backend
  frontendImage: <repo>-frontend
  provisionerImage: <repo>-provisioner
  pullSecrets:
    - { name: regcred }         # only if the GHCR package is private
```

Only a repository actually named `deer-flow` can rely on the chart's legacy
`gatewayImage` / `frontendImage` / `provisionerImage` defaults. Other forks
publish `<repo>-backend`, `<repo>-frontend`, and `<repo>-provisioner`, so they
must set those three names as shown (or use the per-workload repositories
below). New GHCR packages default to **private** — flip the package to public in
its GHCR settings page for unauthenticated pulls, otherwise create a pull secret
(step 1) and reference it via `image.pullSecrets`.

> The OCI chart and the images are versioned independently of the chart's
> `appVersion`; for local evaluation, use the image tag recorded in
> `release-manifest.json`. It includes the leading `v` and uses the registry-safe
> spelling, which differs from the chart `--version` when build metadata is
> present.

For the validated one-replica profile, use immutable per-workload references
from `release-manifest.json`. Map `images.backend` to `gateway.image`, and map
the frontend and provisioner entries directly. The sandbox mirror workflow
prints the separately verified sandbox digest. With a digest set, a workload's
tag is documentation only and is not appended:

```yaml
deployment:
  mode: durable_one_replica
  persistenceTier: shared_durable

gateway:
  image:
    repository: ghcr.io/<owner>/<repo>-backend
    digest: "sha256:..." # release-manifest.json -> images.backend.digest

frontend:
  image:
    repository: ghcr.io/<owner>/<repo>-frontend
    digest: "sha256:..." # release-manifest.json -> images.frontend.digest

provisioner:
  image:
    repository: ghcr.io/<owner>/<repo>-provisioner
    digest: "sha256:..." # release-manifest.json -> images.provisioner.digest
  sandboxImage: "ghcr.io/<owner>/<repo>-sandbox@sha256:..." # mirror workflow summary

sandbox:
  volumeMode: pvc

skills:
  existingClaim: deer-flow-skills

# Production validation accepts only references to separately managed
# credentials; inline passwords and connection URLs are rejected.
postgresql:
  existingSecret: deer-flow-postgres
redis:
  existingSecret: deer-flow-redis

config: |
  # Keep the complete chart config; the relevant production declarations are:
  deployment:
    profile: durable_production
  database:
    backend: postgres
    postgres_url: $DATABASE_URL
```

`nginx.image`, `postgresql.image`, and `redis.image` accept the same optional
`digest` form. They are not part of the current invocation qualification
boundary, but pinning them is recommended for a reproducible full deployment.

## 1. Build & push images (custom builds only)

Skip this section if you're using the published chart above. To build the
images yourself from the existing Dockerfiles:

```bash
REGISTRY=ghcr.io/yourorg
TAG=latest

# backend - build with the `postgres` extra used by the durable profile
docker build -t $REGISTRY/deer-flow-backend:$TAG --build-arg UV_EXTRAS=postgres -f backend/Dockerfile .
# frontend
docker build -t $REGISTRY/deer-flow-frontend:$TAG -f frontend/Dockerfile .
# provisioner
docker build -t $REGISTRY/deer-flow-provisioner:$TAG -f docker/provisioner/Dockerfile docker/provisioner

docker push $REGISTRY/deer-flow-backend:$TAG
docker push $REGISTRY/deer-flow-frontend:$TAG
docker push $REGISTRY/deer-flow-provisioner:$TAG
```

These names match the legacy image defaults. New values files should set each
workload's `image.repository`; keep the legacy block only while migrating.

If your registry needs auth, create a pull secret:

```bash
kubectl create secret docker-registry regcred \
  --docker-server=ghcr.io \
  --docker-username=youruser \
  --docker-password=yourtoken \
  -n deer-flow
```

## 2. Configure values

Copy and edit `values.yaml` → `my-values.yaml`. At minimum set:

<!-- recommended-kubernetes-values:start -->
```yaml
image:
  registry: ghcr.io/yourorg
  tag: latest
  pullSecrets:
    - { name: regcred }

# Kubernetes installs should fail closed unless both sandbox claims resolve.
sandbox:
  volumeMode: pvc

# Pre-create this read-only skills claim in the sandbox namespace.
skills:
  existingClaim: deer-flow-skills

ingress:
  enabled: true
  className: nginx
  host: deer-flow.example.com
  tls:
    enabled: true
    secretName: deer-flow-tls

# Reference a separately managed Secret; do not put provider credentials in
# ordinary values files. It may contain OPENAI_API_KEY and other provider vars.
existingSecret: deer-flow-provider
```
<!-- recommended-kubernetes-values:end -->

The provisioner resolves its volume mode once at startup. `sandbox.volumeMode:
pvc` requires both the home and skills claim names; the chart supplies the home
claim name while `persistence.home.enabled: true`, but operators must configure
`skills.existingClaim`. Helm refuses an enabled provisioner with exactly one
claim before install or upgrade; the provisioner's startup guard still protects
non-Helm deployments and identifies a missing environment variable. The empty
mode infers `pvc` only when both names are present and `hostpath` only when
neither is present. Use explicit `hostpath` only for local or hybrid deployments
that intentionally mount node filesystem paths—the claim values, if present,
are ignored in that mode.

Provide your model config under `config` (keep secrets as `$VAR` references —
they resolve from the selected Secret):

```yaml
config: |
  config_version: 42
  models:
    - name: gpt-4
      use: langchain_openai:ChatOpenAI
      model: gpt-4
      api_key: $OPENAI_API_KEY
      request_timeout: 600.0
  sandbox:
    use: deerflow.community.aio_sandbox:AioSandboxProvider
    provisioner_url: http://provisioner:8002
  database:
    backend: postgres
    postgres_url: $DATABASE_URL
    pool_recycle: 300
    command_timeout: 30
  checkpointer:
    type: postgres
    connection_string: $DATABASE_URL
  stream_bridge:
    type: redis   # cross-pod SSE; URL from DEER_FLOW_STREAM_BRIDGE_REDIS_URL
  # Tools MUST be listed explicitly - the agent gets none otherwise
  # (BUILTIN_TOOLS only adds present_file + ask_clarification). The chart
  # default in values.yaml enables the sandbox tools + web tools (web_search,
  # web_fetch, image_search - no API key); when you override `config:`, copy
  # them in. Full list in values.yaml / config.example.yaml. The web tools need
  # outbound egress from the gateway pod.
  tool_groups:
    - name: web
    - name: file:read
    - name: file:write
    - name: bash
  tools:
    - name: web_search
      group: web
      use: deerflow.community.ddg_search.tools:web_search_tool
      max_results: 5
    - name: web_fetch
      group: web
      use: deerflow.community.jina_ai.tools:web_fetch_tool
      timeout: 10
    - name: image_search
      group: web
      use: deerflow.community.image_search.tools:image_search_tool
      max_results: 5
    - name: bash
      group: bash
      use: deerflow.sandbox.tools:bash_tool
    # also: ls, read_file, glob, grep, write_file, str_replace (see values.yaml)
```

`$DATABASE_URL` is injected from the postgres Secret (see below). The
`checkpointer:` section keeps LangGraph checkpoints and Store data on the same
restart-durable backend; the Store does not fall back to `database:`.
Set `database.poolMaxOverflow: 2` to inject `DATABASE_POOL_MAX_OVERFLOW` and cap
temporary app ORM connections on a shared PostgreSQL server. Leaving the value
unset omits the environment variable and keeps SQLAlchemy's default of 10.
`stream_bridge.type: redis` supplies bounded reconnect replay through the
bundled Redis StatefulSet (or `redis.external`).

For a shared multi-tenant Redis, give each Helm release one tenant prefix:

```yaml
redis:
  tenantPrefix: acme
```

The chart derives `acme`, `acme:ckpt-hist:v1`, and
`acme:deerflow:sandbox:owner` for the stream bridge, checkpoint cache, and
sandbox ownership overrides respectively. The replace-style cache and
ownership overrides retain their subsystem namespaces instead of flattening all
three stores under the bare tenant string. Every emitted name still matches
`acme:*`, so an ACL user can be limited with key and stream-channel patterns
`~acme:* &acme:*`.

`redis.keyPrefixes.streamBridge`, `checkpointCache`, and `sandboxOwnership` are
advanced per-subsystem overrides. A non-empty value wins over the derived value
for that subsystem and must include any desired subsystem namespace for the two
replace-style overrides. Adding or changing the stream-bridge prefix starts
fresh per-run streams; retained legacy names are not migrated.
Because `config:` is a single override blob, a partial `config:` replaces the
chart default entirely - keep the `tools:`/`tool_groups:` block (or the agent
will have no tools) and the `sandbox:`/`database:`/`checkpointer:`/`stream_bridge:`
sections shown above.

### Split release and sandbox namespaces

By default, `namespace: ""` follows Helm's release namespace and
`sandboxNamespace: ""` places sandbox resources there too. For a split
deployment, pre-create a second namespace and provide same-name home and skills
claims in both namespaces before installing the chart:

```bash
kubectl create namespace acme-sbx
helm install deer-flow deploy/helm/deer-flow -n acme -f my-values.yaml
```

```yaml
namespace: ""              # use `helm -n acme`
sandboxNamespace: acme-sbx # must already exist

sandbox:
  volumeMode: pvc

persistence:
  home:
    enabled: true           # required even when existingClaim is set
    existingClaim: acme-home

skills:
  existingClaim: acme-skills
```

The provisioner remains in `acme`, but its namespaced Role and RoleBinding are
rendered into `acme-sbx`; the binding subject is the provisioner ServiceAccount
in `acme`. `K8S_NAMESPACE=acme-sbx` selects where sandbox resources are
created, while `PROVISIONER_GATEWAY_NAMESPACE=acme` deliberately remains the
Gateway identity namespace checked by TokenReview. Accepted-skill NetworkPolicy
peers select Gateway and provisioner Pods in that release namespace.

The chart never creates `sandboxNamespace` and does not grant namespace-create
RBAC. Its ClusterRole grants only name-pinned `get` for that Namespace object
and TokenReview create. The provisioner's `PROVISIONER_CREATE_NAMESPACE` escape hatch defaults to
`false`; set it to `true` only for operator-controlled single-namespace local or
Compose environments. The repository's Compose files opt in explicitly.

When `persistence.home.existingClaim` is set, the chart does not create the
home PVC. Both the Gateway home volume and provisioner `USERDATA_PVC_NAME` use
the existing claim. Keep `persistence.home.enabled: true`, because disabling it
also suppresses the provisioner environment variable.

## 3. Install (from a local chart checkout)

For a custom build or local development, install from the chart directory:

```bash
helm install deer-flow deploy/helm/deer-flow \
  -n deer-flow --create-namespace \
  -f my-values.yaml
```

## 4. Verify

```bash
kubectl -n deer-flow get pods
kubectl -n deer-flow port-forward svc/nginx 2026:2026
curl http://localhost:2026/health          # gateway health via nginx
```

The Gateway pod uses `GET /ready` for readiness and `GET /health` for liveness.
Readiness includes operator-required authoritative capability health, lifecycle-cursor
and transactionally maintained retained-cardinality/bound integrity, database availability, and the configured deployment
durability promise, but its unauthenticated body
is deliberately only `{"status":"ready"}` or `{"status":"not_ready"}`. Defaults use
`deployment.mode: local_evaluation` and `deployment.profile: local_development`,
so tag-based images remain convenient without implying production qualification.
The validated mode requires one replica, pinned Gateway/provisioner images,
`shared_durable`/PostgreSQL storage, and the runtime's
`durable_production` profile. Safe provenance, persistence tier, qualification
state, and safe admission-readiness reason codes are available only to an authenticated administrator
at `GET /api/runtime/v1/deployment`; portable runtime support remains the strict
`GET /api/runtime/v1/capabilities` record. Plugin registrations, required
capabilities, `agent_storage`, `dedupe_storage`, deployment profile, and their
derived manifest/storage composition are startup-only; deploy a restart to adopt
changes, while in-flight invocations stay pinned to the generation they accepted.

Signed GitHub ingress also participates in that deployment truth. The default
`config.dedupe_storage.backend: auto` selects PostgreSQL leased receipt storage when
the chart's database backend is PostgreSQL; an explicit `postgres` is equivalent.
`memory` retains local best-effort behavior and is rejected by the
`durable_production` chart contract. The authenticated deployment report exposes a
versioned `native_ingress` map with `durable` or `best_effort` per enabled source.
Durable requires both current HMAC authentication and PostgreSQL receipt storage, and
means the webhook commits every bounded fan-out receipt before acknowledgment. The
explicit unverified local-development mode remains `best_effort` even with PostgreSQL;
it cannot satisfy `durable_production`. Removing the HMAC secret makes the Gateway
not-ready and requests fail closed rather than falling through to that development mode;
it does not mean that the process-local `MessageBus` is durable, and it does not claim
multi-replica channel ownership. Verified-ingress eligibility is frozen at Gateway
composition, so adding a previously absent HMAC secret requires restart; rotating an
already configured nonblank secret remains request-time behavior.

The default internal health probe timeout is 2 seconds and the complete readiness evaluation
is capped at 5 seconds. The chart's Gateway readiness probe uses `timeoutSeconds: 6`, so
Kubernetes supplies bounded headroom rather than aborting an evaluation first. Readiness
fails on its first unsafe result; liveness uses an independent failure threshold of three.
Override these through `config.deployment.readiness` and `gateway.readinessProbe` only while
preserving `readinessProbe.timeoutSeconds > overall_timeout_seconds >
capability_probe_timeout_seconds`. Render validation also requires the probe
period to cover its timeout and any explicit termination grace to cover the
application shutdown budget, preStop delay, and scheduling headroom.

### Deployment identity and qualification

`deployment.provenance.sourceRevision` and a pinned Gateway digest are injected
through bounded trusted environment fields and appear only in the administrator
deployment report. `deployment.qualificationEvidence` accepts completed safe
identifiers, artifact SHA-256 digests, and RFC3339 completion times. Scoped
evidence additionally requires a bounded `scope` and exact `status: passed`;
legacy three-field records remain readable. It is empty by default, so the
report says `status: unqualified` and `trust: none_declared`; Helm never invents
evidence. A configured reference retains v1 `status: qualified` for compatibility
but explicitly reports `trust: operator_asserted`. It does not mean the Gateway
fetched or verified the artifact. Neither configuration accepts credentials or
arbitrary metadata.

For `durable_one_replica_pod_recovery`, the operator copies only an artifact-bound passing
live result into `deployment.qualificationEvidence`. The Gateway reports that bounded
assertion through the authenticated administrative deployment report. A release or
deployment controller must separately verify the artifact digest and exact image/chart,
configuration, Alembic head, qualification run/namespace, scope, and scenario set. A
collected test, default skip, process-loss simulation, image build, Helm render, or declared
reference alone is not externally verified qualification evidence.

#### Opt-in real-pod recovery qualification

`backend/tests/kubernetes/test_durable_invocation_pod_recovery.py` qualifies the
exact chart checkout and `repository@sha256` Gateway image against a disposable
cluster supplied through `KUBECONFIG`. It uses only Helm and kubectl, never
changes the current context, creates only the explicitly named
`hartmesh-qualification-*` namespace, and keeps its PostgreSQL and Redis pods
alive while replacing the one Gateway pod. The ordinary test suite collects and
skips it; that skip is an unpassed release gate, not evidence.

```bash
export DEERFLOW_TEST_KUBERNETES=1
export KUBECONFIG=/absolute/path/to/disposable-kubeconfig
export DEERFLOW_TEST_KUBERNETES_CONTEXT=kind-hartmesh-qualification
export DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT="$DEERFLOW_TEST_KUBERNETES_CONTEXT"
export DEERFLOW_TEST_KUBERNETES_NAMESPACE=hartmesh-qualification-20260808
export DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID=pod-recovery-20260808
export DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY=registry.example/hartmesh/gateway
export DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST=sha256:<64-lowercase-hex>
export DEERFLOW_TEST_KUBERNETES_EVIDENCE="$PWD/artifacts/kubernetes-qualification.json"
cd backend
PYTHONPATH=. uv run pytest -m kubernetes_contract -v -s
```

Nonempty durable skills use a separate, explicit evidence scope. It reuses the
same marked test, confirmed context, namespace confinement, chart, PostgreSQL,
Redis, and offline verifier, but additionally requires two schedulable nodes,
an RWX storage class, and exact provisioner/verifier/sandbox image identities.
The verifier container is shipped in the provisioner image, so those two
operator-supplied references and digests must match exactly. The v2 evidence
schema and offline expectation reject any other combination rather than
accepting a structurally impossible qualification artifact.

```bash
export DEERFLOW_TEST_KUBERNETES_SCOPE=durable_one_replica_rwx_verified_copy_v2_nonempty_skill
export DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY=registry.example/hartmesh/provisioner
export DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST=sha256:<64-lowercase-hex>
export DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY="$DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY"
export DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST="$DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST"
export DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY=registry.example/hartmesh/sandbox
export DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST=sha256:<64-lowercase-hex>
export DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS=<rwx-storage-class>
PYTHONPATH=. uv run pytest -m kubernetes_contract -v -s
```

That v2 run seeds bounded deterministic skill bytes and allowed-tool metadata,
requires Gateway and accepted sandbox Pods on different Ready schedulable nodes,
observes the TokenReview-protected materialization and a real Lease renewal,
faults Gateway and Lease ownership, proves cleanup of the exact owned sandbox
resources, and writes v2 evidence only after offline exact-subject validation.
The opt-in GitHub Actions Kubernetes qualification workflow selects this v2
scope. Its dispatch therefore requires pinned Gateway, provisioner, and AIO
sandbox images plus an RWX storage class. The runner creates the accepted
sandbox through the provisioner-backed AIO provider and executes a bounded
file read inside the real sandbox container, so the lane also serves as the
hardened-sandbox smoke for the chart's restricted security baseline.
Lease owner loss deliberately fails closed; this scope does not claim same-run
sandbox rehydration or replacement.
A renewal is proven only when the Lease keeps the exact UID, accepted-attempt
holder, and qualified duration while its bounded RFC3339 `spec.renewTime`
strictly advances. A `metadata.resourceVersion` change by itself is not renewal
evidence. Offline fakes pin this predicate and the v2 orchestration, but only an
artifact from the opt-in live run qualifies cross-node Kubernetes behavior.
An absent v2 artifact leaves nonempty remote skill execution unqualified.

After the run, obtain the declared digest/reference from the administrator report and the
artifact from the independently configured evidence path or artifact store. Supply expected
subjects from the deployment controller, not from untrusted fields inside the artifact:

```bash
cd backend
PYTHONPATH=. uv run python scripts/verify_qualification_evidence.py \
  /operator/artifacts/kubernetes-qualification.json \
  --declared-digest "sha256:<report-artifact-digest>" \
  --qualification-id "pod-recovery-20260808" \
  --image-digest "sha256:<deployed-image-digest>" \
  --chart-version "<deployed-chart-version>" \
  --chart-digest "sha256:<deployed-chart-digest>" \
  --configuration-digest "sha256:<rendered-qualification-config-digest>" \
  --migration-head "<expected-alembic-head>" \
  --scope "durable_one_replica_pod_recovery" \
  --namespace "hartmesh-qualification-20260808" \
  --required-scenario accepted_before_client_response \
  --required-scenario accepted_before_worker_start \
  --required-scenario active_execution \
  --required-scenario terminal_before_lifecycle_commit \
  --required-scenario graceful_rollout_termination \
  --required-scenario forced_kill_after_graceful_deadline
```

For the nonempty-skill v2 scope, pass the same Gateway digest through
`--image-digest`, add `--provisioner-image-digest`,
`--verifier-image-digest`, and `--sandbox-image-digest`, select scope
`durable_one_replica_rwx_verified_copy_v2_nonempty_skill`, and independently
require these five scenarios: `nonempty_material_execution`,
`token_review_and_lease_renewal`, `gateway_replacement_cleanup`,
`sandbox_owner_loss_cleanup`, and `process_loss_cleanup`. Do not derive the
expected scenario set from the artifact being verified. The verifier rejects a
v1 artifact under the v2 expectation and vice versa.

Success is one bounded JSON record with `status: verified` and
`trust: external_evidence_verified`; every mismatch exits nonzero with a stable code. The
verifier is offline and never follows report paths or URLs, reads kubeconfig, or emits pod
logs. This digest/exact-subject check is not signature verification or remote attestation.

An enabled run fails for missing CLIs or inputs, unreachable infrastructure,
any skipped scenario, an unreached barrier, timeout, incomplete coverage, or an
unwritable evidence file. Failure preserves bounded namespace logs and the
namespace; success deletes only the namespace the runner created.
`.github/workflows/kubernetes-qualification.yml` exposes the same path as a
manual job using the `QUALIFICATION_KUBECONFIG_B64` secret. Evidence records the
image/chart/config/schema identities, exact PostgreSQL/Redis pod, volume, and
image continuity plus their versions, confirmed context, operator-reported
driver, all six scenario outcomes, and timestamp. It proves
one-replica pod recovery only—not failover, active-active operation, scheduler
HA, or zero-downtime rollout.

### ServiceAccount, metadata, and referenced configuration

`serviceAccount.create` creates a Gateway account with no RBAC and with API-token
automount disabled. Set `create: false` and `name` to select an existing account.
The provisioner has an independent `provisioner.serviceAccount` selector; its
existing Role/RoleBinding remain limited to sandbox lifecycle operations.

Gateway pod labels/annotations are bounded and chart-owned selector/checksum
keys are reserved. `gateway.extraEnvFrom`, `gateway.extraVolumes`, and
`gateway.extraVolumeMounts` accept structured Secret/ConfigMap references. Put
only object names and mount metadata in values—never secret contents.

Hit the Ingress host (map it in `/etc/hosts` for local clusters) to load the UI.

Provisioner sanity check:

```bash
kubectl -n deer-flow exec deploy/deer-flow-provisioner -- curl -s localhost:8002/health
```

## Architecture notes

- **PostgreSQL is the default database.** A bundled single-instance postgres
  StatefulSet (`postgresql.enabled: true`) runs in the namespace and the gateway
  connects via the in-cluster Service. The DSN is auto-generated into a Secret
  (key `database-url`) and injected as `DATABASE_URL`; `config.yaml` references
  it as `$DATABASE_URL` in `database.postgres_url`. Schema is bootstrapped
  automatically on gateway startup (alembic `create_all` + `stamp head`).
  The local-evaluation profile can generate its own Secret. The validated
  production profile requires a separately managed Secret, whether PostgreSQL
  is bundled or external. To use a managed database:
  ```yaml
  postgresql:
    enabled: false
    external:
      existingSecret: deer-flow-managed-postgres # key: database-url
  ```
- **Graceful shutdown & memory drain.** The Gateway owns one ordered deadline: freeze admission; stop channels and scheduler; interrupt/drain local runs; flush memory; close dependencies. Application phase budgets live in `config -> deployment.shutdown`, while `memory.shutdown_flush_timeout_seconds` owns the memory phase. The durable profile also requires a finite `config.database.command_timeout`; `null` remains available only to non-durable local profiles because an unbounded database command could otherwise defeat the admission and shutdown budgets. By default the chart computes `terminationGracePeriodSeconds` from their sum plus `gateway.preStopSleepSeconds` (default 5s) and `gateway.shutdownSchedulingHeadroomSeconds` (default 3s). Set `gateway.terminationGracePeriodSeconds` only for an explicit override, and never below that derived requirement. A timed-out run remains subject to durable orphan recovery after restart. The opt-in suite above, not this configuration statement, is the live one-replica pod-termination evidence.
- **Gateway replicas.** The supported local and production topology is one
  Gateway replica. `durable_one_replica` rejects any other count, and the
  Gateway Deployment uses `strategy.type: Recreate` so an upgrade terminates
  the old execution owner before creating its replacement. The rendered
  strategy explicitly clears previously defaulted RollingUpdate settings
  during upgrade. Replacement causes an availability gap; it is not a
  zero-downtime claim. The chart does
  not install a PodDisruptionBudget, topology spread, leader election, or a
  rolling-zero-downtime policy because those controls would imply coordination
  the runtime does not yet provide. They remain deferred until a real
  multi-replica ownership and scheduler design exists.
- **Scheduled task recovery.** If a deployment explicitly enables
  `scheduler.multi_instance: true`, it must use shared Postgres,
  `run_ownership.heartbeat_enabled: true`, and `run_events.backend: db`.
  Scheduler startup then preserves live scheduled runs owned by another Pod,
  atomically takes over only expired leases, and fences stale post-launch
  bookkeeping. `max_concurrent_runs` is a shared global cap across Pods,
  including pre-launch dispatch reservations. These startup-only controls do
  not qualify scheduler HA or relax this chart's exactly-one-Gateway contract.
- **Redis stream bridge.** A bundled single-instance redis StatefulSet
  (`redis.enabled: true`, `redis:7-alpine`) runs in the namespace and the
  gateway connects via the in-cluster Service. Per-run SSE events are stored in
  Redis Streams so reconnect resumes from `Last-Event-ID`. The URL is
  auto-generated into a Secret for local evaluation (key `redis-url`) and injected as
  `DEER_FLOW_STREAM_BRIDGE_REDIS_URL`; `config.yaml` sets `stream_bridge.type:
  redis` by default. Production validation rejects inline passwords and URLs;
  set `redis.existingSecret` for bundled Redis or
  `redis.external.existingSecret` for managed Redis (key `redis-url`). For a
  Redis shared by tenant releases, set `redis.tenantPrefix` to the release's
  tenant prefix and restrict that release's Redis user to
  `~<tenant>:* &<tenant>:*`. The chart preserves the cache and ownership
  subsystem namespaces when deriving their replace-style overrides. Use
  `redis.keyPrefixes.*` only for explicit per-subsystem replacements. These
  environment variables are injected only into the Gateway; the provisioner
  does not run any of the three Redis-backed subsystems.
- **Persistence.** A PVC (`<release>-home`) backs `/app/backend/.deer-flow`
  (sqlite DB, memory, custom agents, per-thread user-data). The gateway mounts
  it with `subPath: deer-flow` so the layout matches the provisioner's PVC
  user-data mode. Default `ReadWriteOnce`; use `ReadWriteMany` (NFS) on
  multi-node clusters so sandbox Pods on other nodes can mount it. Set
  `persistence.home.existingClaim` to consume a pre-created claim instead;
  leave `persistence.home.enabled: true` so the provisioner receives the same
  claim name. Kubernetes deployments should set `sandbox.volumeMode: pvc` and
  also configure `skills.existingClaim`; Helm rejects a half-configured claim
  pair instead of installing a crashlooping provisioner.
- **Provisioner RBAC.** The provisioner gets a ServiceAccount with a namespaced
  Role in the sandbox namespace (the exact Pod, Service, Secret,
  NetworkPolicy, Lease, and PVC-read verbs used by the provisioner) and a
  ClusterRole containing name-pinned namespace get plus TokenReview create. It uses
  in-cluster service-account credentials — no kubeconfig mount. The Role applies
  to every named resource kind in the sandbox namespace, not only label-matched sandbox objects;
  Kubernetes RBAC cannot narrow these verbs by attempt label. Treat the
  provisioner as a trusted sandbox-namespace control-plane component. Unused
  list/watch/pod-log/update/patch/pods-exec/events verbs were dropped (audited against
  `docker/provisioner/app.py`).
- **Immutable durable skills.** Set
  `provisioner.acceptedSkillProjectionProfile: rwx_verified_copy_v2`, pin both
  `provisioner.image.digest` and `provisioner.sandboxImage` by SHA-256 digest, and
  use `persistence.home.accessMode: ReadWriteMany`. Helm rejects RWO rather than
  adding same-node affinity. The provisioner readiness probe uses `/ready`, which
  confirms the configured claim is `Bound` and actually reports RWX before admitting the profile.
  Each sandbox init verifies the content-addressed snapshot into a private
  `emptyDir`; the main container mounts only that copy read-only and is accessed
  through a per-attempt capability gate. The receipt binds the admitted Pod isolation
  digest, Lease and Pod UIDs, exact NetworkPolicy UID/spec, immutable Secret identities,
  pinned images, verifier receipt, and final materialization digest. A Kubernetes Lease owns every accepted
  attempt; response-loss replay, renewal, reuse, and execution fencing re-read that complete
  tuple, while bounded expiry
  reconciliation cleans process-lost attempts. Scheduling only prefers another
  Gateway node when available and never requires same-node placement. Gateway-to-provisioner
  management calls use a rotating projected ServiceAccount token with a dedicated audience; the
  provisioner validates the exact Gateway namespace and ServiceAccount through TokenReview. This
  management authentication is also rendered when the immutable projection profile is disabled,
  because legacy remote AIO calls use the same protected API. Local evaluation can leave the profile
  `disabled`, in which case remote durable runs remain empty-skill-only. Legacy v1
  receipts are readable compatibility records but also remain empty-skill-only. Fake-Kubernetes
  and rendered-chart tests prove the contract and drift fences, not live cross-node CNI/RWX;
  exact-artifact Kubernetes qualification remains a separate opt-in release gate.

## Upgrading existing values

**Sandbox volume render guard:** a bare default install is now refused at render
time instead of creating a crashlooping provisioner. Existing estate values
must either configure `skills.existingClaim` alongside enabled home persistence,
disable both claim sources, or explicitly set `sandbox.volumeMode: hostpath` for
the legacy local/hybrid layout before `helm upgrade`.

**Namespace default change:** `namespace` now defaults to `""`, so Helm's
`-n/--namespace` selects the release namespace. Existing installations that
relied on the old implicit `deer-flow` value must install with `-n deer-flow`
or set `namespace: deer-flow` explicitly. This is the one intentional default
behavior change in the split-namespace patch; empty `sandboxNamespace` otherwise
preserves single-namespace sandbox placement.

Legacy `image.registry`, `image.tag`, and the three image-name keys continue to
render tag references. Existing raw `config:` overrides remain valid, but an
override that changes `database.backend` must also set the matching
`deployment.persistenceTier`; the chart will not render a contradictory storage
claim. The new `deployment.mode` defaults to `local_evaluation`; adopting
production validation is deliberate: migrate to per-workload
repositories/digests and externally managed credential Secrets, set
`persistenceTier: shared_durable`, and set the embedded runtime config profile
to `durable_production`. Helm then rejects invalid digests, process-local
storage, multiple Gateway replicas, inline credentials, and unsafe
probe/shutdown timing before an install or upgrade. This validation is
deployment reproducibility, not evidence that live Kubernetes
termination/recovery has been qualified.
- **Sandbox volumes.** Set `sandbox.volumeMode: pvc` and provide
  `skills.existingClaim`; the enabled home persistence supplies the other claim
  name. Empty or `pvc` mode now rejects exactly-one-claim configurations during
  Helm rendering. Select `hostpath` explicitly only to preserve the legacy
  local/hybrid layout.
- **Skills.** Disabled by default (emptyDir at `/app/skills`). Populate via
  `skills.existingClaim` or `skills.configMap`, or bake skills into a custom
  gateway image.

## Security

### Enforced posture

All workloads run as **non-root** with **all Linux capabilities dropped**. No
container escalates privileges or runs as uid 0.

| workload | runAsUser | fsGroup | writable-path handling |
|---|---|---|---|
| gateway | 1000 | 1000 | `.deer-flow` PVC group-writable via fsGroup; `PYTHONDONTWRITEBYTECODE=1` suppresses `.pyc` writes; `UV_CACHE_DIR=/tmp` |
| frontend | 1000 (`node`) | 1000 | `emptyDir` at `/app/frontend/.next/cache` (root-owned in the image) |
| nginx | 101 (`nginx`) | 101 | command writes the rendered config to `/tmp/nginx.conf` and loads `nginx -c /tmp/nginx.conf` (since `/etc/nginx` is root-owned); `emptyDir` at `/var/cache/nginx` |
| provisioner | 1000 | — | no PVC; `PYTHONDONTWRITEBYTECODE=1` |
| postgres | 999 (`postgres`) | 999 | official `postgres:16` entrypoint detects non-root and skips the chown/gosu dance; data PVC group-writable via fsGroup |
| redis | 999 (`redis`) | 999 | official `redis:7-alpine` entrypoint detects non-root and skips the gosu dance; data PVC group-writable via fsGroup |

Every container sets:

- `runAsNonRoot: true`
- `allowPrivilegeEscalation: false`
- `capabilities.drop: ["ALL"]`
- `seccompProfile: { type: RuntimeDefault }`

All listening ports are >1024 (8001 / 3000 / 2026 / 8002 / 5432), so no
`NET_BIND_SERVICE` capability is required.

**ConfigMap rollout.** ConfigMaps mount via `subPath`, which does **not** receive
in-place updates — a `helm upgrade` that changes only a ConfigMap would leave
pods on stale config. Each pod template carries a `checksum/*` annotation (SHA256
of the rendered ConfigMap): `checksum/config` + `checksum/extensions` on the
gateway, `checksum/nginx` on nginx. Any content change alters the pod spec and
triggers workload replacement. The Gateway specifically uses `Recreate`, so its
old Pod terminates before the replacement Pod is created.

**Resource defaults.** Every workload ships with modest requests+limits in
`values.yaml`; override per workload (`gateway.resources`, `frontend.resources`,
`nginx.resources`, `provisioner.resources`, `postgresql.primary.resources`,
`redis.primary.resources`).

### Not yet enforced (deferred hardening)

These are intentionally **not** set in this chart revision. Each can be added
per-workload with testing:

- **`readOnlyRootFilesystem: true`** — makes the container's root filesystem
  immutable so a compromised process can't persist changes to the image. Not
  enabled because it requires auditing every runtime write path and mounting an
  `emptyDir` over each. Known paths:
  - gateway / frontend / nginx / provisioner: `/tmp` (uv cache, python tempfiles,
    the nginx config + pid, node temp) — one `emptyDir` at `/tmp` each.
  - postgres: `/tmp` **and** `/var/run/postgresql` (the Unix-socket dir).
  The first four are mechanical. **postgres is the hard case** — the official
  image writes its socket to `/var/run/postgresql` and isn't designed for a
  read-only root, so it may need socket-path redirection (`PGHOST`/`unix_socket_directories`).
  Optionally, add `USER` directives to the `backend/Dockerfile`,
  `frontend/Dockerfile`, and `docker/provisioner/Dockerfile` so the images are
  non-root by default (defense in depth — the chart already forces the uid via
  `securityContext`, so this is not required). A cluster enforcing the
  `restricted` Pod Security Admission standard would require this setting.
- **Provisioner RBAC narrowing.** The Role grants the audited verbs required
  for sandbox lifecycle on resource kinds in the dedicated
  sandbox namespace. These verbs still apply to *all* matching kinds there,
  not just sandbox Pods — RBAC can't scope by label, so the remaining
  option for finer restrictions is admission control (OPA/Kyverno).
- **`startupProbe`.** Workloads have readiness + liveness probes but no startup
  probe. The gateway's `livenessProbe.initialDelaySeconds: 30` covers slow starts
  today; a `startupProbe` would let it take arbitrarily long to initialize
  without risking a liveness kill during a cold start (e.g. slow model config
  load).

None of these affect correctness of the current deployment.

### Migrating an existing volume to non-root

`fsGroup` does **not** apply to `subPath` mounts, and it changes group ownership
but not file mode — so a PVC written by an earlier **root** run (e.g. a cluster
that ran the gateway as root before enabling this hardening, or a backup restore
of root-owned files) will keep files like `.jwt_secret` at `0600 root:root`. The
non-root gateway (uid 1000) then can't read them and crashes on the first auth
request with `RuntimeError: Failed to read JWT secret from .../​.jwt_secret`.

**Fresh installs are unaffected** — uid 1000 creates every file as `1000:1000`.

To fix an existing root-written PVC, run a one-shot root pod that chowns the
volume to the gateway uid (1000), then restart the gateway:

```bash
cat <<'EOF' | kubectl apply -n deer-flow -f -
apiVersion: v1
kind: Pod
metadata: { name: fix-home-perms, namespace: deer-flow }
spec:
  restartPolicy: Never
  containers:
    - name: chown
      image: busybox:1.36
      command: ["sh", "-c"]
      args: ["chown -R 1000:1000 /home-pvc/deer-flow && chmod -R g+rwX /home-pvc/deer-flow"]
      volumeMounts:
        - { name: home, mountPath: /home-pvc }
  volumes:
    - name: home
      persistentVolumeClaim: { claimName: deer-flow-deer-flow-home }
EOF
kubectl -n deer-flow wait --for=condition=Ready pod/fix-home-perms --timeout=30s
kubectl -n deer-flow delete pod fix-home-perms
kubectl -n deer-flow rollout restart deploy/deer-flow-deer-flow-gateway
```

(On a single-node cluster the fix pod can mount the RWO PVC concurrently with the
gateway; on multi-node, scale the gateway to 0 first.) A durable alternative —
an opt-in root `volumePermissions` initContainer that chowns on every start (the
Bitnami pattern) — is not yet wired into this chart; it would introduce a root
container, so it's left as an operator decision for now.

## Sandbox Service type

The provisioner exposes each sandbox Pod behind a per-sandbox Service whose type
is controlled by `provisioner.sandboxServiceType` (default `ClusterIP`).

**`ClusterIP` (default, recommended).** The provisioner returns a cluster-DNS
URL - `http://sandbox-<id>-svc.<namespace>.svc.cluster.local:8080` - so the
gateway reaches its sandbox entirely inside the cluster network. No node IP, no
high port, and the code-execution surface is **not** bound on every node's
interfaces. This is correct for the chart, where the gateway always runs
in-cluster.

**`NodePort` (Docker-Compose/hybrid escape hatch).** Set
`provisioner.sandboxServiceType: NodePort` only when the gateway is *not* in K8s
(e.g. the compose dev path, where the gateway is a container reaching sandbox
Pods on the host's Docker Desktop K8s). The provisioner then returns
`http://{NODE_HOST}:{NodePort}`. `NODE_HOST` defaults to the provisioner pod's
node IP via the [downward API](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/)
(`status.hostIP`); because a NodePort is exposed on every node, the gateway can
reach `<node-IP>:<NodePort>` on most clusters without configuration. Override
`provisioner.nodeHost` only if your CNI or network policy blocks pod->node-IP
traffic:

```bash
kubectl get nodes -o wide    # use INTERNAL-IP or EXTERNAL-IP
```

```yaml
provisioner:
  sandboxServiceType: NodePort
  nodeHost: 192.168.x.x
```

On multi-node clusters, also switch `persistence.home.accessMode` to
`ReadWriteMany` (this is orthogonal to the Service type - it governs whether a
sandbox Pod can be scheduled on a node other than the gateway's).

## Lint / dry-run

```bash
helm lint deploy/helm/deer-flow
helm template deer-flow deploy/helm/deer-flow -n deer-flow -f my-values.yaml | \
  kubectl apply --dry-run=client -f -
```

## Uninstall

```bash
helm uninstall deer-flow -n deer-flow
# the PVC is NOT deleted by default — remove it manually if desired:
kubectl -n deer-flow delete pvc -l app.kubernetes.io/instance=deer-flow
```

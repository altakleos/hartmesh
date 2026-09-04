# HartMesh

[English](README.md) | [简体中文](README_zh.md) | [日本語](README_ja.md) | [Français](README_fr.md) | [Русский](README_ru.md) | [Español](README_es.md) | [Português](README_pt.md) | [Deutsch](README_de.md)

## The dependable execution layer for DeerFlow agents.

HartMesh is an operations-focused DeerFlow distribution for replay-safe invocation, enforceable policy boundaries, inspectable lifecycle evidence, and deployment guarantees that state exactly what they cover.

Built on DeerFlow's workspace, sandboxes, memory, skills, tools, subagents, schedules, and channels.

> [!IMPORTANT]
> **Status: pre-release.** This source tree contains the implemented runtime and offline contract evidence, but no HartMesh release tag contains it.
>
> Exact live-deployment qualification remains a separate, artifact-bound gate.

HartMesh is an independent downstream distribution of ByteDance's [DeerFlow](https://github.com/bytedance/deer-flow). It is not an official DeerFlow release and is not affiliated with or endorsed by ByteDance.

[**Evaluate the preview**](#workspace-quickstart) · [**Inspect the evidence**](backend/docs/INVOCATION_RUNTIME.md) · [**Explore the runtime contract**](backend/packages/runtime-api/README.md)

## Why HartMesh exists

Agents are easy to start and hard to operate.

Clients retry, policies change, skills evolve, processes fail, and multiple services may need to observe the same work.

HartMesh adds a dependable invocation boundary around DeerFlow so accepted work can be retried, governed, inspected, and controlled coherently.

Within one authenticated scope and while the normal run row is retained, repeat the same strict request under one external key and HartMesh returns the retained `run_id`.

Change canonical execution intent under that key and it returns a conflict.

That is replay-safe admission, not exactly-once execution of model, tool, provider, or other external side effects.

HartMesh is designed first for:

- platform developers embedding DeerFlow work in APIs, services, schedules, or channels;
- operators evaluating a governed, durable one-Gateway topology; and
- teams running high-value scheduled and signed-GitHub workflows.

## Built on DeerFlow, hardened for operations

DeerFlow supplies the agent foundation: the workspace, LangGraph harness, sandboxes, memory, skills, tools, subagents, schedules, and native channels.

HartMesh keeps that experience and adds the control plane around accepting, governing, observing, and controlling long-running work.

This is a fixed-snapshot comparison. See [compatibility and provenance](#compatibility-upstream-baseline-and-release-status) for the exact commits; it is not a claim about every future upstream release.

| You keep from the baseline | HartMesh adds around it |
| --- | --- |
| Workspace, harness, memory, sandboxes, skills, tools, subagents, schedules, and channels | One source-aware admission boundary; delivery durability stays transport-specific |
| DeerFlow thread/run lifecycle, Gateway REST routes, and LangGraph-compatible routes | Source-scoped canonical external keys, retained admission identity, and explicit changed-intent conflict |
| Agent, extension, and sandbox configuration | Pinned accepted execution material |
| Gateway and embedded integration surfaces | Strict `ensure`, `observe`, and fenced `control` records |
| Local and Helm operation | Lifecycle evidence plus explicit persistence, topology, and qualification reporting |

What you keep is DeerFlow's agent foundation and compatibility surface. What you gain is an evidence-bearing invocation control plane.

You still do not get active-active Gateway HA, scheduler HA, universal crash-resume, or exactly-once external side effects.

## What happens when…?

| Scenario | HartMesh behavior |
| --- | --- |
| [A client retries after losing the response](backend/tests/test_invocation_idempotency.py) | Equal canonical intent returns the accepted run; changed intent conflicts. |
| [A skill changes after admission](backend/tests/test_accepted_skill_snapshots.py) | The accepted invocation keeps one captured skill tree across agent and sandbox consumers; later work sees the edit. |
| [A remote sandbox executes accepted skills](backend/tests/test_kubernetes_accepted_skill_projection.py) | The supported `rwx_verified_copy_v2` projection binds and revalidates admitted skill and isolation evidence before graph/model work; live cross-node qualification remains artifact-bound. |
| [A service acts for a human or observes another owner](backend/tests/test_service_observation_grants.py) | Human subject, acting service, and source evidence remain separate; cross-owner observation requires a finite grant plus current authorization. |
| [A required policy capability is unhealthy](backend/docs/INVOCATION_RUNTIME.md#capability-health-and-required-mcp-preparation) | Readiness and genuinely new admission fail closed; equal replay reuses sealed acceptance evidence, while current observation authorization still governs disclosure. |
| [An operator asks what is durable or qualified](backend/app/runtime/deployment.py) | The report names persistence; a declared reference is operator-asserted, and only exact evidence matched by the [offline verifier](backend/scripts/verify_qualification_evidence.py) supports independent qualification. |
| [Lifecycle history is pruned or inconsistent](backend/tests/test_invocation_lifecycle_query.py) | Bounded observation returns typed cursor or integrity outcomes instead of silently presenting invalid history. |
| [A signed GitHub delivery is interrupted or its thread is busy](backend/tests/test_durable_inbound_receipts.py) | PostgreSQL-backed receipt recovery can reclaim an expired lease, preserve FIFO deferral, and converge on the same accepted run. |
| [A signed GitHub delivery becomes permanently invalid](backend/tests/test_inbound_receipt_operations.py) | An administrator can inspect only bounded evidence, then exact-fence either a requeue or a logical discard into ordinary completed-row retention. |
| [A supported external search returns mutable or hostile content](backend/docs/EVIDENCE_BEARING_RETRIEVAL.md) | Server policy is fixed before provider I/O; safe source references and the exact post-sanitization, post-budget result digest are committed atomically with the durable tool receipt, without persisting the query or result text. |

<!-- Future demo: add a 30–60 second terminal capture showing a keyed invocation, a simulated lost response, an equal retry returning the same run_id, a changed-intent conflict, and lifecycle observation. -->

## Choose your path

| Path | Best for | First proof |
| --- | --- | --- |
| [Workspace](#workspace-quickstart) | Developers evaluating the full DeerFlow workspace with HartMesh controls | A model response through the unified local entry point |
| [Runtime integration](#durable-invocation-http-api) | Platforms embedding agent work in services, schedules, or channels | Equal keyed requests return one `run_id`, followed by lifecycle observation |

Operators evaluating a governed deployment can go directly to the [deployment and durability boundaries](#deployment-and-durability-boundaries).

> [!CAUTION]
> **Know the boundary before deployment.**
>
> - The validated topology has exactly one Gateway replica.
> - No active-active Gateway HA, scheduler HA, or zero-downtime rollout is claimed.
> - Replay-safe admission is not exactly-once external side effects or universal crash-resume.
> - Local memory and SQLite channel ingress remain best-effort.
> - PostgreSQL and exact independently verified evidence are required for the corresponding shared-durable and qualification claims below.

## Workspace quickstart

Work from the current HartMesh checkout at the repository root.

For this preview path, have Python 3.12+, Node.js 22+, pnpm or Corepack, `uv`, GNU Make, nginx, Docker or Apple Container, model credentials, and approximately 4 CPU cores and 8 GB RAM. Windows local development uses Git Bash.

When `make setup` asks for execution mode, choose **Container sandbox**. Durable invocations using `LocalSandboxProvider` work only with an explicitly empty effective skill set; this checkout enables built-in skills by default.

> [!WARNING]
> `make dev` is a trusted-network development path: Gateway `8001`, frontend `3000`, and local nginx `2026` use wildcard host listeners. Run it only on a trusted or host-firewalled machine while completing first-admin setup.

Configure, diagnose, and launch:

```bash
make check
make setup
make doctor
make dev
```

`make setup` writes the gitignored local configuration. `make dev` reruns the tool check, synchronizes dependencies, and starts the Gateway, frontend, and nginx.

`make install` is optional for contributors who also want pre-commit hooks.
From `backend/`, `make test` selects the lock-pinned OpenSandbox SDK used by
the offline Phase 0 feasibility probe. This test-only selection does not make
OpenSandbox a required runtime or harness dependency.

Open [http://localhost:2026](http://localhost:2026). On a new installation, complete first-admin setup, create a thread, and submit a prompt.

Success means the workspace streams the configured model's response through the Gateway-backed run lifecycle.

Stop the stack from another terminal:

```bash
make stop
```

This evaluates the inherited workspace and local stack. Continue with the runtime path to verify HartMesh's retained `run_id` behavior; workspace success alone does not establish PostgreSQL durability or live Kubernetes qualification.

## Durable Invocation HTTP API

Use the pre-release `/api/runtime/v1` surface when a trusted backend service needs `ensure → observe`.

It uses the strict records in the stdlib-only [`deerflow-runtime-api`](backend/packages/runtime-api/README.md).

This path does not require the browser workspace, but it still requires a configured HartMesh checkout. From the repository root, first-time evaluators should run:

```bash
make check
make setup
make doctor
```

Configure credentials for the model selected by `make setup`. If you already completed the workspace setup, skip these commands.

First-time runtime evaluators must also choose **Container sandbox**. The Local provider requires an empty effective skill set for durable execution.

As above, `make dev` uses wildcard host listeners on ports `8001`, `3000`, and `2026`; use a trusted or host-firewalled machine.

If the root `.env` already defines a nonblank `DEER_FLOW_INTERNAL_AUTH_TOKEN`, use that configured value in the client terminal.

Remove or comment out a blank assignment, because `.env` loading overrides the shell export below. Otherwise generate a token before starting HartMesh:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN="$(
  uv run --project backend python -c 'import secrets; print(secrets.token_urlsafe(32))'
)"
printf 'Copy this token into the trusted client terminal:\n%s\n' \
  "$DEER_FLOW_INTERNAL_AUTH_TOKEN"
make dev
```

In a second terminal, export the printed token and run this standard-library-only client:

```bash
export DEER_FLOW_INTERNAL_AUTH_TOKEN='<paste the generated token>'

uv run --project backend python - <<'PY'
import json
import os
from urllib.request import Request, urlopen
from uuid import uuid4

BASE = "http://localhost:2026/api/runtime/v1"
HEADERS = {
    "Content-Type": "application/json",
    "X-DeerFlow-Internal-Token": os.environ["DEER_FLOW_INTERNAL_AUTH_TOKEN"],
}


def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = Request(BASE + path, data=data, headers=HEADERS, method=method)
    with urlopen(request) as response:
        return response.status, json.load(response)


evaluation_id = uuid4().hex
intent = {
    "api_version": "deerflow.runtime/v1",
    "kind": "invocation.ensure",
    "external_key": f"readme-evaluation-{evaluation_id}",
    "thread_id": f"readme-evaluation-{evaluation_id}",
    "agent_hint": None,
    "input": {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.input.graph",
        "value": {
            "messages": [
                {
                    "role": "user",
                    "content": "Explain replay-safe admission in one sentence.",
                }
            ]
        },
    },
    "options": {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.options",
        "model_name": None,
        "thinking_enabled": None,
        "multitask_strategy": "reject",
        "checkpoint_id": None,
        "interrupt_before": None,
        "interrupt_after": None,
    },
}

first_status, first = call("POST", "/invocations/ensure", intent)
replay_status, replay = call("POST", "/invocations/ensure", intent)
assert first["run_id"] == replay["run_id"]
print("ensure:", first_status, first["disposition"], first["run_id"])
print("replay:", replay_status, replay["disposition"], replay["run_id"])

run_id = first["run_id"]
_, observation = call("GET", f"/invocations/{run_id}?limit=100")
print("observe:", observation["status"], observation["state_version"])
PY
```

A fresh key returns `201 created`; its equal replay returns `200 known` with the same `run_id`. Change the message while retaining the key to receive a typed `409 conflict` instead of different work under one key.

This example uses the built-in `gateway-internal` service and intentionally omits human delegation. Never expose the internal token to a browser or untrusted client.

Runtime-specific owner delegation revalidates `X-DeerFlow-Owner-User-Id` against an existing local user.

See the [principal projection](backend/app/gateway/services.py) and [identity tests](backend/tests/test_invocation_identity_separation.py).

For DTOs, cursor paging, typed failures, and fenced cancellation, see the [runtime contract](backend/packages/runtime-api/README.md) and [HTTP reference](backend/docs/API.md#durable-invocation-runtime-api).

A clarification answer starts a **new invocation on the same DeerFlow thread**.

Stop HartMesh from the repository root when the evaluation is complete:

```bash
make stop
```

## Operational value

### Replay-safe admission

A stable external key plus complete canonical caller intent converges on one retained invocation. Equal intent returns that row in any lifecycle state; changed intent conflicts, and only the creator attaches a worker.

This guarantee lasts while the normal run row is retained. It does not deduplicate arbitrary external side effects.

Durable MCP tasks apply the same fail-closed principle through a separate
private exact-request HMAC; their public lineage remains redacted structural
evidence. Enabling `mcp_tasks` requires the dedicated versioned replay keyring,
and rotation retains old keys until their rows can no longer replay. Exact-two
replicas also compare a non-secret confirmation of the complete keyring in their
topology fingerprint, so key changes use a quiesced restart rather than a
rolling update. See the
[MCP task guide](backend/docs/MCP_SERVER.md#durable-background-tasks-with-ordinary-mcp-tools).

Evidence: [`idempotency.py`](backend/app/runtime/idempotency.py) and [`test_invocation_idempotency.py`](backend/tests/test_invocation_idempotency.py).

### One admission boundary

HTTP create, stream, and wait routes, scheduled tasks, authenticated native channels, and embedded services enter the same `InvocationRuntime`. Source authentication, acknowledgement, and ingress durability stay source-specific.

Evidence: [`invocation.py`](backend/app/runtime/invocation.py) and the [closure matrix](backend/docs/INVOCATION_RUNTIME.md#concern-to-evidence-closure-matrix).

### One server-owned tenant boundary

Each Gateway process/release resolves one immutable tenant identity from
`DEER_FLOW_TENANT_ID` or `deployment.tenant_id`; durable production requires an
explicit non-`local` value. Tenant identity is selected by the operator at service startup. It cannot be selected by an API caller and does not replace per-user authorization.

Accepted evidence, run/event persistence, recovery, extension facts, and all
covered Redis keys use the same pseudonymous tenant reference. Each tenant
release needs a separate database or PostgreSQL schema. Existing deployments
must explicitly bind a legacy nonempty schema and copy retained Redis data
offline; no request and no automatic dual-read path can select or infer a
tenant. See the [tenant identity and migration guide](backend/docs/TENANT_IDENTITY.md).
Populated extension-owned or otherwise unknown tables count as legacy schema
occupancy, and an established binding cannot be removed by migration downgrade.

### Pinned accepted execution material

Admission pins the agent revision, extension generation, trusted context, constraints evidence, effective skill packages, and execution/projection profile.

When governed tool-plane revisions are enabled, admission also binds the active
deployment-base revision, the verified user's overlay (or canonical empty
marker), their generations, the observed projection, secret-safe effective MCP
structure/tool allowlists, and the composed effective digest. Skill bytes and
MCP tool objects are captured before a second generation check, so promotion or
rollback cannot create a mixed accepted snapshot. Running and recovered work
uses that accepted material rather than rereading the newest mutable
`extensions_config.json`. See the
[governed tool-plane guide](docs/GOVERNED_TOOL_PLANE.md).

It also resolves the effective subagent catalog once, including each allowed worker's prompt, model/profile settings, tools, skills, limits, source version, and definition digest. Lead discovery, delegation policy, worker construction, retries, and recovery all use that catalog. Managed subagent changes apply to invocations accepted after the edit; an in-flight or recovered invocation uses its accepted snapshot.

One immutable accepted skill tree contains the transitive union needed by the lead and its allowed subagents. Prompt, discovery, activation, and tool policy expose only the accepted per-agent scope; because all accepted packages may share one sandbox tree, that scoping is not a filesystem-confidentiality boundary.

Non-durable accepted-only isolation can use local container-backed AIO. Durable
accepted sandbox operations require the remote AIO/Kubernetes
`rwx_verified_copy_v2` profile plus a current, byte-digest-pinned live
qualification artifact. This checkout ships no passing artifact.

For a qualified durable run, every command and file operation crosses one
`AcceptedSandboxSession`. It checks the current SQL run (or batch-child attempt)
fence and the existing provider lease/evidence immediately before delegation.
This is explicitly check-then-call: AIO does not atomically carry its ownership
epoch into an operation, so one call racing loss may start, but observed loss
blocks every later call and stale terminal success. Portable V2 evidence binds
the accepted invocation and governed tool-plane digests while replacing the raw
provider resource with a tenant-bound commitment. See the
[accepted sandbox execution contract](backend/docs/ACCEPTED_SANDBOX_EXECUTION.md).

OpenSandbox support for ordinary execution is distinct from HartMesh-qualified immutable accepted material. Nonempty durable skills are supported only for the exact live-qualified profile and artifact.

OpenSandbox has no such qualified profile in this release: its pinned control
plane lacks atomic ownership claims and resolved-image digest readback, while
candidate trusted-setup surfaces remain live-unqualified. Configuration and acquisition reject nonempty
accepted material before model work. `LocalSandboxProvider`, E2B, custom, and
other remote profiles also remain empty-only. Offline projection or SDK-surface
evidence does not establish live qualification. See the
[OpenSandbox Phase 0 decision](backend/docs/OPENSANDBOX_ACCEPTED_MATERIAL_FEASIBILITY.md).

Evidence: accepted-execution sources in [`runtime/`](backend/packages/harness/deerflow/runtime/) — `accepted_invocation.py`, `agent_revision.py`, and `skill_snapshot.py`.

Remote evidence: [`skill_projection.py`](backend/packages/harness/deerflow/runtime/skill_projection.py) and [projection tests](backend/tests/test_kubernetes_accepted_skill_projection.py).

### Bound actual agent assembly

For accepted durable runs, HartMesh now validates and atomically binds the
actual assembled lead graph before checkpoint access or model/tool execution.
The retained V1 record contains only the effective model and full digests for
the prompt, authorized tools, ordered middleware, effective skills, policies,
and overall descriptor. Recovery may proceed only when a newly assembled graph
matches that record; a missing descriptor or drift fails closed without
overwriting the original evidence. Authorized lifecycle observation exposes a
smaller revalidated projection with `pending`, `verified`, or
`legacy_unavailable` status. The same authorized summary exposes only the
accepted subagent catalog version, digest, count, and allowed names—not worker
prompts or definitions.

This is an execution record—what HartMesh assembled and admitted—not a
signature, proof of model/tool correctness, or cryptographic source-code
attestation.

Evidence: [`assembly_evidence.py`](backend/packages/harness/deerflow/runtime/assembly_evidence.py), [store contracts](backend/tests/test_assembly_evidence_store.py), and [worker recovery tests](backend/tests/test_accepted_invocation.py).

### Durable tool-attempt receipts

After accepted assembly is bound, every lead or delegated tool attempt writes a
stable, fenced start before inner policy or tool code and at most one terminal
outcome. A process-loss gap remains visible as `indeterminate`; compact `r1`,
`r2`, … receipts in model context stay renumberable and independent. Authorized
one-run observation exposes an opt-in, cursor-paged receipt projection without
raw arguments, results, credentials, or exception messages.

Recovery binds replay to store-owned durable attempt history: a reconstructed
local retry counter reuses the latest start and any terminal, while its next
live retry reserves the next attempt.

A durable receipt records HartMesh's observation of a tool attempt. It does not
guarantee an external side effect occurred exactly once or that the tool result
was correct.

Evidence: [`tool_evidence.py`](backend/packages/harness/deerflow/runtime/tool_evidence.py), [event-store contracts](backend/tests/test_tool_receipt_event_store.py), and [HTTP reference](backend/docs/API.md#durable-invocation-runtime-api).

### Evidence-bound durable subagent batches

When `subagent_batches.enabled` is true, an accepted lead run may use the
explicit `batch_task` tool to persist many independent subagent items. The tool
schema carries only operational items and requested limits: tenant, parent
invocation and assembly, selected subagent snapshot, and the active tool
receipt come from server-owned runtime context. Acceptance and item commitments
are written before work is claimable, and recovery cannot broaden them from
live configuration.

Attempt delivery is at least once. Lease expiry or process loss can repeat an
item and its external side effects; HartMesh guarantees one fenced accepted
terminal publication, not exactly-once behavior outside the database. The
initial cancellation policy is non-cascading, so cancelling a parent run does
not cancel its batch. Use the explicit batch cancellation tool or API.

Owner-scoped APIs expose batch/item progress, payload-free attempt and lifecycle
evidence, and a separate protected JSONL result export. Durable production
profiles reject enabled batches until their artifact-bound PostgreSQL process-
restart/failover gates have passed. See the [durable subagent batch guide](docs/DURABLE_SUBAGENT_BATCHES.md)
for limits, recovery, legacy cleanup, qualification status, and guarantees.

Live rich-event writes are likewise bound to the admitted tenant, run, worker,
and lifecycle epoch. Authorized graph output remains opaque in `run.end`, while
the additive `run.terminal.v1` and bounded correlated failure records provide a
safe terminal projection without persisting provider exception messages or
tracebacks.

### Auditable automation identities

Every newly accepted durable invocation binds a server-created, secret-free
credential projection to the existing principal, optional acting service,
Origin, and tenant evidence. Browser sessions, PATs, authenticated internal
services, and supported channels share the same versioned contract. A PAT
remains a user credential and uses its existing random UUID as the public
reference; raw tokens, token digests, cookie values, credential names, and
service secrets never enter run or credential-audit evidence.

PAT access is explicit and default-deny per method/path/scope. Its scopes are
intersected with the owner's current permissions on every request. Revocation
does not rewrite historical evidence, but a revoked or expired PAT cannot
submit, observe, control, replay-disclose, or export new data. Required audit
writes close durable admission and privileged run/runtime/MCP/batch/scheduler
controls; routine use/failure observations are bounded daily aggregates. See the
[actor, route, replay, and retention contract](docs/AUDITABLE_AUTOMATION_IDENTITIES.md).

### Portable terminal-run evidence

An authorized user can export a terminal durable run through the dedicated
`GET` status and `POST` download surfaces at
`/api/threads/{thread_id}/runs/{run_id}/artifacts/evidence-bundle`. The ZIP
contains the exact presented artifact bytes plus the canonical
`hartmesh-evidence/manifest.v1.json`, which binds safe admission, assembly,
lifecycle, tool, MCP, batch, sandbox, retrieval, and qualification references.
Missing required evidence fails closed. Capabilities not accepted by the run
are explicit `absent_by_design` sections, while an accepted but unused durable
MCP surface is recorded as a complete empty section. Digest-only evidence links
make child-to-receipt joins checkable offline. The existing ordinary artifact
archive remains separate and carries no HartMesh evidence claim.

Verify a downloaded bundle without application configuration, a database, or
network access:

```bash
python -I scripts/verify_run_evidence_bundle.py path/to/bundle.zip
```

This verifies internal digests, section roots, declarations, and artifact
bytes. Bundles are **not signed or independently attested**. Artifact names and
contents can be sensitive, and deleting the server-side run cannot recall a
downloaded copy. See the [bundle format, API, verifier, privacy, and limits](docs/RUN_EVIDENCE_BUNDLES.md).

### Policy that follows execution

HartMesh keeps effective subject, acting service, and source evidence distinct.

Authenticated services remain owner-scoped unless an operator grants a finite observation scope. The grant bounds discovery; current authorization still decides what may be returned, and cancellation does not inherit it.

When an operator enables invocation-operation authorization or configures authoritative v2 constraints, those named operations fail closed. Operator-required capability health and MCP preparation do too.

Authorization and invocation-operation controls are disabled by default, and `required_capabilities` defaults empty.

Optional observational middleware and the legacy API-writable MCP interceptor retain fail-open or warning-and-skip behavior.

Owner and route checks remain, but HartMesh does not ship a universal organization policy or guarantee arbitrary third-party tools.

Evidence: [extension contract](backend/packages/extension-api/README.md), [authorization](backend/app/runtime/authorization.py), [constraints](backend/app/runtime/constraints.py), and [visibility](backend/app/runtime/visibility.py).

### Portable runtime integration

The stdlib-only `deerflow-runtime-api` defines strict immutable records and one `DurableInvocationPort`: `ensure`, invocation/context `observe`, fenced `control`, and `capabilities`.

Authenticated HTTP and the application-hosted in-process adapter share those records and a conformance suite. The synchronous `DeerFlowClient` is not a durable adapter; v1 does not provide broker push, context export, or context retirement.

Evidence: [runtime package](backend/packages/runtime-api/README.md) and [transport conformance](backend/tests/test_runtime_api_conformance.py).

### Transactional lifecycle integrity

With the SQL store, a state change and its safe lifecycle event commit atomically under one state version. Bounded observation uses an authoritative snapshot and returns typed outcomes for pruned, future, or inconsistent history.

PostgreSQL repeatable-read and cross-session behavior are release claims only when the external PostgreSQL gate passes.

Evidence: [`sql.py`](backend/packages/harness/deerflow/persistence/run/sql.py) and [`0017_lifecycle_integrity.py`](backend/packages/harness/deerflow/persistence/migrations/versions/0017_lifecycle_integrity.py).

Contract tests: [atomic lifecycle store](backend/tests/test_invocation_lifecycle_store.py) and [typed lifecycle queries](backend/tests/test_invocation_lifecycle_query.py).

### Durable signed-GitHub ingress

With HMAC verification and PostgreSQL receipts, signed-GitHub ingress persists bounded source bindings before acknowledgement.

Leases and fences can reclaim an expired lease after interruption, preserve busy-thread FIFO, and converge on the same accepted invocation.

Administrators can inspect one dead letter without its message body, requeue an exact still-matching row, or logically discard a permanently invalid runless row into ordinary completed-row retention. Both mutations are fenced and audited; discard never wakes processing.

This claim is limited to verified signed-GitHub ingress with PostgreSQL; other and local channel paths remain best-effort.

Evidence: [receipt store](backend/app/channels/inbound_receipts.py) and [receipt tests](backend/tests/test_durable_inbound_receipts.py).

## Deployment and durability boundaries

Read these limits before following any deployment guide:

- Default and `durable_one_replica` deployments have exactly one Gateway. The
  exact-two topology is candidate-only in this checkout and cannot be unlocked
  by operator-declared evidence.
- No arbitrary active-active scaling, IM connector HA, or zero-downtime rollout is claimed.
- Replay-safe admission is not universal exactly-once execution of external side effects.
- With durable invocation storage, process-loss recovery preserves authoritative
  terminal evidence. The default remains terminalization. Exact-two execution
  takeover is currently unavailable for every eligible orphan, even if its
  process-local claim flag is enabled; mandatory live recovery scenarios remain
  unpassed. The retained recovery schema/coordinator is a future seam, not a
  capability claim.
- Memory is process-local, SQLite is node-durable for invocation state, and PostgreSQL is the shared-durable store.
- The exact-two candidate validates the constructed run adapter at startup and
  readiness: lease deadlines, renewals, and expired-run scans must use
  PostgreSQL-owned `database_v1` time rather than either Gateway pod clock.
- Memory and SQLite native-channel ingress remain best-effort.
- Durable native ingress currently means verified signed-GitHub delivery with PostgreSQL.
- Nonempty accepted skills require a supported accepted-only sandbox path.
- Kubernetes/PostgreSQL qualification requires exact passing evidence for the named image, chart, configuration, schema, topology, scope, and scenarios.
- A collected or skipped opt-in gate is not a qualification pass.

The PostgreSQL and Kubernetes suites are environment-gated release checks. A
default skip, a configured job that fails before its assertions, or an artifact
that does not independently verify against the exact deployment subjects is an
unpassed gate—not durability or recovery evidence.

| Mode | Reported boundary |
| --- | --- |
| `local_development` | Allows process-local state without a durability claim. |
| `durable_production` | Rejects process-local invocation state and requires database-backed run events for fenced tool receipts at startup and readiness. |
| `durable_two_gateway_v1` | Candidate-only exact-two backend profile with PostgreSQL, tenant-scoped Redis, AIO/RWX, scheduler, and MCP fencing; production startup remains blocked. |
| Helm `local_evaluation` | One-Gateway evaluation defaults; explicitly unqualified. |
| Helm `durable_one_replica` | Requires digest-pinned images, PostgreSQL/shared state, and safe probes and shutdown timing; still unqualified without exact passing evidence. |
| Helm `durable_two_gateway_v1` | Renders only in an isolated qualification namespace; a declared reference cannot enable production. |

The administrator deployment report separates persistence tier, health, provenance, and qualification.

A supplied qualification reference remains `operator_asserted`; only the offline verifier can establish `external_evidence_verified` for exact evidence. Portable capabilities do not carry deployment claims.

`durable_two_gateway_v1` is qualified only for the exact two-replica PostgreSQL + Redis + AIO/RWX profile and artifact. It does not claim arbitrary scaling, IM connector HA, cross-region operation, or zero-downtime upgrades.

This checkout ships the implementation and opt-in 16-scenario harness, not a
passing live artifact. See the [exact two-Gateway qualification and maintenance
guide](docs/MULTI_GATEWAY_QUALIFICATION.md). Missing Kubernetes, PostgreSQL,
Redis, routing, or RWX infrastructure is an unpassed release gate.

`GET /health` reports process liveness. `GET /ready` is a bounded ready/not-ready signal. Administrators inspect persistence and qualification at `GET /api/runtime/v1/deployment`.
When present, that deployment report also includes a versioned process-local snapshot of pending and resolved post-commit ownership obligations; the counters reset on restart and are operational telemetry, not durable or multi-replica evidence.

Evidence: [deployment reporting](backend/app/runtime/deployment.py), [qualification verification](backend/scripts/verify_qualification_evidence.py), and the [Helm deployment contract](deploy/helm/deer-flow/README.md).

With durable invocation storage, when an active invocation is lost with its process, recovery records authoritative terminal evidence such as `stop_reason=orphan_recovered`.

Equal replay returns that retained terminal run. Continuing the product intent requires a new invocation under the new process generation.

See [authoritative lifecycle and failure recovery](backend/docs/INVOCATION_RUNTIME.md#authoritative-lifecycle-and-failure-recovery).

### Security boundary

The Compose stacks publish only nginx and bind it to `127.0.0.1:2026` by default. Local `make dev` instead uses wildcard host listeners on ports `8001`, `3000`, and `2026`; do not treat it as carrying the same published-port fence.

HartMesh agents can execute commands and read or write files allowed by configured tools. Isolation depends on the provider: `LocalSandboxProvider` shares the Gateway host identity and is not an OS isolation boundary.

Command classification and path rewriting are defense in depth. Use a supported isolated provider for untrusted work.

Provider capability declarations are not qualification. Remote AIO is the only
durable candidate in this release; its declared operation fencing and protected
process-loss lookup are both false, exact-two remains rejected, and missing live
qualification fails before model/tool work. Lifecycle events expose only safe
acquired/lost/released/cleanup-pending/orphan diagnostics.

Complete first-admin setup before making the service reachable beyond loopback.

Administrators can configure stdio MCP processes and trusted Python plugins, so administrator access is equivalent to code execution.

See the [Helm deployment guide](deploy/helm/deer-flow/README.md) for the exact render contracts, accepted-skill projection, credentials, and qualification procedures.
The chart also documents non-recursive PVC ownership handling and the
values-driven startup budget used by provisioner-created gVisor sandboxes.

## Extension model

HartMesh preserves DeerFlow's skills, tools, MCP servers, custom agents, and middleware model.

The host-independent [`deerflow-extension-api`](backend/packages/extension-api/README.md) adds typed contracts for authorization, identity and trusted-context contribution.

It also covers restrictive constraints, capability health, and required MCP preparation.

Python plugins are trusted operator code loaded at startup from top-level `plugins:` in `config.yaml`. That list intentionally stays outside `extensions_config.json`, which owns MCP and skill configuration. With the default governed tool plane, active MCP and skill changes go through stage, validation, and promotion; direct configuration mutation is a legacy opt-out mode.

Artifact provenance proves which extension bytes/configuration HartMesh admitted. Extensions still execute with Gateway privileges and must come from a trusted operator source.

The extension manager commits a platform-neutral source lock; production images
embed and verify a platform-specific installed manifest before importing plugin
code. Generated Ruff caches are outside managed snapshot identity, so the
documented lint workflow cannot change that source lock. A separate secret-safe
digest binds the ordered deployment configuration.
Use `deerflow extensions verify`, `manifest [--json]`, and `config-digest
--config <path>` to inspect those identities. See the [extension artifact
provenance guide](docs/EXTENSION_ARTIFACT_PROVENANCE.md) for deployment,
migration, and rollback.

An accepted invocation pins one startup-frozen extension generation plus the
artifact, configuration, and capability-manifest digests. Skill changes affect
later admission; plugin changes require a Gateway restart to create a new
generation. Neither changes already accepted work.

The managed Lark/Feishu CLI integration remains user-scoped. After connecting,
**Change Lark app** can replace that user's App ID and App Secret without
reinstalling the skill pack: the CLI validates the new app before activation,
removes the previous app's OAuth tokens, and starts authorization for the new
app. In sandboxed execution, the credential-bearing config root remains
read-only while its `config/locks` subdirectory is mounted separately for
bounded CLI coordination writes.

## Compatibility, upstream baseline, and release status

HartMesh preserves existing `deerflow.*` namespaces, package names, `DEER_FLOW_*` variables, Docker and Helm identifiers, filesystem paths, and Gateway compatibility surfaces.

The product comparison is the fixed local range `e16ef2969b1446162e19af7bdde1446674851e66...4023cb434aa67011b9d18e90029f473b55323856`.

HartMesh `main` incorporates upstream `deerflow/main` through
`30788c79ffd988e110d97dd69fbc17abc50a96c6` (2026-09-02).

That synchronization point is context, not the comparison baseline above, and
HartMesh makes no evergreen superiority claim.

This repository does not yet document a HartMesh sync cadence, API/configuration/database compatibility window, support window, security-fix intake policy, or upstream-contribution policy.

Treat these hashes as provenance, not a maintenance promise.

The Alembic graph has one head: `0011_mcp_tasks` branches into HartMesh's invocation migrations through `0019_inbound_event_identity` and upstream's result, managed-subagent, and scheduled-enqueue work; merge revisions `0020`–`0022` join those branches, `0023_agent_assembly_evidence` binds actual assembly, `0024_tool_receipt_idempotency` fences receipt appends, `0025_tenant_identity` binds the schema tenant, `0026_mcp_task_lineage` seals MCP lineage, `0027_multi_gateway_topology` adds exact-two topology registration plus persisted scheduler generations, `0028_mcp_request_commitment` adds private exact-request MCP replay commitments while cleaning up superseded indexes, `0029_run_recovery_policy` adds immutable run recovery policy, bounded recovery payloads, and database-ordered admission cursors, `0030_run_delivery_owner_backfill` repairs unowned legacy delivery receipts only when their run, thread, and tenant anchors select one authoritative owner, `0031_merge_upstream_0017` joins the personal-access-token branch, `0032_subagent_batch_evidence` binds durable batches to accepted parent evidence and append-only fenced attempts, `0033_automation_identities` tenant-binds PATs and adds bounded credential audit evidence, `0034_tool_plane_revisions` adds per-scope immutable revisions, transition history, active-generation fences, and base/overlay compatibility attestations without blessing existing mutable files, `0035_batch_sandbox_evidence` attaches accepted material-request, execution-evidence, and sandbox-lifecycle fields to durable batch attempts, keeping pre-existing attempts legacy-readable and refusing downgrade once that evidence exists, and `0036_execution_policy_state` adds paired nullable execution-policy state and digest columns to normal runs under run-only and digest-format check constraints, leaving historical runs explicitly legacy and refusing downgrade once policy state exists.

PostgreSQL operators should quiesce writers and back up data before rollback; use the migration guidance in [backend/AGENTS.md](backend/AGENTS.md).

Version sources report `2.1.0`, but no tag contains the audited HartMesh implementation; version strings do not establish a HartMesh release.

[RELEASING.md](RELEASING.md) documents inherited DeerFlow tag mechanics, not a HartMesh-owned release channel.

## Documentation

- [Durable invocation runtime](backend/docs/INVOCATION_RUNTIME.md) — guarantees, evidence, recovery, and deferred scope
- [Durable subagent batches](docs/DURABLE_SUBAGENT_BATCHES.md) — accepted evidence, retries, cancellation, legacy cleanup, and qualification
- [Portable run evidence bundles](docs/RUN_EVIDENCE_BUNDLES.md) — canonical manifest, terminal snapshot, exact artifact bytes, offline verification, and trust limits
- [Execution policy and Evidence panel](docs/EXECUTION_POLICY_AND_EVIDENCE_UI.md) — accepted budgets, private repeated-call commitments, circuit breakers, and bounded operator projections
- [Accepted sandbox execution](backend/docs/ACCEPTED_SANDBOX_EXECUTION.md) — composed run/provider authority, operation gating, capability matrix, lifecycle evidence, and qualification
- [Runtime API](backend/packages/runtime-api/README.md) — DTOs and `DurableInvocationPort`
- [Gateway API](backend/docs/API.md) — authenticated HTTP behavior
- [Extension API](backend/packages/extension-api/README.md) — policy and trust boundaries
- [Extension artifact provenance](docs/EXTENSION_ARTIFACT_PROVENANCE.md) — source/artifact/config identities, migration, and rollback
- [Governed skill and MCP revisions](docs/GOVERNED_TOOL_PLANE.md) — stage/validate/promote, bootstrap, drift, recovery, selectors, and accepted pinning
- [Tenant identity](backend/docs/TENANT_IDENTITY.md) — server-owned trust boundary, schema/Redis migration, ACLs, and rollback
- [Honcho memory backend](backend/packages/harness/deerflow/agents/memory/backends/honcho/README.md) — tenant/user isolation, durable observation limits, and existing-workspace migration
- [Helm deployment](deploy/helm/deer-flow/README.md) — production and candidate qualification contracts
- [Exact two-Gateway qualification](docs/MULTI_GATEWAY_QUALIFICATION.md) — topology boundary, live evidence, maintenance upgrade, rollback, and exclusions
- [Configuration](config.example.yaml) — operator settings
- [Backend guide](backend/AGENTS.md) and [frontend guide](frontend/AGENTS.md) — architecture and tests

## Support and security

Run local diagnostics from the repository root:

```bash
make doctor
make support-bundle
```

Review generated support material before sharing it.
The extension artifact summary contains only parse/verification status, digest
prefixes, API/platform identifiers, and entry counts; it omits source paths,
URLs, plugin configuration, file lists, and contents.
For Honcho, the generated config summary omits the endpoint, raw workspace/user
overrides, assistant peer, and reserved tenant projection. It retains only
HTTP/HTTPS posture and configured override counts; it never includes the API
key or memory content.

This repository does not yet document a HartMesh-owned issue tracker, release channel, or private vulnerability-reporting route.

[CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md) retain ByteDance DeerFlow's upstream destinations; those destinations are not HartMesh-owned support.

Do not put credentials, tokens, private prompts, customer data, or vulnerability details in a public issue. Treat internal tokens, webhook secrets, provider keys, and database credentials as secrets.

## Contributing

For local work, follow the inherited conventions in [CONTRIBUTING.md](CONTRIBUTING.md) and the nearest [AGENTS.md](AGENTS.md) for repository commands and module ownership.

## License

HartMesh retains DeerFlow's [MIT License](LICENSE) and existing notices.

## Acknowledgments

HartMesh exists because ByteDance and DeerFlow contributors released the agent foundation it extends. We also thank the LangChain, LangGraph, and broader open-source agent ecosystems.

HartMesh's downstream operational claims, release status, qualification, and support boundaries remain its own.

# DeerFlow Extension API

`deerflow-extension-api` is DeerFlow's host-independent plugin contract package. It has
no dependency on `deerflow`, `app`, FastAPI, or the Gateway runtime. Extensions should
depend on this distribution and import contracts from `deerflow_extension_api`.

Version 0.12.1 adds the immutable, pseudonymous `TenantReferenceV1` to trusted
run-context contributor requests. Version 0.12.0 introduced the authorization
contracts `Principal`, `AuthzRequest`,
`AuthzDecision`, `AuthzReason`, and `AuthorizationProvider`. Existing host code may keep
using `deerflow.authz.provider`; those names are compatibility re-exports of the same
objects. It also owns the versioned Origin and run-context contributor contracts described
below.

## Identifier domains

The public contracts use named field domains rather than the plugin contribution-ID
grammar:

- agent input matches `[A-Za-z0-9][A-Za-z0-9-]{0,127}` and has one documented
  lowercase canonical identity for compatibility with case-insensitive agent storage;
  `lead_agent` remains the reserved built-in runtime identity and is not a creatable
  custom-agent name;
- thread IDs preserve exact case and match `[A-Za-z0-9_-]{1,64}`;
- model-profile IDs preserve exact case and Unicode, contain no ASCII controls, and are
  limited to 128 UTF-8 bytes;
- MCP server IDs preserve exact case and Unicode under the same non-control 128-byte
  bound, while MCP tool IDs preserve exact case and match
  `[A-Za-z0-9_-]{1,128}`.

The validators never truncate or hash these identities. A profile that exceeds the bound
must be explicitly renamed in `models[].name` and in every agent/request reference. MCP
server keys that are not valid tool-name components remain supported only with
`tool_name_prefix=false`; otherwise startup rejects the configuration with that action.
Prefix-enabled server keys are additionally limited to 126 characters so the separator
and a non-empty tool name fit the host's 128-character callable bound.

## Invocation identity

`InvocationIdentityV1` separates the immutable `effective_subject` whose authority is
being exercised from an optional `acting_service` that authenticated or delegated the
request. A directly authenticated human has a human subject and no actor. A native-channel
or user-owned Scheduled Task invocation keeps its human owner as the subject and records
the channel provider service or scheduler as the actor. A system-owned schedule and an
embedded service invocation use a service subject and invent no human. Provider, connection,
workspace, chat, event, task, and transport evidence remain in the separately sealed
`SealedOriginV1`; they never imply subject privilege.

The host constructs identity and Origin after authentication and scrubs caller attempts to
provide either. Gateway route checks, start/observe/cancel authorization, contributors, constraints, tool/MCP
authorization, and delegated subagents receive the same split identity and final Origin.
Attribute mappings are defensively frozen and the identity records are bounded, frozen,
JSON-safe records with explicit v1 serialization.

`Principal.is_internal` remains as a deprecated compatibility view. With a v1 identity it
is true only when the effective subject is a service; an acting service never makes a human
internal. For records/providers using the legacy fields, the host clears the flag for an
attributed channel user or a non-service/non-internal role. Thus an older provider may see
less privilege, never an end user promoted by transport trust. Existing accepted rows with
the v1 legacy principal JSON remain readable under that conservative rule; new rows store
principal projection version 2 with the nested v1 identity and a digest bound atomically to
admission.

## Server-owned tenant reference

`TenantReferenceV1` is the only tenant shape exposed by the host-independent
extension API. It contains `version=1`, a bounded `tenant-<16 hex>`
`public_ref`, and the full lowercase SHA-256 digest. It is frozen and contains
no operator-readable canonical identifier. The reference is pseudonymous, not
secret or unguessable.

The host supplies the same reference on `OriginContributionRequestV1.tenant`,
`RunContextContributionRequestV1.tenant`, and `TrustedRunContextV1.tenant` for
newly accepted work. The optional default on request/context contracts preserves
source compatibility for extensions and legacy persisted context, but a current
Gateway always supplies it. A contributor may observe the value and include its
own derived safe evidence; it cannot replace the host's accepted tenant anchor.

Tenant identity is selected by the operator at service startup. It cannot be selected by an API caller and does not replace per-user authorization.

Extensions must not derive tenancy from principal/user IDs, thread IDs,
request fields, release names, Kubernetes namespaces, or extension
configuration. An extension needing provider-specific names should receive a
host adapter backed by the appropriate typed `TenantNamespaceV1`; the public
package deliberately does not expose the canonical config object.

Every authoritative factory descriptor also has an optional `health_probe`. Existing
plugins may omit it; a successfully initialized capability without a probe is healthy.
When present, it is an async zero-argument callable returning only
`CapabilityHealthResult(status="healthy"|"unhealthy", diagnostic_code=<bounded code>)`.
The host applies its own timeout/cache/single-flight policy. Probes must not return
credentials, identities, exception text, or request data.

For every operator-required authoritative capability, Gateway readiness and genuinely new
invocation admission require a successful observation from the current immutable extension
generation inside the configured admission window. That health observation is only a
pre-fence: the subsequent `authorize`, `contribute`, `project`, or MCP `prepare_call` remains
authoritative and independently fail-closed. Accepted keyed replay reuses sealed evidence and
does not call a provider again. Health failures expose only bounded codes and correlation IDs;
diagnostics/logs retain only the stable code, exception class, contribution ID, operation, and
correlation ID—not the provider message or traceback.

## Authorization provider contribution

An installed plugin contributes the process's single authoritative provider through a
typed descriptor:

```python
from deerflow_extension_api import (
    AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
    AUTHORIZATION_PROVIDER_KIND,
    AuthorizationProviderFactory,
    ExtensionRegistry,
)


def install(registry: ExtensionRegistry, config) -> None:
    registry.authorization_provider(
        AuthorizationProviderFactory(
            contribution_id="example.authorization",
            capability_api_version=AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
            factory=ExampleAuthorizationProvider,
            kind=AUTHORIZATION_PROVIDER_KIND,
            health_probe=check_authorization_health,  # optional
        )
    )
```

`contribution_id` must be stable across releases. The factory takes no host object and
returns an `AuthorizationProvider`. A plugin does not declare its package name or version;
the Capability Host derives and stamps installed-distribution provenance while loading.
Only one provider factory may be registered. It cannot be combined with the legacy
`authorization.provider.use` class-path configuration.

`AuthzRequest`, `AuthzDecision`, and `AuthzReason` defensively snapshot and recursively freeze
nested provider/caller mappings and sequences. A later mutation of an input or provider-owned
result cannot change authorization evidence; host adapters thaw a fresh mutable wire copy only
where serialization requires one.

The extension snapshot and its generation are immutable after Gateway construction. The
Gateway-owned authorization resolver constructs an extension provider once at startup and
shares that exact object across current route, resource, model, tool, skill-discovery, and
agent-assembly checks. Legacy class-path providers remain supported and may be atomically
replaced when their hot-reloaded configuration changes.

For durable invocations, one immutable generation means one startup-frozen process generation
in the supported one-replica topology. Accepted invocations pin that generation. A restart
constructs a later process generation; the public extension contract does not coordinate
simultaneous generations, rolling replicas, or hot replacement of accepted material.

Operator `authorization.service_observation_grants` is deliberately not another extension
permission contract. It gives an exactly authenticated service only a finite host-owned
run/thread/owner/source search scope. The provider still receives the current
`resource="invocation", action="observe"` request for every matched run or context and remains
the sole binary authority. Grant revocation is re-read on the next observation request;
it does not replace the provider or its authorization generation. Transport trust, service
role text, and caller-supplied attributes cannot create a scope.

Extensions may also contribute observational middleware. Its existing isolation remains
fail-open; authorization-provider factories are authoritative startup capabilities and do
not use that observational failure policy.

## Invocation contributor contributions

Trusted plugins may register typed `OriginContributorFactory` and
`RunContextContributorFactory` descriptors. The Capability Host stamps package
provenance, initializes each factory once at startup, invokes contributors concurrently,
and composes valid results deterministically by stable `contribution_id`.

Origin contributors receive only source kind, the canonical invocation identity, the
immutable safe tenant reference, an
authenticated subject reference,
and host-selected safe source references. Run-context contributors receive an immutable
split principal projection, safe tenant reference, sealed Origin, bound thread, resolved agent revision reference, and
an optional external-key reference. Neither contract receives Gateway objects, raw
credentials, or caller request metadata.

Contributor results own one namespace and contain `SafeContextReferenceV1` values only.
Keys are 1–64 character ASCII identifiers; values are strings, integers, booleans, null,
or lists of those values. Each result is limited to 32 references, 1 KiB per string, and
8 KiB canonical JSON. A reference declares:

- `storage_class`: `persistable` or `runtime_only`;
- `purpose`: `execution`, `correlation`, or `secret_handle`.

Execution references affect replay identity. Correlation references do not. Secret handles
must be stable string identifiers and contribute the identifier, never secret material.
Runtime-only values are redacted from persistence while their safe execution aggregate is
included in the accepted-context digest.

The host combines both contributor phases into one immutable `TrustedRunContextV1` after
validation. It carries the effective subject/acting service, safe tenant reference,
final `SealedOriginV1`, bound
thread and external-key reference, agent/profile revisions, extension generation/manifest,
and three finite namespaced products: persistable references, runtime-only execution
references, and stable secret handles. Per-result limits also apply to the aggregate across
all contributors: 32 fully qualified keys and 8 KiB canonical reference data. Duplicate
fully qualified keys fail deterministically.

`storage_class` is a request to the host, not plugin authority. The v1 host policy accepts
bounded persistable evidence, stable handle identifiers, and runtime-only execution values;
runtime-only correlation has no consumer and fails closed. The trusted-context digest uses
the persisted-safe projection plus the retained runtime-only digest/count, so it remains
stable when a process reconstructs accepted evidence without ephemeral values. Its separate
execution digest excludes contributor correlation; the evidence digest still binds those
audit references.

Only approved persistable references and persistable secret-handle identifiers are stored;
runtime-only values are replaced by their aggregate digest/count. The live record is passed
unchanged to invocation authorization, constraints, lead execution, MCP preparation, and
delegated subagents. `authorization_attributes` is only a read-only, namespaced compatibility
view derived from execution references, not a second producer-owned context channel. A
secret handle is an identifier, not authority or a credential: authorization must allow the
operation before a narrow consumer resolves it, and resolved values must never be returned
through this contract.

Each call has a host-owned two-second timeout. Optional failures are omitted with bounded,
redacted diagnostics. Operators can make a contribution mandatory in startup-only
`config.yaml` with `required_capabilities: [origin_contributor:<id>]` or
`[run_context_contributor:<id>]`; missing or initialization-failed requirements stop
startup, and required invocation-time failure closes that invocation as indeterminate.
This setting is intentionally absent from API-writable `extensions_config.json`.

At Gateway composition, the operator list is validated and partitioned once by capability
owner. The public `ORIGIN_CONTRIBUTOR_KIND`, `RUN_CONTEXT_CONTRIBUTOR_KIND`, and
`MCP_INTERCEPTOR_KIND` constants route contribution-scoped IDs. The host-owned routing table
uses the public `INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY` and
`INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2` constants for the exact IDs owned by the
singular constraints host. Each host validates only its subset. Unknown or duplicate IDs
fail before host construction; duplicate singular constraints registrations remain
ambiguous after positional loader rollback and fail closed when constraints are required.
Future constraints versions add a public version constant and extend the one host-owned
routing table plus the constraints host, without adding pass-through exceptions to
contributor or MCP hosts.

## Invocation constraints contribution

A trusted plugin may register the process's single
`InvocationConstraintsProviderFactory`. Version 2 is a distinct contract rather than a
widening of v1. Its async `InvocationConstraintsProviderV2` receives an immutable
`ConstraintProjectionRequestV2` containing only the split identity, finalized sealed
Origin, bounded namespaced correlation references, thread/external-key reference,
agent/profile revisions, request/trusted-context/manifest digests, extension generation,
and the host's enforceable subagent ceiling. It receives no prompt, credential, arbitrary
kwargs, or host object.

`ConstraintProjectionV2` binds those request, thread, revision, manifest, and generation
facts to short-lived evidence. Its explicit `mandatory_obligations` discriminator accepts
only `max_total_subagents`; an unknown obligation fails closed. A non-negative ceiling is
mandatory when that obligation is present, so zero prohibits subagent dispatch. The host
intersects it with its own ceiling and never permits a projection to widen local policy.
Authorization and operation-time MCP enforcement remain the only authorities for binary
permission and future dynamic effects.

The Capability Host calls the provider directly, outside observational fail-open
middleware, under a host-owned two-second timeout and an injected timezone-aware clock.
At startup it verifies that `project` is declared async; a synchronous method is malformed even
if it happens to return an awaitable. This same authoritative-operation check applies to required
authorization, contributor, and MCP preparation providers, while health-probe compatibility is
validated separately.
When the selected version is operator-required, admission first requires one fresh healthy
snapshot for that exact capability ID; unhealthy, unknown, stale, or unavailable health is
indeterminate and the provider is not called.
It rejects binding mismatches, naive/future/expired timestamps, validity beyond 15 minutes,
unknown fields, malformed evidence, unsupported obligations, and impossible limits. The
effective subagent ceiling is the lower of the provider projection and the static host
ceiling. Registration is singular and loader-attributed. Operators require v2 only through
startup-controlled `required_capabilities: [invocation_constraints.v2]`; the API-writable
extension config cannot activate or require trusted constraint code.

The accepted run persists only the normalized projection and safe evidence ID/digest.
Workers validate the accepted binding and freshness before graph construction and again
immediately before the first graph stream. One invocation-scoped, concurrency-safe dispatch
ledger is shared by lead and delegated subagents. A new canonical dispatch-ID/intent pair
consumes one physical-start slot; an equal in-flight or completed retry reuses its result without
starting again. Changed intent under that ID conflicts, and a new ID beyond the ceiling is
exhausted. The ledger snapshots only the digest and bounded result, not mutable caller input. Token
budgets remain post-response guards and are not advertised as exact constraints.

Version 1 remains a deliberate compatibility contract with its original request and
projection types, positive-only `max_total_subagents`, and
`required_capabilities: [invocation_constraints.v1]`. A v1 registration cannot satisfy a
v2 requirement and is never represented as full v2 context or obligation support.

## Required MCP call preparation

Trusted operator plugins may register multiple `McpInterceptorDescriptor` values with
`registry.mcp_interceptor(...)`. Each factory returns an async `McpInterceptor` whose
`prepare_call(McpCallProjectionV1)` method receives the same trusted run context and final
contributor-enriched Origin used by authorization, plus the bound thread/run, pinned agent
revision and extension generation, MCP server/tool names, and a canonical arguments digest.
It never receives or owns the network handler.

The strict result union is `PreparedMcpCallV1`, `McpCallRejectedV1`, or
`McpCallIndeterminateV1`. A prepared result may add at most 16 bounded transient headers
and 32 bounded `SafeContextReferenceV1` audit references. Header values are used only for
that underlying MCP call; they are excluded from run rows, checkpoints, lifecycle/rich
events, manifests, diagnostics, and logs. Persistable evidence may identify a stable
secret handle, but must never contain secret material.

Operators require a contribution only from startup-controlled `config.yaml`:

```yaml
required_capabilities:
  - mcp_interceptor:example.credential_broker
```

The host pins the required contribution set and extension generation at tool construction.
It reuses the exact provider/request receipt already allowed by the operation-time tool
authorization check; it neither reconstructs nor repeats that request. Optional legacy
compatibility hooks run inside the host boundary, then the host checks fresh required
health and invokes required interceptors under independent two-second timeouts in
contribution-ID order as the final fence immediately before the handler. A policy deny, missing/mismatched generation,
unhealthy or unavailable contribution, invalid/rejected/indeterminate preparation, header
collision, exception, or timeout calls the MCP handler zero times. Preparation can only
restrict or operationally fail a call; it cannot allow one denied by authorization.

The older `extensions_config.json -> mcpInterceptors` class-path mechanism is an optional,
warning-and-skip compatibility path. It is API-writable, is not an authoritative
Capability Host registration, and cannot satisfy `required_capabilities`. When a required
host is active, compatibility hooks run inside its authorization-to-network boundary,
after the exact authorization receipt is verified and before the final trusted preparation
fence.

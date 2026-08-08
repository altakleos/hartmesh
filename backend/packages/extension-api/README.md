# DeerFlow Extension API

`deerflow-extension-api` is DeerFlow's host-independent plugin contract package. It has
no dependency on `deerflow`, `app`, FastAPI, or the Gateway runtime. Extensions should
depend on this distribution and import contracts from `deerflow_extension_api`.

Version 0.8.0 owns the authorization contracts `Principal`, `AuthzRequest`,
`AuthzDecision`, `AuthzReason`, and `AuthorizationProvider`. Existing host code may keep
using `deerflow.authz.provider`; those names are compatibility re-exports of the same
objects. It also owns the versioned Origin and run-context contributor contracts described
below.

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

Every authoritative factory descriptor also has an optional `health_probe`. Existing
plugins may omit it; a successfully initialized capability without a probe is healthy.
When present, it is an async zero-argument callable returning only
`CapabilityHealthResult(status="healthy"|"unhealthy", diagnostic_code=<bounded code>)`.
The host applies its own timeout/cache/single-flight policy. Probes must not return
credentials, identities, exception text, or request data.

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

The extension snapshot and its generation are immutable after Gateway construction. The
Gateway-owned authorization resolver constructs an extension provider once at startup and
shares that exact object across current route, resource, model, tool, skill-discovery, and
agent-assembly checks. Legacy class-path providers remain supported and may be atomically
replaced when their hot-reloaded configuration changes.

Extensions may also contribute observational middleware. Its existing isolation remains
fail-open; authorization-provider factories are authoritative startup capabilities and do
not use that observational failure policy.

## Invocation contributor contributions

Trusted plugins may register typed `OriginContributorFactory` and
`RunContextContributorFactory` descriptors. The Capability Host stamps package
provenance, initializes each factory once at startup, invokes contributors concurrently,
and composes valid results deterministically by stable `contribution_id`.

Origin contributors receive only source kind, the canonical invocation identity, an
authenticated subject reference,
and host-selected safe source references. Run-context contributors receive an immutable
split principal projection, sealed Origin, bound thread, resolved agent revision reference, and
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
validation. It carries the effective subject/acting service, final `SealedOriginV1`, bound
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

## Invocation constraints contribution

A trusted plugin may register the process's single
`InvocationConstraintsProviderFactory`. Its async provider receives the canonical split
identity and sealed Origin together with the canonical request digest and pinned
agent-revision digest. It returns the strict v1 union:
`ConstraintProjectionV1`, `ConstraintRejected`, or `ConstraintIndeterminate`. The sole v1
control is an optional positive `max_total_subagents`; authorization remains the only
binary permission authority.

The Capability Host calls the provider directly, outside observational fail-open
middleware, under a host-owned two-second timeout and an injected timezone-aware clock.
It rejects digest mismatches, naive/future/expired timestamps, validity beyond 15 minutes,
unknown fields, malformed evidence, and impossible limits. The effective subagent ceiling
is the lower of the provider projection and the static host ceiling. Registration is
singular and loader-attributed. Operators may require it only through startup-controlled
`required_capabilities: [invocation_constraints.v1]`; the API-writable extension config
cannot activate or require trusted constraint code.

The accepted run persists only the normalized projection and safe evidence ID/digest.
Workers validate the accepted binding and freshness before graph construction and again
immediately before the first graph stream. One invocation-scoped, concurrency-safe
reservation counter is shared by lead and delegated subagents and reserves before every
dispatch; retries with the same dispatch ID do not consume the ceiling twice. Token
budgets remain post-response guards and are not advertised as exact constraints.

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

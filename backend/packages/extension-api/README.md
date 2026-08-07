# DeerFlow Extension API

`deerflow-extension-api` is DeerFlow's host-independent plugin contract package. It has
no dependency on `deerflow`, `app`, FastAPI, or the Gateway runtime. Extensions should
depend on this distribution and import contracts from `deerflow_extension_api`.

Version 0.3.0 owns the authorization contracts `Principal`, `AuthzRequest`,
`AuthzDecision`, `AuthzReason`, and `AuthorizationProvider`. Existing host code may keep
using `deerflow.authz.provider`; those names are compatibility re-exports of the same
objects. It also owns the versioned Origin and run-context contributor contracts described
below.

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

Origin contributors receive only source kind, an authenticated internal subject reference,
and host-selected safe source references. Run-context contributors receive an immutable
principal projection, sealed Origin, bound thread, resolved agent revision reference, and
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

Each call has a host-owned two-second timeout. Optional failures are omitted with bounded,
redacted diagnostics. Operators can make a contribution mandatory in startup-only
`config.yaml` with `required_capabilities: [origin_contributor:<id>]` or
`[run_context_contributor:<id>]`; missing or initialization-failed requirements stop
startup, and required invocation-time failure closes that invocation as indeterminate.
This setting is intentionally absent from API-writable `extensions_config.json`.

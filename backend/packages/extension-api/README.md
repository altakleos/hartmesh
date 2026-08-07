# DeerFlow Extension API

`deerflow-extension-api` is DeerFlow's host-independent plugin contract package. It has
no dependency on `deerflow`, `app`, FastAPI, or the Gateway runtime. Extensions should
depend on this distribution and import contracts from `deerflow_extension_api`.

Version 0.2.0 owns the authorization contracts `Principal`, `AuthzRequest`,
`AuthzDecision`, `AuthzReason`, and `AuthorizationProvider`. Existing host code may keep
using `deerflow.authz.provider`; those names are compatibility re-exports of the same
objects.

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

"""Install functions used by the extension loader tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Never

from deerflow_extension_api import (
    AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    INVOCATION_CONSTRAINTS_KIND,
    MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
    MCP_INTERCEPTOR_KIND,
    AuthorizationProviderFactory,
    AuthzDecision,
    AuthzRequest,
    ConstraintProjectionRequestV2,
    ExtensionRegistry,
    InvocationConstraintsProviderFactory,
    McpCallProjectionV1,
    McpInterceptorDescriptor,
    PreparedMcpCallV1,
    Principal,
    extension,
)

INSTALLED: list[str] = []
PROVIDER_INSTANCES: list[object] = []


class _Contributor:
    def __init__(self, tag: str) -> None:
        self.tag = tag


def install_ok(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    INSTALLED.append("ok")
    registry.middlewares(_Contributor("ok"))


@extension(api="0.13", name="stamped")
def install_stamped(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    INSTALLED.append("stamped")
    registry.task_lifecycle(_Contributor("stamped"))


@extension(api="99.0", name="future")
def install_future_api(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    INSTALLED.append("future")
    registry.middlewares(_Contributor("future"))


@extension(api="0.14", name="newer-minor")
def install_newer_minor_api(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Written against a newer 0.x minor than the host provides: before 1.0,
    minors carry no compatibility promise in either direction."""
    INSTALLED.append("newer-minor")
    registry.middlewares(_Contributor("newer-minor"))


def install_partial_then_raise(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Register every contribution kind, then fail to exercise five-bucket rollback."""
    partial = _Contributor("partial")
    registry.middlewares(partial)
    registry.task_lifecycle(partial)
    registry.system_model_observer(partial)
    registry.service(partial)
    registry.routers((partial,))
    raise ValueError("boom")


def install_reads_config(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    INSTALLED.append(f"config:{config.get('mode')}")


def install_disabled(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Registers nothing when disabled — the zero-cost path."""
    if not config.get("enabled", False):
        return
    registry.middlewares(_Contributor("enabled"))


def install_shared_use(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Registers a middleware contributor; raises afterward if configured to.

    Two ExtensionSpecs may legitimately share the same `use` with different
    config (e.g. the same extension mounted twice with different settings).
    This exists to exercise that rollback must be positional, not keyed by
    `use` — a failing instance must not erase an earlier, successful
    instance's registrations just because they share a source string.
    """
    registry.middlewares(_Contributor(f"shared:{config.get('label', '')}"))
    if config.get("fail"):
        raise ValueError("boom-shared")


class CountingAuthorizationProvider:
    name = "fixture-counting"

    def __init__(self, *, label: str = "default") -> None:
        self.label = label
        PROVIDER_INSTANCES.append(self)

    def authorize(self, request: AuthzRequest) -> AuthzDecision:
        return AuthzDecision(allow=True)

    async def aauthorize(self, request: AuthzRequest) -> AuthzDecision:
        return self.authorize(request)

    def filter_resources(
        self,
        principal: Principal,
        resource_type: str,
        candidates: list[str],
    ) -> list[str]:
        return list(candidates)


def _provider_factory() -> CountingAuthorizationProvider:
    return CountingAuthorizationProvider(label="plugin")


def install_authorization_provider(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    registry.authorization_provider(
        AuthorizationProviderFactory(
            contribution_id="fixture.authorization",
            capability_api_version=AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
            factory=_provider_factory,
            kind="authorization_provider",
        )
    )


def install_authorization_then_raise(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    install_authorization_provider(registry, config)
    registry.middlewares(_Contributor("partial-authz"))
    raise ValueError("boom-authz")


class _PreparedMcpInterceptor:
    async def prepare_call(
        self,
        request: McpCallProjectionV1,
    ) -> PreparedMcpCallV1:
        del request
        return PreparedMcpCallV1()


def install_mcp_interceptor(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    del config
    registry.mcp_interceptor(
        McpInterceptorDescriptor(
            contribution_id="fixture.mcp",
            capability_api_version=MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
            factory=_PreparedMcpInterceptor,
            kind=MCP_INTERCEPTOR_KIND,
        )
    )


def install_mcp_and_authorization(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    install_mcp_interceptor(registry, config)
    install_authorization_provider(registry, config)


def install_mcp_then_raise(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    install_mcp_interceptor(registry, config)
    raise ValueError("boom-mcp")


class _ConstraintsProviderV2:
    async def project(self, request: ConstraintProjectionRequestV2) -> Never:
        raise AssertionError("the loader fixture must not project constraints")


def install_constraints_v2(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    del config
    registry.invocation_constraints(
        InvocationConstraintsProviderFactory(
            contribution_id="fixture.constraints-v2",
            capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
            factory=_ConstraintsProviderV2,
            kind=INVOCATION_CONSTRAINTS_KIND,
        )
    )


NOT_CALLABLE = "i am not a function"

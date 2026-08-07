"""Install functions used by the extension loader tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow_extension_api import (
    AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
    AuthorizationProviderFactory,
    AuthzDecision,
    ExtensionRegistry,
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


@extension(api="0.4", name="stamped")
def install_stamped(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    INSTALLED.append("stamped")
    registry.middlewares(_Contributor("stamped"))


@extension(api="99.0", name="future")
def install_future_api(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    INSTALLED.append("future")
    registry.middlewares(_Contributor("future"))


@extension(api="0.5", name="newer-minor")
def install_newer_minor_api(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Written against a newer 0.x minor than the host provides: before 1.0,
    minors carry no compatibility promise in either direction."""
    INSTALLED.append("newer-minor")
    registry.middlewares(_Contributor("newer-minor"))


def install_partial_then_raise(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Registers two contributors, then fails — exercises rollback."""
    registry.middlewares(_Contributor("partial-a"))
    registry.middlewares(_Contributor("partial-b"))
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

    def authorize(self, request):
        return AuthzDecision(allow=True)

    async def aauthorize(self, request):
        return self.authorize(request)

    def filter_resources(self, principal, resource_type, candidates):
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


NOT_CALLABLE = "i am not a function"

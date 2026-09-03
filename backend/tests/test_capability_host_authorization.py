"""Capability Host and coherent authorization-provider regression boundary."""

from __future__ import annotations

import dataclasses
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_extension_api_0131_is_exactly_pinned_by_host_packages():
    backend_root = Path(__file__).parents[1]
    extension_project = tomllib.loads((backend_root / "packages/extension-api/pyproject.toml").read_text())
    harness_project = tomllib.loads((backend_root / "packages/harness/pyproject.toml").read_text())
    application_project = tomllib.loads((backend_root / "pyproject.toml").read_text())

    assert extension_project["project"]["version"] == "0.13.1"
    assert "deerflow-extension-api==0.13.1" in harness_project["project"]["dependencies"]
    assert "deerflow-extension-api==0.13.1" in application_project["project"]["dependencies"]


def test_extension_authorization_contracts_are_legacy_import_identities():
    from deerflow_extension_api import (
        AuthorizationProvider,
        AuthzDecision,
        AuthzRequest,
        Principal,
    )

    from deerflow.authz.provider import (
        AuthorizationProvider as LegacyAuthorizationProvider,
    )
    from deerflow.authz.provider import (
        AuthzDecision as LegacyAuthzDecision,
    )
    from deerflow.authz.provider import (
        AuthzRequest as LegacyAuthzRequest,
    )
    from deerflow.authz.provider import (
        Principal as LegacyPrincipal,
    )

    assert LegacyPrincipal is Principal
    assert LegacyAuthzRequest is AuthzRequest
    assert LegacyAuthzDecision is AuthzDecision
    assert LegacyAuthorizationProvider is AuthorizationProvider


def test_public_factory_descriptor_is_typed_versioned_and_frozen():
    from deerflow_extension_api import (
        AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
        AuthorizationProviderFactory,
        AuthzDecision,
    )

    class _Provider:
        name = "test"

        def authorize(self, request):
            return AuthzDecision(allow=True)

        async def aauthorize(self, request):
            return self.authorize(request)

        def filter_resources(self, principal, resource_type, candidates):
            return list(candidates)

    descriptor = AuthorizationProviderFactory(
        contribution_id="example.authorization",
        capability_api_version=AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
        factory=_Provider,
        kind="authorization_provider",
    )

    assert dataclasses.is_dataclass(descriptor)
    assert descriptor.__dataclass_params__.frozen
    assert not hasattr(descriptor, "package_name")
    assert not hasattr(descriptor, "package_version")
    assert descriptor.factory().name == "test"


def test_loader_owns_authorization_factory_package_attribution(monkeypatch):
    from deerflow.extensions.loader import ExtensionSpec, load_extensions

    monkeypatch.setattr(
        "deerflow.extensions.loader._distribution_provenance",
        lambda install: ("example-policy", "4.2.1"),
    )
    loaded, diagnostics = load_extensions([ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_authorization_provider")])

    assert diagnostics == []
    assert loaded.generation > 0
    registration = loaded.authorization_provider_factory
    assert registration is not None
    assert registration.contribution_id == "fixture.authorization"
    assert registration.source == "extension_test_fixtures.demo_extensions:install_authorization_provider"
    assert registration.package_name == "example-policy"
    assert registration.package_version == "4.2.1"


def test_authorization_factory_registration_is_unique_and_positional_rollback_is_complete():
    from deerflow.extensions.loader import ExtensionSpec, load_extensions

    loaded, diagnostics = load_extensions(
        [
            ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_authorization_then_raise"),
            ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_authorization_provider"),
            ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_authorization_provider"),
        ]
    )

    assert [diagnostic.level for diagnostic in diagnostics] == ["error", "error"]
    assert loaded.authorization_provider_factory is not None
    assert loaded.middleware_contributors == ()


def _legacy_config(*, label: str):
    from deerflow.config.authorization_config import (
        AuthorizationConfig,
        AuthorizationProviderConfig,
    )

    return AuthorizationConfig(
        enabled=True,
        provider=AuthorizationProviderConfig(
            use="extension_test_fixtures.demo_extensions:CountingAuthorizationProvider",
            config={"label": label},
        ),
    )


def _plugin_extensions():
    from deerflow.extensions.loader import ExtensionSpec, load_extensions

    loaded, diagnostics = load_extensions([ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_authorization_provider")])
    assert diagnostics == []
    return loaded


def test_plugin_and_legacy_provider_configuration_are_mutually_exclusive():
    from app.gateway.authorization import AuthorizationProviderResolver

    with pytest.raises(ValueError, match="mutually exclusive"):
        AuthorizationProviderResolver(_plugin_extensions(), _legacy_config(label="legacy"))


def test_legacy_config_replacement_is_generation_atomic_and_reuses_equal_signatures():
    from app.gateway.authorization import AuthorizationProviderResolver
    from deerflow.config.authorization_config import AuthorizationConfig
    from deerflow.extensions.registry import ExtensionRegistry

    resolver = AuthorizationProviderResolver(
        ExtensionRegistry().build(generation=7),
        AuthorizationConfig(),
    )

    first = resolver.resolve(_legacy_config(label="first"))
    same = resolver.resolve(_legacy_config(label="first"))
    second = resolver.resolve(_legacy_config(label="second"))

    assert same is first
    assert second.generation == first.generation + 1
    assert first.provider is not second.provider
    assert first.provider.label == "first"
    assert second.provider.label == "second"
    assert resolver.snapshot() is second


def test_plugin_factory_provider_is_constructed_once_at_startup():
    from app.gateway.authorization import AuthorizationProviderResolver
    from deerflow.config.authorization_config import AuthorizationConfig
    from extension_test_fixtures import demo_extensions

    demo_extensions.PROVIDER_INSTANCES.clear()
    resolver = AuthorizationProviderResolver(
        _plugin_extensions(),
        AuthorizationConfig(enabled=True),
    )
    first = resolver.snapshot()
    disabled = resolver.resolve(AuthorizationConfig(enabled=False))
    second = resolver.resolve(AuthorizationConfig(enabled=True))

    assert len(demo_extensions.PROVIDER_INSTANCES) == 1
    assert first.provider is second.provider
    assert disabled.provider is None


def test_service_visibility_grant_reload_does_not_replace_authorization_generation():
    from app.gateway.authorization import AuthorizationProviderResolver
    from deerflow.config.authorization_config import AuthorizationConfig

    def config(run_id: str) -> AuthorizationConfig:
        return AuthorizationConfig(
            enabled=True,
            invocation_operations={"observe_enabled": True},
            service_observation_grants=[
                {"service_id": "service-1", "run_ids": [run_id]},
            ],
        )

    resolver = AuthorizationProviderResolver(_plugin_extensions(), config("run-a"))
    first = resolver.snapshot()
    after_grant_reload = resolver.resolve(config("run-b"))

    assert after_grant_reload is first


@pytest.mark.asyncio
async def test_gateway_authorization_paths_share_one_provider_at_a_generation(
    monkeypatch,
):
    from app.gateway.authorization import AuthorizationProviderResolver
    from app.gateway.authz import resolve_model_authorization, resolve_route_permissions
    from app.gateway.deps import get_run_context
    from deerflow.agents.lead_agent.agent import _authorize_model_name
    from deerflow.authz.runtime import authorization_provider_from_context
    from deerflow.authz.tool_filter import apply_tool_authorization
    from deerflow.config.app_config import AppConfig
    from deerflow.config.model_config import ModelConfig
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.extensions.registry import ExtensionRegistry
    from deerflow.runtime.runs.worker import _build_runtime_context
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    authorization = _legacy_config(label="coherent")
    app_config = AppConfig(
        models=[
            ModelConfig(
                name="default",
                model="default",
                use="langchain_openai:ChatOpenAI",
            )
        ],
        sandbox=SandboxConfig(use="test"),
        authorization=authorization,
    )
    resolver = AuthorizationProviderResolver(
        ExtensionRegistry().build(generation=11),
        authorization,
    )
    snapshot = resolver.snapshot()
    user = SimpleNamespace(
        id="user-1",
        system_role="member",
        oauth_provider=None,
        oauth_id=None,
    )
    monkeypatch.setattr(
        "app.gateway.authz._get_route_authorization_config",
        lambda: authorization,
    )

    await resolve_route_permissions(user, is_internal=False, resolver=resolver)
    model_provider, _principal = resolve_model_authorization(
        user,
        is_internal=False,
        resolver=resolver,
        config=authorization,
    )
    for dependency_name in (
        "get_checkpointer",
        "get_store",
        "get_run_event_store",
        "get_thread_store",
    ):
        monkeypatch.setattr(f"app.gateway.deps.{dependency_name}", lambda request: None)
    monkeypatch.setattr("app.gateway.deps.get_config", lambda: app_config)
    gateway_request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                authorization_provider_resolver=resolver,
                extensions=ExtensionRegistry().build(generation=11),
                tenant_identity=TenantIdentityV1.from_canonical_id("local"),
            )
        )
    )
    run_context = get_run_context(gateway_request)
    runtime_context = _build_runtime_context(
        "thread-1",
        "run-1",
        {},
        app_config,
        authorization_provider=run_context.authorization_provider,
    )
    runtime_provider = authorization_provider_from_context(runtime_context)
    _tools, tool_provider = apply_tool_authorization(
        [],
        context=runtime_context,
        app_config=app_config,
        authorization_provider=runtime_provider,
    )
    selected_model = _authorize_model_name(
        "default",
        context=runtime_context,
        app_config=app_config,
        authorization_provider=runtime_provider,
    )

    assert model_provider is snapshot.provider
    assert runtime_provider is snapshot.provider
    assert tool_provider is snapshot.provider
    assert selected_model == "default"

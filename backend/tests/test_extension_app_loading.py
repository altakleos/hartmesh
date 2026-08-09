"""Gateway app-construction wiring for configured Python extensions."""

from __future__ import annotations

import pytest

from deerflow.extensions import reset_loaded_extensions, reset_runtime_diagnostics
from deerflow.extensions.loader import ExtensionLoadError, ExtensionSpec
from deerflow.extensions.registry import ExtensionRegistry


@pytest.fixture(autouse=True)
def _reset_extension_process_state():
    reset_loaded_extensions()
    reset_runtime_diagnostics()
    yield
    reset_runtime_diagnostics()
    reset_loaded_extensions()


@pytest.fixture(autouse=True)
def stub_app_config(monkeypatch):
    """Keep ``create_app()`` independent of a real ``config.yaml``.

    The repo-root ``config.yaml`` is gitignored and absent on CI runners, so
    reading it here would make these tests pass locally and fail on every run
    in CI. Tests that need a specific plugin list copy this config instead of
    loading one from disk.
    """
    import app.gateway.app as app_module
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    config = AppConfig(sandbox=SandboxConfig(use="test"))
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    return config


def test_create_app_exposes_loaded_extensions_on_app_state_and_process_singleton(monkeypatch):
    import deerflow.extensions as extensions_module

    loaded = ExtensionRegistry().build()
    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda plugins: (loaded, []),
    )

    from app.gateway.app import create_app

    app = create_app()

    assert app.state.extensions is loaded
    assert extensions_module.get_loaded_extensions() is loaded


def test_create_app_exposes_one_canonical_live_diagnostics_list(monkeypatch):
    import deerflow.extensions as extensions_module

    loaded = ExtensionRegistry().build()
    load_diagnostic = extensions_module.Diagnostic.warning(
        "demo:install",
        "optional extension was skipped",
    )
    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda plugins: (loaded, [load_diagnostic]),
    )

    from app.gateway.app import create_app

    first_app = create_app()
    second_app = create_app()
    runtime_diagnostic = extensions_module.Diagnostic.error(
        "demo:install",
        "middleware observation failed",
    )
    extensions_module.record_runtime_diagnostic(runtime_diagnostic)

    assert first_app.state.extension_diagnostics is second_app.state.extension_diagnostics
    assert first_app.state.extension_diagnostics == [
        load_diagnostic,
        runtime_diagnostic,
    ]


def test_create_app_retains_structured_redacted_contributor_startup_diagnostic(monkeypatch):
    from deerflow_extension_api import (
        ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
        ORIGIN_CONTRIBUTOR_KIND,
        OriginContributorFactory,
    )

    import deerflow.extensions as extensions_module

    def broken_factory():
        raise RuntimeError("credential=resolved-secret-value")

    registry = ExtensionRegistry()
    with registry.attributed_to("demo:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="demo-origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=broken_factory,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )
    loaded = registry.build()
    monkeypatch.setattr(extensions_module, "load_extensions", lambda plugins: (loaded, []))

    from app.gateway.app import create_app

    app = create_app()
    diagnostic = next(item for item in app.state.extension_diagnostics if item.source == "origin_contributor:demo-origin")
    assert diagnostic.contribution_id == "demo-origin"
    assert diagnostic.code == "initialization_failed"
    assert diagnostic.error_class == "RuntimeError"
    assert len(diagnostic.correlation_id or "") == 32
    assert "resolved-secret-value" not in repr(diagnostic)


def test_create_app_fails_open_when_extension_loading_raises_unexpectedly(monkeypatch):
    import deerflow.extensions as extensions_module

    def _raise_unexpectedly(plugins):
        raise RuntimeError("malformed plugins configuration")

    monkeypatch.setattr(extensions_module, "load_extensions", _raise_unexpectedly)

    from app.gateway.app import create_app

    app = create_app()

    assert app.state.extensions is extensions_module.EMPTY_EXTENSIONS
    assert extensions_module.get_loaded_extensions() is extensions_module.EMPTY_EXTENSIONS
    assert app.state.extension_diagnostics == []


def test_create_app_fails_closed_when_a_required_extension_cannot_load(monkeypatch):
    import deerflow.extensions as extensions_module
    from app.gateway.app import create_app

    def _raise_required(plugins):
        raise extensions_module.ExtensionLoadError("required extension example_policy:install failed to install")

    monkeypatch.setattr(extensions_module, "load_extensions", _raise_required)

    with pytest.raises(extensions_module.ExtensionLoadError):
        create_app()


def test_create_app_tolerates_a_missing_config_file_and_loads_no_extensions(monkeypatch):
    """``create_app()`` runs at import time, so an absent config.yaml must not break it.

    Mirrors ``_resolve_trace_enabled_for_app_construction()``: lifespan still
    performs strict config loading before the Gateway serves traffic.
    """
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module

    def _missing_config():
        raise FileNotFoundError("`config.yaml` file not found in the project root or legacy backend/repository root locations")

    monkeypatch.setattr(app_module, "get_app_config", _missing_config)

    observed_plugins = []
    loaded = ExtensionRegistry().build()

    def _record(plugins):
        observed_plugins.append(plugins)
        return loaded, []

    monkeypatch.setattr(extensions_module, "load_extensions", _record)

    app = app_module.create_app()

    assert observed_plugins == [[]]
    assert app.state.extensions is loaded


def test_create_app_propagates_config_failures_instead_of_blaming_extension_loading(monkeypatch):
    """A parseable-but-broken config.yaml must not be swallowed by the fail-open guard.

    Extension loading is fail-open for unexpected errors, but resolving the
    plugin list is not part of it: degrading to zero extensions there would
    drop a ``required: true`` extension without failing the boot.
    """
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module

    def _broken_config():
        raise ValueError("config.yaml failed validation")

    monkeypatch.setattr(app_module, "get_app_config", _broken_config)

    def _must_not_run(plugins):
        raise AssertionError("load_extensions must not run when the plugin list cannot be resolved")

    monkeypatch.setattr(extensions_module, "load_extensions", _must_not_run)

    with pytest.raises(ValueError, match="config.yaml failed validation"):
        app_module.create_app()


def test_create_app_fails_closed_for_required_extension_with_malformed_api_marker(monkeypatch, stub_app_config):
    import app.gateway.app as app_module
    from extension_test_fixtures import demo_extensions

    class _ExplodingAPIMarker:
        def split(self, separator: str) -> list[str]:
            raise RuntimeError("API marker split exploded")

        def __str__(self) -> str:
            return "exploding non-string marker"

    monkeypatch.setattr(
        demo_extensions.install_ok,
        "__deerflow_api__",
        _ExplodingAPIMarker(),
        raising=False,
    )

    config = stub_app_config.model_copy(
        update={
            "plugins": [
                ExtensionSpec(
                    use="extension_test_fixtures.demo_extensions:install_ok",
                    required=True,
                )
            ]
        }
    )
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)

    with pytest.raises(ExtensionLoadError, match="declares invalid api marker"):
        app_module.create_app()


@pytest.mark.asyncio
async def test_full_gateway_reports_only_hmac_authenticated_postgres_ingress_as_durable(
    monkeypatch,
    stub_app_config,
):
    import app.gateway.app as app_module
    from deerflow.config.app_config import AppConfig

    raw = stub_app_config.model_dump(mode="python")
    raw["database"]["backend"] = "postgres"
    raw["deployment"]["profile"] = "durable_production"
    raw["channels"] = {"github": {"enabled": True}}
    config = AppConfig.model_validate(raw)
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "gateway-construction-secret")
    monkeypatch.setenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", "1")

    app = app_module.create_app()
    report = await app.state.deployment_reporter.deployment_report()
    ready_reporter = app.state.deployment_reporter.with_runtime_store(
        profile="durable_production",
        database_backend="postgres",
        atomic_lifecycle=True,
    )

    assert report.native_ingress.to_dict()["sources"] == {"github": "durable"}
    assert ready_reporter.admission_profile_ready is True

    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "   ")
    degraded = await ready_reporter.deployment_report()

    assert degraded.native_ingress.to_dict()["sources"] == {"github": "best_effort"}
    assert ready_reporter.admission_profile_ready is False


@pytest.mark.asyncio
async def test_full_gateway_downgrades_unverified_postgres_ingress_and_refuses_route(
    monkeypatch,
    stub_app_config,
):
    import app.gateway.app as app_module
    from deerflow.config.app_config import AppConfig

    raw = stub_app_config.model_dump(mode="python")
    raw["database"]["backend"] = "postgres"
    raw["deployment"]["profile"] = "durable_production"
    raw["channels"] = {"github": {"enabled": True}}
    config = AppConfig.model_validate(raw)
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", "1")

    app = app_module.create_app()
    report = await app.state.deployment_reporter.deployment_report()
    ready_reporter = app.state.deployment_reporter.with_runtime_store(
        profile="durable_production",
        database_backend="postgres",
        atomic_lifecycle=True,
    )

    assert report.native_ingress.to_dict()["sources"] == {"github": "best_effort"}
    assert ready_reporter.admission_profile_ready is False
    assert not any(getattr(route, "path", None) == "/api/webhooks/github" for route in app.routes)

    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "added-after-composition")
    unchanged = await ready_reporter.deployment_report()

    assert unchanged.native_ingress.to_dict()["sources"] == {"github": "best_effort"}
    assert ready_reporter.admission_profile_ready is False
    assert not any(getattr(route, "path", None) == "/api/webhooks/github" for route in app.routes)


def test_create_app_wires_required_mcp_host_health_and_shared_authorization(
    monkeypatch,
    stub_app_config,
):
    import app.gateway.app as app_module
    from deerflow.config.authorization_config import AuthorizationConfig

    config = stub_app_config.model_copy(
        update={
            "plugins": [
                ExtensionSpec(
                    use=("extension_test_fixtures.demo_extensions:install_mcp_and_authorization"),
                    required=True,
                )
            ],
            "required_capabilities": ["mcp_interceptor:fixture.mcp"],
            "authorization": AuthorizationConfig(enabled=True),
        }
    )
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)

    app = app_module.create_app()

    assert app.state.mcp_interceptor_host.required_capability_ids == frozenset({"mcp_interceptor:fixture.mcp"})
    assert "mcp_interceptor:fixture.mcp" in {item.capability_id for item in app.state.capability_manifest.capabilities}
    assert app.state.authorization_provider_resolver.snapshot().provider is not None


@pytest.mark.anyio
async def test_create_app_wires_operator_service_observation_grants(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import EffectiveSubjectV1, InvocationIdentityV1

    import app.gateway.app as app_module
    from app.runtime.invocation import InvocationPrincipal
    from deerflow.config.authorization_config import AuthorizationConfig

    config = stub_app_config.model_copy(
        update={
            "plugins": [
                ExtensionSpec(
                    use=("extension_test_fixtures.demo_extensions:install_mcp_and_authorization"),
                    required=True,
                )
            ],
            "authorization": AuthorizationConfig(
                enabled=True,
                invocation_operations={"observe_enabled": True},
                service_observation_grants=[
                    {"service_id": "service-1", "thread_ids": ["thread-a"]},
                ],
            ),
        }
    )
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)

    app = app_module.create_app()
    grant = await app.state.service_observation_visibility_resolver.resolve(
        InvocationPrincipal(
            identity=InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="service",
                    subject_id="service-1",
                    role="service",
                )
            )
        )
    )

    assert grant.service_id == "service-1"
    assert grant.thread_ids == ("thread-a",)
    assert app.state.invocation_authorization_config.observe_enabled is True


class _NoopContributor:
    async def contribute(self, request):
        return None


class _ConstraintsProvider:
    async def project(self, request):
        raise AssertionError("startup must not project invocation constraints")


async def _healthy_capability():
    from deerflow_extension_api import CapabilityHealthResult

    return CapabilityHealthResult(status="healthy")


async def _unhealthy_capability():
    from deerflow_extension_api import CapabilityHealthResult

    return CapabilityHealthResult(
        status="unhealthy",
        diagnostic_code="dependency_unavailable",
    )


def _constraints_extensions(
    *,
    api_version="2.0",
    factory=_ConstraintsProvider,
    health_probe=None,
    duplicate=False,
):
    from deerflow_extension_api import (
        INVOCATION_CONSTRAINTS_KIND,
        InvocationConstraintsProviderFactory,
    )

    from deerflow.extensions.registry import (
        DuplicateInvocationConstraintsProviderFactoryError,
    )

    registry = ExtensionRegistry()
    descriptor = InvocationConstraintsProviderFactory(
        contribution_id="fixture.constraints",
        capability_api_version=api_version,
        factory=factory,
        kind=INVOCATION_CONSTRAINTS_KIND,
        health_probe=health_probe,
    )
    with registry.attributed_to("fixture:constraints"):
        registry.invocation_constraints(descriptor)
    if duplicate:
        with registry.attributed_to("fixture:constraints-duplicate"):
            with pytest.raises(DuplicateInvocationConstraintsProviderFactoryError):
                registry.invocation_constraints(descriptor)
    return registry.build(generation=7)


def _create_gateway(monkeypatch, stub_app_config, loaded, required_capabilities):
    import app.gateway.app as app_module
    import deerflow.extensions as extensions_module

    monkeypatch.setattr(
        extensions_module,
        "load_extensions",
        lambda plugins: (loaded, []),
    )
    config = stub_app_config.model_copy(update={"required_capabilities": list(required_capabilities)})
    monkeypatch.setattr(app_module, "get_app_config", lambda: config)
    return app_module.create_app()


@pytest.mark.asyncio
async def test_create_app_routes_required_invocation_contributors_to_their_owner(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import (
        ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
        ORIGIN_CONTRIBUTOR_KIND,
        RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION,
        RUN_CONTEXT_CONTRIBUTOR_KIND,
        OriginContributorFactory,
        RunContextContributorFactory,
    )

    registry = ExtensionRegistry()
    with registry.attributed_to("fixture:contributors"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="fixture.origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_NoopContributor,
                kind=ORIGIN_CONTRIBUTOR_KIND,
                health_probe=_healthy_capability,
            )
        )
        registry.run_context_contributor(
            RunContextContributorFactory(
                contribution_id="fixture.context",
                capability_api_version=RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_NoopContributor,
                kind=RUN_CONTEXT_CONTRIBUTOR_KIND,
                health_probe=_healthy_capability,
            )
        )
    required = (
        "origin_contributor:fixture.origin",
        "run_context_contributor:fixture.context",
    )

    app = _create_gateway(
        monkeypatch,
        stub_app_config,
        registry.build(generation=7),
        required,
    )
    readiness = await app.state.capability_health_monitor.readiness()

    assert app.state.contributor_host.initialized_capability_ids == frozenset(required)
    assert readiness.status == "ready"


@pytest.mark.asyncio
async def test_create_app_routes_required_v2_constraints_to_its_owning_host(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2

    app = _create_gateway(
        monkeypatch,
        stub_app_config,
        _constraints_extensions(health_probe=_healthy_capability),
        [INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2],
    )
    readiness = await app.state.capability_health_monitor.readiness()

    host = app.state.invocation_constraints_host
    assert host.required_capability_id == INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2
    assert host.initialized_capability_ids == frozenset({INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2})
    assert readiness.status == "ready"
    assert [(item.capability_id, item.status) for item in readiness.health] == [
        (INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2, "healthy"),
    ]


def test_create_app_fails_when_required_v2_constraints_provider_is_missing(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2

    from deerflow.extensions.constraints import ConstraintStartupError

    with pytest.raises(
        ConstraintStartupError,
        match="required capability invocation_constraints.v2 is not registered",
    ):
        _create_gateway(
            monkeypatch,
            stub_app_config,
            ExtensionRegistry().build(generation=7),
            [INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2],
        )


def test_create_app_fails_closed_for_malformed_required_v2_constraints_provider(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2

    from deerflow.extensions.constraints import ConstraintStartupError

    with pytest.raises(ConstraintStartupError) as captured:
        _create_gateway(
            monkeypatch,
            stub_app_config,
            _constraints_extensions(factory=object),
            [INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2],
        )

    assert str(captured.value).endswith("failed to initialize: TypeError")


@pytest.mark.asyncio
async def test_create_app_reports_unhealthy_required_v2_constraints_fail_closed(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2

    app = _create_gateway(
        monkeypatch,
        stub_app_config,
        _constraints_extensions(health_probe=_unhealthy_capability),
        [INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2],
    )
    readiness = await app.state.capability_health_monitor.readiness()

    assert readiness.status == "not_ready"
    assert [(item.capability_id, item.status, item.diagnostic_code) for item in readiness.health] == [
        (
            INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
            "unhealthy",
            "dependency_unavailable",
        )
    ]


def test_create_app_fails_when_required_constraints_provider_is_ambiguous(
    monkeypatch,
    stub_app_config,
):
    from deerflow_extension_api import INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2

    from deerflow.extensions.constraints import ConstraintStartupError

    with pytest.raises(
        ConstraintStartupError,
        match=("required capability invocation_constraints.v2 has duplicate provider registrations"),
    ):
        _create_gateway(
            monkeypatch,
            stub_app_config,
            _constraints_extensions(duplicate=True),
            [INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2],
        )


@pytest.mark.parametrize(
    ("required_capability", "expected_error"),
    [
        ("invocation_constraints.v1", None),
        (
            "invocation_constraints.v2",
            "required capability invocation_constraints.v2 is not registered; found invocation_constraints.v1",
        ),
    ],
    ids=("v1-compatible", "v1-cannot-satisfy-v2"),
)
def test_create_app_routes_each_required_constraints_version_without_cross_host_rejection(
    monkeypatch,
    stub_app_config,
    required_capability,
    expected_error,
):
    from deerflow.extensions.constraints import ConstraintStartupError

    if expected_error:
        with pytest.raises(ConstraintStartupError, match=expected_error):
            _create_gateway(
                monkeypatch,
                stub_app_config,
                _constraints_extensions(api_version="1.0"),
                [required_capability],
            )
    else:
        app = _create_gateway(
            monkeypatch,
            stub_app_config,
            _constraints_extensions(api_version="1.0"),
            [required_capability],
        )
        assert app.state.invocation_constraints_host.required_capability_id == (required_capability)


def test_required_capability_ownership_classification_covers_public_contracts() -> None:
    import deerflow_extension_api.constraints as constraints_contract
    from deerflow_extension_api import (
        MCP_INTERCEPTOR_KIND,
        ORIGIN_CONTRIBUTOR_KIND,
        RUN_CONTEXT_CONTRIBUTOR_KIND,
    )

    from deerflow.extensions.capabilities import (
        REQUIRED_CAPABILITY_ID_OWNERS,
        REQUIRED_CAPABILITY_KIND_OWNERS,
        route_required_capabilities,
    )

    constraint_ids = {getattr(constraints_contract, name) for name in constraints_contract.__all__ if name.startswith("INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY") and isinstance(getattr(constraints_contract, name), str)}
    assert set(REQUIRED_CAPABILITY_ID_OWNERS) == constraint_ids
    assert dict(REQUIRED_CAPABILITY_KIND_OWNERS) == {
        ORIGIN_CONTRIBUTOR_KIND: "contributors",
        RUN_CONTEXT_CONTRIBUTOR_KIND: "contributors",
        MCP_INTERCEPTOR_KIND: "mcp",
    }

    routes = route_required_capabilities(
        [
            "origin_contributor:fixture.origin",
            "run_context_contributor:fixture.context",
            *sorted(constraint_ids),
            "mcp_interceptor:fixture.mcp",
        ]
    )
    assert routes.contributors == (
        "origin_contributor:fixture.origin",
        "run_context_contributor:fixture.context",
    )
    assert routes.constraints == tuple(sorted(constraint_ids))
    assert routes.mcp == ("mcp_interceptor:fixture.mcp",)


def test_create_app_rejects_unknown_required_capability_with_bounded_diagnostic(
    monkeypatch,
    stub_app_config,
):
    from deerflow.extensions.capabilities import RequiredCapabilityRoutingError

    unknown = "future_authority:" + ("x" * 500)
    with pytest.raises(RequiredCapabilityRoutingError) as captured:
        _create_gateway(
            monkeypatch,
            stub_app_config,
            ExtensionRegistry().build(generation=7),
            [unknown],
        )

    assert str(captured.value) == "unsupported required capability <invalid>"
    assert unknown not in str(captured.value)

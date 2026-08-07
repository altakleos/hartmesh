"""Required, operator-installed MCP call preparation behavior."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def test_public_mcp_preparation_contract_is_exposed() -> None:
    from deerflow_extension_api import (
        MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
        MCP_INTERCEPTOR_KIND,
        McpCallIndeterminateV1,
        McpCallProjectionV1,
        McpCallRejectedV1,
        McpInterceptorDescriptor,
        PreparedMcpCallV1,
    )

    assert MCP_INTERCEPTOR_CAPABILITY_API_VERSION == "1.0"
    assert MCP_INTERCEPTOR_KIND == "mcp_interceptor"
    assert McpCallProjectionV1
    assert PreparedMcpCallV1
    assert McpCallRejectedV1
    assert McpCallIndeterminateV1
    assert McpInterceptorDescriptor


def test_mcp_contract_imports_without_host_packages() -> None:
    package_root = Path(__file__).parents[1] / "packages" / "extension-api"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (f"import sys; sys.path.insert(0, {str(package_root)!r}); from deerflow_extension_api import McpCallIndeterminateV1; assert McpCallIndeterminateV1(); assert 'deerflow' not in sys.modules; assert 'app' not in sys.modules"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_mcp_contract_rejects_unbounded_or_ambiguous_preparation() -> None:
    from deerflow_extension_api import (
        McpHeaderV1,
        PreparedMcpCallV1,
        SafeContextReferenceV1,
    )

    with pytest.raises(ValueError, match="control"):
        McpHeaderV1(name="Authorization", value="Bearer secret\r\nInjected: yes")
    with pytest.raises(ValueError, match="duplicate MCP header"):
        PreparedMcpCallV1(
            headers=(
                McpHeaderV1(name="Authorization", value="a"),
                McpHeaderV1(name="authorization", value="a"),
            )
        )
    with pytest.raises(ValueError, match="at most 16"):
        PreparedMcpCallV1(headers=tuple(McpHeaderV1(name=f"X-Header-{index}", value="v") for index in range(17)))
    with pytest.raises(ValueError, match="at most 32"):
        PreparedMcpCallV1(
            evidence_references=tuple(
                SafeContextReferenceV1(
                    key=f"evidence_{index}",
                    value=index,
                    storage_class="persistable",
                    purpose="correlation",
                )
                for index in range(33)
            )
        )


def test_registry_stamps_mcp_provenance_rejects_duplicates_and_rolls_back() -> None:
    from deerflow_extension_api import (
        MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
        MCP_INTERCEPTOR_KIND,
        McpCallIndeterminateV1,
        McpInterceptorDescriptor,
    )

    from deerflow.extensions.registry import ExtensionRegistry

    class _Interceptor:
        async def prepare_call(self, request):
            del request
            return McpCallIndeterminateV1()

    descriptor = McpInterceptorDescriptor(
        contribution_id="credential_broker",
        capability_api_version=MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
        factory=_Interceptor,
        kind=MCP_INTERCEPTOR_KIND,
    )
    registry = ExtensionRegistry()
    mark = registry.mark()
    with registry.attributed_to(
        "acme.plugin:install",
        package_name="acme-mcp",
        package_version="2.4.1",
    ):
        registry.mcp_interceptor(descriptor)
    registration = registry.build().mcp_interceptor_descriptors[0]
    assert registration.contribution_id == "credential_broker"
    assert registration.package_name == "acme-mcp"
    assert registration.package_version == "2.4.1"

    with registry.attributed_to("duplicate.plugin:install"):
        with pytest.raises(ValueError, match="duplicate"):
            registry.mcp_interceptor(descriptor)

    registry.rollback_to(mark)
    assert registry.build().mcp_interceptor_descriptors == ()


@pytest.mark.asyncio
async def test_required_load_failure_rolls_back_and_fails_readiness() -> None:
    from deerflow.extensions.capabilities import (
        CapabilityHealthMonitor,
        build_capability_manifest,
    )
    from deerflow.extensions.loader import ExtensionSpec, load_extensions
    from deerflow.extensions.mcp import McpInterceptorHost

    loaded, diagnostics = load_extensions([ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_mcp_then_raise")])

    assert [item.level for item in diagnostics] == ["error"]
    assert loaded.mcp_interceptor_descriptors == ()
    required = ("mcp_interceptor:fixture.mcp",)
    host = McpInterceptorHost(loaded, required_capabilities=required)
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=required,
        authorization_required=True,
        legacy_authorization_initialized=True,
        initialized_capability_ids=host.initialized_capability_ids,
    )
    assert (await CapabilityHealthMonitor(manifest, loaded).readiness()).status == ("not_ready")


def test_empty_required_mcp_contribution_id_is_rejected() -> None:
    from deerflow.extensions.contributors import ContributorHost, RequiredCapabilityError

    with pytest.raises(RequiredCapabilityError, match="unsupported required capability"):
        ContributorHost(
            _registered_extensions(),
            required_capabilities=("mcp_interceptor:",),
        )


@pytest.mark.asyncio
async def test_duplicate_required_registration_poison_readiness_after_rollback() -> None:
    from deerflow.extensions.capabilities import (
        CapabilityHealthMonitor,
        build_capability_manifest,
    )
    from deerflow.extensions.loader import ExtensionSpec, load_extensions
    from deerflow.extensions.mcp import McpInterceptorHost

    specs = [
        ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_mcp_interceptor"),
        ExtensionSpec(use="extension_test_fixtures.demo_extensions:install_mcp_interceptor"),
    ]
    loaded, diagnostics = load_extensions(specs)
    required = ("mcp_interceptor:fixture.mcp",)
    host = McpInterceptorHost(loaded, required_capabilities=required)
    manifest = build_capability_manifest(
        loaded,
        required_capabilities=required,
        authorization_required=True,
        legacy_authorization_initialized=True,
        initialized_capability_ids=host.initialized_capability_ids,
    )

    assert [item.level for item in diagnostics] == ["error"]
    assert (await CapabilityHealthMonitor(manifest, loaded).readiness()).status == ("not_ready")
    assert host.initialized_capability_ids == frozenset()
    assert host.startup_diagnostics[0].diagnostic_code == "duplicate_registration"


def _registered_extensions(*descriptors, generation: int = 7):
    from deerflow.extensions.registry import ExtensionRegistry

    registry = ExtensionRegistry()
    for index, descriptor in enumerate(descriptors):
        with registry.attributed_to(
            f"plugin{index}:install",
            package_name=f"acme-plugin-{index}",
            package_version=f"1.{index}.0",
        ):
            registry.mcp_interceptor(descriptor)
    return registry.build(generation=generation)


def _descriptor(contribution_id: str, interceptor, *, health_probe=None):
    from deerflow_extension_api import (
        MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
        MCP_INTERCEPTOR_KIND,
        McpInterceptorDescriptor,
    )

    return McpInterceptorDescriptor(
        contribution_id=contribution_id,
        capability_api_version=MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
        factory=lambda: interceptor,
        kind=MCP_INTERCEPTOR_KIND,
        health_probe=health_probe,
    )


class _AuthorizationProvider:
    name = "test"

    def __init__(self, decision=None, *, error: Exception | None = None) -> None:
        from deerflow_extension_api import AuthzDecision

        self.decision = decision if decision is not None else AuthzDecision(allow=True)
        self.error = error
        self.requests = []

    def authorize(self, request):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.decision

    async def aauthorize(self, request):
        return self.authorize(request)

    def filter_resources(self, principal, resource_type, candidates):
        del principal, resource_type
        return candidates


def _invocation_runtime(provider, *, generation: int = 7, journal=None):
    from deerflow_extension_api import (
        PrincipalProjectionV1,
        ResolvedAgentRevisionReferenceV1,
        SealedOriginV1,
    )

    from deerflow.authz.runtime import AUTHORIZATION_PROVIDER_CONTEXT_KEY
    from deerflow.extensions.mcp import (
        MCP_INVOCATION_FACTS_CONTEXT_KEY,
        McpInvocationFacts,
    )

    context = {
        "user_id": "user-1",
        "user_role": "member",
        "thread_id": "thread-1",
        "run_id": "run-1",
        AUTHORIZATION_PROVIDER_CONTEXT_KEY: provider,
        MCP_INVOCATION_FACTS_CONTEXT_KEY: McpInvocationFacts(
            principal=PrincipalProjectionV1(user_id="user-1", role="member"),
            origin=SealedOriginV1(source_kind="service", digest="a" * 64),
            thread_id="thread-1",
            run_id="run-1",
            agent_revision=ResolvedAgentRevisionReferenceV1(
                agent_id="lead_agent",
                digest="b" * 64,
            ),
            extension_generation=generation,
        ),
    }
    if journal is not None:
        context["__run_journal"] = journal
    return SimpleNamespace(context=context)


def _request(provider, *, generation: int = 7, journal=None, args=None):
    from langchain_mcp_adapters.interceptors import MCPToolCallRequest

    return MCPToolCallRequest(
        name="lookup",
        args=args or {"query": "weather"},
        server_name="search",
        runtime=_invocation_runtime(provider, generation=generation, journal=journal),
    )


async def _call_with_authorization(interceptor, request, handler, provider):
    from deerflow_extension_api import AuthzRequest, Principal

    from deerflow.authz.runtime import AuthorizedToolCallReceipt
    from deerflow.extensions.mcp import mcp_invocation_facts_from_context
    from deerflow.guardrails.provider import bind_guardrail_provider_receipt

    facts = mcp_invocation_facts_from_context(request.runtime.context)
    assert facts is not None
    authz_request = AuthzRequest(
        principal=Principal(
            user_id=facts.principal.user_id,
            role=facts.principal.role,
            oauth_provider=facts.principal.oauth_provider,
            oauth_id=facts.principal.oauth_id,
            channel_user_id=facts.principal.channel_user_id,
            is_internal=facts.principal.is_internal,
        ),
        resource="tool",
        action="call",
        target=f"{request.server_name}_{request.name}",
        context={
            "thread_id": facts.thread_id,
            "run_id": facts.run_id,
            "tool_call_id": "call-1",
            "tool_input": dict(request.args),
            "is_subagent": False,
            "agent_id": None,
            "timestamp": "2026-08-07T00:00:00+00:00",
        },
    )
    with bind_guardrail_provider_receipt(AuthorizedToolCallReceipt(provider=provider, request=authz_request)):
        return await interceptor(request, handler)


@pytest.mark.asyncio
async def test_guardrail_authorization_receipt_reuses_exact_request_once() -> None:
    from deerflow_extension_api import PreparedMcpCallV1

    from deerflow.authz.adapter import GuardrailAuthorizationAdapter
    from deerflow.authz.runtime import AuthorizedToolCallReceipt
    from deerflow.guardrails.middleware import GuardrailMiddleware
    from deerflow.guardrails.provider import current_guardrail_provider_receipt

    class _Prepared:
        async def prepare_call(self, request):
            del request
            return PreparedMcpCallV1()

    host, monitor, _ = _host_runtime(
        _descriptor("required_one", _Prepared()),
        required=("mcp_interceptor:required_one",),
    )
    trusted = host.build_tool_interceptor(health_monitor=monitor)
    provider = _AuthorizationProvider()
    middleware = GuardrailMiddleware(GuardrailAuthorizationAdapter(provider))
    mcp_request = _request(provider)
    tool_request = MagicMock()
    tool_request.tool_call = {
        "name": "search_lookup",
        "args": {"query": "weather"},
        "id": "call-1",
    }
    tool_request.runtime = mcp_request.runtime
    network_calls = 0

    async def network(request):
        nonlocal network_calls
        del request
        network_calls += 1
        return "ok"

    async def handler(request):
        del request
        receipt = current_guardrail_provider_receipt()
        assert isinstance(receipt, AuthorizedToolCallReceipt)
        assert receipt.provider is provider
        assert receipt.request is provider.requests[0]
        return await trusted(mcp_request, network)

    assert await middleware.awrap_tool_call(tool_request, handler) == "ok"
    assert len(provider.requests) == 1
    assert provider.requests[0].target == "search_lookup"
    assert provider.requests[0].context["tool_call_id"] == "call-1"
    assert network_calls == 1
    assert current_guardrail_provider_receipt() is None


@pytest.mark.asyncio
async def test_non_authorization_guardrail_preserves_outer_authorization_receipt() -> None:
    from deerflow_extension_api import PreparedMcpCallV1

    from deerflow.authz.adapter import GuardrailAuthorizationAdapter
    from deerflow.guardrails.middleware import GuardrailMiddleware
    from deerflow.guardrails.provider import GuardrailDecision

    class _Prepared:
        async def prepare_call(self, request):
            del request
            return PreparedMcpCallV1()

    class _AdditionalGuardrail:
        name = "additional"

        def evaluate(self, request):
            del request
            return GuardrailDecision(allow=True)

        async def aevaluate(self, request):
            return self.evaluate(request)

    host, monitor, _ = _host_runtime(
        _descriptor("required_one", _Prepared()),
        required=("mcp_interceptor:required_one",),
    )
    trusted = host.build_tool_interceptor(health_monitor=monitor)
    provider = _AuthorizationProvider()
    outer = GuardrailMiddleware(GuardrailAuthorizationAdapter(provider))
    inner = GuardrailMiddleware(_AdditionalGuardrail())
    mcp_request = _request(provider)
    tool_request = MagicMock()
    tool_request.tool_call = {
        "name": "search_lookup",
        "args": dict(mcp_request.args),
        "id": "call-1",
    }
    tool_request.runtime = mcp_request.runtime

    async def network(request):
        del request
        return "ok"

    async def invoke_inner(request):
        return await inner.awrap_tool_call(
            request,
            lambda _: trusted(mcp_request, network),
        )

    assert await outer.awrap_tool_call(tool_request, invoke_inner) == "ok"
    assert len(provider.requests) == 1


@pytest.mark.asyncio
async def test_required_preparation_is_final_fence_after_compatibility_headers() -> None:
    from deerflow_extension_api import McpHeaderV1, PreparedMcpCallV1

    observed = []

    class _Prepared:
        async def prepare_call(self, request):
            observed.append(("prepare", request.tool_name))
            return PreparedMcpCallV1(
                headers=(McpHeaderV1(name="X-Prepared", value="trusted"),),
            )

    async def compatibility(request, handler):
        observed.append(("compatibility", request.name))
        headers = dict(request.headers or {})
        headers["Authorization"] = "Bearer compatibility-secret"
        return await handler(request.override(headers=headers))

    host, monitor, _ = _host_runtime(
        _descriptor("required_one", _Prepared()),
        required=("mcp_interceptor:required_one",),
    )
    provider = _AuthorizationProvider()
    network_headers = []

    async def network(request):
        observed.append(("network", request.name))
        network_headers.append(request.headers)
        return "ok"

    interceptor = host.build_tool_interceptor(
        health_monitor=monitor,
        compatibility_interceptors=(compatibility,),
    )
    request = _request(provider)
    assert await _call_with_authorization(interceptor, request, network, provider) == "ok"
    assert observed == [
        ("compatibility", "lookup"),
        ("prepare", "lookup"),
        ("network", "lookup"),
    ]
    assert network_headers == [
        {
            "Authorization": "Bearer compatibility-secret",
            "X-Prepared": "trusted",
        }
    ]


@pytest.mark.asyncio
async def test_compatibility_header_collision_fails_before_network() -> None:
    from deerflow_extension_api import McpHeaderV1, PreparedMcpCallV1

    from deerflow.extensions.mcp import McpCallPreparationError

    class _Prepared:
        async def prepare_call(self, request):
            del request
            return PreparedMcpCallV1(
                headers=(McpHeaderV1(name="authorization", value="trusted"),),
            )

    async def compatibility(request, handler):
        return await handler(request.override(headers={"Authorization": "legacy-secret"}))

    host, monitor, _ = _host_runtime(
        _descriptor("required_one", _Prepared()),
        required=("mcp_interceptor:required_one",),
    )
    provider = _AuthorizationProvider()
    request = _request(provider)
    network_calls = 0

    async def network(request):
        nonlocal network_calls
        del request
        network_calls += 1

    with pytest.raises(McpCallPreparationError) as exc_info:
        await _call_with_authorization(
            host.build_tool_interceptor(
                health_monitor=monitor,
                compatibility_interceptors=(compatibility,),
            ),
            request,
            network,
            provider,
        )
    assert exc_info.value.code == "header_conflict"
    assert network_calls == 0


def _host_runtime(*descriptors, required, generation: int = 7, timeout_seconds: float = 2.0):
    from deerflow.extensions.capabilities import (
        CapabilityHealthMonitor,
        build_capability_manifest,
    )
    from deerflow.extensions.mcp import McpInterceptorHost

    extensions = _registered_extensions(*descriptors, generation=generation)
    host = McpInterceptorHost(
        extensions,
        required_capabilities=required,
        timeout_seconds=timeout_seconds,
    )
    manifest = build_capability_manifest(
        extensions,
        required_capabilities=required,
        authorization_required=True,
        legacy_authorization_initialized=True,
        initialized_capability_ids=host.initialized_capability_ids,
    )
    monitor = CapabilityHealthMonitor(manifest, extensions)
    return host, monitor, manifest


@pytest.mark.asyncio
async def test_required_interceptor_is_attributed_healthy_and_ready() -> None:
    from deerflow_extension_api import CapabilityHealthResult, PreparedMcpCallV1

    from deerflow.extensions.capabilities import capability_manifest_to_dict

    class _Prepared:
        async def prepare_call(self, request):
            del request
            return PreparedMcpCallV1()

    async def healthy():
        return CapabilityHealthResult(status="healthy")

    host, monitor, manifest = _host_runtime(
        _descriptor("credential_broker", _Prepared(), health_probe=healthy),
        required=("mcp_interceptor:credential_broker",),
    )
    entry = next(item for item in manifest.capabilities if item.capability_type == "mcp_interceptor")
    assert entry.capability_id == "mcp_interceptor:credential_broker"
    assert entry.package_name == "acme-plugin-0"
    assert entry.package_version == "1.0.0"
    assert entry.operator_required is True
    assert (await monitor.readiness()).status == "ready"
    assert "private credential configuration" not in str(capability_manifest_to_dict(manifest)).lower()
    assert host.required_capability_ids == frozenset({"mcp_interceptor:credential_broker"})


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "initialization", "health"])
async def test_required_missing_initialization_and_health_fail_readiness(failure: str) -> None:
    from deerflow_extension_api import CapabilityHealthResult, PreparedMcpCallV1

    from deerflow.extensions.capabilities import CapabilityHealthMonitor, build_capability_manifest
    from deerflow.extensions.mcp import McpInterceptorHost

    required = ("mcp_interceptor:required_one",)
    if failure == "missing":
        extensions = _registered_extensions()
    elif failure == "initialization":

        def broken_factory():
            raise RuntimeError("secret startup detail")

        descriptor = _descriptor("required_one", object())
        object.__setattr__(descriptor, "factory", broken_factory)
        extensions = _registered_extensions(descriptor)
    else:

        class _Prepared:
            async def prepare_call(self, request):
                del request
                return PreparedMcpCallV1()

        async def unhealthy():
            return CapabilityHealthResult(
                status="unhealthy",
                diagnostic_code="credential_backend_unavailable",
            )

        extensions = _registered_extensions(_descriptor("required_one", _Prepared(), health_probe=unhealthy))
    host = McpInterceptorHost(extensions, required_capabilities=required)
    manifest = build_capability_manifest(
        extensions,
        required_capabilities=required,
        authorization_required=True,
        legacy_authorization_initialized=True,
        initialized_capability_ids=host.initialized_capability_ids,
    )
    readiness = await CapabilityHealthMonitor(manifest, extensions).readiness()
    assert readiness.status == "not_ready"
    assert "secret startup detail" not in repr((manifest, host.startup_diagnostics))

    from deerflow.extensions.mcp import McpCallPreparationError

    async def handler(request):
        raise AssertionError(f"handler unexpectedly called: {request}")

    with pytest.raises(McpCallPreparationError) as exc_info:
        provider = _AuthorizationProvider()
        request = _request(provider)
        await _call_with_authorization(
            host.build_tool_interceptor(
                health_monitor=CapabilityHealthMonitor(manifest, extensions),
            ),
            request,
            handler,
            provider,
        )
    assert exc_info.value.code in {
        "required_interceptor_unavailable",
        "required_interceptor_unhealthy",
    }


@pytest.mark.asyncio
async def test_stale_required_health_fails_readiness_and_mcp_call_before_preparation() -> None:
    from deerflow_extension_api import CapabilityHealthResult, PreparedMcpCallV1

    from deerflow.extensions.capabilities import CapabilityHealthMonitor, build_capability_manifest
    from deerflow.extensions.mcp import McpCallPreparationError, McpInterceptorHost

    current = [datetime(2026, 8, 7, tzinfo=UTC)]
    refresh_started = asyncio.Event()
    release_refresh = asyncio.Event()
    probe_calls = 0
    preparation_calls = 0
    handler_calls = 0

    async def health_probe() -> CapabilityHealthResult:
        nonlocal probe_calls
        probe_calls += 1
        if probe_calls == 1:
            return CapabilityHealthResult(status="healthy")
        refresh_started.set()
        await release_refresh.wait()
        return CapabilityHealthResult(status="unhealthy", diagnostic_code="credential_backend_unavailable")

    class _Prepared:
        async def prepare_call(self, request):
            nonlocal preparation_calls
            del request
            preparation_calls += 1
            return PreparedMcpCallV1()

    required = ("mcp_interceptor:required_one",)
    extensions = _registered_extensions(
        _descriptor("required_one", _Prepared(), health_probe=health_probe),
    )
    host = McpInterceptorHost(extensions, required_capabilities=required)
    manifest = build_capability_manifest(
        extensions,
        required_capabilities=required,
        authorization_required=True,
        legacy_authorization_initialized=True,
        initialized_capability_ids=host.initialized_capability_ids,
    )
    manifest_digest = manifest.digest
    monitor = CapabilityHealthMonitor(
        manifest,
        extensions,
        clock=lambda: current[0],
    )

    assert (await monitor.readiness()).status == "ready"
    current[0] += timedelta(seconds=31)
    stale = await monitor.readiness(refresh=False)
    assert stale.status == "not_ready"
    assert stale.health[0].diagnostic_code == "snapshot_stale"
    assert manifest.digest == manifest_digest

    refresh = asyncio.create_task(monitor.health_for(required, refresh=True))
    await asyncio.wait_for(refresh_started.wait(), timeout=1)

    async def handler(request):
        nonlocal handler_calls
        del request
        handler_calls += 1

    try:
        provider = _AuthorizationProvider()
        request = _request(provider)
        with pytest.raises(McpCallPreparationError, match="required_interceptor_unhealthy"):
            await _call_with_authorization(
                host.build_tool_interceptor(health_monitor=monitor),
                request,
                handler,
                provider,
            )
        assert preparation_calls == 0
        assert handler_calls == 0
    finally:
        release_refresh.set()
        refreshed = await refresh

    assert refreshed[0].status == "unhealthy"
    assert (await monitor.readiness(refresh=False)).status == "not_ready"
    assert manifest.digest == manifest_digest


@pytest.mark.asyncio
async def test_required_preparations_compose_in_id_order_and_handler_runs_once() -> None:
    from deerflow_extension_api import McpHeaderV1, PreparedMcpCallV1

    order = []
    projections = []

    class _Prepared:
        def __init__(self, contribution_id: str, header: str) -> None:
            self.contribution_id = contribution_id
            self.header = header

        async def prepare_call(self, request):
            order.append(self.contribution_id)
            projections.append(request)
            return PreparedMcpCallV1(
                headers=(McpHeaderV1(name=self.header, value=f"value-{self.contribution_id}"),),
            )

    host, monitor, _ = _host_runtime(
        _descriptor("z_last", _Prepared("z_last", "X-Z")),
        _descriptor("a_first", _Prepared("a_first", "X-A")),
        required=("mcp_interceptor:z_last", "mcp_interceptor:a_first"),
    )
    provider = _AuthorizationProvider()
    handler_calls = []

    async def handler(request):
        handler_calls.append(request)
        return "ok"

    request = _request(provider, args={"ordered": [2, 1]})
    result = await _call_with_authorization(
        host.build_tool_interceptor(health_monitor=monitor),
        request,
        handler,
        provider,
    )

    assert result == "ok"
    assert order == ["a_first", "z_last"]
    assert len(handler_calls) == 1
    assert handler_calls[0].headers == {
        "X-A": "value-a_first",
        "X-Z": "value-z_last",
    }
    assert projections[0].arguments_digest == projections[1].arguments_digest
    assert projections[0].principal.user_id == "user-1"
    assert projections[0].origin.source_kind == "service"
    assert projections[0].thread_id == "thread-1"
    assert projections[0].run_id == "run-1"
    assert projections[0].agent_revision.agent_id == "lead_agent"
    assert projections[0].extension_generation == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["deny", "exception", "malformed"])
async def test_authorization_stops_before_preparation(failure: str) -> None:
    from deerflow_extension_api import AuthzDecision, PreparedMcpCallV1

    from deerflow.authz.adapter import GuardrailAuthorizationAdapter
    from deerflow.guardrails.middleware import GuardrailMiddleware

    class _Prepared:
        def __init__(self) -> None:
            self.calls = 0

        async def prepare_call(self, request):
            del request
            self.calls += 1
            return PreparedMcpCallV1()

    prepared = _Prepared()
    host, monitor, _ = _host_runtime(
        _descriptor("required_one", prepared),
        required=("mcp_interceptor:required_one",),
    )
    if failure == "deny":
        provider = _AuthorizationProvider(decision=AuthzDecision(allow=False))
    elif failure == "exception":
        provider = _AuthorizationProvider(error=RuntimeError("provider secret"))
    else:
        provider = _AuthorizationProvider(decision=object())
    middleware = GuardrailMiddleware(
        GuardrailAuthorizationAdapter(provider),
        fail_closed=True,
    )
    handler_calls = 0

    trusted = host.build_tool_interceptor(health_monitor=monitor)
    mcp_request = _request(provider)
    tool_request = MagicMock()
    tool_request.tool_call = {
        "name": "search_lookup",
        "args": dict(mcp_request.args),
        "id": "call-1",
    }
    tool_request.runtime = mcp_request.runtime

    async def handler(request):
        nonlocal handler_calls
        del request
        handler_calls += 1
        return await trusted(mcp_request, lambda _: "unreachable")

    result = await middleware.awrap_tool_call(tool_request, handler)
    assert result.status == "error"
    assert prepared.calls == 0
    assert handler_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result_kind", "expected_code"),
    [
        ("rejected", "preparation_rejected"),
        ("indeterminate", "preparation_indeterminate"),
        ("exception", "preparation_indeterminate"),
        ("timeout", "preparation_indeterminate"),
        ("invalid", "preparation_indeterminate"),
    ],
)
async def test_preparation_failures_never_call_handler(
    result_kind: str,
    expected_code: str,
) -> None:
    from deerflow_extension_api import McpCallIndeterminateV1, McpCallRejectedV1

    from deerflow.extensions.mcp import McpCallPreparationError

    class _Failure:
        async def prepare_call(self, request):
            del request
            if result_kind == "rejected":
                return McpCallRejectedV1()
            if result_kind == "indeterminate":
                return McpCallIndeterminateV1()
            if result_kind == "exception":
                raise RuntimeError("credential=secret")
            if result_kind == "timeout":
                await asyncio.sleep(0.1)
                return McpCallIndeterminateV1()
            return object()

    host, monitor, _ = _host_runtime(
        _descriptor("required_one", _Failure()),
        required=("mcp_interceptor:required_one",),
        timeout_seconds=0.01,
    )
    handler_calls = 0

    async def handler(request):
        nonlocal handler_calls
        del request
        handler_calls += 1

    with pytest.raises(McpCallPreparationError) as exc_info:
        provider = _AuthorizationProvider()
        request = _request(provider)
        await _call_with_authorization(
            host.build_tool_interceptor(health_monitor=monitor),
            request,
            handler,
            provider,
        )
    assert exc_info.value.code == expected_code
    assert handler_calls == 0
    assert "secret" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
async def test_generation_mismatch_and_header_collision_fail_closed() -> None:
    from deerflow_extension_api import McpHeaderV1, PreparedMcpCallV1

    from deerflow.extensions.mcp import McpCallPreparationError

    class _Prepared:
        def __init__(self, name: str, value: str) -> None:
            self.name = name
            self.value = value

        async def prepare_call(self, request):
            del request
            return PreparedMcpCallV1(
                headers=(McpHeaderV1(name=self.name, value=self.value),),
            )

    host, monitor, _ = _host_runtime(
        _descriptor("one", _Prepared("Authorization", "first")),
        _descriptor("two", _Prepared("authorization", "second")),
        required=("mcp_interceptor:one", "mcp_interceptor:two"),
    )

    async def handler(request):
        raise AssertionError(f"handler unexpectedly called: {request}")

    interceptor = host.build_tool_interceptor(
        health_monitor=monitor,
    )
    generation_provider = _AuthorizationProvider()
    generation_request = _request(generation_provider, generation=8)
    with pytest.raises(McpCallPreparationError) as generation_error:
        await _call_with_authorization(
            interceptor,
            generation_request,
            handler,
            generation_provider,
        )
    assert generation_error.value.code == "capability_generation_mismatch"

    collision_provider = _AuthorizationProvider()
    collision_request = _request(collision_provider)
    with pytest.raises(McpCallPreparationError) as collision_error:
        await _call_with_authorization(
            interceptor,
            collision_request,
            handler,
            collision_provider,
        )
    assert collision_error.value.code == "header_conflict"


@pytest.mark.asyncio
async def test_transient_secret_headers_are_not_written_to_audit_evidence() -> None:
    from deerflow_extension_api import (
        McpHeaderV1,
        PreparedMcpCallV1,
        SafeContextReferenceV1,
    )

    class _Journal:
        def __init__(self) -> None:
            self.events = []

        def record_middleware(self, **event) -> None:
            self.events.append(event)

    class _Prepared:
        async def prepare_call(self, request):
            del request
            return PreparedMcpCallV1(
                headers=(
                    McpHeaderV1(
                        name="Authorization",
                        value="Bearer super-secret-value",
                    ),
                ),
                evidence_references=(
                    SafeContextReferenceV1(
                        key="credential_handle",
                        value="vault-handle-7",
                        storage_class="persistable",
                        purpose="secret_handle",
                    ),
                ),
            )

    host, monitor, manifest = _host_runtime(
        _descriptor("credential_broker", _Prepared()),
        required=("mcp_interceptor:credential_broker",),
    )
    journal = _Journal()
    passed_headers = []

    async def handler(request):
        passed_headers.append(request.headers)
        return "ok"

    provider = _AuthorizationProvider()
    request = _request(provider, journal=journal)
    assert (
        await _call_with_authorization(
            host.build_tool_interceptor(health_monitor=monitor),
            request,
            handler,
            provider,
        )
        == "ok"
    )
    assert passed_headers == [{"Authorization": "Bearer super-secret-value"}]
    assert len(journal.events) == 1
    rendered_audit = repr(journal.events)
    assert "credential_broker" in rendered_audit
    assert "vault-handle-7" in rendered_audit
    assert "super-secret-value" not in rendered_audit
    assert "super-secret-value" not in repr(manifest)


@pytest.mark.asyncio
async def test_legacy_compatibility_interceptor_cannot_satisfy_required_capability() -> None:
    from deerflow.extensions.capabilities import CapabilityHealthMonitor, build_capability_manifest
    from deerflow.extensions.mcp import McpCallPreparationError, McpInterceptorHost

    extensions = _registered_extensions()
    required = ("mcp_interceptor:required_one",)
    host = McpInterceptorHost(extensions, required_capabilities=required)
    manifest = build_capability_manifest(
        extensions,
        required_capabilities=required,
        authorization_required=True,
        legacy_authorization_initialized=True,
        initialized_capability_ids=(),
    )
    legacy_calls = 0

    async def legacy_interceptor(request, handler):
        nonlocal legacy_calls
        legacy_calls += 1
        return await handler(request)

    async def handler(request):
        raise AssertionError(f"handler unexpectedly called: {request}")

    trusted = host.build_tool_interceptor(
        health_monitor=CapabilityHealthMonitor(manifest, extensions),
        compatibility_interceptors=(legacy_interceptor,),
    )
    provider = _AuthorizationProvider()
    request = _request(provider)
    with pytest.raises(McpCallPreparationError) as exc_info:
        await _call_with_authorization(
            trusted,
            request,
            handler,
            provider,
        )
    assert exc_info.value.code == "required_interceptor_unavailable"
    assert legacy_calls == 0


def test_accepted_invocation_facts_are_pinned_and_caller_forgery_is_removed() -> None:
    from deerflow.extensions.mcp import (
        MCP_INVOCATION_FACTS_CONTEXT_KEY,
        McpInvocationFacts,
    )
    from deerflow.runtime.accepted_invocation import (
        AcceptedInvocation,
        InvocationOrigin,
        PrincipalProjection,
        ResolvedAgentRevision,
        canonical_digest,
    )
    from deerflow.runtime.runs.worker import _build_runtime_context

    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="owner-1", role="member"),
        origin=InvocationOrigin(
            source_kind="native_channel",
            references={"provider": "slack", "provider_message_id": "event-1"},
        ),
        thread_id="thread-1",
        context_references={},
        agent_revision=ResolvedAgentRevision(
            agent_id="lead_agent",
            digest="b" * 64,
            storage_source="file",
            storage_version="v1",
        ),
        normalized_input={},
        execution_options={},
        extension_generation=12,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )
    facts = McpInvocationFacts.from_accepted(accepted, run_id="run-1")

    assert facts.principal.user_id == "owner-1"
    assert facts.origin.source_kind == "native_channel"
    assert facts.origin.digest == accepted.base_origin_digest
    assert tuple((item.key, item.value) for item in facts.origin.references) == (
        ("provider", "slack"),
        ("provider_message_id", "event-1"),
    )
    assert facts.agent_revision.digest == "b" * 64
    assert facts.extension_generation == 12

    forged = _build_runtime_context(
        "thread-1",
        "run-1",
        {MCP_INVOCATION_FACTS_CONTEXT_KEY: facts},
    )
    assert MCP_INVOCATION_FACTS_CONTEXT_KEY not in forged


@pytest.mark.asyncio
async def test_mcp_audit_bridge_records_on_the_parent_event_loop() -> None:
    from threading import Thread

    from deerflow.extensions.mcp import build_mcp_preparation_audit_sink

    class _Journal:
        def __init__(self) -> None:
            self.events = []

        def record_middleware(self, tag, *, name, hook, action, changes):
            self.events.append((tag, name, hook, action, changes))

    journal = _Journal()
    sink = build_mcp_preparation_audit_sink({"__run_journal": journal})
    assert sink is not None

    thread = Thread(
        target=lambda: sink.record_middleware(
            "mcp_preparation",
            name="McpInterceptorHost",
            hook="prepare_call",
            action="prepared_mcp_call",
            changes={"extension_generation": 7},
        )
    )
    thread.start()
    thread.join()
    await asyncio.sleep(0)

    assert journal.events == [
        (
            "mcp_preparation",
            "McpInterceptorHost",
            "prepare_call",
            "prepared_mcp_call",
            {"extension_generation": 7},
        )
    ]

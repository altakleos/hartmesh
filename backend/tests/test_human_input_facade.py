"""Negotiation, current plugin admission and the human-only action lifetime."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import BOB
from deerflow_extension_api.auth import EXTENSION_PRINCIPAL_RESOLVER_KEY, ExtensionPrincipal
from deerflow_extension_api.human_input import HumanInputActions
from deerflow_extension_api.plugins import ActionContext, BackendAction, PluginContribution, ToolContext
from test_agent_instances import instances as instances
from test_agent_instances import request_app
from test_agent_work_records import work as work
from test_work_human_input import ask, reply
from test_work_human_input import attention as attention

from deerflow.extensions.registry import ExtensionRegistry


async def handler(payload, context):
    return {"available": context.human_input is not None}


def test_human_contract_negotiates_explicitly_and_preserves_old_contexts():
    plugin = PluginContribution(namespace="example.input", title="Input", api_version=5, human_input_api_version=1, backend=(BackendAction("input", handler),))
    for bad in (replace(plugin, api_version=4), replace(plugin, human_input_api_version=2), replace(plugin, human_input_api_version=True), replace(plugin, backend=())):
        with pytest.raises(ValueError):
            # Use one registry for both attribution and attempted registration.
            registry = ExtensionRegistry()
            with registry.attributed_to("test"):
                registry.plugin(bad)
    registry = ExtensionRegistry()
    with registry.attributed_to("test"):
        assert registry.plugin(plugin)
    assert ActionContext(None, {}).human_input is None
    assert ToolContext(None, {}, "thread").human_input is None


@pytest.mark.asyncio
async def test_unsupported_facade_is_not_empty_success():
    with pytest.raises(NotImplementedError):
        await HumanInputActions().call("list", {})


@pytest.mark.asyncio
async def test_real_action_uses_canonical_service_and_retires_its_handle(attention, monkeypatch):
    import httpx

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.plugins import router

    service, instance, work, sf = attention
    req = await ask(attention)
    captured = []

    async def respond(payload, context):
        captured.append(context.human_input)
        return await context.human_input.call("respond", {"request_id": req["id"], "body": payload})

    plugin = PluginContribution(namespace="example.input", title="Input", enabled=True, api_version=5, human_input_api_version=1, backend=(BackendAction("respond", respond),))
    registry = ExtensionRegistry()
    with registry.attributed_to("test:input"):
        registry.plugin(plugin)
    app = request_app(service.agents, actor=BOB)
    app.state.extensions = registry.build()
    setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda request: ExtensionPrincipal("bob"))
    app.include_router(router)
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        result = await client.post("/api/plugins/example.input/actions/respond", json=reply(req).model_dump(mode="json"))
        assert result.status_code == 200, result.text
        assert result.json()["state"] == "answered"
    with pytest.raises(PermissionError):
        await captured[0].call("get", {"request_id": req["id"]})
    assert len((await service.responses(actor=BOB, request_id=req["id"]))["responses"]) == 1


@pytest.mark.asyncio
async def test_facade_rechecks_disabled_removed_plugin_and_provider_ceiling(attention, monkeypatch):
    from starlette.requests import Request

    from app.gateway.authz import AuthContext
    from app.gateway.human_input_facade import HostHumanInputActions
    from app.gateway.routers import agent_instances as routes

    service, _, _, _ = attention
    req = await ask(attention)
    plugin = PluginContribution(namespace="example.input", title="Input", enabled=True, api_version=5, human_input_api_version=1, backend=(BackendAction("input", handler),))
    registry = ExtensionRegistry()
    with registry.attributed_to("test:input"):
        registry.plugin(plugin)
    app = request_app(service.agents, actor=BOB)
    app.state.extensions = registry.build()
    setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda request: ExtensionPrincipal("bob"))
    request = Request({"type": "http", "method": "POST", "path": "/api/plugins/example.input/actions/input", "headers": [], "app": app})
    request.state.user = SimpleNamespace(id="bob", system_role="user")
    request.state.auth_source = "session"
    request.state.auth = AuthContext(request.state.user, ["agents:read"])
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    facade = HostHumanInputActions(request, "test:input", plugin, plugin.backend[0], ExtensionPrincipal("bob"))
    with pytest.raises(PermissionError):
        await facade.call("respond", {"request_id": req["id"], "body": reply(req).model_dump(mode="json")})
    assert (await facade.call("get", {"request_id": req["id"]}))["can_respond"] is False
    app.state.extensions = SimpleNamespace(plugins=[])
    with pytest.raises(PermissionError):
        await facade.call("get", {"request_id": req["id"]})
    app.state.extensions = registry.build()
    monkeypatch.setattr("app.gateway.human_input_facade.plugin_settings", lambda *_: {"enabled": False})
    with pytest.raises(PermissionError):
        await facade.call("get", {"request_id": req["id"]})

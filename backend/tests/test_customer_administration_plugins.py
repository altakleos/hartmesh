"""Trusted management declarations obey the same independent host floor."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api.auth import EXTENSION_PRINCIPAL_RESOLVER_KEY, ExtensionPrincipal
from deerflow_extension_api.plugins import BackendAction, ModelTool, PluginContribution
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.routers.plugins import router
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.runtime.customer_administration import CustomerAdministrationPolicy


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("role", ["admin", "user"])
def test_declared_management_action_needs_provider_floor_and_role(enabled, role):
    handler = AsyncMock(return_value={"ok": True})
    declaration = BackendAction("manage", handler, purpose="management")
    registry = ExtensionRegistry()
    with registry.attributed_to("provider:fixture"):
        registry.plugin(PluginContribution(namespace="example.management", title="Management", enabled=True, backend=(declaration,), api_version=3))
    app = FastAPI()
    app.state.extensions = registry.build()
    app.state.customer_administration_policy = CustomerAdministrationPolicy(plugin_management=enabled)
    setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda request: ExtensionPrincipal("owner", is_admin=role == "admin"))

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.user = SimpleNamespace(id="owner", system_role=role)
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(router)
    with TestClient(app) as client:
        result = client.post("/api/plugins/example.management/actions/manage", json={})
    allowed = enabled and role == "admin"
    assert result.status_code == (200 if allowed else 403)
    assert handler.await_count == int(allowed)


@pytest.mark.parametrize("kind", ["action", "tool"])
@pytest.mark.parametrize("purpose", ["unknown", None, True])
def test_unknown_management_purpose_rejects_registration_atomically(kind, purpose):
    handler = AsyncMock()
    declaration = BackendAction("operation", handler, purpose=purpose) if kind == "action" else ModelTool("operation", "Operation", {"type": "object"}, handler, purpose=purpose)
    plugin = PluginContribution(namespace="example.management", title="Management", backend=(declaration,) if kind == "action" else (), tools=(declaration,) if kind == "tool" else (), api_version=3)
    registry = ExtensionRegistry()
    with registry.attributed_to("provider:fixture"):
        with pytest.raises(ValueError):
            registry.plugin(plugin)
    assert not registry.build().plugins


@pytest.mark.asyncio
@pytest.mark.parametrize("allow", [False, True])
async def test_management_model_tool_uses_host_config_for_namespace_write(allow, monkeypatch):
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    from deerflow.config.app_config import AppConfig
    from deerflow.extensions.plugin_tools import _build_tool
    from deerflow.runtime.customer_administration import CUSTOMER_ADMINISTRATION_CONTEXT_KEY, CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY, CustomerManagementActor

    config = AppConfig.model_validate(
        {
            "sandbox": {"use": "test"},
            "authorization": {"enabled": True, "default_role": "user", "fail_closed": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"user": {}, "admin": {"tools": ["*"]}}}}},
        }
    )
    if allow:
        config.authorization.enabled = False
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: (_ for _ in ()).throw(FileNotFoundError("Other host has no config")))
    handler = AsyncMock(return_value={"ok": True})
    declaration = ModelTool("manage", "Manage", {"type": "object"}, handler, purpose="management")
    plugin = PluginContribution(namespace="example.management", title="Management", enabled=True, tools=(declaration,), api_version=3)
    tool = _build_tool("provider:fixture", plugin, declaration)
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([tool]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    result = await graph.compile().ainvoke(
        {"messages": [AIMessage(content="", tool_calls=[{"id": "management", "name": tool.name, "args": {}}])]},
        context={
            "user_id": "owner",
            "user_role": "admin",
            "app_config": config,
            CUSTOMER_ADMINISTRATION_CONTEXT_KEY: CustomerAdministrationPolicy(plugin_management=True),
            CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY: CustomerManagementActor(owner_id="owner", administrator=True),
        },
    )
    assert result["messages"][-1].status == ("success" if allow else "error")
    assert handler.await_count == int(allow)

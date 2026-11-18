"""MCP writes on deployments whose connections are written by tenants.

With ``DEER_FLOW_TENANT_WRITTEN_ENDPOINTS=on`` the write routes accept only
public HTTP(S) addresses (or provider-listed private endpoints) and, for a
deployment connection, no value that begins with ``$``. The check is made on
the definition as it would be stored, and only on a definition the request
creates, changes or switches on.

The real routes run under the strict blocking-IO gate: the address check can
look a name up, so it has to stay off the event loop. Imports sit at module top
so import-time IO runs at collection, outside the gate.
"""

from __future__ import annotations

import _socket
import asyncio
import json
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock

import langchain_mcp_adapters.client  # noqa: F401 - imported before the gate is active
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from support.mcp_endpoint_stubs import EndpointStub

from app.gateway.deps import get_config
from app.gateway.routers import capabilities, mcp, personal_mcp
from deerflow.config.extensions_config import McpServerConfig, reset_extensions_config
from deerflow.config.paths import Paths
from deerflow.mcp import user_tools
from deerflow.mcp.cache import reset_mcp_tools_cache
from deerflow.mcp.user_config import load_user_mcp_config, read_user_mcp_config
from deerflow.runtime.user_context import reset_current_user, set_current_user

LINE = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS = "DEER_FLOW_PRIVATE_ENDPOINTS"
ADDRESS_NOT_PUBLIC = "MCP connections on this deployment must use a public HTTP(S) address."
ENVIRONMENT_REFERENCE = "Values that begin with `$` are not accepted for MCP connections on this deployment."
MARKER = "plain-marker-word"
ADMINISTRATOR = "administrator-1"
PUBLIC = "https://8.8.8.8/mcp"
OTHER_PUBLIC = "https://8.8.4.4/mcp"
PRIVATE_NAME = "records.example.test"
TYPED = {"type": "http", "url": PUBLIC, "headers": {"X-Records-Key": "$HM_TEST_VALUE"}}


@pytest.fixture
def gateway(tmp_path, monkeypatch):
    path = tmp_path / "extensions_config.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(path))
    monkeypatch.setattr("deerflow.mcp.user_config.get_paths", lambda: Paths(base_dir=tmp_path / "home"))
    monkeypatch.setenv(LINE, "on")
    monkeypatch.delenv(PRIVATE_ENDPOINTS, raising=False)
    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    monkeypatch.setenv("HM_TEST_VALUE_B", MARKER)
    reset_extensions_config()
    reset_mcp_tools_cache()

    def plant(servers: dict) -> bytes:
        """Write definitions straight into the file, as an earlier save would have left them."""
        path.write_text(json.dumps({"mcpServers": servers, "skills": {}}), encoding="utf-8")
        return path.read_bytes()

    plant({})
    app = FastAPI()
    app.dependency_overrides[get_config] = lambda: SimpleNamespace()

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.user = SimpleNamespace(id=ADMINISTRATOR, system_role=request.headers.get("test-role", "admin"))
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(mcp.router)
    app.include_router(personal_mcp.router)
    app.include_router(capabilities.router)
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, path=path, plant=plant, stored=lambda: json.loads(path.read_text(encoding="utf-8"))["mcpServers"])
    reset_extensions_config()
    reset_mcp_tools_cache()


@pytest.fixture
def private_name(monkeypatch):
    """Resolve one made-up name to a private address, below the instrumented lookup."""
    native_getaddrinfo = _socket.getaddrinfo

    def getaddrinfo(host, port, family=socket.AF_UNSPEC, *args, **kwargs):
        if host == PRIVATE_NAME:
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.5", port or 0))]
        return native_getaddrinfo(host, port, family, *args, **kwargs)

    monkeypatch.setattr(_socket, "getaddrinfo", getaddrinfo)


def _refused(response, detail: str, *, route: str | None = None) -> bool:
    assert response.status_code == 400, response.text
    # The route that takes every definition at once says which one it refused.
    assert response.json()["detail"] == (f"MCP server 'records': {detail}" if route == "whole_file" else detail)
    return True


def _write(client, route: str, name: str, definition: dict):
    if route == "add":
        return client.post("/api/mcp/config/servers", json={"mcp_servers": {name: definition}})
    if route == "replace":
        return client.put("/api/mcp/config/server", json={"server_name": name, "server": definition})
    if route == "whole_file":
        return client.put("/api/mcp/config", json={"mcp_servers": {name: definition}})
    return client.patch("/api/mcp/config", json={"server_name": name, "enabled": True})


def test_adding_a_value_that_begins_with_dollar_is_refused(gateway):
    before = gateway.path.read_bytes()

    assert _refused(gateway.client.post("/api/mcp/config/servers", json={"mcp_servers": {"records": TYPED}}), ENVIRONMENT_REFERENCE)
    for nested in ({"oauth": {"token_url": PUBLIC, "client_id": "records-client", "client_secret": "$HM_TEST_VALUE"}}, {"routing": {"mode": "prefer", "keywords": ["reports", "$HM_TEST_VALUE"]}}, {"description": "$HM_TEST_VALUE"}):
        assert _refused(gateway.client.post("/api/mcp/config/servers", json={"mcp_servers": {"records": {"type": "http", "url": PUBLIC, **nested}}}), ENVIRONMENT_REFERENCE)
    # Installing a capability for the deployment stores through the same check.
    installation = {"plugin_id": "github", "name": "team-records", "scope": "deployment", "configuration": TYPED}
    assert _refused(gateway.client.post("/api/capabilities/installations", json=installation), ENVIRONMENT_REFERENCE)

    assert gateway.path.read_bytes() == before


@pytest.mark.parametrize("route", ["replace", "whole_file", "switch_on"])
def test_a_stored_value_cannot_be_carried_into_a_changed_definition(gateway, route):
    before = gateway.plant({"records": {**TYPED, "enabled": route != "switch_on"}})
    # The list route masks the stored value; sending the mask back restores it.
    assert gateway.client.get("/api/mcp/config").json()["mcp_servers"]["records"]["headers"] == {"X-Records-Key": "***"}

    changed = {"type": "http", "url": OTHER_PUBLIC, "headers": {"X-Records-Key": "***"}}
    assert _refused(_write(gateway.client, route, "records", changed), ENVIRONMENT_REFERENCE, route=route)

    assert gateway.path.read_bytes() == before


def test_switching_off_and_deleting_stay_allowed(gateway):
    gateway.plant({"records": TYPED, "ledger": {**TYPED, "url": "http://127.0.0.1:9/mcp"}})

    response = gateway.client.patch("/api/mcp/config", json={"server_name": "records", "enabled": False})
    assert response.status_code == 200, response.text
    assert gateway.stored()["records"]["enabled"] is False
    assert gateway.stored()["records"]["headers"] == {"X-Records-Key": "$HM_TEST_VALUE"}
    # A stored definition can also be switched off through the route that sends the whole of it.
    listed = gateway.client.get("/api/mcp/config").json()["mcp_servers"]
    response = gateway.client.put("/api/mcp/config/server", json={"server_name": "ledger", "server": {**listed["ledger"], "enabled": False}})
    assert response.status_code == 200, response.text
    assert gateway.stored()["ledger"]["enabled"] is False

    for name in ("records", "ledger"):
        assert gateway.client.delete(f"/api/mcp/config/servers/{name}").status_code == 200
    assert gateway.stored() == {}


def test_an_unchanged_stored_definition_does_not_block_other_writes(gateway):
    gateway.plant({"records": TYPED, "ledger": {"transport": "sse", "url": "http://127.0.0.1:9/sse", "oauth": {"token_url": "http://127.0.0.1:9/token", "client_id": "ledger-client", "client_secret": "$HM_TEST_VALUE"}}})
    planted = {name: McpServerConfig.model_validate(definition) for name, definition in gateway.stored().items()}

    # Exactly what the list route returned, beside one new connection.
    listed = gateway.client.get("/api/mcp/config").json()["mcp_servers"]
    response = gateway.client.put("/api/mcp/config", json={"mcp_servers": {**listed, "fresh": {"type": "http", "url": OTHER_PUBLIC}}})

    assert response.status_code == 200, response.text
    stored = gateway.stored()
    assert set(stored) == {"records", "ledger", "fresh"}
    assert {name: McpServerConfig.model_validate(stored[name]) for name in planted} == planted
    assert stored["records"]["headers"] == {"X-Records-Key": "$HM_TEST_VALUE"}


@pytest.mark.parametrize("route", ["add", "replace", "whole_file", "switch_on"])
@pytest.mark.parametrize(
    "definition",
    [
        pytest.param({"type": "http", "url": "http://127.0.0.1:9/mcp"}, id="loopback"),
        pytest.param({"type": "sse", "url": "https://10.0.0.5/sse"}, id="private-number"),
        pytest.param({"transport": "http", "url": f"https://{PRIVATE_NAME}/mcp"}, id="name-resolving-to-private"),
        pytest.param({"type": "http", "url": PUBLIC, "oauth": {"token_url": "http://127.0.0.1:9/token", "client_id": "records-client", "client_secret": "typed-value"}}, id="token-address"),
    ],
)
def test_addresses_that_are_not_public_are_refused(gateway, private_name, route, definition):
    stored = {} if route == "add" else {"records": {**definition, "enabled": False}} if route == "switch_on" else {"records": {"type": "http", "url": PUBLIC}}
    before = gateway.plant(stored)

    assert _refused(_write(gateway.client, route, "records", definition), ADDRESS_NOT_PUBLIC, route=route)

    assert gateway.path.read_bytes() == before


def test_a_public_address_and_a_listed_private_endpoint_are_accepted(gateway, monkeypatch):
    monkeypatch.setenv(PRIVATE_ENDPOINTS, "10.0.0.5:8443")

    for name, url in (("records", PUBLIC), ("ledger", "https://10.0.0.5:8443/mcp")):
        response = gateway.client.post("/api/mcp/config/servers", json={"mcp_servers": {name: {"type": "http", "url": url, "headers": {"X-Records-Key": "typed-value"}}}})
        assert response.status_code == 200, response.text
    assert _refused(gateway.client.post("/api/mcp/config/servers", json={"mcp_servers": {"other-port": {"type": "http", "url": "https://10.0.0.5:9443/mcp"}}}), ADDRESS_NOT_PUBLIC)
    assert set(gateway.stored()) == {"records", "ledger"}


def test_refused_save_reports_values_as_typed(gateway):
    before = gateway.path.read_bytes()
    # Two different texts as typed.
    toolset = {"name": "reports", "submit_tool": "$HM_TEST_VALUE", "status_tool": "$HM_TEST_VALUE_B", "cancel_tool": "cancel_report"}

    response = gateway.client.put("/api/mcp/config", json={"mcp_servers": {"records": {"type": "http", "url": PUBLIC, "task_toolsets": [toolset]}}})

    assert _refused(response, ENVIRONMENT_REFERENCE, route="whole_file")
    assert MARKER not in response.text
    assert gateway.path.read_bytes() == before


def test_a_switched_off_oauth_setup_is_not_checked(gateway):
    """A token address is used only while its OAuth setup is switched on."""
    unused = {"type": "http", "url": PUBLIC, "oauth": {"enabled": False, "token_url": ""}}
    resting = {"type": "http", "url": PUBLIC, "oauth": {"enabled": False, "token_url": "http://127.0.0.1:9/token", "client_id": "records-client", "client_secret": "typed-value"}}
    response = gateway.client.post("/api/mcp/config/servers", json={"mcp_servers": {"records": unused, "ledger": resting}})
    assert response.status_code == 200, response.text
    before = gateway.path.read_bytes()

    # Switching the setup on is a change, and is checked.
    switched_on = {**resting, "oauth": {**resting["oauth"], "enabled": True}}
    assert _refused(gateway.client.put("/api/mcp/config/server", json={"server_name": "ledger", "server": switched_on}), ADDRESS_NOT_PUBLIC)
    assert gateway.path.read_bytes() == before


def test_personal_values_that_begin_with_dollar_are_ordinary_text(gateway, monkeypatch):
    monkeypatch.setattr("deerflow.mcp.personal_access._admin_checker", AsyncMock(return_value=True))

    async def discover_and_call() -> None:
        config = await asyncio.to_thread(load_user_mcp_config, ADMINISTRATOR)
        tools = await user_tools._load(ADMINISTRATOR, config)
        assert len(tools) == 1
        assert "echo:hello" in str(await tools[0].ainvoke({"text": "hello"}))

    with EndpointStub() as service:
        monkeypatch.setenv(PRIVATE_ENDPOINTS, service.host_port)
        response = gateway.client.post("/api/mcp/personal/config/servers", json={"mcp_servers": {"records": {"type": "http", "url": service.url("/mcp"), "headers": {"X-Records-Key": "$HM_TEST_VALUE"}}}})
        assert response.status_code == 200, response.text
        assert read_user_mcp_config(ADMINISTRATOR)["mcpServers"]["records"]["headers"] == {"X-Records-Key": "$HM_TEST_VALUE"}

        identity = set_current_user(SimpleNamespace(id=ADMINISTRATOR))
        try:
            asyncio.run(discover_and_call())
        finally:
            reset_current_user(identity)

        assert set(service.header_values("X-Records-Key")) == {"$HM_TEST_VALUE"}
        assert MARKER not in service.received_text()

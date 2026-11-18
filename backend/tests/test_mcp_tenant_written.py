"""MCP connections on deployments whose connections are written by tenants.

With ``DEER_FLOW_TENANT_WRITTEN_ENDPOINTS=on`` connection values are used as
typed, and only public HTTP(S) addresses or provider-listed private endpoints
are contacted. Every test drives the real loader, the real connection builder
and the real discovery over a socket to a loopback stub; none replaces the HTTP
client, its transport or the address check.
"""

from __future__ import annotations

import json
from collections import Counter
from contextlib import ExitStack

import pytest
from support.mcp_endpoint_stubs import EndpointStub

LINE = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS = "DEER_FLOW_PRIVATE_ENDPOINTS"
MARKER = "plain-marker-word"


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    from deerflow.config.extensions_config import reset_extensions_config

    # The ordinary client reads proxy settings; loopback stubs must stay direct.
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    monkeypatch.delenv(LINE, raising=False)
    monkeypatch.delenv(PRIVATE_ENDPOINTS, raising=False)
    reset_extensions_config()
    yield
    reset_extensions_config()


@pytest.fixture
def stubs():
    with ExitStack() as stack:
        yield lambda: stack.enter_context(EndpointStub())


@pytest.fixture
def company_file(tmp_path, monkeypatch):
    path = tmp_path / "extensions_config.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(path))

    def write(servers: dict) -> None:
        path.write_text(json.dumps({"mcpServers": servers, "skills": {}}), encoding="utf-8")

    return write


def _tenant_written(monkeypatch, *listed: EndpointStub) -> None:
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, ",".join(stub.host_port for stub in listed))


async def _discover() -> list:
    from deerflow.mcp.tools import get_mcp_tools

    return await get_mcp_tools()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["http", "sse"])
async def test_all_connection_requests_use_the_supplied_client(transport, stubs, company_file, monkeypatch):
    from deerflow.mcp.personal_network import _PersonalTransport

    listed, unlisted = stubs(), stubs()
    _tenant_written(monkeypatch, listed)
    counted: list[tuple[str, str]] = []
    send = _PersonalTransport.handle_async_request

    async def counting(self, request):
        counted.append((request.method, request.url.raw_path.decode("ascii")))
        return await send(self, request)

    monkeypatch.setattr(_PersonalTransport, "handle_async_request", counting)
    company_file({"records": {"type": transport, "url": listed.url("/mcp" if transport == "http" else "/sse"), "session_init_timeout": 2}})

    # A refusal that points at an authorization document on another origin.
    listed.unauthorized_once = unlisted.url("/.well-known/oauth-protected-resource")
    assert await _discover() == []
    if transport == "sse":
        # A session that names another origin as the place to post messages.
        listed.sse_post_endpoint_once = unlisted.url("/messages?session_id=elsewhere")
        assert await _discover() == []
    tools = await _discover()
    assert [tool.name for tool in tools] == ["records_echo"]
    assert "echo:hello" in str(await tools[0].ainvoke({"text": "hello"}))

    assert unlisted.connections == 0
    received = listed.calls()
    assert any(b'"tools/call"' in body for body in listed.bodies("/mcp" if transport == "http" else "/messages"))
    assert not Counter(received) - Counter(counted), "every request the endpoint received went through the supplied client"


@pytest.mark.asyncio
async def test_values_are_sent_as_typed(stubs, company_file, monkeypatch):
    service = stubs()
    _tenant_written(monkeypatch, service)
    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "headers": {"X-Records-Key": "$HM_TEST_VALUE"}}})

    tools = await _discover()
    assert [tool.name for tool in tools] == ["records_echo"]
    assert "echo:hello" in str(await tools[0].ainvoke({"text": "hello"}))

    assert set(service.header_values("X-Records-Key")) == {"$HM_TEST_VALUE"}
    assert len(service.header_values("X-Records-Key")) == len(service.calls())
    assert MARKER not in service.received_text()

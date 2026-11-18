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
    assert listed.unauthorized_once is None, "the refusal was served"
    if transport == "sse":
        # A session that names another origin as the place to post messages.
        listed.sse_post_endpoint_once = unlisted.url("/messages?session_id=elsewhere")
        assert await _discover() == []
        assert listed.sse_post_endpoint_once is None, "the other origin was announced"
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


@pytest.mark.asyncio
async def test_only_public_addresses_are_contacted(stubs, company_file, monkeypatch):
    service, token = stubs(), stubs()
    _tenant_written(monkeypatch)
    company_file({"records": {"type": "http", "url": service.url("/mcp")}, "feed": {"type": "sse", "url": service.url("/sse")}})

    assert await _discover() == []
    assert service.connections == 0

    # A token address is held to the same rule as the connection's own.
    _tenant_written(monkeypatch, service)
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "oauth": {"token_url": token.url("/token"), "client_id": "records-client", "client_secret": "typed-value"}}})
    await _discover()
    assert token.connections == 0


@pytest.mark.asyncio
async def test_a_name_is_checked_again_when_it_is_connected(stubs, company_file, monkeypatch):
    import socket

    from deerflow.community.endpoint_policy import address_problem

    service = stubs()
    _tenant_written(monkeypatch)
    lookups: list[str] = []
    system_lookup = socket.getaddrinfo

    def lookup(host, port, *args, **kwargs):
        if host != "records.example.test":
            return system_lookup(host, port, *args, **kwargs)
        # A public address the first time it is asked, the loopback stub afterwards.
        lookups.append("8.8.8.8" if not lookups else "127.0.0.1")
        return system_lookup(lookups[-1], port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    url = service.url("/mcp", host="records.example.test")
    assert address_problem(url) is None
    company_file({"records": {"type": "http", "url": url}})

    assert await _discover() == []
    assert lookups[:2] == ["8.8.8.8", "127.0.0.1"]
    assert service.connections == 0


@pytest.mark.asyncio
async def test_a_listed_private_endpoint_is_exact(stubs, company_file, monkeypatch):
    listed, other_port = stubs(), stubs()
    company_file({"records": {"type": "http", "url": listed.url("/mcp")}, "ledger": {"type": "http", "url": other_port.url("/mcp")}})

    _tenant_written(monkeypatch, listed)
    assert [tool.name for tool in await _discover()] == ["records_echo"]
    assert other_port.connections == 0

    _tenant_written(monkeypatch)
    before = listed.connections
    assert await _discover() == []
    assert (listed.connections, other_port.connections) == (before, 0)

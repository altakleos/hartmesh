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
from support import loopback_only
from support.mcp_endpoint_stubs import EndpointStub

LINE = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS = "DEER_FLOW_PRIVATE_ENDPOINTS"
MARKER = "plain-marker-word"
MADE_UP_NAME = "records.example.test"


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


@pytest.fixture(autouse=True)
def network(monkeypatch):
    """No test here leaves the loopback interface, whatever the code under test does."""
    guard = loopback_only.install(monkeypatch)
    yield guard
    guard.assert_nothing_blocked()


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
async def test_token_request_values_are_sent_as_typed(stubs, company_file, monkeypatch):
    from urllib.parse import parse_qs

    service, token = stubs(), stubs()
    _tenant_written(monkeypatch, service, token)
    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    company_file(
        {
            "records": {
                "type": "http",
                "url": service.url("/mcp"),
                "headers": {"X-Records-Key": "typed-value"},
                "oauth": {"token_url": token.url("/token"), "client_id": "records-client", "client_secret": "$HM_TEST_VALUE", "extra_token_params": {"resource": "$HM_TEST_VALUE"}},
            },
            "ledger": {"type": "http", "url": service.url("/mcp"), "oauth": {"token_url": token.url("/token"), "grant_type": "refresh_token", "refresh_token": "$HM_TEST_VALUE"}},
        }
    )

    tools = await _discover()
    assert sorted(tool.name for tool in tools) == ["ledger_echo", "records_echo"]
    for tool in tools:
        assert "echo:hello" in str(await tool.ainvoke({"text": "hello"}))

    forms = [parse_qs(body.decode("ascii")) for body in token.bodies("/token")]
    client_credentials = next(form for form in forms if form["grant_type"] == ["client_credentials"])
    refresh = next(form for form in forms if form["grant_type"] == ["refresh_token"])
    assert (client_credentials["client_secret"], client_credentials["resource"], refresh["refresh_token"]) == (["$HM_TEST_VALUE"], ["$HM_TEST_VALUE"], ["$HM_TEST_VALUE"])
    # The fetched token and the connection's fixed header travel together.
    assert set(service.header_values("Authorization")) == {"Bearer stub-access-token"}
    assert "typed-value" in service.header_values("X-Records-Key")
    assert MARKER not in token.received_text() + service.received_text()


def test_the_tool_cache_publishes_tenant_written_connections_once(stubs, company_file, monkeypatch):
    """The cached entry a chat uses publishes the same tools and does not rediscover them."""
    from deerflow.mcp.cache import get_cached_mcp_tools, reset_mcp_tools_cache

    service = stubs()
    _tenant_written(monkeypatch, service)
    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "headers": {"X-Records-Key": "$HM_TEST_VALUE"}}, "feed": {"type": "sse", "url": service.url("/sse")}})
    reset_mcp_tools_cache()
    try:
        tools = get_cached_mcp_tools()
        assert [tool.name for tool in tools] == ["records_echo", "feed_echo"]
        discovered = len(service.calls())
        assert [tool.name for tool in get_cached_mcp_tools()] == ["records_echo", "feed_echo"]
        assert len(service.calls()) == discovered

        for tool in tools:
            assert "echo:hello" in str(tool.invoke({"text": "hello"}))
        assert set(service.header_values("X-Records-Key")) == {"$HM_TEST_VALUE"}
        assert MARKER not in service.received_text()
    finally:
        reset_mcp_tools_cache()


def test_program_environment_values_are_used_as_typed(company_file, monkeypatch):
    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.mcp.client import build_servers_config

    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    company_file({"files": {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0", "$HM_TEST_VALUE"], "env": {"RECORDS_KEY": "$HM_TEST_VALUE"}}})

    monkeypatch.setenv(LINE, "on")
    typed = build_servers_config(ExtensionsConfig.from_file())["files"]
    assert (typed["args"], typed["env"]) == (["example-server@1.0.0", "$HM_TEST_VALUE"], {"RECORDS_KEY": "$HM_TEST_VALUE"})

    monkeypatch.delenv(LINE)
    filled = build_servers_config(ExtensionsConfig.from_file())["files"]
    assert (filled["args"], filled["env"]) == (["example-server@1.0.0", MARKER], {"RECORDS_KEY": MARKER})


@pytest.mark.asyncio
async def test_proxy_settings_of_the_environment_are_not_read(stubs, company_file, monkeypatch):
    service, proxy = stubs(), stubs()
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "session_init_timeout": 2}})
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name)
    for name in ("HTTP_PROXY", "http_proxy"):
        monkeypatch.setenv(name, proxy.url(""))

    # Without the setting the ordinary client hands every request to the proxy.
    await _discover()
    assert (service.connections, proxy.connections > 0) == (0, True)

    _tenant_written(monkeypatch, service)
    handed_to_proxy = proxy.connections
    assert [tool.name for tool in await _discover()] == ["records_echo"]
    assert (service.connections > 0, proxy.connections) == (True, handed_to_proxy)


@pytest.mark.asyncio
async def test_routing_keywords_are_used_as_typed(stubs, company_file, monkeypatch):
    from deerflow.tools.builtins.tool_search import get_mcp_routing_hints_prompt_section

    service = stubs()
    _tenant_written(monkeypatch, service)
    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "routing": {"mode": "prefer", "keywords": ["$HM_TEST_VALUE"]}}})

    tools = await _discover()
    assert [tool.name for tool in tools] == ["records_echo"]
    hints = get_mcp_routing_hints_prompt_section(tools)
    assert "$HM_TEST_VALUE" in hints
    assert MARKER not in hints


@pytest.mark.asyncio
async def test_deployments_without_the_setting_are_unchanged(stubs, company_file, monkeypatch):
    from deerflow.tools.builtins.tool_search import get_mcp_routing_hints_prompt_section

    service = stubs()
    monkeypatch.setenv("HM_TEST_VALUE", MARKER)
    # Listed or not makes no difference: the list is read only with the setting.
    monkeypatch.setenv(PRIVATE_ENDPOINTS, "")
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "headers": {"X-Records-Key": "$HM_TEST_VALUE"}, "routing": {"mode": "prefer", "keywords": ["$HM_TEST_VALUE"]}}})

    for value in (None, "On", "true", "1", "off", ""):
        monkeypatch.delenv(LINE, raising=False) if value is None else monkeypatch.setenv(LINE, value)
        received = len(service.calls())
        tools = await _discover()
        assert [tool.name for tool in tools] == ["records_echo"], value
        assert "echo:hello" in str(await tools[0].ainvoke({"text": "hello"}))
        assert set(service.header_values("X-Records-Key")[received:]) == {MARKER}, value
        assert MARKER in get_mcp_routing_hints_prompt_section(tools)


@pytest.mark.asyncio
@pytest.mark.parametrize("setting", ["on", None])
async def test_redirects_are_not_followed(setting, stubs, company_file, monkeypatch):
    """A redirect to another origin is not followed, with the setting and without it."""
    service, token, elsewhere = stubs(), stubs(), stubs()
    if setting:
        # Every endpoint is listed, so only the redirect rule can keep a request away.
        _tenant_written(monkeypatch, service, token, elsewhere)
    token.redirect_to = elsewhere.url("/token")
    company_file({"records": {"type": "http", "url": service.url("/mcp"), "oauth": {"token_url": token.url("/token"), "client_id": "records-client", "client_secret": "typed-value"}}})
    await _discover()
    assert token.calls() == [("POST", "/token")]
    assert elsewhere.connections == 0

    service.redirect_to = elsewhere.url("/mcp")
    company_file({"records": {"type": "http", "url": service.url("/mcp")}, "feed": {"type": "sse", "url": service.url("/sse")}})
    assert await _discover() == []
    assert {("POST", "/mcp"), ("GET", "/sse")} <= set(service.calls())
    assert elsewhere.connections == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("setting", ["on", None])
async def test_a_redirect_within_the_endpoint_origin_is_followed(setting, stubs, company_file, monkeypatch):
    """The MCP transport itself follows a redirect that stays on the endpoint's origin; that is unchanged."""
    service = stubs()
    if setting:
        _tenant_written(monkeypatch, service)
    service.redirects["/mcp"] = service.url("/mcp/")
    company_file({"records": {"type": "http", "url": service.url("/mcp")}})

    tools = await _discover()

    assert [tool.name for tool in tools] == ["records_echo"]
    assert {("POST", "/mcp"), ("POST", "/mcp/")} <= set(service.calls())


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
async def test_a_name_that_stops_being_public_after_saving_is_not_contacted(stubs, company_file, network, monkeypatch):
    from deerflow.community.endpoint_policy import address_problem

    service = stubs()
    _tenant_written(monkeypatch)
    network.names[MADE_UP_NAME] = ["8.8.8.8", "127.0.0.1"]
    url = service.url("/mcp", host=MADE_UP_NAME)
    assert address_problem(url) is None
    company_file({"records": {"type": "http", "url": url}})

    assert await _discover() == []
    assert network.answers(MADE_UP_NAME)[:2] == ["8.8.8.8", "127.0.0.1"]
    assert service.connections == 0


@pytest.mark.asyncio
async def test_a_connection_goes_to_the_address_that_was_checked(stubs, company_file, network, monkeypatch):
    """One lookup per request: the answer that passed the check is the address connected to."""
    service = stubs()
    _tenant_written(monkeypatch)
    network.names[MADE_UP_NAME] = ["8.8.8.8"]
    # The public address cannot be contacted from a test: a connect to it lands on the stub instead.
    network.routes[("8.8.8.8", service.port)] = ("127.0.0.1", service.port)
    company_file({"records": {"type": "http", "url": service.url("/mcp", host=MADE_UP_NAME)}})

    assert [tool.name for tool in await _discover()] == ["records_echo"]

    assert network.routed and set(network.routed) == {("8.8.8.8", service.port)}
    assert set(service.header_values("Host")) == {f"{MADE_UP_NAME}:{service.port}"}
    # Connecting by name instead would look the name up a second time.
    assert len(network.answers(MADE_UP_NAME)) == len(service.calls())


@pytest.mark.asyncio
async def test_a_listed_endpoint_is_matched_in_the_host_form_the_client_sends(stubs, company_file, network, monkeypatch):
    """A listed name outside ASCII is compared in its encoded form when saving and when connecting."""
    from deerflow.community.endpoint_policy import address_problem

    service = stubs()
    encoded = "xn--bcher-kva.example"
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, f"{encoded}:{service.port}")
    network.names[encoded] = ["127.0.0.1"]
    for typed in (encoded, "bücher.example", "BÜCHER.example"):
        assert address_problem(service.url("/mcp", host=typed)) is None, typed
    company_file({"records": {"type": "http", "url": service.url("/mcp", host=encoded)}, "ledger": {"type": "http", "url": service.url("/mcp", host="bücher.example")}})

    assert sorted(tool.name for tool in await _discover()) == ["ledger_echo", "records_echo"]


@pytest.mark.asyncio
async def test_a_marked_deployment_connection_keeps_the_public_only_client(stubs, company_file, monkeypatch):
    listed = stubs()
    _tenant_written(monkeypatch, listed)
    company_file({"records": {"type": "http", "url": listed.url("/mcp"), "personal_public_network": True}, "ledger": {"type": "http", "url": listed.url("/mcp")}})

    assert [tool.name for tool in await _discover()] == ["ledger_echo"]


@pytest.mark.asyncio
async def test_each_client_names_its_own_rule(monkeypatch):
    from deerflow.mcp.personal_network import personal_httpx_client_factory, tenant_written_httpx_client_factory

    _tenant_written(monkeypatch)
    async with personal_httpx_client_factory() as client:
        with pytest.raises(ValueError, match=r"^Personal MCP connections require a public HTTP\(S\) endpoint$"):
            await client.get("http://127.0.0.1:9/mcp")
    async with tenant_written_httpx_client_factory() as client:
        with pytest.raises(ValueError, match=r"^MCP connections require a public HTTP\(S\) endpoint$"):
            await client.get("http://127.0.0.1:9/mcp")


def test_remote_connections_are_not_handed_to_acp_agents(company_file, monkeypatch):
    """Remote connections are not handed to an ACP agent on a tenant-written deployment."""
    from deerflow.tools.builtins.invoke_acp_agent_tool import _build_acp_mcp_servers

    company_file(
        {
            "files": {"type": "stdio", "command": "npx", "args": ["example-server@1.0.0"]},
            "records": {"type": "http", "url": "https://8.8.8.8/mcp", "headers": {"X-Records-Key": "typed-value"}},
            "feed": {"type": "sse", "url": "https://8.8.8.8/sse"},
        }
    )

    assert [server["name"] for server in _build_acp_mcp_servers()] == ["files", "records", "feed"]
    monkeypatch.setenv(LINE, "on")
    assert _build_acp_mcp_servers() == [{"name": "files", "type": "stdio", "command": "npx", "args": ["example-server@1.0.0"], "env": []}]


def test_the_embedded_client_writer_follows_the_same_rules(company_file, tmp_path, monkeypatch):
    from deerflow.client import DeerFlowClient
    from deerflow.mcp.cache import reset_mcp_tools_cache

    monkeypatch.setenv(LINE, "on")
    stored = {"records": {"type": "http", "url": "https://8.8.8.8/mcp", "headers": {"X-Records-Key": "$HM_TEST_VALUE"}}}
    company_file(stored)
    path = tmp_path / "extensions_config.json"
    before = path.read_bytes()
    client = DeerFlowClient.__new__(DeerFlowClient)
    monkeypatch.setattr(client, "_customer_management_actor_allowed", lambda: True)

    with pytest.raises(ValueError, match=r"Values that begin with `\$` are not accepted"):
        client.update_mcp_config({"records": {**stored["records"], "url": "https://8.8.4.4/mcp"}})
    with pytest.raises(ValueError, match=r"must use a public HTTP\(S\) address"):
        client.update_mcp_config({**stored, "ledger": {"type": "http", "url": "http://127.0.0.1:9/mcp"}})
    assert path.read_bytes() == before

    try:
        # The stored definition as it is, beside one new connection, is accepted.
        saved = client.update_mcp_config({**stored, "ledger": {"type": "http", "url": "https://8.8.4.4/mcp"}})
    finally:
        reset_mcp_tools_cache()
    assert set(saved["mcp_servers"]) == {"records", "ledger"}
    assert json.loads(path.read_text(encoding="utf-8"))["mcpServers"]["records"] == stored["records"]

    # Several refused definitions: the refusal names the first by name.
    private = {"type": "http", "url": "http://127.0.0.1:9/mcp"}
    with pytest.raises(ValueError, match=r"^MCP server 'alpha': MCP connections on this deployment must use a public HTTP\(S\) address\.$"):
        client.update_mcp_config({name: private for name in ("mike", "zulu", "echo", "alpha", "kilo", "sierra", "delta", "bravo")})


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

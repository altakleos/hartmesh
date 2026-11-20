"""Selected company connections use the published definition and a finite discovery budget."""

import asyncio
import json

import httpx
import pytest
from langchain_core.tools import StructuredTool

from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.mcp import cache, tools


@pytest.fixture
def connections(monkeypatch, tmp_path):
    path = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(path))
    records = {"docs": {"enabled": True, "type": "http", "url": "https://docs.example/mcp", "headers": {"X-Sample": "fixture-value"}}, "calendar": {"enabled": True, "type": "sse", "url": "https://calendar.example/mcp"}}

    def save(**extra):
        path.write_text(json.dumps({"mcpServers": records, **extra}))
        return ExtensionsConfig.from_file().mcp_servers["docs"]

    save()
    cache.reset_mcp_tools_cache()
    yield records, save
    cache.reset_mcp_tools_cache()


@pytest.fixture
def adapter(monkeypatch):
    calls = []

    async def result(name):
        return [StructuredTool.from_function(lambda: "sample", name=name + "_sample", description="sample")]

    class Client:
        callbacks = None

        def __init__(self, *args, **kwargs):
            self.tool_interceptors = kwargs.get("tool_interceptors", [])

        async def get_tools(self, *, server_name):
            calls.append(server_name)
            return await behavior[0](server_name)

    behavior = [result]
    monkeypatch.setattr(tools, "_load_mcp_adapter_symbols", lambda: (Client, None))
    return calls, behavior


@pytest.mark.asyncio
@pytest.mark.parametrize("global_first", [True, False])
async def test_selected_discovery_shares_exact_tools_with_ordinary_chats(connections, adapter, global_first):
    _, save = connections
    calls, _ = adapter
    expected = save()
    if global_first:
        await cache.initialize_mcp_tools()
    result = await cache.get_company_server_tools("docs", expected_definition=expected)
    assert result.reason is None and result.definition == expected
    assert "fixture-value" not in repr(result)
    if not global_first:
        assert calls == ["docs"]
    ordinary = await cache.initialize_mcp_tools()
    assert result.tools[0] is next(tool for tool in ordinary if tool.name == "docs_sample")
    assert sorted(calls) == ["calendar", "docs"]


@pytest.mark.asyncio
async def test_selected_ignores_busy_sibling_and_fresh_distinguishes_zero(connections, adapter):
    _, save = connections
    calls, behavior = adapter
    expected = save()
    sibling_started = asyncio.Event()
    release_sibling = asyncio.Event()

    async def list_tools(name):
        if name == "calendar":
            sibling_started.set()
            await release_sibling.wait()
        return []

    behavior[0] = list_tools
    ordinary = asyncio.create_task(cache.initialize_mcp_tools())
    try:
        await asyncio.wait_for(sibling_started.wait(), 3)
        for fresh in (False, False, True):
            result = await asyncio.wait_for(cache.get_company_server_tools("docs", expected_definition=expected, fresh=fresh), 1)
            assert result.reason is None and result.tools == ()
        assert calls.count("docs") == 3
        assert calls.count("calendar") == 1
    finally:
        release_sibling.set()
        await ordinary


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["definition", "chain", "reset", "sibling"])
async def test_recheck_after_discovery(connections, adapter, change):
    records, save = connections
    _, behavior = adapter
    expected = save()

    async def list_tools(_):
        if change == "definition":
            records["docs"]["description"] = "New revision"
            save()
        elif change == "chain":
            save(mcpInterceptors=["test.fixture"])
        elif change == "sibling":
            records["calendar"]["description"] = "Updated"
            save()
        else:
            cache.reset_mcp_tools_cache()
        return []

    behavior[0] = list_tools
    result = await cache.get_company_server_tools("docs", expected_definition=expected)
    assert result.reason == (None if change == "sibling" else "changed")


@pytest.mark.asyncio
@pytest.mark.parametrize(("change", "reason"), [("disabled", "refused"), ("stdio", "refused"), ("missing", "changed"), ("modified", "changed"), ("header", "bad-token-format")])
async def test_invalid_selections_never_dial(connections, adapter, change, reason):
    records, save = connections
    expected = save()
    if change == "disabled":
        records["docs"]["enabled"] = False
        expected = save()
    elif change == "stdio":
        records["docs"] = {"type": "stdio", "enabled": True, "command": "fixture"}
        expected = save()
    elif change == "missing":
        del records["docs"]
        with pytest.raises(KeyError):
            save()
    elif change == "header":
        records["docs"]["headers"]["X-Sample"] = " leading"
        expected = save()
    else:
        records["docs"]["description"] = "Edited"
        save()
    result = await cache.get_company_server_tools("docs", expected_definition=expected)
    assert result.reason == reason and not result.tools
    assert adapter[0] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("configured_timeout", [None, 0.1])
async def test_entire_discovery_returns_by_deadline_despite_slow_cleanup(connections, adapter, monkeypatch, configured_timeout):
    records, save = connections
    records["docs"]["session_init_timeout"] = configured_timeout
    expected = save()
    monkeypatch.setattr(cache, "MAX_COMPANY_DISCOVERY_SECONDS", 2 if configured_timeout else 0.1)
    release = asyncio.Event()
    done = asyncio.Event()

    async def headers(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
            return {}
        finally:
            done.set()

    monkeypatch.setattr(tools, "get_initial_oauth_headers", headers)
    try:
        result = await asyncio.wait_for(cache.get_company_server_tools("docs", expected_definition=expected), 0.8)
        assert result.reason == "timeout"
    finally:
        release.set()
    await asyncio.wait_for(done.wait(), 1)
    await asyncio.sleep(0)
    assert "docs" not in cache._server_tool_cache and "docs" not in cache._company_pending


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "reason"), [("refusal", "refused"), ("network", "unreachable"), ("protocol", "not-mcp"), ("timeout", "timeout")])
async def test_diagnostic_reasons_leave_no_cached_failure(connections, adapter, mode, reason):
    _, save = connections
    _, behavior = adapter
    original = behavior[0]

    async def fail(_):
        response = httpx.Response(403, request=httpx.Request("GET", "https://docs.example"))
        error = {"refusal": httpx.HTTPStatusError("fixture-value", request=response.request, response=response), "network": httpx.ConnectError("fixture-value"), "protocol": ValueError("fixture-value"), "timeout": TimeoutError()}[mode]
        raise ExceptionGroup("server response", [error])

    behavior[0] = fail
    first = await cache.get_company_server_tools("docs", expected_definition=save())
    assert first.reason == reason and first.definition is None and "fixture-value" not in repr(first)
    behavior[0] = original
    second = await cache.get_company_server_tools("docs", expected_definition=save())
    assert second.reason is None


@pytest.mark.asyncio
async def test_normal_lazy_chat_entry_reuses_named_result_after_existing_reset(connections, adapter):
    _, save = connections
    expected = save()
    cache.publish_mcp_tools_cache_reset()
    selected = await cache.get_company_server_tools("docs", expected_definition=expected)
    ordinary = await asyncio.to_thread(cache.get_cached_mcp_tools)
    assert selected.tools[0] is next(tool for tool in ordinary if tool.name == "docs_sample")
    assert sorted(adapter[0]) == ["calendar", "docs"]


@pytest.mark.asyncio
async def test_fresh_tool_list_is_visible_in_ordinary_chat_cache(connections, adapter):
    _, save = connections
    _, behavior = adapter
    expected = save()
    first = await asyncio.to_thread(cache.get_cached_mcp_tools)

    async def fresh(_):
        return []

    behavior[0] = fresh
    selected = await cache.get_company_server_tools("docs", expected_definition=expected, fresh=True)
    assert selected.reason is None and selected.tools == ()
    second = await asyncio.to_thread(cache.get_cached_mcp_tools)
    assert [tool.name for tool in first] == ["docs_sample", "calendar_sample"]
    assert [tool.name for tool in second] == ["calendar_sample"]


@pytest.mark.asyncio
async def test_customer_endpoint_policy_precedes_any_credentials_or_discovery(connections, adapter, monkeypatch):
    records, save = connections
    records["docs"]["oauth"] = {"token_url": "https://tokens.example/oauth", "client_id": "sample", "client_secret": "fixture"}
    expected = save()
    checked = []

    def reject(url):
        checked.append(url)
        return "refused by fixture" if url.endswith("/oauth") else None

    from deerflow.community import endpoint_policy

    monkeypatch.setattr(endpoint_policy, "tenant_written", lambda: True)
    monkeypatch.setattr(endpoint_policy, "address_problem", reject)

    async def headers(*args, **kwargs):
        pytest.fail("A refused definition reached token acquisition")

    monkeypatch.setattr(tools, "get_initial_oauth_headers", headers)
    result = await cache.get_company_server_tools("docs", expected_definition=expected, fresh=True)
    assert result.reason == "address-not-public"
    assert checked == [expected.url, expected.oauth.token_url] and not adapter[0]


@pytest.mark.asyncio
async def test_retry_during_cancelled_cleanup_does_not_open_another_session(connections, adapter, monkeypatch):
    _, save = connections
    calls, behavior = adapter
    expected = save()
    release = asyncio.Event()
    entered = asyncio.Event()

    async def slow(name):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        return []

    behavior[0] = slow
    first = asyncio.create_task(cache.get_company_server_tools("docs", expected_definition=expected))
    await asyncio.wait_for(entered.wait(), 1)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    try:
        monkeypatch.setattr(cache, "MAX_COMPANY_DISCOVERY_SECONDS", 0.1)
        second = await cache.get_company_server_tools("docs", expected_definition=expected, fresh=True)
        assert second.reason == "timeout"
        assert calls == ["docs"]
    finally:
        release.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinary_first", [True, False])
async def test_identical_config_files_keep_selected_discovery_separate(connections, adapter, monkeypatch, tmp_path, ordinary_first):
    _, save = connections
    expected = save()
    if ordinary_first:
        first = next(tool for tool in await asyncio.to_thread(cache.get_cached_mcp_tools) if tool.name == "docs_sample")
    else:
        first = (await cache.get_company_server_tools("docs", expected_definition=expected)).tools[0]
    src = ExtensionsConfig.resolve_config_path()
    other = tmp_path / "second-company.json"
    other.write_bytes(src.read_bytes())
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(other))
    if ordinary_first:
        following = (await cache.get_company_server_tools("docs", expected_definition=expected)).tools[0]
    else:
        following = next(tool for tool in await asyncio.to_thread(cache.get_cached_mcp_tools) if tool.name == "docs_sample")
    assert first is not following
    assert adapter[0].count("docs") == 2


@pytest.mark.asyncio
async def test_plain_http_response_is_reported_on_the_original_initialization_request(connections, monkeypatch):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    requests = []

    class ExamplePage(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"])))["method"])
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Example page")

        def log_message(self, *args):
            pass

    records, save = connections
    with ThreadingHTTPServer(("127.0.0.1", 0), ExamplePage) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.03}, daemon=True)
        thread.start()
        try:
            records["docs"]["url"] = f"http://127.0.0.1:{server.server_port}/example"
            monkeypatch.setattr(cache, "MAX_COMPANY_DISCOVERY_SECONDS", 2)
            result = await cache.get_company_server_tools("docs", expected_definition=save(), fresh=True)
            assert result.reason == "not-mcp"
            assert requests == ["initialize"]
        finally:
            server.shutdown()
            thread.join(3)


@pytest.mark.asyncio
async def test_media_diagnostic_preserves_original_client_factory_after_discovery():
    from deerflow.mcp.discovery_errors import check_discovery_media

    calls = []

    def original(*args, **kwargs):
        calls.append(kwargs)
        return httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, text="Example page")))

    connection = {"transport": "http", "httpx_client_factory": original}
    with check_discovery_media({"docs": connection}):
        snapshot = connection.copy()
        async with snapshot["httpx_client_factory"](headers={"X-Test": "sample"}) as client:
            with pytest.raises(ValueError, match="not an MCP message"):
                await client.post("https://docs.example/", json={"id": 1, "method": "tools/list"})
    assert connection["httpx_client_factory"] is original
    async with snapshot["httpx_client_factory"]() as client:
        response = await client.post("https://docs.example/", json={"id": 1, "method": "tools/list"})
    assert response.text == "Example page" and calls == [{"headers": {"X-Test": "sample"}}, {}]


@pytest.mark.asyncio
async def test_retargeted_config_link_does_not_replace_previous_roots_ordinary_tools(connections, monkeypatch, tmp_path, adapter):
    _, save = connections
    expected = save()
    first_file = ExtensionsConfig.resolve_config_path()
    second_file = tmp_path / "another-company.json"
    second_file.write_bytes(first_file.read_bytes())
    link = tmp_path / "selected.json"
    link.symlink_to(first_file)
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(link))
    prior = await asyncio.to_thread(cache.get_cached_mcp_tools)
    link.unlink()
    link.symlink_to(second_file)
    result = await cache.get_company_server_tools("docs", expected_definition=expected, fresh=True)
    assert result.reason is None and result.tools[0] is not prior[0]
    assert cache._mcp_tools_cache[0] is prior[0]

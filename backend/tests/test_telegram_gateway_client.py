"""Real public SDK calls over socket-free HTTP, with owned client lifetime."""

import asyncio
import json
import threading

import httpx
import pytest

pytestmark = pytest.mark.asyncio


async def test_public_manager_exposes_independently_resolved_runtime_url(tmp_path):
    from app.channels.manager import ChannelManager
    from app.channels.message_bus import MessageBus
    from app.channels.store import ChannelStore

    manager = ChannelManager(MessageBus(), ChannelStore(str(tmp_path / "channels.json")), langgraph_url="https://runtime.example/root/api", gateway_url="https://gateway.example/other")
    assert manager.langgraph_url == "https://runtime.example/root/api"
    assert manager.gateway_url == "https://gateway.example/other"
    with pytest.raises(AttributeError):
        manager.langgraph_url = "https://different.example"


async def test_sdk_uses_runtime_prefix_and_fresh_owner_csrf_and_binding_headers():
    from app.gateway.internal_auth import create_internal_auth_headers
    from app.telegram_bots.gateway_client import GatewayClientOwner

    calls = []

    async def respond(request):
        calls.append(request)
        owner = request.headers["X-DeerFlow-Owner-User-Id"]
        return httpx.Response(200, json={"thread_id": request.url.path.rsplit("/", 1)[-1] if request.method == "GET" else json.loads(request.content)["thread_id"], "owner_user_id": owner, "agent_instance": None, "metadata": {}})

    client = GatewayClientOwner(gateway_url="https://gateway.example/health-root", langgraph_url="https://runtime.example/base/api", transport=httpx.MockTransport(respond))
    try:
        await client.start()
        a, b = await asyncio.gather(client.get_thread("owner-a", "thread-a"), client.get_thread("owner-b", "thread-b"))
        assert [a["owner_user_id"], b["owner_user_id"]] == ["owner-a", "owner-b"]
        await client.create_thread("owner-a", thread_id="candidate", metadata={"channel_agent_name": "lead_agent"})
        assert [request.url.path for request in calls] == ["/base/api/threads/thread-a", "/base/api/threads/thread-b", "/base/api/threads"]
        for request in calls:
            headers = request.headers
            assert headers["X-DeerFlow-Internal-Token"] == create_internal_auth_headers()["X-DeerFlow-Internal-Token"]
            assert headers["Cookie"] == "csrf_token=" + headers["X-CSRF-Token"]
            if request.method == "GET":
                assert headers["X-DeerFlow-Require-Thread-Binding"] == "true"
        first = client.owner_headers("owner-a")
        first["X-DeerFlow-Owner-User-Id"] = "forged"
        assert client.owner_headers("owner-b")["X-DeerFlow-Owner-User-Id"] == "owner-b"
    finally:
        await client.stop()


@pytest.mark.parametrize("url", ["https://user:pass@runtime.example/api", "https://runtime.example/api?secret=x", "https://runtime.example/api#fragment", "file:///tmp/runtime", "https:///missing"])
async def test_endpoint_identity_refuses_ambiguous_or_secret_configuration(url):
    from app.telegram_bots.gateway_client import GatewayClientOwner

    with pytest.raises(ValueError, match="invalid-thread-endpoint"):
        GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url=url)


async def test_endpoint_identity_is_canonical_and_does_not_depend_on_gateway():
    from app.telegram_bots.gateway_client import GatewayClientOwner

    first = GatewayClientOwner(gateway_url="https://gateway-a.example", langgraph_url="https://RUNTIME.example:443/base/api/")
    second = GatewayClientOwner(gateway_url="https://gateway-b.example", langgraph_url="https://runtime.example/base/api")
    different = GatewayClientOwner(gateway_url="https://gateway-b.example", langgraph_url="https://runtime.example/other/api")
    assert first.endpoint_id == second.endpoint_id
    assert first.endpoint_id != different.endpoint_id
    assert "runtime" not in first.endpoint_id


async def test_stop_and_repeated_cancellation_drain_sdk_json_decoder(monkeypatch):
    from langgraph_sdk._async import http as sdk_http

    from app.telegram_bots.gateway_client import GatewayClientOwner

    entered, release = threading.Event(), threading.Event()
    original = sdk_http.orjson.loads

    def decode(body):
        entered.set()
        release.wait(10)
        return original(body)

    monkeypatch.setattr(sdk_http.orjson, "loads", decode)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=b'{"thread_id":"saved","owner_user_id":"owner-a"}'))
    client = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=transport)
    await client.start()
    request = asyncio.create_task(client.get_thread("owner-a", "saved"))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        request.cancel()
        stop = asyncio.create_task(client.stop())
        await asyncio.sleep(0)
        stop.cancel()
        stop.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        assert not client.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await request
        with pytest.raises(asyncio.CancelledError):
            await stop
        await client.stop()
        assert client.closed
        with pytest.raises(RuntimeError, match="thread-client-stopping"):
            await client.get_thread("owner-a", "saved")
    finally:
        release.set()
        await asyncio.gather(request, return_exceptions=True)
        await client.stop()


async def test_cancelled_cold_constructor_keeps_and_closes_its_result(monkeypatch):
    from app.telegram_bots import gateway_client as module

    entered, release = threading.Event(), threading.Event()
    constructed = []
    original = module._construct

    def construct(*args):
        value = original(*args)
        constructed.append(value[0])
        entered.set()
        release.wait(10)
        return value

    monkeypatch.setattr(module, "_construct", construct)
    client = module.GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    start = asyncio.create_task(client.start())
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        start.cancel()
        await asyncio.sleep(0)
        assert not start.done() and not client.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await start
        assert client.closed and constructed[0].http.client.is_closed
    finally:
        release.set()
        await asyncio.gather(start, return_exceptions=True)
        await client.stop()

"""The file preflight reads the bound person's Gateway, not the runtime URL."""

import asyncio

import httpx
import pytest

from app.telegram_bots.gateway_client import GatewayClientOwner

pytestmark = pytest.mark.asyncio


async def test_file_features_uses_gateway_and_fresh_bound_human_headers():
    calls = []
    fences = []

    async def check():
        fences.append(len(calls))

    async def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"storage_spaces": {"state": "ready", "own": "ready", "reason": None}})

    owner = GatewayClientOwner(gateway_url="https://gateway.example/base", langgraph_url="https://runtime.example/runtime/api", transport=httpx.MockTransport(respond))
    try:
        one = await owner.get_features("owner-a", before_dispatch=check)
        two = await owner.get_features("owner-b", before_dispatch=check)
        assert one == two == {"storage_spaces": {"state": "ready", "own": "ready", "reason": None}}
        assert [str(call.url) for call in calls] == ["https://gateway.example/base/api/features"] * 2
        assert [call.headers["X-DeerFlow-Owner-User-Id"] for call in calls] == ["owner-a", "owner-b"]
        assert all(call.headers["Cookie"] == "csrf_token=" + call.headers["X-CSRF-Token"] for call in calls)
        assert fences == [0, 1, 1, 2]
        assert calls[0].headers["X-CSRF-Token"] != calls[1].headers["X-CSRF-Token"]
    finally:
        await owner.stop()


@pytest.mark.parametrize("status,body", [(503, {}), (302, {}), (200, []), (200, None)])
async def test_file_features_refuses_failed_redirected_or_nonobject_response(status, body):
    from app.channels.execution import ChannelExecutionProtocolError

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(lambda _: httpx.Response(status, json=body)))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await owner.get_features("owner", before_dispatch=lambda: asyncio.sleep(0))
    finally:
        await owner.stop()


async def test_file_features_retirement_during_response_refuses_result():
    from app.channels.execution import ChannelExecutionProtocolError

    retired = False

    async def check():
        if retired:
            raise ChannelExecutionProtocolError()

    async def respond(request):
        nonlocal retired
        retired = True
        return httpx.Response(200, json={"storage_spaces": {"own": "ready"}})

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await owner.get_features("owner", before_dispatch=check)
    finally:
        await owner.stop()

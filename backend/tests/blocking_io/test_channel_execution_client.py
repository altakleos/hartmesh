"""Cold construction and real JSON/SSE work under the strict loop detector."""

import asyncio
import threading

import httpx
import pytest
from test_channel_execution_client import creating, facade
from test_channel_http_owner import RecordingStream

from app.channels import execution_client
from app.telegram_bots import gateway_client

pytestmark = pytest.mark.asyncio


async def test_actual_cold_default_httpx_constructor_and_stop_remain_loop_safe():
    owner = gateway_client.GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api")
    try:
        await owner.start()
        assert not owner.closed
    finally:
        await owner.stop()
    assert owner.closed


async def test_snapshot_fresh_headers_json_and_public_sse_decoder_workers_stay_owned_off_loop(tmp_path, monkeypatch):
    from langgraph_sdk.sse import SSEDecoder

    path = tmp_path / "small-cold-fixture.json"
    await asyncio.to_thread(path.write_text, "{}", encoding="utf-8")
    loop, thread = asyncio.get_running_loop(), threading.get_ident()
    workers = []

    def wrap(actual):
        def call(*args, **kwargs):
            workers.append(threading.get_ident())
            path.read_text(encoding="utf-8")
            return actual(*args, **kwargs)

        return call

    monkeypatch.setattr(execution_client, "_snapshot", wrap(execution_client._snapshot))
    monkeypatch.setattr(gateway_client, "_authority_headers", wrap(gateway_client._authority_headers))
    monkeypatch.setattr(gateway_client, "_decode", wrap(gateway_client._decode))
    monkeypatch.setattr(SSEDecoder, "decode", wrap(SSEDecoder.decode))
    response = RecordingStream(block=False)
    calls = []

    async def respond(request):
        assert asyncio.get_running_loop() is loop
        calls.append(request)
        if request.url.path.endswith("/stream"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response)
        return httpx.Response(200, json={"messages": []})

    client, owner, _, _ = facade(respond)
    stream = None
    try:
        await client.runs.wait("thread", "lead_agent", **creating())
        stream = client.runs.stream("thread", "lead_agent", **creating())
        part = await anext(stream)
        assert part.event == "values"
        await stream.aclose()
        assert response.closed
        assert workers and all(identity != thread for identity in workers)
        assert len(calls) == 2
    finally:
        if stream is not None:
            await stream.aclose()
        await owner.stop()

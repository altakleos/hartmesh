"""The actual single HTTP owner, with socket-free transport and owned teardown."""

import asyncio
import threading

import httpx
import pytest

from app.channels.execution import ChannelExecutionProtocolError, ChannelHttpError
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.threads import ThreadCoordinator

pytestmark = pytest.mark.asyncio


def owner(transport):
    return GatewayClientOwner(gateway_url="https://gateway.example/other", langgraph_url="https://runtime.example/base/api", transport=transport)


async def allowed():
    return None


async def test_preparation_stop_does_not_close_its_borrowed_http_owner():
    client = owner(httpx.MockTransport(lambda request: httpx.Response(200, json={"thread_id": "saved"})))
    coordinator = ThreadCoordinator(links=None, authority=None, client=client, channel=lambda _: None, send_allowed=lambda: True, outbox=None, product_name="HartMesh")
    try:
        await client.start()
        await coordinator.stop()
        assert not client.closed
        assert await client.get_thread("human", "saved") == {"thread_id": "saved"}
    finally:
        await client.stop()


async def test_public_json_uses_original_prefix_fresh_authority_and_closed_response_headers():
    calls = []

    async def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"ok": True}, headers={"X-DeerFlow-Channel-Execution-Version": "1", "Set-Cookie": "private-cookie", "X-Provider-Secret": "private"})

    client = owner(httpx.MockTransport(respond))
    try:
        result = await client.request_json(operation="run_read", owner_user_id="human", method="GET", path="threads/saved/runs/run", params={"limit": 2}, headers={"X-DeerFlow-Require-Channel-Execution": "1"}, before_dispatch=allowed)
        assert result.status_code == 200 and result.body == {"ok": True}
        assert dict(result.headers) == {"content-type": "application/json", "x-deerflow-channel-execution-version": "1"}
        assert len(calls) == 1 and str(calls[0].url) == "https://runtime.example/base/api/threads/saved/runs/run?limit=2"
        assert calls[0].headers["X-DeerFlow-Owner-User-Id"] == "human"
        assert calls[0].headers["Cookie"] == "csrf_token=" + calls[0].headers["X-CSRF-Token"]
        assert calls[0].extensions["timeout"] == dict(connect=30, read=30, write=30, pool=30)
    finally:
        await client.stop()


@pytest.mark.parametrize("path", ["/threads/a", "https://foreign.example/x", "//foreign/x", "threads/../a", "threads/%2e%2e/a", "threads/%2fother", "threads/a?private=x", "threads/a#x", "threads/a\\b", "threads/a\n"])
async def test_paths_cannot_escape_the_original_endpoint(path):
    calls = []
    client = owner(httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(200, json={})))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.request_json(operation="run_read", owner_user_id="human", method="GET", path=path, before_dispatch=allowed)
        assert not calls
    finally:
        await client.stop()


@pytest.mark.parametrize("header", ["Cookie", "Host", "Authorization", "X-DeerFlow-Owner-User-Id", "X-DeerFlow-Internal-Token", "X-CSRF-Token"])
async def test_supplied_headers_never_override_authority(header):
    calls = []
    client = owner(httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(200, json={})))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.request_json(operation="run_read", owner_user_id="human", method="GET", path="threads/a/runs/r", headers={header: "forged"}, before_dispatch=allowed)
        assert not calls
    finally:
        await client.stop()


async def test_run_gate_rechecks_after_the_last_awaited_admission():
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    client = owner(httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(200, json={})))

    async def admission():
        entered.set()
        await release.wait()

    task = asyncio.create_task(client.request_json(operation="run_create", owner_user_id="human", method="POST", path="threads/a/runs/wait", body={"input": {}}, before_dispatch=admission))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        client.close_run_admission()
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await task
        assert not calls
        result = await client.request_json(operation="run_read", owner_user_id="human", method="GET", path="threads/a/runs/r", before_dispatch=allowed)
        assert result.body == {}
        assert len(calls) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.stop()


class RecordingStream(httpx.AsyncByteStream):
    def __init__(self, data=b'event: values\ndata: {"answer":"visible"}\n\n', *, block=True):
        self.data, self.block = data, block
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.closed, self.loop = False, None

    async def __aiter__(self):
        self.loop = asyncio.get_running_loop()
        yield self.data
        self.entered.set()
        if self.block:
            await self.release.wait()

    async def aclose(self):
        assert asyncio.get_running_loop() is self.loop or self.loop is None
        self.closed = True


async def test_stream_is_lazy_has_one_post_and_keeps_dispatched_read_after_run_gate_closes():
    body, calls = RecordingStream(), []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)

    client = owner(httpx.MockTransport(respond))
    stream = client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={"input": {"messages": []}}, before_dispatch=allowed)
    try:
        assert not calls
        part = await anext(stream)
        assert part.event == "values" and part.data == {"answer": "visible"}
        client.close_run_admission()
        assert len(calls) == 1 and not client.closed
        assert calls[0].extensions["timeout"] == dict(connect=5, pool=5, read=300, write=300)
        with pytest.raises(ChannelExecutionProtocolError):
            client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={}, before_dispatch=allowed)
        await stream.aclose()
        assert body.closed and len(calls) == 1
    finally:
        await stream.aclose()
        await client.stop()


async def test_final_stop_cancels_an_actual_network_read_without_waiting_for_response():
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(request):
        entered.set()
        await release.wait()
        return httpx.Response(200, json={})

    client = owner(httpx.MockTransport(respond))
    task = asyncio.create_task(client.request_json(operation="run_create", owner_user_id="human", method="POST", path="threads/a/runs/wait", body={}, before_dispatch=allowed))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.wait_for(client.stop(), 3)
        assert client.closed and not release.is_set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.stop()


async def test_cancelled_stream_drains_its_active_public_decoder_worker(monkeypatch):
    from langgraph_sdk.sse import SSEDecoder

    entered, release = threading.Event(), threading.Event()
    original = SSEDecoder.decode

    def decode(self, line):
        if not line:
            entered.set()
            release.wait(10)
        return original(self, line)

    monkeypatch.setattr(SSEDecoder, "decode", decode)
    response_body = RecordingStream()
    client = owner(httpx.MockTransport(lambda request: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response_body)))
    stream = client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={}, before_dispatch=allowed)
    reading = asyncio.create_task(anext(stream))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        reading.cancel()
        stopping = asyncio.create_task(client.stop())
        stopping.cancel()
        await asyncio.sleep(0)
        assert not reading.done() and not client.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await reading
        await asyncio.gather(stopping, return_exceptions=True)
        await client.stop()
        assert response_body.closed and client.closed
    finally:
        release.set()
        await asyncio.gather(reading, return_exceptions=True)
        await client.stop()


async def test_failed_stream_handshake_is_closed_and_retains_only_detached_error_response():
    client = owner(httpx.MockTransport(lambda request: httpx.Response(409, json={"detail": {"code": "thread_busy"}})))
    stream = client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={}, before_dispatch=allowed)
    try:
        with pytest.raises(ChannelHttpError) as error:
            await anext(stream)
        assert error.value.response.status_code == 409
        assert error.value.response.body == {"detail": {"code": "thread_busy"}}
        assert str(error.value) == "Channel execution HTTP request failed"
    finally:
        await stream.aclose()
        await client.stop()


async def test_owner_stop_wakes_a_consumer_waiting_for_its_first_stream_part():
    response = RecordingStream(data=b"")
    client = owner(httpx.MockTransport(lambda request: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response)))
    stream = client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={}, before_dispatch=allowed)
    reading = asyncio.create_task(anext(stream))
    try:
        await asyncio.wait_for(response.entered.wait(), 3)
        await client.stop()
        with pytest.raises(ChannelExecutionProtocolError):
            await asyncio.wait_for(reading, 1)
        assert response.closed
    finally:
        await stream.aclose()
        await asyncio.gather(reading, return_exceptions=True)
        await client.stop()


async def test_failed_response_close_retains_the_response_for_a_later_owned_stop():
    class FailingClose(RecordingStream):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def aclose(self):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("synthetic private close failure")
            await super().aclose()

    response = FailingClose()
    client = owner(httpx.MockTransport(lambda request: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response)))
    stream = client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={}, before_dispatch=allowed)
    try:
        await anext(stream)
        with pytest.raises(ChannelExecutionProtocolError):
            await stream.aclose()
        assert not response.closed and not client.closed
        await client.stop()
        assert response.closed and response.attempts >= 2
    finally:
        await stream.aclose()
        await client.stop()


@pytest.mark.parametrize("streaming", [False, True])
async def test_cancelled_cold_public_operation_cannot_join_cleanup_that_waits_for_itself(monkeypatch, streaming):
    from app.telegram_bots import gateway_client as module

    entered, release = threading.Event(), threading.Event()
    actual = module._construct

    def construct(*args):
        result = actual(*args)
        entered.set()
        release.wait(10)
        return result

    monkeypatch.setattr(module, "_construct", construct)
    calls = []
    client = owner(httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(200, json={})))
    stream = client.stream_sse(owner_user_id="human", path="threads/a/runs/stream", body={}, before_dispatch=allowed) if streaming else None
    request = asyncio.create_task(anext(stream) if streaming else client.request_json(operation="run_read", owner_user_id="human", method="GET", path="threads/a/runs/r", before_dispatch=allowed))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        request.cancel()
        release.set()
        done, _ = await asyncio.wait({request}, timeout=1)
        assert request in done, "A canceled constructor must not join cleanup waiting on its own operation"
        with pytest.raises(asyncio.CancelledError):
            await request
        assert not calls and client.closed
    finally:
        release.set()
        # Break only the earlier implementation's proved self-join cycle so
        # its red receipt does not strand a constructor/client at test teardown.
        if not request.done():
            for task in asyncio.all_tasks():
                if task.get_coro().__qualname__ == "GatewayClientOwner.stop":
                    task.cancel()
        await asyncio.gather(request, return_exceptions=True)
        if stream is not None:
            await stream.aclose()
        await client.stop()


async def test_buffered_read_close_failure_is_not_hidden_by_httpx_closed_flag():
    class FailingClose(RecordingStream):
        def __init__(self):
            super().__init__(data=b"{}", block=False)
            self.attempts = 0

        async def aclose(self):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("synthetic private buffered close failure")
            await super().aclose()

    response = FailingClose()
    client = owner(httpx.MockTransport(lambda request: httpx.Response(200, stream=response)))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.request_json(operation="run_read", owner_user_id="human", method="GET", path="threads/a/runs/r", before_dispatch=allowed)
        await client.stop()
        assert response.closed
    finally:
        await client.stop()

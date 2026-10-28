"""Actual owned HTTPX artifact reads over offline raw streams; no file authority doubles."""

import asyncio
import threading
from dataclasses import FrozenInstanceError

import httpx
import pytest

from app.channels.execution import ChannelExecutionProtocolError, ChannelHttpError
from app.telegram_bots.gateway_client import GatewayClientOwner

PATH = "/mnt/user-data/outputs/quarter report.csv"


class RawStream(httpx.AsyncByteStream):
    def __init__(self, *chunks, fail_close=False):
        self.chunks, self.fail_close = chunks, fail_close
        self.reads = self.closes = 0
        self.entered = self.release = None
        self.close_entered = self.close_release = None

    async def __aiter__(self):
        if self.entered is not None:
            self.entered.set()
            await self.release.wait()
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def aclose(self):
        self.closes += 1
        if self.close_entered is not None:
            self.close_entered.set()
            await self.close_release.wait()
        if self.fail_close and self.closes == 1:
            raise OSError("synthetic close failure")


async def current():
    return None


def client_for(respond):
    return GatewayClientOwner(gateway_url="https://gateway.example/prefix", langgraph_url="https://runtime.example/base/api", transport=httpx.MockTransport(respond))


@pytest.mark.asyncio
async def test_artifact_head_describes_without_body_and_uses_gateway_bound_owner():
    stream, requests = RawStream(b"must not be read"), []

    async def respond(request):
        requests.append(request)
        return httpx.Response(200, headers={"content-length": "8", "content-type": "text/csv; charset=utf-8"}, stream=stream)

    client = client_for(respond)
    try:
        metadata = await client.describe_artifact("actual-owner", "actual-thread", PATH, before_dispatch=current)
        assert (metadata.path, metadata.filename, metadata.size, metadata.mime_type) == (PATH, "quarter report.csv", 8, "text/csv")
        with pytest.raises(FrozenInstanceError):
            metadata.size = 9
        assert stream.reads == 0 and stream.closes == 1
        assert len(requests) == 1 and requests[0].method == "HEAD"
        request = requests[0]
        assert request.url.host == "gateway.example" and request.url.raw_path.startswith(b"/prefix/api/threads/actual-thread/artifacts/mnt/user-data/outputs/quarter%20report.csv")
        assert request.headers["X-DeerFlow-Owner-User-Id"] == "actual-owner"
        assert request.headers["Cookie"] == "csrf_token=" + request.headers["X-CSRF-Token"]
        assert request.headers["Accept-Encoding"] == "identity"
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_artifact_read_is_raw_capped_and_never_populates_httpx_whole_body():
    stream, responses, requests = RawStream(b"a,b\n", b"1,2\n"), [], []

    async def respond(request):
        requests.append(request)
        response = httpx.Response(200, headers={"content-length": "8"}, stream=stream)
        responses.append(response)
        return response

    client = client_for(respond)
    try:
        assert await client.read_artifact_capped("owner", "thread", PATH, ceiling=8, before_dispatch=current) == b"a,b\n1,2\n"
        assert stream.reads == 2 and stream.closes == 1
        assert not hasattr(responses[0], "_content")
        assert dict(requests[0].url.params) == {"download": "true", "stream": "true"}
        assert not client._responses
    finally:
        await client.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["declared_overrun", "body_overrun", "encoded", "redirect"])
async def test_capped_artifact_refuses_unsafe_transfer_without_error_body(case):
    stream = RawStream(b"12345", b"6789")
    headers = {"content-length": "9"} if case == "declared_overrun" else {"content-encoding": "gzip"} if case == "encoded" else {"location": "https://elsewhere.invalid/private"} if case == "redirect" else {}
    requests = []

    async def respond(request):
        requests.append(request)
        return httpx.Response(302 if case == "redirect" else 200, headers=headers, stream=stream)

    client = client_for(respond)
    try:
        with pytest.raises(ChannelHttpError if case == "redirect" else ChannelExecutionProtocolError) as error:
            await client.read_artifact_capped("owner", "thread", PATH, ceiling=8, before_dispatch=current)
        if case == "redirect":
            assert error.value.response.status_code == 302 and error.value.response.body is None
        assert len(requests) == 1 and stream.closes == 1
        assert stream.reads == (2 if case == "body_overrun" else 0)
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_failed_artifact_close_retains_only_its_batch_signal_until_real_shutdown_close():
    stream, signals = RawStream(b"data", fail_close=True), []
    client = client_for(lambda request: httpx.Response(200, headers={"content-length": "4"}, stream=stream))

    async def closed():
        signals.append(True)

    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=current, on_failed_close=closed)
        assert stream.closes == 1 and signals == [] and client.retains_failed_close(closed)
        await client.stop()
        assert stream.closes == 2 and signals == [True] and not client.retains_failed_close(closed)
        assert client.closed
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_cancelled_read_drains_blocked_close_before_returning():
    stream = RawStream(b"data")
    stream.entered, stream.release = asyncio.Event(), asyncio.Event()
    stream.close_entered, stream.close_release = asyncio.Event(), asyncio.Event()
    client = client_for(lambda request: httpx.Response(200, stream=stream))
    task = asyncio.create_task(client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=current))
    try:
        await asyncio.wait_for(stream.entered.wait(), 15)
        task.cancel()
        await asyncio.wait_for(stream.close_entered.wait(), 15)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and stream.closes == 1
        stream.close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not client._responses
    finally:
        stream.release.set()
        stream.close_release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [False, True])
async def test_original_source_retirement_after_http_wait_refuses_handoff(metadata):
    class SourceRetired(RuntimeError):
        pass

    alive = True

    async def source_current():
        if not alive:
            raise SourceRetired("original source retired")

    stream = RawStream(b"data")
    stream.close_entered, stream.close_release = asyncio.Event(), asyncio.Event()
    client = client_for(lambda request: httpx.Response(200, headers={"content-length": "4"}, stream=stream))
    operation = client.describe_artifact("owner", "thread", PATH, before_dispatch=source_current) if metadata else client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=source_current)
    task = asyncio.create_task(operation)
    try:
        await asyncio.wait_for(stream.close_entered.wait(), 15)
        alive = False
        stream.close_release.set()
        with pytest.raises(SourceRetired):
            await task
        assert stream.closes == 1 and not client._responses
        assert stream.reads == (0 if metadata else 1)
    finally:
        stream.close_release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["https://foreign.invalid/private", "/mnt/user-data/uploads/private", "/mnt/user-data/outputs/../private", "/mnt/user-data/outputs//private", "/mnt/user-data/outputs/bad\x00name"])
async def test_artifact_reference_refuses_before_any_http_or_client_construction(path):
    requests = []
    client = client_for(lambda request: requests.append(request) or httpx.Response(200))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.describe_artifact("owner", "thread", path, before_dispatch=current)
        assert requests == [] and client._construction is None
    finally:
        await client.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [None, "-1", "true", "4, 4", "99999999999999999999"])
async def test_head_requires_literal_bounded_size_without_reading_a_body(length):
    stream = RawStream(b"body must not be read")
    client = client_for(lambda request: httpx.Response(200, headers={} if length is None else {"content-length": length}, stream=stream))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.describe_artifact("owner", "thread", PATH, before_dispatch=current)
        assert stream.reads == 0 and stream.closes == 1
    finally:
        await client.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [False, True])
async def test_retired_source_starts_no_client_or_http_leaf(metadata):
    class SourceRetired(RuntimeError):
        pass

    async def retired():
        raise SourceRetired("source already retired")

    requests = []
    client = client_for(lambda request: requests.append(request) or httpx.Response(200))
    try:
        with pytest.raises(SourceRetired):
            if metadata:
                await client.describe_artifact("owner", "thread", PATH, before_dispatch=retired)
            else:
                await client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=retired)
        assert requests == [] and client._construction is None
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_source_retirement_during_header_preparation_prevents_dispatch(monkeypatch):
    from app.telegram_bots import gateway_client as module

    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    actual_headers = module._authority_headers
    alive, requests = True, []

    def headers(owner):
        result = actual_headers(owner)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(15)
        return result

    async def source_current():
        if not alive:
            raise ValueError("source retired")

    monkeypatch.setattr(module, "_authority_headers", headers)
    client = client_for(lambda request: requests.append(request) or httpx.Response(200))
    task = asyncio.create_task(client.describe_artifact("owner", "thread", PATH, before_dispatch=source_current))
    try:
        await asyncio.wait_for(entered.wait(), 15)
        alive = False
        release.set()
        with pytest.raises(ValueError, match="source retired"):
            await task
        assert requests == []
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.stop()


@pytest.mark.asyncio
async def test_converted_read_error_drops_buffer_aliases_in_retained_exception_chain():
    finalized, retained = [], []

    class ChargedBytes(bytes):
        def __del__(self):
            finalized.append(True)

    def inner_read():
        charged_data = ChargedBytes(b"data")
        assert len(charged_data) == 4
        raise OSError(5, "synthetic native read error")

    def outer_read():
        try:
            inner_read()
        except OSError as error:
            raise RuntimeError("synthetic chained transport error") from error

    class FailedStream(RawStream):
        async def __aiter__(self):
            if False:
                yield b""
            try:
                outer_read()
            except Exception as error:
                retained.append(error)
                raise

    stream = FailedStream()
    client = client_for(lambda request: httpx.Response(200, stream=stream))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=current)
        assert finalized == [True], "A retained underlying exception chain must not outlive the charged read's buffer ownership"
        assert retained[0].__cause__.errno == 5 and stream.closes == 1
    finally:
        retained.clear()
        await client.stop()


@pytest.mark.asyncio
async def test_actual_failed_http_close_keeps_whole_batch_until_owner_and_transport_drain():
    from app.telegram_bots.files import FileBytesBudget, OutputBatch

    budget = FileBytesBudget(8)
    batch = OutputBatch(await budget.reserve_batch([4, 4], timeout=1))
    streams = [RawStream(b"one!"), RawStream(b"two!", fail_close=True)]
    index = 0

    async def respond(request):
        nonlocal index
        stream = streams[index]
        index += 1
        return httpx.Response(200, headers={"content-length": "4"}, stream=stream)

    client = client_for(respond)
    waiter = None
    first_data = None
    first_closed, second_closed = batch.begin_transport(0), batch.begin_transport(1)
    try:
        first_data = await client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=current, on_failed_close=first_closed)
        if not client.retains_failed_close(first_closed):
            await first_closed()
        await batch.keep(0, first_data)
        with pytest.raises(ChannelExecutionProtocolError):
            await client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=current, on_failed_close=second_closed)
        assert client.retains_failed_close(second_closed) and budget.used == 8
        first_data = None
        await asyncio.wait_for(batch.aclose(), 15)
        assert budget.used == 8 and batch.buffers == (None, None)

        async def competing_batch():
            reservation = await budget.reserve_batch([8], timeout=15)
            assert batch.buffers == (None, None)
            await reservation.aclose()

        waiter = asyncio.create_task(competing_batch())
        await asyncio.sleep(0)
        assert not waiter.done()
        await client.stop()
        await waiter
        assert budget.used == 0 and streams[1].closes == 2
    finally:
        first_data = None
        await batch.aclose()
        await client.stop()
        if waiter is not None:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [False, True])
async def test_original_source_is_rechecked_after_owned_task_completion_before_public_handoff(monkeypatch, metadata):
    class SourceRetired(RuntimeError):
        pass

    alive = True

    async def source_current():
        if not alive:
            raise SourceRetired("source retired before public handoff")

    stream = RawStream(b"data")
    client = client_for(lambda request: httpx.Response(200, headers={"content-length": "4"}, stream=stream))
    actual_register = client._register

    def register(coroutine, name):
        nonlocal alive
        task = actual_register(coroutine, name)

        def retired_after_completion(done):
            nonlocal alive
            alive = False

        task.add_done_callback(retired_after_completion)
        return task

    monkeypatch.setattr(client, "_register", register)
    try:
        with pytest.raises(SourceRetired):
            if metadata:
                await client.describe_artifact("owner", "thread", PATH, before_dispatch=source_current)
            else:
                await client.read_artifact_capped("owner", "thread", PATH, ceiling=4, before_dispatch=source_current)
        assert stream.closes == 1
    finally:
        await client.stop()

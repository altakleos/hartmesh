"""Actual pinned request/client with controlled raw byte-stream ownership."""

import asyncio
import importlib
from unittest.mock import AsyncMock

import httpx
import pytest
from telegram.ext import ExtBot


def api():
    return importlib.import_module("app.telegram_bots.capped_download")


class Body(httpx.AsyncByteStream):
    def __init__(self, chunks=(), *, pause_read=False, pause_close=False, failures=0):
        self.chunks, self.seen, self.closes = chunks, 0, 0
        self.reading, self.closing = asyncio.Event(), asyncio.Event()
        self.read_release, self.close_release = asyncio.Event(), asyncio.Event()
        if not pause_read:
            self.read_release.set()
        if not pause_close:
            self.close_release.set()
        self.failures, self.closed = failures, False

    async def __aiter__(self):
        self.reading.set()
        await self.read_release.wait()
        for chunk in self.chunks:
            self.seen += 1
            yield chunk

    async def aclose(self):
        self.closes += 1
        self.closing.set()
        await self.close_release.wait()
        if self.failures:
            self.failures -= 1
            raise RuntimeError("sensitive-close-text")
        self.closed = True


@pytest.mark.asyncio
async def test_real_getfile_and_capped_body_share_normal_client_only():
    module = api()
    stream, calls = Body((b"ab", b"cd")), []

    async def handle(request):
        assert request.extensions["timeout"]["read"] == 5
        if request.url.path.endswith("/getFile"):
            calls.append("getFile")
            return httpx.Response(200, json={"ok": True, "result": {"file_id": "file", "file_unique_id": "unique", "file_path": "documents/report.txt", "file_size": 4}})
        calls.append("download")
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(200, stream=stream)

    normal = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(handle)})
    polling = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda request: pytest.fail("Polling client used for download"))})
    token = str(9000000001) + ":" + "a" * 35
    bot = ExtBot(token, request=normal, get_updates_request=polling)
    release, check = AsyncMock(), AsyncMock()
    client = normal._client
    try:
        actual = await bot.get_file("file")
        result = await normal.read_file_capped(actual.file_path, ceiling=4, max_bytes=8, before_dispatch=check, on_failed_close=release)
        assert result == b"abcd" and normal._client is client and calls == ["getFile", "download"]
        assert stream.closed and stream.closes == 1 and not normal.retains_failed_close(release)
        check.assert_awaited_once()
        release.assert_not_awaited()
    finally:
        await normal.shutdown()
        await polling.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("ceiling,maximum,reason", [(3, 8, "overrun"), (4, 4, "too_large")])
async def test_raw_body_cap_stops_before_later_chunks(ceiling, maximum, reason):
    module = api()
    stream = Body((b"ab", b"cde", b"unread"))
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))})
    try:
        with pytest.raises(module.CappedDownloadError) as failure:
            await request.read_file_capped("https://files.test/object", ceiling=ceiling, max_bytes=maximum, before_dispatch=AsyncMock())
        assert failure.value.reason == reason and stream.seen == 2 and stream.closed
    finally:
        await request.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,headers,reason",
    [
        (302, {"location": "https://other.test/secret"}, "status"),
        (404, {}, "status"),
        (401, {}, "status"),
        (200, {"content-encoding": "gzip"}, "not_saved"),
        (200, {"content-length": "9"}, "too_large"),
        (200, {"content-length": "6"}, "overrun"),
    ],
)
async def test_headers_refuse_without_reading_body_or_following_redirect(status, headers, reason):
    module = api()
    stream, calls = Body((b"unread",)), []

    def handle(incoming):
        calls.append(True)
        return httpx.Response(status, headers=headers, stream=stream)

    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(handle)})
    try:
        with pytest.raises(module.CappedDownloadError) as failure:
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=8, before_dispatch=AsyncMock())
        assert failure.value.reason == reason and failure.value.status == (status if reason == "status" else None)
        assert stream.seen == 0 and stream.closed and calls == [True]
    finally:
        await request.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_close", [False, True])
async def test_repeated_cancellation_drains_close_and_preserves_failed_custody(fail_close):
    module = api()
    stream = Body((b"not-returned",), pause_read=True, pause_close=True, failures=int(fail_close))
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))})
    release = AsyncMock()
    task = asyncio.create_task(request.read_file_capped("https://files.test/object", ceiling=16, max_bytes=16, before_dispatch=AsyncMock(), on_failed_close=release))
    try:
        await asyncio.wait_for(stream.reading.wait(), 2)
        task.cancel()
        await asyncio.wait_for(stream.closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.assert_not_awaited()
        stream.close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert request.retains_failed_close(release) is fail_close
        await request.shutdown()
        assert stream.closed and not request.retains_failed_close(release)
        assert release.await_count == int(fail_close)
    finally:
        stream.read_release.set()
        stream.close_release.set()
        await asyncio.gather(task, return_exceptions=True)
        await request.shutdown()


@pytest.mark.asyncio
async def test_failed_response_close_retries_actual_stream_before_releasing_capacity():
    module = api()
    stream = Body((b"data",), failures=2)
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))})
    release = AsyncMock()
    try:
        with pytest.raises(module.CappedDownloadError) as failure:
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=4, before_dispatch=AsyncMock(), on_failed_close=release)
        assert str(failure.value) == "capped-download-failed" and "sensitive" not in repr(failure.value)
        assert request.retains_failed_close(release) and not request._client.is_closed
        release.assert_not_awaited()
        with pytest.raises(module.CappedDownloadError):
            await request.shutdown()
        assert request.retains_failed_close(release) and stream.closes == 2
        release.assert_not_awaited()
        await request.shutdown()
        assert stream.closed and stream.closes == 3 and request._client.is_closed
        assert not request.retains_failed_close(release)
        release.assert_awaited_once()
    finally:
        await request.shutdown()


@pytest.mark.asyncio
async def test_failed_cleanup_release_is_owned_and_not_repeated_after_success():
    module = api()
    stream = Body((b"data",), failures=1)
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))})
    release = AsyncMock(side_effect=[RuntimeError("release failed"), None])
    try:
        with pytest.raises(module.CappedDownloadError):
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=4, before_dispatch=AsyncMock(), on_failed_close=release)
        with pytest.raises(module.CappedDownloadError):
            await request.shutdown()
        assert request.retains_failed_close(release) and stream.closed and stream.closes == 2
        await request.shutdown()
        await request.shutdown()
        assert not request.retains_failed_close(release) and stream.closes == 2 and release.await_count == 2
    finally:
        await request.shutdown()


@pytest.mark.asyncio
async def test_shutdown_repeated_cancellation_drains_actual_close_then_capacity_release():
    module = api()
    stream = Body((b"data",), failures=1)
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))})
    release = AsyncMock()
    with pytest.raises(module.CappedDownloadError):
        await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=4, before_dispatch=AsyncMock(), on_failed_close=release)
    stream.closing.clear()
    stream.close_release.clear()
    task = asyncio.create_task(request.shutdown())
    try:
        await asyncio.wait_for(stream.closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and request.retains_failed_close(release)
        release.assert_not_awaited()
        stream.close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed and request._client.is_closed and not request.retains_failed_close(release)
        release.assert_awaited_once()
    finally:
        stream.close_release.set()
        await asyncio.gather(task, return_exceptions=True)
        await request.shutdown()


@pytest.mark.asyncio
async def test_retired_before_dispatch_has_no_http_effect_and_stopped_owner_cannot_read():
    module = api()
    dispatches = []

    def handle(_):
        dispatches.append(True)
        return httpx.Response(200, content=b"forbidden")

    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(handle)})
    try:
        with pytest.raises(module.CappedDownloadError):
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=4, before_dispatch=AsyncMock(side_effect=ValueError("retired")))
        await request.shutdown()
        with pytest.raises(module.CappedDownloadError):
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=4, before_dispatch=AsyncMock())
        assert dispatches == []
    finally:
        await request.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("length,reads", [("2", 1), ("-1", 0), ("invalid", 0), ("1" * 21, 0)])
async def test_inconsistent_or_unbounded_length_is_a_closed_failure(length, reads):
    module = api()
    stream = Body((b"abcd",))
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, headers={"content-length": length}, stream=stream))})
    try:
        with pytest.raises(module.CappedDownloadError) as failure:
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=8, before_dispatch=AsyncMock())
        assert failure.value.reason == "not_saved" and stream.seen == reads and stream.closed
    finally:
        await request.shutdown()


@pytest.mark.asyncio
async def test_closed_diagnostic_retains_no_http_exception_or_download_buffer_alias():
    module = api()

    class BrokenBody(Body):
        async def __aiter__(self):
            yield b"ab"
            raise httpx.ReadError("sensitive-remote-url")

    stream = BrokenBody()
    request = module.ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))})
    try:
        with pytest.raises(module.CappedDownloadError) as failure:
            await request.read_file_capped("https://files.test/object", ceiling=4, max_bytes=8, before_dispatch=AsyncMock())
        assert failure.value.reason == "not_saved" and failure.value.__context__ is None
        frames = []
        trace = failure.value.__traceback__
        while trace is not None:
            if trace.tb_frame.f_code.co_name == "read_file_capped":
                frames.append(trace.tb_frame.f_locals)
            trace = trace.tb_next
        assert len(frames) == 1
        assert all(frames[0][key] is None for key in ("chunk", "response", "request"))
        assert frames[0]["buffer"].closed and stream.closed
    finally:
        await request.shutdown()

"""Real SDK/request/stream, adapter and budget; file custody is a protocol double.

The full worker/publication path is qualified separately. No URL/token or body
is retained in diagnostics, and every temporary transport is socket-free.
"""

import asyncio

import httpx
import pytest
from _telegram_repository_test_support import db as db
from test_telegram_capped_download import Body
from test_telegram_inbound_reader import fixture as build_reader

from app.telegram_bots.capped_download import ManagedHTTPXRequest


def normal_request(stream, *, file_status=200, verification_status=200):
    calls = []

    def handle(request):
        endpoint = request.url.path.rsplit("/", 1)[-1]
        if endpoint == "getMe":
            calls.append("getMe")
            if calls.count("getMe") > 1 and verification_status != 200:
                return httpx.Response(verification_status, json={"ok": False, "error_code": verification_status, "description": "Not Found"})
            # The synthetic bot ID stays in memory; never record its token URL.
            bot_id = int(request.url.path.split("/", 2)[1][3:].split(":", 1)[0])
            return httpx.Response(200, json={"ok": True, "result": {"id": bot_id, "is_bot": True, "first_name": "Offline", "username": "offline_test_bot"}})
        if endpoint == "getFile":
            calls.append("getFile")
            return httpx.Response(200, json={"ok": True, "result": {"file_id": "input-id", "file_unique_id": "stable-id", "file_size": 3, "file_path": "documents/sample.csv"}})
        calls.append("download")
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(file_status, stream=stream)

    return ManagedHTTPXRequest(httpx_kwargs={"transport": httpx.MockTransport(handle)}), calls


@pytest.mark.asyncio
async def test_actual_reader_success_retains_budget_until_transferred_lease_close(db, monkeypatch):
    stream = Body((b"a", b"bc"))
    request, calls = normal_request(stream)
    e = await build_reader(db, monkeypatch, normal_request=request)
    try:
        lease = await e.channel.read_inbound_file(e.file, execution_context=e.cap)
        assert lease.data == b"abc" and stream.closed and stream.closes == 1
        assert e.services.file_bytes_budget.used == 3 and e.services.turns.file_notice.await_count == 0
        assert e.channel._requests[0] is request and calls == ["getMe", "getFile", "download"]
        assert e.updates.calls == []
        await lease.aclose()
        assert e.services.file_bytes_budget.used == 0
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_actual_failed_stream_close_keeps_budget_until_request_shutdown(db, monkeypatch):
    stream = Body((b"abc",), failures=1)
    request, calls = normal_request(stream)
    e = await build_reader(db, monkeypatch, normal_request=request)
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert e.services.turns.file_notice.await_args.kwargs["key"] == "file.not_saved"
        assert e.services.file_bytes_budget.used == 3 and stream.closes == 1 and not stream.closed
        assert calls == ["getMe", "getFile", "download"] and not request._client.is_closed
        await request.shutdown()
        assert stream.closed and stream.closes == 2 and request._client.is_closed
        assert e.services.file_bytes_budget.used == 0
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_actual_adapter_stop_and_repeated_reader_cancel_drain_close_before_release(db, monkeypatch):
    stream = Body((b"abc",), pause_read=True, pause_close=True)
    request, calls = normal_request(stream)
    e = await build_reader(db, monkeypatch, normal_request=request)
    read = asyncio.create_task(e.channel.read_inbound_file(e.file, execution_context=e.cap))
    stop = None
    try:
        await asyncio.wait_for(stream.reading.wait(), 15)
        stop = asyncio.create_task(e.channel.stop())
        await asyncio.wait_for(stream.closing.wait(), 15)
        read.cancel()
        await asyncio.sleep(0)
        read.cancel()
        await asyncio.sleep(0)
        assert not stop.done() and not read.done()
        assert e.services.file_bytes_budget.used == 3 and not request._client.is_closed and not e.updates.closed
        stream.close_release.set()
        await asyncio.wait_for(stop, 15)
        with pytest.raises(asyncio.CancelledError):
            await read
        assert stream.closed and stream.closes == 1 and request._client.is_closed and e.updates.closed
        assert e.services.file_bytes_budget.used == 0 and not e.channel._file_tasks
        assert calls == ["getMe", "getFile", "download"]
    finally:
        stream.read_release.set()
        stream.close_release.set()
        read.cancel()
        await asyncio.gather(read, return_exceptions=True)
        if stop is not None:
            await asyncio.gather(stop, return_exceptions=True)
        await e.channel.stop()


@pytest.mark.asyncio
async def test_actual_file_404_then_getme_404_preserves_token_and_reports_file_failure(db, monkeypatch):
    stream = Body((b"not-read",))
    request, calls = normal_request(stream, file_status=404, verification_status=404)
    e = await build_reader(db, monkeypatch, normal_request=request)
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert calls == ["getMe", "getFile", "download", "getMe"]
        assert stream.seen == 0 and stream.closed and e.channel._first_401 is None
        assert (await db.links.get(e.link.id)).state == "connected"
        assert e.services.turns.file_notice.await_args.kwargs["key"] == "file.not_saved"
        assert e.services.file_bytes_budget.used == 0
    finally:
        await e.channel.stop()

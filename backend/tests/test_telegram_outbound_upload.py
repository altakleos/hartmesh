"""Pinned SDK multipart behavior; these protocol tests grant no file authority."""

import asyncio

import pytest
from _telegram_bot_test_support import RecordingRequest, offline_bot

from app.telegram_bots.adapter import BoundedRateLimiter

pytestmark = pytest.mark.asyncio


class MultipartRequest(RecordingRequest):
    def __init__(self):
        super().__init__()
        self.uploads = []
        self.handles = []

    async def do_request(self, url, method, request_data=None, **kwargs):
        if request_data and request_data.multipart_data:
            handles = tuple(request_data.multipart_data.values())
            self.handles.extend(value[1] for value in handles)
            captured = []
            for name, handle, _mime in handles:
                handle.seek(0)
                captured.append((name, handle.read()))
                handle.seek(0)
            self.uploads.append(tuple(captured))
        return await super().do_request(url, method, request_data, **kwargs)


def message(identity):
    return {"message_id": identity, "date": 1, "chat": {"id": 7000000001, "type": "private"}}


@pytest.mark.parametrize("kind,count,endpoint", [("document", 1, "sendDocument"), ("photo", 1, "sendPhoto"), ("album", 2, "sendMediaGroup"), ("album", 10, "sendMediaGroup")])
async def test_actual_sdk_uploads_only_supplied_bytes_and_inherits_delivery_fields(kind, count, endpoint):
    from app.telegram_bots.outbound_upload import upload_output

    request = MultipartRequest()
    request.enqueue_get_me()
    request.enqueue(endpoint, result=[message(i + 10) for i in range(count)] if kind == "album" else message(10))
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    fences = []

    async def current():
        fences.append(True)

    buffers = tuple((f"file-{i}.png", bytes([i, 12, 34])) for i in range(count))
    async with bot:
        result = await upload_output(
            bot, kind=kind, files=buffers, chat_id=7000000001, message_thread_id=23, reply_to_message_id=5, silent=True, protect_content=True, caption="<b>Answer</b>" if kind == "document" else None, plain=False, before_dispatch=current
        )
    assert result.message_ids == tuple(range(10, 10 + count)) and result.failure is None
    call = request.calls[-1]
    assert call.endpoint == endpoint
    assert call.parameters["chat_id"] == 7000000001 and call.parameters["message_thread_id"] == 23
    assert call.parameters["reply_parameters"]["message_id"] == 5
    assert call.parameters["disable_notification"] is True and call.parameters["protect_content"] is True
    assert request.uploads == [buffers]
    assert all(handle.closed and handle._data is None for handle in request.handles)
    assert len(fences) == 1
    if kind == "document":
        assert call.parameters["caption"] == "<b>Answer</b>" and call.parameters["parse_mode"] == "HTML"


@pytest.mark.parametrize("kind,files", [("document", (("x", "https://example.invalid/a"),)), ("photo", (("x", "file-id"),)), ("album", (("x", b"a"),)), ("album", tuple(("x", b"a") for _ in range(11))), ("document", (("../secret", b"a"),))])
async def test_rejects_nonbytes_bad_counts_and_path_filenames_before_sdk(kind, files):
    from app.telegram_bots.outbound_upload import upload_output

    request = MultipartRequest()
    bot = offline_bot(request)
    with pytest.raises(ValueError, match="invalid-output-upload"):
        await upload_output(bot, kind=kind, files=files, chat_id=7000000001, message_thread_id=None, reply_to_message_id=None, silent=False, protect_content=False, caption=None, plain=False, before_dispatch=lambda: asyncio.sleep(0))
    assert not request.calls


@pytest.mark.parametrize("description,expected", [("Bad Request: can't parse entities", "entities"), ("Bad Request: message thread not found", "topic"), ("Bad Request: wrong file identifier", "payload")])
async def test_sdk_failure_returns_closed_disposition_without_raw_exception(description, expected):
    from app.telegram_bots.outbound_upload import upload_output

    request = MultipartRequest()
    request.enqueue_get_me()
    request.enqueue("sendPhoto", status=400, body={"ok": False, "error_code": 400, "description": description})
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    async with bot:
        outcome = await upload_output(
            bot, kind="photo", files=(("x.png", b"abc"),), chat_id=7000000001, message_thread_id=None, reply_to_message_id=None, silent=False, protect_content=False, caption=None, plain=False, before_dispatch=lambda: asyncio.sleep(0)
        )
    assert outcome.failure == expected and outcome.message_ids == ()
    assert not any(isinstance(value, BaseException) for value in vars(outcome).values())


async def test_limiter_rechecks_original_fence_and_long_retry_does_not_loop():
    from app.telegram_bots.outbound_upload import upload_output

    request = MultipartRequest()
    request.enqueue_get_me()
    request.enqueue("sendDocument", status=429, body={"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 61}})
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    checked = []

    async def current():
        checked.append(True)

    async with bot:
        result = await upload_output(
            bot, kind="document", files=(("x.txt", b"abc"),), chat_id=7000000001, message_thread_id=None, reply_to_message_id=None, silent=False, protect_content=False, caption=None, plain=False, before_dispatch=current
        )
    assert result.failure == "retry" and result.retry_after == 61
    assert [call.endpoint for call in request.calls] == ["getMe", "sendDocument"] and len(checked) == 1


async def test_retired_original_fence_prevents_any_raw_upload():
    from app.telegram_bots.outbound_upload import upload_output
    from app.telegram_bots.producer import DeliveryClaimRetired

    request = MultipartRequest()
    request.enqueue_get_me()
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())

    async def retired():
        raise DeliveryClaimRetired()

    async with bot:
        with pytest.raises(DeliveryClaimRetired):
            await upload_output(bot, kind="document", files=(("x.txt", b"abc"),), chat_id=7000000001, message_thread_id=None, reply_to_message_id=None, silent=False, protect_content=False, caption=None, plain=False, before_dispatch=retired)
    assert [call.endpoint for call in request.calls] == ["getMe"] and not request.uploads


async def test_cancellation_drains_actual_sdk_request_before_returning():
    from app.telegram_bots.outbound_upload import upload_output

    request = MultipartRequest()
    request.enqueue_get_me()
    reply = request.enqueue("sendDocument", result=message(10), release=asyncio.Event())
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    async with bot:
        task = asyncio.create_task(
            upload_output(
                bot, kind="document", files=(("x.txt", b"abc"),), chat_id=7000000001, message_thread_id=None, reply_to_message_id=None, silent=False, protect_content=False, caption=None, plain=False, before_dispatch=lambda: asyncio.sleep(0)
            )
        )
        await asyncio.wait_for(reply.started.wait(), 15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert reply.cancelled.is_set()


async def test_malformed_album_ack_is_distinct_from_rejected_photo_payload():
    from app.telegram_bots.outbound_upload import upload_output

    request = MultipartRequest()
    request.enqueue_get_me()
    request.enqueue("sendMediaGroup", result=[message(10)])
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    async with bot:
        result = await upload_output(
            bot,
            kind="album",
            files=(("a.png", b"a"), ("b.png", b"b")),
            chat_id=7000000001,
            message_thread_id=None,
            reply_to_message_id=None,
            silent=False,
            protect_content=False,
            caption=None,
            plain=False,
            before_dispatch=lambda: asyncio.sleep(0),
        )
    assert result.failure == "ack" and result.message_ids == ()


async def test_cancelled_native_sdk_preparation_drains_before_reader_cells_close(monkeypatch):
    import threading

    from app.telegram_bots import outbound_upload
    from app.telegram_bots.files import FileBytesBudget, OutputBatch

    request = MultipartRequest()
    request.enqueue_get_me()
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    payload = b"native preparation fixture" * 512
    budget = FileBytesBudget(len(payload))
    batch = OutputBatch(await budget.reserve_batch([len(payload)], timeout=1))
    await batch.keep(0, payload)
    loop, entered, release = asyncio.get_running_loop(), asyncio.Event(), threading.Event()
    readers, original = [], outbound_upload._make_uploads

    def blocked(names, handles):
        readers.extend(handles)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(15)
        return original(names, handles)

    monkeypatch.setattr(outbound_upload, "_make_uploads", blocked)
    try:
        async with bot:
            task = asyncio.create_task(
                outbound_upload.upload_output(
                    bot,
                    kind="document",
                    files=(("x.bin", batch.data(0)),),
                    chat_id=7000000001,
                    message_thread_id=None,
                    reply_to_message_id=None,
                    silent=False,
                    protect_content=False,
                    caption=None,
                    plain=False,
                    before_dispatch=lambda: asyncio.sleep(0),
                )
            )
            try:
                await asyncio.wait_for(entered.wait(), 15)
                task.cancel()
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done() and budget.used == len(payload)
                assert readers and all(not reader.closed and reader._data is payload for reader in readers)
            finally:
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert all(reader.closed and reader._data is None for reader in readers)
            assert [call.endpoint for call in request.calls] == ["getMe"]
    finally:
        release.set()
        await batch.aclose()
    assert budget.used == 0

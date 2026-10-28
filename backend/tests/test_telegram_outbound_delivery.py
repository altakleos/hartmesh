"""Outbound ownership and scope barriers; actual source proof is tested separately."""

import hashlib
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from app.telegram_bots.files import FileBytesBudget
from app.telegram_bots.producer import DeliveryEligibility

pytestmark = pytest.mark.asyncio


@dataclass(frozen=True)
class Metadata:
    path: str
    filename: str
    size: int
    mime_type: str


async def fixture(monkeypatch, *, filename="result.txt"):
    from app.telegram_bots.config import TelegramBotsConfig
    from app.telegram_bots.outbound_upload import UploadOutcome

    content = b"qualified file"
    descriptor = {"path": "/mnt/user-data/outputs/" + filename, "filename": filename, "size": len(content), "mime_type": "text/plain", "state": "ready"}
    part = {"kind": "document", "source_ref": {"output_files": [{"path": descriptor["path"], "sha256": None}]}, "reply_to_message_id": 7, "protect_content": True}
    link = SimpleNamespace(id="link", owner_user_id="owner", chat_id=7000000001, binding_generation=1, token_revision=1)
    saved = SimpleNamespace(part=part, link=link, item=SimpleNamespace(chat_id=link.chat_id, message_thread_id=23, silent=True))
    prepared = SimpleNamespace(record=SimpleNamespace(scope=SimpleNamespace(thread_id="thread")), files=(descriptor,), caption_html="<b>Answer</b>", caption_text="Answer", unavailable=None)
    budget = FileBytesBudget(50_000_000)
    events = []

    async def capture(**kwargs):
        return saved

    async def read_source(actual):
        assert actual is saved
        return prepared

    async def describe(owner, thread, path, *, before_dispatch):
        await before_dispatch()
        assert budget.used in {0, len(content)}
        events.append("describe")
        return Metadata(path, filename, len(content), "text/plain")

    async def read(owner, thread, path, *, ceiling, before_dispatch, on_failed_close=None):
        await before_dispatch()
        assert ceiling == budget.used == len(content)
        events.append("read")
        return content

    async def hashes(actual, values):
        assert values == {descriptor["path"]: hashlib.sha256(content).hexdigest()}
        assert budget.used == len(content)
        events.append("hash")
        return True

    async def eligibility(*args):
        return DeliveryEligibility.allow(protect_content=True)

    async def get_settings(owner):
        return SimpleNamespace()

    async def success():
        events.append("success")

    async def upload(bot, **kwargs):
        assert budget.used == len(content) and kwargs["files"] == ((filename, content),)
        assert events == ["describe", "read", "hash"]
        await kwargs["before_dispatch"]()
        assert kwargs["caption"] == "<b>Answer</b>" and kwargs["protect_content"] and kwargs["silent"]
        events.append("upload")
        return UploadOutcome(message_ids=(51,))

    owner = SimpleNamespace(describe_artifact=describe, read_artifact_capped=read, retains_failed_close=lambda cb: False)
    outbox = SimpleNamespace(capture_sending_part=capture, record_file_hashes=hashes)
    services = SimpleNamespace(
        outbox=outbox, model_source=SimpleNamespace(read_file_part=read_source), file_bytes_budget=budget, thread_client=owner, config=TelegramBotsConfig(), eligibility=eligibility, settings=SimpleNamespace(get=get_settings)
    )
    channel = SimpleNamespace(_services=services, bot=object(), transport_revision=1, link=link, _callback_current=lambda old: True, _after_send_success=success, _chat_message_sent=lambda *args: None)
    monkeypatch.setattr("app.telegram_bots.outbound_upload.upload_output", upload)
    return SimpleNamespace(channel=channel, saved=saved, prepared=prepared, events=events, budget=budget, owner=owner, services=services, content=content)


async def test_reserves_before_read_hashes_before_api_and_releases_after_ack(monkeypatch):
    from app.telegram_bots.outbound import send_file_part

    f = await fixture(monkeypatch)
    assert await send_file_part(f.channel, f.saved, {}) == [51]
    assert f.events == ["describe", "read", "hash", "describe", "upload", "success"] and f.budget.used == 0


async def test_changed_first_read_hash_never_reaches_sdk(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import SourceUnavailable

    f = await fixture(monkeypatch)

    async def changed(*args):
        return False

    f.services.outbox.record_file_hashes = changed
    with pytest.raises(SourceUnavailable):
        await send_file_part(f.channel, f.saved, {})
    assert "upload" not in f.events and f.budget.used == 0


async def test_failed_http_close_retains_charge_only_until_real_transport_closes(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import SourceUnavailable

    f = await fixture(monkeypatch)
    callbacks = []

    async def failed(owner, thread, path, *, ceiling, before_dispatch, on_failed_close):
        callbacks.append(on_failed_close)
        raise OSError("closed-disposition-only")

    f.owner.read_artifact_capped = failed
    f.owner.retains_failed_close = lambda cb: cb in callbacks
    with pytest.raises(SourceUnavailable):
        await send_file_part(f.channel, f.saved, {})
    assert f.budget.used == len(f.content) and "upload" not in f.events
    await callbacks[0]()
    assert f.budget.used == 0


async def test_revoked_during_read_cannot_upload_and_releases_charge(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch)
    original_read = f.owner.read_artifact_capped

    async def revoke(*args, **kwargs):
        data = await original_read(*args, **kwargs)
        f.channel._callback_current = lambda old: False
        return data

    f.owner.read_artifact_capped = revoke
    with pytest.raises(DeliveryClaimRetired):
        await send_file_part(f.channel, f.saved, {})
    assert "upload" not in f.events and f.budget.used == 0


async def test_repeated_cancellation_keeps_budget_until_hash_worker_drains(monkeypatch):
    import asyncio
    import threading

    from app.telegram_bots import outbound

    f = await fixture(monkeypatch)
    loop, entered, release = asyncio.get_running_loop(), asyncio.Event(), threading.Event()
    original_hash = outbound._hash_files

    def blocked(files):
        try:
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)
            return original_hash(files)
        finally:
            files = None

    monkeypatch.setattr(outbound, "_hash_files", blocked)
    task = asyncio.create_task(outbound.send_file_part(f.channel, f.saved, {}))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and f.budget.used == len(f.content)
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert "upload" not in f.events and f.budget.used == 0


async def test_caption_cleanup_uses_ack_owned_ids_and_the_original_scope(monkeypatch):
    from _telegram_bot_test_support import RecordingRequest, offline_bot

    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.outbound import cleanup_caption

    f = await fixture(monkeypatch)
    f.saved.item.payload = {"caption_cleanup": {"part_id": "caption", "message_ids": [19, 21], "state": "pending"}}

    async def capture(**kwargs):
        return f.saved

    f.services.outbox.capture_caption_cleanup = capture
    request = RecordingRequest()
    request.enqueue_get_me()
    request.enqueue("deleteMessage")
    request.enqueue("deleteMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to delete not found"})
    f.channel.bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    async with f.channel.bot:
        assert await cleanup_caption(f.channel, {})
    assert [call.parameters["message_id"] for call in request.calls if call.endpoint == "deleteMessage"] == [19, 21]
    assert not f.events and f.budget.used == 0


async def test_cleanup_long429_preserves_server_wait_and_does_not_upload(monkeypatch):
    from _telegram_bot_test_support import RecordingRequest, offline_bot

    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.outbound import cleanup_caption
    from app.telegram_bots.producer import DeliveryRetry

    f = await fixture(monkeypatch)
    f.saved.item.payload = {"caption_cleanup": {"part_id": "caption", "message_ids": [19], "state": "pending"}}

    async def capture(**kwargs):
        return f.saved

    f.services.outbox.capture_caption_cleanup = capture
    request = RecordingRequest()
    request.enqueue_get_me()
    request.enqueue("deleteMessage", status=429, body={"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 61}})
    f.channel.bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    async with f.channel.bot:
        with pytest.raises(DeliveryRetry) as error:
            await cleanup_caption(f.channel, {})
    assert error.value.retry_after == 61
    assert [call.endpoint for call in request.calls] == ["getMe", "deleteMessage"]


@pytest.mark.parametrize("status", [401, 403, 404])
async def test_gateway_file_refusal_never_blocks_bot_or_verifies_bot_token(monkeypatch, status):
    from app.channels.execution import ChannelHttpError, ChannelJsonResponse
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import SourceUnavailable

    f = await fixture(monkeypatch)

    async def refused(*args, **kwargs):
        raise ChannelHttpError(ChannelJsonResponse(status, {}, None))

    async def forbidden(*args):
        raise AssertionError("A Gateway file refusal cannot block Telegram")

    f.owner.read_artifact_capped = refused
    f.channel._forbidden = f.channel.verify_token = forbidden
    with pytest.raises(SourceUnavailable):
        await send_file_part(f.channel, f.saved, {})
    assert "upload" not in f.events and f.budget.used == 0


async def test_invalid_image_header_persists_document_replacement_before_upload(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch, filename="result.png")
    f.saved.part["kind"] = "photo"
    f.prepared.caption_html = f.prepared.caption_text = None
    replacements = []

    async def replace(captured, templates, *, reason):
        assert reason == "photo-classification" and f.events == ["describe", "read", "hash"]
        assert f.budget.used == len(f.content)
        replacements.extend(templates)
        return True

    f.services.outbox.replace_output_parts = replace
    with pytest.raises(DeliveryClaimRetired):
        await send_file_part(f.channel, f.saved, {})
    assert len(replacements) == 1 and replacements[0]["kind"] == "document"
    assert "upload" not in f.events and f.budget.used == 0


async def test_optional_preview_delete403_does_not_block_successful_message_sender(monkeypatch):
    from _telegram_bot_test_support import RecordingRequest, offline_bot

    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.outbound import cleanup_caption

    f = await fixture(monkeypatch)
    f.saved.item.payload = {"caption_cleanup": {"part_id": "caption", "message_ids": [19], "state": "pending"}}

    async def capture(**kwargs):
        return f.saved

    async def cannot_block(*args):
        raise AssertionError("Optional cleanup cannot block message delivery")

    f.services.outbox.capture_caption_cleanup = capture
    f.channel._forbidden = cannot_block
    request = RecordingRequest()
    request.enqueue_get_me()
    request.enqueue("deleteMessage", status=403, body={"ok": False, "error_code": 403, "description": "Forbidden"})
    f.channel.bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    async with f.channel.bot:
        assert await cleanup_caption(f.channel, {}) == "unavailable"
    assert [call.endpoint for call in request.calls] == ["getMe", "deleteMessage"]


async def test_first_member_hash_is_saved_before_a_later_batch_member_can_fail(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch)
    files = tuple({"path": "/mnt/user-data/outputs/" + name, "filename": name, "size": len(f.content), "mime_type": "image/png", "state": "ready"} for name in ("first.png", "second.png"))
    f.prepared.files, f.prepared.caption_html, f.prepared.caption_text = files, None, None
    f.saved.part["kind"] = "album"
    f.saved.part["source_ref"] = {"output_files": [{"path": member["path"], "sha256": None} for member in files]}
    events, hashes = [], {}

    async def describe(owner, thread, path, *, before_dispatch):
        await before_dispatch()
        assert f.budget.used == 0
        member = next(member for member in files if member["path"] == path)
        return Metadata(path, member["filename"], len(f.content), "image/png")

    async def read(owner, thread, path, *, ceiling, before_dispatch, on_failed_close):
        await before_dispatch()
        assert f.budget.used == 2 * len(f.content)
        events.append(path)
        if path == files[1]["path"]:
            assert hashes == {files[0]["path"]: hashlib.sha256(f.content).hexdigest()}
            raise OSError("second member unavailable")
        return f.content

    async def record(captured, values):
        hashes.update(values)
        events.append("hash")
        return True

    f.owner.describe_artifact, f.owner.read_artifact_capped = describe, read

    async def replace(captured, templates, *, reason):
        assert reason == "photo-classification" and f.budget.used == 0
        assert [part["kind"] for part in templates] == ["document", "document"]
        return True

    f.services.outbox.record_file_hashes = record
    f.services.outbox.replace_output_parts = replace
    with pytest.raises(DeliveryClaimRetired):
        await send_file_part(f.channel, f.saved, {})
    assert events == [files[0]["path"], "hash", files[1]["path"]]
    assert f.budget.used == 0 and "upload" not in f.events


async def test_smaller_restart_capacity_repartitions_saved_album_before_any_bytes(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch)
    files = tuple({"path": f"/mnt/user-data/outputs/{i}.png", "filename": f"{i}.png", "size": 10_000_000, "mime_type": "image/png", "state": "ready"} for i in range(6))
    f.prepared.files, f.prepared.caption_html, f.prepared.caption_text = files, None, None
    f.saved.part["kind"] = "album"
    f.saved.part["source_ref"] = {"output_files": [{"path": member["path"], "sha256": None} for member in files]}
    replacements = []

    async def describe(owner, thread, path, *, before_dispatch):
        await before_dispatch()
        member = next(member for member in files if member["path"] == path)
        return Metadata(path, member["filename"], member["size"], "image/png")

    async def cannot_read(*args, **kwargs):
        raise AssertionError("Repartition must precede any payload read")

    async def replace(captured, templates, *, reason):
        assert reason == "capacity" and f.budget.used == 0
        replacements.extend(templates)
        return True

    f.owner.describe_artifact, f.owner.read_artifact_capped = describe, cannot_read
    f.services.outbox.replace_output_parts = replace
    with pytest.raises(DeliveryClaimRetired):
        await send_file_part(f.channel, f.saved, {})
    assert [part["kind"] for part in replacements] == ["album", "photo"]
    assert [len(part["files"]) for part in replacements] == [5, 1]
    assert f.budget.used == 0 and not f.events


async def test_rejected_photo_logs_once_and_persists_document_fallback_after_buffers_drain(monkeypatch):
    from telegram.error import BadRequest

    from app.telegram_bots import outbound
    from app.telegram_bots.outbound_upload import UploadOutcome
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch, filename="result.png")
    f.saved.part["kind"] = "photo"
    f.prepared.caption_html = f.prepared.caption_text = None
    logs, replacements = [], []

    async def photo(*args):
        return True

    async def reject(bot, **kwargs):
        await kwargs["before_dispatch"]()
        assert f.budget.used == len(f.content)
        return UploadOutcome(failure="payload")

    async def replace(captured, templates, *, reason):
        assert reason == "photo-rejected" and f.budget.used == 0
        replacements.extend(templates)
        return True

    f.services.outbox.replace_output_parts = replace
    f.channel._failure = lambda: None
    f.channel._log_part_failure = lambda method, error, saved: logs.append((method, type(error)))
    monkeypatch.setattr(outbound, "classify_output_photo", photo)
    monkeypatch.setattr("app.telegram_bots.outbound_upload.upload_output", reject)
    with pytest.raises(DeliveryClaimRetired):
        await outbound.send_file_part(f.channel, f.saved, {})
    assert logs == [("sendPhoto", BadRequest)]
    assert [part["kind"] for part in replacements] == ["document"] and f.budget.used == 0


async def test_lowered_limit_isolates_oversize_member_without_suppressing_healthy_file(monkeypatch):
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch)
    files = tuple({"path": f"/mnt/user-data/outputs/{i}.png", "filename": f"{i}.png", "size": size, "mime_type": "image/png", "state": "ready"} for i, size in enumerate((3_000_000, 8_000_000)))
    f.prepared.files, f.prepared.caption_html, f.prepared.caption_text = files, None, None
    f.saved.part["kind"] = "album"
    f.saved.part["source_ref"] = {"output_files": [{"path": member["path"], "sha256": None} for member in files]}
    f.services.config = f.services.config.model_copy(update={"files": f.services.config.files.model_copy(update={"max_outbound_bytes": 5_000_000})})
    replacements = []

    async def describe(owner, thread, path, *, before_dispatch):
        await before_dispatch()
        member = next(member for member in files if member["path"] == path)
        return Metadata(path, member["filename"], member["size"], "image/png")

    async def cannot_read(*args, **kwargs):
        raise AssertionError("Split before any payload is opened")

    async def replace(captured, templates, *, reason):
        assert reason == "photo-classification" and f.budget.used == 0
        replacements.extend(templates)
        return True

    f.owner.describe_artifact, f.owner.read_artifact_capped = describe, cannot_read
    f.services.outbox.replace_output_parts = replace
    with pytest.raises(DeliveryClaimRetired):
        await send_file_part(f.channel, f.saved, {})
    assert [part["kind"] for part in replacements] == ["document", "document"]
    assert [part["files"][0]["path"] for part in replacements] == [member["path"] for member in files]
    assert all(part["unavailable"] is None for part in replacements) and f.budget.used == 0


async def test_space_access_revoked_after_read_is_rechecked_at_actual_sdk_limiter(monkeypatch):
    from _telegram_bot_test_support import RecordingRequest, offline_bot

    from app.channels.execution import ChannelHttpError, ChannelJsonResponse
    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.outbound import send_file_part
    from app.telegram_bots.outbound_upload import upload_output as actual_upload
    from app.telegram_bots.producer import DeliveryClaimRetired

    f = await fixture(monkeypatch)
    describe, calls = f.owner.describe_artifact, []

    async def revoke_after_read(*args, **kwargs):
        calls.append(f.budget.used)
        if f.budget.used:
            # Thread/run source qualification still succeeds. The independent
            # file admission has been withdrawn while bytes were buffered.
            raise ChannelHttpError(ChannelJsonResponse(403, {}, None))
        return await describe(*args, **kwargs)

    f.owner.describe_artifact = revoke_after_read
    request = RecordingRequest()
    request.enqueue_get_me()
    request.enqueue("sendDocument", result={"message_id": 10, "date": 1, "chat": {"id": f.saved.item.chat_id, "type": "private"}})
    f.channel.bot = offline_bot(request, rate_limiter=BoundedRateLimiter())
    # Use the actual pinned SDK leaf, preserved before fixture() installed its
    # protocol double. A raw upload would succeed if the last fence were absent.
    monkeypatch.setattr("app.telegram_bots.outbound_upload.upload_output", actual_upload)
    async with f.channel.bot:
        with pytest.raises(DeliveryClaimRetired):
            await send_file_part(f.channel, f.saved, {})
    assert calls == [0, len(f.content)]
    assert [call.endpoint for call in request.calls] == ["getMe"]
    assert f.budget.used == 0

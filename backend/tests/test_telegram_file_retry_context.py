"""Captionless file context is committed once with the original write receipt."""

from dataclasses import replace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_attachments import next_text, pending_service
from test_telegram_instance_file_puts import retry_button, runner
from test_telegram_instance_publication import publication as publication

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramPendingFileRow

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("caption", ["", "Read the file"])
async def test_save_commits_captionless_context_and_never_recreates_after_take(file_admission, monkeypatch, caption):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, text=caption)
    saved = await service.put(e.link, origin, file, origins=(origin,), **args)
    assert saved.continuation["input"] == dict(captionless=not caption, topic=None, message_ids=(origin.message_id,))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id))
        assert (row is not None) is (not caption)
        if row is not None:
            assert row.agent_file_path == saved.result["path"]
    pending = await pending_service(e, service)
    target = await next_text(e)
    assert await pending.take(e.link, target) == ([file] if not caption else [])
    assert await service.put(e.link, origin, file, origins=(origin,), **args) == saved
    if not caption:
        await pending.add(e.link, origin, file)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
    assert e.bus.inbound_queue.empty()


async def test_retry_after_inbox_collection_attaches_once_without_another_model_turn(file_admission, monkeypatch):
    from app.channels.inbound_leases import InboundFileLease

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    failed = await service.put(e.link, origin, file, origins=(origin,), **args)
    callback = await retry_button(e, service, failed)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        await writer.delete(row)
    downloads = []

    async def released():
        pass

    async def download(descriptor, check):
        await check()
        downloads.append(descriptor)
        return InboundFileLease(b"data", released)

    args["download"] = download
    press = dict(callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None)
    saved = await service.retry(e.link, failed.id, **press, **args)
    pending = await pending_service(e, service)
    target = await next_text(e)
    assert await pending.take(e.link, target) == [file]
    assert await service.retry(e.link, failed.id, **press, **args) == saved
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
    assert len(downloads) == 1 and e.bus.inbound_queue.empty()


@pytest.mark.parametrize("change", ["caption", "topic", "duplicate"])
async def test_input_capsule_rejects_forged_or_duplicate_sources_before_download(file_admission, monkeypatch, change):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, text="Read the file")
    if change == "caption":
        origins = (replace(origin, sanitized_input={**origin.sanitized_input, "text": ""}),)
    elif change == "topic":
        origins = (replace(origin, message_thread_id=99),)
    else:
        origins = (origin, origin)
    with pytest.raises(ValueError):
        await service.put(e.link, origin, file, origins=origins, **args)
    async with e.db.sessions() as reader:
        from sqlalchemy import select

        assert not list(await reader.scalars(select(TelegramActRow)))
    assert not rest[-1]


@pytest.mark.parametrize("caption", ["", "Read this album"])
async def test_joined_album_caption_controls_context_for_every_member(file_admission, monkeypatch, caption):
    from test_telegram_instance_file_puts import admitted

    e = file_admission
    service, _origin, _file, args, *rest = await runner(e, monkeypatch)
    one, file = await admitted(e, message_id=921, album="album-1")
    two, _ = await admitted(e, message_id=922, album="album-1", text=caption)
    saved = await service.put(e.link, one, file, origins=(one, two), **args)
    assert saved.continuation["input"]["captionless"] is (not caption)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramPendingFileRow, (e.link.id, one.message_id)) is not None) is (not caption)


async def test_retry_cannot_consume_control_in_another_topic(file_admission, monkeypatch):
    from test_telegram_instance_file_puts import admitted

    from app.telegram_bots.callbacks import CallbackRefused

    e = file_admission
    service, _origin, _file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    origin, file = await admitted(e, message_id=921, topic=99)
    failed = await service.put(e.link, origin, file, origins=(origin,), **args)
    callback = await retry_button(e, service, failed)
    with pytest.raises(CallbackRefused):
        await service.retry(e.link, failed.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    assert (await service.callbacks.get(callback)).consumed_at is None
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramActRow, failed.id)
        assert row.operation_ids == [failed.id]


async def test_final_writer_rolls_back_completion_and_pending_context_together(file_admission, monkeypatch):
    from sqlalchemy import select, update

    from app.telegram_bots.links import _PublicationGuard
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    original, changed = _PublicationGuard.check, []

    async def revoke_after_pending(guard, writer):
        if not changed and await writer.scalar(select(TelegramPendingFileRow.link_id)):
            changed.append(True)
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return await original(guard, writer)

    monkeypatch.setattr(_PublicationGuard, "check", revoke_after_pending)
    with pytest.raises(AgentDenied):
        await service.put(e.link, origin, file, origins=(origin,), **args)
    assert changed
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramActRow))
        assert row.state == "pending" and row.result is None
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None


async def test_legacy_completion_never_upgrades_or_reapplies_for_new_input_metadata(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    saved = await service.put(e.link, origin, file, **args)
    assert saved.continuation["version"] == 1
    assert await service.put(e.link, origin, file, origins=(origin,), **args) == saved
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
    assert len(rest[-1]) == 1


async def test_general_topic_retry_uses_the_canonical_saved_scope(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    failed = await service.put(e.link, origin, file, origins=(origin,), **args)
    callback = await retry_button(e, service, failed)
    saved = await service.retry(e.link, failed.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=1, **args)
    assert saved.continuation["presses"][-1]["topic"] is None
    assert (await service.callbacks.get(callback)).consumed_at is not None
    replay = await service.retry(e.link, failed.id, callback_id=callback, chat_id=e.link.chat_id, message_id=1911, message_thread_id=None, **args)
    assert replay == saved and len(rest[-1]) == 2

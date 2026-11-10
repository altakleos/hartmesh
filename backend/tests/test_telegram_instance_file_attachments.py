"""Saved instance files reach consumers only through their original Home receipt."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_puts import runner
from test_telegram_instance_publication import publication as publication

from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.spaces.model import SpaceRow

pytestmark = pytest.mark.asyncio


async def test_composer_projects_completed_receipt_without_download_or_write(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    saved = await service.put(e.link, origin, file, **args)

    async def forbidden(*args, **kwargs):
        raise AssertionError("A model attachment reads the saved receipt only")

    monkeypatch.setattr(e.file_service.agent_files, "apply", forbidden)
    actual = await service.composer_files(e.link, [file], authority=e.live.authority, current=lambda: e.active[0])
    assert actual == [{"filename": "input.txt", "size": 4, "path": "/mnt/user-data/" + saved.result["path"], "status": "uploaded"}]
    assert len(calls) == 1 and len(releases) == 1 and e.bus.inbound_queue.empty()


async def test_composer_rejects_unfinished_and_forged_descriptors(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    await service.put(e.link, origin, file, **args)
    for item in [file, {**file, "file_id": "forged"}, {**file, "size": True}]:
        with pytest.raises(ValueError):
            await service.composer_files(e.link, [item], authority=e.live.authority, current=lambda: e.active[0])


async def test_composer_rejects_home_change_and_lost_use(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
    with pytest.raises(ValueError, match="file-target-changed"):
        await service.composer_files(e.link, [file], authority=e.live.authority, current=lambda: e.active[0])
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
    with pytest.raises(AgentDenied):
        await service.composer_files(e.link, [file], authority=e.live.authority, current=lambda: e.active[0])


async def test_composer_rechecks_use_after_access_capture(file_admission, monkeypatch):
    from app.telegram_bots import file_puts as module

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    original = module.prepare_file_write

    async def revoked(*args, **kwargs):
        access = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return access

    monkeypatch.setattr(module, "prepare_file_write", revoked)
    with pytest.raises(AgentDenied):
        await service.composer_files(e.link, [file], authority=e.live.authority, current=lambda: e.active[0])


async def pending_service(e, service):
    from app.telegram_bots.files import PendingFiles
    from app.telegram_bots.instance_file_authority import prepare_file_write

    access = await prepare_file_write(e.live.authority, e.link, current=lambda: e.active[0])

    async def receipts(link):
        return await service.receipt_reader(link, authority=e.live.authority, current=lambda: e.active[0])

    return PendingFiles(service.callbacks, e.inbox, guard_factory=lambda link: access.guard, config=e.db.config, instance_receipts=receipts)


async def next_text(e):
    from test_telegram_bot_handlers import message

    incoming = message(e, 912, content="Use the file")
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    return await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])


async def test_pending_instance_file_keeps_actual_path_and_attaches_without_another_write(file_admission, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramPendingFileRow

    e = file_admission
    service, origin, file, args, sleeps, hints, releases, calls = await runner(e, monkeypatch)
    saved = await service.put(e.link, origin, file, **args)
    pending = await pending_service(e, service)
    await pending.add(e.link, origin, file)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"]))
        assert row.agent_file_path == saved.result["path"]
    target = await next_text(e)
    assert await pending.take(e.link, target) == [file]
    assert await pending.take(e.link, target) == [file]
    attached = await service.composer_files(e.link, [file], authority=e.live.authority, current=lambda: e.active[0])
    assert attached[0]["path"] == "/mnt/user-data/" + saved.result["path"]
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"])) is None
    assert len(calls) == len(releases) == 1 and e.bus.inbound_queue.empty()


async def test_pending_instance_file_requires_a_completed_receipt(file_admission, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramPendingFileRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch, download_result="file.not_saved")
    await service.put(e.link, origin, file, **args)
    pending = await pending_service(e, service)
    with pytest.raises(ValueError, match="file-receipt-unavailable"):
        await pending.add(e.link, origin, file)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"])) is None


@pytest.mark.parametrize("change", ["path", "home"])
async def test_pending_take_rechecks_saved_path_and_home_before_consumption(file_admission, monkeypatch, change):
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramPendingFileRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    pending = await pending_service(e, service)
    await pending.add(e.link, origin, file)
    target = await next_text(e)
    async with e.db.sessions.begin() as writer:
        if change == "path":
            row = await writer.get(TelegramPendingFileRow, (e.link.id, file["message_id"]))
            row.agent_file_path = "uploads/not-the-admitted-file.txt"
        else:
            await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
    with pytest.raises(ValueError):
        await pending.take(e.link, target)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"])) is not None
        original = await reader.get(TelegramInboundMarkRow, (e.link.id, target.message_id, e.link.chat_id))
        assert "files" not in original.sanitized_input


async def test_empty_pending_take_needs_no_file_readiness(file_admission, monkeypatch):
    e = file_admission
    service, _origin, _file, _args, *rest = await runner(e, monkeypatch)
    pending = await pending_service(e, service)
    target = await next_text(e)

    async def forbidden(*args, **kwargs):
        raise AssertionError("A plain message without pending files needs no file admission")

    pending.instance_receipts = forbidden
    from app.gateway import storage_spaces

    monkeypatch.setattr(storage_spaces, "require_execution", forbidden)
    assert await pending.take(e.link, target) == []
    assert await pending.take(e.link, target) == []


async def test_borrowed_pending_writer_uses_prepared_reader_and_rolls_back(file_admission, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramPendingFileRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    pending = await pending_service(e, service)
    receipts = await service.receipt_reader(e.link, authority=e.live.authority, current=lambda: e.active[0])

    async def forbidden(*args, **kwargs):
        raise AssertionError("No external reader preparation may run inside a borrowed writer")

    pending.instance_receipts = forbidden
    with pytest.raises(RuntimeError, match="rollback"):
        async with service.callbacks.writer(e.link, receipts.guard()) as writer:
            await pending.add(e.link, origin, file, session=writer, instance_reader=receipts)
            raise RuntimeError("rollback")
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"])) is None
    async with service.callbacks.writer(e.link, receipts.guard()) as writer:
        await pending.add(e.link, origin, file, session=writer, instance_reader=receipts)
    target = await next_text(e)
    with pytest.raises(RuntimeError, match="rollback"):
        async with service.callbacks.writer(e.link, receipts.guard()) as writer:
            assert await pending.take(e.link, target, session=writer, instance_reader=receipts) == [file]
            raise RuntimeError("rollback")
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"])) is not None
        original = await reader.get(TelegramInboundMarkRow, (e.link.id, target.message_id, e.link.chat_id))
        assert "files" not in original.sanitized_input


@pytest.mark.parametrize("change", ["use", "home", "policy"])
@pytest.mark.parametrize("operation", ["add", "take"])
async def test_late_receipt_change_rolls_back_pending_metadata(file_admission, monkeypatch, change, operation):
    from datetime import UTC, datetime

    from app.telegram_bots.file_puts import FileReceiptReader
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramInstancePolicyRow, TelegramPendingFileRow

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    pending = await pending_service(e, service)
    if operation == "take":
        await pending.add(e.link, origin, file)
    target = await next_text(e)
    original = FileReceiptReader.completed
    changed = []

    async def late(self, file, *, session=None):
        saved = await original(self, file, session=session)
        if session is not None and not changed:
            changed.append(True)
            if change == "use":
                await session.execute(update(AgentInstanceGrantRow).values(permissions=2))
            elif change == "home":
                await session.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
            else:
                session.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="off", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
                await session.flush()
        return saved

    monkeypatch.setattr(FileReceiptReader, "completed", late)
    with pytest.raises((ValueError, AgentDenied)):
        if operation == "take":
            await pending.take(e.link, target)
        else:
            await pending.add(e.link, origin, file)
    assert changed
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramPendingFileRow, (e.link.id, file["message_id"])) is not None) is (operation == "take")
        actual = await reader.get(TelegramInboundMarkRow, (e.link.id, target.message_id, e.link.chat_id))
        assert "files" not in actual.sanitized_input


async def test_late_use_loss_prevents_composer_return(file_admission, monkeypatch):
    from app.telegram_bots.file_puts import FileReceiptReader

    e = file_admission
    service, origin, file, args, *rest = await runner(e, monkeypatch)
    await service.put(e.link, origin, file, **args)
    original = FileReceiptReader.completed

    async def late(self, file, *, session=None):
        saved = await original(self, file, session=session)
        await session.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return saved

    monkeypatch.setattr(FileReceiptReader, "completed", late)
    with pytest.raises(AgentDenied):
        await service.composer_files(e.link, [file], authority=e.live.authority, current=lambda: e.active[0])


async def test_composed_receipt_guard_checks_original_lifetime_after_final_authority_await(file_admission, monkeypatch):
    from app.telegram_bots.instance_file_authority import _FileWrite
    from app.telegram_bots.turns import _Guard

    e = file_admission
    service, _origin, _file, _args, *rest = await runner(e, monkeypatch)
    receipts = await service.receipt_reader(e.link, authority=e.live.authority, current=lambda: e.active[0])
    alive = [True]
    base = _Guard(e.channel._services, e.channel, e.link, lambda: alive[0], borrowed=receipts.access.guard)
    await base.prepare()
    guard = receipts.guard(base)
    original = _FileWrite.check_receipt

    async def retired(self, writer):
        actual = await original(self, writer)
        alive[0] = False
        return actual

    monkeypatch.setattr(_FileWrite, "check_receipt", retired)
    async with service.callbacks.writer(e.link, receipts.access.guard) as writer:
        with pytest.raises(ValueError, match="chat-turn-retired"):
            await guard.check(writer)

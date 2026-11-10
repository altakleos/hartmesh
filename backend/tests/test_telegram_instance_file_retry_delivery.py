"""Real saved file controls retry through the adapter without another model turn."""

from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_attachments import next_text, pending_service
from test_telegram_instance_file_delivery import delivery
from test_telegram_instance_publication import publication as publication

from app.telegram_bots.file_outcomes import FileOutcomes
from deerflow.agent_instances.agent_files import AgentFileRefusal
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPendingFileRow

pytestmark = pytest.mark.asyncio


async def failed_control(e, monkeypatch, *, text=""):
    origin, file, request, reads = await delivery(e, monkeypatch, text=text)
    services = e.channel._services
    original = e.file_service.agent_files.apply

    async def refused(**kwargs):
        raise AgentFileRefusal("name-collision")

    monkeypatch.setattr(e.file_service.agent_files, "apply", refused)
    failed = await e.channel.put_instance_file(e.link, origin, file)
    parent = await FileOutcomes(services).plan(e.channel, e.link, origin, file)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == failed.id))
        identity, group = row.id, row.group_id
    assert await services.callbacks.adopt(e.link, group, 1911, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: e.active[0])
    await e.inbox.finish_route(origin)
    monkeypatch.setattr(e.file_service.agent_files, "apply", original)

    async def forbidden(*args, **kwargs):
        raise AssertionError("An instance file retry must never enter ordinary/model retry")

    async def retire(channel):
        pass

    services.turns = SimpleNamespace(execute_callback=forbidden, retire=retire)
    return origin, file, request, reads, await services.callbacks.get(identity)


def next_download(request, file):
    request.enqueue("sendMessageDraft", result=True)
    request.enqueue("getFile", result={"file_id": file["file_id"], "file_unique_id": file["file_unique_id"], "file_size": 4, "file_path": "documents/input.txt"})
    request.enqueue("sendMessageDraft", result=True)


@pytest.mark.parametrize("collected", [False, True])
async def test_actual_control_retries_after_original_route_closed_without_model_or_reattachment(file_admission, monkeypatch, collected):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    if collected:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
            await writer.delete(row)
    next_download(request, file)
    await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        assert act.state == "done" and len(act.operation_ids) == 2
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))
        finished = [row for row in rows if row.payload.get("file_put_basis", {}).get("state") == "done"]
        assert len(finished) == 1 and finished[0].parts[0]["source_ref"]["key"] == "file.saved_ask"
        assert finished[0].payload.get("inbox_rows", []) == []
    pending = await pending_service(e, e.channel._services.file_puts)
    target = await next_text(e)
    assert await pending.take(e.link, target) == [file]
    await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
    assert len(reads) == 2 and not e.channel._file_tasks and e.bus.inbound_queue.empty()


async def test_retry_failure_plans_a_new_exact_control_after_original_route_closed(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)

    async def refused(**kwargs):
        raise AgentFileRefusal("name-collision")

    monkeypatch.setattr(e.file_service.agent_files, "apply", refused)
    next_download(request, file)
    await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        assert act.state == "refused" and len(act.operation_ids) == 2
        rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == callback.target_id)))
        assert len(rows) == 2 and sum(row.consumed_at is None for row in rows) == 1
    assert len(reads) == 2 and e.bus.inbound_queue.empty()


async def test_lost_retry_confirmation_plans_uncertainty_without_reapplying(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    apply, operations = e.file_service.agent_files.apply, []

    async def lost(**kwargs):
        operations.append(kwargs["operation_id"])
        await apply(**kwargs)
        raise RuntimeError("confirmation lost after the actual write")

    monkeypatch.setattr(e.file_service.agent_files, "apply", lost)
    next_download(request, file)
    await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        assert act.state == "uncertain" and len(act.operation_ids) == 2
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))
        uncertain = [row for row in rows if row.payload.get("file_put_basis", {}).get("state") == "uncertain"]
        assert len(uncertain) == 1 and uncertain[0].parts[0]["source_ref"]["key"] == "file.unconfirmed"
    assert len(operations) == 1 and len(reads) == 2 and e.bus.inbound_queue.empty()


async def test_simultaneous_press_delivery_never_repeats_write_or_notice(file_admission, monkeypatch):
    import asyncio

    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    next_download(request, file)
    await asyncio.gather(e.channel.execute_callback(callback), e.channel.execute_callback(callback))
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        assert act.state == "done" and len(act.operation_ids) == 2
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))
        assert len(rows) == 2
    assert len(reads) == 2 and e.bus.inbound_queue.empty()


async def test_captioned_retry_confirms_save_without_pending_next_message(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch, text="Read the file")
    next_download(request, file)
    await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))
        done = next(row for row in rows if row.payload.get("file_put_basis", {}).get("state") == "done")
        assert done.parts[0]["source_ref"]["key"] == "file.saved"
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
    assert len(reads) == 2 and e.bus.inbound_queue.empty()


async def test_old_press_never_plans_the_later_childs_outcome(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    original = e.file_service.agent_files.apply

    async def refused(**kwargs):
        raise AgentFileRefusal("name-collision")

    monkeypatch.setattr(e.file_service.agent_files, "apply", refused)
    next_download(request, file)
    parent = await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == callback.target_id, TelegramCallbackRow.consumed_at.is_(None)))
        identity, group = row.id, row.group_id
    services = e.channel._services
    assert await services.callbacks.adopt(e.link, group, 1912, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: e.active[0])
    latest = await services.callbacks.get(identity)
    monkeypatch.setattr(e.file_service.agent_files, "apply", original)
    next_download(request, file)
    await e.channel.execute_callback(latest)
    assert await e.channel.execute_callback(callback) is None
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))
        assert len(rows) == 3
    assert len(reads) == 3 and e.bus.inbound_queue.empty()

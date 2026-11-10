"""Managed SDK downloads feed direct instance writes and publication attachments."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_puts import admitted
from test_telegram_instance_publication import publication as publication

from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.file_puts import FilePuts
from app.telegram_bots.files import FileBytesBudget

pytestmark = pytest.mark.asyncio


async def delivery(e, monkeypatch, *, size=4, too_big=False):
    services = e.channel._services
    services.authority = e.live.authority
    services.callbacks = CallbackRepository(e.db.sessions)
    services.file_puts = FilePuts(services.callbacks, e.inbox, config=e.db.config.files)
    services.file_bytes_budget = FileBytesBudget(e.db.config.files.in_memory_max_bytes)
    services.prepare_live_owner = e.live.authority.prepare_live_owner
    services.protect_notice = e.live.authority.protect_notice
    source, file = await admitted(e, size=size)
    request, reads = e.channel._requests[0], []
    request.enqueue("sendMessageDraft", result=True)
    request.enqueue("sendMessageDraft", result=True)
    if too_big:
        request.enqueue("getFile", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: file is too big"})
    else:
        request.enqueue("getFile", result={"file_id": file["file_id"], "file_unique_id": file["file_unique_id"], "file_size": 4, "file_path": "documents/input.txt"})

    async def capped(url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        # This is the only transport protocol double; actual capped HTTP has
        # separate stream tests. The SDK, SQL, byte budget and Home write are real.
        assert services.file_bytes_budget.used == (size if size is not None else max_bytes)
        await before_dispatch()
        reads.append((ceiling, max_bytes))
        return b"data"

    monkeypatch.setattr(request, "read_file_capped", capped, raising=False)
    return source, file, request, reads


async def test_sdk_file_is_saved_directly_without_resident_or_conversation(file_admission, monkeypatch):
    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch)
    saved = await e.channel.put_instance_file(e.link, source, file)
    assert saved.state == "done" and saved.result["path"] == "uploads/input.txt"
    assert reads == [(4, 20_000_000)] and e.channel._services.file_bytes_budget.used == 0
    assert [call.endpoint for call in request.calls].count("getFile") == 1
    drafts = [call.parameters for call in request.calls if call.endpoint == "sendMessageDraft"]
    assert [item["text"] for item in drafts] == ["Saving input.txt for Fixture employee…", ""]
    assert drafts[0]["draft_id"] == drafts[1]["draft_id"]
    assert e.bus.inbound_queue.empty()
    again = await e.channel.put_instance_file(e.link, source, file)
    assert again == saved and len(reads) == 1


async def test_unknown_size_getfile_limit_refuses_before_home_write(file_admission, monkeypatch):
    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch, size=None, too_big=True)
    saved = await e.channel.put_instance_file(e.link, source, file)
    assert saved.state == "refused" and saved.reason == "file-too-large"
    assert not reads and e.channel._services.file_bytes_budget.used == 0


async def test_protected_save_uses_protected_hint_and_never_a_draft(file_admission, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    request.enqueue("sendMessage", result={"message_id": 1901, "date": 1700000000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saving"})
    request.enqueue("deleteMessage", result=True)
    saved = await e.channel.put_instance_file(e.link, source, file)
    assert saved.state == "done" and reads
    calls = [call for call in request.calls if call.endpoint in {"sendMessage", "sendMessageDraft", "deleteMessage"}]
    assert [call.endpoint for call in calls] == ["sendMessage", "deleteMessage"]
    assert calls[0].parameters["protect_content"] is True
    assert calls[1].parameters["message_id"] == 1901


async def test_known_oversize_journals_refusal_without_sdk_call(file_admission, monkeypatch):
    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch, size=20_000_001)
    before = len(request.calls)
    saved = await e.channel.put_instance_file(e.link, source, file)
    assert (saved.state, saved.reason) == ("refused", "file-too-large")
    assert len(request.calls) == before and not reads and not e.channel._file_tasks


async def test_transient_write_updates_same_draft_and_keeps_one_buffer(file_admission, monkeypatch):
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch)
    request.enqueue("sendMessageDraft", result=True)
    original, operations, sleeps = e.file_service.agent_files.apply, [], []

    async def apply(**kwargs):
        operations.append(kwargs["operation_id"])
        assert e.channel._services.file_bytes_budget.used == 4
        if len(operations) == 1:
            raise AgentFileRefusal("file-write-failed")
        return await original(**kwargs)

    async def sleep(seconds):
        sleeps.append(seconds)
        assert e.channel._services.file_bytes_budget.used == 4

    monkeypatch.setattr(e.file_service.agent_files, "apply", apply)
    e.channel._services.file_puts._sleep = sleep
    saved = await e.channel.put_instance_file(e.link, source, file)
    assert saved.state == "done" and operations == [saved.id] * 2 and sleeps == [3]
    drafts = [call.parameters for call in request.calls if call.endpoint == "sendMessageDraft"]
    assert [item["text"] for item in drafts] == ["Saving input.txt for Fixture employee…", "Still saving input.txt for Fixture employee…", ""]
    assert len({item["draft_id"] for item in drafts}) == 1 and len(reads) == 1
    assert e.channel._services.file_bytes_budget.used == 0


async def test_cancelled_adapter_operation_drains_write_and_releases_budget(file_admission, monkeypatch):
    import asyncio

    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch)
    entered, finish = asyncio.Event(), asyncio.Event()
    original = e.file_service.agent_files.apply

    async def held(**kwargs):
        entered.set()
        await finish.wait()
        assert not kwargs["content"].closed and e.channel._services.file_bytes_budget.used == 4
        return await original(**kwargs)

    monkeypatch.setattr(e.file_service.agent_files, "apply", held)
    task = asyncio.create_task(e.channel.put_instance_file(e.link, source, file))
    async with asyncio.timeout(10):
        await entered.wait()
    try:
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and e.channel._file_tasks and e.channel._services.file_bytes_budget.used == 4
    finally:
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not e.channel._file_tasks and e.channel._services.file_bytes_budget.used == 0
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert act.state == "done"


@pytest.mark.parametrize("change", ["use", "lifetime"])
async def test_limiter_rechecks_file_authority_before_any_sdk_effect(file_admission, monkeypatch, change):
    from sqlalchemy import update

    from app.telegram_bots.authority import AuthorityUnavailable
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = file_admission
    source, file, request, reads = await delivery(e, monkeypatch)
    limiter = e.channel.bot.rate_limiter

    async def change_before_dispatch(seconds):
        limiter._short_hold_until = 0
        if change == "use":
            async with e.db.sessions.begin() as writer:
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        else:
            e.active[0] = False

    limiter._sleep = change_before_dispatch
    limiter._short_hold_until = limiter._time() + 1
    before = len(request.calls)
    from app.telegram_bots.producer import DeliveryClaimRetired

    with pytest.raises((AgentDenied, AuthorityUnavailable, DeliveryClaimRetired, ValueError)) as refused:
        await e.channel.put_instance_file(e.link, source, file)
    if change == "lifetime":
        assert str(refused.value) == "publication-stale"
    assert len(request.calls) == before and not reads
    assert not e.channel._file_tasks and e.channel._services.file_bytes_budget.used == 0

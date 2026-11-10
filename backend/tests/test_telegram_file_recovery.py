"""Startup reconciles file metadata without replaying downloads or native writes."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_input import input_environment
from test_telegram_instance_file_retry_delivery import failed_control, next_download
from test_telegram_instance_publication import publication as publication

from app.telegram_bots.file_outcomes import FileOutcomes
from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPendingFileRow

pytestmark = pytest.mark.asyncio


def startup(e, turns, monkeypatch):
    services = e.channel._services
    services._copy_decision = False
    services._cohort_complete = services._ready = True
    services._send_ready = services._stopping = services._stopped = False
    services._cause = None
    services.turns = turns
    monkeypatch.setattr(e.channel, "_send_allowed", lambda: services._send_ready)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Recovery must never download a file or reapply a native write")

    monkeypatch.setattr(e.channel, "_download", forbidden)
    monkeypatch.setattr(e.file_service.agent_files, "apply", forbidden)
    return services


async def test_completed_captionless_file_recovers_its_question_without_reapplying(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    saved = await e.channel.put_instance_file(e.link, origin, file)
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
        assert len(rows) == 1 and rows[0].parts[0]["source_ref"]["key"] == "file.saved_ask"
        assert (await reader.get(TelegramActRow, saved.id)).state == "done"
    assert len(reads) == 1 and not started and e.bus.inbound_queue.empty()


async def test_interrupted_file_owner_becomes_uncertain_with_exact_retry(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(e.channel, "_download", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await e.channel.put_instance_file(e.link, origin, file)
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert (act.state, act.reason, len(act.operation_ids)) == ("uncertain", "interrupted", 1)
        callbacks = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "file_retry")))
        assert len(callbacks) == 1 and callbacks[0].target_id == act.id
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
    assert not reads and not started


async def test_new_file_outcome_receipt_survives_outbox_collection(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    next_download(request, file)
    await e.channel.execute_callback(callback)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, callback.target_id)
        receipt = act.continuation.get("delivery")
        assert receipt and receipt["operation_id"] == act.operation_ids[-1] and receipt["outbox_id"]
        await writer.execute(delete(TelegramOutboxRow).where(TelegramOutboxRow.id == receipt["outbox_id"]))
    turns = ChatTurns(e.channel._services)
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, receipt["outbox_id"]) is None
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:file-put:" + receipt["operation_id"])))
        assert not rows
    assert len(reads) == 2


async def test_completed_retry_without_outcome_recovers_after_input_and_control_collection(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    next_download(request, file)
    # The actual consumed press and native completion settle; the process ends
    # before the adapter can plan its reply.
    await e.channel._own_instance_file(e.link, origin, file, callback=callback)
    async with e.db.sessions.begin() as writer:
        await writer.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id == origin.message_id))
        await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.id == callback.id))
    turns = ChatTurns(e.channel._services)
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:file-put:" + act.operation_ids[-1])))
        assert len(rows) == 1 and rows[0].parts[0]["source_ref"]["key"] == "file.saved_ask"
        assert act.continuation["delivery"]["outbox_id"] == rows[0].id
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is not None
    assert len(reads) == 2 and e.bus.inbound_queue.empty()


@pytest.mark.parametrize("caption", ["", "Read this"])
async def test_unstarted_file_recovers_only_metadata_and_explicit_controls(file_admission, monkeypatch, caption):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, text=caption)
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert (act.state, act.reason) == ("uncertain", "interrupted")
        assert act.continuation["delivery"]["operation_id"] == act.operation_ids[-1]
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "file_retry", TelegramCallbackRow.target_id == act.id))
        assert retry is not None
        restart = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"notice:chat-restart:{e.link.chat_id}:{e.link.binding_generation}:{origin.message_id}"))
        assert bool(restart) == bool(caption)
    assert not reads and not started


async def test_startup_phase_ending_inside_question_writer_rolls_back_closure(file_admission, monkeypatch):
    from app.telegram_bots.file_puts import _FileReceiptGuard

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    await e.channel.put_instance_file(e.link, origin, file)
    services = startup(e, turns, monkeypatch)
    check, changed = _FileReceiptGuard.check, []

    async def release_after_insert(guard, writer):
        if await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))):
            changed.append(True)
            services._send_ready = True
        return await check(guard, writer)

    monkeypatch.setattr(_FileReceiptGuard, "check", release_after_insert)
    await turns.recover_startup()
    assert changed and len(reads) == 1 and not started
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert not mark.route_complete
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))


async def test_collected_reply_is_not_recreated_by_a_repeated_button_delivery(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    next_download(request, file)
    identity = await e.channel.execute_callback(callback)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, callback.target_id)
        identity = act.continuation["delivery"]["outbox_id"]
        await writer.execute(delete(TelegramOutboxRow).where(TelegramOutboxRow.id == identity))
    await e.channel.execute_callback(callback)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, identity) is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:file-put:" + act.operation_ids[-1])))
    assert len(reads) == 2


@pytest.mark.parametrize("change", ["home", "use", "policy"])
async def test_changed_authority_holds_file_recovery_without_other_reply(file_admission, monkeypatch, change):
    from datetime import UTC, datetime

    from sqlalchemy import update

    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    saved = await e.channel.put_instance_file(e.link, origin, file)
    startup(e, turns, monkeypatch)
    async with e.db.sessions.begin() as writer:
        if change == "home":
            await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
        elif change == "use":
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        else:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="off", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert not mark.route_complete and mark.thread_id is None
        assert (await reader.get(TelegramActRow, saved.id)).state == "done"
    assert len(reads) == 1 and not started


async def test_delivery_receipt_and_reply_roll_back_together_on_final_use_change(file_admission, monkeypatch):
    from sqlalchemy import update

    from app.telegram_bots.file_puts import _FileReceiptGuard
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, size=20_000_001)
    saved = await e.channel.put_instance_file(e.link, origin, file)
    original, changed = _FileReceiptGuard.check, []

    async def revoke_after_insert(guard, writer):
        if await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))) and not changed:
            changed.append(True)
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return await original(guard, writer)

    monkeypatch.setattr(_FileReceiptGuard, "check", revoke_after_insert)
    with pytest.raises(AgentDenied):
        await FileOutcomes(e.channel._services).plan(e.channel, e.link, origin, file)
    assert changed == [True] and not reads
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, saved.id)).continuation["delivery"] is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-put:%"))))


async def test_recovery_does_not_take_a_live_file_owner(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    original, entered, release = e.channel._download, asyncio.Event(), asyncio.Event()

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(e.channel, "_download", held)
    task = asyncio.create_task(e.channel.put_instance_file(e.link, origin, file))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await turns.recover_link(e.channel)
        async with e.db.sessions() as reader:
            act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
            assert act.state == "pending" and act.continuation["delivery"] is None
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
            assert not mark.route_complete
        release.set()
        assert (await task).state == "done"
        assert len(reads) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_legacy_terminal_retry_remains_unchanged_without_delivery_evidence(file_admission, monkeypatch):
    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    next_download(request, file)
    await e.channel._own_instance_file(e.link, origin, file, callback=callback)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, callback.target_id)
        act.continuation = {**{key: value for key, value in act.continuation.items() if key != "delivery"}, "version": 2}
        saved = dict(act.continuation)
    turns = ChatTurns(e.channel._services)
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        assert act.state == "done" and act.continuation == saved
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:file-put:" + act.operation_ids[-1])))
    assert len(reads) == 2


async def test_unfinished_legacy_input_cannot_republish_collected_retry_history(file_admission, monkeypatch):
    from deerflow.agent_instances.agent_files import AgentFileRefusal

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    services = e.channel._services

    async def refused(**kwargs):
        raise AgentFileRefusal("name-collision")

    monkeypatch.setattr(e.file_service.agent_files, "apply", refused)
    saved = await e.channel.put_instance_file(e.link, origin, file)
    initial = await FileOutcomes(services).plan(e.channel, e.link, origin, file)
    async with e.db.sessions() as reader:
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == saved.id))
        callback_id, group = control.id, control.group_id
    assert await services.callbacks.adopt(e.link, group, 1911, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=initial, presentation_version=0, check_current=lambda: e.active[0])
    callback = await services.callbacks.get(callback_id)
    next_download(request, file)
    await e.channel.execute_callback(callback)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, saved.id)
        child_id = act.continuation["delivery"]["outbox_id"]
        child_key = "notice:file-put:" + act.operation_ids[-1]
        await writer.execute(delete(TelegramOutboxRow).where(TelegramOutboxRow.id == child_id))
        act.continuation = {**{key: value for key, value in act.continuation.items() if key != "delivery"}, "version": 2}
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert not mark.route_complete
    startup(e, turns, monkeypatch)
    await turns.recover_startup()
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == child_key)))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert not mark.route_complete
        act = await reader.get(TelegramActRow, saved.id)
        assert act.continuation["version"] == 2 and act.state == "refused"
    assert len(reads) == 2 and not started

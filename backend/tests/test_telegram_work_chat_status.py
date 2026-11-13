"""Chat-started status has one durable root and is never concealed."""

from datetime import timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_bot_adapter import message
from test_telegram_work_event_intents import setup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable, start_args
from test_telegram_work_status_delivery import prepare
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.producer import _item
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInstancePolicyRow, TelegramOutboxRow


async def chat_start(e, monkeypatch, *, protected=False, before_activate=None):
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    await prepare_startable(e, monkeypatch)
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: RunRow.__table__.create(sync, checkfirst=True))
    if protected:
        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.sender.outbox.clock()))
    _peer, worker = await setup(e)
    if before_activate is not None:
        await before_activate(worker)
    act = await e.acts.activate(e.link, **start_args(e))
    assert act.state == "done"
    return act, worker


@pytest.mark.asyncio
async def test_activation_and_status_seed_commit_before_reader_catchup(work_delivery, monkeypatch):
    e = work_delivery
    act, worker = await chat_start(e, monkeypatch)
    attempt_id = act.result["activation_receipt"]["attempt_id"]
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + attempt_id))
        assert row is not None and row.state == "pending" and row.parts == []
        identity = row.id
    await worker.tick()
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + attempt_id)) == 1
        assert (await reader.get(TelegramOutboxRow, identity)).class_ == "progress"


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_chat_status_title_reaches_sdk_without_concealment(work_delivery, monkeypatch, protected):
    e = work_delivery
    act, worker = await chat_start(e, monkeypatch, protected=protected)
    await worker.tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + act.result["activation_receipt"]["attempt_id"]))
        item = _item(row)
    await prepare(e, item)
    message(e.channel.bot.request, identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and e.work["objective"] in calls[0].parameters["text"]
    assert calls[0].parameters["protect_content"] is protected and calls[0].parameters["disable_notification"] is True
    assert "Show" not in str(calls[0].parameters.get("reply_markup"))


@pytest.mark.asyncio
@pytest.mark.parametrize("seed_error", [RuntimeError, ValueError])
async def test_failed_seed_rolls_back_done_and_recovers_receipt_without_launch(work_delivery, monkeypatch, seed_error):
    from app.telegram_bots.work_event_intents import WorkEventIntents

    e = work_delivery
    real = WorkEventIntents.project

    async def lost(self, writer, link, event):
        await real(self, writer, link, event)
        raise seed_error("status transaction unavailable")

    monkeypatch.setattr(WorkEventIntents, "project", lost)
    # Keep the common fixture's explicit activation but inspect its unknown
    # result here rather than claiming the deliberately failed seed succeeded.
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    await prepare_startable(e, monkeypatch)
    first = await e.acts.activate(e.link, **start_args(e))
    assert first.state == "pending" and len(e.launches) == 1
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("progress:"))) == 0
        assert (await reader.get(TelegramActRow, first.id)).state == "pending"
    monkeypatch.setattr(WorkEventIntents, "project", real)
    recovered = await e.acts.check(first.id)
    assert recovered.state == "done" and len(e.launches) == 1
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("progress:"))) == 1


@pytest.mark.asyncio
async def test_reader_first_chat_status_is_direct_before_activation_reply(work_delivery, monkeypatch):
    from app.gateway import work_execution

    e = work_delivery
    real = work_execution.activate_work
    captured = []

    async def install(worker):
        async def reader_first(*args, **kwargs):
            result = await real(*args, **kwargs)
            await worker.tick()
            async with e.db.sessions() as reader:
                row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + result["activation_receipt"]["attempt_id"]))
                captured.append(_item(row))
            await prepare(e, captured[0])
            async with e.db.sessions() as reader:
                assert (await reader.get(TelegramOutboxRow, captured[0].id)).payload["work_news_basis"]["concealed"] is False
            return result

        monkeypatch.setattr(work_execution, "activate_work", reader_first)

    act, _worker = await chat_start(e, monkeypatch, protected=True, before_activate=install)
    assert len(e.launches) == 1
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, captured[0].id)
        assert row.payload["work_news_basis"]["source"]["work"]["activation_act_id"] == act.id
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == row.dedupe_key)) == 1


@pytest.mark.asyncio
async def test_chat_origin_change_after_provider_wait_prevents_sdk_send(work_delivery, monkeypatch):
    e = work_delivery
    act, _worker = await chat_start(e, monkeypatch, protected=True)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + act.result["activation_receipt"]["attempt_id"]))
        item = _item(row)
    await prepare(e, item)
    sources = e.channel._services.work_status_sources
    real, changes = sources.settings.get, []

    async def withdraw(*args, **kwargs):
        settings = await real(*args, **kwargs)
        if not changes:
            async with e.db.sessions.begin() as writer:
                saved = await writer.get(TelegramActRow, act.id)
                saved.body = {**saved.body, "expected_revision": 99}
            changes.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", withdraw)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changes and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_one_unavailable_status_seed_does_not_block_another_activation(work_delivery, monkeypatch):
    from app.telegram_bots.acts import ActsWorker
    from app.telegram_bots.work_event_intents import WorkEventIntents
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.agent_instances.work_execution_contract import ActivateWork

    e = work_delivery
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    await prepare_startable(e, monkeypatch)
    first_work = e.work["id"]
    real = WorkEventIntents.project

    async def unavailable(self, writer, link, event):
        if event.work_id == first_work:
            raise ValueError("one source is unavailable")
        return await real(self, writer, link, event)

    monkeypatch.setattr(WorkEventIntents, "project", unavailable)
    first = await e.acts.activate(e.link, **start_args(e))
    assert first.state == "pending"
    record = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=operation(), objective="Check another guide", success_criteria="Report its gaps"))
    identity = operation()
    thread = await e.http_app.state.agent_conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=identity)
    second = await e.acts._save_work_act(
        e.link,
        kind="activate",
        request_id=None,
        work_id=record["id"],
        request=ActivateWork(operation_id=identity, expected_revision=record["revision"], expected_assignment_revision=record["assignment_revision"], thread_id=thread["thread_id"]),
    )
    now = max(first.updated_at, second.updated_at) + timedelta(seconds=e.acts.outbox.config.acts.retry_seconds + 1)
    e.acts.outbox.clock = lambda: now
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    assert (await e.acts._work_record(first.id)).state == "pending"
    assert (await e.acts._work_record(second.id)).state == "done"
    assert len(e.launches) == 2

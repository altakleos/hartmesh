"""Stopped notices identify the exact intervening command and its actor."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def stopped(e, cause):
    e.channel._now = lambda: e.sender.outbox.clock()
    from test_agent_work_records import command
    from test_telegram_work_event_intents import activate, setup

    peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    e.stopped_execution = execution
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    extra = {"attempt_id": scope.attempt_id} if cause == "stop" else {"objective": "Changed assignment"} if cause == "edit" else {}
    await e.human.work.command(actor=peer, instance_id=e.instance.id, work_id=work["id"], request=command(current, cause, **extra), can_stop=True)
    run.status = "interrupted"
    await scope.finish(execution, run, settled=True)
    await worker.tick()
    key = {"stop": "attempt.stopped", "edit": "attempt.stopped_changed", "cancel": "attempt.stopped_withdrawn"}[cause]
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == key))
        assert row is not None
        return _item(row)


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["stop", "edit", "cancel"])
async def test_stopped_news_delivers_exact_colleague_cause(work_delivery, cause):
    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare

    e = work_delivery
    item = await stopped(e, cause)
    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://example.test"}
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=3201)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if e.dispatch_errors:
        raise e.dispatch_errors[0]
    sent = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert "event-peer@example.com" in sent.parameters["text"]
    assert sent.parameters["disable_notification"] is True
    assert bool(sent.parameters.get("reply_markup")) == (cause != "cancel")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["command", "cause_actor", "cause_time", "settlement", "run", "boolean"])
async def test_stop_notice_cannot_seal_or_reuse_malformed_cause(work_delivery, damage):
    from datetime import timedelta

    from sqlalchemy.orm.attributes import flag_modified

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_news_sources import WorkNewsReader
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    item = await stopped(e, "stop")
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    original = await reader.read(e.link, item)
    async with e.db.sessions.begin() as writer:
        event = await writer.get(AgentWorkEventRow, item.event_id)
        cause = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == item.work_id, AgentWorkEventRow.action == "stop"))
        if damage == "command":
            cause.request = {**cause.request, "operation_id": "another-operation"}
        elif damage == "cause_actor":
            cause.actor_id = e.link.owner_user_id
        elif damage == "cause_time":
            cause.created_at += timedelta(hours=2)
        elif damage == "settlement":
            event.request = {**event.request, "status": "succeeded"}
        elif damage == "run":
            event.request = {**event.request, "run_id": "other-run"}
        else:
            cause.result = {**cause.result, "assignment_revision": True}
            flag_modified(cause, "result")
    if damage == "cause_time":
        assert (await reader.read(e.link, item)).event_signature != original.event_signature
    else:
        with pytest.raises(ProductSourceRetired):
            await reader.read(e.link, item)


@pytest.mark.asyncio
async def test_stopped_notice_keeps_original_attempt_after_resume(work_delivery):
    from dataclasses import replace

    from test_work_execution import activation

    from app.telegram_bots.work_news_sources import WorkNewsReader
    from deerflow.agent_instances.work_execution import WorkExecution

    e = work_delivery
    item = await stopped(e, "stop")
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    execution = replace(e.stopped_execution, work=None)
    next_attempt = await WorkExecution(e.human.work).reserve(execution=execution, work_id=item.work_id, request=activation(execution, current))
    assert next_attempt.attempt_id != item.payload["work_event_intent"]["attempt_id"]
    source = await WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, item)
    assert source.key == "attempt.stopped" and source.values["person"] == "event-peer@example.com"


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["stop", "edit", "cancel"])
@pytest.mark.parametrize("change", ["timezone", "locale", "cause"])
async def test_stopped_notice_final_clock_and_cause_are_current(work_delivery, monkeypatch, change, cause):
    from _telegram_bot_test_support import ManualClock
    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare

    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    item = await stopped(e, cause)
    await prepare(e, item=item)
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    sources = e.channel._services.work_news_sources
    original, reads = sources.settings.get, []

    async def drift(*args, **kwargs):
        settings = await original(*args, **kwargs)
        reads.append(True)
        if len(reads) == 2:
            async with e.db.sessions.begin() as writer:
                if change != "cause":
                    row = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
                    setattr(row, change, "Asia/Tokyo" if change == "timezone" else "zh-CN")
                else:
                    event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == item.work_id, AgentWorkEventRow.action == cause))
                    event.actor_id = e.link.owner_user_id
        return settings

    monkeypatch.setattr(sources.settings, "get", drift)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(reads) >= 2 and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    if change != "cause":
        message(e.channel.bot.request, identity=3301)
        for _ in range(2):
            clock.advance(10)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            async with e.db.sessions() as reader:
                if (await reader.get(TelegramOutboxRow, item.id)).state == "sent":
                    break
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, item.id)
            assert row.state == "sent", (row.state, row.skip_reason, e.dispatch_errors)
    else:
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, item.id)).state == "skipped"


@pytest.mark.asyncio
async def test_stop_notice_reformats_when_midnight_passes_during_final_preparation(work_delivery, monkeypatch):
    from datetime import UTC, datetime

    from _telegram_bot_test_support import ManualClock
    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare

    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    item = await stopped(e, "stop")
    clock = ManualClock(datetime.now(UTC).replace(hour=23, minute=59, second=59, microsecond=0))
    e.sender.outbox.clock = clock.now
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).timezone = "UTC"
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, reads = sources.settings.get, []

    async def midnight(*args, **kwargs):
        settings = await original(*args, **kwargs)
        reads.append(True)
        if len(reads) == 2:
            clock.advance(2)
        return settings

    monkeypatch.setattr(sources.settings, "get", midnight)
    message(e.channel.bot.request, identity=3401)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    for _ in range(2):
        clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    sent = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert ", " in sent.parameters["text"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "sent"


@pytest.mark.asyncio
async def test_protected_stopped_news_show_uses_current_local_clock(work_delivery):
    from _telegram_bot_test_support import ManualClock
    from test_telegram_bot_adapter import message
    from test_telegram_work_news_delivery import prepare

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    item = await stopped(e, "stop")
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=3501)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, item.id)
        show = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        settings = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
        settings.timezone, settings.locale = "Asia/Tokyo", "zh-CN"
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    await e.channel._services.work_news_sources.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=3501, message_thread_id=None, guard=guard)
    message(e.channel.bot.request, "editMessageText", identity=3501)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "sent" and row.payload["work_news_basis"]["source"]["values"]["time_zone"] == "Asia/Tokyo"
        assert row.payload["work_news_basis"]["locale"] == "zh-CN"

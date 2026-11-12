"""Colleague news reaches Telegram only through its current source owner."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_news_sources import news
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def prepare(e, action="edit", *, item=None):
    from app.telegram_bots.work_news_delivery import WorkNewsPlans, WorkNewsSources
    from app.telegram_bots.work_news_sources import WorkNewsReader

    if item is None:
        item, _result, _peer = await news(e, action)
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            if row.id != item.id:
                row.state = "sent"
    plans = WorkNewsPlans(reader=WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=lambda: e.sender.outbox.clock()), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_news_sources = WorkNewsSources(plans)
    assert await plans.materialize(e.link, item.id)
    return plans, item.id


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["edit", "cancel", "reopen"])
async def test_current_colleague_notice_reaches_sdk_without_work_controls(work_delivery, action):
    e = work_delivery
    plans, identity = await prepare(e, action)
    assert await plans.materialize(e.link, identity)
    message(e.channel.bot.request, identity=2001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and "event-peer@example.com" in calls[0].parameters["text"]
    assert not calls[0].parameters.get("reply_markup")
    assert calls[0].parameters["disable_notification"] == (action != "cancel")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "work", "grant", "claim", "preferences"])
async def test_news_final_sdk_snapshot_refuses_source_or_authority_change(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    _plans, identity = await prepare(e)
    sources = e.channel._services.work_news_sources
    original, changed = sources.settings.get, []

    async def withdraw(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                if change == "event":
                    (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "another-person"
                elif change == "work":
                    (await writer.get(AgentWorkRow, row.work_id)).objective = "Different assignment title"
                elif change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
                elif change == "claim":
                    row.claim_id = "different-claim"
                else:
                    (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).notify = "none"
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", withdraw)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ok", "missing", "wrong_ack"])
async def test_protected_colleague_show_edits_actual_teaser_without_starting_work(work_delivery, mode):
    from datetime import UTC, datetime

    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    _plans, identity = await prepare(e)
    clock.advance(2)
    message(e.channel.bot.request, identity=2101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["text"] == "There is news about an assignment."
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert show and show.message_id == 2101 and not show.payload["presentation_pending"]
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=2101)
    if mode == "missing":
        e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    else:
        message(e.channel.bot.request, "editMessageText", identity=9999 if mode == "wrong_ack" else 2101)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if mode == "missing":
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "failed" and row.failed_reason == "work-news-refresh-required"
            assert row.payload["presentation_version"] == 1 and row.parts[0]["message_ids"] == [2101]
        return
    if mode == "wrong_ack":
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "failed" and row.failed_reason == "invalid-work-news-receipt"
        return
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert [call.endpoint for call in calls] == ["sendMessage", "editMessageText"]
    assert calls[-1].parameters["message_id"] == 2101
    assert "event-peer@example.com" in calls[-1].parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply"
        assert (await reader.get(TelegramCallbackRow, show.id)).closed_reason == "revealed"


async def long_review_news(e):
    from uuid import uuid4

    from test_agent_work_records import command
    from test_telegram_work_event_intents import activate, setup

    from app.telegram_bots.producer import _item
    from deerflow.agent_instances.work_execution_contract import ReportWork

    peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified findings"))
    await scope.finish(execution, run, settled=True)
    await worker.tick()
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    await e.human.work.command(actor=peer, instance_id=e.instance.id, work_id=work["id"], request=command(current, "changes_requested", note="x" * 4070))
    await worker.tick()
    async with e.db.sessions() as reader:
        return _item(await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "result.changes")))


@pytest.mark.asyncio
async def test_multipart_news_show_retains_cleanup_and_consumed_proof(work_delivery):
    from datetime import UTC, datetime, timedelta

    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.retention import OUTBOX_RETENTION_DAYS, _sweep_outbox
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    _plans, identity = await prepare(e, item=await long_review_news(e))
    clock.advance(2)
    sdk = e.channel.bot.request
    message(sdk, identity=2201)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=2201)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["work_news_basis"]["cleanup_id"]
        assert len(row.parts) == 3
    message(sdk, "editMessageReplyMarkup", identity=2201)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        cleanup_id = row.payload["work_news_basis"]["cleanup_id"]
        (await writer.get(TelegramOutboxRow, cleanup_id)).closed_at = clock.now() - timedelta(days=OUTBOX_RETENTION_DAYS + 1)
    await _sweep_outbox(e.db.sessions, clock.now(), e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, cleanup_id) is not None
    for message_id in (2202, 2203):
        message(sdk, identity=message_id)
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in sdk.calls if call.endpoint in {"sendMessage", "editMessageReplyMarkup"}]
    assert [call.endpoint for call in calls] == ["sendMessage", "editMessageReplyMarkup", "sendMessage", "sendMessage"]
    assert "x" * 4070 in "".join(call.parameters.get("text", "") for call in calls)
    assert calls[-1].parameters["reply_markup"]["inline_keyboard"][0][0]["url"].startswith("https://app.example.test/")
    assert all(call.parameters["protect_content"] for call in calls if call.endpoint == "sendMessage")
    callbacks = e.channel._services.callbacks
    await callbacks.sweep(callbacks.clock() + timedelta(days=callbacks.config.callbacks.max_age_days + 1))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert await reader.get(TelegramCallbackRow, show.id) is not None


@pytest.mark.asyncio
async def test_policy_upgrade_preserves_frozen_news_recipe_while_retiring_send(work_delivery):
    from datetime import UTC, datetime

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    _plans, identity = await prepare(e)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["work_news_basis"]["history"][-1]["basis"]["source"]["protected"] is False
        assert row.payload["work_news_basis"]["source"]["protected"] is True
        assert row.parts[0]["protect_content"] is False


@pytest.mark.asyncio
async def test_show_rejects_parallel_group_not_acknowledged_on_teaser(work_delivery):
    from copy import deepcopy
    from datetime import UTC, datetime
    from uuid import uuid4

    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow

    e = work_delivery
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    _plans, identity = await prepare(e)
    message(e.channel.bot.request, identity=2301)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        original = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        clone = {column.name: deepcopy(getattr(original, column.name)) for column in TelegramCallbackRow.__table__.columns}
        clone.update(id=uuid4().hex, group_id=uuid4().hex)
        writer.add(TelegramCallbackRow(**clone))
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    with pytest.raises(CallbackRefused):
        await e.channel._services.work_news_sources.recovery.reveal(e.link, clone["id"], chat_id=e.link.chat_id, message_id=2301, message_thread_id=None, guard=guard)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert (await reader.get(TelegramCallbackRow, clone["id"])).consumed_at is None
        assert (await reader.get(TelegramCallbackRow, original.id)).consumed_at is None

"""Work status uses actual durable delivery and the recorded Telegram SDK."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_status import status_attempt
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def prepare(e, item):
    from app.telegram_bots.work_news_delivery import WorkNewsSources
    from app.telegram_bots.work_status_delivery import WorkStatusPlans

    plans = WorkStatusPlans(reader=e.reader, outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_status_sources = WorkNewsSources(plans)
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id != item.id)):
            row.state = "sent"
    assert await plans.materialize(e.link, item.id)
    return plans


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_status_first_delivery_is_silent_and_has_no_execution_controls(work_delivery, protected):
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    item, _work, _execution, _scope, _run, _worker = await status_attempt(e)
    if protected:
        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.sender.outbox.clock()))
    await prepare(e, item)
    message(e.channel.bot.request, identity=2101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["disable_notification"] is True
    assert calls[0].parameters["protect_content"] is protected
    if protected:
        assert calls[0].parameters["text"] == "Work has started on an assignment."
        assert [button["text"] for row in calls[0].parameters["reply_markup"]["inline_keyboard"] for button in row] == ["Show"]
    else:
        assert "Report findings" in calls[0].parameters["text"] and "Starting" in calls[0].parameters["text"]
        assert not calls[0].parameters.get("reply_markup")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "sent" and row.message_id == 2101


@pytest.mark.asyncio
async def test_status_progress_before_first_send_refreshes_the_saved_plan(work_delivery):
    from _telegram_bot_test_support import ManualClock

    e = work_delivery
    item, _work, execution, scope, _run, _worker = await status_attempt(e)
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.channel._now = clock.now
    await prepare(e, item)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="progress", statement="Checked the complete guide."))
    message(e.channel.bot.request, identity=2102)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert len(calls) == 1 and "Checked the complete guide." in calls[0].parameters["text"], (e.dispatch_errors, row.state, row.failed_reason, row.skip_reason, row.parts)


@pytest.mark.asyncio
async def test_protected_status_show_reads_current_progress_without_starting_work(work_delivery):
    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.channel._now = clock.now
    item, _work, execution, scope, _run, _worker = await status_attempt(e)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    await prepare(e, item)
    message(e.channel.bot.request, identity=2103)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert show and show.message_id == 2103
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow.id)))
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="progress", statement="Checked the newest guide."))
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=2103)
    message(e.channel.bot.request, "editMessageText", identity=2103)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and "Checked the newest guide." in calls[0].parameters["text"]
    assert calls[0].parameters["message_id"] == 2103
    async with e.db.sessions() as reader:
        assert list(await reader.scalars(select(AgentWorkAttemptRow.id))) == attempts
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "sent"


@pytest.mark.asyncio
async def test_status_refresh_cannot_replace_an_already_frozen_progress_event(work_delivery):
    from _telegram_bot_test_support import ManualClock

    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    item, _work, execution, scope, _run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    operation = uuid4().hex
    await scope.report(execution, ReportWork(operation_id=operation, expected_revision=current["revision"], action="progress", statement="Checked the admitted guide."))
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.channel._now = clock.now
    await prepare(e, item)
    async with e.db.sessions.begin() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
        event.request = {**event.request, "statement": "Coherently replaced history."}
        event.result = {**event.result, "progress": "Coherently replaced history."}
    message(e.channel.bot.request, identity=2104)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "claim", "grant", "preferences"])
async def test_status_final_sdk_check_revalidates_source_and_authority(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    item, _work, _execution, _scope, _run, _worker = await status_attempt(e)
    await prepare(e, item)
    sources = e.channel._services.work_status_sources
    original, changed = sources.settings.get, []

    async def withdraw(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                if change == "event":
                    (await writer.get(AgentWorkEventRow, item.event_id)).actor_id = "different-person"
                elif change == "claim":
                    (await writer.get(TelegramOutboxRow, item.id)).claim_id = "different-claim"
                elif change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
                else:
                    (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", withdraw)
    message(e.channel.bot.request, identity=2105)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == (1 if change == "preferences" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["key", "source", "basis"])
async def test_malformed_status_family_is_a_permanent_refusal(work_delivery, damage):
    from app.telegram_bots.producer import PermanentDeliveryError

    e = work_delivery
    item, _work, _execution, _scope, _run, _worker = await status_attempt(e)
    await prepare(e, item)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, item.id)
        basis = row.payload["work_news_basis"]
        if damage == "key":
            basis = {**basis, "source": {**basis["source"], "key": None}}
        elif damage == "source":
            basis = {**basis, "source": []}
        else:
            basis = []
        row.payload = {**row.payload, "work_news_basis": basis}
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert e.dispatch_errors and isinstance(e.dispatch_errors[-1], PermanentDeliveryError)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "failed"

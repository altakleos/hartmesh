"""One-hour reminders concern the saved answer and never resume Work."""

from uuid import uuid4

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_news_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow, TelegramPersonSettingsRow


async def reminder(e, *, other_requester=False):
    from test_telegram_work_event_intents import activate, setup

    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.agent_instances.work_execution_contract import ReportWork

    # Delivery tests own quiet-hour preconditions; policy cases enable them below.
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).quiet_enabled = False
    e.channel._now = lambda: e.sender.outbox.clock()
    peer, worker = await setup(e)
    actor = e.actor
    try:
        if other_requester:
            e.actor = peer
        _work, execution, scope, run = await activate(e)
    finally:
        e.actor = actor
    e.reminder_execution = execution
    current = (await scope.context(execution))["work"]
    blocked = await scope.report(
        execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="request_input", statement="Which guide?", purpose="information", reason="Need a source", expected_response="A reference")
    )
    await scope.finish(execution, run, settled=True)
    req = await e.human.get(actor=e.actor, request_id=blocked["human_input_request_id"])
    await e.human.respond(
        actor=e.actor, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=req["request_revision"], expected_assignment_revision=req["assignment_revision"], text="Use the published guide.")
    )
    await worker.tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.class_ == "reminder"))
        assert row is not None
        return _item(row)


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [20, 86420])
async def test_reminder_waits_an_hour_then_sends_silently_with_only_a_web_link(work_delivery, delay):
    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item = await reminder(e)
    await prepare(e, item=item)
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    clock.advance(3590)
    assert await e.sender.tick() == 0
    clock.advance(delay)
    message(e.channel.bot.request, identity=2701)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["disable_notification"] is True
    assert 'Ready to resume: "Report findings"' in calls[0].parameters["text"]
    buttons = [button for row in calls[0].parameters["reply_markup"]["inline_keyboard"] for button in row]
    assert len(buttons) == 1 and buttons[0]["text"] == "Open assignment" and "callback_data" not in buttons[0]
    assert await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "sent" and row.class_ == "reminder" and row.payload["kind"] == "resume_reminder"


@pytest.mark.asyncio
@pytest.mark.parametrize("quiet,summary,notify,expected", [(True, True, "all", "quiet-hours"), (True, False, "questions", "sent"), (False, False, "questions", "sent"), (False, True, "none", "notifications-off")])
async def test_reminder_obeys_quiet_summary_and_question_preferences(work_delivery, quiet, summary, notify, expected):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e)
    await prepare(e, item=item)
    async with e.db.sessions.begin() as writer:
        settings = await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)
        settings.quiet_enabled, settings.quiet_start_minute, settings.quiet_end_minute = quiet, 0, 1439
        settings.summary_enabled, settings.notify = summary, notify
    clock.advance(3610)
    if expected == "sent":
        message(e.channel.bot.request, identity=2711)
    await e.sender.tick()
    await e.sender.drain()
    if expected == "sent":
        assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert (row.state, row.skip_reason) == (("sent", None) if expected == "sent" else ("skipped", expected))
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == (expected == "sent")
    assert all(call.parameters["disable_notification"] is True for call in calls)


async def change_work(e, item, action):
    from dataclasses import replace

    from test_agent_work_records import command
    from test_work_execution import activation

    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    if action == "resume":
        execution = replace(e.reminder_execution, work=None)
        await e.reminder_execution.work.service.reserve(execution=execution, work_id=item.work_id, request=activation(execution, current))
    else:
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id, request=command(current, action, **({"objective": "Changed work"} if action == "edit" else {})))


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["resume", "edit", "cancel"])
async def test_reminder_skips_after_actual_work_transition_at_final_boundary(work_delivery, monkeypatch, action):
    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e)
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, changed = sources.settings.get, []

    async def drift(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            await change_work(e, item, action)
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", drift)
    clock.advance(3610)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert (row.state, row.skip_reason) == ("skipped", "resumed")


@pytest.mark.asyncio
async def test_reminder_due_time_comes_from_saved_response_not_mutable_queue_time(work_delivery):
    from datetime import datetime

    from app.telegram_bots.notifications import aware

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e)
    await prepare(e, item=item)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, item.id)).not_before = clock.now()
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "pending" and aware(row.not_before) == datetime.fromisoformat(row.payload["work_news_basis"]["source"]["values"]["due"])


@pytest.mark.asyncio
async def test_reminder_targets_answerer_when_someone_else_started_work(work_delivery):
    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e, other_requester=True)
    await prepare(e, item=item)
    clock.advance(3610)
    message(e.channel.bot.request, identity=2721)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "sent"


@pytest.mark.asyncio
async def test_protected_reminder_show_stays_silent_even_when_summary_becomes_enabled(work_delivery):
    from datetime import UTC, datetime

    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
        settings = await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)
        settings.quiet_enabled, settings.quiet_start_minute, settings.quiet_end_minute, settings.summary_enabled = True, 0, 1439, False
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2731)
    clock.advance(3610)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, item.id)
        assert row.state == "sent"
        show = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).summary_enabled = True
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=2731)
    message(e.channel.bot.request, "editMessageText", identity=2731)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert len(calls) == 2 and calls[0].parameters["text"] == "There is news about an assignment."
    assert calls[0].parameters["protect_content"] is True and calls[0].parameters["disable_notification"] is True
    assert "Ready to resume:" in calls[-1].parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "sent" and row.class_ == "reply" and row.silent is True


@pytest.mark.asyncio
async def test_reminder_final_settings_fence_observes_summary_enable(work_delivery, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e)
    async with e.db.sessions.begin() as writer:
        settings = await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)
        settings.quiet_enabled, settings.quiet_start_minute, settings.quiet_end_minute, settings.summary_enabled = True, 0, 1439, False
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, reads = sources.settings.get, []

    async def drift(*args, **kwargs):
        settings = await original(*args, **kwargs)
        reads.append(True)
        if len(reads) == 2:
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).summary_enabled = True
        return settings

    monkeypatch.setattr(sources.settings, "get", drift)
    clock.advance(3610)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(reads) >= 2 and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert (row.state, row.skip_reason) == ("skipped", "quiet-hours")

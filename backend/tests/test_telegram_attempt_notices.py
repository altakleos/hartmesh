"""Attempt notifications use the exact settlement and its recorded run."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_news_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def ended(e, kind, *, reason=None):
    from test_telegram_work_event_intents import activate, setup

    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.persistence.run.model import RunRow

    e.channel._now = lambda: e.sender.outbox.clock()
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    e.ended_execution, e.ended_run = execution, run
    run.status = {"attempt.no_result": "success", "attempt.failed": "error", "attempt.stopped_restart": "interrupted", "interrupted": "error"}[kind]
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: RunRow.__table__.create(sync, checkfirst=True))
        await connection.run_sync(lambda sync: RunEventRow.__table__.create(sync, checkfirst=True))
    async with e.db.sessions.begin() as writer:
        writer.add(RunRow(run_id=run.run_id, thread_id=run.thread_id, user_id=run.user_id, status=run.status, idempotency_key=run.idempotency_key, metadata_json={**run.metadata, "agent_instance_id": e.instance.id}, stop_reason=reason))
    if kind == "interrupted":
        assert await scope.service.settle_contained(instance_id=e.instance.id, host_id="fixture-host", reason="run-orphaned", attachment_id=None, live_run_ids=frozenset()) == [scope.attempt_id]
    else:
        await scope.finish(execution, run, settled=True)
    await worker.tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == kind))
        assert row is not None
        return _item(row)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,first", [("attempt.no_result", "Ended without a result:"), ("attempt.failed", "Failed:"), ("attempt.stopped_restart", "Stopped:"), ("interrupted", "Interrupted:")])
async def test_attempt_notice_delivers_from_actual_terminal_work_event(work_delivery, kind, first):
    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item = await ended(e, kind)
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2801)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["text"].startswith(first)
    assert calls[0].parameters["disable_notification"] is False
    buttons = [button for row in calls[0].parameters["reply_markup"]["inline_keyboard"] for button in row]
    assert buttons[0]["text"] == "Open assignment"
    assert all("callback_data" not in button for button in buttons)
    if kind == "attempt.no_result":
        assert buttons[-1]["text"] == "Open conversation" and e.ended_run.thread_id in buttons[-1]["url"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.state == "sent" and row.class_ == "interrupt"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["busy", "run-limit", "restarting", "starting-elsewhere", "start-failed", "files-full", "storage-busy", "unrecognized"])
async def test_failed_notice_uses_exact_recorded_run_reason(work_delivery, reason):
    e = work_delivery
    item = await ended(e, "attempt.failed", reason=reason)
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2811)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    text = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][0].parameters["text"]
    assert text.startswith("Failed:" if reason == "unrecognized" else "Did not start:")
    assert "unrecognized" not in text


@pytest.mark.asyncio
@pytest.mark.parametrize("manager", [False, True])
async def test_files_full_advice_and_navigation_follow_current_manager_grant(work_delivery, manager):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item = await ended(e, "attempt.failed", reason="files-full")
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 7 if manager else 3
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2821)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][0]
    assert ("Delete files on the web" in call.parameters["text"]) is manager
    buttons = [button["text"] for row in call.parameters["reply_markup"]["inline_keyboard"] for button in row]
    assert buttons == (["Open Files", "Open assignment"] if manager else ["Open assignment"])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event_run", "event_status", "actor", "requester", "run_owner", "run_thread", "run_attempt", "run_status"])
async def test_attempt_notice_rejects_changed_terminal_provenance(work_delivery, change):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
    from deerflow.persistence.run.model import RunRow

    e = work_delivery
    item = await ended(e, "attempt.failed", reason="busy")
    async with e.db.sessions.begin() as writer:
        event = await writer.get(AgentWorkEventRow, item.event_id)
        attempt = await writer.get(AgentWorkAttemptRow, event.request["attempt_id"])
        run = await writer.get(RunRow, attempt.run_id)
        if change == "event_run":
            event.request = {**event.request, "run_id": "foreign-run"}
        elif change == "event_status":
            event.request = {**event.request, "status": "succeeded"}
        elif change == "actor":
            event.actor_kind = "human"
        elif change == "requester":
            attempt.requester_id = "foreign-requester"
        elif change == "run_owner":
            run.user_id = "foreign-owner"
        elif change == "run_thread":
            run.thread_id = "foreign-thread"
        elif change == "run_attempt":
            run.metadata_json = {**run.metadata_json, "agent_work_attempt_id": "foreign-attempt"}
        else:
            run.status = "success"
    with pytest.raises(ProductSourceRetired):
        await prepare(e, item=item)


@pytest.mark.asyncio
async def test_protected_interruption_show_uses_acknowledged_teaser(work_delivery):
    from datetime import UTC, datetime

    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await ended(e, "interrupted")
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2831)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=2831)
    message(e.channel.bot.request, "editMessageText", identity=2831)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert len(calls) == 2 and calls[0].parameters["text"] == "An assignment was interrupted."
    assert calls[0].parameters["protect_content"] is True and calls[-1].parameters["message_id"] == 2831
    assert "The service restarted while" in calls[-1].parameters["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["reason", "manager", "claim"])
async def test_attempt_notice_final_source_and_grant_snapshot(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.run.model import RunRow

    e = work_delivery
    item = await ended(e, "attempt.failed", reason="files-full")
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, changed = sources.settings.get, []

    async def drift(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                if change == "reason":
                    (await writer.get(RunRow, e.ended_run.run_id)).stop_reason = "busy"
                elif change == "manager":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 3
                else:
                    (await writer.get(TelegramOutboxRow, item.id)).claim_id = "replaced-claim"
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", drift)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_no_result_navigation_keeps_original_attempt_after_later_activation(work_delivery):
    from uuid import uuid4

    from test_work_execution import activation

    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item = await ended(e, "attempt.no_result")
    conversations = e.http_app.state.agent_conversations
    chat = await conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await conversations.execution(actor=e.actor, thread_id=chat["thread_id"])
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    await e.ended_execution.work.service.reserve(execution=execution, work_id=item.work_id, request=activation(execution, current))
    assert chat["thread_id"] != e.ended_run.thread_id
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2841)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][0]
    urls = [button["url"] for row in call.parameters["reply_markup"]["inline_keyboard"] for button in row]
    assert any(e.ended_run.thread_id in url for url in urls) and all(chat["thread_id"] not in url for url in urls)

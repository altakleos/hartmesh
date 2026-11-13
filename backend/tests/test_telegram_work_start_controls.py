"""A full acknowledged Work preview admits one versioned Start intent."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_view_delivery import prepare_view
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


async def start_control(e, monkeypatch):
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    await prepare_startable(e, monkeypatch)
    plans, _ = await prepare_view(e)
    await dispatch(e, 3501)
    async with e.db.sessions() as reader:
        detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"]))
    e.identity = await plans.open(e.link, detail.id, query_id="start-preview", chat_id=e.link.chat_id, message_id=3501, message_thread_id=None, guard=await guard_for(e))
    await dispatch(e, 3502)
    async with e.db.sessions() as reader:
        button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_start", TelegramCallbackRow.target_id == e.work["id"]))
    assert button and not e.dispatch_errors, e.dispatch_errors
    from app.telegram_bots.work_start_controls import WorkStartAdmissions

    return plans, WorkStartAdmissions(plans, e.acts), button


async def stage(e, controller, button):
    return await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_acknowledged_start_stages_once_and_refresh_hides_fresh_start(work_delivery, monkeypatch):
    from app.telegram_bots.work_views import work_buttons

    e = work_delivery
    plans, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    assert act.id == button.id and act.kind == "activate" and act.attempts == 0 and not e.launches
    replay = await stage(e, controller, button)
    assert replay.body == act.body
    current = await plans.reader.detail(e.link, e.work["id"])
    controls = work_buttons(current.record, owner_id=e.link.owner_user_id, permissions=current.permissions, held=current.held, request=current.request)
    assert not any(item["kind"] == "work_start" for line in controls for item in line)
    assert (await e.acts.check(act.id)).state == "done" and len(e.launches) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["assignment", "progress"])
async def test_start_binds_displayed_assignment_but_accepts_new_progress_revision(work_delivery, monkeypatch, change):
    e = work_delivery
    _plans, controller, button = await start_control(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        work = await writer.get(AgentWorkRow, e.work["id"])
        work.revision += 1
        if change == "assignment":
            work.objective, work.assignment_revision = "Changed assignment", work.assignment_revision + 1
        else:
            work.progress = "New progress only"
    if change == "assignment":
        with pytest.raises(AgentConflict):
            await stage(e, controller, button)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, button.id) is None
    else:
        act = await stage(e, controller, button)
        assert act.body["expected_revision"] == e.work["revision"] + 1
        assert act.body["expected_assignment_revision"] == e.work["assignment_revision"]
    assert not e.launches


@pytest.mark.asyncio
async def test_start_marker_without_its_original_journal_cannot_reconstruct_intent(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _plans, controller, button = await start_control(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)))
        for row in rows:
            row.payload = {**row.payload, "work_action_id": button.id}
    with pytest.raises(CallbackRefused):
        await stage(e, controller, button)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None
    assert not e.launches
    current = await controller.plans.reader.detail(e.link, e.work["id"])
    from app.telegram_bots.work_views import work_buttons

    assert not any(item.get("kind") == "work_start" for line in work_buttons(current.record, owner_id=e.link.owner_user_id, permissions=current.permissions, held=current.held, request=current.request) for item in line)


@pytest.mark.asyncio
async def test_start_retired_during_conversation_preparation_rolls_back_journal(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _plans, controller, button = await start_control(e, monkeypatch)
    conversations = e.http_app.state.agent_conversations
    real = conversations.create

    async def retire(*args, **kwargs):
        conversation = await real(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            callback = await writer.get(TelegramCallbackRow, button.id)
            callback.consumed_at, callback.closed_reason = e.clock.now(), "superseded"
        return conversation

    monkeypatch.setattr(conversations, "create", retire)
    with pytest.raises(CallbackRefused):
        await stage(e, controller, button)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("journal", ["pending", "missing", "malformed"])
async def test_unresolved_start_retains_original_control_and_full_ack(work_delivery, monkeypatch, journal):
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    _plans, controller, button = await start_control(e, monkeypatch)
    if journal == "missing":
        async with e.db.sessions.begin() as writer:
            for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)):
                row.payload = {**row.payload, "work_action_id": button.id}
    else:
        await stage(e, controller, button)
        if journal == "malformed":
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramActRow, button.id)
                row.state, row.result = "done", ["unqualified receipt"]
    later = e.clock.now() + timedelta(days=controller.callbacks.config.callbacks.max_age_days + 40)
    await controller.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=controller.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        original = await reader.get(TelegramOutboxRow, e.identity)
        assert original is not None and original.parts[-1]["message_ids"] == [3502]

    current = await controller.plans.reader.detail(e.link, e.work["id"])
    assert current.record["activation_pending"]


@pytest.mark.asyncio
async def test_completed_start_receipt_releases_reservation_and_allows_later_resume(work_delivery, monkeypatch):
    from app.telegram_bots.work_start_retention import reserved_starts
    from app.telegram_bots.work_views import work_buttons
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    plans, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    done = await e.acts.check(act.id)
    assert done.state == "done" and set(done.result["activation_receipt"]) == {"operation_id", "attempt_id"}
    async with e.db.sessions.begin() as writer:
        attempt = await writer.get(AgentWorkAttemptRow, done.result["activation_receipt"]["attempt_id"])
        attempt.status, attempt.settled_at = "failed", e.clock.now()
    source = await plans.reader.detail(e.link, e.work["id"])
    assert source.record["activation_pending"] is None
    assert any(item.get("kind") == "work_start" for line in work_buttons(source.record, owner_id=e.link.owner_user_id, permissions=source.permissions, held=source.held, request=source.request) for item in line)
    async with e.db.sessions() as reader:
        groups, anchors = await reserved_starts(reader)
    assert button.group_id not in groups and e.identity not in anchors


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["phase", "identity", "principal", "assignment"])
async def test_malformed_original_start_binding_holds_refreshed_controls(work_delivery, monkeypatch, changed):
    from uuid import uuid4

    e = work_delivery
    plans, controller, button = await start_control(e, monkeypatch)
    original = await stage(e, controller, button)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, original.id)
        act.state, act.reason = "refused", "denied"
        if changed == "phase":
            act.state = "draft"
        elif changed == "principal":
            act.telegram_user_id += 1
        elif changed == "assignment":
            act.body = {**act.body, "expected_assignment_revision": act.body["expected_assignment_revision"] + 1}
        else:
            identity = uuid4().hex
            act.id, act.body = identity, {**act.body, "operation_id": identity}
            for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)):
                row.payload = {**row.payload, "work_action_id": identity}
    assert (await plans.reader.detail(e.link, e.work["id"])).record["activation_pending"]


@pytest.mark.asyncio
async def test_refused_retry_keeps_an_unresolved_original_launch_promise(work_delivery, monkeypatch):
    from unittest.mock import AsyncMock

    from app.gateway import services
    from app.telegram_bots.work_start_retention import reserved_starts
    from deerflow.agent_instances.contract import ExecutionUnavailable
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    _plans, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=RuntimeError("Unknown admission")))
    assert (await e.acts.check(act.id)).state == "pending"
    monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=ExecutionUnavailable("operator-hold")))
    assert (await e.acts.check(act.id)).state == "refused"
    async with e.db.sessions() as reader:
        attempt = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == act.id))
        assert attempt.status == "starting" and attempt.run_id is None
        groups, anchors = await reserved_starts(reader)
    assert button.group_id in groups and e.identity in anchors


@pytest.mark.asyncio
async def test_definitive_refusal_without_reservation_allows_fresh_start(work_delivery, monkeypatch):
    from unittest.mock import AsyncMock

    from app.telegram_bots.work_start_retention import reserved_starts
    from deerflow.agent_instances.contract import ExecutionUnavailable

    e = work_delivery
    plans, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=ExecutionUnavailable("operator-hold")))
    assert (await e.acts.check(act.id)).state == "refused"
    assert (await plans.reader.detail(e.link, e.work["id"])).record["activation_pending"] is None
    async with e.db.sessions() as reader:
        groups, anchors = await reserved_starts(reader)
    assert button.group_id not in groups and e.identity not in anchors

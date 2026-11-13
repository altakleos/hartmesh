"""Acknowledged Stop controls keep the original attempt through admission."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_activation import seeded_attempt
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_view_delivery import prepare_view
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def stop_control(e):
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    attempt_id, captured, manager = await seeded_attempt(e)
    plans, _ = await prepare_view(e)
    await dispatch(e, 3701)
    button = await selected(e, "work_details")
    e.identity = await plans.open(e.link, button.id, query_id="stop-preview", chat_id=e.link.chat_id, message_id=3701, message_thread_id=None, guard=await guard_for(e))
    await dispatch(e, 3702)
    stop = await selected(e, "stop_attempt")
    assert stop and not e.dispatch_errors, e.dispatch_errors
    from app.telegram_bots.work_stop_controls import WorkStopAdmissions

    return plans, WorkStopAdmissions(plans, e.acts), stop, attempt_id, captured, manager


async def stage(e, controller, button):
    return await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_one_stop_press_stages_once_then_cancels_only_original_run(work_delivery):
    e = work_delivery
    _plans, controller, button, attempt_id, captured, manager = await stop_control(e)
    act = await stage(e, controller, button)
    assert act.id == button.id and act.body["attempt_id"] == attempt_id and act.attempts == 0
    assert (await stage(e, controller, button)).body == act.body
    manager.cancel.assert_not_awaited()
    result = await e.acts.check(act.id)
    assert result.state == "done" and result.stop_dispatch["state"] == "requested"
    manager.cancel.assert_awaited_once_with(captured.run_id)


@pytest.mark.asyncio
async def test_stop_preparation_retries_progress_drift_without_an_admitted_body(work_delivery, monkeypatch):
    e = work_delivery
    plans, controller, button, attempt_id, _captured, manager = await stop_control(e)
    real = plans.reader.prepare_detail
    changed = []

    async def progress(*args, **kwargs):
        check = await real(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                work = await writer.get(AgentWorkRow, e.work["id"])
                work.revision, work.progress = work.revision + 1, "Changed during preparation"
                changed.append(work.revision)
        return check

    monkeypatch.setattr(plans.reader, "prepare_detail", progress)
    act = await stage(e, controller, button)
    assert act.body["attempt_id"] == attempt_id and act.body["expected_revision"] == changed[0]
    assert act.attempts == 0
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_stop_control_never_retargets_a_successor_attempt(work_delivery):
    from datetime import timedelta

    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    _plans, controller, button, attempt_id, _captured, original_manager = await stop_control(e)
    async with e.db.sessions.begin() as writer:
        old = await writer.get(AgentWorkAttemptRow, attempt_id)
        old.status, old.settled_at, old.created_at = "failed", e.clock.now(), e.clock.now() - timedelta(seconds=10)
    successor_id, _successor, successor_manager = await seeded_attempt(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkAttemptRow, successor_id)).created_at = e.clock.now()
    act = await stage(e, controller, button)
    assert act.body["attempt_id"] == attempt_id and act.reason == "no-running-attempt"
    assert act.stop_dispatch is None and act.attempts == 0
    original_manager.cancel.assert_not_awaited()
    successor_manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_press_reads_progress_revision_at_the_press(work_delivery):
    e = work_delivery
    _plans, controller, button, _attempt_id, _captured, manager = await stop_control(e)
    async with e.db.sessions.begin() as writer:
        work = await writer.get(AgentWorkRow, e.work["id"])
        work.revision, work.progress = work.revision + 1, "Updated since the preview"
        revision = work.revision
    act = await stage(e, controller, button)
    assert act.body["expected_revision"] == revision
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_orphan_stop_marker_never_reconstructs_a_missing_original_journal(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _plans, controller, button, _attempt_id, _captured, manager = await stop_control(e)
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)):
            row.payload = {**row.payload, "work_action_id": button.id}
    with pytest.raises(CallbackRefused):
        await stage(e, controller, button)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None
    manager.cancel.assert_not_awaited()

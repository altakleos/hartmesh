"""Cancelling logical retries does not undo a proved original command.

Physical follow-ups use SQL journals and the offline manager fixture.
"""

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from app.telegram_bots.acts import ActsWorker
from deerflow.persistence.agent_instances.work import AgentWorkEventRow


@pytest.mark.asyncio
async def test_cancel_before_attempt_never_issues_the_original_command(actions):
    e = actions
    act = await e.acts.stage_cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    result = await e.acts.cancel_management(act.id)
    assert result.cancel_requested_at is not None and result.state == "refused" and result.reason == "cancelled" and result.attempts == 0
    assert (await e.acts.check(act.id)).state == "refused"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id)) == 0


@pytest.mark.asyncio
async def test_cancelled_unknown_command_has_no_logical_retry(actions, monkeypatch):
    e = actions
    calls = []

    async def unknown(**kwargs):
        calls.append(kwargs["request"].operation_id)
        raise OSError("Synthetic unknown command response")

    monkeypatch.setattr(e.human.work, "command", unknown)
    act = await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    result = await e.acts.cancel_management(act.id)
    assert result.cancel_requested_at is not None and result.state == "uncertain" and result.reason == "cancel-unconfirmed"
    await e.acts.check(act.id)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    assert calls == [act.id]


@pytest.mark.asyncio
async def test_known_late_command_keeps_its_independent_physical_followup(actions, monkeypatch):
    e = actions
    _attempt_id, record, manager = await seeded_attempt(e)
    command, calls = e.human.work.command, []

    async def lost(**kwargs):
        calls.append(kwargs["request"].operation_id)
        await command(**kwargs)
        raise OSError("Synthetic lost committed command response")

    monkeypatch.setattr(e.human.work, "command", lost)
    act = await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    assert act.state == "pending"
    result = await e.acts.cancel_management(act.id)
    assert result.state == "done" and result.cancel_requested_at is not None
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    manager.cancel.assert_awaited_once_with(record.run_id)
    assert calls == [act.id]

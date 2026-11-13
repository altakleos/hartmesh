"""Command receipts and exact original-attempt cancellation remain independent.

Uses the existing SQL journal fixture and an offline run-manager double; these
cases do not claim native launch or worker provenance.
"""

from datetime import UTC, datetime

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.work_edit_drafts import WorkEditDrafts
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


async def manage(e, action, identity):
    if action == "cancel":
        return await e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"])
    draft = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"], draft=DraftAssignment(objective="Review the full guide", success_criteria="List verified corrections"))
    return await e.acts.edit(e.link, draft.id, operation_id=identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "edit"])
async def test_management_receipt_recovers_only_its_original_cancellation(actions, monkeypatch, action):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    command, calls = e.human.work.command, []

    async def lost(**kwargs):
        calls.append(kwargs["request"].operation_id)
        await command(**kwargs)
        raise OSError("Synthetic lost withdrawal response")

    monkeypatch.setattr(e.human.work, "command", lost)
    identity = operation()
    pending = await manage(e, action, identity)
    assert pending.state == "pending"
    manager.cancel.assert_not_awaited()
    result = await e.acts.check(identity)
    assert result.state == "done" and result.stop_dispatch["state"] == "requested"
    assert result.stop_dispatch["attempt_id"] == attempt_id and result.stop_dispatch["run_id"] == record.run_id
    manager.cancel.assert_awaited_once_with(record.run_id)
    assert calls == [identity]
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "edit"])
async def test_management_waits_for_original_late_binding_and_actual_settlement(actions, action):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e, bound=False)
    result = await manage(e, action, operation())
    assert result.state == "done" and result.stop_dispatch["state"] == "ready"
    manager.cancel.assert_not_awaited()
    record.run_id = operation()
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkAttemptRow, attempt_id)).run_id = record.run_id
    manager.get.return_value = record
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "requested" and result.stop_dispatch["run_id"] == record.run_id
    manager.cancel.assert_awaited_once_with(record.run_id)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(AgentWorkAttemptRow, attempt_id)
        row.status, row.settled_at = "cancelled", datetime.now(UTC)
    settled = await e.acts.resume_stop_dispatch(result)
    assert settled.stop_dispatch["state"] == "settled"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "edit"])
async def test_lost_management_response_never_cancels_a_successor(actions, monkeypatch, action):
    e = actions
    original_id, _record, original_manager = await seeded_attempt(e)
    command = e.human.work.command

    async def lost(**kwargs):
        await command(**kwargs)
        raise OSError("Synthetic lost management response")

    monkeypatch.setattr(e.human.work, "command", lost)
    identity = operation()
    result = await manage(e, action, identity)
    assert result.state == "pending"
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkAttemptRow, original_id)
        original.status, original.settled_at = "cancelled", datetime.now(UTC)
    successor_id, _successor, successor_manager = await seeded_attempt(e)
    result = await e.acts.check(identity)
    assert result.state == "done" and result.stop_dispatch["state"] == "settled"
    assert result.stop_dispatch["attempt_id"] == original_id
    original_manager.cancel.assert_not_awaited()
    successor_manager.cancel.assert_not_awaited()
    async with e.db.sessions() as reader:
        assert (await reader.get(AgentWorkAttemptRow, successor_id)).status == "running"

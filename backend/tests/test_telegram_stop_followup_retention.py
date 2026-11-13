"""Physical promises survive history expiry until their original settles.

These cases use SQL journal/offline-manager fixtures, not native workers.
"""

from datetime import UTC, datetime, timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("settled,legacy", [(False, True), (True, False), (False, False)])
async def test_ended_followup_history_needs_authoritative_original_settlement(actions, monkeypatch, settled, legacy):
    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    finish = e.acts._finish_stop

    async def withdraw_authority(*args, **kwargs):
        result = await finish(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 2
        return result

    monkeypatch.setattr(e.acts, "_finish_stop", withdraw_authority)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "ended"
    manager.cancel.assert_not_awaited()
    async with e.db.sessions.begin() as writer:
        if legacy:
            row = await writer.get(TelegramActRow, result.id)
            row.stop_dispatch = {key: value for key, value in row.stop_dispatch.items() if key != "ended_from"}
        if settled:
            original = await writer.get(AgentWorkAttemptRow, attempt_id)
            original.status, original.settled_at = "cancelled", datetime.now(UTC)
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, result.id) is None) is settled


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["stop", "cancel"])
@pytest.mark.parametrize("phase", ["ready", "requested", "uncertain"])
async def test_original_settlement_closes_history_after_binding_retirement(actions, kind, phase):
    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    if phase == "ready":
        manager.get.return_value = None
    elif phase == "uncertain":
        manager.cancel.side_effect = OSError("Synthetic unknown cancellation")
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id) if kind == "stop" else await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    if phase == "uncertain":
        result = await e.acts.resume_stop_dispatch(result)
        result = await e.acts.resume_stop_dispatch(result)
    assert result.state == "done" and result.stop_dispatch["state"] == phase
    await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkAttemptRow, attempt_id)
        original.status, original.settled_at = "cancelled", datetime.now(UTC)
    calls = manager.cancel.await_count
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, result.id) is None
    assert manager.cancel.await_count == calls

"""Physical follow-up recovery preserves acknowledgements and original scope.

Uses SQL journal fixtures and an offline manager; no native launch is claimed.
"""

from datetime import UTC, datetime

import pytest
from _storage_spaces_test_support import operation
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_requested_cancellation_stays_acknowledged_across_authority_loss(actions):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.stop_dispatch["state"] == "requested"
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 2
    held = await e.acts.resume_stop_dispatch(result)
    assert held.stop_dispatch["state"] == "requested"
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
        row = await writer.get(AgentWorkAttemptRow, attempt_id)
        row.status, row.settled_at = "cancelled", datetime.now(UTC)
    settled = await e.acts.resume_stop_dispatch(held)
    assert settled.stop_dispatch["state"] == "settled"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_ended_unrequested_followup_requires_a_fresh_explicit_check(actions, monkeypatch):
    from app.telegram_bots.acts import ActsWorker

    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    finish, changed = e.acts._finish_stop, []

    async def revoke_after_receipt(*args, **kwargs):
        result = await finish(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 2
        return result

    monkeypatch.setattr(e.acts, "_finish_stop", revoke_after_receipt)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "ended"
    assert result.stop_dispatch.get("ended_from") == "ready"
    manager.cancel.assert_not_awaited()
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
    await ActsWorker(e.acts).tick()
    manager.cancel.assert_not_awaited()
    resumed = await e.acts.check(result.id)
    assert resumed.stop_dispatch["state"] == "requested"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_uncertain_followup_reconciles_original_settlement_without_dispatch(actions):
    from app.telegram_bots.acts import ActsWorker

    e = actions
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    attempt_id, _record, manager = await seeded_attempt(e)
    manager.cancel.side_effect = OSError("Synthetic unknown cancellation")
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    result = await e.acts.resume_stop_dispatch(result)
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "uncertain" and manager.cancel.await_count == 3
    async with e.db.sessions.begin() as writer:
        row = await writer.get(AgentWorkAttemptRow, attempt_id)
        row.status, row.settled_at = "cancelled", datetime.now(UTC)
    clock.advance(e.acts.outbox.config.acts.retry_seconds + 1)
    await ActsWorker(e.acts).tick()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, result.id)).stop_dispatch["state"] == "settled"
    assert manager.cancel.await_count == 3

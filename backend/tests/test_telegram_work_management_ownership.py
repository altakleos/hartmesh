"""Retired binding and legacy delivery owners cannot claim saved-work commands."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow


@pytest.mark.asyncio
async def test_pending_withdraw_never_crosses_its_original_binding_generation(actions):
    e = actions
    identity = operation()
    staged = await e.acts.stage_cancel(e.link, operation_id=identity, work_id=e.work["id"])
    assert staged.attempts == 0
    async with e.db.sessions.begin() as writer:
        link = await writer.get(TelegramBotLinkRow, e.link.id)
        link.binding_generation += 1
    result = await e.acts.check(identity)
    assert result.state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 0
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert current["status"] != "cancelled"


@pytest.mark.asyncio
async def test_internal_management_journal_is_not_claimed_by_human_input_outcome_owner(actions, monkeypatch):
    e = actions
    done = await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    assert done.state == "done"
    inspected = []
    real = e.acts._work_record

    async def record(identity):
        if identity == done.id:
            inspected.append(identity)
        return await real(identity)

    monkeypatch.setattr(e.acts, "_work_record", record)
    plans = SimpleNamespace(acts=e.acts, plan=AsyncMock())
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    recovery = WorkOutcomeRecovery(plans)
    await recovery.tick(e.link, guard=guard)
    assert inspected == []
    plans.plan.assert_not_awaited()

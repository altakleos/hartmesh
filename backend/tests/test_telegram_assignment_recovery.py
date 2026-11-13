"""Draft expiry and admitted no-due children keep distinct recovery authority."""

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from test_telegram_assignment_draft_store import draft
from test_telegram_work_actions import actions as actions

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.callbacks import CallbackRefused
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow


@pytest.mark.asyncio
async def test_get_rejects_draft_that_expires_across_provider_await(actions, monkeypatch):
    from app.gateway import authz

    e = actions
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    repo, _, source = await draft(e)
    real = authz.resolve_route_permissions
    changed = []

    async def advance(*args, **kwargs):
        held = await real(*args, **kwargs)
        if not changed:
            changed.append(True)
            clock.advance(repo.config.callbacks.draft_ttl_hours * 3600 + 1)
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", advance)
    with pytest.raises(CallbackRefused):
        await repo.get(e.link, source.id)
    assert changed


@pytest.mark.asyncio
async def test_expired_parent_allows_explicit_recovery_of_its_admitted_no_due_child(actions, monkeypatch):
    from app.telegram_bots.work_delegate_actions import no_due_id

    e = actions
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    proposed = DraftAssignment(objective="Review the guide", success_criteria="Explain verified gaps", due_text="By Friday evening", due_at=clock.now() + timedelta(days=2))
    _, _, source = await draft(e, model=proposed)
    identity = uuid4().hex
    await e.acts.stage_delegate(e.link, source.id, operation_id=identity)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    real = e.human.work.delegate

    async def defer_child(**kwargs):
        if kwargs["request"].operation_id == no_due_id(identity):
            raise RuntimeError("temporary child receipt absence")
        return await real(**kwargs)

    monkeypatch.setattr(e.human.work, "delegate", defer_child)
    first = await e.acts.check(identity)
    assert first.state == "pending" and first.operation_ids == (no_due_id(identity),)
    child = await e.acts._work_record(no_due_id(identity))
    assert child.state == "pending" and child.attempts == 1
    clock.advance(3601)
    parent = await e.acts._attempt_work(await e.acts._work_record(identity), explicit=False)
    assert parent.state == "uncertain"
    monkeypatch.setattr(e.human.work, "delegate", real)
    result = await e.acts.check(identity)
    assert result.state == "done"
    assert result.result["delegate_child_id"] == no_due_id(identity)


@pytest.mark.asyncio
async def test_conclusive_no_due_child_refusal_is_reported_as_unsaved(actions, monkeypatch):
    from sqlalchemy import func, select

    from app.telegram_bots.work_delegate_actions import no_due_id
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = actions
    now = e.acts.outbox.clock()
    _, _, source = await draft(e, model=DraftAssignment(objective="Review the guide", success_criteria="Explain verified gaps", due_text="Friday", due_at=now + timedelta(days=2)))
    identity = uuid4().hex
    await e.acts.stage_delegate(e.link, source.id, operation_id=identity)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    real = e.human.work.delegate

    async def refuse(**kwargs):
        if kwargs["request"].operation_id == no_due_id(identity):
            raise AgentDenied("Current permission required", reason="grant-required")
        return await real(**kwargs)

    monkeypatch.setattr(e.human.work, "delegate", refuse)
    result = await e.acts.check(identity)
    assert result.state == "refused" and result.reason == "grant-required"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
    monkeypatch.setattr(e.human.work, "delegate", real)
    assert (await e.acts.check(identity)).state == "done"


@pytest.mark.asyncio
async def test_no_due_fallback_never_truncates_a_full_criterion(actions):
    from sqlalchemy import func, select

    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = actions
    _, _, source = await draft(e, model=DraftAssignment(objective="Review the guide", success_criteria="C" * 4090, due_text="By Friday evening", due_at=e.acts.outbox.clock() + timedelta(days=2)))
    identity = uuid4().hex
    await e.acts.stage_delegate(e.link, source.id, operation_id=identity)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    result = await e.acts.check(identity)
    assert result.state == "refused" and result.reason == "draft-too-long" and not result.operation_ids
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_refused_private_draft_remains_non_executable_and_receipt_only(actions):
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = actions
    _, _, source = await draft(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, source.id)
        row.state, row.reason = "refused", "discarded"
    assert (await e.acts.check(source.id)).reason == "discarded"
    assert (await e.acts.reconcile_receipt(source.id)).reason == "discarded"


@pytest.mark.asyncio
async def test_private_drafts_stay_out_of_human_input_outcome_recovery(actions):
    from types import SimpleNamespace

    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = actions
    _, _, source = await draft(e)
    plans = SimpleNamespace(acts=e.acts, plan=AsyncMock())
    recovery = WorkOutcomeRecovery(plans)
    guard = SimpleNamespace(prepare=AsyncMock(), current=lambda: None)
    await recovery._tick(e.link, guard)
    plans.plan.assert_not_awaited()

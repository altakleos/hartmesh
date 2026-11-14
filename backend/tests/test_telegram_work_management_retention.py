"""Management history expires as a complete, receipt-qualified family.

Physical cases use SQL journals and an offline manager, not native workers.
"""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.retention import sweep_rows
from app.telegram_bots.work_edit_drafts import WorkEditDrafts
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def edit(e):
    draft = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"], draft=DraftAssignment(objective="Review the complete guide", success_criteria="List verified corrections"))
    result = await e.acts.edit(e.link, draft.id, operation_id=operation())
    assert result.state == "done"
    return draft, result


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["current", "unbind", "remove"])
async def test_completed_edit_expires_both_journals_after_binding_retirement(actions, transition):
    e = actions
    root, result = await edit(e)
    if transition == "unbind":
        await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
    elif transition == "remove":
        await e.db.links.remove(e.link.id, actor_id=e.link.owner_user_id)
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, root.id) is None
        assert await reader.get(TelegramActRow, result.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("settled", [False, True])
async def test_withdraw_retention_requires_exact_original_settlement(actions, settled):
    e = actions
    identity, _record, _manager = await seeded_attempt(e)
    result = await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    assert result.state == "done"
    if settled:
        async with e.db.sessions.begin() as writer:
            original = await writer.get(AgentWorkAttemptRow, identity)
            original.status, original.settled_at = "cancelled", datetime.now(UTC)
        result = await e.acts.resume_stop_dispatch(result)
        assert result.stop_dispatch["state"] == "settled"
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, result.id) is None) is settled


@pytest.mark.asyncio
async def test_unadmitted_edit_expires_without_mutating_saved_work(actions):
    e = actions
    root = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"])
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(hours=25), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, root.id)
        assert (saved.state, saved.reason, saved.attempts, saved.operation_ids) == ("refused", "draft-expired", 0, [])
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert current["revision"] == root.body["expected_revision"]
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, root.id) is None


@pytest.mark.asyncio
async def test_equivalent_due_date_no_effect_history_expires(actions):
    e = actions
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    due = datetime.now(UTC) + timedelta(days=2)
    current = await e.human.work.command(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=e.work["id"],
        request=WorkCommand(operation_id=operation(), expected_revision=current["revision"], expected_assignment_revision=current["assignment_revision"], action="edit", due_at=due),
    )
    proposal = DraftAssignment(objective=current["objective"], success_criteria=current["success_criteria"], due_at=due.astimezone(timezone(timedelta(hours=2))))
    root = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"], draft=proposal)
    result = await e.acts.edit(e.link, root.id, operation_id=operation())
    assert result.state == "refused" and result.reason == "unchanged" and result.attempts == 0
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, root.id) is None
        assert await reader.get(TelegramActRow, result.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing-forward", "unknown-marker", "missing-root"])
async def test_damaged_edit_family_remains_held(actions, damage):
    e = actions
    root, result = await edit(e)
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramActRow, root.id)
        if damage == "missing-forward":
            saved.operation_ids = []
        elif damage == "unknown-marker":
            saved.continuation = {**saved.continuation, "unknown": {"version": 1}}
        else:
            await writer.delete(saved)
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, result.id) is not None
        if damage != "missing-root":
            assert await reader.get(TelegramActRow, root.id) is not None

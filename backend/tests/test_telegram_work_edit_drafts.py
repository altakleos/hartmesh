"""Saved assignment edits use an immutable private draft before their command."""

from datetime import UTC, datetime, timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from app.telegram_bots.assign import DraftAssignment
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_edit_applies_only_its_stored_draft_and_records_one_command(actions):
    from app.telegram_bots.work_edit_drafts import WorkEditDrafts

    e = actions
    proposal = DraftAssignment(objective="Review the complete guide", success_criteria="List verified corrections")
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    draft = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"], draft=proposal)
    assert draft.state == "draft" and not draft.operation_ids
    assert (await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"]))["revision"] == before["revision"]
    identity = operation()
    result = await e.acts.edit(e.link, draft.id, operation_id=identity)
    replay = await e.acts.edit(e.link, draft.id, operation_id=identity)
    assert result.state == replay.state == "done" and result.body == replay.body
    assert result.body["objective"] == proposal.objective and result.body["success_criteria"] == proposal.success_criteria
    assert (result.body["expected_revision"], result.body["expected_assignment_revision"]) == (before["revision"], before["assignment_revision"])
    request = await e.human.get(actor=e.actor, request_id=e.request["id"])
    assert request["state"] == "closed" and request["closed_reason"] == "superseded"
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, draft.id)).operation_ids == [identity]
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 1


@pytest.mark.asyncio
async def test_private_edit_copies_an_overdue_date_without_clearing_it(actions):
    from app.telegram_bots.work_edit_drafts import WorkEditDrafts

    e = actions
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    due = datetime.now(UTC) - timedelta(days=2)
    await e.human.work.command(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=e.work["id"],
        request=WorkCommand(operation_id=operation(), expected_revision=before["revision"], expected_assignment_revision=before["assignment_revision"], action="edit", due_at=due),
    )
    draft = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"])
    assert DraftAssignment.model_validate(draft.body["draft"]).due_at == due


@pytest.mark.asyncio
async def test_already_admitted_edit_draft_cannot_own_another_operation(actions):
    from app.telegram_bots.work_edit_drafts import WorkEditDrafts

    e = actions
    draft = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"], draft=DraftAssignment(objective="Review the whole guide", success_criteria="List verified corrections"))
    assert (await e.acts.edit(e.link, draft.id, operation_id=operation())).state == "done"
    with pytest.raises(ValueError):
        await e.acts.edit(e.link, draft.id, operation_id=operation())


@pytest.mark.asyncio
async def test_unchanged_edit_does_not_supersede_live_request_or_advance_work(actions):
    from app.telegram_bots.work_edit_drafts import WorkEditDrafts

    e = actions
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    draft = await WorkEditDrafts(e.acts).create(e.link, operation_id=operation(), work_id=e.work["id"])
    identity = operation()
    result = await e.acts.edit(e.link, draft.id, operation_id=identity)
    assert result.state == "refused" and result.reason == "unchanged" and result.attempts == 0
    after = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert (after["revision"], after["assignment_revision"]) == (before["revision"], before["assignment_revision"])
    pending = await e.human.get(actor=e.actor, request_id=e.request["id"])
    assert pending["state"] == e.request["state"] and pending["closed_reason"] is None
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 0

"""Explicit delegation uses the stored draft and immutable service receipts."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_assignment_draft_store import draft
from test_telegram_work_actions import actions as actions

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_explicit_delegate_stages_saved_words_without_creating_work(actions):
    e = actions
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    saved = await e.acts.stage_delegate(e.link, source.id, operation_id=identity)
    assert saved.state == "pending" and saved.attempts == 0 and saved.id == identity
    assert saved.body["objective"] == source.draft.objective and saved.body["success_criteria"] == source.draft.success_criteria
    assert "due_at" not in saved.body
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        root = await reader.get(TelegramActRow, source.id)
        assert root.operation_ids == [identity]
    assert await e.acts.stage_delegate(e.link, source.id, operation_id=identity) == saved


@pytest.mark.asyncio
async def test_saved_draft_delegates_once_as_the_bound_person(actions):
    e = actions
    _repo, _inbox, source = await draft(e)
    identity = uuid4().hex
    result = await e.acts.delegate(e.link, source.id, operation_id=identity)
    assert result.state == "done" and result.result["creator_id"] == e.link.owner_user_id
    assert result.result["objective"] == source.draft.objective
    assert (await e.acts.delegate(e.link, source.id, operation_id=identity)).result == result.result
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 1


@pytest.mark.asyncio
async def test_unknown_delegate_response_recovers_only_the_exact_receipt(actions, monkeypatch):
    e = actions
    _repo, _inbox, source = await draft(e)
    real, calls = e.human.work.delegate, []

    async def lose(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        await real(*args, **kwargs)
        raise RuntimeError("Response was lost after commit")

    monkeypatch.setattr(e.human.work, "delegate", lose)
    identity = uuid4().hex
    first = await e.acts.delegate(e.link, source.id, operation_id=identity)
    assert first.state == "pending" and first.attempts == 1
    recovered = await e.acts.check(identity)
    assert recovered.state == "done" and calls == [identity]
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2


@pytest.mark.asyncio
async def test_lost_manage_drops_due_under_one_distinct_child(actions):
    import hashlib
    from datetime import timedelta

    e = actions
    proposed = DraftAssignment(objective="Review the guide", success_criteria="Explain verified gaps", due_text="By Friday evening", due_at=datetime.now(UTC) + timedelta(days=2))
    _repo, _inbox, source = await draft(e, model=proposed)
    identity = uuid4().hex
    staged = await e.acts.stage_delegate(e.link, source.id, operation_id=identity)
    assert "due_at" in staged.body
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    result = await e.acts.check(identity)
    assert result.state == "done"
    child_id = hashlib.sha256((identity + ":no-due").encode()).hexdigest()[:32]
    assert result.operation_ids == (child_id,)
    child = await e.acts._work_record(child_id)
    assert child.state == "done" and "due_at" not in child.body and child.body["success_criteria"].endswith("By Friday evening")
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2
        events = list(await reader.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id.in_([identity, child_id]))))
        assert len(events) == 1 and events[0].operation_id == child_id and "due_at" not in events[0].request
    assert (await e.acts.check(identity)).state == "done"


@pytest.mark.asyncio
async def test_a_reserved_draft_cannot_be_delegated_again_under_another_id(actions):
    e = actions
    _repo, _inbox, source = await draft(e)
    await e.acts.stage_delegate(e.link, source.id, operation_id=uuid4().hex)
    with pytest.raises(ValueError, match="draft-already-admitted"):
        await e.acts.stage_delegate(e.link, source.id, operation_id=uuid4().hex)


@pytest.mark.asyncio
async def test_draft_root_is_not_executable_through_check_or_worker(actions):
    from app.telegram_bots.work_actions import ActsWorker

    e = actions
    _repo, _inbox, source = await draft(e)
    assert (await e.acts.check(source.id)).state == "draft"
    await ActsWorker(e.acts).tick()
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1

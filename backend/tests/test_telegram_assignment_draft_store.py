"""Assignment drafts preserve explicit input and create no executable Work."""

from datetime import timedelta

import pytest
from _telegram_bot_test_support import ManualClock, make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow


async def draft(e, *, model=None, identity=4601):
    from app.telegram_bots.assignment_drafts import AssignmentDrafts

    words = "Review the current guide and explain the verified gaps"
    update = make_update(identity, account=e.link.chat_id, text="/assign " + words)
    inbox, _ = await e.acts.outbox.inbox.accept(e.link, update, sanitized_input={"command": "assign", "text": words})
    repo = AssignmentDrafts(e.acts)
    value = await repo.create(e.link, inbox, draft=model or DraftAssignment(objective="Review the guide", success_criteria="Explain verified gaps"), source="command")
    return repo, inbox, value


@pytest.mark.asyncio
async def test_assignment_draft_is_durable_but_creates_no_work(actions):
    e = actions
    repo, inbox, value = await draft(e)
    assert value.state == "draft" and value.draft.objective == "Review the guide"
    replay = await repo.create(e.link, inbox, draft=value.draft, source="command")
    assert replay == value
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1  # Fixture's pre-existing Work only.
        row = await reader.get(TelegramActRow, value.id)
        assert row.kind == "delegate" and row.attempts == 0 and row.first_attempt_at is None
        assert row.work_id is row.request_id is None


@pytest.mark.asyncio
async def test_draft_for_reader_preserves_deadline_words_without_due_field(actions):
    from datetime import UTC, datetime

    e = actions
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    proposed = DraftAssignment(objective="Review the guide", success_criteria="Explain verified gaps", due_text="By Friday evening", due_at=datetime.now(UTC) + timedelta(days=2))
    _repo, _inbox, value = await draft(e, model=proposed)
    assert value.draft.due_at is None and value.due_note
    assert value.draft.success_criteria.endswith("By Friday evening")
    assert value.original == proposed


@pytest.mark.asyncio
async def test_expired_assignment_draft_cannot_be_reopened(actions):
    from app.telegram_bots.callbacks import CallbackRefused

    e = actions
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    repo, _inbox, value = await draft(e)
    clock.advance(repo.config.callbacks.draft_ttl_hours * 3600 + 1)
    with pytest.raises(CallbackRefused, match="stale.draft"):
        await repo.get(e.link, value.id)


@pytest.mark.asyncio
async def test_draft_replay_does_not_replace_previously_presented_words(actions):
    e = actions
    repo, inbox, value = await draft(e)
    with pytest.raises(ValueError, match="draft-source-conflict"):
        await repo.create(e.link, inbox, draft=DraftAssignment(objective="Different words", success_criteria="Different criteria"), source="command")
    assert (await repo.get(e.link, value.id)) == value


@pytest.mark.asyncio
async def test_original_inbox_custody_is_rechecked_in_the_draft_writer(actions, monkeypatch):
    from app.telegram_bots.assignment_drafts import AssignmentDrafts
    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.producer import ProductSourceRetired

    e = actions
    update = make_update(4610, account=e.link.chat_id, text="/assign Review the guide")
    inbox, _ = await e.acts.outbox.inbox.accept(e.link, update, sanitized_input={"command": "assign", "text": "Review the guide"})
    repo = AssignmentDrafts(e.acts)
    original, changed = e.acts.authority.prepare_live_owner, []

    async def prepare(*args, **kwargs):
        result = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
                row.sanitized_input = {"command": "assign", "text": "Changed input"}
        return result

    monkeypatch.setattr(e.acts.authority, "prepare_live_owner", prepare)
    with pytest.raises((AuthorityUnavailable, ProductSourceRetired)):
        await repo.create(e.link, inbox, draft=DraftAssignment(objective="Review the guide", success_criteria="Explain gaps"), source="command")
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 0
    assert changed

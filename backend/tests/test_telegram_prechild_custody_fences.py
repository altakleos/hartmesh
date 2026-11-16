"""A damaged parent marker cannot erase a genuinely admitted child owner."""

from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from test_telegram_composite_execution_outcomes import db as db
from test_telegram_composite_execution_outcomes import uncertain_child
from test_telegram_composite_execution_outcomes import work_delivery as work_delivery
from test_telegram_composite_execution_outcomes import work_pages as work_pages

from app.telegram_bots.work_actions import thaw
from app.telegram_bots.work_start_conversations import context_child_id
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["direct", "reverse", "native"])
async def test_unplanned_refusal_retains_real_child_custody(work_delivery, monkeypatch, owner):
    e = work_delivery
    _plans, parent_id, child_id = await uncertain_child(e, monkeypatch, runless=owner == "native")
    child = await e.acts._work_record(child_id)
    namespace = [child_id, context_child_id(child_id)]
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramActRow, parent_id)
        original = parent.body, parent.result, parent.attempts
        # Corrupt only bookkeeping after the actual public APIs created the
        # child. No successful owner or Native reservation is synthesized.
        state = thaw(parent.continuation)
        state["activation_continuation"] = {**state["activation_continuation"], "phase": "ready", "child_body": None}
        state["activation_continuation"].pop("refusal", None)
        parent.continuation = state
        parent.operation_ids = [identity for identity in parent.operation_ids if identity != child_id]
        if owner == "reverse":
            (await writer.get(TelegramActRow, child_id)).id = uuid4().hex
        elif owner == "native":
            attempts = list(await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id.in_(namespace))))
            assert attempts, "the real service must have reserved Native ownership"
            await writer.execute(delete(TelegramActRow).where(TelegramActRow.id.in_(namespace)))
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
    before = await e.acts._work_record(parent_id)
    result = await e.acts.resume_continuation(parent_id)
    assert result.continuation == before.continuation
    assert (thaw(result.body), thaw(result.result), result.attempts) == original
    assert result.operation_ids == before.operation_ids
    assert not e.launches
    if owner == "direct":
        assert await e.acts._work_record(child_id) == child

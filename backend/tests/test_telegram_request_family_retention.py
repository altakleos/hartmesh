"""Independent offline SQL retention probes; malformed locators grant no authority."""

from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from test_telegram_request_continuation_retention import db as db
from test_telegram_request_continuation_retention import work_delivery as work_delivery
from test_telegram_request_continuation_retention import work_pages as work_pages
from test_telegram_voice_continuations import pending_voice_child
from test_telegram_work_outcome_delivery import clear_outcome_cleanup
from test_telegram_work_preview_recovery import dispatch

from app.gateway import work_execution
from app.telegram_bots.work_request_continuation_retention import request_dependencies
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def closed_voice(e, monkeypatch):
    acts, parent, original = await pending_voice_child(e, monkeypatch)
    monkeypatch.setattr(work_execution, "activate_work", original)
    completed = await acts.resume_continuation(parent.id)
    assert completed.continuation["activation_continuation"]["phase"] == "done"
    child_id = completed.continuation["activation_continuation"]["child_id"]
    e.outcome_identity = completed.continuation["request_activation"]["outcome_id"]
    await clear_outcome_cleanup(e)
    await dispatch(e, 1181)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions.begin() as writer:
        attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        attempt.status, attempt.settled_at = "cancelled", e.clock()
    later = e.clock() + timedelta(days=401)
    async with e.db.sessions() as reader:
        baseline = await request_dependencies(reader, later, max_age_days=180)
    assert parent.id in baseline.expired_history and child_id in baseline.expired_history
    return parent.id, child_id, later


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["extra_member", "reverse_same_scope", "reverse_foreign_scope"])
async def test_closed_request_does_not_release_damaged_or_shared_family(work_delivery, monkeypatch, damage):
    e = work_delivery
    parent_id, child_id, later = await closed_voice(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        if damage == "extra_member":
            parent = await writer.get(TelegramActRow, parent_id)
            parent.operation_ids = [*parent.operation_ids, uuid4().hex]
        else:
            # Controlled raw reverse witness, deliberately not a canonical Stop
            # or a source qualification claim. An unresolved same-scope owner
            # must retain referenced evidence; foreign scopes must not pin it.
            child = await writer.get(TelegramActRow, child_id)
            values = {column.name: deepcopy(getattr(child, column.name)) for column in TelegramActRow.__table__.columns}
            values.update(id=uuid4().hex, kind="stop", state="pending", body={}, continuation=None, operation_ids=[child_id], result=None, attempts=0, first_attempt_at=None)
            if damage == "reverse_foreign_scope":
                values["binding_generation"] += 1
            writer.add(TelegramActRow(**values))
    async with e.db.sessions() as reader:
        held = await request_dependencies(reader, later, max_age_days=180)
    if damage == "reverse_foreign_scope":
        assert {parent_id, child_id} <= held.expired_history
    else:
        assert {parent_id, child_id} <= held.acts
        assert not {parent_id, child_id} & held.expired_history

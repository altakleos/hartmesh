from datetime import timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions

from app.telegram_bots.retention import sweep_rows
from app.telegram_bots.work_actions import _ACT_SCOPE
from app.telegram_bots.work_stop_retries import child_identity
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_damaged_foreign_stop_child_keeps_management_parent_evidence(actions):
    e = actions
    parent = await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    assert parent.state == "done"
    child_id = child_identity(parent.id, 2)
    async with e.db.sessions.begin() as writer:
        writer.add(
            TelegramActRow(
                id=child_id,
                **{**{key: getattr(parent, key) for key in _ACT_SCOPE}, "kind": "stop"},
                body=WorkCommand(operation_id=child_id, expected_revision=1, expected_assignment_revision=1, action="stop", attempt_id=operation()).model_dump(mode="json", exclude_unset=True),
                state="pending",
                operation_ids=[],
                attempts=0,
                continuation={"version": 1, "stop_retry": {"parent_id": parent.id, "ordinal": 2}},
                created_at=e.acts.outbox.clock(),
                updated_at=e.acts.outbox.clock(),
            )
        )
    await sweep_rows(e.db.sessions, e.acts.outbox.clock() + timedelta(days=401), callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, child_id) is not None
        assert await reader.get(TelegramActRow, parent.id) is not None

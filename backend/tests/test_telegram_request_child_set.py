"""An actual request effect must retain the exact one-child journal family."""

from uuid import uuid4

import pytest
from test_telegram_request_continuations import db as db
from test_telegram_request_continuations import ready_decision
from test_telegram_request_continuations import work_delivery as work_delivery
from test_telegram_request_continuations import work_pages as work_pages

from app.gateway import work_execution
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_request_effect_rejects_extra_child_after_host_entry(work_delivery, monkeypatch):
    e = work_delivery
    _record, _request, parent, _outcome = await ready_decision(e, monkeypatch)
    original = work_execution.activate_work
    entered = []

    async def damage(*args, **kwargs):
        entered.append(True)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, parent.id)
            assert len(row.operation_ids) == 1
            row.operation_ids = [*row.operation_ids, uuid4().hex]
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", damage)
    await e.acts.resume_continuation(parent.id)
    assert entered
    assert not e.launches, "An unexplained extra request child must retire final effect consent"

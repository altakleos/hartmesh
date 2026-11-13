from datetime import UTC, datetime

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from app.telegram_bots.acts import _snapshot
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["requested", "uncertain"])
@pytest.mark.parametrize("retirement", ["lifetime", "binding", "act_scope", "continuation"])
async def test_readonly_settlement_preserves_retired_owner_metadata(actions, monkeypatch, phase, retirement):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    act = await e.acts.cancel(e.link, operation_id=operation(), work_id=e.work["id"])
    assert act.stop_dispatch["state"] == "requested"
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkAttemptRow, attempt_id)
        original.status, original.settled_at = "cancelled", datetime.now(UTC)
        row = await writer.get(TelegramActRow, act.id)
        row.stop_dispatch = {**row.stop_dispatch, "state": phase}
        await writer.flush()
        act = _snapshot(row)
    active = [True]
    original_read = e.human.work.attempt_for_stop

    async def retire_after_read(**kwargs):
        result = await original_read(**kwargs)
        if retirement == "lifetime":
            active[0] = False
        elif retirement == "binding":
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        else:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramActRow, act.id)
                if retirement == "act_scope":
                    row.telegram_user_id += 1
                else:
                    row.continuation = {**row.continuation, "unknown": {"version": 1}}
        return result

    monkeypatch.setattr(e.human.work, "attempt_for_stop", retire_after_read)
    result = await e.acts._dispatch_stop(act, attempt_current=lambda: active[0])
    assert result.stop_dispatch == act.stop_dispatch
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramActRow, act.id)
        assert row.stop_dispatch == act.stop_dispatch
    manager.cancel.assert_awaited_once_with(record.run_id)

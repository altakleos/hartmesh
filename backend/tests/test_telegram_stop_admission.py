"""A Stop callback admits its journal/scope before any command or cancellation."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_activation import seeded_attempt
from test_telegram_work_actions import actions as actions

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_stage_stop_records_exact_scope_without_command_or_cancel(actions):
    e = actions
    attempt_id, captured, manager = await seeded_attempt(e)
    identity = uuid4().hex
    calls = []

    async def admission(writer, act):
        assert writer.in_transaction()
        calls.append(None if act is None else act.id)

    act = await e.acts.stage_stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id, admission=admission)
    assert act.state == "pending" and act.attempts == 0 and calls == [None, identity]
    assert act.stop_dispatch["state"] == "awaiting_command"
    assert act.stop_dispatch["attempt_id"] == attempt_id and act.stop_dispatch["run_id"] == captured.run_id
    async with e.db.sessions() as reader:
        assert (await reader.get(AgentWorkAttemptRow, attempt_id)).status == "running"
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["before", "after"])
async def test_failed_stop_callback_admission_rolls_back_journal_and_scope(actions, phase):
    e = actions
    attempt_id, _captured, manager = await seeded_attempt(e)
    identity = uuid4().hex

    async def admission(writer, act):
        if (act is None) == (phase == "before"):
            raise ValueError("callback-retired")

    with pytest.raises(ValueError, match="callback-retired"):
        await e.acts.stage_stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id, admission=admission)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is None
        assert (await reader.get(AgentWorkAttemptRow, attempt_id)).status == "running"
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_staged_stop_replay_keeps_original_body_without_rereading_work(actions, monkeypatch):
    e = actions
    attempt_id, _captured, manager = await seeded_attempt(e)
    args = dict(operation_id=uuid4().hex, work_id=e.work["id"], attempt_id=attempt_id)
    original = await e.acts.stage_stop(e.link, **args)

    async def unexpected(*args, **kwargs):
        pytest.fail("Saved Stop staging reread Work")

    monkeypatch.setattr(e.human.work, "get", unexpected)
    replay = await e.acts.stage_stop(e.link, **args)
    assert replay.body == original.body and replay.stop_dispatch == original.stop_dispatch
    manager.cancel.assert_not_awaited()

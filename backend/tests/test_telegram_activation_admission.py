"""A callback owner can admit its receipt and activation journal atomically."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import startable as startable

from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_stage_activation_saves_exact_intent_without_reserving_or_launching(startable, monkeypatch):
    e = startable
    identity = uuid4().hex

    async def unexpected(*args, **kwargs):
        pytest.fail("Staging launched an activation")

    monkeypatch.setattr(e.acts, "_attempt_activation", unexpected)
    calls = []

    async def admission(writer, act):
        calls.append(None if act is None else act.id)
        assert writer.in_transaction()

    staged = await e.acts.stage_activation(e.link, operation_id=identity, work_id=e.work["id"], assignment_revision=e.work["assignment_revision"], admission=admission)
    assert staged.state == "pending" and staged.attempts == 0 and calls == [None, identity]
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is not None
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("at", ["before", "after"])
async def test_callback_admission_failure_rolls_back_the_activation_journal(startable, at):
    e = startable
    identity = uuid4().hex

    async def refuse(writer, act):
        if (act is None) == (at == "before"):
            raise ValueError("callback-source-retired")

    with pytest.raises(ValueError, match="callback-source-retired"):
        await e.acts.stage_activation(e.link, operation_id=identity, work_id=e.work["id"], assignment_revision=e.work["assignment_revision"], admission=refuse)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is None
    assert not e.launches


@pytest.mark.asyncio
async def test_staged_replay_returns_original_body_without_repreparing_conversation(startable, monkeypatch):
    e = startable
    identity = uuid4().hex
    staged = await e.acts.stage_activation(e.link, operation_id=identity, work_id=e.work["id"], assignment_revision=e.work["assignment_revision"])

    async def unexpected(*args, **kwargs):
        pytest.fail("Saved staging replay created another conversation")

    monkeypatch.setattr(e.http_app.state.agent_conversations, "create", unexpected)
    replay = await e.acts.stage_activation(e.link, operation_id=identity, work_id=e.work["id"], assignment_revision=e.work["assignment_revision"])
    assert replay.id == staged.id and replay.body == staged.body and not e.launches

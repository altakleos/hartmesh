"""Saved-work commands keep one canonical body and separate cancellation custody."""

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow


@pytest.mark.asyncio
async def test_withdraw_replays_one_exact_command_and_closes_its_request(actions):
    e = actions
    identity = operation()
    first = await e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"])
    again = await e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"])
    assert first.id == again.id == identity and first.kind == "cancel" and first.state == again.state == "done"
    assert first.body == again.body and first.body["operation_id"] == identity and first.body["action"] == "cancel"
    assert first.stop_dispatch is None
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert current["status"] == "cancelled"
    request = await e.human.get(actor=e.actor, request_id=e.request["id"])
    assert request["state"] == "closed" and request["closed_reason"] == "withdrawn"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 1
        assert (await reader.get(TelegramActRow, identity)).attempts == 1


@pytest.mark.asyncio
async def test_lost_withdraw_response_recovers_receipt_without_reissuing_command(actions, monkeypatch):
    e = actions
    command, calls = e.human.work.command, []

    async def lost(**kwargs):
        calls.append(kwargs["request"].operation_id)
        await command(**kwargs)
        raise OSError("Synthetic lost command response")

    monkeypatch.setattr(e.human.work, "command", lost)
    identity = operation()
    result = await e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"])
    assert result.state == "pending"
    replay = await e.acts.check(identity)
    assert replay.state == "done" and calls == [identity]


@pytest.mark.asyncio
async def test_paused_withdraw_does_not_admit_a_command(actions):
    e = actions
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.acts.outbox.clock()
    identity = operation()
    with pytest.raises(AgentDenied):
        await e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"])
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is None
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)) == 0


@pytest.mark.asyncio
async def test_older_refusal_cannot_overwrite_a_later_admitted_attempt(actions, monkeypatch):
    import asyncio

    from app.telegram_bots.acts import Acts

    e = actions
    first_wait, second_wait = asyncio.Event(), asyncio.Event()
    release_first, release_second = asyncio.Event(), asyncio.Event()
    command, calls = e.human.work.command, []

    async def held(**kwargs):
        calls.append(kwargs["request"].operation_id)
        if len(calls) == 1:
            first_wait.set()
            await release_first.wait()
            raise AgentDenied("Synthetic old admission refusal")
        second_wait.set()
        await release_second.wait()
        return await command(**kwargs)

    monkeypatch.setattr(e.human.work, "command", held)
    other = Acts(e.db.sessions, settings=e.db.settings, callbacks=None, outbox=e.acts.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    identity = operation()
    first = asyncio.create_task(e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"]))
    second = None
    try:
        await first_wait.wait()
        second = asyncio.create_task(other.check(identity))
        await second_wait.wait()
        release_first.set()
        earlier = await first
        assert earlier.state == "pending" and earlier.attempts == 2
        release_second.set()
        assert (await second).state == "done"
    finally:
        release_first.set()
        release_second.set()
        await asyncio.gather(first, *([second] if second is not None else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_unreadable_receipt_reaches_uncertain_after_automatic_window(actions, monkeypatch):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots.acts import ActsWorker

    e = actions
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    calls = []

    async def unknown(**kwargs):
        calls.append(kwargs["request"].operation_id)
        raise OSError("Synthetic unknown command result")

    monkeypatch.setattr(e.human.work, "command", unknown)
    identity = operation()
    assert (await e.acts.cancel(e.link, operation_id=identity, work_id=e.work["id"])).state == "pending"

    async def unreadable(_act):
        raise OSError("Synthetic unavailable receipt")

    monkeypatch.setattr(e.acts, "receipt_for", unreadable)
    clock.advance(3601)
    await ActsWorker(e.acts).tick()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramActRow, identity)
        assert row.state == "uncertain" and row.attempts == 1
    assert calls == [identity]

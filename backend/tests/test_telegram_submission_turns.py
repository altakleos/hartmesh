"""Actual saved card answers remain owned through the source turn's release."""

import asyncio
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_submission_publication import submission_environment as submission_environment

from app.telegram_bots.turns import ChatTurns


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["finish", "cancel"])
async def test_answer_during_source_turn_release_keeps_its_own_callback_position(submission_environment, monkeypatch, action):
    e = submission_environment

    class Channel:
        link = e.link
        transport_revision = e.link.token_revision
        application = SimpleNamespace(running=True)
        _stopping = False

        def _callback_current(self, link):
            return link == self.link and e.creating[0] and e.reading[0]

        @staticmethod
        def _same_scope(left, right):
            return left == right

    channel = Channel()
    services = SimpleNamespace(config=e.db.config, outbox=e.outbox, links=e.db.links, callbacks=e.callbacks, channel=lambda _: channel, prepare_live_owner=e.live.authority.prepare_live_owner)
    turns = ChatTurns(services)
    release, started = asyncio.Event(), asyncio.Event()
    observed = []

    async def held(turn):
        observed.append(turn)
        started.set()
        await release.wait()

    monkeypatch.setattr(turns, "_drive", held)
    original = turns._inbound(e.link, e.origins)
    for row in e.origins:
        turns._origins[(e.link.id, row.message_id)] = row
        turns._origin_channels[(e.link.id, row.message_id)] = channel
    turns.queue.admit(e.link, original)
    turns._start(channel, e.link, original)
    try:
        await asyncio.wait_for(started.wait(), 5)
        first = observed[0]
        stage = await e.stage()
        assert await turns.accept_staged(channel, stage)
        assert turns.queue.waiting_count(e.link, None) == 1, "An actual answer is not a duplicate of its historical human origins"
        if action == "cancel":
            turns._drop_waiting(first)
            await turns._settle_cancelled_inputs(first)
            saved = await e.inbox.read_callback_input(e.link, stage.callback_id, guard=e.guard)
            assert saved.route_complete and not turns._callback_origins
            assert turns.queue.waiting_count(e.link, None) == 0
            assert (await e.callbacks.get(stage.callback_id)).consumed_at is not None
            return
        started.clear()
        turns._finish(first)
        await asyncio.wait_for(started.wait(), 5)
        answer = observed[-1]
        assert answer is not first and answer.callback_id == stage.callback_id
        assert answer.inbound.text == "First" and [row.message_id for row in answer.origins] == [701, 702]
    finally:
        release.set()
        await turns.stop()

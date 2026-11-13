"""The place-free queue crosses bot and human ownership boundaries."""

from uuid import uuid4

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import code_of
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import build, message, start
from test_telegram_capacity_offers import waiting
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef


async def second_wait(e, offers):
    from app.telegram_bots.work_news_delivery import WorkNewsSources
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_sources import WorkViewReader

    owner = str(uuid4())
    async with e.db.sessions.begin() as writer:
        writer.add(UserRow(id=owner, email="second-capacity-owner@example.com", password_hash="fixture-unusable", system_role="user"))
        await writer.flush()
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=owner, permissions=7))
    await e.db.settings.ensure(owner, timezone="America/New_York", locale="en-US")
    link = await e.db.links.add(owner_user_id=owner, agent_kind="instance", agent_ref=e.instance.id, bot_id=9000000010, bot_username="second_capacity_fixture_bot", encrypted_token="wrapped-fixture", actor_id=owner)
    link = await e.db.links.bind(
        link,
        telegram_user_id=7000000010,
        chat_id=7000000010,
        first_name="Second",
        username=None,
        code=code_of(await e.db.links.mint_code(link)),
        start_identity={"update_id": 4200, "message_id": 4200, "chat_id": 7000000010},
        hello_parts=[{"source_ref": {"key": "hello.employee", "values": {"agent": "Fixture employee", "email": "second-capacity-owner@example.com"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
    )
    transport = await build(e.db, connected=False, link=link, clock=e.clock, construct_off_loop=True)
    await start(transport)
    transport.services.authority = e.live.authority
    transport.services.callbacks = offers.callbacks
    transport.services.outbox = e.sender.outbox
    transport.services.eligibility = e.channel._services.eligibility
    view = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=offers.callbacks)
    transport.services.work_view_sources = WorkNewsSources(view)
    transport.services.work_capacity_sources = WorkNewsSources(offers)
    original = e.link, e.channel, e.work, e.identity
    try:
        e.link, e.channel = link, transport.channel
        e.work = await e.human.work.delegate(actor=PrincipalRef("human", owner), instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the second document", success_criteria="Report reproducible gaps"))
        async with e.db.sessions.begin() as writer:
            for row in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id)):
                row.state = "sent"
        update = make_update(4201, account=link.chat_id, text="/work")
        inbox, _ = await e.sender.outbox.inbox.accept(link, update, sanitized_input={"command": "work"})
        e.identity = await view.command(link, inbox, guard=await guard_for(e))

        async def send(identity):
            e.clock.advance(10)
            message(e.channel.bot.request, identity=identity, chat=link.chat_id)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            assert not e.dispatch_errors, e.dispatch_errors

        await send(4202)
        async with e.db.sessions() as reader:
            button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == link.id, TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"]))
        e.identity = await view.open(link, button.id, query_id="second-capacity-preview", chat_id=link.chat_id, message_id=4202, message_thread_id=None, guard=await guard_for(e))
        await send(4203)
        async with e.db.sessions() as reader:
            button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == link.id, TelegramCallbackRow.kind == "work_start", TelegramCallbackRow.target_id == e.work["id"]))
        act = await WorkStartAdmissions(view, e.acts).stage(link, button.id, chat_id=link.chat_id, message_id=4203, message_thread_id=None, guard=await guard_for(e))
        act = await e.acts.check(act.id)
        assert act.state == "waiting" and act.reason == "work-limit"
        return link, act
    finally:
        e.link, e.channel, e.work, e.identity = original
        await transport.channel.stop()


@pytest.mark.asyncio
async def test_free_place_is_offered_fifo_across_bots_and_people(work_delivery, monkeypatch):
    e = work_delivery
    offers, first, _ = await waiting(e, monkeypatch)
    other, second = await second_wait(e, offers)
    e.identity = await offers.poll()
    async with e.db.sessions() as reader:
        first_offer = await reader.get(TelegramOutboxRow, e.identity)
        assert first_offer.link_id == first.link_id and first_offer.payload["capacity_offer_intent"]["act_id"] == first.id
    await dispatch(e, 4210)
    assert not e.dispatch_errors, e.dispatch_errors
    assert await offers.poll() is None
    e.clock.advance(offers.outbox.config.outbox.place_free_hold_seconds + 1)
    next_id = await offers.poll()
    async with e.db.sessions() as reader:
        next_offer = await reader.get(TelegramOutboxRow, next_id)
        assert next_offer.link_id == other.id and next_offer.owner_user_id == other.owner_user_id
        assert next_offer.payload["capacity_offer_intent"]["act_id"] == second.id and not e.launches


@pytest.mark.asyncio
async def test_unsent_stale_offer_releases_independent_owner_immediately(work_delivery, monkeypatch, caplog):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import app.telegram_bots.work_capacity_offers as capacity_module

    caplog.set_level("DEBUG", logger="app.telegram_bots.work_capacity_offers")
    e = work_delivery
    offers, first, _ = await waiting(e, monkeypatch)
    other, second = await second_wait(e, offers)
    ids = iter(("f" * 32, "1" * 32))
    monkeypatch.setattr(capacity_module, "uuid4", lambda: SimpleNamespace(hex=next(ids)))
    instant = e.clock.now()
    real = offers.materialize
    monkeypatch.setattr(offers, "materialize", AsyncMock(side_effect=RuntimeError("Interrupted empty presentation")))
    assert await offers.poll() is None
    async with e.db.sessions() as reader:
        old = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "free:" + first.id + ":1"))
        assert old.state == "pending" and not old.parts
        identity = old.id
    assert (await e.acts.cancel_activation(first.id)).reason == "cancelled"
    monkeypatch.setattr(offers, "materialize", real)
    fresh = await offers.poll()
    assert fresh, caplog.text
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramOutboxRow, identity)
        assert old.state == "skipped" and old.skip_reason == "source-retired" and not old.parts
        row = await reader.get(TelegramOutboxRow, fresh)
        assert row.owner_user_id == other.owner_user_id and row.payload["capacity_offer_intent"]["act_id"] == second.id
        assert row.created_at > old.created_at
    assert e.clock.now() == instant
    assert not e.launches


@pytest.mark.asyncio
async def test_misbound_old_offer_never_retires_unrelated_controls(work_delivery, monkeypatch):
    e = work_delivery
    offers, first, _ = await waiting(e, monkeypatch)
    other, _second = await second_wait(e, offers)
    e.identity = await offers.poll()
    await dispatch(e, 4220)
    async with e.db.sessions.begin() as writer:
        control = await writer.get(TelegramCallbackRow, first.id)
        unrelated = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)))
        before = {row.id: (row.consumed_at, row.closed_reason) for row in unrelated}
        row = await writer.get(TelegramOutboxRow, e.identity)
        row.parts = [{**part, "callback_group_id": control.group_id} if part.get("callback_group_id") else part for part in row.parts]
    e.clock.advance(offers.outbox.config.outbox.place_free_hold_seconds + 1)
    fresh = await offers.poll()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, fresh)).owner_user_id == other.owner_user_id
        unrelated = [await reader.get(TelegramCallbackRow, identity) for identity in before]
        assert {row.id: (row.consumed_at, row.closed_reason) for row in unrelated} == before
    assert not e.launches

"""Installed advisory capacity offers never start work without a new gesture."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_capacity_offers import waiting
from test_telegram_notification_runtime import runtime
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
async def test_default_maintenance_offers_a_free_place_with_notifications_disabled(work_delivery, monkeypatch):
    from test_telegram_bot_adapter import message

    e = work_delivery
    _manual, act, _real = await waiting(e, monkeypatch)
    owner = await runtime(e, preserve=True)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    try:
        await owner.tick_maintenance()
        async with e.db.sessions() as reader:
            offers = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string() == act.id)))
            assert len(offers) == 1 and offers[0].parts
            identity = offers[0].id
            assert offers[0].class_ == "service"
            assert (await reader.get(TelegramActRow, act.id)).state == "waiting"
        assert not e.launches
        await owner.tick_maintenance()
        for index in range(4):
            e.clock.advance(10)
            message(e.channel.bot.request, identity=6800 + index)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            async with e.db.sessions() as reader:
                if (await reader.get(TelegramOutboxRow, identity)).state == "sent":
                    break
        else:
            pytest.fail("The installed offer did not reach its original SDK acknowledgement")
        assert not e.dispatch_errors, e.dispatch_errors
        assert not e.launches
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string() == act.id)))) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("db", ["postgres"], indirect=True)
@pytest.mark.parametrize("outcome", ["recovered", "retired"])
async def test_partless_offer_recovery_releases_reader_with_one_connection(work_delivery, monkeypatch, outcome):
    from unittest.mock import AsyncMock

    from sqlalchemy.pool import AsyncAdaptedQueuePool

    from app.gateway.storage_spaces import human_lookup

    e = work_delivery
    _manual, original, _real = await waiting(e, monkeypatch)
    owner = await runtime(e, preserve=True)
    # Use the production borrowed-account resolver rather than the ordinary
    # fixture's stand-alone lookup, which legitimately needs a second session.
    monkeypatch.setattr(e.human.agents.directory, "human", human_lookup(e.live.users))
    offers = owner.chat.offers
    actual = offers.materialize
    monkeypatch.setattr(offers, "materialize", AsyncMock(side_effect=RuntimeError("Interrupted before materialization")))
    assert await offers.poll() is None
    if outcome == "recovered":
        monkeypatch.setattr(offers, "materialize", actual)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string() == original.id))
        assert row is not None and row.state == "pending" and not row.parts
        identity = row.id
    original_engine = e.db.sessions.kw["bind"]
    # Keep the engine identity used by borrowed authority guards. Limit its
    # actual pool, preserving the fixture's original dialect/connect settings.
    await original_engine.dispose()
    original_pool = original_engine.sync_engine.pool
    bounded = AsyncAdaptedQueuePool(original_pool._creator, pool_size=1, max_overflow=0, timeout=0.1, dialect=original_engine.sync_engine.dialect, _dispatch=original_pool.dispatch)
    original_engine.sync_engine.pool = bounded
    try:
        assert await offers.poll() is None
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            if outcome == "recovered":
                assert row.state == "pending" and row.parts
            else:
                assert (row.state, row.skip_reason) == ("skipped", "source-retired") and not row.parts
            assert (await reader.get(TelegramActRow, original.id)).state == "waiting"
        assert not e.launches
        assert bounded.checkedout() == 0
    finally:
        await owner.stop()
        await original_engine.dispose()
        original_engine.sync_engine.pool = original_pool


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["capacity_read", "offer_flush"])
async def test_retired_installed_capacity_owner_cannot_commit_an_offer(work_delivery, monkeypatch, boundary):
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_actions import same_body

    e = work_delivery
    _manual, original, _real = await waiting(e, monkeypatch)
    owner = await runtime(e, preserve=True)
    offers, reached = owner.chat.offers, []
    if boundary == "capacity_read":
        actual = offers.reader.capacity.read

        async def retire(*args, **kwargs):
            value = await actual(*args, **kwargs)
            reached.append(True)
            owner._stopping = True
            return value

        monkeypatch.setattr(offers.reader.capacity, "read", retire)
    else:
        actual = AsyncSession.flush

        async def retire(session, *args, **kwargs):
            value = await actual(session, *args, **kwargs)
            if any(isinstance(row, TelegramOutboxRow) and "capacity_offer_intent" in row.payload for row in session.identity_map.values()):
                reached.append(True)
                owner._stopping = True
            return value

        monkeypatch.setattr(AsyncSession, "flush", retire)
    try:
        try:
            await offers.poll()
        except ProductSourceRetired:
            pass
        except ValueError as error:
            assert str(error) == "publication-guard-unavailable"
        else:
            pytest.fail("The retired offer owner did not refuse its late write")
        assert reached
        assert not e.launches
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string() == original.id)))
            saved = await reader.get(TelegramActRow, original.id)
            assert (saved.state, saved.reason, saved.attempts) == ("waiting", "work-limit", original.attempts)
            assert same_body(saved.body, original.body)
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_new_work_events_wake_capacity_before_the_minute_poll(work_delivery, monkeypatch):
    from types import SimpleNamespace
    from uuid import uuid4

    from deerflow.agent_instances.work_contract import DelegateWork, WorkCommand

    e = work_delivery
    _manual, original, _real = await waiting(e, monkeypatch)
    owner = await runtime(e, preserve=True)
    e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [dict(instance_id=e.instance.id, work_id="f" * 32, run_id="another-run", started_at=None, requester_id=None)])
    try:
        await owner.baselines.initialize()
        await owner.tick_maintenance()
        e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [])
        await owner.tick_maintenance()
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string() == original.id)))
        other = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the appendix", success_criteria="Verify the references"))
        await e.human.work.command(
            actor=e.actor, instance_id=e.instance.id, work_id=other["id"], request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=other["revision"], expected_assignment_revision=other["assignment_revision"])
        )
        assert await owner.tick_events() > 0
        await owner.tick_maintenance()
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["capacity_offer_intent"]["act_id"].as_string() == original.id)))) == 1
        assert not e.launches
    finally:
        await owner.stop()

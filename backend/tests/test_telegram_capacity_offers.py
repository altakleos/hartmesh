"""Capacity offers are owner-read, globally paced, full-consent service messages."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import Refusal
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


async def waiting(e, monkeypatch):
    from app.gateway import work_execution
    from app.gateway.routers.agent_work import router
    from app.telegram_bots.work_capacity_offers import WorkCapacityOffers
    from app.telegram_bots.work_capacity_reads import WorkCapacityReader
    from app.telegram_bots.work_capacity_sources import WorkCapacityOfferReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    real = work_execution.activate_work
    _views, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=Refusal("capacity", reason="work-limit")))
    act = await e.acts.check(act.id)
    assert act.state == "waiting" and act.reason == "work-limit"
    e.http_app.include_router(router)
    e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [])
    capacity = WorkCapacityReader(work=e.human.work, authority=e.live.authority, links=e.db.links, gateway=e.http_client)
    reader = WorkCapacityOfferReader(work=e.human.work, authority=e.live.authority, links=e.db.links, capacity=capacity, clock=e.clock.now)
    plans = WorkCapacityOffers(reader=reader, outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_capacity_sources = WorkNewsSources(plans)
    return plans, act, real


@pytest.mark.asyncio
async def test_free_place_offer_has_full_preview_and_fresh_atomic_start(work_delivery, monkeypatch):
    from app.gateway import work_execution
    from app.telegram_bots.work_start_controls import WorkStartAdmissions

    e = work_delivery
    offers, old, real = await waiting(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    e.identity = await offers.poll()
    assert e.identity and await offers.poll() is None and not e.launches
    await dispatch(e, 4001)
    assert not e.dispatch_errors, e.dispatch_errors
    sent = [c.parameters for c in e.channel.bot.request.calls if c.endpoint == "sendMessage"][-1]
    assert "place" in sent["text"].lower() and e.work["objective"] in sent["text"] and e.work["success_criteria"] in sent["text"]
    button = await selected(e, "work_start")
    assert button and button.id != old.id and button.message_id == 4001
    controller = WorkStartAdmissions(offers, e.acts)
    fresh = await stage(e, controller, button)
    assert fresh.id == button.id and fresh.id != old.id and not e.launches
    assert (await e.acts._work_record(old.id)).reason == "superseded"
    assert await stage(e, controller, button) == fresh
    e.human.agents.residents = None
    monkeypatch.setattr(work_execution, "activate_work", real)
    assert (await e.acts.check(fresh.id)).state == "done" and len(e.launches) == 1


@pytest.mark.asyncio
async def test_one_global_offer_holds_until_ack_and_then_hold_expiry(work_delivery, monkeypatch):
    e = work_delivery
    offers, act, _ = await waiting(e, monkeypatch)
    e.identity = await offers.poll()
    e.clock.advance(offers.outbox.config.outbox.place_free_hold_seconds + 1)
    assert await offers.poll() is None  # Pending SDK delivery cannot lose its place.
    await dispatch(e, 4010)
    assert not e.dispatch_errors, e.dispatch_errors
    assert await offers.poll() is None
    e.clock.advance(offers.outbox.config.outbox.place_free_hold_seconds + 1)
    second = await offers.poll()
    assert second and second != e.identity and not e.launches
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("free:" + act.id + ":%"))))
        assert {r.dedupe_key for r in rows} == {"free:" + act.id + ":1", "free:" + act.id + ":2"}


@pytest.mark.asyncio
async def test_capacity_taken_before_final_sdk_call_suppresses_stale_offer(work_delivery, monkeypatch):
    e = work_delivery
    offers, _act, _ = await waiting(e, monkeypatch)
    e.identity = await offers.poll()
    source = e.channel._services.work_capacity_sources
    real, changed = source.settings.get, []

    async def take(*args, **kwargs):
        value = await real(*args, **kwargs)
        if not changed:
            changed.append(True)
            e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [dict(instance_id=e.instance.id, work_id=e.work["id"], run_id="another-run", started_at=None, requester_id=None)])
        return value

    monkeypatch.setattr(source.settings, "get", take)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 4020)
    assert not [c for c in e.channel.bot.request.calls[before:] if c.endpoint == "sendMessage"]
    assert not e.launches and changed


@pytest.mark.asyncio
async def test_closed_assignment_retires_capacity_wait_without_an_offer(work_delivery, monkeypatch):
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    offers, act, _ = await waiting(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(AgentWorkRow, e.work["id"])
        row.status = "cancelled"
    assert await offers.poll() is None
    assert (await e.acts._work_record(act.id)).reason == "superseded" and not e.launches


@pytest.mark.asyncio
async def test_protected_offer_reveals_complete_preview_without_starting(work_delivery, monkeypatch):
    from test_telegram_work_execution_outcomes import dispatch_edit

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    offers, old, _ = await waiting(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.clock.now()))
    e.identity = await offers.poll()
    await dispatch(e, 4030)
    assert not e.dispatch_errors, e.dispatch_errors
    sent = [c.parameters for c in e.channel.bot.request.calls if c.endpoint == "sendMessage"][-1]
    assert sent["text"] == "There is news about an assignment." and sent["protect_content"]
    show = await selected(e, "reveal")
    sources = e.channel._services.work_capacity_sources
    assert await sources.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=4030, message_thread_id=None, guard=await guard_for(e)) == e.identity
    await dispatch_edit(e, 4030)
    edited = [c.parameters for c in e.channel.bot.request.calls if c.endpoint == "editMessageText"][-1]
    assert e.work["objective"] in edited["text"] and e.work["success_criteria"] in edited["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.parts[-1]["protect_content"] is True and row.parts[-1]["target_protect_content"] is True
    button = await selected(e, "work_start")
    assert button and button.message_id == 4030 and not e.launches
    assert (await e.acts._work_record(old.id)).state == "waiting"


@pytest.mark.asyncio
async def test_offer_press_rolls_back_old_wait_when_original_ack_is_changed(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_start_controls import WorkStartAdmissions

    e = work_delivery
    offers, old, _ = await waiting(e, monkeypatch)
    e.identity = await offers.poll()
    await dispatch(e, 4040)
    button = await selected(e, "work_start")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, old.id)
        row.payload = {**row.payload, "work_action_id": "f" * 32}
    with pytest.raises((ProductSourceRetired, CallbackRefused)):
        await stage(e, WorkStartAdmissions(offers, e.acts), button)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None
        assert (await reader.get(TelegramActRow, old.id)).state == "waiting"
    assert not e.launches


@pytest.mark.asyncio
async def test_two_concurrent_global_polls_publish_only_one_offer(work_delivery, monkeypatch):
    import asyncio

    e = work_delivery
    offers, old, _ = await waiting(e, monkeypatch)
    original, both, calls = offers.reader.capacity.read, asyncio.Event(), []

    async def read(*args, **kwargs):
        result = await original(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            both.set()
        await asyncio.wait_for(both.wait(), 5)
        return result

    monkeypatch.setattr(offers.reader.capacity, "read", read)
    # Only the preparation reads rendezvous; materialization re-reads after it.
    results = await asyncio.gather(offers.poll(), offers.poll())
    assert len([value for value in results if value]) == 1 and not e.launches
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("free:" + old.id + ":%"))))
        assert len(rows) == 1 and rows[0].parts


@pytest.mark.asyncio
async def test_final_assignment_guard_rolls_back_wait_transfer_and_new_act(work_delivery, monkeypatch):
    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    offers, old, _ = await waiting(e, monkeypatch)
    e.identity = await offers.poll()
    await dispatch(e, 4050)
    button = await selected(e, "work_start")
    original, changed = offers.reader.prepare_detail, []

    async def prepare(*args, **kwargs):
        check = await original(*args, **kwargs)

        async def final(*, session=None):
            if session is not None and not changed:
                changed.append(True)
                row = await session.get(AgentWorkRow, e.work["id"])
                row.revision += 1
                await session.flush()
            return await check(session=session)

        return final

    monkeypatch.setattr(offers.reader, "prepare_detail", prepare)
    with pytest.raises(AuthorityUnavailable):
        await stage(e, WorkStartAdmissions(offers, e.acts), button)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None
        assert (await reader.get(TelegramActRow, old.id)).state == "waiting"
        assert (await reader.get(AgentWorkRow, e.work["id"])).revision == e.work["revision"]
    assert changed and not e.launches


@pytest.mark.asyncio
async def test_wait_and_offer_ordinals_survive_production_retention_order(work_delivery, monkeypatch):
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows

    e = work_delivery
    offers, old, _ = await waiting(e, monkeypatch)
    e.identity = await offers.poll()
    await dispatch(e, 4060)
    e.clock.advance(int(timedelta(days=80).total_seconds()))
    await offers.callbacks.sweep(e.clock.now())
    await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=offers.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, old.id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, old.id) is not None
    next_id = await offers.poll()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, next_id)
        assert row.dedupe_key == "free:" + old.id + ":2" and row.parts
    assert not e.launches


@pytest.mark.asyncio
async def test_saved_unmaterialized_offer_recovers_without_another_ordinal(work_delivery, monkeypatch):
    e = work_delivery
    offers, act, _ = await waiting(e, monkeypatch)
    real, failed = offers.materialize, []

    async def once(*args, **kwargs):
        if not failed:
            failed.append(True)
            raise RuntimeError("Interrupted presentation preparation")
        return await real(*args, **kwargs)

    monkeypatch.setattr(offers, "materialize", once)
    assert await offers.poll() is None
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "free:" + act.id + ":1"))
        assert row and not row.parts
        identity = row.id
    assert await offers.poll() is None
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts and row.state == "pending"
    e.identity = identity
    await dispatch(e, 4090)
    assert not e.dispatch_errors and not e.launches, e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("preference", ["all", "questions", "none"])
async def test_service_offer_obeys_quiet_sound_under_every_preference(work_delivery, monkeypatch, preference):
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    offers, _act, _ = await waiting(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
        row.notify, row.quiet_enabled, row.quiet_start_minute, row.quiet_end_minute = preference, True, 0, 1439
        (await writer.get(TelegramBotLinkRow, e.link.id)).last_update_at = None
    e.identity = await offers.poll()
    await dispatch(e, 4100)
    assert not e.dispatch_errors, e.dispatch_errors
    sent = [c.parameters for c in e.channel.bot.request.calls if c.endpoint == "sendMessage"][-1]
    assert sent["disable_notification"] is True and e.work["success_criteria"] in sent["text"] and not e.launches


@pytest.mark.asyncio
async def test_long_offer_attaches_start_only_after_complete_sdk_ack(work_delivery, monkeypatch, caplog):
    from test_telegram_bot_adapter import message

    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    offers, _act, _ = await waiting(e, monkeypatch)
    caplog.set_level("DEBUG", logger="app.telegram_bots.work_capacity_offers")
    criterion = "A complete reproducible criterion. " * 100 + "FINAL CAPACITY CONSENT MARKER"
    async with e.db.sessions.begin() as writer:
        row = await writer.get(AgentWorkRow, e.work["id"])
        row.success_criteria = criterion
        row.objective = "A detailed assignment. " * 150
        row.revision += 1
        row.assignment_revision += 1
    e.identity = await offers.poll()
    assert e.identity, caplog.text
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert len(row.parts) > 1
        assert all(not part.get("callback_group_id") for part in row.parts[:-1])
        count = len(row.parts)
    for i in range(count):
        e.clock.advance(10)
        message(e.channel.bot.request, identity=4110 + i)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and all(part["message_ids"] for part in row.parts)
        assert "FINAL CAPACITY CONSENT MARKER" in row.parts[-1]["html"]
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "work_start"))
        assert control.message_id == 4110 + count - 1 and control.payload["assignment_revision"] == e.work["assignment_revision"] + 1
    assert not e.launches

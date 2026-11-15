"""Actual child journals retain the original SDK source and capacity ownership."""

from datetime import timedelta
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_start_context_ownership import arm_conversation_change
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import ACT_RETENTION_DAYS, OUTBOX_RETENTION_DAYS, sweep_rows
from app.telegram_bots.work_start_conversations import context_child_id
from deerflow.agent_instances.contract import Refusal
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def held_context(e, monkeypatch, *, reason="work-limit"):
    from app.gateway import work_execution

    views, controller, button = await start_control(e, monkeypatch)
    root = await stage(e, controller, button)
    arm_conversation_change(e, monkeypatch)
    native = work_execution.activate_work

    async def scripted_capacity(request, **kwargs):
        if kwargs["body"].operation_id != root.id:
            # This is an explicit host-capacity adapter. The test qualifies
            # actual journal/SDK ownership, not the capacity provider itself.
            raise Refusal("capacity held", reason=reason)
        return await native(request, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", scripted_capacity)
    root = await e.acts.check(root.id)
    child = await e.acts._work_record(context_child_id(root.id))
    assert child.state == "waiting" and child.reason == reason
    assert root.state == "refused" and not e.launches
    return views, root, child, button


@pytest.mark.asyncio
async def test_capacity_offer_transfers_the_actual_waiting_context_child(work_delivery, monkeypatch):
    from app.gateway import work_execution
    from app.gateway.routers.agent_work import router
    from app.telegram_bots.work_capacity_offers import WorkCapacityOffers
    from app.telegram_bots.work_capacity_reads import WorkCapacityReader
    from app.telegram_bots.work_capacity_sources import WorkCapacityOfferReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources
    from app.telegram_bots.work_start_controls import WorkStartAdmissions

    e = work_delivery
    original = work_execution.activate_work
    _views, root, child, _button = await held_context(e, monkeypatch)
    e.http_app.include_router(router)
    e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [])
    capacity = WorkCapacityReader(work=e.human.work, authority=e.live.authority, links=e.db.links, gateway=e.http_client)
    offers = WorkCapacityOffers(
        reader=WorkCapacityOfferReader(work=e.human.work, authority=e.live.authority, links=e.db.links, capacity=capacity, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts
    )
    e.channel._services.work_capacity_sources = WorkNewsSources(offers)
    e.identity = await offers.poll()
    assert e.identity is not None
    await dispatch(e, 8501)
    assert not e.dispatch_errors, e.dispatch_errors
    fresh = await stage(e, WorkStartAdmissions(offers, e.acts), await selected(e, "work_start"))
    assert fresh.id not in {root.id, child.id}
    assert (await e.acts._work_record(child.id)).reason == "superseded"
    e.human.agents.residents = None
    monkeypatch.setattr(work_execution, "activate_work", original)
    result = await e.acts.check(fresh.id)
    assert result.state == "done" and len(e.launches) == 1, (result.state, result.reason)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_((root.id, child.id)))) is None


@pytest.mark.asyncio
async def test_transferred_context_capacity_offer_can_expire_after_its_actual_native_start(work_delivery, monkeypatch):
    e = work_delivery
    await test_capacity_offer_transfers_the_actual_waiting_context_child(e, monkeypatch)
    offer_id = e.identity
    async with e.db.sessions() as reader:
        offer = await reader.get(TelegramOutboxRow, offer_id)
        child = await reader.get(TelegramActRow, offer.payload["capacity_offer_intent"]["act_id"])
        assert child.kind == "activate_context" and child.reason == "superseded"
    later = e.sender.outbox.clock() + timedelta(days=max(ACT_RETENTION_DAYS, OUTBOX_RETENTION_DAYS, e.channel._services.callbacks.config.callbacks.max_age_days) + 1)
    await e.channel._services.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, offer_id) is None
    assert len(e.launches) == 1


@pytest.mark.asyncio
async def test_original_cancel_control_fences_the_waiting_context_child(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
    from app.telegram_bots.work_execution_sources import WorkExecutionReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e = work_delivery
    _views, root, child, _button = await held_context(e, monkeypatch, reason="busy")
    plans = WorkExecutionPlans(reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    e.identity = await plans.observe(e.link, root.id)
    assert e.identity is not None
    await dispatch(e, 8502)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "cancel_wait")
    assert button is not None and button.payload["act_id"] == root.id
    await plans.cancel(e.link, button.id, chat_id=e.link.chat_id, message_id=8502, message_thread_id=None, guard=await guard_for(e))
    for identity in (root.id, child.id):
        row = await e.acts._work_record(identity)
        assert row.cancel_requested_at is not None and row.reason == "cancelled"
        await e.acts.check(identity)
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [False, True])
async def test_aged_sweeps_keep_context_ancestry_even_when_relation_markers_are_lost(work_delivery, monkeypatch, damage):
    e = work_delivery
    _views, root, child, button = await held_context(e, monkeypatch)
    if damage:
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramActRow, root.id)
            member = await writer.get(TelegramActRow, child.id)
            parent.operation_ids = []
            parent.continuation = {key: value for key, value in parent.continuation.items() if key != "activation_context"}
            member.continuation = None
    later = e.sender.outbox.clock() + timedelta(days=max(ACT_RETENTION_DAYS, OUTBOX_RETENTION_DAYS, e.channel._services.callbacks.config.callbacks.max_age_days) + 1)
    await e.channel._services.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        assert await reader.get(TelegramOutboxRow, button.payload["anchor_outbox_id"]) is not None
        assert await reader.get(TelegramActRow, root.id) is not None
        assert await reader.get(TelegramActRow, child.id) is not None
    await e.acts.check(root.id)
    assert not e.launches


@pytest.mark.asyncio
async def test_aged_conclusively_cancelled_context_releases_its_original_control(work_delivery, monkeypatch):
    e = work_delivery
    _views, root, child, button = await held_context(e, monkeypatch, reason="busy")
    await e.acts.cancel_activation(root.id)
    later = e.sender.outbox.clock() + timedelta(days=max(ACT_RETENTION_DAYS, OUTBOX_RETENTION_DAYS, e.channel._services.callbacks.config.callbacks.max_age_days) + 1)
    await e.channel._services.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, button.id) is None
        for identity in (root.id, child.id):
            row = await reader.get(TelegramActRow, identity)
            assert row is not None and row.reason == "cancelled"
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_root_relation", [False, True])
async def test_native_child_evidence_pins_ancestry_after_its_journal_is_deleted(work_delivery, monkeypatch, remove_root_relation):
    e = work_delivery
    _views, controller, button = await start_control(e, monkeypatch)
    root = await stage(e, controller, button)
    arm_conversation_change(e, monkeypatch)
    outcome = e.acts._activation_outcome

    async def interrupted(captured, **kwargs):
        if captured.kind == "activate" and captured.operation_ids and kwargs.get("state") == "done":
            raise RuntimeError("parent completion interrupted")
        return await outcome(captured, **kwargs)

    monkeypatch.setattr(e.acts, "_activation_outcome", interrupted)
    root = await e.acts.check(root.id)
    assert root.state == "refused" and len(e.launches) == 1
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, context_child_id(root.id)))
        if remove_root_relation:
            parent = await writer.get(TelegramActRow, root.id)
            parent.operation_ids = []
            parent.continuation = {key: value for key, value in parent.continuation.items() if key != "activation_context"}
    later = e.sender.outbox.clock() + timedelta(days=max(ACT_RETENTION_DAYS, OUTBOX_RETENTION_DAYS, e.channel._services.callbacks.config.callbacks.max_age_days) + 1)
    await e.channel._services.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        assert await reader.get(TelegramOutboxRow, button.payload["anchor_outbox_id"]) is not None
        assert await reader.get(TelegramActRow, context_child_id(root.id)) is None
    await e.acts.check(root.id)
    assert len(e.launches) == 1

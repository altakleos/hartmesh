"""Actual SDK composite notices retain parent consent without a synthetic button."""

from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_card_delivery import prepare_card
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_controls import AssignmentAdmissions
from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
from app.telegram_bots.work_execution_sources import WorkExecutionReader
from app.telegram_bots.work_news_delivery import WorkNewsSources
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def uncertain_child(e, monkeypatch, *, runless=False, reason=None):
    from app.gateway import services, work_execution
    from deerflow.agent_instances.contract import Refusal

    assignments, _draft, _inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7911)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_start")
    parent = await AssignmentAdmissions(assignments, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7911, message_thread_id=None, guard=await guard_for(e))
    await e.acts.check(parent.id)
    e.actual_activation = work_execution.activate_work
    e.actual_launcher = services.start_run
    if runless:
        monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=RuntimeError("Host admission unavailable")))
    elif reason is not None:
        monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=Refusal("Host capacity", reason=reason)))
    else:
        monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=RuntimeError("Host admission unavailable")))
    await e.acts.resume_continuation(parent.id)
    if runless:
        monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=Refusal("Host restarting", reason="restarting")))
    e.clock.advance(3601)
    planned = await e.acts.resume_continuation(parent.id)
    child_id = planned.continuation["activation_continuation"]["child_id"]
    assert (await e.acts._work_record(child_id)).state == ("waiting" if reason else "uncertain")
    plans = WorkExecutionPlans(acts=e.acts, reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=assignments.callbacks)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    return plans, parent.id, child_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ordinary", "retired", "protected"])
async def test_composite_capacity_offer_uses_parent_consent_and_new_full_start(work_delivery, monkeypatch, mode):
    from types import SimpleNamespace

    from test_telegram_work_start_controls import stage

    from app.gateway.routers.agent_work import router
    from app.telegram_bots.work_capacity_offers import WorkCapacityOffers
    from app.telegram_bots.work_capacity_reads import WorkCapacityReader
    from app.telegram_bots.work_capacity_sources import WorkCapacityOfferReader
    from app.telegram_bots.work_start_controls import WorkStartAdmissions

    e = work_delivery
    _notices, parent_id, child_id = await uncertain_child(e, monkeypatch, reason="work-limit")
    e.http_app.include_router(router)
    e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [])
    capacity = WorkCapacityReader(work=e.human.work, authority=e.live.authority, links=e.db.links, gateway=e.http_client)
    reader = WorkCapacityOfferReader(work=e.human.work, authority=e.live.authority, links=e.db.links, capacity=capacity, clock=e.clock.now)
    offers = WorkCapacityOffers(reader=reader, outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_capacity_sources = WorkNewsSources(offers)
    if mode == "protected":
        from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.clock.now()))
    e.identity = await offers.poll()
    assert e.identity is not None
    if mode == "retired":
        limiter, retired = e.channel.bot.rate_limiter, []

        async def retire(_delay):
            limiter._short_hold_until = 0
            retired.append(True)
            async with e.db.sessions.begin() as writer:
                control = await writer.get(TelegramCallbackRow, parent_id)
                control.payload = {**control.payload, "work_action_id": "f" * 32}

        limiter._sleep = retire
        limiter._short_hold_until = limiter._time() + 10
        before = len(e.channel.bot.request.calls)
    await dispatch(e, 7925)
    if mode == "retired":
        assert retired and not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"], e.dispatch_errors
        assert not e.launches
        return
    assert not e.dispatch_errors, e.dispatch_errors
    if mode == "protected":
        from test_telegram_work_execution_outcomes import dispatch_edit

        show = await selected(e, "reveal")
        await e.channel._services.work_capacity_sources.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=7925, message_thread_id=None, guard=await guard_for(e))
        await dispatch_edit(e, 7925)
    fresh_button = await selected(e, "work_start")
    assert fresh_button.id not in {parent_id, child_id} and fresh_button.message_id == 7925
    fresh = await stage(e, WorkStartAdmissions(offers, e.acts), fresh_button)
    assert fresh.id == fresh_button.id
    assert (await e.acts._work_record(child_id)).reason == "superseded"
    assert not e.launches


@pytest.mark.asyncio
async def test_refused_composite_replay_preserves_original_runless_promise(work_delivery, monkeypatch):
    from test_telegram_work_execution_outcomes import dispatch_edit

    from app.gateway import services, work_execution
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    plans, parent_id, child_id = await uncertain_child(e, monkeypatch, runless=True)
    # An explicit replay receives a conclusive service refusal while the
    # original launch reservation remains unknown.
    e.identity = await plans.observe(e.link, child_id)
    await dispatch(e, 7924)
    retry = await selected(e, "act_check")
    refused = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=7924, message_thread_id=None, guard=await guard_for(e))
    assert refused.state == "refused" and refused.reason == "restarting"
    await e.acts.resume_continuation(parent_id)
    await dispatch_edit(e, 7924)
    retry = await selected(e, "act_check")
    async with e.db.sessions() as reader:
        attempt = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        attempt_id = attempt.id
        assert attempt.run_id is None and attempt.status == "starting"
    monkeypatch.setattr(work_execution, "activate_work", e.actual_activation)
    monkeypatch.setattr(services, "start_run", e.actual_launcher)
    result = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=7924, message_thread_id=None, guard=await guard_for(e))
    assert result.state == "done" and result.id == child_id
    assert result.result["activation_receipt"]["attempt_id"] == attempt_id and len(e.launches) == 1


@pytest.mark.asyncio
async def test_composite_unknown_notice_uses_actual_original_consent(work_delivery, monkeypatch):
    e = work_delivery
    plans, parent_id, child_id = await uncertain_child(e, monkeypatch)
    notice = await plans.observe(e.link, child_id)
    assert notice is not None
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, notice)
        assert row.payload["work_execution_intent"]["origin"]["callback_id"] == parent_id
        assert await reader.get(TelegramCallbackRow, child_id) is None
    await dispatch(e, 7912)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "act_check", TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == notice))
        assert retry is not None and retry.target_id == child_id
        assert retry.message_id == 7912 and retry.payload["presentation_pending"] is False
    assert not e.launches


@pytest.mark.asyncio
async def test_explicit_composite_retry_keeps_child_and_closes_its_notice(work_delivery, monkeypatch):
    from app.gateway import work_execution

    e = work_delivery
    plans, parent_id, child_id = await uncertain_child(e, monkeypatch)
    e.identity = await plans.observe(e.link, child_id)
    await dispatch(e, 7920)
    retry = await selected(e, "act_check")
    original = await e.acts._work_record(child_id)
    monkeypatch.setattr(work_execution, "activate_work", e.actual_activation)
    result = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=7920, message_thread_id=None, guard=await guard_for(e))
    assert result.id == child_id and result.body == original.body and result.state == "done"
    assert len(e.launches) == 1
    assert (await e.acts.resume_continuation(parent_id)).continuation["activation_continuation"]["phase"] == "done"
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, retry.id)).consumed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["child_marker", "parent_forward", "root_body", "consent", "original_input", "inspect"])
async def test_composite_notice_rechecks_actual_consent_after_limiter(work_delivery, monkeypatch, damage):
    from app.telegram_bots.work_actions import thaw
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow

    e = work_delivery
    plans, parent_id, child_id = await uncertain_child(e, monkeypatch)
    e.identity = await plans.observe(e.link, child_id)
    limiter, changed = e.channel.bot.rate_limiter, []

    async def retire(_delay):
        limiter._short_hold_until = 0
        changed.append(True)
        async with e.db.sessions.begin() as writer:
            primary = await writer.get(TelegramActRow, parent_id)
            if damage == "child_marker":
                child = await writer.get(TelegramActRow, child_id)
                child.continuation = None
            elif damage == "parent_forward":
                primary.operation_ids = []
            elif damage == "consent":
                callback = await writer.get(TelegramCallbackRow, parent_id)
                callback.payload = {**callback.payload, "work_action_id": "e" * 32}
            elif damage == "inspect":
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
            else:
                root = await writer.get(TelegramActRow, primary.continuation["delegate_source"]["draft_id"])
                body = thaw(root.body)
                if damage == "root_body":
                    body["draft"]["objective"] = "Changed after the delivery wait"
                    root.body = body
                else:
                    origin = body["origin"]
                    await writer.delete(await writer.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"])))

    limiter._sleep = retire
    limiter._short_hold_until = limiter._time() + 10
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 7921)
    assert changed
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"], e.dispatch_errors
    assert not e.launches


@pytest.mark.asyncio
async def test_composite_unknown_remains_readable_when_work_policy_is_disabled(work_delivery, monkeypatch):
    from deerflow.agent_instances.contract import DefinitionSnapshot
    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow

    e = work_delivery
    plans, _parent_id, child_id = await uncertain_child(e, monkeypatch)
    snapshot = DefinitionSnapshot.capture(owner_id=e.link.owner_user_id, config={"name": "analyst", "work_policy": {"enabled": False}}, soul="Review documentation.")
    async with e.db.sessions.begin() as writer:
        writer.add(AgentDefinitionRevisionRow(revision=snapshot.revision, owner_id=snapshot.owner_id, config=snapshot.config, soul=snapshot.soul))
        await writer.flush()
        (await writer.get(AgentInstanceRow, e.link.agent_ref)).definition_revision = snapshot.revision
    e.identity = await plans.observe(e.link, child_id)
    await dispatch(e, 7922)
    assert not e.dispatch_errors, e.dispatch_errors
    assert not e.launches

"""Explicit refresh keeps the completed response and replaces obsolete choices."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_routing_followups import followup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_withdrawn_recipient_refreshes_without_repeating_wrong_response(work_delivery, monkeypatch):
    e = work_delivery
    plans, _admission, act, _mark, original, control = await followup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery

    recovery = WorkRoutingFollowupRecovery(plans)
    identity = await recovery.refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    assert identity != original.id
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.class_ == "reply" and row.state == "pending"
        assert row.payload["work_outcome_basis"]["routing"]["routing"]["recipients"] == []
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        assert (await reader.get(TelegramActRow, act.id)).continuation["work_outcome"]["outbox_id"] == original.id
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.operation_id == act.id)) == 1
    # Run the durable old-keyboard cleanup before sending the replacement.
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    await dispatch(e, 805)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        saved = await reader.get(TelegramActRow, act.id)
        assert saved.continuation["work_outcome"]["outbox_id"] == identity
        assert saved.attempts == act.attempts + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("message_id", [900, 2**40])
async def test_live_choices_retain_input_until_their_original_lifetime_ends(work_delivery, monkeypatch, message_id):
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    _plans, _admission, _act, mark, parent, control = await followup(e, monkeypatch, input_message_id=message_id, input_update_id=message_id + 1000)
    key = (mark.link_id, mark.message_id, mark.chat_id)
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramInboundMarkRow, key)
        assert saved.state == "done" and saved.route_complete
        saved.closed_at = e.clock.now - timedelta(days=8)
        (await writer.get(TelegramOutboxRow, parent.id)).closed_at = e.clock.now - timedelta(days=31)
    await sweep_rows(e.db.sessions, e.clock.now, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, key) is not None
        assert await reader.get(TelegramOutboxRow, parent.id) is not None
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramCallbackRow, control.id)).created_at = e.clock.now - timedelta(days=181)
    await sweep_rows(e.db.sessions, e.clock.now, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, key) is None
        assert await reader.get(TelegramOutboxRow, parent.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["proof", "input", "candidate"])
async def test_refresh_final_writer_rolls_back_changed_evidence(work_delivery, monkeypatch, change):
    from app.telegram_bots.links import prepare_publication_guard
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    plans, _admission, _act, mark, parent, control = await followup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    base = await guard_for(e)
    reached = []

    class Withdraw:
        async def prepare(self):
            await prepare_publication_guard(base)

        def current(self):
            base.current()

        async def check(self, writer):
            result = await base.check(writer)
            saved = await writer.get(TelegramCallbackRow, control.id, populate_existing=True)
            if not reached and saved.payload.get("work_followup_outbox_id"):
                reached.append(True)
                if change == "proof":
                    saved.closed_reason = "other"
                elif change == "input":
                    await writer.delete(await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id)))
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 3
                await writer.flush()
            return result

    with pytest.raises(ProductSourceRetired):
        await WorkRoutingFollowupRecovery(plans).refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=Withdraw())
    assert reached
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramCallbackRow, control.id)
        assert saved.consumed_at is None and "work_followup_outbox_id" not in saved.payload
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["refresh"]["anchor_outbox_id"].as_string() == parent.id)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("settled", [False, True])
async def test_refresh_preserves_unresolved_route_and_recovers_known_recipient_refusal(work_delivery, monkeypatch, settled):
    from test_telegram_work_routing_followups import route_followup

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery

    e = work_delivery
    plans, admission, _act, _mark, parent, control = await followup(e, monkeypatch)
    staged = await route_followup(e, admission, control)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    if settled:
        result = await admission.acts.check(staged.id)
        assert (result.state, result.reason) == ("refused", "recipient-ineligible")
        identity = await WorkRoutingFollowupRecovery(plans).refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
        assert identity != parent.id
    else:
        with pytest.raises(CallbackRefused):
            await WorkRoutingFollowupRecovery(plans).refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, staged.id)
        assert saved.body == dict(staged.body)
        assert saved.state == ("refused" if settled else "pending")
        assert bool((await reader.get(TelegramCallbackRow, control.id)).consumed_at) == settled


@pytest.mark.asyncio
async def test_refreshed_choice_keeps_lifetime_and_admits_a_separate_route(work_delivery, monkeypatch):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from app.gateway.auth.models import User
    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery

    e = work_delivery
    plans, admission, act, _mark, parent, control = await followup(e, monkeypatch)
    other = await e.live.users.create_user(User(email="new-recipient@example.com"))
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(other.id), permissions=3))
    recovery = WorkRoutingFollowupRecovery(plans)
    kwargs = dict(chat_id=e.link.chat_id, message_id=804, message_thread_id=None)
    identity = await recovery.refresh(e.link, control.id, **kwargs, guard=await guard_for(e))
    assert await recovery.refresh(e.link, control.id, **kwargs, guard=await guard_for(e)) == identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        fresh = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert fresh.created_at == control.created_at
        assert fresh.payload["recipient_id"] == str(other.id) and fresh.payload["presentation_pending"]
        assert len((await reader.get(TelegramCallbackRow, control.id)).closed_reason) <= TelegramCallbackRow.__table__.c.closed_reason.type.length
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    await dispatch(e, 805)
    staged = await admission.stage(e.link, fresh.id, chat_id=e.link.chat_id, message_id=805, message_thread_id=None, guard=await guard_for(e))
    assert staged.id != act.id and (await admission.acts.check(staged.id)).state == "done"
    assert await recovery.refresh(e.link, control.id, **kwargs, guard=await guard_for(e)) == identity
    assert await plans.plan(e.link, act.id, guard=await guard_for(e)) == identity


@pytest.mark.asyncio
async def test_refresh_retains_consumed_source_and_completed_input_through_collection(work_delivery, monkeypatch):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from app.telegram_bots.retention import sweep_rows
    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    plans, _admission, _act, mark, parent, control = await followup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    identity = await WorkRoutingFollowupRecovery(plans).refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    key = (mark.link_id, mark.message_id, mark.chat_id)
    e.clock.advance(32 * 24 * 60 * 60)
    await sweep_rows(e.db.sessions, e.clock.now, callback_max_age_days=180)
    await plans.callbacks.sweep(e.clock.now)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, key)).route_complete
        assert await reader.get(TelegramOutboxRow, parent.id) is not None
        assert await reader.get(TelegramCallbackRow, control.id) is not None
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    await dispatch(e, 805)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        assert (await reader.get(TelegramInboundMarkRow, key)).state == "done"


@pytest.mark.asyncio
async def test_queued_refresh_source_drift_repairs_the_same_explicit_intent(work_delivery, monkeypatch):
    from itertools import count
    from uuid import UUID

    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from app.gateway.auth.models import User
    from app.telegram_bots import callbacks
    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery

    e = work_delivery
    sequence = count(10000000)
    monkeypatch.setattr(callbacks, "uuid4", lambda: UUID(hex="a" * 24 + str(next(sequence))))
    plans, _admission, act, _mark, _parent, control = await followup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    recovery = WorkRoutingFollowupRecovery(plans)
    kwargs = dict(chat_id=e.link.chat_id, message_id=804, message_thread_id=None)
    failed = await recovery.refresh(e.link, control.id, **kwargs, guard=await guard_for(e))
    e.outcome_identity = failed
    await clear_outcome_cleanup(e)
    other = await e.live.users.create_user(User(email="late-recipient@example.com"))
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(other.id), permissions=3))
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, failed)
        assert (row.state, row.failed_reason) == ("failed", "work-outcome-refresh-required")
    repaired = await recovery.refresh(e.link, control.id, **kwargs, guard=await guard_for(e))
    assert repaired != failed
    await dispatch(e, 805)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, repaired)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        assert row.payload["work_outcome_basis"]["routing"]["routing"]["recipients"][0]["user_id"] == str(other.id)
        assert (await reader.get(TelegramOutboxRow, failed)).state == "skipped"
        assert (await reader.get(TelegramActRow, act.id)).attempts == act.attempts + 1


@pytest.mark.asyncio
async def test_successive_refreshes_preserve_original_input_and_choice_lifetime(work_delivery, monkeypatch):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    plans, admission, _act, _mark, parent, control = await followup(e, monkeypatch)
    recovery = WorkRoutingFollowupRecovery(plans)
    anchor = 804
    first_identity = parent.id
    created = control.created_at
    for index in range(2):
        async with e.db.sessions.begin() as writer:
            (await writer.get(UserRow, e.recipient_id)).email = f"updated-{index}@example.com"
        identity = await recovery.refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=anchor, message_thread_id=None, guard=await guard_for(e))
        e.outcome_identity = identity
        await clear_outcome_cleanup(e)
        anchor += 1
        await dispatch(e, anchor)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
            assert row.payload["work_outcome_basis"]["refresh"]["input_outbox_id"] == first_identity
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            assert control.created_at == created
    staged = await admission.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=anchor, message_thread_id=None, guard=await guard_for(e))
    assert (await admission.acts.check(staged.id)).state == "done"


@pytest.mark.asyncio
async def test_restored_recipient_after_known_refusal_gets_fresh_unreserved_choice(work_delivery, monkeypatch):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_routing_followups import route_followup

    from app.telegram_bots.work_followup_refresh import WorkRoutingFollowupRecovery

    e = work_delivery
    plans, admission, _act, _mark, parent, control = await followup(e, monkeypatch)
    original = await route_followup(e, admission, control)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    refused = await admission.acts.check(original.id)
    assert (refused.state, refused.reason) == ("refused", "recipient-ineligible")
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 3
    identity = await WorkRoutingFollowupRecovery(plans).refresh(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    assert identity != parent.id
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    await dispatch(e, 805)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        fresh = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert "work_action_id" not in fresh.payload
        saved = await reader.get(TelegramActRow, original.id)
        assert saved.state == "refused" and saved.body == dict(original.body)
    new = await admission.stage(e.link, fresh.id, chat_id=e.link.chat_id, message_id=805, message_thread_id=None, guard=await guard_for(e))
    assert new.id != original.id and (await admission.acts.check(new.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["message_id", "version", "operation_id"])
async def test_malformed_historical_reply_cannot_break_or_pin_inbound_collection(work_delivery, monkeypatch, damage):
    from copy import deepcopy
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    _plans, _admission, _act, mark, parent, _control = await followup(e, monkeypatch)
    key = (mark.link_id, mark.message_id, mark.chat_id)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramInboundMarkRow, key)).closed_at = e.clock.now - timedelta(days=8)
        row = await writer.get(TelegramOutboxRow, parent.id)
        payload = deepcopy(row.payload)
        basis = payload["work_outcome_basis"]
        if damage == "version":
            basis["version"] = "malformed"
        else:
            basis["input"][damage] = "malformed"
        row.payload = payload
    await sweep_rows(e.db.sessions, e.clock.now, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, key) is None

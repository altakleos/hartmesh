"""Own execution replies need original intent and current authority at the SDK."""

from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import Refusal
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramPersonSettingsRow


async def busy_notice(e, monkeypatch):
    from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
    from app.telegram_bots.work_execution_sources import WorkExecutionReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    _views, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=Refusal("capacity", reason="busy")))
    act = await e.acts.check(act.id)
    assert act.state == "waiting" and act.reason == "busy"
    plans = WorkExecutionPlans(reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    e.identity = await plans.observe(e.link, act.id)
    return plans, act


async def dispatch_edit(e, identity):
    from test_telegram_bot_adapter import message

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e.clock.advance(10)
    message(e.channel.bot.request, "editMessageText", identity=identity)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.state, row.failed_reason, row.skip_reason, e.dispatch_errors)


def execution_plans(e):
    from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
    from app.telegram_bots.work_execution_sources import WorkExecutionReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    plans = WorkExecutionPlans(reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks, acts=e.acts)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    return plans


async def stop_notice(e):
    from test_telegram_work_stop_controls import stage as stop_stage
    from test_telegram_work_stop_controls import stop_control

    _views, controller, button, attempt_id, _captured, manager = await stop_control(e)
    act = await e.acts.check((await stop_stage(e, controller, button)).id)
    return execution_plans(e), act, attempt_id, manager


async def unknown_notice(e, monkeypatch):
    from app.gateway import work_execution

    real = work_execution.activate_work
    _views, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=RuntimeError("response unavailable")))
    for _ in range(e.sender.outbox.config.acts.max_attempts):
        act = await e.acts.check(act.id)
    assert act.state == "uncertain"
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, act.id)
    return plans, act, real


@pytest.mark.asyncio
async def test_busy_start_reply_is_durable_and_has_original_cancel_control(work_delivery, monkeypatch):
    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    await dispatch(e, 3801)
    calls = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage" and "Too much" in call.parameters["text"]]
    assert len(calls) == 1 and e.work["objective"] in calls[0]["text"] and "two minutes" in calls[0]["text"], e.dispatch_errors
    button = await selected(e, "cancel_wait")
    assert button.target_id == act.id and button.payload["act_id"] == act.id
    assert await plans.observe(e.link, act.id) == e.identity
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["marker", "body", "inspect"])
async def test_execution_final_sdk_fence_rejects_late_source_changes(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    _plans, act = await busy_notice(e, monkeypatch)
    sources = e.channel._services.work_execution_sources
    real, changed = sources.settings.get, []

    async def retire(*args, **kwargs):
        result = await real(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if change == "marker":
                    control = await writer.get(TelegramCallbackRow, act.id)
                    control.payload = {**control.payload, "work_action_id": "f" * 32}
                elif change == "body":
                    row = await writer.get(TelegramActRow, act.id)
                    row.body = {**row.body, "expected_assignment_revision": row.body["expected_assignment_revision"] + 1}
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
        return result

    monkeypatch.setattr(sources.settings, "get", retire)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 3810)
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"], e.dispatch_errors
    assert not e.launches


@pytest.mark.asyncio
async def test_cancel_wait_edits_its_reply_and_never_launches(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    await dispatch(e, 3820)
    control = await selected(e, "cancel_wait")
    result = await plans.cancel(e.link, control.id, chat_id=e.link.chat_id, message_id=3820, message_thread_id=None, guard=await guard_for(e))
    assert result.reason == "cancelled"
    await dispatch_edit(e, 3820)
    edits = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(edits) == 1 and edits[0]["message_id"] == 3820 and edits[0]["text"] == "Not started.", e.dispatch_errors
    assert not e.launches
    assert (await e.acts.check(act.id)).reason == "cancelled"
    assert (await plans.cancel(e.link, control.id, chat_id=e.link.chat_id, message_id=3820, message_thread_id=None, guard=await guard_for(e))).reason == "cancelled"


@pytest.mark.asyncio
async def test_stop_outcome_keeps_requested_separate_from_original_settlement(work_delivery):
    from test_telegram_work_stop_controls import stage as stop_stage
    from test_telegram_work_stop_controls import stop_control

    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    from _storage_spaces_test_support import operation

    from deerflow.agent_instances.work_contract import DelegateWork

    # A distinct assignment has no unanswered question blocking its Resume.
    e.work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=operation(), objective="Review the release guide", success_criteria="Explain verified omissions"))
    _views, controller, button, attempt_id, _captured, manager = await stop_control(e)
    act = await stop_stage(e, controller, button)
    act = await e.acts.check(act.id)
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, act.id)
    await dispatch(e, 3830)
    assert not e.dispatch_errors, e.dispatch_errors
    messages = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert "Stopping" in messages[-1] and "Stopped at" not in messages[-1]
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkAttemptRow, attempt_id)
        original.status, original.settled_at = "cancelled", e.clock.now()
    assert await plans.observe(e.link, act.id) == e.identity
    await dispatch_edit(e, 3830)
    edits = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(edits) == 1 and "Stopped at" in edits[-1]["text"], e.dispatch_errors
    assert e.work["success_criteria"] in edits[-1]["text"]
    resume = await selected(e, "work_start")
    assert resume is not None and resume.target_id == e.work["id"] and resume.message_id == 3830
    manager.cancel.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_outcome_never_adopts_a_changed_original_run_binding(work_delivery):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    plans, act, attempt_id, manager = await stop_notice(e)
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkAttemptRow, attempt_id)
        original.run_id, original.status, original.settled_at = "f" * 32, "cancelled", e.clock.now()
    with pytest.raises(ProductSourceRetired):
        await plans.observe(e.link, act.id)
    manager.cancel.assert_awaited_once()


@pytest.mark.asyncio
async def test_success_closes_busy_controls_behind_single_canonical_status_root(work_delivery, monkeypatch):
    from sqlalchemy import select

    from app.gateway import work_execution
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    real = work_execution.activate_work
    plans, act = await busy_notice(e, monkeypatch)
    await dispatch(e, 3840)
    control = await selected(e, "cancel_wait")
    monkeypatch.setattr(work_execution, "activate_work", real)
    result = await e.acts.check(act.id)
    assert result.state == "done" and len(e.launches) == 1
    assert await plans.observe(e.link, act.id) == e.identity
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        roots = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + result.result["activation_receipt"]["attempt_id"])))
        assert len(roots) == 1


@pytest.mark.asyncio
async def test_explicit_retry_keeps_original_operation_and_starts_once(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.gateway import work_execution

    e = work_delivery
    plans, act, real = await unknown_notice(e, monkeypatch)
    await dispatch(e, 3850)
    control = await selected(e, "act_check")
    assert control.target_id == act.id
    monkeypatch.setattr(work_execution, "activate_work", real)
    result = await plans.retry(e.link, control.id, chat_id=e.link.chat_id, message_id=3850, message_thread_id=None, guard=await guard_for(e))
    assert result.state == "done" and result.body == act.body and result.id == act.id and len(e.launches) == 1


@pytest.mark.asyncio
async def test_refused_runless_original_can_be_recovered_only_by_explicit_same_action(work_delivery, monkeypatch):
    from sqlalchemy import select
    from test_telegram_work_interaction_delivery import guard_for

    from app.gateway import services, work_execution
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    real = work_execution.activate_work
    _views, controller, button = await start_control(e, monkeypatch)
    launcher = services.start_run
    act = await stage(e, controller, button)
    monkeypatch.setattr(services, "start_run", AsyncMock(side_effect=RuntimeError("host response unavailable")))
    act = await e.acts.check(act.id)
    assert act.state == "pending" and not e.launches
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=Refusal("restarting", reason="restarting")))
    act = await e.acts.check(act.id)
    assert act.state == "refused" and act.reason == "restarting"
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == act.id))
        attempt_id = original.id
        assert original.run_id is None and original.status == "starting"
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, act.id)
    await dispatch(e, 3860)
    control = await selected(e, "act_check")
    assert control.target_id == act.id
    monkeypatch.setattr(work_execution, "activate_work", real)
    monkeypatch.setattr(services, "start_run", launcher)
    result = await plans.retry(e.link, control.id, chat_id=e.link.chat_id, message_id=3860, message_thread_id=None, guard=await guard_for(e))
    assert result.state == "done" and result.id == act.id and result.body == act.body
    assert result.result["activation_receipt"]["attempt_id"] == attempt_id and len(e.launches) == 1


@pytest.mark.asyncio
async def test_refusal_reply_full_preview_owns_a_fresh_start_consent(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.gateway import work_execution

    e = work_delivery
    real = work_execution.activate_work
    views, controller, first = await start_control(e, monkeypatch)
    act = await stage(e, controller, first)
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=Refusal("restarting", reason="restarting")))
    act = await e.acts.check(act.id)
    assert act.state == "refused"
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, act.id)
    await dispatch(e, 3870)
    async with e.db.sessions() as reader:
        from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

        parent = await reader.get(TelegramOutboxRow, e.identity)
        shown = "\n".join(part["visible_text"] for part in parent.parts)
        assert e.work["objective"] in shown and e.work["success_criteria"] in shown
        from sqlalchemy import select

        button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "work_start"))
    assert button and button.id != first.id
    new = await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=3870, message_thread_id=None, guard=await guard_for(e))
    assert new.id != act.id and new.body["expected_assignment_revision"] == act.body["expected_assignment_revision"]
    monkeypatch.setattr(work_execution, "activate_work", real)
    assert (await e.acts.check(new.id)).state == "done" and len(e.launches) == 1


@pytest.mark.asyncio
async def test_pending_recovery_keeps_acknowledged_busy_cancel_until_known_outcome(work_delivery, monkeypatch):
    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    await dispatch(e, 3880)
    control = await selected(e, "cancel_wait")
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=RuntimeError("response unavailable")))
    pending = await e.acts.check(act.id)
    assert pending.state == "pending"
    await plans.observe(e.link, act.id)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramCallbackRow, control.id)
        assert saved.consumed_at is None


@pytest.mark.asyncio
async def test_completion_before_first_materialization_closes_empty_reply(work_delivery, monkeypatch):
    from app.gateway import work_execution
    from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    real, materialize = work_execution.activate_work, WorkExecutionPlans.materialize
    monkeypatch.setattr(WorkExecutionPlans, "materialize", AsyncMock(return_value=False))
    plans, act = await busy_notice(e, monkeypatch)
    monkeypatch.setattr(WorkExecutionPlans, "materialize", materialize)
    monkeypatch.setattr(work_execution, "activate_work", real)
    assert (await e.acts.check(act.id)).state == "done"
    await plans.observe(e.link, act.id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "skipped" and not row.parts


@pytest.mark.asyncio
async def test_unpublished_busy_reply_can_recover_after_intermediate_pending_state(work_delivery, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=RuntimeError("response unavailable")))
    assert (await e.acts.check(act.id)).state == "pending"
    await dispatch(e, 3890)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "skipped"
    for _ in range(e.sender.outbox.config.acts.max_attempts - 1):
        result = await e.acts.check(act.id)
    assert result.state == "uncertain"
    await plans.observe(e.link, act.id)
    await dispatch(e, 3891)
    calls = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage" and "not confirmed" in call.parameters["text"]]
    assert len(calls) == 1 and not e.launches, e.dispatch_errors


@pytest.mark.asyncio
async def test_success_during_admitted_sdk_send_preserves_its_claim_and_ack(work_delivery, monkeypatch):
    import asyncio

    from test_telegram_bot_adapter import message

    from app.gateway import work_execution
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    real = work_execution.activate_work
    plans, act = await busy_notice(e, monkeypatch)
    reply = message(e.channel.bot.request, identity=3900)
    reply.release = asyncio.Event()
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    try:
        await asyncio.wait_for(reply.started.wait(), 5)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            claim = (row.claim_owner, row.claim_id, row.claimed_at)
            assert row.state == "sending" and all(value is not None for value in claim)
        monkeypatch.setattr(work_execution, "activate_work", real)
        assert (await e.acts.check(act.id)).state == "done"
        await plans.observe(e.link, act.id)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            assert row.state == "sending" and (row.claim_owner, row.claim_id, row.claimed_at) == claim
    finally:
        reply.release.set()
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and row.parts[-1]["message_ids"] == [3900]
    await plans.observe(e.link, act.id)
    assert len(e.launches) == 1


@pytest.mark.asyncio
async def test_cancel_fence_still_owns_its_reply_when_receipt_observation_fails(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    await dispatch(e, 3910)
    control = await selected(e, "cancel_wait")
    monkeypatch.setattr(e.acts, "receipt_for", AsyncMock(side_effect=RuntimeError("receipt store temporarily unreadable")))
    result = await plans.cancel(e.link, control.id, chat_id=e.link.chat_id, message_id=3910, message_thread_id=None, guard=await guard_for(e))
    assert result.cancel_requested_at is not None and result.reason == "cancelled"
    await dispatch_edit(e, 3910)
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["bot_id", "binding_generation", "chat_id"])
async def test_observer_refuses_other_binding_before_borrowing_receipt_authority(work_delivery, monkeypatch, field):
    from dataclasses import replace

    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    receipt = AsyncMock(wraps=e.acts.reconcile_receipt)
    monkeypatch.setattr(e.acts, "reconcile_receipt", receipt)
    other = replace(e.link, **{field: getattr(e.link, field) + 1})
    with pytest.raises(ProductSourceRetired):
        await plans.observe(other, act.id)
    receipt.assert_not_awaited()


@pytest.mark.asyncio
async def test_busy_reply_uses_the_actual_activation_wait_configuration(work_delivery, monkeypatch):
    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    config = e.acts.outbox.config
    e.acts.outbox.config = config.model_copy(update={"queue": config.queue.model_copy(update={"max_wait_seconds": 90})})
    await plans.observe(e.link, act.id)
    await dispatch(e, 3940)
    messages = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage" and "Too much" in call.parameters["text"]]
    assert len(messages) == 1 and "90 seconds" in messages[0] and "two minutes" not in messages[0]


@pytest.mark.asyncio
async def test_stop_child_receipt_and_dispatch_own_the_parent_reply(work_delivery, monkeypatch):
    from test_telegram_work_stop_controls import stage as stop_stage
    from test_telegram_work_stop_controls import stop_control

    from deerflow.agent_instances.work import AgentWork
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    _views, controller, button, _attempt_id, _captured, manager = await stop_control(e)
    act = await stop_stage(e, controller, button)
    original, operations = AgentWork.command, []

    async def progress_before_command(self, **kwargs):
        operations.append(kwargs["request"].operation_id)
        if len(operations) <= 2:
            async with e.db.sessions.begin() as writer:
                work = await writer.get(AgentWorkRow, e.work["id"])
                work.revision += 1
        return await original(self, **kwargs)

    monkeypatch.setattr(AgentWork, "command", progress_before_command)
    result = await e.acts.check(act.id)
    assert result.state == "done" and len(result.operation_ids) == 2 and result.result["stop_child_id"] == result.operation_ids[-1]
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, result.id)
    await dispatch(e, 3950)
    messages = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert "Stopping" in messages[-1] and not e.dispatch_errors, e.dispatch_errors
    manager.cancel.assert_awaited_once()


@pytest.mark.asyncio
async def test_denied_start_names_the_current_supervisor(work_delivery, monkeypatch):
    from sqlalchemy import select

    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    _views, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=AgentDenied("permission withdrawn", reason="denied")))
    assert (await e.acts.check(act.id)).state == "refused"
    plans = execution_plans(e)
    e.identity = await plans.observe(e.link, act.id)
    async with e.db.sessions() as reader:
        instance = await reader.get(AgentInstanceRow, e.instance.id)
        name = await reader.scalar(select(UserRow.email).where(UserRow.id == instance.supervisor_id))
        assert name
    await dispatch(e, 3960)
    messages = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert name in messages[-1]


@pytest.mark.asyncio
async def test_late_saved_start_body_change_does_not_consume_original_cancel_consent(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    await dispatch(e, 3830)
    control = await selected(e, "cancel_wait")
    real = e.acts.cancel_activation
    changed = []

    async def late(identity, **kwargs):
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, identity)
            row.body = {**row.body, "expected_revision": row.body["expected_revision"] + 1}
            changed.append(True)
        return await real(identity, **kwargs)

    monkeypatch.setattr(e.acts, "cancel_activation", late)
    try:
        await plans.cancel(e.link, control.id, chat_id=e.link.chat_id, message_id=3830, message_thread_id=None, guard=await guard_for(e))
    except (CallbackRefused, ProductSourceRetired):
        pass
    assert changed
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, act.id)
        assert saved.cancel_requested_at is None
    assert not e.launches


@pytest.mark.asyncio
async def test_original_ack_moved_to_other_chat_cannot_support_private_execution_delivery(work_delivery, monkeypatch):
    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    _plans, act = await busy_notice(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        callback = await writer.get(TelegramCallbackRow, act.id)
        parent = await writer.get(TelegramOutboxRow, callback.payload["anchor_outbox_id"])
        other = parent.chat_id + 100
        parent.chat_id = other
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id)):
            control.chat_id = other
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 3831)
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
    assert not e.launches


@pytest.mark.asyncio
async def test_pending_cancel_edit_keeps_its_original_target_through_source_refresh(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    plans, act = await busy_notice(e, monkeypatch)
    await dispatch(e, 3850)
    button = await selected(e, "cancel_wait")
    await plans.cancel(e.link, button.id, chat_id=e.link.chat_id, message_id=3850, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        assert row.parts[-1]["kind"] == "edit" and row.parts[-1]["target_message_id"] == 3850
        work = await writer.get(AgentWorkRow, e.work["id"])
        work.revision, work.progress = work.revision + 1, "New progress during the final send fence"
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "pending"
        assert row.parts[-1]["kind"] == "edit"
        assert row.parts[-1]["target_message_id"] == 3850
    await dispatch_edit(e, 3850)

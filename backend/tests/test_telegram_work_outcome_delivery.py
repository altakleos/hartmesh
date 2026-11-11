"""A result reply is delivered from its exact receipt without repeating actions."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_outcome_reads import review_confirmation
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_completed_acceptance_delivers_once_and_retires_original_controls(work_delivery):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources

    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    saved = await stage(e, actions, control)
    done = await actions.acts.check(saved.id)
    plans = WorkOutcomePlans(e.plans, actions.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    e.outcome_identity = identity
    assert await plans.plan(e.link, saved.id, guard=await guard_for(e)) == identity
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).closed_reason == "work-done"
        assert (await reader.get(TelegramActRow, saved.id)).attempts == done.attempts
    await clear_outcome_cleanup(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, e.dispatch_errors)
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert "Accepted by you" in call.parameters["text"]


async def clear_outcome_cleanup(e):
    from datetime import timedelta

    # Removal and result are independent reply rows. Park the result while
    # scripting its cleanup so UUID tie order cannot consume the wrong reply.
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.outcome_identity)
        deadline = row.not_before
        row.not_before = e.clock() + timedelta(hours=1)
    try:
        await clear_old(e)
    finally:
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, e.outcome_identity)).not_before = deadline


async def outcome(e, *, uncertain=False):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources

    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    saved = await stage(e, actions, control)
    if uncertain:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, saved.id)
            row.state, row.attempts = "uncertain", 3
    else:
        await actions.acts.check(saved.id)
    plans = WorkOutcomePlans(e.plans, actions.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    e.outcome_identity = identity
    return plans, saved.id, identity


@pytest.mark.asyncio
async def test_pending_delivery_retains_its_settled_action(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = work_delivery
    _plans, act_id, identity = await outcome(e)
    now = e.clock()
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, act_id)).created_at = now - timedelta(days=ACT_RETENTION_DAYS + 1)
    assert await _sweep_acts(e.db.sessions, now) == 0
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, identity)).state = "sent"
    assert await _sweep_acts(e.db.sessions, now) == 1


@pytest.mark.asyncio
async def test_uncertain_result_retry_control_keeps_original_operation(work_delivery):
    e = work_delivery
    plans, act_id, identity = await outcome(e, uncertain=True)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", ["".join(__import__("traceback").format_exception(exc)) for exc in e.dispatch_errors]
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert len(controls) == 1
        assert controls[0].kind == "act_check" and controls[0].target_id == act_id
        assert not controls[0].payload.get("presentation_pending")
        assert (await reader.get(TelegramActRow, act_id)).attempts == 3
    assert await plans.plan(e.link, act_id, guard=await guard_for(e)) == identity


@pytest.mark.asyncio
async def test_input_completion_waits_for_outcome_ack(work_delivery):
    from test_telegram_work_input_admission import controller, stage_input
    from test_telegram_work_input_capture import accept, capture, prompt

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    act = await stage_input(e, host, mark)
    assert (await host.controls.acts.check(act.id)).state == "done"
    plans = WorkOutcomePlans(e.plans, host.controls.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, act.id, guard=await guard_for(e))
    key = (mark.link_id, mark.message_id, mark.chat_id)
    async with e.db.sessions() as reader:
        current = await reader.get(TelegramInboundMarkRow, key)
        assert not current.route_complete and identity in current.delivery_ids
        assert current.state != "done"
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, key)).state == "done"
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["grant", "journal", "supervisor"])
async def test_final_result_authority_rejects_late_changes(work_delivery, monkeypatch, damage):
    import app.telegram_bots.work_outcome_delivery as module
    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow

    e = work_delivery
    _plans, act_id, identity = await outcome(e)
    await clear_outcome_cleanup(e)
    original = module.current_attention

    async def changed(*args, **kwargs):
        async with e.db.sessions.begin() as writer:
            if damage == "grant":
                grant = await writer.scalar(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref, AgentInstanceGrantRow.user_id == e.link.owner_user_id))
                grant.permissions = 1
            elif damage == "journal":
                (await writer.get(TelegramActRow, act_id)).state = "uncertain"
            else:
                from deerflow.persistence.user.model import UserRow

                instance = await writer.get(AgentInstanceRow, e.link.agent_ref)
                (await writer.get(UserRow, instance.supervisor_id)).email = "renamed-supervisor@example.com"
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", changed)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 803)
    assert len(e.channel.bot.request.calls) == before
    assert e.dispatch_errors and all(isinstance(error, (DeliveryRetry, ProductSourceRetired)) for error in e.dispatch_errors)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state != "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["body", "protect", "markup", "version", "topic", "topic_and_basis"])
async def test_forged_result_plan_never_reaches_sdk(work_delivery, damage):
    from copy import deepcopy

    e = work_delivery
    _plans, _act_id, identity = await outcome(e)
    await clear_outcome_cleanup(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        if damage in {"topic", "topic_and_basis"}:
            row.message_thread_id = 73
            if damage == "topic_and_basis":
                payload = deepcopy(row.payload)
                payload["work_outcome_basis"]["destination"]["message_thread_id"] = 73
                row.payload = payload
        elif damage in {"protect", "markup"}:
            parts = deepcopy(row.parts)
            parts[0]["protect_content" if damage == "protect" else "force_reply"] = True
            row.parts = parts
        else:
            payload = deepcopy(row.payload)
            if damage == "body":
                payload["work_outcome_basis"]["context"]["act"]["body"]["expected_revision"] += 1
            else:
                payload["presentation_version"] = False
            row.payload = payload
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(row, "payload")
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 803)
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state != "sent"


@pytest.mark.asyncio
async def test_collected_acknowledged_reply_is_not_republished(work_delivery):
    e = work_delivery
    plans, act_id, identity = await outcome(e)
    await clear_outcome_cleanup(e)
    await dispatch(e, 803)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        assert row.state == "sent"
        await writer.delete(row)
    assert await plans.plan(e.link, act_id, guard=await guard_for(e)) == identity
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, identity) is None


@pytest.mark.asyncio
async def test_terminally_retired_unsent_reply_can_be_replanned(work_delivery):
    e = work_delivery
    plans, act_id, identity = await outcome(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.skip_reason = "skipped", "source-gone"
    replacement = await plans.plan(e.link, act_id, guard=await guard_for(e))
    assert replacement != identity
    assert await plans.plan(e.link, act_id, guard=await guard_for(e)) == replacement


async def long_input(e):
    from test_telegram_inbox import incoming
    from test_telegram_work_input_admission import controller, stage_input
    from test_telegram_work_input_capture import capture, prompt

    from app.telegram_bots.inbox import InboxRepository
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    request, prompt_id, _seen = await prompt(e)
    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    inbox = InboxRepository(e.db.sessions)
    note = "Complete review note <&> " + "😀" * 3000
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802), sanitized_input={"text": note})
    await capture(e, inbox, mark, prompt_id)
    actions = controller(e, inbox)
    act = await stage_input(e, actions, mark)
    assert (await actions.controls.acts.check(act.id)).state == "done"
    plans = WorkOutcomePlans(e.plans, actions.controls.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, act.id, guard=await guard_for(e))
    return plans, act.id, identity, mark, note


@pytest.mark.asyncio
async def test_multipart_result_resumes_without_repeating_ack_or_action(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomeSources
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    plans, act_id, identity, mark, note = await long_input(e)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        count = len(row.parts)
        assert count >= 2 and row.parts[0]["state"] == "sent" and row.parts[1]["state"] == "pending"
        assert (await reader.get(TelegramActRow, act_id)).continuation is None
        assert not (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_complete
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)

    async def forbidden(*args, **kwargs):
        raise AssertionError("delivery must never repeat the action")

    monkeypatch.setattr(e.human.work, "command", forbidden)
    for index in range(1, count):
        await dispatch(e, 803 + index)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and [part["message_ids"] for part in row.parts] == [[803 + index] for index in range(count)]
        assert note in "".join(part["visible_text"] for part in row.parts)
        assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).state == "done"
    assert len([call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]) == count


@pytest.mark.asyncio
async def test_missing_topic_holds_original_multipart_plan_for_source_aware_repair(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    _plans, act_id, identity, mark, _note = await long_input(e)
    await dispatch(e, 803)
    e.clock.advance(10)
    e.channel.bot.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "work-outcome-refresh-required"
        assert row.claim_id is None and row.payload["presentation_version"] == 0
        assert len(row.parts) >= 2 and row.parts[0]["message_ids"] == [803]
        assert (await reader.get(TelegramActRow, act_id)).attempts == 1
        assert not (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_complete


@pytest.mark.asyncio
async def test_obsolete_input_result_can_refresh_before_ack(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramPersonSettingsRow

    e = work_delivery
    plans, act_id, identity, mark, _note = await long_input(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    # The stale recipe is retired at the actual SDK boundary without sending.
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 803)
    assert len(e.channel.bot.request.calls) == before
    replacement = await plans.plan(e.link, act_id, guard=await guard_for(e))
    assert replacement != identity
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
        current = await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        assert not current.route_complete and replacement in current.delivery_ids
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, replacement)).parts)
    for index in range(count):
        await dispatch(e, 804 + index)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, replacement)).state == "sent", e.dispatch_errors
        assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unavailable", "multiple_ids", "boolean_id", "naive_time"])
async def test_terminal_part_without_ack_cannot_complete_outcome(work_delivery, damage):
    from copy import deepcopy

    e = work_delivery
    _plans, act_id, identity = await outcome(e, uncertain=True)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        parts = deepcopy(row.parts)
        parts[0].update(state="sent", sent_at=e.clock().isoformat(), message_ids=[803])
        if damage == "unavailable":
            parts[0]["state"] = "unavailable"
        elif damage == "multiple_ids":
            parts[0]["message_ids"] = [803, 804]
        elif damage == "boolean_id":
            parts[0]["message_ids"] = [True]
        else:
            parts[0]["sent_at"] = "2026-11-11T18:00:00"
        row.parts = parts
        await e.sender._complete(writer, row)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "invalid-work-outcome-receipt"
        assert (await reader.get(TelegramActRow, act_id)).continuation is None
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert callback.payload["presentation_pending"] is True


@pytest.mark.asyncio
async def test_pending_callback_outcome_pins_its_original_destination_proof(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import _sweep_outbox

    e = work_delivery
    plans, act_id, identity = await outcome(e)
    now = e.clock()
    async with e.db.sessions.begin() as writer:
        control = await writer.get(TelegramCallbackRow, act_id)
        parent_id = control.payload["anchor_outbox_id"]
        parent = await writer.get(TelegramOutboxRow, parent_id)
        parent.created_at = parent.closed_at = now - timedelta(days=401)
        control.created_at = control.consumed_at = now - timedelta(days=401)
    await _sweep_outbox(e.db.sessions, now, 180)
    await plans.callbacks.sweep(now)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, parent_id) is not None
        assert await reader.get(TelegramCallbackRow, act_id) is not None
        assert (await reader.get(TelegramOutboxRow, identity)).state == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "unknown_version", "malformed_phase"])
async def test_settled_work_receipt_expires_but_unknown_continuations_are_retained(work_delivery, damage):
    from copy import deepcopy
    from datetime import timedelta

    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = work_delivery
    _plans, act_id, _identity = await outcome(e)
    await clear_outcome_cleanup(e)
    await dispatch(e, 803)
    now = e.clock()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act_id)
        assert row.continuation["work_outcome"]["phase"][0] == "done"
        row.created_at = now - timedelta(days=ACT_RETENTION_DAYS + 1)
        value = deepcopy(row.continuation)
        if damage == "unknown_version":
            value["version"] = 2
        elif damage == "malformed_phase":
            value["work_outcome"]["phase"] = ["done"]
        row.continuation = value
    assert await _sweep_acts(e.db.sessions, now) == (1 if damage is None else 0)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, act_id) is None) == (damage is None)

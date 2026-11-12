"""Replies to input-owned confirmations save comments without discarding drafts."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_input_navigation import typed_draft
from test_telegram_work_input_refresh import deliver_refreshed
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_retained_drafts import draft
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["review", "decision"])
async def test_reply_to_confirmation_saves_comment_and_keeps_unsubmitted_draft(work_delivery, monkeypatch, purpose):
    e = work_delivery
    if purpose == "review":
        request, _inbox, original = await draft(e, monkeypatch)
        install_inputs(e)
    else:
        request, _inputs, original = await typed_draft(e, monkeypatch)
    old_id = e.identity
    control = await selected(e, "changes_confirm" if purpose == "review" else "decide_confirm")
    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    update = incoming(message=901, update_id=2901, account=e.link.chat_id, reply=803)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        fresh = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert fresh.route_kind == "work_input" and fresh.route_basis["work_input"]["purpose"] == "comment"
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"
        held = await reader.get(TelegramInboundMarkRow, (e.link.id, original.message_id, e.link.chat_id))
        assert not held.route_complete and held.route_basis["work_input"] == original.route_basis["work_input"]
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        parent = await reader.get(TelegramOutboxRow, old_id)
        state = parent.payload["work_request_basis"]["interaction"]
        assert state["note" if purpose == "review" else "decision"] == original.route_basis["work_input"]["text"]
        outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == actions[0].id))
        assert outcome is not None and outcome.payload["work_outcome_basis"]["version"] == 5
        e.identity = outcome.id
    await dispatch(e, 804)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent", e.dispatch_errors
        assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))).route_complete
        assert not (await reader.get(TelegramInboundMarkRow, (e.link.id, original.message_id, e.link.chat_id))).route_complete
    await e.channel._services.work_text_inputs.handle_text(e.channel, update, captured_link=e.link)
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
    # Its next Confirm refreshes the original draft against the new comment;
    # the comment never consumes or silently submits that original statement.
    await press_work(e, control, message_id=803, update_id=2902)
    async with e.db.sessions() as reader:
        refreshed = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id != old_id, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == original.operation_ids[0]))
        assert refreshed is not None
        state = refreshed.payload["work_request_basis"]["interaction"]
        assert state["note" if purpose == "review" else "decision"] == original.route_basis["work_input"]["text"]
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        e.identity = refreshed.id
    await deliver_refreshed(e, 805)
    fresh_control = await selected(e, "changes_confirm" if purpose == "review" else "decide_confirm")
    await press_work(e, fresh_control, message_id=805, update_id=2903)
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 2 and all(row.state == "done" for row in actions)
        assert {row.kind for row in actions} == {"respond", "changes_requested" if purpose == "review" else "decide"}


@pytest.mark.asyncio
@pytest.mark.parametrize("retired", ["expired", "reserved"])
async def test_expired_or_reserved_confirmation_cannot_admit_a_comment(work_delivery, monkeypatch, retired):
    from datetime import timedelta

    from test_telegram_work_control_admission import admissions, stage

    e = work_delivery
    _request, _inbox, original = await draft(e, monkeypatch)
    install_inputs(e)
    expected = None
    if retired == "expired":
        e.clock.now = original.accepted_at + timedelta(hours=24, seconds=1)
    else:
        expected = await stage(e, admissions(e), await selected(e, "changes_confirm"))
    assert await e.channel.handle_update(incoming(message=901, update_id=2901, account=e.link.chat_id, reply=803))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.operation_ids and not mark.route_complete
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == (1 if expected else 0)
        if expected:
            assert actions[0].id == expected.id and actions[0].body == expected.body and actions[0].attempts == 0


@pytest.mark.asyncio
async def test_captured_comment_survives_discarding_original_draft(work_delivery, monkeypatch):
    from test_telegram_work_card_inputs import accepted
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    _request, _inbox, _original = await draft(e, monkeypatch)
    _plans, inputs = install_inputs(e)
    discard = await selected(e, "dismiss")
    host = WorkCardInputs(admissions(e))
    origin = await accepted(e, reply=803, message=901)
    captured = await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    await press_work(e, discard, message_id=803, update_id=2902)
    assert await inputs.resume(e.channel, captured)
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].id == captured.operation_ids[0]
        assert actions[0].kind == "respond" and actions[0].state == "done"


@pytest.mark.asyncio
async def test_unknown_comment_reconciles_after_original_draft_is_discarded(work_delivery, monkeypatch):
    from app.telegram_bots.inbox import InboxRow
    from app.telegram_bots.work_actions import request_for
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    _request, _inbox, _original = await draft(e, monkeypatch)
    plans, inputs = install_inputs(e)
    discard = await selected(e, "dismiss")
    respond, calls = HumanInput.respond, []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise TimeoutError()

    monkeypatch.setattr(HumanInput, "respond", unknown)
    assert await e.channel.handle_update(incoming(message=901, update_id=2901, account=e.link.chat_id, reply=803))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id)))
    action = await plans.acts._work_record(origin.operation_ids[0])
    assert action.state == "pending" and action.attempts == 1 and calls == [action.id]
    await press_work(e, discard, message_id=803, update_id=2902)
    await respond(e.human, actor=e.actor, request_id=action.request_id, request=request_for(action.kind, action.body, identity=action.id))
    assert await inputs.resume(e.channel, origin)
    saved = await plans.acts._work_record(action.id)
    assert saved.state == "done" and saved.attempts == 1 and calls == [action.id]


@pytest.mark.asyncio
async def test_fresh_capture_still_rechecks_unreserved_original_controls(work_delivery, monkeypatch):
    from test_telegram_work_card_inputs import accepted
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    await draft(e, monkeypatch)
    host = WorkCardInputs(admissions(e))
    origin = await accepted(e, reply=803, message=901)
    control = await selected(e, "changes_confirm")
    route, reached = host.inbox.route, []

    async def reserve(*args, **kwargs):
        result = await route(*args, **kwargs)
        writer = kwargs["session"]
        row = await writer.get(TelegramCallbackRow, control.id)
        row.payload = {**row.payload, "work_action_id": control.id}
        await writer.flush()
        reached.append(True)
        return result

    monkeypatch.setattr(host.inbox, "route", reserve)
    with pytest.raises(CallbackRefused):
        await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    assert reached
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))).route_kind is None
        assert "work_action_id" not in (await reader.get(TelegramCallbackRow, control.id)).payload

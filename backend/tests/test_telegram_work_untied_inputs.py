"""Untied text needs an actual choice before Work or ordinary publication."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_card_inputs import card
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def offer(e, purpose="information", *, deliver=True):
    request = await card(e, purpose)
    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    _, inputs = install_inputs(e)
    turns = SimpleNamespace(accept=AsyncMock(), accept_staged=AsyncMock(), retire=AsyncMock(), message_sent=Mock())
    e.channel._services.turns = turns
    update = incoming(message=900, update_id=2900, account=e.link.chat_id)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    assert turns.accept.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_answer" and not mark.route_complete
        assert mark.operation_ids == []
        assert not list(await reader.scalars(select(TelegramActRow)))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_answer"]["notice_id"])
        assert [[c["kind"] for c in line] for line in notice.payload["product_keyboard_basis"]] == [["answer_yes"], ["answer_no"]]
        e.identity = notice.id
    if not deliver:
        return request, inputs, turns, update, {}
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        assert notice.state == "sent", (notice.state, notice.parts, e.dispatch_errors)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"])))
    return request, inputs, turns, update, {c.kind: c for c in controls}


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "review", "decision"])
async def test_save_untied_text_only_after_actual_choice(work_delivery, purpose):
    e = work_delivery
    request, inputs, turns, update, controls = await offer(e, purpose)
    await press_work(e, controls["answer_yes"], message_id=901)
    await press_work(e, controls["answer_yes"], message_id=901, update_id=2902)
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"
        assert actions[0].id == controls["answer_yes"].id
        assert actions[0].body["text"] == update.message.text
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert "reply_to_message_id" not in mark.sanitized_input
        assert mark.route_kind == "work_input"
        outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == actions[0].id))
        assert outcome is not None
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1
    assert turns.accept.await_count == turns.accept_staged.await_count == 0
    from test_telegram_work_input_refresh import deliver_refreshed

    e.identity = outcome.id
    await deliver_refreshed(e, 902)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete


@pytest.mark.asyncio
async def test_new_message_choice_has_one_stable_actual_publication_owner(work_delivery):
    e = work_delivery
    _, inputs, turns, update, controls = await offer(e)
    await press_work(e, controls["answer_no"], message_id=901)
    original = turns.accept_staged.await_args.args[1]
    assert original.text == update.message.text and original.callback_id == controls["answer_no"].id
    assert original.human_message_kwargs == {}
    await press_work(e, controls["answer_no"], message_id=901, update_id=2902)
    assert turns.accept_staged.await_args.args[1] == original
    await press_work(e, controls["answer_yes"], message_id=901, update_id=2903)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "turn" and mark.route_complete


@pytest.mark.asyncio
async def test_untied_choice_reconciles_unknown_response_without_resubmission(work_delivery, monkeypatch):
    from app.telegram_bots.inbox import InboxRow
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    _, inputs, turns, update, controls = await offer(e)
    calls = []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise ConnectionError("Unknown response")

    monkeypatch.setattr(HumanInput, "respond", unknown)
    await press_work(e, controls["answer_yes"], message_id=901)
    await press_work(e, controls["answer_no"], message_id=901, update_id=2902)
    assert turns.accept_staged.await_count == 0
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)
    async with e.db.sessions() as reader:
        mark = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
    assert await inputs.resume(e.channel, mark)
    await press_work(e, controls["answer_yes"], message_id=901, update_id=2903)
    assert calls == [controls["answer_yes"].id]


@pytest.mark.asyncio
async def test_changed_question_retains_untied_text_for_fresh_explicit_choice(work_delivery):
    from test_telegram_work_refusal_recovery import comment

    e = work_delivery
    request, inputs, turns, update, controls = await offer(e, "review")
    await comment(e, request)
    await press_work(e, controls["answer_yes"], message_id=901)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and "work_closed" in mark.route_basis
    assert turns.accept_staged.await_count == turns.accept.await_count == 0
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)


@pytest.mark.asyncio
async def test_ordinary_clarification_has_priority_over_untied_work_question(work_delivery):
    e = work_delivery
    await card(e, "information")
    install_inputs(e)
    turns = SimpleNamespace(accept=AsyncMock(), retire=AsyncMock(), message_sent=Mock())
    e.channel._services.turns = turns
    e.channel._services.human_inputs = SimpleNamespace(handle_text=AsyncMock(return_value=True))
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id))
    await e.channel._drain_ingress()
    assert e.channel._services.human_inputs.handle_text.await_count == 1
    assert turns.accept.await_count == 0
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)) is None


@pytest.mark.asyncio
async def test_saved_untied_route_precedes_new_ordinary_clarification(work_delivery):
    e = work_delivery
    _, inputs, turns, update, _ = await offer(e)
    e.channel._services.human_inputs = SimpleNamespace(handle_text=AsyncMock(return_value=True))
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    assert e.channel._services.human_inputs.handle_text.await_count == 0
    assert turns.accept.await_count == 0


@pytest.mark.asyncio
async def test_fresh_answer_rejects_coherent_input_changes_after_action_staging(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.inbox import InboxRow

    e = work_delivery
    _, inputs, _, _, controls = await offer(e)
    host = inputs.untied
    async with e.db.sessions() as reader:
        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
    stage, reached = host.controls.acts._stage_work_act, []

    async def drift(*args, **kwargs):
        result = await stage(*args, **kwargs)
        writer = kwargs["session"]
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        from datetime import timedelta

        mark.accepted_at += timedelta(seconds=30)
        await writer.flush()
        reached.append(True)
        return result

    monkeypatch.setattr(host.controls.acts, "_stage_work_act", drift)
    with pytest.raises(CallbackRefused):
        await host.stage(e.link, origin, controls["answer_yes"], guard=e.channel._services.commands._guard(e.channel, e.link))
    assert reached
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_answer" and mark.sanitized_input["text"] == "synthetic accepted text"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["claim", "grant", "input", "choices"])
async def test_final_sdk_notice_authority_rejects_late_changes(work_delivery, monkeypatch, damage):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    _, inputs, _, _, _ = await offer(e, deliver=False)
    notice = inputs.untied.notice
    authorize, calls = notice.authorize, []

    async def change(captured, **kwargs):
        calls.append(captured.item.id)
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                if damage == "claim":
                    (await writer.get(TelegramOutboxRow, e.identity)).claim_id = "different-owner"
                elif damage == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                elif damage == "input":
                    (await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete = True
                else:
                    for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity)):
                        row.consumed_at, row.closed_reason = e.clock(), "command"
        return await authorize(captured, **kwargs)

    monkeypatch.setattr(notice, "authorize", change)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    await dispatch(e, 901)
    assert len(calls) == 2
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
    assert e.dispatch_errors


@pytest.mark.asyncio
async def test_notice_authorization_drains_through_repeated_cancellation(work_delivery, monkeypatch):
    import asyncio

    from app.telegram_bots import work_untied_delivery

    e = work_delivery
    _, inputs, _, _, _ = await offer(e, deliver=False)
    notice = inputs.untied.notice
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    real_prepare = work_untied_delivery.prepare_attention
    tasks = []

    async def prepare(*args, **kwargs):
        prepared = await real_prepare(*args, **kwargs)

        async def check(**options):
            result = await prepared(**options)
            entered.set()
            await release.wait()
            finished.set()
            return result

        return check

    monkeypatch.setattr(work_untied_delivery, "prepare_attention", prepare)
    real_read = notice.read_part

    async def cancel(captured):
        task = asyncio.create_task(real_read(captured))
        tasks.append(task)
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        raise asyncio.CancelledError()

    monkeypatch.setattr(notice, "read_part", cancel)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    await dispatch(e, 901)
    assert tasks and all(task.done() for task in tasks) and finished.is_set()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["finished", "version_bool", "revision_bool"])
async def test_saved_input_damage_never_creates_a_fresh_answer(work_delivery, damage):
    from sqlalchemy.orm.attributes import flag_modified

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.inbox import InboxRow

    e = work_delivery
    _, inputs, _, _, controls = await offer(e)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        if damage == "finished":
            mark.route_complete, mark.state, mark.closed_at = True, "done", e.clock()
        elif damage == "version_bool":
            mark.route_basis["work_answer"]["version"] = True
            flag_modified(mark, "route_basis")
        else:
            mark.route_basis["work_answer"]["seen"]["request_revision"] = True
            flag_modified(mark, "route_basis")
        await writer.flush()
        origin = InboxRow.from_row(mark)
    with pytest.raises(CallbackRefused):
        await inputs.untied.stage(e.link, origin, controls["answer_yes"], guard=e.channel._services.commands._guard(e.channel, e.link))
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_held_untied_input_pins_original_question_until_settlement(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import _sweep_outbox

    e = work_delivery
    await offer(e)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        source_id = mark.route_basis["work_answer"]["source_outbox_id"]
        group_id = mark.route_basis["work_answer"]["group_id"]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id)):
            control.consumed_at, control.closed_reason = e.clock(), "superseded"
    later = e.clock() + timedelta(days=31)
    await _sweep_outbox(e.db.sessions, later, e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, source_id) is not None
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        mark.state, mark.route_complete, mark.closed_at = "done", True, e.clock()
    await _sweep_outbox(e.db.sessions, later, e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, source_id) is None


@pytest.mark.asyncio
async def test_unknown_answer_keeps_question_for_a_late_receipt_after_collection(work_delivery, monkeypatch):
    from datetime import timedelta

    from app.telegram_bots.inbox import InboxRow
    from app.telegram_bots.retention import _sweep_outbox
    from app.telegram_bots.work_actions import request_for
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    _, inputs, _, _, controls = await offer(e)
    respond, calls = HumanInput.respond, []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise TimeoutError()

    monkeypatch.setattr(HumanInput, "respond", unknown)
    await press_work(e, controls["answer_yes"], message_id=901)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        origin = InboxRow.from_row(mark)
        data = mark.route_basis["work_answer"]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == data["group_id"])):
            control.consumed_at, control.closed_reason = e.clock(), "superseded"
    await _sweep_outbox(e.db.sessions, e.clock() + timedelta(days=31), e.channel._services.callbacks.config.callbacks.max_age_days)
    action = await inputs.callbacks.acts._work_record(controls["answer_yes"].id)
    assert action.state == "pending" and action.attempts == 1
    await respond(e.human, actor=e.actor, request_id=action.request_id, request=request_for(action.kind, action.body, identity=action.id))
    assert await inputs.resume(e.channel, origin)
    result = await inputs.callbacks.acts._work_record(action.id)
    assert result.state == "done" and result.attempts == 1 and calls == [action.id]

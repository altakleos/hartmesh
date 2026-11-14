"""Actual voice ingress never records a Work answer before an explicit press."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_voice_notes import Transcribing, arrive, note, script_file, settle, voice_on
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_card_inputs import card
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def voice_answer(e, monkeypatch, *, text="Use the current account.", quoted=801, prompt=False, late_ack=False, combined=False):
    if prompt:
        from test_telegram_work_typed_input import question_prompt

        await question_prompt(e, "answer")
        request = e.request
    else:
        request = await card(e, "information")
    if late_ack:
        from copy import deepcopy
        from datetime import timedelta

        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, e.identity)
            parts = deepcopy(parent.parts)
            for part in parts:
                part["dispatched_at"] = e.clock().isoformat()
                part["sent_at"] = (e.clock() + timedelta(seconds=20)).isoformat()
            parent.parts = parts
        e.clock.advance(10)
    install_inputs(e)
    e.services, e.request = e.channel._services, e.channel.bot.request
    e.services.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    await voice_on(e, e.db, monkeypatch, agreed=True, transcriber=Transcribing(text=text))
    script_file(e, 900)
    await arrive(e, 900, quoted=quoted)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_answer", (mark.route_kind, mark.route_basis)
        assert not mark.route_complete and not mark.operation_ids
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_answer"]["notice_id"])
        expected = ([["voice_save_resume"]] if combined else []) + [["voice_save_answer"], ["voice_discard"]]
        assert [[c["kind"] for c in line] for line in notice.payload["product_keyboard_basis"]] == expected
        e.identity = notice.id
    assert e.services.turns.accept_transcript.await_count == 0
    assert len(e.transcriber.calls) == 1
    return request, notice


@pytest.mark.asyncio
async def test_voice_answer_waits_for_actual_save_and_replays_without_resubmission(work_delivery, monkeypatch):
    e = work_delivery
    request, _ = await voice_answer(e, monkeypatch)
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        assert notice.state == "sent", e.dispatch_errors
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"])))
    save = next(row for row in controls if row.kind == "voice_save_answer")
    await press_work(e, save, message_id=901)
    await press_work(e, save, message_id=901, update_id=2902)
    replies = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(replies) == 1 and replies[0]["text"] == "Use the current account."
    assert len(e.transcriber.calls) == 1
    async with e.db.sessions() as reader:
        assert "transcript" not in (await reader.get(TelegramCallbackRow, save.id)).payload


@pytest.mark.asyncio
async def test_voice_discard_records_no_answer_and_erases_saved_button_transcript(work_delivery, monkeypatch):
    e = work_delivery
    request, _ = await voice_answer(e, monkeypatch)
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"])))
    discard = next(row for row in controls if row.kind == "voice_discard")
    await press_work(e, discard, message_id=901)
    save = next(row for row in controls if row.kind == "voice_save_answer")
    await press_work(e, save, message_id=901, update_id=2902)
    assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    async with e.db.sessions() as reader:
        current = await reader.get(TelegramCallbackRow, save.id)
        assert "transcript" not in current.payload
        audits = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "voice_note")))
        assert len(audits) == 1 and audits[0].body == {"duration": 5, "provider": "openai", "used_as": "discarded"}
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id)))
        assert any(part.get("kind") == "edit" and part.get("source_ref", {}).get("key") == "voice.discarded" for row in rows for part in row.parts)


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [None, 802])
async def test_voice_answer_keeps_the_shown_answer_prompt(work_delivery, monkeypatch, quoted):
    e = work_delivery
    request, _ = await voice_answer(e, monkeypatch, quoted=quoted, prompt=True)
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        assert notice.state == "sent", e.dispatch_errors
        save = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "voice_save_answer"))
    await press_work(e, save, message_id=901)
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1


@pytest.mark.asyncio
async def test_unquoted_voice_after_question_still_requires_save(work_delivery, monkeypatch):
    await voice_answer(work_delivery, monkeypatch, quoted=None)


@pytest.mark.asyncio
async def test_full_voice_transcript_precedes_controls(work_delivery, monkeypatch):
    e = work_delivery
    transcript = "💡 " * 1600
    request, notice = await voice_answer(e, monkeypatch, text=transcript)
    assert len(notice.parts) > 1
    assert all(not part.get("stored_keyboard") for part in notice.parts[:-1])
    assert notice.parts[-1]["stored_keyboard"]
    assert transcript.strip() in "".join(part["visible_text"] for part in notice.parts)
    for index in range(len(notice.parts)):
        await dispatch(e, 901 + index)
    assert not e.dispatch_errors
    calls = [call for call in e.request.calls if call.endpoint == "sendMessage" and "💡" in call.parameters.get("text", "")]
    assert len(calls) == len(notice.parts)
    assert all(not call.parameters.get("reply_markup") for call in calls[:-1])
    assert calls[-1].parameters["reply_markup"]
    async with e.db.sessions() as reader:
        save = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "voice_save_answer"))
    await press_work(e, save, message_id=900 + len(notice.parts))
    replies = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(replies) == 1 and replies[0]["text"] == transcript.strip()


async def prepare_voice(e, monkeypatch, *, held=False):
    install_inputs(e)
    e.services, e.request = e.channel._services, e.channel.bot.request
    e.services.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    await voice_on(e, e.db, monkeypatch, agreed=True, transcriber=Transcribing(text="Use the current account."))
    if held:
        e.transcriber.held = asyncio.Event()
    script_file(e, 900)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinary", ["forwarded", "clarification"])
async def test_ordinary_voice_preserves_existing_owner(work_delivery, monkeypatch, ordinary):
    e = work_delivery
    await card(e, "information")
    await prepare_voice(e, monkeypatch)
    if ordinary == "clarification":
        e.services.human_inputs = SimpleNamespace(answers_prompt=AsyncMock(return_value=True))
    await arrive(e, 900, forwarded=ordinary == "forwarded")
    assert e.services.turns.accept_transcript.await_count == 1
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "turn" and "work_voice" not in mark.route_basis


@pytest.mark.asyncio
async def test_prompt_closed_while_transcribing_keeps_voice_out_of_chat(work_delivery, monkeypatch):
    from test_telegram_chat_turns import until
    from test_telegram_work_typed_input import question_prompt

    e = work_delivery
    await question_prompt(e, "answer")
    await prepare_voice(e, monkeypatch, held=True)
    assert await e.channel.handle_update(note(e, 900))
    try:
        await until(lambda: bool(e.transcriber.calls))
        async with e.db.sessions.begin() as writer:
            for prompt in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt")):
                prompt.consumed_at, prompt.closed_reason = e.clock(), "navigation"
    finally:
        e.transcriber.held.set()
        await settle(e)
    assert e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and "work_closed" in mark.route_basis
        assert mark.route_basis["voice_note"]["original"]["type"] == "voice"
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
    assert await e.channel.handle_update(note(e, 900))
    await settle(e)
    assert len(e.transcriber.calls) == 1


@pytest.mark.asyncio
async def test_voice_notice_rejects_changed_transcript_part_before_sdk(work_delivery, monkeypatch):
    from copy import deepcopy

    e = work_delivery
    _, notice = await voice_answer(e, monkeypatch)
    before = len([call for call in e.request.calls if call.endpoint == "sendMessage"])
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramOutboxRow, notice.id)
        parts = deepcopy(saved.parts)
        parts[0]["html"] = "A different answer."
        saved.parts = parts
    await dispatch(e, 901)
    assert len([call for call in e.request.calls if call.endpoint == "sendMessage"]) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["decision", "changes"])
async def test_voice_draft_preserves_specific_confirmation_intent(work_delivery, monkeypatch, purpose):
    from test_telegram_work_input_capture import prompt
    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_typing_prompts import clear_old

    e = work_delivery
    if purpose == "changes":
        request, _, _ = await prompt(e)
    else:
        request = await card(e, "decision")
        e.identity = await open_view(e, await selected(e, "decide_own"))
        await clear_old(e)
        await dispatch(e, 802)
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    await prepare_voice(e, monkeypatch)
    await arrive(e, 900, quoted=802)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_input"
        assert "voice_note" in mark.route_basis
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind != "voice_note")))
        preview = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == mark.operation_ids[0]))
        assert preview is not None
        e.identity = preview.id
        assert preview.payload["work_request_basis"]["interaction"]["note" if purpose == "changes" else "decision"] == "Use the current account."
        if purpose == "changes":
            from app.telegram_bots.render import render_plain

            assert render_plain("stale.changed_resend", "en-US") not in "".join(part["visible_text"] for part in preview.parts)
    await dispatch(e, 901)
    control = await selected(e, "changes_confirm" if purpose == "changes" else "decide_confirm")
    await press_work(e, control, message_id=901)
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind != "voice_note")))
        assert len(actions) == 1 and actions[0].state == "done", [(act.kind, act.state, act.reason) for act in actions]
        assert actions[0].kind == ("changes_requested" if purpose == "changes" else "decide")


@pytest.mark.asyncio
async def test_transcription_cannot_adopt_a_changed_captured_context(work_delivery, monkeypatch):
    from test_telegram_chat_turns import until

    e = work_delivery
    await card(e, "information")
    await prepare_voice(e, monkeypatch, held=True)
    assert await e.channel.handle_update(note(e, 900, quoted=801))
    try:
        await until(lambda: bool(e.transcriber.calls))
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            data = mark.route_basis["work_voice"]
            mark.route_basis = {**mark.route_basis, "work_voice": {**data, "purpose": "cannot_provide"}}
    finally:
        e.transcriber.held.set()
        await settle(e)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert "work_answer" not in mark.route_basis
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
    assert len(e.transcriber.calls) == 1 and e.services.turns.accept_transcript.await_count == 0


@pytest.mark.asyncio
async def test_quoted_voice_accepted_before_question_ack_can_be_saved(work_delivery, monkeypatch):
    e = work_delivery
    request, _ = await voice_answer(e, monkeypatch, late_ack=True)
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        save = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "voice_save_answer"))
    await press_work(e, save, message_id=901)
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1


@pytest.mark.asyncio
async def test_overlong_voice_offers_reentry_without_truncating_or_saving(work_delivery, monkeypatch):
    e = work_delivery
    request = await card(e, "information")
    await prepare_voice(e, monkeypatch)
    text = "A complete long transcript. " * 250
    e.transcriber.text = text
    await arrive(e, 900, quoted=801)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and mark.sanitized_input["text"] == text.strip()
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        assert notice.parts[-1]["source_ref"]["key"] == "voice.answer_too_long"
        transcript = "".join(part["source_ref"]["values"].get("text", "") for part in notice.parts)
        assert transcript == text.strip()
        assert {control["kind"] for line in notice.payload["product_keyboard_basis"] for control in line} == {"prompt_reopen", "prompt_send_new", "prompt_discard"}
    assert not (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_reopen", "prompt_send_new", "prompt_discard"])
async def test_closed_voice_choices_require_every_transcript_ack(work_delivery, monkeypatch, choice):
    from copy import deepcopy

    e = work_delivery
    await test_overlong_voice_offers_reentry_without_truncating_or_saving(e, monkeypatch)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
    e.identity = notice.id
    for index in range(len(notice.parts)):
        await dispatch(e, 901 + index)
    async with e.db.sessions.begin() as writer:
        notice = await writer.get(TelegramOutboxRow, notice.id)
        control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == choice))
        parts = deepcopy(notice.parts)
        parts[0].update(state="unavailable", message_ids=[])
        notice.parts = parts
    await press_work(e, control, message_id=900 + len(notice.parts))
    async with e.db.sessions() as reader:
        current = await reader.get(TelegramCallbackRow, control.id)
        assert current.consumed_at is None and "publication_owner" not in current.payload
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert not mark.route_complete and mark.route_kind == "work_card_hold"


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_reopen", "prompt_send_new", "prompt_discard"])
async def test_complete_closed_voice_choices_keep_their_actual_intent(work_delivery, monkeypatch, choice):
    from datetime import UTC, datetime

    e = work_delivery
    await test_overlong_voice_offers_reentry_without_truncating_or_saving(e, monkeypatch)
    e.services.turns.accept_staged = AsyncMock()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
    for index in range(len(notice.parts)):
        await dispatch(e, 901 + index)
    async with e.db.sessions() as reader:
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == choice))
    await press_work(e, control, message_id=900 + len(notice.parts))
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete
        audits = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "voice_note")))
        assert len(audits) == 1
        assert audits[0].body["used_as"] == {"prompt_reopen": "answer", "prompt_send_new": "message", "prompt_discard": "discarded"}[choice]
    assert await e.services.acts.voice_seconds_since(e.link.owner_user_id, datetime(2000, 1, 1, tzinfo=UTC)) == 5
    assert e.services.turns.accept_staged.await_count == (1 if choice == "prompt_send_new" else 0)


@pytest.mark.asyncio
async def test_waiting_voice_keeps_retired_source_controls_through_cleanup(work_delivery, monkeypatch):
    from datetime import timedelta

    e = work_delivery
    _, _ = await voice_answer(e, monkeypatch)
    await dispatch(e, 901)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        group_id = mark.route_basis["work_answer"]["group_id"]
        originals = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id)))
        for control in originals:
            control.consumed_at, control.closed_reason = e.clock(), "navigation"
        notice = await writer.get(TelegramOutboxRow, e.identity)
        save = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "voice_save_answer"))
    await e.services.callbacks.sweep(e.clock() + timedelta(days=8))
    async with e.db.sessions() as reader:
        assert all([await reader.get(TelegramCallbackRow, control.id) is not None for control in originals])
    await press_work(e, save, message_id=901)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and "work_closed" in mark.route_basis


@pytest.mark.asyncio
@pytest.mark.parametrize("retirement", ["deleted", "incomplete"])
async def test_recognized_voice_source_retirement_keeps_input_owned(work_delivery, monkeypatch, retirement):
    e = work_delivery
    await card(e, "information")
    await prepare_voice(e, monkeypatch)
    cards = e.services.work_text_inputs.cards
    find = cards.find

    async def retire(*args, **kwargs):
        identity = await find(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, identity)
            if retirement == "deleted":
                await writer.delete(parent)
            else:
                parent.state = "pending"
        return identity

    monkeypatch.setattr(cards, "find", retire)
    await arrive(e, 900, quoted=801)
    assert e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and "work_closed" in mark.route_basis
        assert mark.sanitized_input["text"] == "Use the current account."


@pytest.mark.asyncio
async def test_voice_unknown_result_reconciles_one_receipt_and_completes_delivery(work_delivery, monkeypatch):
    from test_telegram_work_input_refresh import deliver_refreshed

    from app.telegram_bots.inbox import InboxRow
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    request, _ = await voice_answer(e, monkeypatch)
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        save = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "voice_save_answer"))
    respond, calls = HumanInput.respond, []

    async def lose_response(self, *args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        await respond(self, *args, **kwargs)
        raise TimeoutError("Receipt delivery interrupted")

    monkeypatch.setattr(HumanInput, "respond", lose_response)
    await press_work(e, save, message_id=901)
    async with e.db.sessions() as reader:
        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
    await e.services.work_text_inputs.resume(e.channel, origin)
    await press_work(e, save, message_id=901, update_id=2902)
    assert calls == [save.id]
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1
    async with e.db.sessions() as reader:
        action = await reader.get(TelegramActRow, save.id)
        assert action.state == "done"
        outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == save.id))
        e.identity = outcome.id
    await deliver_refreshed(e, 902)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete


@pytest.mark.asyncio
@pytest.mark.parametrize("closed", [False, True])
async def test_voice_topic_failure_retries_only_unacknowledged_original_parts(work_delivery, monkeypatch, closed):
    e = work_delivery
    if closed:
        await test_overlong_voice_offers_reentry_without_truncating_or_saving(e, monkeypatch)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            notice_id = mark.route_basis["work_closed"]["notice_id"]
    else:
        _, notice = await voice_answer(e, monkeypatch, text="💡 " * 1600)
        notice_id = notice.id
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        first = (await reader.get(TelegramOutboxRow, notice_id)).parts[0]
    e.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice_id)
        assert notice.state == "failed" and notice.payload["presentation_version"] == 0
        assert notice.parts[0] == first
    # The normal delivery-only retry owns recovery, with no new transcription,
    # business operation, target change, or replay of the acknowledged prefix.
    await dispatch(e, 950)
    async with e.db.sessions() as reader:
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "delivery_retry"))
    await press_work(e, retry, message_id=950)
    for index in range(1, len(notice.parts)):
        await dispatch(e, 950 + index)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice_id)
        assert notice.state == "sent", (notice.state, notice.failed_reason, e.dispatch_errors)
        assert notice.parts[0] == first
        kind = "prompt_discard" if closed else "voice_discard"
        discard = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == kind))
    await press_work(e, discard, message_id=950 + len(notice.parts) - 1, update_id=2902)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete
    assert len(e.transcriber.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["cannot_provide", "wrong_recipient"])
async def test_voice_keeps_original_response_disposition(work_delivery, monkeypatch, purpose):
    from test_telegram_work_typed_input import question_prompt

    e = work_delivery
    request_id = e.request["id"]
    message_id = await question_prompt(e, purpose)
    await prepare_voice(e, monkeypatch)
    await arrive(e, 900, quoted=message_id)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_answer"]["notice_id"])
        e.identity = notice.id
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        save = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "voice_save_answer"))
    await press_work(e, save, message_id=901)
    replies = (await e.human.responses(actor=e.actor, request_id=request_id))["responses"]
    assert len(replies) == 1 and replies[0]["disposition"] == purpose


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_discard"])
async def test_closed_voice_unavailable_target_preserves_recovery_choices(work_delivery, monkeypatch, choice):
    from uuid import uuid4

    from deerflow.agent_instances.work_contract import WorkCommand

    e = work_delivery
    await test_overlong_voice_offers_reentry_without_truncating_or_saving(e, monkeypatch)
    e.services.turns.accept_staged = AsyncMock()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        parent = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        work_id = mark.route_basis["work_closed"]["work_id"]
        transcript = mark.sanitized_input["text"]
    e.identity = parent.id
    for index in range(len(parent.parts)):
        await dispatch(e, 901 + index)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, parent.id)
        reopen = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
    record = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=work_id)
    await e.human.work.command(
        actor=e.actor,
        instance_id=e.link.agent_ref,
        work_id=work_id,
        request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=record["revision"], expected_assignment_revision=record["assignment_revision"]),
        can_stop=False,
    )
    await press_work(e, reopen, message_id=900 + len(parent.parts))
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        replacement = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        assert replacement.id != parent.id
        assert replacement.parts[-1]["source_ref"]["key"] == "prompt.unavailable"
        assert "".join(part["source_ref"]["values"].get("text", "") for part in replacement.parts) == transcript
    e.identity = replacement.id
    for index in range(len(replacement.parts)):
        await dispatch(e, 920 + index)
    async with e.db.sessions() as reader:
        replacement = await reader.get(TelegramOutboxRow, replacement.id)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == replacement.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == choice))
    await press_work(e, selected, message_id=919 + len(replacement.parts), update_id=2902)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete
        audits = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "voice_note")))
        assert len(audits) == 1 and audits[0].body["used_as"] == ("message" if choice == "prompt_send_new" else "discarded")
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
    assert e.services.turns.accept_staged.await_count == (1 if choice == "prompt_send_new" else 0)
    assert len(e.transcriber.calls) == 1

"""Actual voice prompts draft privately and never become an ordinary turn."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_prompt_delivery import prepare_words
from test_telegram_voice_notes import Transcribing, arrive, script_file, voice_on
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_inputs import AssignmentInputs
from app.telegram_bots.assignment_voice_inputs import AssignmentVoiceInputs
from app.telegram_bots.callbacks import CallbackRefused, Prompts
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.voice_inputs import VoiceInputRouter
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def assignment_voice(e, monkeypatch):
    from app.telegram_bots.assignment_delivery import AssignmentPlans
    from app.telegram_bots.assignment_inputs import AssignmentInputs
    from app.telegram_bots.assignment_sources import AssignmentReader
    from app.telegram_bots.assignment_voice_inputs import AssignmentVoiceInputs
    from app.telegram_bots.voice_inputs import VoiceInputRouter

    prompts, _ = await prepare_words(e)
    await dispatch(e, 1201)
    install_inputs(e)
    services = e.channel._services
    services.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    cards = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=prompts.callbacks)
    inputs = AssignmentInputs(prompts, cards)
    services.voice_inputs = VoiceInputRouter(services.work_text_inputs.voice, {"assignment": AssignmentVoiceInputs(inputs)})
    e.services, e.request = services, e.channel.bot.request
    e.voice_drafts = []

    async def draft(words, **kwargs):
        e.voice_drafts.append(words)
        return DraftAssignment(objective=words, success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", draft)
    await voice_on(e, e.db, monkeypatch, agreed=True, transcriber=Transcribing(text="Review the handbook"))
    return inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [1201, None])
async def test_actual_voice_assignment_keeps_original_prompt_and_one_private_draft(work_delivery, monkeypatch, quoted):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=quoted)
    assert e.voice_drafts == ["Review the handbook"]
    assert e.services.turns.accept_transcript.await_count == 0
    assert len(e.transcriber.calls) == 1
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "assignment_draft" and "voice_note" in mark.route_basis
        assert mark.sanitized_input["text"] == "Review the handbook"
        draft = await reader.get(TelegramActRow, mark.route_target_id)
        assert draft.state == "draft" and not draft.operation_ids
        card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == draft.id))
        assert card is not None and card.state == "pending"
        assert card.parts[0]["visible_text"].startswith("Heard:")
        assert not card.parts[0].get("stored_keyboard") and card.parts[-1]["stored_keyboard"]
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    await arrive(e, 1202, quoted=quoted)
    assert e.voice_drafts == ["Review the handbook"] and len(e.transcriber.calls) == 1
    await inputs.requests.stop()


@pytest.mark.asyncio
async def test_spoken_manual_criteria_keep_both_transcripts_without_a_second_draft_call(work_delivery, monkeypatch):
    from test_telegram_bot_adapter import message

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)

    async def unavailable(words, **kwargs):
        e.voice_drafts.append(words)
        return None

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", unavailable)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    async with e.db.sessions() as reader:
        prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["purpose"].as_string() == "assign_done"))
        assert prompt is not None and prompt.parts[0]["visible_text"].startswith("Heard:")
    for identity in (1203, 1204):
        e.clock.advance(10)
        message(e.channel.bot.request, identity=identity)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    e.transcriber.text = "List every verified correction"
    script_file(e, 1205)
    await arrive(e, 1205, quoted=1204)
    async with e.db.sessions() as reader:
        criterion = await reader.get(TelegramInboundMarkRow, (e.link.id, 1205, e.link.chat_id))
        assert criterion.route_kind == "assignment_criteria"
        root = await reader.get(TelegramActRow, criterion.route_target_id)
        assert root.body["draft"]["objective"] == "Review the handbook"
        assert root.body["draft"]["success_criteria"] == "List every verified correction"
        card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == root.id))
        assert len(card.parts) == 3 and all(part["visible_text"].startswith("Heard:") for part in card.parts[:2])
        assert not root.operation_ids and root.state == "draft"
    assert e.voice_drafts == ["Review the handbook"] and len(e.transcriber.calls) == 2
    assert e.services.turns.accept_transcript.await_count == 0
    await inputs.requests.stop()


@pytest.mark.asyncio
async def test_spoken_draft_controls_wait_for_heard_and_full_card_sdk_ack(work_delivery, monkeypatch):
    from test_telegram_bot_adapter import message
    from test_telegram_work_interaction_delivery import guard_for, selected

    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import PermanentDeliveryError

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == mark.route_target_id))
        e.identity = card.id
    save = await selected(e, "assign_save")
    admissions = AssignmentAdmissions(inputs.cards, inputs.acts)

    async def stage(button):
        return await admissions.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))

    with pytest.raises((CallbackRefused, PermanentDeliveryError)):
        await stage(save)
    e.clock.advance(10)
    message(e.channel.bot.request, identity=1203)
    message(e.channel.bot.request, identity=1204)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        partial = await reader.get(TelegramOutboxRow, e.identity)
        assert partial.state == "pending" and [part["message_ids"] for part in partial.parts] == [[1203], []]
    with pytest.raises((CallbackRefused, PermanentDeliveryError)):
        await stage(await selected(e, "assign_save"))
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        card = await reader.get(TelegramOutboxRow, e.identity)
        assert card.state == "sent" and [part["message_ids"] for part in card.parts] == [[1203], [1204]]
    save = await selected(e, "assign_save")
    assert save.message_id == 1204 and save.payload["presentation_pending"] is False
    saved = await inputs.acts.check((await stage(save)).id)
    assert saved.state == "done" and saved.kind == "delegate"
    assert e.voice_drafts == ["Review the handbook"] and len(e.transcriber.calls) == 1
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    await inputs.requests.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["closed", "ack", "topic"])
async def test_assignment_voice_keeps_captured_prompt_after_transcriber_wait(work_delivery, monkeypatch, damage):
    import asyncio

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.transcriber.transcribe

    async def transcribe(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(e.transcriber, "transcribe", transcribe)
    script_file(e, 1202)
    task = asyncio.create_task(arrive(e, 1202, quoted=1201))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with e.db.sessions.begin() as writer:
            prompt = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 1201))
            assert prompt is not None
            if damage == "closed":
                prompt.consumed_at, prompt.closed_reason = e.sender.outbox.clock(), "cancelled"
            elif damage == "topic":
                prompt.message_thread_id = 9901
            else:
                row = await writer.get(TelegramOutboxRow, prompt.payload["anchor_outbox_id"])
                row.parts = [{**part, "message_ids": []} for part in row.parts]
    finally:
        release.set()
        await task
        await inputs.requests.stop()
    assert e.voice_drafts == [] and e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and mark.route_complete
        assert mark.route_basis["work_voice"]["phase"] == "closed"


@pytest.mark.asyncio
async def test_expired_assignment_prompt_voice_stays_owned_without_an_ordinary_turn(work_delivery, monkeypatch):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    e.clock.advance(601)
    script_file(e, 1202)
    await arrive(e, 1202)
    assert e.voice_drafts == [] and e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and mark.route_complete
    await inputs.requests.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_ack", [False, True])
async def test_voice_reply_to_assignment_with_missing_prompt_never_falls_through_to_chat(work_delivery, monkeypatch, missing_ack):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        prompt = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 1201))
        if missing_ack:
            row = await writer.get(TelegramOutboxRow, prompt.payload["anchor_outbox_id"])
            assert row.message_id == 1201
            row.parts = [{**part, "message_ids": []} for part in row.parts]
        await writer.delete(prompt)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    assert e.voice_drafts == [] and e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and mark.route_complete
    await inputs.requests.stop()


@pytest.mark.asyncio
async def test_forwarded_voice_does_not_claim_the_assignment_prompt(work_delivery, monkeypatch):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201, forwarded=True)
    assert not e.voice_drafts and e.services.turns.accept_transcript.await_count == 1
    await inputs.requests.stop()


@pytest.mark.asyncio
async def test_captured_assignment_voice_stays_held_without_its_new_router(work_delivery, monkeypatch):
    import asyncio

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.transcriber.transcribe

    async def transcribe(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(e.transcriber, "transcribe", transcribe)
    script_file(e, 1202)
    task = asyncio.create_task(arrive(e, 1202, quoted=1201))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        e.services.voice_inputs = None
    finally:
        release.set()
        await task
        await inputs.requests.stop()
    assert not e.voice_drafts and e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind.startswith("work_") and mark.route_basis["work_voice"]["held"] is True


@pytest.mark.asyncio
async def test_oversized_spoken_assignment_gets_no_private_draft_or_chat_turn(work_delivery, monkeypatch):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    e.transcriber.text = "a" * (e.services.config.assign.max_chars + 1)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    assert not e.voice_drafts and e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and mark.route_complete
        assert any(part.get("source_ref", {}).get("key") == "assign.too_long" for row in await reader.scalars(select(TelegramOutboxRow)) for part in row.parts)
    await inputs.requests.stop()


@pytest.mark.asyncio
async def test_voice_quote_to_a_retired_prompt_part_closes_before_draft_routing(work_delivery, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        prompt = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 1201))
        parent = await writer.get(TelegramOutboxRow, prompt.payload["anchor_outbox_id"])
        # A historical ACK locator retains custody but cannot authorize input
        # against the fully acknowledged current presentation.
        parent.parts = [{**parent.parts[0], "part_id": "retired-prompt-part", "presentation_version": -1, "state": "retired", "message_ids": [1199]}, *parent.parts]
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1199)
    assert not e.voice_drafts and e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and mark.route_complete
        assert mark.route_basis["work_voice"]["phase"] == "closed"
    await inputs.requests.stop()


@pytest.mark.asyncio
async def test_ready_assignment_voice_replays_from_sql_without_transcribing_again(work_delivery, monkeypatch):
    from app.telegram_bots.assignment_inputs import AssignmentInputs
    from app.telegram_bots.assignment_voice_inputs import AssignmentVoiceInputs
    from app.telegram_bots.inbox import InboxRow
    from app.telegram_bots.voice_inputs import VoiceInputRouter

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    old_router = e.services.voice_inputs
    monkeypatch.setattr(old_router, "resume", AsyncMock(return_value=True))
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.route_complete
        assert mark.route_basis["work_voice"]["phase"] == "ready"
        durable = InboxRow.from_row(mark)
    await inputs.requests.stop()
    recovered = AssignmentInputs(inputs.prompts, inputs.cards)
    router = VoiceInputRouter(old_router.fallback, {"assignment": AssignmentVoiceInputs(recovered)})
    assert await router.resume(e.channel, durable)
    assert await router.resume(e.channel, durable)
    assert e.voice_drafts == ["Review the handbook"] and len(e.transcriber.calls) == 1
    assert e.services.turns.accept_transcript.await_count == 0
    await recovered.requests.stop()


@pytest.mark.asyncio
async def test_spoken_edit_replaces_only_the_original_private_draft(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for, selected

    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        original_id = mark.route_target_id
        e.identity = (await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == original_id))).id
    await dispatch(e, 1203)
    await dispatch(e, 1204)
    button = await selected(e, "assign_edit")
    e.identity = await inputs.prompts.edit(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    await dispatch(e, 1205)
    e.transcriber.text = "Include the expanded handbook"
    script_file(e, 1206)
    await arrive(e, 1206, quoted=1205)
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramActRow, original_id)
        assert original.state == "refused" and original.reason == "replaced" and not original.operation_ids
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1206, e.link.chat_id))
        replacement = await reader.get(TelegramActRow, mark.route_target_id)
        assert replacement.state == "draft" and not replacement.operation_ids
        assert replacement.body["draft"]["objective"] == "Include the expanded handbook"
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
        assert all(not row.operation_ids for row in await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "delegate")))
    assert e.voice_drafts == ["Review the handbook", "Include the expanded handbook"] and len(e.transcriber.calls) == 2
    assert e.services.turns.accept_transcript.await_count == 0
    await inputs.requests.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "closed", "expired"])
async def test_ready_voice_retires_with_one_closed_notice_without_drafting(work_delivery, monkeypatch, damage):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    old_router = e.services.voice_inputs
    monkeypatch.setattr(old_router, "resume", AsyncMock(return_value=True))
    recovered = None
    try:
        script_file(e, 1202)
        await arrive(e, 1202, quoted=1201)
        key = (e.link.id, 1202, e.link.chat_id)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, key)
            assert mark.route_kind == "work_card_hold" and not mark.route_complete
            assert mark.route_basis["work_voice"]["phase"] == "ready"
            durable = InboxRow.from_row(mark)
            prompt_id = mark.route_basis["work_voice"]["prompt_id"]
        async with e.db.sessions.begin() as writer:
            prompt = await writer.get(TelegramCallbackRow, prompt_id)
            if damage == "missing":
                await writer.delete(prompt)
            elif damage == "closed":
                prompt.consumed_at, prompt.closed_reason = e.clock.now(), "cancelled"
            else:
                e.clock.advance(601)
        await inputs.requests.stop()
        recovered = AssignmentInputs(inputs.prompts, inputs.cards)
        router = VoiceInputRouter(old_router.fallback, {"assignment": AssignmentVoiceInputs(recovered)})
        try:
            await router.resume(e.channel, durable)
        except (CallbackRefused, ProductSourceRetired):
            pass
        assert not e.voice_drafts and len(e.transcriber.calls) == 1
        assert e.services.turns.accept_transcript.await_count == 0
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, key)
            assert mark.route_complete and mark.route_basis["work_voice"]["phase"] == "closed"
            rows = list(await reader.scalars(select(TelegramOutboxRow)))
            notices = [row for row in rows if any(part.get("source_ref", {}).get("key") == "prompt.closed" for part in row.parts)]
            assert len(notices) == 1
        assert await router.resume(e.channel, durable)
        assert not e.voice_drafts and len(e.transcriber.calls) == 1
    finally:
        await inputs.requests.stop()
        if recovered is not None:
            await recovered.requests.stop()


@pytest.mark.asyncio
async def test_transient_prompt_selection_preserves_ready_voice(work_delivery, monkeypatch):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    old = e.services.voice_inputs
    monkeypatch.setattr(old, "resume", AsyncMock(return_value=True))
    script_file(e, 1202)
    await arrive(e, 1202, quoted=1201)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.route_complete
        durable = InboxRow.from_row(mark)
    await inputs.requests.stop()
    recovered = AssignmentInputs(inputs.prompts, inputs.cards)
    router = VoiceInputRouter(old.fallback, {"assignment": AssignmentVoiceInputs(recovered)})
    try:
        with monkeypatch.context() as fault:
            fault.setattr(Prompts, "current", AsyncMock(side_effect=CallbackRefused))
            with pytest.raises(CallbackRefused):
                await router.resume(e.channel, durable)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
            assert not mark.route_complete and mark.route_basis["work_voice"]["phase"] == "ready"
        assert not e.voice_drafts and len(e.transcriber.calls) == 1
        assert e.services.turns.accept_transcript.await_count == 0
        assert await router.resume(e.channel, durable)
        assert e.voice_drafts == ["Review the handbook"] and len(e.transcriber.calls) == 1
    finally:
        await recovered.requests.stop()


@pytest.mark.asyncio
async def test_transient_completion_selection_does_not_claim_prompt_closed(work_delivery, monkeypatch):
    e = work_delivery
    inputs = await assignment_voice(e, monkeypatch)
    original = Prompts.current
    entered = []

    async def transient(prompts, *args, **kwargs):
        if not entered:
            writer = kwargs["session"]
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
            assert mark.route_kind in {"turn", "work_card_hold"} and "voice_note" in mark.route_basis
            entered.append(True)
            raise CallbackRefused()
        return await original(prompts, *args, **kwargs)

    monkeypatch.setattr(Prompts, "current", transient)
    try:
        script_file(e, 1202)
        await arrive(e, 1202, quoted=1201)
        assert entered and len(e.transcriber.calls) == 1
        assert not e.voice_drafts and e.services.turns.accept_transcript.await_count == 0
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1202, e.link.chat_id))
            assert mark.route_kind == "work_card_hold" and not mark.route_complete
            assert mark.route_basis["work_voice"]["phase"] == "ready"
            durable = InboxRow.from_row(mark)
            closed = [row for row in await reader.scalars(select(TelegramOutboxRow)) if any(part.get("source_ref", {}).get("key") == "prompt.closed" for part in row.parts)]
            assert not closed
        assert await e.services.voice_inputs.resume(e.channel, durable)
        assert await e.services.voice_inputs.resume(e.channel, durable)
        assert e.voice_drafts == ["Review the handbook"] and len(e.transcriber.calls) == 1
        assert e.services.turns.accept_transcript.await_count == 0
    finally:
        await inputs.requests.stop()

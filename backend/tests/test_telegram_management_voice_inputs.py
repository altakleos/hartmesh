"""Spoken saved-work corrections retain their actual private prompt."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callback_commands import press
from test_telegram_voice_notes import Transcribing, arrive, script_file, voice_on
from test_telegram_work_chat_management_runtime import open_detail
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import DeliveryRetry, PermanentDeliveryError, ProductSourceRetired
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def enable_voice(e, owner, monkeypatch, text="Include the additional chapter"):
    # The consent helper supplies a minimal audit owner for isolated voice tests.
    # Preserve the default runtime's real Work journal for the subsequent Save.
    acts = owner.host.acts
    e.services, e.request = owner.host, e.channel.bot.request
    await voice_on(e, e.db, monkeypatch, agreed=True, transcriber=Transcribing(text=text))
    owner.host.acts = acts


async def shown_editor(e):
    await open_detail(e)
    change = await selected(e, "work_change")
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 6175, change.id, message_id=6174))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == "edit"))
        parent_id, e.identity = parent.id, parent.id
    await dispatch(e, 6176)
    edit = await selected(e, "assign_edit")
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 6177, edit.id, message_id=6176))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["management_prompt_intent"]["purpose"].as_string() == "work_edit"))
        cleanup_id = prompt.payload["management_prompt_cleanup_id"]
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    for _ in range(8):
        async with e.db.sessions() as reader:
            cleanup = await reader.get(TelegramOutboxRow, cleanup_id)
            if cleanup.state == "sent":
                assert cleanup.parts[0]["message_ids"] == [6176]
                break
        assert await e.sender.tick() in {0, 1}
        await e.sender.drain()
        e.clock.advance(1)
    else:
        pytest.fail("The original editor keyboard was not cleared")
    assert all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
    e.dispatch_errors.clear()
    await dispatch(e, 6180)
    return parent_id


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [None, 6180])
async def test_default_spoken_correction_stays_private_and_needs_fresh_save(work_delivery, monkeypatch, quoted):
    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    calls = []

    async def proposal(words, **kwargs):
        calls.append((words, kwargs["previous"]))
        return DraftAssignment(objective="Review the expanded handbook", success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    try:
        parent_id = await shown_editor(e)
        async with e.db.sessions() as reader:
            before = (await reader.get(AgentWorkRow, e.work["id"])).revision
        await enable_voice(e, owner, monkeypatch)
        script_file(e, 6181)
        await arrive(e, 6181, quoted=quoted)
        assert len(calls) == 1 and calls[0][0] == "Include the additional chapter"
        assert owner.host.turns.accept_transcript.await_count == 0
        assert len(e.transcriber.calls) == 1
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
            assert mark.route_kind == "management_edit"
            assert mark.route_basis["work_voice"]["owner"] == "management"
            assert mark.route_basis["voice_note"]["original"]["type"] == "voice"
            root = await reader.get(TelegramActRow, mark.route_target_id)
            assert root.state == "draft" and not root.operation_ids
            assert (await reader.get(AgentWorkRow, e.work["id"])).revision == before
            card = await reader.get(TelegramOutboxRow, parent_id)
            current = [part for part in card.parts if part["presentation_version"] == card.payload["presentation_version"]]
            assert card.state == "pending" and current[0]["visible_text"].startswith("Heard:")
            assert "Review the expanded handbook" in "".join(part["visible_text"] for part in current)
        await arrive(e, 6181, quoted=quoted)
        assert len(calls) == 1 and len(e.transcriber.calls) == 1
        e.identity = parent_id
        save = await selected(e, "work_change_save")
        from test_telegram_work_interaction_delivery import guard_for

        with pytest.raises((CallbackRefused, PermanentDeliveryError)):
            await owner.chat.management.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
        e.clock.advance(10)
        e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 6176, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Heard and revised card"}})
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors, e.dispatch_errors
        sent = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
        assert sent[-1]["text"].startswith("Heard:") and "Review the expanded handbook" in sent[-1]["text"]
        save = await selected(e, "work_change_save")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6182, save.id, message_id=6176))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert (await reader.get(AgentWorkRow, e.work["id"])).objective == "Review the expanded handbook"
            assert (await reader.get(TelegramActRow, save.id)).state == "done"
        assert len(calls) == 1 and len(e.transcriber.calls) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["closed", "ack", "topic"])
async def test_spoken_correction_keeps_captured_editor_across_provider_wait(work_delivery, monkeypatch, damage):
    import asyncio

    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    proposal = AsyncMock(return_value=DraftAssignment(objective="Changed", success_criteria="Verified"))
    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    release, entered = asyncio.Event(), asyncio.Event()
    task = None
    try:
        await shown_editor(e)
        await enable_voice(e, owner, monkeypatch)
        original = e.transcriber.transcribe

        async def transcribe(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(e.transcriber, "transcribe", transcribe)
        script_file(e, 6181)
        task = asyncio.create_task(arrive(e, 6181, quoted=6180))
        await asyncio.wait_for(entered.wait(), 5)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
            assert mark.route_kind == "voice" and mark.route_basis["work_voice"]["owner"] == "management"
            prompt = await writer.get(TelegramCallbackRow, mark.route_basis["work_voice"]["prompt_id"])
            if damage == "closed":
                prompt.consumed_at, prompt.closed_reason = e.sender.outbox.clock(), "cancelled"
            elif damage == "topic":
                prompt.message_thread_id = 9901
            else:
                parent = await writer.get(TelegramOutboxRow, prompt.payload["anchor_outbox_id"])
                parent.parts = [{**part, "message_ids": []} for part in parent.parts]
        release.set()
        await task
        assert proposal.await_count == 0 and owner.host.turns.accept_transcript.await_count == 0
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
            assert mark.route_kind == "work_card_hold" and mark.route_complete
            assert mark.route_basis["work_voice"]["phase"] == "closed"
    finally:
        release.set()
        if task is not None:
            await task
        await owner.stop()


@pytest.mark.asyncio
async def test_ready_spoken_correction_recovers_from_real_sql_once(work_delivery, monkeypatch):
    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective="Review the expanded handbook", success_criteria="Verified corrections")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    try:
        await shown_editor(e)
        await enable_voice(e, owner, monkeypatch)
        router = owner.host.voice_inputs
        original = router.resume
        monkeypatch.setattr(router, "resume", AsyncMock(return_value=True))
        script_file(e, 6181)
        await arrive(e, 6181, quoted=6180)
        assert not calls and len(e.transcriber.calls) == 1
        async with e.db.sessions() as reader:
            origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id)))
        assert origin.route_kind == "work_card_hold" and origin.route_basis["work_voice"]["phase"] == "ready"
        monkeypatch.setattr(router, "resume", original)
        from test_telegram_work_interaction_delivery import guard_for

        assert await owner.chat.recover_input(e.channel, origin, guard=await guard_for(e))
        assert await owner.chat.recover_input(e.channel, origin, guard=await guard_for(e))
        assert calls == ["Include the additional chapter"] and len(e.transcriber.calls) == 1
        assert owner.host.turns.accept_transcript.await_count == 0
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True, "missing_ack", "foreign_ack", "recipe"])
async def test_oversized_spoken_correction_delivers_heard_and_the_limit(work_delivery, monkeypatch, legacy):
    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    proposal = AsyncMock()
    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    try:
        await shown_editor(e)
        maximum = owner.host.config.assign.max_chars
        await enable_voice(e, owner, monkeypatch, "a" * (maximum + 1))
        if legacy:
            from app.telegram_bots.voice import _echo

            voice = owner.host.voice_inputs.owners["management"]
            original = voice.complete

            async def old_capture(link, origin, **kwargs):
                captured = await original(link, origin, **kwargs)
                await owner.host.outbox.plan_product(
                    link,
                    source_id=f"voice-heard:{origin.chat_id}:{origin.message_id}",
                    parts=[{"kind": "text", "reply_to_message_id": origin.message_id, "source_ref": {"key": "voice.heard", "values": {"text": _echo(captured.sanitized_input["text"])}}}],
                    inbox_rows=[captured],
                    session=kwargs["session"],
                )
                return captured

            monkeypatch.setattr(voice, "complete", old_capture)
        script_file(e, 6181)
        await arrive(e, 6181, quoted=6180)
        assert proposal.await_count == 0 and owner.host.turns.accept_transcript.await_count == 0
        await dispatch(e, 6182)
        if legacy:
            from test_telegram_work_interaction_delivery import guard_for

            async with e.db.sessions() as reader:
                origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id)))
            assert not origin.route_complete
            if isinstance(legacy, str):
                async with e.db.sessions.begin() as writer:
                    old = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"notice:voice-heard:{origin.chat_id}:{origin.message_id}", TelegramOutboxRow.link_id == e.link.id))
                    assert old.state == "sent"
                    part = old.parts[0]
                    change = {"message_ids": [] if legacy == "missing_ack" else [999999]} if legacy != "recipe" else {"source_ref": {"key": "voice.heard", "values": {"text": "Different recording"}}}
                    old.parts = [{**part, **change}]
                with pytest.raises(ProductSourceRetired):
                    await owner.chat.recover_input(e.channel, origin, guard=await guard_for(e))
                async with e.db.sessions() as reader:
                    mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
                    assert not mark.route_complete
                    assert await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"notice:assignment-voice-closed:{origin.chat_id}:{origin.message_id}", TelegramOutboxRow.link_id == e.link.id)) is None
                return
            assert await owner.chat.recover_input(e.channel, origin, guard=await guard_for(e))
        await dispatch(e, 6183)
        assert not e.dispatch_errors, e.dispatch_errors
        sent = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert sent[-2].startswith("Heard:") and str(maximum) in sent[-1]
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
            assert mark.route_kind == "work_card_hold" and mark.route_complete
            assert mark.state == "done"
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["voice_note", "work_voice"])
@pytest.mark.parametrize("moment", ["before", "writer"])
async def test_unknown_model_recovery_holds_damaged_spoken_custody(work_delivery, monkeypatch, damage, moment):
    from test_telegram_work_interaction_delivery import guard_for

    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = SimpleNamespace(accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    proposal = AsyncMock(side_effect=RuntimeError("Synthetic interrupted model call"))
    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    try:
        await shown_editor(e)
        await enable_voice(e, owner, monkeypatch)
        script_file(e, 6181)
        await arrive(e, 6181, quoted=6180)
        assert proposal.await_count == 1
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
            prompt_id = mark.route_basis["management_edit"]["intent"]["prompt_id"]
            prompt = await writer.get(TelegramCallbackRow, prompt_id)
            assert prompt.payload["management_input"]["phase"] == "extracting"
            if moment == "before":
                mark.route_basis = {key: value for key, value in mark.route_basis.items() if key != damage}
        if moment == "writer":
            original = owner.chat.management.reader.detail

            async def drift(*args, **kwargs):
                result = await original(*args, **kwargs)
                async with e.db.sessions.begin() as writer:
                    mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
                    mark.route_basis = {key: value for key, value in mark.route_basis.items() if key != damage}
                return result

            monkeypatch.setattr(owner.chat.management.reader, "detail", drift)
        await owner.chat.management_edits.recover(e.link, guard=await guard_for(e))
        async with e.db.sessions() as reader:
            prompt = await reader.get(TelegramCallbackRow, prompt_id)
            assert prompt.payload["management_input"]["phase"] == "extracting"
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6181, e.link.chat_id))
            assert not mark.route_complete
        assert proposal.await_count == 1 and owner.host.turns.accept_transcript.await_count == 0
    finally:
        await owner.stop()

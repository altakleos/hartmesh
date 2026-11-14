"""Actual installed assignment speech stays private across stop and recovery."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_voice_notes import Transcribing, arrive, script_file, voice_on
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


async def installed_prompt(e, monkeypatch):
    owner = await installed_work_runtime(e)
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective=words, success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    assert await e.channel.handle_update(make_update(6131, account=e.link.chat_id, text="/assign"))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["purpose"].as_string() == "assign_words"))
        e.identity = row.id
    await dispatch(e, 6132)
    assert not e.dispatch_errors, e.dispatch_errors
    return owner, calls


async def installed_speech(e, monkeypatch):
    owner, calls = await installed_prompt(e, monkeypatch)
    e.services, e.request = owner.host, e.channel.bot.request
    e.services.turns = SimpleNamespace(accept=AsyncMock(), accept_transcript=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    acts = owner.host.acts
    await voice_on(e, e.db, monkeypatch, agreed=True, transcriber=Transcribing(text="Review the handbook"))
    owner.host.acts = acts
    return owner, calls


@pytest.mark.asyncio
async def test_installed_voice_prompt_drafts_without_an_ordinary_turn_or_work(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_speech(e, monkeypatch)
    try:
        script_file(e, 6133)
        await arrive(e, 6133, quoted=6132)
        assert calls == ["Review the handbook"] and len(e.transcriber.calls) == 1
        assert e.services.turns.accept_transcript.await_count == 0
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6133, e.link.chat_id))
            assert mark.route_kind == "assignment_draft"
            draft = await reader.get(TelegramActRow, mark.route_target_id)
            assert draft.state == "draft" and not draft.operation_ids
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == draft.id))
            assert card.parts[0]["visible_text"].startswith("Heard:") and card.parts[-1]["stored_keyboard"]
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_runtime_stop_fences_private_drafting_after_voice_provider_wait(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_speech(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.transcriber.transcribe

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(e.transcriber, "transcribe", held)
    script_file(e, 6133)
    task = asyncio.create_task(arrive(e, 6133, quoted=6132))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await owner.stop()
    finally:
        release.set()
        await task
        await owner.stop()
    assert calls == [] and len(e.transcriber.calls) == 1
    assert e.services.turns.accept_transcript.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6133, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.route_complete
        assert mark.route_basis["work_voice"]["phase"] == "ready"


@pytest.mark.asyncio
async def test_default_recovery_dispatches_the_stored_assignment_voice_owner(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_speech(e, monkeypatch)
    try:
        with monkeypatch.context() as stopped:
            stopped.setattr(e.services.voice_inputs, "resume", AsyncMock(return_value=True))
            script_file(e, 6133)
            await arrive(e, 6133, quoted=6132)
        assert calls == [] and len(e.transcriber.calls) == 1
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6133, e.link.chat_id))
            assert mark.route_kind == "work_card_hold" and mark.route_basis["work_voice"]["phase"] == "ready"
        await owner.recover_inputs(lambda: True, link_id=e.link.id)
        await owner.recover_inputs(lambda: True, link_id=e.link.id)
        assert calls == ["Review the handbook"] and len(e.transcriber.calls) == 1
        assert e.services.turns.accept_transcript.await_count == 0
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6133, e.link.chat_id))
            assert mark.route_kind == "assignment_draft"
    finally:
        await owner.stop()

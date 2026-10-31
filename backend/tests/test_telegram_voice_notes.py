"""Actual SDK ingress and SQL for voice notes, with a scripted transcriber and bot file bytes.

The provider adapters have their own tests in ``test_telegram_voice.py``, which
forbids sockets and therefore cannot reach PostgreSQL.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import update_payload
from _telegram_human_input_test_support import request
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_bot_handlers import message as sdk_message
from test_telegram_callback_commands import press, setup
from test_telegram_chat_turns import until
from test_telegram_human_input_flows import click, show
from test_telegram_human_input_sdk import ui as ui
from test_telegram_media_turns import controller, media
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots import voice
from app.telegram_bots.acts import Acts
from app.telegram_bots.capped_download import CappedDownloadError
from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.files import FileBytesBudget
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio
TRANSCRIPT = "Pay the Jones invoice tomorrow and tell me when it is done."


class Transcribing:
    """Protocol double: what a provider returned, not how its SDK behaves."""

    def __init__(self, provider="openai", text=TRANSCRIPT):
        self.provider, self.text, self.calls, self.held = provider, text, [], None

    async def transcribe(self, audio, *, timeout):
        self.calls.append((audio, timeout))
        if self.held is not None:
            await self.held.wait()
        if isinstance(self.text, Exception):
            raise self.text
        return self.text


async def voice_on(e, db, monkeypatch, *, agreed=False, transcriber="default"):
    e.transcriber = Transcribing() if transcriber == "default" else transcriber
    monkeypatch.setattr(voice, "resolve_transcriber", lambda config, **kwargs: e.transcriber)
    e.routed, e.reads, e.drafts = [], [], []

    async def read(url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        await before_dispatch()
        e.reads.append((ceiling, max_bytes))
        return e.audio

    async def writing(link, topic):
        e.drafts.append(topic)

    e.audio = b"opus-audio"
    e.services.config = TelegramBotsConfig()
    e.services.settings = db.settings
    e.services.file_bytes_budget = FileBytesBudget(200_000_000)
    e.services.acts = Acts(db.sessions, settings=db.settings, callbacks=e.services.callbacks, outbox=e.services.outbox)
    e.notes = e.services.voice_notes = voice.VoiceNotes(e.services)
    monkeypatch.setattr(e.request, "read_file_capped", read, raising=False)
    monkeypatch.setattr(e.channel, "show_voice_writing", writing)
    if agreed:
        await db.settings.update(e.link.owner_user_id, voice_agreed_for="openai", voice_agreed_at=datetime.now(UTC))


async def environment(db, monkeypatch, *, agreed=False, transcriber="default"):
    """Actual ingress and consent; the turn after the transcript is only recorded."""
    e = await setup(db, monkeypatch)

    async def routed(channel, link, row):
        e.routed.append(row)
        return True

    e.services.turns = SimpleNamespace(accept_transcript=routed, retire=AsyncMock(), typing_allowed=lambda *args, **kwargs: False, message_sent=lambda *args, **kwargs: None)
    await voice_on(e, db, monkeypatch, agreed=agreed, transcriber=transcriber)
    return e


def note(e, identity, *, duration=5, size=1000, forwarded=False, quoted=None):
    payload = update_payload(identity, message_id=identity, account=e.link.telegram_user_id, text="unused")
    message = payload["message"]
    message.pop("text")
    message["voice"] = {"file_id": f"voice-{identity}", "file_unique_id": f"unique-{identity}", "duration": duration, "mime_type": "audio/ogg", **({} if size is None else {"file_size": size})}
    if forwarded:
        message["forward_origin"] = {"type": "hidden_user", "date": 1_893_456_000, "sender_user_name": "Sam"}
    if quoted is not None:
        message["reply_to_message"] = {"message_id": quoted, "date": 1_893_456_000, "chat": message["chat"], "text": "Earlier"}
    return Update.de_json(payload, e.channel.bot)


async def arrive(e, identity, **kwargs):
    """The note reaches the handler; the service answers Telegram at once."""
    assert await e.channel.handle_update(note(e, identity, **kwargs)), "A voice note must not hold the updates behind it"
    await settle(e)


async def settle(e):
    while e.channel._ingress_tasks:
        await asyncio.gather(*tuple(e.channel._ingress_tasks))


def script_file(e, identity):
    e.request.enqueue("getFile", result={"file_id": f"voice-{identity}", "file_unique_id": f"unique-{identity}", "file_size": 10, "file_path": "voice/note.oga"})


async def plans(e, db):
    async with db.sessions() as session:
        rows = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state == "pending").order_by(TelegramOutboxRow.created_at, TelegramOutboxRow.id)))
    return [part["source_ref"] for row in rows for part in row.parts]


async def keys(e, db):
    return [source["key"] for source in await plans(e, db)]


async def inbox_row(db, e, identity):
    async with db.sessions() as session:
        return await session.get(TelegramInboundMarkRow, (e.link.id, identity, e.link.chat_id))


async def controls(db, e, kind):
    async with db.sessions() as session:
        return list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind, TelegramCallbackRow.consumed_at.is_(None))))


async def pressing(e, db, kind, identity, message_id):
    """Telegram shows the question, then the person presses one of its buttons."""
    selected = (await controls(db, e, kind))[-1]
    await e.services.callbacks.adopt(
        e.link,
        selected.group_id,
        message_id,
        chat_id=e.link.chat_id,
        message_thread_id=selected.message_thread_id,
        outbox_id=selected.payload["anchor_outbox_id"],
        presentation_version=selected.payload["presentation_version"],
        check_current=lambda: True,
    )
    e.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, identity, selected.id, message_id=message_id))
    await settle(e)


async def stop(e):
    await e.sender.stop()
    await e.channel.stop()


async def test_first_voice_note_asks_before_any_download(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3001)
        assert await keys(e, db) == ["voice.first_notice"]
        assert (await plans(e, db))[0]["values"] == {"transcriber": "OpenAI", "product": "HartMesh"}
        assert e.transcriber.calls == [] and e.reads == [] and "getFile" not in [call.endpoint for call in e.request.calls]
        assert [row.kind for row in await controls(db, e, "voice_ok")] == ["voice_ok"] and len(await controls(db, e, "voice_no")) == 1
        saved = (await controls(db, e, "voice_ok"))[0].payload
        assert (saved["offered_provider"], saved["file_id"], saved["duration"], saved["message_id"], saved["reply_to_message_id"]) == ("openai", "voice-3001", 5, 3001, None)
        assert (await inbox_row(db, e, 3001)).route_target_id == "asked"
    finally:
        await stop(e)


async def test_use_voice_notes_is_remembered_per_person_across_bots(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3002)
        script_file(e, 3002)
        await pressing(e, db, "voice_ok", 9001, 700)
        settings = await db.settings.get(e.link.owner_user_id)
        assert settings.voice_agreed_for == "openai" and settings.voice_agreed_at is not None
        assert [call[0] for call in e.transcriber.calls] == [b"opus-audio"] and [row.message_id for row in e.routed] == [3002]
        assert "voice.agreed" in await keys(e, db)
        script_file(e, 3003)
        await arrive(e, 3003)
        assert [row.message_id for row in e.routed] == [3002, 3003], "The agreement belongs to the person, so no second question"
        assert (await keys(e, db)).count("voice.first_notice") == 1
    finally:
        await stop(e)


async def test_changed_transcriber_asks_again(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    e.transcriber = Transcribing("gemini")
    try:
        await arrive(e, 3004)
        assert await keys(e, db) == ["voice.first_notice"]
        assert (await plans(e, db))[0]["values"]["transcriber"] == "Google"
        assert e.transcriber.calls == [] and e.reads == []
    finally:
        await stop(e)


async def test_not_now_stores_nothing_and_asks_again(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3005)
        await pressing(e, db, "voice_no", 9002, 701)
        settings = await db.settings.get(e.link.owner_user_id)
        assert settings.voice_agreed_for is None and settings.voice_agreed_at is None
        assert "voice.not_used" in await keys(e, db) and e.reads == [] and e.transcriber.calls == []
        row = await inbox_row(db, e, 3005)
        assert row.route_complete and row.sanitized_input["type"] == "voice"
        await arrive(e, 3006)
        assert (await keys(e, db)).count("voice.first_notice") == 2
    finally:
        await stop(e)


async def test_not_now_declines_even_when_the_service_changed_and_keeps_an_older_agreement(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    e.transcriber = Transcribing("gemini")
    try:
        await arrive(e, 3041)
        e.transcriber = Transcribing("openai")
        await pressing(e, db, "voice_no", 9041, 741)
        settings = await db.settings.get(e.link.owner_user_id)
        assert settings.voice_agreed_for == "openai" and "voice.not_used" in await keys(e, db) and e.reads == []
    finally:
        await stop(e)


async def test_transcript_echoed_then_handled_as_text(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    script_file(e, 3007)
    try:
        await arrive(e, 3007)
        assert await plans(e, db) == [{"key": "voice.heard", "values": {"text": TRANSCRIPT}}]
        row = e.routed[0]
        assert row.sanitized_input == {"type": "text", "text": TRANSCRIPT}
        assert (row.route_kind, row.route_target_id) == ("turn", e.link.agent_ref)
        assert row.route_basis["input_profile"] == {"version": 1, "kind": "plain_text", "forwarded": False}
        assert e.drafts == [None], "The draft reads Writing down your voice note…"
    finally:
        await stop(e)


@pytest.mark.parametrize(("outcome", "key"), [(RuntimeError("provider-text-is-never-shown"), "voice.failed"), (TimeoutError(), "voice.failed"), ("   ", "voice.unclear"), ("", "voice.unclear")])
async def test_a_note_with_no_transcript_is_told_so_and_counts_for_nothing(db, monkeypatch, outcome, key):
    e = await environment(db, monkeypatch, agreed=True)
    e.transcriber.text = outcome
    script_file(e, 3008)
    try:
        await arrive(e, 3008)
        assert await keys(e, db) == [key] and e.routed == []
        assert (await inbox_row(db, e, 3008)).route_complete
        async with db.sessions() as session:
            assert list(await session.scalars(select(TelegramActRow).where(TelegramActRow.kind == "voice_note"))) == []
        assert e.services.file_bytes_budget.used == 0
    finally:
        await stop(e)


@pytest.mark.parametrize("cause", ["deleted_target", "authority", "after_the_provider"])
async def test_a_note_that_cannot_continue_is_still_answered(db, monkeypatch, cause):
    from app.telegram_bots import authority

    e = await environment(db, monkeypatch, agreed=True)
    script_file(e, 3042)
    if cause == "deleted_target":
        monkeypatch.setattr(e.services, "prepare_ingress_values", AsyncMock(side_effect=FileNotFoundError()))
    elif cause == "authority":
        monkeypatch.setattr(authority, "require", AsyncMock(side_effect=PermissionError()))
    else:
        monkeypatch.setattr(e.services.acts, "write_voice_note", AsyncMock(side_effect=RuntimeError()))
    try:
        await arrive(e, 3042)
        assert await keys(e, db) == ["voice.failed"] and e.routed == []
        row = await inbox_row(db, e, 3042)
        assert row.route_complete and row.sanitized_input["type"] == "voice", "The writer that failed saved no text"
        assert len(e.transcriber.calls) == (1 if cause == "after_the_provider" else 0), "Nothing is sent twice"
    finally:
        await stop(e)


async def test_a_spent_question_does_nothing_on_a_second_press(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3043)
        selected = (await controls(db, e, "voice_no"))[0]
        await pressing(e, db, "voice_no", 9043, 743)
        before = await keys(e, db)
        await e.notes.execute_callback(e.channel, await e.services.callbacks.get(selected.id))
        assert await keys(e, db) == before
    finally:
        await stop(e)


async def test_the_audit_identity_follows_the_chat_as_the_inbox_does(db, monkeypatch):
    from dataclasses import replace

    e = await environment(db, monkeypatch)
    try:
        async with db.sessions.begin() as session:
            first = await e.services.acts.write_voice_note(e.link, message_id=3044, duration=1, provider="openai", used_as="message", session=session)
            second = await e.services.acts.write_voice_note(replace(e.link, chat_id=e.link.chat_id + 1), message_id=3044, duration=1, provider="openai", used_as="message", session=session)
        assert first.id != second.id, "A rebound link can reuse a message id in its new chat"
    finally:
        await stop(e)


async def test_a_token_shaped_transcript_is_not_saved(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    e.transcriber.text = "my bot is 123456789:AAHfiqksKZ8WmR2zSjiQ7_v4TMAKdiHm9T0"
    script_file(e, 3009)
    try:
        await arrive(e, 3009)
        assert await keys(e, db) == ["voice.failed"] and e.routed == []
        assert (await inbox_row(db, e, 3009)).sanitized_input["type"] == "voice"
    finally:
        await stop(e)


@pytest.mark.parametrize(
    ("failure", "verdict", "calls_get_me"),
    [
        ("download", 404, True),
        ("invalid", 404, True),
        ("invalid", None, True),
    ],
)
async def test_a_404_on_a_voice_download_does_not_reject_the_token(db, monkeypatch, failure, verdict, calls_get_me):
    from telegram.error import InvalidToken

    e = await environment(db, monkeypatch, agreed=True)
    verified = []

    async def verify(error=None, **kwargs):
        verified.append(type(error).__name__)

    async def failing(url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        if failure == "invalid":
            raise InvalidToken("Not Found")
        raise CappedDownloadError("status", status=404)

    monkeypatch.setattr(e.channel, "verify_token", verify)
    monkeypatch.setattr(e.request, "read_file_capped", failing, raising=False)
    script_file(e, 3010)
    try:
        await arrive(e, 3010)
        assert verified == ["InvalidToken"], "One getMe decides; the note itself is told as failed"
        assert await keys(e, db) == ["voice.failed"] and e.transcriber.calls == [] and e.channel.link.health == "ok"
    finally:
        await stop(e)


async def test_daily_budget_summed_from_act_rows(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    owner = e.link.owner_user_id
    settings = await db.settings.get(owner)
    moment = datetime(2030, 6, 15, 16, 0, tzinfo=UTC)  # 12:00 in the fixture's New York
    today = moment - timedelta(hours=11)
    yesterday = moment - timedelta(hours=13)

    def act(identity, *, created, duration=100, state="done", kind="voice_note"):
        return TelegramActRow(
            id=f"{identity:032x}",
            link_id=None,
            bot_id=e.link.bot_id + identity,
            owner_user_id=owner,
            binding_generation=1,
            telegram_user_id=e.link.telegram_user_id,
            kind=kind,
            body={"duration": duration, "provider": "openai", "used_as": "message"},
            state=state,
            operation_ids=[],
            attempts=1,
            created_at=created,
            updated_at=created,
        )

    async with db.sessions.begin() as session:
        session.add_all([act(1, created=today), act(2, created=today, duration=40), act(3, created=yesterday), act(4, created=today, state="refused"), act(5, created=today, kind="settings")])
    try:
        assert await e.notes.voice_seconds_today(owner, settings, now=moment) == 140
        async with db.sessions.begin() as session:
            session.add(act(6, created=datetime.now(UTC) - timedelta(minutes=1), duration=1798))
        await arrive(e, 3011, duration=3)
        assert await keys(e, db) == ["voice.daily_limit"]
        assert (await plans(e, db))[0]["values"] == {"n": 30}
        assert e.reads == [] and e.transcriber.calls == [], "A note over the budget is not fetched"
    finally:
        await stop(e)


async def test_voice_act_row_holds_no_audio_transcript_or_file_id(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    script_file(e, 3012)
    try:
        await arrive(e, 3012, duration=7)
        async with db.sessions() as session:
            act = await session.scalar(select(TelegramActRow).where(TelegramActRow.kind == "voice_note"))
        assert act.body == {"duration": 7, "provider": "openai", "used_as": "message"}
        assert (act.state, act.attempts, act.message_id, act.owner_user_id) == ("done", 1, 3012, e.link.owner_user_id)
        recorded = json.dumps([act.body, act.result, act.operation_ids])
        assert "voice-3012" not in recorded and "Jones" not in recorded and "opus" not in recorded
        row = await inbox_row(db, e, 3012)
        assert "opus" not in json.dumps([row.sanitized_input, row.route_basis])
        assert e.services.file_bytes_budget.used == 0, "The memory charge ends with the audio"
    finally:
        await stop(e)


async def test_forwarded_voice_note_is_marked_as_material(db, monkeypatch):
    from app.telegram_bots.inbox import FORWARDED_PREFIX, chat_input_parts

    e = await environment(db, monkeypatch, agreed=True)
    script_file(e, 3013)
    try:
        await arrive(e, 3013, forwarded=True)
        text, files = chat_input_parts(e.routed[0], bot_id=e.link.bot_id)
        assert text == FORWARDED_PREFIX.format(origin="Sam") + "\n\n" + TRANSCRIPT and files == []
    finally:
        await stop(e)


async def test_voice_without_transcriber_gets_voice_unavailable(db, monkeypatch):
    e = await environment(db, monkeypatch, transcriber=None)
    try:
        await arrive(e, 3014)
        assert await plans(e, db) == [{"key": "voice.unavailable", "values": {"product": "HartMesh"}}]
        assert e.reads == [] and (await inbox_row(db, e, 3014)).route_complete
    finally:
        await stop(e)


async def test_voice_over_byte_limit_is_refused_before_download(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    limit = e.services.config.files.max_inbound_bytes
    try:
        await arrive(e, 3015, duration=301)
        await arrive(e, 3016, size=limit + 1)
        assert sorted(await keys(e, db)) == ["file.too_large_in", "voice.too_long"]
        assert {"key": "voice.too_long", "values": {"n": 5}} in await plans(e, db)
        assert e.reads == [] and "getFile" not in [call.endpoint for call in e.request.calls]
    finally:
        await stop(e)


@pytest.mark.parametrize("cause", ["download", "get_file"])
async def test_a_note_that_turns_out_too_large_is_not_transcribed_or_counted(db, monkeypatch, cause):
    e = await environment(db, monkeypatch, agreed=True)

    async def too_large(url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        raise CappedDownloadError("too_large")

    if cause == "download":
        script_file(e, 3017)
        monkeypatch.setattr(e.request, "read_file_capped", too_large, raising=False)
    else:
        e.request.enqueue("getFile", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: file is too big"})
    try:
        await arrive(e, 3017, size=None)
        assert await keys(e, db) == ["file.too_large_in"] and e.transcriber.calls == [] and e.routed == []
        async with db.sessions() as session:
            assert await session.scalar(select(TelegramActRow).where(TelegramActRow.kind == "voice_note")) is None
    finally:
        await stop(e)


async def test_outstanding_consent_cannot_authorize_changed_provider(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3018)
        old = (await controls(db, e, "voice_ok"))[0]
        e.transcriber = Transcribing("gemini")
        await pressing(e, db, "voice_ok", 9003, 702)
        assert (await db.settings.get(e.link.owner_user_id)).voice_agreed_for is None
        assert e.reads == [] and e.transcriber.calls == [] and (await inbox_row(db, e, 3018)).route_target_id == "asked"
        assert old.id not in [row.id for row in await controls(db, e, "voice_ok")], "The old press is spent"
        fresh = (await controls(db, e, "voice_ok"))[0]
        assert fresh.payload["offered_provider"] == "gemini"
        assert {"key": "voice.first_notice", "values": {"transcriber": "Google", "product": "HartMesh"}} in await plans(e, db)
        script_file(e, 3018)
        await pressing(e, db, "voice_ok", 9004, 702)
        assert (await db.settings.get(e.link.owner_user_id)).voice_agreed_for == "gemini" and [row.message_id for row in e.routed] == [3018]
    finally:
        await stop(e)


async def test_a_press_after_the_service_stopped_qualifying_is_unavailable(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3019)
        e.transcriber = None
        await pressing(e, db, "voice_ok", 9005, 703)
        assert "voice.unavailable" in await keys(e, db) and e.reads == [] and (await inbox_row(db, e, 3019)).route_complete
        assert await controls(db, e, "voice_ok") == []
    finally:
        await stop(e)


async def test_queued_consent_rechecks_offered_provider_under_owner_lock(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        await arrive(e, 3020)
        async with e.notes._locks.hold(e.link.owner_user_id):
            selected = (await controls(db, e, "voice_ok"))[0]
            await e.services.callbacks.adopt(
                e.link,
                selected.group_id,
                704,
                chat_id=e.link.chat_id,
                message_thread_id=None,
                outbox_id=selected.payload["anchor_outbox_id"],
                presentation_version=selected.payload["presentation_version"],
                check_current=lambda: True,
            )
            e.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 9006, selected.id, message_id=704))
            await asyncio.sleep(0.05)
            assert e.reads == [] and (await db.settings.get(e.link.owner_user_id)).voice_agreed_for is None, "A confirmed note waits holding only identifiers"
            e.transcriber = Transcribing("gemini")
        await settle(e)
        assert e.reads == [] and e.transcriber.calls == [] and (await db.settings.get(e.link.owner_user_id)).voice_agreed_for is None
        assert (await controls(db, e, "voice_ok"))[0].payload["offered_provider"] == "gemini"
    finally:
        await stop(e)


async def test_two_notes_of_one_person_never_overrun_the_day_together(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    e.services.config.voice.max_seconds_per_person_per_day = 8
    held = e.transcriber.held = asyncio.Event()
    script_file(e, 3021)
    try:
        assert await e.channel.handle_update(note(e, 3021, duration=5))
        assert await e.channel.handle_update(note(e, 3022, duration=5))
        while not e.transcriber.calls:
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        assert len(e.transcriber.calls) == 1 and e.services.file_bytes_budget.used > 0, "The second note waits with no audio"
        assert len(e.reads) == 1
        held.set()
        await settle(e)
        assert [row.message_id for row in e.routed] == [3021] and "voice.daily_limit" in await keys(e, db)
    finally:
        held.set()
        await stop(e)


async def test_a_redelivered_note_is_never_transcribed_twice(db, monkeypatch):
    e = await environment(db, monkeypatch, agreed=True)
    script_file(e, 3023)
    try:
        await arrive(e, 3023)
        await arrive(e, 3023)
        assert len(e.transcriber.calls) == 1 and len(e.routed) == 1
    finally:
        await stop(e)


async def test_a_long_transcript_is_clipped_only_in_the_echo():
    text = "word " * 2000
    clipped = voice._echo(text)
    assert len(clipped) < 4000 and clipped.endswith("…") and voice._echo("short") == "short"
    assert voice._echo("😀" * 3000).endswith("…"), "A surrogate pair is never cut in half"


async def actual_turns(db, monkeypatch, *, agreed=True):
    """Everything after the transcript is the real typed-text path."""
    from app.telegram_bots.files import PendingFiles

    e, turns, state = await controller(db, monkeypatch)
    e.services.pending_files = PendingFiles(e.services.callbacks, e.services.outbox.inbox, guard_factory=lambda link: None)
    monkeypatch.setattr(e.channel, "admit_file", AsyncMock(return_value=True))
    await voice_on(e, db, monkeypatch, agreed=agreed)
    return e, turns, state


async def test_transcript_takes_the_file_sent_before_it_as_typed_text_would(db, monkeypatch):
    e, turns, state = await actual_turns(db, monkeypatch)
    script_file(e, 3102)
    try:
        assert await turns.accept(e.channel, media(e, 3101))
        await e.notes.accept(e.channel, note(e, 3102))
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        assert turn.inbound.text == TRANSCRIPT and [file["message_id"] for file in turn.inbound.files] == [3101]
        assert [file["message_id"] for file in turn.inbound.files if file["file_id"].startswith("voice-")] == [], "The recording is never a file for the model"
    finally:
        await turns.stop()
        await e.channel.stop()


async def test_file_sent_while_transcribing_travels_with_the_transcript(db, monkeypatch):
    e, turns, state = await actual_turns(db, monkeypatch)
    held = e.transcriber.held = asyncio.Event()
    script_file(e, 3103)
    try:
        await e.notes.accept(e.channel, note(e, 3103))
        await until(lambda: len(e.transcriber.calls) == 1)
        assert await turns.accept(e.channel, media(e, 3104))
        held.set()
        await until(lambda: state.posts == 1)
        assert [file["message_id"] for file in next(iter(turns._turns.values())).inbound.files] == [3104]
    finally:
        held.set()
        await turns.stop()
        await e.channel.stop()


async def test_a_failed_note_leaves_the_file_for_the_next_message(db, monkeypatch):
    e, turns, state = await actual_turns(db, monkeypatch)
    e.transcriber.text = RuntimeError("failed")
    script_file(e, 3106)
    try:
        assert await turns.accept(e.channel, media(e, 3105))
        await e.notes.accept(e.channel, note(e, 3106))
        await settle(e)
        assert state.posts == 0
        e.request.enqueue("sendChatAction", result=True)
        assert await turns.accept(e.channel, sdk_message(e, 3107, content="Use that file"))
        await until(lambda: state.posts == 1)
        assert [file["message_id"] for file in next(iter(turns._turns.values())).inbound.files] == [3105]
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.parametrize("spoken", ["/report the numbers", " //report the numbers", "/new report the numbers"])
async def test_a_spoken_slash_selects_no_skill_and_runs_no_command(db, monkeypatch, spoken):
    from deerflow.skills.slash import parse_slash_skill_reference

    e, turns, state = await actual_turns(db, monkeypatch)
    e.transcriber.text = spoken
    script_file(e, 3108)
    try:
        await e.notes.accept(e.channel, note(e, 3108))
        await until(lambda: state.posts == 1)
        text = next(iter(turns._turns.values())).inbound.text
        assert text.endswith("report the numbers") and not text.startswith("/") and parse_slash_skill_reference(text) is None
    finally:
        await turns.stop()
        await e.channel.stop()


async def test_startup_tells_an_interrupted_note_and_leaves_a_waiting_question(db, monkeypatch):
    e, turns, state = await actual_turns(db, monkeypatch, agreed=False)
    host = e.services
    host._copy_decision, host._cohort_complete, host._ready, host._send_ready, host._stopping, host._stopped, host._cause = False, True, True, False, False, False, None
    inbox = host.outbox.inbox
    interrupted = note(e, 3109)
    try:
        row, _ = await inbox.accept(e.link, interrupted, sanitized_input={"type": "voice", "voice": {"file_id": "voice-3109", "duration": 5}})
        await inbox.route(row, kind="voice", target_id="working", basis={"forwarded": False, "origin": None}, operation_ids=[])
        await e.notes.accept(e.channel, note(e, 3110))
        await settle(e)
        assert (await inbox_row(db, e, 3110)).route_target_id == "asked"
        await turns.recover_startup()
        saved = await inbox_row(db, e, 3109)
        assert saved.route_complete and e.transcriber.calls == [] and e.reads == [] and not turns._turns
        async with db.sessions() as session:
            reply = await session.get(TelegramOutboxRow, saved.delivery_ids[-1])
        assert reply.parts[0]["source_ref"] == {"key": "voice.failed", "values": {}}
        assert reply.payload["product_keyboard_basis"] == [], "A restarted note cannot be retried from a button: there is no text to retry"
        waiting = await inbox_row(db, e, 3110)
        assert not waiting.route_complete and waiting.route_target_id == "asked" and len(await controls(db, e, "voice_ok")) == 1
    finally:
        await turns.stop()
        await e.channel.stop()


async def test_a_transcript_saved_before_a_restart_is_kept_and_never_sent_again(db, monkeypatch):
    e, turns, state = await actual_turns(db, monkeypatch)
    host = e.services
    host._copy_decision, host._cohort_complete, host._ready, host._send_ready, host._stopping, host._stopped, host._cause = False, True, True, False, False, False, None

    async def crashed(*args, **kwargs):
        return False

    monkeypatch.setattr(turns, "accept_transcript", crashed)
    script_file(e, 3111)
    try:
        await e.notes.accept(e.channel, note(e, 3111))
        await settle(e)
        assert len(e.transcriber.calls) == 1
        await turns.recover_startup()
        assert len(e.transcriber.calls) == 1
        saved = await inbox_row(db, e, 3111)
        async with db.sessions() as session:
            reply = await session.get(TelegramOutboxRow, saved.delivery_ids[-1])
        assert reply.parts[0]["source_ref"]["key"] == "chat.restarted" and saved.sanitized_input["text"] == TRANSCRIPT
    finally:
        await turns.stop()
        await e.channel.stop()


async def prompts_on(e, db, monkeypatch):
    """The actual prompt owner decides; a transcript it declines would enter the queue."""
    await voice_on(e, db, monkeypatch, agreed=True)
    e.services.turns.typing_allowed = lambda *args, **kwargs: False
    e.declined = []

    async def routed(channel, link, row):
        if not await e.services.human_inputs.handle_transcript(channel, link, row):
            e.declined.append(row.message_id)

    e.services.turns.accept_transcript = routed


async def test_open_question_is_answered_by_the_transcript(ui, db, monkeypatch):
    e = ui
    await prompts_on(e, db, monkeypatch)
    await show(e, request())
    await click(e, "clarify_other", 3201, send=True, result_id=702)
    script_file(e, 3202)
    await arrive(e, 3202)
    stage = e.services.turns.accept_staged.await_args.args[1]
    assert stage.text == TRANSCRIPT and stage.human_message_kwargs["human_input_response"]["value"] == TRANSCRIPT and e.declined == []
    async with db.sessions() as session:
        act = await session.scalar(select(TelegramActRow).where(TelegramActRow.kind == "voice_note"))
    assert act.body["used_as"] == "answer"


async def test_open_form_field_takes_the_transcript(ui, db, monkeypatch):
    e = ui
    await prompts_on(e, db, monkeypatch)
    raw = request(version=2, input_mode="form", fields=[{"name": "name", "label": "Your name", "type": "text", "required": True}])
    raw.pop("options")
    e.transcriber.text = "Alice"
    await show(e, raw)
    script_file(e, 3203)
    await arrive(e, 3203)
    assert e.declined == []
    row = await inbox_row(db, e, 3203)
    assert row.route_complete and "human_input_ui" in row.route_basis


async def test_a_question_that_appeared_after_the_note_does_not_claim_it(ui, db, monkeypatch):
    e = ui
    await prompts_on(e, db, monkeypatch)
    held = e.transcriber.held = asyncio.Event()
    script_file(e, 3204)
    assert await e.channel.handle_update(note(e, 3204))
    await until(lambda: len(e.transcriber.calls) == 1)
    # The test helpers drain ingress tasks; this one is deliberately held open.
    transcribing = e.channel._ingress_tasks.pop()
    await show(e, request())
    await click(e, "clarify_other", 3205, send=True, result_id=702)
    held.set()
    await transcribing
    assert e.declined == [3204], "Spoken before the question, so it stays an ordinary message"
    e.services.turns.accept_staged.assert_not_awaited()
    async with db.sessions() as session:
        act = await session.scalar(select(TelegramActRow).where(TelegramActRow.kind == "voice_note"))
    assert act.body["used_as"] == "message"


async def test_a_note_that_quotes_a_question_answers_it(ui, db, monkeypatch):
    e = ui
    await prompts_on(e, db, monkeypatch)
    await show(e, request())
    await click(e, "clarify_other", 3206, send=True, result_id=702)
    script_file(e, 3207)
    await arrive(e, 3207, quoted=702)
    assert e.services.turns.accept_staged.await_args.args[1].text == TRANSCRIPT and e.declined == []


async def test_a_forwarded_note_cannot_answer_a_question(ui, db, monkeypatch):
    e = ui
    await prompts_on(e, db, monkeypatch)
    await show(e, request())
    await click(e, "clarify_other", 3208, send=True, result_id=702)
    script_file(e, 3209)
    await arrive(e, 3209, forwarded=True)
    assert e.declined == [3209]
    e.services.turns.accept_staged.assert_not_awaited()


async def test_writing_draft_is_sent_through_the_actual_sdk(db, monkeypatch):
    e = await environment(db, monkeypatch)
    e.services.turns.typing_allowed = lambda *args, **kwargs: True
    e.request.enqueue("sendMessageDraft", result=True)
    try:
        await type(e.channel).show_voice_writing(e.channel, e.link, None)
        call = next(call for call in e.request.calls if call.endpoint == "sendMessageDraft")
        assert call.parameters["text"] == "Writing down your voice note…" and call.parameters["chat_id"] == e.link.chat_id
    finally:
        await stop(e)


@pytest.mark.parametrize(("transcriber", "mentions"), [("default", True), (None, False)])
async def test_help_privacy_and_refusal_say_voice_only_when_it_works(db, monkeypatch, transcriber, mentions):
    from app.telegram_bots.render import render

    e = await environment(db, monkeypatch, transcriber=transcriber)
    try:
        link = e.link
        helped = await e.services.commands._presentation("help", link)
        private = await e.services.commands._presentation("privacy", link)
        assert ("voice note" in render(helped["key"], "en-US", **helped["values"])) is mentions
        assert ("turned into text by OpenAI" in render(private["key"], "en-US", **private["values"])) is mentions
        e.request.enqueue("sendMessage", result={"message_id": 800, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "reply"})
        payload = update_payload(3301, message_id=3301, account=link.telegram_user_id, text="unused")
        payload["message"].pop("text")
        payload["message"]["sticker"] = {"file_id": "s", "file_unique_id": "u", "type": "regular", "width": 1, "height": 1, "is_animated": False, "is_video": False}
        assert await e.channel.handle_update(Update.de_json(payload, e.channel.bot))
        await settle(e)
        source = (await plans(e, db))[-1]
        assert source["key"] == "media.unsupported" and ("voice notes" in render(source["key"], "en-US", **source["values"])) is mentions
    finally:
        await stop(e)

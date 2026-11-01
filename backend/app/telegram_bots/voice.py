"""Qualify a transcribing service and turn one admitted voice note into typed text."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar, Literal, Protocol
from urllib.parse import urlsplit

from pydantic import SecretStr

from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.settings import local_now
from app.telegram_bots.texts import KEYBOARDS
from app.telegram_bots.token_pattern import TOKEN_IN_TEXT
from deerflow.config import get_app_config
from deerflow.config.model_config import ModelConfig
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)

_OPENAI_BASE_URL = "https://api.openai.com/v1"
_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/"
_TRANSCRIPTION_PROMPT = "Transcribe this audio word for word. Output only the transcript."
PROVIDER_NAMES = {"openai": "OpenAI", "gemini": "Google Gemini"}
_ECHO_UTF16_MAX = 3900
_TOKEN = re.compile(TOKEN_IN_TEXT)
_unqualified_reasons: set[str] = set()
_reason_lock = threading.Lock()
_voice_sdk_call: ContextVar[bool] = ContextVar("telegram_voice_sdk_call", default=False)
_filter_lock = threading.Lock()
_SDK_LOGGERS = ("openai", "google_genai", "google.genai", "google.auth", "httpx", "httpcore", "aiohttp", "urllib3", "requests")


class _VoiceSDKLogFilter(logging.Filter):
    """Keep voice payloads out of SDK logs without muting concurrent calls."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not (_voice_sdk_call.get() and any(record.name == name or record.name.startswith(name + ".") for name in _SDK_LOGGERS))


_voice_sdk_log_filter = _VoiceSDKLogFilter()


@contextmanager
def _voice_sdk_logging() -> Iterator[None]:
    # Provider imports may configure handlers. Install after those imports and
    # before constructing a client, on every current log destination. Logger
    # filters would miss records propagated from their descendant loggers.
    with _filter_lock:
        handlers = set(logging.getLogger().handlers)
        for candidate in list(logging.root.manager.loggerDict.values()):
            if isinstance(candidate, logging.Logger):
                handlers.update(candidate.handlers)
        for handler in handlers:
            handler.addFilter(_voice_sdk_log_filter)
    token = _voice_sdk_call.set(True)
    try:
        yield
    finally:
        _voice_sdk_call.reset(token)


class Transcriber(Protocol):
    provider: Literal["openai", "gemini"]

    async def transcribe(self, audio: bytes, *, timeout: float) -> str: ...


@dataclass(frozen=True)
class OpenAITranscriber:
    """A captured entry's credential and model, with no environment-selected URL."""

    api_key: str = field(repr=False)
    model: str
    provider: ClassVar[Literal["openai"]] = "openai"

    async def transcribe(self, audio: bytes, *, timeout: float) -> str:
        def load():
            from openai import AsyncOpenAI

            return AsyncOpenAI

        # A cold SDK import and the client's TLS context are slow file work.
        factory = await await_drained(asyncio.to_thread(load))
        with _voice_sdk_logging():
            client = await await_drained(asyncio.to_thread(factory, api_key=self.api_key, base_url=_OPENAI_BASE_URL, timeout=timeout, max_retries=0))
            async with client:
                async with asyncio.timeout(timeout):
                    response = await client.audio.transcriptions.create(model=self.model, file=("voice.ogg", audio, "audio/ogg"), timeout=timeout)
            return response.text


@dataclass(frozen=True)
class GeminiTranscriber:
    """Use one inline-audio generation request against the Gemini Developer API."""

    api_key: str = field(repr=False)
    model: str
    provider: ClassVar[Literal["gemini"]] = "gemini"

    async def transcribe(self, audio: bytes, *, timeout: float) -> str:
        def load():
            from google import genai
            from google.genai import types

            return genai, types

        genai, types = await await_drained(asyncio.to_thread(load))

        def build():
            return genai.Client(
                api_key=self.api_key,
                vertexai=False,
                http_options=types.HttpOptions(base_url=_GEMINI_BASE_URL, timeout=math.ceil(timeout * 1000), retry_options=types.HttpRetryOptions(attempts=1)),
            )

        with _voice_sdk_logging():
            client = await await_drained(asyncio.to_thread(build))
            try:
                async with client.aio as async_client:
                    async with asyncio.timeout(timeout):
                        response = await async_client.models.generate_content(
                            model=self.model,
                            contents=[_TRANSCRIPTION_PROMPT, types.Part.from_bytes(data=audio, mime_type="audio/ogg")],
                            config=types.GenerateContentConfig(automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True)),
                        )
            finally:
                # The SDK constructs a sync HTTP pool as well as its async
                # pool; async exit closes only the latter. No sync request runs.
                await await_drained(asyncio.to_thread(client.close))
            return response.text or ""


def _unqualified(provider: str, name: str) -> None:
    """Log only a fixed field/variable name, once across resolver callers."""
    reason = f"{provider}.{name}"
    with _reason_lock:
        if reason in _unqualified_reasons:
            return
        _unqualified_reasons.add(reason)
    logger.info("telegram_bots voice_unqualified reason=%s", reason, extra={"reason": reason})


def _class_path(use: str) -> tuple[str, str]:
    # Configuration uses ``module:Class``. Accept the dotted form documented
    # by ModelConfig as well, without importing the provider or a custom class.
    module, separator, name = use.rpartition(":" if ":" in use else ".")
    return (module, name) if separator else ("", "")


def _in_module(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(prefix + ".")


def _openai_endpoint(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        url = urlsplit(value)
        # Evaluate the port too: malformed addresses must fail qualification,
        # even though the captured transcriber never uses this entry's URL.
        _ = url.port
        return url.scheme in {"http", "https"} and url.hostname == "api.openai.com" and url.username is None and url.password is None
    except ValueError:
        return False


def _entry_key(entry: ModelConfig, names: tuple[str, ...]) -> str | None:
    for name in names:
        value = (entry.model_extra or {}).get(name)
        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        if isinstance(value, str) and value.strip():
            return value
    return None


def _openai_entry(entry: ModelConfig, class_name: str, config: TelegramBotsConfig) -> OpenAITranscriber | None:
    if class_name != "ChatOpenAI":
        _unqualified("openai", "use")
        return None
    extra = entry.model_extra or {}
    for name in ("base_url", "openai_api_base", "api_base"):
        if extra.get(name) is not None and not _openai_endpoint(extra[name]):
            _unqualified("openai", name)
            return None
    for name in ("OPENAI_BASE_URL", "OPENAI_API_BASE"):
        if name in os.environ and not _openai_endpoint(os.environ[name]):
            _unqualified("openai", name)
            return None
    key = _entry_key(entry, ("api_key", "openai_api_key"))
    if key is None:
        _unqualified("openai", "api_key")
        return None
    return OpenAITranscriber(api_key=key, model=config.voice.openai_model)


def _gemini_entry(entry: ModelConfig, class_name: str, config: TelegramBotsConfig) -> GeminiTranscriber | None:
    if class_name != "ChatGoogleGenerativeAI":
        _unqualified("gemini", "use")
        return None
    extra = entry.model_extra or {}
    for name in ("base_url", "client_options", "vertexai", "project", "location", "credentials"):
        # Presence matters here, including null/false/empty values.
        if name in extra:
            _unqualified("gemini", name)
            return None
    if "GOOGLE_GEMINI_BASE_URL" in os.environ:
        _unqualified("gemini", "GOOGLE_GEMINI_BASE_URL")
        return None
    if os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "").lower() in {"true", "1"}:
        _unqualified("gemini", "GOOGLE_GENAI_USE_VERTEXAI")
        return None
    key = _entry_key(entry, ("gemini_api_key", "google_api_key", "api_key"))
    if key is None:
        _unqualified("gemini", "gemini_api_key")
        return None
    return GeminiTranscriber(api_key=key, model=config.voice.gemini_model if config.voice.gemini_model is not None else entry.model)


def resolve_transcriber(config: TelegramBotsConfig, *, models: Sequence[ModelConfig] | None = None) -> Transcriber | None:
    """Qualify supplied models or a fresh snapshot; environment keys supply no credentials."""
    provider = config.voice.provider
    if provider == "off":
        return None
    models = get_app_config().models if models is None else models
    if provider in {"auto", "openai"}:
        for entry in models:
            module, class_name = _class_path(entry.use)
            if _in_module(module, "langchain_openai"):
                transcriber = _openai_entry(entry, class_name, config)
                if transcriber is not None:
                    return transcriber
    if provider in {"auto", "gemini"}:
        for entry in models:
            module, class_name = _class_path(entry.use)
            if _in_module(module, "langchain_google_genai"):
                transcriber = _gemini_entry(entry, class_name, config)
                if transcriber is not None:
                    return transcriber
    return None


def _echo(text: str) -> str:
    """One Telegram message shows what was heard; the model receives all of it."""
    units = text.encode("utf-16-le")
    if len(units) // 2 <= _ECHO_UTF16_MAX:
        return text
    return units[: _ECHO_UTF16_MAX * 2].decode("utf-16-le", "ignore").rstrip() + "…"


def _local_midnight(settings, now: datetime) -> datetime:
    return local_now(settings, now).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)


class VoiceNotes:
    """Turn an admitted note into typed text, once.

    The inbox row is the only state: ``working`` until the note has an outcome,
    ``asked`` while its consent question waits. Audio exists only inside
    ``_transcribe``. A note that stops working is told so and never sent to a
    service again: its call may have been dispatched, and a completed one is
    already typed text. The per-owner lock spans bots and makes the daily budget
    hold; text and Stop never wait on it.
    """

    def __init__(self, services):
        self.services = services
        self._locks = AsyncKeyedLockTable()

    async def transcriber(self) -> Transcriber | None:
        return await await_drained(asyncio.to_thread(resolve_transcriber, self.services.config))

    def _guard(self, channel, link):
        from app.telegram_bots.turns import _Guard

        return _Guard(self.services, channel, link, lambda: channel._callback_current(link), repair=True)

    async def accept(self, channel, update) -> None:
        """Save the note and return: Telegram's next update never waits for it."""
        from app.telegram_bots.turns import forward_origin

        link, message = deepcopy(channel._admission(update).link), update.message
        if not await channel._input_current(link):
            return
        # The wire form: the SDK's attribute is changing from seconds to timedelta.
        voice = message.voice.to_dict()
        descriptor = {
            "file_id": voice["file_id"],
            "file_unique_id": voice["file_unique_id"],
            "mime_type": voice.get("mime_type"),
            "size": voice.get("file_size"),
            "duration": voice["duration"],
            "message_id": message.message_id,
            "telegram_bot_id": link.bot_id,
        }
        inbox = self.services.outbox.inbox
        row, created = await await_drained(inbox.accept(link, update, sanitized_input={"type": "voice", "voice": descriptor}))
        if created or row.route_kind is None:
            forwarded, origin = forward_origin(message)
            row = await await_drained(inbox.route(row, kind="voice", target_id="working", basis={"forwarded": forwarded, "origin": origin}, operation_ids=[]))
            channel._spawn_ingress(self._run(channel, link, row.message_id, lambda guard: self._consider(channel, link, row, guard)))
        channel._classified(update, remember=True)

    async def execute_callback(self, channel, row) -> None:
        link = deepcopy(channel.link)
        await self._run(channel, link, row.payload.get("message_id"), lambda guard: self._press(channel, link, row, guard))

    async def _run(self, channel, link, message_id, step) -> None:
        """Hold the owner's lock for the step; finished text is routed after it."""
        from app.telegram_bots.callbacks import CallbackRefused

        try:
            async with self._locks.hold(link.owner_user_id):
                row = await step(self._guard(channel, link))
            if row is not None:
                await self.services.turns.accept_transcript(channel, link, row)
        except CallbackRefused:
            pass  # A spent press: its note already has its owner.
        except Exception as error:
            logger.warning("telegram_bots voice_note_held link=%s error=%s", link.id, type(error).__name__)
            await self._give_up(channel, link, message_id)

    async def _give_up(self, channel, link, message_id) -> None:
        """Whatever stopped a note, the person hears so once; it is never resent."""
        try:
            row = await self.services.outbox.inbox.voice_note(link, message_id)
            if row is not None:
                await self._answer(channel, link, row, "voice.failed", {})
        except Exception as error:
            logger.warning("telegram_bots voice_note_unanswered link=%s error=%s", link.id, type(error).__name__)

    async def voice_seconds_today(self, owner_user_id: str, settings, *, now: datetime | None = None) -> int:
        midnight = await await_drained(asyncio.to_thread(_local_midnight, settings, now or datetime.now(UTC)))
        return await self.services.acts.voice_seconds_since(owner_user_id, midnight)

    async def _refusal(self, link, row, transcriber) -> tuple[str, dict] | None:
        """The design's checks in order. Nothing is fetched until all have passed."""
        voice, limits = row.sanitized_input["voice"], self.services.config.voice
        product = (await self.services.prepare_ingress_values(link))["product"]
        if transcriber is None:
            return "voice.unavailable", {"product": product}
        if voice["duration"] > limits.max_seconds:
            return "voice.too_long", {"n": limits.max_seconds // 60}
        settings = await self.services.settings.get(link.owner_user_id)
        if await self.voice_seconds_today(link.owner_user_id, settings) + voice["duration"] > limits.max_seconds_per_person_per_day:
            return "voice.daily_limit", {"n": limits.max_seconds_per_person_per_day // 60}
        if type(voice["size"]) is int and voice["size"] > self.services.config.files.max_inbound_bytes:
            return "file.too_large_in", {"product": product}
        return None

    async def _consider(self, channel, link, row, guard):
        transcriber = await self.transcriber()
        refusal = await self._refusal(link, row, transcriber)
        if refusal is not None:
            await self._answer(channel, link, row, *refusal)
            return None
        settings = await self.services.settings.get(link.owner_user_id)
        if settings.voice_agreed_for == transcriber.provider:
            return await self._transcribe(channel, link, row, transcriber, guard)
        key, values, keyboard = await self._question(link, row, transcriber.provider)
        async with self.services.callbacks.writer(link, guard) as writer:
            row = await self.services.outbox.inbox.voice_phase(link, row, "asked", session=writer, guard=guard)
            await self.services.outbox.plan_product_keyboard(
                link,
                source_id=f"voice-offer:{row.chat_id}:{row.message_id}",
                parts=[{"kind": "text", "reply_to_message_id": row.message_id, "source_ref": {"key": key, "values": values}}],
                keyboard=keyboard,
                callbacks=self.services.callbacks,
                guard=guard,
                inbox_rows=[row],
                message_thread_id=row.message_thread_id,
                session=writer,
            )
        return None

    async def _question(self, link, row, provider):
        voice = row.sanitized_input["voice"]
        name, product = PROVIDER_NAMES[provider], (await self.services.prepare_ingress_values(link))["product"]
        payload = {"offered_provider": provider, "file_id": voice["file_id"], "duration": voice["duration"], "message_id": row.message_id, "reply_to_message_id": row.sanitized_input.get("reply_to_message_id")}
        return "voice.first_notice", {"transcriber": name, "product": product}, KEYBOARDS["voice.first_notice"](payload=payload)

    async def _press(self, channel, link, row, guard):
        """Answer one consent press; agreement names exactly the offered service."""
        from app.telegram_bots.callbacks import CallbackRefused

        inbox = self.services.outbox.inbox
        note = await inbox.voice_note(link, row.payload.get("message_id"))
        if note is None or note.route_target_id != "asked" or note.sanitized_input["voice"]["file_id"] != row.payload.get("file_id"):
            raise CallbackRefused() from None
        transcriber = await self.transcriber()
        agreed, finished, keyboard = False, True, []
        if row.kind == "voice_no":
            key, values = "voice.not_used", {}
        elif transcriber is not None and row.payload.get("offered_provider") != transcriber.provider:
            key, values, keyboard = await self._question(link, note, transcriber.provider)
            finished = False
        else:
            refusal = await self._refusal(link, note, transcriber)
            key, values = refusal or ("voice.agreed", {})
            agreed, finished = refusal is None, refusal is not None
        async with self.services.callbacks.writer(link, guard) as writer:
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            fresh = await inbox.voice_note(link, note.message_id, session=writer)
            if group.selected.kind != row.kind or fresh is None or fresh.route_target_id != "asked":
                raise CallbackRefused() from None
            await self.services.outbox.plan_product_keyboard(
                link,
                source_id="voice:" + row.id,
                parts=[{"kind": "edit", "target_message_id": row.message_id, "source_ref": {"key": key, "values": values}}],
                keyboard=keyboard,
                callbacks=self.services.callbacks,
                guard=guard,
                inbox_rows=[fresh],
                finish_route=finished,
                message_thread_id=row.message_thread_id,
                session=writer,
            )
            await self.services.callbacks.consume_group(group, reason="done", session=writer)
            if agreed:
                await self.services.settings.update(link.owner_user_id, voice_agreed_for=transcriber.provider, voice_agreed_at=datetime.now(UTC), session=writer)
                fresh = await inbox.voice_phase(link, fresh, "working", session=writer, guard=guard)
        return await self._transcribe(channel, link, fresh, transcriber, guard) if agreed else None

    async def _answer(self, channel, link, row, key, values) -> None:
        await channel.send_text(key, source_id=f"voice-note:{row.chat_id}:{row.message_id}", reply_to=row.message_id, thread=row.message_thread_id, inbox_rows=[row], finish_route=True, _captured_link=link, **values)

    async def _transcribe(self, channel, link, row, transcriber, guard):
        """Fetch and transcribe under the lock; the audit commits with the text."""
        from app.telegram_bots.authority import require
        from app.telegram_bots.producer import DeliveryClaimRetired

        async def check():
            guard.current()
            await self.services.prepare_live_owner(link.owner_user_id)
            await require(link.owner_user_id, "threads:read", "threads:write")
            if not await channel._input_current(link):
                raise DeliveryClaimRetired()

        voice = row.sanitized_input["voice"]
        await check()
        channel._spawn_ingress(channel.show_voice_writing(link, row.message_thread_id))
        lease = await channel.read_voice_audio(link, voice, check=check)
        if type(lease) is str:
            await self._answer(channel, link, row, "voice.failed" if lease == "file.not_saved" else lease, {"product": (await self.services.prepare_ingress_values(link))["product"]} if lease == "file.too_large_in" else {})
            return None
        try:
            await check()
            if await self.transcriber() is None:
                await self._answer(channel, link, row, *await self._refusal(link, row, None))
                return None
            text = await transcriber.transcribe(lease.data, timeout=self.services.config.voice.timeout_seconds)
        except DeliveryClaimRetired:
            raise
        except Exception as error:
            logger.info("telegram_bots voice_note_failed link=%s error=%s", link.id, type(error).__name__)
            await self._answer(channel, link, row, "voice.failed", {})
            return None
        finally:
            await await_drained(lease.aclose())
        # Speech has no commands: a spoken slash must not select a skill.
        text = text.strip().lstrip("/").lstrip() if type(text) is str else ""
        if not text:
            await self._answer(channel, link, row, "voice.unclear", {})
            return None
        if _TOKEN.search(text):
            await self._answer(channel, link, row, "voice.failed", {})
            return None
        human_inputs = getattr(self.services, "human_inputs", None)
        used_as = "answer" if human_inputs is not None and await human_inputs.answers_prompt(channel, link, row) else "message"
        async with self.services.callbacks.writer(link, guard) as writer:
            row = await self.services.outbox.inbox.complete_voice(link, row, text=text, session=writer, guard=guard)
            await self.services.acts.write_voice_note(link, message_id=row.message_id, duration=voice["duration"], provider=transcriber.provider, used_as=used_as, session=writer)
            await self.services.outbox.plan_product(
                link,
                source_id=f"voice-heard:{row.chat_id}:{row.message_id}",
                parts=[{"kind": "text", "reply_to_message_id": row.message_id, "source_ref": {"key": "voice.heard", "values": {"text": _echo(text)}}}],
                inbox_rows=[row],
                session=writer,
            )
        return row

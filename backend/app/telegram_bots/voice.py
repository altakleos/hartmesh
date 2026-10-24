"""Qualify deployment models and capture one provider for a voice transcription."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import ClassVar, Literal, Protocol
from urllib.parse import urlsplit

from pydantic import SecretStr

from app.telegram_bots.config import TelegramBotsConfig
from deerflow.config import get_app_config
from deerflow.config.model_config import ModelConfig

logger = logging.getLogger(__name__)

_OPENAI_BASE_URL = "https://api.openai.com/v1"
_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/"
_TRANSCRIPTION_PROMPT = "Transcribe this audio word for word. Output only the transcript."
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
        from openai import AsyncOpenAI

        with _voice_sdk_logging():
            async with AsyncOpenAI(api_key=self.api_key, base_url=_OPENAI_BASE_URL, timeout=timeout, max_retries=0) as client:
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
        from google import genai
        from google.genai import types

        with _voice_sdk_logging():
            client = genai.Client(
                api_key=self.api_key,
                vertexai=False,
                http_options=types.HttpOptions(base_url=_GEMINI_BASE_URL, timeout=math.ceil(timeout * 1000), retry_options=types.HttpRetryOptions(attempts=1)),
            )
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
                client.close()
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

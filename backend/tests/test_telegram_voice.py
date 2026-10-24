"""Provider qualification and transcription without provider traffic or credentials."""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
import socket
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import SecretStr

from app.telegram_bots import voice
from app.telegram_bots.config import TelegramBotsConfig
from deerflow.config.app_config import AppConfig
from deerflow.config.model_config import ModelConfig

_VOICE_RECORD_LOGGERS = ("openai._base_client", "google_genai._api_client", "google.genai", "google.auth.credentials", "httpx", "httpcore.http11", "aiohttp.client", "urllib3.connectionpool", "requests")


@contextmanager
def _voice_loggers_at_debug():
    # Materialize SDK loggers before capturing their state. A preceding test
    # may have set an explicit level, disabled a logger, or stopped propagation.
    importlib.import_module("openai")
    importlib.import_module("google.genai")
    families = ("openai", "google_genai", "google.genai", "google.auth", "httpx", "httpcore", "aiohttp", "urllib3", "requests", "synthetic_application")
    loggers = {logging.getLogger(name) for name in (*families, *_VOICE_RECORD_LOGGERS, "google_genai.synthetic_nonpropagating")}
    loggers.update(candidate for candidate in list(logging.root.manager.loggerDict.values()) if isinstance(candidate, logging.Logger) and any(candidate.name == family or candidate.name.startswith(family + ".") for family in families))
    for candidate in list(loggers):
        parent = candidate.parent
        while parent is not None and parent is not logging.root:
            loggers.add(parent)
            parent = parent.parent
    saved = [(candidate, candidate.level, candidate.disabled, candidate.propagate) for candidate in loggers]
    try:
        for candidate, *_ in saved:
            candidate.setLevel(logging.DEBUG)
            candidate.disabled = False
            candidate.propagate = True
        yield
    finally:
        for candidate, level, disabled, propagate in saved:
            candidate.setLevel(level)
            candidate.disabled = disabled
            candidate.propagate = propagate


def _key() -> str:
    return "synthetic-" + uuid4().hex


def _model(provider: str = "openai", **extra: object) -> ModelConfig:
    classes = {"openai": "langchain_openai:ChatOpenAI", "gemini": "langchain_google_genai:ChatGoogleGenerativeAI"}
    return ModelConfig.model_validate({"name": "synthetic", "model": "synthetic-chat-model", "use": classes[provider], "api_key": _key(), **extra})


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for variable in ("OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_API_BASE", "GOOGLE_API_KEY", "GEMINI_API_KEY", "GOOGLE_GEMINI_BASE_URL", "GOOGLE_GENAI_USE_VERTEXAI"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(voice, "_unqualified_reasons", set())

    def no_connect(*args, **kwargs):
        raise AssertionError("Voice tests must never open a socket")

    monkeypatch.setattr(socket.socket, "connect", no_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", no_connect)
    monkeypatch.setattr(socket, "create_connection", no_connect)


@pytest.fixture
def models(monkeypatch):
    entries = []
    monkeypatch.setattr(voice, "get_app_config", lambda: SimpleNamespace(models=entries))
    return entries


def _resolve(provider="auto", **voice_settings):
    return voice.resolve_transcriber(TelegramBotsConfig(voice={"provider": provider, **voice_settings}))


def test_openai_chosen_when_key_present(models):
    models.extend([_model("gemini"), _model()])
    transcriber = _resolve()
    assert isinstance(transcriber, voice.OpenAITranscriber)
    assert transcriber.provider == "openai"


def test_gemini_chosen_without_openai_key(models):
    models.extend([_model(api_key=None), _model("gemini")])
    assert isinstance(_resolve(), voice.GeminiTranscriber)


@pytest.mark.parametrize(
    ("use", "expected"),
    [
        ("langchain_openai:ChatOpenAI", "openai"),
        ("langchain_openai.chat_models.base:ChatOpenAI", "openai"),
        ("langchain_openai.ChatOpenAI", "openai"),
        ("langchain_google_genai:ChatGoogleGenerativeAI", "gemini"),
        ("langchain_google_genai.chat_models:ChatGoogleGenerativeAI", "gemini"),
        ("langchain_google_genai.ChatGoogleGenerativeAI", "gemini"),
        ("langchain_openai_fake:ChatOpenAI", None),
        ("third_party.langchain_openai:ChatOpenAI", None),
        ("langchain_openai:ChatOpenAISubclass", None),
        ("langchain_google_genai_fake:ChatGoogleGenerativeAI", None),
        ("third_party:ChatGoogleGenerativeAI", None),
        ("langchain_google_genai:ChatGoogleGenerativeAISubclass", None),
    ],
)
def test_provider_qualifies_only_as_a_listed_model_at_its_own_endpoint(models, use, expected):
    models.append(_model(use=use))
    result = _resolve()
    assert (result.provider if result else None) == expected


def test_key_variable_alone_does_not_qualify(models, monkeypatch):
    for variable in ("OPENAI_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_KEY"):
        monkeypatch.setenv(variable, _key())
    assert _resolve() is None
    models.extend([_model(api_key=None), _model("gemini", api_key=None)])
    assert _resolve() is None


@pytest.mark.parametrize("address", ["https://elsewhere.invalid/v1", "https://api.openai.com.elsewhere.invalid/v1", "not-an-address", "", "https://[broken", "https://api.openai.com:invalid"])
def test_openai_class_with_another_base_url_is_never_used(models, address):
    models.append(_model(base_url=address))
    assert _resolve() is None


@pytest.mark.parametrize("field", ["openai_api_base", "api_base"])
def test_openai_entry_with_api_base_elsewhere_does_not_qualify(models, field):
    models.append(_model(base_url="https://api.openai.com/v1", **{field: "https://elsewhere.invalid/v1"}))
    assert _resolve() is None


@pytest.mark.parametrize("field", ["base_url", "openai_api_base", "api_base"])
@pytest.mark.parametrize("address", [None, "https://api.openai.com/v1", "https://API.OPENAI.COM/v1/"])
def test_openai_entry_allows_absent_or_official_endpoint(models, field, address):
    models.append(_model(**{field: address}))
    assert _resolve().provider == "openai"


@pytest.mark.parametrize("variable", ["OPENAI_BASE_URL", "OPENAI_API_BASE"])
@pytest.mark.parametrize("address", ["https://elsewhere.invalid/v1", "", "not-an-address"])
def test_openai_does_not_qualify_when_the_environment_names_another_address(models, monkeypatch, variable, address):
    models.append(_model())
    monkeypatch.setenv(variable, address)
    assert _resolve() is None


@pytest.mark.parametrize("variable", ["OPENAI_BASE_URL", "OPENAI_API_BASE"])
def test_openai_environment_may_name_its_own_host(models, monkeypatch, variable):
    models.append(_model())
    monkeypatch.setenv(variable, "https://api.openai.com/v1")
    assert _resolve().provider == "openai"


def test_azure_class_is_not_openai(models):
    models.append(_model(use="langchain_openai:AzureChatOpenAI"))
    assert _resolve() is None


@pytest.mark.parametrize("field", ["base_url", "client_options", "vertexai", "project", "location", "credentials"])
@pytest.mark.parametrize("value", [None, False, "", "synthetic-value"])
def test_gemini_entry_with_another_address_or_vertex_does_not_qualify(models, field, value):
    models.append(_model("gemini", **{field: value}))
    assert _resolve() is None


@pytest.mark.parametrize(
    ("variable", "value"),
    [("GOOGLE_GEMINI_BASE_URL", ""), ("GOOGLE_GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/"), ("GOOGLE_GENAI_USE_VERTEXAI", "true"), ("GOOGLE_GENAI_USE_VERTEXAI", "TRUE"), ("GOOGLE_GENAI_USE_VERTEXAI", "1")],
)
def test_gemini_does_not_qualify_when_the_environment_moves_the_client(models, monkeypatch, variable, value):
    models.append(_model("gemini"))
    monkeypatch.setenv(variable, value)
    assert _resolve() is None


@pytest.mark.parametrize("value", ["false", "0", ""])
def test_gemini_non_vertex_environment_qualifies(models, monkeypatch, value):
    models.append(_model("gemini"))
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", value)
    assert _resolve().provider == "gemini"


@pytest.mark.parametrize("provider", ["openai", "gemini", "off"])
def test_explicit_provider_without_a_listed_model_is_unavailable(models, provider):
    assert _resolve(provider) is None
    models.append(_model("gemini" if provider == "openai" else "openai"))
    assert _resolve(provider) is None


def test_an_invalid_entry_does_not_hide_a_later_qualifying_entry(models):
    models.extend([_model(base_url="https://elsewhere.invalid"), _model()])
    assert _resolve().provider == "openai"


def test_auto_may_select_qualified_gemini_when_openai_is_unqualified(models):
    models.extend([_model(base_url="https://elsewhere.invalid"), _model("gemini")])
    assert _resolve().provider == "gemini"
    assert _resolve("openai") is None


@pytest.fixture
def clients(monkeypatch):
    import openai
    from google import genai

    created = []

    class Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.request = AsyncMock(return_value=SimpleNamespace(text="synthetic transcript"))
            self.audio = SimpleNamespace(transcriptions=SimpleNamespace(create=self.request))
            self.models = SimpleNamespace(generate_content=self.request)
            self.aio = self
            self.closed = False
            self.sync_closed = False
            created.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True

        def close(self):
            self.sync_closed = True

    monkeypatch.setattr(openai, "AsyncOpenAI", Client)
    monkeypatch.setattr(genai, "Client", Client)
    return created


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "fields", "expected_field"),
    [
        ("openai", ("api_key", "openai_api_key"), "api_key"),
        ("openai", ("openai_api_key",), "openai_api_key"),
        ("gemini", ("gemini_api_key", "google_api_key", "api_key"), "gemini_api_key"),
        ("gemini", ("google_api_key", "api_key"), "google_api_key"),
        ("gemini", ("api_key",), "api_key"),
    ],
)
@pytest.mark.parametrize("secret_type", [str, SecretStr])
async def test_transcriber_uses_the_model_entrys_key_not_the_environment(models, clients, monkeypatch, provider, fields, expected_field, secret_type):
    values = {field: secret_type(_key()) for field in fields}
    models.append(_model(provider, **{"api_key": None, **values}))
    for variable in ("OPENAI_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_KEY"):
        monkeypatch.setenv(variable, _key())
    result = _resolve(provider)
    assert clients == []
    assert await result.transcribe(b"synthetic ogg", timeout=2.5) == "synthetic transcript"
    expected = values[expected_field]
    expected = expected.get_secret_value() if isinstance(expected, SecretStr) else expected
    assert clients[0].kwargs["api_key"] == expected
    assert expected not in repr(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["openai", "gemini"])
async def test_transcriber_address_is_fixed_whatever_the_environment_says(models, clients, monkeypatch, provider):
    models.append(_model(provider))
    transcriber = _resolve(provider)
    for variable in ("OPENAI_BASE_URL", "OPENAI_API_BASE", "GOOGLE_GEMINI_BASE_URL"):
        monkeypatch.setenv(variable, "https://elsewhere.invalid/")
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
    await transcriber.transcribe(b"synthetic ogg", timeout=2.5)
    client = clients[0]
    assert client.closed
    if provider == "openai":
        assert client.kwargs["base_url"] == "https://api.openai.com/v1"
        assert client.kwargs["max_retries"] == 0
        client.request.assert_awaited_once_with(model="gpt-4o-mini-transcribe", file=("voice.ogg", b"synthetic ogg", "audio/ogg"), timeout=2.5)
    else:
        assert client.kwargs["vertexai"] is False
        options = client.kwargs["http_options"]
        assert options.base_url == "https://generativelanguage.googleapis.com/"
        assert options.timeout == 2500
        assert options.retry_options.attempts == 1
        request = client.request.await_args.kwargs
        assert request["model"] == "synthetic-chat-model"
        prompt, audio = request["contents"]
        assert prompt == "Transcribe this audio word for word. Output only the transcript."
        assert audio.inline_data.data == b"synthetic ogg"
        assert audio.inline_data.mime_type == "audio/ogg"
        assert client.sync_closed
        client.request.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["openai", "gemini"])
async def test_transcriber_follows_a_key_changed_while_the_gateway_runs(models, clients, monkeypatch, provider):
    first_key, second_key = _key(), _key()
    # Exercise the actual runtime model type and environment resolution, without
    # reading any application configuration or credential file.
    monkeypatch.setenv("TELEGRAM_SYNTHETIC_KEY", first_key)
    values = {"name": "synthetic", "model": "synthetic-model", "use": _model(provider).use, "api_key": "$TELEGRAM_SYNTHETIC_KEY"}
    models.append(ModelConfig.model_validate(AppConfig.resolve_env_variables(values)))
    first = _resolve(provider)
    monkeypatch.setenv("TELEGRAM_SYNTHETIC_KEY", second_key)
    models[:] = [ModelConfig.model_validate(AppConfig.resolve_env_variables(values))]
    second = _resolve(provider)
    await first.transcribe(b"first", timeout=2)
    await second.transcribe(b"second", timeout=2)
    assert [client.kwargs["api_key"] for client in clients] == [first_key, second_key]


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["openai", "gemini"])
async def test_voice_model_override_is_captured(models, clients, provider):
    models.append(_model(provider))
    config = TelegramBotsConfig(voice={"provider": provider, f"{provider}_model": "synthetic-transcription-model"})
    transcriber = voice.resolve_transcriber(config)
    setattr(config.voice, f"{provider}_model", "changed-after-resolution")
    await transcriber.transcribe(b"synthetic ogg", timeout=2)
    assert clients[0].request.await_args.kwargs["model"] == "synthetic-transcription-model"


@pytest.mark.parametrize(
    ("provider", "extra", "reason"),
    [
        ("openai", {"base_url": "https://elsewhere.invalid/"}, "openai.base_url"),
        ("openai", {"api_key": None}, "openai.api_key"),
        ("openai", {"use": "langchain_openai:AzureChatOpenAI"}, "openai.use"),
        ("gemini", {"client_options": {"api_endpoint": "synthetic-sensitive-endpoint"}}, "gemini.client_options"),
        ("gemini", {"api_key": None}, "gemini.gemini_api_key"),
    ],
)
def test_reason_log_names_the_field_or_variable_never_its_value(models, caplog, provider, extra, reason):
    secret = _key()
    models.append(_model(provider, **{"api_key": secret, **extra}))
    with caplog.at_level(logging.INFO, logger=voice.__name__):
        assert _resolve(provider) is None
        assert _resolve(provider) is None
    records = [record for record in caplog.records if record.name == voice.__name__]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].getMessage() == f"telegram_bots voice_unqualified reason={reason}"
    assert records[0].reason == reason
    assert records[0].exc_info is None
    assert secret not in str(vars(records[0]))
    assert "elsewhere.invalid" not in str(vars(records[0]))
    assert "synthetic-sensitive-endpoint" not in str(vars(records[0]))


@pytest.mark.parametrize(("provider", "variable"), [("openai", "OPENAI_BASE_URL"), ("openai", "OPENAI_API_BASE"), ("gemini", "GOOGLE_GEMINI_BASE_URL"), ("gemini", "GOOGLE_GENAI_USE_VERTEXAI")])
def test_environment_rejection_logs_the_variable_once(models, monkeypatch, caplog, provider, variable):
    models.append(_model(provider))
    monkeypatch.setenv(variable, "true" if variable == "GOOGLE_GENAI_USE_VERTEXAI" else "https://synthetic-sensitive.invalid")
    with caplog.at_level(logging.INFO, logger=voice.__name__):
        assert _resolve(provider) is None
        assert _resolve(provider) is None
    records = [record for record in caplog.records if record.name == voice.__name__]
    assert [(record.getMessage(), record.reason) for record in records] == [(f"telegram_bots voice_unqualified reason={provider}.{variable}", f"{provider}.{variable}")]
    assert "synthetic-sensitive.invalid" not in str(vars(records[0]))


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["openai", "gemini"])
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError, TimeoutError])
async def test_transcription_failure_closes_the_client_without_fallback_or_logging(models, clients, monkeypatch, caplog, provider, failure):
    models.extend([_model(), _model("gemini")])
    transcriber = _resolve(provider)
    secret = _key()

    async def fail(*args, **kwargs):
        raise failure(secret)

    import openai
    from google import genai

    factory = openai.AsyncOpenAI if provider == "openai" else genai.Client

    def failed_client(**kwargs):
        client = factory(**kwargs)
        client.request.side_effect = fail
        return client

    monkeypatch.setattr(openai if provider == "openai" else genai, "AsyncOpenAI" if provider == "openai" else "Client", failed_client)
    with pytest.raises(failure), caplog.at_level(logging.INFO, logger=voice.__name__):
        await transcriber.transcribe(b"synthetic ogg", timeout=2)
    assert len(clients) == 1
    assert clients[0].closed
    assert not [record for record in caplog.records if record.name == voice.__name__]
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("openai._base_client").debug("after-failure-visible")
        logging.getLogger("google_genai._api_client").debug("after-failure-visible")
    assert [record.getMessage() for record in caplog.records].count("after-failure-visible") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["openai", "gemini"])
async def test_transcription_deadline_cancels_the_request_and_closes_client(models, clients, monkeypatch, provider):
    models.append(_model(provider))
    import openai
    from google import genai

    factory = openai.AsyncOpenAI if provider == "openai" else genai.Client
    cancelled = asyncio.Event()

    async def wait(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    def waiting_client(**kwargs):
        client = factory(**kwargs)
        client.request.side_effect = wait
        return client

    monkeypatch.setattr(openai if provider == "openai" else genai, "AsyncOpenAI" if provider == "openai" else "Client", waiting_client)
    with pytest.raises(TimeoutError):
        await _resolve(provider).transcribe(b"synthetic ogg", timeout=0.001)
    assert cancelled.is_set()
    assert clients[0].closed


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["openai", "gemini"])
@pytest.mark.parametrize("outcome", ["success", "exception", "cancelled"])
async def test_actual_sdk_voice_logs_are_suppressed_without_hiding_concurrent_calls(models, monkeypatch, caplog, provider, outcome):
    """Run real SDK request packing on an offline transport at DEBUG level."""
    # Restore deliberate per-case routing changes before restoring the ambient
    # logger state, including when an assertion or SDK operation raises.
    with _voice_loggers_at_debug(), monkeypatch.context() as local_patch:
        await _assert_actual_sdk_voice_logs(models, local_patch, caplog, provider, outcome)


async def _assert_actual_sdk_voice_logs(models, monkeypatch, caplog, provider, outcome):
    import httpx
    import openai
    from google import genai

    secret_key, transcript = _key(), "synthetic-private-transcript-" + uuid4().hex
    audio = ("synthetic-private-audio-" + uuid4().hex).encode()
    models.append(_model(provider, api_key=secret_key))
    entered, release = asyncio.Event(), asyncio.Event()
    requests = []
    isolated_stream = io.StringIO()
    isolated_handler = logging.StreamHandler(isolated_stream)
    isolated_logger = logging.getLogger("google_genai.synthetic_nonpropagating")
    monkeypatch.setattr(isolated_logger, "handlers", [isolated_handler])
    monkeypatch.setattr(isolated_logger, "propagate", False)
    monkeypatch.setattr(isolated_logger, "level", logging.DEBUG)
    propagating_stream = io.StringIO()
    propagating_handler = logging.StreamHandler(propagating_stream)
    sdk_logger = logging.getLogger("openai._base_client" if provider == "openai" else "google_genai._api_client")
    monkeypatch.setattr(sdk_logger, "handlers", [*sdk_logger.handlers, propagating_handler])
    monkeypatch.setattr(sdk_logger, "propagate", True)
    monkeypatch.setattr(sdk_logger, "level", logging.DEBUG)

    async def transport(request):
        requests.append(request)
        # Exercise SDK/HTTP families with both nonpropagating and propagating
        # handlers, without recording actual provider credentials or traffic.
        for family in _VOICE_RECORD_LOGGERS:
            logging.getLogger(family).debug("private voice data %s %s %s", secret_key, audio, transcript)
        isolated_logger.debug("private voice data %s", audio)
        logging.getLogger("synthetic_application").info("voice-context-application-visible")
        entered.set()
        await release.wait()
        if outcome == "exception":
            raise httpx.ReadError(f"private voice data {secret_key}", request=request)
        payload = {"text": transcript} if provider == "openai" else {"candidates": [{"content": {"parts": [{"text": transcript}], "role": "model"}}]}
        return httpx.Response(200, json=payload)

    if provider == "openai":
        real_factory = openai.AsyncOpenAI

        def factory(**kwargs):
            return real_factory(**kwargs, http_client=httpx.AsyncClient(transport=httpx.MockTransport(transport)))

        monkeypatch.setattr(openai, "AsyncOpenAI", factory)
    else:
        real_factory = genai.Client

        def factory(**kwargs):
            kwargs["http_options"].async_client_args = {"transport": httpx.MockTransport(transport)}
            return real_factory(**kwargs)

        monkeypatch.setattr(genai, "Client", factory)

    with caplog.at_level(logging.DEBUG):
        task = asyncio.create_task(_resolve(provider).transcribe(audio, timeout=5))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            logging.getLogger("openai._base_client").debug("unrelated-openai-call-visible")
            logging.getLogger("google_genai._api_client").debug("unrelated-gemini-call-visible")
            isolated_logger.debug("unrelated-nonpropagating-call-visible")
            for family in _VOICE_RECORD_LOGGERS:
                logging.getLogger(family).debug("unrelated-%s-visible", family)
        finally:
            if outcome == "cancelled":
                task.cancel()
            release.set()
            if outcome == "success":
                result = await task
            else:
                with pytest.raises(asyncio.CancelledError if outcome == "cancelled" else Exception):
                    await task
        logging.getLogger("httpx").debug("after-voice-visible")

    if outcome == "success":
        assert result == transcript
    assert len(requests) == 1
    assert requests[0].url.host == ("api.openai.com" if provider == "openai" else "generativelanguage.googleapis.com")
    assert secret_key in (requests[0].headers.get("authorization", "") + requests[0].headers.get("x-goog-api-key", ""))
    captured = caplog.text + isolated_stream.getvalue() + propagating_stream.getvalue()
    assert secret_key not in captured
    assert transcript not in captured
    assert audio.decode() not in captured
    assert "private voice data" not in captured
    for marker in ("voice-context-application-visible", "unrelated-openai-call-visible", "unrelated-gemini-call-visible", "unrelated-nonpropagating-call-visible", "after-voice-visible"):
        assert marker in captured
    for family in _VOICE_RECORD_LOGGERS:
        assert f"unrelated-{family}-visible" in captured
    assert f"unrelated-{provider}-call-visible" in propagating_stream.getvalue()
    # Installation can run for every note without duplicating handler filters.
    for handler in (isolated_handler, propagating_handler):
        filters = [item for item in handler.filters if isinstance(item, voice._VoiceSDKLogFilter)]
        assert len(filters) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ambient_level", [logging.WARNING, logging.ERROR])
async def test_actual_sdk_logging_with_restricted_provider_and_http_levels(models, monkeypatch, caplog, ambient_level):
    with ExitStack() as stack:
        for name in (*_VOICE_RECORD_LOGGERS, "google"):
            stack.enter_context(caplog.at_level(ambient_level, logger=name))
        await test_actual_sdk_voice_logs_are_suppressed_without_hiding_concurrent_calls(models, monkeypatch, caplog, "gemini", "success")


@pytest.mark.parametrize("level", [logging.WARNING, logging.ERROR])
@pytest.mark.parametrize("disabled", [False, True])
@pytest.mark.parametrize("propagate", [False, True])
def test_voice_log_capture_normalizes_and_restores_ambient_logger_state(monkeypatch, caplog, level, disabled, propagate):
    with ExitStack() as stack:
        loggers = [logging.getLogger(name) for name in (*_VOICE_RECORD_LOGGERS, "google", "synthetic_application")]
        for candidate in loggers:
            stack.enter_context(caplog.at_level(level, logger=candidate.name))
            monkeypatch.setattr(candidate, "disabled", disabled)
            monkeypatch.setattr(candidate, "propagate", propagate)
        # Even a failing assertion inside the capture scope must restore every
        # preceding test's state; setLevel also invalidates logging's caches.
        with pytest.raises(RuntimeError, match="synthetic capture failure"), caplog.at_level(logging.DEBUG):
            with _voice_loggers_at_debug():
                for candidate in loggers:
                    assert (candidate.level, candidate.disabled, candidate.propagate) == (logging.DEBUG, False, True)
                    candidate.debug("capture-enabled-%s", candidate.name)
                raise RuntimeError("synthetic capture failure")
        for candidate in loggers:
            assert f"capture-enabled-{candidate.name}" in caplog.text
            assert (candidate.level, candidate.disabled, candidate.propagate) == (level, disabled, propagate)

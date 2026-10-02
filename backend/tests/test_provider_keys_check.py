"""Testing a provider key before it is set: one short message, nothing stored, nothing applied.

A mistyped key is otherwise found only when a chat fails. These tests pin
what the test says and what it must not do: it builds the client a run would
build from the catalog's own entry, with the candidate key in place of the
catalog's reference; it tells an accepted key from a refused one and from a
question it could not settle; and the key reaches nothing but that one
client -- not the environment, the configuration, the database, the answer or
the log.
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
import yaml

# Loading a rendered profile with the real config loader writes process-wide singletons.
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult

from app.gateway.provider_keys import probe as probe_module
from app.gateway.provider_keys.cipher import WRAPPING_KEY_ENV
from app.gateway.provider_keys.probe import ProbeOutcome, carries, classify, probe_model, with_key
from app.gateway.provider_keys.profile import PROFILE_DIR_ENV
from app.gateway.provider_keys.service import ProviderKeyService, ProviderKeysRefused

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
SEED = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-SEED-A"
GOOD = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-GOOD-B"
BAD = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-BAD-C"


class _ProviderError(Exception):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        if status_code is not None:
            self.status_code = status_code


class _KeyCheckingChat(BaseChatModel):
    """A provider that answers the good key and refuses every other, quoting it as real ones do."""

    model: str
    api_key: str
    max_retries: int = 2
    hang: bool = False

    @property
    def _llm_type(self) -> str:
        return "key-checking"

    def _refuse_unless_good(self) -> None:
        if self.api_key != GOOD:
            raise _ProviderError(f"Incorrect key provided: {self.api_key}", status_code=401)

    def _generate(self, messages, stop=None, run_manager=None, **kwargs: Any) -> ChatResult:
        self._refuse_unless_good()
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="pong"))])

    def _stream(self, messages, stop=None, run_manager=None, **kwargs: Any) -> Iterator[ChatGenerationChunk]:
        self._refuse_unless_good()
        yield ChatGenerationChunk(message=AIMessageChunk(content="po"))
        yield ChatGenerationChunk(message=AIMessageChunk(content="ng"))

    async def _astream(self, messages, stop=None, run_manager=None, **kwargs: Any) -> AsyncIterator[ChatGenerationChunk]:
        if self.hang:
            await asyncio.sleep(60)
        self._refuse_unless_good()
        yield ChatGenerationChunk(message=AIMessageChunk(content="po"))
        yield ChatGenerationChunk(message=AIMessageChunk(content="ng"))


class _KeylessChat(BaseChatModel):
    """A client that reads no key from what it is built with."""

    model: str

    @property
    def _llm_type(self) -> str:
        return "keyless"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs: Any) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="pong"))])


def _entry(key_reference: str = "$FAKE_PROVIDER_KEY", **extra: Any) -> dict[str, Any]:
    return {"name": "probe-model", "use": f"{__name__}:_KeyCheckingChat", "model": "probe-1", "api_key": key_reference, **extra}


def _catalog_variables() -> list[str]:
    return sorted({yaml.safe_load(path.read_text(encoding="utf-8"))["env"] for path in (PROFILE / "providers").rglob("*.yaml")})


@pytest.fixture
def environ(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """The Gateway's process environment as the profile starts it, with the base config rendered as run.sh renders it."""
    for name in _catalog_variables():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("HARTMESH_MODELS_FILE", raising=False)
    values = {
        "DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow",
        "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0",
        "HARTMESH_LOCAL_PASSWORDS": "allowed",
        "HARTMESH_LOCAL_REGISTRATION": "closed",
        PROFILE_DIR_ENV: str(PROFILE),
        WRAPPING_KEY_ENV: "w" * 44,
        "DEER_FLOW_CONFIG_PATH": str(tmp_path / "home" / "config.yaml"),
        "OPENAI_API_KEY": SEED,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_provider_keys_check", PROFILE / "gateway" / "render_config.py")
    renderer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = renderer
    spec.loader.exec_module(renderer)
    rendered, _ = renderer.render_text((PROFILE / "config.yaml").read_text(encoding="utf-8"), renderer.load_catalog(PROFILE / "providers"), os.environ)
    base = Path(os.environ["DEER_FLOW_CONFIG_PATH"])
    base.parent.mkdir(parents=True, exist_ok=True)
    base.write_text(rendered, encoding="utf-8")
    return os.environ


@pytest_asyncio.fixture
async def repository(tmp_path: Path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    path = tmp_path / "db" / "keys.db"
    path.parent.mkdir()
    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{path}", sqlite_dir=str(path.parent))
    try:
        yield ProviderKeyRepository(get_session_factory())
    finally:
        await close_engine()


class _RecordingProbe:
    def __init__(self, outcome: ProbeOutcome = ProbeOutcome("accepted")) -> None:
        self.outcome = outcome
        self.entries: list[dict[str, Any]] = []
        self.keys: list[str] = []

    async def __call__(self, entry, key: str) -> ProbeOutcome:
        self.entries.append(dict(entry))
        self.keys.append(key)
        return self.outcome


def _service(repository, environ, probe) -> ProviderKeyService:
    from app.gateway.provider_keys.profile import ProfileRenderer

    return ProviderKeyService(renderer=ProfileRenderer.from_environ(environ), repository=repository, environ=environ, probe=probe)


# ── the probe: the client a run would build, asked one question ──────────────


@pytest.mark.asyncio
async def test_a_key_the_provider_answers_is_accepted(environ) -> None:
    assert await probe_model(_entry(GOOD), GOOD) == ProbeOutcome("accepted")


@pytest.mark.asyncio
async def test_a_key_the_provider_refuses_is_rejected_and_its_text_goes_nowhere(environ, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        outcome = await probe_model(_entry(BAD), BAD)

    assert outcome == ProbeOutcome("rejected")
    # The provider quoted the key back in its refusal; neither the answer nor the log repeats it.
    assert BAD not in repr(outcome) and BAD not in caplog.text


@pytest.mark.asyncio
async def test_a_provider_that_does_not_answer_in_time_is_inconclusive(environ, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe_module, "PROBE_TIMEOUT_SECONDS", 0.2)

    assert await probe_model(_entry(GOOD, hang=True), GOOD) == ProbeOutcome("inconclusive", "timeout")


@pytest.mark.asyncio
async def test_the_probe_does_not_retry_a_refusal(environ, monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[BaseChatModel] = []
    build = probe_module._build_client
    monkeypatch.setattr(probe_module, "_build_client", lambda entry, key: built.append(build(entry, key)) or built[-1])

    await probe_model(_entry(GOOD), GOOD)

    assert built[0].max_retries == 0


@pytest.mark.asyncio
async def test_the_probe_leaves_the_loaded_configuration_as_it_was(environ) -> None:
    from deerflow.config.app_config import get_app_config

    before = [model.name for model in get_app_config().models]

    await probe_model(_entry(GOOD), GOOD)

    assert [model.name for model in get_app_config().models] == before
    assert "probe-model" not in before


@pytest.mark.parametrize("fragment", sorted((PROFILE / "providers" / "models").glob("*.yaml")), ids=lambda path: path.stem)
def test_the_probe_builds_a_client_for_every_model_provider_in_the_catalog(environ, fragment: Path) -> None:
    """Each provider's own class, through the factory, with the candidate key and no retries. No request is sent."""
    document = yaml.safe_load(fragment.read_text(encoding="utf-8"))
    entry = with_key(document["models"][0], document["env"], GOOD)
    before = dict(os.environ)

    client = probe_module._build_client(entry, GOOD)

    assert carries(client, GOOD), type(client).__name__
    assert client.max_retries == 0
    assert dict(os.environ) == before


def test_a_client_that_would_ask_with_another_key_is_not_asked(environ, monkeypatch: pytest.MonkeyPatch) -> None:
    """A provider whose SDK reads its key from the environment must not be
    tested with the key already there: the answer would be about that key."""
    monkeypatch.setenv("GEMINI_API_KEY", SEED)
    document = yaml.safe_load((PROFILE / "providers" / "models" / "30-gemini.yaml").read_text(encoding="utf-8"))

    client = probe_module._build_client(with_key(document["models"][0], document["env"], GOOD), GOOD)

    assert carries(client, GOOD) and not carries(client, SEED)


@pytest.mark.asyncio
async def test_a_client_that_cannot_be_given_the_key_is_inconclusive(environ) -> None:
    """The entry points the key at a field the class does not read, and the class takes no ``api_key`` either."""
    entry = {"name": "probe-model", "use": f"{__name__}:_KeylessChat", "model": "probe-1", "token": GOOD}

    assert await probe_model(entry, GOOD) == ProbeOutcome("inconclusive", "key_not_used")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_ProviderError("no", status_code=401), ProbeOutcome("rejected")),
        (_ProviderError("no", status_code=403), ProbeOutcome("rejected")),
        (_ProviderError("slow down", status_code=429), ProbeOutcome("inconclusive", "http_429")),
        (_ProviderError("bad request: API_KEY_INVALID", status_code=400), ProbeOutcome("rejected")),
        (_ProviderError("model not found", status_code=404), ProbeOutcome("inconclusive", "http_404")),
        (TimeoutError(), ProbeOutcome("inconclusive", "timeout")),
        (ConnectionError("name resolution failed"), ProbeOutcome("inconclusive", "no_answer")),
    ],
    ids=["401", "403", "429", "invalid-key-as-400", "404", "timeout", "no-network"],
)
def test_a_failure_is_read_for_what_it_says_about_the_key(error: BaseException, expected: ProbeOutcome) -> None:
    assert classify(error) == expected


def test_a_status_on_the_error_a_client_wrapped_is_still_read() -> None:
    try:
        try:
            raise _ProviderError("no", status_code=401)
        except _ProviderError as inner:
            raise RuntimeError("the client failed") from inner
    except RuntimeError as wrapped:
        assert classify(wrapped) == ProbeOutcome("rejected")


def test_only_the_providers_own_reference_is_replaced() -> None:
    entry = {"api_key": "$ACME_API_KEY", "base_url": "https://api.acme.invalid/$ACME_API_KEY", "other": "$OTHER_API_KEY", "nested": {"token": "$ACME_API_KEY"}, "list": ["$ACME_API_KEY", 3]}

    assert with_key(entry, "ACME_API_KEY", GOOD) == {"api_key": GOOD, "base_url": "https://api.acme.invalid/$ACME_API_KEY", "other": "$OTHER_API_KEY", "nested": {"token": GOOD}, "list": [GOOD, 3]}


# ── the service: which client, and what a test leaves behind ─────────────────


@pytest.mark.asyncio
async def test_a_model_provider_is_asked_with_the_catalogs_first_model_and_the_candidate_key(environ, repository) -> None:
    probe = _RecordingProbe()
    service = _service(repository, environ, probe)
    catalog_entry = yaml.safe_load((PROFILE / "providers" / "models" / "20-anthropic.yaml").read_text(encoding="utf-8"))["models"][0]

    answer = await service.check("anthropic", GOOD)

    assert answer == {"provider": "anthropic", "kind": "models", "result": "accepted", "reason": None, "model": catalog_entry["name"]}
    assert probe.entries == [{**catalog_entry, "api_key": GOOD}]
    assert probe.keys == [GOOD]


@pytest.mark.asyncio
async def test_a_test_stores_nothing_applies_nothing_and_does_not_answer_with_the_key(environ, repository, caplog: pytest.LogCaptureFixture) -> None:
    from deerflow.config.app_config import get_app_config

    service = _service(repository, environ, _RecordingProbe(ProbeOutcome("rejected")))
    base = Path(environ["DEER_FLOW_CONFIG_PATH"])
    before = (base.read_bytes(), dict(environ), [model.name for model in get_app_config().models])

    with caplog.at_level(logging.DEBUG):
        answer = await service.check("anthropic", BAD)

    assert answer["result"] == "rejected"
    assert BAD not in str(answer) and BAD not in caplog.text
    assert await repository.stored() == {} and await repository.events() == []
    assert (base.read_bytes(), dict(environ), [model.name for model in get_app_config().models]) == before
    assert not base.with_name("config.effective.yaml").exists()
    assert "ANTHROPIC_API_KEY" not in environ


@pytest.mark.asyncio
async def test_an_inconclusive_test_says_why_in_fixed_words(environ, repository) -> None:
    service = _service(repository, environ, _RecordingProbe(ProbeOutcome("inconclusive", "http_429")))

    answer = await service.check("openai", GOOD)

    assert (answer["result"], answer["reason"]) == ("inconclusive", "http_429")


@pytest.mark.asyncio
async def test_a_search_provider_is_reported_as_not_testable_without_being_called(environ, repository) -> None:
    probe = _RecordingProbe()
    service = _service(repository, environ, probe)

    answer = await service.check("tavily", GOOD)

    assert answer == {"provider": "tavily", "kind": "tools", "result": "not_testable", "reason": None, "model": None}
    assert probe.entries == []


@pytest.mark.asyncio
async def test_every_model_provider_in_the_catalog_has_a_model_to_ask(environ, repository) -> None:
    probe = _RecordingProbe()
    service = _service(repository, environ, probe)
    model_providers = [provider["provider"] for provider in (await service.status())["providers"] if provider["kind"] == "models"]

    for provider in model_providers:
        assert (await service.check(provider, GOOD))["result"] == "accepted", provider

    assert len(probe.entries) == len(model_providers) > 0
    # No entry reaches a client still carrying a reference in place of the key.
    for entry in probe.entries:
        assert GOOD in entry.values(), entry["name"]


@pytest.mark.asyncio
async def test_a_test_is_refused_for_an_unknown_provider_and_a_key_that_is_not_one_token(environ, repository) -> None:
    probe = _RecordingProbe()
    service = _service(repository, environ, probe)

    with pytest.raises(ProviderKeysRefused) as unknown:
        await service.check("acme", GOOD)
    with pytest.raises(ProviderKeysRefused) as invalid:
        await service.check("openai", "two words")

    assert (unknown.value.code, unknown.value.status) == ("unknown_provider", 404)
    assert (invalid.value.code, invalid.value.status) == ("key_invalid", 422)
    assert probe.entries == []


@pytest.mark.asyncio
async def test_a_test_is_refused_where_the_deployer_curates_the_models(environ, repository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _RecordingProbe()
    service = _service(repository, environ, probe)
    monkeypatch.setenv("HARTMESH_MODELS_FILE", str(tmp_path / "models.yaml"))

    with pytest.raises(ProviderKeysRefused) as refused:
        await service.check("openai", GOOD)

    assert refused.value.code == "operator_model_file"
    assert probe.entries == []


@pytest.mark.asyncio
async def test_a_key_can_be_tested_where_none_can_be_stored(environ, repository, monkeypatch: pytest.MonkeyPatch) -> None:
    """No wrapping key refuses a write, because a write stores; a test stores nothing."""
    monkeypatch.delenv(WRAPPING_KEY_ENV)
    service = _service(repository, environ, _RecordingProbe())

    assert (await service.check("openai", GOOD))["result"] == "accepted"

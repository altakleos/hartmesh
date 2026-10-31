"""The ``telegram`` key of ``GET /api/features`` fails closed."""

from types import SimpleNamespace

import pytest

from app.telegram_bots import router as module
from app.telegram_bots.config import TelegramBotsConfig

REASONS = {None, "off", "secret-missing", "secret-invalid", "host-changed", "polled-elsewhere", "misconfigured"}


def configured(monkeypatch, *, transcriber=None, section=True):
    monkeypatch.setattr(module, "section_present", lambda config: section)
    monkeypatch.setattr(module, "load_config", lambda config: TelegramBotsConfig(enabled=True, public_base_url="https://example.test"))
    monkeypatch.setattr(module, "resolve_transcriber", lambda settings, *, models: transcriber)
    return SimpleNamespace(models=[])


def app(reason=None, *, raises=False):
    def read():
        if raises:
            raise RuntimeError("no state")
        return reason

    return SimpleNamespace(state=SimpleNamespace(telegram_bots=SimpleNamespace(reason=read)))


@pytest.mark.parametrize("reason", sorted(REASONS, key=str))
def test_reason_vocabulary_closed(monkeypatch, reason):
    feature = module.telegram_feature(app(reason), configured(monkeypatch))
    assert feature.reason == reason and feature.reason in REASONS
    assert feature.bots == ("ready" if reason is None else "unavailable") and feature.enabled is True


@pytest.mark.parametrize(("transcriber", "voice"), [(None, "unavailable"), (SimpleNamespace(provider="openai"), "available")])
def test_voice_reported(monkeypatch, transcriber, voice):
    assert module.telegram_feature(app(), configured(monkeypatch, transcriber=transcriber)).voice == voice


@pytest.mark.parametrize(("transcriber", "provider"), [(None, None), (SimpleNamespace(provider="openai"), "openai"), (SimpleNamespace(provider="gemini"), "gemini")])
def test_voice_provider_reported(monkeypatch, transcriber, provider):
    assert module.telegram_feature(app(), configured(monkeypatch, transcriber=transcriber)).voice_provider == provider


def test_missing_key_means_unavailable(monkeypatch):
    assert module.telegram_feature(app(), configured(monkeypatch, section=False)) is None
    closed = {"enabled": False, "bots": "unavailable", "reason": None, "voice": "unavailable", "voice_provider": None}
    assert module.telegram_feature(SimpleNamespace(state=SimpleNamespace()), configured(monkeypatch)).model_dump() == closed, "No supervisor"
    assert module.telegram_feature(app(raises=True), configured(monkeypatch)).model_dump() == closed, "A supervisor whose call raises"

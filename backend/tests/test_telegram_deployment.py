"""Static refusal precedence is shared by future runtime/operator callers."""

import pytest

from app.telegram_bots import deployment
from app.telegram_bots.config import TelegramBotsConfig
from deerflow.config.app_config import AppConfig
from deerflow.config.channel_connections_config import ChannelConnectionsConfig


@pytest.fixture(autouse=True)
def authenticated_environment(monkeypatch):
    for key in ("DEER_FLOW_AUTH_DISABLED", "DEER_FLOW_ENV", "ENVIRONMENT"):
        monkeypatch.delenv(key, raising=False)


def app(*, enabled=True, bound=True):
    return AppConfig.model_construct(channel_connections=ChannelConnectionsConfig(enabled=enabled, require_bound_identity=bound))


@pytest.mark.parametrize(
    "enabled,auth,connections,bound,current,previous,expected",
    [
        (False, True, False, False, "short", "short", "off"),
        (True, True, False, False, "short", "short", "auth-disabled"),
        (True, False, False, True, "short", "short", "unbound-allowed"),
        (True, False, True, False, "short", "short", "unbound-allowed"),
        (True, False, True, True, None, "short", "secret-missing"),
        (True, False, True, True, "   ", "short", "secret-missing"),
        (True, False, True, True, "short", None, "secret-invalid"),
        (True, False, True, True, "x" * 32, "short", "secret-invalid"),
        (True, False, True, True, "x" * 32, None, None),
        (True, False, True, True, "  " + "x" * 32 + "  ", "y" * 32, None),
    ],
)
def test_static_cause_obeys_complete_precedence(monkeypatch, enabled, auth, connections, bound, current, previous, expected):
    if auth:
        monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    env = {}
    if current is not None:
        env["HARTMESH_PROVIDER_KEYS_SECRET"] = current
    if previous is not None:
        env["HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS"] = previous
    assert deployment.static_cause(TelegramBotsConfig(enabled=enabled), app(enabled=connections, bound=bound), env) == expected


def test_static_check_uses_existing_production_auth_semantics(monkeypatch):
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    monkeypatch.setenv("ENVIRONMENT", "production")
    assert deployment.static_cause(TelegramBotsConfig(), app(), {"HARTMESH_PROVIDER_KEYS_SECRET": "x" * 32}) is None


def test_missing_current_secret_wins_before_constructing_cipher(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("cipher must not be constructed without a current secret")

    monkeypatch.setattr(deployment, "token_cipher", unexpected)
    assert deployment.static_cause(TelegramBotsConfig(), app(), {"HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS": "short"}) == "secret-missing"


@pytest.mark.parametrize(
    "cause,reason",
    [
        (None, None),
        ("off", "off"),
        ("auth-disabled", "misconfigured"),
        ("unbound-allowed", "misconfigured"),
        ("secret-missing", "secret-missing"),
        ("secret-invalid", "secret-invalid"),
        ("host-changed", "host-changed"),
        ("polled-elsewhere", "polled-elsewhere"),
        ("channel-service-failed", "misconfigured"),
        ("gateway-not-ready", "misconfigured"),
    ],
)
def test_feature_reason_names_each_deployment_cause(cause, reason):
    assert deployment.feature_reason(cause) == reason


def test_unknown_cause_is_not_silently_reported_healthy():
    with pytest.raises(ValueError, match="invalid-deployment-cause"):
        deployment.feature_reason("unknown")

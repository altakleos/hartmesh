"""Shared static deployment checks; constructors start no service or network work."""

from collections.abc import Mapping

from app.gateway.auth_disabled import is_auth_disabled
from app.gateway.provider_keys.cipher import WrappingKeyInvalid
from app.telegram_bots.cipher import token_cipher
from app.telegram_bots.config import TelegramBotsConfig
from deerflow.config.app_config import AppConfig

_REASONS = {
    "off": "off",
    "auth-disabled": "misconfigured",
    "unbound-allowed": "misconfigured",
    "secret-missing": "secret-missing",
    "secret-invalid": "secret-invalid",
    "host-changed": "host-changed",
    "polled-elsewhere": "polled-elsewhere",
    "channel-service-failed": "misconfigured",
    "gateway-not-ready": "misconfigured",
}


def static_cause(config: TelegramBotsConfig, app_config: AppConfig, environ: Mapping[str, str]) -> str | None:
    if not config.enabled:
        return "off"
    if is_auth_disabled():
        return "auth-disabled"
    connections = app_config.channel_connections
    if not connections.enabled or not connections.require_bound_identity:
        return "unbound-allowed"
    if not environ.get("HARTMESH_PROVIDER_KEYS_SECRET", "").strip():
        return "secret-missing"
    try:
        token_cipher(environ)
    except WrappingKeyInvalid:
        return "secret-invalid"
    return None


def feature_reason(cause: str | None) -> str | None:
    if cause is None:
        return None
    if cause not in _REASONS:
        raise ValueError("invalid-deployment-cause")
    return _REASONS[cause]

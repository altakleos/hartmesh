"""Authentication configuration for DeerFlow."""

import logging
import os
import secrets

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_SECRET_FILE = ".jwt_secret"
TOKEN_EXPIRY_DAYS_ENV_VAR = "AUTH_TOKEN_EXPIRY_DAYS"
TOKEN_EXPIRY_DAYS_RANGE = (1, 30)


class AuthConfig(BaseModel):
    """JWT and auth-related configuration. Parsed once at startup.

    Note: the ``users`` table now lives in the shared persistence
    database managed by ``deerflow.persistence.engine``. The old
    ``users_db_path`` config key has been removed — user storage is
    configured through ``config.database`` like every other table.
    """

    jwt_secret: str = Field(
        ...,
        description="Secret key for JWT signing. MUST be set via AUTH_JWT_SECRET.",
    )
    token_expiry_days: int = Field(default=7, ge=1, le=30)
    oauth_github_client_id: str | None = Field(default=None)
    oauth_github_client_secret: str | None = Field(default=None)


_auth_config: AuthConfig | None = None


def token_expiry_days_from_environment() -> int | None:
    """The session lifetime the operator set, or None for the model default.

    Whole days within :data:`TOKEN_EXPIRY_DAYS_RANGE`, the bound the model
    has always carried. Anything else raises here, naming the variable and
    the rule and not the value, so a start can refuse it before a session
    is ever minted.
    """
    raw = os.environ.get(TOKEN_EXPIRY_DAYS_ENV_VAR, "").strip()
    if not raw:
        return None
    low, high = TOKEN_EXPIRY_DAYS_RANGE
    if not raw.isdigit() or not low <= int(raw) <= high:
        raise ValueError(f"{TOKEN_EXPIRY_DAYS_ENV_VAR} must be a whole number of days from {low} to {high} (or unset)")
    return int(raw)


def _load_or_create_secret() -> str:
    """Load persisted JWT secret from ``{base_dir}/.jwt_secret``, or generate and persist a new one."""
    from deerflow.config.paths import get_paths

    paths = get_paths()
    secret_file = paths.base_dir / _SECRET_FILE

    try:
        if secret_file.exists():
            secret = secret_file.read_text(encoding="utf-8").strip()
            if secret:
                return secret
    except OSError as exc:
        raise RuntimeError(f"Failed to read JWT secret from {secret_file}. Set AUTH_JWT_SECRET explicitly or fix DEER_FLOW_HOME/base directory permissions so DeerFlow can read its persisted auth secret.") from exc

    secret = secrets.token_urlsafe(32)
    try:
        secret_file.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(secret_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(secret)
    except OSError as exc:
        raise RuntimeError(f"Failed to persist JWT secret to {secret_file}. Set AUTH_JWT_SECRET explicitly or fix DEER_FLOW_HOME/base directory permissions so DeerFlow can store a stable auth secret.") from exc
    return secret


def get_auth_config() -> AuthConfig:
    """Get the global AuthConfig instance. Parses from env on first call."""
    global _auth_config
    if _auth_config is None:
        from dotenv import load_dotenv

        load_dotenv()
        jwt_secret = os.environ.get("AUTH_JWT_SECRET")
        if not jwt_secret:
            jwt_secret = _load_or_create_secret()
            os.environ["AUTH_JWT_SECRET"] = jwt_secret
            logger.warning(
                "⚠ AUTH_JWT_SECRET is not set — using an auto-generated secret "
                "persisted to .jwt_secret. Sessions will survive restarts. "
                "For production, add AUTH_JWT_SECRET to your .env file: "
                'python -c "import secrets; print(secrets.token_urlsafe(32))"'
            )
        expiry_days = token_expiry_days_from_environment()
        _auth_config = AuthConfig(jwt_secret=jwt_secret) if expiry_days is None else AuthConfig(jwt_secret=jwt_secret, token_expiry_days=expiry_days)
    return _auth_config


def set_auth_config(config: AuthConfig) -> None:
    """Set the global AuthConfig instance (for testing)."""
    global _auth_config
    _auth_config = config

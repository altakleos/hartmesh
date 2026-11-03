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
    """Load persisted JWT secret from ``{base_dir}/.jwt_secret``, or generate and persist a new one.

    The file is created exclusively and read back, so processes that share the
    runtime home and cold-start together (uvicorn workers, replicas on a shared
    volume) converge on one secret instead of each keeping its own.
    """
    from deerflow.config.paths import get_paths
    from deerflow.config.secret_file import read_or_create_secret_file

    paths = get_paths()
    secret_file = paths.base_dir / _SECRET_FILE

    try:
        return read_or_create_secret_file(secret_file, lambda: secrets.token_urlsafe(32))
    except OSError as exc:
        raise RuntimeError(f"Failed to read or persist the JWT secret at {secret_file}. Set AUTH_JWT_SECRET explicitly or fix DEER_FLOW_HOME/base directory permissions so DeerFlow can keep a stable auth secret.") from exc


def get_auth_config() -> AuthConfig:
    """Get the global AuthConfig instance. Parses from env on first call."""
    global _auth_config
    if _auth_config is None:
        from dotenv import load_dotenv

        from deerflow.env import load_selected_env_file

        if not load_selected_env_file():
            load_dotenv()
        jwt_secret = os.environ.get("AUTH_JWT_SECRET")
        if not jwt_secret:
            jwt_secret = _load_or_create_secret()
            os.environ["AUTH_JWT_SECRET"] = jwt_secret
            logger.warning(
                "⚠ AUTH_JWT_SECRET is not set — using an auto-generated secret "
                "persisted to .jwt_secret. Sessions will survive restarts. "
                "For production, add AUTH_JWT_SECRET to your dotenv file "
                "(the DEER_FLOW_ENV_FILE target, or .env by default): "
                'python -c "import secrets; print(secrets.token_urlsafe(32))"'
            )
        expiry_days = token_expiry_days_from_environment()
        _auth_config = AuthConfig(jwt_secret=jwt_secret) if expiry_days is None else AuthConfig(jwt_secret=jwt_secret, token_expiry_days=expiry_days)
    return _auth_config


def set_auth_config(config: AuthConfig) -> None:
    """Set the global AuthConfig instance (for testing)."""
    global _auth_config
    _auth_config = config

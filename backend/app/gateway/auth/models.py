"""User Pydantic models for authentication."""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, EmailStr, Field


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(UTC)


class User(BaseModel):
    """Internal user representation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid4, description="Primary key")
    email: EmailStr = Field(..., description="Unique email address")
    password_hash: str | None = Field(None, description="bcrypt hash, nullable for OAuth users")
    system_role: Literal["admin", "user"] = Field(default="user")
    created_at: datetime = Field(default_factory=_utc_now)

    # OAuth linkage (optional)
    oauth_provider: str | None = Field(None, description="e.g. 'github', 'google'")
    oauth_id: str | None = Field(None, description="User ID from OAuth provider")
    oauth_issuer: str | None = Field(None, description="Issuer URL the account was created under; None on accounts linked before it was recorded")

    # Auth lifecycle
    needs_setup: bool = Field(default=False, description="True when a reset account must complete setup")
    token_version: int = Field(default=0, description="Incremented on password change to invalidate old JWTs")
    last_sign_in_at: datetime | None = Field(None, description="When the account last signed in through its identity provider; None if never, or for a local account")
    email_released_from: str | None = Field(
        None, description="The address this account held before the deployer released it; None while its email is its own. Not an EmailStr: it records what was, and a legacy row may hold an address the validator would now refuse"
    )
    # Derived at every read from ``disabled_identities`` (the account's
    # issuer and subject have a row there); never stored on the user row.
    disabled_at: datetime | None = Field(None, description="When the deployer turned the account off; None while it is on")
    # Derived at every read from ``role_limits`` like ``disabled_at``; the
    # role above is already held at it. A plain string on purpose: a value
    # this model refused would raise on every read and lock the account out.
    role_limit: str | None = Field(None, description="The highest role the deployer lets this account's identity hold; None while no limit holds")


class UserResponse(BaseModel):
    """Response model for user info endpoint."""

    id: str
    email: str
    system_role: Literal["admin", "user"]
    needs_setup: bool = False
    oauth_provider: str | None = Field(None, description="OAuth/SSO provider ID if the user logged in via SSO (e.g. 'keycloak')")

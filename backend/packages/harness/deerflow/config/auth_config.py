"""OIDC / SSO authentication configuration models."""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


class OIDCProviderConfig(BaseModel):
    """Configuration for a single OIDC identity provider (Keycloak, Google, Azure AD, etc.)."""

    display_name: str = Field(description="Human-readable name shown on the login button")
    issuer: str = Field(description="OIDC issuer URL (e.g. https://keycloak.example.com/realms/deerflow)")
    client_id: str = Field(description="OAuth2 client ID assigned by the provider")
    client_secret: str | None = Field(default=None, description="OAuth2 client secret ($ENV_VAR references supported)")
    redirect_uri: str | None = Field(default=None, description="Callback URL the provider will redirect to after auth")
    scopes: list[str] = Field(
        default_factory=lambda: ["openid", "email", "profile"],
        description="OIDC scopes to request (must include openid)",
    )
    token_endpoint_auth_method: Literal["client_secret_post", "client_secret_basic", "none"] = Field(
        default="client_secret_post",
        description="How the client authenticates at the token endpoint",
    )

    # ── User provisioning ─────────────────────────────────────────────
    auto_create_users: bool = Field(
        default=True,
        description="Automatically create a DeerFlow user on first SSO login",
    )
    require_verified_email: bool = Field(
        default=True,
        description="Reject authentication if the provider does not report the email as verified",
    )
    allowed_email_domains: list[str] = Field(
        default_factory=list,
        description="If non-empty, only allow users whose email domain is in this list (e.g. ['example.com'])",
    )
    admin_emails: list[str] = Field(
        default_factory=list,
        description="Users with these email addresses are automatically granted the admin role on first login",
    )

    # ── PKCE / nonce ──────────────────────────────────────────────────
    pkce_enabled: bool = Field(default=True, description="Enable PKCE (S256) for the authorization code flow")
    nonce_enabled: bool = Field(default=True, description="Include and validate the nonce claim in ID tokens")

    # ── Endpoint overrides (for providers with non-standard discovery) ─
    authorization_endpoint: str | None = Field(default=None)
    token_endpoint: str | None = Field(default=None)
    userinfo_endpoint: str | None = Field(default=None)
    jwks_uri: str | None = Field(default=None)


class OIDCAuthConfig(BaseModel):
    """Top-level OIDC authentication configuration."""

    enabled: bool = Field(default=False, description="Enable OIDC SSO authentication")
    frontend_base_url: str | None = Field(
        default=None,
        description="Base URL of the frontend (used for callback redirects when behind a reverse proxy)",
    )
    providers: dict[str, OIDCProviderConfig] = Field(
        default_factory=dict,
        description="Map of provider IDs to their configuration (e.g. keycloak, google, azure)",
    )


class LocalAuthConfig(BaseModel):
    """Configuration for the built-in email/password authentication provider."""

    allow_registration: bool = Field(
        default=True,
        description=(
            "Allow visitors to self-register a local account via POST /api/v1/auth/register. "
            "Set to false when accounts are provisioned exclusively through SSO — the OIDC "
            "provisioning policy (allowed_email_domains, require_verified_email, auto_create_users) "
            "does not apply to local registration."
        ),
    )

    # ── Per-account lockout (the primary control) ─────────────────────
    account_max_attempts: int | None = Field(
        default=None,
        ge=2,
        description=(
            "Failed login attempts allowed against one account before that account is locked out "
            "of POST /api/v1/auth/login/local. The lock follows the submitted email address, not "
            "the client address, so a company whose staff share one office egress address never "
            "locks its colleagues out. Minimum 2: one failed attempt must never lock an account. "
            "Unset falls back to the deprecated max_login_attempts, then to 5."
        ),
    )
    account_lockout_seconds: float | None = Field(
        default=None,
        gt=0,
        allow_inf_nan=False,
        description=("Seconds an account stays locked after reaching account_max_attempts. Unset falls back to the deprecated lockout_seconds, then to 300 (5 minutes)."),
    )

    # ── Deprecated per-address aliases ────────────────────────────────
    max_login_attempts: int = Field(
        default=5,
        ge=2,
        description=(
            "DEPRECATED — renamed to account_max_attempts, and its dimension changed: it counted "
            "failures from one client address and now counts failures against one account. The "
            "number you set is carried over unchanged; an operator who raised it to survive a "
            "shared office address should lower it again, because the per-address problem it "
            "worked around no longer exists. Ignored when account_max_attempts is set."
        ),
    )
    lockout_seconds: float = Field(
        default=300.0,
        gt=0,
        allow_inf_nan=False,
        description=("DEPRECATED — renamed to account_lockout_seconds. The value is carried over unchanged. Ignored when account_lockout_seconds is set."),
    )

    # ── Per-source spray guard (the loose second limit) ───────────────
    source_window_seconds: float = Field(
        default=900.0,
        gt=0,
        allow_inf_nan=False,
        description="Rolling window over which one client address's failed logins are counted for the spray guard.",
    )
    source_max_distinct_accounts: int = Field(
        default=50,
        ge=2,
        description=(
            "Distinct accounts one client address may fail against within source_window_seconds "
            "before that address is locked out. Sized so a whole office never reaches it: it "
            "takes more accounts than a small or medium business has staff. This is the guard "
            "against untargeted spraying, which the per-account lock cannot see."
        ),
    )
    source_max_failures: int = Field(
        default=300,
        ge=2,
        description=("Total failed logins one client address may make within source_window_seconds before that address is locked out. Bounds sheer volume from one source, including repeated attempts against an already-locked account."),
    )
    source_lockout_seconds: float = Field(
        default=900.0,
        gt=0,
        allow_inf_nan=False,
        description="Seconds a client address stays locked after tripping either source limit.",
    )

    # ── Where the counters live ───────────────────────────────────────
    lockout_store: Literal["memory", "redis"] = Field(
        default="memory",
        description=(
            "Where login failure counters and lockouts are kept. 'memory' is per Gateway process: "
            "safe for a single-worker single-replica deployment, but the counters die with the "
            "process and cannot be cleared without a restart. 'redis' shares them across workers "
            "and replicas and survives a restart, and is required for the admin unlock to mean "
            "anything after a rollout. A Redis outage fails the login path closed (503)."
        ),
    )
    lockout_store_redis_url: str | None = Field(
        default=None,
        description=("Redis URL for lockout_store: redis. If omitted, DEER_FLOW_LOGIN_THROTTLE_REDIS_URL, DEER_FLOW_STREAM_BRIDGE_REDIS_URL, REDIS_URL, or redis://localhost:6379/0 is used."),
    )

    @property
    def effective_account_max_attempts(self) -> int:
        """The per-account threshold, honouring the deprecated alias."""
        return self.account_max_attempts if self.account_max_attempts is not None else self.max_login_attempts

    @property
    def effective_account_lockout_seconds(self) -> float:
        """The per-account lock duration, honouring the deprecated alias."""
        return self.account_lockout_seconds if self.account_lockout_seconds is not None else self.lockout_seconds

    @model_validator(mode="after")
    def _warn_on_deprecated_aliases(self) -> LocalAuthConfig:
        """Name every deprecated key an operator actually set, once per load.

        The rename is not cosmetic: ``max_login_attempts`` counted failures per
        client address and ``account_max_attempts`` counts them per account, so
        an operator who raised the old key to keep an office behind one NAT
        address working now has a *looser* per-account limit than the default.
        Carrying the number over silently would be exactly the "meaning changed
        under you" this warning exists to prevent.
        """
        deprecated = sorted(name for name in ("max_login_attempts", "lockout_seconds") if name in self.model_fields_set)
        if deprecated:
            replacement = {"max_login_attempts": "account_max_attempts", "lockout_seconds": "account_lockout_seconds"}
            pairs = ", ".join(f"auth.local.{name} -> auth.local.{replacement[name]}" for name in deprecated)
            logger.warning(
                "auth.local: %s is deprecated (%s). The value is still applied, but it now limits failures per ACCOUNT rather than per client address; review the number before it becomes the account policy.",
                " and ".join(f"auth.local.{name}" for name in deprecated),
                pairs,
            )
        return self


class AuthAppConfig(BaseModel):
    """Authentication configuration section for the DeerFlow app config."""

    oidc: OIDCAuthConfig = Field(default_factory=OIDCAuthConfig, description="OIDC SSO authentication settings")
    local: LocalAuthConfig = Field(default_factory=LocalAuthConfig, description="Built-in email/password authentication settings")

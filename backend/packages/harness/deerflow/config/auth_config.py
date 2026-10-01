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

    # ── Membership follows a claim ────────────────────────────────────
    access_claim: str | None = Field(
        default=None,
        description=(
            "The literal name of one claim (colons and dots are ordinary characters, not a "
            "path: 'urn:zitadel:iam:org:project:roles' is one name) that must carry one of "
            "access_values for a sign-in to be admitted. Read from the ID token, and from "
            "userinfo when the ID token does not carry it. Accepted as a list of strings, a "
            "single string, or an object whose keys are the values. Checked at every "
            "sign-in before an account is created or an existing one returned; a claim that "
            "is missing, empty or of another type refuses, and nothing falls back to "
            "allowed_email_domains. Unset, admission is as before."
        ),
    )
    access_values: list[str] = Field(
        default_factory=list,
        description="The values of access_claim that admit a sign-in. Required with access_claim, refused without it.",
    )
    access_roles: dict[str, Literal["admin", "user"]] = Field(
        default_factory=dict,
        description=(
            "Optional: which product role each admitting value carries. When set it covers "
            "every access value exactly, the role is re-read at every sign-in and written to "
            "the account in both directions ('admin' wins when a token carries several), and "
            "admin_emails plays no part (setting both is refused). Unset, roles come from "
            "admin_emails, read at account creation and again whenever a sign-in changes the "
            "account's address -- that list is keyed by address, so a role read from an address "
            "the account no longer holds would be the wrong one."
        ),
    )

    @model_validator(mode="after")
    def _access_claim_is_whole(self) -> OIDCProviderConfig:
        """Refuse a half-configured admission rule rather than admit by accident.

        Every refusal here names what is missing: a claim with no values would
        admit nobody, values with no claim would check nothing, and a role
        mapping that misses an admitting value would hand that value an
        unspecified role.
        """
        claim = (self.access_claim or "").strip()
        values = [value.strip() for value in self.access_values]
        if any(not value for value in values):
            raise ValueError("access_values must not contain empty entries")
        if len(set(values)) != len(values):
            raise ValueError("access_values must not repeat a value")
        if self.access_claim is not None and not claim:
            raise ValueError("access_claim must not be blank")
        if claim and not values:
            raise ValueError(f"access_claim is set ({claim!r}) but access_values is empty: no value would admit anyone. Name the admitting values, or unset the claim")
        if values and not claim:
            raise ValueError("access_values is set but access_claim is not: there is no claim to look the values up in. Name the claim, or unset the values")
        self.access_claim = claim or None
        self.access_values = values
        if self.access_roles:
            if not claim:
                raise ValueError("access_roles is set but access_claim is not: a role mapping needs the admission claim it maps. Set access_claim and access_values, or unset the mapping")
            if self.admin_emails:
                raise ValueError("access_roles and admin_emails are both set: roles come from the claim or from the email list, not both. Unset one")
            unmapped = sorted(set(values) - set(self.access_roles))
            if unmapped:
                raise ValueError(f"access_roles gives no role to admitting value(s) {', '.join(unmapped)}: every value that admits must carry a role")
            stray = sorted(set(self.access_roles) - set(values))
            if stray:
                raise ValueError(f"access_roles maps value(s) {', '.join(stray)} that are not in access_values: a role can only follow a value that admits")
        return self

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
    clock_skew_leeway_seconds: float = Field(
        default=60.0,
        ge=0,
        le=300,
        allow_inf_nan=False,
        description=(
            "How far this Gateway's clock may disagree with the identity provider's before an "
            "ID token is refused, applied to iat, nbf and exp. The product used to allow none, "
            "so a VM a second or two behind the provider between NTP polls refused every sign-in "
            "by everyone -- the ordinary state of a guest after a hypervisor snapshot, and a "
            "whole-company outage showing only a generic sso_failed. It is one number rather "
            "than one per provider because it describes this host's clock, not any provider's. "
            "Bounded at 300: beyond a few minutes it stops being a clock tolerance and starts "
            "accepting tokens that have genuinely expired. 0 restores the old no-tolerance "
            "behaviour."
        ),
    )


class LocalAuthConfig(BaseModel):
    """Configuration for the built-in email/password authentication provider."""

    enabled: bool = Field(
        default=True,
        description=(
            "Whether local passwords are a way in at all. False is sign-on-only mode: people "
            "sign in through an enabled OIDC provider and nothing else. Local login, "
            "registration, first-admin initialization and password change all refuse, whatever "
            "the admin count; an account without a provider identity is inert (no session, no "
            "personal access token); setup-status tells an unauthenticated caller nothing about "
            "bootstrap; the reset_admin command refuses; DEER_FLOW_AUTH_DISABLED refuses the "
            "start. Requires auth.oidc.enabled with at least one provider, or the config is "
            "refused: a deployment nobody can enter is a mistake to name, not a mode."
        ),
    )
    allow_registration: bool = Field(
        default=True,
        description=(
            "Allow visitors to self-register a local account via POST /api/v1/auth/register. "
            "Set to false when accounts are provisioned exclusively through SSO — the OIDC "
            "provisioning policy (allowed_email_domains, require_verified_email, auto_create_users) "
            "does not apply to local registration — or when an administrator should decide who has "
            "an account: an administrator adds a person either way (POST /api/v1/auth/users, or "
            "`python -m app.gateway.auth.add_user`). /health reports it as registration: open | closed."
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
        description=(
            "Window over which one client address's failed logins are counted for the spray "
            "guard. It is a fixed window, not a sliding one: it opens on that address's first "
            "counted failure and closes source_window_seconds later, after which the next "
            "failure opens a new one. (In the redis backend the counter simply carries this "
            "TTL from its first increment.) Counts are not decayed inside an open window."
        ),
    )
    source_max_distinct_accounts: int = Field(
        default=50,
        ge=2,
        description=(
            "Distinct accounts one client address may fail against within source_window_seconds "
            "before that address is locked out. This is the guard against untargeted spraying, "
            "which the per-account lock cannot see. The default is above the staff count of a "
            "small or medium business, but it counts *submitted* addresses, so mistyped ones "
            "count as distinct too -- it is a headroom figure, not a promise that a legitimate "
            "office can never reach it."
        ),
    )
    source_max_failures: int = Field(
        default=300,
        ge=2,
        description=(
            "Total failed logins one client address may make within source_window_seconds before "
            "that address is locked out. Bounds sheer volume from one source, including repeated "
            "attempts against an already-locked account -- which a person has no cue to stop "
            "making, because a locked account answers exactly as a wrong password does. Size it "
            "against the shape behind the address: N staff who each reach their own account lock "
            "and then retry cost N x (account_max_attempts + retries). Raising it after an "
            "address is already locked does not release it; the lock runs out or an "
            "administrator clears it."
        ),
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

    @model_validator(mode="after")
    def _sign_on_only_needs_a_provider(self) -> AuthAppConfig:
        """Refuse a configuration under which nobody could ever sign in.

        Sign-on-only mode closes every local door, so the identity provider
        is the one way in; a config that closes the doors and names no
        provider is refused at load rather than started as an empty tenant.
        """
        if self.local.enabled or (self.oidc.enabled and self.oidc.providers):
            return self
        raise ValueError("auth.local.enabled is false but auth.oidc has no enabled provider: nobody could sign in. Enable auth.oidc with at least one provider, or leave local passwords on.")

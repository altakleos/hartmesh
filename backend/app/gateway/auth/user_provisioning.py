"""User provisioning for OIDC logins.

Handles the logic of finding existing users, auto-creating new ones, and
enforcing email domain restrictions. A pre-existing local account is never
auto-linked to an OIDC identity: an email collision blocks the SSO login with
a 409 instead, so an SSO login can never seize a local password account.

Two checks come before anything else at every sign-in, first or not: an
identity the deployer turned off is refused, and, where an admission claim
is configured, a token that does not carry an admitting value is refused,
with no account created and none returned.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status

from app.gateway.auth.access import admitted_values, refuse_turned_off, role_for
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.oidc import OIDCIdentity
from deerflow.config.auth_config import OIDCProviderConfig

logger = logging.getLogger(__name__)


async def get_or_provision_oidc_user(
    provider_id: str,
    provider_config: OIDCProviderConfig,
    identity: OIDCIdentity,
    local_provider: LocalAuthProvider,
) -> dict:
    """Resolve an OIDC identity to a DeerFlow user.

    Flow:
    0. Refuse an identity the deployer turned off, and (with an admission
       claim configured) a token without an admitting value
    1. Look up existing user by (provider, subject); re-read its role when
       the role mapping is set; stamp the sign-in
    2. If not found, enforce domain/email-verified rules
    3. Block if a local account already owns the email (never auto-link)
    4. Auto-create if enabled

    Returns a dict with ``user`` (the User model instance) and ``created`` (bool).
    """
    # 0. Before any account is created or returned.
    if await local_provider.is_identity_disabled(provider_config.issuer, identity.subject):
        raise refuse_turned_off(provider_config, identity)
    admitted = admitted_values(provider_config, identity)
    now = datetime.now(UTC)

    # 1. Existing OAuth link, pinned to the issuer that created it. The lookup
    # key is (provider name, subject); the issuer is what stops a provider
    # name pointed at a new issuer from handing this account to whoever holds
    # the same subject there. A row linked before the issuer was recorded
    # carries None and adopts the configured issuer now: nothing else could
    # ever know which issuer it belonged to.
    existing = await local_provider.get_user_by_oauth(provider_id, identity.subject)
    if existing:
        recorded = getattr(existing, "oauth_issuer", None)
        if recorded is None:
            existing.oauth_issuer = provider_config.issuer
        elif _issuer_key(recorded) != _issuer_key(provider_config.issuer):
            logger.warning("OIDC sign-in refused: the subject under provider %s is linked to issuer %s, and the provider is configured for %s", provider_id, recorded, provider_config.issuer)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your account is linked to a different identity provider. Contact your administrator.",
            )
        if provider_config.access_roles:
            # The role follows the claim at every sign-in, in both directions.
            role = role_for(provider_config, admitted, existing.email)
            if role != existing.system_role:
                logger.info("OIDC sign-in: role of subject %s at issuer %s is now %s (was %s)", identity.subject, provider_config.issuer, role, existing.system_role)
                existing.system_role = role  # type: ignore[assignment]
        existing.last_sign_in_at = now
        # A targeted write: never the whole row, so a token_version read a
        # moment ago cannot land back over an end-sessions that ran meanwhile.
        await local_provider.record_sign_in(existing)
        return {"user": existing, "created": False}

    # 2. Verified email requirement
    if provider_config.require_verified_email and not identity.email_verified:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=("Your email could not be verified by the identity provider. Please contact your administrator."),
        )

    if not identity.email:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The identity provider did not provide an email address.",
        )

    email = identity.email.lower()

    # 3. Domain restriction
    if provider_config.allowed_email_domains:
        domain = email.rsplit("@", 1)[-1]
        if domain not in {d.lower().lstrip("@") for d in provider_config.allowed_email_domains}:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your email domain is not allowed. Please use an approved email address.",
            )

    # 4. Block if a local account already owns this email. We never auto-link an
    # SSO identity onto a pre-existing local account, since that would let an SSO
    # login take over a password account that happens to share the email.
    local_user = await local_provider.get_user_by_email(email)

    if local_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=("An account with this email already exists. Contact your administrator to link it to your SSO account."),
        )

    # 5. Auto-create
    if not provider_config.auto_create_users:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Automatic account creation is disabled. Contact your administrator.",
        )

    role = role_for(provider_config, admitted, email)
    try:
        user = await local_provider.create_oauth_user(
            email=email,
            oauth_provider=provider_id,
            oauth_id=identity.subject,
            system_role=role,
            oauth_issuer=provider_config.issuer,
            last_sign_in_at=now,
        )
    except ValueError:
        # Lost a race: a concurrent callback (double-click, replayed code) already
        # inserted a row that collides on the unique index. Re-resolve instead of
        # bubbling a raw 500. If the winner created this same identity, return it;
        # otherwise the email now belongs to a different account → 409.
        existing = await local_provider.get_user_by_oauth(provider_id, identity.subject)
        if existing:
            return {"user": existing, "created": False}
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=("An account with this email already exists. Contact your administrator to link it to your SSO account."),
        ) from None
    logger.info("Auto-created OIDC user %s (provider=%s, role=%s)", email, provider_id, role)
    return {"user": user, "created": True}


def _issuer_key(issuer: str) -> str:
    """Two spellings of one issuer: discovery already treats a trailing slash as the same address."""
    return issuer.strip().rstrip("/")


def _resolve_role(email: str, admin_emails: list[str]) -> str:
    """Return ``admin`` if the email is in the admin list, otherwise ``user``."""
    email_lower = email.lower()
    return "admin" if any(e.lower() == email_lower for e in admin_emails) else "user"

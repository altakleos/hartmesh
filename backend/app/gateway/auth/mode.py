"""Which way people sign in: local passwords, or the identity provider only.

One fact, ``auth.local.enabled``, selects the mode; every local-password
door reads it here and nowhere keeps a second copy. ``False`` is
sign-on-only: the enabled OIDC provider is the one way in, whatever the
admin count and whatever accounts the database holds.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status

from app.gateway.auth.errors import AuthErrorCode, AuthErrorResponse

AUTH_MODE_LOCAL = "local"
AUTH_MODE_SIGN_ON_ONLY = "sign_on_only"


def sign_on_only() -> bool:
    """Whether local passwords are switched off, read from the live config.

    Only ``FileNotFoundError`` falls back, and to local mode: a bare app
    without a ``config.yaml`` is today's local-password deployment. A
    malformed config propagates, so a deployment that switched local
    passwords off never silently gets them back.
    """
    from deerflow.config.app_config import get_app_config

    try:
        return get_app_config().auth.local.enabled is False
    except FileNotFoundError:
        return False


def auth_mode() -> str:
    return AUTH_MODE_SIGN_ON_ONLY if sign_on_only() else AUTH_MODE_LOCAL


def is_provider_account(user: Any) -> bool:
    """Whether the account was created by an identity provider's assertion.

    In sign-on-only mode this is the one shape of account that may hold a
    session: a row with a local password and no provider identity is inert,
    however it got there (a restore, a switch after the fact).
    """
    return getattr(user, "oauth_provider", None) is not None


def sign_on_required(status_code: int = status.HTTP_403_FORBIDDEN) -> HTTPException:
    """The one answer every closed local door gives; it names no account."""
    return HTTPException(
        status_code=status_code,
        detail=AuthErrorResponse(
            code=AuthErrorCode.SIGN_ON_REQUIRED,
            message="This deployment signs people in through its identity provider; local passwords are not used.",
        ).model_dump(),
    )


ACCOUNT_INERT = "inert"
ACCOUNT_DISABLED = "disabled"


def access_turned_off(status_code: int = status.HTTP_401_UNAUTHORIZED) -> HTTPException:
    """What a credential of an account the deployer turned off gets."""
    return HTTPException(
        status_code=status_code,
        detail=AuthErrorResponse(
            code=AuthErrorCode.ACCOUNT_DISABLED,
            message="Your access to this workspace has been turned off. Ask your administrator.",
        ).model_dump(),
    )


def account_refusal(user: Any) -> str | None:
    """Why nothing may act for this account right now, or ``None``.

    ``disabled``: the deployer turned the identity off (``user.disabled_at``,
    derived at every read from ``disabled_identities``), in either mode.
    ``inert``: sign-on only, and the account has no provider identity.
    """
    if getattr(user, "disabled_at", None) is not None:
        return ACCOUNT_DISABLED
    if sign_on_only() and not is_provider_account(user):
        return ACCOUNT_INERT
    return None


def refusal_response(refusal: str, status_code: int = status.HTTP_401_UNAUTHORIZED) -> HTTPException:
    return access_turned_off(status_code) if refusal == ACCOUNT_DISABLED else sign_on_required(status_code)


def require_live_account(user: Any) -> None:
    """Refuse an account nothing may act for, wherever a credential resolves to one.

    Every path that turns a credential into a user row -- the auth
    middleware, the browser WebSocket, the LangGraph auth hook -- ends here,
    so the inert-account and disabled-account rules have one home.
    """
    refusal = account_refusal(user)
    if refusal is not None:
        raise refusal_response(refusal)


async def owner_is_refused(owner_user_id: str) -> str | None:
    """Why an internal caller may not act for the owner its header names, or ``None``.

    An IM connection bound while its owner held a session would otherwise
    keep running turns as that owner after the owner was turned off, or
    after the switch to sign-on only made a local owner inert. An owner id
    with no row (an unbound channel's own id) is not an account and is left
    alone; a bare app with no users table cannot answer and refuses nothing.
    """
    from app.gateway.deps import get_local_provider

    try:
        owner = await get_local_provider().get_user(owner_user_id)
    except RuntimeError:
        return None
    if owner is None:
        return None
    return account_refusal(owner)

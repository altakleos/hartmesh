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


REGISTRATION_OPEN = "open"
REGISTRATION_CLOSED = "closed"


def local_registration_open() -> bool:
    """Whether a visitor may create their own local account, read from the live config.

    ``auth.local.allow_registration`` in local mode, and never in sign-on-only
    mode, where nobody creates a local account. Only ``FileNotFoundError``
    falls back, and to open: registration was unconditionally open before the
    setting existed, and a bare app without a ``config.yaml`` is that
    deployment. A malformed config propagates, so a closed deployment never
    silently reopens.
    """
    from deerflow.config.app_config import get_app_config

    try:
        local = get_app_config().auth.local
    except FileNotFoundError:
        return True
    return local.enabled is not False and local.allow_registration


def registration_state() -> str:
    """``open`` or ``closed``: the readiness signal's name for :func:`local_registration_open`."""
    return REGISTRATION_OPEN if local_registration_open() else REGISTRATION_CLOSED


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
# What a session of an account whose setup is pending may call: who it is,
# and completing the setup. ``/logout``, ``/login/local`` and
# ``/setup-status`` are public and need no account at all.
SETUP_ROUTES = frozenset({"/api/v1/auth/me", "/api/v1/auth/change-password"})


def access_turned_off(status_code: int = status.HTTP_401_UNAUTHORIZED) -> HTTPException:
    """What a credential of an account the deployer turned off gets."""
    return HTTPException(
        status_code=status_code,
        detail=AuthErrorResponse(
            code=AuthErrorCode.ACCOUNT_DISABLED,
            message="Your access to this workspace has been turned off. Ask your administrator.",
        ).model_dump(),
    )


def setup_required() -> HTTPException:
    """What a session of an account whose setup is pending gets, outside the setup routes."""
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=AuthErrorResponse(
            code=AuthErrorCode.SETUP_REQUIRED,
            message="Finish setting up your account first: choose your own password.",
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


def require_live_account(user: Any, *, completing_setup: bool = False) -> None:
    """Refuse a session nothing may act for, wherever a session cookie resolves to an account.

    Every path that turns a session into a user row -- the auth middleware,
    the browser WebSocket, the LangGraph auth hook -- ends here, so the
    inert-account, disabled-account and pending-setup rules have one home.

    A pending setup (``user.needs_setup``: the password was set for the
    person, by an administrator adding them or by ``reset_admin``) confines
    the session to :data:`SETUP_ROUTES`; ``completing_setup`` is true only
    for a request to one of them. The rule binds sessions and nothing else,
    because a session is the only credential a set password produces: an
    added account holds no token, channel or schedule and cannot create one
    before setup, and a reset account's tokens were never exposed by the
    reset, so its automation keeps running. The answer is 403, not 401: the
    credential is good, and a 401 would send the page to sign in again
    instead of to setup.
    """
    refusal = account_refusal(user)
    if refusal is not None:
        raise refusal_response(refusal)
    if getattr(user, "needs_setup", False) and not completing_setup:
        raise setup_required()


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

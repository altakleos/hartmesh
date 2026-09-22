"""Typed error definitions for auth module.

AuthErrorCode: exhaustive enum of all auth failure conditions.
TokenError: exhaustive enum of JWT decode failures.
AuthErrorResponse: structured error payload for HTTP responses.
"""

from enum import StrEnum

from pydantic import BaseModel


class AuthErrorCode(StrEnum):
    """Exhaustive list of auth error conditions."""

    INVALID_CREDENTIALS = "invalid_credentials"
    TOKEN_EXPIRED = "token_expired"
    TOKEN_INVALID = "token_invalid"
    USER_NOT_FOUND = "user_not_found"
    EMAIL_ALREADY_EXISTS = "email_already_exists"
    PROVIDER_NOT_FOUND = "provider_not_found"
    NOT_AUTHENTICATED = "not_authenticated"
    SYSTEM_ALREADY_INITIALIZED = "system_already_initialized"
    REGISTRATION_DISABLED = "registration_disabled"
    # Sign-on-only mode (auth.local.enabled: false): the deployment signs
    # people in through its identity provider and local passwords are no
    # way in -- not for a new account, not for one restored from before.
    SIGN_ON_REQUIRED = "sign_on_required"
    # The deployer turned the account off (``disabled_identities``); every
    # credential that resolves to it is refused until it is turned on again.
    ACCOUNT_DISABLED = "account_disabled"
    # The account's first password was set for it (``reset_admin``, or an
    # administrator adding the person): until the person chooses their own,
    # the one thing any credential of it may do is complete that setup.
    SETUP_REQUIRED = "setup_required"


class TokenError(StrEnum):
    """Exhaustive list of JWT decode failure reasons."""

    EXPIRED = "expired"
    INVALID_SIGNATURE = "invalid_signature"
    MALFORMED = "malformed"


class AuthErrorResponse(BaseModel):
    """Structured error response — replaces bare `detail` strings."""

    code: AuthErrorCode
    message: str


def token_error_to_code(err: TokenError) -> AuthErrorCode:
    """Map TokenError to AuthErrorCode — single source of truth."""
    if err == TokenError.EXPIRED:
        return AuthErrorCode.TOKEN_EXPIRED
    return AuthErrorCode.TOKEN_INVALID

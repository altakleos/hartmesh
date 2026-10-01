"""An administrator adds a local-password account for someone.

One operation behind two surfaces: ``POST /api/v1/auth/users`` for an
administrator signed in to the product, and ``python -m
app.gateway.auth.add_user`` for a deployer with a shell in the container
and no browser session. With self-registration closed this is the only way
an ordinary account comes into existence in local-password mode, so it works
whether registration is open or closed. It never works in sign-on-only
mode, where the identity provider decides who has an account.

How the first credential reaches the person: the operation sets a random
one-time password and marks the account ``needs_setup``. The password is
returned once, to the caller that created it, and only its hash is stored,
so it cannot be recovered afterwards and is never logged. Until the person
signs in with it and chooses their own password (the existing first-sign-in
setup: ``/setup`` in the product, ``POST /api/v1/auth/change-password``
behind it), every session of the account is refused everywhere but the
setup routes (``app.gateway.auth.mode.require_live_account``); it holds no
token, channel or schedule, and cannot create one. Choosing a password
replaces the one-time password, which from then on opens nothing.

It stays usable for setup until that happens, rather than dying at its first
sign-in: nothing in the product can issue a second one for an existing
account (only the deployer's ``reset_admin --email`` can), so a person who
signed in and closed the tab before choosing a password would otherwise hold
an account only a container command could reopen.

The role is always ``user``. Creating an administrator this way is not
offered.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field

from app.gateway.auth.errors import AuthErrorCode
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.mode import sign_on_only
from app.gateway.auth.models import User

ADDED_ROLE = "user"

#: 16 random bytes, URL-safe: 22 characters, the same strength ``reset_admin``
#: gives a reset password. Well past the 8-character minimum and not on any
#: common-password list, so the change-password form's own rules never refuse
#: it as the current password.
ONE_TIME_PASSWORD_BYTES = 16


@dataclass(frozen=True)
class AddedAccount:
    """The new account and the one-time password that opens its setup; the only copy of it."""

    user: User
    one_time_password: str = field(repr=False)


class AddAccountRefused(Exception):
    """The operation created nothing; ``code`` is the stable reason both surfaces report."""

    def __init__(self, code: AuthErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


SIGN_ON_ONLY_MESSAGE = "This deployment signs people in through its identity provider, which decides who has an account; local accounts are not added here."
EMAIL_TAKEN_MESSAGE = "Email already registered"


def refuse_unless_local_passwords() -> None:
    """Refuse in sign-on-only mode, before anything is read or written."""
    if sign_on_only():
        raise AddAccountRefused(AuthErrorCode.SIGN_ON_REQUIRED, SIGN_ON_ONLY_MESSAGE)


async def add_local_account(provider: LocalAuthProvider, email: str) -> AddedAccount:
    """Create a ``user`` account for ``email`` whose first sign-in must choose a password.

    ``email`` has already been validated as an address by the caller (the
    route's ``EmailStr``, the command's parser), exactly as ``/register``
    validates it. An address any account already holds is refused with
    ``email_already_exists``, the answer ``/register`` gives.
    """
    refuse_unless_local_passwords()
    one_time_password = secrets.token_urlsafe(ONE_TIME_PASSWORD_BYTES)
    try:
        user = await provider.create_user(email=email, password=one_time_password, system_role=ADDED_ROLE, needs_setup=True)
    except ValueError:
        raise AddAccountRefused(AuthErrorCode.EMAIL_ALREADY_EXISTS, EMAIL_TAKEN_MESSAGE) from None
    return AddedAccount(user=user, one_time_password=one_time_password)

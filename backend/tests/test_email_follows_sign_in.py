"""The address an account holds follows the sign-in that carries it.

A person's subject at the provider is permanent; their address is something
the provider says about them, and a company that corrects it there must not
have to correct it here too. So at every sign-in of a linked account, the
account's address becomes the token's -- unless one of four things says
otherwise, each of which still signs the person in, because an attribute of
theirs is not grounds to turn them away:

- the token carries no usable address;
- the deployment requires a verified address and this one is not;
- no account record could hold it (``record_sign_in`` writes with an UPDATE,
  not through the model, so storing one would leave a row that raises on
  every later read of that account);
- another account already holds it, which is the deployer's to resolve.

The other half is what a *new* subject meets when an old account still holds
its address. Which account holds it decides what the person is told: their
own password account is theirs to sign in with, a provider account of
another subject is not.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import HTTPException

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-email-follows-min-32")

from app.gateway.auth.config import AuthConfig, set_auth_config
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.oidc import OIDCIdentity
from app.gateway.auth.user_provisioning import (
    EMAIL_TAKEN_CODE,
    EMAIL_TAKEN_MESSAGE,
    EMAIL_UNUSABLE_CODE,
    get_or_provision_oidc_user,
)
from deerflow.config.auth_config import OIDCProviderConfig

_TEST_SECRET = "test-secret-key-email-follows-min-32"
_ISSUER = "https://id.example.com/realms/co"
_PROVIDER = OIDCProviderConfig(display_name="Company", issuer=_ISSUER, client_id="hartmesh", client_secret="FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")


def _identity(**overrides) -> OIDCIdentity:
    values = {"provider": "sso", "subject": "sub-pat", "email": "pat@example.com", "email_verified": True, "name": "Pat", "claims": {}}
    values.update(overrides)
    return OIDCIdentity(**values)


@pytest.fixture
def users_db(tmp_path: Path) -> Iterator[Path]:
    """A fresh, empty users table; yields the database file."""
    from app.gateway import deps
    from deerflow.persistence.engine import close_engine, init_engine

    database = tmp_path / "users.db"
    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{database}", sqlite_dir=str(tmp_path)))
    deps._cached_local_provider = None
    deps._cached_repo = None
    try:
        yield database
    finally:
        deps._cached_local_provider = None
        deps._cached_repo = None
        asyncio.run(close_engine())


def _provider() -> LocalAuthProvider:
    from app.gateway.deps import get_local_provider

    return get_local_provider()


def _sign_in(identity: OIDCIdentity, *, provider_config: OIDCProviderConfig = _PROVIDER) -> dict:
    return asyncio.run(get_or_provision_oidc_user(provider_id="sso", provider_config=provider_config, identity=identity, local_provider=_provider()))


def _stored_email(database: Path, subject: str) -> str:
    with sqlite3.connect(database) as connection:
        return connection.execute("SELECT email FROM users WHERE oauth_id = ?", (subject,)).fetchone()[0]


# ── 1. The address follows ──────────────────────────────────────────────


def test_a_changed_verified_address_becomes_the_accounts(users_db: Path) -> None:
    created = _sign_in(_identity())
    assert created["created"] is True and created["user"].email == "pat@example.com"

    again = _sign_in(_identity(email="pat.chen@example.com"))

    assert again["created"] is False, "the same subject is the same account"
    assert again["user"].email == "pat.chen@example.com"
    assert _stored_email(users_db, "sub-pat") == "pat.chen@example.com", "and it was written, not only returned"


def test_a_change_of_case_alone_is_not_a_change(users_db: Path) -> None:
    """Compared the way the unique index compares, so an account never conflicts with itself."""
    _sign_in(_identity())
    again = _sign_in(_identity(email="PAT@Example.COM"))
    assert again["user"].email == "pat@example.com"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"


def test_an_unverified_change_leaves_the_address_as_it_was(users_db: Path) -> None:
    _sign_in(_identity())

    again = _sign_in(_identity(email="someone.else@example.com", email_verified=False))

    assert again["user"].email == "pat@example.com"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"


def test_an_unverified_change_does_follow_where_the_deployment_does_not_require_verification(users_db: Path) -> None:
    """The same rule as account creation, not a stricter one of its own."""
    lenient = OIDCProviderConfig(display_name="Company", issuer=_ISSUER, client_id="hartmesh", require_verified_email=False)
    _sign_in(_identity(), provider_config=lenient)

    again = _sign_in(_identity(email="pat.chen@example.com", email_verified=False), provider_config=lenient)

    assert again["user"].email == "pat.chen@example.com"


def test_a_token_with_no_address_leaves_the_one_the_account_has(users_db: Path) -> None:
    _sign_in(_identity())
    for carried in (None, "", "   "):
        again = _sign_in(_identity(email=carried))
        assert again["user"].email == "pat@example.com", carried
        assert _stored_email(users_db, "sub-pat") == "pat@example.com"


def test_an_address_no_record_can_hold_leaves_the_one_the_account_has(users_db: Path) -> None:
    """Without this the sign-in would write a row that raises on every later read.

    ``record_sign_in`` is a targeted UPDATE, so nothing between here and the
    column would refuse an address the model refuses -- the account would be
    unreadable, and unreachable, from the next request on.
    """
    _sign_in(_identity())

    again = _sign_in(_identity(email="pat@company.local"))

    assert again["user"].email == "pat@example.com"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"
    # The proof that matters: the account still reads back.
    assert asyncio.run(_provider().get_user_by_oauth("sso", "sub-pat")) is not None


def test_a_non_string_address_claim_leaves_the_account_alone_and_does_not_raise(users_db: Path) -> None:
    """A directory attribute mapped as multivalued arrives as a list."""
    _sign_in(_identity())

    again = _sign_in(_identity(email=["pat.chen@example.com"]))

    assert again["user"].email == "pat@example.com"


def test_the_role_follows_the_address_the_sign_in_settles_on(users_db: Path) -> None:
    """Without the claim mapping the administrators' list decides, and it is keyed by address."""
    config = OIDCProviderConfig(display_name="Company", issuer=_ISSUER, client_id="hartmesh", admin_emails=["boss@example.com"])
    created = _sign_in(_identity(), provider_config=config)
    assert created["user"].system_role == "user"

    promoted = _sign_in(_identity(email="boss@example.com"), provider_config=config)
    assert promoted["user"].system_role == "admin", "the list is read from the address this sign-in settled on"

    demoted = _sign_in(_identity(email="pat@example.com"), provider_config=config)
    assert demoted["user"].system_role == "user", "and in the other direction"


# ── 2. A held address does not block the owner ──────────────────────────


def test_an_address_another_account_holds_does_not_block_the_owners_sign_in(users_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    _sign_in(_identity())
    _sign_in(_identity(subject="sub-sam", email="sam@example.com"))

    with caplog.at_level("WARNING"):
        again = _sign_in(_identity(email="sam@example.com"))

    assert again["created"] is False and again["user"].email == "pat@example.com", "the person is who the subject says they are"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"
    assert _stored_email(users_db, "sub-sam") == "sam@example.com", "and the holder is untouched"
    held = [record.getMessage() for record in caplog.records if "sub-sam" in record.getMessage()]
    assert held, "the journal says which account holds it"
    assert any(_ISSUER in line for line in held)


# ── 3. A new subject meets an address an old account holds ──────────────


def test_a_new_subject_carrying_a_provider_accounts_address_is_refused_in_its_own_words(users_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    _sign_in(_identity())

    with caplog.at_level("WARNING"), pytest.raises(HTTPException) as refused:
        _sign_in(_identity(subject="sub-new"))

    assert refused.value.redirect_code == EMAIL_TAKEN_CODE
    assert refused.value.detail == EMAIL_TAKEN_MESSAGE
    assert "password" not in str(refused.value.detail).lower(), "there is no password account to send them to"
    holder_named = [record.getMessage() for record in caplog.records if "sub-pat" in record.getMessage() and _ISSUER in record.getMessage()]
    assert holder_named, "the journal names the holder"


def test_a_local_password_account_holding_the_address_still_refuses_in_the_words_it_always_did(users_db: Path) -> None:
    """The real conflict, unchanged: a sign-in can never take over a password account."""
    asyncio.run(_provider().create_user(email="pat@example.com", password="Str0ng!Pass99"))

    with pytest.raises(HTTPException) as refused:
        _sign_in(_identity())

    assert refused.value.status_code == 409
    assert "An account with this email already exists" in str(refused.value.detail)
    assert getattr(refused.value, "redirect_code", None) is None, "the callback maps 409 to sso_account_exists"


def test_a_token_with_no_address_at_all_is_refused_as_an_unusable_address(users_db: Path) -> None:
    """It used to fall to the status map and say SSO was not allowed for their account."""
    with pytest.raises(HTTPException) as refused:
        _sign_in(_identity(email=None, subject="sub-nothing"))

    assert refused.value.redirect_code == EMAIL_UNUSABLE_CODE
    assert "not allowed for your account" not in str(refused.value.detail)


# ── The whole story: a recreated person ─────────────────────────────────


def test_a_recreated_person_is_locked_out_until_the_deployer_releases_the_address(users_db: Path) -> None:
    """The case this exists for, end to end."""
    from app.gateway.auth.accounts import AccountsCommand, released_email_for
    from app.gateway.deps import get_user_repo

    first = _sign_in(_identity())["user"]
    users = get_user_repo()
    assert users is not None
    command = AccountsCommand(users, tokens=None, schedules=None)
    asyncio.run(command.run("disable", issuer=_ISSUER, subject="sub-pat"))

    # The provider deleted them and invited them again: a new subject, the same address.
    with pytest.raises(HTTPException) as refused:
        _sign_in(_identity(subject="sub-pat-again"))
    assert refused.value.redirect_code == EMAIL_TAKEN_CODE

    document = asyncio.run(command.run("release-email", issuer=_ISSUER, subject="sub-pat"))
    assert document["verdict"] == "released" and document["released"] == "pat@example.com"

    created = _sign_in(_identity(subject="sub-pat-again"))
    assert created["created"] is True and created["user"].email == "pat@example.com"
    assert str(created["user"].id) != str(first.id), "a new account, not the old one handed over"

    # The old account is still there, turned off, released, with its identity.
    old = asyncio.run(users.get_user_by_identity(_ISSUER, "sub-pat"))
    assert old is not None and old.disabled_at is not None
    assert old.email == released_email_for(str(first.id)) and old.email_released_from == "pat@example.com"


def test_a_released_account_that_is_turned_back_on_does_not_reclaim_its_old_address(users_db: Path) -> None:
    """The address is someone else's now; the account keeps the one it was given."""
    from app.gateway.auth.accounts import AccountsCommand, released_email_for
    from app.gateway.deps import get_user_repo

    first = _sign_in(_identity())["user"]
    users = get_user_repo()
    assert users is not None
    command = AccountsCommand(users, tokens=None, schedules=None)
    asyncio.run(command.run("disable", issuer=_ISSUER, subject="sub-pat"))
    asyncio.run(command.run("release-email", issuer=_ISSUER, subject="sub-pat"))
    _sign_in(_identity(subject="sub-pat-again"))
    asyncio.run(command.run("enable", issuer=_ISSUER, subject="sub-pat"))

    back = _sign_in(_identity())

    assert back["user"].email == released_email_for(str(first.id)), "the address it follows is held by the new account"
    assert _stored_email(users_db, "sub-pat-again") == "pat@example.com"


def test_an_account_that_takes_a_real_address_again_can_be_released_again(users_db: Path) -> None:
    """A release is not a once-per-account act, or the lock-out comes straight back.

    Released, turned back on, signing in again with an address nobody holds:
    the account holds a real one, so it is no longer released -- and when the
    deployer later turns it off, that address can be given up in its turn.
    """
    from app.gateway.auth.accounts import AccountsCommand, released_email_for
    from app.gateway.deps import get_user_repo

    first = _sign_in(_identity())["user"]
    users = get_user_repo()
    assert users is not None
    command = AccountsCommand(users, tokens=None, schedules=None)
    asyncio.run(command.run("disable", issuer=_ISSUER, subject="sub-pat"))
    asyncio.run(command.run("release-email", issuer=_ISSUER, subject="sub-pat"))
    asyncio.run(command.run("enable", issuer=_ISSUER, subject="sub-pat"))

    back = _sign_in(_identity(email="pat.chen@example.com"))

    assert back["user"].email == "pat.chen@example.com", "the address follows as it does for any account"
    listed = {entry["subject"]: entry for entry in asyncio.run(command.run("list"))["accounts"]}
    assert listed["sub-pat"]["released"] is False and listed["sub-pat"]["released_from"] is None, "it holds a real address; nothing of its is going spare"

    asyncio.run(command.run("disable", issuer=_ISSUER, subject="sub-pat"))
    again = asyncio.run(command.run("release-email", issuer=_ISSUER, subject="sub-pat"))

    assert again["verdict"] == "released" and again["released"] == "pat.chen@example.com"
    assert _stored_email(users_db, "sub-pat") == released_email_for(str(first.id))


def test_an_address_from_a_domain_the_deployment_does_not_allow_leaves_the_one_the_account_has(users_db: Path) -> None:
    """The rule that governs creation governs the follow, or the two drift into an escalation.

    An address a *new* subject may not have must not be one an existing
    subject can acquire -- and since the role is read from the address this
    sign-in settled on, acquiring an ``admin_emails`` address would be a
    promotion the domain list exists to prevent.
    """
    restricted = OIDCProviderConfig(
        display_name="Company",
        issuer=_ISSUER,
        client_id="hartmesh",
        allowed_email_domains=["example.com"],
        admin_emails=["ops@vendor.example"],
    )
    created = _sign_in(_identity(), provider_config=restricted)
    assert created["user"].system_role == "user"

    # The same address is refused outright for a subject with no account.
    with pytest.raises(HTTPException) as refused:
        _sign_in(_identity(subject="sub-new", email="ops@vendor.example"), provider_config=restricted)
    assert refused.value.status_code == 403 and "domain is not allowed" in str(refused.value.detail)

    again = _sign_in(_identity(email="ops@vendor.example"), provider_config=restricted)

    assert again["user"].email == "pat@example.com", "the account keeps the address it has"
    assert again["user"].system_role == "user", "and the role read from it"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"


def test_an_allowed_domain_still_follows_and_still_re_grades_the_role(users_db: Path) -> None:
    """The refusal above must not cost the ordinary case anything."""
    restricted = OIDCProviderConfig(
        display_name="Company",
        issuer=_ISSUER,
        client_id="hartmesh",
        allowed_email_domains=["example.com"],
        admin_emails=["boss@example.com"],
    )
    _sign_in(_identity(), provider_config=restricted)

    promoted = _sign_in(_identity(email="boss@example.com"), provider_config=restricted)

    assert promoted["user"].email == "boss@example.com" and promoted["user"].system_role == "admin"


def test_an_address_taken_between_the_check_and_the_write_leaves_the_account_signed_in(users_db: Path) -> None:
    """The holder check and the write are two statements; the contract is the same either way.

    Simulated at the seam: the address is free when ``_address_that_follows``
    looks, and another account holds it by the time the UPDATE runs. The
    person still signs in as the account they are, keeping the address it
    had -- and the role read from that one, not from the address that got
    away.
    """
    from app.gateway.auth import user_provisioning

    config = OIDCProviderConfig(display_name="Company", issuer=_ISSUER, client_id="hartmesh", admin_emails=["boss@example.com"])
    _sign_in(_identity(), provider_config=config)
    provider = _provider()
    taker = asyncio.run(provider.create_oauth_user(email="parked@example.com", oauth_provider="sso", oauth_id="sub-taker"))

    real = user_provisioning._address_that_follows

    async def free_when_looked_at(provider_id, provider_config, identity, existing, local_provider):
        settled = await real(provider_id, provider_config, identity, existing, local_provider)
        if settled is None and identity.email == "boss@example.com":
            # It was held; pretend the check ran a moment earlier, when it was not.
            return identity.email
        return settled

    asyncio.run(provider.update_user(type(taker)(**{**taker.model_dump(), "email": "boss@example.com"})))
    user_provisioning._address_that_follows = free_when_looked_at
    try:
        landed = _sign_in(_identity(email="boss@example.com"), provider_config=config)
    finally:
        user_provisioning._address_that_follows = real

    assert landed["created"] is False and landed["user"].email == "pat@example.com"
    assert landed["user"].system_role == "user", "the role follows the address the account actually holds"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"
    assert _stored_email(users_db, "sub-taker") == "boss@example.com", "the holder is untouched"
    # The stamp is not optional: the sign-in happened.
    assert asyncio.run(provider.get_user_by_oauth("sso", "sub-pat")).last_sign_in_at is not None


def test_a_local_password_account_holding_the_address_leaves_the_one_the_account_has(users_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    """The holder need not be a provider account, and the journal must say so.

    "The account of subject None at issuer None" would tell a deployer
    nothing; what they need to know is that the thing in the way is a
    password account, which is cleared, not released.
    """
    _sign_in(_identity())
    asyncio.run(_provider().create_user(email="sam@example.com", password="Str0ng!Pass99"))

    with caplog.at_level("WARNING"):
        again = _sign_in(_identity(email="sam@example.com"))

    assert again["user"].email == "pat@example.com"
    assert _stored_email(users_db, "sub-pat") == "pat@example.com"
    held = [record.getMessage() for record in caplog.records if "held by" in record.getMessage()]
    assert held and all("a local password account" in line for line in held), held
    assert not any("subject None" in line for line in held), held

"""An address the account record cannot hold is its own refusal, not a collision.

A provider's assertion reaches the row unvalidated: ``OIDCIdentity.email`` is a
plain ``str`` while ``User.email`` is an ``EmailStr``, so the model is the first
thing in the path with an opinion about the address. Its refusal is a pydantic
``ValidationError``, which subclasses ``ValueError`` -- the very exception the
repository raises on a uniqueness violation. One ``except ValueError`` covering
both made a **first** sign-in on an empty table answer "An account with this
email already exists", with nothing an operator could read to contradict it: the
journal said it, the account list was empty, and ``select count(*) from users``
returned 0.

What is pinned here: the refusal carries its own code and its own words, the
table stays empty, the journal names the issuer and the subject and never
claims a conflict, and the genuine race -- and the genuine local-account
conflict -- still answer ``sso_account_exists``. The last test is the survey:
the two other callers that turn an address into a row cannot be bitten by this
shape, because their request model validates the address to the same type
before the handler runs.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-unusable-email-min-32")

from app.gateway.auth.config import AuthConfig, set_auth_config
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.models import User
from app.gateway.auth.oidc import OIDCIdentity
from app.gateway.auth.user_provisioning import EMAIL_UNUSABLE_CODE, EMAIL_UNUSABLE_MESSAGE, get_or_provision_oidc_user
from deerflow.config.auth_config import OIDCProviderConfig

_TEST_SECRET = "test-secret-key-unusable-email-min-32"
_ISSUER = "https://id.example.com/realms/co"
_PROVIDER = OIDCProviderConfig(display_name="Company", issuer=_ISSUER, client_id="hartmesh", client_secret="FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")

# The measured trigger: ``email_validator`` refuses a special-use domain. The
# class is wider -- any address the model refuses arrives here the same way.
UNUSABLE = "someone@example.invalid"


def _identity(**overrides) -> OIDCIdentity:
    values = {"provider": "sso", "subject": "sub-1", "email": UNUSABLE, "email_verified": True, "name": "Someone", "claims": {}}
    values.update(overrides)
    return OIDCIdentity(**values)


async def _provision(local_provider, identity: OIDCIdentity) -> dict:
    return await get_or_provision_oidc_user(provider_id="sso", provider_config=_PROVIDER, identity=identity, local_provider=local_provider)


@pytest.fixture
def users_db(tmp_path: Path) -> Iterator[Path]:
    """A fresh, empty users table; yields the database file so a test can count rows."""
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


def _row_count(database: Path) -> int:
    with sqlite3.connect(database) as connection:
        return connection.execute("SELECT count(*) FROM users").fetchone()[0]


def _live_provider() -> LocalAuthProvider:
    from app.gateway.deps import get_local_provider

    return get_local_provider()


def _racing_provider(*, resolves_to: User | None) -> AsyncMock:
    """A provider whose insert loses the unique index the way a concurrent callback makes it lose."""
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_email.return_value = None
    local_provider.get_user_by_oauth.side_effect = [None, resolves_to]
    local_provider.create_oauth_user.side_effect = ValueError("Email already registered: someone@example.com")
    return local_provider


# ── The defect ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "address",
    [
        UNUSABLE,  # the measured trigger: a special-use domain
        "someone@example.local",  # an on-prem directory's UPN, the likeliest tenant-side cause
        "someone@companyad",  # a domain with no dot at all, likewise
        "not-an-address",
    ],
)
def test_an_address_the_record_cannot_hold_is_refused_in_its_own_words(address: str, users_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    """The reported defect: an empty table, a first sign-in, and no account to conflict with.

    Parametrized over the class the operator guide names, so the guide's
    claim about what an account record will not hold is true by test.
    """
    with caplog.at_level("WARNING"):
        with pytest.raises(HTTPException) as refused:
            asyncio.run(_provision(_live_provider(), _identity(email=address)))

    assert refused.value.status_code == 403
    assert refused.value.redirect_code == EMAIL_UNUSABLE_CODE
    assert refused.value.detail == EMAIL_UNUSABLE_MESSAGE
    assert "already exists" not in str(refused.value.detail), "no account exists to conflict with"
    assert _row_count(users_db) == 0, "nothing was created, and nothing was there before"
    assert asyncio.run(_live_provider().get_user_by_email(address)) is None


def test_the_journal_names_the_issuer_and_the_subject_and_claims_no_conflict(users_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"), pytest.raises(HTTPException):
        asyncio.run(_provision(_live_provider(), _identity(subject="sub-journal")))

    refusals = [record.getMessage() for record in caplog.records if record.levelname == "WARNING"]
    assert any("sub-journal" in line and _ISSUER in line for line in refusals), refusals
    assert not any("already exists" in line for line in refusals), refusals
    assert not any(_PROVIDER.client_secret in line for line in refusals if _PROVIDER.client_secret)


@pytest.mark.parametrize(
    "address",
    [
        "someone@example.invalid",  # the measured trigger
        "someone@example.test",
        "someone@example.local",  # an on-prem AD UPN, the likeliest tenant-side cause
        "someone@localhost",
        "someone@companyad",  # a single-label domain, likewise
        "someone@example.arpa",
        "someone",  # no @ at all
        "",
        "x" * 300 + "@example.com",
    ],
)
def test_every_way_the_record_refuses_an_address_is_one_complaint_about_the_address(address: str) -> None:
    """The coupling ``_only_the_address_was_refused`` rests on, pinned.

    It asks whether the record's complaint is about ``email`` and nothing
    else. Were a refusal of the address ever to arrive under another
    location -- a rename, an alias, a second complaint -- the predicate
    would answer no and the refusal would propagate as a fault instead of
    reaching the person. This is the fact that makes the predicate exact.
    """
    from app.gateway.auth.user_provisioning import _only_the_address_was_refused

    with pytest.raises(ValidationError) as refused:
        User(email=address, system_role="user")
    assert [complaint["loc"] for complaint in refused.value.errors()] == [("email",)]
    assert _only_the_address_was_refused(refused.value) is True


def test_the_refusal_detail_names_neither_the_syntax_rule_nor_the_provider_internals() -> None:
    """The detail the journal and the API carry, pinned: no validator vocabulary, no field names.

    In the browser flow the callback redirects and this detail never reaches
    the person -- the login page's own string does, and that one is pinned in
    ``frontend-hm/tests/unit/app/auth/login-sign-on-only.dom.test.tsx``.
    """
    lowered = EMAIL_UNUSABLE_MESSAGE.lower()
    for leak in ("special-use", "reserved", "@-sign", "validator", "emailstr", "pydantic", "domain", ".invalid", "already exists"):
        assert leak not in lowered, f"{leak!r} is not the person's business"
    assert EMAIL_UNUSABLE_CODE.startswith("sso_") and EMAIL_UNUSABLE_CODE != "sso_account_exists"


def test_a_complaint_about_any_other_field_is_a_fault_here_and_propagates() -> None:
    """Only the address is a provider's assertion; anything else refused is this code's bug, not a refusal to dress up."""
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = None
    try:
        User(email="someone@example.com", system_role="superuser")
    except ValidationError as exc:
        local_provider.create_oauth_user.side_effect = exc
    else:
        pytest.fail("the record no longer refuses a bad system_role; this test needs another non-address complaint")

    with pytest.raises(ValidationError):
        asyncio.run(_provision(local_provider, _identity(email="someone@example.com")))


def test_the_address_refused_alongside_another_field_is_a_fault_too() -> None:
    """The branch ``all()`` exists to decide: the address is the reason only when it is the whole reason.

    A complaint about the address *and* something else means this code sent a
    row it should not have. Refusing the person for the address would hide
    that, which is the very substitution this change exists to stop.
    """
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = None
    try:
        User(email=UNUSABLE, system_role="superuser")
    except ValidationError as exc:
        assert {complaint["loc"] for complaint in exc.errors()} == {("email",), ("system_role",)}
        local_provider.create_oauth_user.side_effect = exc
    else:
        pytest.fail("the record no longer refuses this pair; this test needs another mixed complaint")

    with pytest.raises(ValidationError):
        asyncio.run(_provision(local_provider, _identity()))


# ── What must not have changed ───────────────────────────────────────────


def test_an_address_the_record_holds_still_creates_the_account(users_db: Path) -> None:
    """The arm is narrow: an ordinary address takes the path it always did."""
    result = asyncio.run(_provision(_live_provider(), _identity(email="pat@example.com")))

    assert result["created"] is True
    assert result["user"].email == "pat@example.com"
    assert _row_count(users_db) == 1


def test_a_local_account_that_really_holds_the_address_is_still_a_conflict(users_db: Path) -> None:
    provider = _live_provider()
    asyncio.run(provider.create_user(email="pat@example.com", password="Str0ng!Pass99"))

    with pytest.raises(HTTPException) as refused:
        asyncio.run(_provision(provider, _identity(email="pat@example.com")))

    assert refused.value.status_code == 409
    assert "already exists" in str(refused.value.detail)
    assert getattr(refused.value, "redirect_code", None) is None, "the callback maps 409 to sso_account_exists"
    assert _row_count(users_db) == 1


def test_the_lost_race_still_re_resolves_to_the_winners_row() -> None:
    winner = User(email="someone@example.com", password_hash=None, oauth_provider="sso", oauth_id="sub-1")
    local_provider = _racing_provider(resolves_to=winner)

    result = asyncio.run(_provision(local_provider, _identity(email="someone@example.com")))

    assert result == {"user": winner, "created": False}
    assert local_provider.get_user_by_oauth.await_count == 2


def test_a_race_that_collides_on_the_address_alone_still_answers_a_conflict() -> None:
    local_provider = _racing_provider(resolves_to=None)

    with pytest.raises(HTTPException) as refused:
        asyncio.run(_provision(local_provider, _identity(email="someone@example.com")))

    assert refused.value.status_code == 409
    assert getattr(refused.value, "redirect_code", None) is None, "the callback maps 409 to sso_account_exists"


# ── The survey ───────────────────────────────────────────────────────────


def test_the_other_two_callers_validate_the_address_before_their_own_except_valueerror() -> None:
    """``/register`` and ``/initialize`` wrap ``create_user`` in the same shape.

    They cannot be bitten by it: each request model declares ``email:
    EmailStr``, the same type the row is held to, so FastAPI refuses an
    unusable address with a 422 before the handler -- and its ``except
    ValueError`` -- ever runs.
    """
    from app.gateway.routers.auth import InitializeAdminRequest, RegisterRequest

    for model in (RegisterRequest, InitializeAdminRequest):
        with pytest.raises(ValidationError) as refused:
            model(email=UNUSABLE, password="Str0ng!Pass99")
        assert refused.value.errors()[0]["loc"] == ("email",)
        assert model(email="pat@example.com", password="Str0ng!Pass99").email == "pat@example.com"

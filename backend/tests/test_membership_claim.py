"""Admission by claim and the role that follows it, on the bare pieces.

The identity provider is the membership authority. Configuration names one
claim and the values that admit (``access_claim``, ``access_values``) and may
map values to roles (``access_roles``); every sign-in reads the claim before
any account is created or returned. This file proves the configuration
refusals, the three claim shapes from either source in the stated order, the
role in both directions, and that a turned-off identity is refused first --
on the provisioning function with a users double. The served Gateway and
the command are ``test_membership_e2e.py`` and ``test_accounts_command.py``.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.gateway.auth.oidc import OIDCIdentity
from deerflow.config.auth_config import OIDCProviderConfig

ISSUER = "https://login.example.com/realms/tenant"
CLAIM = "urn:zitadel:iam:org:project:roles"


def _provider(**overrides) -> OIDCProviderConfig:
    fields = {"display_name": "Company", "issuer": ISSUER, "client_id": "hartmesh", "client_secret": "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"}
    fields.update(overrides)
    return OIDCProviderConfig(**fields)


def _identity(*, subject: str = "sub-1", email: str = "pat@example.com", id_token: dict | None = None, userinfo: dict | None = None) -> OIDCIdentity:
    id_token = {"sub": subject, "email": email, "email_verified": True, **(id_token or {})}
    userinfo = {"sub": subject, **(userinfo or {})}
    return OIDCIdentity(provider="sso", subject=subject, email=email, email_verified=True, name="Pat", claims={**id_token, **userinfo}, id_token_claims=id_token, userinfo_claims=userinfo)


# ── Evidence 4: the configuration refusals ───────────────────────────────


@pytest.mark.parametrize(
    ("fields", "names"),
    [
        ({"access_claim": CLAIM}, "access_values is empty"),
        ({"access_values": ["member"]}, "access_claim is not"),
        ({"access_claim": "   ", "access_values": ["member"]}, "must not be blank"),
        ({"access_claim": CLAIM, "access_values": ["member", "member"]}, "repeat"),
        ({"access_claim": CLAIM, "access_values": ["member", " "]}, "empty entries"),
        ({"access_roles": {"admin": "admin"}}, "access_roles is set but access_claim is not"),
        ({"access_claim": CLAIM, "access_values": ["admin", "member"], "access_roles": {"admin": "admin", "member": "user"}, "admin_emails": ["owner@example.com"]}, "access_roles and admin_emails are both set"),
        ({"access_claim": CLAIM, "access_values": ["admin", "member"], "access_roles": {"admin": "admin"}}, "gives no role to admitting value(s) member"),
        ({"access_claim": CLAIM, "access_values": ["admin"], "access_roles": {"admin": "admin", "guest": "user"}}, "maps value(s) guest that are not in access_values"),
    ],
)
def test_a_half_configured_rule_refuses_and_names_what_is_missing(fields: dict, names: str) -> None:
    with pytest.raises(ValueError, match=names.replace("(", r"\(").replace(")", r"\)")):
        _provider(**fields)


def test_a_wrong_role_name_refuses() -> None:
    with pytest.raises(ValueError):
        _provider(access_claim=CLAIM, access_values=["admin"], access_roles={"admin": "owner"})


def test_admission_without_a_mapping_keeps_roles_from_the_email_list() -> None:
    provider = _provider(access_claim=f" {CLAIM} ", access_values=[" member "], admin_emails=["owner@example.com"])
    assert provider.access_claim == CLAIM and provider.access_values == ["member"] and provider.access_roles == {}


def test_without_the_keys_nothing_is_required() -> None:
    provider = _provider()
    assert provider.access_claim is None and provider.access_values == [] and provider.access_roles == {}


# ── Evidence 2: the shapes, from either source, in order ─────────────────


@pytest.mark.parametrize("shape", [["member", "auditor"], "member", {"member": {"org": "1"}, "auditor": {}}])
@pytest.mark.parametrize("source", ["id_token", "userinfo"])
def test_each_shape_admits_from_either_source(shape, source: str) -> None:
    from app.gateway.auth.access import admitted_values, read_claim

    provider = _provider(access_claim=CLAIM, access_values=["member", "admin"])
    identity = _identity(**{source: {CLAIM: shape}})
    reading = read_claim(identity, CLAIM)
    assert reading.source == source and reading.problem is None
    assert admitted_values(provider, identity) == frozenset({"member"})


def test_the_id_token_is_read_first_and_userinfo_only_when_the_id_token_lacks_the_name() -> None:
    from app.gateway.auth.access import admitted_values, read_claim

    provider = _provider(access_claim=CLAIM, access_values=["member"])
    both = _identity(id_token={CLAIM: ["member"]}, userinfo={CLAIM: ["stranger"]})
    assert read_claim(both, CLAIM).source == "id_token"
    assert admitted_values(provider, both) == frozenset({"member"})
    # A wrong shape in the ID token is not repaired from userinfo.
    wrong_then_right = _identity(id_token={CLAIM: 7}, userinfo={CLAIM: ["member"]})
    assert read_claim(wrong_then_right, CLAIM).problem == "wrong type"


@pytest.mark.parametrize(
    ("claims", "problem"),
    [
        ({}, "missing"),
        ({CLAIM: []}, "empty"),
        ({CLAIM: ""}, "empty"),
        ({CLAIM: {}}, "empty"),
        ({CLAIM: ["  "]}, "empty"),
        ({CLAIM: 42}, "wrong type"),
        ({CLAIM: [1, 2]}, "wrong type"),
        ({CLAIM: {1: "x"}}, "wrong type"),
        ({CLAIM: None}, "wrong type"),
    ],
)
def test_a_missing_empty_or_wrongly_typed_claim_refuses(claims: dict, problem: str, caplog: pytest.LogCaptureFixture) -> None:
    from app.gateway.auth.access import NO_ACCESS_CODE, NO_ACCESS_MESSAGE, AccessRefused, admitted_values, read_claim

    provider = _provider(access_claim=CLAIM, access_values=["member"])
    identity = _identity(id_token=claims)
    assert read_claim(identity, CLAIM).problem == problem
    with caplog.at_level(logging.WARNING), pytest.raises(AccessRefused) as refused:
        admitted_values(provider, identity)
    assert refused.value.status_code == 403 and refused.value.redirect_code == NO_ACCESS_CODE and refused.value.detail == NO_ACCESS_MESSAGE
    assert CLAIM not in refused.value.detail, "the person's message never names the claim"
    line = caplog.records[-1].getMessage()
    assert "sub-1" in line and ISSUER in line and problem in line


def test_a_claim_with_no_admitting_value_refuses_and_the_name_is_literal() -> None:
    from app.gateway.auth.access import AccessRefused, admitted_values, read_claim

    provider = _provider(access_claim=CLAIM, access_values=["member"])
    with pytest.raises(AccessRefused):
        admitted_values(provider, _identity(id_token={CLAIM: ["stranger"]}))
    # "urn:zitadel:iam:org:project:roles" is one name: nothing walks "urn" then "zitadel".
    nested = _identity(id_token={"urn": {"zitadel": {"iam": {"org": {"project": {"roles": ["member"]}}}}}})
    assert read_claim(nested, CLAIM).problem == "missing"


def test_no_claim_configured_means_no_check() -> None:
    from app.gateway.auth.access import admitted_values

    assert admitted_values(_provider(), _identity()) is None


# ── Evidence 3: the role ─────────────────────────────────────────────────


def test_the_role_follows_the_mapping_and_admin_wins() -> None:
    from app.gateway.auth.access import role_for

    provider = _provider(access_claim=CLAIM, access_values=["admin", "member"], access_roles={"admin": "admin", "member": "user"})
    assert role_for(provider, frozenset({"member"}), "owner@example.com") == "user"
    assert role_for(provider, frozenset({"admin"}), "pat@example.com") == "admin"
    assert role_for(provider, frozenset({"member", "admin"}), "pat@example.com") == "admin"


def test_without_the_mapping_the_role_comes_from_the_email_list() -> None:
    from app.gateway.auth.access import role_for

    provider = _provider(access_claim=CLAIM, access_values=["member"], admin_emails=["Owner@example.com"])
    assert role_for(provider, frozenset({"member"}), "owner@example.com") == "admin"
    assert role_for(provider, frozenset({"member"}), "pat@example.com") == "user"
    assert role_for(_provider(), None, "pat@example.com") == "user"


# ── Provisioning: the order of the checks, on a users double ─────────────


class _Users:
    def __init__(self, *users, disabled: set[tuple[str, str]] = frozenset()) -> None:
        self.users = list(users)
        self.disabled = set(disabled)
        self.updated: list = []
        self.created: list = []

    async def is_identity_disabled(self, issuer: str, subject: str) -> bool:
        return (issuer.rstrip("/"), subject) in self.disabled

    async def get_user_by_oauth(self, provider: str, oauth_id: str):
        return next((u for u in self.users if u.oauth_provider == provider and u.oauth_id == oauth_id), None)

    async def get_user_by_email(self, email: str):
        return next((u for u in self.users if u.email == email), None)

    async def update_user(self, user):
        self.updated.append(user)
        return user

    async def record_sign_in(self, user, *, email: str | None = None) -> bool:
        if email is not None:
            user.email = email
        self.updated.append(user)
        return email is not None

    async def create_oauth_user(self, **kwargs):
        user = SimpleNamespace(id=uuid4(), needs_setup=False, token_version=0, password_hash=None, disabled_at=None, **kwargs)
        self.created.append(user)
        self.users.append(user)
        return user


def _linked(role: str = "user") -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), email="pat@example.com", oauth_provider="sso", oauth_id="sub-1", oauth_issuer=ISSUER, system_role=role, needs_setup=False, token_version=0, password_hash=None, last_sign_in_at=None, disabled_at=None)


MAPPED = {"access_claim": CLAIM, "access_values": ["admin", "member"], "access_roles": {"admin": "admin", "member": "user"}}


@pytest.mark.anyio
async def test_an_admitting_token_creates_the_account_with_the_mapped_role_and_stamps_the_sign_in() -> None:
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    users = _Users()
    result = await get_or_provision_oidc_user("sso", _provider(**MAPPED), _identity(id_token={CLAIM: {"admin": {}}}), users)
    assert result["created"] is True and users.created[0].system_role == "admin" and users.created[0].last_sign_in_at is not None


@pytest.mark.anyio
async def test_a_token_without_an_admitting_value_creates_nothing_and_is_refused_for_a_linked_account_too() -> None:
    from app.gateway.auth.access import AccessRefused
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    provider = _provider(access_claim=CLAIM, access_values=["member"])
    users = _Users()
    with pytest.raises(AccessRefused):
        await get_or_provision_oidc_user("sso", provider, _identity(id_token={CLAIM: ["stranger"]}), users)
    assert users.created == []
    linked = _linked()
    users = _Users(linked)
    with pytest.raises(AccessRefused):
        await get_or_provision_oidc_user("sso", provider, _identity(id_token={CLAIM: ["stranger"]}), users)
    assert users.updated == [], "the linked account is not returned, and not touched"


@pytest.mark.anyio
async def test_the_role_is_rewritten_at_every_sign_in_in_both_directions() -> None:
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    linked = _linked("user")
    users = _Users(linked)
    promoted = await get_or_provision_oidc_user("sso", _provider(**MAPPED), _identity(id_token={CLAIM: ["admin"]}), users)
    assert promoted["user"] is linked and linked.system_role == "admin"
    demoted = await get_or_provision_oidc_user("sso", _provider(**MAPPED), _identity(id_token={CLAIM: ["member"]}), users)
    assert demoted["user"] is linked and linked.system_role == "user"
    assert users.updated == [linked, linked] and linked.last_sign_in_at is not None


@pytest.mark.anyio
async def test_without_the_mapping_an_existing_role_is_left_alone() -> None:
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    linked = _linked("admin")
    users = _Users(linked)
    await get_or_provision_oidc_user("sso", _provider(access_claim=CLAIM, access_values=["member"]), _identity(id_token={CLAIM: ["member"]}), users)
    assert linked.system_role == "admin"


@pytest.mark.anyio
async def test_a_turned_off_identity_is_refused_before_anything_else_even_when_the_claim_admits(caplog: pytest.LogCaptureFixture) -> None:
    from app.gateway.auth.access import ACCESS_OFF_CODE, ACCESS_OFF_MESSAGE, AccessRefused
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user

    users = _Users(disabled={(ISSUER, "sub-1")})
    with caplog.at_level(logging.WARNING), pytest.raises(AccessRefused) as refused:
        await get_or_provision_oidc_user("sso", _provider(**MAPPED), _identity(id_token={CLAIM: ["admin"], "at_hash": "FAKE-TOKEN-SENTINEL"}), users)
    assert refused.value.redirect_code == ACCESS_OFF_CODE and refused.value.detail == ACCESS_OFF_MESSAGE
    assert users.created == [], "no account is created"
    assert "sub-1" in caplog.text and ISSUER in caplog.text and "FAKE-TOKEN-SENTINEL" not in caplog.text
    # With an account: refused before it is returned.
    users = _Users(_linked(), disabled={(ISSUER, "sub-1")})
    with pytest.raises(AccessRefused):
        await get_or_provision_oidc_user("sso", _provider(), _identity(), users)
    assert users.updated == []


# ── The refusal every credential path shares ─────────────────────────────


def test_a_disabled_account_is_refused_wherever_a_credential_resolves_to_one(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import HTTPException

    from app.gateway.auth.mode import account_refusal, require_live_account

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: SimpleNamespace(auth=SimpleNamespace(local=SimpleNamespace(enabled=True))))
    live = SimpleNamespace(oauth_provider="sso", disabled_at=None)
    off = SimpleNamespace(oauth_provider="sso", disabled_at="2026-09-21T00:00:00+00:00")
    assert account_refusal(live) is None
    assert account_refusal(off) == "disabled"
    require_live_account(live)
    with pytest.raises(HTTPException) as refused:
        require_live_account(off)
    assert refused.value.status_code == 401 and refused.value.detail["code"] == "account_disabled"
    # In either mode: a disabled local-mode provider account is refused too, and
    # in sign-on-only mode an inert account still is.
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: SimpleNamespace(auth=SimpleNamespace(local=SimpleNamespace(enabled=False))))
    assert account_refusal(SimpleNamespace(oauth_provider=None, disabled_at=None)) == "inert"
    assert account_refusal(off) == "disabled"


def test_the_callback_maps_the_refusal_to_its_own_code() -> None:
    """The login page shows one message per code; a refusal carries the code it wants shown."""
    from app.gateway.auth.access import ACCESS_OFF_CODE, NO_ACCESS_CODE, AccessRefused

    assert AccessRefused(code=NO_ACCESS_CODE, message="m").redirect_code == "sso_no_access"
    assert AccessRefused(code=ACCESS_OFF_CODE, message="m").redirect_code == "sso_access_off"

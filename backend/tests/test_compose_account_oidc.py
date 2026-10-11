"""Full seats refuse new identities, while racing sign-ins resolve their winner."""

import asyncio

import pytest
from fastapi import HTTPException
from test_compose_account_seats import seated_accounts as seated_accounts

from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.models import User
from app.gateway.auth.oidc import OIDCIdentity
from app.gateway.auth.user_provisioning import get_or_provision_oidc_user
from deerflow.config.auth_config import OIDCProviderConfig

PROVIDER = OIDCProviderConfig(display_name="Test", issuer="https://identity.example.com", client_id="fixture")


def identity(subject):
    return OIDCIdentity(provider="sso", subject=subject, email=f"{subject}@example.com", email_verified=True, name="Fixture", claims={})


@pytest.mark.asyncio
async def test_oidc_no_seat_refusal_has_login_redirect_code(seated_accounts):
    repo, _sf, *_ = seated_accounts
    await repo.create_first_admin(User(email="owner@example.com", system_role="admin"))

    async def allowance(_session):
        return 1

    repo.seats_allowed = allowance
    with pytest.raises(HTTPException) as refusal:
        await get_or_provision_oidc_user("sso", PROVIDER, identity("new"), LocalAuthProvider(repo))
    assert refusal.value.status_code == 403
    assert refusal.value.redirect_code == "no_seats_left"
    assert await repo.count_users() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("same", [True, False])
async def test_last_seat_racing_oidc_sign_ins_resolve_only_the_exact_identity(seated_accounts, monkeypatch, same):
    repo, _sf, *_ = seated_accounts
    await repo.create_first_admin(User(email="owner@example.com", system_role="admin"))

    async def allowance(_session):
        return 2

    repo.seats_allowed = allowance
    provider = LocalAuthProvider(repo)
    original = provider.get_user_by_oauth
    entered, release = 0, asyncio.Event()

    async def initially_absent(name, subject):
        nonlocal entered
        if entered < 2:
            entered += 1
            if entered == 2:
                release.set()
            await asyncio.wait_for(release.wait(), 5)
            return None
        return await original(name, subject)

    monkeypatch.setattr(provider, "get_user_by_oauth", initially_absent)
    outcomes = await asyncio.wait_for(asyncio.gather(*(get_or_provision_oidc_user("sso", PROVIDER, identity(subject), provider) for subject in ("one", "one" if same else "two")), return_exceptions=True), 10)
    winners = [item for item in outcomes if isinstance(item, dict)]
    assert len(winners) == (2 if same else 1), outcomes
    if same:
        assert len({str(item["user"].id) for item in winners}) == 1
        assert sorted(item["created"] for item in winners) == [False, True]
    else:
        refusal = next(item for item in outcomes if isinstance(item, BaseException))
        assert isinstance(refusal, HTTPException) and refusal.redirect_code == "no_seats_left"
    assert await repo.count_seated_users() == 2


@pytest.mark.asyncio
async def test_new_oidc_account_commits_before_storage_and_keeps_success_if_storage_fails(seated_accounts, monkeypatch):
    from types import SimpleNamespace

    from app.gateway import storage_spaces as ss

    repo, _sf, *_ = seated_accounts
    observed = []

    async def pending(_app, account):
        assert await repo.get_user_by_id(str(account.id)) is not None
        observed.append(str(account.id))
        raise OSError("fixture post-commit storage unavailable")

    monkeypatch.setattr(ss, "assign_user_space", pending)
    response = await get_or_provision_oidc_user("sso", PROVIDER, identity("new"), LocalAuthProvider(repo), app=SimpleNamespace(state=SimpleNamespace()))
    assert response["created"] is True
    assert observed == [str(response["user"].id)]
    assert await repo.get_user_by_id(observed[0]) is not None

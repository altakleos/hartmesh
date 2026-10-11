"""Account capacity is distinguished from duplicate-email refusals at each door."""

import pytest
from test_local_add_user import _add, _admin, _client, _local, _row
from test_local_add_user import users_db as users_db


def seat_limit(value):
    from app.gateway.deps import get_user_repo

    async def allowed(_session):
        return value

    get_user_repo().seats_allowed = allowed


@pytest.mark.parametrize("door,status", [("register", 403), ("admin", 409)])
def test_local_account_creation_returns_specific_no_seat_refusal(users_db, monkeypatch, door, status):
    client = _admin(monkeypatch, _local(open_=True))
    seat_limit(1)
    client.raise_server_exceptions = False
    response = client.post("/api/v1/auth/register", json={"email": "new@example.com", "password": "S0me-Un1que-Pass"}) if door == "register" else _add(client, "new@example.com")
    assert response.status_code == status, response.text
    assert response.json()["detail"]["code"] == "no_seats_left"
    assert _row("new@example.com") is None


def test_initialize_is_possible_when_pool_has_no_prepared_seat(users_db, monkeypatch):
    client = _client(monkeypatch, _local(open_=True))
    seat_limit(0)
    response = client.post("/api/v1/auth/initialize", json={"email": "owner@example.com", "password": "Str0ng!Pass99"})
    assert response.status_code == 201, response.text
    assert _row("owner@example.com") is not None


@pytest.mark.parametrize("door", ["initialize", "register", "admin"])
def test_created_account_is_committed_before_storage_and_storage_failure_keeps_success(users_db, monkeypatch, door):
    from app.gateway import storage_spaces as ss
    from app.gateway.deps import get_user_repo

    client = _admin(monkeypatch, _local(open_=True)) if door == "admin" else _client(monkeypatch, _local(open_=True))
    observed = []

    async def pending(_app, account):
        stored = await get_user_repo().get_user_by_id(str(account.id))
        assert stored is not None
        observed.append(str(account.id))
        raise OSError("fixture post-commit storage unavailable")

    monkeypatch.setattr(ss, "assign_user_space", pending)
    if door == "admin":
        response = _add(client, "new@example.com")
    else:
        response = client.post(f"/api/v1/auth/{door}", json={"email": "new@example.com", "password": "S0me-Un1que-Pass"})
    assert response.status_code == 201, response.text
    assert observed == [response.json()["id"]]
    assert _row("new@example.com") is not None


def test_local_disable_and_release_refuse_browser_pat_and_password_login(users_db, monkeypatch):
    import asyncio

    from test_local_add_user import _csrf, _login

    from app.gateway.auth.accounts import AccountsCommand
    from app.gateway.deps import get_user_repo
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository

    client = _admin(monkeypatch, _local(open_=True))
    pats = PersonalAccessTokenRepository(get_session_factory())
    client.app.state.pat_repo = pats
    minted = client.post("/api/v1/auth/pats", json={"name": "local-disable-test", "scopes": ["threads:read"]}, headers=_csrf(client))
    assert minted.status_code == 201, minted.text
    as_token = _client(monkeypatch, _local(open_=True))
    as_token.app.state.pat_repo = pats
    headers = {"Authorization": "Bearer " + minted.json()["token"]}
    before = as_token.get("/api/threads/unknown/goal", headers=headers)
    assert before.status_code == 503, before.text  # Authentication succeeds; this bare app has no thread store.
    owner = _row("owner@example.com")
    command = AccountsCommand(get_user_repo(), tokens=None, schedules=None)
    asyncio.run(command.run("disable", email=owner.email))
    browser = client.get("/api/v1/auth/me")
    assert browser.status_code in (401, 403), browser.text
    token = as_token.get("/api/threads/unknown/goal", headers=headers)
    assert token.status_code == 401 and token.json()["detail"] == "Invalid token", token.text
    signin = _login(as_token, owner.email, "Str0ng!Pass99")
    assert signin.status_code == 401 and signin.json()["detail"]["code"] == "account_disabled"
    assert "access_token" not in signin.cookies
    released = asyncio.run(command.run("release-seat", account_id=str(owner.id)))
    assert released["returncode"] == 0
    assert released["account"]["released_at"] is not None
    refused = as_token.get("/api/threads/unknown/goal", headers=headers)
    assert refused.status_code == 401 and refused.json()["detail"] == "Invalid token", refused.text

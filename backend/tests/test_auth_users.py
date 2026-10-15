"""People lookup uses real sessions, current account state and literal prefixes."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_auth_me_permissions import _initialize_admin
from test_auth_me_permissions import _setup_auth as _setup_auth
from test_compose_account_seats import seated_accounts as seated_accounts

from app.gateway.auth import create_access_token
from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.models import User


@pytest.mark.asyncio
async def test_lookup_paginates_past_inactive_candidates_and_uses_literal_sql_on_each_database(seated_accounts, monkeypatch):
    from app.gateway.auth import people as directory
    from deerflow.persistence.user.model import UserRow

    repo, sf, _files, _catalog = seated_accounts
    monkeypatch.setattr(directory, "get_user_repo", lambda: repo)
    monkeypatch.setattr(directory, "local_registration_open", lambda: False)
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    ids = [str(uuid4()) for _ in range(56)]
    async with sf() as session, session.begin():
        session.add_all([UserRow(id=identity, email=f"al%{i:03}@example.com", released_at=datetime.now(UTC) if i < 54 else None) for i, identity in enumerate(ids)])
        session.add(UserRow(id=str(uuid4()), email="alX000@example.com"))
    rows = await directory.lookup_people(query="AL%", ids=None, limit=2)
    assert [row["id"] for row in rows] == ids[54:]
    assert all(row["active"] for row in rows)
    assert await directory.lookup_people(query="al\\%", ids=None, limit=2) == []
    monkeypatch.setattr(directory, "local_registration_open", lambda: True)
    assert await directory.lookup_people(query="AL%054@EXAMPLE.COM", ids=None, limit=2) == [rows[0]]


@pytest.fixture
def directory_client(_setup_auth, monkeypatch):
    from app.gateway import deps
    from app.gateway.app import create_app
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "auth": {"local": {"enabled": True, "allow_registration": False}}})
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
    monkeypatch.setattr(deps, "get_config", lambda: config)
    app = create_app()
    app.state.pat_repo = PersonalAccessTokenRepository(get_session_factory())
    client = TestClient(app)
    try:
        _initialize_admin(client)
        yield client, config
    finally:
        client.close()


def people(*addresses):
    from app.gateway.deps import get_user_repo

    async def create():
        repo = get_user_repo()
        return [await repo.create_user(User(email=address, password_hash="fixture-password-hash")) for address in addresses]

    return asyncio.run(create())


def account_command(command, person):
    from app.gateway.deps import get_user_repo

    result = asyncio.run(AccountsCommand(get_user_repo(), tokens=None, schedules=None).run(command, account_id=str(person.id)))
    assert result["returncode"] == 0, result


def test_query_returns_active_accounts_by_prefix_when_registration_is_closed(directory_client, monkeypatch):
    from app.gateway import auth_middleware

    client, _config = directory_client
    disabled, alice, _alina = people("al-disabled@example.com", "alice@example.com", "alina@example.com")
    account_command("disable", disabled)
    # A regular human account needs no agent grant or administrator role.
    client.cookies.clear()
    client.cookies.set("access_token", create_access_token(str(alice.id)))
    monkeypatch.setattr(auth_middleware, "resolve_route_permissions", AsyncMock(return_value=[]))
    response = client.get("/api/v1/auth/users", params={"query": "AL", "limit": 1})
    assert response.status_code == 200, response.text
    assert response.json() == {"users": [{"id": str(alice.id), "email": alice.email, "name": None, "active": True}]}


def test_query_needs_a_complete_address_of_up_to_320_characters_when_registration_is_open(directory_client):
    client, config = directory_client
    (person,) = people("match@example.com")
    config.auth.local.allow_registration = True
    response = client.get("/api/v1/auth/users", params={"query": "ma"})
    assert response.status_code == 200 and response.json() == {"users": []}
    response = client.get("/api/v1/auth/users", params={"query": "MATCH@EXAMPLE.COM"})
    assert response.status_code == 200 and response.json()["users"][0]["id"] == str(person.id)
    response = client.get("/api/v1/auth/users", params={"query": "q" * 308 + "@example.com"})
    assert response.status_code == 200 and response.json() == {"users": []}


@pytest.mark.parametrize("literal", ["_", "%", "\\"])
def test_prefix_wildcards_and_escape_are_literal(directory_client, literal):
    client, _config = directory_client
    if literal == "\\":
        # A backslash cannot occur in an account address. It must not escape
        # the following percent and accidentally select this valid address.
        people("al%person@example.com", "alXperson@example.com")
        query, expected = "al\\%person", []
    else:
        wanted, _other = people("al" + literal + "person@example.com", "alXperson@example.com")
        query, expected = "al" + literal, [str(wanted.id)]
    response = client.get("/api/v1/auth/users", params={"query": query})
    assert response.status_code == 200, response.text
    assert [u["id"] for u in response.json()["users"]] == expected


def test_ids_also_return_deactivated_accounts_marked_inactive_and_omit_unknown_ids(directory_client):
    client, _config = directory_client
    disabled, released, active = people("disabled@example.com", "released@example.com", "active@example.com")
    account_command("disable", disabled)
    account_command("disable", released)
    account_command("release-seat", released)
    params = [("ids", str(person.id)) for person in (disabled, released, active)] + [("ids", str(uuid4()))]
    response = client.get("/api/v1/auth/users", params=params)
    assert response.status_code == 200, response.text
    users = {user["id"]: user for user in response.json()["users"]}
    assert set(users) == {str(p.id) for p in (disabled, released, active)}
    assert users[str(active.id)]["active"] is True
    assert users[str(disabled.id)]["active"] is users[str(released.id)]["active"] is False
    assert all(set(user) == {"id", "email", "name", "active"} for user in users.values())
    response = client.get("/api/v1/auth/users", params={"query": "disabled@"})
    assert response.status_code == 200 and response.json() == {"users": []}


@pytest.mark.parametrize(
    "params,code,detail",
    [
        ([], 400, "Give either query or ids"),
        ([("query", "al"), ("ids", "alice")], 400, "Give either query or ids"),
        ([("query", "a")], 422, None),
        ([("query", "a" * 65)], 400, "Search text is at most 64 characters"),
        ([("query", "a" * 321)], 422, None),
        ([("query", "al"), ("limit", "0")], 422, None),
        ([("query", "al"), ("limit", "51")], 422, None),
        ([("ids", "alice")] * 51, 400, "At most 50 ids"),
        ([("ids", "has a space")], 422, None),
    ],
)
def test_exactly_one_of_query_or_ids_and_their_bounds(directory_client, params, code, detail):
    client, _config = directory_client
    response = client.get("/api/v1/auth/users", params=params)
    assert response.status_code == code, response.text
    if detail is not None:
        assert response.json() == {"detail": detail}


def test_users_route_refuses_a_personal_access_token(directory_client):
    client, _config = directory_client
    response = client.post("/api/v1/auth/pats", headers={"X-CSRF-Token": client.cookies.get("csrf_token")}, json={"name": "directory", "scopes": ["threads:read"]})
    assert response.status_code == 201, response.text
    token = response.json()["token"]
    client.cookies.clear()
    response = client.get("/api/v1/auth/users", params={"query": "ad"}, headers={"Authorization": "Bearer " + token})
    assert response.status_code == 403
    assert response.json() == {"detail": "This endpoint requires interactive session authentication"}


def test_users_route_refuses_auth_disabled_callers(directory_client, monkeypatch):
    client, _config = directory_client
    client.cookies.clear()
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    response = client.get("/api/v1/auth/users", params={"query": "ad"})
    assert response.status_code == 403

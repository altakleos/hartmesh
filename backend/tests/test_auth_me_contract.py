"""GET /api/v1/auth/me keeps the shape frontend-hm's synthetic user expects.

frontend-hm's auth-disabled real-backend spec compares the whole payload with
``AUTH_DISABLED_USER`` plus the permission list. Upstream's account lifecycle
added a ``disabled`` flag for its admin user list; HartMesh refuses a disabled
account at sign-in instead, so the flag is not part of its /me contract.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from deerflow.config.authorization_config import AuthorizationConfig

_TEST_SECRET = "test-secret-key-auth-me-contract-min-32"


@pytest.fixture(autouse=True)
def _auth_engine(tmp_path, monkeypatch):
    """A fresh SQLite engine and auth config, as the /me permission tests use."""
    from app.gateway import deps
    from app.gateway.auth.config import AuthConfig, set_auth_config
    from deerflow.persistence.engine import close_engine, init_engine

    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: AuthorizationConfig())
    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/auth_me_contract.db", sqlite_dir=str(tmp_path)))
    deps._cached_local_provider = None
    deps._cached_repo = None
    try:
        yield
    finally:
        deps._cached_local_provider = None
        deps._cached_repo = None
        asyncio.run(close_engine())


def test_auth_disabled_me_matches_the_frontend_synthetic_user(monkeypatch):
    from app.gateway.app import create_app

    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    response = TestClient(create_app()).get("/api/v1/auth/me")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"id", "email", "system_role", "needs_setup", "oauth_provider", "permissions"}
    assert {key: payload[key] for key in ("id", "email", "system_role", "needs_setup", "oauth_provider")} == {
        "id": "default",
        "email": "default@test.local",
        "system_role": "admin",
        "needs_setup": False,
        "oauth_provider": None,
    }

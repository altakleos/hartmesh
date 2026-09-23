"""The provider-key routes: an administrator's session writes, nothing ever reads a key back.

The Gateway here is built on the compose profile's own rendered config, so
``/api/models`` and every other route that returns configuration answer
from the config the product's key was applied to.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-provider-keys-min-32")

from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture

from app.gateway.auth.config import AuthConfig, set_auth_config
from app.gateway.provider_keys.cipher import WRAPPING_KEY_ENV
from app.gateway.provider_keys.profile import PROFILE_DIR_ENV

_TEST_SECRET = "test-secret-key-provider-keys-min-32"
REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
SENTINEL = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-PRODUCT"
SEED = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-SEED"
_ADMIN = {"email": "owner@example.com", "password": "Str0ng!Pass99"}
_PERSON = {"email": "person@example.com", "password": "Tr0ub4dor3a!x"}


def _catalog_variables() -> list[str]:
    return sorted({yaml.safe_load(path.read_text(encoding="utf-8"))["env"] for path in (PROFILE / "providers").rglob("*.yaml")})


@pytest.fixture
def gateway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A Gateway on the profile's rendered config, a fresh database, and the provider-key service a start would build."""
    from app.gateway import deps
    from app.gateway.provider_keys.service import ProviderKeyService
    from app.gateway.routers.auth import _SETUP_STATUS_CACHE, _SETUP_STATUS_INFLIGHT
    from deerflow.config.app_config import reset_app_config
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    for name in _catalog_variables():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("HARTMESH_MODELS_FILE", raising=False)
    # Set by the rotation test; registered here so it never outlives one.
    monkeypatch.delenv("HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS", raising=False)
    values = {
        "DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow",
        "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0",
        "HARTMESH_LOCAL_PASSWORDS": "allowed",
        # Open, so a second, ordinary account can sign itself up.
        "HARTMESH_LOCAL_REGISTRATION": "open",
        PROFILE_DIR_ENV: str(PROFILE),
        WRAPPING_KEY_ENV: "w" * 44,
        "DEER_FLOW_CONFIG_PATH": str(tmp_path / "home" / "config.yaml"),
        "DEER_FLOW_HOME": str(tmp_path / "home"),
        "OPENAI_API_KEY": SEED,
        "DEER_FLOW_AUTH_DISABLED": "",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_provider_keys_routes", PROFILE / "gateway" / "render_config.py")
    renderer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = renderer
    spec.loader.exec_module(renderer)
    rendered, _ = renderer.render_text((PROFILE / "config.yaml").read_text(encoding="utf-8"), renderer.load_catalog(PROFILE / "providers"), os.environ)
    (tmp_path / "home").mkdir()
    (tmp_path / "home" / "config.yaml").write_text(rendered, encoding="utf-8")
    reset_app_config()

    set_auth_config(AuthConfig(jwt_secret=_TEST_SECRET))
    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/gateway.db", sqlite_dir=str(tmp_path)))
    deps._cached_local_provider = None
    deps._cached_repo = None
    _SETUP_STATUS_CACHE.clear()
    _SETUP_STATUS_INFLIGHT.clear()
    try:
        from app.gateway.app import create_app

        app = create_app()
        app.state.provider_keys = ProviderKeyService.from_environ(ProviderKeyRepository(get_session_factory()))
        yield app
    finally:
        deps._cached_local_provider = None
        deps._cached_repo = None
        _SETUP_STATUS_CACHE.clear()
        _SETUP_STATUS_INFLIGHT.clear()
        asyncio.run(close_engine())
        reset_app_config()


def _csrf(client: TestClient) -> dict[str, str]:
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token

    token = generate_csrf_token()
    client.cookies.set(CSRF_COOKIE_NAME, token)
    return {CSRF_HEADER_NAME: token}


def _admin(app) -> TestClient:
    client = TestClient(app)
    created = client.post("/api/v1/auth/initialize", json=_ADMIN)
    assert created.status_code == 201, created.text
    return client


def _person(app) -> TestClient:
    client = TestClient(app)
    registered = client.post("/api/v1/auth/register", json=_PERSON)
    assert registered.status_code == 201, registered.text
    return client


def _put(client: TestClient, provider: str, body) -> object:
    return client.put(f"/api/provider-keys/{provider}", json=body, headers=_csrf(client))


def test_an_administrator_sets_replaces_and_removes_a_key_and_reads_back_only_its_source(gateway) -> None:
    admin = _admin(gateway)

    listed = admin.get("/api/provider-keys")
    assert listed.status_code == 200, listed.text
    sources = {provider["provider"]: provider["source"] for provider in listed.json()["providers"]}
    assert (sources["openai"], sources["anthropic"]) == ("environment", "none")

    added = _put(admin, "anthropic", {"key": SENTINEL})
    assert added.status_code == 200, added.text
    assert added.headers["cache-control"] == "no-store"
    assert added.json()["action"] == "added"
    assert added.json()["provider"]["source"] == "product"
    assert SENTINEL not in added.text

    models = admin.get("/api/models").json()["models"]
    assert "claude-sonnet-4" in [model["name"] for model in models]

    replaced = _put(admin, "anthropic", {"key": SENTINEL + "-2"})
    assert replaced.json()["action"] == "replaced"

    removed = admin.request("DELETE", "/api/provider-keys/anthropic", headers=_csrf(admin))
    assert removed.status_code == 200, removed.text
    assert (removed.json()["action"], removed.json()["provider"]["source"]) == ("removed", "none")
    assert "claude-sonnet-4" not in [model["name"] for model in admin.get("/api/models").json()["models"]]

    events = admin.get("/api/provider-keys/events").json()["events"]
    assert [(event["variable"], event["action"], event["actor_email"]) for event in events] == [
        ("ANTHROPIC_API_KEY", "removed", _ADMIN["email"]),
        ("ANTHROPIC_API_KEY", "replaced", _ADMIN["email"]),
        ("ANTHROPIC_API_KEY", "added", _ADMIN["email"]),
    ]
    assert SENTINEL not in str(events)
    newest = admin.get("/api/provider-keys/events", params={"limit": 1}).json()["events"]
    assert [event["action"] for event in newest] == ["removed"]
    for limit in (0, 201):
        refused = admin.get("/api/provider-keys/events", params={"limit": limit})
        assert (refused.status_code, refused.json()["detail"]["code"]) == (422, "limit_invalid")


def test_no_route_that_returns_configuration_returns_the_key(gateway, caplog: pytest.LogCaptureFixture) -> None:
    """Every GET the Gateway mounts without a path parameter, and every model by name, after a key is set."""
    admin = _admin(gateway)
    with caplog.at_level("DEBUG"):
        assert _put(admin, "openai", {"key": SENTINEL}).status_code == 200
    assert SENTINEL not in caplog.text

    paths = sorted({route.path for route in gateway.routes if "GET" in getattr(route, "methods", set()) and "{" not in route.path and not route.path.startswith(("/docs", "/redoc", "/openapi"))})
    assert "/api/models" in paths and "/api/provider-keys" in paths
    names = [model["name"] for model in admin.get("/api/models").json()["models"]]
    assert "gpt-4" in names
    paths += [f"/api/models/{name}" for name in names]
    answered = {}
    for path in paths:
        response = admin.get(path)
        answered[path] = response.status_code
        assert SENTINEL not in response.text, path
        assert SEED not in response.text, path
    # The sweep read real answers, not a wall of refusals.
    assert all(answered[path] == 200 for path in ("/api/models", "/api/models/gpt-4", "/api/provider-keys", "/api/features", "/health")), answered


def test_a_person_who_is_not_an_administrator_is_refused_every_route(gateway) -> None:
    _admin(gateway)
    person = _person(gateway)
    assert person.get("/api/provider-keys").status_code == 403
    assert person.get("/api/provider-keys/events").status_code == 403
    assert _put(person, "openai", {"key": SENTINEL}).status_code == 403
    assert person.request("DELETE", "/api/provider-keys/openai", headers=_csrf(person)).status_code == 403


def test_an_administrators_personal_access_token_is_refused_every_route(gateway) -> None:
    from deerflow.persistence.engine import get_session_factory
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    gateway.state.pat_repo = PersonalAccessTokenRepository(get_session_factory(), tenant=TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference())
    admin = _admin(gateway)
    minted = admin.post("/api/v1/auth/pats", json={"name": "automation", "scopes": ["threads:read", "threads:write"]}, headers=_csrf(admin))
    assert minted.status_code == 201, minted.text
    bearer = TestClient(gateway, headers={"Authorization": f"Bearer {minted.json()['token']}"})
    assert bearer.get("/api/provider-keys").status_code in {401, 403}
    assert bearer.put("/api/provider-keys/openai", json={"key": SENTINEL}).status_code in {401, 403}
    assert bearer.delete("/api/provider-keys/openai").status_code in {401, 403}
    assert bearer.get("/api/provider-keys/events").status_code in {401, 403}
    assert _put(admin, "openai", {"key": SENTINEL}).status_code == 200


def test_every_route_itself_requires_an_interactive_session() -> None:
    """Not only the middleware's default for routes it does not list: the router refuses a token too."""
    from app.gateway.routers import provider_keys
    from app.gateway.routers.auth import require_session_source

    for route in provider_keys.router.routes:
        assert any(dependency.call is require_session_source for dependency in route.dependant.dependencies), route.path


@pytest.mark.parametrize(
    "body",
    [{"api_key": SENTINEL}, {"key": SENTINEL, "base_url": "https://api.acme.invalid"}, [SENTINEL], {"key": [SENTINEL]}, SENTINEL],
    ids=["wrong-name", "extra-field", "list", "not-a-string", "bare-string"],
)
def test_a_body_it_cannot_read_is_refused_without_quoting_it(gateway, body) -> None:
    admin = _admin(gateway)
    refused = _put(admin, "openai", body)
    assert refused.status_code == 422
    assert refused.json()["detail"]["code"] == "body_invalid"
    assert SENTINEL not in refused.text


@pytest.mark.parametrize(
    "content",
    [b"[" * 100_000, b'{"key": "' + b"k" * 70_000 + b'"}'],
    ids=["nested-past-the-parser", "past-the-size-limit"],
)
def test_a_body_too_deep_or_too_large_to_be_a_key_gets_the_same_fixed_refusal(gateway, content) -> None:
    admin = _admin(gateway)
    refused = admin.put("/api/provider-keys/openai", content=content, headers={**_csrf(admin), "Content-Type": "application/json"})
    assert (refused.status_code, refused.json()["detail"]["code"]) == (422, "body_invalid")


def test_a_provider_outside_the_catalog_is_refused(gateway) -> None:
    admin = _admin(gateway)
    refused = _put(admin, "acme", {"key": SENTINEL})
    assert refused.status_code == 404
    assert refused.json()["detail"]["code"] == "unknown_provider"


def test_where_keys_are_not_managed_in_the_product_the_list_says_so_and_writes_are_refused(gateway) -> None:
    gateway.state.provider_keys = None
    admin = _admin(gateway)
    listed = admin.get("/api/provider-keys").json()
    assert (listed["available"], listed["refusal"]["code"], listed["providers"]) == (False, "not_available", [])
    refused = _put(admin, "openai", {"key": SENTINEL})
    assert (refused.status_code, refused.json()["detail"]["code"]) == (409, "not_available")
    removed = admin.request("DELETE", "/api/provider-keys/openai", headers=_csrf(admin))
    assert (removed.status_code, removed.json()["detail"]["code"]) == (409, "not_available")
    record = admin.get("/api/provider-keys/events")
    assert (record.status_code, record.json()["detail"]["code"]) == (409, "not_available")

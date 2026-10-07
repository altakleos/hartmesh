"""Default-denied mutations, owned reads, and provider-only public state."""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

from _router_auth_helpers import make_authed_test_app
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.deps import get_config
from app.gateway.routers import skills as skills_router
from deerflow.config.authorization_config import AuthorizationConfig


def _make_user(system_role: str) -> User:
    return User(email=f"{system_role}-test@example.com", password_hash="x", system_role=system_role, id=uuid4())


def _make_app(*, system_role: str, delegated: bool = False) -> FastAPI:
    config = SimpleNamespace(
        skills=SimpleNamespace(get_skills_path=lambda: "/tmp/skills", container_path="/mnt/skills", use="deerflow.skills.storage.local_skill_storage:LocalSkillStorage"),
        skill_evolution=SimpleNamespace(enabled=True, moderation_model_name=None),
        authorization=AuthorizationConfig(enabled=False),
    )
    user = _make_user(system_role)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.state.test_owner = user
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy

    app.state.customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=delegated)
    app.state.config = config
    app.dependency_overrides[get_config] = lambda: config
    app.include_router(skills_router.router)
    return app


# Mutations denied before business logic when provider delegation is absent.
_GUARDED_ENDPOINTS = [
    ("post", "/api/skills/install", {"thread_id": "t1", "path": "mnt/user-data/outputs/x.skill"}),
    ("post", "/api/skills/reload", None),
    ("put", "/api/skills/custom/demo", {"content": "---\nname: demo\ndescription: hijacked\n---\n"}),
    ("delete", "/api/skills/custom/demo", None),
    ("post", "/api/skills/custom/demo/rollback", {"history_index": -1}),
    ("put", "/api/skills/demo", {"enabled": False}),
]


def test_undelegated_owner_is_forbidden_on_all_mutating_skills_endpoints():
    """Default denial occurs before paths, scans or storage access."""
    app = _make_app(system_role="user")
    with TestClient(app) as client:
        for method, path, body in _GUARDED_ENDPOINTS:
            resp = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
            assert resp.status_code == 403, f"{method.upper()} {path} expected default-denied 403, got {resp.status_code}"


def test_undelegated_upload_is_rejected_before_multipart_parsing(monkeypatch):
    parse_called = False

    async def _unexpected_parse(request):
        nonlocal parse_called
        parse_called = True
        raise AssertionError("multipart parsing ran before delegation admission")

    monkeypatch.setattr(skills_router, "_parse_skill_archive_form", _unexpected_parse)
    app = _make_app(system_role="user")

    with TestClient(app) as client:
        response = client.post(
            "/api/skills/install/upload",
            files={"archive": ("demo.skill", b"archive bytes", "application/octet-stream")},
        )

    assert response.status_code == 403
    assert parse_called is False


def test_basic_skill_listing_stays_open_to_normal_users(monkeypatch, tmp_path):
    """The basic list/detail endpoints expose only name/description and are
    needed by the normal-user UI, so they must NOT be admin-gated.

    Under per-user skill isolation, ``list_custom_skills`` (GET /api/skills/custom)
    is also open to normal users — they see only their own custom skills.
    """

    def _load_skills(*, enabled_only: bool):
        from pathlib import Path

        from deerflow.skills.types import Skill

        return [
            Skill(
                name="demo",
                description="d",
                license="MIT",
                skill_dir=Path("/tmp/demo"),
                skill_file=Path("/tmp/demo/SKILL.md"),
                relative_path=Path("demo"),
                category="public",
                enabled=True,
            )
        ]

    app = _make_app(system_role="user")
    # list_skills reads config.authorization.fail_closed even when
    # authorization is disabled (mirroring list_models); give the fake the
    # real disabled shape so the open-to-normal-users path stays exercised.
    app.dependency_overrides[get_config] = lambda: SimpleNamespace(authorization=AuthorizationConfig(enabled=False))
    from deerflow.config.paths import Paths
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=tmp_path))
    storage = UserScopedSkillStorage(str(app.state.test_owner.id), host_path=str(tmp_path / "skills"))
    monkeypatch.setattr(storage, "load_skills", _load_skills)
    monkeypatch.setattr(skills_router, "_get_user_skill_storage", lambda cfg: storage)
    with TestClient(app) as client:
        assert client.get("/api/skills").status_code == 200
        assert client.get("/api/skills/custom").status_code == 200
        assert client.get("/api/skills/demo").status_code == 200


def test_customer_admin_cannot_toggle_a_public_baseline(monkeypatch, tmp_path):
    """Private delegation does not grant deployment-wide baseline writes."""
    from pathlib import Path

    from deerflow.skills.types import Skill

    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps(
            {
                "mcpServers": {},
                "skills": {},
                "middlewares": ["pkg:Middleware"],
                "mcpInterceptors": ["pkg.interceptor:build"],
            }
        ),
        encoding="utf-8",
    )

    def _load_skills(*, enabled_only: bool):
        return [
            Skill(
                name="demo",
                description="d",
                license="MIT",
                skill_dir=Path("/tmp/demo"),
                skill_file=Path("/tmp/demo/SKILL.md"),
                relative_path=Path("demo"),
                category="public",
                enabled=True,
            )
        ]

    app = _make_app(system_role="admin", delegated=True)
    # Not a real LocalSkillStorage instance, so _write_extensions_skill_state's
    # projection-mutation branch is skipped (nullcontext) and it reads the
    # config_path fresh via ExtensionsConfig.from_file.
    monkeypatch.setattr(skills_router, "_get_user_skill_storage", lambda cfg: SimpleNamespace(load_skills=_load_skills))
    monkeypatch.setattr(skills_router, "reload_extensions_config", lambda: None)
    monkeypatch.setattr(skills_router.ExtensionsConfig, "resolve_config_path", staticmethod(lambda _config_path=None: config_path))

    async def _refresh(_user_id: str):
        return None

    monkeypatch.setattr(skills_router, "refresh_user_skills_system_prompt_cache_async", _refresh)
    before = config_path.read_bytes()
    with TestClient(app) as client:
        resp = client.put("/api/skills/demo", json={"enabled": False})
        assert resp.status_code == 403, f"public baseline toggle must be denied, got {resp.status_code}"
    assert config_path.read_bytes() == before
    written = json.loads(config_path.read_text(encoding="utf-8"))
    assert written["middlewares"] == ["pkg:Middleware"]
    assert written["mcpInterceptors"] == ["pkg.interceptor:build"]

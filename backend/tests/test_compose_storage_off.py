"""Inactive managed storage retains the ordinary file and sandbox paths."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import yaml
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient


def test_compose_does_not_explicitly_install_storage_features():
    template = Path(__file__).resolve().parents[2] / "deploy/compose/config.yaml"
    config = yaml.safe_load(template.read_text(encoding="utf-8"))
    assert not any(name in str(config.get("plugins", [])) for name in ("hm.my-files", "hm.shared", "hm.projects", "deerflow.features"))


def test_disabled_storage_keeps_legacy_chat_binds(tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox import aio_sandbox_provider as aio
    from deerflow.config.paths import Paths

    paths = Paths(tmp_path)
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(storage_spaces=SimpleNamespace(enabled=False)))
    monkeypatch.setattr(aio, "get_paths", lambda: paths)
    mounts = {target: (Path(source), readonly) for source, target, readonly in aio.AioSandboxProvider._get_thread_mounts("conversation", user_id="alice")}
    for folder in ("workspace", "uploads", "outputs"):
        assert mounts[f"/mnt/user-data/{folder}"] == (tmp_path / "users/alice/threads/conversation/user-data" / folder, False)
    assert mounts["/mnt/user-data/files"] == (tmp_path / "users/alice/files", False)
    assert mounts["/mnt/user-data/shared"] == (tmp_path / "shared", True)
    assert mounts["/mnt/acp-workspace"] == (tmp_path / "users/alice/threads/conversation/acp-workspace", True)


def test_disabled_storage_dispatches_file_routes_to_legacy_services(monkeypatch):
    from contextlib import asynccontextmanager

    import _router_auth_helpers

    from app.gateway.auth.models import User
    from app.gateway.routers import files, projects, shared

    @asynccontextmanager
    async def shared_lock(_repo):
        yield

    user = User(email="storage-test@example.com", password_hash="x", system_role="user")
    monkeypatch.setattr(_router_auth_helpers, "_STUB_PERMISSIONS", [*_router_auth_helpers._STUB_PERMISSIONS, "projects:read"])
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True)
    app.state.storage_spaces_enabled = False
    own_files = Mock(return_value=([], False))
    shared_files = Mock(return_value=([], False))
    project_repo = SimpleNamespace(list=AsyncMock(return_value=[]))
    publication_repo = SimpleNamespace(live_publications=AsyncMock(return_value={}))
    monkeypatch.setattr(files, "list_user_files", own_files)
    monkeypatch.setattr(shared, "list_shared_files", shared_files)
    monkeypatch.setattr(shared, "_shared_mutation", shared_lock)
    monkeypatch.setattr(shared, "get_shared_publications_repo", lambda _request: publication_repo)
    monkeypatch.setattr(projects, "get_project_repo", lambda _request: project_repo)
    for router in (files.router, shared.router, projects.router):
        app.include_router(router)
    with TestClient(app) as client:
        for route in ("/api/files", "/api/shared", "/api/projects"):
            assert client.get(route).status_code == 200
    own_files.assert_called_once()
    shared_files.assert_called_once()
    project_repo.list.assert_awaited_once()
    publication_repo.live_publications.assert_awaited_once()

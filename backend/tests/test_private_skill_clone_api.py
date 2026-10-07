"""Owner clone APIs expose registered identities and preserve real scanners."""

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.deps import get_config
from app.gateway.routers import skills
from deerflow.config.app_config import AppConfig
from deerflow.config.paths import Paths
from deerflow.runtime.customer_administration import CustomerAdministrationPolicy
from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage


@pytest.fixture
def clone_app(tmp_path, monkeypatch):
    from deerflow.skills.security_scanner import ScanResult

    paths = Paths(base_dir=tmp_path / "home")
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: paths)
    config = AppConfig.model_validate({"sandbox": {"use": "test"}, "skills": {"path": str(tmp_path / "skills")}})
    user = User(email="clone-owner@example.com", password_hash="unused", system_role="user")
    store = UserScopedSkillStorage(str(user.id), app_config=config)
    baseline = paths.integration_skills_dir() / "provider" / "office" / "helper"
    baseline.mkdir(parents=True)
    (baseline / "SKILL.md").write_text("---\nname: helper\ndescription: Provider workflow\n---\nPrepare ordinary notes.\n", encoding="utf-8")
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.state.customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    app.dependency_overrides[get_config] = lambda: config
    app.include_router(skills.router)
    monkeypatch.setattr(skills, "_get_user_skill_storage", lambda _: store)
    scans = []

    async def scan(content, **kwargs):
        scans.append(content)
        return ScanResult(decision="allow", reason="Offline model decision")

    async def refresh(owner):
        assert owner == str(user.id)

    monkeypatch.setattr("deerflow.skills.installer.scan_skill_content", scan)
    monkeypatch.setattr(skills, "refresh_user_skills_system_prompt_cache_async", refresh)
    return app, store, baseline, scans


def test_source_preview_then_private_clone_exposes_origin_without_host_paths(clone_app):
    app, store, baseline, scans = clone_app
    before = (baseline / "SKILL.md").read_bytes()
    with TestClient(app) as client:
        sources = client.get("/api/skills/clone-sources")
        assert sources.status_code == 200, sources.text
        source = sources.json()["sources"][0]
        assert str(baseline) not in sources.text
        preview = client.post("/api/skills/clone-preview", json={"source_id": source["source_id"]})
        assert preview.status_code == 200, preview.text
        response = client.post("/api/skills/clone", json={"source_id": source["source_id"], "expected_revision": preview.json()["revision"]})
        assert response.status_code == 200, response.text
        assert response.json()["skill_name"] == "helper-private"
        listing = client.get("/api/skills").json()["skills"]
        clone = next(s for s in listing if s["name"] == "helper-private")
        assert clone["origin"]["source_name"] == "helper"
    assert scans
    assert (baseline / "SKILL.md").read_bytes() == before
    assert store.custom_skill_exists("helper-private")


def test_clone_floor_denies_before_source_capture_or_scan(clone_app):
    app, store, _, scans = clone_app
    app.state.customer_administration_policy = CustomerAdministrationPolicy()
    with TestClient(app) as client:
        response = client.post("/api/skills/clone", json={"source_id": "integrations:provider/office/helper", "expected_revision": "a" * 64})
    assert response.status_code == 403
    assert not scans
    assert not store.custom_skill_exists("helper-private")


def test_clone_visibility_denial_and_arbitrary_source_ids_do_not_capture_files(clone_app, monkeypatch):
    app, store, _, scans = clone_app
    from types import SimpleNamespace

    monkeypatch.setattr(skills, "resolve_skill_authorization", lambda *args, **kwargs: (SimpleNamespace(filter_resources=lambda *args: []), object()))
    with TestClient(app) as client:
        assert client.get("/api/skills/clone-sources").json()["sources"] == []
        hidden = client.post("/api/skills/clone-preview", json={"source_id": "integrations:provider/office/helper"})
        assert hidden.status_code == 404
        arbitrary = client.post("/api/skills/clone-preview", json={"source_id": "/etc/passwd"})
        assert arbitrary.status_code == 404
    assert not scans
    assert not store.custom_skill_exists("helper-private")


def test_upload_explicit_override_and_target_visibility_are_enforced_before_scan(clone_app, tmp_path, monkeypatch):
    import zipfile
    from types import SimpleNamespace

    app, store, _, scans = clone_app
    archive = tmp_path / "helper.skill"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("helper/SKILL.md", "---\nname: helper\ndescription: Owner copy\n---\nPrepare ordinary notes.\n")
    with TestClient(app) as client:
        denied = client.post("/api/skills/install/upload", files={"archive": (archive.name, archive.read_bytes())})
        assert denied.status_code == 400
        monkeypatch.setattr(skills, "resolve_skill_authorization", lambda *args, **kwargs: (SimpleNamespace(filter_resources=lambda *args: []), object()))
        hidden = client.post("/api/skills/install/upload", files={"archive": (archive.name, archive.read_bytes())}, data={"allow_baseline_override": "true"})
        assert hidden.status_code == 404, hidden.text
        assert not scans
        monkeypatch.setattr(skills, "resolve_skill_authorization", lambda *args, **kwargs: (None, None))
        installed = client.post("/api/skills/install/upload", files={"archive": (archive.name, archive.read_bytes())}, data={"allow_baseline_override": "true"})
        assert installed.status_code == 200, installed.text
    assert scans
    assert store.custom_skill_exists("helper")


def test_clone_respects_existing_export_capacity_before_capture(clone_app, monkeypatch):
    from app.gateway.skill_export import ExportLease

    app, store, _, scans = clone_app
    leases = [ExportLease.acquire(), ExportLease.acquire()]
    try:
        monkeypatch.setattr("deerflow.skills.private_variants.clone_provider_skill", lambda *args, **kwargs: pytest.fail("clone reached capture without capacity"))
        with TestClient(app) as client:
            response = client.post("/api/skills/clone", json={"source_id": "integrations:provider/office/helper", "expected_revision": "a" * 64})
        assert response.status_code == 429, response.text
    finally:
        for lease in leases:
            lease.release()
    assert not scans
    assert not store.custom_skill_exists("helper-private")

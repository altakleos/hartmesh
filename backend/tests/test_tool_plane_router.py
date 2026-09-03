"""HTTP contract and authorization for governed tool-plane revisions."""

from __future__ import annotations

import io
import zipfile
from uuid import uuid4

from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.auth.pat import is_pat_allowed_route
from app.gateway.authz import _ALL_PERMISSIONS
from app.gateway.routers import tool_plane as tool_plane_router
from deerflow.tool_plane import (
    DeterministicToolPlaneValidator,
    GovernedSkillArtifactStore,
    InMemoryToolPlaneProjection,
    InMemoryToolPlaneRevisionRepository,
    ToolPlaneRevisionService,
)

_POLICY = "b" * 64


def _user(role: str = "admin") -> User:
    return User(
        email=f"{role}@example.com",
        password_hash="x",
        system_role=role,
        id=uuid4(),
    )


def _app(*, immutable: bool = False, role: str = "admin"):
    app = make_authed_test_app(user_factory=lambda: _user(role))
    tenant = app.state.tenant_identity.to_persisted_reference()
    app.state.tool_plane_revision_service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=tenant),
        projection=InMemoryToolPlaneProjection(),
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=True,
        immutable=immutable,
        authority_universe=tuple(_ALL_PERMISSIONS),
    )
    app.include_router(tool_plane_router.read_router)
    if not immutable:
        app.include_router(tool_plane_router.mutation_router)
    return app


def test_admin_can_stage_validate_inspect_and_promote_base_revision() -> None:
    with TestClient(_app()) as client:
        staged_response = client.post(
            "/api/tool-plane/revisions",
            json={
                "scope_kind": "deployment_base",
                "candidate": {
                    "validation_policy_digest": _POLICY,
                    "mcp_servers": {},
                    "public_skills": {},
                    "managed_integrations": {},
                },
            },
        )
        assert staged_response.status_code == 201
        staged = staged_response.json()
        assert staged["state"] == "staged"
        revision_id = staged["revision_id"]

        inspected = client.get(f"/api/tool-plane/revisions/{revision_id}")
        assert inspected.status_code == 200
        assert inspected.json()["manifest"]["kind"] == "deployment_base"

        validated = client.post(
            f"/api/tool-plane/revisions/{revision_id}/validate",
            json={"scope_kind": "deployment_base"},
        )
        assert validated.status_code == 200
        assert validated.json()["result"] == "passed"

        promoted = client.post(
            f"/api/tool-plane/revisions/{revision_id}/promote",
            json={"scope_kind": "deployment_base"},
        )
        assert promoted.status_code == 200
        assert promoted.json()["state"] == "promoted"

        status = client.get(
            "/api/tool-plane/status",
            params={"scope_kind": "deployment_base"},
        )
        assert status.status_code == 200
        assert status.json()["governance_state"] == "governed"
        assert status.json()["active_revision_digest"] == staged["revision_digest"]


def test_non_admin_cannot_stage_deployment_base() -> None:
    with TestClient(_app(role="user")) as client:
        response = client.post(
            "/api/tool-plane/revisions",
            json={
                "scope_kind": "deployment_base",
                "candidate": {
                    "validation_policy_digest": _POLICY,
                    "mcp_servers": {},
                },
            },
        )

    assert response.status_code == 403


def test_exact_two_immutable_service_exposes_no_live_mutation_or_bootstrap_route() -> None:
    with TestClient(_app(immutable=True)) as client:
        paths = client.get("/openapi.json").json()["paths"]
        stage = client.post(
            "/api/tool-plane/revisions",
            json={"scope_kind": "deployment_base", "candidate": {}},
        )
        bootstrap = client.post("/api/tool-plane/bootstrap/stage-current")
        status = client.get(
            "/api/tool-plane/status",
            params={"scope_kind": "deployment_base"},
        )

    assert stage.status_code == 405
    assert bootstrap.status_code == 404
    assert status.status_code == 200
    assert status.json()["governance_state"] == "immutable"
    assert "post" not in paths["/api/tool-plane/revisions"]
    assert "/api/tool-plane/bootstrap/stage-current" not in paths


def test_personal_access_tokens_have_no_tool_plane_management_route() -> None:
    for method, suffix in (
        ("GET", "/status"),
        ("POST", "/revisions"),
        ("POST", "/revisions/rev/promote"),
    ):
        assert not is_pat_allowed_route(method, f"/api/tool-plane{suffix}")


def test_skill_archive_upload_only_stages_candidate_bytes(tmp_path) -> None:
    app = _app()
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    app.state.tool_plane_revision_service._artifact_store = store
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "helper/SKILL.md",
            "---\nname: helper\ndescription: Helps safely\n---\n\n# Instructions\n",
        )
    stream.seek(0)

    with TestClient(app) as client:
        response = client.post(
            "/api/tool-plane/skill-artifacts",
            data={"scope_kind": "deployment_base"},
            files={"archive": ("helper.skill", stream, "application/zip")},
        )
        status = client.get(
            "/api/tool-plane/status",
            params={"scope_kind": "deployment_base"},
        )

    assert response.status_code == 201
    assert response.json()["skill_name"] == "helper"
    assert set(response.json()) >= {
        "archive_digest",
        "tree_digest",
        "manifest_digest",
    }
    assert status.json()["active_revision_id"] is None

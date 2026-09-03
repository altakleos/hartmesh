"""Protected skill candidate staging and exact-material validation."""

from __future__ import annotations

import io
import json
import stat
import zipfile

import pytest
from test_tool_plane_service import _POLICY, _TENANT, _actor

from deerflow.tool_plane import (
    GovernedSkillArtifactStore,
    GovernedToolPlaneValidator,
    InMemoryToolPlaneProjection,
    InMemoryToolPlaneRevisionRepository,
    LockedFileToolPlaneProjection,
    ScopedStageRevisionRequest,
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    ToolPlaneRevisionService,
    user_scope_reference,
)


def _archive(
    *,
    skill_markdown: str | None = None,
    extra: dict[str, bytes] | None = None,
) -> io.BytesIO:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "helper/SKILL.md",
            skill_markdown or "---\nname: helper\ndescription: Helps safely\n---\n\n# Instructions\nDo the work.\n",
        )
        for path, content in (extra or {}).items():
            archive.writestr(path, content)
    stream.seek(0)
    return stream


def _candidate(artifact) -> dict[str, object]:
    return {
        "validation_policy_digest": _POLICY,
        "mcp_servers": {},
        "public_skills": {
            artifact.skill_name: {
                "enabled": True,
                "archive_digest": artifact.archive_digest,
                "tree_digest": artifact.tree_digest,
                "manifest_digest": artifact.manifest_digest,
                "entry_points": list(artifact.entry_points),
            }
        },
        "managed_integrations": {},
    }


def test_staged_archive_is_immutable_and_safe_result_contains_no_path_or_bytes(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")

    first = store.stage_archive(_archive())
    second = store.stage_archive(_archive(extra={"helper/reference.txt": b"changed"}))

    assert first.skill_name == "helper"
    assert first.entry_points == ("SKILL.md",)
    assert first.archive_digest != second.archive_digest
    assert first.tree_digest != second.tree_digest
    payload = first.to_safe_json()
    assert set(payload) == {
        "version",
        "artifact_ref",
        "skill_name",
        "archive_digest",
        "tree_digest",
        "manifest_digest",
        "entry_points",
        "staged_at",
    }
    assert str(tmp_path) not in str(payload)
    assert "Instructions" not in str(payload)


@pytest.mark.parametrize("unsafe_name", ["../escape", "/absolute"])
def test_governed_stage_maps_unsafe_paths_to_typed_safe_error(
    tmp_path,
    unsafe_name,
) -> None:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(unsafe_name, "do not expose this payload")
    stream.seek(0)
    store = GovernedSkillArtifactStore(tmp_path / "candidates")

    with pytest.raises(ToolPlaneRevisionError) as caught:
        store.stage_archive(stream)

    assert caught.value.code == "unsafe_archive"
    assert "do not expose" not in str(caught.value)
    assert caught.value.safe_details == {}


def test_governed_stage_rejects_symlink_member(tmp_path) -> None:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        info = zipfile.ZipInfo("helper/link")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "/private/secret")
    stream.seek(0)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        GovernedSkillArtifactStore(tmp_path / "candidates").stage_archive(stream)

    assert caught.value.code == "unsafe_archive"


@pytest.mark.asyncio
async def test_validator_runs_review_for_exact_staged_skill_material(tmp_path) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive())
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_candidate(artifact),
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "passed"
    assert report.validator_versions["skill_review"]
    assert report.validator_versions["skillscan"]


@pytest.mark.asyncio
async def test_validator_rejects_unstaged_or_structurally_invalid_skill(tmp_path) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    invalid = store.stage_archive(
        _archive(
            skill_markdown="---\nname: helper\n---\n\nNo description.\n",
        )
    )
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    candidate = _candidate(invalid)
    candidate["public_skills"]["helper"]["tree_digest"] = "f" * 64
    missing = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=candidate,
        ),
        admin,
    )

    missing_report = await service.validate(missing.revision_id, admin)

    assert missing_report.result == "failed"
    assert {finding.code for finding in missing_report.findings} == {"skill_artifact_not_staged"}

    invalid_revision = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_candidate(invalid),
        ),
        admin,
    )
    invalid_report = await service.validate(invalid_revision.revision_id, admin)
    assert invalid_report.result == "failed"
    assert "structure.missing-description" in {finding.code for finding in invalid_report.findings}


@pytest.mark.asyncio
async def test_validator_rejects_public_and_integration_skill_name_collision(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive())
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    candidate = _candidate(artifact)
    candidate["managed_integrations"] = {
        "helper": candidate["public_skills"]["helper"],
    }
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=candidate,
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "failed"
    assert "base_skill_name_conflict" in {finding.code for finding in report.findings}


@pytest.mark.asyncio
async def test_validator_rejects_unsafe_mcp_execution_and_endpoint_policy(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {
                    "shell": {
                        "type": "stdio",
                        "command": "sh",
                        "args": ["-c", "run-server"],
                    },
                    "metadata": {
                        "type": "http",
                        "url": "http://127.0.0.1:8080/mcp",
                    },
                },
                "public_skills": {},
                "managed_integrations": {},
            },
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "failed"
    assert {finding.code for finding in report.findings} >= {
        "mcp_command_not_allowed",
        "mcp_private_endpoint_not_allowed",
    }


@pytest.mark.asyncio
async def test_validator_rejects_invalid_mcp_runtime_schema_before_projection(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    projection = InMemoryToolPlaneProjection()
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "oauth": {
                            "enabled": True,
                            "token_url": "https://auth.example.test/token",
                            "grant_type": "unsupported",
                        },
                    }
                },
            },
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "failed"
    assert "mcp_schema_invalid" in {finding.code for finding in report.findings}
    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(staged.revision_id, admin)
    assert caught.value.code == "validation_failed"
    assert projection.project_count == 0


@pytest.mark.asyncio
async def test_overlay_cannot_widen_base_or_select_another_credential_binding(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {
                    "search": {
                        "enabled": False,
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "credential_binding_id": "search-primary",
                        "credential_version": 2,
                    }
                },
                "public_skills": {},
                "managed_integrations": {},
            },
        ),
        admin,
    )
    await service.validate(base.revision_id, admin)
    await service.promote(base.revision_id, admin)
    overlay = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(
                kind="user_overlay",
                user_ref=user_scope_reference(user),
            ),
            candidate={
                "base_revision_digest": base.revision_digest,
                "mcp_enablement": {"search": True},
                "credential_selectors": {
                    "search": {
                        "binding_ref": "search-secondary",
                        "version": 1,
                    }
                },
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(overlay.revision_id, user)

    assert caught.value.code == "overlay_preflight_failed"


@pytest.mark.asyncio
async def test_promotion_projects_exact_staged_skill_bytes_and_detects_drift(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    skills_root = tmp_path / "skills"
    integrations_root = tmp_path / "integrations"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive())
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        skills_root=skills_root,
        integrations_root=integrations_root,
        artifact_store=store,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
        ),
        artifact_store=store,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_candidate(artifact),
        ),
        admin,
    )
    await service.validate(staged.revision_id, admin)

    await service.promote(staged.revision_id, admin)

    active_skill = skills_root / "public" / "helper" / "SKILL.md"
    assert active_skill.read_text(encoding="utf-8").endswith("Do the work.\n")
    active_skill.write_text("direct unmanaged edit", encoding="utf-8")
    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.effective_for_actor(_actor("user-1"))
    assert caught.value.code == "unmanaged_drift"

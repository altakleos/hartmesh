"""Protected skill candidate staging and exact-material validation."""

from __future__ import annotations

import io
import ipaddress
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
    skill_entry = {
        "enabled": True,
        "archive_digest": artifact.archive_digest,
        "tree_digest": artifact.tree_digest,
        "manifest_digest": artifact.manifest_digest,
        "entry_points": list(artifact.entry_points),
    }
    if artifact.declared_version is not None:
        skill_entry["version"] = artifact.declared_version
    return {
        "validation_policy_digest": _POLICY,
        "mcp_servers": {},
        "public_skills": {
            artifact.skill_name: skill_entry,
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
        "declared_version",
        "archive_digest",
        "tree_digest",
        "manifest_digest",
        "entry_points",
        "staged_at",
    }
    assert str(tmp_path) not in str(payload)
    assert "Instructions" not in str(payload)


def test_staged_artifact_binds_declared_version_and_entry_points(tmp_path) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive(skill_markdown=("---\nname: helper\ndescription: Helps safely\nversion: 1.2.3-alpha.1+build.7\n---\n\n# Instructions\nDo the work.\n")))

    assert artifact.declared_version == "1.2.3-alpha.1+build.7"
    assert artifact.entry_points == ("SKILL.md",)
    assert artifact.to_safe_json()["declared_version"] == "1.2.3-alpha.1+build.7"


@pytest.mark.parametrize(
    "version",
    ["1.2", "01.2.3", "1.2.3-01", "v1.2.3", "1.2.٣", 123],
)
def test_staged_artifact_rejects_non_semver_declared_version(
    tmp_path,
    version: object,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")

    with pytest.raises(ToolPlaneRevisionError) as caught:
        store.stage_archive(_archive(skill_markdown=(f"---\nname: helper\ndescription: Helps safely\nversion: {version}\n---\n\n# Instructions\nDo the work.\n")))

    assert caught.value.code == "validation_failed"
    assert caught.value.safe_details == {
        "field": "SKILL.md.version",
        "reason": "invalid_semantic_version",
    }


def test_artifact_verification_recomputes_declared_metadata(tmp_path) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive(skill_markdown=("---\nname: helper\ndescription: Helps safely\nversion: 1.2.3\n---\n\n# Instructions\nDo the work.\n")))
    metadata_path = next((tmp_path / "candidates").rglob("metadata.json"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["declared_version"] = "9.9.9"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ToolPlaneRevisionError) as caught:
        store.verify(
            tree_digest=artifact.tree_digest,
            archive_digest=artifact.archive_digest,
            manifest_digest=artifact.manifest_digest,
        )

    assert caught.value.code == "skill_artifact_not_staged"


def test_distinct_archives_for_the_same_tree_remain_independently_verifiable(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    content = "---\nname: helper\ndescription: Helps safely\n---\n\n# Instructions\nDo the work.\n"

    def archive_with(compression: int, timestamp: tuple[int, int, int, int, int, int]) -> io.BytesIO:
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w", compression) as archive:
            info = zipfile.ZipInfo("helper/SKILL.md", date_time=timestamp)
            info.compress_type = compression
            archive.writestr(info, content)
        stream.seek(0)
        return stream

    compressed = store.stage_archive(archive_with(zipfile.ZIP_DEFLATED, (2020, 1, 1, 0, 0, 0)))
    stored = store.stage_archive(archive_with(zipfile.ZIP_STORED, (2021, 1, 1, 0, 0, 0)))

    assert compressed.tree_digest == stored.tree_digest
    assert compressed.archive_digest != stored.archive_digest
    assert (
        store.verify(
            tree_digest=compressed.tree_digest,
            archive_digest=compressed.archive_digest,
            manifest_digest=compressed.manifest_digest,
        ).metadata
        == compressed
    )
    assert (
        store.verify(
            tree_digest=stored.tree_digest,
            archive_digest=stored.archive_digest,
            manifest_digest=stored.manifest_digest,
        ).metadata
        == stored
    )


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
async def test_validator_preserves_integration_shadow_of_public_skill(
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
    user = _actor("user-1")
    candidate = _candidate(artifact)
    candidate["public_skills"]["helper"]["enabled"] = False
    candidate["managed_integrations"] = {
        "helper": {
            **candidate["public_skills"]["helper"],
            "provider": "example-provider",
            "enabled": True,
        },
    }
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=candidate,
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "passed"
    await service.promote(staged.revision_id, admin)
    effective = await service.effective_for_actor(user)
    assert effective.effective_global_skill_states == ({"name": "helper", "enabled": True},)


@pytest.mark.asyncio
async def test_custom_skill_can_shadow_a_base_skill(tmp_path) -> None:
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
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_candidate(artifact),
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
                "custom_skills": {
                    "helper": {
                        "enabled": True,
                        "archive_digest": artifact.archive_digest,
                        "tree_digest": artifact.tree_digest,
                        "manifest_digest": artifact.manifest_digest,
                        "entry_points": list(artifact.entry_points),
                    }
                },
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    promoted = await service.promote(overlay.revision_id, user)

    assert promoted.state == "promoted"
    assert (await service.effective_for_actor(user)).user_overlay_digest == (overlay.revision_digest)


@pytest.mark.asyncio
async def test_validator_enforces_provider_and_skill_capability_policy(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive(skill_markdown=("---\nname: helper\ndescription: Helps safely\nallowed-tools:\n  - bash\n---\n\n# Instructions\nDo the work.\n")))
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
            allowed_managed_integration_providers=("approved-provider",),
            forbidden_skill_capabilities=("tool:bash",),
        ),
        artifact_store=store,
        durable=True,
    )
    entry = _candidate(artifact)["public_skills"]["helper"]
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {},
                "public_skills": {},
                "managed_integrations": {
                    "helper": {
                        **entry,
                        "provider": "unapproved-provider",
                    }
                },
            },
        ),
        _actor("admin-1", role="admin"),
    )

    report = await service.validate(staged.revision_id, _actor("admin-1", role="admin"))

    codes = {finding.code for finding in report.findings}
    assert report.result == "failed"
    assert "managed_integration_provider_not_allowed" in codes
    assert "skill_capability_forbidden" in codes


@pytest.mark.asyncio
async def test_validator_derives_default_autonomous_secret_capability(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive(skill_markdown=("---\nname: helper\ndescription: Helps safely\nrequired-secrets:\n  - SEARCH_TOKEN\n---\n\n# Instructions\nDo the work.\n")))
    service = ToolPlaneRevisionService(
        repository=InMemoryToolPlaneRevisionRepository(tenant=_TENANT),
        projection=InMemoryToolPlaneProjection(),
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=store,
            durable=True,
            forbidden_skill_capabilities=("autonomous-secrets",),
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

    assert report.result == "failed"
    assert "skill_capability_forbidden" in {finding.code for finding in report.findings}


@pytest.mark.asyncio
async def test_validator_rejects_caller_asserted_skill_version_and_entry_points(
    tmp_path,
) -> None:
    store = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = store.stage_archive(_archive(skill_markdown=("---\nname: helper\ndescription: Helps safely\nversion: 1.2.3\n---\n\n# Instructions\nDo the work.\n")))
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
    candidate = _candidate(artifact)
    candidate["public_skills"]["helper"]["version"] = "9.9.9"
    candidate["public_skills"]["helper"]["entry_points"] = [
        "SKILL.md",
        "scripts/run.py",
    ]
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=candidate,
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "failed"
    assert {finding.code for finding in report.findings} >= {
        "skill_version_mismatch",
        "skill_entry_points_mismatch",
    }


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
            endpoint_resolver=lambda hostname: [ipaddress.ip_address("127.0.0.1" if hostname.endswith("attacker.example.com") else "8.8.8.8")],
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
                    "path-launcher": {
                        "type": "stdio",
                        "command": "/usr/bin/npx",
                        "args": ["server-package"],
                    },
                    "inline-eval": {
                        "type": "stdio",
                        "command": "npx",
                        "args": ["--call=do-not-run"],
                    },
                    "environment-injection": {
                        "type": "stdio",
                        "command": "uvx",
                        "args": ["server-package"],
                        "env": {"PYTHONPATH": "$MCP_PYTHONPATH"},
                    },
                    "dns-rebind": {
                        "type": "http",
                        "url": "https://attacker.example.com/mcp",
                    },
                    "oauth-rebind": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "oauth": {
                            "enabled": True,
                            "token_url": "https://auth.attacker.example.com/token",
                            "client_id": "client-id",
                            "client_secret": "$OAUTH_CLIENT_SECRET",
                        },
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
        "mcp_environment_not_allowed",
        "mcp_private_endpoint_not_allowed",
    }
    private_locations = {finding.location for finding in report.findings if finding.code == "mcp_private_endpoint_not_allowed"}
    assert "mcp_servers.dns-rebind" in private_locations
    assert "mcp_servers.oauth-rebind.oauth.token_url" in private_locations


@pytest.mark.asyncio
async def test_validator_allows_server_flags_after_package_launcher_boundary(
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
                    "safe-launcher": {
                        "type": "stdio",
                        "command": "npx",
                        "args": ["server-package", "-c", "config.json"],
                    }
                },
            },
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "passed"


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
async def test_overlay_cannot_override_deployment_public_skill_state(tmp_path) -> None:
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
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_candidate(artifact),
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
                "skill_states": {"helper": {"enabled": False}},
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(overlay.revision_id, user)

    assert caught.value.code == "overlay_preflight_failed"
    attestation = await repository.compatibility_attestation(
        base_revision_digest=base.revision_digest,
        overlay_revision_digest=overlay.revision_digest,
        validator_policy_digest=_POLICY,
    )
    assert attestation is not None
    assert "overlay_skill_missing_from_composition" in {finding.code for finding in attestation.report.findings}


@pytest.mark.asyncio
async def test_overlay_rejects_conflicting_duplicate_custom_skill_state(tmp_path) -> None:
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
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {},
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
                "custom_skills": {
                    "helper": {
                        "enabled": True,
                        "archive_digest": artifact.archive_digest,
                        "tree_digest": artifact.tree_digest,
                        "manifest_digest": artifact.manifest_digest,
                        "entry_points": list(artifact.entry_points),
                    }
                },
                "skill_states": {"helper": {"enabled": False}},
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(overlay.revision_id, user)

    assert caught.value.code == "overlay_preflight_failed"
    attestation = await repository.compatibility_attestation(
        base_revision_digest=base.revision_digest,
        overlay_revision_digest=overlay.revision_digest,
        validator_policy_digest=_POLICY,
    )
    assert attestation is not None
    assert "overlay_skill_state_conflict" in {finding.code for finding in attestation.report.findings}


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

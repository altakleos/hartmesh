"""Locked filesystem projection and unmanaged-drift detection."""

from __future__ import annotations

import json

import pytest
from deerflow_extension_api import (
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    TenantReferenceV1,
    VerifiedActorContextV1,
    effective_authority_digest_v1,
)

from deerflow.skills.storage import UserScopedSkillStorage
from deerflow.tool_plane import (
    EMPTY_OVERLAY_MARKER_V1,
    DeterministicToolPlaneValidator,
    GovernedSkillArtifactStore,
    GovernedToolPlaneValidator,
    InMemoryToolPlaneRevisionRepository,
    LockedFileToolPlaneProjection,
    ScopedStageRevisionRequest,
    StaticToolPlaneUserInventory,
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    ToolPlaneRevisionService,
    user_scope_reference,
)

_TENANT = TenantReferenceV1(
    version=1,
    public_ref="tenant-aaaaaaaaaaaaaaaa",
    digest="a" * 64,
)
_POLICY = "b" * 64


def _actor(user_id: str, role: str) -> VerifiedActorContextV1:
    authorities = ("tool_plane:admin",) if role == "admin" else ("tool_plane:read", "tool_plane:mutate")
    return VerifiedActorContextV1(
        identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id=user_id,
                role=role,
            )
        ),
        credential=CredentialEvidenceV1(
            method="session",
            credential_ref=None,
            effective_authority_digest=effective_authority_digest_v1(authorities),
            authority_categories=("tool_plane",),
        ),
        tenant=_TENANT,
    )


@pytest.mark.asyncio
async def test_base_projection_uses_existing_locks_and_detects_direct_edit(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"middlewares": ["example:Middleware"], "mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    projection = LockedFileToolPlaneProjection(config_path=config_path)
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    user = _actor("user-1", "member")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "headers": {"Authorization": "$SEARCH_TOKEN"},
                    }
                },
                "public_skills": {},
                "managed_integrations": {},
            },
        ),
        admin,
    )
    await service.validate(staged.revision_id, admin)
    await service.promote(staged.revision_id, admin)

    projected = json.loads(config_path.read_text(encoding="utf-8"))
    assert projected["middlewares"] == ["example:Middleware"]
    assert projected["mcpServers"]["search"]["headers"] == {"authorization": "$SEARCH_TOKEN"}
    assert await service.effective_for_actor(user)

    projected["mcpServers"]["search"]["url"] = "https://drift.example.test"
    config_path.write_text(json.dumps(projected), encoding="utf-8")

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.effective_for_actor(user)
    assert caught.value.code == "unmanaged_drift"


@pytest.mark.asyncio
async def test_bootstrap_stage_captures_current_projection_without_activation(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "headers": {"Authorization": "$SEARCH_TOKEN"},
                    }
                },
                "skills": {"helper": {"enabled": True}},
            }
        ),
        encoding="utf-8",
    )
    skills_root = tmp_path / "skills"
    skill_dir = skills_root / "public" / "helper"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: helper\ndescription: Helps safely\n---\n\n# Instructions\n",
        encoding="utf-8",
    )
    integration_dir = tmp_path / "integrations" / "lark-cli" / "lark-helper"
    integration_dir.mkdir(parents=True)
    (integration_dir / "SKILL.md").write_text(
        "---\nname: lark-helper\ndescription: Managed helper\n---\n\n# Instructions\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        skills_root=skills_root,
        integrations_root=tmp_path / "integrations",
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    await service.initialize(existing_projection=True)

    staged = await service.stage_current_projection(admin)

    record = await repository.get(staged.revision_id)
    assert record is not None
    assert record.bootstrap_inventory_digest is not None
    assert record.manifest["mcp_servers"][0]["secret_selectors"] == [{"field": "headers.authorization", "selector": "env:SEARCH_TOKEN"}]
    assert record.manifest["public_skills"][0]["name"] == "helper"
    assert record.manifest["managed_integrations"][0]["name"] == "lark-helper"
    assert record.manifest["managed_integrations"][0]["provider"] == "lark-cli"
    assert await repository.active(scope) is None
    assert (await service.admin_status(scope, admin)).governance_state == ("bootstrap_required")


@pytest.mark.asyncio
async def test_managed_integration_projection_preserves_provider_layout_and_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    skills_root = tmp_path / "skills"
    integrations_root = tmp_path / "integrations"
    package_root = integrations_root / "lark-cli" / "managed-helper"
    package_root.mkdir(parents=True)
    (package_root / "SKILL.md").write_text(
        "---\nname: managed-helper\ndescription: Managed helper\n---\n\n# Instructions\nHelp safely.\n",
        encoding="utf-8",
    )
    provider_manifest = integrations_root / "lark-cli" / ".provider-manifest.json"
    provider_manifest.write_text('{"version":"v1"}\n', encoding="utf-8")
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")
    artifact = artifacts.stage_directory(package_root)
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        skills_root=skills_root,
        integrations_root=integrations_root,
        artifact_store=artifacts,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {},
                "public_skills": {},
                "managed_integrations": {
                    "managed-helper": {
                        "provider": "lark-cli",
                        "enabled": True,
                        "archive_digest": artifact.archive_digest,
                        "tree_digest": artifact.tree_digest,
                        "manifest_digest": artifact.manifest_digest,
                        "entry_points": list(artifact.entry_points),
                    }
                },
            },
        ),
        admin,
    )
    report = await service.validate(staged.revision_id, admin)
    assert report.result == "passed"

    await service.promote(staged.revision_id, admin)

    active = await repository.active(ToolPlaneRevisionScopeV1(kind="deployment_base"))
    assert active is not None
    assert active.manifest["managed_integrations"][0]["provider"] == "lark-cli"
    assert (integrations_root / "lark-cli" / "managed-helper" / "SKILL.md").is_file()
    assert not (integrations_root / "managed-helper").exists()
    assert provider_manifest.read_text(encoding="utf-8") == '{"version":"v1"}\n'


@pytest.mark.asyncio
async def test_bootstrap_promotion_rejects_concurrent_inventory_change(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "search": {
                        "type": "http",
                        "url": "https://first.example.test",
                    }
                },
                "skills": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        skills_root=tmp_path / "skills",
        integrations_root=tmp_path / "integrations",
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    await service.initialize(existing_projection=True)
    staged = await service.stage_current_projection(admin)
    await service.validate(staged.revision_id, admin)
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["mcpServers"]["search"]["url"] = "https://second.example.test"
    config_path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(staged.revision_id, admin)

    assert caught.value.code == "bootstrap_inventory_changed"
    assert await repository.active(ToolPlaneRevisionScopeV1(kind="deployment_base")) is None


@pytest.mark.asyncio
async def test_bootstrap_stages_and_promotes_all_nonempty_user_overlays(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    skills_root = tmp_path / "skills"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")

    def storage_for(subject_id: str):
        return UserScopedSkillStorage(subject_id, host_path=str(skills_root))

    alice_storage = storage_for("alice")
    alice_storage.write_custom_skill(
        "alice-helper",
        "SKILL.md",
        "---\nname: alice-helper\ndescription: Helps Alice\n---\n\n# Alice\n",
    )
    alice_storage.set_skill_enabled_state("alice-helper", False)
    # An indexed empty store uses the canonical empty-overlay marker and does
    # not create a fabricated revision row.
    storage_for("empty")

    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        state_root=tmp_path / "tool-plane",
        skills_root=skills_root,
        integrations_root=tmp_path / "integrations",
        artifact_store=artifacts,
        user_storage_factory=storage_for,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        user_inventory=StaticToolPlaneUserInventory(("alice", "empty")),
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    alice = _actor("alice", "member")
    empty = _actor("empty", "member")
    await service.initialize(existing_projection=True)

    staged = await service.stage_current_projection(admin)

    assert len(staged.overlay_revisions) == 1
    overlay = staged.overlay_revisions[0]
    assert overlay.scope.user_ref == user_scope_reference(alice)
    await service.validate(staged.revision_id, admin)
    await service.admin_validate(overlay.revision_id, admin)
    await service.promote(staged.revision_id, admin)
    assert (await service.admin_status(staged.scope, admin)).governance_state == ("bootstrap_required")

    await service.admin_promote(overlay.revision_id, admin)

    assert (await service.admin_status(staged.scope, admin)).governance_state == ("governed")
    assert (await service.effective_for_actor(alice)).user_overlay_digest == (overlay.revision_digest)
    assert (await service.effective_for_actor(empty)).user_overlay_digest == (EMPTY_OVERLAY_MARKER_V1)


@pytest.mark.asyncio
async def test_bootstrap_migrates_visible_legacy_skill_into_user_overlay(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    skills_root = tmp_path / "skills"
    legacy_root = skills_root / "custom" / "legacy-helper"
    legacy_root.mkdir(parents=True)
    (legacy_root / "SKILL.md").write_text(
        "---\nname: legacy-helper\ndescription: Legacy helper\n---\n\n# Legacy\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")

    def storage_for(subject_id: str):
        return UserScopedSkillStorage(subject_id, host_path=str(skills_root))

    alice_storage = storage_for("alice")
    alice_storage.set_skill_enabled_state("legacy-helper", False)
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        state_root=tmp_path / "tool-plane",
        skills_root=skills_root,
        integrations_root=tmp_path / "integrations",
        artifact_store=artifacts,
        user_storage_factory=storage_for,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        user_inventory=StaticToolPlaneUserInventory(("alice",)),
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    await service.initialize(existing_projection=True)

    staged = await service.stage_current_projection(admin)

    assert len(staged.overlay_revisions) == 1
    overlay = await repository.get(staged.overlay_revisions[0].revision_id)
    assert overlay is not None
    assert [item["name"] for item in overlay.manifest["custom_skills"]] == [
        "legacy-helper",
    ]
    assert overlay.manifest["custom_skills"][0]["enabled"] is False
    assert overlay.manifest["skill_states"] == []

    await service.validate(staged.revision_id, admin)
    await service.admin_validate(overlay.revision_id, admin)
    await service.promote(staged.revision_id, admin)
    await service.admin_promote(overlay.revision_id, admin)

    assert (alice_storage.get_user_custom_root() / "legacy-helper" / "SKILL.md").is_file()
    assert alice_storage.get_skill_enabled_state("legacy-helper") is False


@pytest.mark.asyncio
async def test_user_only_upgrade_enters_bootstrap_required(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    skills_root = tmp_path / "skills"
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))

    def storage_for(subject_id: str):
        return UserScopedSkillStorage(subject_id, host_path=str(skills_root))

    storage_for("alice").write_custom_skill(
        "alice-helper",
        "SKILL.md",
        "---\nname: alice-helper\ndescription: Helps Alice\n---\n\n# Alice\n",
    )
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        state_root=tmp_path / "tool-plane",
        skills_root=skills_root,
        integrations_root=tmp_path / "integrations",
        user_storage_factory=storage_for,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        user_inventory=StaticToolPlaneUserInventory(("alice",)),
        durable=True,
    )

    await service.initialize(existing_projection=False)

    assert await repository.bootstrap_required() is True


@pytest.mark.asyncio
async def test_bootstrap_fails_closed_for_unindexed_nonempty_user_store(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    home = tmp_path / ".deer-flow"
    orphan = home / "users" / "unindexed-bucket" / "skills" / "custom" / "helper"
    orphan.mkdir(parents=True)
    (orphan / "SKILL.md").write_text(
        "---\nname: helper\ndescription: Unindexed\n---\n\n# Helper\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_HOME", str(home))
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")
    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        state_root=tmp_path / "tool-plane",
        skills_root=tmp_path / "skills",
        integrations_root=tmp_path / "integrations",
        artifact_store=artifacts,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        user_inventory=StaticToolPlaneUserInventory(),
        durable=True,
    )
    admin = _actor("admin-1", "admin")

    await service.initialize(existing_projection=False)

    assert await repository.bootstrap_required() is True
    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.stage_current_projection(admin)
    assert caught.value.code == "bootstrap_inventory_changed"


@pytest.mark.asyncio
async def test_two_user_overlays_project_distinct_bytes_state_and_selectors(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "extensions_config.json"
    config_path.write_text(
        json.dumps({"mcpServers": {}, "skills": {}}),
        encoding="utf-8",
    )
    skills_root = tmp_path / "skills"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / ".deer-flow"))
    artifacts = GovernedSkillArtifactStore(tmp_path / "candidates")

    def make_package(root, name: str, instruction: str):
        package = root / name
        package.mkdir(parents=True)
        (package / "SKILL.md").write_text(
            (f"---\nname: {name}\ndescription: A safe helper\n---\n\n# Instructions\n{instruction}\n"),
            encoding="utf-8",
        )
        return artifacts.stage_directory(package)

    alice_artifact = make_package(tmp_path / "alice-source", "alice-helper", "Alice")
    bob_artifact = make_package(tmp_path / "bob-source", "bob-helper", "Bob")

    def storage_for(subject_id: str):
        return UserScopedSkillStorage(subject_id, host_path=str(skills_root))

    projection = LockedFileToolPlaneProjection(
        config_path=config_path,
        state_root=tmp_path / "tool-plane",
        skills_root=skills_root,
        integrations_root=tmp_path / "integrations",
        artifact_store=artifacts,
        user_storage_factory=storage_for,
    )
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest=_POLICY,
            artifact_store=artifacts,
            durable=True,
        ),
        artifact_store=artifacts,
        durable=True,
    )
    admin = _actor("admin-1", "admin")
    alice = _actor("alice", "member")
    bob = _actor("bob", "member")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                "validation_policy_digest": _POLICY,
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
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

    async def promote_overlay(actor, artifact, binding_ref: str):
        scope = ToolPlaneRevisionScopeV1(
            kind="user_overlay",
            user_ref=user_scope_reference(actor),
        )
        staged = await service.stage(
            ScopedStageRevisionRequest(
                scope=scope,
                candidate={
                    "base_revision_digest": base.revision_digest,
                    "custom_skills": {
                        artifact.skill_name: {
                            "enabled": True,
                            "archive_digest": artifact.archive_digest,
                            "tree_digest": artifact.tree_digest,
                            "manifest_digest": artifact.manifest_digest,
                            "entry_points": list(artifact.entry_points),
                        }
                    },
                    "mcp_enablement": {"search": actor is alice},
                    "credential_selectors": {"search": {"binding_ref": binding_ref, "version": 1}},
                },
            ),
            actor,
        )
        await service.validate(staged.revision_id, actor)
        await service.promote(staged.revision_id, actor)
        return staged

    alice_revision = await promote_overlay(alice, alice_artifact, "binding:alice")
    bob_revision = await promote_overlay(bob, bob_artifact, "binding:bob")

    alice_effective = await service.effective_for_actor(alice)
    bob_effective = await service.effective_for_actor(bob)
    assert alice_effective.effective_digest != bob_effective.effective_digest
    alice_scope = ToolPlaneRevisionScopeV1(
        kind="user_overlay",
        user_ref=user_scope_reference(alice),
    )
    assert (
        await projection.observed_digest_for_actor(
            alice_scope,
            storage_subject_id="alice",
        )
        == alice_revision.content_digest
    )
    assert (
        await projection.observed_digest_for_actor(
            alice_scope,
            storage_subject_id="bob",
        )
        != alice_revision.content_digest
    )
    assert (tmp_path / ".deer-flow" / "users" / "alice" / "skills" / "custom" / "alice-helper" / "SKILL.md").exists()
    assert (tmp_path / ".deer-flow" / "users" / "bob" / "skills" / "custom" / "bob-helper" / "SKILL.md").exists()
    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.inspect_for_actor(bob_revision.revision_id, alice)
    assert caught.value.code == "promotion_not_authorized"
    alice_record = await service.inspect_for_actor(
        alice_revision.revision_id,
        alice,
    )
    assert "binding:alice" in str(alice_record.manifest)
    assert "binding:bob" not in str(alice_record.manifest)

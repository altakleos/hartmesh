"""Governed tool-plane lifecycle and scope isolation."""

from __future__ import annotations

import asyncio

import pytest
from deerflow_extension_api import (
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    TenantReferenceV1,
    VerifiedActorContextV1,
    effective_authority_digest_v1,
)

from deerflow.tool_plane import (
    EMPTY_OVERLAY_MARKER_V1,
    DeterministicToolPlaneValidator,
    InMemoryToolPlaneProjection,
    InMemoryToolPlaneRevisionRepository,
    ScopedStageRevisionRequest,
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    ToolPlaneRevisionService,
    ToolPlaneValidationReportV1,
    user_scope_reference,
)

_TENANT = TenantReferenceV1(
    version=1,
    public_ref="tenant-aaaaaaaaaaaaaaaa",
    digest="a" * 64,
)
_POLICY = "b" * 64


def _actor(
    user_id: str,
    *,
    role: str = "member",
    categories: tuple[str, ...] = ("tool_plane",),
    authorities: tuple[str, ...] | None = None,
) -> VerifiedActorContextV1:
    if authorities is None:
        if categories == ("tool_plane",):
            authorities = (
                "tool_plane:read",
                "tool_plane:mutate",
                *(("tool_plane:admin",) if role == "admin" else ()),
            )
        else:
            authorities = tuple(f"{category}:read" for category in categories)
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
            authority_categories=categories,
        ),
        tenant=_TENANT,
    )


def _base_candidate(*, summary: str = "initial") -> dict[str, object]:
    return {
        "validation_policy_digest": _POLICY,
        "mcp_servers": {},
        "public_skills": {},
        "managed_integrations": {},
        "change_summary": summary,
    }


def _service() -> tuple[
    ToolPlaneRevisionService,
    InMemoryToolPlaneRevisionRepository,
    InMemoryToolPlaneProjection,
]:
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    projection = InMemoryToolPlaneProjection()
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=True,
    )
    return service, repository, projection


@pytest.mark.asyncio
async def test_stage_and_validate_do_not_activate_until_authorized_promotion() -> None:
    service, repository, projection = _service()
    admin = _actor("admin-1", role="admin")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")

    staged = await service.stage(
        ScopedStageRevisionRequest(scope=scope, candidate=_base_candidate()),
        admin,
    )

    assert staged.state == "staged"
    assert await repository.active(scope) is None
    assert projection.project_count == 0

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "passed"
    assert report.revision_digest == staged.revision_digest
    assert await repository.active(scope) is None
    assert projection.project_count == 0

    promoted = await service.promote(staged.revision_id, admin)

    assert promoted.state == "promoted"
    assert promoted.actor_digest == admin.digest
    assert promoted.observed_projection_digest == staged.content_digest
    assert projection.project_count == 1
    active = await repository.active(scope)
    assert active is not None
    assert active.revision_id == staged.revision_id
    assert [event.state for event in await repository.events(staged.revision_id)] == [
        "staged",
        "validating",
        "validated",
        "prepared",
        "promoted",
    ]
    assert all(event.actor_digest == admin.digest for event in await repository.events(staged.revision_id))


@pytest.mark.asyncio
async def test_rejected_validation_report_is_immutable() -> None:
    service, repository, _ = _service()
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                **_base_candidate(),
                "validation_policy_digest": "c" * 64,
            },
        ),
        admin,
    )
    first = await service.validate(staged.revision_id, admin)
    assert first.result == "failed"

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.validate(staged.revision_id, admin)

    assert caught.value.code == "validation_stale"
    stored = await repository.get(staged.revision_id)
    assert stored is not None
    assert stored.validation_report == first


class _TypedUnavailableValidator(DeterministicToolPlaneValidator):
    async def validate(self, revision) -> ToolPlaneValidationReportV1:
        raise ToolPlaneRevisionError("validator_unavailable")


@pytest.mark.asyncio
async def test_typed_validator_failure_finishes_as_rejected_unqualified_report() -> None:
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=InMemoryToolPlaneProjection(),
        validator=_TypedUnavailableValidator(policy_digest=_POLICY),
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_base_candidate(),
        ),
        admin,
    )

    report = await service.validate(staged.revision_id, admin)

    assert report.result == "unqualified"
    assert [finding.code for finding in report.findings] == [
        "validator_unavailable",
    ]
    stored = await repository.get(staged.revision_id)
    assert stored is not None
    assert stored.state == "rejected"
    assert stored.validation_report == report


@pytest.mark.asyncio
async def test_user_cannot_stage_or_observe_another_users_overlay() -> None:
    service, repository, _ = _service()
    first = _actor("user-1")
    second = _actor("user-2")
    forged_scope = ToolPlaneRevisionScopeV1(
        kind="user_overlay",
        user_ref=user_scope_reference(second),
    )

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.stage(
            ScopedStageRevisionRequest(
                scope=forged_scope,
                candidate={
                    "base_revision_digest": "c" * 64,
                    "custom_skills": {},
                },
            ),
            first,
        )

    assert caught.value.code == "promotion_not_authorized"
    assert await repository.list_scope(forged_scope) == []


@pytest.mark.asyncio
async def test_service_read_paths_require_tool_plane_authority() -> None:
    service, _, _ = _service()
    user = _actor("user-1", categories=())
    admin = _actor("admin-1", role="admin", categories=())

    with pytest.raises(ToolPlaneRevisionError) as user_error:
        await service.status_for_actor(user)
    with pytest.raises(ToolPlaneRevisionError) as admin_error:
        await service.admin_status(
            ToolPlaneRevisionScopeV1(kind="deployment_base"),
            admin,
        )

    assert user_error.value.code == "promotion_not_authorized"
    assert admin_error.value.code == "promotion_not_authorized"


@pytest.mark.asyncio
async def test_service_requires_exact_authority_not_only_tool_plane_category() -> None:
    service, _, _ = _service()
    read_only_admin = _actor(
        "admin-1",
        role="admin",
        authorities=("tool_plane:read",),
    )
    read_only_user = _actor(
        "user-1",
        authorities=("tool_plane:read",),
    )

    with pytest.raises(ToolPlaneRevisionError) as admin_error:
        await service.stage(
            ScopedStageRevisionRequest(
                scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
                candidate=_base_candidate(),
            ),
            read_only_admin,
        )
    with pytest.raises(ToolPlaneRevisionError) as user_error:
        await service.stage(
            ScopedStageRevisionRequest(
                scope=ToolPlaneRevisionScopeV1(
                    kind="user_overlay",
                    user_ref=user_scope_reference(read_only_user),
                ),
                candidate={"base_revision_digest": "c" * 64},
            ),
            read_only_user,
        )

    assert admin_error.value.code == "promotion_not_authorized"
    assert user_error.value.code == "promotion_not_authorized"


@pytest.mark.asyncio
async def test_local_readiness_allows_visible_unmanaged_bootstrap_state() -> None:
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=InMemoryToolPlaneProjection(),
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=False,
    )
    await service.initialize(existing_projection=True)

    assert await service.readiness_reason() is None
    status = await service.status_for_actor(_actor("user-1"))
    assert status.governance_state == "bootstrap_required"


@pytest.mark.asyncio
async def test_overlay_promotion_requires_the_current_base_generation() -> None:
    service, _, _ = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    base = await service.stage(
        ScopedStageRevisionRequest(scope=base_scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(base.revision_id, admin)
    await service.promote(base.revision_id, admin)

    overlay_scope = ToolPlaneRevisionScopeV1(
        kind="user_overlay",
        user_ref=user_scope_reference(user),
    )
    overlay = await service.stage(
        ScopedStageRevisionRequest(
            scope=overlay_scope,
            candidate={
                "base_revision_digest": base.revision_digest,
                "custom_skills": {},
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    replacement = await service.stage(
        ScopedStageRevisionRequest(
            scope=base_scope,
            candidate={
                **_base_candidate(summary="replacement"),
                "parent_revision_digest": base.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(replacement.revision_id, admin)
    await service.promote(replacement.revision_id, admin)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(overlay.revision_id, user)

    assert caught.value.code == "base_revision_changed"


@pytest.mark.asyncio
async def test_concurrent_promotions_from_one_active_parent_have_one_winner() -> None:
    service, _, _ = _service()
    admin = _actor("admin-1", role="admin")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    initial = await service.stage(
        ScopedStageRevisionRequest(scope=scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(initial.revision_id, admin)
    await service.promote(initial.revision_id, admin)

    candidates = []
    for summary in ("candidate-a", "candidate-b"):
        candidate = await service.stage(
            ScopedStageRevisionRequest(
                scope=scope,
                candidate={
                    **_base_candidate(summary=summary),
                    "parent_revision_digest": initial.revision_digest,
                },
            ),
            admin,
        )
        await service.validate(candidate.revision_id, admin)
        candidates.append(candidate)

    outcomes = await asyncio.gather(
        *(service.promote(candidate.revision_id, admin) for candidate in candidates),
        return_exceptions=True,
    )

    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    failures = [item for item in outcomes if isinstance(item, ToolPlaneRevisionError)]
    assert len(failures) == 1
    assert failures[0].code == "revision_conflict"


@pytest.mark.asyncio
async def test_effective_revision_uses_canonical_empty_overlay_marker() -> None:
    service, _, _ = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_base_candidate(),
        ),
        admin,
    )
    await service.validate(staged.revision_id, admin)
    await service.promote(staged.revision_id, admin)

    effective = await service.effective_for_actor(user)

    assert effective.base_revision_digest == staged.revision_digest
    assert effective.user_overlay_digest == EMPTY_OVERLAY_MARKER_V1
    assert len(effective.effective_digest) == 64


@pytest.mark.asyncio
async def test_overlay_promotion_persists_exact_compatibility_attestation() -> None:
    service, repository, _ = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_base_candidate(),
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
                "custom_skills": {},
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    await service.promote(overlay.revision_id, user)

    attestation = await repository.compatibility_attestation(
        base_revision_digest=base.revision_digest,
        overlay_revision_digest=overlay.revision_digest,
        validator_policy_digest=_POLICY,
    )
    assert attestation is not None
    assert attestation.compatible is True
    assert attestation.report.revision_digest == overlay.revision_digest
    assert len(attestation.attestation_digest) == 64


class _GenerationChangingRepository(InMemoryToolPlaneRevisionRepository):
    change_overlay_generation_before_prepare = False

    async def prepare_activation(self, revision_id: str, **kwargs):
        if self.change_overlay_generation_before_prepare:
            self.change_overlay_generation_before_prepare = False
            self._overlay_set_generation += 1
        return await super().prepare_activation(revision_id, **kwargs)


@pytest.mark.asyncio
async def test_base_prepare_rejects_overlay_set_change_after_preflight() -> None:
    repository = _GenerationChangingRepository(tenant=_TENANT)
    projection = InMemoryToolPlaneProjection()
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_base_candidate(),
        ),
        admin,
    )
    await service.validate(base.revision_id, admin)
    await service.promote(base.revision_id, admin)
    replacement = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                **_base_candidate(summary="replacement"),
                "parent_revision_digest": base.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(replacement.revision_id, admin)
    repository.change_overlay_generation_before_prepare = True

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(replacement.revision_id, admin)

    assert caught.value.code == "active_overlay_set_changed"
    active = await repository.active(ToolPlaneRevisionScopeV1(kind="deployment_base"))
    assert active is not None
    assert active.revision_id == base.revision_id


@pytest.mark.asyncio
async def test_rollback_creates_new_attributed_revision_with_exact_prior_projection() -> None:
    service, repository, projection = _service()
    admin = _actor("admin-1", role="admin")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    initial = await service.stage(
        ScopedStageRevisionRequest(scope=scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(initial.revision_id, admin)
    await service.promote(initial.revision_id, admin)
    replacement = await service.stage(
        ScopedStageRevisionRequest(
            scope=scope,
            candidate={
                **_base_candidate(summary="replacement"),
                "parent_revision_digest": initial.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(replacement.revision_id, admin)
    await service.promote(replacement.revision_id, admin)

    rolled_back = await service.rollback(initial.revision_id, admin)

    assert rolled_back.revision_id not in {
        initial.revision_id,
        replacement.revision_id,
    }
    active = await repository.active(scope)
    assert active is not None
    assert active.content_digest == initial.content_digest
    assert active.manifest == (await repository.get(initial.revision_id)).manifest
    assert active.parent_revision_digest == replacement.revision_digest
    assert active.rollback_source_revision_id == initial.revision_id
    assert active.promotion_actor_digest == admin.digest
    assert await projection.observed_digest(scope) == initial.content_digest
    assert [event.state for event in await repository.events(active.revision_id)] == [
        "staged",
        "validating",
        "validated",
        "prepared",
        "promoted",
    ]


class _UnavailableCompatibilityValidator(DeterministicToolPlaneValidator):
    async def validate_compatibility(self, *, base, overlay) -> ToolPlaneValidationReportV1:
        raise TimeoutError


@pytest.mark.asyncio
async def test_base_preflight_unavailability_is_typed_and_leaves_active_base() -> None:
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    projection = InMemoryToolPlaneProjection()
    validator = _UnavailableCompatibilityValidator(policy_digest=_POLICY)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=validator,
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    initial = await service.stage(
        ScopedStageRevisionRequest(scope=scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(initial.revision_id, admin)
    await service.promote(initial.revision_id, admin)
    overlay = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(
                kind="user_overlay",
                user_ref=user_scope_reference(user),
            ),
            candidate={
                "base_revision_digest": initial.revision_digest,
                "custom_skills": {
                    "helper": {
                        "enabled": True,
                        "tree_digest": "d" * 64,
                        "manifest_digest": "e" * 64,
                        "entry_points": ["SKILL.md"],
                    }
                },
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)
    # Seed an active overlay using the deterministic compatibility result, then
    # restore the unavailable validator for the replacement-base preflight.
    service._validator = DeterministicToolPlaneValidator(policy_digest=_POLICY)
    await service.promote(overlay.revision_id, user)
    service._validator = validator
    replacement = await service.stage(
        ScopedStageRevisionRequest(
            scope=scope,
            candidate={
                **_base_candidate(summary="replacement"),
                "parent_revision_digest": initial.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(replacement.revision_id, admin)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(replacement.revision_id, admin)

    assert caught.value.code == "overlay_preflight_incomplete"
    active = await repository.active(scope)
    assert active is not None
    assert active.revision_id == initial.revision_id


@pytest.mark.asyncio
async def test_base_preflight_pages_all_overlays_and_enforces_bound() -> None:
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    projection = InMemoryToolPlaneProjection()
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=True,
        active_overlay_page_size=1,
        maximum_active_overlays=1,
    )
    admin = _actor("admin-1", role="admin")
    base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    initial = await service.stage(
        ScopedStageRevisionRequest(scope=base_scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(initial.revision_id, admin)
    await service.promote(initial.revision_id, admin)

    for user_id in ("user-1", "user-2"):
        user = _actor(user_id)
        overlay = await service.stage(
            ScopedStageRevisionRequest(
                scope=ToolPlaneRevisionScopeV1(
                    kind="user_overlay",
                    user_ref=user_scope_reference(user),
                ),
                candidate={
                    "base_revision_digest": initial.revision_digest,
                    "mcp_enablement": {"search": False},
                },
            ),
            user,
        )
        await service.validate(overlay.revision_id, user)
        await service.promote(overlay.revision_id, user)

    replacement = await service.stage(
        ScopedStageRevisionRequest(
            scope=base_scope,
            candidate={
                **_base_candidate(summary="replacement"),
                "parent_revision_digest": initial.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(replacement.revision_id, admin)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(replacement.revision_id, admin)

    assert caught.value.code == "overlay_preflight_incomplete"
    active = await repository.active(base_scope)
    assert active is not None
    assert active.revision_id == initial.revision_id


@pytest.mark.asyncio
async def test_in_memory_records_are_immutable_across_inspection_boundaries() -> None:
    service, repository, _ = _service()
    admin = _actor("admin-1", role="admin")
    staged = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate=_base_candidate(),
        ),
        admin,
    )

    inspected = await service.inspect_for_actor(staged.revision_id, admin)
    inspected.manifest["change_summary"] = "mutated outside repository"

    stored = await repository.get(staged.revision_id)
    assert stored is not None
    assert stored.manifest["change_summary"] == "initial"
    assert stored.content_digest == staged.content_digest


class _FinalizeCrashRepository(InMemoryToolPlaneRevisionRepository):
    crash_before_finalize = True

    async def finalize_activation(self, revision_id: str, **kwargs):
        if self.crash_before_finalize:
            self.crash_before_finalize = False
            raise RuntimeError("simulated crash before SQL finalize")
        return await super().finalize_activation(revision_id, **kwargs)


@pytest.mark.asyncio
async def test_reconcile_finalizes_exact_projection_after_finalize_crash() -> None:
    repository = _FinalizeCrashRepository(tenant=_TENANT)
    projection = InMemoryToolPlaneProjection()
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
        durable=True,
    )
    admin = _actor("admin-1", role="admin")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    staged = await service.stage(
        ScopedStageRevisionRequest(scope=scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(staged.revision_id, admin)

    with pytest.raises(RuntimeError, match="simulated crash"):
        await service.promote(staged.revision_id, admin)

    prepared = await repository.get(staged.revision_id)
    assert prepared is not None
    assert prepared.state == "prepared"
    assert await repository.active(scope) is None

    await service.reconcile(admin)

    active = await repository.active(scope)
    assert active is not None
    assert active.revision_id == staged.revision_id
    assert active.state == "promoted"


@pytest.mark.asyncio
async def test_reconcile_retries_projection_after_projection_crash() -> None:
    service, repository, projection = _service()
    admin = _actor("admin-1", role="admin")
    scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    staged = await service.stage(
        ScopedStageRevisionRequest(scope=scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(staged.revision_id, admin)
    projection.fail_next = "projection_failed"

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(staged.revision_id, admin)

    assert caught.value.code == "projection_failed"
    recovering = await repository.get(staged.revision_id)
    assert recovering is not None
    assert recovering.state == "recovery_required"
    assert await repository.active(scope) is None

    await service.reconcile(admin)

    active = await repository.active(scope)
    assert active is not None
    assert active.revision_id == staged.revision_id
    assert active.state == "promoted"


@pytest.mark.asyncio
async def test_reconcile_reuses_overlay_attestation_after_projection_crash() -> None:
    service, repository, projection = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                **_base_candidate(),
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                    }
                },
            },
        ),
        admin,
    )
    await service.validate(base.revision_id, admin)
    await service.promote(base.revision_id, admin)
    scope = ToolPlaneRevisionScopeV1(
        kind="user_overlay",
        user_ref=user_scope_reference(user),
    )
    overlay = await service.stage(
        ScopedStageRevisionRequest(
            scope=scope,
            candidate={
                "base_revision_digest": base.revision_digest,
                "mcp_enablement": {"search": False},
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)
    projection.fail_next = "projection_failed"

    with pytest.raises(ToolPlaneRevisionError):
        await service.promote(overlay.revision_id, user)

    first_attestation = await repository.compatibility_attestation(
        base_revision_digest=base.revision_digest,
        overlay_revision_digest=overlay.revision_digest,
        validator_policy_digest=_POLICY,
    )
    assert first_attestation is not None

    await service.reconcile(admin)

    active = await repository.active(scope)
    assert active is not None
    assert active.revision_id == overlay.revision_id
    persisted = await repository.compatibility_attestation(
        base_revision_digest=base.revision_digest,
        overlay_revision_digest=overlay.revision_digest,
        validator_policy_digest=_POLICY,
    )
    assert persisted == first_attestation


@pytest.mark.asyncio
async def test_effective_revision_carries_exact_secret_safe_mcp_runtime_structure() -> None:
    service, _, _ = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
            candidate={
                **_base_candidate(),
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "headers": {"Authorization": "$SEARCH_TOKEN"},
                        "tools": {
                            "lookup": {
                                "routing": {
                                    "mode": "prefer",
                                    "priority": 10,
                                    "keywords": ["search"],
                                }
                            }
                        },
                    }
                },
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
                "credential_selectors": {
                    "search": {
                        "binding_ref": "search:user-primary",
                        "version": 7,
                    }
                },
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)
    await service.promote(overlay.revision_id, user)

    effective = await service.effective_for_actor(user)

    assert effective.effective_mcp_server_ids == ("search",)
    assert len(effective.effective_mcp_servers) == 1
    server = effective.effective_mcp_servers[0]
    assert server["url"] == "https://mcp.example.test"
    assert server["tool_allowlist"] == ["lookup"]
    assert server["secret_selectors"] == [
        {
            "field": "headers.authorization",
            "selector": "env:SEARCH_TOKEN",
        }
    ]
    assert server["credential_binding"] == {
        "binding_ref": "search:user-primary",
        "version": 7,
    }
    assert "SEARCH_TOKEN" in effective.to_json()["effective_mcp_servers"][0]["secret_selectors"][0]["selector"]


@pytest.mark.asyncio
async def test_prepared_revision_blocks_promotion_in_another_scope() -> None:
    service, repository, _ = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    base = await service.stage(
        ScopedStageRevisionRequest(scope=base_scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(base.revision_id, admin)
    await service.promote(base.revision_id, admin)

    replacement = await service.stage(
        ScopedStageRevisionRequest(
            scope=base_scope,
            candidate={
                **_base_candidate(summary="replacement"),
                "parent_revision_digest": base.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(replacement.revision_id, admin)
    await repository.prepare_activation(
        replacement.revision_id,
        actor_digest=admin.digest,
        expected_active_digest=base.revision_digest,
        expected_base_generation=None,
        expected_overlay_set_generation=0,
    )

    overlay = await service.stage(
        ScopedStageRevisionRequest(
            scope=ToolPlaneRevisionScopeV1(
                kind="user_overlay",
                user_ref=user_scope_reference(user),
            ),
            candidate={
                "base_revision_digest": base.revision_digest,
                "custom_skills": {},
            },
        ),
        user,
    )
    await service.validate(overlay.revision_id, user)

    with pytest.raises(ToolPlaneRevisionError) as caught:
        await service.promote(overlay.revision_id, user)

    assert caught.value.code == "recovery_required"


@pytest.mark.asyncio
async def test_overlay_rollback_revalidates_exact_old_overlay_against_current_base() -> None:
    service, repository, _ = _service()
    admin = _actor("admin-1", role="admin")
    user = _actor("user-1")
    base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
    overlay_scope = ToolPlaneRevisionScopeV1(
        kind="user_overlay",
        user_ref=user_scope_reference(user),
    )
    base_one = await service.stage(
        ScopedStageRevisionRequest(scope=base_scope, candidate=_base_candidate()),
        admin,
    )
    await service.validate(base_one.revision_id, admin)
    await service.promote(base_one.revision_id, admin)
    overlay_one = await service.stage(
        ScopedStageRevisionRequest(
            scope=overlay_scope,
            candidate={
                "base_revision_digest": base_one.revision_digest,
                "mcp_enablement": {},
            },
        ),
        user,
    )
    await service.validate(overlay_one.revision_id, user)
    await service.promote(overlay_one.revision_id, user)

    base_two = await service.stage(
        ScopedStageRevisionRequest(
            scope=base_scope,
            candidate={
                **_base_candidate(summary="second base"),
                "parent_revision_digest": base_one.revision_digest,
            },
        ),
        admin,
    )
    await service.validate(base_two.revision_id, admin)
    await service.promote(base_two.revision_id, admin)
    overlay_two = await service.stage(
        ScopedStageRevisionRequest(
            scope=overlay_scope,
            candidate={
                "base_revision_digest": base_two.revision_digest,
                "mcp_enablement": {},
                "parent_revision_digest": overlay_one.revision_digest,
                "change_summary": "second overlay",
            },
        ),
        user,
    )
    await service.validate(overlay_two.revision_id, user)
    await service.promote(overlay_two.revision_id, user)

    result = await service.rollback(overlay_one.revision_id, user)

    active = await repository.active(overlay_scope)
    assert active is not None
    assert active.revision_id == result.revision_id
    assert active.content_digest == overlay_one.content_digest
    assert active.manifest == (await repository.get(overlay_one.revision_id)).manifest
    attestation = await repository.compatibility_attestation(
        base_revision_digest=base_two.revision_digest,
        overlay_revision_digest=active.revision_digest,
        validator_policy_digest=_POLICY,
    )
    assert attestation is not None
    assert attestation.compatible

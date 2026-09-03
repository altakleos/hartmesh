"""SQLite contract for the durable tool-plane revision repository."""

from __future__ import annotations

import pytest
from deerflow_extension_api import (
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    TenantReferenceV1,
    VerifiedActorContextV1,
    effective_authority_digest_v1,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.base import Base
from deerflow.persistence.tool_plane import (
    SQLToolPlaneRevisionRepository,
    SQLToolPlaneUserInventory,
)
from deerflow.persistence.tool_plane.model import (
    ToolPlaneOverlayCompatibilityRow,
    ToolPlaneRevisionEventRow,
)
from deerflow.persistence.user.model import UserRow
from deerflow.tool_plane import (
    DeterministicToolPlaneValidator,
    InMemoryToolPlaneProjection,
    ScopedStageRevisionRequest,
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


def _admin() -> VerifiedActorContextV1:
    return VerifiedActorContextV1(
        identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id="admin-1",
                role="admin",
            )
        ),
        credential=CredentialEvidenceV1(
            method="session",
            credential_ref=None,
            effective_authority_digest=effective_authority_digest_v1(("tool_plane:admin",)),
            authority_categories=("tool_plane",),
        ),
        tenant=_TENANT,
    )


@pytest.mark.asyncio
async def test_sql_repository_persists_active_revision_and_append_only_events(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tool-plane.db'}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        repository = SQLToolPlaneRevisionRepository(sessions, tenant=_TENANT)
        projection = InMemoryToolPlaneProjection()
        service = ToolPlaneRevisionService(
            repository=repository,
            projection=projection,
            validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
            durable=True,
        )
        admin = _admin()
        scope = ToolPlaneRevisionScopeV1(kind="deployment_base")

        await service.initialize(existing_projection=False)
        status = await service.admin_status(scope, admin)
        assert status.governance_state == "unmanaged"

        staged = await service.stage(
            ScopedStageRevisionRequest(
                scope=scope,
                candidate={
                    "validation_policy_digest": _POLICY,
                    "mcp_servers": {},
                    "public_skills": {},
                    "managed_integrations": {},
                },
            ),
            admin,
        )
        await service.validate(staged.revision_id, admin)
        await service.promote(staged.revision_id, admin)

        restarted_repository = SQLToolPlaneRevisionRepository(
            sessions,
            tenant=_TENANT,
        )
        active = await restarted_repository.active(scope)
        assert active is not None
        assert active.revision_digest == staged.revision_digest
        assert await restarted_repository.bootstrap_required() is False
        assert [event.state for event in await restarted_repository.events(staged.revision_id)] == [
            "staged",
            "validating",
            "validated",
            "prepared",
            "promoted",
        ]

        async with sessions() as session:
            event_count = len((await session.scalars(select(ToolPlaneRevisionEventRow))).all())
        assert event_count == 5
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sql_repository_persists_digest_bound_compatibility_attestation(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tool-plane-compatibility.db'}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        repository = SQLToolPlaneRevisionRepository(sessions, tenant=_TENANT)
        service = ToolPlaneRevisionService(
            repository=repository,
            projection=InMemoryToolPlaneProjection(),
            validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
            durable=True,
        )
        admin = _admin()
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
        user = VerifiedActorContextV1(
            identity=InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="human",
                    subject_id="user-1",
                    role="member",
                )
            ),
            credential=CredentialEvidenceV1(
                method="session",
                credential_ref=None,
                effective_authority_digest=effective_authority_digest_v1(("tool_plane:mutate",)),
                authority_categories=("tool_plane",),
            ),
            tenant=_TENANT,
        )
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
        await service.promote(overlay.revision_id, user)

        async with sessions() as session:
            rows = (await session.scalars(select(ToolPlaneOverlayCompatibilityRow))).all()
        assert len(rows) == 1
        assert rows[0].base_revision_digest == base.revision_digest
        assert rows[0].overlay_revision_digest == overlay.revision_digest
        assert rows[0].compatible is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sql_repository_tenant_scope_hides_foreign_revision(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tool-plane-tenants.db'}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        first = SQLToolPlaneRevisionRepository(sessions, tenant=_TENANT)
        other_tenant = TenantReferenceV1(
            version=1,
            public_ref="tenant-cccccccccccccccc",
            digest="c" * 64,
        )
        second = SQLToolPlaneRevisionRepository(sessions, tenant=other_tenant)
        service = ToolPlaneRevisionService(
            repository=first,
            projection=InMemoryToolPlaneProjection(),
            validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
            durable=True,
        )
        staged = await service.stage(
            ScopedStageRevisionRequest(
                scope=ToolPlaneRevisionScopeV1(kind="deployment_base"),
                candidate={
                    "validation_policy_digest": _POLICY,
                    "mcp_servers": {},
                    "public_skills": {},
                    "managed_integrations": {},
                },
            ),
            _admin(),
        )

        assert await second.get(staged.revision_id) is None
        assert await second.events(staged.revision_id) == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sql_user_inventory_is_keyset_paged_and_bounded(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tool-plane-inventory.db'}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session, session.begin():
            session.add_all(
                [
                    UserRow(
                        id=user_id,
                        email=f"{user_id}@example.test",
                        system_role="user",
                    )
                    for user_id in ("user-c", "user-a", "user-b")
                ]
            )

        snapshot = await SQLToolPlaneUserInventory(
            sessions,
            page_size=1,
            maximum_subjects=3,
        ).snapshot()

        assert snapshot.subject_ids == ("user-a", "user-b", "user-c")
        with pytest.raises(ToolPlaneRevisionError) as caught:
            await SQLToolPlaneUserInventory(
                sessions,
                page_size=1,
                maximum_subjects=2,
            ).snapshot()
        assert caught.value.code == "bootstrap_inventory_changed"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sql_overlay_prepare_and_finalize_use_base_then_overlay_lock_order(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'locks.db'}")

    class RecordingRepository(SQLToolPlaneRevisionRepository):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.lock_order: list[str] = []

        async def _scope_row(
            self,
            session,
            scope,
            *,
            create,
            for_update=False,
        ):
            if for_update:
                self.lock_order.append(scope.key)
            return await super()._scope_row(
                session,
                scope,
                create=create,
                for_update=for_update,
            )

    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        repository = RecordingRepository(sessions, tenant=_TENANT)
        service = ToolPlaneRevisionService(
            repository=repository,
            projection=InMemoryToolPlaneProjection(),
            validator=DeterministicToolPlaneValidator(policy_digest=_POLICY),
            durable=True,
        )
        admin = _admin()
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
        repository.lock_order.clear()
        user = VerifiedActorContextV1(
            identity=InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="human",
                    subject_id="user-lock-order",
                    role="member",
                )
            ),
            credential=CredentialEvidenceV1(
                method="session",
                credential_ref=None,
                effective_authority_digest=effective_authority_digest_v1(("tool_plane:mutate",)),
                authority_categories=("tool_plane",),
            ),
            tenant=_TENANT,
        )
        overlay_scope = ToolPlaneRevisionScopeV1(
            kind="user_overlay",
            user_ref=user_scope_reference(user),
        )
        overlay = await service.stage(
            ScopedStageRevisionRequest(
                scope=overlay_scope,
                candidate={
                    "base_revision_digest": base.revision_digest,
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
        await service.promote(overlay.revision_id, user)

        assert repository.lock_order == [
            "deployment_base",
            overlay_scope.key,
            "deployment_base",
            overlay_scope.key,
        ]
    finally:
        await engine.dispose()

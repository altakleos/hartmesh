"""Host-only resource access independent of optional route authorization.

Mutations lock the parent before reading grants. SQLite reserves its writer
before the first read; Postgres uses FOR UPDATE. All changes require the
caller's current lifecycle generation. Files and writer retirement are
deliberately separate capabilities, not inferred from these metadata rows.
Actor arguments must originate from authenticated host context/adapters;
directory existence does not authenticate an arbitrary caller-supplied ID.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Custody, FeatureBinding, InvalidPrincipal, MutationMode, Permission, PrincipalRef, ResolvedPrincipal, Space, SpaceConflict, SpaceDenied, SpaceNotFound, validate_name, validate_permissions
from deerflow.spaces.principals import PrincipalResolver


def _stored_permissions(value: int, mode: MutationMode, *, allow_empty: bool = False) -> Permission:
    # IntFlag normalizes negative values into positive named flags. Check the
    # stored integer first so malformed rows cannot acquire valid authority.
    if type(value) is not int or not (0 if allow_empty else 1) <= value <= 31:
        raise ValueError("Invalid persisted space permissions")
    return validate_permissions(Permission(value), mode)


class SpaceRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], resolver: PrincipalResolver) -> None:
        self._sf = session_factory
        self._resolver = resolver

    async def _actor(self, actor: PrincipalRef) -> ResolvedPrincipal:
        if not isinstance(actor, PrincipalRef):
            raise InvalidPrincipal("Storage requires a host-validated principal")
        resolved = await self._resolver.resolve(actor)
        if not isinstance(resolved, ResolvedPrincipal) or resolved.reference != actor or type(resolved.can_provision_company) is not bool:
            raise InvalidPrincipal("Host identity resolution did not match the requested subject")
        return resolved

    @staticmethod
    def _view(row: SpaceRow, permissions: int, *, allow_empty: bool = False) -> Space:
        if row.custody_kind == "company" and (row.custodian_kind is not None or row.custodian_id is not None):
            raise ValueError("Invalid persisted company custody")
        custody = Custody(row.custody_kind, PrincipalRef(row.custodian_kind, row.custodian_id) if row.custody_kind == "personal" else None)
        feature_fields = (row.feature_namespace, row.feature_controller, row.feature_metadata_version)
        if any(value is None for value in feature_fields) and not all(value is None for value in feature_fields):
            raise ValueError("Incomplete persisted feature binding")
        feature = FeatureBinding(row.feature_namespace, row.feature_controller, row.feature_metadata_version) if row.feature_namespace is not None else None
        mode = MutationMode(row.mode)
        permissions = _stored_permissions(permissions, mode, allow_empty=allow_empty)
        if row.status == "archived":
            permissions &= ~(Permission.WRITE | Permission.OPERATE)
        return Space(row.id, row.backing_handle, row.name, custody, mode, row.generation, row.status, permissions, feature)

    @staticmethod
    async def _grant(session: AsyncSession, row: SpaceRow, actor: PrincipalRef) -> SpaceGrantRow | None:
        return await session.get(SpaceGrantRow, (row.id, actor.kind, actor.subject_id))

    @asynccontextmanager
    async def _mutation(self, *, actor: PrincipalRef, space_id: str, expected_generation: int, action: str, details: dict):
        await self._actor(actor)
        if type(expected_generation) is not int or expected_generation < 1:
            raise ValueError("Expected generation must be a positive integer")
        async with self._sf() as session:
            async with session.begin():
                if session.bind.dialect.name == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                row = (await session.execute(select(SpaceRow).where(SpaceRow.id == space_id).with_for_update())).scalar_one_or_none()
                if row is None or row.status != "active":
                    raise SpaceNotFound("Space is unavailable")
                # Identity may retire while this operation waits for another
                # writer. Check it again at the serialized admission boundary.
                await self._actor(actor)
                grant = await self._grant(session, row, actor)
                if grant is None:
                    raise SpaceDenied("Space administration is not granted")
                self._view(row, grant.permissions)  # Validate before any authority/commit.
                if not grant.permissions & Permission.ADMIN:
                    raise SpaceDenied("Space administration is not granted")
                if row.generation != expected_generation:
                    raise SpaceConflict("Stale space generation")
                if row.generation >= 2**31 - 1:
                    raise SpaceConflict("Space generation is exhausted; preserve the resource for provider recovery")
                yield session, row, grant
                row.generation += 1
                row.updated_at = datetime.now(UTC)
                session.add(SpaceEventRow(space_id=row.id, generation=row.generation, action=action, actor_kind=actor.kind, actor_id=actor.subject_id, details=details))

    async def create(self, *, actor: PrincipalRef, name: str, custody: Custody, mode: MutationMode, feature: FeatureBinding | None = None) -> Space:
        resolved = await self._actor(actor)
        name = validate_name(name)
        if not isinstance(custody, Custody) or not isinstance(mode, MutationMode) or (feature is not None and not isinstance(feature, FeatureBinding)):
            raise ValueError("Invalid space resource definition")
        if custody.kind == "personal" and custody.principal != actor:
            raise SpaceDenied("Personal custody cannot claim another principal")
        if custody.kind == "company" and not resolved.can_provision_company:
            raise SpaceDenied("Company provisioning requires host authority")
        if mode == MutationMode.MEDIATED and feature is None:
            raise ValueError("Mediated resources require a bound controller")
        permissions = Permission.READ | Permission.EXPORT | Permission.ADMIN | (Permission.WRITE if mode == MutationMode.NATIVE else Permission.OPERATE)
        row = SpaceRow(
            id=uuid.uuid4().hex,
            backing_handle=uuid.uuid4().hex,
            name=name,
            custody_kind=custody.kind,
            custodian_kind=custody.principal.kind if custody.principal else None,
            custodian_id=custody.principal.subject_id if custody.principal else None,
            mode=mode.value,
            status="active",
            generation=1,
            feature_namespace=feature.namespace if feature else None,
            feature_controller=feature.controller if feature else None,
            feature_metadata_version=feature.metadata_version if feature else None,
        )
        async with self._sf() as session:
            async with session.begin():
                session.add(row)
                await session.flush()
                session.add(SpaceGrantRow(space_id=row.id, principal_kind=actor.kind, subject_id=actor.subject_id, permissions=int(permissions)))
                session.add(SpaceEventRow(space_id=row.id, generation=1, action="create", actor_kind=actor.kind, actor_id=actor.subject_id, details={"mode": mode.value, "custody": custody.kind}))
            return self._view(row, int(permissions))

    async def get(self, *, actor: PrincipalRef, space_id: str, permission: Permission = Permission.READ, expected_generation: int | None = None) -> Space:
        await self._actor(actor)
        if not isinstance(permission, Permission) or not permission or int(permission) & ~31:
            raise ValueError("A valid nonempty permission is required")
        if expected_generation is not None and (type(expected_generation) is not int or expected_generation < 1):
            raise ValueError("Expected generation must be a positive integer")
        async with self._sf() as session:
            result = (
                await session.execute(
                    select(SpaceRow, SpaceGrantRow.permissions)
                    .join(SpaceGrantRow, SpaceGrantRow.space_id == SpaceRow.id)
                    .where(SpaceRow.id == space_id, SpaceRow.status.in_(("active", "archived")), SpaceGrantRow.principal_kind == actor.kind, SpaceGrantRow.subject_id == actor.subject_id)
                )
            ).one_or_none()
            if result is None:
                raise SpaceNotFound("Space is unavailable")
            row, permissions = result
            if permissions & permission != permission or (row.status == "archived" and permission & (Permission.WRITE | Permission.OPERATE)):
                raise SpaceDenied("Space operation is not granted")
            if expected_generation is not None and row.generation != expected_generation:
                raise SpaceConflict("Stale space generation")
            return self._view(row, permissions)

    async def list(self, *, actor: PrincipalRef, limit: int = 100, offset: int = 0) -> list[Space]:
        await self._actor(actor)
        if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0:
            raise ValueError("Invalid space pagination")
        async with self._sf() as session:
            rows = await session.execute(
                select(SpaceRow, SpaceGrantRow.permissions)
                .join(SpaceGrantRow, SpaceGrantRow.space_id == SpaceRow.id)
                .where(SpaceGrantRow.principal_kind == actor.kind, SpaceGrantRow.subject_id == actor.subject_id, SpaceGrantRow.permissions.op("&")(int(Permission.READ)) != 0, SpaceRow.status.in_(("active", "archived")))
                .order_by(SpaceRow.created_at, SpaceRow.id)
                .limit(limit)
                .offset(offset)
            )
            return [self._view(row, permissions) for row, permissions in rows]

    async def rename(self, *, actor: PrincipalRef, space_id: str, name: str, expected_generation: int) -> Space:
        name = validate_name(name)
        async with self._mutation(actor=actor, space_id=space_id, expected_generation=expected_generation, action="rename", details={"name": name}) as (_, row, grant):
            row.name = name
        return self._view(row, grant.permissions)

    async def set_grant(self, *, actor: PrincipalRef, space_id: str, subject: PrincipalRef, permissions: Permission, expected_generation: int, acknowledge_existing_data: bool = False) -> Space:
        if not isinstance(subject, PrincipalRef):
            raise InvalidPrincipal("Membership requires a typed principal")
        if not isinstance(permissions, Permission):
            raise ValueError("Invalid space permissions")
        # Retired members must remain removable. Only positive authority grants
        # require a currently resolvable recipient; revocation creates no rights.
        if permissions:
            await self._actor(subject)
        details = {"subject_kind": subject.kind, "subject_id": subject.subject_id, "permissions": int(permissions)}
        async with self._mutation(actor=actor, space_id=space_id, expected_generation=expected_generation, action="grant" if permissions else "revoke", details=details) as (session, row, actor_grant):
            if permissions:
                await self._actor(subject)
            permissions = validate_permissions(permissions, MutationMode(row.mode))
            existing = await self._grant(session, row, subject)
            previous = _stored_permissions(existing.permissions, MutationMode(row.mode)) if existing else Permission(0)
            if permissions & (Permission.READ | Permission.EXPORT) & ~previous and acknowledge_existing_data is not True:
                raise SpaceDenied("Granting access to existing data requires explicit acknowledgement")
            if previous & Permission.ADMIN and not permissions & Permission.ADMIN:
                grants = (await session.execute(select(SpaceGrantRow).where(SpaceGrantRow.space_id == row.id, SpaceGrantRow.permissions.op("&")(int(Permission.ADMIN)) != 0))).scalars().all()
                active_other_admin = False
                for admin in grants:
                    _stored_permissions(admin.permissions, MutationMode(row.mode))
                    reference = PrincipalRef(admin.principal_kind, admin.subject_id)
                    if reference == subject:
                        continue
                    try:
                        await self._actor(reference)
                    except InvalidPrincipal:
                        continue
                    active_other_admin = True
                if not active_other_admin:
                    raise SpaceConflict("A space must retain an administrator")
            if not permissions:
                if existing is not None:
                    await session.delete(existing)
            elif existing is not None:
                existing.permissions = int(permissions)
            else:
                session.add(SpaceGrantRow(space_id=row.id, principal_kind=subject.kind, subject_id=subject.subject_id, permissions=int(permissions)))
        return self._view(row, int(permissions) if subject == actor else actor_grant.permissions, allow_empty=subject == actor and not permissions)

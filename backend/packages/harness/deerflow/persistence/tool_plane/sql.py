"""Tenant-bound SQL repository for governed tool-plane revisions."""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from deerflow_extension_api import TenantReferenceV1
from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.tool_plane.model import (
    ToolPlaneOverlayCompatibilityRow,
    ToolPlaneRevisionEventRow,
    ToolPlaneRevisionRow,
    ToolPlaneScopeRow,
)
from deerflow.persistence.user.model import UserRow
from deerflow.tool_plane.contracts import ToolPlaneRevisionError, ToolPlaneRevisionScopeV1
from deerflow.tool_plane.service import (
    OverlayCompatibilityV1,
    RevisionEventV1,
    ToolPlaneRevisionRecord,
    ToolPlaneUserInventorySnapshot,
    ToolPlaneValidationFindingV1,
    ToolPlaneValidationReportV1,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class SQLToolPlaneUserInventory:
    """Authoritative, bounded keyset-paged inventory of registered users."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        page_size: int = 500,
        maximum_subjects: int = 10_000,
    ) -> None:
        if page_size < 1 or page_size > 1_000:
            raise ValueError("page_size must be between 1 and 1000")
        if maximum_subjects < 1 or maximum_subjects > 10_000:
            raise ValueError("maximum_subjects must be between 1 and 10000")
        self._sf = session_factory
        self._page_size = page_size
        self._maximum_subjects = maximum_subjects

    async def snapshot(self) -> ToolPlaneUserInventorySnapshot:
        """Read one bounded keyset-paged snapshot from registered users."""

        subject_ids: list[str] = []
        cursor: str | None = None
        async with self._sf() as session:
            while True:
                statement = select(UserRow.id).order_by(UserRow.id).limit(self._page_size)
                if cursor is not None:
                    statement = statement.where(UserRow.id > cursor)
                page = list((await session.scalars(statement)).all())
                if not page:
                    break
                subject_ids.extend(str(subject_id) for subject_id in page)
                if len(subject_ids) > self._maximum_subjects:
                    raise ToolPlaneRevisionError("bootstrap_inventory_changed")
                cursor = str(page[-1])
                if len(page) < self._page_size:
                    break
        return ToolPlaneUserInventorySnapshot(tuple(subject_ids))


class SQLToolPlaneRevisionRepository:
    """SQL source of truth with per-scope optimistic promotion fences."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tenant: TenantReferenceV1,
    ) -> None:
        if not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        self._sf = session_factory
        self.tenant = tenant
        # SQLite ignores SELECT FOR UPDATE.  This lock preserves the same
        # prepare ordering inside a process; PostgreSQL additionally takes the
        # database row lock for cross-process coordination.
        self._prepare_lock = asyncio.Lock()

    def _tenant_predicate(self, row_type: Any) -> Any:
        return and_(
            row_type.tenant_ref == self.tenant.public_ref,
            row_type.tenant_digest == self.tenant.digest,
        )

    @staticmethod
    def _scope_ref(scope: ToolPlaneRevisionScopeV1) -> str:
        return scope.user_ref or ""

    def _scope_predicate(self, row_type: Any, scope: ToolPlaneRevisionScopeV1) -> Any:
        return and_(
            self._tenant_predicate(row_type),
            row_type.scope_kind == scope.kind,
            row_type.scope_ref == self._scope_ref(scope),
        )

    async def _scope_row(
        self,
        session: AsyncSession,
        scope: ToolPlaneRevisionScopeV1,
        *,
        create: bool,
        for_update: bool = False,
    ) -> ToolPlaneScopeRow | None:
        statement = select(ToolPlaneScopeRow).where(self._scope_predicate(ToolPlaneScopeRow, scope))
        if for_update:
            statement = statement.with_for_update()
        row = (await session.scalars(statement)).one_or_none()
        if row is None and create:
            row = ToolPlaneScopeRow(
                id=str(uuid.uuid4()),
                tenant_ref=self.tenant.public_ref,
                tenant_digest=self.tenant.digest,
                scope_kind=scope.kind,
                scope_ref=self._scope_ref(scope),
                generation=0,
                overlay_set_generation=0,
                bootstrap_required=False,
                updated_at=_now(),
            )
            session.add(row)
            await session.flush()
        return row

    async def initialize(self, *, existing_projection: bool) -> None:
        """Create base scope state and set the legacy bootstrap gate if needed."""

        scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
        async with self._sf() as session, session.begin():
            row = await self._scope_row(session, scope, create=True, for_update=True)
            assert row is not None
            revision_exists = (await session.scalar(select(ToolPlaneRevisionRow.id).where(self._tenant_predicate(ToolPlaneRevisionRow)).limit(1))) is not None
            if not revision_exists and row.active_revision_id is None and existing_projection:
                row.bootstrap_required = True
                row.updated_at = _now()

    async def bootstrap_required(self) -> bool:
        """Return the durable deployment bootstrap gate."""

        scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
        async with self._sf() as session:
            row = await self._scope_row(session, scope, create=False)
            return bool(row is not None and row.bootstrap_required)

    async def clear_bootstrap(self) -> None:
        """Clear the durable bootstrap gate after exact verification."""

        scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
        async with self._sf() as session, session.begin():
            row = await self._scope_row(session, scope, create=True, for_update=True)
            assert row is not None
            row.bootstrap_required = False
            row.updated_at = _now()

    def _event(
        self,
        record: ToolPlaneRevisionRecord | ToolPlaneRevisionRow,
        *,
        state: str,
        actor_digest: str,
        safe_details: Mapping[str, object] | None = None,
    ) -> ToolPlaneRevisionEventRow:
        return ToolPlaneRevisionEventRow(
            id=str(uuid.uuid4()),
            tenant_ref=self.tenant.public_ref,
            tenant_digest=self.tenant.digest,
            revision_id=record.revision_id if isinstance(record, ToolPlaneRevisionRecord) else record.id,
            state=state,
            actor_digest=actor_digest,
            safe_details_json=copy.deepcopy(dict(safe_details or {})),
            occurred_at=_now(),
        )

    @staticmethod
    def _report_from_json(value: object) -> ToolPlaneValidationReportV1 | None:
        if value is None:
            return None
        if not isinstance(value, Mapping) or value.get("version") != 1:
            raise ToolPlaneRevisionError("validation_report_invalid")
        findings_value = value.get("findings")
        versions = value.get("validator_versions")
        if not isinstance(findings_value, list) or not isinstance(versions, Mapping):
            raise ToolPlaneRevisionError("validation_report_invalid")
        validated_raw = value.get("validated_at")
        if not isinstance(validated_raw, str):
            raise ToolPlaneRevisionError("validation_report_invalid")
        try:
            validated_at = datetime.fromisoformat(validated_raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ToolPlaneRevisionError("validation_report_invalid") from exc
        report = ToolPlaneValidationReportV1(
            revision_digest=str(value.get("revision_digest")),
            content_digest=str(value.get("content_digest")),
            validator_policy_digest=str(value.get("validator_policy_digest")),
            validator_versions={str(key): str(item) for key, item in versions.items()},
            result=value.get("result"),  # type: ignore[arg-type]
            findings=tuple(
                ToolPlaneValidationFindingV1(
                    code=str(item.get("code")),
                    severity=item.get("severity"),  # type: ignore[arg-type]
                    location=item.get("location"),  # type: ignore[arg-type]
                )
                for item in findings_value
                if isinstance(item, Mapping)
            ),
            validated_at=validated_at,
        )
        if report.report_digest != value.get("report_digest"):
            raise ToolPlaneRevisionError("validation_report_invalid")
        return report

    @classmethod
    def _record(cls, row: ToolPlaneRevisionRow) -> ToolPlaneRevisionRecord:
        report = cls._report_from_json(row.validation_report_json)
        if report is not None and report.report_digest != row.validation_report_digest:
            raise ToolPlaneRevisionError("validation_report_invalid")
        scope = ToolPlaneRevisionScopeV1(
            kind=row.scope_kind,  # type: ignore[arg-type]
            user_ref=row.scope_ref or None,
        )
        return ToolPlaneRevisionRecord(
            revision_id=row.id,
            revision_digest=row.revision_digest,
            tenant_ref=row.tenant_ref,
            tenant_digest=row.tenant_digest,
            scope=scope,
            content_digest=row.content_digest,
            manifest=copy.deepcopy(row.manifest_json),
            parent_revision_digest=row.parent_revision_digest,
            base_revision_digest=row.base_revision_digest,
            state=row.state,  # type: ignore[arg-type]
            staging_actor_digest=row.staging_actor_digest,
            staged_at=_aware(row.staged_at) or _now(),
            validation_report=report,
            promotion_actor_digest=row.promotion_actor_digest,
            previous_revision_id=row.previous_revision_id,
            desired_projection_digest=row.desired_projection_digest,
            observed_projection_digest=row.observed_projection_digest,
            promoted_at=_aware(row.promoted_at),
            rollback_source_revision_id=row.rollback_source_revision_id,
            bootstrap_inventory_digest=row.bootstrap_inventory_digest,
            bootstrap_source_digest=getattr(row, "bootstrap_source_digest", None),
            storage_subject_id=getattr(row, "storage_subject_id", None),
            bootstrap_overlay_revision_ids=tuple(getattr(row, "bootstrap_overlay_revision_ids_json", None) or ()),
            bootstrap_inventory_subject_ids=tuple(getattr(row, "bootstrap_inventory_subject_ids_json", None) or ()),
        )

    async def add(self, record: ToolPlaneRevisionRecord) -> None:
        """Append one staged revision and its first transition atomically."""

        if record.tenant_digest != self.tenant.digest or record.tenant_ref != self.tenant.public_ref:
            raise ToolPlaneRevisionError("promotion_not_authorized")
        async with self._sf() as session, session.begin():
            await self._scope_row(session, record.scope, create=True)
            row = self._row_for_record(record)
            session.add(row)
            await session.flush((row,))
            session.add(self._staged_event(record))

    def _row_for_record(self, record: ToolPlaneRevisionRecord) -> ToolPlaneRevisionRow:
        return ToolPlaneRevisionRow(
            id=record.revision_id,
            schema_writer_version=1,
            tenant_ref=self.tenant.public_ref,
            tenant_digest=self.tenant.digest,
            scope_kind=record.scope.kind,
            scope_ref=self._scope_ref(record.scope),
            revision_digest=record.revision_digest,
            content_digest=record.content_digest,
            manifest_json=copy.deepcopy(dict(record.manifest)),
            parent_revision_digest=record.parent_revision_digest,
            base_revision_digest=record.base_revision_digest,
            state="staged",
            staging_actor_digest=record.staging_actor_digest,
            staged_at=record.staged_at,
            rollback_source_revision_id=record.rollback_source_revision_id,
            bootstrap_inventory_digest=record.bootstrap_inventory_digest,
            bootstrap_source_digest=record.bootstrap_source_digest,
            storage_subject_id=record.storage_subject_id,
            bootstrap_overlay_revision_ids_json=list(record.bootstrap_overlay_revision_ids),
            bootstrap_inventory_subject_ids_json=list(record.bootstrap_inventory_subject_ids),
        )

    def _staged_event(self, record: ToolPlaneRevisionRecord) -> ToolPlaneRevisionEventRow:
        return self._event(
            record,
            state="staged",
            actor_digest=record.staging_actor_digest,
            safe_details=(
                {
                    "operation": "rollback",
                    "source_revision_id": record.rollback_source_revision_id,
                }
                if record.rollback_source_revision_id is not None
                else None
            ),
        )

    async def add_bootstrap(
        self,
        base: ToolPlaneRevisionRecord,
        overlays: tuple[ToolPlaneRevisionRecord, ...],
    ) -> None:
        """Append a base and all captured overlays in one transaction."""

        records = (base, *overlays)
        if any(record.tenant_digest != self.tenant.digest or record.tenant_ref != self.tenant.public_ref for record in records):
            raise ToolPlaneRevisionError("promotion_not_authorized")
        async with self._sf() as session, session.begin():
            for record in records:
                await self._scope_row(session, record.scope, create=True)
                row = self._row_for_record(record)
                session.add(row)
                await session.flush((row,))
                session.add(self._staged_event(record))

    @classmethod
    def _compatibility(
        cls,
        row: ToolPlaneOverlayCompatibilityRow,
    ) -> OverlayCompatibilityV1:
        report = cls._report_from_json(row.report_json)
        if report is None or report.report_digest != row.report_digest:
            raise ToolPlaneRevisionError("validation_report_invalid")
        attestation = OverlayCompatibilityV1(
            base_revision_digest=row.base_revision_digest,
            overlay_revision_digest=row.overlay_revision_digest,
            validator_policy_digest=row.validator_policy_digest,
            report=report,
            compatible=row.compatible,
            created_at=_aware(row.created_at) or _now(),
        )
        if attestation.attestation_digest != row.attestation_digest:
            raise ToolPlaneRevisionError("validation_report_invalid")
        return attestation

    async def save_compatibility(
        self,
        attestation: OverlayCompatibilityV1,
    ) -> OverlayCompatibilityV1:
        """Persist or return an identical immutable compatibility attestation."""

        try:
            async with self._sf() as session, session.begin():
                existing = (
                    await session.scalars(
                        select(ToolPlaneOverlayCompatibilityRow)
                        .where(
                            self._tenant_predicate(ToolPlaneOverlayCompatibilityRow),
                            ToolPlaneOverlayCompatibilityRow.base_revision_digest == attestation.base_revision_digest,
                            ToolPlaneOverlayCompatibilityRow.overlay_revision_digest == attestation.overlay_revision_digest,
                            ToolPlaneOverlayCompatibilityRow.validator_policy_digest == attestation.validator_policy_digest,
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if existing is not None:
                    return self._compatibility(existing)
                session.add(
                    ToolPlaneOverlayCompatibilityRow(
                        id=str(uuid.uuid4()),
                        tenant_ref=self.tenant.public_ref,
                        tenant_digest=self.tenant.digest,
                        base_revision_digest=attestation.base_revision_digest,
                        overlay_revision_digest=attestation.overlay_revision_digest,
                        validator_policy_digest=attestation.validator_policy_digest,
                        report_json=attestation.report.to_json(),
                        report_digest=attestation.report.report_digest,
                        attestation_digest=attestation.attestation_digest,
                        compatible=attestation.compatible,
                        created_at=attestation.created_at,
                    )
                )
        except IntegrityError as exc:
            # A second Gateway may win the immutable key race after our read.
            existing = await self.compatibility_attestation(
                base_revision_digest=attestation.base_revision_digest,
                overlay_revision_digest=attestation.overlay_revision_digest,
                validator_policy_digest=attestation.validator_policy_digest,
            )
            if existing is None:
                raise ToolPlaneRevisionError("revision_conflict") from exc
            return existing
        return attestation

    async def compatibility_attestation(
        self,
        *,
        base_revision_digest: str,
        overlay_revision_digest: str,
        validator_policy_digest: str,
    ) -> OverlayCompatibilityV1 | None:
        """Look up an exact base/overlay/policy attestation."""

        async with self._sf() as session:
            row = (
                await session.scalars(
                    select(ToolPlaneOverlayCompatibilityRow).where(
                        self._tenant_predicate(ToolPlaneOverlayCompatibilityRow),
                        ToolPlaneOverlayCompatibilityRow.base_revision_digest == base_revision_digest,
                        ToolPlaneOverlayCompatibilityRow.overlay_revision_digest == overlay_revision_digest,
                        ToolPlaneOverlayCompatibilityRow.validator_policy_digest == validator_policy_digest,
                    )
                )
            ).one_or_none()
            return None if row is None else self._compatibility(row)

    async def get(self, revision_id: str) -> ToolPlaneRevisionRecord | None:
        """Return one tenant-bound revision by identifier."""

        async with self._sf() as session:
            row = (
                await session.scalars(
                    select(ToolPlaneRevisionRow).where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.id == revision_id,
                    )
                )
            ).one_or_none()
            return None if row is None else self._record(row)

    async def list_scope(
        self,
        scope: ToolPlaneRevisionScopeV1,
        *,
        limit: int = 100,
    ) -> list[ToolPlaneRevisionRecord]:
        """Return bounded newest-first history for one tenant scope."""

        if type(limit) is not int or limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        async with self._sf() as session:
            rows = (await session.scalars(select(ToolPlaneRevisionRow).where(self._scope_predicate(ToolPlaneRevisionRow, scope)).order_by(ToolPlaneRevisionRow.staged_at.desc(), ToolPlaneRevisionRow.id.desc()).limit(limit))).all()
            return [self._record(row) for row in rows]

    async def events(self, revision_id: str) -> list[RevisionEventV1]:
        """Return append-order transition evidence for one revision."""

        async with self._sf() as session:
            rows = (
                await session.scalars(
                    select(ToolPlaneRevisionEventRow)
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionEventRow),
                        ToolPlaneRevisionEventRow.revision_id == revision_id,
                    )
                    .order_by(
                        ToolPlaneRevisionEventRow.occurred_at.asc(),
                        ToolPlaneRevisionEventRow.id.asc(),
                    )
                )
            ).all()
            return [
                RevisionEventV1(
                    event_id=row.id,
                    revision_id=row.revision_id,
                    state=row.state,  # type: ignore[arg-type]
                    actor_digest=row.actor_digest,
                    occurred_at=_aware(row.occurred_at) or _now(),
                    safe_details=copy.deepcopy(row.safe_details_json),
                )
                for row in rows
            ]

    async def active(
        self,
        scope: ToolPlaneRevisionScopeV1,
    ) -> ToolPlaneRevisionRecord | None:
        """Return the revision selected by one scope's active pointer."""

        async with self._sf() as session:
            scope_row = await self._scope_row(session, scope, create=False)
            if scope_row is None or scope_row.active_revision_id is None:
                return None
            row = await session.get(ToolPlaneRevisionRow, scope_row.active_revision_id)
            if row is None or row.tenant_digest != self.tenant.digest or row.tenant_ref != self.tenant.public_ref:
                raise ToolPlaneRevisionError("recovery_required")
            return self._record(row)

    async def generation(self, scope: ToolPlaneRevisionScopeV1) -> int:
        """Return one scope's current active-pointer generation."""

        async with self._sf() as session:
            row = await self._scope_row(session, scope, create=False)
            return 0 if row is None else int(row.generation)

    async def overlay_set_generation(self) -> int:
        """Return the deployment's active-overlay-set generation."""

        base = ToolPlaneRevisionScopeV1(kind="deployment_base")
        async with self._sf() as session:
            row = await self._scope_row(session, base, create=False)
            return 0 if row is None else int(row.overlay_set_generation)

    async def active_overlays(self) -> tuple[int, tuple[ToolPlaneRevisionRecord, ...]]:
        """Return a generation-bound snapshot of all active overlays."""

        base = ToolPlaneRevisionScopeV1(kind="deployment_base")
        async with self._sf() as session:
            base_row = await self._scope_row(session, base, create=False)
            generation = 0 if base_row is None else int(base_row.overlay_set_generation)
            rows = (
                await session.scalars(
                    select(ToolPlaneRevisionRow)
                    .join(
                        ToolPlaneScopeRow,
                        ToolPlaneScopeRow.active_revision_id == ToolPlaneRevisionRow.id,
                    )
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        self._tenant_predicate(ToolPlaneScopeRow),
                        ToolPlaneScopeRow.scope_kind == "user_overlay",
                    )
                    .order_by(ToolPlaneScopeRow.scope_ref.asc())
                )
            ).all()
            return generation, tuple(self._record(row) for row in rows)

    async def active_overlays_page(
        self,
        *,
        after_ref: str | None,
        limit: int,
    ) -> tuple[int, tuple[ToolPlaneRevisionRecord, ...], str | None]:
        """Return one generation-bound keyset page of active overlays."""

        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        base = ToolPlaneRevisionScopeV1(kind="deployment_base")
        async with self._sf() as session:
            base_row = await self._scope_row(session, base, create=False)
            generation = 0 if base_row is None else int(base_row.overlay_set_generation)
            statement = (
                select(ToolPlaneRevisionRow, ToolPlaneScopeRow.scope_ref)
                .join(
                    ToolPlaneScopeRow,
                    ToolPlaneScopeRow.active_revision_id == ToolPlaneRevisionRow.id,
                )
                .where(
                    self._tenant_predicate(ToolPlaneRevisionRow),
                    self._tenant_predicate(ToolPlaneScopeRow),
                    ToolPlaneScopeRow.scope_kind == "user_overlay",
                )
                .order_by(ToolPlaneScopeRow.scope_ref.asc())
                .limit(limit + 1)
            )
            if after_ref is not None:
                statement = statement.where(ToolPlaneScopeRow.scope_ref > after_ref)
            rows = list((await session.execute(statement)).all())
            selected = rows[:limit]
            next_cursor = str(selected[-1][1]) if len(rows) > limit and selected else None
            return (
                generation,
                tuple(self._record(row[0]) for row in selected),
                next_cursor,
            )

    async def begin_validation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
    ) -> ToolPlaneRevisionRecord:
        """Transition a staged revision to validating under a row lock."""

        async with self._sf() as session, session.begin():
            row = (
                await session.scalars(
                    select(ToolPlaneRevisionRow)
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.id == revision_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if row is None:
                raise ToolPlaneRevisionError("revision_not_found")
            if row.state != "staged":
                raise ToolPlaneRevisionError("validation_stale")
            row.state = "validating"
            row.validation_report_json = None
            row.validation_report_digest = None
            session.add(self._event(row, state="validating", actor_digest=actor_digest))
            await session.flush()
            return self._record(row)

    async def complete_validation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        report: ToolPlaneValidationReportV1,
    ) -> ToolPlaneRevisionRecord:
        """Persist an immutable report and terminal validation state."""

        async with self._sf() as session, session.begin():
            row = (
                await session.scalars(
                    select(ToolPlaneRevisionRow)
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.id == revision_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or row.state != "validating":
                raise ToolPlaneRevisionError("revision_conflict")
            if report.revision_digest != row.revision_digest or report.content_digest != row.content_digest:
                raise ToolPlaneRevisionError("validation_stale")
            row.state = "validated" if report.result == "passed" else "rejected"
            row.validation_report_json = report.to_json()
            row.validation_report_digest = report.report_digest
            session.add(
                self._event(
                    row,
                    state=row.state,
                    actor_digest=actor_digest,
                    safe_details={"report_digest": report.report_digest, "result": report.result},
                )
            )
            await session.flush()
            return self._record(row)

    async def prepare_activation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        expected_active_digest: str | None,
        expected_base_generation: int | None,
        expected_overlay_set_generation: int | None,
        required_compatibility: tuple[tuple[str, str, str], ...] = (),
    ) -> ToolPlaneRevisionRecord:
        """Journal prepared intent after checking all optimistic fences."""

        async with self._prepare_lock:
            async with self._sf() as session, session.begin():
                row = (
                    await session.scalars(
                        select(ToolPlaneRevisionRow)
                        .where(
                            self._tenant_predicate(ToolPlaneRevisionRow),
                            ToolPlaneRevisionRow.id == revision_id,
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if row is None:
                    raise ToolPlaneRevisionError("revision_not_found")
                scope = ToolPlaneRevisionScopeV1(
                    kind=row.scope_kind,  # type: ignore[arg-type]
                    user_ref=row.scope_ref or None,
                )
                # Every prepare takes the deployment-base scope row first. It is
                # the tenant-wide promotion mutex and establishes one lock order
                # for base and overlay transitions across Gateway processes.
                base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
                base_row = await self._scope_row(
                    session,
                    base_scope,
                    create=True,
                    for_update=True,
                )
                assert base_row is not None
                if scope.kind == "deployment_base":
                    scope_row = base_row
                else:
                    scope_row = await self._scope_row(
                        session,
                        scope,
                        create=True,
                        for_update=True,
                    )
                    assert scope_row is not None
                pending = await session.scalar(
                    select(ToolPlaneRevisionRow.id)
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.state.in_(("prepared", "recovery_required")),
                        ToolPlaneRevisionRow.id != revision_id,
                    )
                    .limit(1)
                )
                if pending is not None:
                    raise ToolPlaneRevisionError("recovery_required")
                if row.state not in {"validated", "superseded"}:
                    raise ToolPlaneRevisionError("validation_stale")
                if row.validation_report_json is None:
                    raise ToolPlaneRevisionError("validation_failed")
                active: ToolPlaneRevisionRow | None = None
                if scope_row.active_revision_id is not None:
                    active = await session.get(ToolPlaneRevisionRow, scope_row.active_revision_id)
                active_digest = None if active is None else active.revision_digest
                if active_digest != expected_active_digest or row.parent_revision_digest != active_digest:
                    raise ToolPlaneRevisionError("revision_conflict")
                if expected_base_generation is not None and int(base_row.generation) != expected_base_generation:
                    raise ToolPlaneRevisionError("base_revision_changed")
                if expected_overlay_set_generation is not None and int(base_row.overlay_set_generation) != expected_overlay_set_generation:
                    raise ToolPlaneRevisionError("active_overlay_set_changed")
                for base_digest, overlay_digest, policy_digest in required_compatibility:
                    attestation = (
                        await session.scalars(
                            select(ToolPlaneOverlayCompatibilityRow)
                            .where(
                                self._tenant_predicate(ToolPlaneOverlayCompatibilityRow),
                                ToolPlaneOverlayCompatibilityRow.base_revision_digest == base_digest,
                                ToolPlaneOverlayCompatibilityRow.overlay_revision_digest == overlay_digest,
                                ToolPlaneOverlayCompatibilityRow.validator_policy_digest == policy_digest,
                                ToolPlaneOverlayCompatibilityRow.compatible.is_(True),
                            )
                            .with_for_update()
                        )
                    ).one_or_none()
                    if attestation is None:
                        raise ToolPlaneRevisionError("overlay_preflight_incomplete")
                row.state = "prepared"
                row.promotion_actor_digest = actor_digest
                row.previous_revision_id = scope_row.active_revision_id
                row.desired_projection_digest = row.content_digest
                session.add(self._event(row, state="prepared", actor_digest=actor_digest))
                await session.flush()
                return self._record(row)

    async def finalize_activation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        observed_projection_digest: str,
    ) -> ToolPlaneRevisionRecord:
        """Move the active pointer after the projection digest is verified."""

        async with self._sf() as session, session.begin():
            row = (
                await session.scalars(
                    select(ToolPlaneRevisionRow)
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.id == revision_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or row.state not in {"prepared", "recovery_required"}:
                raise ToolPlaneRevisionError("revision_conflict")
            scope = ToolPlaneRevisionScopeV1(
                kind=row.scope_kind,  # type: ignore[arg-type]
                user_ref=row.scope_ref or None,
            )
            # Match prepare_activation's tenant-wide lock order: deployment
            # base first, then one user overlay. This prevents an overlay
            # finalize racing a second overlay prepare from taking the same
            # two scope rows in opposite order on PostgreSQL.
            base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
            base_row = await self._scope_row(
                session,
                base_scope,
                create=True,
                for_update=True,
            )
            assert base_row is not None
            scope_row = (
                base_row
                if scope.kind == "deployment_base"
                else await self._scope_row(
                    session,
                    scope,
                    create=True,
                    for_update=True,
                )
            )
            assert scope_row is not None
            if observed_projection_digest != row.desired_projection_digest:
                row.state = "recovery_required"
                row.observed_projection_digest = observed_projection_digest
                session.add(
                    self._event(
                        row,
                        state="recovery_required",
                        actor_digest=actor_digest,
                        safe_details={"reason": "projection_digest_mismatch"},
                    )
                )
                await session.flush()
                raise ToolPlaneRevisionError("projection_digest_mismatch")
            if row.previous_revision_id is not None and row.previous_revision_id != row.id:
                previous = await session.get(ToolPlaneRevisionRow, row.previous_revision_id)
                if previous is not None:
                    previous.state = "superseded"
                    session.add(self._event(previous, state="superseded", actor_digest=actor_digest))
            row.state = "promoted"
            row.observed_projection_digest = observed_projection_digest
            row.promoted_at = _now()
            scope_row.active_revision_id = row.id
            scope_row.generation = int(scope_row.generation) + 1
            scope_row.updated_at = _now()
            if scope.kind == "user_overlay":
                base_row.overlay_set_generation = int(base_row.overlay_set_generation) + 1
                base_row.updated_at = _now()
            session.add(self._event(row, state="promoted", actor_digest=actor_digest))
            await session.flush()
            return self._record(row)

    async def mark_recovery_required(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        reason: str,
    ) -> None:
        """Persist a recovery gate for a prepared projection failure."""

        async with self._sf() as session, session.begin():
            row = (
                await session.scalars(
                    select(ToolPlaneRevisionRow)
                    .where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.id == revision_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or row.state not in {"prepared", "recovery_required"}:
                return
            row.state = "recovery_required"
            session.add(
                self._event(
                    row,
                    state="recovery_required",
                    actor_digest=actor_digest,
                    safe_details={"reason": reason},
                )
            )

    async def prepared_or_recovery(self) -> tuple[ToolPlaneRevisionRecord, ...]:
        """Return every prepared or recovery-blocking revision for this tenant."""

        async with self._sf() as session:
            rows = (
                await session.scalars(
                    select(ToolPlaneRevisionRow).where(
                        self._tenant_predicate(ToolPlaneRevisionRow),
                        ToolPlaneRevisionRow.state.in_(("prepared", "recovery_required")),
                    )
                )
            ).all()
            return tuple(self._record(row) for row in rows)


__all__ = ["SQLToolPlaneRevisionRepository", "SQLToolPlaneUserInventory"]

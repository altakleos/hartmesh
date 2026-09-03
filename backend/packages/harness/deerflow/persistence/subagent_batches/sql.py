from __future__ import annotations

import base64
import json
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from deerflow_extension_api import TenantReferenceV1
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.subagent_batches.model import (
    SUBAGENT_BATCH_SCHEMA_WRITER_VERSION,
    SubagentBatchAttemptRow,
    SubagentBatchItemRow,
    SubagentBatchRow,
)
from deerflow.runtime.accepted_invocation import canonical_digest
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchItemV1,
    AcceptedBatchV1,
    BatchAdmissionConflict,
    BatchAdmissionError,
    BatchAttemptEvidenceV1,
    BatchItemRequestV1,
    ParentBoundBatchExecutionV1,
)
from deerflow.utils.time import coerce_iso

BATCH_ACTIVE_STATUSES = ("queued", "running", "paused")
BATCH_TERMINAL_STATUSES = ("completed", "failed", "cancelled")
ITEM_ACTIVE_STATUSES = ("queued", "leased", "running")
ITEM_TERMINAL_STATUSES = ("succeeded", "failed", "cancelled")
_BATCH_PUBLIC_FIELDS = (
    "id",
    "thread_id",
    "title",
    "subagent_type",
    "status",
    "total_items",
    "max_live_items",
    "max_running_items",
    "max_attempts",
    "parent_cancellable",
    "cancel_epoch",
    "acceptance_digest",
    "terminal_code",
    "accepted_at",
    "created_at",
    "updated_at",
    "completed_at",
)
_BATCH_TIMESTAMP_FIELDS = (
    "accepted_at",
    "created_at",
    "updated_at",
    "completed_at",
)
_ITEM_PUBLIC_FIELDS = (
    "id",
    "batch_id",
    "item_key",
    "position",
    "status",
    "attempt",
    "request_digest",
    "lease_epoch",
    "model_name",
    "result_truncated",
    "error",
    "stop_reason",
    "terminal_code",
    "terminal_evidence_digest",
    "token_usage",
    "started_at",
    "completed_at",
    "created_at",
    "updated_at",
)
_ITEM_TIMESTAMP_FIELDS = ("started_at", "completed_at", "created_at", "updated_at")
_BATCH_PARENT_CURSOR_VERSION = "deerflow.subagent-batch-parent-cursor/v1"
_MAX_BATCH_PARENT_PAGE_SIZE = 100


class SubagentBatchRepository:
    """Durable batch/item state with lease-based multi-worker claiming."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tenant: TenantReferenceV1 | None = None,
    ) -> None:
        self._sf = session_factory
        if tenant is not None and not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")
        self._tenant = tenant

    @property
    def tenant(self) -> TenantReferenceV1 | None:
        """Return the immutable tenant scope, if this is not a legacy adapter."""

        return self._tenant

    async def verify_schema_writer_compatibility(self) -> None:
        """Refuse mutation after a newer binary has written batch rows."""

        async with self._sf() as session:
            maximum = (await session.execute(select(func.max(SubagentBatchRow.schema_writer_version)))).scalar_one_or_none()
        if maximum is not None and int(maximum) > SUBAGENT_BATCH_SCHEMA_WRITER_VERSION:
            raise BatchAdmissionError("batch_schema_writer_unsupported")

    async def _now(
        self,
        session: AsyncSession,
        supplied: datetime | None,
    ) -> datetime:
        """Return the lease authority's time.

        Accepted rows always use database time.  The process-clock argument is
        retained solely for the explicit single-process legacy adapter used by
        SQLite tests and local compatibility mode.
        """

        if self._tenant is None and supplied is not None:
            return supplied
        bind = session.get_bind()
        clock = func.clock_timestamp() if bind.dialect.name == "postgresql" else func.current_timestamp()
        value = (await session.execute(select(clock))).scalar_one()
        if not isinstance(value, datetime):
            raise RuntimeError("database_time_unavailable")
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _can_execute(self, batch: SubagentBatchRow | None) -> bool:
        if batch is None:
            return False
        if self._tenant is None:
            return batch.schema_writer_version == 1 and batch.tenant_digest is None
        return batch.schema_writer_version == 2 and batch.tenant_digest == self._tenant.digest

    @staticmethod
    def _requires_attempt_fence(batch: SubagentBatchRow) -> bool:
        return batch.schema_writer_version == 2

    @staticmethod
    def _attempt_dict(row: SubagentBatchAttemptRow) -> dict[str, Any]:
        value = {
            "attempt_id": row.id,
            "batch_id": row.batch_id,
            "item_id": row.item_id,
            "attempt_number": row.attempt_number,
            "lease_epoch": row.lease_epoch,
            "worker_ref": row.worker_ref,
            "status": row.status,
            "consumed": row.consumed,
            "terminal_code": row.terminal_code,
            "evidence_digest": row.evidence_digest,
            "claimed_at": coerce_iso(row.claimed_at),
            "started_at": (None if row.started_at is None else coerce_iso(row.started_at)),
            "terminal_at": (None if row.terminal_at is None else coerce_iso(row.terminal_at)),
        }
        return value

    @staticmethod
    def _batch_dict(row: SubagentBatchRow) -> dict[str, Any]:
        """Return the stable owner-facing projection, never execution context."""
        data = {key: getattr(row, key) for key in _BATCH_PUBLIC_FIELDS}
        for key in _BATCH_TIMESTAMP_FIELDS:
            if data.get(key) is not None:
                data[key] = coerce_iso(data[key])
        data["compatibility_state"] = "accepted_v1" if row.schema_writer_version == 2 and row.acceptance_json is not None and row.acceptance_digest is not None else "legacy_unbound"
        data["evidence"] = (
            {
                "tenant_ref": row.tenant_ref,
                "tenant_digest": row.tenant_digest,
                "parent_run_id": row.run_id,
                "parent_invocation_digest": row.parent_invocation_digest,
                "parent_assembly_fingerprint": (row.parent_assembly_fingerprint),
                "parent_tool_receipt_id": row.parent_tool_receipt_id,
                "parent_tool_attempt": row.parent_tool_attempt,
                "subagent_catalog_digest": row.subagent_catalog_digest,
                "subagent_definition_digest": (row.subagent_definition_digest),
                "item_root_digest": row.item_root_digest,
            }
            if data["compatibility_state"] == "accepted_v1"
            else None
        )
        return data

    @staticmethod
    def _execution_batch_dict(row: SubagentBatchRow) -> dict[str, Any]:
        """Return worker-only fields required to reconstruct an execution."""
        if row.schema_writer_version == 2:
            if row.acceptance_json is None or row.execution_json is None:
                raise BatchAdmissionError("execution_material_unavailable")
            acceptance = AcceptedBatchV1.from_persisted_json(row.acceptance_json)
            execution = ParentBoundBatchExecutionV1.from_persisted_json(row.execution_json)
            if (
                row.acceptance_digest != acceptance.acceptance_digest
                or row.execution_digest != execution.execution_digest
                or execution.acceptance_digest != acceptance.acceptance_digest
                or acceptance.batch_id != row.id
                or execution.batch_id != row.id
                or acceptance.tenant.public_ref != row.tenant_ref
                or acceptance.tenant.digest != row.tenant_digest
                or acceptance.parent_run_id != row.run_id
                or acceptance.parent_thread_id != row.thread_id
                or acceptance.parent_tool_call_id != row.tool_call_id
                or acceptance.parent_invocation_digest != row.parent_invocation_digest
                or acceptance.parent_assembly_fingerprint != row.parent_assembly_fingerprint
                or acceptance.parent_tool_receipt_id != row.parent_tool_receipt_id
                or acceptance.parent_tool_attempt != row.parent_tool_attempt
                or acceptance.subagent_catalog_digest != row.subagent_catalog_digest
                or acceptance.subagent_definition_digest != row.subagent_definition_digest
                or acceptance.item_root_digest != row.item_root_digest
                or acceptance.item_count != row.total_items
                or acceptance.limits.max_live_items != row.max_live_items
                or acceptance.limits.max_running_items != row.max_running_items
                or acceptance.limits.max_attempts != row.max_attempts
                or acceptance.parent_cancellable != row.parent_cancellable
                or execution.user_id != row.user_id
                or execution.selected_subagent_name != row.subagent_type
            ):
                raise BatchAdmissionError("execution_material_unavailable")
            try:
                execution.verify_against_acceptance(acceptance)
            except BatchAdmissionError as exc:
                raise BatchAdmissionError("execution_material_unavailable") from exc
            return {
                "id": row.id,
                "user_id": row.user_id,
                "thread_id": row.thread_id,
                "run_id": row.run_id,
                "acceptance": acceptance,
                "execution": execution,
            }
        return {
            "id": row.id,
            "user_id": row.user_id,
            "thread_id": row.thread_id,
            "run_id": row.run_id,
            "execution_spec": row.execution_spec,
            "compatibility_state": "legacy_unbound",
        }

    @staticmethod
    def _item_matches_acceptance(
        item: SubagentBatchItemRow,
        accepted: AcceptedBatchV1,
    ) -> bool:
        if item.position < 0 or item.position >= accepted.item_count:
            return False
        try:
            expected = AcceptedBatchItemV1.from_request(
                BatchItemRequestV1(
                    key=item.item_key,
                    prompt=item.prompt,
                ),
                batch_id=accepted.batch_id,
                ordinal=item.position,
            )
        except BatchAdmissionError:
            return False
        return expected.item_id == item.id and expected.request_digest == item.request_digest

    def _tenant_visible_clause(self):
        if self._tenant is None:
            return SubagentBatchRow.tenant_digest.is_(None)
        return and_(
            SubagentBatchRow.schema_writer_version == 2,
            SubagentBatchRow.tenant_digest == self._tenant.digest,
        )

    def _tenant_executable_clause(self):
        if self._tenant is None:
            return and_(
                SubagentBatchRow.schema_writer_version == 1,
                SubagentBatchRow.tenant_digest.is_(None),
            )
        return and_(
            SubagentBatchRow.schema_writer_version == 2,
            SubagentBatchRow.tenant_digest == self._tenant.digest,
        )

    async def accept_batch(
        self,
        *,
        accepted: AcceptedBatchV1,
        execution: ParentBoundBatchExecutionV1,
        item_requests: tuple[BatchItemRequestV1, ...],
        user_id: str,
        submission_key: str,
        title: str,
        subagent_type: str,
    ) -> dict[str, Any]:
        """Atomically persist immutable acceptance, execution, and item inputs."""

        tenant = self._tenant
        if tenant is None:
            raise BatchAdmissionError("batch_tenant_unavailable")
        if accepted.tenant != tenant:
            raise BatchAdmissionError("batch_tenant_mismatch")
        if (
            execution.batch_id != accepted.batch_id
            or execution.acceptance_digest != accepted.acceptance_digest
            or execution.catalog.digest != accepted.subagent_catalog_digest
            or execution.selected_definition.definition_digest != accepted.subagent_definition_digest
            or execution.user_id != user_id
            or execution.selected_subagent_name != subagent_type
        ):
            raise BatchAdmissionError("batch_acceptance_mismatch")
        execution.verify_against_acceptance(accepted)
        operational_items = tuple(item_requests)
        immutable_items = AcceptedBatchItemV1.from_requests(
            operational_items,
            batch_id=accepted.batch_id,
        )
        if len(operational_items) != accepted.item_count or AcceptedBatchItemV1.root_digest(immutable_items) != accepted.item_root_digest:
            raise BatchAdmissionError("batch_item_count_invalid")

        async with self._sf() as session:
            existing = await self._find_submission(
                session,
                parent_tool_receipt_id=accepted.parent_tool_receipt_id,
                submission_key=submission_key,
            )
            if existing is not None:
                return await self._resolve_idempotent_submission(
                    session,
                    existing,
                    acceptance_digest=accepted.acceptance_digest,
                )

            now = await self._now(session, None)
            batch = SubagentBatchRow(
                id=accepted.batch_id,
                schema_writer_version=SUBAGENT_BATCH_SCHEMA_WRITER_VERSION,
                tenant_ref=tenant.public_ref,
                tenant_digest=tenant.digest,
                user_id=user_id,
                thread_id=accepted.parent_thread_id,
                run_id=accepted.parent_run_id,
                tool_call_id=accepted.parent_tool_call_id,
                submission_key=submission_key,
                title=title,
                subagent_type=subagent_type,
                status="queued",
                total_items=accepted.item_count,
                max_live_items=accepted.limits.max_live_items,
                max_running_items=accepted.limits.max_running_items,
                max_attempts=accepted.limits.max_attempts,
                # Retained only for the explicit legacy decoder. New execution
                # consumes the separately versioned protected payload below.
                execution_spec={},
                acceptance_json=accepted.to_persisted_json(),
                acceptance_digest=accepted.acceptance_digest,
                execution_json=execution.to_persisted_json(),
                execution_digest=execution.execution_digest,
                parent_invocation_digest=accepted.parent_invocation_digest,
                parent_assembly_fingerprint=accepted.parent_assembly_fingerprint,
                parent_tool_receipt_id=accepted.parent_tool_receipt_id,
                parent_tool_attempt=accepted.parent_tool_attempt,
                subagent_catalog_digest=accepted.subagent_catalog_digest,
                subagent_definition_digest=accepted.subagent_definition_digest,
                item_root_digest=accepted.item_root_digest,
                parent_cancellable=accepted.parent_cancellable,
                cancel_epoch=0,
                accepted_at=now,
                created_at=now,
                updated_at=now,
            )
            rows = [
                SubagentBatchItemRow(
                    id=immutable.item_id,
                    batch_id=accepted.batch_id,
                    item_key=operational.key,
                    position=immutable.ordinal,
                    prompt=operational.prompt,
                    request_digest=immutable.request_digest,
                    status="pending",
                    attempt=0,
                    lease_epoch=0,
                    result_truncated=False,
                    created_at=now,
                    updated_at=now,
                )
                for operational, immutable in zip(
                    operational_items,
                    immutable_items,
                    strict=True,
                )
            ]
            try:
                session.add(batch)
                await session.flush()
                session.add_all(rows)
                await session.commit()
            except IntegrityError:
                await session.rollback()
                existing = await self._find_submission(
                    session,
                    parent_tool_receipt_id=accepted.parent_tool_receipt_id,
                    submission_key=submission_key,
                )
                if existing is not None:
                    return await self._resolve_idempotent_submission(
                        session,
                        existing,
                        acceptance_digest=accepted.acceptance_digest,
                    )
                raise
            return await self._with_counts(session, batch)

    async def _find_submission(
        self,
        session: AsyncSession,
        *,
        parent_tool_receipt_id: str,
        submission_key: str,
    ) -> SubagentBatchRow | None:
        if self._tenant is None:
            return None
        return (
            await session.execute(
                select(SubagentBatchRow).where(
                    SubagentBatchRow.schema_writer_version == 2,
                    SubagentBatchRow.tenant_digest == self._tenant.digest,
                    SubagentBatchRow.parent_tool_receipt_id == parent_tool_receipt_id,
                    SubagentBatchRow.submission_key == submission_key,
                )
            )
        ).scalar_one_or_none()

    async def _resolve_idempotent_submission(
        self,
        session: AsyncSession,
        existing: SubagentBatchRow,
        *,
        acceptance_digest: str,
    ) -> dict[str, Any]:
        if existing.acceptance_digest != acceptance_digest:
            raise BatchAdmissionConflict()
        return await self._with_counts(session, existing)

    @staticmethod
    def _item_dict(row: SubagentBatchItemRow, *, include_result: bool = False) -> dict[str, Any]:
        data = {key: getattr(row, key) for key in _ITEM_PUBLIC_FIELDS}
        if include_result:
            data["result"] = row.result
            data["result_preview"] = row.result_preview
        for key in _ITEM_TIMESTAMP_FIELDS:
            if data.get(key) is not None:
                data[key] = coerce_iso(data[key])
        return data

    async def create_batch(
        self,
        *,
        batch_id: str,
        user_id: str,
        thread_id: str,
        run_id: str | None,
        tool_call_id: str | None,
        submission_key: str,
        title: str,
        subagent_type: str,
        items: list[dict[str, str]],
        max_live_items: int,
        max_running_items: int,
        max_attempts: int,
        execution_spec: dict[str, Any],
    ) -> dict[str, Any]:
        if self._tenant is not None:
            # This compatibility adapter intentionally has no accepted-parent
            # inputs. A tenant-bound production repository must never create
            # a new row whose evidence can only be labelled legacy_unbound.
            raise BatchAdmissionError("legacy_batch_unbound")
        now = datetime.now(UTC)
        batch = SubagentBatchRow(
            id=batch_id,
            schema_writer_version=1,
            user_id=user_id,
            thread_id=thread_id,
            run_id=run_id,
            tool_call_id=tool_call_id,
            submission_key=submission_key,
            title=title,
            subagent_type=subagent_type,
            status="queued",
            total_items=len(items),
            max_live_items=max_live_items,
            max_running_items=max_running_items,
            max_attempts=max_attempts,
            execution_spec=execution_spec,
            created_at=now,
            updated_at=now,
        )
        rows = [
            SubagentBatchItemRow(
                id=f"batch-item-{uuid.uuid4().hex}",
                batch_id=batch_id,
                item_key=item["key"],
                position=position,
                prompt=item["prompt"],
                status="pending",
                attempt=0,
                result_truncated=False,
                created_at=now,
                updated_at=now,
            )
            for position, item in enumerate(items)
        ]
        async with self._sf() as session:
            try:
                session.add(batch)
                # The models intentionally do not declare an ORM relationship;
                # flush the parent explicitly so SQLite's immediate FK check
                # never observes item inserts before their batch row. Keep the
                # flush inside the idempotency handler: a duplicate submission
                # key can fail here before commit.
                await session.flush()
                session.add_all(rows)
                await session.commit()
            except IntegrityError:
                await session.rollback()
                existing = (
                    await session.execute(
                        select(SubagentBatchRow).where(
                            SubagentBatchRow.user_id == user_id,
                            SubagentBatchRow.submission_key == submission_key,
                        )
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    return await self._with_counts(session, existing)
                raise
            return await self._with_counts(session, batch)

    async def _counts(self, session: AsyncSession, batch_id: str) -> Counter[str]:
        rows = await session.execute(select(SubagentBatchItemRow.status, func.count()).where(SubagentBatchItemRow.batch_id == batch_id).group_by(SubagentBatchItemRow.status))
        return Counter({status: int(count) for status, count in rows})

    async def _with_counts(self, session: AsyncSession, batch: SubagentBatchRow) -> dict[str, Any]:
        counts = await self._counts(session, batch.id)
        data = self._batch_dict(batch)
        data["counts"] = {status: counts.get(status, 0) for status in ("pending", "queued", "leased", "running", "succeeded", "failed", "cancelled")}
        return data

    async def get_batch(self, batch_id: str, *, user_id: str) -> dict[str, Any] | None:
        async with self._sf() as session:
            batch = (
                await session.execute(
                    select(SubagentBatchRow).where(
                        SubagentBatchRow.id == batch_id,
                        SubagentBatchRow.user_id == user_id,
                        self._tenant_visible_clause(),
                    )
                )
            ).scalar_one_or_none()
            if batch is None:
                return None
            return await self._with_counts(session, batch)

    async def list_by_thread(self, thread_id: str, *, user_id: str, limit: int = 20) -> list[dict[str, Any]]:
        async with self._sf() as session:
            rows = list(
                (
                    await session.execute(
                        select(SubagentBatchRow)
                        .where(
                            SubagentBatchRow.thread_id == thread_id,
                            SubagentBatchRow.user_id == user_id,
                            self._tenant_visible_clause(),
                        )
                        .order_by(SubagentBatchRow.created_at.desc(), SubagentBatchRow.id.desc())
                        .limit(limit)
                    )
                ).scalars()
            )
            return [await self._with_counts(session, row) for row in rows]

    async def load_execution(self, batch_id: str) -> dict[str, Any]:
        """Load protected execution material for this repository's tenant."""

        async with self._sf() as session:
            batch = (
                await session.execute(
                    select(SubagentBatchRow).where(
                        SubagentBatchRow.id == batch_id,
                        self._tenant_executable_clause(),
                    )
                )
            ).scalar_one_or_none()
            if batch is None:
                raise BatchAdmissionError("execution_material_unavailable")
            if batch.schema_writer_version == 1:
                raise BatchAdmissionError("legacy_batch_unbound")
            return self._execution_batch_dict(batch)

    async def list_items(
        self,
        batch_id: str,
        *,
        user_id: str,
        offset: int = 0,
        limit: int = 100,
        status: str | None = None,
        include_prompt: bool = False,
        include_result: bool = False,
    ) -> list[dict[str, Any]] | None:
        async with self._sf() as session:
            batch = (
                await session.execute(
                    select(SubagentBatchRow).where(
                        SubagentBatchRow.id == batch_id,
                        SubagentBatchRow.user_id == user_id,
                        self._tenant_visible_clause(),
                    )
                )
            ).scalar_one_or_none()
            if batch is None:
                return None
            stmt = select(SubagentBatchItemRow).where(SubagentBatchItemRow.batch_id == batch_id)
            if status is not None:
                stmt = stmt.where(SubagentBatchItemRow.status == status)
            stmt = stmt.order_by(SubagentBatchItemRow.position).offset(offset).limit(limit)
            rows = list((await session.execute(stmt)).scalars())
            values = []
            for row in rows:
                value = self._item_dict(row, include_result=include_result)
                if include_prompt:
                    value["prompt"] = row.prompt
                values.append(value)
            return values

    async def claim_items(
        self,
        *,
        now: datetime | None,
        lease_owner: str,
        lease_seconds: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Promote pending work and atomically claim runnable items."""
        if limit <= 0:
            return []
        claimed: list[dict[str, Any]] = []
        async with self._sf() as session:
            authority_now = await self._now(session, now)
            batches = list(
                (
                    await session.execute(
                        select(SubagentBatchRow)
                        .where(
                            SubagentBatchRow.status.in_(("queued", "running")),
                            self._tenant_executable_clause(),
                        )
                        .order_by(
                            SubagentBatchRow.created_at,
                            SubagentBatchRow.id,
                        )
                        .with_for_update(skip_locked=True)
                    )
                ).scalars()
            )
            for batch in batches:
                if len(claimed) >= limit:
                    break
                execution_batch: dict[str, Any] | None = None
                if batch.schema_writer_version == 2:
                    try:
                        execution_batch = self._execution_batch_dict(batch)
                        acceptance = execution_batch["acceptance"]
                        item_commitment_rows = list(
                            (
                                await session.execute(
                                    select(
                                        SubagentBatchItemRow.id,
                                        SubagentBatchItemRow.position,
                                        SubagentBatchItemRow.request_digest,
                                    )
                                    .where(SubagentBatchItemRow.batch_id == batch.id)
                                    .order_by(SubagentBatchItemRow.position)
                                )
                            ).all()
                        )
                        item_commitments = tuple(
                            AcceptedBatchItemV1(
                                version=1,
                                item_id=item_id,
                                ordinal=position,
                                request_digest=request_digest,
                            )
                            for item_id, position, request_digest in (item_commitment_rows)
                        )
                        if len(item_commitments) != acceptance.item_count or AcceptedBatchItemV1.root_digest(item_commitments) != acceptance.item_root_digest:
                            raise BatchAdmissionError("execution_material_unavailable")
                    except Exception:
                        await self._stop_batch_for_reason(
                            session,
                            batch=batch,
                            now=authority_now,
                            terminal_code="execution_material_unavailable",
                        )
                        continue
                    accepted_at = None if batch.accepted_at is None else _as_utc(batch.accepted_at)
                    if accepted_at is None or accepted_at + timedelta(seconds=(acceptance.limits.max_total_runtime_seconds)) <= authority_now:
                        await self._stop_batch_for_reason(
                            session,
                            batch=batch,
                            now=authority_now,
                            terminal_code="policy_stopped",
                        )
                        continue
                else:
                    execution_batch = self._execution_batch_dict(batch)

                expired = list(
                    (
                        await session.execute(
                            select(SubagentBatchItemRow)
                            .where(
                                SubagentBatchItemRow.batch_id == batch.id,
                                SubagentBatchItemRow.status.in_(("leased", "running")),
                                SubagentBatchItemRow.lease_expires_at <= authority_now,
                            )
                            .with_for_update(skip_locked=True)
                        )
                    ).scalars()
                )
                for item in expired:
                    if self._requires_attempt_fence(batch):
                        await self._terminalize_attempt_fail_closed(
                            session,
                            batch=batch,
                            item=item,
                            terminal_code="lease_expired",
                            consumed=True,
                            terminal_at=authority_now,
                        )
                    item.lease_owner = None
                    item.lease_expires_at = None
                    item.active_attempt_id = None
                    item.updated_at = authority_now
                    if item.cancel_requested_at is not None:
                        item.status = "cancelled"
                        item.terminal_code = "cancelled"
                        item.completed_at = authority_now
                    elif item.attempt >= batch.max_attempts:
                        item.status = "failed"
                        item.error = item.error or "Execution lease expired after the maximum retry count"
                        item.terminal_code = "attempt_limit_exhausted"
                        item.completed_at = authority_now
                    else:
                        item.status = "queued"
                        item.error = "Previous worker lease expired; retrying"
                if expired:
                    await self._refresh_batch_status(
                        session,
                        batch,
                        now=authority_now,
                    )
                    if batch.status in BATCH_TERMINAL_STATUSES:
                        continue

                counts = await self._counts(session, batch.id)
                live = counts["queued"] + counts["leased"] + counts["running"]
                promote_count = max(0, batch.max_live_items - live)
                if promote_count:
                    pending = list(
                        (
                            await session.execute(
                                select(SubagentBatchItemRow)
                                .where(
                                    SubagentBatchItemRow.batch_id == batch.id,
                                    SubagentBatchItemRow.status == "pending",
                                )
                                .order_by(SubagentBatchItemRow.position)
                                .limit(promote_count)
                                .with_for_update(skip_locked=True)
                            )
                        ).scalars()
                    )
                    for item in pending:
                        item.status = "queued"
                        item.updated_at = authority_now

                counts = await self._counts(session, batch.id)
                batch_available = max(0, batch.max_running_items - counts["leased"] - counts["running"])
                take = min(limit - len(claimed), batch_available)
                if take <= 0:
                    continue
                runnable = list(
                    (
                        await session.execute(
                            select(SubagentBatchItemRow)
                            .where(
                                SubagentBatchItemRow.batch_id == batch.id,
                                SubagentBatchItemRow.status == "queued",
                                SubagentBatchItemRow.cancel_requested_at.is_(None),
                            )
                            .order_by(SubagentBatchItemRow.position)
                            .limit(take)
                            .with_for_update(skip_locked=True)
                        )
                    ).scalars()
                )
                if batch.schema_writer_version == 2 and any(not self._item_matches_acceptance(item, acceptance) for item in runnable):
                    await self._stop_batch_for_reason(
                        session,
                        batch=batch,
                        now=authority_now,
                        terminal_code="execution_material_unavailable",
                    )
                    continue
                expires_at = authority_now + timedelta(seconds=lease_seconds)
                evidence_limit_exhausted = False
                claimed_for_batch = False
                for item in runnable:
                    if self._requires_attempt_fence(batch) and item.lease_epoch >= acceptance.limits.max_attempt_records_per_item:
                        item.status = "failed"
                        item.error = "evidence_limit_exhausted"
                        item.terminal_code = "evidence_limit_exhausted"
                        item.terminal_evidence_digest = None
                        item.completed_at = authority_now
                        item.updated_at = authority_now
                        evidence_limit_exhausted = True
                        continue
                    item.status = "leased"
                    item.attempt += 1
                    item.lease_epoch += 1
                    item.lease_owner = lease_owner
                    item.lease_expires_at = expires_at
                    item.started_at = authority_now if not self._requires_attempt_fence(batch) else None
                    item.updated_at = authority_now
                    item.error = None
                    item.terminal_code = None
                    item.terminal_evidence_digest = None
                    if self._requires_attempt_fence(batch):
                        attempt_id = f"ba_{uuid.uuid4().hex}"
                        item.active_attempt_id = attempt_id
                        session.add(
                            SubagentBatchAttemptRow(
                                id=attempt_id,
                                batch_id=batch.id,
                                item_id=item.id,
                                tenant_digest=batch.tenant_digest or "",
                                attempt_number=item.attempt,
                                lease_epoch=item.lease_epoch,
                                worker_ref=canonical_digest(
                                    {
                                        "version": 1,
                                        "domain": "subagent_batch_worker_ref",
                                        "tenant_digest": batch.tenant_digest,
                                        "lease_owner": lease_owner,
                                    }
                                ),
                                status="claimed",
                                consumed=True,
                                claimed_at=authority_now,
                            )
                        )
                    else:
                        attempt_id = None
                    value = self._item_dict(item)
                    value["prompt"] = item.prompt
                    assert execution_batch is not None
                    value["batch"] = execution_batch
                    value["attempt_id"] = attempt_id
                    claimed.append(value)
                    claimed_for_batch = True
                if evidence_limit_exhausted:
                    await self._refresh_batch_status(
                        session,
                        batch,
                        now=authority_now,
                    )
                if claimed_for_batch:
                    batch.status = "running"
                    batch.updated_at = authority_now
            await session.commit()
        return claimed

    async def _stop_batch_for_reason(
        self,
        session: AsyncSession,
        *,
        batch: SubagentBatchRow,
        now: datetime,
        terminal_code: str,
    ) -> None:
        items = list(
            (
                await session.execute(
                    select(SubagentBatchItemRow)
                    .where(
                        SubagentBatchItemRow.batch_id == batch.id,
                        SubagentBatchItemRow.status.not_in(ITEM_TERMINAL_STATUSES),
                    )
                    .with_for_update()
                )
            ).scalars()
        )
        for item in items:
            if item.active_attempt_id is not None:
                await self._terminalize_attempt_fail_closed(
                    session,
                    batch=batch,
                    item=item,
                    terminal_code=terminal_code,
                    consumed=True,
                    terminal_at=now,
                )
            item.status = "failed"
            item.terminal_code = terminal_code
            item.error = terminal_code
            item.lease_owner = None
            item.lease_expires_at = None
            item.active_attempt_id = None
            item.completed_at = now
            item.updated_at = now
        batch.status = "failed"
        batch.terminal_code = terminal_code
        batch.completed_at = now
        batch.updated_at = now

    async def _execution_window_open(
        self,
        session: AsyncSession,
        *,
        batch: SubagentBatchRow,
        now: datetime,
    ) -> bool:
        """Fence active work when accepted material or its deadline is stale."""

        if not self._requires_attempt_fence(batch):
            return True
        terminal_code: str | None = None
        try:
            if batch.acceptance_json is None:
                raise BatchAdmissionError("execution_material_unavailable")
            acceptance = AcceptedBatchV1.from_persisted_json(batch.acceptance_json)
            accepted_at = None if batch.accepted_at is None else _as_utc(batch.accepted_at)
            if accepted_at is None or acceptance.batch_id != batch.id or acceptance.acceptance_digest != batch.acceptance_digest or acceptance.tenant.digest != batch.tenant_digest:
                raise BatchAdmissionError("execution_material_unavailable")
        except BatchAdmissionError:
            terminal_code = "execution_material_unavailable"
        else:
            if accepted_at + timedelta(seconds=acceptance.limits.max_total_runtime_seconds) <= now:
                terminal_code = "policy_stopped"
        if terminal_code is None:
            return True
        await self._stop_batch_for_reason(
            session,
            batch=batch,
            now=now,
            terminal_code=terminal_code,
        )
        return False

    async def _terminalize_attempt(
        self,
        session: AsyncSession,
        *,
        batch: SubagentBatchRow,
        item: SubagentBatchItemRow,
        terminal_code: str,
        consumed: bool,
        terminal_at: datetime,
        result: str | None = None,
    ) -> bool:
        attempt_id = item.active_attempt_id
        if attempt_id is None or item.request_digest is None:
            return False
        attempt = await session.get(
            SubagentBatchAttemptRow,
            attempt_id,
            with_for_update=True,
        )
        if (
            not self._attempt_matches_current(
                batch=batch,
                item=item,
                attempt=attempt,
                statuses=("claimed", "started"),
            )
            or batch.acceptance_digest is None
        ):
            return False
        assert attempt is not None
        evidence = BatchAttemptEvidenceV1.terminal(
            batch_id=batch.id,
            item_id=item.id,
            attempt_id=attempt.id,
            acceptance_digest=batch.acceptance_digest,
            request_digest=item.request_digest,
            attempt_number=attempt.attempt_number,
            lease_epoch=attempt.lease_epoch,
            terminal_code=terminal_code,
            consumed=consumed,
            result_digest=(
                None
                if result is None
                else canonical_digest(
                    {
                        "version": 1,
                        "domain": "subagent_batch_result",
                        "result": result,
                    }
                )
            ),
        )
        attempt.status = "terminal"
        attempt.consumed = consumed
        attempt.terminal_code = terminal_code
        attempt.evidence_json = evidence.to_persisted_json()
        attempt.evidence_digest = evidence.evidence_digest
        attempt.terminal_at = terminal_at
        item.terminal_evidence_digest = evidence.evidence_digest
        return True

    @staticmethod
    def _attempt_matches_current(
        *,
        batch: SubagentBatchRow,
        item: SubagentBatchItemRow,
        attempt: SubagentBatchAttemptRow | None,
        statuses: tuple[str, ...],
    ) -> bool:
        if attempt is None or item.lease_owner is None:
            return False
        expected_worker_ref = canonical_digest(
            {
                "version": 1,
                "domain": "subagent_batch_worker_ref",
                "tenant_digest": batch.tenant_digest,
                "lease_owner": item.lease_owner,
            }
        )
        return bool(
            attempt.id == item.active_attempt_id
            and attempt.item_id == item.id
            and attempt.batch_id == batch.id
            and attempt.tenant_digest == batch.tenant_digest
            and attempt.attempt_number == item.attempt
            and attempt.lease_epoch == item.lease_epoch
            and attempt.worker_ref == expected_worker_ref
            and attempt.status in statuses
            and attempt.terminal_at is None
        )

    async def _terminalize_attempt_fail_closed(
        self,
        session: AsyncSession,
        *,
        batch: SubagentBatchRow,
        item: SubagentBatchItemRow,
        terminal_code: str,
        consumed: bool,
        terminal_at: datetime,
    ) -> bool:
        """Fence a corrupt active attempt without fabricating evidence."""

        try:
            accepted = await self._terminalize_attempt(
                session,
                batch=batch,
                item=item,
                terminal_code=terminal_code,
                consumed=consumed,
                terminal_at=terminal_at,
            )
        except BatchAdmissionError:
            accepted = False
        if accepted:
            return True
        attempt_id = item.active_attempt_id
        if attempt_id is None:
            return False
        attempt = await session.get(
            SubagentBatchAttemptRow,
            attempt_id,
            with_for_update=True,
        )
        if attempt is None or attempt.item_id != item.id or attempt.batch_id != batch.id or attempt.lease_epoch != item.lease_epoch or attempt.terminal_at is not None:
            return False
        attempt.status = "terminal"
        attempt.consumed = consumed
        attempt.terminal_code = terminal_code
        attempt.evidence_json = None
        attempt.evidence_digest = None
        attempt.terminal_at = terminal_at
        item.terminal_evidence_digest = None
        return True

    async def renew_item_lease(
        self,
        item_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
        now: datetime | None,
        attempt_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> dict[str, bool]:
        async with self._sf() as session:
            loaded = await self._locked_fenced_item(
                session,
                item_id=item_id,
                lease_owner=lease_owner,
                attempt_id=attempt_id,
                lease_epoch=lease_epoch,
                statuses=("leased", "running"),
            )
            if loaded is None:
                return {"valid": False, "cancel_requested": True}
            item, batch = loaded
            authority_now = await self._now(session, now)
            if not await self._execution_window_open(
                session,
                batch=batch,
                now=authority_now,
            ):
                await session.commit()
                return {"valid": False, "cancel_requested": True}
            expired = item.lease_expires_at is None or _as_utc(item.lease_expires_at) <= authority_now
            cancel_requested = item.cancel_requested_at is not None or batch.status == "cancelled" or expired
            if not cancel_requested:
                item.lease_expires_at = authority_now + timedelta(seconds=lease_seconds)
                item.updated_at = authority_now
                await session.commit()
            return {"valid": not cancel_requested, "cancel_requested": cancel_requested}

    async def item_attempt_authorized(
        self,
        item_id: str,
        *,
        lease_owner: str,
        attempt_id: str,
        lease_epoch: int,
    ) -> bool:
        """Sample whether one accepted item attempt still owns execution.

        The database locks exist only for this authority sample. Callers must
        release this method before invoking a sandbox provider; this is the
        baseline check-then-call fence, not an atomic provider-operation fence.
        """

        async with self._sf() as session:
            loaded = await self._locked_fenced_item(
                session,
                item_id=item_id,
                lease_owner=lease_owner,
                attempt_id=attempt_id,
                lease_epoch=lease_epoch,
                statuses=("leased", "running"),
            )
            if loaded is None:
                return False
            item, batch = loaded
            authority_now = await self._now(session, None)
            if not await self._execution_window_open(
                session,
                batch=batch,
                now=authority_now,
            ):
                await session.commit()
                return False
            expected_attempt_status = ("claimed",) if item.status == "leased" else ("started",)
            attempt = await session.get(
                SubagentBatchAttemptRow,
                attempt_id,
                with_for_update=True,
            )
            return bool(
                batch.status in BATCH_ACTIVE_STATUSES
                and item.cancel_requested_at is None
                and item.lease_expires_at is not None
                and _as_utc(item.lease_expires_at) > authority_now
                and self._attempt_matches_current(
                    batch=batch,
                    item=item,
                    attempt=attempt,
                    statuses=expected_attempt_status,
                )
            )

    async def _locked_fenced_item(
        self,
        session: AsyncSession,
        *,
        item_id: str,
        lease_owner: str,
        attempt_id: str | None,
        lease_epoch: int | None,
        statuses: tuple[str, ...],
    ) -> tuple[SubagentBatchItemRow, SubagentBatchRow] | None:
        # Resolve the immutable parent key without taking a row lock, then
        # lock parent before child. Control paths (notably cancellation) use
        # the same order, preventing a PostgreSQL item->batch / batch->item
        # deadlock while still fencing the mutable item under FOR UPDATE.
        batch_id = (await session.execute(select(SubagentBatchItemRow.batch_id).where(SubagentBatchItemRow.id == item_id))).scalar_one_or_none()
        if batch_id is None:
            return None
        batch = await session.get(
            SubagentBatchRow,
            batch_id,
            with_for_update=True,
        )
        if not self._can_execute(batch):
            return None
        assert batch is not None
        item = (
            await session.execute(
                select(SubagentBatchItemRow)
                .where(
                    SubagentBatchItemRow.id == item_id,
                    SubagentBatchItemRow.status.in_(statuses),
                    SubagentBatchItemRow.lease_owner == lease_owner,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if item is None:
            return None
        if item.batch_id != batch.id:
            return None
        if self._requires_attempt_fence(batch) and (attempt_id is None or lease_epoch is None or item.active_attempt_id != attempt_id or item.lease_epoch != lease_epoch):
            return None
        return item, batch

    async def mark_item_running(
        self,
        item_id: str,
        *,
        lease_owner: str,
        now: datetime | None,
        attempt_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> bool:
        async with self._sf() as session:
            loaded = await self._locked_fenced_item(
                session,
                item_id=item_id,
                lease_owner=lease_owner,
                attempt_id=attempt_id,
                lease_epoch=lease_epoch,
                statuses=("leased",),
            )
            if loaded is None:
                return False
            item, batch = loaded
            authority_now = await self._now(session, now)
            if not await self._execution_window_open(
                session,
                batch=batch,
                now=authority_now,
            ):
                await session.commit()
                return False
            if item.cancel_requested_at is not None or batch.status == "cancelled" or item.lease_expires_at is None or _as_utc(item.lease_expires_at) <= authority_now:
                return False
            item.status = "running"
            item.started_at = authority_now
            item.updated_at = authority_now
            if self._requires_attempt_fence(batch):
                attempt = await session.get(
                    SubagentBatchAttemptRow,
                    item.active_attempt_id,
                    with_for_update=True,
                )
                if not self._attempt_matches_current(
                    batch=batch,
                    item=item,
                    attempt=attempt,
                    statuses=("claimed",),
                ):
                    return False
                assert attempt is not None
                attempt.status = "started"
                attempt.started_at = authority_now
            await session.commit()
            return True

    async def finalize_item(
        self,
        item_id: str,
        *,
        lease_owner: str,
        attempt_id: str | None = None,
        lease_epoch: int | None = None,
        succeeded: bool,
        result: str | None,
        result_preview: str | None,
        result_truncated: bool,
        error: str | None,
        stop_reason: str | None,
        token_usage: dict[str, Any] | None,
        model_name: str | None,
        completed_at: datetime | None,
        terminal_code: str | None = None,
    ) -> bool:
        async with self._sf() as session:
            loaded = await self._locked_fenced_item(
                session,
                item_id=item_id,
                lease_owner=lease_owner,
                attempt_id=attempt_id,
                lease_epoch=lease_epoch,
                statuses=("leased", "running"),
            )
            if loaded is None:
                return False
            item, batch = loaded
            authority_now = await self._now(session, completed_at)
            if not await self._execution_window_open(
                session,
                batch=batch,
                now=authority_now,
            ):
                await session.commit()
                return False
            if item.lease_expires_at is None or _as_utc(item.lease_expires_at) <= authority_now:
                return False
            cancelled = item.cancel_requested_at is not None or batch.status == "cancelled"
            code = terminal_code or ("cancelled" if cancelled else "succeeded" if succeeded else "execution_failed")
            if self._requires_attempt_fence(batch):
                accepted = await self._terminalize_attempt(
                    session,
                    batch=batch,
                    item=item,
                    terminal_code=code,
                    consumed=True,
                    terminal_at=authority_now,
                    result=result if succeeded else None,
                )
                if not accepted:
                    return False
            item.lease_owner = None
            item.lease_expires_at = None
            item.active_attempt_id = None
            item.model_name = model_name
            item.stop_reason = stop_reason
            item.token_usage = token_usage
            item.updated_at = authority_now
            if cancelled:
                item.status = "cancelled"
                item.error = "Cancelled by user"
                item.terminal_code = "cancelled"
                item.completed_at = authority_now
            elif succeeded:
                item.status = "succeeded"
                item.result = result
                item.result_preview = result_preview
                item.result_truncated = result_truncated
                item.error = None
                item.terminal_code = code
                item.completed_at = authority_now
            elif (
                code
                not in {
                    "policy_stopped",
                    "provider_not_qualified",
                    "result_too_large",
                    "execution_material_unavailable",
                }
                and item.attempt < batch.max_attempts
            ):
                item.status = "queued"
                item.error = error
                item.started_at = None
            else:
                item.status = "failed"
                item.error = error
                item.terminal_code = code if code != "execution_failed" else "attempt_limit_exhausted"
                item.completed_at = authority_now
            await self._refresh_batch_status(
                session,
                batch,
                now=authority_now,
            )
            await session.commit()
            return True

    async def requeue_item_after_admission_failure(
        self,
        item_id: str,
        *,
        lease_owner: str,
        error: str | None,
        now: datetime | None,
        attempt_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> bool:
        """Undo a claim rejected before execution admission.

        Claiming increments ``attempt`` so crash recovery can bound real
        executions. A process-wide capacity rejection happens before an
        execution starts, so it must release the lease and restore that
        attempt instead of consuming the batch's retry budget.
        """
        async with self._sf() as session:
            loaded = await self._locked_fenced_item(
                session,
                item_id=item_id,
                lease_owner=lease_owner,
                attempt_id=attempt_id,
                lease_epoch=lease_epoch,
                statuses=("leased",),
            )
            if loaded is None:
                return False
            item, batch = loaded
            authority_now = await self._now(session, now)
            if not await self._execution_window_open(
                session,
                batch=batch,
                now=authority_now,
            ):
                await session.commit()
                return False
            if item.lease_expires_at is None or _as_utc(item.lease_expires_at) <= authority_now:
                return False
            cancelled = item.cancel_requested_at is not None or batch.status == "cancelled"
            if self._requires_attempt_fence(batch):
                attempt = await session.get(
                    SubagentBatchAttemptRow,
                    item.active_attempt_id,
                    with_for_update=True,
                )
                if not self._attempt_matches_current(
                    batch=batch,
                    item=item,
                    attempt=attempt,
                    statuses=("claimed",),
                ):
                    return False
                accepted = await self._terminalize_attempt(
                    session,
                    batch=batch,
                    item=item,
                    terminal_code=("cancelled" if cancelled else "queue_rejected"),
                    consumed=False,
                    terminal_at=authority_now,
                )
                if not accepted:
                    return False
            item.lease_owner = None
            item.lease_expires_at = None
            item.active_attempt_id = None
            item.updated_at = authority_now
            if cancelled:
                item.status = "cancelled"
                item.error = "Cancelled by user"
                item.terminal_code = "cancelled"
                item.completed_at = authority_now
            else:
                item.status = "queued"
                item.attempt = max(0, item.attempt - 1)
                item.started_at = None
                item.error = error
            await self._refresh_batch_status(
                session,
                batch,
                now=authority_now,
            )
            await session.commit()
            return True

    async def _refresh_batch_status(self, session: AsyncSession, batch: SubagentBatchRow, *, now: datetime) -> None:
        counts = await self._counts(session, batch.id)
        terminal = sum(counts[state] for state in ITEM_TERMINAL_STATUSES)
        if terminal >= batch.total_items:
            if batch.status != "cancelled":
                batch.status = "failed" if counts["failed"] > 0 and counts["succeeded"] == 0 else "completed"
                if batch.status == "failed":
                    terminal_codes = {
                        code
                        for code in (
                            await session.execute(
                                select(SubagentBatchItemRow.terminal_code).where(
                                    SubagentBatchItemRow.batch_id == batch.id,
                                    SubagentBatchItemRow.status == "failed",
                                )
                            )
                        ).scalars()
                        if code is not None
                    }
                    batch.terminal_code = next(iter(terminal_codes)) if len(terminal_codes) == 1 else "failed"
                else:
                    batch.terminal_code = "completed_with_failures" if counts["failed"] > 0 else "succeeded"
            batch.completed_at = now
        elif batch.status not in ("paused", "cancelled"):
            batch.status = "running"
        batch.updated_at = now

    async def pause_batch(self, batch_id: str, *, user_id: str) -> dict[str, Any] | None:
        return await self._set_control(batch_id, user_id=user_id, action="pause")

    async def resume_batch(self, batch_id: str, *, user_id: str) -> dict[str, Any] | None:
        return await self._set_control(batch_id, user_id=user_id, action="resume")

    async def cancel_batch(self, batch_id: str, *, user_id: str) -> dict[str, Any] | None:
        return await self._set_control(batch_id, user_id=user_id, action="cancel")

    async def _set_control(self, batch_id: str, *, user_id: str, action: str) -> dict[str, Any] | None:
        async with self._sf() as session:
            batch = await session.get(SubagentBatchRow, batch_id, with_for_update=True)
            if batch is None or batch.user_id != user_id or not self._can_execute(batch):
                return None
            now = await self._now(session, None)
            if action == "pause" and batch.status in ("queued", "running"):
                batch.status = "paused"
            elif action == "resume" and batch.status == "paused":
                batch.status = "queued"
            elif action == "cancel" and batch.status not in BATCH_TERMINAL_STATUSES:
                batch.status = "cancelled"
                batch.cancel_epoch += 1
                batch.terminal_code = "cancelled"
                batch.completed_at = now
                items = list(
                    (
                        await session.execute(
                            select(SubagentBatchItemRow)
                            .where(
                                SubagentBatchItemRow.batch_id == batch_id,
                                SubagentBatchItemRow.status.not_in(ITEM_TERMINAL_STATUSES),
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                for item in items:
                    if self._requires_attempt_fence(batch) and item.active_attempt_id is not None:
                        await self._terminalize_attempt_fail_closed(
                            session,
                            batch=batch,
                            item=item,
                            terminal_code="cancelled",
                            consumed=True,
                            terminal_at=now,
                        )
                    item.cancel_requested_at = now
                    item.updated_at = now
                    item.status = "cancelled"
                    item.error = "Cancelled by user"
                    item.lease_owner = None
                    item.lease_expires_at = None
                    item.active_attempt_id = None
                    item.terminal_code = "cancelled"
                    item.completed_at = now
            batch.updated_at = now
            await session.commit()
            return await self._with_counts(session, batch)

    async def retry_item(self, batch_id: str, item_id: str, *, user_id: str) -> dict[str, Any] | None:
        async with self._sf() as session:
            batch = await session.get(SubagentBatchRow, batch_id, with_for_update=True)
            if batch is None or batch.user_id != user_id or not self._can_execute(batch):
                return None
            item = await session.get(SubagentBatchItemRow, item_id, with_for_update=True)
            if item is None or item.batch_id != batch_id or item.status != "failed":
                return None
            if self._requires_attempt_fence(batch) and item.attempt >= batch.max_attempts:
                return None
            now = await self._now(session, None)
            item.status = "pending"
            if not self._requires_attempt_fence(batch):
                item.attempt = 0
            item.started_at = None
            item.error = None
            item.result = None
            item.result_preview = None
            item.result_truncated = False
            item.terminal_code = None
            item.terminal_evidence_digest = None
            item.completed_at = None
            item.cancel_requested_at = None
            item.updated_at = now
            batch.status = "queued"
            batch.terminal_code = None
            batch.completed_at = None
            batch.updated_at = now
            await session.commit()
            return self._item_dict(item)

    async def list_attempts(
        self,
        batch_id: str,
        *,
        user_id: str,
        item_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]] | None:
        """Return bounded safe attempt observations for an authorized owner."""

        bounded_limit = max(1, min(limit, 100))
        async with self._sf() as session:
            batch = (
                await session.execute(
                    select(SubagentBatchRow).where(
                        SubagentBatchRow.id == batch_id,
                        SubagentBatchRow.user_id == user_id,
                        self._tenant_visible_clause(),
                    )
                )
            ).scalar_one_or_none()
            if batch is None:
                return None
            if batch.schema_writer_version == 1:
                return []
            stmt = select(SubagentBatchAttemptRow).where(
                SubagentBatchAttemptRow.batch_id == batch_id,
                SubagentBatchAttemptRow.tenant_digest == batch.tenant_digest,
            )
            if item_id is not None:
                stmt = stmt.where(SubagentBatchAttemptRow.item_id == item_id)
            rows = list(
                (
                    await session.execute(
                        stmt.order_by(
                            SubagentBatchAttemptRow.claimed_at,
                            SubagentBatchAttemptRow.lease_epoch,
                        ).limit(bounded_limit)
                    )
                ).scalars()
            )
            return [self._attempt_dict(row) for row in rows]

    async def list_observations(
        self,
        batch_id: str,
        *,
        user_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]] | None:
        """Derive a bounded payload-free lifecycle view from durable rows.

        Acceptance, attempt timestamps, and terminal projection remain owned by
        their existing tables. This additive view gives portable consumers one
        stable vocabulary without creating a second lifecycle authority.
        """

        bounded_limit = max(1, min(limit, 100))
        async with self._sf() as session:
            batch = (
                await session.execute(
                    select(SubagentBatchRow).where(
                        SubagentBatchRow.id == batch_id,
                        SubagentBatchRow.user_id == user_id,
                        self._tenant_visible_clause(),
                    )
                )
            ).scalar_one_or_none()
            if batch is None:
                return None
            if batch.schema_writer_version != 2 or batch.accepted_at is None or batch.acceptance_digest is None:
                return []

            attempt_rows = list(
                (
                    await session.execute(
                        select(SubagentBatchAttemptRow)
                        .where(
                            SubagentBatchAttemptRow.batch_id == batch_id,
                            SubagentBatchAttemptRow.tenant_digest == batch.tenant_digest,
                        )
                        .order_by(
                            SubagentBatchAttemptRow.claimed_at.desc(),
                            SubagentBatchAttemptRow.id.desc(),
                        )
                        .limit(bounded_limit)
                    )
                ).scalars()
            )
            accepted_observation = {
                "version": 1,
                "event": "batch.accepted",
                "batch_id": batch.id,
                "acceptance_digest": batch.acceptance_digest,
                "parent_run_id": batch.run_id,
                "parent_tool_receipt_id": batch.parent_tool_receipt_id,
                "item_count": batch.total_items,
                "occurred_at": coerce_iso(batch.accepted_at),
            }
            terminal_observation = (
                {
                    "version": 1,
                    "event": "batch.terminal",
                    "batch_id": batch.id,
                    "acceptance_digest": batch.acceptance_digest,
                    "terminal_code": batch.terminal_code,
                    "occurred_at": coerce_iso(batch.completed_at),
                }
                if batch.completed_at is not None
                else None
            )
            attempt_observations: list[dict[str, Any]] = []
            for attempt in reversed(attempt_rows):
                common = {
                    "version": 1,
                    "event": "batch.item_attempt",
                    "batch_id": batch.id,
                    "item_id": attempt.item_id,
                    "attempt_id": attempt.id,
                    "attempt_number": attempt.attempt_number,
                    "lease_epoch": attempt.lease_epoch,
                }
                attempt_observations.append(
                    {
                        **common,
                        "transition": "claimed",
                        "occurred_at": coerce_iso(attempt.claimed_at),
                    }
                )
                if attempt.started_at is not None:
                    attempt_observations.append(
                        {
                            **common,
                            "transition": "started",
                            "occurred_at": coerce_iso(attempt.started_at),
                        }
                    )
                if attempt.terminal_at is not None:
                    attempt_observations.append(
                        {
                            **common,
                            "transition": "terminal",
                            "terminal_code": attempt.terminal_code,
                            "consumed": attempt.consumed,
                            "evidence_digest": attempt.evidence_digest,
                            "occurred_at": coerce_iso(attempt.terminal_at),
                        }
                    )
            reserved_edges = 1 + int(terminal_observation is not None)
            transition_budget = max(0, bounded_limit - reserved_edges)
            observations = [accepted_observation]
            if transition_budget:
                observations.extend(attempt_observations[-transition_budget:])
            if terminal_observation is not None and len(observations) < bounded_limit:
                observations.append(terminal_observation)
            return observations

    def _parent_cursor_scope(
        self,
        *,
        parent_run_id: str,
        user_id: str,
    ) -> str:
        if self._tenant is None:
            raise BatchAdmissionError("legacy_batch_unbound")
        return canonical_digest(
            {
                "version": 1,
                "domain": "subagent_batch_parent_cursor_scope",
                "tenant_digest": self._tenant.digest,
                "parent_run_id": parent_run_id,
                "user_id": user_id,
            }
        )

    def _encode_parent_cursor(
        self,
        *,
        parent_run_id: str,
        user_id: str,
        accepted_at: datetime,
        batch_id: str,
    ) -> str:
        core = {
            "version": _BATCH_PARENT_CURSOR_VERSION,
            "scope": self._parent_cursor_scope(
                parent_run_id=parent_run_id,
                user_id=user_id,
            ),
            "accepted_at": _as_utc(accepted_at).isoformat(),
            "batch_id": batch_id,
        }
        payload = {**core, "checksum": canonical_digest(core)}
        encoded = base64.urlsafe_b64encode(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).rstrip(b"=")
        return "sbc1." + encoded.decode("ascii")

    def _decode_parent_cursor(
        self,
        cursor: str,
        *,
        parent_run_id: str,
        user_id: str,
    ) -> tuple[datetime, str]:
        if not isinstance(cursor, str) or len(cursor.encode("utf-8")) > 4096 or not cursor.startswith("sbc1."):
            raise BatchAdmissionError("subagent_batch_cursor_invalid")
        try:
            raw = cursor[5:]
            payload = json.loads(
                base64.b64decode(
                    raw + "=" * (-len(raw) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            )
            expected = {
                "version",
                "scope",
                "accepted_at",
                "batch_id",
                "checksum",
            }
            if not isinstance(payload, dict) or set(payload) != expected:
                raise ValueError
            core = {key: payload[key] for key in ("version", "scope", "accepted_at", "batch_id")}
            if (
                payload["version"] != _BATCH_PARENT_CURSOR_VERSION
                or payload["scope"]
                != self._parent_cursor_scope(
                    parent_run_id=parent_run_id,
                    user_id=user_id,
                )
                or payload["checksum"] != canonical_digest(core)
                or not isinstance(payload["batch_id"], str)
                or not payload["batch_id"]
            ):
                raise ValueError
            accepted_at = datetime.fromisoformat(payload["accepted_at"])
            if accepted_at.tzinfo is None:
                raise ValueError
            return accepted_at.astimezone(UTC), payload["batch_id"]
        except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise BatchAdmissionError("subagent_batch_cursor_invalid") from exc

    async def list_lifecycle_by_parent_run(
        self,
        parent_run_id: str,
        *,
        user_id: str,
        limit: int = 20,
        cursor: str | None = None,
        tenant_digest: str,
    ) -> dict[str, Any]:
        """Return owner-scoped child lifecycle through the portable run seam."""

        if self._tenant is None:
            raise BatchAdmissionError("legacy_batch_unbound")
        if tenant_digest != self._tenant.digest:
            raise BatchAdmissionError("batch_tenant_mismatch")
        if type(limit) is not int or not 1 <= limit <= _MAX_BATCH_PARENT_PAGE_SIZE:
            raise ValueError("subagent batch lineage limit must be between 1 and 100")
        stmt = select(SubagentBatchRow).where(
            SubagentBatchRow.tenant_digest == tenant_digest,
            SubagentBatchRow.run_id == parent_run_id,
            SubagentBatchRow.user_id == user_id,
            SubagentBatchRow.schema_writer_version == 2,
            SubagentBatchRow.acceptance_digest.is_not(None),
            SubagentBatchRow.accepted_at.is_not(None),
        )
        if cursor is not None:
            accepted_at, batch_id = self._decode_parent_cursor(
                cursor,
                parent_run_id=parent_run_id,
                user_id=user_id,
            )
            stmt = stmt.where((SubagentBatchRow.accepted_at < accepted_at) | ((SubagentBatchRow.accepted_at == accepted_at) & (SubagentBatchRow.id < batch_id)))
        stmt = stmt.order_by(
            SubagentBatchRow.accepted_at.desc(),
            SubagentBatchRow.id.desc(),
        ).limit(limit + 1)
        async with self._sf() as session:
            rows = list((await session.execute(stmt)).scalars())
        has_more = len(rows) > limit
        rows = rows[:limit]
        items: list[dict[str, Any]] = []
        for row in rows:
            observations = await self.list_observations(
                row.id,
                user_id=user_id,
                limit=100,
            )
            if observations is None:
                continue
            items.append(
                {
                    "batch_id": row.id,
                    "acceptance_digest": row.acceptance_digest,
                    "parent_tool_receipt_id": row.parent_tool_receipt_id,
                    "status": row.status,
                    "terminal_code": row.terminal_code,
                    "total_items": row.total_items,
                    "accepted_at": coerce_iso(row.accepted_at),
                    "updated_at": coerce_iso(row.updated_at),
                    "completed_at": (None if row.completed_at is None else coerce_iso(row.completed_at)),
                    "observations": observations,
                }
            )
        next_cursor = None
        if has_more and rows:
            tail = rows[-1]
            assert tail.accepted_at is not None
            next_cursor = self._encode_parent_cursor(
                parent_run_id=parent_run_id,
                user_id=user_id,
                accepted_at=tail.accepted_at,
                batch_id=tail.id,
            )
        return {
            "items": items,
            "next_cursor": next_cursor,
            "pruning_status": "not_pruned",
        }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)

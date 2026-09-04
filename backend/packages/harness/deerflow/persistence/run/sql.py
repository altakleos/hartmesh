"""SQLAlchemy-backed RunStore implementation.

Each method acquires and releases its own short-lived session.
Run status updates happen from background workers that may live
minutes -- we don't hold connections across long execution.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from deerflow_extension_api import (
    TenantReferenceV1,
    validate_model_profile_identifier,
    validate_thread_identifier,
)
from sqlalchemy import delete, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.run.model import (
    RunAdmissionCursorStateRow,
    RunLifecycleCursorStateRow,
    RunLifecycleEventRow,
    RunRow,
)
from deerflow.persistence.sql_clock import (
    coerce_database_wall_clock,
    database_wall_clock_expression,
)
from deerflow.runtime.assembly_evidence import (
    AssemblyEvidenceError,
    AssemblyEvidenceV1,
    assembly_evidence_binding_matches,
    assembly_evidence_digest,
)
from deerflow.runtime.execution_policy import ExecutionPolicyStateV1
from deerflow.runtime.runs.lifecycle_query import (
    CursorAhead,
    LifecycleOrderingCorruption,
    LifecyclePage,
    LifecycleQuery,
    LifecycleVisibilityScope,
    build_invocation_summary,
    decode_lifecycle_cursor,
    encode_lifecycle_cursor,
    validate_cursor_window,
)
from deerflow.runtime.runs.store.base import (
    AdmissionOutcome,
    ApplyExecutionPolicyStateOutcome,
    BindAssemblyEvidenceOutcome,
    CancellationRequestOutcome,
    CancellationRequestResult,
    DuplicateRunIdentityError,
    ExecutionTakeoverClaim,
    ExecutionTakeoverOutcome,
    LeaseClockAuthority,
    LeaseRenewal,
    LifecycleReadiness,
    LifecycleTransition,
    LifecycleTransitionResult,
    LifecycleType,
    RecoveryPayloadIntegrityError,
    RecoveryPolicy,
    RunEnsureResult,
    RunIdempotencyConflict,
    RunStore,
    StatusFinalization,
    ThreadOperationReleaseOutcome,
    ThreadOperationReleaseResult,
    build_lifecycle_payload,
    lifecycle_owner_scope,
    lifecycle_type_for_status,
    tenant_store_columns,
    validate_execution_evidence_run,
    validate_lease_deadline_request,
)
from deerflow.runtime.tenant_identity import TenantIdentityError
from deerflow.runtime.user_context import AUTO, _AutoSentinel, resolve_user_id
from deerflow.utils.time import coerce_iso

_DATETIME_TYPE = datetime


def _lease_expired_or_null(lease_col, cutoff: datetime):
    """SQLAlchemy filter: True when the lease is NULL or has expired past *cutoff*."""
    return or_(lease_col.is_(None), lease_col <= cutoff)


async def _database_now_after_lock(session: AsyncSession) -> datetime:
    """Sample statement-time database wall clock after relevant locks."""

    observed = await session.scalar(select(database_wall_clock_expression(session.get_bind().dialect.name)))
    if observed is None:
        raise RuntimeError("database clock is unavailable")
    return coerce_database_wall_clock(observed)


def _lease_expired_at(
    lease_expires_at: datetime | None,
    *,
    observed_at: datetime,
    grace_seconds: int,
) -> bool:
    if lease_expires_at is None:
        return True
    deadline = lease_expires_at
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=UTC)
    cutoff = observed_at - timedelta(seconds=grace_seconds)
    return deadline <= cutoff


def _database_lease_deadline(
    *,
    lease_expires_at: str | None,
    lease_duration_seconds: int | None,
    observed_at: datetime,
    required: bool,
) -> datetime | None:
    validate_lease_deadline_request(
        lease_expires_at=lease_expires_at,
        lease_duration_seconds=lease_duration_seconds,
        required=required,
    )
    if lease_duration_seconds is not None:
        return observed_at + timedelta(seconds=lease_duration_seconds)
    return datetime.fromisoformat(lease_expires_at) if lease_expires_at else None


def _is_run_primary_key_violation(exc: IntegrityError) -> bool:
    """Return whether *exc* is exactly a duplicate ``runs.run_id``."""

    original = exc.orig
    pending: list[BaseException] = [original] if isinstance(original, BaseException) else []
    seen: set[int] = set()
    # SQLAlchemy's asyncpg adapter retains the native UniqueViolation as its
    # cause, while psycopg exposes ``diag`` directly. Traverse only this small,
    # bounded exception chain and compare the exact schema constraint name;
    # neither SQLSTATE alone nor provider text identifies the violated key.
    for _depth in range(4):
        if not pending:
            break
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        diagnostic = getattr(current, "diag", None)
        constraint_name = getattr(
            diagnostic,
            "constraint_name",
            None,
        ) or getattr(current, "constraint_name", None)
        if constraint_name == "runs_pkey":
            return True
        for linked in (
            getattr(current, "__cause__", None),
            getattr(current, "__context__", None),
        ):
            if isinstance(linked, BaseException) and id(linked) not in seen:
                pending.append(linked)
    return isinstance(original, sqlite3.IntegrityError) and "unique constraint failed: runs.run_id" in str(original).lower()


class RunRepository(RunStore):
    durable_lifecycle = True
    lease_clock_authority = LeaseClockAuthority.database_v1

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tenant: TenantReferenceV1 | None = None,
    ) -> None:
        if tenant is not None and not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")
        self._sf = session_factory
        self._tenant = tenant

    def _run_tenant_clause(self) -> Any:
        if self._tenant is None:
            return None
        return RunRow.tenant_digest == self._tenant.digest

    def _event_tenant_clause(self) -> Any:
        if self._tenant is None:
            return None
        return RunLifecycleEventRow.tenant_digest == self._tenant.digest

    def _scope_run(self, statement: Any) -> Any:
        clause = self._run_tenant_clause()
        return statement.where(clause) if clause is not None else statement

    def scope_run_statement(self, statement: Any) -> Any:
        """Apply this repository's tenant boundary to a related SQL query.

        Scheduler persistence occasionally needs to inspect the underlying
        ``runs`` row in the same transaction as its own bookkeeping. Keeping
        that predicate here prevents those repositories from re-deriving the
        process tenant or reaching into private state.
        """

        return self._scope_run(statement)

    def _scope_event(self, statement: Any) -> Any:
        clause = self._event_tenant_clause()
        return statement.where(clause) if clause is not None else statement

    async def _begin_lifecycle_write(self, session: AsyncSession) -> None:
        bind = session.get_bind()
        if bind.dialect.name == "sqlite":
            # SQLite has no row-level FOR UPDATE. Acquiring the write lock
            # before reading the singleton provides the equivalent ordering.
            await session.execute(text("BEGIN IMMEDIATE"))
        else:
            await session.begin()

    async def _lock_cursor_state(self, session: AsyncSession) -> RunLifecycleCursorStateRow:
        stmt = select(RunLifecycleCursorStateRow).where(RunLifecycleCursorStateRow.singleton_id == 1)
        if session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        state = (await session.execute(stmt)).scalar_one_or_none()
        if state is not None:
            return state
        event_count = await session.scalar(self._scope_event(select(func.count()).select_from(RunLifecycleEventRow)))
        if event_count:
            raise RuntimeError("lifecycle events exist without cursor singleton; ordering state is corrupt")
        state = RunLifecycleCursorStateRow(
            singleton_id=1,
            last_cursor=0,
            pruned_through=0,
            retained_count=0,
        )
        session.add(state)
        await session.flush()
        return state

    async def _next_admission_cursor(self, session: AsyncSession) -> int:
        """Allocate one DB-ordered admission cursor in the caller transaction."""

        # Use the existing lifecycle singleton as the first lock in every
        # admission transaction. It makes lazy repair of the admission
        # singleton race-free and keeps lock ordering identical to lifecycle
        # event writes.
        await self._lock_cursor_state(session)
        stmt = select(RunAdmissionCursorStateRow).where(RunAdmissionCursorStateRow.singleton_id == 1)
        if session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        state = (await session.execute(stmt)).scalar_one_or_none()
        if state is None:
            maximum = await session.scalar(select(func.max(RunRow.admission_cursor)))
            state = RunAdmissionCursorStateRow(
                singleton_id=1,
                last_cursor=int(maximum or 0),
            )
            session.add(state)
            await session.flush()
        state.last_cursor += 1
        await session.flush()
        return state.last_cursor

    async def initialize_lifecycle(self) -> None:
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            await self._lock_cursor_state(session)
            # Validate/repair only the empty singleton authority. Existing run
            # cursors are populated by migration 0029 and are never rewritten
            # during ordinary startup.
            stmt = select(RunAdmissionCursorStateRow).where(RunAdmissionCursorStateRow.singleton_id == 1)
            if session.get_bind().dialect.name == "postgresql":
                stmt = stmt.with_for_update()
            state = (await session.execute(stmt)).scalar_one_or_none()
            if state is None:
                maximum = await session.scalar(select(func.max(RunRow.admission_cursor)))
                session.add(
                    RunAdmissionCursorStateRow(
                        singleton_id=1,
                        last_cursor=int(maximum or 0),
                    )
                )
            await session.commit()

    async def lifecycle_ready(self) -> bool:
        """Check ordering metadata without repairing or mutating it."""

        return (await self.lifecycle_readiness()).ready

    async def lifecycle_readiness(self) -> LifecycleReadiness:
        """Validate cursor metadata and retained event edges with bounded reads."""

        async with self._sf() as session:
            await self._begin_lifecycle_read(session)
            states = tuple((await session.execute(select(RunLifecycleCursorStateRow).order_by(RunLifecycleCursorStateRow.singleton_id).limit(2))).scalars().all())
            if len(states) != 1 or states[0].singleton_id != 1:
                return LifecycleReadiness(
                    ready=False,
                    reason_code="lifecycle_cursor_missing",
                )
            state = states[0]
            if state.last_cursor < 0 or state.pruned_through < 0 or state.pruned_through > state.last_cursor:
                return LifecycleReadiness(
                    ready=False,
                    reason_code="lifecycle_pruning_invalid",
                )
            expected_retained_count = state.last_cursor - state.pruned_through

            maximum_admission_cursor = select(func.coalesce(func.max(RunRow.admission_cursor), 0)).scalar_subquery()
            admission_states = tuple(
                (
                    await session.execute(
                        select(
                            RunAdmissionCursorStateRow.singleton_id,
                            RunAdmissionCursorStateRow.last_cursor,
                            maximum_admission_cursor.label("maximum_admission_cursor"),
                        )
                        .order_by(RunAdmissionCursorStateRow.singleton_id)
                        .limit(2)
                    )
                ).all()
            )
            if len(admission_states) != 1 or admission_states[0].singleton_id != 1 or admission_states[0].last_cursor < admission_states[0].maximum_admission_cursor:
                return LifecycleReadiness(
                    ready=False,
                    reason_code="admission_cursor_state_invalid",
                )

            first = tuple((await session.execute(self._scope_event(select(RunLifecycleEventRow.cursor)).order_by(RunLifecycleEventRow.cursor).limit(2))).scalars().all())
            last = tuple((await session.execute(self._scope_event(select(RunLifecycleEventRow.cursor)).order_by(RunLifecycleEventRow.cursor.desc()).limit(2))).scalars().all())
            if not first:
                if state.pruned_through != state.last_cursor:
                    return LifecycleReadiness(
                        ready=False,
                        reason_code="lifecycle_event_bounds_invalid",
                    )
                if state.retained_count < 0 or state.retained_count != expected_retained_count:
                    return LifecycleReadiness(
                        ready=False,
                        reason_code="lifecycle_event_cardinality_invalid",
                    )
                return LifecycleReadiness(ready=True)
            if first[0] != state.pruned_through + 1 or last[0] != state.last_cursor:
                return LifecycleReadiness(
                    ready=False,
                    reason_code="lifecycle_event_bounds_invalid",
                )
            if (len(first) == 2 and first[1] != first[0] + 1) or (len(last) == 2 and last[1] != last[0] - 1):
                return LifecycleReadiness(
                    ready=False,
                    reason_code="lifecycle_event_sequence_invalid",
                )
            if state.retained_count < 0 or state.retained_count != expected_retained_count:
                return LifecycleReadiness(
                    ready=False,
                    reason_code="lifecycle_event_cardinality_invalid",
                )
            return LifecycleReadiness(ready=True)

    @staticmethod
    async def _begin_lifecycle_read(session: AsyncSession) -> None:
        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            # This must be the first statement in the transaction. A default
            # READ COMMITTED transaction can observe a row snapshot and a
            # later cursor/page from different commits.
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        elif dialect == "sqlite":
            # SQLite pins its read snapshot at the first read in this explicit
            # transaction and retains it through the page query.
            await session.execute(text("BEGIN"))
        else:
            await session.begin()

    async def _after_lifecycle_snapshot(self) -> None:
        """Behavior-neutral deterministic interleaving seam for snapshot tests."""

    @staticmethod
    def _event_to_dict(row: RunLifecycleEventRow) -> dict[str, Any]:
        return {
            "event_id": row.event_id,
            "cursor": row.cursor,
            "run_id": row.run_id,
            "thread_id": row.thread_id,
            "owner_scope": row.owner_scope,
            "tenant_ref": row.tenant_ref,
            "tenant_digest": row.tenant_digest,
            "lifecycle_type": LifecycleType(row.lifecycle_type),
            "state_version": row.state_version,
            "status": row.status,
            "created_at": coerce_iso(row.created_at),
            "payload": row.payload_json,
        }

    async def _append_lifecycle_event(
        self,
        session: AsyncSession,
        cursor_state: RunLifecycleCursorStateRow,
        row: RunRow,
        transition: LifecycleTransition,
        *,
        payload: dict[str, Any] | None = None,
    ) -> RunLifecycleEventRow:
        cursor_state.last_cursor += 1
        event = RunLifecycleEventRow(
            event_id=str(uuid.uuid4()),
            cursor=cursor_state.last_cursor,
            run_id=row.run_id,
            thread_id=row.thread_id,
            owner_scope=lifecycle_owner_scope(row.user_id),
            tenant_ref=row.tenant_ref,
            tenant_digest=row.tenant_digest,
            lifecycle_type=transition.lifecycle_type.value,
            state_version=row.state_version,
            status=row.status,
            created_at=datetime.now(UTC),
            payload_json=payload if payload is not None else build_lifecycle_payload(transition),
        )
        session.add(event)
        return event

    async def list_lifecycle_events(
        self,
        *,
        run_id: str | None = None,
        thread_id: str | None = None,
    ) -> list[dict[str, Any]]:
        stmt = self._scope_event(select(RunLifecycleEventRow))
        if run_id is not None:
            stmt = stmt.where(RunLifecycleEventRow.run_id == run_id)
        if thread_id is not None:
            stmt = stmt.where(RunLifecycleEventRow.thread_id == thread_id)
        stmt = stmt.order_by(RunLifecycleEventRow.cursor.asc())
        async with self._sf() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return [self._event_to_dict(row) for row in rows]

    async def query_lifecycle(self, query: LifecycleQuery) -> LifecyclePage:
        async with self._sf() as session:
            await self._begin_lifecycle_read(session)

            cursor_state = await session.get(RunLifecycleCursorStateRow, 1)
            if cursor_state is None:
                event_count = await session.scalar(self._scope_event(select(func.count()).select_from(RunLifecycleEventRow)))
                await session.rollback()
                detail = "events exist without cursor metadata" if event_count else "cursor metadata is missing"
                raise LifecycleOrderingCorruption(detail)
            # The cursor metadata read is the first SELECT and therefore pins
            # the repeatable-read/SQLite snapshot used by events and summaries.
            await self._after_lifecycle_snapshot()
            requested = validate_cursor_window(
                query.cursor,
                pruned_through=cursor_state.pruned_through,
                last_cursor=cursor_state.last_cursor,
            )
            fence = cursor_state.last_cursor

            event_stmt = select(RunLifecycleEventRow).where(
                RunLifecycleEventRow.cursor > requested,
                RunLifecycleEventRow.cursor <= fence,
            )
            event_stmt = self._scope_event(event_stmt)
            if query.run_id is not None:
                event_stmt = event_stmt.where(RunLifecycleEventRow.run_id == query.run_id)
            else:
                event_stmt = event_stmt.where(RunLifecycleEventRow.thread_id == query.thread_id)
            if query.owner_scope is not None:
                event_stmt = event_stmt.where(RunLifecycleEventRow.owner_scope == query.owner_scope)
            joined_run = False
            if query.source_kind is not None:
                event_stmt = event_stmt.join(
                    RunRow,
                    RunRow.run_id == RunLifecycleEventRow.run_id,
                ).where(
                    RunRow.operation_kind == "run",
                    RunRow.origin_json["source_kind"].as_string() == query.source_kind,
                )
                event_stmt = self._scope_run(event_stmt)
                joined_run = True
            if query.visibility_scope is not None and not query.visibility_scope.allow_context:
                scope = query.visibility_scope
                visibility = []
                if scope.run_ids:
                    visibility.append(RunLifecycleEventRow.run_id.in_(scope.run_ids))
                if scope.owner_ids:
                    visibility.append(RunLifecycleEventRow.owner_scope.in_(tuple(lifecycle_owner_scope(owner_id) for owner_id in scope.owner_ids)))
                if scope.source_kinds:
                    if not joined_run:
                        event_stmt = event_stmt.join(
                            RunRow,
                            RunRow.run_id == RunLifecycleEventRow.run_id,
                        ).where(RunRow.operation_kind == "run")
                        event_stmt = self._scope_run(event_stmt)
                        joined_run = True
                    visibility.append(RunRow.origin_json["source_kind"].as_string().in_(scope.source_kinds))
                event_stmt = event_stmt.where(or_(*visibility))
            event_stmt = event_stmt.order_by(RunLifecycleEventRow.cursor.asc()).limit(query.limit + 1)
            rows = (await session.execute(event_stmt)).scalars().all()
            pruned_through = cursor_state.pruned_through
            has_more = len(rows) > query.limit
            events = [self._event_to_dict(row) for row in rows[: query.limit]]
            snapshots: list[dict[str, Any]] = []
            summaries: list[dict[str, Any]] = []
            if query.include_snapshot:
                if query.run_id is not None:
                    summary_run_ids = (query.run_id,)
                else:
                    summary_run_ids = tuple(dict.fromkeys(event["run_id"] for event in events))
                summary_rows = await self._load_lifecycle_summary_rows(
                    session,
                    run_ids=summary_run_ids,
                )
                by_id = {row.run_id: row for row in summary_rows}
                for run_id in summary_run_ids:
                    row = by_id.get(run_id)
                    if row is None or (query.owner_scope is not None and lifecycle_owner_scope(row.user_id) != query.owner_scope):
                        continue
                    row_dict = self._row_to_dict(row)
                    summary = build_invocation_summary(row_dict)
                    if summary is not None:
                        summaries.append(summary)
                    snapshots.append(
                        {
                            "run_id": row.run_id,
                            "thread_id": row.thread_id,
                            "status": row.status,
                            "state_version": row.state_version,
                        }
                    )
            await session.rollback()

        next_value = events[-1]["cursor"] if has_more else fence
        return LifecyclePage(
            snapshots=tuple(snapshots),
            events=tuple(events),
            next_cursor=encode_lifecycle_cursor(next_value),
            minimum_available_cursor=encode_lifecycle_cursor(pruned_through),
            read_fence_cursor=encode_lifecycle_cursor(fence),
            summaries=tuple(summaries),
        )

    async def context_visible_in_scope(
        self,
        thread_id: str,
        scope: LifecycleVisibilityScope,
    ) -> bool:
        if scope.thread_id != thread_id:
            raise ValueError("lifecycle visibility scope is bound to another context")
        stmt = self._scope_run(select(RunRow.run_id)).where(
            RunRow.thread_id == thread_id,
            RunRow.operation_kind == "run",
        )
        if not scope.allow_context:
            visibility = []
            if scope.run_ids:
                visibility.append(RunRow.run_id.in_(scope.run_ids))
            if scope.owner_ids:
                visibility.append(RunRow.user_id.in_(scope.owner_ids))
            if scope.source_kinds:
                visibility.append(RunRow.origin_json["source_kind"].as_string().in_(scope.source_kinds))
            stmt = stmt.where(or_(*visibility))
        async with self._sf() as session:
            return (await session.execute(stmt.limit(1))).first() is not None

    async def _load_lifecycle_summary_rows(
        self,
        session: AsyncSession,
        *,
        run_ids: tuple[str, ...],
    ) -> list[RunRow]:
        """Load at most the distinct normal runs present in one event page."""

        if not run_ids:
            return []
        stmt = self._scope_run(select(RunRow)).where(
            RunRow.run_id.in_(run_ids),
            RunRow.operation_kind == "run",
        )
        return list((await session.execute(stmt)).scalars().all())

    async def prune_lifecycle_through(self, cursor: str) -> str:
        requested = decode_lifecycle_cursor(cursor)
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            cursor_state = await self._lock_cursor_state(session)
            if requested > cursor_state.last_cursor:
                fence = encode_lifecycle_cursor(cursor_state.last_cursor)
                await session.rollback()
                raise CursorAhead(fence)
            if requested > cursor_state.pruned_through:
                await session.execute(
                    self._scope_event(
                        delete(RunLifecycleEventRow).where(
                            RunLifecycleEventRow.cursor <= requested,
                        )
                    )
                )
                cursor_state.pruned_through = requested
            result = encode_lifecycle_cursor(cursor_state.pruned_through)
            await session.commit()
            return result

    async def transition_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        user_id: str | None = None,
    ) -> LifecycleTransitionResult:
        return await self._transition_run_atomic(
            run_id,
            expected_state_version=expected_state_version,
            expected_statuses=expected_statuses,
            transition=transition,
            user_id=user_id,
        )

    async def transition_owned_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
        user_id: str | None = None,
    ) -> LifecycleTransitionResult:
        return await self._transition_run_atomic(
            run_id,
            expected_state_version=expected_state_version,
            expected_statuses=expected_statuses,
            transition=transition,
            user_id=user_id,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        )

    @asynccontextmanager
    async def hold_execution_fence(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
        terminal_state_version: int | None = None,
        allowed_active_statuses: tuple[str, ...] = ("running",),
    ) -> AsyncIterator[bool]:
        """Lock the run row while a checkpoint mutation uses its owner fence."""

        if not allowed_active_statuses or any(status not in {"pending", "running"} for status in allowed_active_statuses):
            raise ValueError("allowed_active_statuses must contain active run states")
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            active = bool(
                row is not None
                and terminal_state_version is not None
                and row.state_version == terminal_state_version
                and row.status in {"success", "error", "timeout", "interrupted"}
                and row.terminal_projection_owner_worker_id == owner_worker_id
                and row.terminal_projection_active_state_version == state_version
                and row.owner_worker_id is None
                and row.lease_expires_at is None
            )
            if row is not None and row.status in allowed_active_statuses and row.owner_worker_id == owner_worker_id and row.state_version == state_version:
                if row.lease_expires_at is None:
                    # Heartbeat-disabled single-node deployments still have a
                    # serializable owner/epoch capability: this transaction
                    # holds the lifecycle write lock through the external
                    # mutation. A competing transition cannot interleave.
                    active = True
                else:
                    deadline = row.lease_expires_at
                    if deadline.tzinfo is None:
                        deadline = deadline.replace(tzinfo=UTC)
                    database_now = await session.scalar(
                        select(
                            database_wall_clock_expression(
                                session.get_bind().dialect.name,
                            )
                        )
                    )
                    if database_now is None:
                        await session.rollback()
                        raise RuntimeError("database clock is unavailable")
                    database_now = coerce_database_wall_clock(database_now)
                    active = deadline > database_now
            try:
                yield active
            finally:
                await session.rollback()

    async def _transition_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        user_id: str | None,
        expected_owner_worker_id: str | None = None,
        require_unexpired_lease: bool = False,
    ) -> LifecycleTransitionResult:
        payload = build_lifecycle_payload(transition)
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            stmt = self._scope_run(select(RunRow)).where(RunRow.run_id == run_id, RunRow.operation_kind == "run")
            if user_id is not None:
                stmt = stmt.where(RunRow.user_id == user_id)
            if session.get_bind().dialect.name == "postgresql":
                stmt = stmt.with_for_update()
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                await session.rollback()
                return LifecycleTransitionResult(applied=False)
            if expected_owner_worker_id is not None and not await self._owned_run_fence_matches(
                session,
                row,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            ):
                result_row = self._row_to_dict(row)
                await session.rollback()
                return LifecycleTransitionResult(applied=False, row=result_row)
            if row.state_version != expected_state_version or (expected_statuses is not None and row.status not in expected_statuses):
                result_row = self._row_to_dict(row)
                await session.rollback()
                return LifecycleTransitionResult(applied=False, row=result_row)
            cursor_state = await self._lock_cursor_state(session)
            if expected_owner_worker_id is not None and not await self._owned_run_fence_matches(
                session,
                row,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            ):
                result_row = self._row_to_dict(row)
                await session.rollback()
                return LifecycleTransitionResult(applied=False, row=result_row)
            if transition.status in {
                "success",
                "error",
                "timeout",
                "interrupted",
            }:
                row.terminal_projection_owner_worker_id = row.owner_worker_id
                row.terminal_projection_active_state_version = row.state_version if row.owner_worker_id is not None else None
            else:
                row.terminal_projection_owner_worker_id = None
                row.terminal_projection_active_state_version = None
            row.status = transition.status
            row.state_version += 1
            if transition.status in {
                "success",
                "error",
                "timeout",
                "interrupted",
            }:
                row.owner_worker_id = None
                row.lease_expires_at = None
            if transition.error is not None:
                row.error = transition.error
            if transition.stop_reason is not None:
                row.stop_reason = transition.stop_reason
            if transition.execution_evidence_json is not None:
                row.execution_evidence_json = transition.execution_evidence_json
                row.execution_evidence_digest = transition.execution_evidence_digest
            row.updated_at = datetime.now(UTC)
            event = await self._append_lifecycle_event(session, cursor_state, row, transition, payload=payload)
            await session.flush()
            result_row = self._row_to_dict(row)
            result_event = self._event_to_dict(event)
            await session.commit()
            return LifecycleTransitionResult(applied=True, row=result_row, event=result_event)

    async def request_cancel_fenced(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        return await self._request_cancel_atomic(
            run_id,
            action=action,
            expected_state_version=expected_state_version,
            user_id=user_id,
        )

    async def request_cancel_compat(
        self,
        run_id: str,
        *,
        action: str,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        return await self._request_cancel_atomic(run_id, action=action, user_id=user_id)

    async def request_cancel_owned(
        self,
        run_id: str,
        *,
        action: str,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        return await self._request_cancel_atomic(
            run_id,
            action=action,
            user_id=user_id,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        )

    async def _request_cancel_atomic(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int | None = None,
        user_id: str | None = None,
        expected_owner_worker_id: str | None = None,
        require_unexpired_lease: bool = False,
    ) -> CancellationRequestResult:
        if action not in ("interrupt", "rollback"):
            raise ValueError(f"Unsupported cancellation action: {action}")
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            stmt = self._scope_run(select(RunRow)).where(RunRow.run_id == run_id, RunRow.operation_kind == "run")
            if user_id is not None:
                stmt = stmt.where(RunRow.user_id == user_id)
            if session.get_bind().dialect.name == "postgresql":
                stmt = stmt.with_for_update()
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                await session.rollback()
                return CancellationRequestResult(CancellationRequestOutcome.not_found_or_invisible)
            if expected_owner_worker_id is not None and not await self._owned_run_fence_matches(
                session,
                row,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            ):
                await session.rollback()
                return CancellationRequestResult(CancellationRequestOutcome.stale)
            result_row = self._row_to_dict(row)
            if row.cancel_action == action:
                await session.rollback()
                return CancellationRequestResult(CancellationRequestOutcome.already_requested, row=result_row)
            if row.status in ("success", "error", "timeout", "interrupted"):
                await session.rollback()
                return CancellationRequestResult(CancellationRequestOutcome.already_terminal, row=result_row)
            if row.cancel_action is not None or (expected_state_version is not None and row.state_version != expected_state_version):
                await session.rollback()
                return CancellationRequestResult(CancellationRequestOutcome.stale, row=result_row)
            cursor_state = await self._lock_cursor_state(session)
            if expected_owner_worker_id is not None and not await self._owned_run_fence_matches(
                session,
                row,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            ):
                await session.rollback()
                return CancellationRequestResult(CancellationRequestOutcome.stale)
            now = datetime.now(UTC)
            row.cancel_action = action
            row.cancel_requested_at = now
            row.state_version += 1
            row.updated_at = now
            transition = LifecycleTransition(
                lifecycle_type=LifecycleType.cancellation_requested,
                status=row.status,
                evidence={"action": action},
            )
            event = await self._append_lifecycle_event(session, cursor_state, row, transition)
            await session.flush()
            result_row = self._row_to_dict(row)
            result_event = self._event_to_dict(event)
            await session.commit()
            return CancellationRequestResult(
                CancellationRequestOutcome.requested,
                row=result_row,
                event=result_event,
            )

    @staticmethod
    async def _owned_run_fence_matches(
        session: AsyncSession,
        row: RunRow,
        *,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
    ) -> bool:
        """Check an attachment owner's authority while its run row is locked."""

        observed_at = await _database_now_after_lock(session)
        return RunRepository._owned_run_fence_matches_at(
            row,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
            observed_at=observed_at,
        )

    @staticmethod
    def _owned_run_fence_matches_at(
        row: RunRow,
        *,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
        observed_at: datetime,
    ) -> bool:
        if row.owner_worker_id != expected_owner_worker_id:
            return False
        if not require_unexpired_lease:
            return True
        return not _lease_expired_at(
            row.lease_expires_at,
            observed_at=observed_at,
            grace_seconds=0,
        )

    async def _owned_observation_fence_matches(
        self,
        session: AsyncSession,
        row: RunRow,
        *,
        expected_owner_worker_id: str | None,
        expected_state_version: int | None,
        require_unexpired_lease: bool,
    ) -> bool:
        """Validate one optional owner epoch for a locked observation row."""

        fenced = expected_owner_worker_id is not None or expected_state_version is not None or require_unexpired_lease
        if not fenced:
            return row.lease_expires_at is None
        if expected_owner_worker_id is None or expected_state_version is None:
            return False
        if row.state_version != expected_state_version:
            return False
        if row.lease_expires_at is None:
            return not require_unexpired_lease and row.owner_worker_id == expected_owner_worker_id
        if not require_unexpired_lease:
            return False
        return await self._owned_run_fence_matches(
            session,
            row,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=True,
        )

    async def bind_assembly_evidence(
        self,
        run_id: str,
        *,
        owner_id: str,
        lease_epoch: int,
        evidence_json: Mapping[str, object],
        evidence_digest: str,
    ) -> BindAssemblyEvidenceOutcome:
        actual = AssemblyEvidenceV1.from_persisted_json(evidence_json)
        if assembly_evidence_digest(actual) != evidence_digest:
            raise AssemblyEvidenceError("assembly_descriptor_invalid")
        normalized = actual.to_persisted_json()

        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                await session.commit()
                return BindAssemblyEvidenceOutcome.not_found
            if (
                row.status != "running"
                or row.state_version != lease_epoch
                or not await self._owned_run_fence_matches(
                    session,
                    row,
                    expected_owner_worker_id=owner_id,
                    require_unexpired_lease=row.lease_expires_at is not None,
                )
            ):
                await session.commit()
                return BindAssemblyEvidenceOutcome.ownership_lost

            stored_json = row.assembly_evidence_json
            stored_digest = row.assembly_evidence_digest
            if stored_json is None and stored_digest is None:
                row.assembly_evidence_json = normalized
                row.assembly_evidence_digest = evidence_digest
                row.updated_at = datetime.now(UTC)
                await session.commit()
                return BindAssemblyEvidenceOutcome.bound
            if stored_json is None or stored_digest is None:
                await session.commit()
                return BindAssemblyEvidenceOutcome.mismatch
            if assembly_evidence_binding_matches(
                actual,
                actual_digest=evidence_digest,
                persisted_json=stored_json,
                persisted_digest=stored_digest,
            ):
                await session.commit()
                return BindAssemblyEvidenceOutcome.already_matching
            await session.commit()
            return BindAssemblyEvidenceOutcome.mismatch

    async def apply_execution_policy_state(
        self,
        run_id: str,
        *,
        owner_id: str,
        lease_epoch: int,
        expected_digest: str | None,
        state_json: Mapping[str, object],
        state_digest: str,
    ) -> ApplyExecutionPolicyStateOutcome:
        state = ExecutionPolicyStateV1.from_json(state_json)
        if state.digest != state_digest:
            raise ValueError("execution policy state digest mismatch")

        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                await session.commit()
                return ApplyExecutionPolicyStateOutcome.not_found
            if (
                row.status not in {"pending", "running"}
                or row.state_version != lease_epoch
                or not await self._owned_run_fence_matches(
                    session,
                    row,
                    expected_owner_worker_id=owner_id,
                    require_unexpired_lease=row.lease_expires_at is not None,
                )
            ):
                await session.commit()
                return ApplyExecutionPolicyStateOutcome.ownership_lost
            if row.execution_policy_state_digest != expected_digest:
                await session.commit()
                return ApplyExecutionPolicyStateOutcome.conflict
            row.execution_policy_state_json = state.to_json()
            row.execution_policy_state_digest = state_digest
            row.updated_at = datetime.now(UTC)
            await session.commit()
            return ApplyExecutionPolicyStateOutcome.applied

    @staticmethod
    def _normalize_model_name(model_name: str | None) -> str | None:
        """Validate a model-profile identity without changing it for storage."""
        if model_name is None:
            return None
        return validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")

    @staticmethod
    def _safe_json(obj: Any) -> Any:
        """Ensure obj is JSON-serializable. Falls back to model_dump() or str()."""
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, dict):
            return {k: RunRepository._safe_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [RunRepository._safe_json(v) for v in obj]
        if hasattr(obj, "model_dump"):
            try:
                return obj.model_dump()
            except Exception:
                pass
        if hasattr(obj, "dict"):
            try:
                return obj.dict()
            except Exception:
                pass
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

    @staticmethod
    def _row_to_dict(row: RunRow) -> dict[str, Any]:
        d = row.to_dict()
        # Remap JSON columns to match RunStore interface
        d["metadata"] = d.pop("metadata_json", {})
        d["kwargs"] = d.pop("kwargs_json", {})
        # Convert datetime to ISO string for consistency with MemoryRunStore.
        # SQLite drops tzinfo on read despite ``DateTime(timezone=True)`` —
        # ``coerce_iso`` normalizes naive datetimes as UTC.
        for key in ("created_at", "updated_at", "lease_expires_at", "cancel_requested_at"):
            val = d.get(key)
            if isinstance(val, _DATETIME_TYPE):
                d[key] = coerce_iso(val)
        return d

    async def put(
        self,
        run_id: str,
        *,
        thread_id: str,
        assistant_id: str | None = None,
        user_id: str | None | _AutoSentinel = AUTO,
        model_name: str | None = None,
        status: str = "pending",
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        error: str | None = None,
        stop_reason: str | None = None,
        created_at: str | None = None,
        follow_up_to_run_id: str | None = None,
        owner_worker_id: str | None = None,
        lease_expires_at: str | None = None,
        origin_json: dict[str, Any] | None = None,
        principal_projection_json: dict[str, Any] | None = None,
        principal_projection_digest: str | None = None,
        base_origin_digest: str | None = None,
        accepted_context_digest: str | None = None,
        tenant: TenantReferenceV1 | None = None,
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
        caller_intent_json: dict[str, Any] | None = None,
        caller_intent_digest: str | None = None,
        caller_intent_digest_version: str | None = None,
        idempotency_key: str | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy.terminalize_v1,
        recovery_payload_json: dict[str, Any] | None = None,
    ) -> None:
        """Insert or update a run row.

        ``RunManager`` retries ``put`` after transient SQLite failures.  Making
        this operation idempotent prevents a successful-but-unacknowledged first
        commit from turning the retry into a primary-key failure.
        """
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.put")
        thread_id = validate_thread_identifier(thread_id)
        tenant_ref, tenant_digest = tenant_store_columns(self._tenant, tenant)
        recovery_policy = RecoveryPolicy(recovery_policy)
        if operation_kind != "run" and recovery_policy is not RecoveryPolicy.terminalize_v1:
            raise ValueError("execution recovery policy applies only to normal runs")
        if (recovery_policy is RecoveryPolicy.exact_two_takeover_v1) != (recovery_payload_json is not None):
            raise ValueError("execution recovery policy and payload must be admitted together")
        now = datetime.now(UTC)
        created = datetime.fromisoformat(created_at) if created_at else now
        lease_dt = datetime.fromisoformat(lease_expires_at) if lease_expires_at else None
        requested_status = status
        lifecycle_row = operation_kind == "run" and requested_status is not None
        terminal_statuses = {
            "success",
            "error",
            "timeout",
            "interrupted",
        }
        terminal_status = requested_status in terminal_statuses
        normalized_recovery_payload = self._safe_json(recovery_payload_json) if operation_kind == "run" else None
        values = {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": resolved_user_id,
            "model_name": self._normalize_model_name(model_name),
            # Non-pending compatibility snapshots still begin as an accepted
            # pending invocation and transition to their requested status in
            # this transaction. ``None`` is reserved for historical-row test
            # fixtures and remains a version-zero legacy shape.
            "status": "pending" if lifecycle_row else requested_status,
            "operation_kind": operation_kind,
            "recovery_policy": recovery_policy.value,
            "recovery_payload_json": normalized_recovery_payload,
            "multitask_strategy": multitask_strategy,
            "metadata_json": self._safe_json(metadata) or {},
            "kwargs_json": self._safe_json(kwargs) or {},
            "error": error,
            "stop_reason": stop_reason,
            "follow_up_to_run_id": follow_up_to_run_id,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_dt,
            "tenant_ref": tenant_ref,
            "tenant_digest": tenant_digest,
            "origin_json": self._safe_json(origin_json) if operation_kind == "run" else None,
            "principal_projection_json": self._safe_json(principal_projection_json) if operation_kind == "run" else None,
            "principal_projection_digest": principal_projection_digest if operation_kind == "run" else None,
            "base_origin_digest": base_origin_digest if operation_kind == "run" else None,
            "accepted_context_digest": accepted_context_digest if operation_kind == "run" else None,
            "agent_revision_json": self._safe_json(agent_revision_json) if operation_kind == "run" else None,
            "agent_revision_digest": agent_revision_digest if operation_kind == "run" else None,
            "extension_generation": extension_generation if operation_kind == "run" else None,
            "decision_evidence_json": self._safe_json(decision_evidence_json) if operation_kind == "run" else None,
            "external_scope": external_scope if operation_kind == "run" else None,
            "external_key": external_key if operation_kind == "run" else None,
            "request_digest": request_digest if operation_kind == "run" else None,
            "request_digest_version": request_digest_version if operation_kind == "run" else None,
            "caller_intent_json": self._safe_json(caller_intent_json) if operation_kind == "run" else None,
            "caller_intent_digest": caller_intent_digest if operation_kind == "run" else None,
            "caller_intent_digest_version": caller_intent_digest_version if operation_kind == "run" else None,
            "idempotency_key": idempotency_key,
            "updated_at": now,
        }
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            row_stmt = select(RunRow).where(RunRow.run_id == run_id)
            if session.get_bind().dialect.name == "postgresql":
                row_stmt = row_stmt.with_for_update()
            row = (await session.execute(row_stmt)).scalar_one_or_none()
            if self._tenant is not None and row is not None and row.tenant_digest != self._tenant.digest:
                await session.rollback()
                raise TenantIdentityError(
                    "tenant_identity_mismatch",
                    "run identity is already bound to a different tenant",
                )
            if row is not None and row.operation_kind == "run":
                if row.recovery_policy != recovery_policy.value or row.recovery_payload_json != normalized_recovery_payload:
                    await session.rollback()
                    raise RecoveryPayloadIntegrityError()
            if row is None:
                row = RunRow(
                    run_id=run_id,
                    created_at=created,
                    admission_cursor=await self._next_admission_cursor(session),
                    state_version=1 if lifecycle_row else 0,
                    **values,
                )
                session.add(row)
                if lifecycle_row:
                    await session.flush()
                    cursor_state = await self._lock_cursor_state(session)
                    await self._append_lifecycle_event(
                        session,
                        cursor_state,
                        row,
                        LifecycleTransition(
                            lifecycle_type=LifecycleType.accepted,
                            status="pending",
                        ),
                    )
                    if requested_status != "pending":
                        if terminal_status:
                            row.terminal_projection_owner_worker_id = row.owner_worker_id
                            row.terminal_projection_active_state_version = row.state_version if row.owner_worker_id is not None else None
                        row.status = requested_status
                        row.state_version += 1
                        if terminal_status:
                            row.owner_worker_id = None
                            row.lease_expires_at = None
                        await self._append_lifecycle_event(
                            session,
                            cursor_state,
                            row,
                            LifecycleTransition(
                                lifecycle_type=lifecycle_type_for_status(requested_status),
                                status=requested_status,
                                error=error,
                                stop_reason=stop_reason,
                                reason=stop_reason,
                            ),
                        )
            else:
                # Snapshot repair is idempotent but may not rewrite the
                # authoritative status/version pair outside a transition.
                values.pop("status")
                values.pop("operation_kind", None)
                values.pop("recovery_policy", None)
                values.pop("recovery_payload_json", None)
                if row.operation_kind == "run":
                    values.pop("error", None)
                    values.pop("stop_reason", None)
                    values.pop("owner_worker_id", None)
                    values.pop("lease_expires_at", None)
                for key, value in values.items():
                    setattr(row, key, value)
                if row.status != requested_status:
                    if row.operation_kind != "run":
                        row.status = requested_status
                        if terminal_status:
                            row.owner_worker_id = None
                            row.lease_expires_at = None
            if row.status in terminal_statuses:
                row.owner_worker_id = None
                row.lease_expires_at = None
            await session.commit()

    async def get(
        self,
        run_id: str,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> dict[str, Any] | None:
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.get")
        async with self._sf() as session:
            row = (await session.execute(self._scope_run(select(RunRow)).where(RunRow.run_id == run_id))).scalar_one_or_none()
            if row is None:
                return None
            if resolved_user_id is not None and row.user_id != resolved_user_id:
                return None
            return self._row_to_dict(row)

    async def authoritative_get(self, run_id: str) -> dict[str, Any] | None:
        """Return one row by primary identity without applying owner scope."""

        async with self._sf() as session:
            row = (await session.execute(self._scope_run(select(RunRow)).where(RunRow.run_id == run_id))).scalar_one_or_none()
            return self._row_to_dict(row) if row is not None else None

    async def thread_projection_authorized(
        self,
        *,
        run_id: str,
        thread_id: str,
        run_status: str,
        owner_worker_id: str,
        active_state_version: int,
        terminal_state_version: int | None = None,
    ) -> bool:
        async with self._sf() as session:
            candidate = (
                await session.execute(
                    self._scope_run(select(RunRow)).where(
                        RunRow.run_id == run_id,
                        RunRow.thread_id == thread_id,
                        RunRow.operation_kind == "run",
                        RunRow.status == run_status,
                    )
                )
            ).scalar_one_or_none()
            if candidate is None or candidate.admission_cursor is None:
                return False
            normal_rows = list(
                (
                    await session.execute(
                        self._scope_run(select(RunRow)).where(
                            RunRow.thread_id == thread_id,
                            RunRow.operation_kind == "run",
                        )
                    )
                ).scalars()
            )
            if not normal_rows or any(row.admission_cursor is None for row in normal_rows) or max(row.admission_cursor for row in normal_rows if row.admission_cursor is not None) != candidate.admission_cursor:
                return False
            if terminal_state_version is None:
                if not (run_status == "running" and candidate.owner_worker_id == owner_worker_id and candidate.state_version == active_state_version):
                    return False
                if candidate.lease_expires_at is None:
                    return True
                database_now = await session.scalar(
                    select(
                        database_wall_clock_expression(
                            session.get_bind().dialect.name,
                        )
                    )
                )
                if database_now is None:
                    return False
                lease_expires_at = candidate.lease_expires_at
                if lease_expires_at.tzinfo is None:
                    lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
                return lease_expires_at > coerce_database_wall_clock(database_now)
            return bool(
                run_status in {"success", "error", "timeout", "interrupted"}
                and terminal_state_version > active_state_version
                and candidate.state_version == terminal_state_version
                and candidate.terminal_projection_owner_worker_id == owner_worker_id
                and candidate.terminal_projection_active_state_version == active_state_version
                and candidate.owner_worker_id is None
                and candidate.lease_expires_at is None
            )

    async def get_by_external_identity(
        self,
        external_scope: str,
        external_key: str,
    ) -> dict[str, Any] | None:
        async with self._sf() as session:
            result = await session.execute(
                self._scope_run(select(RunRow)).where(
                    RunRow.external_scope == external_scope,
                    RunRow.external_key == external_key,
                    RunRow.operation_kind == "run",
                )
            )
            row = result.scalar_one_or_none()
            return self._row_to_dict(row) if row is not None else None

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        async with self._sf() as session:
            result = await session.execute(
                self._scope_run(select(RunRow)).where(
                    RunRow.idempotency_key == idempotency_key,
                )
            )
            row = result.scalar_one_or_none()
            return self._row_to_dict(row) if row is not None else None

    async def list_by_thread(
        self,
        thread_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
        limit=100,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.list_by_thread")
        stmt = self._scope_run(select(RunRow)).where(RunRow.thread_id == thread_id, RunRow.operation_kind == "run")
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        stmt = stmt.order_by(RunRow.created_at.desc()).limit(limit)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def list_successful_regenerate_sources(
        self,
        thread_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.list_successful_regenerate_sources")
        source = RunRow.metadata_json["regenerate_from_run_id"].as_string()
        stmt = self._scope_run(select(source)).where(
            RunRow.thread_id == thread_id,
            RunRow.operation_kind == "run",
            RunRow.status == "success",
            source.is_not(None),
            source != "",
        )
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return {value for value in result.scalars() if isinstance(value, str) and value}

    async def list_edit_regenerate_runs(
        self,
        thread_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.list_edit_regenerate_runs")
        replay_kind = RunRow.metadata_json["replay_kind"].as_string()
        source = RunRow.metadata_json["regenerate_from_run_id"].as_string()
        stmt = self._scope_run(select(RunRow)).where(
            RunRow.thread_id == thread_id,
            replay_kind == "edit",
            source.is_not(None),
            source != "",
        )
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        stmt = stmt.order_by(RunRow.created_at.asc())
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(row) for row in result.scalars()]

    async def get_many_by_thread(
        self,
        thread_id,
        run_ids,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        if not run_ids:
            return {}
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.get_many_by_thread")
        stmt = self._scope_run(select(RunRow)).where(RunRow.thread_id == thread_id, RunRow.operation_kind == "run", RunRow.run_id.in_(run_ids))
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return {row.run_id: self._row_to_dict(row) for row in result.scalars()}

    async def update_status(self, run_id, status, *, error=None, stop_reason=None) -> bool:
        current = await self.get(run_id, user_id=None)
        if current is None:
            return False
        if current.get("operation_kind", "run") != "run":
            values: dict[str, Any] = {"status": status, "updated_at": datetime.now(UTC)}
            if error is not None:
                values["error"] = error
            if stop_reason is not None:
                values["stop_reason"] = stop_reason
            async with self._sf() as session:
                result = await session.execute(
                    self._scope_run(update(RunRow))
                    .where(
                        RunRow.run_id == run_id,
                        RunRow.operation_kind != "run",
                        RunRow.status.in_(("pending", "running", "interrupted")),
                    )
                    .values(**values)
                )
                await session.commit()
                return result.rowcount != 0
        lifecycle_type = lifecycle_type_for_status(status)
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=current["state_version"],
            expected_statuses=("pending", "running", "interrupted"),
            transition=LifecycleTransition(
                lifecycle_type=lifecycle_type,
                status=status,
                error=error,
                stop_reason=stop_reason,
                reason=stop_reason,
            ),
        )
        return result.applied

    async def start_run(
        self,
        run_id: str,
        *,
        execution_evidence_json: dict[str, Any] | None = None,
        execution_evidence_digest: str | None = None,
    ) -> bool:
        """Start only a still-pending run; cancelled rows must not be resurrected."""
        validate_execution_evidence_run(run_id, execution_evidence_json)
        current = await self.get(run_id, user_id=None)
        if current is None:
            return False
        if current.get("operation_kind", "run") != "run":
            return False
        if current.get("cancel_action") is not None:
            return False
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=current["state_version"],
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.started,
                status="running",
                execution_evidence_json=execution_evidence_json,
                execution_evidence_digest=execution_evidence_digest,
            ),
        )
        return result.applied

    async def update_model_name(
        self,
        run_id: str,
        model_name: str | None,
        *,
        expected_owner_worker_id: str | None = None,
        expected_state_version: int | None = None,
        require_unexpired_lease: bool = False,
    ) -> bool:
        normalized_model_name = self._normalize_model_name(model_name)
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None or not await self._owned_observation_fence_matches(
                session,
                row,
                expected_owner_worker_id=expected_owner_worker_id,
                expected_state_version=expected_state_version,
                require_unexpired_lease=require_unexpired_lease,
            ):
                await session.rollback()
                return False
            row.model_name = normalized_model_name
            row.updated_at = datetime.now(UTC)
            await session.commit()
            return True

    async def delete(
        self,
        run_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.delete")
        async with self._sf() as session:
            row = (await session.execute(self._scope_run(select(RunRow)).where(RunRow.run_id == run_id))).scalar_one_or_none()
            if row is None:
                return
            if resolved_user_id is not None and row.user_id != resolved_user_id:
                return
            await session.delete(row)
            await session.commit()

    async def delete_thread_operation(self, run_id: str, *, user_id: str | None) -> None:
        """Release a reservation using its captured owner, not request context."""
        await self.delete(run_id, user_id=user_id)

    async def release_thread_operation_owned(
        self,
        run_id: str,
        *,
        thread_id: str,
        operation_kind: str,
        user_id: str | None,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
    ) -> ThreadOperationReleaseResult:
        """Release one exact auxiliary reservation under a row lock."""

        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(RunRow.run_id == run_id)
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                await session.commit()
                return ThreadOperationReleaseResult(
                    outcome=ThreadOperationReleaseOutcome.absent,
                )
            if row.operation_kind == "run" or row.operation_kind != operation_kind or row.thread_id != thread_id or row.user_id != user_id:
                await session.commit()
                return ThreadOperationReleaseResult(
                    outcome=ThreadOperationReleaseOutcome.identity_mismatch,
                )
            if row.status not in {"pending", "running"}:
                await session.commit()
                return ThreadOperationReleaseResult(
                    outcome=ThreadOperationReleaseOutcome.inactive,
                )
            if row.owner_worker_id != expected_owner_worker_id:
                await session.commit()
                return ThreadOperationReleaseResult(
                    outcome=ThreadOperationReleaseOutcome.ownership_lost,
                )
            if require_unexpired_lease:
                lease = coerce_iso(row.lease_expires_at)
                try:
                    lease_datetime = datetime.fromisoformat(lease)
                    if lease_datetime.tzinfo is None:
                        lease_datetime = lease_datetime.replace(tzinfo=UTC)
                except (TypeError, ValueError):
                    lease_datetime = datetime.min.replace(tzinfo=UTC)
                dialect_name = session.get_bind().dialect.name
                database_now = await session.scalar(select(database_wall_clock_expression(dialect_name)))
                if database_now is None:
                    await session.rollback()
                    raise RuntimeError("database clock is unavailable")
                database_now = coerce_database_wall_clock(database_now)
                if lease_datetime <= database_now:
                    await session.commit()
                    return ThreadOperationReleaseResult(
                        outcome=ThreadOperationReleaseOutcome.ownership_lost,
                    )
            await session.delete(row)
            await session.commit()
            return ThreadOperationReleaseResult(
                outcome=ThreadOperationReleaseOutcome.released,
            )

    async def list_pending(self, *, before=None):
        if before is None:
            before_dt = datetime.now(UTC)
        elif isinstance(before, datetime):
            before_dt = before
        else:
            before_dt = datetime.fromisoformat(before)
        stmt = self._scope_run(select(RunRow)).where(RunRow.operation_kind == "run", RunRow.status == "pending", RunRow.created_at <= before_dt).order_by(RunRow.created_at.asc())
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def list_inflight(self, *, before=None):
        """Return persisted active runs for startup recovery."""
        if before is None:
            before_dt = datetime.now(UTC)
        elif isinstance(before, datetime):
            before_dt = before
        else:
            before_dt = datetime.fromisoformat(before)
        stmt = (
            self._scope_run(select(RunRow))
            .where(
                RunRow.status.in_(("pending", "running")),
                RunRow.created_at <= before_dt,
            )
            .order_by(RunRow.created_at.asc())
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def update_run_completion(
        self,
        run_id: str,
        *,
        status: str,
        expected_owner_worker_id: str | None = None,
        expected_active_state_version: int | None = None,
        expected_terminal_state_version: int | None = None,
        total_input_tokens: int = 0,
        total_output_tokens: int = 0,
        total_tokens: int = 0,
        llm_call_count: int = 0,
        lead_agent_tokens: int = 0,
        subagent_tokens: int = 0,
        middleware_tokens: int = 0,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int = 0,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Project metrics onto one exactly authorized terminal row."""
        capability = (
            expected_owner_worker_id,
            expected_active_state_version,
            expected_terminal_state_version,
        )
        if any(value is not None for value in capability) and not all(value is not None for value in capability):
            raise ValueError(
                "terminal completion authority must be supplied together",
            )
        if status not in {"success", "error", "timeout", "interrupted"}:
            return False

        values: dict[str, Any] = {
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "llm_call_count": llm_call_count,
            "lead_agent_tokens": lead_agent_tokens,
            "subagent_tokens": subagent_tokens,
            "middleware_tokens": middleware_tokens,
            "token_usage_by_model": self._safe_json(token_usage_by_model) or {},
            "message_count": message_count,
            "updated_at": datetime.now(UTC),
        }
        if last_ai_message is not None:
            values["last_ai_message"] = last_ai_message[:2000]
        if first_human_message is not None:
            values["first_human_message"] = first_human_message[:2000]
        if error is not None:
            values["error"] = error
        async with self._sf() as session:
            row = (
                await session.execute(
                    self._scope_run(select(RunRow))
                    .where(
                        RunRow.run_id == run_id,
                        RunRow.status == status,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return False
            if all(value is not None for value in capability):
                if (
                    row.owner_worker_id is not None
                    or row.lease_expires_at is not None
                    or row.terminal_projection_owner_worker_id != expected_owner_worker_id
                    or row.terminal_projection_active_state_version != expected_active_state_version
                    or row.state_version != expected_terminal_state_version
                ):
                    return False
            elif row.terminal_projection_owner_worker_id is not None or row.terminal_projection_active_state_version is not None:
                return False
            for key, value in values.items():
                setattr(row, key, value)
            await session.commit()
            return True

    async def update_run_progress(
        self,
        run_id: str,
        *,
        expected_owner_worker_id: str | None = None,
        expected_state_version: int | None = None,
        require_unexpired_lease: bool = False,
        total_input_tokens: int | None = None,
        total_output_tokens: int | None = None,
        total_tokens: int | None = None,
        llm_call_count: int | None = None,
        lead_agent_tokens: int | None = None,
        subagent_tokens: int | None = None,
        middleware_tokens: int | None = None,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int | None = None,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
    ) -> bool:
        """Update token usage + convenience fields while a run is still active."""
        values: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        optional_counters = {
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "llm_call_count": llm_call_count,
            "lead_agent_tokens": lead_agent_tokens,
            "subagent_tokens": subagent_tokens,
            "middleware_tokens": middleware_tokens,
            "message_count": message_count,
        }
        for key, value in optional_counters.items():
            if value is not None:
                values[key] = value
        if token_usage_by_model is not None:
            values["token_usage_by_model"] = self._safe_json(token_usage_by_model) or {}
        if last_ai_message is not None:
            values["last_ai_message"] = last_ai_message[:2000]
        if first_human_message is not None:
            values["first_human_message"] = first_human_message[:2000]
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.status == "running",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None or not await self._owned_observation_fence_matches(
                session,
                row,
                expected_owner_worker_id=expected_owner_worker_id,
                expected_state_version=expected_state_version,
                require_unexpired_lease=require_unexpired_lease,
            ):
                await session.rollback()
                return False
            for key, value in values.items():
                setattr(row, key, value)
            await session.commit()
            return True

    async def aggregate_tokens_by_thread(self, thread_id: str, *, include_active: bool = False) -> dict[str, Any]:
        """Aggregate token usage for a thread.

        ``by_model`` is reduced in Python from each row's ``token_usage_by_model``
        JSON column so subagent / middleware tokens land on the model that
        actually produced them (issue #3645). Rows written before that column
        existed fall back to ``RunRow.model_name`` + ``RunRow.total_tokens``,
        preserving the legacy lead-only behavior instead of dropping the data.

        Headline totals (``total_tokens``, ``total_input_tokens``,
        ``total_output_tokens``) and the ``by_caller`` bucket are summed from
        their own columns and are therefore unaffected by the JSON column being
        empty.
        """
        statuses = ("success", "error", "running") if include_active else ("success", "error")
        _completed = RunRow.status.in_(statuses)
        _thread = RunRow.thread_id == thread_id
        _run_operation = RunRow.operation_kind == "run"

        stmt = self._scope_run(
            select(
                RunRow.model_name,
                RunRow.total_tokens,
                RunRow.total_input_tokens,
                RunRow.total_output_tokens,
                RunRow.lead_agent_tokens,
                RunRow.subagent_tokens,
                RunRow.middleware_tokens,
                RunRow.token_usage_by_model,
            )
        ).where(_thread, _run_operation, _completed)

        async with self._sf() as session:
            rows = (await session.execute(stmt)).all()

        total_tokens = total_input = total_output = total_runs = 0
        lead_agent = subagent = middleware = 0
        by_model: dict[str, dict] = {}
        for r in rows:
            total_runs += 1
            total_tokens += r.total_tokens
            total_input += r.total_input_tokens
            total_output += r.total_output_tokens
            lead_agent += r.lead_agent_tokens
            subagent += r.subagent_tokens
            middleware += r.middleware_tokens

            # ``or {}`` covers rows written before ``token_usage_by_model``
            # existed (the column is NULL on a manual ALTER ADD COLUMN without
            # backfill); fresh rows always carry the journal-produced dict.
            usage_by_model = r.token_usage_by_model or {}
            if usage_by_model:
                for model, usage in usage_by_model.items():
                    entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                    entry["tokens"] += usage.get("total_tokens", 0)
                    entry["runs"] += 1
            else:
                model = r.model_name or "unknown"
                entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                entry["tokens"] += r.total_tokens
                entry["runs"] += 1

        return {
            "total_tokens": total_tokens,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_runs": total_runs,
            "by_model": by_model,
            "by_caller": {
                "lead_agent": lead_agent,
                "subagent": subagent,
                "middleware": middleware,
            },
        }

    # ------------------------------------------------------------------
    # Multi-worker run ownership methods
    # ------------------------------------------------------------------

    async def update_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
    ) -> bool:
        validate_lease_deadline_request(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            required=True,
        )
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                await session.rollback()
                return False
            observed_at = await _database_now_after_lock(session)
            if row.status not in {"pending", "running"} or not self._owned_run_fence_matches_at(
                row,
                expected_owner_worker_id=owner_worker_id,
                require_unexpired_lease=row.lease_expires_at is not None,
                observed_at=observed_at,
            ):
                await session.rollback()
                return False
            lease_dt = _database_lease_deadline(
                lease_expires_at=lease_expires_at,
                lease_duration_seconds=lease_duration_seconds,
                observed_at=observed_at,
                required=True,
            )
            row.lease_expires_at = lease_dt
            row.updated_at = observed_at
            await session.commit()
            return True

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
    ) -> LeaseRenewal:
        """Renew the owner lease and read cancellation intent atomically."""
        validate_lease_deadline_request(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            required=True,
        )
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                await session.rollback()
                return LeaseRenewal(renewed=False)
            observed_at = await _database_now_after_lock(session)
            if row.status not in {"pending", "running"} or not self._owned_run_fence_matches_at(
                row,
                expected_owner_worker_id=owner_worker_id,
                require_unexpired_lease=row.lease_expires_at is not None,
                observed_at=observed_at,
            ):
                await session.rollback()
                return LeaseRenewal(renewed=False)
            lease_dt = _database_lease_deadline(
                lease_expires_at=lease_expires_at,
                lease_duration_seconds=lease_duration_seconds,
                observed_at=observed_at,
                required=True,
            )
            row.lease_expires_at = lease_dt
            row.updated_at = observed_at
            cancel_action = row.cancel_action
            await session.commit()
        return LeaseRenewal(
            renewed=True,
            cancel_action=cancel_action,
            lease_expires_at=coerce_iso(lease_dt),
        )

    async def execution_owner_authorized(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
    ) -> bool:
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            statement = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                await session.rollback()
                return False
            observed_at = await _database_now_after_lock(session)
            authorized = bool(
                row.status in {"pending", "running"}
                and row.owner_worker_id == owner_worker_id
                and row.state_version == state_version
                and (
                    row.lease_expires_at is None
                    or not _lease_expired_at(
                        row.lease_expires_at,
                        observed_at=observed_at,
                        grace_seconds=0,
                    )
                )
            )
            await session.rollback()
            return authorized

    async def request_cancel(self, run_id: str, *, action: str) -> str | None:
        result = await self.request_cancel_compat(run_id, action=action)
        if result.outcome in (
            CancellationRequestOutcome.requested,
            CancellationRequestOutcome.already_requested,
            CancellationRequestOutcome.stale,
        ):
            return result.row.get("cancel_action") if result.row is not None else None
        return None

    async def finalize_if_not_cancelled(
        self,
        run_id: str,
        *,
        status: str,
        error: str | None = None,
        stop_reason: str | None = None,
        expected_owner_worker_id: str | None = None,
        expected_state_version: int | None = None,
        require_unexpired_lease: bool = False,
    ) -> StatusFinalization:
        """Atomically let completion win only before cancellation."""
        current = await self.get(run_id, user_id=None)
        if current is None:
            return StatusFinalization(finalized=False)
        if current.get("cancel_action") is not None:
            return StatusFinalization(finalized=False, cancel_action=current["cancel_action"])
        if current["status"] not in ("pending", "running"):
            return StatusFinalization(finalized=False)
        if (expected_owner_worker_id is None) != (expected_state_version is None):
            return StatusFinalization(finalized=False)
        if expected_state_version is not None and current["state_version"] != expected_state_version:
            return StatusFinalization(finalized=False)
        lifecycle_type = lifecycle_type_for_status(status)
        transition = LifecycleTransition(
            lifecycle_type=lifecycle_type,
            status=status,
            error=error,
            stop_reason=stop_reason,
            reason=stop_reason,
        )
        if expected_owner_worker_id is not None:
            result = await self.transition_owned_run_atomic(
                run_id,
                expected_state_version=expected_state_version,
                expected_statuses=("pending", "running"),
                transition=transition,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            )
        else:
            result = await self.transition_run_atomic(
                run_id,
                expected_state_version=current["state_version"],
                expected_statuses=("pending", "running"),
                transition=transition,
            )
        if result.applied:
            return StatusFinalization(finalized=True)
        latest = await self.get(run_id, user_id=None)
        return StatusFinalization(
            finalized=False,
            cancel_action=latest.get("cancel_action") if latest else None,
        )

    async def claim_for_takeover(
        self,
        run_id: str,
        *,
        grace_seconds: int,
        error: str,
        stop_reason: str | None = None,
        expected_state_version: int | None = None,
    ) -> bool:
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            stmt = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.status.in_(("pending", "running")),
            )
            if session.get_bind().dialect.name == "postgresql":
                stmt = stmt.with_for_update()
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                await session.rollback()
                return False
            if expected_state_version is not None and row.state_version != expected_state_version:
                await session.rollback()
                return False
            if row.recovery_policy == RecoveryPolicy.exact_two_takeover_v1.value:
                await session.rollback()
                return False
            observed_at = await _database_now_after_lock(session)
            if not _lease_expired_at(
                row.lease_expires_at,
                observed_at=observed_at,
                grace_seconds=grace_seconds,
            ):
                await session.rollback()
                return False
            if row.operation_kind != "run":
                row.status = "error"
                row.error = error
                row.owner_worker_id = None
                row.lease_expires_at = None
                if stop_reason is not None:
                    row.stop_reason = stop_reason
                row.updated_at = observed_at
                await session.commit()
                return True
            cursor_state = await self._lock_cursor_state(session)
            row.terminal_projection_owner_worker_id = row.owner_worker_id
            row.terminal_projection_active_state_version = row.state_version if row.owner_worker_id is not None else None
            row.status = "error"
            row.state_version += 1
            row.error = error
            row.owner_worker_id = None
            row.lease_expires_at = None
            if stop_reason is not None:
                row.stop_reason = stop_reason
            row.updated_at = observed_at
            transition = LifecycleTransition(
                lifecycle_type=LifecycleType.failed,
                status="error",
                error=error,
                stop_reason=stop_reason,
                reason=stop_reason,
            )
            await self._append_lifecycle_event(session, cursor_state, row, transition)
            await session.commit()
            return True

    async def claim_for_execution_takeover(
        self,
        run_id: str,
        *,
        new_owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
        grace_seconds: int,
        expected_state_version: int,
    ) -> ExecutionTakeoverClaim:
        """Transfer one expired exact-two execution lease under a row CAS."""

        validate_lease_deadline_request(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            required=True,
        )
        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            stmt = self._scope_run(select(RunRow)).where(
                RunRow.run_id == run_id,
                RunRow.operation_kind == "run",
                RunRow.recovery_policy == RecoveryPolicy.exact_two_takeover_v1.value,
                RunRow.status.in_(("pending", "running")),
                RunRow.cancel_action.is_(None),
                RunRow.state_version == expected_state_version,
            )
            if session.get_bind().dialect.name == "postgresql":
                stmt = stmt.with_for_update()
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                observed = (
                    await session.execute(
                        self._scope_run(select(RunRow)).where(
                            RunRow.run_id == run_id,
                        )
                    )
                ).scalar_one_or_none()
                result_row = self._row_to_dict(observed) if observed is not None else None
                await session.rollback()
                return ExecutionTakeoverClaim(
                    ExecutionTakeoverOutcome.not_eligible,
                    result_row,
                )
            observed_at = await _database_now_after_lock(session)
            if not _lease_expired_at(
                row.lease_expires_at,
                observed_at=observed_at,
                grace_seconds=grace_seconds,
            ):
                result_row = self._row_to_dict(row)
                await session.rollback()
                return ExecutionTakeoverClaim(
                    ExecutionTakeoverOutcome.not_eligible,
                    result_row,
                )
            new_expiry = _database_lease_deadline(
                lease_expires_at=lease_expires_at,
                lease_duration_seconds=lease_duration_seconds,
                observed_at=observed_at,
                required=True,
            )
            row.owner_worker_id = new_owner_worker_id
            row.lease_expires_at = new_expiry
            row.state_version += 1
            row.updated_at = observed_at
            await session.flush()
            result_row = self._row_to_dict(row)
            await session.commit()
            return ExecutionTakeoverClaim(
                ExecutionTakeoverOutcome.claimed,
                result_row,
            )

    async def list_inflight_with_expired_lease(
        self,
        *,
        before: str | None = None,
        grace_seconds: int = 10,
    ) -> list[dict[str, Any]]:
        async with self._sf() as session:
            observed_at = await _database_now_after_lock(session)
            if before is None:
                before_dt = observed_at
            elif isinstance(before, _DATETIME_TYPE):
                before_dt = before
            else:
                before_dt = datetime.fromisoformat(before)
            cutoff = observed_at - timedelta(seconds=grace_seconds)
            stmt = (
                self._scope_run(select(RunRow))
                .where(
                    RunRow.status.in_(("pending", "running")),
                    RunRow.created_at <= before_dt,
                    _lease_expired_or_null(RunRow.lease_expires_at, cutoff),
                )
                .order_by(RunRow.created_at.asc())
            )
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def create_thread_operation_atomic(
        self,
        run_id: str,
        *,
        thread_id: str,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        created_at: str | None = None,
        grace_seconds: int = 10,
        origin_json: dict[str, Any] | None = None,
        principal_projection_json: dict[str, Any] | None = None,
        principal_projection_digest: str | None = None,
        base_origin_digest: str | None = None,
        accepted_context_digest: str | None = None,
        tenant: TenantReferenceV1 | None = None,
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
        caller_intent_json: dict[str, Any] | None = None,
        caller_intent_digest: str | None = None,
        caller_intent_digest_version: str | None = None,
        idempotency_key: str | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy.terminalize_v1,
        recovery_payload_json: dict[str, Any] | None = None,
        require_predecessor_inactive: bool = False,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Atomically create a run with cross-process thread-uniqueness.

        - For ``reject``: INSERT, let the partial unique index enforce
          single-active-run. Returns ``(row_dict, [])`` on success, raises
          ``IntegrityError`` on conflict.
        - For ``interrupt`` / ``rollback``: SELECT FOR UPDATE inflight
          rows for the thread, cancel them (unless their lease is still valid),
          then INSERT the new row — all in one transaction. Returns
          ``(row_dict, claimed_row_dicts)``.
        - With ``require_predecessor_inactive=True``: reject any active row
          under that same lock. Run and delivery-event stores have no shared
          replacement transaction yet, so callers requiring receipt evidence
          must explicitly cancel, observe terminal evidence, and retry.

        Exact-two rows always reject generic replacement regardless of either
        lease expiry or the compatibility flag.

        Returns:
            Tuple of ``(new_run_dict, claimed_run_dicts)``.
        """
        from deerflow.runtime.runs.manager import ConflictError

        thread_id = validate_thread_identifier(thread_id)
        tenant_ref, tenant_digest = tenant_store_columns(self._tenant, tenant)
        recovery_policy = RecoveryPolicy(recovery_policy)
        if operation_kind != "run" and recovery_policy is not RecoveryPolicy.terminalize_v1:
            raise ValueError("execution recovery policy applies only to normal runs")
        if (recovery_policy is RecoveryPolicy.exact_two_takeover_v1) != (recovery_payload_json is not None):
            raise ValueError("execution recovery policy and payload must be admitted together")
        validate_lease_deadline_request(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            required=False,
        )
        resolved_user_id = resolve_user_id(user_id or AUTO, method_name="RunRepository.create_thread_operation_atomic")
        now = datetime.now(UTC)
        if lease_duration_seconds is not None:
            created = now
        elif created_at:
            created = datetime.fromisoformat(created_at)
        else:
            created = now

        values = {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": resolved_user_id,
            "model_name": self._normalize_model_name(model_name),
            "status": "pending",
            "operation_kind": operation_kind,
            "recovery_policy": recovery_policy.value,
            "recovery_payload_json": (self._safe_json(recovery_payload_json) if operation_kind == "run" else None),
            "multitask_strategy": multitask_strategy,
            "metadata_json": self._safe_json(metadata) or {},
            "kwargs_json": self._safe_json(kwargs) or {},
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": None,
            "idempotency_key": idempotency_key,
            "created_at": created,
            "updated_at": now,
            "tenant_ref": tenant_ref,
            "tenant_digest": tenant_digest,
            "origin_json": self._safe_json(origin_json) if operation_kind == "run" else None,
            "principal_projection_json": self._safe_json(principal_projection_json) if operation_kind == "run" else None,
            "principal_projection_digest": principal_projection_digest if operation_kind == "run" else None,
            "base_origin_digest": base_origin_digest if operation_kind == "run" else None,
            "accepted_context_digest": accepted_context_digest if operation_kind == "run" else None,
            "agent_revision_json": self._safe_json(agent_revision_json) if operation_kind == "run" else None,
            "agent_revision_digest": agent_revision_digest if operation_kind == "run" else None,
            "extension_generation": extension_generation if operation_kind == "run" else None,
            "decision_evidence_json": self._safe_json(decision_evidence_json) if operation_kind == "run" else None,
            "external_scope": external_scope if operation_kind == "run" else None,
            "external_key": external_key if operation_kind == "run" else None,
            "request_digest": request_digest if operation_kind == "run" else None,
            "request_digest_version": request_digest_version if operation_kind == "run" else None,
            "caller_intent_json": self._safe_json(caller_intent_json) if operation_kind == "run" else None,
            "caller_intent_digest": caller_intent_digest if operation_kind == "run" else None,
            "caller_intent_digest_version": caller_intent_digest_version if operation_kind == "run" else None,
            "state_version": 1 if operation_kind == "run" else 0,
        }

        async with self._sf() as session:
            await self._begin_lifecycle_write(session)
            if await session.get(RunRow, run_id) is not None:
                raise DuplicateRunIdentityError(run_id)
            claimed: list[dict[str, Any]] = []
            cursor_state: RunLifecycleCursorStateRow | None = None
            inflight_rows: list[RunRow] = []

            if multitask_strategy in ("interrupt", "rollback"):
                stmt = (
                    self._scope_run(select(RunRow))
                    .where(
                        RunRow.thread_id == thread_id,
                        RunRow.status.in_(("pending", "running")),
                    )
                    .order_by(RunRow.created_at.asc(), RunRow.run_id.asc())
                    .with_for_update()
                )
                result = await session.execute(stmt)
                inflight_rows = list(result.scalars())

            admission_cursor = await self._next_admission_cursor(session)
            if operation_kind == "run":
                cursor_state = await self._lock_cursor_state(session)
            database_now = await _database_now_after_lock(session)
            values["lease_expires_at"] = _database_lease_deadline(
                lease_expires_at=lease_expires_at,
                lease_duration_seconds=lease_duration_seconds,
                observed_at=database_now,
                required=False,
            )
            if lease_duration_seconds is not None:
                # A DB-minted lease and its admission timestamps share one
                # clock domain. In particular, a fast pod clock cannot create
                # a future row that startup recovery omits from its scan.
                values["created_at"] = database_now
            values["updated_at"] = database_now

            if multitask_strategy in ("interrupt", "rollback"):
                cutoff = database_now - timedelta(seconds=grace_seconds)
                for row in inflight_rows:
                    if row.recovery_policy == RecoveryPolicy.exact_two_takeover_v1.value:
                        # Generic replacement is not an execution-takeover
                        # capability. Keep exact-two rows unchanged until the
                        # separately qualified takeover primitive is enabled.
                        raise ConflictError(
                            f"Thread {thread_id} has an active exact-two run",
                            active_run_id=row.run_id,
                        )
                    if require_predecessor_inactive:
                        raise ConflictError(
                            f"Thread {thread_id} requires explicit predecessor cancellation",
                            active_run_id=row.run_id,
                        )
                    lease_expired = False
                    if row.lease_expires_at is not None:
                        # SQLite drops tzinfo on read despite
                        # ``DateTime(timezone=True)`` (see ``_row_to_dict``).
                        # Treat naive values as UTC — same convention as
                        # ``coerce_iso`` — so the Python-side comparison
                        # against the aware ``cutoff`` does not raise
                        # ``TypeError: can't compare offset-naive and
                        # offset-aware datetimes`` when heartbeat is enabled
                        # on SQLite.
                        row_lease = row.lease_expires_at
                        if row_lease.tzinfo is None:
                            row_lease = row_lease.replace(tzinfo=UTC)
                        lease_expired = row_lease <= cutoff
                        if row_lease > cutoff and row.owner_worker_id != owner_worker_id:
                            # Live run owned by another worker — we cannot
                            # interrupt it and the partial unique index would
                            # reject our INSERT anyway. Surface as
                            # ConflictError so the caller gets a clean signal
                            # instead of a retry loop on IntegrityError.
                            raise ConflictError(f"Thread {thread_id} already has an active run owned by another worker")
                    if row.operation_kind != "run" and not lease_expired:
                        raise ConflictError(f"Thread {thread_id} has an active checkpoint write")
                    replacement_status = "error" if multitask_strategy == "rollback" else "interrupted"
                    replacement_error = "Rolled back by user" if multitask_strategy == "rollback" else "Cancelled by newer run"
                    if row.operation_kind == "run":
                        active_state_version = row.state_version
                        row.terminal_projection_owner_worker_id = row.owner_worker_id
                        row.terminal_projection_active_state_version = active_state_version if row.owner_worker_id is not None else None
                    else:
                        active_state_version = None
                        row.terminal_projection_owner_worker_id = None
                        row.terminal_projection_active_state_version = None
                    row.status = replacement_status
                    row.error = replacement_error
                    row.owner_worker_id = None
                    row.lease_expires_at = None
                    row.updated_at = database_now
                    if row.operation_kind == "run":
                        # Make the terminal authority tuple constraint-valid
                        # before locking the cursor: the SELECT below may
                        # autoflush this row.
                        assert active_state_version is not None
                        row.state_version = active_state_version + 1
                        if cursor_state is None:
                            cursor_state = await self._lock_cursor_state(session)
                        await self._append_lifecycle_event(
                            session,
                            cursor_state,
                            row,
                            LifecycleTransition(
                                lifecycle_type=LifecycleType.interrupted,
                                status=replacement_status,
                                error=replacement_error,
                                reason="rollback" if multitask_strategy == "rollback" else "replacement",
                            ),
                        )
                    claimed.append(self._row_to_dict(row))

            new_run = RunRow(
                run_id=run_id,
                admission_cursor=admission_cursor,
                **values,
            )
            session.add(new_run)
            try:
                if operation_kind == "run":
                    await session.flush()
                    if cursor_state is None:
                        cursor_state = await self._lock_cursor_state(session)
                    await self._append_lifecycle_event(
                        session,
                        cursor_state,
                        new_run,
                        LifecycleTransition(lifecycle_type=LifecycleType.accepted, status="pending"),
                    )
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                if _is_run_primary_key_violation(exc):
                    raise DuplicateRunIdentityError(run_id) from None
                if idempotency_key is not None:
                    existing = (
                        await session.execute(
                            self._scope_run(select(RunRow)).where(
                                RunRow.idempotency_key == idempotency_key,
                            )
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        raise RunIdempotencyConflict(self._row_to_dict(existing)) from exc
                raise

            new_row = (await session.execute(self._scope_run(select(RunRow)).where(RunRow.run_id == run_id))).scalar_one()
            return self._row_to_dict(new_row), claimed

    async def ensure_run_atomic(
        self,
        run_id: str,
        *,
        external_scope: str,
        external_key: str,
        request_digest: str,
        request_digest_version: str,
        caller_intent_json: dict[str, Any],
        caller_intent_digest: str,
        caller_intent_digest_version: str,
        **kwargs: Any,
    ) -> RunEnsureResult:
        existing = await self.get_by_external_identity(external_scope, external_key)
        if existing is not None:
            same_intent = existing.get("caller_intent_json") == caller_intent_json and existing.get("caller_intent_digest") == caller_intent_digest and existing.get("caller_intent_digest_version") == caller_intent_digest_version
            outcome = AdmissionOutcome.known_same if same_intent else AdmissionOutcome.key_conflict
            return RunEnsureResult(outcome=outcome, row=existing)

        try:
            row, claimed = await self.create_thread_operation_atomic(
                run_id,
                operation_kind="run",
                external_scope=external_scope,
                external_key=external_key,
                request_digest=request_digest,
                request_digest_version=request_digest_version,
                caller_intent_json=caller_intent_json,
                caller_intent_digest=caller_intent_digest,
                caller_intent_digest_version=caller_intent_digest_version,
                **kwargs,
            )
        except DuplicateRunIdentityError:
            raise
        except IntegrityError:
            # The external-identity partial index is the race arbiter. If it
            # did not win the conflict, preserve the independent thread-busy
            # error by re-raising the original integrity failure.
            existing = await self.get_by_external_identity(external_scope, external_key)
            if existing is None:
                raise
            same_intent = existing.get("caller_intent_json") == caller_intent_json and existing.get("caller_intent_digest") == caller_intent_digest and existing.get("caller_intent_digest_version") == caller_intent_digest_version
            outcome = AdmissionOutcome.known_same if same_intent else AdmissionOutcome.key_conflict
            return RunEnsureResult(outcome=outcome, row=existing)
        return RunEnsureResult(outcome=AdmissionOutcome.created, row=row, claimed=tuple(claimed))

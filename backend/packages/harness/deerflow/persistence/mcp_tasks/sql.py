from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from deerflow_extension_api import TenantReferenceV1
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.constants import MCP_TASK_CANCEL_ACTOR_REF_LENGTH, MCP_TASK_CANCEL_REASON_CODES
from deerflow.mcp.tasks import ATTENTION_TASK_STATUSES, POLLABLE_TASK_STATUSES, TERMINAL_TASK_STATUSES
from deerflow.mcp.tasks.lineage import McpTaskLineageError, McpTaskLineageV1
from deerflow.persistence.mcp_tasks.model import McpTaskRow
from deerflow.utils.time import coerce_iso

_POLLABLE_STATUS_VALUES = tuple(status.value for status in POLLABLE_TASK_STATUSES)
_ATTENTION_STATUS_VALUES = frozenset(status.value for status in ATTENTION_TASK_STATUSES)
_TERMINAL_STATUS_VALUES = frozenset(status.value for status in TERMINAL_TASK_STATUSES)
_TIMESTAMP_FIELDS = (
    "next_poll_at",
    "last_polled_at",
    "lease_expires_at",
    "notification_lease_expires_at",
    "next_notification_at",
    "cancel_requested_at",
    "next_cancel_at",
    "completed_at",
    "created_at",
    "updated_at",
)

_INFLIGHT_NOTIFICATION_STATUSES = frozenset({"claimed", "dispatched", "retry"})
MCP_TASK_SCHEMA_WRITER_VERSION = 2
_MCP_TASK_CURSOR_VERSION = "deerflow.mcp-task-lineage.cursor/v1"
MAX_MCP_TASK_LINEAGE_PAGE_SIZE = 100


def _notification_event(row: McpTaskRow, *, tracking_degraded: bool) -> dict[str, Any] | None:
    if row.status not in _ATTENTION_STATUS_VALUES and not tracking_degraded:
        return None
    return {
        "task_id": row.id,
        "task_name": row.task_name,
        "status": row.status,
        "result": row.result,
        "result_preview": row.result_preview,
        "result_truncated": bool(row.result_truncated),
        "result_artifact": row.result_artifact,
        "error": row.error,
        "input_required": row.input_required,
        "tracking_degraded": tracking_degraded,
        "last_poll_error": row.last_poll_error if tracking_degraded else None,
    }


def _event_fingerprint(event: dict[str, Any]) -> str:
    encoded = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _record_event_if_changed(row: McpTaskRow, *, tracking_degraded: bool, now: datetime) -> bool:
    event = _notification_event(row, tracking_degraded=tracking_degraded)
    if event is None:
        return False
    fingerprint = _event_fingerprint(event)
    if fingerprint == row.event_fingerprint:
        return False
    row.event_fingerprint = fingerprint
    row.event_version = int(row.event_version or 0) + 1
    if row.notification_status not in _INFLIGHT_NOTIFICATION_STATUSES:
        row.notification_status = "pending"
        row.next_notification_at = now
        row.notification_error = None
        row.notification_attempt_count = 0
        row.dispatch_version = None
        row.dispatch_attempt = 0
        row.dispatch_event = None
    return True


class DuplicateMcpRemoteTaskError(RuntimeError):
    """The current user already tracks this server's remote task handle."""


class DuplicateMcpTaskLineageError(RuntimeError):
    """The tenant already tracks a task for this immutable lineage."""


class DuplicateMcpTaskIdError(RuntimeError):
    """A deterministic local task identifier was inserted concurrently."""


class McpTaskRepositoryError(RuntimeError):
    """A safe, stable repository boundary failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _is_remote_task_unique_conflict(exc: IntegrityError) -> bool:
    original = exc.orig
    diagnostic = getattr(original, "diag", None)
    if getattr(diagnostic, "constraint_name", None) == "uq_mcp_tasks_tenant_user_server_remote":
        return True
    message = str(original)
    return "uq_mcp_tasks_tenant_user_server_remote" in message or "mcp_tasks.tenant_digest, mcp_tasks.user_id, mcp_tasks.server_name, mcp_tasks.remote_task_id" in message


def _is_lineage_unique_conflict(exc: IntegrityError) -> bool:
    original = exc.orig
    diagnostic = getattr(original, "diag", None)
    if getattr(diagnostic, "constraint_name", None) == "uq_mcp_tasks_tenant_lineage":
        return True
    message = str(original)
    return "uq_mcp_tasks_tenant_lineage" in message or "mcp_tasks.tenant_digest, mcp_tasks.lineage_digest" in message


def _is_task_id_unique_conflict(exc: IntegrityError) -> bool:
    original = exc.orig
    diagnostic = getattr(original, "diag", None)
    if getattr(diagnostic, "constraint_name", None) == "mcp_tasks_pkey":
        return True
    message = str(original)
    return "mcp_tasks_pkey" in message or "UNIQUE constraint failed: mcp_tasks.id" in message


class McpTaskRepository:
    """Durable source of truth for long-running MCP task lifecycle state."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tenant: TenantReferenceV1,
    ) -> None:
        if not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        self._sf = session_factory
        self._tenant = tenant

    @property
    def tenant(self) -> TenantReferenceV1:
        """Return the immutable tenant scope for every repository operation."""

        return self._tenant

    async def verify_schema_writer_compatibility(self) -> None:
        """Refuse mutation when a newer binary has written task rows."""

        async with self._sf() as session:
            maximum = (await session.execute(select(func.max(McpTaskRow.schema_writer_version)))).scalar_one_or_none()
        if maximum is not None and int(maximum) > MCP_TASK_SCHEMA_WRITER_VERSION:
            raise McpTaskRepositoryError("mcp_task_schema_writer_unsupported")

    def _tenant_digest(self, value: str) -> str:
        if not isinstance(value, str) or value != self._tenant.digest:
            raise McpTaskRepositoryError("mcp_task_tenant_mismatch")
        return value

    def _tenant_scope(self, value: str):
        digest = self._tenant_digest(value)
        # Rows written before Project 05 have no tenant columns. Project 04's
        # schema binding permits only this process tenant to finish those rows;
        # no repository bound to another tenant can see them.
        return or_(
            McpTaskRow.tenant_digest == digest,
            and_(
                McpTaskRow.tenant_digest.is_(None),
                McpTaskRow.tenant_ref.is_(None),
            ),
        )

    def _row_to_dict(self, row: McpTaskRow) -> dict[str, Any]:
        writer_version = int(row.schema_writer_version or 1)
        if writer_version > MCP_TASK_SCHEMA_WRITER_VERSION:
            raise McpTaskRepositoryError("mcp_task_schema_writer_unsupported")
        lineage: McpTaskLineageV1 | None = None
        if row.lineage_json is None and row.lineage_digest is None:
            if writer_version >= MCP_TASK_SCHEMA_WRITER_VERSION:
                raise McpTaskRepositoryError("mcp_task_lineage_invalid")
            lineage_status = "legacy_unavailable"
        elif row.lineage_json is None or row.lineage_digest is None:
            raise McpTaskRepositoryError("mcp_task_lineage_invalid")
        else:
            try:
                lineage = McpTaskLineageV1.from_persisted_json(row.lineage_json)
            except McpTaskLineageError as exc:
                raise McpTaskRepositoryError("mcp_task_lineage_invalid") from exc
            if (
                lineage.digest != row.lineage_digest
                or lineage.tenant.public_ref != row.tenant_ref
                or lineage.tenant.digest != row.tenant_digest
                or lineage.parent_run_id != row.parent_run_id
                or lineage.parent_tool_receipt_id != row.parent_tool_receipt_id
                or lineage.mcp_server_name != row.server_name
            ):
                raise McpTaskRepositoryError("mcp_task_lineage_invalid")
            lineage_status = "verified"
        data = row.to_dict()
        for key in _TIMESTAMP_FIELDS:
            if data.get(key) is not None:
                data[key] = coerce_iso(data[key])
        data["lineage_status"] = lineage_status
        data["lineage"] = None if lineage is None else lineage.to_persisted_json()
        return data

    async def create(
        self,
        *,
        task_id: str,
        user_id: str,
        thread_id: str,
        lineage: McpTaskLineageV1,
        tenant_digest: str,
        driver_name: str,
        remote_task_id: str,
        task_name: str,
        status: str,
        result: Any | None,
        result_preview: str | None,
        result_truncated: bool,
        result_artifact: dict[str, str] | None,
        error: str | None,
        input_required: dict[str, Any] | None,
        next_poll_at: datetime | None,
        driver_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._tenant_digest(tenant_digest)
        if not isinstance(lineage, McpTaskLineageV1):
            raise TypeError("lineage must be McpTaskLineageV1")
        if lineage.tenant != self._tenant:
            raise McpTaskRepositoryError("mcp_task_tenant_mismatch")
        now = datetime.now(UTC)
        row = McpTaskRow(
            id=task_id,
            schema_writer_version=MCP_TASK_SCHEMA_WRITER_VERSION,
            tenant_ref=lineage.tenant.public_ref,
            tenant_digest=lineage.tenant.digest,
            lineage_json=lineage.to_persisted_json(),
            lineage_digest=lineage.digest,
            parent_run_id=lineage.parent_run_id,
            parent_tool_receipt_id=lineage.parent_tool_receipt_id,
            user_id=user_id,
            thread_id=thread_id,
            run_id=lineage.parent_run_id,
            tool_call_id=lineage.parent_tool_receipt_id,
            server_name=lineage.mcp_server_name,
            driver_name=driver_name,
            remote_task_id=remote_task_id,
            task_name=task_name,
            status=status,
            result=result,
            result_preview=result_preview,
            result_truncated=result_truncated,
            result_artifact=result_artifact,
            error=error,
            input_required=input_required,
            driver_data=dict(driver_data or {}),
            notification_status="none",
            next_poll_at=next_poll_at,
            completed_at=now if status in _TERMINAL_STATUS_VALUES else None,
            created_at=now,
            updated_at=now,
        )
        _record_event_if_changed(row, tracking_degraded=False, now=now)
        async with self._sf() as session:
            session.add(row)
            try:
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                if _is_lineage_unique_conflict(exc):
                    raise DuplicateMcpTaskLineageError("MCP task lineage is already tracked for this tenant") from exc
                if _is_remote_task_unique_conflict(exc):
                    raise DuplicateMcpRemoteTaskError("Remote MCP task is already tracked for this tenant, server, and principal") from exc
                if _is_task_id_unique_conflict(exc):
                    raise DuplicateMcpTaskIdError("MCP task identifier is already tracked") from exc
                raise
            await session.refresh(row)
            return self._row_to_dict(row)

    async def get(
        self,
        task_id: str,
        *,
        user_id: str,
        tenant_digest: str,
    ) -> dict[str, Any] | None:
        async with self._sf() as session:
            row = (
                await session.execute(
                    select(McpTaskRow).where(
                        McpTaskRow.id == task_id,
                        McpTaskRow.user_id == user_id,
                        self._tenant_scope(tenant_digest),
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return self._row_to_dict(row)

    async def get_by_lineage_digest(
        self,
        lineage_digest: str,
        *,
        user_id: str,
        tenant_digest: str,
    ) -> dict[str, Any] | None:
        """Find an owner-visible task by its immutable lineage commitment."""

        stmt = select(McpTaskRow).where(
            McpTaskRow.lineage_digest == lineage_digest,
            McpTaskRow.user_id == user_id,
            McpTaskRow.tenant_digest == self._tenant_digest(tenant_digest),
        )
        async with self._sf() as session:
            row = (await session.execute(stmt)).scalar_one_or_none()
            return None if row is None else self._row_to_dict(row)

    async def list_by_thread(
        self,
        thread_id: str,
        *,
        user_id: str,
        limit: int = 50,
        active_only: bool = False,
        tenant_digest: str,
    ) -> list[dict[str, Any]]:
        stmt = select(McpTaskRow).where(
            McpTaskRow.thread_id == thread_id,
            McpTaskRow.user_id == user_id,
            self._tenant_scope(tenant_digest),
        )
        if active_only:
            stmt = stmt.where(McpTaskRow.status.in_(_POLLABLE_STATUS_VALUES))
        stmt = stmt.order_by(McpTaskRow.created_at.desc(), McpTaskRow.id.desc()).limit(limit)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(row) for row in result.scalars()]

    def _parent_cursor_scope(
        self,
        *,
        parent_run_id: str,
        user_id: str,
    ) -> str:
        return hashlib.sha256((_MCP_TASK_CURSOR_VERSION + "\0" + self._tenant.digest + "\0" + parent_run_id + "\0" + user_id).encode("utf-8")).hexdigest()

    def _encode_parent_cursor(
        self,
        *,
        parent_run_id: str,
        user_id: str,
        created_at: datetime,
        task_id: str,
    ) -> str:
        core = {
            "version": _MCP_TASK_CURSOR_VERSION,
            "scope": self._parent_cursor_scope(
                parent_run_id=parent_run_id,
                user_id=user_id,
            ),
            "created_at": created_at.isoformat(),
            "task_id": task_id,
        }
        checksum = hashlib.sha256(json.dumps(core, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        encoded = base64.urlsafe_b64encode(
            json.dumps(
                {**core, "checksum": checksum},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).rstrip(b"=")
        return "mtc1." + encoded.decode("ascii")

    def _decode_parent_cursor(
        self,
        cursor: str,
        *,
        parent_run_id: str,
        user_id: str,
    ) -> tuple[datetime, str]:
        if not isinstance(cursor, str) or len(cursor.encode("utf-8")) > 4096 or not cursor.startswith("mtc1."):
            raise McpTaskRepositoryError("mcp_task_cursor_invalid")
        try:
            encoded = cursor[5:]
            payload = json.loads(
                base64.b64decode(
                    encoded + "=" * (-len(encoded) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            )
            expected = {
                "version",
                "scope",
                "created_at",
                "task_id",
                "checksum",
            }
            if not isinstance(payload, dict) or set(payload) != expected:
                raise ValueError
            core = {key: payload[key] for key in expected - {"checksum"}}
            checksum = hashlib.sha256(
                json.dumps(
                    core,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if (
                payload["version"] != _MCP_TASK_CURSOR_VERSION
                or payload["scope"]
                != self._parent_cursor_scope(
                    parent_run_id=parent_run_id,
                    user_id=user_id,
                )
                or payload["checksum"] != checksum
                or not isinstance(payload["task_id"], str)
                or not payload["task_id"]
            ):
                raise ValueError
            created_at = datetime.fromisoformat(payload["created_at"])
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            return created_at, payload["task_id"]
        except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise McpTaskRepositoryError("mcp_task_cursor_invalid") from exc

    async def list_by_parent_run(
        self,
        parent_run_id: str,
        *,
        user_id: str,
        limit: int = 50,
        cursor: str | None = None,
        tenant_digest: str,
    ) -> dict[str, Any]:
        """Return one bounded, indexed child projection for an authorized run."""

        if type(limit) is not int or not 1 <= limit <= MAX_MCP_TASK_LINEAGE_PAGE_SIZE:
            raise ValueError("MCP task lineage limit must be between 1 and 100")
        stmt = select(McpTaskRow).where(
            McpTaskRow.tenant_digest == self._tenant_digest(tenant_digest),
            McpTaskRow.parent_run_id == parent_run_id,
            McpTaskRow.user_id == user_id,
            McpTaskRow.lineage_digest.is_not(None),
        )
        if cursor is not None:
            # The cursor is a bounded position token, never an authorization
            # credential. Tenant, owner, and parent visibility remain enforced
            # by the SQL predicates above even if a caller alters its position.
            created_at, task_id = self._decode_parent_cursor(
                cursor,
                parent_run_id=parent_run_id,
                user_id=user_id,
            )
            stmt = stmt.where(
                or_(
                    McpTaskRow.created_at < created_at,
                    and_(
                        McpTaskRow.created_at == created_at,
                        McpTaskRow.id < task_id,
                    ),
                )
            )
        stmt = stmt.order_by(
            McpTaskRow.created_at.desc(),
            McpTaskRow.id.desc(),
        ).limit(limit + 1)
        async with self._sf() as session:
            rows = list((await session.execute(stmt)).scalars())
        has_more = len(rows) > limit
        rows = rows[:limit]
        items: list[dict[str, Any]] = []
        for row in rows:
            record = self._row_to_dict(row)
            lineage = record.get("lineage")
            if not isinstance(lineage, dict):
                continue
            status = str(record.get("status") or "")
            terminal_code = "remote_failed" if status == "failed" else ("cancelled" if status == "cancelled" else None)
            items.append(
                {
                    "task_id": record["id"],
                    "lineage_digest": lineage["digest"],
                    "submitting_task_id": lineage["parent_execution_task_id"],
                    "receipt_id": lineage["parent_tool_receipt_id"],
                    "server_name": lineage["mcp_server_name"],
                    "tool_name": lineage["mcp_tool_name"],
                    "status": status,
                    "safe_terminal_code": terminal_code,
                    "notification_run_id": record.get("notification_run_id"),
                    "created_at": record["created_at"],
                    "updated_at": record["updated_at"],
                    "completed_at": record.get("completed_at"),
                }
            )
        next_cursor = None
        if has_more and rows:
            tail = rows[-1]
            next_cursor = self._encode_parent_cursor(
                parent_run_id=parent_run_id,
                user_id=user_id,
                created_at=tail.created_at,
                task_id=tail.id,
            )
        return {
            "items": items,
            "next_cursor": next_cursor,
            "pruning_status": "not_pruned",
        }

    async def claim_due_tasks(
        self,
        *,
        now: datetime,
        lease_owner: str,
        lease_seconds: int,
        limit: int,
        tenant_digest: str,
    ) -> list[dict[str, Any]]:
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        stmt = (
            select(McpTaskRow)
            .where(
                self._tenant_scope(tenant_digest),
                McpTaskRow.status.in_(_POLLABLE_STATUS_VALUES),
                McpTaskRow.cancel_requested_at.is_(None),
                McpTaskRow.next_poll_at.is_not(None),
                McpTaskRow.next_poll_at <= now,
                or_(
                    McpTaskRow.lease_expires_at.is_(None),
                    McpTaskRow.lease_expires_at < now,
                ),
            )
            .order_by(McpTaskRow.next_poll_at.asc(), McpTaskRow.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            rows = list(result.scalars())
            for row in rows:
                row.lease_owner = lease_owner
                row.lease_expires_at = lease_expires_at
                row.poll_attempt_count += 1
                row.updated_at = now
            await session.commit()
            return [self._row_to_dict(row) for row in rows]

    async def apply_snapshot(
        self,
        task_id: str,
        *,
        lease_owner: str,
        status: str,
        result: Any | None,
        result_preview: str | None,
        result_truncated: bool,
        result_artifact: dict[str, str] | None,
        error: str | None,
        input_required: dict[str, Any] | None,
        next_poll_at: datetime | None,
        polled_at: datetime,
        tenant_digest: str,
    ) -> bool:
        async with self._sf() as session:
            stmt = (
                select(McpTaskRow)
                .where(
                    McpTaskRow.id == task_id,
                    self._tenant_scope(tenant_digest),
                    McpTaskRow.lease_owner == lease_owner,
                    McpTaskRow.lease_expires_at >= polled_at,
                    McpTaskRow.status.not_in(_TERMINAL_STATUS_VALUES),
                    McpTaskRow.cancel_requested_at.is_(None),
                )
                .with_for_update()
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return False
            row.status = status
            row.result = result
            row.result_preview = result_preview
            row.result_truncated = result_truncated
            row.result_artifact = result_artifact
            row.error = error
            row.input_required = input_required
            row.next_poll_at = next_poll_at
            row.last_polled_at = polled_at
            row.last_poll_error = None
            row.consecutive_poll_error_count = 0
            row.lease_owner = None
            row.lease_expires_at = None
            row.updated_at = polled_at
            if status in _TERMINAL_STATUS_VALUES:
                row.completed_at = polled_at
            _record_event_if_changed(row, tracking_degraded=False, now=polled_at)
            await session.commit()
            return True

    async def release_claim(
        self,
        task_id: str,
        *,
        lease_owner: str,
        next_poll_at: datetime,
        error: str,
        tracking_degraded_after_errors: int = 3,
        tenant_digest: str,
    ) -> bool:
        async with self._sf() as session:
            stmt = (
                select(McpTaskRow)
                .where(
                    McpTaskRow.id == task_id,
                    McpTaskRow.lease_owner == lease_owner,
                    self._tenant_scope(tenant_digest),
                )
                .with_for_update()
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return False
            now = datetime.now(UTC)
            row.next_poll_at = next_poll_at
            row.last_poll_error = error
            row.consecutive_poll_error_count = int(row.consecutive_poll_error_count or 0) + 1
            row.lease_owner = None
            row.lease_expires_at = None
            row.updated_at = now
            _record_event_if_changed(
                row,
                tracking_degraded=row.consecutive_poll_error_count >= tracking_degraded_after_errors,
                now=now,
            )
            await session.commit()
            return True

    async def request_cancel(
        self,
        task_id: str,
        *,
        user_id: str,
        thread_id: str,
        requested_at: datetime,
        actor_ref: str,
        reason_code: str,
        tenant_digest: str,
    ) -> dict[str, Any] | None:
        """Persist a user-scoped cancellation request without exposing the remote id."""
        self._tenant_digest(tenant_digest)
        if (
            not isinstance(actor_ref, str)
            or len(actor_ref) != MCP_TASK_CANCEL_ACTOR_REF_LENGTH
            or any(character not in "0123456789abcdef" for character in actor_ref)
            or not isinstance(reason_code, str)
            or reason_code not in MCP_TASK_CANCEL_REASON_CODES
        ):
            raise McpTaskRepositoryError("mcp_task_cancel_intent_invalid")
        async with self._sf() as session:
            stmt = (
                select(McpTaskRow)
                .where(
                    McpTaskRow.id == task_id,
                    McpTaskRow.user_id == user_id,
                    McpTaskRow.thread_id == thread_id,
                    self._tenant_scope(tenant_digest),
                )
                .with_for_update()
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return None
            if row.status not in _TERMINAL_STATUS_VALUES and row.cancel_requested_at is None:
                row.cancel_requested_at = requested_at
                row.cancel_actor_ref = actor_ref
                row.cancel_reason_code = reason_code
                if row.next_cancel_at is None:
                    row.next_cancel_at = requested_at
                # A cancel request fences any in-flight poll result, so its
                # poll lease can be released immediately for the cancellation
                # worker. A repeated request must preserve an existing cancel
                # lease so it cannot trigger a concurrent remote cancellation.
                row.lease_owner = None
                row.lease_expires_at = None
                row.updated_at = requested_at
                await session.commit()
            return self._row_to_dict(row)

    async def claim_cancel_requests(
        self,
        *,
        now: datetime,
        lease_owner: str,
        lease_seconds: int,
        limit: int,
        task_id: str | None = None,
        tenant_digest: str,
    ) -> list[dict[str, Any]]:
        stmt = select(McpTaskRow).where(
            self._tenant_scope(tenant_digest),
            McpTaskRow.cancel_requested_at.is_not(None),
            McpTaskRow.status.not_in(_TERMINAL_STATUS_VALUES),
            McpTaskRow.next_cancel_at.is_not(None),
            McpTaskRow.next_cancel_at <= now,
            or_(McpTaskRow.lease_expires_at.is_(None), McpTaskRow.lease_expires_at < now),
        )
        if task_id is not None:
            stmt = stmt.where(McpTaskRow.id == task_id)
        stmt = stmt.order_by(McpTaskRow.next_cancel_at.asc(), McpTaskRow.id.asc()).limit(limit).with_for_update(skip_locked=True)
        async with self._sf() as session:
            rows = list((await session.execute(stmt)).scalars())
            expires_at = now + timedelta(seconds=lease_seconds)
            for row in rows:
                row.lease_owner = lease_owner
                row.lease_expires_at = expires_at
                row.cancel_attempt_count = int(row.cancel_attempt_count or 0) + 1
                row.updated_at = now
            await session.commit()
            return [self._row_to_dict(row) for row in rows]

    async def apply_cancel_snapshot(
        self,
        task_id: str,
        *,
        lease_owner: str,
        status: str,
        result: Any | None,
        result_preview: str | None,
        result_truncated: bool,
        result_artifact: dict[str, str] | None,
        error: str | None,
        input_required: dict[str, Any] | None,
        completed_at: datetime,
        tenant_digest: str,
    ) -> bool:
        if status not in _TERMINAL_STATUS_VALUES:
            raise ValueError("A cancellation response must report a terminal task status")
        async with self._sf() as session:
            stmt = (
                select(McpTaskRow)
                .where(
                    McpTaskRow.id == task_id,
                    self._tenant_scope(tenant_digest),
                    McpTaskRow.lease_owner == lease_owner,
                    McpTaskRow.lease_expires_at >= completed_at,
                    McpTaskRow.status.not_in(_TERMINAL_STATUS_VALUES),
                )
                .with_for_update()
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return False
            row.status = status
            row.result = result
            row.result_preview = result_preview
            row.result_truncated = result_truncated
            row.result_artifact = result_artifact
            row.error = error
            row.input_required = input_required
            row.next_poll_at = None
            row.next_cancel_at = None
            row.last_cancel_error = None
            row.lease_owner = None
            row.lease_expires_at = None
            row.completed_at = completed_at
            row.updated_at = completed_at
            _record_event_if_changed(row, tracking_degraded=False, now=completed_at)
            await session.commit()
            return True

    async def release_cancel_claim(
        self,
        task_id: str,
        *,
        lease_owner: str,
        next_cancel_at: datetime,
        error: str,
        tenant_digest: str,
    ) -> bool:
        stmt = (
            update(McpTaskRow)
            .where(
                McpTaskRow.id == task_id,
                McpTaskRow.lease_owner == lease_owner,
                self._tenant_scope(tenant_digest),
            )
            .values(
                next_cancel_at=next_cancel_at,
                last_cancel_error=error,
                lease_owner=None,
                lease_expires_at=None,
                updated_at=datetime.now(UTC),
            )
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            await session.commit()
            return bool(result.rowcount)

    async def claim_notification_work(
        self,
        *,
        now: datetime,
        lease_owner: str,
        lease_seconds: int,
        limit: int,
        tracking_degraded_after_errors: int,
        tenant_digest: str,
    ) -> list[dict[str, Any]]:
        statuses = ("pending", "claimed", "retry", "dispatched")
        stmt = (
            select(McpTaskRow)
            .where(
                self._tenant_scope(tenant_digest),
                McpTaskRow.event_version > McpTaskRow.notified_version,
                McpTaskRow.notification_status.in_(statuses),
                or_(McpTaskRow.next_notification_at.is_(None), McpTaskRow.next_notification_at <= now),
                or_(McpTaskRow.notification_lease_expires_at.is_(None), McpTaskRow.notification_lease_expires_at < now),
            )
            .order_by(McpTaskRow.next_notification_at.asc(), McpTaskRow.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        async with self._sf() as session:
            rows = list((await session.execute(stmt)).scalars())
            expires_at = now + timedelta(seconds=lease_seconds)
            for row in rows:
                row.notification_lease_owner = lease_owner
                row.notification_lease_expires_at = expires_at
                rebuild_snapshot = row.notification_status in ("pending", "claimed") or (row.notification_status == "retry" and row.dispatch_version != row.event_version)
                if rebuild_snapshot:
                    if row.dispatch_version != row.event_version:
                        row.dispatch_attempt = 0
                        row.notification_attempt_count = 0
                    row.dispatch_version = row.event_version
                    row.dispatch_event = _notification_event(
                        row,
                        tracking_degraded=int(row.consecutive_poll_error_count or 0) >= tracking_degraded_after_errors,
                    )
                    row.notification_status = "claimed"
                row.updated_at = now
            await session.commit()
            return [self._row_to_dict(row) for row in rows]

    async def mark_notification_dispatched(
        self,
        task_id: str,
        *,
        lease_owner: str,
        dispatch_version: int,
        run_id: str,
        now: datetime,
        tenant_digest: str,
    ) -> bool:
        stmt = (
            update(McpTaskRow)
            .where(
                McpTaskRow.id == task_id,
                self._tenant_scope(tenant_digest),
                McpTaskRow.notification_lease_owner == lease_owner,
                McpTaskRow.notification_lease_expires_at >= now,
                McpTaskRow.dispatch_version == dispatch_version,
                McpTaskRow.notification_status.in_(("claimed", "retry")),
            )
            .values(
                notification_status="dispatched",
                notification_run_id=run_id,
                notification_error=None,
                next_notification_at=now,
                notification_lease_owner=None,
                notification_lease_expires_at=None,
                updated_at=now,
            )
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            await session.commit()
            return bool(result.rowcount)

    async def release_notification_claim(
        self,
        task_id: str,
        *,
        lease_owner: str,
        next_notification_at: datetime,
        error: str,
        replace_with_latest: bool,
        count_failure: bool = False,
        tenant_digest: str,
    ) -> bool:
        values: dict[str, Any] = {
            "notification_status": "pending" if replace_with_latest else "retry",
            "notification_error": error,
            "next_notification_at": next_notification_at,
            "notification_lease_owner": None,
            "notification_lease_expires_at": None,
            "updated_at": datetime.now(UTC),
        }
        if replace_with_latest:
            values.update(
                dispatch_event=None,
            )
        if count_failure:
            values["notification_attempt_count"] = McpTaskRow.notification_attempt_count + 1
        stmt = (
            update(McpTaskRow)
            .where(
                McpTaskRow.id == task_id,
                McpTaskRow.notification_lease_owner == lease_owner,
                self._tenant_scope(tenant_digest),
            )
            .values(**values)
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            await session.commit()
            return bool(result.rowcount)

    async def finish_notification_run(
        self,
        task_id: str,
        *,
        lease_owner: str,
        dispatch_version: int,
        delivered: bool,
        next_notification_at: datetime | None,
        error: str | None,
        now: datetime,
        tenant_digest: str,
    ) -> bool:
        async with self._sf() as session:
            stmt = (
                select(McpTaskRow)
                .where(
                    McpTaskRow.id == task_id,
                    self._tenant_scope(tenant_digest),
                    McpTaskRow.notification_lease_owner == lease_owner,
                    McpTaskRow.notification_lease_expires_at >= now,
                    McpTaskRow.dispatch_version == dispatch_version,
                    McpTaskRow.notification_status == "dispatched",
                )
                .with_for_update()
            )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return False
            if delivered:
                row.notified_version = dispatch_version
                row.notification_status = "pending" if row.event_version > dispatch_version else "delivered"
                row.dispatch_version = None
                row.dispatch_attempt = 0
                row.dispatch_event = None
                row.notification_error = None
                row.notification_attempt_count = 0
                row.next_notification_at = now if row.event_version > dispatch_version else None
            else:
                row.notification_status = "retry"
                row.dispatch_attempt = int(row.dispatch_attempt or 0) + 1
                row.notification_attempt_count = int(row.notification_attempt_count or 0) + 1
                row.notification_error = error
                row.next_notification_at = next_notification_at
            row.notification_lease_owner = None
            row.notification_lease_expires_at = None
            row.updated_at = now
            await session.commit()
            return True

    async def release_notification_lease(
        self,
        task_id: str,
        *,
        lease_owner: str,
        next_notification_at: datetime,
        error: str,
        count_failure: bool = False,
        tenant_digest: str,
    ) -> bool:
        """Release unexpected notification work without changing its phase."""
        values: dict[str, Any] = {
            "notification_error": error,
            "next_notification_at": next_notification_at,
            "notification_lease_owner": None,
            "notification_lease_expires_at": None,
            "updated_at": datetime.now(UTC),
        }
        if count_failure:
            values["notification_attempt_count"] = McpTaskRow.notification_attempt_count + 1
        stmt = (
            update(McpTaskRow)
            .where(
                McpTaskRow.id == task_id,
                McpTaskRow.notification_lease_owner == lease_owner,
                self._tenant_scope(tenant_digest),
            )
            .values(**values)
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            await session.commit()
            return bool(result.rowcount)

    async def dead_letter_notification(
        self,
        task_id: str,
        *,
        lease_owner: str,
        dispatch_version: int,
        error: str,
        count_failure: bool,
        now: datetime,
        tenant_digest: str,
    ) -> bool:
        """Stop one failed snapshot, preserving any newer event for delivery."""
        base_filters = (
            McpTaskRow.id == task_id,
            self._tenant_scope(tenant_digest),
            McpTaskRow.notification_lease_owner == lease_owner,
            McpTaskRow.notification_lease_expires_at >= now,
            McpTaskRow.dispatch_version == dispatch_version,
            McpTaskRow.notification_status.in_(("claimed", "retry", "dispatched")),
        )
        dead_letter_values: dict[str, Any] = {
            "notification_status": "dead_letter",
            "notification_error": error,
            "next_notification_at": None,
            "notification_lease_owner": None,
            "notification_lease_expires_at": None,
            "dispatch_version": None,
            "dispatch_attempt": 0,
            "dispatch_event": None,
            "updated_at": now,
        }
        if count_failure:
            dead_letter_values["notification_attempt_count"] = McpTaskRow.notification_attempt_count + 1

        async with self._sf() as session:
            dead_lettered = await session.execute(update(McpTaskRow).where(*base_filters, McpTaskRow.event_version <= dispatch_version).values(**dead_letter_values))
            if dead_lettered.rowcount:
                await session.commit()
                return True

            replaced_by_latest = await session.execute(
                update(McpTaskRow)
                .where(*base_filters, McpTaskRow.event_version > dispatch_version)
                .values(
                    notification_status="pending",
                    notification_error=None,
                    notification_attempt_count=0,
                    next_notification_at=now,
                    notification_lease_owner=None,
                    notification_lease_expires_at=None,
                    dispatch_version=None,
                    dispatch_attempt=0,
                    dispatch_event=None,
                    updated_at=now,
                )
            )
            await session.commit()
            return bool(replaced_by_latest.rowcount)

    async def defer_dispatched_notification(
        self,
        task_id: str,
        *,
        lease_owner: str,
        dispatch_version: int,
        next_notification_at: datetime,
        now: datetime,
        tenant_digest: str,
    ) -> bool:
        """Release a notification lease while its Agent run is still active."""
        stmt = (
            update(McpTaskRow)
            .where(
                McpTaskRow.id == task_id,
                self._tenant_scope(tenant_digest),
                McpTaskRow.notification_lease_owner == lease_owner,
                McpTaskRow.notification_lease_expires_at >= now,
                McpTaskRow.dispatch_version == dispatch_version,
                McpTaskRow.notification_status == "dispatched",
            )
            .values(
                next_notification_at=next_notification_at,
                notification_lease_owner=None,
                notification_lease_expires_at=None,
                updated_at=now,
            )
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            await session.commit()
            return bool(result.rowcount)

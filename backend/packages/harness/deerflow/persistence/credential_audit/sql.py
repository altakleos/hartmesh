"""Aggregated credential audit observations with finite retention."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.credential_audit.contract import (
    normalize_audit_timestamp,
    validate_credential_audit_fields,
    validate_credential_reference,
)
from deerflow.persistence.credential_audit.model import CredentialAuditEventRow
from deerflow.runtime.tenant_identity import TenantReferenceV1
from deerflow.utils.time import coerce_iso


class CredentialAuditUnavailable(RuntimeError):
    """Required audit persistence failed without exposing backend details."""

    code = "audit_record_unavailable"

    def __init__(self) -> None:
        super().__init__(self.code)


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def principal_reference_digest(tenant: TenantReferenceV1, subject_id: str) -> str:
    """Return a pseudonymous commitment without persisting the subject id."""

    if not isinstance(subject_id, str) or not subject_id or len(subject_id.encode("utf-8")) > 256:
        raise ValueError("subject_id must be a bounded non-empty string")
    return _canonical_digest(
        {
            "version": 1,
            "tenant_digest": tenant.digest,
            "subject_id": subject_id,
        }
    )


class CredentialAuditRepository:
    """Write daily aggregates and expose only bounded safe projections."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tenant: TenantReferenceV1,
        retention_days: int = 90,
    ) -> None:
        if not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(retention_days) is not int or retention_days < 1 or retention_days > 3650:
            raise ValueError("retention_days must be between 1 and 3650")
        self._sf = session_factory
        self._tenant = tenant
        self._retention = timedelta(days=retention_days)

    def _tenant_predicates(self) -> tuple[Any, Any]:
        return (
            CredentialAuditEventRow.tenant_ref == self._tenant.public_ref,
            CredentialAuditEventRow.tenant_digest == self._tenant.digest,
        )

    @staticmethod
    def _safe_projection(row: CredentialAuditEventRow) -> dict[str, object]:
        def timestamp(value: datetime) -> str:
            return coerce_iso(value)

        return {
            "credential_ref": row.credential_ref,
            "actor_digest": row.actor_digest,
            "method": row.method,
            "authority_digest": row.authority_digest,
            "action": row.action,
            "route_category": row.route_category,
            "reason_code": row.reason_code,
            "first_occurred_at": timestamp(row.first_occurred_at),
            "last_occurred_at": timestamp(row.last_occurred_at),
            "event_count": int(row.event_count),
        }

    def _record_values(
        self,
        *,
        method: str,
        action: str,
        credential_ref: str | None,
        actor_digest: str | None,
        authority_digest: str | None,
        route_category: str,
        reason_code: str | None,
        occurred_at: datetime | None,
    ) -> dict[str, object]:
        validate_credential_audit_fields(
            method=method,
            action=action,
            credential_ref=credential_ref,
            actor_digest=actor_digest,
            authority_digest=authority_digest,
            route_category=route_category,
            reason_code=reason_code,
        )
        occurred = normalize_audit_timestamp(occurred_at)
        bucket = occurred.replace(hour=0, minute=0, second=0, microsecond=0)
        aggregation_key = _canonical_digest(
            {
                "version": 1,
                "tenant_digest": self._tenant.digest,
                "credential_ref": credential_ref,
                "actor_digest": actor_digest,
                "method": method,
                "authority_digest": authority_digest,
                "action": action,
                "route_category": route_category,
                "reason_code": reason_code,
                "bucket_start": bucket.isoformat(),
            }
        )
        return {
            "id": str(uuid.uuid4()),
            "aggregation_key": aggregation_key,
            "tenant_ref": self._tenant.public_ref,
            "tenant_digest": self._tenant.digest,
            "credential_ref": credential_ref,
            "actor_digest": actor_digest,
            "method": method,
            "authority_digest": authority_digest,
            "action": action,
            "route_category": route_category,
            "reason_code": reason_code,
            "bucket_start": bucket,
            "first_occurred_at": occurred,
            "last_occurred_at": occurred,
            "event_count": 1,
        }

    async def record_in_session(
        self,
        session: AsyncSession,
        *,
        method: str,
        action: str,
        credential_ref: str | None,
        actor_digest: str | None,
        authority_digest: str | None,
        route_category: str,
        reason_code: str | None = None,
        occurred_at: datetime | None = None,
        retention_now: datetime | None = None,
    ) -> None:
        """Append or aggregate one observation in the caller's transaction."""

        values = self._record_values(
            method=method,
            action=action,
            credential_ref=credential_ref,
            actor_digest=actor_digest,
            authority_digest=authority_digest,
            route_category=route_category,
            reason_code=reason_code,
            occurred_at=occurred_at,
        )
        dialect = session.get_bind().dialect.name
        if dialect == "sqlite":
            statement = sqlite_insert(CredentialAuditEventRow).values(**values)
            statement = statement.on_conflict_do_update(
                index_elements=[CredentialAuditEventRow.aggregation_key],
                set_={
                    "first_occurred_at": func.min(
                        CredentialAuditEventRow.first_occurred_at,
                        statement.excluded.first_occurred_at,
                    ),
                    "last_occurred_at": func.max(
                        CredentialAuditEventRow.last_occurred_at,
                        statement.excluded.last_occurred_at,
                    ),
                    "event_count": CredentialAuditEventRow.event_count + 1,
                },
            )
            await session.execute(statement)
        elif dialect == "postgresql":
            statement = postgresql_insert(CredentialAuditEventRow).values(**values)
            statement = statement.on_conflict_do_update(
                index_elements=[CredentialAuditEventRow.aggregation_key],
                set_={
                    "first_occurred_at": func.least(
                        CredentialAuditEventRow.first_occurred_at,
                        statement.excluded.first_occurred_at,
                    ),
                    "last_occurred_at": func.greatest(
                        CredentialAuditEventRow.last_occurred_at,
                        statement.excluded.last_occurred_at,
                    ),
                    "event_count": CredentialAuditEventRow.event_count + 1,
                },
            )
            await session.execute(statement)
        else:  # pragma: no cover - shipped database backends are SQLite/PostgreSQL
            existing = (await session.execute(select(CredentialAuditEventRow).where(CredentialAuditEventRow.aggregation_key == values["aggregation_key"]))).scalar_one_or_none()
            if existing is None:
                session.add(CredentialAuditEventRow(**values))
            else:
                await session.execute(
                    update(CredentialAuditEventRow)
                    .where(CredentialAuditEventRow.id == existing.id)
                    .values(
                        first_occurred_at=min(
                            existing.first_occurred_at,
                            values["first_occurred_at"],
                        ),
                        last_occurred_at=max(
                            existing.last_occurred_at,
                            values["last_occurred_at"],
                        ),
                        event_count=CredentialAuditEventRow.event_count + 1,
                    )
                )
        retention_reference = normalize_audit_timestamp(retention_now)
        await session.execute(
            delete(CredentialAuditEventRow).where(
                *self._tenant_predicates(),
                CredentialAuditEventRow.last_occurred_at < retention_reference - self._retention,
            )
        )

    async def record(self, **values: Any) -> None:
        async with self._sf() as session:
            await self.record_in_session(session, **values)
            await session.commit()

    @staticmethod
    def _validate_limit(limit: int) -> int:
        if type(limit) is not int or limit < 1 or limit > 100:
            raise ValueError("audit observation limit must be between 1 and 100")
        return limit

    async def list_for_credential(self, credential_ref: str, *, limit: int = 50) -> list[dict[str, object]]:
        validate_credential_reference(credential_ref)
        bounded = self._validate_limit(limit)
        async with self._sf() as session:
            rows = (
                await session.execute(
                    select(CredentialAuditEventRow)
                    .where(
                        *self._tenant_predicates(),
                        CredentialAuditEventRow.credential_ref == credential_ref,
                    )
                    .order_by(CredentialAuditEventRow.last_occurred_at.desc())
                    .limit(bounded)
                )
            ).scalars()
            return [self._safe_projection(row) for row in rows]

    async def list_recent(self, *, limit: int = 50) -> list[dict[str, object]]:
        bounded = self._validate_limit(limit)
        async with self._sf() as session:
            rows = (await session.execute(select(CredentialAuditEventRow).where(*self._tenant_predicates()).order_by(CredentialAuditEventRow.last_occurred_at.desc()).limit(bounded))).scalars()
            return [self._safe_projection(row) for row in rows]


__all__ = [
    "CredentialAuditRepository",
    "CredentialAuditUnavailable",
    "principal_reference_digest",
]

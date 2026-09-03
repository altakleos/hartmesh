"""SQLAlchemy-backed personal access token storage.

Each method acquires its own short-lived session. The raw ``dfp_…`` token is
generated and returned by the caller (the app layer) exactly once; this
repository only ever persists the SHA-256 digest passed to :meth:`create`.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from deerflow_extension_api import effective_authority_digest_v1
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.credential_audit import (
    CredentialAuditRepository,
    CredentialAuditUnavailable,
)
from deerflow.persistence.credential_audit.sql import principal_reference_digest
from deerflow.persistence.personal_access_tokens.model import PersonalAccessTokenRow
from deerflow.runtime.tenant_identity import TenantReferenceV1
from deerflow.utils.time import coerce_iso

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PersonalAccessTokenAuthenticationResult:
    record: dict[str, Any] | None
    failure_reason: str | None


class PersonalAccessTokenRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        tenant: TenantReferenceV1,
        audit_repository: CredentialAuditRepository | None = None,
        last_used_write_interval_seconds: float = 300.0,
    ) -> None:
        if not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        self._sf = session_factory
        self._tenant = tenant
        self._audit = audit_repository or CredentialAuditRepository(
            session_factory,
            tenant=tenant,
        )
        self._last_used_write_interval = last_used_write_interval_seconds
        self._last_used_written_at: dict[str, float] = {}

    def _tenant_predicates(self) -> tuple[Any, Any]:
        return (
            PersonalAccessTokenRow.tenant_ref == self._tenant.public_ref,
            PersonalAccessTokenRow.tenant_digest == self._tenant.digest,
        )

    @property
    def audit_repository(self) -> CredentialAuditRepository:
        return self._audit

    @property
    def tenant(self) -> TenantReferenceV1:
        return self._tenant

    @staticmethod
    def _row_to_dict(row: PersonalAccessTokenRow) -> dict[str, Any]:
        d = row.to_dict()
        for key in ("expires_at", "last_used_at", "created_at", "revoked_at"):
            val = d.get(key)
            if isinstance(val, datetime):
                # SQLite drops tzinfo on read; normalize so output is tz-aware.
                d[key] = coerce_iso(val)
        return d

    async def create(
        self,
        *,
        user_id: str,
        name: str,
        scopes: list[str],
        token_digest: str,
        expires_at: datetime | None = None,
    ) -> dict[str, Any]:
        row = PersonalAccessTokenRow(
            id=str(uuid.uuid4()),
            tenant_ref=self._tenant.public_ref,
            tenant_digest=self._tenant.digest,
            user_id=user_id,
            name=name,
            token_digest=token_digest,
            scopes=sorted(scopes),
            expires_at=expires_at,
            created_at=datetime.now(UTC),
        )
        async with self._sf() as session:
            session.add(row)
            try:
                with session.no_autoflush:
                    await self._audit.record_in_session(
                        session,
                        method="personal_access_token",
                        action="created",
                        credential_ref=row.id,
                        actor_digest=principal_reference_digest(
                            self._tenant,
                            user_id,
                        ),
                        authority_digest=effective_authority_digest_v1(row.scopes),
                        route_category="credential_management",
                        occurred_at=row.created_at,
                    )
            except Exception as exc:
                raise CredentialAuditUnavailable() from exc
            await session.commit()
            await session.refresh(row)
            return self._row_to_dict(row)

    async def get_active_by_digest(self, token_digest: str) -> dict[str, Any] | None:
        """Return the non-revoked, non-expired row for *token_digest*.

        Revocation and expiry are evaluated here so a stale durable row can
        never authenticate even though it remains readable for audit history.
        """
        result = await self.resolve_for_authentication(token_digest)
        return result.record if result.failure_reason is None else None

    async def resolve_for_authentication(
        self,
        token_digest: str,
    ) -> PersonalAccessTokenAuthenticationResult:
        """Resolve one tenant-scoped credential with a safe internal verdict."""

        async with self._sf() as session:
            row = (
                await session.execute(
                    select(PersonalAccessTokenRow).where(
                        *self._tenant_predicates(),
                        PersonalAccessTokenRow.token_digest == token_digest,
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                return PersonalAccessTokenAuthenticationResult(
                    record=None,
                    failure_reason="credential_invalid",
                )
            record = self._row_to_dict(row)
            if row.revoked_at is not None:
                return PersonalAccessTokenAuthenticationResult(
                    record=record,
                    failure_reason="credential_revoked",
                )
            expires_at = row.expires_at
            if expires_at is not None:
                # SQLite drops tzinfo on read; normalize before comparing.
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=UTC)
                if expires_at <= datetime.now(UTC):
                    return PersonalAccessTokenAuthenticationResult(
                        record=record,
                        failure_reason="credential_expired",
                    )
            return PersonalAccessTokenAuthenticationResult(
                record=record,
                failure_reason=None,
            )

    async def list_for_user(self, user_id: str) -> list[dict[str, Any]]:
        async with self._sf() as session:
            rows = (
                await session.execute(
                    select(PersonalAccessTokenRow)
                    .where(
                        *self._tenant_predicates(),
                        PersonalAccessTokenRow.user_id == user_id,
                    )
                    .order_by(PersonalAccessTokenRow.created_at.desc())
                )
            ).scalars()
            return [self._row_to_dict(row) for row in rows]

    async def revoke(self, pat_id: str, user_id: str) -> bool:
        """Revoke one of *user_id*'s tokens; returns False if not owned/absent."""
        async with self._sf() as session:
            statement = select(PersonalAccessTokenRow).where(
                *self._tenant_predicates(),
                PersonalAccessTokenRow.id == pat_id,
                PersonalAccessTokenRow.user_id == user_id,
                PersonalAccessTokenRow.revoked_at.is_(None),
            )
            if session.get_bind().dialect.name == "postgresql":
                statement = statement.with_for_update()
            row = (await session.execute(statement)).scalar_one_or_none()
            if row is None:
                return False
            revoked_at = datetime.now(UTC)
            row.revoked_at = revoked_at
            try:
                with session.no_autoflush:
                    await self._audit.record_in_session(
                        session,
                        method="personal_access_token",
                        action="revoked",
                        credential_ref=row.id,
                        actor_digest=principal_reference_digest(
                            self._tenant,
                            user_id,
                        ),
                        authority_digest=effective_authority_digest_v1(row.scopes),
                        route_category="credential_management",
                        occurred_at=revoked_at,
                    )
            except Exception as exc:
                raise CredentialAuditUnavailable() from exc
            await session.commit()
            return True

    async def list_audit_for_user(
        self,
        pat_id: str,
        user_id: str,
        *,
        limit: int = 50,
    ) -> list[dict[str, object]] | None:
        """Return bounded audit observations after tenant + owner checks."""

        async with self._sf() as session:
            owned = (
                await session.execute(
                    select(PersonalAccessTokenRow.id).where(
                        *self._tenant_predicates(),
                        PersonalAccessTokenRow.id == pat_id,
                        PersonalAccessTokenRow.user_id == user_id,
                    )
                )
            ).scalar_one_or_none()
        if owned is None:
            return None
        return await self._audit.list_for_credential(pat_id, limit=limit)

    async def record_audit(self, **values: Any) -> None:
        """Record a required credential observation; failures propagate."""

        await self._audit.record(**values)

    async def record_audit_best_effort(self, **values: Any) -> None:
        """Record a low-value observation without failing authentication."""

        try:
            await self._audit.record(**values)
        except Exception as exc:
            logger.debug(
                "Credential audit observation failed error_class=%s (non-fatal)",
                type(exc).__name__,
            )

    def _should_write_last_used(self, pat_id: str) -> bool:
        now = time.monotonic()
        last = self._last_used_written_at.get(pat_id)
        if last is not None and (now - last) < self._last_used_write_interval:
            return False
        # Bound the stamp cache: revoked/expired tokens never return here, so
        # their entries are stale by definition once the cache outgrows very
        # active token populations.
        if len(self._last_used_written_at) > 4096:
            self._last_used_written_at.clear()
        self._last_used_written_at[pat_id] = now
        return True

    async def touch_last_used(self, pat_id: str) -> None:
        """Best-effort, throttled usage stamp (at most one write per interval).

        Never raises: a failure to stamp usage must not fail the request. On
        failure the throttle window is rolled back so the next attempt
        retries promptly instead of waiting out the full interval.
        """
        if not self._should_write_last_used(pat_id):
            return
        try:
            async with self._sf() as session:
                await session.execute(
                    update(PersonalAccessTokenRow)
                    .where(
                        *self._tenant_predicates(),
                        PersonalAccessTokenRow.id == pat_id,
                    )
                    .values(last_used_at=datetime.now(UTC))
                )
                await session.commit()
        except Exception as exc:
            self._last_used_written_at.pop(pat_id, None)
            logger.debug(
                "PAT last-used stamp failed credential_ref=%s error_class=%s (non-fatal)",
                pat_id,
                type(exc).__name__,
            )

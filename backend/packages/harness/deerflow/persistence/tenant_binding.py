"""Atomic one-tenant binding for one DeerFlow database schema."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Integer,
    String,
    select,
    text,
)
from sqlalchemy import (
    inspect as sa_inspect,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base
from deerflow.runtime.tenant_identity import (
    LegacyRedisPrefixRecordV1,
    TenantIdentityError,
    TenantIdentityV1,
    TenantReferenceV1,
    tenant_admission_scope,
)

_SINGLETON_KEY = 1
_EMPTY_SCHEMA_METADATA_TABLES = frozenset(
    {
        "alembic_version",
        "checkpoint_migrations",
        "hartmesh_deployment_identity",
        "run_lifecycle_cursor_state",
        "run_admission_cursor_state",
    }
)


class DeploymentIdentityRow(Base):
    """Pseudonymous binding for the current application schema."""

    __tablename__ = "hartmesh_deployment_identity"

    singleton_key: Mapped[int] = mapped_column(Integer, primary_key=True)
    identity_version: Mapped[int] = mapped_column(Integer, nullable=False)
    tenant_ref: Mapped[str] = mapped_column(String(23), nullable=False)
    tenant_digest: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        unique=True,
    )
    legacy_redis_prefixes_json: Mapped[dict[str, object] | None] = mapped_column(
        JSON,
        nullable=True,
    )
    bound_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        CheckConstraint(
            "singleton_key = 1",
            name="ck_hartmesh_deployment_identity_singleton",
        ),
        CheckConstraint(
            "identity_version = 1",
            name="ck_hartmesh_deployment_identity_version",
        ),
    )


class TenantSchemaBindingAction(StrEnum):
    """Finite outcomes from schema identity inspection or binding."""

    already_bound = "already_bound"
    bound_empty_schema = "bound_empty_schema"
    bound_nonempty_schema = "bound_nonempty_schema"
    would_bind_empty_schema = "would_bind_empty_schema"
    would_bind_nonempty_schema = "would_bind_nonempty_schema"


@dataclass(frozen=True)
class TenantSchemaBindingResult:
    """Typed result of one atomic schema identity operation."""

    action: TenantSchemaBindingAction
    tenant: TenantReferenceV1
    legacy_redis_prefixes: LegacyRedisPrefixRecordV1


def _schema_has_application_data(sync_connection: Any) -> bool:
    """Return whether any known or unknown application table has one row.

    Extension tables intentionally use private SQLAlchemy metadata, and an
    older/newer binary may not know every table in a shared schema.  Treating
    only host metadata as occupancy would let startup claim populated
    extension or unknown tables for a new tenant.  The allowlist contains only
    schema-version/cursor metadata that carries no tenant-owned application
    state.
    """

    from sqlalchemy import inspect

    # Ensure mapped tables are registered before inspection initializes any
    # dialect-specific metadata state. Unknown tables remain candidates.
    import deerflow.persistence.models  # noqa: F401

    present = set(inspect(sync_connection).get_table_names())
    candidates = sorted(present - _EMPTY_SCHEMA_METADATA_TABLES)
    quote = sync_connection.dialect.identifier_preparer.quote
    for table_name in candidates:
        if sync_connection.execute(text(f"SELECT 1 FROM {quote(table_name)} LIMIT 1")).first() is not None:
            return True
    return False


async def _begin_binding_transaction(session: AsyncSession) -> None:
    dialect = session.get_bind().dialect.name
    if dialect == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))
    else:
        await session.begin()
        if dialect == "postgresql":
            # Serializes the missing-singleton decision across processes.
            await session.execute(text("LOCK TABLE hartmesh_deployment_identity IN EXCLUSIVE MODE"))


async def _backfill_legacy_tenant_rows(
    session: AsyncSession,
    *,
    tenant: TenantReferenceV1,
) -> None:
    """Bind nullable pre-migration rows inside the singleton transaction."""

    connection = await session.connection()
    present_tables = await connection.run_sync(lambda sync_connection: set(sa_inspect(sync_connection).get_table_names()))
    tenant_columns = await connection.run_sync(lambda sync_connection: {table_name: {str(column["name"]) for column in sa_inspect(sync_connection).get_columns(table_name)} for table_name in present_tables})

    legacy_scopes = (await session.execute(text("SELECT run_id, external_scope FROM runs WHERE tenant_digest IS NULL AND external_scope IS NOT NULL"))).all()
    for run_id, base_scope in legacy_scopes:
        await session.execute(
            text("UPDATE runs SET external_scope = :external_scope WHERE run_id = :run_id AND tenant_digest IS NULL"),
            {
                "run_id": run_id,
                "external_scope": tenant_admission_scope(
                    tenant,
                    base_scope,
                ),
            },
        )

    for table_name in (
        "runs",
        "run_lifecycle_events",
        "run_events",
        "personal_access_tokens",
    ):
        if table_name not in present_tables:
            continue
        # An explicit operator bind can precede a migration that first adds
        # tenant anchors to this table. The singleton is authoritative; the
        # later migration backfills the new columns from it.
        if not {"tenant_ref", "tenant_digest"} <= tenant_columns[table_name]:
            continue
        conflict = (
            await session.execute(
                text(f"SELECT 1 FROM {table_name} WHERE tenant_digest IS NOT NULL AND (tenant_digest <> :tenant_digest OR tenant_ref <> :tenant_ref) LIMIT 1"),
                {
                    "tenant_ref": tenant.public_ref,
                    "tenant_digest": tenant.digest,
                },
            )
        ).first()
        if conflict is not None:
            raise TenantIdentityError(
                "tenant_identity_mismatch",
                f"legacy table {table_name} contains a different tenant binding",
            )
        await session.execute(
            text(f"UPDATE {table_name} SET tenant_ref = :tenant_ref, tenant_digest = :tenant_digest WHERE tenant_ref IS NULL AND tenant_digest IS NULL"),
            {
                "tenant_ref": tenant.public_ref,
                "tenant_digest": tenant.digest,
            },
        )


async def ensure_schema_tenant_binding(
    session_factory: async_sessionmaker[AsyncSession],
    identity: TenantIdentityV1,
    *,
    allow_nonempty_legacy: bool = False,
    require_nonempty_legacy: bool = False,
    dry_run: bool = False,
    legacy_redis_prefixes: LegacyRedisPrefixRecordV1 | None = None,
) -> TenantSchemaBindingResult:
    """Validate or atomically establish the schema's one tenant identity.

    Startup calls this without ``allow_nonempty_legacy``. Only the explicit
    operator migration command may set that flag and
    ``require_nonempty_legacy`` for a legacy nonempty schema.
    """

    if not isinstance(identity, TenantIdentityV1):
        raise TypeError("identity must be TenantIdentityV1")
    if legacy_redis_prefixes is not None and not isinstance(
        legacy_redis_prefixes,
        LegacyRedisPrefixRecordV1,
    ):
        raise TypeError("legacy_redis_prefixes must be LegacyRedisPrefixRecordV1 or None")
    requested_legacy_prefixes = legacy_redis_prefixes or LegacyRedisPrefixRecordV1()
    if not requested_legacy_prefixes.is_empty and not allow_nonempty_legacy:
        raise ValueError("legacy Redis prefixes may be recorded only by the explicit legacy schema binding operation")
    reference = identity.to_persisted_reference()

    async with session_factory() as session:
        await _begin_binding_transaction(session)
        statement = select(DeploymentIdentityRow).where(DeploymentIdentityRow.singleton_key == _SINGLETON_KEY)
        if session.get_bind().dialect.name == "postgresql":
            statement = statement.with_for_update()
        existing = (await session.execute(statement)).scalar_one_or_none()
        if existing is not None:
            if existing.identity_version != reference.version or existing.tenant_ref != reference.public_ref or existing.tenant_digest != reference.digest:
                await session.rollback()
                raise TenantIdentityError(
                    "tenant_identity_mismatch",
                    "configured tenant does not match the tenant already bound to this database schema",
                )
            existing_version = existing.identity_version
            existing_ref = existing.tenant_ref
            existing_digest = existing.tenant_digest
            stored_legacy_prefixes = LegacyRedisPrefixRecordV1() if existing.legacy_redis_prefixes_json is None else LegacyRedisPrefixRecordV1.from_json(existing.legacy_redis_prefixes_json)
            if not requested_legacy_prefixes.is_empty and requested_legacy_prefixes != stored_legacy_prefixes:
                await session.rollback()
                raise TenantIdentityError(
                    "tenant_namespace_conflict",
                    "the database schema already records different legacy Redis prefixes",
                )
            await session.rollback()
            return TenantSchemaBindingResult(
                action=TenantSchemaBindingAction.already_bound,
                tenant=TenantReferenceV1(
                    version=existing_version,
                    public_ref=existing_ref,
                    digest=existing_digest,
                ),
                legacy_redis_prefixes=stored_legacy_prefixes,
            )

        connection = await session.connection()
        nonempty = await connection.run_sync(_schema_has_application_data)
        if require_nonempty_legacy and not allow_nonempty_legacy:
            await session.rollback()
            raise ValueError("require_nonempty_legacy requires allow_nonempty_legacy")
        if require_nonempty_legacy and not nonempty:
            await session.rollback()
            raise TenantIdentityError(
                "tenant_schema_unbound",
                "expected a nonempty legacy schema but found no application data; refusing the explicit legacy binding operation",
            )
        if nonempty and not allow_nonempty_legacy:
            await session.rollback()
            raise TenantIdentityError(
                "tenant_schema_unbound",
                "the nonempty legacy schema has no tenant binding; stop the Gateway and run 'deerflow deployment bind-tenant' first",
            )

        if dry_run:
            await session.rollback()
            action = TenantSchemaBindingAction.would_bind_nonempty_schema if nonempty else TenantSchemaBindingAction.would_bind_empty_schema
        else:
            if nonempty:
                await _backfill_legacy_tenant_rows(
                    session,
                    tenant=reference,
                )
            session.add(
                DeploymentIdentityRow(
                    singleton_key=_SINGLETON_KEY,
                    identity_version=reference.version,
                    tenant_ref=reference.public_ref,
                    tenant_digest=reference.digest,
                    legacy_redis_prefixes_json=(None if requested_legacy_prefixes.is_empty else requested_legacy_prefixes.to_json()),
                    bound_at=datetime.now(UTC),
                )
            )
            await session.commit()
            action = TenantSchemaBindingAction.bound_nonempty_schema if nonempty else TenantSchemaBindingAction.bound_empty_schema
        return TenantSchemaBindingResult(
            action=action,
            tenant=reference,
            legacy_redis_prefixes=requested_legacy_prefixes,
        )


__all__ = [
    "DeploymentIdentityRow",
    "TenantSchemaBindingAction",
    "TenantSchemaBindingResult",
    "ensure_schema_tenant_binding",
]

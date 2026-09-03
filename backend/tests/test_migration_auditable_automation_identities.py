"""Migration coverage for tenant-bound PATs and credential audit evidence."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateTable

from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.credential_audit.model import CredentialAuditEventRow
from deerflow.persistence.personal_access_tokens.model import PersonalAccessTokenRow
from deerflow.persistence.tenant_binding import ensure_schema_tenant_binding
from deerflow.runtime.tenant_identity import TenantIdentityV1

_REVISION = "0033_automation_identities"
_PREVIOUS_REVISION = "0032_subagent_batch_evidence"


def test_models_render_tenant_and_audit_indexes_for_postgres() -> None:
    pat_ddl = str(
        CreateTable(PersonalAccessTokenRow.__table__).compile(
            dialect=postgresql.dialect(),
        )
    ).lower()
    audit_ddl = str(
        CreateTable(CredentialAuditEventRow.__table__).compile(
            dialect=postgresql.dialect(),
        )
    ).lower()
    assert "tenant_ref varchar(23)" in pat_ddl
    assert "tenant_digest varchar(64)" in pat_ddl
    assert "tenant_ref varchar(23) not null" in pat_ddl
    assert "tenant_digest varchar(64) not null" in pat_ddl
    assert "credential_ref varchar(128)" in audit_ddl
    assert "event_count bigint not null" in audit_ddl
    assert "ck_credential_audit_safe_references" in audit_ddl
    assert "ck_credential_audit_bounds" in audit_ddl


@pytest.mark.asyncio
async def test_bound_singleton_backfills_pat_without_replacing_uuid(tmp_path: Path) -> None:
    path = tmp_path / "credential-migration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    pat_id = "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89"
    tenant_ref = "tenant-aaaaaaaaaaaaaaaa"
    tenant_digest = "a" * 64
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO hartmesh_deployment_identity (singleton_key, identity_version, tenant_ref, tenant_digest, legacy_redis_prefixes_json, bound_at) VALUES (1, 1, ?, ?, NULL, CURRENT_TIMESTAMP)",
                (tenant_ref, tenant_digest),
            )
            connection.execute(
                "INSERT INTO personal_access_tokens "
                "(id, user_id, name, token_digest, scopes, expires_at, last_used_at, "
                "created_at, revoked_at) VALUES (?, 'user-1', 'private name', ?, "
                "'[\"runs:read\"]', NULL, NULL, CURRENT_TIMESTAMP, NULL)",
                (pat_id, "b" * 64),
            )
            connection.commit()

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            column_rows = {row[1]: row for row in connection.execute("PRAGMA table_info(personal_access_tokens)")}
            columns = set(column_rows)
            assert {"tenant_ref", "tenant_digest"} <= columns
            assert column_rows["tenant_ref"][3] == 1
            assert column_rows["tenant_digest"][3] == 1
            assert connection.execute("SELECT id, tenant_ref, tenant_digest FROM personal_access_tokens").fetchone() == (pat_id, tenant_ref, tenant_digest)
            assert connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='credential_audit_events'").fetchone() == ("credential_audit_events",)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_populated_unbound_legacy_pat_fails_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credential-unbound.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO personal_access_tokens "
                "(id, user_id, name, token_digest, scopes, expires_at, last_used_at, "
                "created_at, revoked_at) VALUES (?, 'user-1', 'private name', ?, "
                "'[\"runs:read\"]', NULL, NULL, CURRENT_TIMESTAMP, NULL)",
                ("018f2d70-0fca-4f88-b0c3-a0f83ebf2c89", "b" * 64),
            )
            connection.commit()

        with pytest.raises(
            RuntimeError,
            match="credential_tenant_binding_required",
        ):
            await asyncio.to_thread(command.upgrade, config, _REVISION)

        identity = TenantIdentityV1.from_canonical_id("tenant-a")
        await ensure_schema_tenant_binding(
            async_sessionmaker(engine, expire_on_commit=False),
            identity,
            allow_nonempty_legacy=True,
            require_nonempty_legacy=True,
        )
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            row = connection.execute("SELECT tenant_ref, tenant_digest FROM personal_access_tokens").fetchone()
            assert row == (identity.public_ref, identity.digest)
            columns = {item[1]: item for item in connection.execute("PRAGMA table_info(personal_access_tokens)")}
            assert columns["tenant_ref"][3] == 1
            assert columns["tenant_digest"][3] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_empty_unbound_schema_gets_required_anchors_and_downgrades(
    tmp_path: Path,
) -> None:
    path = tmp_path / "credential-empty-unbound.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1]: row for row in connection.execute("PRAGMA table_info(personal_access_tokens)")}
            assert columns["tenant_ref"][3] == 1
            assert columns["tenant_digest"][3] == 1

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(personal_access_tokens)")}
            assert "tenant_ref" not in columns
            assert "credential_audit_events" not in {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_downgrade_refuses_bound_pat_or_audit_evidence(tmp_path: Path) -> None:
    path = tmp_path / "credential-used.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO personal_access_tokens (id, tenant_ref, tenant_digest, user_id, name, token_digest, scopes, created_at) VALUES (?, ?, ?, 'user-1', 'private', ?, '[\"runs:read\"]', CURRENT_TIMESTAMP)",
                (
                    "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
                    "tenant-aaaaaaaaaaaaaaaa",
                    "a" * 64,
                    "b" * 64,
                ),
            )
            connection.commit()

        with pytest.raises(
            RuntimeError,
            match="auditable_automation_identities_downgrade_blocked",
        ):
            await asyncio.to_thread(
                command.downgrade,
                config,
                _PREVIOUS_REVISION,
            )
    finally:
        await engine.dispose()

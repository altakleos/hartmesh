from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.personal_access_tokens.model import PersonalAccessTokenRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.tenant_binding import (
    DeploymentIdentityRow,
    TenantSchemaBindingAction,
    ensure_schema_tenant_binding,
)
from deerflow.runtime.tenant_identity import (
    LegacyRedisPrefixRecordV1,
    TenantIdentityError,
    TenantIdentityV1,
    tenant_admission_scope,
)


async def _database(tmp_path, name: str):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / name}")
    # Register every model, including the tenant singleton, before create_all.
    import deerflow.persistence.models  # noqa: F401

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


@pytest.mark.asyncio
async def test_empty_schema_binds_once_without_raw_operator_id(tmp_path) -> None:
    engine, sessions = await _database(tmp_path, "empty.db")
    identity = TenantIdentityV1.from_canonical_id("customer-readable-name")
    try:
        result = await ensure_schema_tenant_binding(sessions, identity)
        repeated = await ensure_schema_tenant_binding(sessions, identity)

        assert result.action is TenantSchemaBindingAction.bound_empty_schema
        assert repeated.action is TenantSchemaBindingAction.already_bound
        async with sessions() as session:
            row = await session.get(DeploymentIdentityRow, 1)
            assert row is not None
            rendered = repr(row.to_dict())
            assert row.tenant_ref == identity.public_ref
            assert row.tenant_digest == identity.digest
            assert "customer-readable-name" not in rendered
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_admission_cursor_singleton_is_empty_schema_metadata(
    tmp_path,
) -> None:
    engine, sessions = await _database(tmp_path, "admission-cursor-metadata.db")
    identity = TenantIdentityV1.from_canonical_id("tenant-a")
    try:
        async with sessions() as session:
            last_cursor = await session.scalar(text("SELECT last_cursor FROM run_admission_cursor_state WHERE singleton_id = 1"))
        assert last_cursor == 0

        result = await ensure_schema_tenant_binding(sessions, identity)

        assert result.action is TenantSchemaBindingAction.bound_empty_schema
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_different_process_identity_fails_against_bound_schema(tmp_path) -> None:
    engine, sessions = await _database(tmp_path, "mismatch.db")
    try:
        await ensure_schema_tenant_binding(
            sessions,
            TenantIdentityV1.from_canonical_id("tenant-a"),
        )

        with pytest.raises(TenantIdentityError) as error:
            await ensure_schema_tenant_binding(
                sessions,
                TenantIdentityV1.from_canonical_id("tenant-b"),
            )

        assert error.value.code == "tenant_identity_mismatch"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_nonempty_legacy_schema_requires_explicit_operator_binding(tmp_path) -> None:
    engine, sessions = await _database(tmp_path, "legacy.db")
    identity = TenantIdentityV1.from_canonical_id("tenant-a")
    legacy_scope = "http:v1:sha256:" + ("a" * 64)
    try:
        async with sessions() as session:
            session.add(
                RunRow(
                    run_id="legacy-run",
                    thread_id="legacy-thread",
                    status="success",
                    metadata_json={},
                    kwargs_json={},
                    external_scope=legacy_scope,
                    external_key="raw:legacy-request",
                    request_digest="b" * 64,
                    request_digest_version="sha256-canonical-json-v1",
                )
            )
            session.add(
                PersonalAccessTokenRow(
                    id="018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
                    user_id="user-1",
                    name="legacy private name",
                    token_digest="c" * 64,
                    scopes=["runs:read"],
                )
            )
            await session.commit()

        with pytest.raises(TenantIdentityError) as error:
            await ensure_schema_tenant_binding(sessions, identity)
        assert error.value.code == "tenant_schema_unbound"

        preview = await ensure_schema_tenant_binding(
            sessions,
            identity,
            allow_nonempty_legacy=True,
            dry_run=True,
        )
        assert preview.action is TenantSchemaBindingAction.would_bind_nonempty_schema
        async with sessions() as session:
            assert await session.get(DeploymentIdentityRow, 1) is None

        legacy_prefixes = LegacyRedisPrefixRecordV1(
            stream_bridge="legacy:stream",
            checkpoint_cache="legacy:checkpoint",
            sandbox_ownership="legacy:ownership",
        )
        bound = await ensure_schema_tenant_binding(
            sessions,
            identity,
            allow_nonempty_legacy=True,
            legacy_redis_prefixes=legacy_prefixes,
        )
        assert bound.action is TenantSchemaBindingAction.bound_nonempty_schema
        assert bound.legacy_redis_prefixes == legacy_prefixes
        async with sessions() as session:
            legacy = await session.get(RunRow, "legacy-run")
            legacy_pat = await session.get(
                PersonalAccessTokenRow,
                "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
            )
            binding = await session.get(DeploymentIdentityRow, 1)
            assert legacy is not None
            assert legacy_pat is not None
            assert binding is not None
            assert binding.legacy_redis_prefixes_json == legacy_prefixes.to_json()
            assert legacy.tenant_ref == identity.public_ref
            assert legacy.tenant_digest == identity.digest
            assert legacy_pat.tenant_ref == identity.public_ref
            assert legacy_pat.tenant_digest == identity.digest
            assert legacy.external_scope == tenant_admission_scope(
                identity.to_persisted_reference(),
                legacy_scope,
            )

        repeated = await ensure_schema_tenant_binding(sessions, identity)
        assert repeated.legacy_redis_prefixes == legacy_prefixes
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table_name",
    ("checkpoints", "checkpoint_blobs", "checkpoint_writes", "store", "store_vectors"),
)
async def test_langgraph_durable_data_requires_explicit_operator_binding(
    tmp_path,
    table_name: str,
) -> None:
    engine, sessions = await _database(tmp_path, f"{table_name}.db")
    identity = TenantIdentityV1.from_canonical_id("tenant-a")
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE TABLE "{table_name}" (item_id TEXT PRIMARY KEY)'))
            await connection.execute(text(f"INSERT INTO \"{table_name}\" (item_id) VALUES ('legacy')"))

        with pytest.raises(TenantIdentityError) as error:
            await ensure_schema_tenant_binding(sessions, identity)

        assert error.value.code == "tenant_schema_unbound"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_langgraph_migration_metadata_does_not_make_schema_nonempty(
    tmp_path,
) -> None:
    engine, sessions = await _database(tmp_path, "checkpoint-metadata.db")
    identity = TenantIdentityV1.from_canonical_id("tenant-a")
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE TABLE checkpoint_migrations (version INTEGER PRIMARY KEY)"))
            await connection.execute(text("INSERT INTO checkpoint_migrations (version) VALUES (1)"))

        result = await ensure_schema_tenant_binding(sessions, identity)

        assert result.action is TenantSchemaBindingAction.bound_empty_schema
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table_name",
    ("acme_extension_jobs", "unknown_application_table"),
)
async def test_populated_extension_or_unknown_table_requires_explicit_binding(
    tmp_path,
    table_name: str,
) -> None:
    engine, sessions = await _database(tmp_path, f"{table_name}.db")
    identity = TenantIdentityV1.from_canonical_id("tenant-a")
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE TABLE "{table_name}" (item_id TEXT PRIMARY KEY)'))
            await connection.execute(text(f"INSERT INTO \"{table_name}\" (item_id) VALUES ('legacy')"))

        with pytest.raises(TenantIdentityError) as error:
            await ensure_schema_tenant_binding(sessions, identity)

        assert error.value.code == "tenant_schema_unbound"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_operator_nonempty_acknowledgement_refuses_an_empty_schema(
    tmp_path,
) -> None:
    engine, sessions = await _database(tmp_path, "unexpected-empty.db")
    try:
        with pytest.raises(TenantIdentityError) as error:
            await ensure_schema_tenant_binding(
                sessions,
                TenantIdentityV1.from_canonical_id("tenant-a"),
                allow_nonempty_legacy=True,
                require_nonempty_legacy=True,
                dry_run=True,
            )

        assert error.value.code == "tenant_schema_unbound"
        assert "expected a nonempty legacy schema" in str(error.value)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_first_startup_can_bind_only_one_identity(tmp_path) -> None:
    engine, sessions = await _database(tmp_path, "concurrent.db")
    identities = (
        TenantIdentityV1.from_canonical_id("tenant-a"),
        TenantIdentityV1.from_canonical_id("tenant-b"),
    )
    try:
        results = await asyncio.gather(
            *(ensure_schema_tenant_binding(sessions, identity) for identity in identities),
            return_exceptions=True,
        )

        assert sum(not isinstance(result, Exception) for result in results) == 1
        failure = next(result for result in results if isinstance(result, Exception))
        assert isinstance(failure, TenantIdentityError)
        assert failure.code == "tenant_identity_mismatch"
    finally:
        await engine.dispose()

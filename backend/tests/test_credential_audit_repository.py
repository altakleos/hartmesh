"""Tenant-bound, bounded credential audit observations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.base import Base
from deerflow.persistence.credential_audit import (
    CredentialAuditRepository,
    InMemoryCredentialAuditRepository,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1


@pytest_asyncio.fixture
async def audit_env(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/audit.db")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    tenant = TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference()
    yield CredentialAuditRepository(sf, tenant=tenant), sf, tenant
    await engine.dispose()


@pytest.mark.asyncio
async def test_repeated_use_is_aggregated_and_safe(audit_env) -> None:
    repo, _sf, _tenant = audit_env
    now = datetime(2026, 9, 2, 10, 0, tzinfo=UTC)
    for minute in range(100):
        await repo.record(
            method="personal_access_token",
            action="authenticated",
            credential_ref="018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
            actor_digest="a" * 64,
            authority_digest="b" * 64,
            route_category="runs",
            occurred_at=now + timedelta(minutes=minute),
        )

    observations = await repo.list_for_credential(
        "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
        limit=20,
    )

    assert len(observations) == 1
    assert observations[0]["event_count"] == 100
    assert observations[0]["action"] == "authenticated"
    rendered = repr(observations)
    assert "dfp_" not in rendered
    assert "token_digest" not in rendered
    assert "authorization" not in rendered.lower()


@pytest.mark.asyncio
async def test_out_of_order_use_preserves_true_aggregate_time_bounds(audit_env) -> None:
    repo, _sf, _tenant = audit_env
    credential_ref = "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89"
    later = datetime(2026, 9, 2, 18, 0, tzinfo=UTC)
    earlier = later - timedelta(hours=8)
    values = {
        "method": "personal_access_token",
        "action": "authenticated",
        "credential_ref": credential_ref,
        "actor_digest": "a" * 64,
        "authority_digest": "b" * 64,
        "route_category": "runs",
    }

    await repo.record(**values, occurred_at=later, retention_now=later)
    await repo.record(**values, occurred_at=earlier, retention_now=later)

    [observation] = await repo.list_for_credential(credential_ref)
    assert observation["first_occurred_at"] == earlier.isoformat()
    assert observation["last_occurred_at"] == later.isoformat()
    assert observation["event_count"] == 2


@pytest.mark.asyncio
async def test_failed_unknown_credentials_aggregate_without_candidate_material(audit_env) -> None:
    repo, _sf, _tenant = audit_env
    for _ in range(5):
        await repo.record(
            method="personal_access_token",
            action="authentication_failed",
            credential_ref=None,
            actor_digest=None,
            authority_digest=None,
            route_category="threads",
            reason_code="credential_invalid",
        )

    observations = await repo.list_recent(limit=10)
    assert len(observations) == 1
    assert observations[0]["credential_ref"] is None
    assert observations[0]["event_count"] == 5
    assert observations[0]["reason_code"] == "credential_invalid"


@pytest.mark.asyncio
async def test_audit_queries_are_tenant_scoped_and_response_bounded(audit_env) -> None:
    repo_a, sf, _tenant = audit_env
    tenant_b = TenantIdentityV1.from_canonical_id("tenant-b").to_persisted_reference()
    repo_b = CredentialAuditRepository(sf, tenant=tenant_b)
    credential_ref = "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89"
    await repo_a.record(
        method="personal_access_token",
        action="created",
        credential_ref=credential_ref,
        actor_digest="a" * 64,
        authority_digest="b" * 64,
        route_category="credential_management",
    )

    assert await repo_b.list_for_credential(credential_ref, limit=10) == []
    with pytest.raises(ValueError, match="between 1 and 100"):
        await repo_a.list_recent(limit=101)


@pytest.mark.asyncio
async def test_retention_prunes_old_aggregates(audit_env) -> None:
    _repo, sf, tenant = audit_env
    repo = CredentialAuditRepository(sf, tenant=tenant, retention_days=90)
    credential_ref = "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89"
    old = datetime(2025, 1, 1, tzinfo=UTC)
    await repo.record(
        method="personal_access_token",
        action="authenticated",
        credential_ref=credential_ref,
        actor_digest="a" * 64,
        authority_digest="b" * 64,
        route_category="runs",
        occurred_at=old,
        retention_now=old,
    )
    await repo.record(
        method="personal_access_token",
        action="revoked",
        credential_ref=credential_ref,
        actor_digest="a" * 64,
        authority_digest="b" * 64,
        route_category="credential_management",
        occurred_at=old + timedelta(days=100),
        retention_now=old + timedelta(days=100),
    )

    observations = await repo.list_for_credential(credential_ref, limit=10)
    assert [item["action"] for item in observations] == ["revoked"]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sql", "memory"])
async def test_audit_adapters_reject_bearer_material_as_credential_reference(
    audit_env,
    backend,
) -> None:
    sql_repo, _sf, tenant = audit_env
    repo = sql_repo if backend == "sql" else InMemoryCredentialAuditRepository(tenant=tenant)

    with pytest.raises(ValueError, match="credential_ref"):
        await repo.record(
            method="personal_access_token",
            action="authenticated",
            credential_ref="dfp_not-a-public-reference",
            actor_digest=None,
            authority_digest=None,
            route_category="runs",
        )


@pytest.mark.asyncio
async def test_best_effort_repository_failure_never_logs_rejected_material(
    audit_env,
    caplog,
) -> None:
    sql_repo, sf, tenant = audit_env
    from deerflow.persistence.personal_access_tokens import (
        PersonalAccessTokenRepository,
    )

    pat_repo = PersonalAccessTokenRepository(
        sf,
        tenant=tenant,
        audit_repository=sql_repo,
    )
    rejected = "dfp_must-never-reach-a-log"

    with caplog.at_level("DEBUG"):
        await pat_repo.record_audit_best_effort(
            method="personal_access_token",
            action="authenticated",
            credential_ref=rejected,
            actor_digest=None,
            authority_digest=None,
            route_category="runs",
        )

    assert rejected not in caplog.text

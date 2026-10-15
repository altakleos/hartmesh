"""Operator run reads use the real database without a Gateway task registry."""

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from _storage_spaces_test_support import make_storage_fixture

from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def operator_runs(tmp_path, request):
    async for _files, sf, _catalog in make_storage_fixture(tmp_path, request):
        async with sf.kw["bind"].begin() as connection:
            await connection.run_sync(lambda c: RunRow.__table__.create(c))
            await connection.run_sync(lambda c: RunChangeClockRow.__table__.create(c))
        yield RunRepository(sf)


@pytest.mark.asyncio
async def test_list_inflight_by_metadata_returns_only_unfinished_leased_runs_with_literal_key_and_value(operator_runs):
    repo = operator_runs
    now = datetime.now(UTC)
    live = (now + timedelta(minutes=5)).isoformat()
    expired = (now - timedelta(minutes=1)).isoformat()
    key = "agent_instance_id"
    rows = [
        ("pending-a", "pending", {key: "alpha"}, live, "run"),
        ("running-a", "running", {key: "alpha"}, live, "run"),
        ("running-b", "running", {key: "beta"}, live, "run"),
        ("expired", "running", {key: "alpha"}, expired, "run"),
        ("no-lease", "running", {key: "alpha"}, None, "run"),
        ("success", "success", {key: "alpha"}, live, "run"),
        ("error", "error", {key: "alpha"}, live, "run"),
        ("interrupted", "interrupted", {key: "alpha"}, live, "run"),
        ("checkpoint", "running", {key: "alpha"}, live, "checkpoint_write"),
        ("missing", "running", {"other": "alpha"}, live, "run"),
        ("null", "running", {key: None}, live, "run"),
        ("empty", "running", {key: ""}, live, "run"),
        ("literal-key", "running", {"literal.key_%": "alpha"}, live, "run"),
    ]
    for identifier, status, metadata, lease, kind in rows:
        await repo.put(identifier, thread_id="thread-" + identifier, user_id=None, status=status, metadata=metadata, lease_expires_at=lease, owner_worker_id="worker", operation_kind=kind)
    # Creation-clock skew cannot conceal a genuinely leased unfinished run.
    await repo.put("future", thread_id="future", user_id=None, metadata={key: "alpha"}, status="pending", lease_expires_at=live, created_at=(now + timedelta(days=1)).isoformat())

    assert {row["run_id"] for row in await repo.list_inflight_by_metadata(key)} == {"pending-a", "running-a", "running-b", "null", "empty", "future"}
    assert {row["run_id"] for row in await repo.list_inflight_by_metadata(key, "alpha")} == {"pending-a", "running-a", "future"}
    assert [row["run_id"] for row in await repo.list_inflight_by_metadata(key, "")] == ["empty"]
    assert [row["run_id"] for row in await repo.list_inflight_by_metadata("literal.key_%", "alpha")] == ["literal-key"]
    assert await repo.list_inflight_by_metadata("absent") == []

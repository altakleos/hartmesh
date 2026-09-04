"""Evidence snapshots over the production SQL run and event repositories."""

from __future__ import annotations

import os

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url
from test_gateway_run_evidence_snapshot import (
    _OWNER,
    _RUN,
    _THREAD,
    _events,
    _request,
    _row,
)

from app.gateway.run_evidence import GatewayRunEvidenceSnapshotReader
from deerflow.persistence.base import Base
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.run_evidence import RunEvidenceSnapshotService
from deerflow.runtime.runs.store.base import (
    BindAssemblyEvidenceOutcome,
    LifecycleTransition,
    LifecycleType,
)

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")
_WORKER = "worker-run-evidence"


async def _clear_fixture_rows(session_factory) -> None:
    async with session_factory() as session:
        await session.execute(delete(RunEventRow).where(RunEventRow.run_id == _RUN))
        await session.execute(delete(RunRow).where(RunRow.run_id == _RUN))
        await session.commit()


async def _assert_sql_snapshot_contract(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    await _clear_fixture_rows(session_factory)
    row = _row()
    run_store = RunRepository(session_factory, tenant=_request().tenant)
    event_store = DbRunEventStore(session_factory, tenant=_request().tenant)
    try:
        await run_store.put(
            _RUN,
            thread_id=_THREAD,
            user_id=_OWNER,
            status="running",
            owner_worker_id=_WORKER,
            created_at=row["created_at"],
            kwargs=row["kwargs"],
            origin_json=row["origin_json"],
            principal_projection_json=row["principal_projection_json"],
            principal_projection_digest=row["principal_projection_digest"],
            base_origin_digest=row["base_origin_digest"],
            accepted_context_digest=row["accepted_context_digest"],
            tenant=_request().tenant,
            agent_revision_json=row["agent_revision_json"],
            agent_revision_digest=row["agent_revision_digest"],
            extension_generation=row["extension_generation"],
            decision_evidence_json=row["decision_evidence_json"],
            request_digest=row["request_digest"],
            request_digest_version=row["request_digest_version"],
        )
        running = await run_store.get(_RUN, user_id=_OWNER)
        assert running is not None
        outcome = await run_store.bind_assembly_evidence(
            _RUN,
            owner_id=_WORKER,
            lease_epoch=running["state_version"],
            evidence_json=row["assembly_evidence_json"],
            evidence_digest=row["assembly_evidence_digest"],
        )
        assert outcome is BindAssemblyEvidenceOutcome.bound
        terminal = await run_store.transition_run_atomic(
            _RUN,
            expected_state_version=running["state_version"],
            expected_statuses=("running",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.succeeded,
                status="success",
                stop_reason="completed",
            ),
            user_id=_OWNER,
        )
        assert terminal.applied
        await event_store.put_batch(
            [
                {
                    **event,
                    "user_id": _OWNER,
                }
                for event in _events()
            ]
        )

        reader = GatewayRunEvidenceSnapshotReader(
            run_store=run_store,
            event_store=event_store,
        )
        snapshot = await RunEvidenceSnapshotService(reader).build(_request())

        assert snapshot.terminal_status == "success"
        assert snapshot.lifecycle["high_water_mark"] == 2
        assert snapshot.artifact_paths == ("/mnt/user-data/outputs/report.txt",)
        assert snapshot.to_manifest(()).manifest_digest

        source = await reader.read(_request())
        await event_store.put_batch(
            [
                {
                    "thread_id": _THREAD,
                    "run_id": _RUN,
                    "user_id": _OWNER,
                    "event_type": "late.evidence",
                    "category": "trace",
                    "content": {},
                }
            ]
        )
        assert await reader.revalidate(_request(), source) is False
    finally:
        await _clear_fixture_rows(session_factory)


@pytest.mark.anyio
async def test_sqlite_run_evidence_snapshot_matches_repository_contract(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'run-evidence.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        await _assert_sql_snapshot_contract(engine)
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for the evidence snapshot gate",
)
async def test_postgres_run_evidence_snapshot_matches_repository_contract() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        await _assert_sql_snapshot_contract(engine)
    finally:
        await engine.dispose()

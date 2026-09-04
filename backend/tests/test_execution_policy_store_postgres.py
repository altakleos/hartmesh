"""Real-PostgreSQL contract for fenced execution-policy state.

Runs the same scenario bodies as ``test_execution_policy_store`` against a live
PostgreSQL schema so the ``FOR UPDATE`` CAS path, concurrent-claim behavior,
and cross-worker takeover fencing are proven on the production engine. Missing
infrastructure skips the module; that is an unpassed qualification gate, not
evidence.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
import pytest_asyncio
from sqlalchemy import text
from test_execution_policy_store import (
    cross_worker_takeover_yields_one_accepted_stop,
)

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import close_engine, get_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.run import RunRepository
from deerflow.runtime.execution_policy import (
    ExecutionBudgetV1,
    ExecutionPolicyEvaluator,
    ExecutionPolicyObservationV1,
    ExecutionPolicyStateV1,
)
from deerflow.runtime.runs.store.base import ApplyExecutionPolicyStateOutcome

POSTGRES_URL = os.environ.get("TEST_POSTGRES_URI")

pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="requires TEST_POSTGRES_URI (real Postgres for policy-state fencing)",
)


def _postgres_url(url: str) -> str:
    parts = urlsplit(url)
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key not in {"sslmode", "channel_binding"}])
    return urlunsplit(parts._replace(query=query))


@pytest_asyncio.fixture()
async def postgres_run_repository():
    assert POSTGRES_URL is not None
    schema = f"policy_state_{uuid.uuid4().hex}"
    await init_engine_from_config(
        DatabaseConfig(
            backend="postgres",
            postgres_url=_postgres_url(POSTGRES_URL),
            postgres_schema=schema,
        )
    )
    session_factory = get_session_factory()
    assert session_factory is not None
    try:
        yield RunRepository(session_factory)
    finally:
        engine = get_engine()
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await close_engine()


async def _expire_lease(store: RunRepository, run_id: str) -> None:
    engine = get_engine()
    async with engine.begin() as connection:
        await connection.execute(
            text("UPDATE runs SET lease_expires_at = :expired WHERE run_id = :run_id"),
            {"expired": "2000-01-01T00:00:00+00:00", "run_id": run_id},
        )


@pytest.mark.asyncio
async def test_postgres_policy_state_cas_accepts_exactly_one_concurrent_writer(
    postgres_run_repository,
) -> None:
    store = postgres_run_repository
    await store.put(
        "run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        lease_expires_at="2999-01-01T00:00:00+00:00",
    )
    assert await store.start_run("run-1") is True
    row = await store.get("run-1")
    assert row is not None
    lease_epoch = row["state_version"]

    budget = ExecutionBudgetV1.build()
    initial = ExecutionPolicyStateV1.initial(budget)
    next_state = (
        ExecutionPolicyEvaluator()
        .evaluate(
            budget,
            initial,
            ExecutionPolicyObservationV1(kind="turn"),
        )
        .next_state
    )
    assert (
        await store.apply_execution_policy_state(
            "run-1",
            owner_id="worker-1",
            lease_epoch=lease_epoch,
            expected_digest=None,
            state_json=initial.to_json(),
            state_digest=initial.digest,
        )
        is ApplyExecutionPolicyStateOutcome.applied
    )

    outcomes = await asyncio.gather(
        *(
            store.apply_execution_policy_state(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                expected_digest=initial.digest,
                state_json=next_state.to_json(),
                state_digest=next_state.digest,
            )
            for _ in range(2)
        )
    )
    assert sorted(outcomes) == sorted(
        [
            ApplyExecutionPolicyStateOutcome.applied,
            ApplyExecutionPolicyStateOutcome.conflict,
        ]
    )
    row = await store.get("run-1")
    assert row is not None
    assert row["execution_policy_state_digest"] == next_state.digest
    assert ExecutionPolicyStateV1.from_json(row["execution_policy_state_json"]) == next_state

    assert (
        await store.apply_execution_policy_state(
            "run-1",
            owner_id="stale-worker",
            lease_epoch=lease_epoch,
            expected_digest=next_state.digest,
            state_json=next_state.to_json(),
            state_digest=next_state.digest,
        )
        is ApplyExecutionPolicyStateOutcome.ownership_lost
    )


@pytest.mark.asyncio
async def test_postgres_cross_worker_takeover_yields_one_accepted_stop(
    postgres_run_repository,
) -> None:
    await cross_worker_takeover_yields_one_accepted_stop(
        postgres_run_repository,
        expire_lease=lambda store, run_id: _expire_lease(store, run_id),
    )

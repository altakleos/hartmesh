"""Tests for RunRepository (SQLAlchemy-backed RunStore).

Uses a temp SQLite DB to test ORM-backed CRUD operations.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.run import RunRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.sql_clock import database_wall_clock_expression
from deerflow.runtime import CancelOutcome, RunManager, RunStatus, ThreadOperationKind
from deerflow.runtime.runs.manager import ConflictError
from deerflow.runtime.runs.store.base import (
    ExecutionTakeoverOutcome,
    LeaseClockAuthority,
    LifecycleTransition,
    LifecycleType,
    RecoveryPolicy,
    RunStore,
    ThreadOperationReleaseOutcome,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore


async def _make_repo(tmp_path):
    from deerflow.persistence.engine import get_session_factory, init_engine

    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    await init_engine("sqlite", url=url, sqlite_dir=str(tmp_path))
    return RunRepository(get_session_factory())


async def _cleanup():
    from deerflow.persistence.engine import close_engine

    await close_engine()


def test_auxiliary_release_uses_statement_time_after_lock_acquisition() -> None:
    """Lease fencing cannot use PostgreSQL's transaction-start timestamp."""

    postgres_sql = str(
        select(database_wall_clock_expression("postgresql")).compile(
            dialect=postgresql.dialect(),
        )
    ).lower()
    sqlite_sql = str(
        select(database_wall_clock_expression("sqlite")).compile(
            dialect=sqlite.dialect(),
        )
    ).lower()

    assert "clock_timestamp" in postgres_sql
    assert "now()" not in postgres_sql
    assert "strftime" in sqlite_sql
    assert "current_timestamp" not in sqlite_sql


class _CustomRunStoreWithoutProgress(RunStore):
    def __init__(self):
        self.legacy_atomic_calls = 0

    async def put(self, *args, **kwargs):
        return None

    async def get(self, *args, **kwargs):
        return None

    async def list_by_thread(self, *args, **kwargs):
        return []

    async def update_status(self, *args, **kwargs):
        return None

    async def start_run(self, *args, **kwargs):
        return False

    async def delete(self, *args, **kwargs):
        return None

    async def update_model_name(self, *args, **kwargs):
        return None

    async def update_run_completion(self, *args, **kwargs):
        return None

    async def list_pending(self, *args, **kwargs):
        return []

    async def list_inflight(self, *args, **kwargs):
        return []

    async def aggregate_tokens_by_thread(self, *args, **kwargs):
        return {}

    async def update_lease(self, *args, **kwargs):
        return True

    async def list_inflight_with_expired_lease(self, *args, **kwargs):
        return []

    async def create_run_atomic(self, *args, **kwargs):
        self.legacy_atomic_calls += 1
        return {}, []

    async def claim_for_takeover(self, *args, **kwargs):
        return False


@pytest.mark.anyio
async def test_update_run_progress_defaults_to_noop_for_custom_store():
    store = _CustomRunStoreWithoutProgress()

    await store.update_run_progress("r1", total_tokens=1)


def test_run_store_lease_clock_authority_capabilities_are_explicit() -> None:
    assert _CustomRunStoreWithoutProgress().lease_clock_authority is LeaseClockAuthority.process_v1
    assert MemoryRunStore().lease_clock_authority is LeaseClockAuthority.process_v1
    assert RunRepository.lease_clock_authority is LeaseClockAuthority.database_v1


@pytest.mark.anyio
@pytest.mark.parametrize("clock_offset_hours", (-24, 24))
@pytest.mark.parametrize("keyed", (False, True))
async def test_sql_duration_admission_uses_database_clock(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    clock_offset_hours: int,
    keyed: bool,
) -> None:
    from deerflow.persistence.run import sql as run_sql

    repo = await _make_repo(tmp_path)
    actual_now = datetime.now(UTC)

    class SkewedProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = actual_now + timedelta(hours=clock_offset_hours)
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(run_sql, "datetime", SkewedProcessDateTime)
    common = {
        "thread_id": f"thread-duration-{clock_offset_hours}-{keyed}",
        "owner_worker_id": "worker-duration",
        "lease_duration_seconds": 60,
    }
    try:
        if keyed:
            result = await repo.ensure_run_atomic(
                f"run-duration-{clock_offset_hours}-{keyed}",
                external_scope="service:test",
                external_key=f"key-{clock_offset_hours}",
                request_digest="a" * 64,
                request_digest_version="sha256-canonical-json-v1",
                caller_intent_json={"version": 1},
                caller_intent_digest="b" * 64,
                caller_intent_digest_version="caller-intent-canonical-json-v1",
                **common,
            )
            row = result.row
        else:
            row, _ = await repo.create_thread_operation_atomic(
                f"run-duration-{clock_offset_hours}-{keyed}",
                **common,
            )

        deadline = datetime.fromisoformat(row["lease_expires_at"])
        assert actual_now + timedelta(seconds=55) <= deadline
        assert deadline <= actual_now + timedelta(seconds=65)
        admitted_at = datetime.fromisoformat(row["created_at"])
        assert actual_now - timedelta(seconds=5) <= admitted_at
        assert admitted_at <= actual_now + timedelta(seconds=5)

        assert await repo.update_lease(
            row["run_id"],
            owner_worker_id="worker-duration",
            lease_expires_at=(actual_now - timedelta(seconds=1)).isoformat(),
        )
        expired = await repo.list_inflight_with_expired_lease(grace_seconds=0)
        assert row["run_id"] in {candidate["run_id"] for candidate in expired}
    finally:
        await _cleanup()


@pytest.mark.anyio
@pytest.mark.parametrize("clock_offset_hours", (-24, 24))
async def test_sql_duration_renewal_and_owner_authority_use_database_clock(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    clock_offset_hours: int,
) -> None:
    from deerflow.persistence.run import sql as run_sql

    repo = await _make_repo(tmp_path)
    actual_now = datetime.now(UTC)
    try:
        await repo.put(
            "run-duration-renewal",
            thread_id="thread-duration-renewal",
            status="running",
            owner_worker_id="worker-duration",
            lease_expires_at=(actual_now + timedelta(minutes=5)).isoformat(),
        )
        admitted = await repo.get("run-duration-renewal")
        assert admitted is not None

        class SkewedProcessDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = actual_now + timedelta(hours=clock_offset_hours)
                return value if tz is not None else value.replace(tzinfo=None)

        monkeypatch.setattr(run_sql, "datetime", SkewedProcessDateTime)
        assert await repo.execution_owner_authorized(
            "run-duration-renewal",
            owner_worker_id="worker-duration",
            state_version=admitted["state_version"],
        )

        assert await repo.update_lease(
            "run-duration-renewal",
            owner_worker_id="worker-duration",
            lease_duration_seconds=90,
        )
        updated = await repo.get("run-duration-renewal")
        assert updated is not None
        updated_deadline = datetime.fromisoformat(updated["lease_expires_at"])
        assert actual_now + timedelta(seconds=85) <= updated_deadline
        assert updated_deadline <= actual_now + timedelta(seconds=95)

        renewal = await repo.renew_lease(
            "run-duration-renewal",
            owner_worker_id="worker-duration",
            lease_duration_seconds=60,
        )

        assert renewal.renewed is True
        assert renewal.lease_expires_at is not None
        deadline = datetime.fromisoformat(renewal.lease_expires_at)
        assert actual_now + timedelta(seconds=55) <= deadline
        assert deadline <= actual_now + timedelta(seconds=65)
        retained = await repo.get("run-duration-renewal")
        assert retained is not None
        assert retained["lease_expires_at"] == renewal.lease_expires_at

        assert await repo.update_lease(
            "run-duration-renewal",
            owner_worker_id="worker-duration",
            lease_expires_at=(actual_now - timedelta(seconds=1)).isoformat(),
        )
        assert not await repo.execution_owner_authorized(
            "run-duration-renewal",
            owner_worker_id="worker-duration",
            state_version=admitted["state_version"],
        )
    finally:
        await _cleanup()


@pytest.mark.anyio
@pytest.mark.parametrize("clock_offset_hours", (-24, 24))
async def test_sql_duration_takeover_uses_database_clock(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    clock_offset_hours: int,
) -> None:
    from deerflow.persistence.run import sql as run_sql

    repo = await _make_repo(tmp_path)
    actual_now = datetime.now(UTC)
    try:
        await repo.put(
            "run-duration-takeover",
            thread_id="thread-duration-takeover",
            status="running",
            owner_worker_id="worker-before",
            lease_expires_at=(actual_now - timedelta(minutes=1)).isoformat(),
            recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
            recovery_payload_json={"version": 1},
        )
        admitted = await repo.get("run-duration-takeover")
        assert admitted is not None

        class SkewedProcessDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = actual_now + timedelta(hours=clock_offset_hours)
                return value if tz is not None else value.replace(tzinfo=None)

        monkeypatch.setattr(run_sql, "datetime", SkewedProcessDateTime)
        claim = await repo.claim_for_execution_takeover(
            "run-duration-takeover",
            new_owner_worker_id="worker-after",
            lease_duration_seconds=60,
            grace_seconds=0,
            expected_state_version=admitted["state_version"],
        )

        assert claim.outcome is ExecutionTakeoverOutcome.claimed
        assert claim.row is not None
        deadline = datetime.fromisoformat(claim.row["lease_expires_at"])
        assert actual_now + timedelta(seconds=55) <= deadline
        assert deadline <= actual_now + timedelta(seconds=65)
    finally:
        await _cleanup()


@pytest.mark.anyio
async def test_sql_duration_renewal_rechecks_expiry_after_lock_wait(tmp_path) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'duration-renewal-lock.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = RunRepository(session_factory)
    original_deadline = datetime.now(UTC) + timedelta(milliseconds=250)
    try:
        await store.put(
            "run-duration-renewal-lock",
            thread_id="thread-duration-renewal-lock",
            status="running",
            owner_worker_id="worker-duration",
            lease_expires_at=original_deadline.isoformat(),
        )
        async with session_factory() as blocker:
            await blocker.execute(text("BEGIN IMMEDIATE"))
            locked = await blocker.scalar(
                select(RunRow).where(
                    RunRow.run_id == "run-duration-renewal-lock",
                )
            )
            assert locked is not None
            renewal_task = asyncio.create_task(
                store.renew_lease(
                    "run-duration-renewal-lock",
                    owner_worker_id="worker-duration",
                    lease_duration_seconds=60,
                )
            )
            await asyncio.sleep(0.4)
            assert renewal_task.done() is False
            await blocker.commit()

        renewal = await asyncio.wait_for(renewal_task, timeout=2)
        assert renewal.renewed is False
        retained = await store.get("run-duration-renewal-lock")
        assert retained is not None
        assert datetime.fromisoformat(retained["lease_expires_at"]) == original_deadline
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_memory_duration_path_uses_process_clock_and_returns_deadlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.runtime.runs.store import memory as memory_store

    actual_now = datetime.now(UTC)

    class FastProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = actual_now + timedelta(hours=24)
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(memory_store, "datetime", FastProcessDateTime)
    store = MemoryRunStore()
    row, _ = await store.create_thread_operation_atomic(
        "run-memory-duration",
        thread_id="thread-memory-duration",
        owner_worker_id="worker-memory",
        lease_duration_seconds=60,
    )
    deadline = datetime.fromisoformat(row["lease_expires_at"])
    assert deadline == actual_now + timedelta(hours=24, seconds=60)

    renewal = await store.renew_lease(
        "run-memory-duration",
        owner_worker_id="worker-memory",
        lease_duration_seconds=120,
    )
    assert renewal.renewed is True
    assert renewal.lease_expires_at == (actual_now + timedelta(hours=24, seconds=120)).isoformat()
    assert await store.execution_owner_authorized(
        "run-memory-duration",
        owner_worker_id="worker-memory",
        state_version=row["state_version"],
    )

    takeover_candidate, _ = await store.create_thread_operation_atomic(
        "run-memory-duration-takeover",
        thread_id="thread-memory-duration-takeover",
        owner_worker_id="worker-before",
        lease_expires_at=(actual_now - timedelta(seconds=1)).isoformat(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json={"version": 1},
    )
    takeover = await store.claim_for_execution_takeover(
        "run-memory-duration-takeover",
        new_owner_worker_id="worker-after",
        lease_duration_seconds=60,
        grace_seconds=0,
        expected_state_version=takeover_candidate["state_version"],
    )
    assert takeover.outcome is ExecutionTakeoverOutcome.claimed
    assert takeover.row is not None
    assert takeover.row["lease_expires_at"] == (actual_now + timedelta(hours=24, seconds=60)).isoformat()

    with pytest.raises(ValueError, match="mutually exclusive"):
        await store.update_lease(
            "run-memory-duration",
            owner_worker_id="worker-memory",
            lease_expires_at=deadline.isoformat(),
            lease_duration_seconds=60,
        )
    with pytest.raises(ValueError, match="positive integer"):
        await store.update_lease(
            "run-memory-duration",
            owner_worker_id="worker-memory",
            lease_duration_seconds=True,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_memory_atomic_replacement_rejects_expired_exact_two_predecessor(
    strategy: str,
) -> None:
    """Generic replacement cannot consume exact-two execution authority."""

    store = MemoryRunStore()
    predecessor, _ = await store.create_thread_operation_atomic(
        "run-memory-exact-two-predecessor",
        thread_id="thread-memory-exact-two-predecessor",
        owner_worker_id="worker-before",
        lease_expires_at=(datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json={"version": 1},
    )

    with pytest.raises(ConflictError) as raised:
        await store.create_thread_operation_atomic(
            "run-memory-exact-two-replacement",
            thread_id=predecessor["thread_id"],
            owner_worker_id="worker-after",
            multitask_strategy=strategy,
            grace_seconds=0,
        )

    assert raised.value.active_run_id == predecessor["run_id"]
    assert await store.get(predecessor["run_id"]) == predecessor
    assert await store.get("run-memory-exact-two-replacement") is None


@pytest.mark.anyio
async def test_legacy_create_run_atomic_store_remains_compatible():
    store = _CustomRunStoreWithoutProgress()

    await store.create_thread_operation_atomic(
        "r1",
        thread_id="t1",
        owner_worker_id="worker-1",
        lease_expires_at=None,
    )

    assert store.legacy_atomic_calls == 1
    with pytest.raises(NotImplementedError, match="cannot create non-run"):
        await store.create_thread_operation_atomic(
            "checkpoint-write-1",
            thread_id="t1",
            owner_worker_id="worker-1",
            lease_expires_at=None,
            operation_kind=ThreadOperationKind.checkpoint_write,
        )


class TestRunRepository:
    @pytest.mark.anyio
    async def test_put_and_get(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="pending")
        row = await repo.get("r1")
        assert row is not None
        assert row["run_id"] == "r1"
        assert row["thread_id"] == "t1"
        assert row["status"] == "pending"
        await _cleanup()

    @pytest.mark.anyio
    @pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
    async def test_atomic_replacement_rejects_expired_exact_two_predecessor(
        self,
        tmp_path,
        strategy: str,
    ) -> None:
        """SQL replacement leaves an expired exact-two row byte-for-byte intact."""

        repo = await _make_repo(tmp_path)
        try:
            predecessor, _ = await repo.create_thread_operation_atomic(
                "run-sql-exact-two-predecessor",
                thread_id="thread-sql-exact-two-predecessor",
                owner_worker_id="worker-before",
                lease_expires_at=(datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
                recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
                recovery_payload_json={"version": 1},
            )

            with pytest.raises(ConflictError) as raised:
                await repo.create_thread_operation_atomic(
                    "run-sql-exact-two-replacement",
                    thread_id=predecessor["thread_id"],
                    owner_worker_id="worker-after",
                    multitask_strategy=strategy,
                    grace_seconds=0,
                )

            assert raised.value.active_run_id == predecessor["run_id"]
            assert await repo.get(predecessor["run_id"]) == predecessor
            assert await repo.get("run-sql-exact-two-replacement") is None
        finally:
            await _cleanup()

    @pytest.mark.anyio
    async def test_put_is_idempotent_for_retried_writes(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", assistant_id="old-agent", status="pending")

        await repo.put("r1", thread_id="t1", assistant_id="new-agent", status="running", error="retry")

        row = await repo.get("r1")
        assert row["assistant_id"] == "new-agent"
        # Snapshot retries may refresh non-lifecycle fields, but status,
        # reason, and version remain owned by the transition primitive.
        assert row["status"] == "pending"
        assert row["error"] is None
        assert row["state_version"] == 1
        assert len(await repo.list_lifecycle_events(run_id="r1")) == 1
        await _cleanup()

    @pytest.mark.anyio
    async def test_get_missing_returns_none(self, tmp_path):
        repo = await _make_repo(tmp_path)
        assert await repo.get("nope") is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_status(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1")
        updated = await repo.update_status("r1", "running")
        row = await repo.get("r1")
        assert updated is True
        assert row["status"] == "running"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_status_returns_false_for_missing_row(self, tmp_path):
        repo = await _make_repo(tmp_path)
        updated = await repo.update_status("missing", "error", error="lost")
        assert updated is False
        await _cleanup()

    @pytest.mark.anyio
    async def test_start_run_only_updates_pending_rows(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("pending-run", thread_id="t1", status="pending")
        await repo.put("cancelled-run", thread_id="t2", status="pending")
        await repo.update_status("cancelled-run", "interrupted")

        assert await repo.start_run("pending-run") is True
        assert await repo.start_run("cancelled-run") is False

        pending_row = await repo.get("pending-run")
        cancelled_row = await repo.get("cancelled-run")
        assert pending_row["status"] == "running"
        assert cancelled_row["status"] == "interrupted"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_status_with_error(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1")
        await repo.update_status("r1", "error", error="boom")
        row = await repo.get("r1")
        assert row["status"] == "error"
        assert row["error"] == "boom"
        await _cleanup()

    @pytest.mark.anyio
    async def test_list_by_thread(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="success")
        await repo.put("r2", thread_id="t1", status="pending")
        await repo.put("r3", thread_id="t2", status="pending")
        rows = await repo.list_by_thread("t1")
        assert len(rows) == 2
        assert all(r["thread_id"] == "t1" for r in rows)
        await _cleanup()

    @pytest.mark.anyio
    async def test_run_history_excludes_internal_thread_operations(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="success")
        await repo.put(
            "checkpoint-write-1",
            thread_id="t1",
            status="error",
            operation_kind=ThreadOperationKind.checkpoint_write,
        )

        rows = await repo.list_by_thread("t1")

        assert [row["run_id"] for row in rows] == ["r1"]
        await _cleanup()

    @pytest.mark.anyio
    async def test_list_by_thread_owner_filter(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", user_id="alice", status="success")
        await repo.put("r2", thread_id="t1", user_id="bob", status="pending")
        rows = await repo.list_by_thread("t1", user_id="alice")
        assert len(rows) == 1
        assert rows[0]["user_id"] == "alice"
        await _cleanup()

    @pytest.mark.anyio
    async def test_delete(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1")
        await repo.delete("r1")
        assert await repo.get("r1") is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_delete_nonexistent_is_noop(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.delete("nope")  # should not raise
        await _cleanup()

    @pytest.mark.anyio
    async def test_list_pending(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="pending")
        await repo.put("r2", thread_id="t2", status="running")
        await repo.put("r3", thread_id="t3", status="pending")
        pending = await repo.list_pending()
        assert len(pending) == 2
        assert all(r["status"] == "pending" for r in pending)
        await _cleanup()

    @pytest.mark.anyio
    async def test_list_inflight_returns_pending_and_running_before_cutoff(self, tmp_path):
        repo = await _make_repo(tmp_path)
        # Each thread can hold at most one pending/running row (partial unique
        # index ``uq_runs_thread_active``), so spread the inflight rows across
        # distinct threads to exercise the before-cutoff filter.
        await repo.put("pending-old", thread_id="t1", status="pending", created_at="2026-01-01T00:00:00+00:00")
        await repo.put("running-old", thread_id="t2", status="running", created_at="2026-01-01T00:00:01+00:00")
        await repo.put("success-old", thread_id="t3", status="success", created_at="2026-01-01T00:00:02+00:00")
        await repo.put("pending-new", thread_id="t4", status="pending", created_at="2026-01-01T00:00:03+00:00")

        inflight = await repo.list_inflight(before="2026-01-01T00:00:02+00:00")

        assert [row["run_id"] for row in inflight] == ["pending-old", "running-old"]
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_completion(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="success")
        updated = await repo.update_run_completion(
            "r1",
            status="success",
            total_input_tokens=100,
            total_output_tokens=50,
            total_tokens=150,
            llm_call_count=2,
            lead_agent_tokens=120,
            subagent_tokens=20,
            middleware_tokens=10,
            message_count=3,
            last_ai_message="The answer is 42",
            first_human_message="What is the meaning?",
        )
        row = await repo.get("r1")
        assert updated is True
        assert row["status"] == "success"
        assert row["total_tokens"] == 150
        assert row["llm_call_count"] == 2
        assert row["lead_agent_tokens"] == 120
        assert row["message_count"] == 3
        assert row["last_ai_message"] == "The answer is 42"
        assert row["first_human_message"] == "What is the meaning?"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_completion_requires_exact_terminal_projection(
        self,
        tmp_path,
    ):
        repo = await _make_repo(tmp_path)
        await repo.put(
            "r1",
            thread_id="t1",
            status="running",
            owner_worker_id="worker-a",
            lease_expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        )
        active = await repo.get("r1")
        transition = await repo.transition_owned_run_atomic(
            "r1",
            expected_state_version=active["state_version"],
            expected_statuses=("running",),
            expected_owner_worker_id="worker-a",
            require_unexpired_lease=True,
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.succeeded,
                status="success",
            ),
        )
        assert transition.applied is True
        terminal = transition.row
        assert terminal is not None

        assert (
            await repo.update_run_completion(
                "r1",
                status="success",
                total_tokens=999,
            )
            is False
        )
        assert (
            await repo.update_run_completion(
                "r1",
                status="success",
                total_tokens=999,
                expected_owner_worker_id="worker-a",
                expected_active_state_version=active["state_version"] + 1,
                expected_terminal_state_version=terminal["state_version"],
            )
            is False
        )
        assert await repo.update_run_completion(
            "r1",
            status="success",
            total_tokens=42,
            expected_owner_worker_id="worker-a",
            expected_active_state_version=active["state_version"],
            expected_terminal_state_version=terminal["state_version"],
        )
        stored = await repo.get("r1")
        assert stored["total_tokens"] == 42

    @pytest.mark.anyio
    async def test_update_run_completion_never_terminalizes_active_row(
        self,
        tmp_path,
    ):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="running")

        assert (
            await repo.update_run_completion(
                "r1",
                status="error",
                total_tokens=1,
            )
            is False
        )
        assert (await repo.get("r1"))["status"] == "running"

    @pytest.mark.anyio
    async def test_update_run_completion_returns_false_for_missing_row(self, tmp_path):
        repo = await _make_repo(tmp_path)
        updated = await repo.update_run_completion("missing", status="error", total_tokens=1)
        assert updated is False
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_completion_does_not_replace_terminal_status(self, tmp_path):
        """Late completion data cannot rewrite a peer's terminal outcome."""
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="running")
        await repo.update_status("r1", "error", error="peer takeover")

        updated = await repo.update_run_completion("r1", status="success", total_tokens=1)

        row = await repo.get("r1")
        assert updated is False
        assert row["status"] == "error"
        assert row["error"] == "peer takeover"
        await _cleanup()

    @pytest.mark.anyio
    async def test_metadata_preserved(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", metadata={"key": "value"})
        row = await repo.get("r1")
        assert row["metadata"] == {"key": "value"}
        await _cleanup()

    @pytest.mark.anyio
    async def test_kwargs_with_non_serializable(self, tmp_path):
        """kwargs containing non-JSON-serializable objects should be safely handled."""
        repo = await _make_repo(tmp_path)

        class Dummy:
            pass

        await repo.put("r1", thread_id="t1", kwargs={"obj": Dummy()})
        row = await repo.get("r1")
        assert "obj" in row["kwargs"]
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_completion_preserves_existing_fields(self, tmp_path):
        """update_run_completion does not overwrite thread_id or assistant_id."""
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", assistant_id="agent1", status="running")
        await repo.update_status("r1", "success")
        await repo.update_run_completion("r1", status="success", total_tokens=100)
        row = await repo.get("r1")
        assert row["thread_id"] == "t1"
        assert row["assistant_id"] == "agent1"
        assert row["total_tokens"] == 100
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_progress_keeps_status_running(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="running")
        await repo.update_run_progress(
            "r1",
            total_input_tokens=40,
            total_output_tokens=10,
            total_tokens=50,
            llm_call_count=1,
            message_count=2,
            last_ai_message="partial answer",
        )
        row = await repo.get("r1")
        assert row["status"] == "running"
        assert row["total_tokens"] == 50
        assert row["llm_call_count"] == 1
        assert row["message_count"] == 2
        assert row["last_ai_message"] == "partial answer"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_progress_preserves_omitted_fields(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="running")
        await repo.update_run_progress(
            "r1",
            total_input_tokens=40,
            total_output_tokens=10,
            total_tokens=50,
            llm_call_count=1,
            lead_agent_tokens=30,
            subagent_tokens=20,
            message_count=2,
        )

        await repo.update_run_progress("r1", total_tokens=60, last_ai_message="updated")

        row = await repo.get("r1")
        assert row["total_input_tokens"] == 40
        assert row["total_output_tokens"] == 10
        assert row["total_tokens"] == 60
        assert row["llm_call_count"] == 1
        assert row["lead_agent_tokens"] == 30
        assert row["subagent_tokens"] == 20
        assert row["message_count"] == 2
        assert row["last_ai_message"] == "updated"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_run_progress_skips_terminal_runs(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="running")
        await repo.update_status("r1", "success")
        await repo.update_run_completion("r1", status="success", total_tokens=100, llm_call_count=1)

        await repo.update_run_progress("r1", total_tokens=200, llm_call_count=2)

        row = await repo.get("r1")
        assert row["status"] == "success"
        assert row["total_tokens"] == 100
        assert row["llm_call_count"] == 1
        await _cleanup()

    @pytest.mark.anyio
    async def test_aggregate_tokens_by_thread_counts_completed_runs_only(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("success-run", thread_id="t1", status="running")
        await repo.update_status("success-run", "success")
        await repo.update_run_completion(
            "success-run",
            status="success",
            total_input_tokens=70,
            total_output_tokens=30,
            total_tokens=100,
            lead_agent_tokens=80,
            subagent_tokens=15,
            middleware_tokens=5,
        )
        await repo.put("error-run", thread_id="t1", status="running")
        await repo.update_status("error-run", "error")
        await repo.update_run_completion(
            "error-run",
            status="error",
            total_input_tokens=20,
            total_output_tokens=30,
            total_tokens=50,
            lead_agent_tokens=40,
            subagent_tokens=10,
        )
        await repo.put("running-run", thread_id="t1", status="running")
        await repo.update_run_progress(
            "running-run",
            total_input_tokens=900,
            total_output_tokens=99,
            total_tokens=999,
            lead_agent_tokens=999,
        )
        await repo.put("other-thread-run", thread_id="t2", status="running")
        await repo.update_status("other-thread-run", "success")
        await repo.update_run_completion(
            "other-thread-run",
            status="success",
            total_tokens=888,
            lead_agent_tokens=888,
        )

        agg = await repo.aggregate_tokens_by_thread("t1")

        assert agg["total_tokens"] == 150
        assert agg["total_input_tokens"] == 90
        assert agg["total_output_tokens"] == 60
        assert agg["total_runs"] == 2
        assert agg["by_model"] == {"unknown": {"tokens": 150, "runs": 2}}
        assert agg["by_caller"] == {
            "lead_agent": 120,
            "subagent": 25,
            "middleware": 5,
        }
        await _cleanup()

    @pytest.mark.anyio
    async def test_aggregate_tokens_by_thread_can_include_active_runs(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("success-run", thread_id="t1", status="running")
        await repo.update_status("success-run", "success")
        await repo.update_run_completion("success-run", status="success", total_tokens=100, lead_agent_tokens=100)
        await repo.put("running-run", thread_id="t1", status="running")
        await repo.update_run_progress("running-run", total_tokens=25, lead_agent_tokens=20, subagent_tokens=5)

        without_active = await repo.aggregate_tokens_by_thread("t1")
        with_active = await repo.aggregate_tokens_by_thread("t1", include_active=True)

        assert without_active["total_tokens"] == 100
        assert without_active["total_runs"] == 1
        assert with_active["total_tokens"] == 125
        assert with_active["total_runs"] == 2
        assert with_active["by_caller"] == {
            "lead_agent": 120,
            "subagent": 5,
            "middleware": 0,
        }
        await _cleanup()

    @pytest.mark.anyio
    async def test_list_by_thread_ordered_desc(self, tmp_path):
        """list_by_thread returns newest first."""
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", status="success", created_at="2024-01-01T00:00:00+00:00")
        await repo.put("r2", thread_id="t1", status="pending", created_at="2024-01-02T00:00:00+00:00")
        rows = await repo.list_by_thread("t1")
        assert rows[0]["run_id"] == "r2"
        assert rows[1]["run_id"] == "r1"
        await _cleanup()

    @pytest.mark.anyio
    async def test_list_by_thread_limit(self, tmp_path):
        repo = await _make_repo(tmp_path)
        # Only one row can be pending/running per thread; mark earlier ones
        # terminal so the partial unique index still holds.
        for i in range(4):
            await repo.put(f"r{i}", thread_id="t1", status="success")
        await repo.put("r4", thread_id="t1", status="pending")
        rows = await repo.list_by_thread("t1", limit=2)
        assert len(rows) == 2
        await _cleanup()

    @pytest.mark.anyio
    async def test_owner_none_returns_all(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", user_id="alice", status="success")
        await repo.put("r2", thread_id="t1", user_id="bob", status="pending")
        rows = await repo.list_by_thread("t1", user_id=None)
        assert len(rows) == 2
        await _cleanup()

    @pytest.mark.anyio
    async def test_model_name_persistence(self, tmp_path):
        """RunRepository persists an exact validated model-profile identity."""
        from deerflow.persistence.engine import get_session_factory, init_engine

        url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
        await init_engine("sqlite", url=url, sqlite_dir=str(tmp_path))
        repo = RunRepository(get_session_factory())

        await repo.put("run-1", thread_id="thread-1", model_name="gpt-4o", status="success")
        row = await repo.get("run-1")
        assert row is not None
        assert row["model_name"] == "gpt-4o"

        exact_name = " Profile V2 "
        await repo.put("run-2", thread_id="thread-1", model_name=exact_name, status="success")
        row2 = await repo.get("run-2")
        assert row2["model_name"] == exact_name

        with pytest.raises(ValueError, match="128 UTF-8 bytes"):
            await repo.put("run-3", thread_id="thread-1", model_name="a" * 129, status="success")

        with pytest.raises(ValueError, match="profile identifier"):
            await repo.put("run-3", thread_id="thread-1", model_name=123, status="success")

        await repo.put("run-4", thread_id="thread-1", model_name=None, status="pending")
        row4 = await repo.get("run-4")
        assert row4["model_name"] is None

        await _cleanup()

    @pytest.mark.anyio
    async def test_aggregate_tokens_by_thread_returns_zeros_when_no_rows(self):
        """Empty thread aggregates to all-zero totals, no model buckets, and a
        single query — replaces the older test that pinned the now-removed
        ``GROUP BY coalesce(model_name)`` shape (issue #3645 reduces by_model
        in Python from each row's per-model JSON column instead)."""
        captured = []

        class FakeResult:
            def all(self):
                return []

        class FakeSession:
            async def execute(self, stmt):
                captured.append(stmt)
                return FakeResult()

        class FakeSessionContext:
            async def __aenter__(self):
                return FakeSession()

            async def __aexit__(self, exc_type, exc, tb):
                return None

        repo = RunRepository(lambda: FakeSessionContext())

        agg = await repo.aggregate_tokens_by_thread("t1")
        assert agg == {
            "total_tokens": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_runs": 0,
            "by_model": {},
            "by_caller": {"lead_agent": 0, "subagent": 0, "middleware": 0},
        }
        assert len(captured) == 1

    @pytest.mark.anyio
    async def test_aggregate_tokens_by_thread_compiles_on_postgres_dialect(self):
        """Compile-smoke the new SELECT on the postgres dialect.

        The project ships both SQLite and Postgres backends. The new aggregation
        projects ``RunRow.token_usage_by_model`` (a JSON column) directly into
        the row set instead of grouping on a scalar, so the SQL needs to compile
        cleanly under PG's JSON/JSONB binding too. Pins:
          * the JSON column is selected by name (PG would otherwise need a
            ``::jsonb`` cast or coalesce around it)
          * there is no GROUP BY / aggregate function left (the per-model
            reduction now happens in Python — see issue #3645)
        """

        captured = []

        class FakeResult:
            def all(self):
                return []

        class FakeSession:
            async def execute(self, stmt):
                captured.append(stmt)
                return FakeResult()

        class FakeSessionContext:
            async def __aenter__(self):
                return FakeSession()

            async def __aexit__(self, exc_type, exc, tb):
                return None

        repo = RunRepository(lambda: FakeSessionContext())
        await repo.aggregate_tokens_by_thread("t1")

        compiled = str(captured[0].compile(dialect=postgresql.dialect()))
        assert "token_usage_by_model" in compiled
        assert "GROUP BY" not in compiled.upper()

    @pytest.mark.anyio
    async def test_run_manager_hydrates_store_only_run_from_sql(self, tmp_path):
        """RunManager should hydrate historical runs from SQL-backed store."""
        repo = await _make_repo(tmp_path)
        await repo.put(
            "sql-store-only",
            thread_id="thread-1",
            assistant_id="lead_agent",
            status="success",
            metadata={"source": "sql"},
            kwargs={"input": "value"},
            model_name="model-a",
        )
        manager = RunManager(store=repo)

        record = await manager.get("sql-store-only")
        rows = await manager.list_by_thread("thread-1")

        assert record is not None
        assert record.run_id == "sql-store-only"
        assert record.status == RunStatus.success
        assert record.metadata == {"source": "sql"}
        assert record.kwargs == {"input": "value"}
        assert record.model_name == "model-a"
        assert [run.run_id for run in rows] == ["sql-store-only"]
        await _cleanup()

    @pytest.mark.anyio
    async def test_run_manager_cancel_persists_interrupted_status_to_sql(self, tmp_path):
        """RunManager.cancel should write interrupted status to SQL-backed store."""
        repo = await _make_repo(tmp_path)
        manager = RunManager(store=repo)
        record = await manager.create("thread-1")
        await manager.set_status(record.run_id, RunStatus.running)

        cancelled = await manager.cancel(record.run_id)
        row = await repo.get(record.run_id)

        assert cancelled == CancelOutcome.cancelled
        assert row is not None
        assert row["status"] == "interrupted"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_model_name(self, tmp_path):
        """RunRepository.update_model_name should update model_name for existing run."""
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", model_name="initial-model")
        await repo.update_model_name("r1", "updated-model")
        row = await repo.get("r1")
        assert row["model_name"] == "updated-model"
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_model_name_rejects_overlong_value_without_truncation(self, tmp_path):
        """RunRepository.update_model_name never aliases an overlong identity."""
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1")
        with pytest.raises(ValueError, match="128 UTF-8 bytes"):
            await repo.update_model_name("r1", "a" * 129)
        row = await repo.get("r1")
        assert row["model_name"] is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_update_model_name_to_none(self, tmp_path):
        """RunRepository.update_model_name should allow setting model_name to None."""
        repo = await _make_repo(tmp_path)
        await repo.put("r1", thread_id="t1", model_name="initial-model")
        await repo.update_model_name("r1", None)
        row = await repo.get("r1")
        assert row["model_name"] is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_run_manager_update_model_name_persists_to_sql(self, tmp_path):
        """RunManager.update_model_name should persist to SQL-backed store without integrity error."""
        repo = await _make_repo(tmp_path)
        manager = RunManager(store=repo)
        record = await manager.create("thread-1")

        await manager.update_model_name(record.run_id, "gpt-4o")

        row = await repo.get(record.run_id)
        assert row is not None
        assert row["model_name"] == "gpt-4o"
        await _cleanup()

    @pytest.mark.anyio
    async def test_run_manager_update_model_name_twice(self, tmp_path):
        """RunManager.update_model_name should support multiple updates."""
        repo = await _make_repo(tmp_path)
        manager = RunManager(store=repo)
        record = await manager.create("thread-1")

        await manager.update_model_name(record.run_id, "model-1")
        await manager.update_model_name(record.run_id, "model-2")

        row = await repo.get(record.run_id)
        assert row["model_name"] == "model-2"
        await _cleanup()

    @pytest.mark.anyio
    async def test_create_thread_operation_atomic_rejects_unique_violation(self, tmp_path):
        """reject path against a real SQLite-backed store must surface as ConflictError, not raw IntegrityError.

        The partial unique index ``uq_runs_thread_active`` is created by
        ``Base.metadata.create_all`` on SQLite too. Every other atomic-create
        test in the suite uses ``MemoryRunStore``, which raises ConflictError
        directly and never exercises the manager's
        ``_is_unique_violation``-based conversion. This test is the load-bearing
        coverage for that branch on a real DB: pre-insert an active run on
        thread T, then attempt a reject-strategy create for the same thread,
        and assert ConflictError (HTTP 409) — not a leaking IntegrityError
        (HTTP 500).
        """
        from datetime import UTC, datetime, timedelta

        from deerflow.config.run_ownership_config import RunOwnershipConfig

        repo = await _make_repo(tmp_path)
        manager = RunManager(
            store=repo,
            run_ownership_config=RunOwnershipConfig(
                lease_seconds=30,
                grace_seconds=10,
                heartbeat_enabled=False,
            ),
        )

        # Pre-insert an active run on thread T directly through the store so
        # the partial unique index has something to enforce on the second insert.
        lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        await repo.create_thread_operation_atomic(
            "run-A",
            thread_id="thread-T",
            owner_worker_id="worker-A",
            lease_expires_at=lease,
            multitask_strategy="reject",
            created_at=datetime.now(UTC).isoformat(),
        )

        # Second reject-strategy create against the same thread must convert the
        # underlying IntegrityError into ConflictError via ``_is_unique_violation``.
        with pytest.raises(ConflictError, match="already has an active run"):
            await manager.create_or_reject(
                "thread-T",
                multitask_strategy="reject",
            )

        await _cleanup()

    @pytest.mark.anyio
    async def test_run_admission_reuses_process_wide_idempotency_key(self, tmp_path):
        repo = await _make_repo(tmp_path)
        first_manager = RunManager(store=repo, worker_id="worker-a")
        second_manager = RunManager(store=repo, worker_id="worker-b")

        first = await first_manager.create_or_reject(
            "thread-T",
            user_id="user-1",
            idempotency_key="mcp-task:task-1:1:0",
        )
        reused = await second_manager.create_or_reject(
            "thread-T",
            user_id="user-1",
            idempotency_key="mcp-task:task-1:1:0",
        )

        assert reused.run_id == first.run_id
        assert reused.idempotency_reused is True
        assert len(await repo.list_by_thread("thread-T", user_id="user-1")) == 1
        await _cleanup()

    @pytest.mark.anyio
    async def test_heartbeat_disabled_database_clock_store_keeps_null_lease_compatibility(self, tmp_path):
        """Database-clock capability must not require leases in single-worker mode."""
        from deerflow.config.run_ownership_config import RunOwnershipConfig

        repo = await _make_repo(tmp_path)
        manager = RunManager(
            store=repo,
            worker_id="worker-heartbeat-disabled",
            run_ownership_config=RunOwnershipConfig(
                lease_seconds=30,
                grace_seconds=10,
                heartbeat_enabled=False,
            ),
        )
        worker = asyncio.sleep(0)
        try:
            assert repo.lease_clock_authority is LeaseClockAuthority.database_v1
            assert manager.heartbeat_enabled is False

            record = await manager.create_or_reject(
                "thread-heartbeat-disabled-run",
                candidate_run_id="00000000-0000-4000-8000-000000000201",
            )
            assert record.lease_expires_at is None
            persisted = await repo.get(record.run_id)
            assert persisted is not None
            assert persisted["lease_expires_at"] is None

            task = await manager.attach_worker_once(
                record.run_id,
                worker,
                asyncio.create_task,
            )
            await task
            assert record.task is task
            assert record.attachment_supervised is False

            reservation_thread_id = "thread-heartbeat-disabled-reservation"
            async with manager.reserve_thread_operation(
                reservation_thread_id,
                kind=ThreadOperationKind.checkpoint_write,
            ):
                reservations = [row for row in await repo.list_inflight() if row["thread_id"] == reservation_thread_id]
                assert len(reservations) == 1
                assert reservations[0]["operation_kind"] == ThreadOperationKind.checkpoint_write.value
                assert reservations[0]["lease_expires_at"] is None

            assert all(row["thread_id"] != reservation_thread_id for row in await repo.list_inflight())
        finally:
            if worker.cr_frame is not None:
                worker.close()
            await _cleanup()

    @pytest.mark.anyio
    async def test_checkpoint_write_reservation_blocks_interrupt_run_on_sql_store(self, tmp_path):
        """An interrupt-strategy run cannot displace a durable checkpoint writer."""
        repo = await _make_repo(tmp_path)
        compaction_worker = RunManager(store=repo, worker_id="worker-a")
        run_worker = RunManager(store=repo, worker_id="worker-b")

        async with compaction_worker.reserve_thread_operation("thread-T", kind=ThreadOperationKind.checkpoint_write):
            with pytest.raises(ConflictError, match="checkpoint write"):
                await run_worker.create_or_reject("thread-T", multitask_strategy="interrupt")

        assert await repo.list_by_thread("thread-T") == []
        admitted = await run_worker.create_or_reject("thread-T")
        assert admitted.status == RunStatus.pending
        await _cleanup()

    @pytest.mark.anyio
    async def test_reservation_release_uses_record_user_without_ambient_context(self, tmp_path):
        """Release must not depend on the request ContextVar still being set."""
        repo = await _make_repo(tmp_path)
        manager = RunManager(store=repo)

        async with manager.reserve_thread_operation(
            "thread-T",
            kind=ThreadOperationKind.checkpoint_write,
            user_id="reservation-owner",
        ):
            inflight = await repo.list_inflight()
            assert len(inflight) == 1
            assert inflight[0]["user_id"] == "reservation-owner"

        assert await repo.list_inflight() == []
        await _cleanup()

    @pytest.mark.anyio
    async def test_exact_auxiliary_release_is_owner_fenced_and_idempotent(
        self,
        tmp_path,
    ):
        """SQL release cannot delete a differently owned reservation or a run."""

        repo = await _make_repo(tmp_path)
        run_id = "checkpoint-exact-release"
        thread_id = "thread-exact-release"
        await repo.create_thread_operation_atomic(
            run_id,
            thread_id=thread_id,
            owner_worker_id="worker-owner",
            lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
            operation_kind=ThreadOperationKind.checkpoint_write.value,
            user_id="reservation-owner",
        )

        stale = await repo.release_thread_operation_owned(
            run_id,
            thread_id=thread_id,
            operation_kind=ThreadOperationKind.checkpoint_write.value,
            user_id="reservation-owner",
            expected_owner_worker_id="worker-stale",
            require_unexpired_lease=True,
        )
        assert stale.outcome is ThreadOperationReleaseOutcome.ownership_lost
        assert await repo.get(run_id, user_id="reservation-owner") is not None

        released = await repo.release_thread_operation_owned(
            run_id,
            thread_id=thread_id,
            operation_kind=ThreadOperationKind.checkpoint_write.value,
            user_id="reservation-owner",
            expected_owner_worker_id="worker-owner",
            require_unexpired_lease=True,
        )
        assert released.outcome is ThreadOperationReleaseOutcome.released
        assert await repo.list_lifecycle_events(run_id=run_id) == []

        repeated = await repo.release_thread_operation_owned(
            run_id,
            thread_id=thread_id,
            operation_kind=ThreadOperationKind.checkpoint_write.value,
            user_id="reservation-owner",
            expected_owner_worker_id="worker-owner",
            require_unexpired_lease=True,
        )
        assert repeated.outcome is ThreadOperationReleaseOutcome.absent
        await _cleanup()

    @pytest.mark.anyio
    async def test_exact_auxiliary_release_treats_database_clock_equality_as_expired(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """A lease ending exactly at authoritative DB time no longer owns release."""
        from sqlalchemy import literal

        from deerflow.persistence.run import sql as run_sql

        database_now = datetime(2040, 1, 2, 3, 4, 5, 678000, tzinfo=UTC)
        monkeypatch.setattr(
            run_sql,
            "database_wall_clock_expression",
            lambda _dialect_name: literal(database_now.isoformat()),
        )
        repo = await _make_repo(tmp_path)
        run_id = "checkpoint-release-at-database-now"
        thread_id = "thread-release-at-database-now"
        try:
            await repo.create_thread_operation_atomic(
                run_id,
                thread_id=thread_id,
                owner_worker_id="worker-owner",
                lease_expires_at=database_now.isoformat(),
                operation_kind=ThreadOperationKind.checkpoint_write.value,
                user_id="reservation-owner",
            )

            result = await repo.release_thread_operation_owned(
                run_id,
                thread_id=thread_id,
                operation_kind=ThreadOperationKind.checkpoint_write.value,
                user_id="reservation-owner",
                expected_owner_worker_id="worker-owner",
                require_unexpired_lease=True,
            )

            assert result.outcome is ThreadOperationReleaseOutcome.ownership_lost
            assert await repo.get(run_id, user_id="reservation-owner") is not None
        finally:
            await _cleanup()

    @pytest.mark.anyio
    async def test_interrupt_reclaims_expired_checkpoint_write_reservation_on_sql_store(self, tmp_path):
        """An expired durable checkpoint writer is immediately reclaimable."""
        repo = await _make_repo(tmp_path)
        expired = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
        await repo.put(
            "checkpoint-write-1",
            thread_id="thread-T",
            status="pending",
            operation_kind=ThreadOperationKind.checkpoint_write,
            owner_worker_id="dead-worker",
            lease_expires_at=expired,
            created_at=expired,
        )
        manager = RunManager(store=repo, worker_id="worker-b")

        admitted = await manager.create_or_reject("thread-T", multitask_strategy="interrupt")

        assert admitted.status == RunStatus.pending
        stale = await repo.get("checkpoint-write-1")
        assert stale is not None
        assert stale["status"] == "interrupted"
        assert stale["owner_worker_id"] is None
        assert stale["lease_expires_at"] is None
        assert stale["terminal_projection_owner_worker_id"] is None
        assert stale["terminal_projection_active_state_version"] is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_is_unique_violation_detects_real_sqlite_integrity_error(self, tmp_path):
        """``_is_unique_violation`` must return True for a real SQLite IntegrityError.

        SQLite raises ``UNIQUE constraint failed: runs.uq_runs_thread_active``
        which contains "unique" but neither "violat" nor "duplicate" — the
        previous substring-only heuristic returned False on SQLite, leaking the
        raw IntegrityError. This test triggers a real violation against the
        partial unique index and feeds the resulting SQLAlchemy IntegrityError
        (with the wrapped sqlite3.IntegrityError on ``.orig``) through the
        detector to assert True.
        """
        import sqlite3

        from sqlalchemy.exc import IntegrityError

        from deerflow.runtime.runs.manager import _is_unique_violation

        repo = await _make_repo(tmp_path)

        # First insert succeeds; second collides on the partial unique index.
        await repo.put("first", thread_id="thread-T", status="pending")
        with pytest.raises(IntegrityError) as exc_info:
            await repo.put("second", thread_id="thread-T", status="pending")

        # The wrapped driver exception must be a sqlite3 IntegrityError carrying
        # SQLITE_CONSTRAINT_UNIQUE. Walk the chain so we assert on the actual
        # driver-level signal, not the SQLAlchemy wrapper.
        driver = exc_info.value.orig
        assert isinstance(driver, sqlite3.IntegrityError)
        assert driver.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_UNIQUE

        # The detector must return True regardless of message phrasing.
        assert _is_unique_violation(exc_info.value) is True

        await _cleanup()

    @pytest.mark.anyio
    async def test_is_unique_violation_does_not_misclassify_application_exception(self):
        """Message fallbacks must not fire on non-IntegrityError exceptions.

        A ``ValueError`` / ``RuntimeError`` whose ``str()`` happens to
        contain ``"duplicate key"`` or ``"unique" + "violat"`` substrings
        must NOT be classified as a unique violation — that would silently
        mask real application bugs as HTTP 409 conflicts instead of 500.
        Pre-fix the substring-only fallback fired regardless of exception
        type. The fix gates the fallback on
        ``isinstance(current, (SAIntegrityError, sqlite3.IntegrityError))``.
        """
        from deerflow.runtime.runs.manager import _is_unique_violation

        assert _is_unique_violation(ValueError("duplicate key in input data: 'email'")) is False
        assert _is_unique_violation(RuntimeError("unique violat detected in config")) is False
        assert _is_unique_violation(Exception("unique constraint failed (in a unit test mock)")) is False

    @pytest.mark.anyio
    async def test_is_unique_violation_detects_psycopg3_sqlstate(self):
        """psycopg3 exposes the error code via ``sqlstate``, not ``pgcode``.

        On Postgres (the only supported multi-worker backend), psycopg3's
        ``sqlstate=23505`` must be detected as a unique violation without
        falling through to the message-substring fallback.
        """
        from sqlalchemy.exc import IntegrityError as SAIntegrityError

        from deerflow.runtime.runs.manager import _is_unique_violation

        # Simulate psycopg3's sqlstate attribute on a wrapped IntegrityError
        dbapi_err = Exception()
        dbapi_err.sqlstate = "23505"  # psycopg3 uses sqlstate

        sa_err = SAIntegrityError(
            "duplicate key value violates unique constraint",
            params=None,
            orig=dbapi_err,
        )

        assert _is_unique_violation(sa_err) is True

    @pytest.mark.anyio
    async def test_create_thread_operation_atomic_tolerates_tz_naive_lease_on_sqlite(self, tmp_path):
        """Interrupt path must not raise TypeError comparing naive vs aware datetimes.

        SQLite drops tzinfo on read despite ``DateTime(timezone=True)`` (see
        the comment in ``RunRepository._row_to_dict``). The interrupt branch
        of ``create_thread_operation_atomic`` compares ``row.lease_expires_at`` against
        the aware ``cutoff = datetime.now(UTC) - ...`` in Python. Under
        default config (heartbeat disabled) leases are always NULL so the
        ``is not None`` check short-circuits, but there is no guard against
        ``heartbeat_enabled=true`` on SQLite — a naive lease would raise
        ``TypeError: can't compare offset-naive and offset-aware datetimes``
        and surface as an opaque 500.

        Pre-fix this test fails with TypeError; post-fix it raises
        ConflictError (the live other-worker run blocks the interrupt).
        """
        from datetime import UTC, datetime, timedelta

        repo = await _make_repo(tmp_path)

        # Seed an active run owned by another worker with a still-valid lease.
        # The lease value is stored as ISO; SQLite reads it back as a tz-naive
        # datetime — exactly the shape that triggered the bug.
        valid_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        await repo.create_thread_operation_atomic(
            "valid-lease-run",
            thread_id="thread-T",
            owner_worker_id="other-worker",
            lease_expires_at=valid_lease,
            multitask_strategy="reject",
            created_at=datetime.now(UTC).isoformat(),
        )

        # The interrupt path must surface a clean ConflictError, not a
        # TypeError from the naive-vs-aware comparison.
        with pytest.raises(ConflictError, match="another worker"):
            await repo.create_thread_operation_atomic(
                "run-new",
                thread_id="thread-T",
                owner_worker_id="w1",
                lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
                multitask_strategy="interrupt",
                created_at=datetime.now(UTC).isoformat(),
            )

        await _cleanup()

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("multitask_strategy", "expected_status"),
        (("interrupt", "interrupted"), ("rollback", "error")),
    )
    async def test_create_thread_operation_atomic_records_terminal_projection_epoch_before_flush(
        self,
        tmp_path,
        multitask_strategy,
        expected_status,
    ):
        """Replacement preserves the displaced owner's exact active epoch.

        The terminal projection proof must become valid before any await that
        can autoflush the row.  In particular, locking the lifecycle cursor
        cannot observe ``active_state_version == state_version``.
        """

        repo = await _make_repo(tmp_path)
        now = datetime.now(UTC)
        try:
            previous, _ = await repo.create_thread_operation_atomic(
                "run-before-replacement",
                thread_id="thread-replacement",
                owner_worker_id="worker-owner",
                lease_expires_at=(now + timedelta(minutes=5)).isoformat(),
            )

            replacement, displaced = await repo.create_thread_operation_atomic(
                "run-after-replacement",
                thread_id="thread-replacement",
                owner_worker_id="worker-owner",
                lease_expires_at=(now + timedelta(minutes=5)).isoformat(),
                multitask_strategy=multitask_strategy,
            )

            assert replacement["status"] == "pending"
            assert len(displaced) == 1
            terminal = displaced[0]
            assert terminal["status"] == expected_status
            assert terminal["owner_worker_id"] is None
            assert terminal["terminal_projection_owner_worker_id"] == "worker-owner"
            assert terminal["terminal_projection_active_state_version"] == previous["state_version"]
            assert terminal["state_version"] == previous["state_version"] + 1
        finally:
            await _cleanup()

    @pytest.mark.anyio
    async def test_create_thread_operation_atomic_uses_database_clock_for_live_peer_lease(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """A fast process clock cannot interrupt a peer still live by DB time."""

        from deerflow.persistence.run import sql as run_sql

        repo = await _make_repo(tmp_path)
        actual_now = datetime.now(UTC)

        class FastProcessDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = actual_now + timedelta(hours=1)
                return value if tz is not None else value.replace(tzinfo=None)

        try:
            original, _ = await repo.create_thread_operation_atomic(
                "run-live-peer",
                thread_id="thread-live-peer",
                owner_worker_id="worker-peer",
                lease_expires_at=(actual_now + timedelta(minutes=5)).isoformat(),
            )
            monkeypatch.setattr(run_sql, "datetime", FastProcessDateTime)

            with pytest.raises(ConflictError, match="another worker"):
                await repo.create_thread_operation_atomic(
                    "run-fast-clock-claimant",
                    thread_id="thread-live-peer",
                    owner_worker_id="worker-claimant",
                    lease_expires_at=(actual_now + timedelta(hours=2)).isoformat(),
                    multitask_strategy="interrupt",
                )

            retained = await repo.get("run-live-peer")
            assert retained is not None
            assert retained["status"] == "pending"
            assert retained["owner_worker_id"] == "worker-peer"
            assert retained["state_version"] == original["state_version"]
            assert await repo.get("run-fast-clock-claimant") is None
        finally:
            await _cleanup()

    # ------------------------------------------------------------------
    # claim_for_takeover SQL path
    # ------------------------------------------------------------------

    @pytest.mark.anyio
    async def test_claim_for_takeover_succeeds_with_expired_lease(self, tmp_path):
        repo = await _make_repo(tmp_path)
        grace = 10
        expired = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
        await repo.put("run-1", thread_id="t1", status="running", owner_worker_id="w-a", lease_expires_at=expired, created_at=datetime.now(UTC).isoformat())

        ok = await repo.claim_for_takeover("run-1", grace_seconds=grace, error="claimed")
        assert ok is True

        row = await repo.get("run-1")
        assert row["status"] == "error"
        assert row["error"] == "claimed"
        await _cleanup()

    @pytest.mark.anyio
    async def test_generic_claim_for_takeover_rejects_exact_two_policy(
        self,
        tmp_path,
    ):
        repo = await _make_repo(tmp_path)
        expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        try:
            await repo.put(
                "run-exact-two",
                thread_id="t1",
                status="running",
                owner_worker_id="worker-a",
                lease_expires_at=expired,
                recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
                recovery_payload_json={"version": 1},
            )
            before = await repo.get("run-exact-two")

            claimed = await repo.claim_for_takeover(
                "run-exact-two",
                grace_seconds=0,
                error="legacy terminalization",
            )
            retained = await repo.get("run-exact-two")

            assert claimed is False
            assert retained == before
        finally:
            await _cleanup()

    @pytest.mark.anyio
    async def test_claim_for_takeover_fails_on_valid_lease(self, tmp_path):
        repo = await _make_repo(tmp_path)
        grace = 10
        valid = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        await repo.put("run-1", thread_id="t1", status="running", owner_worker_id="w-a", lease_expires_at=valid, created_at=datetime.now(UTC).isoformat())

        ok = await repo.claim_for_takeover("run-1", grace_seconds=grace, error="claimed")
        assert ok is False

        row = await repo.get("run-1")
        assert row["status"] == "running"
        await _cleanup()

    @pytest.mark.anyio
    async def test_request_cancel_is_returned_by_owner_lease_renewal(self, tmp_path):
        """The SQL store must atomically carry the first cancel action to the owner."""
        repo = await _make_repo(tmp_path)
        lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        await repo.put(
            "run-1",
            thread_id="t1",
            status="running",
            owner_worker_id="worker-a",
            lease_expires_at=lease,
        )

        assert await repo.request_cancel("run-1", action="rollback") == "rollback"
        assert await repo.request_cancel("run-1", action="interrupt") == "rollback"

        renewal = await repo.renew_lease(
            "run-1",
            owner_worker_id="worker-a",
            lease_expires_at=(datetime.now(UTC) + timedelta(seconds=60)).isoformat(),
        )

        assert renewal.renewed is True
        assert renewal.cancel_action == "rollback"
        row = await repo.get("run-1")
        assert row["cancel_action"] == "rollback"
        assert row["cancel_requested_at"] is not None
        await _cleanup()

    @pytest.mark.anyio
    async def test_request_cancel_rejects_terminal_run(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("run-1", thread_id="t1", status="success")

        assert await repo.request_cancel("run-1", action="interrupt") is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_cancel_request_wins_before_owner_completion(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put(
            "run-1",
            thread_id="t1",
            status="running",
            owner_worker_id="worker-a",
        )

        assert await repo.request_cancel("run-1", action="rollback") == "rollback"
        result = await repo.finalize_if_not_cancelled(
            "run-1",
            status="success",
        )

        assert result.finalized is False
        assert result.cancel_action == "rollback"
        assert (await repo.get("run-1"))["status"] == "running"
        await _cleanup()

    @pytest.mark.anyio
    async def test_owner_completion_wins_before_cancel_request(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put(
            "run-1",
            thread_id="t1",
            status="running",
            owner_worker_id="worker-a",
        )

        result = await repo.finalize_if_not_cancelled(
            "run-1",
            status="success",
        )

        assert result.finalized is True
        assert await repo.request_cancel("run-1", action="rollback") is None
        row = await repo.get("run-1")
        assert row["status"] == "success"
        assert row["owner_worker_id"] is None
        assert row["lease_expires_at"] is None
        await _cleanup()

    @pytest.mark.anyio
    async def test_reconciliation_rechecks_live_lease_after_stale_scan(self, tmp_path):
        """The SQL takeover CAS must reject a stale scan of a live row."""
        repo = await _make_repo(tmp_path)
        grace = 10
        run_id = "live-after-stale-scan"
        owner_worker_id = "worker-alive"
        try:
            await repo.put(
                run_id,
                thread_id="t1",
                status="running",
                owner_worker_id=owner_worker_id,
                lease_expires_at=(datetime.now(UTC) + timedelta(seconds=60)).isoformat(),
                created_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
            )

            async def stale_scan(*, before=None, grace_seconds=10):
                del before, grace_seconds
                stale = await repo.get(run_id)
                assert stale is not None
                stale["lease_expires_at"] = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
                return [stale]

            repo.list_inflight_with_expired_lease = stale_scan
            manager = RunManager(store=repo)

            recovered = await manager.reconcile_orphaned_inflight_runs(error="orphaned")

            row = await repo.get(run_id)
            assert recovered == []
            assert row is not None
            assert row["status"] == "running"
            assert datetime.fromisoformat(row["lease_expires_at"]) > datetime.now(UTC)
        finally:
            await _cleanup()

    @pytest.mark.anyio
    async def test_sql_expired_owner_cannot_resurrect_lease(self, tmp_path):
        """Expired SQL authority cannot renew itself after the deadline."""

        repo = await _make_repo(tmp_path)
        expired_lease = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        try:
            await repo.put(
                "expired-owner-run",
                thread_id="thread-expired-owner",
                status="running",
                owner_worker_id="worker-expired",
                lease_expires_at=expired_lease,
            )
            requested_lease = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()

            updated = await repo.update_lease(
                "expired-owner-run",
                owner_worker_id="worker-expired",
                lease_expires_at=requested_lease,
            )
            renewed = await repo.renew_lease(
                "expired-owner-run",
                owner_worker_id="worker-expired",
                lease_expires_at=requested_lease,
            )

            assert updated is False
            assert renewed.renewed is False
            retained = await repo.get("expired-owner-run")
            assert retained is not None
            assert retained["lease_expires_at"] == expired_lease
        finally:
            await _cleanup()

    @pytest.mark.anyio
    async def test_claim_for_takeover_succeeds_with_null_lease(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("run-null", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat())

        ok = await repo.claim_for_takeover("run-null", grace_seconds=10, error="claimed")
        assert ok is True

        row = await repo.get("run-null")
        assert row["status"] == "error"
        await _cleanup()

    @pytest.mark.anyio
    async def test_claim_for_takeover_fails_on_terminal_row(self, tmp_path):
        repo = await _make_repo(tmp_path)
        await repo.put("run-done", thread_id="t1", status="success", created_at=datetime.now(UTC).isoformat())

        ok = await repo.claim_for_takeover("run-done", grace_seconds=10, error="claimed")
        assert ok is False
        await _cleanup()

    @pytest.mark.anyio
    async def test_claim_for_takeover_nonexistent_run(self, tmp_path):
        repo = await _make_repo(tmp_path)
        ok = await repo.claim_for_takeover("no-such-run", grace_seconds=10, error="claimed")
        assert ok is False
        await _cleanup()

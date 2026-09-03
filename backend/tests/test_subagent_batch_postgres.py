"""Real-PostgreSQL lease and terminal-fence qualification for batches.

Without ``DEERFLOW_TEST_POSTGRES_URL`` these remain explicitly unpassed
``postgres_contract`` gates; SQLite coverage is not treated as a substitute.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import uuid
from dataclasses import replace
from enum import Enum
from types import SimpleNamespace

import pytest
from _subagent_batch_helpers import make_parent_batch_request
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.base import Base
from deerflow.persistence.subagent_batches import (
    SubagentBatchAttemptRow,
    SubagentBatchItemRow,
    SubagentBatchRepository,
    SubagentBatchRow,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchV1,
    ParentBoundBatchExecutionV1,
)

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


class _ProcessWorkerStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"

    @property
    def is_terminal(self) -> bool:
        return self is _ProcessWorkerStatus.COMPLETED


def _claim_then_wait_for_process_kill(
    postgres_url: str,
    batch_id: str,
    phase: str,
    connection,
) -> None:
    """Run one real worker process up to the requested durable crash window."""

    async def run() -> None:
        from deerflow.config.subagent_batches_config import SubagentBatchesConfig
        from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
        from deerflow.subagents import batch_service as service_module
        from deerflow.subagents.batch_service import SubagentBatchService

        engine = create_async_engine(postgres_async_url(postgres_url))
        repository = SubagentBatchRepository(
            async_sessionmaker(engine, expire_on_commit=False),
            tenant=TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference(),
        )
        lease_owner = f"crash-worker-{phase}"
        claimed = (
            await repository.claim_items(
                now=None,
                lease_owner=lease_owner,
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert claimed["batch"]["id"] == batch_id
        crash_evidence = {
            "phase": phase,
            "item_id": claimed["id"],
            "attempt_id": claimed["attempt_id"],
            "lease_epoch": claimed["lease_epoch"],
        }
        if phase == "after_claim":
            connection.send(crash_evidence)
            await asyncio.Event().wait()

        result = SimpleNamespace(
            status=_ProcessWorkerStatus.PENDING,
            result=None,
            error=None,
            stop_reason=None,
            token_usage_records=None,
        )
        callback = None

        class Executor:
            def __init__(self, **kwargs) -> None:
                nonlocal callback
                callback = kwargs["execution_admitted_callback"]

            def execute_async(self, _prompt, task_id=None):
                async def execute() -> None:
                    assert callback is not None
                    await callback()
                    if phase == "after_start":
                        connection.send(crash_evidence)
                        await asyncio.Event().wait()
                    result.status = _ProcessWorkerStatus.COMPLETED
                    result.result = "unpublished result"

                asyncio.create_task(execute())
                return "crash-execution"

        async def publication_barrier(**kwargs) -> bool:
            assert kwargs == {
                "scenario": "subagent_batch",
                "point": "before_terminal_publication",
                "subject_id": claimed["id"],
            }
            if phase == "before_terminal_publication":
                connection.send(crash_evidence)
                await asyncio.Event().wait()
            return False

        service_module.SubagentExecutor = Executor
        service_module.SubagentStatus = _ProcessWorkerStatus
        service_module.get_background_task_result = lambda _execution_id: result
        service_module.cleanup_background_task = lambda _execution_id: None
        service_module.resolve_subagent_model_name = lambda *_args, **_kwargs: "model-a"
        service_module.qualification_service_barrier = publication_barrier
        import deerflow.tools

        deerflow.tools.get_available_tools = lambda **_kwargs: []
        service = SubagentBatchService(
            repository=repository,
            config=SubagentBatchesConfig(poll_interval_seconds=0.1),
            runtime_config=SubagentRuntimeConfig(max_running=1),
            app_config=SimpleNamespace(get_model_config=lambda _name: {}),
        )
        service._lease_owner = lease_owner
        await service._execute_item(claimed)

    asyncio.run(run())


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for real batch worker process-kill qualification"),
)
@pytest.mark.parametrize(
    "phase",
    ["after_claim", "after_start", "before_terminal_publication"],
)
def test_postgres_process_kill_windows_recover_one_terminal_outcome(
    phase: str,
) -> None:
    """Kill a real claimant process and recover through a fresh DB engine."""

    assert _POSTGRES_URL is not None
    unique = uuid.uuid4().hex
    base_request = make_parent_batch_request()
    request = replace(
        base_request,
        submission_key=f"process-crash:{phase}:{unique}",
        limits=replace(base_request.limits, max_attempts=2),
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )

    async def prepare() -> None:
        engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
        repository = SubagentBatchRepository(
            async_sessionmaker(engine, expire_on_commit=False),
            tenant=request.tenant,
        )
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
            await repository.accept_batch(
                accepted=accepted,
                execution=execution,
                item_requests=request.items,
                user_id=request.user_id,
                submission_key=request.submission_key,
                title=request.title,
                subagent_type=request.subagent_name,
            )
        finally:
            await engine.dispose()

    async def recover(first: dict[str, object]) -> None:
        # A new engine, repository, and SubagentBatchService represent the
        # restarted Gateway worker. PostgreSQL, not the replacement process
        # clock, expires the killed worker's durable lease.
        engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        repository = SubagentBatchRepository(
            session_factory,
            tenant=request.tenant,
        )
        try:
            async with session_factory() as session:
                await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == first["item_id"]).values(lease_expires_at=func.current_timestamp() - text("INTERVAL '1 second'")))
                await session.commit()
            replacement = (
                await repository.claim_items(
                    now=None,
                    lease_owner="replacement-worker",
                    lease_seconds=60,
                    limit=1,
                )
            )[0]
            assert replacement["lease_epoch"] == first["lease_epoch"] + 1
            assert not await repository.finalize_item(
                str(first["item_id"]),
                attempt_id=str(first["attempt_id"]),
                lease_epoch=int(first["lease_epoch"]),
                lease_owner=f"crash-worker-{phase}",
                succeeded=True,
                result="stale private result",
                result_preview="stale private preview",
                result_truncated=False,
                error=None,
                stop_reason="completed",
                token_usage=None,
                model_name="model-a",
                completed_at=None,
            )
            from deerflow.config.subagent_batches_config import (
                SubagentBatchesConfig,
            )
            from deerflow.config.subagent_runtime_config import (
                SubagentRuntimeConfig,
            )
            from deerflow.subagents import batch_service as service_module
            from deerflow.subagents.batch_service import SubagentBatchService

            result = SimpleNamespace(
                status=_ProcessWorkerStatus.PENDING,
                result=None,
                error=None,
                stop_reason=None,
                token_usage_records=None,
            )
            callback = None

            class Executor:
                def __init__(self, **kwargs) -> None:
                    nonlocal callback
                    callback = kwargs["execution_admitted_callback"]

                def execute_async(self, _prompt, task_id=None):
                    async def execute() -> None:
                        assert callback is not None
                        await callback()
                        result.status = _ProcessWorkerStatus.COMPLETED
                        result.result = "accepted result"

                    asyncio.create_task(execute())
                    return "replacement-execution"

            async def no_barrier(**_kwargs) -> bool:
                return False

            original_executor = service_module.SubagentExecutor
            original_status = service_module.SubagentStatus
            original_result = service_module.get_background_task_result
            original_cleanup = service_module.cleanup_background_task
            original_model = service_module.resolve_subagent_model_name
            original_barrier = service_module.qualification_service_barrier
            import deerflow.tools

            original_tools = deerflow.tools.get_available_tools
            try:
                service_module.SubagentExecutor = Executor
                service_module.SubagentStatus = _ProcessWorkerStatus
                service_module.get_background_task_result = lambda _execution_id: result
                service_module.cleanup_background_task = lambda _execution_id: None
                service_module.resolve_subagent_model_name = lambda *_args, **_kwargs: "model-a"
                service_module.qualification_service_barrier = no_barrier
                deerflow.tools.get_available_tools = lambda **_kwargs: []
                restarted_service = SubagentBatchService(
                    repository=repository,
                    config=SubagentBatchesConfig(poll_interval_seconds=0.1),
                    runtime_config=SubagentRuntimeConfig(max_running=1),
                    app_config=SimpleNamespace(
                        get_model_config=lambda _name: {},
                    ),
                )
                restarted_service._lease_owner = "replacement-worker"
                await restarted_service._execute_item(replacement)
            finally:
                service_module.SubagentExecutor = original_executor
                service_module.SubagentStatus = original_status
                service_module.get_background_task_result = original_result
                service_module.cleanup_background_task = original_cleanup
                service_module.resolve_subagent_model_name = original_model
                service_module.qualification_service_barrier = original_barrier
                deerflow.tools.get_available_tools = original_tools
            attempts = await repository.list_attempts(
                batch_id,
                user_id=request.user_id,
            )
            assert attempts is not None
            assert [row["terminal_code"] for row in attempts] == [
                "lease_expired",
                "succeeded",
            ]
            items = await repository.list_items(
                batch_id,
                user_id=request.user_id,
                include_result=True,
            )
            assert items is not None
            assert items[0]["result"] == "accepted result"
            assert "stale private" not in str(attempts)
        finally:
            async with session_factory() as session:
                await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
                await session.commit()
            await engine.dispose()

    asyncio.run(prepare())
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_claim_then_wait_for_process_kill,
        args=(
            _POSTGRES_URL,
            batch_id,
            phase,
            child_connection,
        ),
    )
    try:
        process.start()
        child_connection.close()
        assert parent_connection.poll(30), "worker did not reach crash window"
        first = parent_connection.recv()
        assert first["phase"] == phase
        process.terminate()
        process.join(timeout=10)
        assert not process.is_alive()
        asyncio.run(recover(first))
    finally:
        parent_connection.close()
        if process.is_alive():
            process.kill()
            process.join(timeout=10)


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for durable batch lease and terminal-fence qualification"),
)
@pytest.mark.asyncio
async def test_postgres_claim_and_terminal_publication_have_one_winner() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    request = replace(
        make_parent_batch_request(),
        submission_key=f"submission:{unique}",
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id=batch_id,
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    first_repo = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    second_repo = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await first_repo.accept_batch(
            accepted=accepted,
            execution=execution,
            item_requests=request.items,
            user_id=request.user_id,
            submission_key=request.submission_key,
            title=request.title,
            subagent_type=request.subagent_name,
        )

        claims = await asyncio.gather(
            first_repo.claim_items(
                now=None,
                lease_owner="postgres-worker-one",
                lease_seconds=60,
                limit=1,
            ),
            second_repo.claim_items(
                now=None,
                lease_owner="postgres-worker-two",
                lease_seconds=60,
                limit=1,
            ),
        )
        winners = [item for claim in claims for item in claim]
        assert len(winners) == 1
        item = winners[0]
        owner = "postgres-worker-one" if claims[0] else "postgres-worker-two"
        repo = first_repo if claims[0] else second_repo
        assert await repo.mark_item_running(
            item["id"],
            attempt_id=item["attempt_id"],
            lease_epoch=item["lease_epoch"],
            lease_owner=owner,
            now=None,
        )

        outcomes = await asyncio.gather(
            *(
                repo.finalize_item(
                    item["id"],
                    attempt_id=item["attempt_id"],
                    lease_epoch=item["lease_epoch"],
                    lease_owner=owner,
                    succeeded=True,
                    result="private result",
                    result_preview="preview",
                    result_truncated=False,
                    error=None,
                    stop_reason="completed",
                    token_usage=None,
                    model_name="model-a",
                    completed_at=None,
                )
                for _ in range(2)
            )
        )
        assert sorted(outcomes) == [False, True]
        attempts = await repo.list_attempts(
            batch_id,
            user_id=request.user_id,
        )
        assert attempts is not None
        assert len(attempts) == 1
        assert attempts[0]["terminal_code"] == "succeeded"
    finally:
        async with session_factory() as session:
            await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for durable batch restart, expiry, cancellation, and observation qualification"),
)
@pytest.mark.asyncio
async def test_postgres_restart_expiry_retry_and_observation_contract() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    request = make_parent_batch_request()
    request = replace(
        request,
        submission_key=f"recovery:{unique}",
        limits=replace(request.limits, max_attempts=2),
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    first_worker = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    restarted_worker = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await first_worker.accept_batch(
            accepted=accepted,
            execution=execution,
            item_requests=request.items,
            user_id=request.user_id,
            submission_key=request.submission_key,
            title=request.title,
            subagent_type=request.subagent_name,
        )
        first = (
            await first_worker.claim_items(
                now=None,
                lease_owner="postgres-worker-before-restart",
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert await first_worker.mark_item_running(
            first["id"],
            attempt_id=first["attempt_id"],
            lease_epoch=first["lease_epoch"],
            lease_owner="postgres-worker-before-restart",
            now=None,
        )
        assert await first_worker.renew_item_lease(
            first["id"],
            attempt_id=first["attempt_id"],
            lease_epoch=first["lease_epoch"],
            lease_owner="postgres-worker-before-restart",
            lease_seconds=60,
            now=None,
        ) == {"valid": True, "cancel_requested": False}

        # Simulate a process dying after durable start. The replacement makes
        # its decision against PostgreSQL time and creates a higher epoch.
        async with session_factory() as session:
            await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == first["id"]).values(lease_expires_at=func.current_timestamp() - text("INTERVAL '1 second'")))
            await session.commit()
        second = (
            await restarted_worker.claim_items(
                now=None,
                lease_owner="postgres-worker-after-restart",
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert second["lease_epoch"] > first["lease_epoch"]
        assert not await first_worker.finalize_item(
            first["id"],
            attempt_id=first["attempt_id"],
            lease_epoch=first["lease_epoch"],
            lease_owner="postgres-worker-before-restart",
            succeeded=True,
            result="stale private result",
            result_preview="stale private result",
            result_truncated=False,
            error=None,
            stop_reason=None,
            token_usage=None,
            model_name="model-a",
            completed_at=None,
        )
        assert await restarted_worker.finalize_item(
            second["id"],
            attempt_id=second["attempt_id"],
            lease_epoch=second["lease_epoch"],
            lease_owner="postgres-worker-after-restart",
            succeeded=False,
            result=None,
            result_preview=None,
            result_truncated=False,
            error="safe failure",
            stop_reason=None,
            token_usage=None,
            model_name="model-a",
            completed_at=None,
        )

        attempts = await restarted_worker.list_attempts(
            batch_id,
            user_id=request.user_id,
        )
        assert attempts is not None
        assert [row["terminal_code"] for row in attempts] == [
            "lease_expired",
            "execution_failed",
        ]
        observations = await restarted_worker.list_observations(
            batch_id,
            user_id=request.user_id,
        )
        assert observations is not None
        assert observations[0]["event"] == "batch.accepted"
        assert observations[-1]["event"] == "batch.terminal"
        assert "stale private result" not in str(observations)
    finally:
        async with session_factory() as session:
            await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for durable batch cancellation and terminal-publication race qualification"),
)
@pytest.mark.asyncio
async def test_postgres_cancellation_race_accepts_one_terminal_outcome() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    request = replace(
        make_parent_batch_request(),
        submission_key=f"cancel-race:{unique}",
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    control_repo = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    worker_repo = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await control_repo.accept_batch(
            accepted=accepted,
            execution=execution,
            item_requests=request.items,
            user_id=request.user_id,
            submission_key=request.submission_key,
            title=request.title,
            subagent_type=request.subagent_name,
        )
        claimed = (
            await worker_repo.claim_items(
                now=None,
                lease_owner="postgres-cancel-race-worker",
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert await worker_repo.mark_item_running(
            claimed["id"],
            attempt_id=claimed["attempt_id"],
            lease_epoch=claimed["lease_epoch"],
            lease_owner="postgres-cancel-race-worker",
            now=None,
        )

        cancelled, finalized = await asyncio.wait_for(
            asyncio.gather(
                control_repo.cancel_batch(
                    batch_id,
                    user_id=request.user_id,
                ),
                worker_repo.finalize_item(
                    claimed["id"],
                    attempt_id=claimed["attempt_id"],
                    lease_epoch=claimed["lease_epoch"],
                    lease_owner="postgres-cancel-race-worker",
                    succeeded=True,
                    result="private winner payload",
                    result_preview="private winner payload",
                    result_truncated=False,
                    error=None,
                    stop_reason=None,
                    token_usage=None,
                    model_name="model-a",
                    completed_at=None,
                ),
            ),
            timeout=10,
        )

        assert cancelled is not None
        batch = await control_repo.get_batch(
            batch_id,
            user_id=request.user_id,
        )
        attempts = await control_repo.list_attempts(
            batch_id,
            user_id=request.user_id,
        )
        assert batch is not None
        assert attempts is not None
        assert len(attempts) == 1
        if finalized:
            assert batch["status"] == "completed"
            assert attempts[0]["terminal_code"] == "succeeded"
        else:
            assert batch["status"] == "cancelled"
            assert attempts[0]["terminal_code"] == "cancelled"
        assert "private winner payload" not in str(attempts)
    finally:
        async with session_factory() as session:
            await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for claimed-before-start expiry and retry-exhaustion qualification"),
)
@pytest.mark.asyncio
async def test_postgres_claimed_before_start_expiry_consumes_retry_budget() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    request = make_parent_batch_request()
    request = replace(
        request,
        submission_key=f"claimed-expiry:{unique}",
        limits=replace(request.limits, max_attempts=2),
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    repository = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await repository.accept_batch(
            accepted=accepted,
            execution=execution,
            item_requests=request.items,
            user_id=request.user_id,
            submission_key=request.submission_key,
            title=request.title,
            subagent_type=request.subagent_name,
        )

        first = (
            await repository.claim_items(
                now=None,
                lease_owner="postgres-never-started-one",
                lease_seconds=60,
                limit=1,
            )
        )[0]
        async with session_factory() as session:
            await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == first["id"]).values(lease_expires_at=func.clock_timestamp() - text("INTERVAL '1 second'")))
            await session.commit()

        second = (
            await repository.claim_items(
                now=None,
                lease_owner="postgres-never-started-two",
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert second["attempt"] == 2
        assert second["lease_epoch"] > first["lease_epoch"]
        async with session_factory() as session:
            await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == second["id"]).values(lease_expires_at=func.clock_timestamp() - text("INTERVAL '1 second'")))
            await session.commit()

        assert (
            await repository.claim_items(
                now=None,
                lease_owner="postgres-never-started-three",
                lease_seconds=60,
                limit=1,
            )
            == []
        )
        attempts = await repository.list_attempts(
            batch_id,
            user_id=request.user_id,
        )
        items = await repository.list_items(
            batch_id,
            user_id=request.user_id,
        )
        assert attempts is not None
        assert [row["terminal_code"] for row in attempts] == [
            "lease_expired",
            "lease_expired",
        ]
        assert all(row["consumed"] is True for row in attempts)
        assert all(row["started_at"] is None for row in attempts)
        assert items is not None
        assert items[0]["status"] == "failed"
        assert items[0]["terminal_code"] == "attempt_limit_exhausted"
    finally:
        async with session_factory() as session:
            await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for database wall-clock qualification after row-lock waits"),
)
@pytest.mark.asyncio
async def test_postgres_start_timestamp_is_sampled_after_row_lock_wait() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    request = replace(
        make_parent_batch_request(),
        submission_key=f"wall-clock:{unique}",
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    repository = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await repository.accept_batch(
            accepted=accepted,
            execution=execution,
            item_requests=request.items,
            user_id=request.user_id,
            submission_key=request.submission_key,
            title=request.title,
            subagent_type=request.subagent_name,
        )
        claimed = (
            await repository.claim_items(
                now=None,
                lease_owner="postgres-wall-clock-worker",
                lease_seconds=60,
                limit=1,
            )
        )[0]

        async with session_factory() as locker:
            async with locker.begin():
                await locker.execute(select(SubagentBatchRow).where(SubagentBatchRow.id == batch_id).with_for_update())
                start_task = asyncio.create_task(
                    repository.mark_item_running(
                        claimed["id"],
                        attempt_id=claimed["attempt_id"],
                        lease_epoch=claimed["lease_epoch"],
                        lease_owner="postgres-wall-clock-worker",
                        now=None,
                    )
                )
                await asyncio.sleep(0.1)
                assert not start_task.done()
                released_at = (await locker.execute(select(func.clock_timestamp()))).scalar_one()
            assert await asyncio.wait_for(start_task, timeout=10)

        async with session_factory() as session:
            started_at = (await session.execute(select(SubagentBatchAttemptRow.started_at).where(SubagentBatchAttemptRow.id == claimed["attempt_id"]))).scalar_one()
        assert started_at is not None
        assert started_at >= released_at
    finally:
        async with session_factory() as session:
            await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for cancellation and lease renewal race qualification"),
)
@pytest.mark.asyncio
async def test_postgres_cancellation_fences_concurrent_lease_renewal() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    request = replace(
        make_parent_batch_request(),
        submission_key=f"cancel-renew:{unique}",
    )
    batch_id = f"sb_{unique}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    control_repository = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    worker_repository = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await control_repository.accept_batch(
            accepted=accepted,
            execution=execution,
            item_requests=request.items,
            user_id=request.user_id,
            submission_key=request.submission_key,
            title=request.title,
            subagent_type=request.subagent_name,
        )
        claimed = (
            await worker_repository.claim_items(
                now=None,
                lease_owner="postgres-cancel-renew-worker",
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert await worker_repository.mark_item_running(
            claimed["id"],
            attempt_id=claimed["attempt_id"],
            lease_epoch=claimed["lease_epoch"],
            lease_owner="postgres-cancel-renew-worker",
            now=None,
        )

        cancelled, renewal = await asyncio.wait_for(
            asyncio.gather(
                control_repository.cancel_batch(
                    batch_id,
                    user_id=request.user_id,
                ),
                worker_repository.renew_item_lease(
                    claimed["id"],
                    attempt_id=claimed["attempt_id"],
                    lease_epoch=claimed["lease_epoch"],
                    lease_owner="postgres-cancel-renew-worker",
                    lease_seconds=60,
                    now=None,
                ),
            ),
            timeout=10,
        )
        assert cancelled is not None
        assert renewal in (
            {"valid": True, "cancel_requested": False},
            {"valid": False, "cancel_requested": True},
        )
        assert await worker_repository.renew_item_lease(
            claimed["id"],
            attempt_id=claimed["attempt_id"],
            lease_epoch=claimed["lease_epoch"],
            lease_owner="postgres-cancel-renew-worker",
            lease_seconds=60,
            now=None,
        ) == {"valid": False, "cancel_requested": True}
        attempts = await control_repository.list_attempts(
            batch_id,
            user_id=request.user_id,
        )
        assert attempts is not None
        assert len(attempts) == 1
        assert attempts[0]["terminal_code"] == "cancelled"
        assert attempts[0]["consumed"] is True
    finally:
        async with session_factory() as session:
            await session.execute(delete(SubagentBatchRow).where(SubagentBatchRow.id == batch_id))
            await session.commit()
        await engine.dispose()

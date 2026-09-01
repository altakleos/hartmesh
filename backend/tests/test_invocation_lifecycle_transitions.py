"""Production RunManager lifecycle mapping and race characterization."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import CancelOutcome, RunManager, RunStatus
from deerflow.runtime.runs.manager import ORPHAN_RECOVERY_STOP_REASON, RunRecord
from deerflow.runtime.runs.store.base import (
    LifecycleTransition,
    LifecycleTransitionResult,
    LifecycleType,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RunContext, run_agent


async def _manager_and_run(
    *,
    running: bool = True,
) -> tuple[RunManager, MemoryRunStore, RunRecord]:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")
    if running:
        await manager.set_status(record.run_id, RunStatus.running)
    return manager, store, record


def _assert_latest_agrees(row: dict, events: list[dict]) -> None:
    latest = events[-1]
    assert latest["state_version"] == row["state_version"]
    assert latest["status"] == row["status"]


def _bridge() -> SimpleNamespace:
    return SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )


@pytest.mark.anyio
async def test_clarification_completes_then_answer_starts_new_same_thread_invocation() -> None:
    """Clarification is a successful turn boundary, not a suspended run state."""

    store = MemoryRunStore()
    manager = RunManager(store=store)
    clarification = await manager.create("thread-clarification")

    class ClarifyingAgent:
        async def astream(self, *_args, **_kwargs):
            request = SimpleNamespace(
                tool_call={
                    "name": "ask_clarification",
                    "id": "clarification-1",
                    "args": {
                        "question": "Which environment?",
                        "clarification_type": "missing_info",
                    },
                },
                runtime=SimpleNamespace(context={}),
            )
            command = ClarificationMiddleware().wrap_tool_call(
                request,
                lambda _request: pytest.fail("clarification must not call the tool handler"),
            )
            assert command.goto == "__end__"
            yield {"messages": command.update["messages"]}

    await run_agent(
        _bridge(),
        manager,
        clarification,
        ctx=RunContext(checkpointer=None),
        agent_factory=lambda *, config: ClarifyingAgent(),
        graph_input={"messages": [{"role": "user", "content": "Deploy it"}]},
        config={},
    )

    answer = await manager.create("thread-clarification")

    class AnswerAgent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    await run_agent(
        _bridge(),
        manager,
        answer,
        ctx=RunContext(checkpointer=None),
        agent_factory=lambda *, config: AnswerAgent(),
        graph_input={"messages": [{"role": "user", "content": "Production"}]},
        config={},
    )

    first_row = await store.get(clarification.run_id)
    second_row = await store.get(answer.run_id)
    first_events = await store.list_lifecycle_events(run_id=clarification.run_id)
    second_events = await store.list_lifecycle_events(run_id=answer.run_id)

    assert clarification.run_id != answer.run_id
    assert first_row is not None and second_row is not None
    assert first_row["thread_id"] == second_row["thread_id"] == "thread-clarification"
    assert first_row["status"] == second_row["status"] == RunStatus.success
    assert [event["lifecycle_type"] for event in first_events] == [
        LifecycleType.accepted,
        LifecycleType.started,
        LifecycleType.succeeded,
    ]
    assert [event["lifecycle_type"] for event in second_events] == [
        LifecycleType.accepted,
        LifecycleType.started,
        LifecycleType.succeeded,
    ]
    assert all(event["lifecycle_type"] != "input_required" for event in first_events + second_events)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "lifecycle_type", "stop_reason"),
    [
        (RunStatus.success, LifecycleType.succeeded, None),
        (RunStatus.error, LifecycleType.failed, "agent_revision_drift"),
        (RunStatus.timeout, LifecycleType.timed_out, None),
        (RunStatus.interrupted, LifecycleType.interrupted, None),
    ],
)
async def test_terminal_state_mappings_are_authoritative(
    status: RunStatus,
    lifecycle_type: LifecycleType,
    stop_reason: str | None,
) -> None:
    manager, store, record = await _manager_and_run()

    await manager.set_status(
        record.run_id,
        status,
        error="safe error" if status == RunStatus.error else None,
        stop_reason=stop_reason,
    )

    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert [event["lifecycle_type"] for event in events] == [
        LifecycleType.accepted,
        LifecycleType.started,
        lifecycle_type,
    ]
    _assert_latest_agrees(row, events)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("action", "status", "error"),
    [
        ("interrupt", "interrupted", None),
        ("rollback", "error", "Rolled back by user"),
    ],
)
async def test_compatibility_cancel_records_request_then_cancelled(
    action: str,
    status: str,
    error: str | None,
) -> None:
    manager, store, record = await _manager_and_run()

    outcome = await manager.cancel(record.run_id, action=action)
    duplicate = await manager.cancel(record.run_id, action=action)

    assert outcome == CancelOutcome.cancelled
    assert duplicate == CancelOutcome.cancelled
    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert (row["status"], row["error"]) == (status, error)
    assert [event["lifecycle_type"] for event in events] == [
        LifecycleType.accepted,
        LifecycleType.started,
        LifecycleType.cancellation_requested,
        LifecycleType.cancelled,
    ]
    _assert_latest_agrees(row, events)


@pytest.mark.anyio
async def test_attachment_failure_records_stable_failed_reason() -> None:
    manager, store, record = await _manager_and_run(running=False)

    assert await manager.fail_start_if_pending(
        record.run_id,
        error="Failed to attach run worker: task factory unavailable",
    )

    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert row["stop_reason"] == "worker_attachment_failed"
    assert events[-1]["lifecycle_type"] == LifecycleType.failed
    assert events[-1]["payload"]["reason"] == "worker_attachment_failed"
    _assert_latest_agrees(row, events)


@pytest.mark.anyio
async def test_orphan_recovery_records_failed_with_stable_reason() -> None:
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
    await store.put(
        "orphan",
        thread_id="thread-1",
        status="running",
        owner_worker_id="dead-worker",
        lease_expires_at=expired,
        created_at=expired,
    )
    manager = RunManager(store=store)

    recovered = await manager.reconcile_orphaned_inflight_runs(error="owner lost")

    assert [record.run_id for record in recovered] == ["orphan"]
    row = await store.get("orphan")
    events = await store.list_lifecycle_events(run_id="orphan")
    assert row is not None
    assert (row["status"], row["stop_reason"]) == (
        "error",
        ORPHAN_RECOVERY_STOP_REASON,
    )
    assert events[-1]["lifecycle_type"] == LifecycleType.failed
    assert events[-1]["payload"]["reason"] == ORPHAN_RECOVERY_STOP_REASON
    _assert_latest_agrees(row, events)


@pytest.mark.anyio
async def test_duplicate_terminal_writer_changes_no_version_or_event() -> None:
    manager, store, record = await _manager_and_run()
    await manager.set_status(record.run_id, RunStatus.success)
    before = await store.get(record.run_id)
    before_events = await store.list_lifecycle_events(run_id=record.run_id)

    await manager.set_status(record.run_id, RunStatus.success)

    after = await store.get(record.run_id)
    after_events = await store.list_lifecycle_events(run_id=record.run_id)
    assert before is not None and after is not None
    assert after["state_version"] == before["state_version"]
    assert after_events == before_events


@pytest.mark.anyio
async def test_missing_authoritative_row_is_never_resurrected_as_terminal_snapshot() -> None:
    manager, store, record = await _manager_and_run()
    await store.delete(record.run_id)

    await manager.set_status(record.run_id, RunStatus.success)

    assert await store.get(record.run_id) is None
    assert await store.list_lifecycle_events(run_id=record.run_id) == []


@pytest.mark.anyio
async def test_cancelled_admission_compensation_preserves_cancelled_mapping() -> None:
    class FailInitialCancellationStore(MemoryRunStore):
        durable_lifecycle = True

        def __init__(self) -> None:
            super().__init__()
            self.cancel_transition_attempts = 0

        async def transition_owned_run_atomic(
            self,
            run_id: str,
            *,
            expected_state_version: int,
            expected_statuses: tuple[str, ...] | None,
            transition: LifecycleTransition,
            expected_owner_worker_id: str,
            require_unexpired_lease: bool,
            user_id: str | None = None,
        ) -> LifecycleTransitionResult:
            if transition.lifecycle_type == LifecycleType.cancelled:
                self.cancel_transition_attempts += 1
                if self.cancel_transition_attempts <= 2:
                    return LifecycleTransitionResult(
                        applied=False,
                        row=await self.get(run_id),
                    )
            return await super().transition_owned_run_atomic(
                run_id,
                expected_state_version=expected_state_version,
                expected_statuses=expected_statuses,
                transition=transition,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
                user_id=user_id,
            )

    store = FailInitialCancellationStore()
    manager = RunManager(store=store)
    record = await manager.create("thread-1")

    await manager._close_cancelled_admission(
        record,
        claim_manager_admission=True,
    )
    assert await manager.drain_admission_compensations(timeout=1) is True

    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert row["status"] == "interrupted"
    assert [event["lifecycle_type"] for event in events] == [
        LifecycleType.accepted,
        LifecycleType.cancellation_requested,
        LifecycleType.cancelled,
    ]


@pytest.mark.anyio
async def test_cancellation_wins_pending_to_started_race_without_graph_start() -> None:
    manager, store, record = await _manager_and_run(running=False)
    requested = await store.request_cancel_compat(record.run_id, action="rollback")

    start = await manager.try_start(record.run_id)

    assert requested.outcome.value == "requested"
    assert start.value == "cancelled"
    assert record.abort_event.is_set()
    assert record.abort_action == "rollback"
    assert [event["lifecycle_type"] for event in await store.list_lifecycle_events(run_id=record.run_id)] == [LifecycleType.accepted, LifecycleType.cancellation_requested]


@pytest.mark.anyio
async def test_cancel_and_finalization_races_have_one_authoritative_winner() -> None:
    _, cancel_store, cancel_record = await _manager_and_run()
    await cancel_store.request_cancel_compat(cancel_record.run_id, action="interrupt")
    completion_lost = await cancel_store.finalize_if_not_cancelled(
        cancel_record.run_id,
        status="success",
    )
    assert completion_lost.finalized is False
    assert completion_lost.cancel_action == "interrupt"

    _, complete_store, complete_record = await _manager_and_run()
    completion_won = await complete_store.finalize_if_not_cancelled(
        complete_record.run_id,
        status="success",
    )
    cancellation_lost = await complete_store.request_cancel_compat(
        complete_record.run_id,
        action="interrupt",
    )
    assert completion_won.finalized is True
    assert cancellation_lost.outcome.value == "already_terminal"
    assert [event["lifecycle_type"] for event in await complete_store.list_lifecycle_events(run_id=complete_record.run_id)] == [LifecycleType.accepted, LifecycleType.started, LifecycleType.succeeded]


@pytest.mark.anyio
async def test_stale_owner_cannot_finalize_after_active_owner_changes() -> None:
    _, store, record = await _manager_and_run()
    stale_epoch = record.state_version
    row = store._runs[record.run_id]
    row["owner_worker_id"] = "replacement-worker"
    row["state_version"] += 1
    row["lease_expires_at"] = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()

    result = await store.finalize_if_not_cancelled(
        record.run_id,
        status="success",
        expected_owner_worker_id=record.owner_worker_id,
        expected_state_version=stale_epoch,
        require_unexpired_lease=True,
    )

    assert result.finalized is False
    assert (await store.get(record.run_id))["status"] == "running"


@pytest.mark.anyio
async def test_peer_terminal_row_never_mints_local_checkpoint_authority() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id="gateway-a",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create("thread-peer-terminal")
    assert await manager.try_start(record.run_id)

    peer_row = store._runs[record.run_id]
    peer_row["status"] = "error"
    peer_row["state_version"] += 1
    peer_row["owner_worker_id"] = None
    peer_row["lease_expires_at"] = None

    await manager.set_status(record.run_id, RunStatus.error)

    assert record.ownership_lost is True
    assert record.checkpoint_execution_fence_revoked is True
    assert record.checkpoint_terminal_state_version is None


@pytest.mark.anyio
async def test_own_terminal_cas_mints_local_checkpoint_authority() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id="gateway-a",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create("thread-own-terminal")
    await manager.try_start(record.run_id)

    await manager.set_status(record.run_id, RunStatus.success)

    assert record.ownership_lost is False
    assert record.checkpoint_execution_fence_revoked is False
    assert record.checkpoint_terminal_state_version == record.state_version


@pytest.mark.anyio
async def test_owned_finalize_if_not_cancelled_mints_local_checkpoint_authority() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id="gateway-a",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create("thread-owned-finalize")
    await manager.try_start(record.run_id)

    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)

    assert record.ownership_lost is False
    assert record.checkpoint_execution_fence_revoked is False
    assert record.checkpoint_terminal_state_version == record.state_version


@pytest.mark.anyio
async def test_peer_owned_active_row_cannot_be_terminalized_locally() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id="gateway-a",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create("thread-peer-owner")
    await manager.try_start(record.run_id)
    peer_row = store._runs[record.run_id]
    peer_row["owner_worker_id"] = "gateway-peer"
    peer_row["state_version"] += 1
    peer_row["lease_expires_at"] = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    manager._sync_record_from_store_row(record, peer_row)

    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "running"
    assert stored["owner_worker_id"] == "gateway-peer"
    assert record.ownership_lost is True
    assert record.checkpoint_terminal_state_version is None


@pytest.mark.anyio
async def test_memory_store_execution_fence_fails_closed() -> None:
    store = MemoryRunStore()
    async with store.hold_execution_fence(
        "run-1",
        owner_worker_id="gateway-a",
        state_version=2,
        terminal_state_version=3,
    ) as active:
        assert active is False


@pytest.mark.anyio
async def test_shutdown_interruption_is_not_misclassified_as_user_cancellation() -> None:
    manager, store, record = await _manager_and_run()
    blocker = asyncio.Event()
    record.task = asyncio.create_task(blocker.wait())
    await asyncio.sleep(0)

    await manager.shutdown(timeout=1.0)

    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert row["status"] == "interrupted"
    assert events[-1]["lifecycle_type"] == LifecycleType.interrupted
    assert LifecycleType.cancelled not in [event["lifecycle_type"] for event in events]
    _assert_latest_agrees(row, events)

    await manager.shutdown(timeout=1.0)
    repeated = await store.list_lifecycle_events(run_id=record.run_id)
    assert repeated == events

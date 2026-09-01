from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from deerflow.runtime.events.store.jsonl import JsonlRunEventStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    RunEventToolReceiptSink,
    ToolAttemptContextV1,
    ToolDispatchObservationV1,
    ToolEvidenceRuntimeBinding,
    ToolReceiptIntegrityError,
    ToolReceiptOwnershipLost,
)

_DISPATCH_1 = ToolDispatchObservationV1(
    lineage_digest="1" * 64,
    node_attempt=1,
)
_DISPATCH_2 = ToolDispatchObservationV1(
    lineage_digest="1" * 64,
    node_attempt=2,
)


class _OwnedRunStore:
    def __init__(self) -> None:
        self.row = {
            "run_id": "run-1",
            "thread_id": "thread-1",
            "user_id": "user-1",
            "operation_kind": "run",
            "status": "running",
            "owner_worker_id": "worker-1",
            "state_version": 5,
            "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }

    async def authoritative_get(self, run_id: str) -> dict | None:
        return dict(self.row) if run_id == self.row["run_id"] else None


def _binding(runs: _OwnedRunStore) -> ToolEvidenceRuntimeBinding:
    return ToolEvidenceRuntimeBinding(
        run_id="run-1",
        execution_task_id="task-1",
        execution_kind="subagent",
        subagent_name="researcher",
        owner_id=str(runs.row["owner_worker_id"]),
        lease_epoch=int(runs.row["state_version"]),
        agent_revision_digest="a" * 64,
        assembly_fingerprint="b" * 64,
        extension_generation=3,
        subagent_catalog_digest="c" * 64,
        subagent_definition_digest="d" * 64,
    )


def _body(phase: str = "started") -> dict[str, object]:
    started = DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id="run-1",
            execution_task_id="run-1",
            execution_kind="lead",
            subagent_name=None,
            tool_call_id="call-1",
            attempt=1,
            owner_id="worker-1",
            lease_epoch=5,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            extension_generation=3,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest=None,
        ),
        tool_name="web_search",
        request_projection_digest="d" * 64,
    )
    if phase == "started":
        return started.to_event_body()
    return started.outcome(
        phase=phase,  # type: ignore[arg-type]
        result_projection_digest="e" * 64 if phase == "succeeded" else None,
        result_kind="tool_message" if phase == "succeeded" else None,
        safe_error_code=None if phase == "succeeded" else "tool_error",
    ).to_event_body()


async def _append(
    store,
    *,
    event_type: str = "tool_receipt.started.v1",
    key: str | None = None,
    body: dict | None = None,
):
    logical_body = dict(body or _body())
    key = key or str(logical_body["idempotency_key"])
    return await store.append_idempotent(
        "run-1",
        event_type=event_type,
        idempotency_key=key,
        body=logical_body,
        owner_id="worker-1",
        lease_epoch=5,
    )


@pytest.fixture(params=["memory", "jsonl"])
def local_store(request, tmp_path):
    runs = _OwnedRunStore()
    if request.param == "memory":
        return MemoryRunEventStore(run_store=runs), runs
    return JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs), runs


@pytest.mark.anyio
async def test_identical_append_is_idempotent_and_keeps_store_timestamp(local_store) -> None:
    store, _ = local_store
    first = await _append(store)
    duplicate = await _append(store, body=dict(reversed(list(_body().items()))))

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.event == first.event
    assert first.event["created_at"]
    assert len(await store.list_events("thread-1", "run-1")) == 1


@pytest.mark.anyio
async def test_conflicting_duplicate_is_integrity_failure(local_store) -> None:
    store, _ = local_store
    await _append(store)

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_idempotency_conflict"):
        await _append(store, body={**_body(), "tool_name": "different_tool"})


@pytest.mark.anyio
async def test_concurrent_duplicates_produce_one_event(local_store) -> None:
    store, _ = local_store
    outcomes = await asyncio.gather(*(_append(store) for _ in range(12)))

    assert sum(outcome.created for outcome in outcomes) == 1
    assert len(await store.list_events("thread-1", "run-1")) == 1


@pytest.mark.anyio
async def test_stale_or_expired_owner_cannot_append(local_store) -> None:
    store, runs = local_store
    runs.row["state_version"] = 6
    with pytest.raises(ToolReceiptOwnershipLost, match="tool_receipt_ownership_lost"):
        await _append(store)


@pytest.mark.anyio
async def test_receipt_sink_reports_rejected_stale_append(local_store) -> None:
    store, runs = local_store
    rejected = AsyncMock()
    sink = RunEventToolReceiptSink(
        store,
        on_ownership_lost=rejected,
    )
    receipt = DurableToolReceiptV1.from_event_body(
        _body(),
        occurred_at=datetime.now(UTC),
    )
    runs.row["state_version"] = 6

    with pytest.raises(ToolReceiptOwnershipLost, match="tool_receipt_ownership_lost"):
        await sink.record_started(receipt)

    rejected.assert_awaited_once_with("append")

    runs.row["state_version"] = 5
    runs.row["lease_expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    with pytest.raises(ToolReceiptOwnershipLost, match="tool_receipt_ownership_lost"):
        await _append(store)


@pytest.mark.anyio
async def test_attempt_reservation_rejects_binding_from_another_fence(
    local_store,
) -> None:
    store, runs = local_store
    stale_binding = _binding(runs)
    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 6

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_attempt_binding_invalid"):
        await store.reserve_tool_attempt(
            "run-1",
            binding=stale_binding,
            tool_call_id="call-mixed-fence",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            observed_node_attempt=1,
            expected_attempt=None,
            owner_id="worker-2",
            lease_epoch=6,
        )
    assert await store.list_events("thread-1", "run-1") == []


@pytest.mark.anyio
async def test_start_then_terminal_are_monotonic_and_terminals_conflict(local_store) -> None:
    store, _ = local_store
    started = await _append(store)
    succeeded = await _append(
        store,
        event_type="tool_receipt.outcome.v1",
        body=_body("succeeded"),
    )

    assert started.event["seq"] < succeeded.event["seq"]
    with pytest.raises(ToolReceiptIntegrityError):
        await _append(
            store,
            event_type="tool_receipt.outcome.v1",
            body={**_body("failed"), "safe_error_code": "tool_error"},
        )


@pytest.mark.anyio
async def test_terminal_without_start_is_rejected(local_store) -> None:
    store, _ = local_store

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_start_missing"):
        await _append(
            store,
            event_type="tool_receipt.outcome.v1",
            body=_body("succeeded"),
        )


@pytest.mark.anyio
async def test_jsonl_reopen_rebuilds_dedupe_index(tmp_path) -> None:
    runs = _OwnedRunStore()
    first_store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    first = await _append(first_store)
    reopened = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)

    duplicate = await _append(reopened)

    assert duplicate.created is False
    assert duplicate.event == first.event
    assert len(await reopened.list_events("thread-1", "run-1")) == 1


@pytest.mark.anyio
async def test_jsonl_ownership_transfer_during_read_cannot_commit_receipt(
    tmp_path,
    monkeypatch,
) -> None:
    runs = _OwnedRunStore()
    store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    original_read = store._read_run_events

    def transfer_then_read(thread_id: str, run_id: str) -> list[dict]:
        events = original_read(thread_id, run_id)
        runs.row["owner_worker_id"] = "worker-2"
        runs.row["state_version"] = 6
        return events

    monkeypatch.setattr(store, "_read_run_events", transfer_then_read)

    with pytest.raises(ToolReceiptOwnershipLost, match="tool_receipt_ownership_lost"):
        await _append(store)
    assert await store.list_events("thread-1", "run-1") == []


@pytest.mark.anyio
async def test_jsonl_ownership_transfer_during_prepare_cannot_commit_receipt(
    tmp_path,
    monkeypatch,
) -> None:
    runs = _OwnedRunStore()
    store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    original_prepare = store._prepare_record_replace

    def prepare_then_transfer(record: dict) -> tuple:
        prepared = original_prepare(record)
        runs.row["owner_worker_id"] = "worker-2"
        runs.row["state_version"] = 6
        return prepared

    monkeypatch.setattr(store, "_prepare_record_replace", prepare_then_transfer)

    with pytest.raises(ToolReceiptOwnershipLost, match="tool_receipt_ownership_lost"):
        await _append(store)
    assert await store.list_events("thread-1", "run-1") == []
    assert list((tmp_path / "events").rglob("*.tmp")) == []


@pytest.mark.anyio
async def test_jsonl_bounded_dedupe_cache_falls_back_to_run_scan(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "deerflow.runtime.events.store.jsonl.MAX_JSONL_RECEIPT_DEDUPE_ENTRIES",
        2,
    )
    runs = _OwnedRunStore()
    store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    first = await _append(store)
    sink = RunEventToolReceiptSink(store)
    for index in (2, 3):
        await sink.reserve_started(
            binding=_binding(runs),
            tool_call_id=f"call-cache-{index}",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=_DISPATCH_1,
        )

    assert len(store._dedupe_index) == 2
    duplicate = await _append(store)
    assert duplicate.created is False
    assert duplicate.event == first.event
    assert len(store._dedupe_index) == 2
    assert len(await store.list_events("thread-1", "run-1")) == 3


@pytest.mark.anyio
async def test_jsonl_reopen_reuses_unfinished_attempt_reservation(tmp_path) -> None:
    runs = _OwnedRunStore()
    first_store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    first = await RunEventToolReceiptSink(first_store).reserve_started(
        binding=_binding(runs),
        tool_call_id="call-reopen",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    reopened = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    replay = await RunEventToolReceiptSink(reopened).reserve_started(
        binding=_binding(runs),
        tool_call_id="call-reopen",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    assert replay.started.receipt_id == first.started.receipt_id
    assert len(await reopened.list_events("thread-1", "run-1")) == 1


@pytest.mark.anyio
async def test_attempt_reservation_reuses_crash_gap_across_store_restart(local_store) -> None:
    store, runs = local_store
    first_sink = RunEventToolReceiptSink(store)
    first = await first_sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-recovery",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    # Simulate a new worker process taking ownership after the first worker
    # persisted start but died before a terminal event.
    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 6
    recovered_sink = RunEventToolReceiptSink(store)
    recovered_binding = _binding(runs)
    recovered = await recovered_sink.reserve_started(
        binding=recovered_binding,
        tool_call_id="call-recovery",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    assert recovered.started.receipt_id == first.started.receipt_id
    assert recovered.started.context.attempt == 1
    assert recovered.started.context.owner_id == "worker-1"
    assert len(await store.list_events("thread-1", "run-1")) == 1

    await recovered_sink.record_outcome(
        recovered.started.outcome(
            phase="succeeded",
            result_projection_digest="f" * 64,
            result_kind="tool_message",
            safe_error_code=None,
        )
    )
    next_attempt = await recovered_sink.reserve_started(
        binding=recovered_binding,
        tool_call_id="call-recovery",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_2,
    )
    assert next_attempt.started.context.attempt == 2
    assert next_attempt.started.receipt_id != first.started.receipt_id


@pytest.mark.anyio
async def test_completed_attempt_replay_under_new_fence_does_not_reserve_again(
    local_store,
) -> None:
    store, runs = local_store
    first_sink = RunEventToolReceiptSink(store)
    first = await first_sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-completed-replay",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )
    await first_sink.record_outcome(
        first.started.outcome(
            phase="succeeded",
            result_projection_digest="f" * 64,
            result_kind="tool_message",
            safe_error_code=None,
        )
    )

    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 6
    replay = await RunEventToolReceiptSink(store).reserve_started(
        binding=_binding(runs),
        tool_call_id="call-completed-replay",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    assert replay.started.receipt_id == first.started.receipt_id
    assert replay.replayed_outcome is not None
    assert replay.replayed_outcome.phase == "succeeded"
    assert len(await store.list_events("thread-1", "run-1")) == 2


@pytest.mark.anyio
async def test_recovery_retry_reset_reuses_latest_unfinished_attempt(
    local_store,
) -> None:
    store, runs = local_store
    original_sink = RunEventToolReceiptSink(store)
    original_binding = _binding(runs)
    first = await original_sink.reserve_started(
        binding=original_binding,
        tool_call_id="call-retry-reset-gap",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )
    await original_sink.record_outcome(
        first.started.outcome(
            phase="failed",
            result_projection_digest=None,
            result_kind=None,
            safe_error_code="tool_error",
        )
    )
    second = await original_sink.reserve_started(
        binding=original_binding,
        tool_call_id="call-retry-reset-gap",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_2,
    )
    assert second.started.context.attempt == 2

    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 6
    recovered_sink = RunEventToolReceiptSink(store)
    recovered_binding = _binding(runs)
    recovered = await recovered_sink.reserve_started(
        binding=recovered_binding,
        tool_call_id="call-retry-reset-gap",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    assert recovered.started.context.attempt == 2
    assert recovered.replayed_outcome is None
    await recovered_sink.record_outcome(
        recovered.started.outcome(
            phase="failed",
            result_projection_digest=None,
            result_kind=None,
            safe_error_code="tool_error",
        )
    )
    next_retry = await recovered_sink.reserve_started(
        binding=recovered_binding,
        tool_call_id="call-retry-reset-gap",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_2,
    )
    assert next_retry.started.context.attempt == 3


@pytest.mark.anyio
async def test_recovery_retry_reset_replays_latest_completed_attempt(
    local_store,
) -> None:
    store, runs = local_store
    original_sink = RunEventToolReceiptSink(store)
    original_binding = _binding(runs)
    first = await original_sink.reserve_started(
        binding=original_binding,
        tool_call_id="call-retry-reset-terminal",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )
    await original_sink.record_outcome(
        first.started.outcome(
            phase="failed",
            result_projection_digest=None,
            result_kind=None,
            safe_error_code="tool_error",
        )
    )
    second = await original_sink.reserve_started(
        binding=original_binding,
        tool_call_id="call-retry-reset-terminal",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_2,
    )
    await original_sink.record_outcome(
        second.started.outcome(
            phase="failed",
            result_projection_digest=None,
            result_kind=None,
            safe_error_code="tool_error",
        )
    )

    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 6
    recovered = await RunEventToolReceiptSink(store).reserve_started(
        binding=_binding(runs),
        tool_call_id="call-retry-reset-terminal",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    assert recovered.started.context.attempt == 2
    assert recovered.replayed_outcome is not None


@pytest.mark.anyio
async def test_completed_attempt_replay_under_same_fence_does_not_reserve_again(
    local_store,
) -> None:
    store, runs = local_store
    sink = RunEventToolReceiptSink(store)
    binding = _binding(runs)
    first = await sink.reserve_started(
        binding=binding,
        tool_call_id="call-same-fence-completed",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )
    await sink.record_outcome(
        first.started.outcome(
            phase="succeeded",
            result_projection_digest="f" * 64,
            result_kind="tool_message",
            safe_error_code=None,
        )
    )

    replay = await sink.reserve_started(
        binding=binding,
        tool_call_id="call-same-fence-completed",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    assert replay.started.receipt_id == first.started.receipt_id
    assert replay.replayed_outcome is not None
    assert len(await store.list_events("thread-1", "run-1")) == 2


@pytest.mark.anyio
async def test_concurrent_attempt_reservations_share_one_durable_start(local_store) -> None:
    store, runs = local_store
    sinks = [RunEventToolReceiptSink(store) for _ in range(12)]
    receipts = await asyncio.gather(
        *(
            sink.reserve_started(
                binding=_binding(runs),
                tool_call_id="call-concurrent",
                tool_name="web_search",
                request_projection_digest="e" * 64,
                dispatch=_DISPATCH_1,
            )
            for sink in sinks
        )
    )

    assert {receipt.started.receipt_id for receipt in receipts} == {receipts[0].started.receipt_id}
    assert len(await store.list_events("thread-1", "run-1")) == 1


@pytest.mark.anyio
async def test_reconstructed_dispatch_cannot_skip_durable_attempts(local_store) -> None:
    store, runs = local_store
    sink = RunEventToolReceiptSink(store)
    await sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-gap",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_attempt_history_invalid"):
        await sink.reserve_started(
            binding=_binding(runs),
            tool_call_id="call-gap",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=ToolDispatchObservationV1(
                lineage_digest=_DISPATCH_1.lineage_digest,
                node_attempt=3,
            ),
        )


@pytest.mark.anyio
async def test_attempt_reservation_rejects_changed_recovery_projection(local_store) -> None:
    store, runs = local_store
    sink = RunEventToolReceiptSink(store)
    await sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-conflict",
        tool_name="web_search",
        request_projection_digest="e" * 64,
        dispatch=_DISPATCH_1,
    )

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_attempt_replay_conflict"):
        await sink.reserve_started(
            binding=_binding(runs),
            tool_call_id="call-conflict",
            tool_name="web_search",
            request_projection_digest="f" * 64,
            dispatch=_DISPATCH_1,
        )

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_attempt_replay_conflict"):
        await sink.reserve_started(
            binding=ToolEvidenceRuntimeBinding(
                run_id="run-1",
                execution_task_id="task-1",
                execution_kind="subagent",
                subagent_name="researcher",
                owner_id="worker-1",
                lease_epoch=5,
                agent_revision_digest="a" * 64,
                assembly_fingerprint="b" * 64,
                extension_generation=3,
                subagent_catalog_digest="c" * 64,
                subagent_definition_digest="9" * 64,
            ),
            tool_call_id="call-conflict",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=_DISPATCH_1,
        )


@pytest.mark.anyio
async def test_database_append_is_fenced_idempotent_and_recoverable(tmp_path) -> None:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.run.model import RunRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    url = f"sqlite+aiosqlite:///{tmp_path / 'receipt-events.db'}"
    await init_engine("sqlite", url=url, sqlite_dir=str(tmp_path))
    try:
        session_factory = get_session_factory()
        async with session_factory() as session:
            session.add(
                RunRow(
                    run_id="run-1",
                    thread_id="thread-1",
                    user_id="user-1",
                    status="running",
                    state_version=5,
                    operation_kind="run",
                    owner_worker_id="worker-1",
                    lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                )
            )
            await session.commit()

        store = DbRunEventStore(session_factory)
        outcomes = await asyncio.gather(*(_append(store) for _ in range(8)))
        assert sum(outcome.created for outcome in outcomes) == 1
        reopened = DbRunEventStore(session_factory)
        duplicate = await _append(reopened)
        assert duplicate.created is False
        assert len(await reopened.list_events("thread-1", "run-1", user_id=None)) == 1

        reservation_binding = ToolEvidenceRuntimeBinding(
            run_id="run-1",
            execution_task_id="task-db",
            execution_kind="subagent",
            subagent_name="researcher",
            owner_id="worker-1",
            lease_epoch=5,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            extension_generation=3,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest="d" * 64,
        )
        reserved = await RunEventToolReceiptSink(store).reserve_started(
            binding=reservation_binding,
            tool_call_id="call-db-recovery",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=_DISPATCH_1,
        )
        replay_sink = RunEventToolReceiptSink(reopened)
        replay = await replay_sink.reserve_started(
            binding=reservation_binding,
            tool_call_id="call-db-recovery",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=_DISPATCH_1,
        )
        assert replay.started.receipt_id == reserved.started.receipt_id
        await replay_sink.record_outcome(
            replay.started.outcome(
                phase="succeeded",
                result_projection_digest="f" * 64,
                result_kind="tool_message",
                safe_error_code=None,
            )
        )
        completed_replay = await RunEventToolReceiptSink(reopened).reserve_started(
            binding=reservation_binding,
            tool_call_id="call-db-recovery",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=_DISPATCH_1,
        )
        assert completed_replay.started.receipt_id == reserved.started.receipt_id
        assert completed_replay.replayed_outcome is not None
        next_attempt = await replay_sink.reserve_started(
            binding=reservation_binding,
            tool_call_id="call-db-recovery",
            tool_name="web_search",
            request_projection_digest="e" * 64,
            dispatch=_DISPATCH_2,
        )
        assert next_attempt.started.context.attempt == 2

        with pytest.raises(ToolReceiptIntegrityError):
            await _append(reopened, body={**_body(), "tool_name": "conflict"})

        async with session_factory() as session:
            row = await session.get(RunRow, "run-1")
            assert row is not None
            row.state_version = 6
            await session.commit()
        with pytest.raises(ToolReceiptOwnershipLost):
            await _append(reopened, event_type="tool_receipt.outcome.v1", body=_body("succeeded"))
    finally:
        await close_engine()

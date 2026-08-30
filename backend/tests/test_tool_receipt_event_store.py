from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from deerflow.runtime.events.store.jsonl import JsonlRunEventStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    RunEventToolReceiptSink,
    ToolAttemptContextV1,
    ToolEvidenceRuntimeBinding,
    ToolReceiptIntegrityError,
    ToolReceiptOwnershipLost,
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


async def _append(store, *, event_type: str = "tool_receipt.started.v1", key: str | None = None, body: dict | None = None):
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

    runs.row["state_version"] = 5
    runs.row["lease_expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    with pytest.raises(ToolReceiptOwnershipLost, match="tool_receipt_ownership_lost"):
        await _append(store)


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
async def test_jsonl_reopen_reuses_unfinished_attempt_reservation(tmp_path) -> None:
    runs = _OwnedRunStore()
    first_store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    first = await RunEventToolReceiptSink(first_store).reserve_started(
        binding=_binding(runs),
        tool_call_id="call-reopen",
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )

    reopened = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    replay = await RunEventToolReceiptSink(reopened).reserve_started(
        binding=_binding(runs),
        tool_call_id="call-reopen",
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )

    assert replay.receipt_id == first.receipt_id
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
    )

    # Simulate a new worker process taking ownership after the first worker
    # persisted start but died before a terminal event.
    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 6
    recovered_sink = RunEventToolReceiptSink(store)
    recovered = await recovered_sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-recovery",
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )

    assert recovered.receipt_id == first.receipt_id
    assert recovered.context.attempt == 1
    assert recovered.context.owner_id == "worker-1"
    assert len(await store.list_events("thread-1", "run-1")) == 1

    await recovered_sink.record_outcome(
        recovered.outcome(
            phase="succeeded",
            result_projection_digest="f" * 64,
            result_kind="tool_message",
            safe_error_code=None,
        )
    )
    next_attempt = await recovered_sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-recovery",
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )
    assert next_attempt.context.attempt == 2
    assert next_attempt.receipt_id != first.receipt_id


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
            )
            for sink in sinks
        )
    )

    assert {receipt.receipt_id for receipt in receipts} == {receipts[0].receipt_id}
    assert len(await store.list_events("thread-1", "run-1")) == 1


@pytest.mark.anyio
async def test_attempt_reservation_rejects_changed_recovery_projection(local_store) -> None:
    store, runs = local_store
    sink = RunEventToolReceiptSink(store)
    await sink.reserve_started(
        binding=_binding(runs),
        tool_call_id="call-conflict",
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )

    with pytest.raises(ToolReceiptIntegrityError, match="receipt_attempt_replay_conflict"):
        await sink.reserve_started(
            binding=_binding(runs),
            tool_call_id="call-conflict",
            tool_name="web_search",
            request_projection_digest="f" * 64,
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
        )
        replay_sink = RunEventToolReceiptSink(reopened)
        replay = await replay_sink.reserve_started(
            binding=reservation_binding,
            tool_call_id="call-db-recovery",
            tool_name="web_search",
            request_projection_digest="e" * 64,
        )
        assert replay.receipt_id == reserved.receipt_id
        await replay_sink.record_outcome(
            replay.outcome(
                phase="succeeded",
                result_projection_digest="f" * 64,
                result_kind="tool_message",
                safe_error_code=None,
            )
        )
        next_attempt = await replay_sink.reserve_started(
            binding=reservation_binding,
            tool_call_id="call-db-recovery",
            tool_name="web_search",
            request_projection_digest="e" * 64,
        )
        assert next_attempt.context.attempt == 2

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

"""Authoritative output proof is distinct from a staged local run record."""

import asyncio
import traceback
from copy import deepcopy

import pytest
from _run_output_store_test_support import RAW_OWNER, capture_run
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import event, update
from sqlalchemy.util import await_only

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore


@pytest.mark.asyncio
async def test_snapshot_uses_actual_store_instead_of_staged_cached_status(output_store):
    manager, record = await capture_run(output_store)
    await manager.set_status(record.run_id, RunStatus.success, persist=False)
    assert (await output_store.repository.get(record.run_id, user_id=RAW_OWNER))["status"] == "running"
    # The baseline only has get(), whose documented cached-positive behavior is
    # exactly why a separate output snapshot is needed. This is not a request
    # to change that legacy API. Once available, exercise the new public seam.
    snapshot_reader = getattr(manager, "get_output_snapshot", None)
    if snapshot_reader is None:
        snapshot = await manager.get(record.run_id, user_id=RAW_OWNER, require_store=True, raise_on_store_error=True)
    else:
        snapshot = await snapshot_reader(record.run_id, user_id=RAW_OWNER)
    assert snapshot.status == RunStatus.running


@pytest.mark.asyncio
async def test_new_snapshot_capability_is_explicit(output_store):
    manager, _record = await capture_run(output_store)
    assert callable(getattr(manager, "get_output_snapshot", None))


@pytest.mark.asyncio
async def test_snapshot_is_detached_from_local_and_stored_mutable_kwargs(output_store):
    manager, record = await capture_run(output_store)
    snapshot = await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    record.kwargs["host_value"]["nested"].append("local-mutation")
    assert snapshot.owner_user_id == RAW_OWNER
    assert snapshot.kwargs["host_value"]["nested"] == ("retained",)
    with pytest.raises(TypeError):
        snapshot.kwargs["host_value"]["nested"][0] = "forged"
    assert (await output_store.repository.get(record.run_id, user_id=RAW_OWNER))["kwargs"]["host_value"]["nested"] == ["retained"]


@pytest.mark.asyncio
async def test_snapshot_foreign_owner_is_not_acknowledgeable_absence(output_store):
    from deerflow.runtime.runs.output import OutputEvidenceScopeError

    manager, record = await capture_run(output_store)
    with pytest.raises(OutputEvidenceScopeError):
        await manager.get_output_snapshot(record.run_id, user_id="some-other-owner")
    assert await manager.get_output_snapshot("not-a-record", user_id=RAW_OWNER) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["not_mapping", "missing_owner", "malformed_run", "empty_thread", "null_owner"])
async def test_snapshot_malformed_rows_are_not_an_observed_foreign_scope(output_store, monkeypatch, shape):
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, record = await capture_run(output_store)
    original = output_store.repository.get

    async def malformed(*args, **kwargs):
        row = await original(*args, **kwargs)
        if shape == "not_mapping":
            return []
        if shape == "missing_owner":
            row.pop("user_id")
        elif shape == "malformed_run":
            row["run_id"] = 17
        elif shape == "empty_thread":
            row["thread_id"] = ""
        elif shape == "null_owner":
            row["user_id"] = None
        return row

    monkeypatch.setattr(output_store.repository, "get", malformed)
    with pytest.raises(OutputEvidenceError) as caught:
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    assert type(caught.value) is OutputEvidenceError


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", ["", None, False, 17])
async def test_snapshot_invalid_requested_identity_cannot_be_a_completed_miss(output_store, monkeypatch, run_id):
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, _record = await capture_run(output_store)
    calls = []

    async def missing(*args, **kwargs):
        calls.append((args, kwargs))
        return None

    monkeypatch.setattr(output_store.repository, "get", missing)
    with pytest.raises(OutputEvidenceError):
        await manager.get_output_snapshot(run_id, user_id=RAW_OWNER)
    assert not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [" raw owner with spaces ", "opaque:\towner"])
async def test_snapshot_preserves_exact_nonempty_raw_owner(output_store, owner):
    manager, record = await capture_run(output_store, owner=owner)
    snapshot = await manager.get_output_snapshot(record.run_id, user_id=owner)
    assert snapshot.owner_user_id == owner
    assert (await output_store.repository.get(record.run_id, user_id=owner))["user_id"] == owner


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [None, {"version": True}, {"version": 2}])
async def test_snapshot_does_not_wait_for_a_non_capture_or_malformed_intent(output_store, capture):
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, record = await capture_run(output_store)
    kwargs = deepcopy(record.kwargs)
    if capture is None:
        kwargs.pop("output_capture")
    else:
        kwargs["output_capture"] = capture
    async with output_store.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs))
    with pytest.raises(OutputEvidenceError):
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)


@pytest.mark.asyncio
@pytest.mark.parametrize("stored", ["active_seal", "foreign_seal", "wrong_status"])
async def test_snapshot_rejects_inconsistent_actual_stored_evidence(output_store, stored):
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, record = await capture_run(output_store)
    source = {"version": 1, "kind": "text", "owner_user_id": RAW_OWNER, "thread_id": record.thread_id, "run_id": record.run_id, "input_message_id": None, "assistant_message_id": "actual-source", "event_seq": 2, "text_sha256": "a" * 64}
    if stored == "foreign_seal":
        source["owner_user_id"] = "different-raw-owner"
    kwargs = {**deepcopy(record.kwargs), "output_evidence": source}
    status = "running" if stored == "active_seal" else "error" if stored == "wrong_status" else "success"
    async with output_store.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(status=status, kwargs_json=kwargs))
    with pytest.raises(OutputEvidenceError):
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)


@pytest.mark.asyncio
async def test_unsupported_memory_keeps_ordinary_behavior_and_refuses_new_proof():
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager = RunManager(MemoryRunStore())
    record = await manager.create_or_reject("memory-thread", user_id=RAW_OWNER)
    assert manager.supports_output_receipts is False
    with pytest.raises(OutputEvidenceError):
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
    assert (await manager.get(record.run_id)).status == RunStatus.success


@pytest.mark.asyncio
async def test_snapshot_cancellation_retains_actual_driver_query_and_session(output_store):
    manager, record = await capture_run(output_store)
    entered, release, query_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    baseline = output_store.engine.sync_engine.pool.checkedout()

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if "FROM runs" in statement and "WHERE runs.run_id" in statement:

            async def hold():
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    query_cancelled.set()
                    raise

            await_only(hold())

    event.listen(output_store.engine.sync_engine, "after_cursor_execute", pause)
    task = asyncio.create_task(manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER))
    watcher = asyncio.create_task(query_cancelled.wait())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        done, _ = await asyncio.wait({task, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        assert not done, "proof read detached or canceled its admitted driver query"
        release.set()
        done, _ = await asyncio.wait({task}, timeout=5)
        assert task in done
        with pytest.raises(asyncio.CancelledError):
            await task
        assert output_store.engine.sync_engine.pool.checkedout() == baseline
    finally:
        release.set()
        watcher.cancel()
        await asyncio.gather(task, watcher, return_exceptions=True)
        event.remove(output_store.engine.sync_engine, "after_cursor_execute", pause)


@pytest.mark.asyncio
async def test_snapshot_store_failure_cannot_hide_behind_cached_success(output_store, monkeypatch):
    from deerflow.runtime.runs.output import OutputEvidenceError

    manager, record = await capture_run(output_store)
    await manager.set_status(record.run_id, RunStatus.success, persist=False)

    async def failure(*_args, **_kwargs):
        raise RuntimeError("private-output-and-SQL-parameters")

    monkeypatch.setattr(output_store.repository, "get", failure)
    with pytest.raises(OutputEvidenceError) as caught:
        await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    assert "private-output-and-SQL-parameters" not in "".join(traceback.format_exception(caught.value))

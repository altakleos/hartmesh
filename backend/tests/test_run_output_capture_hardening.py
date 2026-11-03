"""What a captured run may lose, and what it must not, when something around it fails."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from langgraph.checkpoint.memory import InMemorySaver
from test_run_output_capture_worker import _environment, _execute, _graph

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime.capture_resources import OwnedCheckpointSaver
from deerflow.runtime.runs.capture_io import CaptureIOClosed
from deerflow.runtime.runs.manager import ConflictError, RunManager
from deerflow.runtime.runs.schemas import RunStatus

CAPTURE = {"output_capture": {"version": 1}}


def lease() -> RunOwnershipConfig:
    return RunOwnershipConfig(heartbeat_enabled=True, lease_seconds=30, grace_seconds=10)


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["duration", "workspace"])
async def test_a_finished_answer_survives_a_write_neither_proof_needs(output_store, monkeypatch, failing):
    env = await _environment(output_store, monkeypatch)
    env.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())
    if failing == "duration":
        monkeypatch.setattr("deerflow.runtime.runs.worker._persist_run_duration", AsyncMock(side_effect=OSError("transient")))
    else:
        monkeypatch.setattr("deerflow.runtime.runs.worker.capture_workspace_snapshot", AsyncMock(return_value=SimpleNamespace(truncated=False)))
        monkeypatch.setattr("deerflow.runtime.runs.worker.record_workspace_changes", AsyncMock(side_effect=OSError("transient")))

    async def node(state, config):
        return {"messages": [await env.model.ainvoke(state["messages"], config)]}

    env.graph = _graph(node, checkpointer=env.checkpointer)
    await _execute(env)

    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] == "success" and not env.record.ownership_lost


@pytest.mark.asyncio
async def test_a_retired_owner_still_stops_a_captured_run_in_a_write_neither_proof_needs(output_store, monkeypatch):
    """Retirement is not a failed write: it stays the end of the run's I/O."""
    env = await _environment(output_store, monkeypatch)
    env.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())
    monkeypatch.setattr("deerflow.runtime.runs.worker._persist_run_duration", AsyncMock(side_effect=CaptureIOClosed()))

    async def node(state, config):
        return {"messages": [await env.model.ainvoke(state["messages"], config)]}

    env.graph = _graph(node, checkpointer=env.checkpointer)
    await _execute(env)

    row = await output_store.repository.get(env.record.run_id, user_id=RAW_OWNER)
    assert row["status"] != "success"


@pytest.mark.asyncio
async def test_a_refused_captured_admission_leaves_nothing_behind(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    # This worker holds no local record; only the durable row refuses.
    refusing = RunManager(output_store.repository, event_store=env.events)
    for _ in range(3):
        with pytest.raises(ConflictError):
            await refusing.create_or_reject(env.record.thread_id, "lead_agent", user_id=RAW_OWNER, kwargs=dict(CAPTURE))
    assert not refusing._capture_records and not refusing._capture_lifetimes


async def renewing_manager(output_store):
    manager = RunManager(store=output_store.repository, run_ownership_config=lease())
    first = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=RAW_OWNER, kwargs=dict(CAPTURE))
    second = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=RAW_OWNER, kwargs=dict(CAPTURE))
    await manager.set_status(first.run_id, RunStatus.running)
    await manager.set_status(second.run_id, RunStatus.running)
    return manager, first, second


@pytest.mark.asyncio
async def test_a_run_retiring_during_its_renewal_costs_no_other_run_its_lease(output_store):
    manager, first, second = await renewing_manager(output_store)
    soon = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    first.lease_expires_at = second.lease_expires_at = soon
    owner = manager.get_capture_io_owner(first)
    original = owner.run

    async def run(factory):
        # Retired in the loop tick between its own open check and the call's start.
        asyncio.get_running_loop().call_soon(owner.retire)
        return await original(factory)

    owner.run = run
    try:
        await manager._renew_leases()
    finally:
        owner.run = original
    assert second.lease_expires_at != soon, "A later run keeps its lease"
    await manager.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_the_heartbeat_outlives_a_retired_owner_and_ends_with_admissions(output_store, monkeypatch):
    manager = RunManager(store=output_store.repository, run_ownership_config=lease())
    manager._heartbeat_stop = asyncio.Event()
    cycles = []

    async def renew(*, wait=True):
        # Upstream's heartbeat renews with ``wait=False``; a stand-in that
        # rejects it would fail every cycle inside the loop's own guard.
        cycles.append(len(cycles))
        if len(cycles) == 2:
            manager.seal_capture_io_admissions()
        raise CaptureIOClosed()

    async def instantly(awaitable, timeout):
        awaitable.close()
        await asyncio.sleep(0)
        raise TimeoutError()

    monkeypatch.setattr(manager, "_renew_leases", renew)
    monkeypatch.setattr("deerflow.runtime.runs.manager.asyncio.wait_for", instantly)
    await manager._heartbeat_loop()
    assert cycles == [0, 1]

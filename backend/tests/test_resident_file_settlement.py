"""Exercise abandoned Home writes through the scheduled maintenance owner."""

import asyncio
import logging
import time
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, operation
from test_resident_home_write_recovery import execution as execution
from test_resident_home_write_recovery import fixture as fixture
from test_resident_home_write_recovery import instances as instances
from test_resident_home_write_recovery import turns as turns
from test_resident_home_write_recovery import work as work
from test_work_execution import activation

from app.gateway import resident_runtime, services
from app.gateway.resident_runtime import ResidentRuntime
from app.gateway.storage_spaces import _maintenance_step
from deerflow.agent_instances import runtime
from deerflow.agent_instances.residents import PreparationClaim, ResidentRegistry
from deerflow.persistence.run.model import CompletedRunSnapshotRow, RunChangeClockRow, RunProducerPolicyRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles, SpaceOperationPending
from deerflow.spaces.workflows import acquire, close


@pytest_asyncio.fixture
async def resident(turns):
    async with turns.sessions.kw["bind"].begin() as connection:
        for model in (RunRow, RunChangeClockRow, RunProducerPolicyRow, CompletedRunSnapshotRow):
            await connection.run_sync(model.__table__.create)
    turns.host.app.state.run_store = RunRepository(turns.sessions)
    turns.host.registry.run_manager._store = turns.host.app.state.run_store
    # Idle expiry is independent of the file maintenance passes under test.
    turns.host.registry.idle_timeout = 3600
    return turns


def pending(resident, action="remove"):
    return SpaceFileOperationRow(
        space_id=resident.original.instance.home_id,
        generation=resident.original.home_generation,
        operation_id=operation(),
        actor_kind="human",
        actor_id="alice",
        phase="pending",
        request={"action": action, "path": "note"},
    )


async def tick(resident):
    assert await _maintenance_step(resident.host.app, timeout=10)


async def read_state(resident, row):
    async with resident.sessions() as session:
        attachment = await session.get(SpaceAttachmentRow, resident.claim.id)
        operation = await session.get(SpaceFileOperationRow, (row.space_id, row.operation_id))
        return attachment.phase, attachment.fence_reason, operation.phase


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_two_passes_close_the_retained_workspace_then_settle_the_write(resident, monkeypatch, restart):
    row = pending(resident)
    async with resident.sessions.begin() as session:
        session.add(row)
    if restart:
        await tick(resident)
        old = resident.host
        app = old.app
        agents = app.state.agent_instances
        # Drop all file observations and local associations. Keep the SQL
        # database, Home directory and running container across the restart.
        files = SpaceFiles(SpaceRegistry(resident.sessions, resident.files.registry._resolver), resident.files.catalog)
        agents.files = app.state.storage_spaces = resident.files = files
        files.home_lookup = agents._bound_home_ids
        monkeypatch.setattr(files, "quota", AsyncMock(return_value={"available_bytes": 2 << 30, "available_inodes": 50000}))
        attachments = SpaceAttachments(files, resident.adapter)
        residents = ResidentRegistry(
            attachments=attachments,
            provider=old.provider,
            run_manager=old.registry.run_manager,
            settle=old.settle,
            owner_of=old.owner_of,
            replicas=old.registry.replicas,
            idle_timeout=3600,
        )
        old.provider.external_changed(old.admission.close)
        host = ResidentRuntime(app, registry=residents, admission=old.admission, entered=time.monotonic())
        agents.residents = residents
        attachments.on_contained, residents.owner_of, residents.settle = residents.contained, host.owner_of, host.settle
        residents.finish_restore(residents.begin_restore(), await attachments.retained_claims())
        resident.host = app.state.resident_runtime = host
        app.state.agent_execution = type(app.state.agent_execution)()
        host.coordinator.result = old.coordinator.result
        host._startup_settled = True
        host.provider.external_changed(lambda: host.admission.open(host.provider))
        monkeypatch.setattr(runtime, "qualified_attachments", AsyncMock(return_value=(host.provider, attachments)))
    await tick(resident)
    assert await read_state(resident, row) == ("active", None, "pending")
    assert (await resident.turn(resident.other)).status == RunStatus.success
    await tick(resident)
    assert await read_state(resident, row) == ("fenced", "outcome-uncertain", "complete")
    assert resident.claim.container_id not in resident.adapter.containers
    fresh = await resident.turn(resident.original.thread_id)
    assert fresh.status == RunStatus.success, fresh.error


@pytest.mark.asyncio
async def test_scheduled_closure_waits_for_a_running_reply(resident, monkeypatch):
    entered, finish = asyncio.Event(), asyncio.Event()
    graph = type(services.resolve_agent_factory(None)())
    stream = graph.astream

    async def held_stream(self, *args, **kwargs):
        if not entered.is_set():
            entered.set()
            await finish.wait()
        async for chunk in stream(self, *args, **kwargs):
            yield chunk

    monkeypatch.setattr(graph, "astream", held_stream)
    reply = asyncio.create_task(resident.turn(resident.original.thread_id))
    row = pending(resident)
    try:
        await asyncio.wait_for(entered.wait(), 10)
        active = await resident.host.app.state.run_store.list_inflight()
        assert [run["thread_id"] for run in active] == [resident.original.thread_id]
        async with resident.sessions.begin() as session:
            session.add(row)
        await tick(resident)
        await tick(resident)
        assert await read_state(resident, row) == ("active", None, "pending")
        assert (await resident.turn(resident.other)).status == RunStatus.success
    finally:
        finish.set()
        result = await reply
    assert result.status == RunStatus.success, result.error
    await tick(resident)
    await tick(resident)
    assert await read_state(resident, row) == ("fenced", "outcome-uncertain", "complete")


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", ["writer", "feature-workflow", "copy"])
async def test_live_writer_and_non_direct_operations_leave_the_workspace_running(resident, blocker):
    row = pending(resident, action="remove" if blocker == "writer" else blocker)
    async with resident.sessions.begin() as session:
        session.add(row)
        parent = await session.get(SpaceRow, row.space_id)
        backing = await session.get(SpaceBackingRow, parent.backing_handle)
        volume = resident.files.catalog.verified[backing.slot_id]
    descriptors = acquire({row.space_id: volume}) if blocker == "writer" else []
    try:
        await tick(resident)
        await tick(resident)
        assert await read_state(resident, row) == ("active", None, "pending")
    finally:
        close(descriptors)
    if blocker == "writer":
        await tick(resident)
        assert await read_state(resident, row) == ("active", None, "pending")
        await tick(resident)
        assert await read_state(resident, row) == ("fenced", "outcome-uncertain", "complete")


@pytest.mark.asyncio
async def test_unconfirmed_closure_logs_the_exact_operation_and_home(resident, monkeypatch, caplog):
    row = pending(resident)
    async with resident.sessions.begin() as session:
        session.add(row)
    await tick(resident)

    def unavailable(*args):
        raise OSError("Container stop unavailable")

    # Verification still succeeds. Only the physical stop fails, after
    # the transaction recording closure has committed.
    monkeypatch.setattr(resident.adapter, "fence", unavailable)
    with caplog.at_level(logging.INFO):
        await tick(resident)
    assert await read_state(resident, row) == ("fence_pending", "outcome-uncertain", "pending")
    assert any(row.operation_id in record.getMessage() and row.space_id in record.getMessage() for record in caplog.records)
    assert resident.claim.id in resident.host.app.state.agent_execution.pending_reconcile


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["generation", "incarnation", "operation", "replacement", "replacement-before-native-admission", "durable-run"])
async def test_changed_facts_at_containment_admission_preserve_the_current_workspace(resident, monkeypatch, changed):
    row, following = pending(resident), pending(resident)
    async with resident.sessions.begin() as session:
        session.add(row)
    await tick(resident)
    attachments = resident.host.attachments
    contain = attachments.contain_host
    checked, current = [], resident.claim

    async def race(identifier, reason, **kwargs):
        nonlocal current
        assert identifier == resident.claim.id and reason == "outcome-uncertain"
        checked.append(identifier)
        if changed.startswith("replacement"):
            recovery = SpaceRecovery(resident.files, attachments=attachments)
            await recovery.accept_current_state(actor=ALICE, space_id=row.space_id, expected_generation=row.generation, operation_id=row.operation_id, acknowledge_uncertain_outcome=True)
            current = await attachments.attach(actor=resident.original.instance.principal, incarnation=operation(), resources=(ResourceMount(row.space_id, row.generation, "home", True),))
            async with resident.sessions.begin() as session:
                session.add(following)
            if changed == "replacement-before-native-admission":

                def missing_original(*_args):
                    raise RuntimeError("Original resident graph already retired")

                monkeypatch.setattr(resident.adapter, "_containment_scope", missing_original, raising=False)
        else:
            async with resident.sessions.begin() as session:
                if changed == "generation":
                    (await session.get(SpaceRow, row.space_id)).generation += 1
                elif changed == "incarnation":
                    (await session.get(SpaceAttachmentRow, current.id)).incarnation = operation()
                elif changed == "operation":
                    (await session.get(SpaceFileOperationRow, (row.space_id, row.operation_id))).phase = "complete"
                    session.add(following)
                else:
                    session.add(RunRow(run_id=operation(), thread_id=resident.original.thread_id, status="running", owner_worker_id="another-worker", metadata_json={"agent_instance_id": resident.original.instance.id}))
        return await contain(identifier, reason, **kwargs)

    monkeypatch.setattr(attachments, "contain_host", race)
    await tick(resident)
    assert checked == [resident.claim.id]
    assert current.container_id in resident.adapter.containers
    assert current.id not in attachments.uncontained
    assert resident.claim.id not in attachments.uncontained
    async with resident.sessions() as session:
        actual = await session.get(SpaceAttachmentRow, current.id)
        assert (actual.phase, actual.fence_reason) == ("active", None)
        if changed == "operation" or changed.startswith("replacement"):
            assert (await session.get(SpaceFileOperationRow, (row.space_id, following.operation_id))).phase == "pending"
    assert not resident.host.app.state.agent_execution.pending_reconcile


@pytest.mark.asyncio
async def test_a_reserved_assignment_keeps_its_workspace_until_admission_finishes(resident):
    row = pending(resident)
    registry = resident.host.registry
    reservation = await registry.admit(resident.original.instance, work=True)
    try:
        async with resident.sessions.begin() as session:
            session.add(row)
        await tick(resident)
        await tick(resident)
        assert await read_state(resident, row) == ("active", None, "pending")
        assert registry.reservations[reservation.id] is reservation
    finally:
        registry.cancel(reservation)
    await tick(resident)
    assert await read_state(resident, row) == ("fenced", "outcome-uncertain", "complete")


@pytest.mark.asyncio
@pytest.mark.parametrize("activity", ["writer", "preparation", "assignment"])
async def test_closure_rechecks_activity_immediately_before_recording_its_intent(resident, execution, monkeypatch, activity):
    row = pending(resident)
    async with resident.sessions.begin() as session:
        session.add(row)
        parent = await session.get(SpaceRow, row.space_id)
        backing = await session.get(SpaceBackingRow, parent.backing_handle)
        volume = resident.files.catalog.verified[backing.slot_id]
    await tick(resident)
    attachments = resident.host.attachments
    contain = attachments.contain_host
    registry = resident.host.registry
    entry = registry.homes[row.space_id]
    descriptors, checked = [], []

    async def activity_after_confirmation(identifier, reason, **kwargs):
        assert identifier == resident.claim.id and reason == "outcome-uncertain"
        if activity == "writer":
            descriptors.extend(acquire({row.space_id: volume}))
        elif activity == "preparation":
            # Inject the exact local flag at the transaction boundary. Normal
            # preparers also serialize on this Home's in-process fence.
            entry.preparation = PreparationClaim(operation(), row.space_id, row.generation, resident.claim.host_id)
        else:
            work, admitted, assignment, _ = execution
            scope = await work.reserve(execution=admitted, work_id=assignment["id"], request=activation(admitted, assignment))
            assert scope.attempt_id
        checked.append(identifier)
        return await contain(identifier, reason, **kwargs)

    monkeypatch.setattr(attachments, "contain_host", activity_after_confirmation)
    try:
        await tick(resident)
        assert checked == [resident.claim.id]
        assert await read_state(resident, row) == ("active", None, "pending")
        assert not resident.host.app.state.agent_execution.pending_reconcile
    finally:
        close(descriptors)
        entry.preparation = None


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", ["malformed", "replaced"])
async def test_workspace_closure_requires_the_same_valid_directory_evidence_on_both_passes(resident, evidence):
    row = pending(resident, action="mkdir")
    row.result = {"inode": 123, "inode_generation": 456 if evidence == "replaced" else -1}
    async with resident.sessions.begin() as session:
        session.add(row)
    await tick(resident)
    if evidence == "replaced":
        async with resident.sessions.begin() as session:
            current = await session.get(SpaceFileOperationRow, (row.space_id, row.operation_id))
            current.result = {"inode": 456, "inode_generation": 789}
    await tick(resident)
    assert await read_state(resident, row) == ("active", None, "pending")
    await tick(resident)
    if evidence == "malformed":
        assert await read_state(resident, row) == ("active", None, "pending")
    else:
        # A fresh observation can close for this exact intent. Its directory
        # was never published, which the subsequent file inspection records.
        assert await read_state(resident, row) == ("fenced", "outcome-uncertain", "failed")


@pytest.mark.asyncio
async def test_cancelled_reconciliation_drains_the_queued_file_unlock(resident, monkeypatch):
    row = pending(resident)
    async with resident.sessions.begin() as session:
        session.add(row)
        parent = await session.get(SpaceRow, row.space_id)
        backing = await session.get(SpaceBackingRow, parent.backing_handle)
        volumes = {row.space_id: resident.files.catalog.verified[backing.slot_id]}
    await tick(resident)
    entered, release, released = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_io = resident_runtime.run_file_io
    held = []

    async def queued(func, *args, **kwargs):
        if func is close and args[0]:
            held.extend(args[0])
            entered.set()
            await release.wait()
            await original_io(func, *args, **kwargs)
            released.set()
            return
        return await original_io(func, *args, **kwargs)

    monkeypatch.setattr(resident_runtime, "run_file_io", queued)
    task = asyncio.create_task(resident.host.reconcile_rows(deadline=time.monotonic() + 30))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        with pytest.raises(SpaceOperationPending):
            acquire(volumes)
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert released.is_set()
        close(acquire(volumes))
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        if not released.is_set():
            close(held)

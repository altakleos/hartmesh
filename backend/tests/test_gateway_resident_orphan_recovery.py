"""Startup recovery uses real agent/Home SQL and original disk journal; Docker stays offline."""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_aio_sandbox_provider import _make_provider
from test_native_create_bridge import fixture as fixture
from test_native_resident_containment_scope import prepare
from test_native_resident_recovery_scope import eligibility
from test_storage_spaces_attachments import ContainedProvider

from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.utils.file_io import run_file_io


async def build(instances, fixture, tmp_path, *, missing, retain_previous=False):
    from app.gateway.resident_runtime import ResidentRuntime
    from app.gateway.storage_spaces import ExecutionStatus

    agents, files, sf, catalog = instances
    identity = await create(agents)
    adapter = ContainedProvider()
    attachments = SpaceAttachments(files, adapter)
    storage = files, sf, catalog, attachments, adapter
    factory, record, _captured, objects, calls = await prepare(storage, fixture, space_id=identity.home_id, actor=identity.principal)
    previous = await attachments.retained_claims() if retain_previous else None
    await eligibility(storage, record, missing=missing)
    provider = _make_provider(tmp_path)
    provider._backend, provider._native_creation = factory.provider._backend, factory.provider._native_creation
    factory.provider = provider
    admission = ProviderAdmission()
    provider._bind_runtime_admission(admission)
    manager, store = RunManager(), MemoryRunStore()
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, agent_instances=agents, run_manager=manager, run_store=store, startup_recovered_runs=[], agent_execution=ExecutionStatus()))
    registry = ResidentRegistry(attachments=attachments, provider=provider, run_manager=manager, settle=AsyncMock(return_value=[]), owner_of=AsyncMock(return_value=identity), replicas=3, idle_timeout=60)
    token = registry.begin_restore()
    registry.finish_restore(token, previous if previous is not None else await attachments.retained_claims())
    host = ResidentRuntime(app, registry=registry, admission=admission, entered=time.monotonic())
    app.state.resident_runtime = host
    return host, record, identity, objects, calls


def close(host):
    host.provider._acquire_serializer.close()
    host.provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_startup_original_recovery_removes_missing_or_fenced_rows_without_reopening_ownership(instances, fixture, tmp_path, missing):
    host, record, _identity, objects, calls = await build(instances, fixture, tmp_path, missing=missing)
    try:
        assert await host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert not objects and host.provider._native_creation.residents.records() == ()
        assert [command[1] for command in calls if command[1] in ("stop", "rm")] == ["stop", "rm"]
    finally:
        close(host)


@pytest.mark.asyncio
async def test_foreign_run_lease_blocks_original_recovery_and_retains_charge(instances, fixture, tmp_path):
    host, record, identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    try:
        await host.app.state.run_store.put("foreign-live", thread_id="thread", user_id="requester", status="running", metadata={"agent_instance_id": identity.id}, owner_worker_id="foreign-worker", lease_expires_at="2099-01-01T00:00:00Z")
        assert await host.recover_originals(deadline=time.monotonic() + 30) == (record.attachment_id,)
        assert objects and len(host.provider._native_creation.residents.records()) == 1
        assert not any(command[1] in ("stop", "rm") for command in calls)
    finally:
        close(host)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["readonly", "alias", "additional"])
async def test_missing_row_recovery_requires_exact_supported_agent_home(instances, fixture, tmp_path, change):
    host, original, _identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    journal, _handoffs, bridge, _owner = fixture
    try:
        retiring = await run_file_io(journal.retire_resident, original)
        await run_file_io(bridge.qualify)
        await run_file_io(journal.finish_resident, retiring)
        bridge.residents.remove(retiring)
        await run_file_io(bridge.qualify)
        home = original.roots[0]
        if change == "readonly":
            roots = (replace(home, writable=False),)
        elif change == "alias":
            roots = (replace(home, destination="/mnt/spaces/other"),)
        else:
            from _storage_spaces_test_support import home as create_home

            from deerflow.spaces.attachments import ResourceMount
            from deerflow.spaces.native_resident_evidence import NativeResidentRoot

            extra = await create_home(host.files, name="Another")
            async with host.files.registry.host_locked([extra.id]) as (session, parents):
                volume = await host.files._volume(session, parents[extra.id])
                root = await run_file_io(NativeResidentRoot.from_view, ResourceMount(extra.id, extra.generation, "additional", False), volume)
            roots = (home, root)
        changed = replace(original, id=uuid4().hex, roots=roots)
        await run_file_io(journal.begin_resident, changed)
        await run_file_io(bridge.qualify)
        assert await host.recover_originals(deadline=time.monotonic() + 30) == (original.attachment_id,)
        assert objects and bridge.residents.for_attachment(original.attachment_id) == changed
        assert not any(command[1] in ("stop", "rm") for command in calls)
    finally:
        close(host)


@pytest.mark.asyncio
async def test_foreign_run_appearing_during_final_absence_proof_keeps_original_charge(instances, fixture, tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox.resident_containment import ResidentContainmentWorker

    host, record, identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    entered, release = Event(), Event()
    confirm = ResidentContainmentWorker.confirm_absent

    def held(worker, attachment_id):
        confirm(worker, attachment_id)
        entered.set()
        assert release.wait(10)

    monkeypatch.setattr(ResidentContainmentWorker, "confirm_absent", held)
    task = asyncio.create_task(host.recover_originals(deadline=time.monotonic() + 30))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        await host.app.state.run_store.put(
            "late-foreign", thread_id="thread", user_id="requester", status="running", metadata={"agent_attachment_id": record.attachment_id}, owner_worker_id="foreign-worker", lease_expires_at="2099-01-01T00:00:00Z"
        )
        release.set()
        assert await task == (record.attachment_id,)
        assert not objects and host.provider._native_creation.residents.for_attachment(record.attachment_id).phase == "retiring"
        effects = [command for command in calls if command[1] in ("stop", "rm")]
        await host.app.state.run_store.update_status("late-foreign", "success")
        assert await host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert host.provider._native_creation.residents.records() == ()
        assert [command for command in calls if command[1] in ("stop", "rm")] == effects
    finally:
        release.set()
        await task
        close(host)


@pytest.mark.asyncio
async def test_missing_row_inserted_while_waiting_for_parent_lock_refuses_original_effects(instances, fixture, tmp_path, monkeypatch):
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow

    host, record, identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    registry = host.files.registry
    original = registry.host_locked
    entered, release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def held(parents):
        entered.set()
        await release.wait()
        async with original(parents) as context:
            yield context

    monkeypatch.setattr(registry, "host_locked", held)
    task = asyncio.create_task(host.recover_originals(deadline=time.monotonic() + 30))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with registry._sf() as session, session.begin():
            session.add(
                SpaceAttachmentRow(
                    id=record.attachment_id, incarnation=record.incarnation, actor_kind=identity.principal.kind, actor_id=identity.principal.subject_id, host_id=record.daemon_id, container_id=record.container_id, phase="active"
                )
            )
        release.set()
        assert await task == (record.attachment_id,)
        assert objects and host.provider._native_creation.residents.for_attachment(record.attachment_id) == record
        assert not any(command[1] in ("stop", "rm") for command in calls)
    finally:
        release.set()
        await task
        close(host)


@pytest.mark.asyncio
async def test_pending_original_recovery_cannot_publish_even_when_primary_is_absent(instances, fixture, tmp_path):
    from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

    host, record, identity, objects, _calls = await build(instances, fixture, tmp_path, missing=True)
    try:
        objects.clear()
        await host.app.state.run_store.put("foreign", thread_id="thread", user_id="requester", status="running", metadata={"agent_instance_id": identity.id})
        with pytest.raises(StorageRuntimeUnavailable, match="recovery remains pending"):
            await host.account_native(object())
        assert host.provider._native_creation.residents.for_attachment(record.attachment_id) == record
        assert not host.workspace_qualified
    finally:
        close(host)


@pytest.mark.asyncio
async def test_successful_original_recovery_precedes_fresh_real_native_accounting_proof(instances, fixture, tmp_path):
    from deerflow.agent_instances.bootstrap import NativeAccountingProof
    from deerflow.spaces.runtime_owner import PoolRuntimeOwner

    host, record, _identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    provider, adapter = host.provider, host.attachments.provider
    await run_file_io((fixture[0].pool / ".gateway-owner.lock").touch, mode=0o600)
    owner = PoolRuntimeOwner(fixture[0].pool)
    try:
        await run_file_io(owner.resolve_provider, lambda: provider)
        evidence = await run_file_io(owner.qualify_native_evidence, provider, daemon_id=record.daemon_id)
        provider._native_creation = None
        adapter._same_host = lambda: owner.assert_provider(provider)
        adapter._configured_mounts = ()
        await run_file_io(provider._install_native_creation, evidence, adapter)
        census = []

        def primary_inventory(**_kwargs):
            assert not objects and provider._native_creation.residents.records() == ()
            census.append(True)
            return ()

        provider._backend._bootstrap_primary_inventory = primary_inventory
        permit = await run_file_io(provider._begin_bootstrap_accounting, host.admission)
        try:
            proof = await host.account_native(permit)
            assert isinstance(proof, NativeAccountingProof) and census == [True]
            provider.external_changed(lambda: proof.require_current_locked(provider, permit))
            assert [command[1] for command in calls if command[1] in ("stop", "rm")] == ["stop", "rm"]
            assert any(command[1] == "ps" for command in calls)
        finally:
            provider._end_bootstrap_accounting(permit)
    finally:
        close(host)
        await run_file_io(owner.close)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 6])
async def test_recovery_rotates_bounded_originals_and_blocked_same_home_does_not_starve(instances, fixture, tmp_path, count):
    host, first, identity, objects, _calls = await build(instances, fixture, tmp_path, missing=True)
    factory = host.attachments.provider._containment_scope
    storage = host.files, instances[2], instances[3], host.attachments, host.attachments.provider
    records = [first]
    try:
        for _ in range(count - 1):
            _extra_factory, extra, _captured, extra_objects, _extra_calls = await prepare(storage, fixture, space_id=identity.home_id, actor=identity.principal)
            await eligibility(storage, extra, missing=True)
            objects.update(extra_objects)
            records.append(extra)
        host.attachments.provider._backend = host.provider._backend
        host.attachments.provider._containment_scope = factory
        blocked = min(records, key=lambda item: item.id)
        await host.app.state.run_store.put("foreign-blocked", thread_id="thread", user_id="requester", status="running", metadata={"agent_attachment_id": blocked.attachment_id})
        pending = await host.recover_originals(deadline=time.monotonic() + 30)
        assert blocked.attachment_id in pending
        remaining = host.provider._native_creation.residents.records()
        assert len(remaining) == count - min(4, count) + 1
        assert host.provider._native_creation.residents.for_attachment(blocked.attachment_id) == blocked
        await host.app.state.run_store.update_status("foreign-blocked", "success")
        assert await host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert not objects and host.provider._native_creation.residents.records() == ()
    finally:
        close(host)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["initial", "final"])
async def test_foreign_run_during_graph_qualification_prevents_all_effects(instances, fixture, tmp_path, monkeypatch, stage):
    from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
    from deerflow.community.aio_sandbox.resident_containment import ResidentContainmentWorker

    host, record, identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    entered, release = Event(), Event()
    qualify = NativeResourceSet.qualify_resident_record
    observed = []

    def held(reader, original):
        result = qualify(reader, original)
        if not observed:
            observed.append(True)
            entered.set()
            assert release.wait(10)
        return result

    if stage == "initial":
        monkeypatch.setattr(NativeResourceSet, "qualify_resident_record", held)
    else:
        bridge = host.provider._native_creation
        check = bridge._check_daemon
        command = ResidentContainmentWorker._command
        before_command = []

        def in_command(worker, reader, effect):
            before_command.append(True)
            return command(worker, reader, effect)

        def held_daemon():
            check()
            if before_command and not observed:
                observed.append(True)
                entered.set()
                assert release.wait(10)

        monkeypatch.setattr(ResidentContainmentWorker, "_command", in_command)
        monkeypatch.setattr(bridge, "_check_daemon", held_daemon)
    task = asyncio.create_task(host.recover_originals(deadline=time.monotonic() + 30))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        await host.app.state.run_store.put("late-foreign", thread_id="thread", user_id="requester", status="running", metadata={"agent_instance_id": identity.id}, owner_worker_id="foreign-worker")
        release.set()
        assert await task == (record.attachment_id,)
        assert objects and host.provider._native_creation.residents.for_attachment(record.attachment_id) is not None
        assert not any(command[1] in ("stop", "rm") for command in calls)
    finally:
        release.set()
        await task
        close(host)


@pytest.mark.asyncio
async def test_cancelled_eligibility_callback_keeps_home_and_provider_fences_until_drain(instances, fixture, tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy

    host, record, identity, _objects, _calls = await build(instances, fixture, tmp_path, missing=True)
    store = host.app.state.run_store
    snapshot = store._borrowed_inflight
    entered, release = asyncio.Event(), asyncio.Event()
    reads = []

    async def held(session):
        reads.append(True)
        if len(reads) == 3:
            entered.set()
            await release.wait()
        return await snapshot(session)

    monkeypatch.setattr(store, "_borrowed_inflight", held)
    task = asyncio.create_task(host.recover_originals(deadline=time.monotonic() + 30))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        await asyncio.sleep(0)
        entry = host.registry.entry(identity)
        assert not task.done() and entry.lock.locked() and entry.reconciling
        with pytest.raises(NativeRetirementBusy):
            host.provider._owner_admission_gate().reserve("other-owner", record.roots[0].slot_id)
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not host.registry.entry(identity).lock.locked()
        close(host)


@pytest.mark.asyncio
async def test_validation_only_commit_failure_after_terminal_completion_clears_exact_stale_claim(instances, fixture, tmp_path, monkeypatch):
    host, record, _identity, objects, _calls = await build(instances, fixture, tmp_path, missing=True, retain_previous=True)
    registry = host.files.registry
    original = registry.host_locked
    transactions = []

    @asynccontextmanager
    async def failed_release(parents):
        transactions.append(True)
        async with original(parents) as context:
            yield context
            if len(transactions) == 2:
                raise RuntimeError("Validation-only transaction release failed")

    monkeypatch.setattr(registry, "host_locked", failed_release)
    try:
        assert record.attachment_id in host.registry.retained
        assert await host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert not objects and host.provider._native_creation.residents.records() == ()
        assert record.attachment_id not in host.registry.retained
    finally:
        close(host)


@pytest.mark.asyncio
async def test_cancel_after_terminal_completion_drains_exact_claim_before_releasing_fences(instances, fixture, tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy

    host, record, identity, objects, _calls = await build(instances, fixture, tmp_path, missing=True, retain_previous=True)
    registry = host.files.registry
    original = registry.host_locked
    entered, release = asyncio.Event(), asyncio.Event()
    transactions = []

    @asynccontextmanager
    async def held_release(parents):
        transactions.append(True)
        async with original(parents) as context:
            yield context
            if len(transactions) == 2:
                entered.set()
                await release.wait()

    monkeypatch.setattr(registry, "host_locked", held_release)
    task = asyncio.create_task(host.recover_originals(deadline=time.monotonic() + 30))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert not objects and host.provider._native_creation.residents.records() == ()
        assert record.attachment_id in host.registry.retained
        task.cancel()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and host.registry.entry(identity).lock.locked()
        with pytest.raises(NativeRetirementBusy):
            host.provider._owner_admission_gate().reserve("other-owner", record.roots[0].slot_id)
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert record.attachment_id not in host.registry.retained
        assert not host.registry.entry(identity).lock.locked()
        close(host)


@pytest.mark.asyncio
async def test_committed_foreign_sql_run_during_qualification_refuses_every_effect(instances, fixture, tmp_path, monkeypatch):
    from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
    from deerflow.persistence.run.model import RunRow
    from deerflow.persistence.run.sql import RunRepository

    sf = instances[2]
    engine = sf.kw["bind"]
    if engine.dialect.name != "postgresql":
        pytest.skip("SQLite's writer already excludes concurrent foreign inserts")
    async with engine.begin() as connection:
        await connection.run_sync(RunRow.__table__.create, checkfirst=True)
    host, record, identity, objects, calls = await build(instances, fixture, tmp_path, missing=True)
    host.app.state.run_store = RunRepository(sf)
    entered, release = Event(), Event()
    qualify = NativeResourceSet.qualify_resident_record
    observed = []

    def held(reader, original):
        result = qualify(reader, original)
        if not observed:
            observed.append(True)
            entered.set()
            assert release.wait(10)
        return result

    monkeypatch.setattr(NativeResourceSet, "qualify_resident_record", held)
    task = asyncio.create_task(host.recover_originals(deadline=time.monotonic() + 30))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        async with sf() as session, session.begin():
            session.add(
                RunRow(
                    run_id="late-foreign",
                    thread_id="thread",
                    user_id="requester",
                    operation_kind="run",
                    status="running",
                    metadata_json={"agent_instance_id": identity.id},
                    kwargs_json={},
                    owner_worker_id="foreign-worker",
                    created_at=datetime.now(UTC) + timedelta(days=1),
                    lease_expires_at=datetime.now(UTC) + timedelta(days=2),
                )
            )
        release.set()
        assert await task == (record.attachment_id,)
        assert objects and host.provider._native_creation.residents.for_attachment(record.attachment_id) is not None
        assert not any(command[1] in ("stop", "rm") for command in calls)
    finally:
        release.set()
        await task
        close(host)

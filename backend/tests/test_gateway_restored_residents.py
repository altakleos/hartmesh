"""Restored Gateway accounting uses real SQL/journal and offline Docker only."""

import asyncio
import json
import os
import time
from contextlib import ExitStack, asynccontextmanager
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import delete
from test_agent_instances import instances as instances
from test_gateway_resident_orphan_recovery import build, close
from test_native_create_bridge import fixture as fixture

from deerflow.agent_instances.bootstrap import NativeAccountingProof
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.spaces.docker import DockerStorageAdapter
from deerflow.spaces.native_resident_evidence import NativeResidentRoot
from deerflow.spaces.runtime_owner import PoolRuntimeOwner, StorageRuntimeUnavailable
from deerflow.utils.file_io import run_file_io


class _MetadataLock:
    """Real non-reentrant provider exclusion, with bounded test failure."""

    def __init__(self):
        self._lock = Lock()

    def __getattr__(self, name):
        return getattr(self._lock, name)

    def __enter__(self):
        assert self._lock.acquire(timeout=2), "Provider metadata lock was acquired recursively"
        return self

    def __exit__(self, *_exc):
        self._lock.release()


@asynccontextmanager
async def restored(instances, fixture, tmp_path, *, phase="prepared", sql_phase="active", mapped=True, replaced=True, remounted=False, readonly=False, bad_revision=False, survivor=False, extra=False):
    from_view, from_readonly = NativeResidentRoot.from_view, NativeResidentRoot.from_readonly

    def historical(root):
        # Construct original evidence once, as observed on the previous host.
        # Current physical fixtures and published records are never rewritten.
        old_device = root.device + 17
        revision = root.revision
        if revision is not None:
            facts = (root.backing_handle, root.slot_id, root.filesystem_uuid, root.source, root.root_inode, root.control_inode, (old_device, root.source_inode))
            revision = sha256(json.dumps(facts, separators=(",", ":")).encode("utf-8")).hexdigest()
            if bad_revision:
                revision = "f" * 64
        return replace(root, device=old_device, revision=revision)

    with ExitStack() as stack:
        if remounted:
            stack.enter_context(patch.object(NativeResidentRoot, "from_view", side_effect=lambda *args: historical(from_view(*args))))
            stack.enter_context(patch.object(NativeResidentRoot, "from_readonly", side_effect=lambda *args: historical(from_readonly(*args))))
        host, original, identity, objects, calls = await build(instances, fixture, tmp_path, missing=False, readonly=readonly)
    owner = None
    try:
        extras = ()
        if extra:
            from test_agent_instances import create
            from test_native_resident_containment_scope import prepare

            adapter = host.attachments.provider
            containment, backend = adapter._containment_scope, adapter._backend
            other = await create(instances[0])
            storage = host.files, instances[2], instances[3], host.attachments, adapter
            _factory, saved, _captured, actual, _calls = await prepare(storage, fixture, space_id=other.home_id, actor=other.principal)
            objects.update(actual)
            adapter._containment_scope, adapter._backend = containment, backend
            extras = (saved,)
        journal = fixture[0]
        if phase == "retiring":
            original = await run_file_io(journal.retire_resident, original)
        current = {"ID": "replacement-daemon" if replaced else original.daemon_id, "OSType": "linux"}
        async with instances[2]() as session, session.begin():
            row = await session.get(SpaceAttachmentRow, original.attachment_id)
            if sql_phase == "missing":
                await session.execute(delete(SpaceMountRow).where(SpaceMountRow.attachment_id == original.attachment_id))
                await session.delete(row)
            else:
                row.phase = sql_phase
            if sql_phase == "active":
                row.fenced_by_kind = row.fenced_by_id = row.fence_reason = row.fenced_at = None
            elif sql_phase == "fence_pending":
                row.fenced_at = None
            if mapped and replaced:
                session.add(StorageHostReplacementRow(old_host_id=original.daemon_id, new_host_id=current["ID"], attachments_named=1 + len(extras)))
        original_objects = dict(objects)
        if not survivor:
            objects.clear()
        provider, adapter = host.provider, host.attachments.provider
        provider._lock = _MetadataLock()
        for volume in host.files.catalog.verified.values():
            volume.spec.mount_path = volume.data_path.parent
        await run_file_io((journal.pool / ".gateway-owner.lock").touch, mode=0o600)
        owner = PoolRuntimeOwner(journal.pool)
        await run_file_io(owner.resolve_provider, lambda: provider)
        evidence = await run_file_io(owner.qualify_native_evidence, provider, daemon_id=current["ID"])

        def daemon(command, **_kwargs):
            calls.append(command)
            assert command[1] == "info"
            return SimpleNamespace(returncode=0, stdout=json.dumps(current), stderr="")

        driver = await run_file_io(DockerStorageAdapter, prepare=lambda _plan: None, runner=daemon, host_verifier=lambda: owner.assert_provider(provider))
        adapter.host_id, adapter._same_host = current["ID"], driver._same_host
        adapter._configured_mounts = ()
        adapter.readonly_roots = AsyncMock(return_value=())
        provider._native_creation = None
        await run_file_io(provider._install_native_creation, evidence, adapter)
        provider._backend._bootstrap_primary_inventory = lambda **_kwargs: ()
        host.attachments.on_contained = host.registry.contained
        token = host.registry.begin_restore()
        host.registry.finish_restore(token, await host.attachments.retained_claims())
        yield SimpleNamespace(host=host, original=original, extras=extras, identity=identity, owner=owner, objects=objects, original_objects=original_objects, calls=calls, daemon=current)
    finally:
        close(host)
        if owner is not None:
            await run_file_io(owner.close)


@pytest.mark.asyncio
async def test_exact_index_completion_uses_the_already_held_provider_lock(instances, fixture, tmp_path):
    async with restored(instances, fixture, tmp_path) as case:
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert case.host.provider._native_creation.residents.records() == ()
        assert case.original.attachment_id not in case.host.registry.retained


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepared", "retiring"])
@pytest.mark.parametrize("replaced", [False, True])
async def test_changed_device_completes_only_absent_original_with_shared_projection(instances, fixture, tmp_path, phase, replaced):
    async with restored(instances, fixture, tmp_path, phase=phase, replaced=replaced, remounted=True, readonly=True) as case:
        original = case.original.document()
        assert len(case.original.roots) == 2
        assert all(case.host.files.catalog.verify(root.slot_id).device != root.device for root in case.original.roots)
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == ()
        bridge = case.host.provider._native_creation
        assert bridge.residents.records() == bridge.evidence.snapshot().residents == ()
        assert case.original.document() == original
        assert case.original.attachment_id not in case.host.registry.retained
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepared", "retiring"])
@pytest.mark.parametrize("sql_phase", ["active", "fence_pending", "fenced", "missing"])
async def test_gateway_completes_acknowledged_absent_foreign_graph_before_census(instances, fixture, tmp_path, phase, sql_phase):
    async with restored(instances, fixture, tmp_path, phase=phase, sql_phase=sql_phase) as case:
        host, provider = case.host, case.host.provider
        permit = await run_file_io(provider._begin_bootstrap_accounting, host.admission)
        try:
            proof = await host.account_native(permit)
            assert isinstance(proof, NativeAccountingProof)
            provider.external_changed(lambda: proof.require_current_locked(provider, permit))
            assert provider._native_creation.evidence.snapshot().residents == provider._native_creation.residents.records() == ()
            assert case.original.attachment_id not in host.registry.retained
            async with instances[2]() as session:
                row = await session.get(SpaceAttachmentRow, case.original.attachment_id)
                if sql_phase == "missing":
                    assert row is None
                elif sql_phase == "active":
                    assert row.phase == "fenced"
                    assert (row.fence_reason, row.fenced_by_id) == ("host-replaced", case.daemon["ID"])
                else:
                    assert row.phase == "fenced"
                    assert (row.fence_reason, row.fenced_by_id) == ("operator", case.original.daemon_id)
            assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)
        finally:
            provider._end_bootstrap_accounting(permit)


@pytest.mark.asyncio
@pytest.mark.parametrize("expire_classification", [False, True], ids=["healthy", "classification-deadline"])
async def test_more_than_four_healthy_originals_do_not_enter_the_recovery_budget(instances, fixture, tmp_path, monkeypatch, expire_classification):
    from _storage_spaces_test_support import FixtureCatalog
    from test_agent_instances import create
    from test_native_resident_containment_scope import prepare

    host, first, _identity, objects, calls = await build(instances, fixture, tmp_path, missing=False)
    factory, backend = host.attachments.provider._containment_scope, host.provider._backend
    try:
        extra_path = tmp_path / "extra-slots"
        extra_path.mkdir()
        extra = FixtureCatalog(extra_path)
        host.files.catalog.volumes.update(extra.volumes)
        host.files.catalog.verified.update(extra.verified)
        async with instances[2]() as session, session.begin():
            row = await session.get(SpaceAttachmentRow, first.attachment_id)
            row.phase = "active"
            row.fenced_by_kind = row.fenced_by_id = row.fence_reason = row.fenced_at = None
        storage = host.files, instances[2], instances[3], host.attachments, host.attachments.provider
        for _ in range(4):
            identity = await create(instances[0])
            _factory, _record, _captured, actual, _calls = await prepare(storage, fixture, space_id=identity.home_id, actor=identity.principal)
            objects.update(actual)
        host.attachments.provider._backend = backend
        host.attachments.provider._containment_scope = factory
        token = host.registry.begin_restore()
        host.registry.finish_restore(token, await host.attachments.retained_claims())
        originals = host.provider._native_creation.residents.records()
        assert len(originals) == 5
        if expire_classification:
            import app.gateway.resident_runtime as runtime

            observed, verify = [], host.files.catalog.verify
            now = time.monotonic()

            def one_verify(*args, **kwargs):
                observed.append(args)
                return verify(*args, **kwargs)

            monkeypatch.setattr(host.files.catalog, "verify", one_verify)
            monkeypatch.setattr(runtime, "time", SimpleNamespace(monotonic=lambda: now + (60 if observed else 0)))
            pending = await host.recover_originals(deadline=now + 30)
            assert len(observed) == 1, "Classification started new physical proofs after its deadline"
            assert set(pending) == {record.attachment_id for record in originals}
        else:
            for _ in range(2):
                assert await host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert host.provider._native_creation.residents.records() == originals
        assert len(host.registry.retained) == 5
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in calls)
    finally:
        close(host)


@pytest.mark.asyncio
async def test_same_host_active_absence_does_not_fence_a_live_foreign_run(instances, fixture, tmp_path):
    async with restored(instances, fixture, tmp_path, replaced=False) as case:
        await case.host.app.state.run_store.put(
            "foreign-live", thread_id="thread", user_id="requester", status="running", metadata={"agent_instance_id": case.identity.id}, owner_worker_id="foreign-worker", lease_expires_at="2099-01-01T00:00:00Z"
        )
        await case.host.recover_originals(deadline=time.monotonic() + 30)
        async with instances[2]() as session:
            assert (await session.get(SpaceAttachmentRow, case.original.attachment_id)).phase == "active"
        assert case.host.provider._native_creation.residents.records() == (case.original,)
        assert case.original.attachment_id in case.host.registry.retained


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["deadline", "closing", "post-index-qualification", "terminal-scan"])
async def test_terminal_receipt_finishes_exact_accounting_after_durable_journal_removal(instances, fixture, tmp_path, monkeypatch, interruption):
    async with restored(instances, fixture, tmp_path) as case:
        bridge = case.host.provider._native_creation
        journal = bridge.evidence
        finish, qualify, scan = journal._finish_restored_resident, bridge.qualify, journal._scan
        interrupted = []

        def after_finish(record, permit):
            result = finish(record, permit)
            if interruption == "deadline":
                permit.worker.lifetime.deadline = time.monotonic() - 1
                interrupted.append(True)
            elif interruption == "closing":
                case.owner.mark_closing()
                interrupted.append(True)
            return result

        def after_index(**kwargs):
            if interruption == "post-index-qualification" and not interrupted and not bridge.residents.records() and not journal.snapshot().residents:
                interrupted.append(True)
                raise OSError("Qualification reply was lost after exact terminal completion")
            return qualify(**kwargs)

        def terminal_scan():
            result = scan()
            if interruption == "terminal-scan" and not interrupted and case.original.id not in journal._teardown_removals:
                # Only inject after the terminal original and retry ghost left.
                if not any(record.id == case.original.id for record, _metadata in result[0]):
                    interrupted.append(True)
                    raise OSError("Post-terminal journal census failed")
            return result

        monkeypatch.setattr(journal, "_finish_restored_resident", after_finish)
        monkeypatch.setattr(bridge, "qualify", after_index)
        monkeypatch.setattr(journal, "_scan", terminal_scan)
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert interrupted
        assert journal.snapshot().residents == bridge.residents.records() == ()
        assert case.original.attachment_id not in case.host.registry.retained
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["uuid", "marker", "root-inode", "control-inode", "shared-inode", "shared-revision", "reused-name", "surviving-id"])
async def test_changed_restored_original_retains_journal_and_charge(instances, fixture, tmp_path, change):
    from uuid import uuid4

    from deerflow.spaces.incarnations import BINDING_MARKER

    async with restored(instances, fixture, tmp_path, remounted=True, readonly=True, bad_revision=change == "shared-revision", survivor=change == "surviving-id") as case:
        root = case.original.roots[0]
        volume = case.host.files.catalog.verify(root.slot_id)
        if change == "uuid":
            volume.spec.filesystem_uuid = str(uuid4())
        elif change == "marker":
            await run_file_io((volume.control_path / BINDING_MARKER).write_text, "f" * 32, encoding="utf-8")
        elif change in {"root-inode", "control-inode", "shared-inode"}:
            selected = volume.data_path if change == "root-inode" else volume.control_path if change == "control-inode" else Path(case.original.roots[1].source)
            await run_file_io(selected.rename, selected.with_name(selected.name + "-previous"))
            await run_file_io(selected.mkdir, mode=0o700)
        elif change == "reused-name":
            case.objects["f" * 64] = {"Id": "f" * 64, "Name": "/" + case.original.name}
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == (case.original.attachment_id,)
        bridge = case.host.provider._native_creation
        assert bridge.residents.records() == bridge.evidence.snapshot().residents == (case.original,)
        assert case.original.attachment_id in case.host.registry.retained
        async with instances[2]() as session:
            assert (await session.get(SpaceAttachmentRow, case.original.attachment_id)).phase == "active"
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
async def test_same_daemon_remount_retains_surviving_original_without_effects(instances, fixture, tmp_path):
    async with restored(instances, fixture, tmp_path, replaced=False, remounted=True, survivor=True) as case:
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == (case.original.attachment_id,)
        assert case.host.provider._native_creation.residents.records() == (case.original,)
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["sql-commit", "retire-fsync", "unlink-fsync"])
async def test_uncertain_restored_completion_retains_exact_charge_and_retries(instances, fixture, tmp_path, monkeypatch, fault):
    import deerflow.spaces.native_create_evidence as native

    async with restored(instances, fixture, tmp_path, phase="retiring" if fault == "unlink-fsync" else "prepared") as case:
        bridge, failed = case.host.provider._native_creation, []
        if fault == "sql-commit":
            locked = case.host.files.registry.host_locked

            @asynccontextmanager
            async def uncertain(parents):
                async with locked(parents) as context:
                    yield context
                    if not failed:
                        failed.append(True)
                        raise OSError("SQL commit acknowledgement was lost")

            monkeypatch.setattr(case.host.files.registry, "host_locked", uncertain)
        else:
            proxy = SimpleNamespace(**{name: getattr(os, name) for name in dir(os)})

            def uncertain(fd):
                if fd == bridge.evidence._directory_fd and not failed:
                    failed.append(True)
                    raise OSError("Directory fsync was not confirmed")
                return os.fsync(fd)

            proxy.fsync = uncertain
            monkeypatch.setattr(native, "os", proxy)
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == (case.original.attachment_id,)
        assert failed and len(bridge.residents.records()) == len(bridge.evidence.snapshot().residents) == 1
        assert case.original.attachment_id in case.host.registry.retained
        assert await case.host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert bridge.residents.records() == bridge.evidence.snapshot().residents == ()
        assert case.original.attachment_id not in case.host.registry.retained
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepared", "retiring"])
@pytest.mark.parametrize("mapped", [True, False])
async def test_real_bootstrap_opens_only_after_acknowledged_restoration_and_fresh_census(instances, fixture, tmp_path, monkeypatch, phase, mapped):
    from app.gateway import storage_spaces
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
    from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError

    async with instances[2].kw["bind"].begin() as connection:
        for row in (AgentWorkRow, AgentWorkAttemptRow, AgentWorkEventRow):
            await connection.run_sync(row.__table__.create, checkfirst=True)
    monkeypatch.setattr(storage_spaces, "AGENTS_HOLD_PATH", tmp_path / "no-hold")
    async with restored(instances, fixture, tmp_path, phase=phase, mapped=mapped) as case:
        host = case.host
        host.app.state.storage_spaces_enabled = True
        result = await host.coordinator.poll(timeout=30)
        if mapped:
            assert result is not None and result.workspace_qualified, repr(host.coordinator.last_error)
            assert host.workspace_qualified
            host.provider.external_changed(lambda: host.admission.require(host.provider))
            assert host.provider._native_creation.residents.records() == ()
        else:
            assert result is None and host.coordinator.last_error is not None
            assert not host.workspace_qualified
            with pytest.raises(SandboxRuntimeAdmissionError):
                host.provider.external_changed(lambda: host.admission.require(host.provider))
            assert host.provider._native_creation.residents.records() == (case.original,)
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["mapping", "live-run", "device", "shared-inode", "cancel"])
async def test_restored_completion_rechecks_changes_and_drains_cancellation(instances, fixture, tmp_path, monkeypatch, change):
    from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
    from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy

    async with restored(instances, fixture, tmp_path, readonly=True) as case:
        entered, release = Event(), Event()
        prove, observations = NativeResourceSet.prove_resident_absent, []

        def held(reader, record):
            observations.append(True)
            if len(observations) == (1 if change == "mapping" else 2):
                entered.set()
                assert release.wait(15)
            return prove(reader, record)

        monkeypatch.setattr(NativeResourceSet, "prove_resident_absent", held)
        task = asyncio.create_task(case.host.recover_originals(deadline=time.monotonic() + 30))
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            if change == "mapping":
                async with instances[2]() as session, session.begin():
                    row = await session.get(StorageHostReplacementRow, (case.original.daemon_id, case.daemon["ID"]))
                    row.attachments_named += 1
            elif change == "live-run":
                await case.host.app.state.run_store.put("late-run", thread_id="thread", user_id="requester", status="running", metadata={"agent_instance_id": case.identity.id}, owner_worker_id="foreign")
            elif change == "device":
                volume = case.host.files.catalog.verify(case.original.roots[0].slot_id)
                volume.device += 1
            elif change == "shared-inode":
                source = Path(case.original.roots[1].source)
                await run_file_io(source.rename, source.with_name("previous-shared"))
                await run_file_io(source.mkdir)
            else:
                task.cancel()
                task.cancel()
                await asyncio.sleep(0)
                entry = case.host.registry.entry(case.identity)
                assert not task.done() and entry.lock.locked() and entry.reconciling
                with pytest.raises(NativeRetirementBusy):
                    case.host.provider._owner_admission_gate().reserve("other", case.original.roots[0].slot_id)
        finally:
            release.set()
            if change == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                assert await task == (case.original.attachment_id,)
        bridge = case.host.provider._native_creation
        if change == "cancel":
            assert bridge.residents.records() == bridge.evidence.snapshot().residents == ()
            assert case.original.attachment_id not in case.host.registry.retained
        else:
            assert bridge.residents.records() == bridge.evidence.snapshot().residents == (case.original,)
            assert case.original.attachment_id in case.host.registry.retained
        assert not case.host.registry.entry(case.identity).lock.locked()
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
async def test_two_originals_complete_independently_without_losing_uncertain_charge(instances, fixture, tmp_path):
    async with restored(instances, fixture, tmp_path, extra=True) as case:
        blocked = min((case.original, *case.extras), key=lambda record: record.id)
        host = case.host
        await host.app.state.run_store.put("held-run", thread_id="thread", user_id="requester", status="running", metadata={"agent_attachment_id": blocked.attachment_id}, owner_worker_id="foreign")
        assert await host.recover_originals(deadline=time.monotonic() + 30) == (blocked.attachment_id,)
        bridge = host.provider._native_creation
        assert bridge.residents.records() == bridge.evidence.snapshot().residents == (blocked,)
        assert set(host.registry.retained) == {blocked.attachment_id}
        assert host.registry.external_slots() == 1
        with host.provider._lock:
            assert bridge.residents.extra_slots_locked(known_claims=host.registry.external_native_claims()) == 0
        await host.app.state.run_store.update_status("held-run", "success")
        assert await host.recover_originals(deadline=time.monotonic() + 30) == ()
        assert bridge.residents.records() == bridge.evidence.snapshot().residents == ()
        assert not host.registry.retained and host.registry.external_slots() == 0
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
async def test_committed_foreign_sql_run_is_rechecked_before_restored_completion(instances, fixture, tmp_path, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
    from deerflow.persistence.run.model import RunRow
    from deerflow.persistence.run.sql import RunRepository

    engine = instances[2].kw["bind"]
    if engine.dialect.name != "postgresql":
        pytest.skip("SQLite already excludes concurrent foreign writes under parent exclusion")
    async with engine.begin() as connection:
        await connection.run_sync(RunRow.__table__.create, checkfirst=True)
    async with restored(instances, fixture, tmp_path) as case:
        case.host.app.state.run_store = RunRepository(instances[2])
        entered, release = Event(), Event()
        prove, observed = NativeResourceSet.prove_resident_absent, []

        def held(reader, record):
            observed.append(True)
            if len(observed) == 2:
                entered.set()
                assert release.wait(15)
            return prove(reader, record)

        monkeypatch.setattr(NativeResourceSet, "prove_resident_absent", held)
        task = asyncio.create_task(case.host.recover_originals(deadline=time.monotonic() + 30))
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            async with instances[2]() as session, session.begin():
                session.add(
                    RunRow(
                        run_id="late-foreign",
                        thread_id="thread",
                        user_id="requester",
                        operation_kind="run",
                        status="running",
                        metadata_json={"agent_instance_id": case.identity.id},
                        kwargs_json={},
                        owner_worker_id="foreign-worker",
                        created_at=datetime.now(UTC),
                        lease_expires_at=datetime.now(UTC) + timedelta(days=1),
                    )
                )
        finally:
            release.set()
            assert await task == (case.original.attachment_id,)
        bridge = case.host.provider._native_creation
        assert bridge.residents.records() == bridge.evidence.snapshot().residents == (case.original,)
        assert case.original.attachment_id in case.host.registry.retained
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)


@pytest.mark.asyncio
async def test_parent_lock_wait_is_inside_the_owned_startup_deadline(instances, fixture, tmp_path, monkeypatch):
    async with restored(instances, fixture, tmp_path) as case:
        registry = case.host.files.registry
        locked = registry.host_locked
        entered, release = asyncio.Event(), asyncio.Event()

        @asynccontextmanager
        async def held(parents):
            entered.set()
            await release.wait()
            async with locked(parents) as context:
                yield context

        monkeypatch.setattr(registry, "host_locked", held)
        task = asyncio.create_task(case.host.recover_originals(deadline=time.monotonic() + 2))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            await asyncio.wait({task}, timeout=2.5)
            assert task.done(), "Owned SQL admission exceeded the inherited deadline"
            assert await task == (case.original.attachment_id,)
            assert case.host.provider._native_creation.residents.records() == (case.original,)
        finally:
            release.set()
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepared", "retiring"])
async def test_gateway_keeps_unacknowledged_foreign_original_and_closed_admission(instances, fixture, tmp_path, phase):
    async with restored(instances, fixture, tmp_path, phase=phase, mapped=False) as case:
        host, provider = case.host, case.host.provider
        permit = await run_file_io(provider._begin_bootstrap_accounting, host.admission)
        try:
            with pytest.raises(StorageRuntimeUnavailable):
                await host.account_native(permit)
            assert provider._native_creation.residents.records() == (case.original,)
            async with instances[2]() as session:
                assert (await session.get(SpaceAttachmentRow, case.original.attachment_id)).phase == "active"
            assert case.original.attachment_id in host.registry.retained
            assert not host.workspace_qualified
            assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in case.calls)
        finally:
            provider._end_bootstrap_accounting(permit)

"""Real SQL, bound disk roots and journal; Docker effects use an offline double."""

import json
import os
from contextlib import asynccontextmanager
from threading import Event, Lock
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from test_native_create_bridge import fixture as fixture
from test_native_resident_evidence import record
from test_storage_spaces_attachments import storage as storage

from deerflow.community.aio_sandbox.owner_admission import OwnerAdmissions
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import AttachmentPending, ResourceMount
from deerflow.spaces.contract import Permission
from deerflow.spaces.docker import DockerStorageAdapter
from deerflow.spaces.native_resident_evidence import NativeResidentRoot
from deerflow.spaces.readonly_roots import ReadOnlyRoot
from deerflow.spaces.resident_containment import NativeResidentContainmentScopes
from deerflow.utils.file_io import run_file_io


async def prepare(storage, fixture, *, readonly=False, space_id=None, actor=ALICE):
    files, sf, _catalog, attachments, adapter = storage
    journal, _handoffs, bridge, _owner = fixture
    adapter.host_id = journal.daemon_id
    space = await home(files) if space_id is None else await files.registry.get(actor=actor, space_id=space_id)
    resource = ResourceMount(space.id, space.generation, "home", True)
    attached = await attachments.attach(actor=actor, incarnation=operation(), resources=[resource])
    async with files.registry.host_locked([space.id]) as (session, parents):
        volume = await files._volume(session, parents[space.id])
        root = await run_file_io(NativeResidentRoot.from_view, resource, volume)
    roots = (root,)
    if readonly:
        shared = await home(files, name="Shared")
        async with files.registry.host_locked([shared.id]) as (session, parents):
            volume = await files._volume(session, parents[shared.id])
            await run_file_io((volume.data_path / "shared").mkdir)
            projection = await run_file_io(ReadOnlyRoot.capture, volume, "shared", "/mnt/user-data/shared")
            roots += (await run_file_io(NativeResidentRoot.from_readonly, projection),)
    saved = record(
        id=operation(),
        attachment_id=attached.id,
        incarnation=attached.incarnation,
        container_id=attached.container_id,
        name="hartmesh-space-" + attached.id,
        roots=roots,
        network_mode="open",
        network_target="none",
        policy_digest=None,
        resources=(),
        configured_mounts=(),
    )
    await run_file_io(journal.begin_resident, saved)
    await run_file_io(bridge.qualify)
    entry = {
        "Id": saved.container_id,
        "Name": "/" + saved.name,
        "Created": saved.created_at,
        "Config": {"Labels": {"deerflow.role": "resident", "hartmesh.storage.attachment": attached.id, "hartmesh.storage.host": saved.daemon_id, "hartmesh.storage.space." + root.slot_id: "1"}},
        "HostConfig": {"NetworkMode": "none", "PortBindings": {}},
        "NetworkSettings": {"Ports": {}, "Networks": {}},
        "State": {"Running": True, "Status": "running"},
        "Mounts": [{"Type": "bind", "Source": source, "Destination": destination, "RW": writable, "Propagation": "rprivate"} for source, destination, writable in saved.mounts],
    }
    from hashlib import sha256

    for view in roots:
        entry["Config"]["Labels"]["hartmesh.storage.space." + view.slot_id] = "1"
        if view.revision is not None:
            entry["Config"]["Labels"]["hartmesh.storage.readonly." + sha256(view.destination.encode()).hexdigest()[:16]] = view.revision
    objects, calls = {saved.container_id: entry}, []

    def engine(command, **kwargs):
        calls.append(command)
        if command[1] == "inspect":
            actual = next((value for value in objects.values() if command[-1] in (value["Id"], value["Name"].removeprefix("/"))), None)
            return SimpleNamespace(returncode=0 if actual else 1, stdout=json.dumps([actual] if actual else []), stderr="" if actual else "Error: No such object: " + command[-1])
        if command[1] == "ps" or command[1:3] == ["network", "ls"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[1] == "stop":
            objects[command[-1]]["State"] = {"Running": False, "Status": "exited"}
        elif command[1] == "rm":
            objects.pop(command[-1])
        else:
            pytest.fail("Unexpected offline Docker effect")
        return SimpleNamespace(returncode=0, stdout=command[-1], stderr="")

    gate = OwnerAdmissions(lock=Lock())
    provider = SimpleNamespace(_native_creation=bridge, _backend=SimpleNamespace(_run=engine), _owner_admission_gate=lambda: gate)
    adapter._backend = provider._backend
    adapter._same_host = lambda: bridge.assert_current(teardown=True)
    adapter.bootstrap_residents = lambda **kwargs: DockerStorageAdapter.bootstrap_residents(adapter, **kwargs)
    factory = NativeResidentContainmentScopes(provider=provider, files=files, adapter=adapter)
    adapter._containment_scope = factory
    captured = {attached.id: {(space.id, space.generation, "home", True)}}
    return factory, saved, captured, objects, calls


@pytest.mark.asyncio
async def test_real_bound_scope_fences_then_finalizes_and_releases_batch(storage, fixture):
    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    assert not objects and factory.provider._native_creation.residents.records() == ()
    assert [command[1] for command in calls if command[1] in ("stop", "rm")] == ["stop", "rm"]
    async with storage[1]() as session:
        assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "fenced"
    gate = factory.provider._owner_admission_gate()
    reservation = gate.reserve("another-owner", saved.roots[0].slot_id)
    gate.end_retirement(reservation)


@pytest.mark.asyncio
async def test_lowercase_absence_resumes_through_sql_fence_and_journal_before_replacement(storage, fixture, monkeypatch):
    from deerflow.agent_instances.resident_preparation import _resume

    factory, saved, _captured, objects, _calls = await prepare(storage, fixture, readonly=True)
    mounts, adapter = storage[3], storage[4]
    original = factory.provider._backend._run

    def liveness(command, **kwargs):
        if command[1] == "info":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ID": saved.daemon_id, "OSType": "linux"}), stderr="")
        reply = original(command, **kwargs)
        if command[1] == "inspect" and reply.returncode == 1:
            return SimpleNamespace(returncode=1, stdout=reply.stdout, stderr="error: no such object: " + command[-1])
        return reply

    driver = DockerStorageAdapter(prepare=lambda _plan: None, runner=liveness, host_verifier=lambda: factory.provider._native_creation.assert_current(teardown=True))
    monkeypatch.setattr(adapter, "container_state", driver.container_state, raising=False)
    objects.pop(saved.container_id)
    adapter.containers.pop(saved.container_id)
    root = saved.roots[0]
    instance = SimpleNamespace(id=operation(), home_id=root.space_id, principal=ALICE, name="Qualification AI employee")
    execution = SimpleNamespace(instance=instance, home_generation=root.generation)
    residents = SimpleNamespace(_restoration_started=False, claimed=Mock())
    resources = [ResourceMount(root.space_id, root.generation, "home", True)]
    assert await _resume(execution, residents, mounts, resources) is None
    residents.claimed.assert_called_once_with(instance, saved.attachment_id, attempt_id=None)
    async with storage[1]() as session:
        assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "fenced"
    assert factory.provider._native_creation.residents.records() == ()
    assert not mounts.uncontained
    replacement = await mounts.attach(actor=ALICE, incarnation=operation(), resources=resources)
    assert replacement.id != saved.attachment_id and replacement.container_id != saved.container_id


@pytest.mark.asyncio
async def test_already_finalized_fenced_retry_observes_absence_without_effects(storage, fixture):
    _factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    calls.clear()
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    assert not objects and calls and all(command[1] in ("inspect", "ps", "network") for command in calls)


@pytest.mark.asyncio
async def test_finalized_retry_refuses_a_new_container_at_the_original_name(storage, fixture):
    _factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    objects["f" * 64] = {"Id": "f" * 64, "Name": "/" + saved.name}
    calls.clear()
    assert await storage[3].contain_host(saved.attachment_id, "operator") is False
    assert objects and all(command[1] == "inspect" for command in calls)


@pytest.mark.asyncio
async def test_changed_backing_under_parent_fence_refuses_every_effect(storage, fixture):
    factory, saved, captured, objects, calls = await prepare(storage, fixture)
    async with factory(storage[3], captured) as scope:
        async with storage[0].registry.host_locked(scope.parent_ids) as (session, _parents):
            backing = await session.get(SpaceBackingRow, saved.roots[0].backing_handle)
            backing.root_inode += 1
            with pytest.raises(AttachmentPending, match="backing changed"):
                await scope.admit(session, {saved.attachment_id: await session.get(SpaceAttachmentRow, saved.attachment_id)})
    assert objects and not calls


@pytest.mark.asyncio
async def test_terminal_sync_retry_uses_fresh_factory_without_repeating_effects(storage, fixture, monkeypatch):
    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    journal = factory.provider._native_creation.evidence
    fsync = os.fsync

    def interrupted(fd):
        if fd == journal._directory_fd and not objects:
            raise OSError("Terminal sync outcome was lost")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", interrupted)
    assert await storage[3].contain_host(saved.attachment_id, "operator") is False
    assert len(journal.snapshot().residents) == 1 and not objects
    monkeypatch.setattr(os, "fsync", fsync)
    storage[4]._containment_scope = NativeResidentContainmentScopes(provider=factory.provider, files=storage[0], adapter=storage[4])
    effects = [command for command in calls if command[1] in ("stop", "rm")]
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    assert journal.snapshot().residents == ()
    assert [command for command in calls if command[1] in ("stop", "rm")] == effects


@pytest.mark.asyncio
async def test_scope_expands_sorted_parents_to_original_readonly_backings(storage, fixture):
    factory, saved, captured, _objects, _calls = await prepare(storage, fixture, readonly=True)
    async with factory(storage[3], captured) as scope:
        assert len(scope.parent_ids) == 2 and list(scope.parent_ids) == sorted(scope.parent_ids)
        async with storage[0].registry.host_locked(scope.parent_ids) as (session, parents):
            await scope.admit(session, {saved.attachment_id: await session.get(SpaceAttachmentRow, saved.attachment_id)})
            assert set(scope.parent_ids) == set(parents)
    assert await storage[3].contain_host(saved.attachment_id, "operator")


@pytest.mark.asyncio
async def test_lost_fenced_commit_retains_original_graph_for_fresh_sql_retry(storage, fixture, monkeypatch):
    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    registry = storage[0].registry
    original = registry.host_locked

    @asynccontextmanager
    async def lost_commit(parents):
        async with original(parents) as context:
            yield context
            row = await context[0].get(SpaceAttachmentRow, saved.attachment_id)
            if row.phase == "fenced":
                raise RuntimeError("Fenced commit outcome lost")

    monkeypatch.setattr(registry, "host_locked", lost_commit)
    assert await storage[3].contain_host(saved.attachment_id, "operator") is False
    assert not objects and factory.provider._native_creation.residents.for_attachment(saved.attachment_id).phase == "retiring"
    async with storage[1]() as session:
        assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "fence_pending"
    monkeypatch.setattr(registry, "host_locked", original)
    effects = [command for command in calls if command[1] in ("stop", "rm")]
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    assert [command for command in calls if command[1] in ("stop", "rm")] == effects


@pytest.mark.asyncio
async def test_cancelled_caller_drains_real_worker_before_releasing_lifecycle(storage, fixture):
    import asyncio

    from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy

    factory, saved, _captured, _objects, _calls = await prepare(storage, fixture)
    entered, release = Event(), Event()
    backend = factory.provider._backend
    original = backend._run

    def held(command, **kwargs):
        if command[1] == "stop":
            entered.set()
            assert release.wait(10)
        return original(command, **kwargs)

    backend._run = held
    task = asyncio.create_task(storage[3].contain_host(saved.attachment_id, "operator"))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(NativeRetirementBusy):
            factory.provider._owner_admission_gate().reserve("another-owner", saved.roots[0].slot_id)
        fixture[3].live = False
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert factory.provider._native_creation.residents.records() == ()
    gate = factory.provider._owner_admission_gate()
    token = gate.reserve("another-owner", saved.roots[0].slot_id)
    gate.end_retirement(token)


@pytest.mark.asyncio
async def test_partial_batch_finalization_retries_finished_and_retained_members(storage, fixture, monkeypatch):
    first_factory, first, _captured, first_objects, _calls = await prepare(storage, fixture)
    factory, second, _captured, second_objects, _calls = await prepare(storage, fixture, space_id=first.roots[0].space_id)
    first_objects.update(second_objects)
    factory.provider._backend._run = first_factory.provider._backend._run
    journal = factory.provider._native_creation.evidence
    original = journal.finish_resident
    last = max(first.attachment_id, second.attachment_id)

    def interrupted(record):
        if record.attachment_id == last:
            raise OSError("Second terminal sync failed")
        original(record)

    monkeypatch.setattr(journal, "finish_resident", interrupted)
    arguments = dict(actor=ALICE, space_id=first.roots[0].space_id, expected_generation=1, reason="storage-recovery", attachment_ids=[first.attachment_id, second.attachment_id])
    with pytest.raises(AttachmentPending):
        await storage[3].retire(**arguments)
    assert not first_objects
    assert {item.attachment_id for item in journal.snapshot().residents} == {last}
    monkeypatch.setattr(journal, "finish_resident", original)
    storage[4]._containment_scope = NativeResidentContainmentScopes(provider=factory.provider, files=storage[0], adapter=storage[4])
    await storage[3].retire(**arguments)
    assert journal.snapshot().residents == ()


@pytest.mark.asyncio
async def test_finished_retry_rechecks_reserved_backing_under_sql_parent_lock(storage, fixture):
    factory, saved, captured, _objects, calls = await prepare(storage, fixture)
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    calls.clear()
    async with factory(storage[3], captured) as scope:
        async with storage[0].registry.host_locked(scope.parent_ids) as (session, _parents):
            binding = await session.get(SpaceBackingRow, saved.roots[0].backing_handle)
            binding.slot_id = "f" * 32
            with pytest.raises(AttachmentPending, match="backing changed"):
                await scope.admit(session, {saved.attachment_id: await session.get(SpaceAttachmentRow, saved.attachment_id)})
    assert not calls


@pytest.mark.asyncio
async def test_finished_retry_rechecks_valid_closure_changes_after_final_observation(storage, fixture):
    import asyncio

    factory, saved, captured, _objects, calls = await prepare(storage, fixture)
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    async with factory(storage[3], captured) as scope:
        async with storage[0].registry.host_locked(scope.parent_ids) as (session, _parents):
            await scope.admit(session, {saved.attachment_id: await session.get(SpaceAttachmentRow, saved.attachment_id)})
        loop = asyncio.get_running_loop()
        original = factory.provider._backend._run
        changed = False

        async def change_closure():
            async with storage[1]() as session, session.begin():
                row = await session.get(SpaceAttachmentRow, saved.attachment_id)
                row.fence_reason = "idle"

        def change_during_census(command, **kwargs):
            nonlocal changed
            if command[1] == "ps" and not changed:
                changed = True
                asyncio.run_coroutine_threadsafe(change_closure(), loop).result(timeout=5)
            return original(command, **kwargs)

        factory.provider._backend._run = change_during_census
        with pytest.raises(AttachmentPending, match="confirmed fenced SQL"):
            await scope.finalize()
    assert changed and all(command[1] in ("inspect", "ps", "network", "stop", "rm") for command in calls)


@pytest.mark.asyncio
async def test_finished_retry_refuses_matching_journal_uncertainty_published_during_census(storage, fixture):
    factory, saved, captured, _objects, calls = await prepare(storage, fixture)
    assert await storage[3].contain_host(saved.attachment_id, "operator")
    calls.clear()
    async with factory(storage[3], captured) as scope:
        original = factory.provider._backend._run
        bridge = factory.provider._native_creation
        changed = False

        def change_during_census(command, **kwargs):
            nonlocal changed
            if command[1] == "ps" and not changed:
                changed = True
                bridge.evidence.begin(owner_id="alice", thread_id="retry", name=saved.name, slots=(saved.roots[0].slot_id,))
                bridge.qualify(teardown=True)
            return original(command, **kwargs)

        factory.provider._backend._run = change_during_census
        async with storage[0].registry.host_locked(scope.parent_ids) as (session, _parents):
            with pytest.raises(AttachmentPending, match="journal uncertainty"):
                await scope.admit(session, {saved.attachment_id: await session.get(SpaceAttachmentRow, saved.attachment_id)})
    assert changed and all(command[1] in ("inspect", "ps", "network") for command in calls)


@pytest.mark.asyncio
async def test_sql_fenced_with_unfinished_journal_still_blocks_backing_admin(storage, fixture, monkeypatch):
    factory, saved, _captured, _objects, _calls = await prepare(storage, fixture)

    def unfinished(_record):
        raise OSError("Terminal journal completion remains unknown")

    monkeypatch.setattr(factory.provider._native_creation.evidence, "finish_resident", unfinished)
    assert await storage[3].contain_host(saved.attachment_id, "operator") is False
    async with storage[1]() as session:
        assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "fenced"
    with pytest.raises(AttachmentPending):
        async with storage[0].registry.admitted(actor=ALICE, requests={saved.roots[0].space_id: (Permission.ADMIN, 1)}):
            pytest.fail("SQL fenced alone admitted backing mutation before durable completion")

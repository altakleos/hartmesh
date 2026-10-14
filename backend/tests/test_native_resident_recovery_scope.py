"""Explicit host recovery uses original graphs, real SQL/disk and offline Docker."""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from threading import Event

import pytest
from sqlalchemy import delete
from test_native_create_bridge import fixture as fixture
from test_native_resident_containment_scope import prepare
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.resident_containment import _fenced_facts


async def eligibility(storage, saved, *, missing):
    async with storage[1]() as session, session.begin():
        row = await session.get(SpaceAttachmentRow, saved.attachment_id)
        if missing:
            await session.execute(delete(SpaceMountRow).where(SpaceMountRow.attachment_id == saved.attachment_id))
            await session.delete(row)
            expected = None
        else:
            row.phase, row.fenced_by_kind, row.fenced_by_id = "fenced", "host", saved.daemon_id
            row.fence_reason, row.fenced_at = "operator", datetime.now(UTC)
    async with storage[1]() as session:
        expected = None if missing else _fenced_facts(await session.get(SpaceAttachmentRow, saved.attachment_id))

    async def eligible(session, original):
        assert original.attachment_id == saved.attachment_id
        row = await session.get(SpaceAttachmentRow, saved.attachment_id, populate_existing=True)
        if (row is None) != missing or (row is not None and _fenced_facts(row) != expected):
            raise AttachmentPending("Original orphan SQL eligibility changed")

    return eligible


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_explicit_orphan_scope_contains_surviving_originals_then_completes(storage, fixture, missing):
    from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes

    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    eligible = await eligibility(storage, saved, missing=missing)
    recovery = NativeResidentRecoveryScopes(factory)
    async with recovery(saved, eligible=eligible, deadline=time.monotonic() + 30) as scope:
        await scope.contain()
        assert not objects and factory.provider._native_creation.residents.for_attachment(saved.attachment_id).phase == "retiring"
        await scope.finalize()
    assert factory.provider._native_creation.residents.records() == ()
    assert [command[1] for command in calls if command[1] in ("stop", "rm")] == ["stop", "rm"]
    async with storage[1]() as session:
        row = await session.get(SpaceAttachmentRow, saved.attachment_id)
        assert row is None if missing else _fenced_facts(row)[0] == "fenced"


@pytest.mark.asyncio
async def test_fenced_row_changed_after_recovery_capture_refuses_all_effects(storage, fixture):
    from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes

    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    eligible = await eligibility(storage, saved, missing=False)
    async with NativeResidentRecoveryScopes(factory)(saved, eligible=eligible, deadline=time.monotonic() + 30) as scope:
        async with storage[0].registry.host_locked(scope.parent_ids) as (session, _parents):
            row = await session.get(SpaceAttachmentRow, saved.attachment_id)
            row.container_id = "f" * 64
            await session.flush()
            with pytest.raises(AttachmentPending, match="eligibility changed"):
                await scope.admit(session)
    assert objects and not any(command[1] in ("stop", "rm") for command in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ["name", "writer", "acknowledged"])
async def test_original_recovery_preserves_untransferred_create_requests(storage, fixture, collision):
    from uuid import uuid4

    from deerflow.spaces.docker import StorageHostUnqualified
    from deerflow.spaces.native_create_evidence import NativeContainerIdentity, NativeCreateAttempt
    from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes

    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    eligible = await eligibility(storage, saved, missing=True)
    _journal, handoffs, bridge, _owner = fixture
    name = saved.name if collision == "name" else "other-primary"
    slot = saved.roots[0].slot_id if collision != "name" else "f" * 32
    # A retained handoff is independent of the journal: an acknowledged
    # request has already erased its create file but has not transferred yet.
    attempt = NativeCreateAttempt(uuid4().hex, saved.daemon_id, "other-owner", "chat", name, (slot,))
    handoff = handoffs.record_pending(attempt, sandbox_id="other-sandbox")
    if collision == "acknowledged":
        identity = NativeContainerIdentity(attempt.id, saved.daemon_id, attempt.owner_id, attempt.thread_id, name, (slot,), "e" * 64)
        handoffs.record_verified(handoff, identity)
        handoffs.record_acknowledged(handoff)
    async with NativeResidentRecoveryScopes(factory)(saved, eligible=eligible, deadline=time.monotonic() + 30) as scope:
        with pytest.raises(StorageHostUnqualified, match="create|handoff|request"):
            await scope.contain()
    assert objects and bridge.residents.for_attachment(saved.attachment_id) == saved
    assert not any(command[1] in ("stop", "rm") for command in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["commit", "terminal-sync", "late-binding"])
async def test_recovery_retry_preserves_original_after_failed_commit_or_completion(storage, fixture, monkeypatch, failure):
    from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes

    factory, saved, _captured, objects, calls = await prepare(storage, fixture)
    eligible = await eligibility(storage, saved, missing=True)
    recovery = NativeResidentRecoveryScopes(factory)
    registry, journal = storage[0].registry, fixture[0]
    original = registry.host_locked
    fsync = os.fsync

    @asynccontextmanager
    async def lost_commit(parents):
        async with original(parents) as context:
            yield context
            raise RuntimeError("Eligibility commit failed")

    def interrupted(fd):
        if fd == journal._directory_fd and not objects:
            raise OSError("Terminal directory sync failed")
        return fsync(fd)

    async with recovery(saved, eligible=eligible, deadline=time.monotonic() + 30) as scope:
        if failure == "commit":
            monkeypatch.setattr(registry, "host_locked", lost_commit)
            with pytest.raises(RuntimeError, match="commit failed"):
                await scope.contain()
            with pytest.raises(AttachmentPending, match="confirmed eligibility commit"):
                await scope.finalize()
            monkeypatch.setattr(registry, "host_locked", original)
        else:
            await scope.contain()
            if failure == "terminal-sync":
                monkeypatch.setattr(os, "fsync", interrupted)
                with pytest.raises(OSError, match="sync failed"):
                    await scope.finalize()
                monkeypatch.setattr(os, "fsync", fsync)
            else:
                async with original(scope.parent_ids) as (session, _parents):
                    binding = await session.get(SpaceBackingRow, saved.roots[0].backing_handle)
                    binding.root_inode += 1
                with pytest.raises(AttachmentPending, match="backing changed"):
                    await scope.finalize()
                async with original(scope.parent_ids) as (session, _parents):
                    binding = await session.get(SpaceBackingRow, saved.roots[0].backing_handle)
                    binding.root_inode -= 1
    retained = factory.provider._native_creation.residents.for_attachment(saved.attachment_id)
    assert not objects and retained.phase == "retiring"
    effects = [command for command in calls if command[1] in ("stop", "rm")]
    async with recovery(retained, eligible=eligible, deadline=time.monotonic() + 30) as retry:
        await retry.contain()
        await retry.finalize()
    assert journal.snapshot().residents == ()
    assert [command for command in calls if command[1] in ("stop", "rm")] == effects


@pytest.mark.asyncio
async def test_cancelled_recovery_drains_worker_before_releasing_original_batch(storage, fixture):
    from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy
    from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes

    factory, saved, _captured, _objects, _calls = await prepare(storage, fixture)
    eligible = await eligibility(storage, saved, missing=True)
    entered, release = Event(), Event()
    engine = factory.provider._backend._run

    def held(command, **kwargs):
        if command[1] == "stop":
            entered.set()
            assert release.wait(10)
        return engine(command, **kwargs)

    factory.provider._backend._run = held

    async def recover():
        async with NativeResidentRecoveryScopes(factory)(saved, eligible=eligible, deadline=time.monotonic() + 30) as scope:
            await scope.contain()
            await scope.finalize()

    task = asyncio.create_task(recover())
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(NativeRetirementBusy):
            factory.provider._owner_admission_gate().reserve("other-owner", saved.roots[0].slot_id)
        fixture[3].live = False
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert factory.provider._native_creation.residents.for_attachment(saved.attachment_id).phase == "retiring"
    gate = factory.provider._owner_admission_gate()
    token = gate.reserve("other-owner", saved.roots[0].slot_id)
    gate.end_retirement(token)

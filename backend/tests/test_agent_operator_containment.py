"""Real bound SQL roots and v5 evidence, with every Docker command offline."""

import asyncio
import json
from dataclasses import replace
from threading import Event

import pytest
from test_native_create_bridge import fixture as fixture
from test_native_resident_containment_scope import prepare
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, StorageHostReplacementRow
from deerflow.spaces.native_create_evidence import NativeEvidenceReader


async def operator_scope(storage, fixture):
    from deerflow.spaces.operator_containment import OperatorResidentContainmentScopes

    previous, saved, captured, objects, calls = await prepare(storage, fixture)
    storage[4]._operator_only = True
    reader = NativeEvidenceReader(fixture[0].pool, daemon_id=storage[4].host_id)
    factory = OperatorResidentContainmentScopes(files=storage[0], adapter=storage[4], evidence=reader)
    storage[4]._containment_scope = factory
    return factory, saved, captured, objects, calls, previous


def journal_bytes(fixture):
    return {path.name: (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) for path in (fixture[0].pool / ".native-creates").iterdir()}


@pytest.mark.asyncio
async def test_operator_closes_exact_graph_and_leaves_native_journal_for_gateway(storage, fixture, monkeypatch):
    factory, saved, _captured, objects, calls, previous = await operator_scope(storage, fixture)
    before = journal_bytes(fixture)
    monkeypatch.setattr(fixture[0], "retire_resident", lambda *args: pytest.fail("Operator must not retire a journal record"))
    monkeypatch.setattr(fixture[0], "finish_resident", lambda *args: pytest.fail("Operator must not finish a journal record"))
    try:
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        assert not objects and journal_bytes(fixture) == before
        assert previous.provider._native_creation.residents.records() == (saved,)
        async with storage[1]() as session:
            row = await session.get(SpaceAttachmentRow, saved.attachment_id)
            assert row.phase == "fenced" and row.fence_reason == "operator"
        assert [command[1] for command in calls if command[1] in {"stop", "rm"}] == ["stop", "rm"]
        calls.clear()
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in calls)
        assert journal_bytes(fixture) == before
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["created", "mount", "port", "unknown-preparation"])
async def test_operator_rejects_changed_original_or_unknown_preparation_before_any_effect(storage, fixture, change):
    factory, saved, _captured, objects, calls, _previous = await operator_scope(storage, fixture)
    before = journal_bytes(fixture)
    if change == "created":
        objects[saved.container_id]["Created"] = "2026-09-01T00:00:00Z"
    elif change == "mount":
        objects[saved.container_id]["Mounts"][0]["Source"] = "/changed"
    elif change == "port":
        objects[saved.container_id]["HostConfig"]["PortBindings"] = {"1234/tcp": [{"HostPort": "1234"}]}
    else:
        async with storage[1]() as session, session.begin():
            (await session.get(SpaceAttachmentRow, saved.attachment_id)).container_id = None
    try:
        assert not await storage[3].contain_host(saved.attachment_id, "operator")
        assert objects and journal_bytes(fixture) == before
        assert not any(command[1] in {"stop", "rm", "create", "start", "run"} for command in calls)
        async with storage[1]() as session:
            assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase != "fenced"
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
async def test_operator_resumes_after_lost_remove_reply_with_unchanged_original_evidence(storage, fixture):
    factory, saved, _captured, objects, calls, _previous = await operator_scope(storage, fixture)
    before = journal_bytes(fixture)
    backend = storage[4]._backend
    original = backend._run

    def lost_reply(command, **kwargs):
        reply = original(command, **kwargs)
        if command[1] == "rm":
            return replace_reply(reply, returncode=1)
        return reply

    backend._run = lost_reply
    try:
        assert not await storage[3].contain_host(saved.attachment_id, "operator")
        assert not objects and journal_bytes(fixture) == before
        backend._run = original
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        assert journal_bytes(fixture) == before
    finally:
        factory.evidence.close()


def replace_reply(reply, **values):
    from types import SimpleNamespace

    return SimpleNamespace(**{**vars(reply), **values})


@pytest.mark.asyncio
async def test_live_gateway_finishes_operator_prepared_journal_without_restart(storage, fixture):
    factory, saved, _captured, objects, calls, gateway = await operator_scope(storage, fixture)
    try:
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        assert not objects and fixture[2].residents.records() == (saved,)
        storage[4]._operator_only = False
        storage[4]._containment_scope = gateway
        calls.clear()
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        assert fixture[2].residents.records() == ()
        assert fixture[0].snapshot().residents == ()
        assert not any(command[1] in {"run", "create", "start", "stop", "rm"} for command in calls)
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
async def test_finished_operator_scope_rechecks_late_matching_journal_uncertainty(storage, fixture, monkeypatch):
    factory, saved, _captured, _objects, _calls, gateway = await operator_scope(storage, fixture)
    try:
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        storage[4]._containment_scope = gateway
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        storage[4]._containment_scope = factory
        async with factory(storage[3], _captured) as scope:
            original = storage[4].bootstrap_residents

            def late_uncertainty(**kwargs):
                result = original(**kwargs)
                fixture[0].begin(owner_id="resident:" + saved.attachment_id, thread_id="late", name=saved.name, slots=saved.slots)
                return result

            monkeypatch.setattr(storage[4], "bootstrap_residents", late_uncertainty)
            from deerflow.spaces.attachments import AttachmentPending
            from deerflow.utils.file_io import run_file_io

            with pytest.raises(AttachmentPending, match="journal uncertainty"):
                await run_file_io(scope._observe_finished, saved.attachment_id)
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["retire-before", "retire-after", "finish-before"])
async def test_gateway_retries_operator_handoff_after_journal_transition_failure(storage, fixture, monkeypatch, failure):
    factory, saved, _captured, objects, calls, gateway = await operator_scope(storage, fixture)
    try:
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        storage[4]._containment_scope = gateway
        method = "finish_resident" if failure == "finish-before" else "retire_resident"
        original = getattr(fixture[0], method)

        def interrupted(*args, **kwargs):
            if failure == "retire-after":
                original(*args, **kwargs)
            raise OSError("Simulated journal transition interruption")

        monkeypatch.setattr(fixture[0], method, interrupted)
        calls.clear()
        assert not await storage[3].contain_host(saved.attachment_id, "operator")
        assert not objects
        async with storage[1]() as session:
            assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "fenced"
        monkeypatch.setattr(fixture[0], method, original)
        assert await storage[3].contain_host(saved.attachment_id, "operator")
        assert fixture[0].snapshot().residents == fixture[2].residents.records() == ()
        assert not any(command[1] in {"run", "create", "start", "stop", "rm"} for command in calls)
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mapped", [False, True])
async def test_operator_foreign_host_requires_adoption_and_proves_absence_without_effects(storage, fixture, mapped):
    factory, saved, _captured, objects, calls, _gateway = await operator_scope(storage, fixture)
    before = journal_bytes(fixture)
    factory.evidence.close()
    storage[4].host_id = "replacement-daemon"
    factory.evidence = NativeEvidenceReader(fixture[0].pool, daemon_id="replacement-daemon")
    if mapped:
        async with storage[1]() as session, session.begin():
            session.add(StorageHostReplacementRow(old_host_id=saved.daemon_id, new_host_id="replacement-daemon", attachments_named=1))
    try:
        assert not await storage[3].contain_host(saved.attachment_id, "operator")
        assert objects and journal_bytes(fixture) == before
        assert not any(command[1] in {"run", "create", "start", "stop", "rm"} for command in calls)
        objects.clear()
        assert await storage[3].contain_host(saved.attachment_id, "operator") is mapped
        async with storage[1]() as session:
            row = await session.get(SpaceAttachmentRow, saved.attachment_id)
            assert (row.phase == "fenced") is mapped
            if mapped:
                assert (row.fence_reason, row.fenced_by_id) == ("host-replaced", "replacement-daemon")
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["removed", "replaced"])
async def test_operator_rechecks_original_journal_before_destructive_commands(storage, fixture, change):
    factory, saved, _captured, objects, calls, _gateway = await operator_scope(storage, fixture)
    backend = storage[4]._backend
    original = backend._run
    changed = False

    def concurrent_change(command, **kwargs):
        nonlocal changed
        reply = original(command, **kwargs)
        if command[1] == "inspect" and not changed:
            changed = True
            path = fixture[0].pool / ".native-creates" / (saved.id + ".json")
            if change == "removed":
                path.unlink()
            else:
                path.write_text(json.dumps(replace(saved, container_id="f" * 64).document()), encoding="utf-8")
        return reply

    backend._run = concurrent_change
    try:
        assert not await storage[3].contain_host(saved.attachment_id, "operator")
        assert changed and objects
        assert not any(command[1] in {"run", "create", "start", "stop", "rm"} for command in calls)
    finally:
        factory.evidence.close()


@pytest.mark.asyncio
async def test_cancelled_operator_drains_removal_before_unlocking_sql_parents(storage, fixture):
    factory, saved, _captured, _objects, _calls, _gateway = await operator_scope(storage, fixture)
    backend = storage[4]._backend
    original = backend._run
    entered, release = Event(), Event()
    admitted = asyncio.Event()

    def delayed(command, **kwargs):
        if command[1] == "stop":
            entered.set()
            assert release.wait(10), "Test did not release the owned removal worker"
        return original(command, **kwargs)

    async def competing_writer():
        async with storage[0].registry.host_locked([saved.roots[0].space_id]):
            admitted.set()

    backend._run = delayed
    task = asyncio.create_task(storage[3].contain_host(saved.attachment_id, "operator"))
    competitor = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        competitor = asyncio.create_task(competing_writer())
        await asyncio.sleep(0.03)
        assert not task.done() and not admitted.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        if competitor is not None:
            await competitor
        factory.evidence.close()
    assert admitted.is_set()


@pytest.mark.parametrize("interruption", ["missing-primary", "network-proxy", "network", "egress-network"])
def test_operator_retries_each_restricted_graph_partial_cleanup_without_journal_writes(fixture, interruption):
    from types import SimpleNamespace

    from test_resident_containment_worker import worker

    from deerflow.spaces.operator_containment import _OperatorWorker

    gateway, gate, token, saved, objects, calls = worker(fixture)
    evidence = NativeEvidenceReader(fixture[0].pool, daemon_id=saved.daemon_id)
    factory = SimpleNamespace(adapter=SimpleNamespace(_backend=gateway.backend, host_id=saved.daemon_id, _same_host=lambda: None), evidence=evidence)
    operator = _OperatorWorker(factory, (saved,), lambda record: None)
    original = gateway.backend._run
    before = journal_bytes(fixture)
    failed = False
    if interruption == "missing-primary":
        objects.pop(("container", saved.container_id))
        for entry in objects.values():
            entry.get("Containers", {}).pop(saved.container_id, None)
    else:
        member = next(member for member in saved.resources if member.role == interruption)

        def interrupted(command, **kwargs):
            nonlocal failed
            reply = original(command, **kwargs)
            if command[-1] == member.identity and "inspect" not in command and not failed:
                failed = True
                raise OSError("Simulated lost paired removal reply")
            return reply

        gateway.backend._run = interrupted
    try:
        if interruption != "missing-primary":
            with pytest.raises(OSError, match="paired removal"):
                operator.contain(saved.attachment_id)
            assert failed and journal_bytes(fixture) == before
            gateway.backend._run = original
        operator.contain(saved.attachment_id)
        assert not objects and journal_bytes(fixture) == before
        assert gateway.bridge.residents.records() == (saved,)
        assert not any(command[1] in {"run", "create", "start"} for command in calls)
    finally:
        operator.close()
        evidence.close()
        gateway.close()
        gate.end_resident_batch(token)

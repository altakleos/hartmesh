"""Durable closure facts use real SQL; injected containers are not native proof."""

from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from sqlalchemy import select
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import AttachmentPending, ResourceMount
from deerflow.spaces.contract import PrincipalRef, SpaceDenied


@pytest.mark.asyncio
async def test_human_closure_records_reason_before_unconfirmed_effects(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    provider.uncertain = True
    with pytest.raises(AttachmentPending):
        await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="lifecycle", attachment_ids=[attached.id])
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fence_pending"
        assert (row.fenced_by_kind, row.fenced_by_id, row.fence_reason, row.fenced_at) == ("human", ALICE.subject_id, "lifecycle", None)
    provider.uncertain = False
    await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="storage-recovery", attachment_ids=[attached.id])
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fenced" and row.fenced_at is not None
        assert (row.fenced_by_kind, row.fenced_by_id, row.fence_reason) == ("human", ALICE.subject_id, "lifecycle")


@pytest.mark.asyncio
async def test_contained_hook_runs_after_commit_and_captured_retry_keeps_replacement(storage):
    import asyncio

    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    seen = []

    async def contained(attachment_id, space_ids, reason):
        async with sf() as session:
            row = await session.get(SpaceAttachmentRow, attachment_id)
            assert row.phase == "fenced" and row.fenced_at is not None
            seen.append((attachment_id, space_ids, reason, row.fenced_at))
        # A callback can reenter the registry after the closure transaction.
        assert (await files.registry.get(actor=ALICE, space_id=space.id)).id == space.id

    attachments.on_contained = contained
    await asyncio.wait_for(attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="storage-recovery", attachment_ids=[attached.id]), timeout=5)
    replacement = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="lifecycle", attachment_ids=[attached.id])
    assert len(seen) == 2
    assert seen[0] == seen[1]
    assert seen[0][:3] == (attached.id, [space.id], "storage-recovery")
    assert replacement.container_id in provider.containers


@pytest.mark.asyncio
async def test_hook_failure_cannot_reopen_confirmed_closure(storage):
    files, sf, _, attachments, _ = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    attachments.on_contained = AsyncMock(side_effect=RuntimeError("consumer hook failed"))
    await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="storage-recovery")
    attachments.on_contained.assert_awaited_once_with(attached.id, [space.id], "storage-recovery")
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fenced" and row.fence_reason == "storage-recovery"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle", "operator", "", None, []])
async def test_human_retirement_refuses_nonhuman_reasons_without_effects(storage, reason):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    with pytest.raises(ValueError):
        await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason=reason)
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "active" and row.fence_reason is None
    assert attached.container_id in provider.containers


@pytest.mark.asyncio
async def test_nonhuman_cannot_be_falsely_recorded_as_human_closer(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    with pytest.raises(SpaceDenied):
        await attachments.retire(actor=PrincipalRef("nonhuman", "worker"), space_id=space.id, expected_generation=1, reason="lifecycle")
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "active" and row.fenced_by_kind is None
    assert attached.container_id in provider.containers


@pytest.mark.asyncio
async def test_human_notification_reports_complete_mount_scope(storage):
    files, _sf, _, attachments, _provider = storage
    first, second = await home(files), await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(first.id, 1, "first", writable=True), ResourceMount(second.id, 1, "second")])
    attachments.on_contained = AsyncMock()
    await attachments.retire(actor=ALICE, space_id=first.id, expected_generation=1, reason="storage-recovery", attachment_ids=[attached.id])
    attachments.on_contained.assert_awaited_once_with(attached.id, sorted([first.id, second.id]), "storage-recovery")


@pytest.mark.asyncio
async def test_host_closes_without_principal_or_storage_event_and_notifies_after_commit(storage, monkeypatch):
    from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow

    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    async with sf() as session:
        before_events = len((await session.execute(select(SpaceEventRow))).scalars().all())
    monkeypatch.setattr(files.registry, "_actor", AsyncMock(side_effect=AssertionError("Host closure invents no principal")))

    async def contained(attachment_id, space_ids, reason):
        async with sf() as session:
            row = await session.get(SpaceAttachmentRow, attachment_id)
            assert row.phase == "fenced" and row.fenced_at is not None
        assert space_ids == [space.id] and reason == "operator"

    attachments.on_contained = contained
    assert await attachments.contain_host(attached.id, "operator")
    assert not provider.containers and not attachments.uncontained
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert (row.fenced_by_kind, row.fenced_by_id, row.fence_reason) == ("host", provider.host_id, "operator")
        assert (await session.get(SpaceRow, space.id)).generation == 1
        assert len((await session.execute(select(SpaceEventRow))).scalars().all()) == before_events


@pytest.mark.asyncio
async def test_host_uncertainty_retains_first_reason_and_retries(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    provider.uncertain = True
    assert await attachments.contain_host(attached.id, "idle") is False
    assert attachments.uncontained == {attached.id: "idle"}
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fence_pending" and row.fence_reason == "idle" and row.fenced_at is None
    provider.uncertain = False
    assert await attachments.contain_host(attached.id, "operator")
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.fence_reason == "idle" and row.fenced_at is not None
    assert not attachments.uncontained


@pytest.mark.asyncio
async def test_unknown_prepare_is_not_settled_by_one_discovered_container_or_empty_census(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    prepare = provider.prepare

    def lost_reply(plan):
        prepare(plan)
        raise RuntimeError("Preparation reply lost")

    provider.prepare = lost_reply
    with pytest.raises(AttachmentPending):
        await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    async with sf() as session:
        row = (await session.execute(select(SpaceAttachmentRow))).scalar_one()
        identifier = row.id
        assert row.container_id is None
    assert provider.containers
    assert await attachments.contain_host(identifier, "outcome-uncertain") is False
    provider.containers.clear()  # Even confirmed absence of the observed ID is insufficient.
    assert await attachments.contain_host(identifier, "operator") is False
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, identifier)
        assert row.phase == "fence_pending" and row.fenced_at is None
        assert row.fence_reason == "outcome-uncertain"


@pytest.mark.asyncio
async def test_host_requires_recorded_replacement_before_cross_host_closure(storage):
    from deerflow.persistence.spaces.lifecycle import StorageHostReplacementRow

    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    previous = provider.host_id
    provider.host_id = "replacement-host"
    assert await attachments.contain_host(attached.id, "operator") is False
    assert attached.container_id in provider.containers
    async with sf() as session, session.begin():
        session.add(StorageHostReplacementRow(old_host_id=previous, new_host_id=provider.host_id, attachments_named=1))
    assert await attachments.contain_host(attached.id, "operator")
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert (row.fenced_by_kind, row.fenced_by_id, row.fence_reason) == ("host", provider.host_id, "host-replaced")


@pytest.mark.asyncio
async def test_unfenced_reads_are_exact_and_fence_reason_is_visible_when_closure_begins(storage):
    from deerflow.spaces.contract import SpaceConflict

    files, _sf, _, attachments, provider = storage
    space = await home(files)
    assert await attachments.unfenced(space.id) is None
    first = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    state = await attachments.unfenced(space.id)
    assert (state.id, state.phase, state.host_id, state.container_id) == (first.id, "active", provider.host_id, first.container_id)
    assert await attachments.fence_reason(first.id) is None
    provider.uncertain = True
    assert await attachments.contain_host(first.id, "idle") is False
    assert await attachments.fence_reason(first.id) == "idle"
    provider.uncertain = False
    await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "parallel", writable=True)])
    with pytest.raises(SpaceConflict, match="one attachment"):
        await attachments.unfenced(space.id)


@pytest.mark.asyncio
@pytest.mark.parametrize("host", [False, True])
async def test_cancellation_drains_removal_commit_and_notification(storage, host):
    import asyncio
    import threading

    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    entered, release = threading.Event(), threading.Event()
    fence = provider.fence

    def held_fence(*args):
        entered.set()
        assert release.wait(5)
        fence(*args)

    provider.fence = held_fence
    attachments.on_contained = AsyncMock()
    close = attachments.contain_host(attached.id, "idle") if host else attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="lifecycle")
    task = asyncio.create_task(close)
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done() and attached.container_id in provider.containers
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fenced" and row.fenced_at is not None
    attachments.on_contained.assert_awaited_once()


@pytest.mark.asyncio
async def test_commit_failure_after_removal_retains_durable_retry(storage):
    from sqlalchemy import event

    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    attachments.on_contained = AsyncMock()
    commits = 0

    def fail_second_commit(_connection):
        nonlocal commits
        commits += 1
        if commits == 2:
            raise RuntimeError("Commit outcome unconfirmed")

    engine = sf.kw["bind"].sync_engine
    event.listen(engine, "commit", fail_second_commit)
    try:
        assert await attachments.contain_host(attached.id, "idle") is False
    finally:
        event.remove(engine, "commit", fail_second_commit)
    assert attached.container_id not in provider.containers
    attachments.on_contained.assert_not_awaited()
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fence_pending" and row.fenced_at is None
        assert row.fence_reason == "idle"
    assert await attachments.contain_host(attached.id, "operator")
    attachments.on_contained.assert_awaited_once_with(attached.id, [space.id], "idle")


@pytest.mark.asyncio
async def test_host_does_not_fence_while_preparation_worker_owns_sql(storage):
    import asyncio
    import threading

    files, sf, _, attachments, provider = storage
    space = await home(files)
    entered, release = threading.Event(), threading.Event()
    prepare = provider.prepare

    def held_prepare(plan):
        entered.set()
        assert release.wait(5)
        return prepare(plan)

    provider.prepare = held_prepare
    preparing = asyncio.create_task(attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)]))
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        async with sf() as session:
            identifier = (await session.execute(select(SpaceAttachmentRow.id))).scalar_one()
        closing = asyncio.create_task(attachments.contain_host(identifier, "outcome-uncertain"))
        await asyncio.sleep(0.05)
        assert not closing.done()
    finally:
        release.set()
    prepared, contained = await asyncio.gather(preparing, closing, return_exceptions=True)
    assert not isinstance(prepared, Exception) or isinstance(prepared, AttachmentPending)
    assert contained is True and not provider.containers
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, identifier)
        assert row.phase == "fenced" and row.container_id is not None


@pytest.mark.asyncio
async def test_late_prepare_after_empty_census_does_not_release_attachment(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    plans = []
    prepare = provider.prepare

    def unknown_prepare(plan):
        plans.append(plan)
        raise RuntimeError("Reply lost; creation can finish later")

    provider.prepare = unknown_prepare
    with pytest.raises(AttachmentPending):
        await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    identifier = plans[0].id
    assert await attachments.contain_host(identifier, "outcome-uncertain") is False
    late_container = prepare(plans[0])
    assert late_container in provider.containers
    assert await attachments.contain_host(identifier, "operator") is False
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, identifier)
        assert row.phase == "fence_pending" and row.container_id is None and row.fenced_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [{}, {"fenced_by_kind": "host"}, {"fenced_by_kind": "host", "fenced_by_id": "host", "fence_reason": "idle"}])
async def test_historical_fenced_rows_allow_only_completely_empty_legacy_metadata(storage, metadata):
    files, sf, _, attachments, _ = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    async with sf() as session, session.begin():
        row = await session.get(SpaceAttachmentRow, attached.id)
        row.phase = "fenced"
        for key, value in metadata.items():
            setattr(row, key, value)
    attachments.on_contained = AsyncMock()
    assert await attachments.contain_host(attached.id, "operator") is (not metadata)
    if metadata:
        attachments.on_contained.assert_not_awaited()
        with pytest.raises(AttachmentPending):
            await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="lifecycle", attachment_ids=[attached.id])
    else:
        attachments.on_contained.assert_awaited_once_with(attached.id, [space.id], "operator")
    async with sf() as session:
        assert (await session.get(SpaceAttachmentRow, attached.id)).fenced_at is None


@pytest.mark.asyncio
async def test_host_inspection_is_read_only_and_requires_active_current_identity(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    calls = []

    def container_state(container, identifier):
        calls.append((container, identifier))
        return "gone"

    provider.container_state = container_state
    assert await attachments.container_gone(attached.id)
    assert calls == [(attached.container_id, attached.id)]
    assert attached.container_id in provider.containers
    provider.host_id = "other-host"
    assert await attachments.container_state(attached.id) == "unknown"
    assert len(calls) == 1
    provider.host_id = attached.host_id
    async with sf() as session, session.begin():
        row = await session.get(SpaceAttachmentRow, attached.id)
        row.phase = "fence_pending"
    assert not await attachments.container_gone(attached.id)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_host_provider_value_error_retains_uncertainty(storage):
    files, _sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])

    def invalid_discovery(_identifier):
        raise ValueError("Malformed provider reply")

    provider.discover = invalid_discovery
    assert await attachments.contain_host(attached.id, "idle") is False
    assert attachments.uncontained == {attached.id: "idle"}


@pytest.mark.asyncio
async def test_already_fenced_host_retry_does_not_require_current_daemon(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    assert await attachments.contain_host(attached.id, "idle")
    async with sf() as session:
        timestamp = (await session.get(SpaceAttachmentRow, attached.id)).fenced_at
    provider.host_id = "replacement-host"
    provider.uncertain = True
    attachments.on_contained = AsyncMock()
    assert await attachments.contain_host(attached.id, "operator")
    attachments.on_contained.assert_awaited_once_with(attached.id, [space.id], "idle")
    async with sf() as session:
        assert (await session.get(SpaceAttachmentRow, attached.id)).fenced_at == timestamp


@pytest.mark.asyncio
@pytest.mark.parametrize("host", [False, True])
async def test_cleanup_failure_retains_closure_until_retry(storage, host):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    cleanup_calls = []

    def failed_cleanup(identifier):
        cleanup_calls.append(identifier)
        raise RuntimeError("Paired resources remain unconfirmed")

    provider.cleanup = failed_cleanup
    attachments.on_contained = AsyncMock()
    if host:
        assert await attachments.contain_host(attached.id, "idle") is False
    else:
        with pytest.raises(AttachmentPending):
            await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="lifecycle")
    assert cleanup_calls == [attached.id] and not provider.containers
    attachments.on_contained.assert_not_awaited()
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "fence_pending" and row.fenced_at is None
    provider.cleanup = cleanup_calls.append
    assert await attachments.contain_host(attached.id, "operator")
    assert cleanup_calls == [attached.id, attached.id]
    attachments.on_contained.assert_awaited_once_with(attached.id, [space.id], "idle" if host else "lifecycle")


@pytest.mark.asyncio
async def test_host_retry_queue_uses_host_reason_and_preserves_human_attribution(storage):
    files, sf, _, attachments, provider = storage
    space = await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    provider.uncertain = True
    with pytest.raises(AttachmentPending):
        await attachments.retire(actor=ALICE, space_id=space.id, expected_generation=1, reason="lifecycle")
    assert await attachments.contain_host(attached.id, "idle") is False
    assert attachments.uncontained == {attached.id: "idle"}
    provider.uncertain = False
    assert await attachments.contain_host(attached.id, attachments.uncontained[attached.id])
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert (row.fenced_by_kind, row.fenced_by_id, row.fence_reason) == ("human", ALICE.subject_id, "lifecycle")


@pytest.mark.asyncio
async def test_mount_scope_change_refuses_before_effects(storage, monkeypatch):
    from contextlib import asynccontextmanager

    from deerflow.persistence.spaces.lifecycle import SpaceMountRow

    files, sf, _, attachments, provider = storage
    first, second = await home(files), await home(files)
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(first.id, 1, "first", writable=True), ResourceMount(second.id, 1, "second")])
    host_locked = files.registry.host_locked
    scopes = []

    @asynccontextmanager
    async def changed_scope(identifiers):
        scopes.append(identifiers)
        if len(scopes) == 1:
            async with sf() as session, session.begin():
                mount = await session.get(SpaceMountRow, (attached.id, second.id))
                mount.alias = "changed"
        async with host_locked(identifiers) as admitted:
            yield admitted

    monkeypatch.setattr(files.registry, "host_locked", changed_scope)
    assert await attachments.contain_host(attached.id, "operator") is False
    assert scopes == [sorted([first.id, second.id])]
    assert attached.container_id in provider.containers
    async with sf() as session:
        row = await session.get(SpaceAttachmentRow, attached.id)
        assert row.phase == "active" and row.fence_reason is None


@pytest.mark.asyncio
async def test_host_hook_reenters_with_a_single_connection_pool(tmp_path):
    import asyncio
    from types import SimpleNamespace

    from _storage_spaces_test_support import make_storage_fixture
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import SpaceAttachments

    async for files, sf, _catalog in make_storage_fixture(tmp_path, SimpleNamespace(param="sqlite")):
        attachments = SpaceAttachments(files, ContainedProvider())
        space = await home(files)
        attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
        engine = create_async_engine(sf.kw["bind"].url, pool_size=1, max_overflow=0, pool_timeout=1)
        single = async_sessionmaker(engine, expire_on_commit=False)
        files.registry._sf = single
        observed = []

        async def contained(identifier, space_ids, reason):
            async with single() as session:
                assert (await session.get(SpaceAttachmentRow, identifier)).phase == "fenced"
            assert (await files.registry.get(actor=ALICE, space_id=space.id)).id == space.id
            observed.append((identifier, space_ids, reason))

        attachments.on_contained = contained
        try:
            assert await asyncio.wait_for(attachments.contain_host(attached.id, "idle"), timeout=5)
            assert observed == [(attached.id, [space.id], "idle")]
        finally:
            files.registry._sf = sf
            await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier,reason", [("a" * 31, "idle"), ("g" * 32, "idle"), (None, "idle"), ("a" * 32, "lifecycle"), ("a" * 32, []), ("a" * 32, None)])
async def test_host_invalid_inputs_are_rejected_before_database_admission(identifier, reason):
    from types import SimpleNamespace

    from deerflow.spaces.attachments import SpaceAttachments

    registry = SimpleNamespace(_sf=AsyncMock(side_effect=AssertionError("Invalid input must not admit SQL")))
    attachments = SpaceAttachments(SimpleNamespace(registry=registry), SimpleNamespace(host_id="host"))
    with pytest.raises(ValueError):
        await attachments.contain_host(identifier, reason)
    registry._sf.assert_not_called()

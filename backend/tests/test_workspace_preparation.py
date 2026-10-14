"""Real SQL and worker contexts prove preparation provenance, not native quotas."""

import asyncio
import contextvars
import sqlite3
import threading

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from sqlalchemy import select
from test_storage_spaces_attachments import storage as storage

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import ResourceMount


@pytest.mark.asyncio
async def test_preparation_token_binds_the_exact_provider_and_committed_intent(storage):
    from deerflow.spaces.preparation import current_preparation

    files, sf, _, attachments, provider = storage
    space = await home(files)
    async with sf() as session:
        slot = (await session.get(SpaceBackingRow, space.backing_handle)).slot_id
    prepare = provider.prepare
    observed = []

    def admitted_prepare(plan):
        token = current_preparation()
        assert token is not None and token.live
        assert token.provider is provider and token.plan is plan
        assert plan.views[0].slot_id == slot
        if sf.kw["bind"].dialect.name == "sqlite":
            with sqlite3.connect(sf.kw["bind"].url.database) as connection:
                assert connection.execute("SELECT phase, container_id FROM storage_space_attachments WHERE id = ?", (plan.id,)).fetchone() == ("pending", None)
        observed.append((token, contextvars.copy_context()))
        return prepare(plan)

    provider.prepare = admitted_prepare
    attached = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    token, copied = observed[0]
    assert token.plan.id == attached.id and not token.live
    assert current_preparation() is None and copied.run(current_preparation) is None
    async with sf() as session:
        assert (await session.get(SpaceAttachmentRow, attached.id)).phase == "active"


@pytest.mark.asyncio
async def test_cancelled_preparation_invalidates_token_only_after_worker_drains(storage):
    from deerflow.spaces.preparation import current_preparation

    files, sf, _, attachments, provider = storage
    space = await home(files)
    entered, release = threading.Event(), threading.Event()
    prepare = provider.prepare
    observed = []

    def held_prepare(plan):
        observed.append(current_preparation())
        entered.set()
        assert release.wait(5)
        return prepare(plan)

    provider.prepare = held_prepare
    task = asyncio.create_task(attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)]))
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done() and observed[0].live
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not observed[0].live and current_preparation() is None
    async with sf() as session:
        row = (await session.execute(select(SpaceAttachmentRow))).scalar_one()
        assert row.phase == "active" and row.container_id is not None


@pytest.mark.asyncio
async def test_preparation_rechecks_recorded_host_before_minting_token(storage, monkeypatch):
    from contextlib import asynccontextmanager
    from unittest.mock import Mock

    from deerflow.spaces.attachments import AttachmentPending

    files, sf, _, attachments, provider = storage
    space = await home(files)
    admitted = files.registry.admitted
    calls = 0

    @asynccontextmanager
    async def host_changes_after_intent(**kwargs):
        nonlocal calls
        calls += 1
        async with admitted(**kwargs) as result:
            yield result
        if calls == 1:
            provider.host_id = "different-host"

    monkeypatch.setattr(files.registry, "admitted", host_changes_after_intent)
    provider.prepare = Mock(side_effect=AssertionError("Changed host cannot prepare"))
    with pytest.raises(AttachmentPending):
        await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    provider.prepare.assert_not_called()
    async with sf() as session:
        row = (await session.execute(select(SpaceAttachmentRow))).scalar_one()
        assert row.phase == "pending" and row.container_id is None


@pytest.mark.asyncio
async def test_preparation_retains_slot_coordination_through_identity_commit(storage):
    from sqlalchemy import event

    from deerflow.spaces.preparation import current_preparation

    files, sf, _, attachments, provider = storage
    space = await home(files)
    prepare, start = provider.prepare, provider.start
    held = []
    commits = []

    def release():
        held.clear()

    def admitted_prepare(plan):
        held.append(plan.id)
        current_preparation().retain_release(release)
        return prepare(plan)

    def after_identity_commit(container, plan):
        assert not held
        start(container, plan)

    def before_commit(_connection):
        commits.append(bool(held))

    engine = sf.kw["bind"].sync_engine
    event.listen(engine, "commit", before_commit)
    provider.prepare, provider.start = admitted_prepare, after_identity_commit
    try:
        await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    finally:
        event.remove(engine, "commit", before_commit)
    assert commits == [False, True, False] and not held

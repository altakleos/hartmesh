"""Company storage is host-owned and never grants ambient human mutation rights."""

import asyncio
import threading

import pytest
from sqlalchemy import select
from test_agent_instances import instances as instances
from test_storage_spaces_startup import startup as startup

from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Permission, SpaceConflict


@pytest.mark.asyncio
async def test_gateway_first_start_creates_company_space_without_human_actor(startup):
    ss, app, config, sf, catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    async with sf() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
        assert link is not None
        row = await session.get(SpaceRow, link.space_id)
        assert (row.name, row.custody_kind, row.mode, row.status, row.generation) == ("Company", "company", "native", "active", 1)
        assert (row.feature_namespace, row.feature_controller, row.feature_metadata_version) == (None, None, None)
        assert not (await session.execute(select(SpaceGrantRow))).first()
        assert not (await session.execute(select(SpaceEventRow))).first()
        backing = await session.get(SpaceBackingRow, row.backing_handle)
        volume = catalog.verify(backing.slot_id)
        assert volume.spec.kind == "company"
        assert sorted(entry.name for entry in volume.data_path.iterdir()) == ["projects", "shared"]


@pytest.mark.asyncio
async def test_company_creation_is_idempotent_after_restart(startup):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    async with sf() as session:
        identifier = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
    await ss.close_storage_spaces(app)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    await asyncio.gather(ss.ensure_company_space(app), ss.ensure_company_space(app))
    async with sf() as session:
        assert (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id == identifier
        assert len((await session.execute(select(SpaceRow))).scalars().all()) == 1
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == 1
        assert not (await session.execute(select(SpaceEventRow))).first()


@pytest.mark.asyncio
async def test_company_grant_is_read_export_only_and_repeated_grant_is_noop(startup):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    await ss.grant_company_space(app, "alice")
    await ss.grant_company_space(app, "alice")
    async with sf() as session:
        identifier = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
        grants = (await session.execute(select(SpaceGrantRow))).scalars().all()
        assert len(grants) == 1
        assert (grants[0].space_id, grants[0].principal_kind, grants[0].subject_id, grants[0].permissions) == (identifier, "human", "alice", int(Permission.READ | Permission.EXPORT))
        assert (await session.get(SpaceRow, identifier)).generation == 2
        events = (await session.execute(select(SpaceEventRow))).scalars().all()
        assert len(events) == 1 and events[0].details["by"] == "host"


@pytest.mark.asyncio
async def test_company_creation_is_not_attempted_while_pool_unavailable(startup, monkeypatch):
    ss, app, config, sf, _catalog, adapter, _agents = startup
    from deerflow.spaces.docker import StorageHostUnqualified

    adapter.side_effect = StorageHostUnqualified("fixture provider cannot qualify")
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    await ss.storage_recheck(app)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.company-space", "company")) is None
        assert not (await session.execute(select(SpaceRow))).first()


@pytest.mark.asyncio
async def test_cancelled_company_verification_drains_worker_before_releasing_lock(startup, monkeypatch):
    ss, app, config, sf, catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    verify = catalog.verify

    def slow(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(10)
            return verify(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(catalog, "verify", slow)
    task = asyncio.create_task(ss.ensure_company_space(app))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        assert app.state.agent_execution.company_lock.locked()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 5)


@pytest.mark.asyncio
async def test_company_timeout_retains_one_owned_task_and_shutdown_drains_it(startup, monkeypatch):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    gate, entered = asyncio.Event(), asyncio.Event()
    calls = []

    async def pending(_app):
        calls.append(True)
        entered.set()
        await gate.wait()

    monkeypatch.setattr(ss, "ensure_company_space", pending)
    assert not await ss._company_step(app, timeout=0.01)
    await entered.wait()
    owned = app.state.agent_execution.company_task
    assert owned is not None and not owned.done()
    assert not await ss._company_step(app, timeout=0)
    assert calls == [True] and app.state.agent_execution.company_task is owned
    close = asyncio.create_task(ss.close_storage_spaces(app))
    try:
        await asyncio.sleep(0.03)
        assert not close.done() and not owned.cancelled()
    finally:
        gate.set()
        await close
    assert owned.done() and not owned.cancelled()


@pytest.mark.asyncio
async def test_company_bootstrap_never_replaces_an_inactive_link(startup):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    async with sf() as session, session.begin():
        identifier = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
        row = await session.get(SpaceRow, identifier)
        row.status = "archived"
    with pytest.raises(SpaceConflict, match="identity is retained"):
        await ss.ensure_company_space(app)
    async with sf() as session:
        assert (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id == identifier
        assert len((await session.execute(select(SpaceRow))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_failed_company_bootstrap_can_retry_after_qualification(startup, monkeypatch):
    ss, app, config, sf, _catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    original = ss.ensure_company_space
    calls = []

    async def retry(_app):
        calls.append(True)
        if len(calls) == 1:
            raise SpaceConflict("fixture transient assignment failure")
        await original(_app)

    monkeypatch.setattr(ss, "ensure_company_space", retry)
    assert not await ss._company_step(app, timeout=5)
    assert app.state.agent_execution.company_task is None
    assert await ss._company_step(app, timeout=5)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_concurrent_first_company_creation_claims_one_slot(startup, monkeypatch):
    ss, app, config, sf, _catalog, _adapter, _agents = startup

    async def defer(_app, *, timeout):
        return False

    monkeypatch.setattr(ss, "_company_step", defer)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.company-space", "company")) is None
    await asyncio.gather(*(ss.ensure_company_space(app) for _ in range(4)))
    async with sf() as session:
        assert len((await session.execute(select(SpaceRow))).scalars().all()) == 1
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == 1
        assert await session.get(StorageFeatureLinkRow, ("hm.company-space", "company")) is not None


@pytest.mark.asyncio
async def test_company_grant_accepts_setup_pending_and_rejects_released_or_unknown(startup):
    from datetime import UTC, datetime

    from deerflow.persistence.user.model import UserRow
    from deerflow.spaces.contract import InvalidPrincipal

    ss, app, config, sf, _catalog, _adapter, _agents = startup
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    async with sf() as session, session.begin():
        (await session.get(UserRow, "alice")).needs_setup = True
        (await session.get(UserRow, "bob")).released_at = datetime.now(UTC)
    await ss.grant_company_space(app, "alice")
    for account in ("bob", "unregistered"):
        with pytest.raises(InvalidPrincipal):
            await ss.grant_company_space(app, account)
    async with sf() as session:
        assert [row.subject_id for row in (await session.execute(select(SpaceGrantRow))).scalars().all()] == ["alice"]

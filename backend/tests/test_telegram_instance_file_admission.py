"""Use-only Telegram file admission over real SQL and confined Space writes."""

import asyncio
import io
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from _storage_spaces_test_support import FixtureCatalog
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select, update
from test_telegram_instance_publication import publication as publication

from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.base import Base
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Custody, MutationMode, Permission, PrincipalRef
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def file_admission(publication, tmp_path, monkeypatch):
    yield await prepare_file_home(publication, tmp_path, monkeypatch)


async def prepare_file_home(e, tmp_path, monkeypatch):
    agents = e.http_app.state.agent_instances
    async with e.db.engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all, tables=[c.__table__ for c in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, SpaceFileOperationRow, SpaceAttachmentRow, SpaceMountRow, SpaceBackupRow, StorageHostReplacementRow)]
        )
    root = tmp_path / "file-admission"
    await asyncio.to_thread(root.mkdir)

    def catalog_for_host():
        catalog = FixtureCatalog(root)
        # Production verified volumes retain inode facts; this fixture's
        # convenience properties stat on access, so capture them off-loop too.
        for key, volume in catalog.verified.items():
            catalog.verified[key] = SimpleNamespace(**vars(volume), root_inode=volume.root_inode, control_inode=volume.control_inode, filesystem=volume.filesystem)
        return catalog

    catalog = await asyncio.to_thread(catalog_for_host)
    registry = SpaceRegistry(e.db.sessions, HostPrincipalResolver(human=agents.directory.human, nonhuman=agents.directory.lookup, nonhuman_in_session=agents.directory.lookup_in_session))
    files = SpaceFiles(registry, catalog)
    actor = PrincipalRef("human", e.link.owner_user_id)
    home = await files.create(actor=actor, name="File admission Home", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
    view = await agents.get(actor=actor, instance_id=e.link.agent_ref, permission=AgentPermission.USE)
    await registry.set_grant(actor=actor, space_id=home.id, expected_generation=home.generation, subject=view.principal, permissions=Permission.READ | Permission.WRITE | Permission.EXPORT, acknowledge_existing_data=True)
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == view.id).values(home_id=home.id))
        await writer.execute(delete(SpaceGrantRow).where(SpaceGrantRow.space_id == home.id, SpaceGrantRow.principal_kind == "human"))
    agents.files = files

    async def quota(**kwargs):
        return {"available_bytes": 8 << 20, "available_inodes": 100}

    monkeypatch.setattr(files, "quota", quota)

    class NoWorkspace:
        def __getattr__(self, name):
            raise AssertionError("Saving a file must never inspect or start a workspace")

    agents.residents = NoWorkspace()
    e.file_service, e.files, e.home, e.actor = agents, files, home, actor
    return e


async def test_use_only_put_is_direct_attributed_and_replayable_without_workspace(file_admission):
    from app.telegram_bots.instance_file_authority import prepare_file_write

    e = file_admission
    access = await prepare_file_write(e.live.authority, e.link, current=lambda: e.active[0])
    operation = uuid4().hex
    with io.BytesIO(b"document") as content:
        result = await access.apply(operation_id=operation, name="report.pdf", content=content, size=8)
    with io.BytesIO(b"document") as content:
        again = await access.apply(operation_id=operation, name="report.pdf", content=content, size=8)
    assert result == again and result.path == "uploads/report.pdf"
    assert await e.files.read(actor=access.instance.principal, space_id=e.home.id, path=result.path, max_bytes=20) == b"document"
    async with e.db.sessions() as reader:
        operation_row = await reader.get(SpaceFileOperationRow, (e.home.id, operation))
        assert operation_row.phase == "complete" and operation_row.actor_kind == "nonhuman"
        assert operation_row.request["on_behalf_of"] == e.link.owner_user_id
    assert e.bus.inbound_queue.empty()


async def test_lost_use_precedes_readiness_and_home_diagnostics(file_admission, monkeypatch):
    from app.gateway import storage_spaces
    from app.telegram_bots.instance_file_authority import prepare_file_write
    from deerflow.agent_instances.contract import AgentDenied

    e = file_admission
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))

    async def forbidden(*args, **kwargs):
        raise AssertionError("No storage fact may precede current Use")

    monkeypatch.setattr(storage_spaces, "require_execution", forbidden)
    monkeypatch.setattr(e.files.registry, "get", forbidden)
    with pytest.raises(AgentDenied):
        await prepare_file_write(e.live.authority, e.link, current=lambda: True)


@pytest.mark.parametrize("change", ["grant", "status", "generation", "home_generation", "policy", "binding", "service", "lifetime"])
async def test_prepared_file_access_rechecks_mutable_authority_in_write_transaction(file_admission, change):
    from app.telegram_bots.instance_file_authority import prepare_file_write
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow

    e = file_admission
    access = await prepare_file_write(e.live.authority, e.link, current=lambda: e.active[0])
    async with e.db.sessions.begin() as writer:
        if change == "grant":
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        elif change == "status":
            await writer.execute(update(AgentInstanceRow).values(status="suspended"))
        elif change == "generation":
            await writer.execute(update(AgentInstanceRow).values(generation=2))
        elif change == "home_generation":
            await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
        elif change == "policy":
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="off", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
        elif change == "binding":
            await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=e.link.binding_generation + 1))
    if change == "service":
        e.http_app.state.agent_instances = SimpleNamespace()
    if change == "lifetime":
        e.active[0] = False
    with io.BytesIO(b"document") as content, pytest.raises((ValueError, PermissionError)):
        await access.apply(operation_id=uuid4().hex, name="report.pdf", content=content, size=8)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(SpaceFileOperationRow)))


async def test_use_withdrawn_after_receipt_preflight_cannot_write_bytes(file_admission, monkeypatch):
    from app.telegram_bots.instance_file_authority import prepare_file_write
    from deerflow.agent_instances.contract import AgentDenied

    e = file_admission
    access = await prepare_file_write(e.live.authority, e.link, current=lambda: True)
    original = e.files.put_unique
    entered = []

    async def put(**kwargs):
        entered.append(True)
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return await original(**kwargs)

    monkeypatch.setattr(e.files, "put_unique", put)
    with io.BytesIO(b"document") as content, pytest.raises(AgentDenied):
        await access.apply(operation_id=uuid4().hex, name="report.pdf", content=content, size=8)
    assert entered
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(SpaceFileOperationRow)))


async def test_file_write_rechecks_execution_without_resident_admission(file_admission, monkeypatch):
    from app.gateway import storage_spaces
    from app.telegram_bots.instance_file_authority import prepare_file_write
    from deerflow.agent_instances.contract import ExecutionUnavailable

    e = file_admission
    access = await prepare_file_write(e.live.authority, e.link, current=lambda: True)

    async def unavailable(*args):
        raise ExecutionUnavailable("operator-hold")

    monkeypatch.setattr(storage_spaces, "require_execution", unavailable)
    with io.BytesIO(b"document") as content, pytest.raises(ExecutionUnavailable):
        await access.apply(operation_id=uuid4().hex, name="report.pdf", content=content, size=8)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(SpaceFileOperationRow)))


async def test_repeated_cancellation_retains_buffer_until_direct_write_settles(file_admission, monkeypatch):
    import threading

    from app.telegram_bots.instance_file_authority import prepare_file_write

    e = file_admission
    access = await prepare_file_write(e.live.authority, e.link, current=lambda: True)
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    original = e.file_service.agent_files._hash

    def hashing(content, size):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10)
        assert not content.closed
        return original(content, size)

    monkeypatch.setattr(e.file_service.agent_files, "_hash", hashing)
    operation = uuid4().hex
    with io.BytesIO(b"document") as content:
        task = asyncio.create_task(access.apply(operation_id=operation, name="report.pdf", content=content, size=8))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not content.closed
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            async with e.db.sessions() as reader:
                row = await reader.get(SpaceFileOperationRow, (e.home.id, operation))
                assert row.phase == "complete"
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

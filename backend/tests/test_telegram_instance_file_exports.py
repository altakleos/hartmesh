"""Direct Space exports keep the recipient's current instance and file grants."""

import asyncio
import threading

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, update
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_publication import publication as publication

from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Permission, SpaceDenied
from deerflow.spaces.filesystem import ConfinedFilesystem, FileTooLarge

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def exports(file_admission, monkeypatch):
    from uuid import uuid4

    e = file_admission
    e.http_app.state.storage_spaces = e.files
    view = await e.file_service.get(actor=e.actor, instance_id=e.link.agent_ref, permission=AgentPermission.USE)
    home = await e.files.registry.get(actor=view.principal, space_id=e.home.id)
    await e.files.mkdir(actor=view.principal, space_id=home.id, expected_generation=home.generation, operation_id=uuid4().hex, path="outputs")
    await e.files.write(actor=view.principal, space_id=home.id, expected_generation=home.generation, operation_id=uuid4().hex, path="outputs/result.txt", content=b"result", create=True)
    # Seed the derived human grant, then exercise the real mandatory Space
    # admission. The instance grant API has separate membership tests.
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == view.id).values(permissions=3))
        writer.add(SpaceGrantRow(space_id=home.id, principal_kind="human", subject_id=e.actor.subject_id, permissions=int(Permission.READ | Permission.EXPORT)))
    e.export_calls, e.native_reads = [], []
    original, read = e.files.export_file, ConfinedFilesystem.read_bytes

    async def exported(**kwargs):
        e.export_calls.append((kwargs["actor"], kwargs["space_id"], kwargs["path"], kwargs["max_bytes"]))
        return await original(**kwargs)

    def reading(filesystem, path, **kwargs):
        e.native_reads.append(path)
        return read(filesystem, path, **kwargs)

    async def no_execution(*args, **kwargs):
        raise AssertionError("File export must not require execution readiness or a workspace")

    monkeypatch.setattr(e.files, "export_file", exported)
    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", reading)
    monkeypatch.setattr("app.gateway.storage_spaces.require_execution", no_execution)
    return e


async def test_exports_as_the_human_without_execution_or_workspace(exports):
    from app.telegram_bots.instance_file_exports import prepare_file_export

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    assert await access.read("/mnt/user-data/outputs/result.txt", max_bytes=20) == b"result"
    assert e.export_calls == [(e.actor, e.home.id, "outputs/result.txt", 20)]
    assert e.native_reads == ["outputs/result.txt"]


@pytest.mark.parametrize("change", ["use_only", "no_use", "no_space_grant", "no_export"])
async def test_missing_instance_or_space_grant_never_reads_bytes(exports, change):
    from app.telegram_bots.instance_file_exports import prepare_file_export

    e = exports
    async with e.db.sessions.begin() as writer:
        if change in {"use_only", "no_use"}:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=1 if change == "use_only" else 2))
        elif change == "no_space_grant":
            await writer.execute(delete(SpaceGrantRow).where(SpaceGrantRow.principal_kind == "human"))
        else:
            await writer.execute(update(SpaceGrantRow).where(SpaceGrantRow.principal_kind == "human").values(permissions=int(Permission.READ)))
    with pytest.raises((AgentDenied, SpaceDenied)):
        access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
        await access.read("/mnt/user-data/outputs/result.txt", max_bytes=20)
    assert not e.native_reads


async def test_actual_confined_read_enforces_export_ceiling(exports):
    from app.telegram_bots.instance_file_exports import prepare_file_export

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    with pytest.raises(FileTooLarge):
        await access.read("/mnt/user-data/outputs/result.txt", max_bytes=3)
    assert e.export_calls == [(e.actor, e.home.id, "outputs/result.txt", 3)]


async def test_inspect_lost_after_volume_validation_prevents_native_read(exports, monkeypatch):
    from app.telegram_bots.instance_file_exports import prepare_file_export

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    volume = e.files._volume

    async def validated(session, row):
        result = await volume(session, row)
        await session.execute(update(AgentInstanceGrantRow).values(permissions=1))
        return result

    monkeypatch.setattr(e.files, "_volume", validated)
    with pytest.raises(AgentDenied):
        await access.read("/mnt/user-data/outputs/result.txt", max_bytes=20)
    assert not e.native_reads


async def test_inspect_lost_after_native_read_discards_result(exports, monkeypatch):
    from app.telegram_bots.instance_file_exports import prepare_file_export
    from deerflow.spaces import service

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    original, snapshots = service.run_file_io, []
    admit = access._admit

    async def observed(writer, rows):
        snapshots.append(writer)
        await admit(writer, rows)

    async def reading(call, *args, **kwargs):
        result = await original(call, *args, **kwargs)
        if call == e.files._io:
            await snapshots[-1].execute(update(AgentInstanceGrantRow).values(permissions=1))
        return result

    monkeypatch.setattr(type(access), "_admit", lambda self, writer, rows: observed(writer, rows))
    monkeypatch.setattr(service, "run_file_io", reading)
    with pytest.raises(AgentDenied):
        await access.read("/mnt/user-data/outputs/result.txt", max_bytes=20)
    assert e.native_reads == ["outputs/result.txt"]


@pytest.mark.parametrize("change", ["inspect", "export", "binding", "disabled", "home_generation"])
async def test_final_authorization_uses_one_current_joined_snapshot(exports, change):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from app.telegram_bots.authority import AccessOff
    from app.telegram_bots.instance_file_exports import prepare_file_export
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    changed = []

    def withdraw(connection, cursor, statement, parameters, context, many):
        if "FROM telegram_bot_links JOIN agent_instances" not in statement or changed:
            return
        changed.append(True)
        if change == "inspect":
            connection.execute(update(AgentInstanceGrantRow).values(permissions=1))
        elif change == "export":
            connection.execute(update(SpaceGrantRow).where(SpaceGrantRow.principal_kind == "human").values(permissions=int(Permission.READ)))
        elif change == "binding":
            connection.execute(update(TelegramBotLinkRow).values(binding_generation=TelegramBotLinkRow.binding_generation + 1))
        elif change == "disabled":
            connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
        else:
            connection.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", withdraw)
    try:
        with pytest.raises((AgentDenied, SpaceDenied, AccessOff)):
            await access.authorize()
        assert changed == [True] and not e.native_reads
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", withdraw)


async def test_repeated_cancellation_drains_native_export(exports, monkeypatch):
    from app.telegram_bots.instance_file_exports import prepare_file_export

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    entered, release = asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), ConfinedFilesystem.read_bytes

    def read(filesystem, *args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10)
        return original(filesystem, *args, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", read)
    task = asyncio.create_task(access.read("/mnt/user-data/outputs/result.txt", max_bytes=20))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert e.native_reads == ["outputs/result.txt"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_repeated_cancellation_retains_authorization_writer(exports, monkeypatch):
    from app.telegram_bots.instance_file_exports import prepare_file_export

    e = exports
    access = await prepare_file_export(e.live.authority, e.link, current=lambda: e.active[0])
    entered, release, completed = asyncio.Event(), asyncio.Event(), []
    original = type(access)._admit

    async def admitted(self, writer, rows):
        await original(self, writer, rows)
        entered.set()
        await release.wait()
        assert writer.in_transaction() and writer.is_active
        completed.append(True)

    monkeypatch.setattr(type(access), "_admit", admitted)
    task = asyncio.create_task(access.authorize())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not completed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert completed == [True] and not e.native_reads
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)

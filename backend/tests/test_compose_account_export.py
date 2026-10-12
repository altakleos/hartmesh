"""A managed account export reads only its qualified User Space and owned rows."""

import asyncio
import hashlib
import io
import json
import threading
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from langgraph.store.memory import InMemoryStore
from test_account_export import _config, _Deployment
from test_compose_native_file_sources import native_request
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app

from app.gateway import account_export
from app.gateway.native_file_sources import NativeFileSources
from app.gateway.routers import account_export as account_export_router
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.projects.documents import shelf_relpath
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.facade import storage_actor_scope


@pytest_asyncio.fixture
async def native_export(feature_app, monkeypatch):
    client, app, files, catalog, paths, alice, bob, current = feature_app
    native_request(app)
    app.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    await app.state.thread_store.create(THREAD, user_id=str(alice.id), display_name="Owned conversation")
    monkeypatch.setattr(account_export.transcript, "read_conversation", AsyncMock(return_value=None))
    monkeypatch.setattr(account_export, "preferences_export_document", AsyncMock(return_value={"notification_enabled": True}))
    monkeypatch.setattr(account_export, "memory_export_document", AsyncMock(return_value=None))
    monkeypatch.setattr(account_export, "owned_agent_documents", lambda _user_id: None)
    service = account_export.AccountExportService(app, paths=paths, config=_config)
    app.state.account_export = service
    app.include_router(account_export_router.router)
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
    try:
        yield SimpleNamespace(client=client, app=app, files=files, catalog=catalog, paths=paths, alice=alice, bob=bob, current=current, service=service, sources=sources)
    finally:
        await service.close()


async def export_bytes(fixture):
    response = await fixture.client.post("/api/account/export")
    assert response.status_code == 202, response.text
    job = fixture.service.status(str(fixture.alice.id))
    await asyncio.wait_for(job.task, 30)
    assert job.state == "ready", job.document()
    response = await fixture.client.get("/api/account/export/parts/1")
    assert response.status_code == 200, response.text
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}, job


@pytest.mark.asyncio
async def test_native_account_export_reads_chat_and_my_files_without_a_legacy_user_root(native_export):
    fixture = native_export
    root = fixture.sources.user.data_path
    output = root / f"conversations/{THREAD}/user-data/outputs/report.txt"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"owned output")
    (root / "files/notes.txt").write_bytes(b"owned notes")
    orphan = root / "conversations/orphan/user-data/outputs/private.txt"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"orphan bytes")
    frames = output.parent / ".browser-frames/private.jpg"
    frames.parent.mkdir()
    frames.write_bytes(b"process bytes")
    (fixture.sources.user.control_path / "private-stage").write_bytes(b"control bytes")
    assert not fixture.paths.user_dir(str(fixture.alice.id)).exists()
    entries, _job = await export_bytes(fixture)
    assert entries[f"conversations/{THREAD}/files/outputs/report.txt"] == b"owned output"
    assert entries["my-files/notes.txt"] == b"owned notes"
    assert json.loads(entries["preferences.json"]) == {"notification_enabled": True}
    assert not any(name.startswith("conversations/orphan/") or "browser-frames" in name or "private-stage" in name for name in entries)
    assert not fixture.paths.user_dir(str(fixture.alice.id)).exists()


@pytest.mark.asyncio
async def test_native_account_export_includes_only_live_row_referenced_project_files(native_export):
    fixture = native_export
    project = await fixture.app.state.project_repo.create(name="Shelf", user_id=str(fixture.alice.id))
    data, document_id = b"immutable original", uuid4().hex
    digest = hashlib.sha256(data).hexdigest()
    relative = shelf_relpath(project["id"], digest, document_id)
    document = await fixture.app.state.project_document_repo.insert_active(project["id"], document_id=document_id, name="source.txt", relpath=relative, sha256=digest, size_bytes=len(data), user_id=str(fixture.alice.id))
    assert document is not None
    namespace = fixture.sources.user.data_path / "projects" / relative
    original = namespace / "original/source.txt"
    original.parent.mkdir(parents=True)
    original.write_bytes(data)
    derived = namespace / "derived/converted.md"
    derived.parent.mkdir()
    derived.write_bytes(b"# Derived")
    (namespace / "original/orphan.txt").write_bytes(b"not a shelf row")
    entries, _job = await export_bytes(fixture)
    prefix = f"projects/{project['id']}/documents/{document_id}"
    assert entries[f"{prefix}/original/source.txt"] == data
    assert entries[f"{prefix}/derived/converted.md"] == b"# Derived"
    assert not any("orphan.txt" in name for name in entries)


@pytest.mark.asyncio
async def test_native_account_export_download_refuses_after_storage_mode_changes(native_export):
    fixture = native_export
    _entries, job = await export_bytes(fixture)
    fixture.app.state.storage_spaces_enabled = False
    response = await fixture.client.get("/api/account/export/parts/1")
    assert response.status_code == 503, response.text
    assert job.streams == 0


@pytest.mark.asyncio
async def test_discarded_export_builder_is_drained_before_service_shutdown(tmp_path, monkeypatch):
    deployment = _Deployment(tmp_path, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    original = deployment.service._plan

    def held_plan(*args):
        entered.set()
        release.wait(5)
        return original(*args)

    monkeypatch.setattr(deployment.service, "_plan", held_plan)
    from test_account_export import PERSON_A

    job = deployment.service.start(PERSON_A)
    await asyncio.wait_for(asyncio.to_thread(entered.wait), 2)
    deployment.service.discard(str(PERSON_A.id))
    closing = asyncio.create_task(deployment.service.close())
    try:
        await asyncio.sleep(0.05)
        assert not closing.done(), "Shutdown abandoned a discarded export worker"
        job.task.cancel()  # Repeated cancellation must still retain worker ownership.
        await asyncio.sleep(0.05)
        assert not closing.done()
    finally:
        release.set()
        await asyncio.wait_for(closing, 5)
        await asyncio.gather(job.task, return_exceptions=True)
    assert deployment.left() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["symlink", "unreadable"])
async def test_native_export_skips_only_a_changed_source_and_keeps_healthy_files(native_export, tmp_path, monkeypatch, damage):
    from app.gateway.native_account_export import NativeAccountFiles
    from deerflow.spaces.filesystem import ConfinedFilesystem

    fixture = native_export
    root = fixture.sources.user.data_path / "files"
    bad = root / "bad.txt"
    bad.write_bytes(b"planned bytes")
    (root / "good.txt").write_bytes(b"healthy")
    original = NativeAccountFiles.plan
    open_regular = ConfinedFilesystem.open_regular
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"must not be exported")

    def change_after_plan(self, *args):
        plan = original(self, *args)
        if damage == "symlink":
            bad.unlink()
            bad.symlink_to(outside)
        return plan

    def unreadable(self, relative, **kwargs):
        if damage == "unreadable" and relative == "files/bad.txt":
            raise PermissionError("The planned source became unreadable")
        return open_regular(self, relative, **kwargs)

    monkeypatch.setattr(NativeAccountFiles, "plan", change_after_plan)
    monkeypatch.setattr(ConfinedFilesystem, "open_regular", unreadable)
    entries, _job = await export_bytes(fixture)
    assert entries["my-files/good.txt"] == b"healthy"
    assert "my-files/bad.txt" not in entries
    skipped = {item["path"]: item["reason"] for item in json.loads(entries["manifest.json"])["skipped"]}
    assert skipped["my-files/bad.txt"] == ("changed" if damage == "symlink" else "unreadable")


@pytest.mark.asyncio
async def test_native_export_directory_iteration_failure_keeps_other_sources(native_export, monkeypatch):
    import errno
    import os

    fixture = native_export
    output = fixture.sources.user.data_path / f"conversations/{THREAD}/user-data/outputs"
    output.mkdir(parents=True)
    (output / "unreadable.txt").write_bytes(b"not scanned")
    (fixture.sources.user.data_path / "files/good.txt").write_bytes(b"healthy")
    inode, original = output.stat().st_ino, os.scandir

    class BrokenScan:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def __iter__(self):
            raise OSError(errno.EIO, "Directory iteration failed")

    def scan(path):
        if isinstance(path, int) and os.fstat(path).st_ino == inode:
            return BrokenScan()
        return original(path)

    monkeypatch.setattr(os, "scandir", scan)
    entries, _job = await export_bytes(fixture)
    assert entries["my-files/good.txt"] == b"healthy"
    skipped = {item["path"]: item["reason"] for item in json.loads(entries["manifest.json"])["skipped"]}
    assert skipped[f"conversations/{THREAD}/files/outputs"] == "unreadable"


@pytest.mark.asyncio
async def test_native_export_revocation_during_preparation_never_becomes_ready(native_export, monkeypatch):
    from app.gateway.native_account_export import NativeAccountFiles
    from deerflow.spaces.contract import Permission

    fixture = native_export
    (fixture.sources.user.data_path / "files/private.txt").write_bytes(b"private")
    original = fixture.service._off_loop

    async def revoke_after_plan(job, function, *args):
        result = await original(job, function, *args)
        if getattr(function, "__func__", None) is NativeAccountFiles.plan:
            await fixture.files.registry.host_grant(fixture.sources.space_id, fixture.sources.actor, Permission.READ)
        return result

    monkeypatch.setattr(fixture.service, "_off_loop", revoke_after_plan)
    response = await fixture.client.post("/api/account/export")
    assert response.status_code == 202, response.text
    job = fixture.service.status(str(fixture.alice.id))
    await asyncio.wait_for(job.task, 30)
    await fixture.service.settled()
    assert job.state == "failed" and job.parts == []
    assert not job.directory.exists()


@pytest.mark.asyncio
async def test_discarded_native_download_refuses_before_headers_and_closes_once(native_export):
    from fastapi import HTTPException

    fixture = native_export
    _entries, job = await export_bytes(fixture)
    request = SimpleNamespace(app=fixture.app, state=SimpleNamespace(user=fixture.alice))
    response = await account_export_router.download_part(1, request)
    assert job.streams == 1
    fixture.service.discard(str(fixture.alice.id))
    sent = []

    async def send(message):
        sent.append(message)

    with pytest.raises(HTTPException) as refused:
        await response({"type": "http", "asgi": {"spec_version": "2.4"}, "method": "GET", "headers": []}, AsyncMock(), send)
    assert refused.value.status_code == 503
    assert sent == [] and job.streams == 0


@pytest.mark.asyncio
async def test_native_export_keeps_independent_skills_and_ignores_legacy_chat_and_files(native_export):
    fixture = native_export
    user_id = str(fixture.alice.id)
    legacy = fixture.paths.user_files_dir(user_id) / "legacy.txt"
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"must not be exported")
    skill = fixture.paths.user_custom_skills_dir(user_id) / "owned/SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_bytes(b"# Owned skill")
    (fixture.sources.user.data_path / "files/native.txt").write_bytes(b"native")
    entries, _job = await export_bytes(fixture)
    assert entries["my-skills/owned/SKILL.md"] == b"# Owned skill"
    assert entries["my-files/native.txt"] == b"native"
    assert "my-files/legacy.txt" not in entries


@pytest.mark.asyncio
async def test_native_export_without_user_role_refuses_before_creating_a_job(native_export):
    from sqlalchemy import delete

    from deerflow.features.resources import StorageFeatureLinkRow

    fixture = native_export
    async with fixture.files.registry._sf() as session, session.begin():
        await session.execute(delete(StorageFeatureLinkRow).where(StorageFeatureLinkRow.namespace == "hm.user-space", StorageFeatureLinkRow.key == "human:" + str(fixture.alice.id)))
    response = await fixture.client.post("/api/account/export")
    assert response.status_code == 503, response.text
    assert fixture.service.status(str(fixture.alice.id)) is None
    assert not fixture.paths.exports_dir().exists()


@pytest.mark.asyncio
async def test_cancelled_download_acquisition_drains_and_closes_the_owned_handle(native_export, monkeypatch):
    from pathlib import Path

    fixture = native_export
    _entries, job = await export_bytes(fixture)
    entered, release = threading.Event(), threading.Event()
    handles, original = [], Path.open

    def held_open(path, *args, **kwargs):
        handle = original(path, *args, **kwargs)
        if path == job.parts[0] and args == ("rb",):
            handles.append(handle)
            entered.set()
            release.wait(5)
        return handle

    monkeypatch.setattr(Path, "open", held_open)
    request = SimpleNamespace(app=fixture.app, state=SimpleNamespace(user=fixture.alice))
    task = asyncio.create_task(account_export_router.download_part(1, request))
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert len(handles) == 1 and handles[0].closed
    assert job.streams == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["root-replaced", "control-permissions"])
async def test_native_export_backing_failure_is_fatal_instead_of_a_file_skip(native_export, monkeypatch, damage):
    from app.gateway.native_account_export import NativeAccountFiles

    fixture = native_export
    root, control = fixture.sources.user.data_path, fixture.sources.user.control_path
    (root / "files/private.txt").write_bytes(b"private")
    original = NativeAccountFiles.plan

    def damage_after_plan(self, *args):
        plan = original(self, *args)
        if damage == "root-replaced":
            root.rename(root.with_name("retired-data"))
            root.mkdir()
            (root / "files").mkdir()
            (root / "files/private.txt").write_bytes(b"foreign")
        else:
            control.chmod(0o755)
        return plan

    monkeypatch.setattr(NativeAccountFiles, "plan", damage_after_plan)
    try:
        response = await fixture.client.post("/api/account/export")
        assert response.status_code == 202, response.text
        job = fixture.service.status(str(fixture.alice.id))
        await asyncio.wait_for(job.task, 30)
        await fixture.service.settled()
        assert job.state == "failed" and job.parts == []
        assert not job.directory.exists()
    finally:
        control.chmod(0o700)


@pytest.mark.asyncio
async def test_discarded_native_download_stops_after_the_inflight_chunk(native_export, monkeypatch):
    fixture = native_export
    _entries, job = await export_bytes(fixture)
    monkeypatch.setattr(account_export_router, "_CHUNK_BYTES", 64)
    request = SimpleNamespace(app=fixture.app, state=SimpleNamespace(user=fixture.alice))
    response = await account_export_router.download_part(1, request)
    bodies = []

    async def send(message):
        if message["type"] == "http.response.body" and message.get("body"):
            bodies.append(message["body"])
            fixture.service.discard(str(fixture.alice.id))

    await response({"type": "http", "asgi": {"spec_version": "2.4"}, "method": "GET", "headers": []}, AsyncMock(), send)
    assert len(bodies) == 1
    assert job.streams == 0


@pytest.mark.asyncio
async def test_capacity_workers_use_a_loop_owned_reservation_snapshot_during_start_and_discard(tmp_path, monkeypatch):
    from test_account_export import PERSON_A, PERSON_B

    deployment = _Deployment(tmp_path, monkeypatch)
    deployment.own_file(PERSON_A, "notes.txt", b"owned")
    loop_thread = threading.get_ident()
    entered, release = threading.Event(), threading.Event()
    reserve, disk_usage = deployment.service._others_to_write, account_export.shutil.disk_usage

    def loop_owned(job):
        assert threading.get_ident() == loop_thread, "A capacity worker accessed mutable jobs"
        return reserve(job)

    def held_usage(path):
        assert threading.get_ident() != loop_thread
        if not entered.is_set():
            entered.set()
            release.wait(5)
        return disk_usage(path)

    monkeypatch.setattr(deployment.service, "_others_to_write", loop_owned)
    monkeypatch.setattr(account_export.shutil, "disk_usage", held_usage)
    job = deployment.service.start(PERSON_A)
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
        deployment.service.start(PERSON_B)
        deployment.service.discard(str(PERSON_B.id))
        release.set()
        await asyncio.wait_for(job.task, 10)
        assert job.state == "ready", job.document()
    finally:
        release.set()
        await deployment.service.close()

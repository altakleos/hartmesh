"""Conversation file effects use admitted volumes and never legacy paths."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.gateway import chat_storage
from deerflow.config.paths import Paths
from deerflow.spaces.chat_paths import ChatFilesFull, ChatStoragePaths


def test_mountless_presentation_refuses_stale_output_state_without_host_file_probes(tmp_path, monkeypatch):
    import importlib
    from pathlib import Path

    from deerflow.config.paths import paths_scope

    presentation = importlib.import_module("deerflow.tools.builtins.present_file_tool")
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path / "legacy-output")}}, context={"thread_id": "chat-a", "user_id": "alice"}, config={})

    def reject_probe(*args, **kwargs):
        pytest.fail("Mountless presentation probed host files from stale state")

    monkeypatch.setattr(Path, "resolve", reject_probe)
    with paths_scope(paths):
        result = presentation.present_file_tool.func(runtime=runtime, filepaths=["/mnt/user-data/outputs/result.txt"], tool_call_id="probe")
    assert "artifacts" not in result.update
    assert "no personal files" in result.update["messages"][0].content


@pytest.mark.parametrize("operation", ["list", "delete", "edit"])
def test_native_file_routes_use_admitted_descriptors(tmp_path, monkeypatch, operation):
    import hashlib
    from contextlib import asynccontextmanager

    from fastapi.testclient import TestClient
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.routers import artifacts, uploads

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    parent = paths.sandbox_outputs_dir("chat-a", user_id="alice") if operation == "edit" else paths.sandbox_uploads_dir("chat-a", user_id="alice")
    (parent / "report.txt").write_bytes(b"original")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))

    def reject_legacy(*args, **kwargs):
        pytest.fail("Native file routes must not use path-based legacy effects")

    monkeypatch.setattr(uploads, "_list_uploaded_files_for_thread", reject_legacy)
    monkeypatch.setattr(uploads, "_delete_uploaded_file_for_thread", reject_legacy)
    monkeypatch.setattr(artifacts, "resolve_outputs_confined_path", reject_legacy)

    @asynccontextmanager
    async def reservation(*args, **kwargs):
        yield

    monkeypatch.setattr(artifacts, "reserve_artifact_write", reservation)
    app = make_chat_test_app()
    app.include_router(uploads.router)
    app.include_router(artifacts.router)
    client = TestClient(app)
    if operation == "list":
        response = client.get("/api/threads/chat-a/uploads/list")
        assert response.status_code == 200
        assert [entry["filename"] for entry in response.json()["files"]] == ["report.txt"]
        assert response.json()["files"][0]["size"] == 8
    elif operation == "delete":
        response = client.delete("/api/threads/chat-a/uploads/report.txt")
        assert response.status_code == 200
        assert not (parent / "report.txt").exists()
    else:
        response = client.put("/api/threads/chat-a/artifacts/mnt/user-data/outputs/report.txt", json={"content": "updated", "expected_sha256": hashlib.sha256(b"original").hexdigest()})
        assert response.status_code == 200
        assert (parent / "report.txt").read_bytes() == b"updated"
    assert list(volume.control_path.iterdir()) == []
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["ENOSPC", "EDQUOT", "revoked", "changed"])
async def test_native_edit_fault_preserves_old_or_concurrently_changed_artifact(tmp_path, monkeypatch, failure):
    import errno
    import hashlib

    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import ChatFileAccess, QualifiedUploadStage

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    artifact = paths.sandbox_outputs_dir("chat-a", user_id="alice") / "report.txt"
    artifact.write_bytes(b"original")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    revoked = False

    async def prepare(*args):
        if revoked:
            raise HTTPException(403, "Grant revoked before edit publication")
        return prepared

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepare)
    original_write = QualifiedUploadStage.write

    def write(stage, content):
        nonlocal revoked
        if failure in ("ENOSPC", "EDQUOT"):
            raise OSError(getattr(errno, failure), "Full during staging")
        original_write(stage, content)
        if failure == "revoked":
            revoked = True
        else:
            artifact.write_bytes(b"concurrent edit")

    monkeypatch.setattr(QualifiedUploadStage, "write", write)
    access = ChatFileAccess(prepared)
    with pytest.raises(ChatFilesFull if failure in ("ENOSPC", "EDQUOT") else HTTPException) as caught:
        await access.edit_artifact(SimpleNamespace(), "mnt/user-data/outputs/report.txt", b"updated", hashlib.sha256(b"original").hexdigest(), maximum_bytes=1024)
    assert (caught.value.status if isinstance(caught.value, ChatFilesFull) else caught.value.status_code) == (507 if failure in ("ENOSPC", "EDQUOT") else 403 if failure == "revoked" else 412)
    assert artifact.read_bytes() == (b"concurrent edit" if failure == "changed" else b"original")
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["aclose", "cleanup_written"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_upload_settlement_drains_every_stage_after_cancellation_or_error(monkeypatch, method, cancel):
    import asyncio
    import threading

    from app.gateway.chat_upload_ingestion import ChatUploadSession

    entered, finish = threading.Event(), threading.Event()
    settled = []

    def held():
        entered.set()
        assert finish.wait(5)
        settled.append("first")
        if not cancel:
            raise OSError("First settlement failed")

    def remaining():
        settled.append("remaining")

    session = ChatUploadSession(SimpleNamespace(_thread_id="chat-a"), None)
    session.finalized = True
    session.stages = [SimpleNamespace(close=remaining, rollback_published=remaining), SimpleNamespace(close=held, rollback_published=held)]
    task = asyncio.create_task(getattr(session, method)())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        if cancel:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError if cancel else OSError):
            await task
        assert settled == ["first", "remaining"]
    finally:
        finish.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("failure", [None, "ENOSPC", "EDQUOT"])
def test_upload_route_uses_private_quota_staging_and_cleans_rejected_batch(tmp_path, monkeypatch, failure):
    import errno

    from fastapi.testclient import TestClient
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import QualifiedUploadStage
    from app.gateway.deps import get_config
    from app.gateway.routers import uploads

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))

    def reject_legacy(*args, **kwargs):
        pytest.fail("Native uploads must never use the legacy directory or allocate a sandbox")

    monkeypatch.setattr(uploads, "ensure_uploads_dir", reject_legacy)
    monkeypatch.setattr(uploads, "get_sandbox_provider", reject_legacy)
    monkeypatch.setattr(uploads, "get_effective_user_id", lambda: "alice")
    original_write = QualifiedUploadStage.write

    def write(stage, content):
        assert list(paths.sandbox_uploads_dir("chat-a", user_id="alice").glob("second*")) == []
        if failure and content == b"second bytes":
            raise OSError(getattr(errno, failure), "Quota exhausted while streaming")
        original_write(stage, content)

    monkeypatch.setattr(QualifiedUploadStage, "write", write)
    app = make_chat_test_app()
    app.dependency_overrides[get_config] = lambda: SimpleNamespace(uploads={})
    app.include_router(uploads.router)
    response = TestClient(app).post("/api/threads/chat-a/uploads", files=[("files", ("first.txt", b"first bytes")), ("files", ("second.txt", b"second bytes"))])
    assert response.status_code == (507 if failure else 200)
    parent = paths.sandbox_uploads_dir("chat-a", user_id="alice")
    if failure:
        assert response.json()["reason"] == "files-full"
        assert list(parent.iterdir()) == []
    else:
        assert (parent / "first.txt").read_bytes() == b"first bytes"
        assert (parent / "second.txt").read_bytes() == b"second bytes"
        assert len(response.json()["files"]) == 2
    assert list(volume.control_path.iterdir()) == []
    assert not (tmp_path / "legacy/users").exists()
    assert {child.name for child in (tmp_path / "legacy").iterdir()} == {"chat-runtime"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "companion-full", "revoked"])
async def test_native_document_conversion_uses_private_quota_source_before_publication(tmp_path, monkeypatch, failure):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import QualifiedUploadStage
    from app.gateway.upload_ingestion import ThreadUploadIngestionService
    from deerflow.spaces.chat_paths import ChatFilesFull
    from deerflow.utils import file_conversion

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    revoked = False

    async def prepare(*args):
        if revoked:
            raise HTTPException(403, "Current grant was revoked")
        return prepared

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepare)
    parent = paths.sandbox_uploads_dir("chat-a", user_id="alice")

    def convert(source, mode):
        nonlocal revoked
        assert source.suffix == ".pdf"
        assert source.read_bytes() == b"original"
        assert list(parent.iterdir()) == []
        if failure == "revoked":
            revoked = True
        return "converted markdown"

    monkeypatch.setattr(file_conversion, "_do_convert", convert)
    check_capacity = QualifiedUploadStage.check_capacity

    def capacity(stage, size, **kwargs):
        if failure == "companion-full" and size == len(b"converted markdown"):
            raise ChatFilesFull()
        check_capacity(stage, size, **kwargs)

    monkeypatch.setattr(QualifiedUploadStage, "check_capacity", capacity)
    service = ThreadUploadIngestionService(request=SimpleNamespace(), thread_id="chat-a", user_id="alice", app_config=SimpleNamespace(uploads={"auto_convert_documents": True}))

    async def chunks():
        yield b"original"

    try:
        await service.open()
        if failure:
            with pytest.raises(ChatFilesFull if failure == "companion-full" else HTTPException) as caught:
                await service.ingest_chunks(chunks(), display_name="input.pdf", expected_size=8)
            assert (caught.value.status if isinstance(caught.value, ChatFilesFull) else caught.value.status_code) == (507 if failure == "companion-full" else 403)
            await service.cleanup_written()
            assert list(parent.iterdir()) == []
        else:
            result = await service.ingest_chunks(chunks(), display_name="input.pdf", expected_size=8)
            await service.finalize()
            assert result["markdown_file"] == "input.md"
            assert (parent / "input.md").read_text(encoding="utf-8") == "converted markdown"
            assert (parent / "input.pdf").read_bytes() == b"original"
    finally:
        await service.aclose()
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("finalize", [False, True])
async def test_shared_native_ingestion_rolls_back_until_final_admission_succeeds(tmp_path, monkeypatch, finalize):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.upload_ingestion import ThreadUploadIngestionService

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    revoked = False

    async def prepare(*args):
        if revoked:
            raise HTTPException(403, "Grant revoked before final settlement")
        return prepared

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepare)
    service = ThreadUploadIngestionService(request=SimpleNamespace(), thread_id="chat-a", user_id="alice", app_config=SimpleNamespace())

    async def chunks():
        yield b"attached bytes"

    try:
        await service.open()
        await service.ingest_chunks(chunks(), display_name="attached.txt")
        if finalize:
            revoked = True
            with pytest.raises(HTTPException):
                await service.finalize()
    finally:
        await service.aclose()
    assert list(paths.sandbox_uploads_dir("chat-a", user_id="alice").iterdir()) == []
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.parametrize("archive", [False, True])
@pytest.mark.parametrize("available", [False, True])
def test_artifact_route_uses_qualified_files_without_legacy_resolution(tmp_path, monkeypatch, archive, available):
    import zipfile

    from fastapi.testclient import TestClient
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.routers import artifacts

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume if available else None)
    path = "mnt/user-data/outputs/result.skill/SKILL.md" if archive else "mnt/user-data/outputs/result.txt"
    if available:
        paths.ensure_thread_dirs("chat-a", user_id="alice")
        output = paths.sandbox_outputs_dir("chat-a", user_id="alice")
        if archive:
            with zipfile.ZipFile(output / "result.skill", "w") as target:
                target.writestr("SKILL.md", "abcdef")
        else:
            (output / "result.txt").write_bytes(b"abcdef")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"), None if available else "space-not-assigned")
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))

    def reject_legacy(*args, **kwargs):
        pytest.fail("A qualified artifact must never use legacy resolution")

    monkeypatch.setattr(artifacts, "resolve_thread_virtual_path", reject_legacy)
    app = make_chat_test_app()
    app.include_router(artifacts.router)
    response = TestClient(app).get(f"/api/threads/chat-a/artifacts/{path}", headers={"range": "bytes=1-3"})
    assert response.status_code == (206 if available else 503)
    if available:
        assert response.content == b"bcd"
        assert response.headers["cache-control"] == "private, no-store"
    assert not (tmp_path / "legacy").exists()


def test_upload_stage_creation_collision_preserves_existing_control_file(tmp_path, monkeypatch):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_files

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    existing = volume.control_path / "chat-upload-collision"
    existing.write_bytes(b"another upload")
    monkeypatch.setattr(chat_files.uuid, "uuid4", lambda: SimpleNamespace(hex="collision"))
    with pytest.raises(FileExistsError):
        chat_files.QualifiedUploadStage(volume, "conversations/chat-a/user-data/uploads")
    assert existing.read_bytes() == b"another upload"


def test_upload_stage_tracks_link_even_when_directory_flush_fails(tmp_path, monkeypatch):
    import errno
    import os

    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_files

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    stage = chat_files.QualifiedUploadStage(volume, "conversations/chat-a/user-data/uploads")
    parent = paths.sandbox_uploads_dir("chat-a", user_id="alice")
    original_fsync = os.fsync

    def fail_parent_flush(fd):
        if fd == stage.parent_fd:
            raise OSError(errno.ENOSPC, "Full during publication")
        original_fsync(fd)

    try:
        stage.write(b"uploaded")
        monkeypatch.setattr(chat_files.os, "fsync", fail_parent_flush)
        with pytest.raises(OSError):
            stage.publish("report.txt")
        stage.rollback_published()
        assert not (parent / "report.txt").exists()
    finally:
        stage.close()
    assert list(volume.control_path.iterdir()) == []


def test_upload_rollback_never_unlinks_a_replacement_after_inspecting_its_own_inode(tmp_path, monkeypatch):
    import os

    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_files

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    parent = paths.sandbox_uploads_dir("chat-a", user_id="alice")
    stage = chat_files.QualifiedUploadStage(volume, "conversations/chat-a/user-data/uploads")
    original_stat = os.stat
    try:
        stage.write(b"request bytes")
        stage.publish("report.txt")
        expected_inode = os.fstat(stage.fd).st_ino
        changed = False

        def replace_after_observation(*args, **kwargs):
            nonlocal changed
            result = original_stat(*args, **kwargs)
            if not changed and result.st_ino == expected_inode:
                changed = True
                (parent / "report.txt").unlink(missing_ok=True)
                (parent / "report.txt").write_bytes(b"concurrent replacement")
            return result

        monkeypatch.setattr(chat_files.os, "stat", replace_after_observation)
        stage.rollback_published()
        assert changed
        assert (parent / "report.txt").read_bytes() == b"concurrent replacement"
    finally:
        stage.close()
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,status", [("credential-unsupported", 403), ("space-not-assigned", 503), ("space-unverified", 503)])
async def test_file_access_refuses_mountless_chat_before_touching_legacy(tmp_path, monkeypatch, reason, status):
    from app.gateway.chat_files import prepare_chat_files

    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"), reason)
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    with pytest.raises(HTTPException) as caught:
        await prepare_chat_files(SimpleNamespace(), "chat-a")
    assert caught.value.status_code == status
    assert caught.value.detail["reason"] == reason
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
async def test_file_access_maps_virtual_paths_to_qualified_conversation_and_files(tmp_path, monkeypatch):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import prepare_chat_files

    user, company = pinned_chat_volume(tmp_path / "user"), pinned_chat_volume(tmp_path / "company")
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=user, company=company)
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    access = await prepare_chat_files(SimpleNamespace(), "chat-a")
    assert access.source("mnt/user-data/outputs/report.txt") == (user, "conversations/chat-a/user-data/outputs/report.txt")
    assert access.source("/mnt/user-data/files/notes.txt") == (user, "files/notes.txt")
    assert access.source("/mnt/user-data/shared/statement.pdf") == (company, "shared/statement.pdf")
    assert access.source("/mnt/acp-workspace/work.txt") == (user, "conversations/chat-a/acp-workspace/work.txt")
    for invalid in ("/etc/passwd", "mnt/user-data/outputs/../../files/secret", "mnt/user-data/projects/secret", "mnt/user-data/outputs//report.txt"):
        with pytest.raises(HTTPException):
            access.source(invalid)
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
async def test_file_refresh_rejects_role_generation_change_on_same_volume(tmp_path, monkeypatch):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import ChatFileAccess

    volume = pinned_chat_volume(tmp_path)
    base = Paths(tmp_path / "legacy")
    before = ChatStoragePaths(base, user_id="alice", user=volume, user_reference=("space-id", 1))
    after = ChatStoragePaths(base, user_id="alice", user=volume, user_reference=("space-id", 2))
    access = ChatFileAccess(chat_storage.PreparedChatStorage(before, before.mounts("chat-a", user_id="alice")))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=chat_storage.PreparedChatStorage(after, after.mounts("chat-a", user_id="alice"))))
    with pytest.raises(HTTPException) as caught:
        await access.refresh(SimpleNamespace())
    assert caught.value.status_code == 503


@pytest.mark.parametrize("redirect", [False, True])
def test_qualified_upload_stages_privately_and_never_publishes_through_redirected_parent(tmp_path, redirect):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import QualifiedUploadStage
    from deerflow.spaces.filesystem import FilesystemUnavailable, UnsafeSpacePath

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    relative = "conversations/chat-a/user-data/uploads"
    parent = volume.data_path / relative
    stage = QualifiedUploadStage(volume, relative)
    try:
        stage.write(b"uploaded bytes")
        assert list(parent.iterdir()) == []
        assert len(list(volume.control_path.iterdir())) == 1
        if redirect:
            outside = tmp_path / "outside"
            outside.mkdir()
            parent.rename(parent.with_name("old-uploads"))
            parent.symlink_to(outside, target_is_directory=True)
            with pytest.raises((FilesystemUnavailable, UnsafeSpacePath)):
                stage.publish("report.txt")
            assert list(outside.iterdir()) == []
        else:
            stage.publish("report.txt")
            assert (parent / "report.txt").read_bytes() == b"uploaded bytes"
            with pytest.raises(FileExistsError):
                stage.publish("report.txt")
    finally:
        stage.close()
    assert list(volume.control_path.iterdir()) == []
    assert not (tmp_path / "legacy").exists()


@pytest.mark.parametrize("revoked", [False, True])
@pytest.mark.parametrize("redirect", [None, "file", "directory"])
def test_artifact_descriptor_refreshes_after_open_before_first_bytes(tmp_path, monkeypatch, revoked, redirect):
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.chat_files import ChatFileAccess, ChatFileResponse

    user = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=user)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    (paths.sandbox_outputs_dir("chat-a", user_id="alice") / "result.txt").write_text("abcdef", encoding="utf-8")
    if redirect is not None:
        files = user.data_path / "files"
        files.mkdir()
        (files / "result.txt").write_text("secret", encoding="utf-8")
        outputs = paths.sandbox_outputs_dir("chat-a", user_id="alice")
        if redirect == "file":
            (outputs / "result.txt").unlink()
            (outputs / "result.txt").symlink_to("../../../../files/result.txt")
        else:
            outputs.rename(outputs.with_name("old-outputs"))
            outputs.symlink_to("../../../files", target_is_directory=True)
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    calls = []

    async def refresh(*args):
        calls.append("admitted")
        if revoked and len(calls) == 2:
            raise HTTPException(403, "Grant was revoked while opening the file")
        return prepared

    monkeypatch.setattr(chat_storage, "prepare_chat_request", refresh)
    app = FastAPI()

    @app.get("/probe")
    async def probe(request: Request):
        return ChatFileResponse(request, ChatFileAccess(prepared), "mnt/user-data/outputs/result.txt")

    response = TestClient(app).get("/probe", headers={"range": "bytes=1-3"})
    assert response.status_code == (400 if redirect else 403 if revoked else 206)
    assert calls == (["admitted"] if redirect else ["admitted", "admitted"])
    if not revoked and redirect is None:
        assert response.content == b"bcd"
        assert response.headers["cache-control"] == "private, no-store"
    assert not (tmp_path / "legacy").exists()


def make_chat_test_app():
    from _router_auth_helpers import make_authed_test_app

    from app.gateway.refusals import refusal_handler
    from deerflow.agent_instances.contract import Refusal

    app = make_authed_test_app()
    app.add_exception_handler(Refusal, refusal_handler)
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", [False, True])
async def test_many_chunk_upload_qualifies_by_phase_and_refuses_publication_after_revocation(tmp_path, monkeypatch, revoked):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway.upload_ingestion import ThreadUploadIngestionService

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    denied = False
    qualifications = 0

    async def prepare(*args):
        nonlocal qualifications
        qualifications += 1
        if denied:
            raise HTTPException(403, "Revoked during private streaming")
        return prepared

    async def chunks():
        nonlocal denied
        for index in range(100):
            if revoked and index == 50:
                denied = True
            yield b"x" * 8

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepare)
    service = ThreadUploadIngestionService(request=SimpleNamespace(), thread_id="chat-a", user_id="alice", app_config=SimpleNamespace())
    await service.open()
    try:
        if revoked:
            with pytest.raises(HTTPException) as caught:
                await service.ingest_chunks(chunks(), display_name="stream.txt")
            assert caught.value.status_code == 403
        else:
            await service.ingest_chunks(chunks(), display_name="stream.txt")
            await service.finalize()
    finally:
        await service.aclose()
    assert qualifications <= 12, "Storage qualification must not grow with the number of private chunks"
    uploads = paths.sandbox_uploads_dir("chat-a", user_id="alice")
    assert list(uploads.iterdir()) == [] if revoked else (uploads / "stream.txt").read_bytes() == b"x" * 800
    assert list(volume.control_path.iterdir()) == []

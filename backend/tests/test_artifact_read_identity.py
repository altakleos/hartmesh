"""Owned temporary files exercise metadata/body identity during replacement."""

import asyncio
import hashlib
import os
import threading
import zipfile

import pytest
from _router_auth_helpers import call_unwrapped, make_authed_test_app
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import FileResponse

from app.gateway.routers import artifacts as artifacts_router
from deerflow.files import store


@pytest.mark.parametrize("suffix", ["txt", "skill/SKILL.md"])
def test_artifact_read_reports_unavailable_host_capability(tmp_path, monkeypatch, suffix):
    target = tmp_path / "sample.skill" if suffix.startswith("skill") else tmp_path / "sample.txt"
    if target.suffix == ".skill":
        with zipfile.ZipFile(target, "w") as archive:
            archive.writestr("SKILL.md", "owned fixture")
    else:
        target.write_text("owned fixture", encoding="utf-8")
    monkeypatch.setattr(artifacts_router, "resolve_thread_virtual_path", lambda _thread, _path, user_id=None: target)
    monkeypatch.setattr(store, "_DIR_FD", False)
    app = make_authed_test_app()
    app.include_router(artifacts_router.router)
    with TestClient(app) as client:
        response = client.get(f"/api/threads/thread-1/artifacts/mnt/user-data/outputs/sample.{suffix}")
    assert response.status_code == 501
    assert "directory descriptors" in response.json()["detail"]


@pytest.mark.parametrize("extra_bytes", [0, 1])
def test_artifact_capture_limit_is_inclusive(tmp_path, monkeypatch, extra_bytes):
    payload = b"a" * (artifacts_router.MAX_EDITABLE_ARTIFACT_BYTES + extra_bytes)
    target = tmp_path / "note.txt"
    target.write_bytes(payload)
    monkeypatch.setattr(artifacts_router, "resolve_thread_virtual_path", lambda _thread, _path, user_id=None: target)
    app = make_authed_test_app()
    app.include_router(artifacts_router.router)
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/artifacts/mnt/user-data/outputs/note.txt", headers={"Range": "bytes=0-3"})
    assert response.status_code == 206
    assert response.content == b"aaaa"
    if extra_bytes == 0:
        assert response.headers["etag"] == f'"{hashlib.sha256(payload).hexdigest()}"'
    else:
        assert len(response.headers["etag"].strip('"')) != 64


@pytest.mark.parametrize("range_header", [None, "bytes=2-8", "bytes=0-2,9-11"])
def test_artifact_headers_and_body_keep_the_opened_file(tmp_path, monkeypatch, range_header):
    original = b"original artifact bytes"
    target = tmp_path / "note.txt"
    target.write_bytes(original)
    replacement = tmp_path / "replacement.txt"
    replacement.write_bytes(b"replacement file bytes!")
    monkeypatch.setattr(artifacts_router, "resolve_thread_virtual_path", lambda _thread, _path, user_id=None: target)
    app = make_authed_test_app()
    app.include_router(artifacts_router.router)
    replaced = False

    real_serve = FileResponse.__call__

    async def replace_before_serving(response, scope, receive, send):
        nonlocal replaced
        os.replace(replacement, target)
        replaced = True
        await real_serve(response, scope, receive, send)

    monkeypatch.setattr(FileResponse, "__call__", replace_before_serving)

    with TestClient(app) as client:
        response = client.get(
            "/api/threads/thread-1/artifacts/mnt/user-data/outputs/note.txt",
            headers={"Range": range_header} if range_header else {},
        )
    assert replaced
    assert response.headers["etag"] == f'"{hashlib.sha256(original).hexdigest()}"'
    if range_header == "bytes=2-8":
        assert response.status_code == 206
        assert response.content == original[2:9]
    elif range_header:
        assert response.status_code == 206
        assert b"\r\nori\r\n" in response.content
        assert b"\r\nart\r\n" in response.content
    else:
        assert response.status_code == 200
        assert response.content == original


def test_skill_archive_detection_and_extraction_share_the_opened_file(tmp_path, monkeypatch):
    target = tmp_path / "sample.skill"
    replacement = tmp_path / "replacement.skill"
    for path, payload in [(target, "original skill"), (replacement, "replacement skill")]:
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("SKILL.md", payload)
    real_is_zipfile = zipfile.is_zipfile

    def replace_after_detection(source):
        result = real_is_zipfile(source)
        os.replace(replacement, target)
        return result

    monkeypatch.setattr(zipfile, "is_zipfile", replace_after_detection)
    assert artifacts_router._extract_file_from_skill_archive(target, "SKILL.md") == b"original skill"


@pytest.mark.asyncio
async def test_skill_archive_cancellation_drains_owned_source(tmp_path, monkeypatch):
    target = tmp_path / "sample.skill"
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr("SKILL.md", "owned fixture")
    monkeypatch.setattr(artifacts_router, "resolve_thread_virtual_path", lambda _thread, _path, user_id=None: target)
    entered = asyncio.Event()
    release = threading.Event()
    held = []
    loop = asyncio.get_running_loop()
    real_extract = artifacts_router._extract_open_skill_member

    def blocked(archive, path):
        held.append(archive.fp)
        result = real_extract(archive, path)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        assert not held[0].closed
        return result

    monkeypatch.setattr(artifacts_router, "_extract_open_skill_member", blocked)
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": [], "query_string": b""})
    task = asyncio.create_task(call_unwrapped(artifacts_router.get_artifact, "thread-1", "mnt/user-data/outputs/sample.skill/SKILL.md", request))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not held[0].closed
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
    assert held[0].closed

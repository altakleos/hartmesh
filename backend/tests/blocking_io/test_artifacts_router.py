"""Regression anchor: serving artifacts must not block the event loop.

Ordinary artifacts open and prepare their headers during ASGI response
ownership, then serve bytes from that descriptor. These tests send the
response under the strict Blockbuster gate, covering preparation, hashing,
reading and close. Skill archive members are extracted in the handler.

The ``@require_permission`` decorator is bypassed via ``__wrapped__`` so the
anchor exercises the handler's own filesystem IO, not the authz layer. Imports
sit at module top so any import-time IO runs at collection, outside the gate.
"""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import os
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _projection_support import ready_projection_host
from deerflow_extension_api import ArtifactPresentation, PluginContribution
from starlette.requests import Request
from starlette.responses import FileResponse
from support.artifact_projectors import worker_pid

import app.gateway.routers.artifacts as artifacts_router
from app.gateway import services
from app.gateway.authz import AuthContext
from app.gateway.path_utils import resolve_thread_virtual_path
from app.gateway.routers.artifacts import ArtifactUpdateRequest, get_artifact, update_artifact
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.storage import BlobRef

pytestmark = pytest.mark.asyncio

# The undecorated coroutine (``require_permission`` uses ``functools.wraps``).
_get_artifact = get_artifact.__wrapped__
_update_artifact = update_artifact.__wrapped__


async def _seed(tmp_path: Path, monkeypatch, thread_id: str, virtual_path: str) -> Path:
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    # Rebuild cached Paths against the tmp home so the artifact resolves under it.
    import deerflow.config.paths as paths_mod

    monkeypatch.setattr(paths_mod, "_paths", None)
    # Test-side path resolution also touches the filesystem (`.resolve()`); offload
    # it so this seeding helper doesn't itself trip the gate.
    target = await asyncio.to_thread(resolve_thread_virtual_path, thread_id, virtual_path)
    await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
    return target


async def _serve(response) -> bytes:
    messages = []

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    await response({"type": "http", "method": "GET", "headers": [], "extensions": {}}, receive, send)
    assert messages[0]["status"] == 200
    return b"".join(message.get("body", b"") for message in messages)


async def test_get_artifact_text_does_not_block_event_loop(tmp_path: Path, monkeypatch) -> None:
    vpath = "mnt/user-data/outputs/notes.txt"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    await asyncio.to_thread(target.write_text, "hello world", encoding="utf-8")

    resp = await _get_artifact("t1", vpath, request=None, download=False)

    assert isinstance(resp, FileResponse)
    assert resp.status_code == 200
    assert Path(resp.path) == target
    assert await _serve(resp) == b"hello world"
    assert resp.headers.get("content-disposition", "").startswith("inline;")


async def test_get_artifact_binary_does_not_block_event_loop(tmp_path: Path, monkeypatch) -> None:
    vpath = "mnt/user-data/outputs/blob.bin"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    payload = b"\x00\x01\x02PNGDATA"  # null byte -> binary branch (inline FileResponse)
    await asyncio.to_thread(target.write_bytes, payload)

    resp = await _get_artifact("t1", vpath, request=None, download=False)

    assert isinstance(resp, FileResponse)
    assert resp.status_code == 200
    assert Path(resp.path) == target
    assert await _serve(resp) == payload
    assert resp.headers.get("content-disposition", "").startswith("inline;")


async def test_get_artifact_skill_archive_member_does_not_block_event_loop(tmp_path: Path, monkeypatch) -> None:
    skill_vpath = "mnt/user-data/outputs/demo.skill"
    target = await _seed(tmp_path, monkeypatch, "t1", skill_vpath)

    def _build_skill_zip() -> None:
        with zipfile.ZipFile(target, "w") as zf:
            zf.writestr("SKILL.md", "# demo skill\n")

    await asyncio.to_thread(_build_skill_zip)

    resp = await _get_artifact("t1", f"{skill_vpath}/SKILL.md", request=None, download=False)

    assert resp.status_code == 200
    assert b"# demo skill" in resp.body


def _projection_request(project) -> Request:
    registry = ExtensionRegistry()
    with registry.attributed_to("test:install"):
        registry.plugin(
            PluginContribution(namespace="example.summary", title="Summary", enabled=True, api_version=2, artifacts=(ArtifactPresentation(id="summary", suffixes=(".summary.json",), project=project, projection_marker="example-summary-v1"),))
        )
    app = SimpleNamespace(state=SimpleNamespace(extensions=registry.build(), thread_store=SimpleNamespace(get=AsyncMock(return_value=None), check_access=AsyncMock(return_value=True))))
    request = Request({"type": "http", "app": app, "headers": [], "query_string": b"preview=example.summary%2Fsummary", "method": "GET"})
    request.state.auth = AuthContext(user=SimpleNamespace(id="reader", system_role="user"), permissions=["threads:read"])
    return request


async def test_installed_projection_reads_and_runs_package_code_off_loop(tmp_path: Path, monkeypatch) -> None:
    vpath = "mnt/user-data/outputs/a.summary.json"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    await asyncio.to_thread(target.write_bytes, b'{"original":true}')
    request = await asyncio.to_thread(_projection_request, worker_pid)
    async with ready_projection_host(request.app):
        response = await _get_artifact("t1", vpath, request=request, download=False)
    assert int(response.body) != os.getpid()


async def test_cancelled_projection_read_drains_before_its_descriptor_closes(tmp_path: Path, monkeypatch) -> None:
    import threading

    from app.gateway.routers import _artifact_projection as adapter

    vpath = "mnt/user-data/outputs/a.summary.json"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    await asyncio.to_thread(target.write_bytes, b"{}")
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    fingerprint = adapter._fingerprint

    def held_read(source, budget):
        started.set()
        assert release.wait(5)
        result = fingerprint(source, budget)
        finished.set()
        return result

    monkeypatch.setattr(adapter, "_fingerprint", held_read)
    request = await asyncio.to_thread(_projection_request, worker_pid)
    async with ready_projection_host(request.app):
        task = asyncio.create_task(_get_artifact("t1", vpath, request=request, download=False))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()


async def test_update_artifact_does_not_block_event_loop(tmp_path: Path, monkeypatch) -> None:
    vpath = "/mnt/user-data/outputs/notes.txt"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    original = b"hello world"
    await asyncio.to_thread(target.write_bytes, original)

    @asynccontextmanager
    async def allow_write(*_args, **_kwargs):
        yield

    class MountedProvider:
        uses_thread_data_mounts = True

    monkeypatch.setattr(artifacts_router, "reserve_artifact_write", allow_write)
    monkeypatch.setattr(artifacts_router, "get_sandbox_provider", lambda: MountedProvider())

    result = await _update_artifact(
        "t1",
        vpath,
        ArtifactUpdateRequest(
            content="updated",
            expected_sha256=hashlib.sha256(original).hexdigest(),
        ),
        request=None,
    )

    assert result.sha256 == hashlib.sha256(b"updated").hexdigest()
    assert await asyncio.to_thread(target.read_bytes) == b"updated"


async def test_shared_image_blob_cold_mime_lookup_does_not_block(tmp_path, monkeypatch):
    payload = b"\x89PNG\r\n\x1a\nimage"
    virtual_path = "/mnt/user-data/outputs/chart.png"
    ref = BlobRef(sha256=hashlib.sha256(payload).hexdigest(), size=len(payload), kind="viewed-image", content_type="image/png")
    mime_file = tmp_path / "mime.types"
    await asyncio.to_thread(mime_file.write_text, "image/png png\n", encoding="utf-8")
    # Restore all globals replaced by init(), not just its lazy-init flag.
    for name in ("suffix_map", "encodings_map", "types_map", "common_types"):
        monkeypatch.setattr(mimetypes, name, getattr(mimetypes, name))
    monkeypatch.setattr(mimetypes, "inited", False)
    monkeypatch.setattr(mimetypes, "_db", None)
    monkeypatch.setattr(mimetypes, "knownfiles", [str(mime_file)])

    class Accessor:
        async def aget(self, _config):
            return SimpleNamespace(values={"viewed_images": {virtual_path: {"mime_type": "image/png", "size": len(payload), "sha256": ref.sha256, "blob_ref": ref.model_dump(mode="json")}}})

    async def build_accessor(_request, *, thread_id):
        return Accessor(), {"configurable": {"thread_id": thread_id}}

    class Store:
        def get_bytes(self, requested_ref):
            assert requested_ref == ref
            return payload

    monkeypatch.setattr(services, "build_thread_checkpoint_state_accessor", build_accessor)
    monkeypatch.setattr("deerflow.storage.get_blob_store_if_enabled", lambda: Store())
    monkeypatch.setattr(artifacts_router, "resolve_thread_virtual_path", lambda *_args, **_kwargs: tmp_path / "missing.png")
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": [], "query_string": b"", "app": SimpleNamespace(state=SimpleNamespace(thread_store=SimpleNamespace(check_access=AsyncMock(return_value=True))))})
    user = SimpleNamespace(id="owner", system_role="user")
    request.state.user = user
    request.state.auth = AuthContext(user=user, permissions=["threads:read"])

    response = await _get_artifact("thread-1", virtual_path.lstrip("/"), request=request)
    assert response.status_code == 200
    assert response.body == payload

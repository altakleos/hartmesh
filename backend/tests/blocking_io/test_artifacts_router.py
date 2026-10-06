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
import threading
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from deerflow_extension_api import ArtifactPresentation, PluginContribution
from starlette.requests import Request
from starlette.responses import FileResponse

import app.gateway.routers.artifacts as artifacts_router
from app.gateway.path_utils import resolve_thread_virtual_path
from app.gateway.routers.artifacts import ArtifactUpdateRequest, get_artifact, update_artifact
from deerflow.extensions.registry import ExtensionRegistry

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
    app = SimpleNamespace(state=SimpleNamespace(extensions=registry.build()))
    return Request({"type": "http", "app": app, "headers": [], "query_string": b"preview=example.summary%2Fsummary", "method": "GET"})


async def test_installed_projection_reads_and_runs_package_code_off_loop(tmp_path: Path, monkeypatch) -> None:
    vpath = "mnt/user-data/outputs/a.summary.json"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    await asyncio.to_thread(target.write_bytes, b'{"original":true}')
    called = []
    loop_thread = threading.get_ident()

    def project(raw):
        assert threading.get_ident() != loop_thread
        assert target.read_bytes() == raw
        called.append(raw)
        return b'{"summary":"ready"}'

    request = await asyncio.to_thread(_projection_request, project)
    response = await _get_artifact("t1", vpath, request=request, download=False)
    assert response.body == b'{"summary":"ready"}'
    assert called == [b'{"original":true}']


async def test_cancelled_projection_drains_the_owned_worker(tmp_path: Path, monkeypatch) -> None:
    vpath = "mnt/user-data/outputs/a.summary.json"
    target = await _seed(tmp_path, monkeypatch, "t1", vpath)
    await asyncio.to_thread(target.write_bytes, b"{}")
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def project(raw):
        started.set()
        assert release.wait(5)
        assert raw == b"{}"
        finished.set()
        return b"{}"

    request = await asyncio.to_thread(_projection_request, project)
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

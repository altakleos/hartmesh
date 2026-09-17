"""Regression anchor: the Files API must not block the event loop.

Listing walks a directory tree, streaming stats and sniffs a file, keeping
copies bytes — all filesystem IO. Each handler offloads it via
``asyncio.to_thread``; if any regresses onto the event loop, the strict
Blockbuster gate raises ``BlockingError`` here. ``@require_permission`` is
bypassed via ``__wrapped__`` so the anchor exercises the handlers' own IO, not
the authz layer, and the owner is fixed through the ``user_context``
contextvar the production middleware sets.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.responses import FileResponse

import app.gateway.routers.files as files_router
from app.gateway.routers.files import KeepFileRequest, delete_file, get_file, keep_thread_file, list_files
from deerflow.config.paths import Paths
from deerflow.files import manager
from deerflow.runtime.user_context import reset_current_user, set_current_user

pytestmark = pytest.mark.asyncio

_list_files = list_files.__wrapped__
_get_file = get_file.__wrapped__
_delete_file = delete_file.__wrapped__
# Two decorators: files:write outside, the thread owner check inside.
_keep_thread_file = keep_thread_file.__wrapped__.__wrapped__

USER = "u-blocking"
THREAD = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Paths:
    paths = Paths(tmp_path)
    monkeypatch.setattr(manager, "get_paths", lambda: paths)
    monkeypatch.setattr(files_router, "get_paths", lambda: paths)
    monkeypatch.setattr("app.gateway.path_utils.get_paths", lambda: paths)
    return paths


@pytest.fixture
def owner():
    token = set_current_user(SimpleNamespace(id=USER))
    try:
        yield USER
    finally:
        reset_current_user(token)


def _request() -> SimpleNamespace:
    return SimpleNamespace(headers={}, state=SimpleNamespace())


async def _seed_file(paths: Paths, relative: str, content: bytes) -> Path:
    root = await asyncio.to_thread(paths.ensure_user_files_dir, USER)
    target = root / relative
    await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(target.write_bytes, content)
    return target


async def test_list_files_does_not_block_event_loop(paths: Paths, owner: str) -> None:
    await _seed_file(paths, "Reports/august.pdf", b"pdf")

    listed = await _list_files(request=_request())

    assert [entry.path for entry in listed.files] == ["Reports/august.pdf"]


async def test_get_file_does_not_block_event_loop(paths: Paths, owner: str) -> None:
    target = await _seed_file(paths, "notes.txt", b"hello")

    response = await _get_file("notes.txt", request=_request(), download=False)

    assert isinstance(response, FileResponse)
    assert Path(response.path) == target


async def test_delete_file_does_not_block_event_loop(paths: Paths, owner: str) -> None:
    target = await _seed_file(paths, "notes.txt", b"hello")

    result = await _delete_file("notes.txt", request=_request())

    assert result.success is True
    assert not await asyncio.to_thread(target.exists)


async def test_keep_thread_file_does_not_block_event_loop(paths: Paths, owner: str) -> None:
    await asyncio.to_thread(paths.ensure_thread_dirs, THREAD, user_id=USER)
    source = paths.sandbox_outputs_dir(THREAD, user_id=USER) / "august.pdf"
    await asyncio.to_thread(source.write_bytes, b"%PDF exact")

    kept = await _keep_thread_file(THREAD, KeepFileRequest(path="/mnt/user-data/outputs/august.pdf", folder="Reports"), request=_request())

    assert kept.path == "Reports/august.pdf"
    assert await asyncio.to_thread((paths.user_files_dir(USER) / "Reports" / "august.pdf").read_bytes) == b"%PDF exact"

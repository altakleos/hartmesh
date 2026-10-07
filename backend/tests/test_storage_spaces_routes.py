"""Real resource/file grants behind browser and attributed internal requests."""

from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, make_storage_fixture, operation


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def space_file_storage(tmp_path, request):
    async for value in make_storage_fixture(tmp_path, request):
        yield value


def request_app(service, actor=ALICE, source="session"):
    from fastapi import FastAPI

    from app.gateway.routers.spaces import router

    app = FastAPI()
    app.state.storage_spaces = service

    @app.middleware("http")
    async def bind(request, call_next):
        request.state.user = SimpleNamespace(id=actor.subject_id, system_role="user") if actor is not None else None
        request.state.auth_source = source
        return await call_next(request)

    app.include_router(router)
    return app


@pytest.mark.asyncio
async def test_resource_urls_and_revisions_are_independent_of_threads(space_file_storage):
    service, _, _ = space_file_storage
    space = await home(service)
    app = request_app(service)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        path = f"/api/spaces/{space.id}"
        assert (await client.put(path + "/content", params={"path": ".page", "generation": 1, "operation_id": operation(), "create": "true"}, content=b"hello")).status_code == 200
        listing = (await client.get(path + "/files")).json()
        assert listing["files"][0]["path"] == ".page"
        assert listing["files"][0]["url"].startswith(path + "/content?")
        assert "thread" not in listing["files"][0]["url"]
        response = await client.get(path + "/content", params={"path": ".page"}, headers={"Range": "bytes=1-3"})
        assert response.status_code == 206 and response.content == b"ell"
        revision = (await client.get(path + "/text", params={"path": ".page"})).json()
        assert revision["text"] == "hello" and len(revision["sha256"]) == 64
        stale = await client.put(path + "/content", params={"path": ".page", "generation": 1, "operation_id": operation(), "expected_sha256": "0" * 64}, content=b"bad")
        assert stale.status_code == 409
        assert (await client.get(path + "/content", params={"path": ".page"})).content == b"hello"


@pytest.mark.asyncio
async def test_ungranted_actor_never_gets_files_even_with_optional_authorization_off(space_file_storage, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_AUTHORIZATION_ENABLED", "0")
    service, _, _ = space_file_storage
    space = await home(service)
    app = request_app(service, BOB)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/api/spaces")).json()["spaces"] == []
        assert (await client.get(f"/api/spaces/{space.id}/files", params={"user_id": "alice"})).status_code == 404
        assert (await client.get(f"/api/spaces/{space.id}/content", params={"path": "x"}, headers={"X-DeerFlow-Owner-User-Id": "alice"})).status_code == 404
        forged = await client.post("/api/spaces", json={"name": "forged", "custody": "personal", "actor": {"kind": "human", "subject_id": "alice"}})
        assert forged.status_code == 422


@pytest.mark.asyncio
async def test_active_content_is_downloaded_and_unknown_actor_never_defaults(space_file_storage):
    service, _, _ = space_file_storage
    space = await home(service)
    await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page.html", content=b"<script>document.cookie</script>", create=True)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(service)), base_url="http://test") as client:
        response = await client.get(f"/api/spaces/{space.id}/content", params={"path": "page.html"})
        assert response.headers["content-disposition"].startswith("attachment")
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["cache-control"] == "private, no-store"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(service, None)), base_url="http://test") as client:
        assert (await client.get("/api/spaces")).status_code == 401


@pytest.mark.asyncio
async def test_pat_without_resource_scope_is_explicitly_unsupported(space_file_storage):
    service, _, _ = space_file_storage
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(service, source="pat")), base_url="http://test") as client:
        assert (await client.get("/api/spaces")).status_code == 403


@pytest.mark.asyncio
async def test_stalled_download_releases_sql_before_streaming_an_open_inode(space_file_storage, monkeypatch):
    import asyncio
    import hashlib
    import os

    from app.gateway.routers.spaces import SpaceFileResponse

    service, _, _ = space_file_storage
    space = await home(service)
    await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="old", content=b"original" * 10000, create=True)
    started, release = asyncio.Event(), asyncio.Event()
    chunks = []

    async def send(message):
        if message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))
            if not started.is_set():
                started.set()
                await release.wait()

    async def receive():
        await asyncio.Event().wait()

    response = SpaceFileResponse(service, ALICE, space.id, "old", False)
    opened = []
    original_open = response._open_descriptor

    def open_once():
        fd = original_open()
        opened.append(fd)
        return fd

    monkeypatch.setattr(response, "_open_descriptor", open_once)
    stream = asyncio.create_task(response({"type": "http", "method": "GET", "headers": [], "extensions": {}}, receive, send))
    writer = None
    try:
        await asyncio.wait_for(started.wait(), 3)
        writer = asyncio.create_task(service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="old", content=b"replacement", expected_sha256=hashlib.sha256(b"original" * 10000).hexdigest()))
        done, _ = await asyncio.wait({writer}, timeout=1)
        admitted_before_stream_finished = bool(done)
    finally:
        release.set()
        await stream
        if writer is not None:
            await writer
    assert admitted_before_stream_finished, "A slow read must not reserve SQLite's global application writer"
    assert b"".join(chunks) == b"original" * 10000
    assert len(opened) == 1
    assert response._fd is None and response._filesystem is None
    with pytest.raises(OSError):
        os.fstat(opened[0])


@pytest.mark.asyncio
async def test_cancelled_resource_send_closes_its_file_and_root_descriptors(space_file_storage):
    import asyncio
    import os

    from app.gateway.routers.spaces import SpaceFileResponse

    service, _, _ = space_file_storage
    space = await home(service)
    await service.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"content", create=True)
    response = SpaceFileResponse(service, ALICE, space.id, "page", False)
    started = asyncio.Event()
    descriptors = []

    async def send(message):
        if message["type"] == "http.response.body" and not started.is_set():
            descriptors.extend([response._fd, response._filesystem._data_fd, response._filesystem._control_fd])
            started.set()
            await asyncio.Event().wait()

    async def receive():
        await asyncio.Event().wait()

    stream = asyncio.create_task(response({"type": "http", "method": "GET", "headers": [], "extensions": {}}, receive, send))
    await asyncio.wait_for(started.wait(), 3)
    stream.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stream
    assert len(descriptors) == 3
    assert response._fd is None and response._filesystem is None
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)

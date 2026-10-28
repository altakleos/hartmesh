"""Descriptor metadata and explicitly streamed transfers never prefetch a payload."""

import os
from types import SimpleNamespace

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.gateway.routers import _file_http, artifacts


async def serve(response, *, method="GET", messages=None):
    messages = [] if messages is None else messages

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await response({"type": "http", "method": method, "headers": []}, receive, send)
    return dict(messages[0]["headers"]), b"".join(message.get("body", b"") for message in messages[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("hash_max_bytes", [None, 32])
async def test_head_never_reads_payload_for_hash_or_mime_sniff(tmp_path, monkeypatch, hash_max_bytes):
    target = tmp_path / "result.unknown-extension"
    target.write_bytes(b"original result")
    opened = []
    actual_open = _file_http.open_regular_source

    def opened_source(path):
        descriptor = actual_open(path)
        opened.append(descriptor)
        return descriptor

    def payload_read_forbidden(*args, **kwargs):
        pytest.fail("Metadata-only HEAD read payload for capture or MIME sniffing")

    monkeypatch.setattr(_file_http, "open_regular_source", opened_source)
    monkeypatch.setattr(_file_http.os, "read", payload_read_forbidden)
    monkeypatch.setattr(_file_http.os, "fdopen", payload_read_forbidden)
    headers, body = await serve(_file_http.DescriptorFileResponse(target, hash_max_bytes=hash_max_bytes), method="HEAD")
    assert body == b"" and headers[b"content-length"] == b"15"
    assert headers[b"content-type"] == b"application/octet-stream"
    assert opened
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("method", ["HEAD", "GET"])
@pytest.mark.parametrize("raw", [False, True])
def test_artifact_raw_door_never_prefetches_or_hashes(tmp_path, monkeypatch, method, raw):
    target = tmp_path / "result.csv"
    target.write_bytes(b"a,b\n1,2\n")
    monkeypatch.setattr(artifacts, "resolve_thread_virtual_path", lambda *args, **kwargs: target)

    opened, captures, hashes = set(), [], []
    actual_open = _file_http.DescriptorFileResponse._open_descriptor
    actual_fdopen, actual_hash = _file_http.os.fdopen, _file_http.hashlib.sha256

    def open_source(response):
        descriptor = actual_open(response)
        opened.add(descriptor)
        return descriptor

    def capture(descriptor, *args, **kwargs):
        if descriptor in opened:
            captures.append(descriptor)
        return actual_fdopen(descriptor, *args, **kwargs)

    def digest(value=b"", *args, **kwargs):
        if value == b"a,b\n1,2\n":
            hashes.append(True)
        return actual_hash(value, *args, **kwargs)

    monkeypatch.setattr(_file_http.DescriptorFileResponse, "_open_descriptor", open_source)
    monkeypatch.setattr(_file_http.os, "fdopen", capture)
    monkeypatch.setattr(_file_http.hashlib, "sha256", digest)
    app = make_authed_test_app()
    app.include_router(artifacts.router)
    with TestClient(app) as client:
        response = client.request(method, "/api/threads/thread-1/artifacts/mnt/user-data/outputs/result.csv", params={"download": "true", **({"stream": "true"} if raw else {})})
    assert response.status_code == 200
    assert response.content == (b"" if method == "HEAD" else b"a,b\n1,2\n")
    assert response.headers["content-length"] == "8"
    assert bool(captures) is (method == "GET" and not raw)
    assert bool(hashes) is (method == "GET" and not raw)
    if method == "HEAD" or raw:
        assert response.headers["cache-control"] == "private, no-store"


@pytest.mark.parametrize("source", ["archive_member", "projection"])
def test_raw_artifact_door_refuses_eager_non_file_sources(tmp_path, monkeypatch, source):
    target = tmp_path / "result.skill"
    target.write_bytes(b"must not be opened as an archive")
    monkeypatch.setattr(artifacts, "resolve_thread_virtual_path", lambda *args, **kwargs: target)

    def eager_payload_forbidden(*args, **kwargs):
        raise AssertionError("Raw artifact transfer used an eager projection/archive payload")

    monkeypatch.setattr(artifacts, "_load_skill_archive_member", eager_payload_forbidden)
    monkeypatch.setattr(artifacts, "installed_projection", lambda *args: object())
    monkeypatch.setattr(artifacts, "project_artifact", eager_payload_forbidden)
    app = make_authed_test_app()
    app.include_router(artifacts.router)
    path = "result.skill/SKILL.md" if source == "archive_member" else "result.txt"
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/artifacts/mnt/user-data/outputs/" + path, params={"stream": "true", **({"preview": "example/projection"} if source == "projection" else {"download": "true"})})
    assert response.status_code == 501


@pytest.mark.asyncio
async def test_explicit_stream_reads_only_bounded_descriptor_chunks_without_capture(tmp_path, monkeypatch):
    target = tmp_path / "result.bin"
    payload = b"x" * (2 * _file_http.DescriptorFileResponse.chunk_size + 7)
    target.write_bytes(payload)
    response = _file_http.DescriptorFileResponse(target, hash_max_bytes=len(payload), stream=True)
    reads = []
    actual_read = response._read

    def read(offset, size):
        assert response._content is None
        reads.append((offset, size))
        return actual_read(offset, size)

    monkeypatch.setattr(response, "_read", read)
    headers, body = await serve(response)
    assert body == payload and headers[b"content-length"] == str(len(payload)).encode()
    assert reads == [(0, response.chunk_size), (response.chunk_size, response.chunk_size), (2 * response.chunk_size, 7)]
    assert response._fd is None and response._content is None


@pytest.mark.parametrize("method", ["HEAD", "GET"])
@pytest.mark.parametrize("link", ["leaf", "parent"])
def test_raw_artifact_door_preserves_no_follow_source_refusal(tmp_path, monkeypatch, method, link):
    parent = tmp_path / "outputs"
    parent.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "result.txt").write_bytes(b"private")
    if link == "leaf":
        target = parent / "result.txt"
        target.symlink_to(outside / target.name)
    else:
        parent.rmdir()
        parent.symlink_to(outside, target_is_directory=True)
        target = parent / "result.txt"
    monkeypatch.setattr(artifacts, "resolve_thread_virtual_path", lambda *args, **kwargs: target)
    app = make_authed_test_app()
    app.include_router(artifacts.router)
    with TestClient(app) as client:
        response = client.request(method, "/api/threads/thread-1/artifacts/mnt/user-data/outputs/result.txt", params={"stream": "true"})
    assert response.status_code == 400 and b"private" not in response.content


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["HEAD", "GET"])
async def test_chat_source_rechecks_space_after_prepare_and_closes_before_denied_headers(tmp_path, monkeypatch, method):
    from app.gateway import chat_files

    relative = "conversations/thread/user-data/outputs/result.txt"
    target = tmp_path / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(b"private result")
    descriptors, filesystems, checks = [], [], []

    class Filesystem:
        closed = False

        def open_regular(self, path, *, follow_symlinks):
            assert path == relative and follow_symlinks is False
            descriptor = _file_http.open_regular_source(tmp_path / path)
            descriptors.append(descriptor)
            return descriptor

        def close(self):
            self.closed = True

    def filesystem():
        result = Filesystem()
        filesystems.append(result)
        return result

    volume = SimpleNamespace(filesystem=filesystem)
    prepared = SimpleNamespace(paths=SimpleNamespace(routing_signature=lambda: ("owner", "user", None)), mounts=SimpleNamespace(thread_id="thread", user=volume, company=None))
    access = chat_files.ChatFileAccess(prepared)

    async def admitted(request, thread):
        checks.append(thread)
        if len(checks) == 2:
            raise HTTPException(403, "Current Space access revoked")
        return access

    monkeypatch.setattr(chat_files, "prepare_chat_files", admitted)
    response = chat_files.ChatFileResponse(SimpleNamespace(), access, "/mnt/user-data/outputs/result.txt", hash_max_bytes=100, stream=True)
    messages = []
    with pytest.raises(HTTPException) as error:
        await serve(response, method=method, messages=messages)
    assert error.value.status_code == 403 and checks == ["thread", "thread"] and messages == []
    assert filesystems and all(item.closed for item in filesystems)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("method", ["HEAD", "GET"])
def test_artifact_metadata_and_stream_keep_existing_thread_owner_fence(monkeypatch, method):
    def source_forbidden(*args, **kwargs):
        raise AssertionError("Denied thread ownership reached an artifact source")

    monkeypatch.setattr(artifacts, "resolve_thread_virtual_path", source_forbidden)
    app = make_authed_test_app(owner_check_passes=False)
    app.include_router(artifacts.router)
    with TestClient(app) as client:
        response = client.request(method, "/api/threads/thread-1/artifacts/mnt/user-data/outputs/result.txt", params={"stream": "true"})
    assert response.status_code in {403, 404}

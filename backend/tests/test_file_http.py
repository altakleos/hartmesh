"""Files and Shared bind every streamed byte to the validated descriptor."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.gateway.routers import files, shared
from app.gateway.routers._file_http import existing_regular_file

pytestmark = [pytest.mark.asyncio, pytest.mark.skipif(os.name != "posix", reason="Descriptor-relative file serving requires POSIX")]


async def _receive():
    return {"type": "http.request", "body": b"", "more_body": False}


async def _run(response, *, method="GET", headers=(), send=None):
    messages = []

    async def collect(message):
        messages.append(message)
        if send is not None:
            await send(message)

    await response({"type": "http", "method": method, "headers": list(headers), "extensions": {"http.response.pathsend": {}}}, _receive, collect)
    return messages


@pytest.mark.parametrize("module,handler,preflight", [(files, files.get_file, "_existing_regular_file"), (shared, shared.get_shared_file, "_published_file")])
@pytest.mark.parametrize("swap", ["parent", "leaf"])
async def test_stream_refuses_link_swapped_after_preflight(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, module, handler, preflight, swap: str) -> None:
    parent = tmp_path / "files" / "reports"
    parent.mkdir(parents=True)
    target = parent / "report.txt"
    target.write_bytes(b"allowed")
    outside = tmp_path / "outside"
    outside.mkdir()
    private = outside / target.name
    private.write_bytes(b"synthetic-private")

    def checked_then_swapped(*args):
        actual = existing_regular_file(lambda: target, label=target.name)
        if swap == "parent":
            parent.rename(parent.with_name("previous"))
            parent.symlink_to(outside, target_is_directory=True)
        else:
            target.unlink()
            target.symlink_to(private)
        return actual

    monkeypatch.setattr(module, preflight, checked_then_swapped)
    monkeypatch.setattr(module, "acting_user_id", lambda request: "owner")
    response = await handler.__wrapped__("reports/report.txt", request=SimpleNamespace(), download=False)
    with pytest.raises(HTTPException) as exc:
        await _run(response)
    assert exc.value.status_code == 400


@pytest.fixture
def opened_descriptors(monkeypatch: pytest.MonkeyPatch):
    from app.gateway.routers import _file_http

    opened = []
    real_open = _file_http.open_regular_source

    def record(path):
        fd = real_open(path)
        opened.append(fd)
        return fd

    monkeypatch.setattr(_file_http, "open_regular_source", record)
    return opened


def _assert_closed(opened):
    assert opened
    for fd in opened:
        with pytest.raises(OSError):
            os.fstat(fd)


def _parts(messages):
    start = messages[0]
    assert start["type"] == "http.response.start"
    assert all(message["type"] == "http.response.body" for message in messages[1:])
    return start["status"], dict(start["headers"]), b"".join(message.get("body", b"") for message in messages[1:])


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("hash_max_bytes", [None, 10])
@pytest.mark.parametrize("range_header,status,body", [(None, 200, b"0123456789"), (b"bytes=2-5", 206, b"2345"), (b"bytes=-3", 206, b"789"), (b"bytes=0-1,5-7", 206, None), (b"bytes=99-100", 416, b""), (b"not-a-range", 400, None)])
async def test_descriptor_response_retains_http_range_contract(tmp_path: Path, opened_descriptors, method: str, hash_max_bytes, range_header, status: int, body) -> None:
    from app.gateway.routers._file_http import DescriptorFileResponse

    target = tmp_path / "résumé.txt"
    target.write_bytes(b"0123456789")
    response = DescriptorFileResponse(target, hash_max_bytes=hash_max_bytes)
    assert opened_descriptors == [], "constructing an unused response owns no descriptor"
    messages = await _run(response, method=method, headers=[] if range_header is None else [(b"range", range_header)])
    actual_status, headers, actual_body = _parts(messages)
    assert actual_status == status
    if status in (200, 206):
        assert headers[b"accept-ranges"] == b"bytes"
        assert headers[b"x-content-type-options"] == b"nosniff"
        assert headers[b"content-disposition"] == b"inline; filename*=UTF-8''r%C3%A9sum%C3%A9.txt"
        assert headers[b"etag"]
        assert headers[b"last-modified"]
        if method == "HEAD":
            assert actual_body == b""
        elif body is not None:
            assert actual_body == body
            assert int(headers[b"content-length"]) == len(actual_body)
        else:
            assert headers[b"content-type"].startswith(b"multipart/byteranges; boundary=")
            assert b"Content-Range: bytes 0-1/10\r\n\r\n01\r\n" in actual_body
            assert b"Content-Range: bytes 5-7/10\r\n\r\n567\r\n" in actual_body
            assert int(headers[b"content-length"]) == len(actual_body)
    if status == 416:
        assert headers[b"content-range"] == b"bytes */10"
    _assert_closed(opened_descriptors)


@pytest.mark.parametrize("matching", [True, False])
@pytest.mark.parametrize("hash_max_bytes", [None, 10])
async def test_if_range_uses_opened_file_metadata(tmp_path: Path, matching: bool, hash_max_bytes) -> None:
    from app.gateway.routers._file_http import DescriptorFileResponse

    target = tmp_path / "report.txt"
    target.write_bytes(b"0123456789")
    _, headers, _ = _parts(await _run(DescriptorFileResponse(target, hash_max_bytes=hash_max_bytes)))
    result = await _run(DescriptorFileResponse(target, hash_max_bytes=hash_max_bytes), headers=[(b"range", b"bytes=2-3"), (b"if-range", headers[b"etag"] if matching else b'"old"')])
    status, _, body = _parts(result)
    assert (status, body) == ((206, b"23") if matching else (200, b"0123456789"))


@pytest.mark.parametrize(
    "suffix,download,mime,disposition",
    [("txt", False, "text/plain", "inline"), ("txt", True, "text/plain", "attachment"), ("html", False, "text/html", "attachment"), ("svg", False, "image/svg+xml", "attachment"), ("rss", False, "application/x-rss+xml", "attachment")],
)
async def test_content_policy_is_preserved(tmp_path: Path, suffix: str, download: bool, mime: str, disposition: str) -> None:
    from app.gateway.routers._file_http import DescriptorFileResponse

    target = tmp_path / f"report.{suffix}"
    target.write_bytes(b"contents")
    _, headers, _ = _parts(await _run(DescriptorFileResponse(target, download=download)))
    assert headers[b"content-disposition"].startswith(disposition.encode() + b";")
    # Some platforms register application/rss+xml instead of x-rss+xml.
    if suffix == "rss":
        assert headers[b"content-type"].endswith(b"+xml")
    else:
        assert headers[b"content-type"].startswith(mime.encode())


async def test_sniff_metadata_and_body_keep_opened_file_after_parent_swap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, opened_descriptors) -> None:
    from app.gateway.routers import _file_http

    parent = tmp_path / "reports"
    parent.mkdir()
    target = parent / "report.unknown-extension"
    target.write_bytes(b"allowed")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / target.name).write_bytes(b"synthetic-private\x00")
    real_open = _file_http.open_regular_source

    def open_then_swap(path):
        fd = real_open(path)
        parent.rename(parent.with_name("previous"))
        parent.symlink_to(outside, target_is_directory=True)
        return fd

    monkeypatch.setattr(_file_http, "open_regular_source", open_then_swap)
    _, headers, body = _parts(await _run(_file_http.DescriptorFileResponse(target)))
    assert headers[b"content-type"] == b"text/plain; charset=utf-8"
    assert headers[b"content-length"] == b"7"
    assert body == b"allowed"
    _assert_closed(opened_descriptors)


@pytest.mark.parametrize("phase", ["prepare", "read", "send"])
async def test_descriptor_closes_after_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, opened_descriptors, phase: str) -> None:
    from app.gateway.routers import _file_http

    target = tmp_path / "report.txt"
    target.write_bytes(b"allowed")
    response = _file_http.DescriptorFileResponse(target)

    def fail(*args):
        raise RuntimeError("synthetic failure")

    async def send(message):
        if phase == "send":
            fail()

    if phase == "prepare":
        monkeypatch.setattr(_file_http.mimetypes, "guess_type", fail)
    elif phase == "read":
        monkeypatch.setattr(response, "_read", fail)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        await _run(response, send=send)
    _assert_closed(opened_descriptors)


@pytest.mark.parametrize("phase", ["acquire", "read"])
async def test_cancellation_drains_worker_before_closing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, opened_descriptors, phase: str) -> None:
    import asyncio
    import threading

    from app.gateway.routers import _file_http

    target = tmp_path / "report.txt"
    target.write_bytes(b"allowed")
    response = _file_http.DescriptorFileResponse(target)
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    real_work = _file_http.open_regular_source if phase == "acquire" else response._read

    def blocked(*args):
        value = real_work(*args)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5), "test failed to release worker"
        # The worker must still own its descriptor after repeated cancellation.
        assert os.fstat(opened_descriptors[-1]).st_size == 7
        return value

    if phase == "acquire":
        monkeypatch.setattr(_file_http, "open_regular_source", blocked)
    else:
        monkeypatch.setattr(response, "_read", blocked)
    task = asyncio.create_task(_run(response))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "cancellation must wait for the owned worker"
        assert os.fstat(opened_descriptors[-1]).st_size == 7
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
    _assert_closed(opened_descriptors)


async def test_missing_file_is_reported_before_headers(tmp_path: Path) -> None:
    from app.gateway.routers._file_http import DescriptorFileResponse

    sent = []

    async def send(message):
        sent.append(message)

    with pytest.raises(HTTPException) as exc:
        await _run(DescriptorFileResponse(tmp_path / "missing.txt"), send=send)
    assert exc.value.status_code == 404
    assert sent == []


async def test_mutation_during_capture_conflicts_and_closes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, opened_descriptors) -> None:
    from app.gateway.routers import _file_http

    target = tmp_path / "report.txt"
    target.write_bytes(b"original")
    real_stat = os.fstat
    calls = 0

    def stat_then_mutate(fd):
        nonlocal calls
        if opened_descriptors and fd == opened_descriptors[-1]:
            calls += 1
            if calls == 2:
                target.write_bytes(b"changed contents")
        return real_stat(fd)

    monkeypatch.setattr(_file_http.os, "fstat", stat_then_mutate)
    sent = []

    async def send(message):
        sent.append(message)

    with pytest.raises(HTTPException) as exc:
        await _run(_file_http.DescriptorFileResponse(target, hash_max_bytes=100), send=send)
    assert exc.value.status_code == 409
    assert sent == []
    _assert_closed(opened_descriptors)

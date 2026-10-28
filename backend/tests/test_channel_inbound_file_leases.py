"""Inbound buffer ownership through actual disk workers and cancellation."""

from __future__ import annotations

import asyncio
import threading

import pytest

from app.channels import manager
from app.channels.message_bus import InboundMessage
from deerflow.uploads import manager as uploads


@pytest.fixture
def ingestion(tmp_path, monkeypatch):
    destination = tmp_path / "owner-bucket" / "uploads"
    destination.mkdir(parents=True)
    monkeypatch.setattr(uploads, "ensure_uploads_dir", lambda thread, *, user_id=None: destination)

    def message(*files):
        return InboundMessage(channel_name="lease-test", chat_id="chat", user_id="platform-person", text="A message", files=list(files))

    return destination, message


@pytest.mark.asyncio
async def test_previous_buffer_is_destroyed_before_next_reader_starts(ingestion, monkeypatch):
    _destination, message = ingestion
    finalized = threading.Event()

    class OwnedBytes(bytes):
        def __del__(self):
            finalized.set()

    async def reader(file, _client):
        if file["filename"] == "first.txt":
            return OwnedBytes(b"The first reader exclusively creates this buffer")
        assert finalized.is_set(), "previous file bytes are retained while the next reader starts"
        return b"Second file"

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    result = await manager._ingest_inbound_files("thread", message({"filename": "first.txt"}, {"filename": "second.txt"}), user_id="owner")
    assert [item["filename"] for item in result] == ["first.txt", "second.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["write", "permissions"])
async def test_cancelled_manager_waits_for_actual_native_worker(ingestion, monkeypatch, phase):
    destination, message = ingestion
    started, finish, completed = threading.Event(), threading.Event(), threading.Event()
    actual = uploads.write_upload_file_no_symlink if phase == "write" else manager._make_inbound_file_sandbox_readable

    def blocked(*args):
        started.set()
        assert finish.wait(15), "test did not release the filesystem worker"
        try:
            return actual(*args)
        finally:
            completed.set()

    monkeypatch.setattr(uploads if phase == "write" else manager, "write_upload_file_no_symlink" if phase == "write" else "_make_inbound_file_sandbox_readable", blocked)
    task = asyncio.create_task(manager._ingest_inbound_files("thread", message({"filename": "file.txt", "_content": b"Actual worker bytes"})))
    try:
        assert await asyncio.to_thread(started.wait, 15)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=0.05)
        assert not done, "manager returned while its filesystem worker still owns the buffer/path"
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(completed.wait, 15)
    assert task.cancelled() and (destination / "file.txt").read_bytes() == b"Actual worker bytes"


def lease_type():
    import importlib

    return importlib.import_module("app.channels.inbound_files").InboundFileLease


@pytest.mark.asyncio
async def test_close_clears_bytes_and_joins_one_release_through_repeated_cancellation():
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def release():
        calls.append("release")
        assert lease.data is None
        entered.set()
        await finish.wait()

    lease = lease_type()(b"Owned bytes", release)
    assert lease.data == b"Owned bytes"
    first = asyncio.create_task(lease.aclose())
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 15)
        second = asyncio.create_task(lease.aclose())
        first.cancel()
        await asyncio.sleep(0)
        first.cancel()
        done, _pending = await asyncio.wait({first}, timeout=0.05)
        assert not done and calls == ["release"]
    finally:
        finish.set()
        results = await asyncio.gather(*[task for task in (first, second) if task is not None], return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError) and results[1] is None
    await lease.aclose()
    assert lease.data is None and calls == ["release"]


@pytest.mark.asyncio
async def test_failed_release_is_observed_without_a_second_invocation():
    calls = []
    failure = RuntimeError("release failed")

    async def release():
        calls.append("release")
        raise failure

    lease = lease_type()(b"Owned bytes", release)
    for _ in range(2):
        with pytest.raises(RuntimeError) as caught:
            await lease.aclose()
        assert caught.value is failure
    assert lease.data is None and calls == ["release"]


@pytest.mark.asyncio
async def test_transferred_lease_bytes_die_before_next_reader_and_release_is_complete(ingestion, monkeypatch):
    destination, message = ingestion
    finalized = threading.Event()
    released = []
    leases = []

    class OwnedBytes(bytes):
        def __del__(self):
            finalized.set()

    async def release():
        assert leases[0].data is None
        released.append("done")

    async def reader(file, _client):
        if file["filename"] == "first.txt":
            lease = lease_type()(OwnedBytes(b"Payload owned by the first lease only"), release)
            leases.append(lease)
            return lease
        assert finalized.is_set() and released == ["done"]
        return b"Second file"

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    result = await manager._ingest_inbound_files("thread", message({"filename": "first.txt"}, {"filename": "second.txt"}), user_id="owner")
    assert len(result) == 2 and result[0]["size"] == len(b"Payload owned by the first lease only")
    assert (destination / "first.txt").read_bytes() == b"Payload owned by the first lease only"
    assert leases[0].data is None and released == ["done"]


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe", ["filename", "destination"])
async def test_transferred_lease_is_released_for_unsafe_name_or_destination(ingestion, monkeypatch, unsafe):
    from support.symlinks import symlink_or_skip

    destination, message = ingestion
    released = []
    outside = destination.parent / "outside.txt"
    if unsafe == "destination":
        symlink_or_skip(destination / "victim.txt", outside)

    async def release():
        released.append("done")

    lease = lease_type()(b"Never leave the bucket", release)

    async def reader(_file, _client):
        return lease

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    result = await manager._ingest_inbound_files("thread", message({"filename": ".." if unsafe == "filename" else "victim.txt"}))
    assert result == [] and not outside.exists()
    assert lease.data is None and released == ["done"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["write", "permissions"])
@pytest.mark.parametrize("failure_kind", ["quota", "other", "chained"])
async def test_write_failure_notifies_actual_channel_and_releases_before_next_file(ingestion, monkeypatch, phase, failure_kind):
    import errno

    destination, message = ingestion
    failure = OSError(errno.ENOSPC, "full") if failure_kind == "quota" else RuntimeError("native failure")
    finalized = threading.Event()
    notified, released, leases = [], [], []
    actual_write, actual_permits = uploads.write_upload_file_no_symlink, manager._make_inbound_file_sandbox_readable

    class OwnedBytes(bytes):
        def __del__(self):
            finalized.set()

    async def release():
        released.append("done")

    async def reader(file, _client):
        if file["filename"] == "bad.txt":
            lease = lease_type()(OwnedBytes(b"Failed file's charged buffer"), release)
            leases.append(lease)
            return lease
        assert finalized.is_set() and released == ["done"]
        return b"Good file"

    def write(base, name, data):
        if phase == "write" and name == "bad.txt":
            if failure_kind == "chained":
                try:
                    raise OSError(errno.EDQUOT, "inner failure")
                except OSError as cause:
                    raise failure from cause
            raise failure
        return actual_write(base, name, data)

    def permits(path):
        if phase == "permissions" and path.name == "bad.txt":
            if failure_kind == "chained":
                try:
                    raise OSError(errno.EDQUOT, "inner failure")
                except OSError as cause:
                    raise failure from cause
            raise failure
        return actual_permits(path)

    async def notify(file, error):
        assert error is failure and file["filename"] == "bad.txt"
        assert error.__traceback__ is None  # Diagnostics must not retain native buffer arguments.
        if failure_kind == "chained":
            assert isinstance(error.__cause__, OSError) and error.__cause__.errno == errno.EDQUOT
            assert error.__cause__.__traceback__ is None
        notified.append((file, error))

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    monkeypatch.setitem(manager.INBOUND_FILE_WRITE_FAILED, "lease-test", notify)
    monkeypatch.setattr(uploads, "write_upload_file_no_symlink", write)
    monkeypatch.setattr(manager, "_make_inbound_file_sandbox_readable", permits)
    result = await manager._ingest_inbound_files("thread", message({"filename": "bad.txt"}, {"filename": "good.txt"}))
    assert [item["filename"] for item in result] == ["good.txt"]
    assert len(notified) == 1 and notified[0][1] is failure
    assert leases[0].data is None and (destination / "good.txt").read_bytes() == b"Good file"


@pytest.mark.asyncio
async def test_notifier_failure_does_not_stop_following_file_or_expose_exception_text(ingestion, monkeypatch, caplog):
    _destination, message = ingestion
    released = []
    actual = uploads.write_upload_file_no_symlink

    async def release():
        released.append("done")

    async def reader(file, _client):
        return lease_type()(b"First file", release) if file["filename"] == "bad.txt" else b"Following file"

    def write(base, name, data):
        if name == "bad.txt":
            raise OSError("native private sentinel")
        return actual(base, name, data)

    async def notify(_file, _error):
        raise RuntimeError("https://private.example/file?signed=sentinel")

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    monkeypatch.setattr(manager, "INBOUND_FILE_WRITE_FAILED", {})
    manager.register_inbound_file_write_failed("lease-test", notify)
    monkeypatch.setattr(uploads, "write_upload_file_no_symlink", write)
    result = await manager._ingest_inbound_files("thread", message({"filename": "bad.txt"}, {"filename": "good.txt"}))
    assert [item["filename"] for item in result] == ["good.txt"] and released == ["done"]
    assert "private.example" not in caplog.text and "native private sentinel" not in caplog.text


@pytest.mark.asyncio
async def test_manager_finally_drains_cancelled_lease_release(ingestion, monkeypatch):
    _destination, message = ingestion
    entered, finish = asyncio.Event(), asyncio.Event()
    released = []

    async def release():
        entered.set()
        await finish.wait()
        released.append("done")

    lease = lease_type()(b"Owned until native work ends", release)

    async def reader(_file, _client):
        return lease

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    task = asyncio.create_task(manager._ingest_inbound_files("thread", message({"filename": "file.txt"})))
    try:
        await asyncio.wait_for(entered.wait(), 15)
        assert lease.data is None
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=0.05)
        assert not done and released == []
    finally:
        finish.set()
        results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError) and released == ["done"]


@pytest.mark.asyncio
async def test_initial_directory_failure_keeps_existing_boundary_before_readers(ingestion, monkeypatch):
    _destination, message = ingestion
    called = []
    failure = OSError("directory preparation failed")

    def prepare(_thread, *, user_id=None):
        raise failure

    async def reader(_file, _client):
        called.append("read")

    monkeypatch.setattr(uploads, "ensure_uploads_dir", prepare)
    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    with pytest.raises(OSError) as caught:
        await manager._ingest_inbound_files("thread", message({"filename": "file.txt"}))
    assert caught.value is failure and called == []


@pytest.mark.asyncio
@pytest.mark.parametrize("chained", [False, True], ids=["plain-error", "chained-error"])
async def test_cancelled_native_failure_releases_buffer_without_waiting_for_garbage_collection(ingestion, monkeypatch, chained):
    import errno

    _destination, message = ingestion
    started, finish, completed = threading.Event(), threading.Event(), threading.Event()
    finalized = threading.Event()
    failure = OSError(errno.ENOSPC, "Native worker failed after cancellation")
    released, notified = [], []

    class OwnedBytes(bytes):
        def __del__(self):
            finalized.set()

    async def release():
        assert finalized.is_set(), "completed native exception still retains leased buffer"
        released.append("done")

    async def reader(_file, _client):
        return lease_type()(OwnedBytes(b"Only the lease and native worker own this buffer"), release)

    def write(_base, _name, _data):
        started.set()
        assert finish.wait(15)
        try:
            if chained:
                try:
                    raise RuntimeError("Inner native frame also owns bytes")
                except RuntimeError as cause:
                    raise failure from cause
            raise failure
        finally:
            completed.set()

    async def notify(_file, _error):
        notified.append("must not start after cancellation")

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    monkeypatch.setitem(manager.INBOUND_FILE_WRITE_FAILED, "lease-test", notify)
    monkeypatch.setattr(uploads, "write_upload_file_no_symlink", write)
    task = asyncio.create_task(manager._ingest_inbound_files("thread", message({"filename": "file.txt"})))
    try:
        assert await asyncio.to_thread(started.wait, 15)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=0.05)
        assert not done and released == []
    finally:
        finish.set()
        results = await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(completed.wait, 15)
    assert isinstance(results[0], asyncio.CancelledError)
    assert finalized.is_set() and released == ["done"] and notified == []
    assert failure.__traceback__ is None
    if chained:
        assert failure.__cause__.__traceback__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["raw", "empty-raw", "none", "empty-lease", "inline", "inline-bytearray", "inline-memoryview"])
async def test_raw_none_inline_and_empty_leased_readers_keep_existing_behavior(ingestion, monkeypatch, kind):
    destination, message = ingestion
    read, released = [], []

    async def release():
        released.append("done")

    async def reader(_file, _client):
        read.append("called")
        if kind == "empty-lease":
            return lease_type()(b"", release)
        return None if kind == "none" else b"" if kind == "empty-raw" else b"Read bytes"

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    file = {"type": "image"}
    if kind.startswith("inline"):
        file["_content"] = {"inline": b"Inline bytes", "inline-bytearray": bytearray(b"Inline bytes"), "inline-memoryview": memoryview(b"Inline bytes")}[kind]
    result = await manager._ingest_inbound_files("thread", message(file), user_id="owner")
    assert "_content" not in file
    if kind == "none":
        assert result == [] and list(destination.iterdir()) == []
    else:
        wanted = b"Inline bytes" if kind.startswith("inline") else b"" if kind in {"empty-lease", "empty-raw"} else b"Read bytes"
        assert result == [{"filename": "msg_0.png", "size": len(wanted), "path": "/mnt/user-data/uploads/msg_0.png", "is_image": True}]
        assert (destination / "msg_0.png").read_bytes() == wanted
    assert read == ([] if kind.startswith("inline") else ["called"])
    assert released == (["done"] if kind == "empty-lease" else [])


@pytest.mark.asyncio
async def test_cancelled_manager_drains_started_failure_notification_before_releasing_lease(ingestion, monkeypatch):
    _destination, message = ingestion
    entered, finish = asyncio.Event(), asyncio.Event()
    read, released, notified = [], [], []

    async def release():
        released.append("done")

    async def reader(file, _client):
        read.append(file["filename"])
        return lease_type()(b"Charged file", release)

    def write(_base, _name, _data):
        raise OSError("write failed")

    async def notify(_file, _error):
        entered.set()
        await finish.wait()
        notified.append("done")

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "lease-test", reader)
    monkeypatch.setitem(manager.INBOUND_FILE_WRITE_FAILED, "lease-test", notify)
    monkeypatch.setattr(uploads, "write_upload_file_no_symlink", write)
    task = asyncio.create_task(manager._ingest_inbound_files("thread", message({"filename": "bad.txt"}, {"filename": "must-not-start.txt"})))
    try:
        await asyncio.wait_for(entered.wait(), 15)
        task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=0.05)
        assert not done and released == []
    finally:
        finish.set()
        results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    assert read == ["bad.txt"] and notified == ["done"] and released == ["done"]

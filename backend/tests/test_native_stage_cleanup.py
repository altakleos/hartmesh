"""Faults before ownership transfer must still close native stage resources."""

import asyncio
import errno
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway.native_directory_changes import NativeDirectoryChanges
from app.gateway.native_project_documents import stage_native_document


def test_private_directory_cleanup_failure_still_closes_owned_descriptors(tmp_path, monkeypatch):
    data = tmp_path / "data"
    control = tmp_path / "control"
    data.mkdir()
    control.mkdir()
    (data / "files").mkdir()
    control_fd = os.open(control, os.O_RDONLY | os.O_DIRECTORY)
    descriptors = []
    real_open, real_rmdir = os.open, os.rmdir

    def open_directory(relative, flags, **_kwargs):
        descriptor = real_open(data / relative, flags)
        descriptors.append(descriptor)
        return descriptor

    def record_owned(path, flags, **kwargs):
        descriptor = real_open(path, flags, **kwargs)
        descriptors.append(descriptor)
        return descriptor

    def publication_failed(*_args):
        raise RuntimeError("Publication failed")

    def cleanup_failed(path, **kwargs):
        if str(path).startswith("native-directory-"):
            raise OSError(errno.EIO, "Private cleanup failed")
        return real_rmdir(path, **kwargs)

    filesystem = SimpleNamespace(_control_fd=control_fd, _open=open_directory, _rename_no_replace=publication_failed)
    monkeypatch.setattr(os, "open", record_owned)
    monkeypatch.setattr(os, "rmdir", cleanup_failed)
    try:
        with pytest.raises(OSError, match="Private cleanup failed"):
            NativeDirectoryChanges(filesystem).ensure("files", "new", mode=0o755, check_capacity=lambda *_args, **_kwargs: None)
        assert len(descriptors) == 3
        for descriptor in descriptors:
            with pytest.raises(OSError) as closed:
                os.fstat(descriptor)
            assert closed.value.errno == errno.EBADF
    finally:
        os.close(control_fd)


@pytest.mark.asyncio
@pytest.mark.parametrize("close_error", [ValueError("Iterator close failed"), asyncio.CancelledError()])
async def test_iterator_close_failure_cleans_untransferred_document_stage(monkeypatch, close_error):
    stages = []

    class Stage:
        def __init__(self, *_args):
            self.closed = False
            self.private_path = "/private/stage"
            self.filesystem = object()
            stages.append(self)

        def check_capacity(self, _size):
            pass

        def write(self, _chunk):
            pass

        def close(self):
            self.closed = True

    # Exhaustion must succeed so the error happens specifically during aclose.
    class Iterator:
        def __init__(self):
            self.read = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.read:
                raise StopAsyncIteration
            self.read = True
            return b"document"

        async def aclose(self):
            raise close_error

    monkeypatch.setattr("app.gateway.native_project_documents.QualifiedUploadStage", Stage)
    with pytest.raises(type(close_error)):
        await stage_native_document(object(), object(), admission=AsyncMock(), project_id="project", chunks=Iterator(), max_bytes=1024)
    assert len(stages) == 1
    assert stages[0].closed

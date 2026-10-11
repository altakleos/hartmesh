"""Borrowed, pinned sources never reopen mutable conversation paths."""

import hashlib
import os

import pytest

from deerflow.files import store


def test_descriptor_source_survives_parent_replacement_and_repeated_digest_copy(tmp_path):
    parent = tmp_path / "conversation"
    parent.mkdir()
    path = parent / "report.txt"
    content = b"qualified conversation bytes"
    path.write_bytes(content)
    destination = tmp_path / "files"
    destination.mkdir()
    descriptor = os.open(path, os.O_RDONLY)
    try:
        source = store.DescriptorSource(descriptor, name=path.name)
        parent.rename(tmp_path / "retired")
        parent.mkdir()
        path.write_bytes(b"unqualified replacement bytes")
        digest, metadata = store.digest_and_stat(source)
        copied = store.copy_into(destination, source, name="report.txt", folder=None, folder_mode=0o770, file_mode=0o660)
        assert digest == copied.sha256 == hashlib.sha256(content).hexdigest()
        assert metadata.st_size == len(content)
        assert (destination / copied.path).read_bytes() == content
        assert os.fstat(descriptor).st_size == len(content)
    finally:
        os.close(descriptor)


def test_descriptor_source_readers_have_independent_offsets_and_leave_borrower_open(tmp_path):
    path = tmp_path / "source.txt"
    path.write_bytes(b"abcdef")
    descriptor = os.open(path, os.O_RDONLY)
    first = second = None
    try:
        os.lseek(descriptor, 3, os.SEEK_SET)
        source = store.DescriptorSource(descriptor, name=path.name)
        first = store.open_regular_source(source)
        second = store.open_regular_source(source)
        assert os.read(first, 2) == b"ab"
        assert os.read(second, 3) == b"abc"
        assert os.read(first, 2) == b"cd"
        assert os.lseek(descriptor, 0, os.SEEK_CUR) == 3
    finally:
        for held in (first, second, descriptor):
            if held is not None:
                os.close(held)


def test_descriptor_source_refuses_a_reused_descriptor_before_reading_foreign_bytes(tmp_path):
    original = tmp_path / "original.txt"
    original.write_bytes(b"original")
    foreign = tmp_path / "foreign.txt"
    foreign.write_bytes(b"foreign")
    descriptor = os.open(original, os.O_RDONLY)
    replacement = None
    try:
        source = store.DescriptorSource(descriptor, name=original.name)
        os.close(descriptor)
        descriptor = None
        replacement = os.open(foreign, os.O_RDONLY)
        os.dup2(replacement, source.descriptor)
        with pytest.raises(store.StoreError):
            store.open_regular_source(source)
        assert os.lseek(replacement, 0, os.SEEK_CUR) == 0
    finally:
        if replacement is not None:
            os.close(replacement)
            if replacement != source.descriptor:
                os.close(source.descriptor)
        elif descriptor is not None:
            os.close(descriptor)


def test_descriptor_source_refuses_a_directory(tmp_path):
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(store.StoreError):
            store.DescriptorSource(descriptor, name="folder")
    finally:
        os.close(descriptor)


def test_archive_uses_qualified_sources_without_reopening_replaced_paths(tmp_path):
    import zipfile

    from app.gateway.artifact_archive import build_artifact_archive

    path = tmp_path / "report.txt"
    path.write_bytes(b"pinned archive member")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        source = store.DescriptorSource(descriptor, name=path.name)
        path.rename(tmp_path / "old.txt")
        path.write_bytes(b"foreign replacement")
        virtual = "/mnt/user-data/outputs/report.txt"
        result = build_artifact_archive(None, [virtual], user_data_dir=None, descriptor_sources={virtual: source})
        try:
            with zipfile.ZipFile(result.file) as archive:
                assert archive.namelist() == ["report.txt"]
                assert archive.read("report.txt") == b"pinned archive member"
            assert result.input_bytes == 21
        finally:
            result.file.close()
        assert os.fstat(descriptor).st_size == 21
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("virtual", ["/mnt/user-data/files/report.txt", "/mnt/user-data/outputs/../report.txt", "/mnt/user-data/outputs/.artifact-edit-private", "/mnt/user-data/outputs/.browser-frames/report.txt"])
def test_archive_pinned_sources_preserve_archive_path_exclusions(tmp_path, virtual):
    from app.gateway.artifact_archive import ArtifactArchiveError, build_artifact_archive

    path = tmp_path / "report.txt"
    path.write_bytes(b"private")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        source = store.DescriptorSource(descriptor, name=path.name)
        with pytest.raises(ArtifactArchiveError):
            build_artifact_archive(None, [virtual], user_data_dir=None, descriptor_sources={virtual: source})
    finally:
        os.close(descriptor)


def test_archive_pinned_sources_refuse_an_unqualified_path(tmp_path):
    from app.gateway.artifact_archive import ArtifactArchiveError, build_artifact_archive

    path = tmp_path / "report.txt"
    path.write_bytes(b"not qualified")
    virtual = "/mnt/user-data/outputs/report.txt"
    with pytest.raises(ArtifactArchiveError):
        build_artifact_archive(None, [virtual], user_data_dir=None, descriptor_sources={virtual: path})


@pytest.mark.asyncio
async def test_project_chunk_reader_uses_the_pinned_source_not_replaced_path(tmp_path):
    from deerflow.projects.documents import read_file_chunks

    path = tmp_path / "guide.txt"
    path.write_bytes(b"pinned project guide")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        source = store.DescriptorSource(descriptor, name=path.name)
        path.rename(tmp_path / "old.txt")
        path.write_bytes(b"foreign path")
        chunks = [chunk async for chunk in read_file_chunks(source, chunk_size=4)]
        assert b"".join(chunks) == b"pinned project guide"
        assert os.fstat(descriptor).st_size == 20
    finally:
        os.close(descriptor)


@pytest.mark.asyncio
async def test_cancelled_project_reader_acquisition_closes_the_drained_descriptor(tmp_path, monkeypatch):
    import asyncio
    import threading

    from deerflow.projects import documents

    path = tmp_path / "guide.txt"
    path.write_bytes(b"held reader")
    descriptor = os.open(path, os.O_RDONLY)
    entered, release = threading.Event(), threading.Event()
    original_open = store.DescriptorSource.open
    owned = []

    def held_open(source):
        result = original_open(source)
        owned.append(result)
        entered.set()
        if not release.wait(5):
            os.close(result)
            raise AssertionError("The source acquisition was not released")
        return result

    monkeypatch.setattr(store.DescriptorSource, "open", held_open)
    iterator = documents.read_file_chunks(store.DescriptorSource(descriptor, path.name))
    task = asyncio.create_task(anext(iterator))
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(OSError):
            os.fstat(owned[0])
        assert os.fstat(descriptor).st_size == 11
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await iterator.aclose()
        os.close(descriptor)
        for opened in owned:
            try:
                os.close(opened)
            except OSError:
                pass


def test_pinned_digest_refuses_a_file_that_grows_during_hashing(tmp_path, monkeypatch):
    path = tmp_path / "changing.txt"
    path.write_bytes(b"original")
    descriptor = os.open(path, os.O_RDONLY)
    original_fdopen = os.fdopen

    class ChangingReader:
        def __init__(self, handle):
            self.handle = handle
            self.changed = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def read(self, size):
            chunk = self.handle.read(size)
            if chunk and not self.changed:
                self.changed = True
                path.write_bytes(b"original with more bytes")
            return chunk

    monkeypatch.setattr(os, "fdopen", lambda *args, **kwargs: ChangingReader(original_fdopen(*args, **kwargs)))
    try:
        with pytest.raises(store.StoreError):
            store.digest_and_stat(store.DescriptorSource(descriptor, path.name))
    finally:
        os.close(descriptor)

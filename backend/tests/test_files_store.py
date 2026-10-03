"""The primitives both file areas stand on: listing, copying, and what happens when either goes wrong.

``deerflow.files.store`` is the one place the person's own files
(``/mnt/user-data/files``) and the company's Shared area
(``/mnt/user-data/shared``) get their walking, copying and removing from.
These behaviours belong to neither area in particular, so they are tested
here once rather than in each.
"""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from deerflow.files import store


@pytest.mark.parametrize("copied", [False, True])
def test_copy_is_invisible_until_all_bytes_are_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, copied: bool) -> None:
    """Both empty and partly written copies stay outside the reader-visible namespace."""
    root = tmp_path / "root"
    root.mkdir()
    source = tmp_path / "source.txt"
    source.write_bytes(b"complete report")
    paused = threading.Event()
    resume = threading.Event()
    real_copy = store._copy_bytes

    def paused_copy(source_fd: int, target_fd: int) -> str:
        if copied:
            os.write(target_fd, b"complete")
            os.lseek(target_fd, 0, os.SEEK_SET)
        paused.set()
        assert resume.wait(10)
        return real_copy(source_fd, target_fd)

    monkeypatch.setattr(store, "_copy_bytes", paused_copy)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(store.copy_into, root, source, name="report.txt", folder=None, folder_mode=0o777, file_mode=0o666)
        try:
            assert paused.wait(10)
            assert store.list_under(root) == ([], False)
            with pytest.raises(FileNotFoundError):
                store.open_regular_source(root / "report.txt")
            for stage in root.iterdir():
                with pytest.raises(store.StoreError, match="plain name"):
                    store.resolve_under(root, stage.name)
        finally:
            resume.set()
        result = future.result(timeout=10)
    assert result.path == "report.txt"
    assert result.size == len(b"complete report")
    assert (root / result.path).read_bytes() == b"complete report"
    assert list(root.iterdir()) == [root / "report.txt"]


def test_listing_stops_at_the_ceiling_and_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A folder too big to render is cut short, and the caller is told, so nobody is shown a lie."""
    root = tmp_path / "root"
    root.mkdir()
    for index in range(5):
        (root / f"{index}.txt").write_bytes(b"x")
    monkeypatch.setattr(store, "MAX_LISTED_FILES", 3)

    listed, truncated = store.list_under(root)

    assert len(listed) == 3
    assert truncated is True
    assert [entry.path for entry in listed] == sorted(entry.path for entry in listed)


def test_a_copy_survives_a_race_on_the_chosen_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two writers picking the same free name both keep their bytes; neither overwrites the other."""
    root = tmp_path / "root"
    root.mkdir()
    source = tmp_path / "august.pdf"
    source.write_bytes(b"mine")
    real_link = os.link
    raced: list[str] = []

    def link_with_a_rival(source, target, **kwargs):
        # Somebody claims the final name after copying and listing, just
        # before publication. An overwrite-capable rename would lose it.
        if not raced:
            raced.append(target)
            rival = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666, dir_fd=kwargs["dst_dir_fd"])
            os.write(rival, b"rival")
            os.close(rival)
        return real_link(source, target, **kwargs)

    monkeypatch.setattr(store.os, "link", link_with_a_rival)

    stored = store.copy_into(root, source, name="august.pdf", folder=None, folder_mode=0o777, file_mode=0o666)

    assert stored.path == "august_1.pdf"
    assert (root / "august.pdf").read_bytes() == b"rival"
    assert (root / "august_1.pdf").read_bytes() == b"mine"
    assert sorted(path.name for path in root.iterdir()) == ["august.pdf", "august_1.pdf"]


def test_publication_failure_removes_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    root.mkdir()
    source = tmp_path / "source.txt"
    source.write_bytes(b"complete report")

    def broken_link(*_args, **_kwargs):
        raise OSError("links unavailable")

    monkeypatch.setattr(store.os, "link", broken_link)
    with pytest.raises(OSError, match="links unavailable"):
        store.copy_into(root, source, name="report.txt", folder=None, folder_mode=0o755, file_mode=0o644)
    assert list(root.iterdir()) == []


def test_copy_refuses_platform_without_atomic_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(store, "_ATOMIC_PUBLICATION", False)
    with pytest.raises(store.StoreError, match="Atomic file publication"):
        store.copy_into(tmp_path, tmp_path / "source.txt", name="report.txt", folder=None, folder_mode=0o755, file_mode=0o644)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("replacement", ["permissions", "owner"])
def test_replaced_staging_directory_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str) -> None:
    """A sandbox replacement between mkdir and open cannot supply the link source."""
    root = tmp_path / "root"
    root.mkdir()
    source = tmp_path / "source.txt"
    source.write_bytes(b"complete report")
    real_open = os.open
    real_fstat = os.fstat
    replaced_fd = None

    def replace_before_open(path, flags, *args, **kwargs):
        nonlocal replaced_fd
        if str(path).startswith(".copy-"):
            (root / path).rmdir()
            (root / path).mkdir(mode=0o777 if replacement == "permissions" else 0o700)
            fd = real_open(path, flags, *args, **kwargs)
            replaced_fd = fd
            return fd
        return real_open(path, flags, *args, **kwargs)

    def replaced_owner(fd):
        metadata = real_fstat(fd)
        if fd == replaced_fd and replacement == "owner":
            fields = list(metadata)
            fields[4] = os.geteuid() + 1
            return os.stat_result(fields)
        return metadata

    monkeypatch.setattr(store.os, "open", replace_before_open)
    monkeypatch.setattr(store.os, "fstat", replaced_owner)
    with pytest.raises(store.StoreError, match="Private staging"):
        store.copy_into(root, source, name="report.txt", folder=None, folder_mode=0o777, file_mode=0o666)
    assert list(root.iterdir()) == []


def test_a_failed_copy_leaves_nothing_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A disk that fills mid-copy leaves no half file for the next person to open."""
    root = tmp_path / "root"
    root.mkdir()
    source = tmp_path / "august.pdf"
    source.write_bytes(b"x" * 10)

    def broken_copy(_source_fd: int, target_fd: int):
        os.write(target_fd, b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(store, "_copy_bytes", broken_copy)

    with pytest.raises(OSError, match="disk full"):
        store.copy_into(root, source, name="august.pdf", folder=None, folder_mode=0o777, file_mode=0o666)

    assert list(root.iterdir()) == []


@pytest.mark.parametrize("operation", ["copy", "digest"])
def test_source_parent_replaced_after_preflight_is_refused(tmp_path: Path, operation: str) -> None:
    """Neither copying nor hashing may follow a parent installed after validation."""
    root = tmp_path / "root"
    root.mkdir()
    parent = root / "Reports"
    parent.mkdir()
    (parent / "result.txt").write_bytes(b"allowed")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "result.txt").write_bytes(b"outside")
    source = store.resolve_under(root, "Reports/result.txt")
    parent.rename(root / "original")
    parent.symlink_to(outside, target_is_directory=True)
    destination = tmp_path / "destination"
    destination.mkdir()

    with pytest.raises(store.StoreError):
        if operation == "copy":
            store.copy_into(destination, source, name="result.txt", folder=None, folder_mode=0o777, file_mode=0o666)
        else:
            store.digest_and_stat(source)
    assert list(destination.iterdir()) == []


def test_source_walk_remains_bound_to_open_parent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Replacing a directory after its descriptor is opened cannot redirect its leaf."""
    import hashlib

    parent = tmp_path / "Reports"
    parent.mkdir()
    (parent / "result.txt").write_bytes(b"allowed")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "result.txt").write_bytes(b"outside")
    real_open = os.open
    swapped = False

    def swap_after_open(path, flags, *args, **kwargs):
        nonlocal swapped
        fd = real_open(path, flags, *args, **kwargs)
        if str(path) == "Reports" and flags & os.O_DIRECTORY and not swapped:
            swapped = True
            parent.rename(tmp_path / "original")
            parent.symlink_to(outside, target_is_directory=True)
        return fd

    monkeypatch.setattr(store.os, "open", swap_after_open)
    digest, metadata = store.digest_and_stat(parent / "result.txt")
    assert swapped
    assert digest == hashlib.sha256(b"allowed").hexdigest()
    assert metadata.st_size == len(b"allowed")


def test_source_open_refuses_unsafe_platform_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "result.txt"
    source.write_bytes(b"allowed")
    monkeypatch.setattr(store, "_DIR_FD", False)
    with pytest.raises(store.StoreError, match="descriptor"):
        store.digest_and_stat(source)

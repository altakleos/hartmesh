"""The primitives both file areas stand on: listing, copying, and what happens when either goes wrong.

``deerflow.files.store`` is the one place the person's own files
(``/mnt/user-data/files``) and the company's Shared area
(``/mnt/user-data/shared``) get their walking, copying and removing from.
These behaviours belong to neither area in particular, so they are tested
here once rather than in each.
"""

import os
from pathlib import Path

import pytest

from deerflow.files import store


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
    real_open = os.open
    raced: list[str] = []

    def open_with_a_rival(path, flags, *args, **kwargs):
        # Somebody else claims the same name between the listing and the create.
        if not raced and str(path).endswith("august.pdf") and flags & os.O_EXCL:
            raced.append(str(path))
            if kwargs.get("dir_fd") is not None:
                rival = real_open(path, os.O_WRONLY | os.O_CREAT, 0o666, dir_fd=kwargs["dir_fd"])
            else:
                rival = real_open(path, os.O_WRONLY | os.O_CREAT, 0o666)
            os.write(rival, b"rival")
            os.close(rival)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(store.os, "open", open_with_a_rival)

    stored = store.copy_into(root, source, name="august.pdf", folder=None, folder_mode=0o777, file_mode=0o666)

    assert stored.path == "august_1.pdf"
    assert (root / "august.pdf").read_bytes() == b"rival"
    assert (root / "august_1.pdf").read_bytes() == b"mine"


def test_a_failed_copy_leaves_nothing_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A disk that fills mid-copy leaves no half file for the next person to open."""
    root = tmp_path / "root"
    root.mkdir()
    source = tmp_path / "august.pdf"
    source.write_bytes(b"x" * 10)

    def broken_copy(*_args, **_kwargs):
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

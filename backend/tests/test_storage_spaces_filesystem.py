"""Actual filesystem semantics; quota qualification has a separate real-volume tier."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
from pathlib import Path

import pytest

from deerflow.spaces.filesystem import ConfinedFilesystem, FileConflict, FilesystemUnavailable, FileTooLarge, UnsafeSpacePath

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="This confined filesystem adapter is explicitly Linux-only")


@pytest.fixture
def roots(tmp_path: Path):
    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    return data, control


@pytest.fixture
def fs(roots):
    with ConfinedFilesystem(*roots) as value:
        yield value


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_native_bytes_dotfiles_and_repository_need_no_manifest(fs, roots):
    data, _ = roots
    (data / ".git").mkdir()
    (data / ".git" / "HEAD").write_bytes(b"ref: refs/heads/main\n")
    (data / "binary").write_bytes(bytes(range(256)))
    entries, truncated = fs.list_directory()
    assert {entry.name for entry in entries} == {".git", "binary"}
    assert not truncated
    assert fs.read_bytes(".git/HEAD", max_bytes=100) == b"ref: refs/heads/main\n"
    assert fs.read_bytes("binary", max_bytes=256) == bytes(range(256))


def test_sqlite_native_application_can_reopen_its_database(fs, roots):
    data, _ = roots
    with sqlite3.connect(data / "wiki.sqlite") as connection:
        connection.execute("CREATE TABLE pages (title TEXT, body TEXT)")
        connection.execute("INSERT INTO pages VALUES (?, ?)", ("Home", "First page"))
    assert fs.read_bytes("wiki.sqlite", max_bytes=1 << 20).startswith(b"SQLite format 3\0")
    with sqlite3.connect(data / "wiki.sqlite") as connection:
        assert connection.execute("SELECT body FROM pages").fetchone() == ("First page",)


def test_relative_internal_links_are_readable_and_escaping_links_are_not(fs, roots):
    data, control = roots
    (data / "notes").mkdir()
    (data / "notes" / ".page").write_bytes(b"inside")
    (data / "link").symlink_to("notes/.page")
    (data / "notes" / "back").symlink_to("../link")
    (control / "private").write_bytes(b"private")
    (data / "escape").symlink_to("../control/private")
    (data / "absolute").symlink_to(data / "notes" / ".page")
    (data / "loop").symlink_to("loop")
    assert fs.read_bytes("link", max_bytes=20) == b"inside"
    assert fs.read_bytes("notes/back", max_bytes=20) == b"inside"
    for path in ("escape", "absolute", "loop"):
        with pytest.raises(UnsafeSpacePath):
            fs.read_bytes(path, max_bytes=20)
    entries, _ = fs.list_directory()
    by_name = {entry.name: entry for entry in entries}
    assert by_name["link"].kind == "symlink" and by_name["link"].accessible
    assert not by_name["escape"].accessible


@pytest.mark.parametrize("path", ["/etc/passwd", "..", "a/../x", "a//x", "./x", "x\0y"])
def test_resource_paths_reject_ambiguous_or_absolute_forms(fs, path):
    with pytest.raises(UnsafeSpacePath):
        fs.read_bytes(path, max_bytes=100)


def test_symlinked_ancestor_remains_confined(fs, roots):
    data, control = roots
    (data / "folder").mkdir()
    (data / "folder" / "page").write_bytes(b"visible")
    (data / "alias").symlink_to("folder", target_is_directory=True)
    assert fs.read_bytes("alias/page", max_bytes=20) == b"visible"
    (data / "alias").unlink()
    (data / "alias").symlink_to(control, target_is_directory=True)
    with pytest.raises(UnsafeSpacePath):
        fs.read_bytes("alias/page", max_bytes=20)
    with pytest.raises(UnsafeSpacePath):
        fs.write_atomic("alias/page", b"changed", expected_sha256=None, create=True)
    assert not (control / "page").exists()


def test_regular_descriptor_does_not_follow_a_later_path_replacement(fs, roots):
    data, _ = roots
    (data / "page").write_bytes(b"original")
    fd = fs.open_regular("page")
    try:
        (data / "page").rename(data / "old")
        (data / "page").write_bytes(b"replacement")
        assert os.read(fd, 20) == b"original"
    finally:
        os.close(fd)


def test_fifo_and_directories_are_refused_without_blocking(fs, roots):
    data, _ = roots
    os.mkfifo(data / "fifo")
    (data / "folder").mkdir()
    for path in ("fifo", "folder"):
        with pytest.raises(UnsafeSpacePath):
            fs.open_regular(path)


def test_bounded_reads_and_directory_listing(fs, roots):
    data, _ = roots
    for i in range(6):
        (data / str(i)).write_bytes(b"abcdef")
    entries, truncated = fs.list_directory(limit=3)
    assert len(entries) == 3 and truncated
    with pytest.raises(FileTooLarge):
        fs.read_bytes("0", max_bytes=5)
    assert fs.read_bytes("0", max_bytes=6) == b"abcdef"


def test_atomic_edit_preserves_mode_and_conflicts_preserve_bytes(fs, roots):
    data, control = roots
    (data / "page").write_bytes(b"before")
    (data / "page").chmod(0o640)
    assert fs.write_atomic("page", b"after", expected_sha256=digest(b"before")) == digest(b"after")
    assert (data / "page").read_bytes() == b"after"
    assert (data / "page").stat().st_mode & 0o777 == 0o640
    with pytest.raises(FileConflict):
        fs.write_atomic("page", b"stale", expected_sha256=digest(b"before"))
    assert (data / "page").read_bytes() == b"after"
    assert not list(control.glob("stage-*"))


def test_creation_is_explicit_and_existing_leaf_links_cannot_be_replaced(fs, roots):
    data, _ = roots
    fs.mkdir("notes")
    fs.write_atomic("notes/new", b"created", expected_sha256=None, create=True)
    with pytest.raises(FileConflict):
        fs.write_atomic("notes/new", b"duplicate", expected_sha256=None, create=True)
    with pytest.raises(FileNotFoundError):
        fs.write_atomic("notes/missing", b"edit", expected_sha256=digest(b"old"))
    (data / "link").symlink_to("notes/new")
    with pytest.raises(UnsafeSpacePath):
        fs.write_atomic("link", b"edit", expected_sha256=digest(b"created"))
    assert (data / "link").is_symlink()


def test_stage_failure_keeps_old_file_and_cleans_private_temporary(fs, roots, monkeypatch):
    data, control = roots
    (data / "page").write_bytes(b"before")
    original_write = os.write

    def full_disk(fd, content):
        raise OSError(28, "fixture full disk")

    monkeypatch.setattr(os, "write", full_disk)
    with pytest.raises(OSError, match="full disk"):
        fs.write_atomic("page", b"after", expected_sha256=digest(b"before"))
    monkeypatch.setattr(os, "write", original_write)
    assert (data / "page").read_bytes() == b"before"
    assert not list(control.glob("stage-*"))


def test_remove_and_rename_are_scoped_and_do_not_mutate_link_targets(fs, roots):
    data, control = roots
    (data / "page").write_bytes(b"bytes")
    fs.rename("page", "renamed")
    assert (data / "renamed").read_bytes() == b"bytes"
    (data / "link").symlink_to("renamed")
    fs.remove("link")
    assert (data / "renamed").read_bytes() == b"bytes"
    with pytest.raises(UnsafeSpacePath):
        fs.rename("renamed", "../control/stolen")
    assert not (control / "stolen").exists()
    fs.remove("renamed")
    assert not (data / "renamed").exists()


def test_control_root_must_be_outside_visible_data(roots):
    data, _ = roots
    nested = data / ".control"
    nested.mkdir()
    with pytest.raises(UnsafeSpacePath):
        ConfinedFilesystem(data, nested)


def test_held_backing_identity_rejects_a_replaced_root(roots):
    data, control = roots
    original = data.stat()
    data.rename(data.with_name("retired-data"))
    data.mkdir()
    with pytest.raises(FilesystemUnavailable, match="incarnation"):
        ConfinedFilesystem(data, control, expected_data=(original.st_dev, original.st_ino))

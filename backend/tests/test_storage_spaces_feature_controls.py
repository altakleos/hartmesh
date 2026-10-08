"""Feature copies must not expose private bytes through ordinary root browsing."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from deerflow.files import store
from deerflow.files.shared_removals import SharedMutationState
from deerflow.spaces.filesystem import ConfinedFilesystem


def test_copy_staging_lives_outside_the_visible_resource(tmp_path, monkeypatch):
    root, control = tmp_path / "data", tmp_path / "control"
    root.mkdir()
    control.mkdir(mode=0o700)
    source = tmp_path / "source"
    source.write_bytes(b"published bytes")
    started, resume = threading.Event(), threading.Event()
    original = store._copy_bytes

    def paused(source_fd, target_fd):
        started.set()
        assert resume.wait(10)
        return original(source_fd, target_fd)

    monkeypatch.setattr(store, "_copy_bytes", paused)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(store.copy_into, root, source, name="page.md", folder=None, folder_mode=0o755, file_mode=0o644, control_root=control)
        try:
            assert started.wait(10)
            with ConfinedFilesystem(root, control) as filesystem:
                assert filesystem.list_directory()[0] == []
            assert list(root.iterdir()) == []
            assert any(control.iterdir())
        finally:
            resume.set()
        result = future.result(timeout=10)
    assert result.path == "page.md"
    assert (root / "page.md").read_bytes() == b"published bytes"
    assert list(control.iterdir()) == []


@pytest.mark.parametrize("unsafe", ["visible", "nested", "public", "link"])
def test_copy_refuses_unqualified_private_control_location(tmp_path, unsafe):
    root = tmp_path / "data"
    root.mkdir()
    source = tmp_path / "source"
    source.write_bytes(b"private")
    if unsafe == "visible":
        control = root
    elif unsafe == "nested":
        control = root / "private"
        control.mkdir(mode=0o700)
    elif unsafe == "public":
        control = tmp_path / "public"
        control.mkdir(mode=0o755)
    else:
        target = tmp_path / "private"
        target.mkdir(mode=0o700)
        control = tmp_path / "link"
        control.symlink_to(target, target_is_directory=True)
    with pytest.raises(store.StoreError):
        store.copy_into(root, source, name="page.md", folder=None, folder_mode=0o755, file_mode=0o644, control_root=control)
    assert not (root / "page.md").exists()


def test_shared_removal_journal_is_outside_generic_root_browsing(tmp_path):
    root, control = tmp_path / "data", tmp_path / "control"
    root.mkdir()
    control.mkdir(mode=0o700)
    (root / "page.md").write_bytes(b"recoverable publication")
    state = SharedMutationState(root, control_root=control)
    state.acquire()
    try:
        pending = state.stage("page.md", "publication-1")
        try:
            assert list(root.iterdir()) == []
            assert not (root / ".shared-state").exists()
            pending.restore()
        finally:
            pending.close()
    finally:
        state.close()
    assert (root / "page.md").read_bytes() == b"recoverable publication"


def test_shared_control_refuses_unrelocated_legacy_private_state(tmp_path):
    root, control = tmp_path / "data", tmp_path / "control"
    root.mkdir()
    control.mkdir(mode=0o700)
    legacy = root / ".shared-state"
    legacy.mkdir(mode=0o700)
    journal = legacy / "retained-journal"
    journal.write_bytes(b"unknown outcome")
    state = SharedMutationState(root, control_root=control)
    try:
        with pytest.raises(store.StoreError, match="legacy"):
            state.acquire()
    finally:
        state.close()
    assert journal.read_bytes() == b"unknown outcome"

"""Owned filesystem fixtures verify removal handoffs and replay without data loss."""

import os

import pytest

from deerflow.files import shared_removals as removals
from deerflow.files.store import StoreError

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Shared staging requires descriptor-relative filesystem operations")


@pytest.fixture
def state(tmp_path):
    root = tmp_path / "shared"
    root.mkdir()
    owner = removals.SharedMutationState(root)
    owner.acquire()
    try:
        yield owner
    finally:
        owner.close()


def _restore_live(state):
    for name in state.pending_names():
        pending = state.open_removal(name)
        try:
            if pending.journal is None:
                pending.finish()
            else:
                pending.restore()
        finally:
            pending.close()


@pytest.mark.parametrize("boundary", ["journal", "rename", "parent_sync", "payload_sync"])
def test_stage_failure_keeps_original_or_recoverable_bytes(state, monkeypatch, boundary):
    target = state.root / "report.txt"
    target.write_bytes(b"owned fixture")
    moved, syncs, failed = False, 0, False
    real_rename, real_sync = os.rename, os.fsync
    real_journal = removals.PendingRemoval.write_journal

    def fail():
        nonlocal failed
        failed = True
        raise OSError("synthetic handoff failure")

    def rename(*args, **kwargs):
        nonlocal moved
        real_rename(*args, **kwargs)
        moved = True
        if boundary == "rename" and not failed:
            fail()

    def sync(fd):
        nonlocal syncs
        real_sync(fd)
        if moved:
            syncs += 1
            if not failed and ((boundary == "parent_sync" and syncs == 1) or (boundary == "payload_sync" and syncs == 2)):
                fail()

    def journal(pending):
        real_journal(pending)
        if boundary == "journal" and not failed:
            fail()

    with monkeypatch.context() as patch:
        patch.setattr(removals.os, "rename", rename)
        patch.setattr(removals.os, "fsync", sync)
        patch.setattr(removals.PendingRemoval, "write_journal", journal)
        with pytest.raises(OSError, match="synthetic handoff"):
            state.stage("report.txt", "owned-publication")
    assert failed
    _restore_live(state)
    assert target.read_bytes() == b"owned fixture"
    assert state.pending_names() == []


@pytest.mark.parametrize("boundary", ["payload", "journal.json", "directory"])
def test_committed_cleanup_replays_after_failure(state, monkeypatch, boundary):
    target = state.root / "report.txt"
    target.write_bytes(b"owned fixture")
    pending = state.stage("report.txt", "owned-publication")
    failed = False
    real_unlink, real_rmdir = os.unlink, os.rmdir

    def unlink(name, **kwargs):
        nonlocal failed
        real_unlink(name, **kwargs)
        if name == boundary and not failed:
            failed = True
            raise OSError("synthetic cleanup failure")

    def rmdir(name, **kwargs):
        nonlocal failed
        real_rmdir(name, **kwargs)
        if boundary == "directory" and not failed:
            failed = True
            raise OSError("synthetic cleanup failure")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(removals.os, "unlink", unlink)
            patch.setattr(removals.os, "rmdir", rmdir)
            with pytest.raises(OSError, match="synthetic cleanup"):
                pending.finish()
    finally:
        pending.close()
    assert failed
    for name in state.pending_names():
        replay = state.open_removal(name)
        try:
            replay.finish()
        finally:
            replay.close()
    assert not target.exists()
    assert state.pending_names() == []


@pytest.mark.parametrize("boundary", ["link", "parent_sync"])
def test_restoration_replays_without_overwriting_after_failure(state, monkeypatch, boundary):
    target = state.root / "report.txt"
    target.write_bytes(b"owned fixture")
    pending = state.stage("report.txt", "owned-publication")
    real_link, real_sync = os.link, os.fsync
    linked, failed = False, False

    def link(*args, **kwargs):
        nonlocal linked, failed
        real_link(*args, **kwargs)
        linked = True
        if boundary == "link" and not failed:
            failed = True
            raise OSError("synthetic restoration failure")

    def sync(fd):
        nonlocal failed
        real_sync(fd)
        if linked and boundary == "parent_sync" and not failed:
            failed = True
            raise OSError("synthetic restoration failure")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(removals.os, "link", link)
            patch.setattr(removals.os, "fsync", sync)
            with pytest.raises(OSError, match="synthetic restoration"):
                pending.restore()
    finally:
        pending.close()
    _restore_live(state)
    assert target.read_bytes() == b"owned fixture"
    assert state.pending_names() == []


def test_occupied_restoration_preserves_both_owned_files(state):
    target = state.root / "report.txt"
    target.write_bytes(b"original owned fixture")
    pending = state.stage("report.txt", "owned-publication")
    try:
        target.write_bytes(b"later owned fixture")
        with pytest.raises(StoreError, match="occupied"):
            pending.restore()
        assert target.read_bytes() == b"later owned fixture"
        assert (state.root / ".shared-state" / pending.name / "payload").read_bytes() == b"original owned fixture"
    finally:
        pending.close()


def test_payload_without_journal_is_retained(state):
    (state.root / "report.txt").write_bytes(b"owned fixture")
    pending = state.stage("report.txt", "owned-publication")
    name = pending.name
    pending.close()
    journal = state.root / ".shared-state" / name / "journal.json"
    journal.unlink()
    with pytest.raises(StoreError, match="no recovery journal"):
        state.open_removal(name)
    assert (journal.parent / "payload").read_bytes() == b"owned fixture"

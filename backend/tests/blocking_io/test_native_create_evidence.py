"""Journal operations are blocking; only readiness is a metadata-lock fact."""

import os

import pytest

from deerflow.spaces.native_create_evidence import NativeContainerIdentity, NativeCreateEvidence
from deerflow.utils.file_io import run_file_io

pytestmark = pytest.mark.asyncio


def make_store(tmp_path):
    pool = tmp_path / "pool"
    pool.mkdir(mode=0o750)
    (pool / ".native-creates").mkdir(mode=0o700)
    return NativeCreateEvidence(pool, daemon_id="daemon-A")


async def test_journal_readiness_does_no_io_on_the_loop(tmp_path, monkeypatch):
    store = await run_file_io(make_store, tmp_path)
    try:

        def forbidden(*args, **kwargs):
            raise AssertionError("readiness performed filesystem IO")

        with monkeypatch.context() as scoped:
            scoped.setattr(os, "fstat", forbidden)
            scoped.setattr(os, "stat", forbidden)
            assert store.ready
    finally:
        await run_file_io(store.close)


async def test_journal_lifecycle_is_offloaded_with_real_files(tmp_path):
    store = await run_file_io(make_store, tmp_path)
    try:
        attempt = await run_file_io(store.begin, owner_id="alice", thread_id="chat", name="native-chat", slots=("a" * 32,))
        assert await run_file_io(store.pending) == (attempt,)
        actual = NativeContainerIdentity(attempt.id, attempt.daemon_id, attempt.owner_id, attempt.thread_id, attempt.name, attempt.slots, "c" * 64)
        await run_file_io(store.acknowledge, attempt, actual)
        assert await run_file_io(store.pending) == ()
    finally:
        await run_file_io(store.close)


async def test_inline_journal_scan_trips_strict_detector(tmp_path):
    from blockbuster import BlockingError

    store = await run_file_io(make_store, tmp_path)
    try:
        with pytest.raises(BlockingError):
            store.pending()
    finally:
        await run_file_io(store.close)

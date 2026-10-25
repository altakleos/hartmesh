"""Actual disk snapshot caches and scans remain owned across retirement."""

import asyncio
import threading
from pathlib import Path

import pytest

from deerflow.config.paths import Paths
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.capture_io import CaptureIOCleanupError, CaptureIOClosed, CaptureIOOwner, capture_io_scope
from deerflow.workspace_changes import recorder


def prepare_paths(tmp_path, monkeypatch):
    paths = Paths(tmp_path)
    root = paths.sandbox_work_dir("thread", user_id="alice")
    root.mkdir(parents=True, exist_ok=True)
    (root / "draft.txt").write_text("before\n", encoding="utf-8")
    monkeypatch.setattr(recorder, "get_paths", lambda: paths)
    return root


@pytest.mark.asyncio
async def test_idle_snapshot_cache_is_removed_by_its_original_owner(tmp_path, monkeypatch):
    prepare_paths(tmp_path, monkeypatch)
    owner, before = CaptureIOOwner(), None
    try:
        with capture_io_scope(owner):
            before = await owner.run(lambda: recorder.capture_workspace_snapshot("thread", user_id="alice"))
        assert before.text_cache_dir and Path(before.text_cache_dir).is_dir()
        owner.retire()
        await owner.drain()
        assert not Path(before.text_cache_dir).exists(), "The returned snapshot left an idle disk cache outside its owner"
    finally:
        owner.retire()
        await owner.drain()
        if before is not None:
            await recorder._cleanup_snapshot_text_cache(before)


@pytest.mark.asyncio
@pytest.mark.parametrize("retire_between_scans", [True, False])
async def test_each_changed_workspace_scan_is_a_fresh_admission(tmp_path, monkeypatch, retire_between_scans):
    root = prepare_paths(tmp_path, monkeypatch)
    before = await recorder.capture_workspace_snapshot("thread", user_id="alice")
    (root / "draft.txt").write_text("after\n", encoding="utf-8")
    owner, events, calls = CaptureIOOwner(), MemoryRunEventStore(), []
    loop, original = asyncio.get_running_loop(), recorder.scan_workspace_roots

    def observed(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(kwargs["include_text"])
        if retire_between_scans and len(calls) == 1:
            loop.call_soon_threadsafe(owner.retire)
        return result

    monkeypatch.setattr(recorder, "scan_workspace_roots", observed)
    try:
        with capture_io_scope(owner):
            if retire_between_scans:
                with pytest.raises(CaptureIOClosed):
                    await recorder.record_workspace_changes(events, "thread", "run", before, user_id="alice")
            else:
                result = await recorder.record_workspace_changes(events, "thread", "run", before, user_id="alice")
                assert result is not None
        assert calls == ([False] if retire_between_scans else [False, True])
        assert len(await events.list_events("thread", "run")) == (0 if retire_between_scans else 1)
        assert not Path(before.text_cache_dir).exists()
    finally:
        owner.retire()
        await owner.drain()
        await recorder._cleanup_snapshot_text_cache(before)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_text", [False, True])
async def test_direct_public_snapshot_helper_refuses_retired_owner_before_scanning(tmp_path, monkeypatch, include_text):
    prepare_paths(tmp_path, monkeypatch)
    owner, calls, result = CaptureIOOwner(), [], None
    original = recorder.scan_workspace_roots

    def scan(*args, **kwargs):
        calls.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(recorder, "scan_workspace_roots", scan)
    owner.retire()
    try:
        with capture_io_scope(owner):
            with pytest.raises(CaptureIOClosed):
                result = await recorder.capture_workspace_snapshot("thread", user_id="alice", include_text=include_text)
        assert calls == []
    finally:
        await owner.drain()
        if result is not None:
            await recorder._cleanup_snapshot_text_cache(result)


@pytest.mark.asyncio
async def test_direct_metadata_scan_cancellation_retains_actual_worker(tmp_path, monkeypatch):
    prepare_paths(tmp_path, monkeypatch)
    owner = CaptureIOOwner()
    started, release, returned = threading.Event(), threading.Event(), threading.Event()
    original = recorder.scan_workspace_roots

    def scan(*args, **kwargs):
        started.set()
        assert release.wait(10)
        result = original(*args, **kwargs)
        returned.set()
        return result

    monkeypatch.setattr(recorder, "scan_workspace_roots", scan)
    with capture_io_scope(owner):
        pending = asyncio.create_task(recorder.capture_output_snapshot("thread", user_id="alice"))
    draining = None
    try:
        assert await asyncio.to_thread(started.wait, 3)
        pending.cancel()
        await asyncio.wait({pending}, timeout=0.01)
        pending.cancel()
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        done, _ = await asyncio.wait({pending, draining}, timeout=0.02)
        premature = bool(done)
        release.set()
        result, _ = await asyncio.gather(pending, draining, return_exceptions=True)
        assert await asyncio.to_thread(returned.wait, 3)
        assert isinstance(result, asyncio.CancelledError)
        assert not premature, "Direct middleware snapshot path detached a started filesystem scan"
    finally:
        release.set()
        await asyncio.gather(*(task for task in (pending, draining) if task is not None), return_exceptions=True)
        assert await asyncio.to_thread(returned.wait, 3)
        await owner.drain()


@pytest.mark.asyncio
async def test_secondary_cache_cleanup_failure_preserves_retirement_and_is_not_replayed(tmp_path, monkeypatch):
    root = prepare_paths(tmp_path, monkeypatch)
    owner, calls = CaptureIOOwner(), []
    with capture_io_scope(owner):
        before = await owner.run(lambda: recorder.capture_workspace_snapshot("thread", user_id="alice"))
    (root / "draft.txt").write_text("after\n", encoding="utf-8")
    original_remove = recorder.shutil.rmtree
    loop, original_scan = asyncio.get_running_loop(), recorder.scan_workspace_roots

    def fail_remove(*args, **kwargs):
        calls.append(args)
        raise OSError("private-cleanup-failure")

    def scan(*args, **kwargs):
        result = original_scan(*args, **kwargs)
        loop.call_soon_threadsafe(owner.retire)
        return result

    monkeypatch.setattr(recorder.shutil, "rmtree", fail_remove)
    monkeypatch.setattr(recorder, "scan_workspace_roots", scan)
    try:
        with capture_io_scope(owner):
            with pytest.raises(CaptureIOClosed):
                await recorder.record_workspace_changes(MemoryRunEventStore(), "thread", "run", before, user_id="alice")
        for _ in range(2):
            with pytest.raises(CaptureIOCleanupError) as error:
                await owner.drain()
            assert error.value.__context__ is None and error.value.__cause__ is None
        assert len(calls) == 1 and Path(before.text_cache_dir).exists()
    finally:
        owner.retire()
        await asyncio.gather(owner.drain(), return_exceptions=True)
        await asyncio.to_thread(original_remove, before.text_cache_dir, ignore_errors=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["prepare", "scan_cancel", "scan_failure"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_initial_snapshot_failure_uses_one_retained_cache_close(tmp_path, monkeypatch, stage, cleanup_fails):
    prepare_paths(tmp_path, monkeypatch)
    owner, paths, removals = CaptureIOOwner(), [], []
    entered, release = threading.Event(), threading.Event()
    prepare, scan, remove = recorder._prepare_capture, recorder.scan_workspace_roots, recorder.shutil.rmtree

    def prepare_observed(*args, **kwargs):
        result = prepare(*args, **kwargs)
        paths.append(result[1])
        if stage == "prepare":
            entered.set()
            assert release.wait(10)
        return result

    def scan_observed(*args, **kwargs):
        result = scan(*args, **kwargs)
        if stage == "scan_failure":
            raise ValueError("Controlled primary scanner failure")
        entered.set()
        assert release.wait(10)
        return result

    def remove_observed(path, *args, **kwargs):
        if Path(path) in paths:
            removals.append(path)
            if cleanup_fails:
                raise OSError("Controlled private cleanup failure")
        return remove(path, *args, **kwargs)

    monkeypatch.setattr(recorder, "_prepare_capture", prepare_observed)
    monkeypatch.setattr(recorder, "scan_workspace_roots", scan_observed)
    monkeypatch.setattr(recorder.shutil, "rmtree", remove_observed)
    with capture_io_scope(owner):
        pending = asyncio.create_task(recorder.capture_workspace_snapshot("thread", user_id="alice"))
    draining = None
    try:
        if stage != "scan_failure":
            assert await asyncio.to_thread(entered.wait, 3)
            pending.cancel()
            await asyncio.wait({pending}, timeout=0.01)
            pending.cancel()
        else:
            await asyncio.gather(pending, return_exceptions=True)
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        release.set()
        primary, cleanup = await asyncio.gather(pending, draining, return_exceptions=True)
        assert isinstance(primary, ValueError if stage == "scan_failure" else asyncio.CancelledError)
        assert isinstance(cleanup, CaptureIOCleanupError) if cleanup_fails else cleanup is None
        assert len(removals) == 1, "Initial snapshot cleanup bypassed its registered single-close lifetime"
        if cleanup_fails:
            with pytest.raises(CaptureIOCleanupError):
                await owner.drain()
            assert len(removals) == 1
    finally:
        release.set()
        owner.retire()
        await asyncio.gather(*(task for task in (pending, draining) if task is not None), return_exceptions=True)
        await asyncio.gather(owner.drain(), return_exceptions=True)
        for path in paths:
            await asyncio.to_thread(remove, path, ignore_errors=True)

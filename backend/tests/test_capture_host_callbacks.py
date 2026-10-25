"""Retirement reaches real host callbacks at each finite I/O boundary."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from test_conversation_access import _put, _setup

from app.gateway import chat_storage
from app.scheduler.service import ScheduledTaskService
from deerflow.config.paths import Paths, get_paths
from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope


@pytest.mark.asyncio
@pytest.mark.parametrize("retire_at", ["before", "visibility", "page", "open"])
async def test_conversation_reader_refuses_retirement_instead_of_returning_empty_history(retire_at, monkeypatch):
    prepare, events, threads, manager, _ = _setup()
    await threads.create("source", user_id="alice")
    for value in range(3):
        await _put(events, str(value))
    reader, _ = prepare(["source"])
    owner, page_calls = CaptureIOOwner(), []
    original_visibility = manager.list_successful_regenerate_sources
    original_page = events.list_messages

    async def visibility(*args, **kwargs):
        result = await original_visibility(*args, **kwargs)
        if retire_at == "visibility":
            owner.retire()
        return result

    async def page(*args, **kwargs):
        result = await original_page(*args, **kwargs)
        page_calls.append(kwargs)
        if retire_at == "page":
            owner.retire()
        return result

    monkeypatch.setattr(manager, "list_successful_regenerate_sources", visibility)
    monkeypatch.setattr(events, "list_messages", page)
    if retire_at == "before":
        owner.retire()
    try:
        with capture_io_scope(owner):
            if retire_at == "open":
                result = await reader(thread_id="source", limit=1)
                assert json.loads(result)["status"] == "ok"
            else:
                with pytest.raises(CaptureIOClosed):
                    await reader(thread_id="source", limit=1)
        assert len(page_calls) == (1 if retire_at in {"page", "open"} else 0)
        assert manager.list_edit_replay_visibility.await_count == (0 if retire_at in {"before", "visibility"} else 1)
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
async def test_chat_scope_keeps_original_task_tokens_and_blocks_retired_capacity(tmp_path, monkeypatch):
    owner, original_task, original_paths = CaptureIOOwner(), asyncio.current_task(), get_paths()
    checks = []

    class ObservedPaths(Paths):
        def check_thread_capacity(self, thread_id, *, user_id):
            checks.append((thread_id, user_id))
            (tmp_path / "checked").write_text("37", encoding="utf-8")

    prepared = SimpleNamespace(paths=ObservedPaths(base_dir=tmp_path), mounts=SimpleNamespace(user_id="alice"))

    async def prepare(*args, **kwargs):
        return prepared

    monkeypatch.setattr(chat_storage, "prepare_chat_storage", prepare)
    scope = chat_storage.chat_storage_scope_factory(SimpleNamespace())
    try:
        with capture_io_scope(owner):
            async with scope(user_id="alice", thread_id="thread"):
                assert asyncio.current_task() is original_task and get_paths() is prepared.paths
            assert get_paths() is original_paths
            owner.retire()
            with pytest.raises(CaptureIOClosed):
                await chat_storage.check_chat_thread_capacity(prepared, "late")
        assert checks == [("thread", "alice")]
        assert (tmp_path / "checked").read_text(encoding="utf-8") == "37"
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("retired", [True, False])
async def test_scheduler_completion_only_admits_its_terminal_repository_write(retired):
    owner, calls = CaptureIOOwner(), []

    class Repository:
        async def complete_run(self, *args, **kwargs):
            calls.append((args, kwargs))

    service = ScheduledTaskService(task_repo=Repository(), task_run_repo=None, launch_run=None, poll_interval_seconds=1, lease_seconds=30, max_concurrent_runs=1)
    record = SimpleNamespace(metadata={"scheduled_task_id": "task", "scheduled_task_run_id": "occurrence"}, user_id="alice", run_id="run", status=SimpleNamespace(value="success"), error=None)
    if retired:
        owner.retire()
    try:
        with capture_io_scope(owner):
            if retired:
                with pytest.raises(CaptureIOClosed):
                    await service.handle_run_completion(record)
            else:
                await service.handle_run_completion(record)
        assert len(calls) == (0 if retired else 1)
        if calls:
            assert calls[0][1]["only_if_active"] is True
    finally:
        owner.retire()
        await owner.drain()

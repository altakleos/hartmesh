"""Native branching owns quota-counted copies and cleans failed target creation."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_compose_chat_storage import pinned_chat_volume

from app.gateway import chat_storage
from deerflow.config.paths import Paths
from deerflow.spaces.chat_paths import ChatStoragePaths


def branch_fixture(tmp_path):
    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("source", user_id="alice")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("source", user_id="alice"))
    return volume, paths, prepared


@pytest.mark.asyncio
async def test_native_branch_copies_only_latest_conversation_files_with_empty_acp(tmp_path, monkeypatch):
    from app.gateway.chat_branch import NativeBranchClone

    volume, paths, prepared = branch_fixture(tmp_path)
    for leaf in ("workspace", "uploads", "outputs"):
        (paths.sandbox_user_data_dir("source", user_id="alice") / leaf / "keep.txt").write_bytes(leaf.encode())
    source = paths.sandbox_user_data_dir("source", user_id="alice")
    (source / "uploads/.upload-abcd.part").write_bytes(b"partial")
    (source / "outputs/link").symlink_to(tmp_path)
    (source / "other-private").write_bytes(b"not a mounted leaf")
    (paths.thread_dir("source", user_id="alice") / "acp-workspace/private.txt").write_bytes(b"not cloned")
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    clone = NativeBranchClone(prepared, "target")
    try:
        await clone.stage(SimpleNamespace(), "source")
        assert not paths.thread_dir("target", user_id="alice").exists()
        await clone.publish(SimpleNamespace())
        target = paths.thread_dir("target", user_id="alice")
        assert sorted(str(p.relative_to(target)) for p in target.rglob("*") if p.is_file()) == ["user-data/outputs/keep.txt", "user-data/uploads/keep.txt", "user-data/workspace/keep.txt"]
        assert list((target / "acp-workspace").iterdir()) == []
        assert clone.mode == "current_thread_best_effort"
    finally:
        await clone.close()
    assert list(volume.control_path.iterdir()) == []
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
async def test_native_branch_quota_failure_leaves_no_target_or_private_copy(tmp_path, monkeypatch):
    import errno

    from app.gateway.chat_branch import NativeBranchClone
    from deerflow.spaces.chat_paths import ChatFilesFull

    volume, paths, prepared = branch_fixture(tmp_path)
    (paths.sandbox_outputs_dir("source", user_id="alice") / "keep.txt").write_bytes(b"payload")
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    clone = NativeBranchClone(prepared, "target")
    monkeypatch.setattr("app.gateway.chat_branch.os.write", lambda *_: (_ for _ in ()).throw(OSError(errno.ENOSPC, "Full")))
    try:
        with pytest.raises(ChatFilesFull):
            await clone.stage(SimpleNamespace(), "source")
    finally:
        await clone.close()
    assert not paths.thread_dir("target", user_id="alice").exists()
    assert list(volume.control_path.iterdir()) == []
    assert (paths.sandbox_outputs_dir("source", user_id="alice") / "keep.txt").read_bytes() == b"payload"


@pytest.mark.asyncio
async def test_native_branch_revocation_before_publish_rolls_back_private_copy(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from app.gateway.chat_branch import NativeBranchClone

    volume, paths, prepared = branch_fixture(tmp_path)
    (paths.sandbox_outputs_dir("source", user_id="alice") / "keep.txt").write_bytes(b"payload")
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    clone = NativeBranchClone(prepared, "target")
    try:
        await clone.stage(SimpleNamespace(), "source")
        monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(side_effect=HTTPException(403, "Revoked")))
        with pytest.raises(HTTPException):
            await clone.publish(SimpleNamespace())
    finally:
        await clone.close()
    assert not paths.thread_dir("target", user_id="alice").exists()
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.parametrize("failure", [None, "checkpoint", "row", "final-admission"])
def test_native_branch_route_rolls_back_target_stores_and_files_on_failed_materialization(tmp_path, monkeypatch, failure):
    import asyncio

    from fastapi import HTTPException
    from fastapi.testclient import TestClient
    from test_threads_router import _build_thread_app, _seed_extension_source, _wire_extension_agent

    from app.gateway.routers import threads
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    volume, paths, prepared = branch_fixture(tmp_path)
    (paths.sandbox_outputs_dir("source", user_id="alice") / "report.txt").write_bytes(b"report")
    app, _, checkpointer = _build_thread_app()
    factory = _wire_extension_agent(monkeypatch, app, checkpointer, "full")
    events = MemoryRunEventStore()
    app.state.run_event_store = events
    stored = False

    async def prepare(*args):
        if failure == "final-admission" and stored:
            raise HTTPException(403, "User role revoked")
        return prepared

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepare)
    monkeypatch.setattr(threads, "_copy_branch_user_data", AsyncMock(side_effect=AssertionError("Native branch used legacy copy")))
    original_create = app.state.thread_store.create

    async def create(thread_id, **kwargs):
        nonlocal stored
        result = await original_create(thread_id, **kwargs)
        if thread_id != "source":
            stored = True
            if failure == "row":
                raise RuntimeError("Partial thread metadata write")
        return result

    monkeypatch.setattr(app.state.thread_store, "create", create)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/api/threads", json={"thread_id": "source", "assistant_id": "extension-agent"})
        assert response.status_code == 200, response.text
        asyncio.run(_seed_extension_source(checkpointer, factory, "full", "source"))
        if failure == "checkpoint":
            original = threads.build_checkpoint_state_mutation_accessor

            def builder(*args, **kwargs):
                accessor, config = original(*args, **kwargs)
                update = accessor.aupdate

                async def partial(*args, **kwargs):
                    await update(*args, **kwargs)
                    raise RuntimeError("Partial checkpoint write")

                accessor.aupdate = partial
                return accessor, config

            monkeypatch.setattr(threads, "build_checkpoint_state_mutation_accessor", builder)
        response = client.post("/api/threads/source/branches", json={"message_id": "a2"})
    target = app.state.run_manager.reservations[-1][0]
    assert target != "source", "A native branch must reserve its target before publication"
    assert (paths.sandbox_outputs_dir("source", user_id="alice") / "report.txt").read_bytes() == b"report"
    if failure is None:
        assert response.status_code == 200, response.text
        assert response.json()["workspace_clone_mode"] == "current_thread_best_effort"
        assert (paths.sandbox_outputs_dir(target, user_id="alice") / "report.txt").read_bytes() == b"report"
        assert asyncio.run(app.state.thread_store.get(target, user_id=None)) is not None
    else:
        assert response.status_code == (403 if failure == "final-admission" else 500), response.text
        assert not paths.thread_dir(target, user_id="alice").exists()
        assert asyncio.run(app.state.thread_store.get(target, user_id=None)) is None
        assert asyncio.run(checkpointer.aget_tuple({"configurable": {"thread_id": target, "checkpoint_ns": ""}})) is None
        assert asyncio.run(events.list_messages(target, limit=100)) == []
    assert list(volume.control_path.iterdir()) == []
    assert not (tmp_path / "legacy/users").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["file-changed", "same-device-mount", "cancelled-copy"])
async def test_native_branch_settles_private_tree_on_changed_source_or_cancellation(tmp_path, monkeypatch, failure):
    import asyncio
    import threading

    from app.gateway.chat_branch import NativeBranchClone
    from deerflow.spaces.filesystem import ConfinedFilesystem, FilesystemUnavailable, UnsafeSpacePath

    volume, paths, prepared = branch_fixture(tmp_path)
    output = paths.sandbox_outputs_dir("source", user_id="alice") / "report.txt"
    output.write_bytes(b"original")
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    clone = NativeBranchClone(prepared, "target")
    original_copy = clone._copy_file
    entered, finish = threading.Event(), threading.Event()
    if failure == "same-device-mount":
        original_open = ConfinedFilesystem._open

        def open_file(filesystem, path, flags, **kwargs):
            if path.endswith("/report.txt"):
                raise UnsafeSpacePath("NO_XDEV rejects a same-device mounted file")
            return original_open(filesystem, path, flags, **kwargs)

        monkeypatch.setattr(ConfinedFilesystem, "_open", open_file)
    else:

        def copy_file(*args):
            if failure == "file-changed":
                output.write_bytes(b"concurrent bytes")
            else:
                entered.set()
                assert finish.wait(5)
            return original_copy(*args)

        monkeypatch.setattr(clone, "_copy_file", copy_file)
    try:
        if failure == "cancelled-copy":
            task = asyncio.create_task(clone.stage(SimpleNamespace(), "source"))
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(0.01)
            assert entered.is_set()
            task.cancel()
            await asyncio.sleep(0.03)
            assert not task.done(), "Cancellation must drain the owned copy worker"
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(UnsafeSpacePath if failure == "same-device-mount" else FilesystemUnavailable):
                await clone.stage(SimpleNamespace(), "source")
    finally:
        finish.set()
        await clone.close()
    assert not paths.thread_dir("target", user_id="alice").exists()
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("no_user", [False, True])
async def test_historical_or_mountless_native_branch_never_copies_legacy_files(tmp_path, monkeypatch, no_user):
    from contextlib import asynccontextmanager

    from app.gateway.chat_branch import native_branch_scope

    _, paths, prepared = branch_fixture(tmp_path)
    if no_user:
        paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
        prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("source", user_id="alice"))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))

    @asynccontextmanager
    async def reserve(*args, **kwargs):
        yield

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(run_manager=SimpleNamespace(reserve_thread_operation=reserve))))
    async with native_branch_scope(request, "source", "target", latest=no_user, ordinary=True) as mode:
        assert mode == ("no_personal_files" if no_user else "skipped_historical_turn")
    assert not (tmp_path / "legacy/users").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["rename", "parent-flush", "control-flush"])
@pytest.mark.parametrize("error", ["ENOSPC", "EDQUOT"])
async def test_branch_publication_quota_failure_is_507_only_after_owned_clone_is_removed(tmp_path, monkeypatch, phase, error):
    import errno
    import os

    from app.gateway.chat_branch import NativeBranchClone
    from deerflow.spaces.chat_paths import ChatFilesFull

    volume, paths, prepared = branch_fixture(tmp_path)
    (paths.sandbox_outputs_dir("source", user_id="alice") / "report.txt").write_bytes(b"payload")
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    clone = NativeBranchClone(prepared, "target")
    await clone.stage(SimpleNamespace(), "source")
    if phase == "rename":
        original = clone.filesystem._rename_no_replace
        calls = 0

        def rename(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError(getattr(errno, error), "Quota exhausted at publication")
            return original(*args)

        monkeypatch.setattr(clone.filesystem, "_rename_no_replace", rename)
    else:
        original = os.fsync
        calls = 0

        def flush(*args):
            nonlocal calls
            calls += 1
            if calls == (1 if phase == "parent-flush" else 2):
                raise OSError(getattr(errno, error), "Quota exhausted at final flush")
            return original(*args)

        monkeypatch.setattr("app.gateway.chat_branch.os.fsync", flush)
    try:
        with pytest.raises(ChatFilesFull):
            await clone.publish(SimpleNamespace())
        assert not paths.thread_dir("target", user_id="alice").exists()
        assert list(volume.control_path.iterdir()) == []
    finally:
        await clone.close()


@pytest.mark.asyncio
async def test_branch_layout_modes_ignore_the_host_umask(tmp_path, monkeypatch):
    import os
    import stat

    from app.gateway.chat_branch import NativeBranchClone

    volume, paths, prepared = branch_fixture(tmp_path)
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    clone = NativeBranchClone(prepared, "target")
    old = os.umask(0o022)
    try:
        await clone.stage(SimpleNamespace(), "source")
        await clone.publish(SimpleNamespace())
        root = paths.thread_dir("target", user_id="alice")
        assert [stat.S_IMODE((root / relative).stat().st_mode) for relative in (".", "user-data", "acp-workspace", "user-data/workspace", "user-data/uploads", "user-data/outputs")] == [0o770] * 6
    finally:
        os.umask(old)
        await clone.close()
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.asyncio
async def test_branch_rollback_supports_legacy_event_store_owner_signature(monkeypatch):
    from app.gateway.chat_branch import _rollback_target

    deleted = []

    async def delete_events(thread_id):
        deleted.append(thread_id)

    state = SimpleNamespace(checkpointer=SimpleNamespace(adelete_thread=AsyncMock()), run_event_store=SimpleNamespace(delete_by_thread=delete_events), thread_store=SimpleNamespace(delete=AsyncMock()))
    request = SimpleNamespace(app=SimpleNamespace(state=state))
    await _rollback_target(request, "target", "alice")
    assert deleted == ["target"]

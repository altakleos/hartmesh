"""Conversation creation and host writes share a stable cross-process gate."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.config.paths import Paths
from deerflow.spaces.chat_paths import ChatStoragePaths


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupted", [False, True])
async def test_confined_conversation_removal_retries_private_ticket_and_preserves_siblings(tmp_path, monkeypatch, interrupted):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_cleanup
    from deerflow.spaces.contract import PrincipalRef

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "operational"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    paths.ensure_thread_dirs("chat-b", user_id="alice")
    (paths.sandbox_outputs_dir("chat-a", user_id="alice") / "report.txt").write_bytes(b"remove me")
    sibling = paths.sandbox_outputs_dir("chat-b", user_id="alice") / "keep.txt"
    sibling.write_bytes(b"keep me")
    files = volume.data_path / "files"
    files.mkdir()
    (files / "keep.txt").write_bytes(b"personal")
    cleanup = chat_cleanup.ConversationCleanup(None, PrincipalRef("human", "alice"), "chat-a", volume, None)
    monkeypatch.setattr(cleanup, "refresh", AsyncMock(return_value=volume))
    original = chat_cleanup.purge_private_tree
    calls = 0

    def remove(*args, **kwargs):
        nonlocal calls
        calls += 1
        if interrupted and calls == 1:
            raise OSError("Interrupted after private directory move")
        original(*args, **kwargs)

    monkeypatch.setattr(chat_cleanup, "purge_private_tree", remove)
    if interrupted:
        with pytest.raises(OSError):
            await cleanup.remove_files()
        assert len(list(volume.control_path.iterdir())) == 1
    assert await cleanup.remove_files()
    assert not paths.thread_dir("chat-a", user_id="alice").exists()
    assert sibling.read_bytes() == b"keep me"
    assert (files / "keep.txt").read_bytes() == b"personal"
    assert list(volume.control_path.iterdir()) == []


@pytest.mark.asyncio
async def test_native_delete_retains_creation_fence_through_metadata_cleanup(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from unittest.mock import Mock

    from app.gateway import chat_cleanup
    from app.gateway.routers import threads

    held = False
    calls = []

    @asynccontextmanager
    async def fence(*args, **kwargs):
        nonlocal held
        held = True
        calls.append("containers-absent")
        try:
            yield
        finally:
            calls.append("fence-closed")
            held = False

    async def remove():
        assert held
        calls.append("files-removed")
        return True

    async def logical(thread_id, request, *, native_response, cleanup_owner):
        assert held and cleanup_owner == "alice"
        calls.append("metadata-removed")
        return native_response

    cleanup = SimpleNamespace(_actor=SimpleNamespace(subject_id="alice"), paths=object(), remove_files=remove)
    monkeypatch.setattr(chat_cleanup, "prepare_conversation_cleanup", AsyncMock(return_value=cleanup))
    monkeypatch.setattr("app.gateway.agent_conversations.require_conversation", AsyncMock(return_value=False))
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_sandbox_provider", Mock(return_value=SimpleNamespace(conversation_cleanup_scope=fence)))
    monkeypatch.setattr(threads, "_delete_thread_data_impl", logical)
    result = await threads._delete_thread_data_with_reservation("chat-a", SimpleNamespace())
    assert result.success
    assert calls == ["containers-absent", "files-removed", "metadata-removed", "fence-closed"]


@pytest.mark.parametrize("failure", ["list", "inspect", "missing-inspection"])
def test_strict_cleanup_inventory_never_reports_daemon_failure_as_absence(monkeypatch, failure):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend

    backend = LocalContainerBackend.__new__(LocalContainerBackend)
    backend._runtime, backend._container_prefix, backend._network_mode = "docker", "sandbox", "open"
    result = SimpleNamespace(returncode=1 if failure == "list" else 0, stdout="" if failure == "list" else "sandbox-known", stderr="daemon unavailable")
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.subprocess.run", Mock(return_value=result))
    backend._batch_inspect = Mock(return_value={}) if failure == "missing-inspection" else Mock(side_effect=RuntimeError("Inspection unavailable"))
    with pytest.raises(RuntimeError):
        backend.list_running(strict=True)


def test_native_creation_gate_does_not_depend_on_policy_container_id(tmp_path):
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    first = AioSandboxProvider._thread_lock_path(paths, "chat-a", "old-policy", user_id="alice")
    second = AioSandboxProvider._thread_lock_path(paths, "chat-a", "new-policy", user_id="alice")
    assert first == second == paths.operational_thread_dir("chat-a", user_id="alice") / "conversation.lock"
    assert not (tmp_path / "legacy/users").exists()


@pytest.mark.asyncio
async def test_conversation_gate_cancellation_does_not_release_another_holder(tmp_path):
    from deerflow.spaces.chat_gate import conversation_gate

    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    entered = asyncio.Event()

    async def waiting():
        async with conversation_gate(paths, "chat-a", user_id="alice"):
            entered.set()

    async with conversation_gate(paths, "chat-a", user_id="alice"):
        task = asyncio.create_task(waiting())
        await asyncio.sleep(0.05)
        assert not entered.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        task = asyncio.create_task(waiting())
        await asyncio.sleep(0.05)
        assert not entered.is_set()
    await asyncio.wait_for(task, 2)
    assert entered.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [False, True])
@pytest.mark.parametrize("tracked", ["active", "warm", "discovered"])
async def test_conversation_cleanup_confirms_absence_and_preserves_held_borrowers(tmp_path, monkeypatch, held, tracked):
    from unittest.mock import Mock

    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
    from deerflow.sandbox.lease import discard_sandbox_lease_manager, get_sandbox_lease_manager
    from deerflow.spaces.backings import BackingUnavailable

    provider = _make_provider(tmp_path)
    client = Mock()
    client.requires_container_recycle = False
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.AioSandbox", Mock(return_value=client))
    info = SandboxInfo("sandbox-id", "http://localhost:8080", container_id="a" * 64, owner=("alice", "chat-a"))
    provider._register_created_sandbox("chat-a", "sandbox-id", info, user_id="alice")
    provider._backend = Mock(spec=LocalContainerBackend)
    provider._backend._runtime = "docker"
    provider._backend.destroy.return_value = DestroyOutcome.ABSENT
    provider._backend.list_running.side_effect = [[info], []]
    manager = get_sandbox_lease_manager(provider)
    if tracked == "warm":
        provider.release("sandbox-id")
    elif tracked == "discovered":
        provider._remove_tracked_sandbox("sandbox-id")
    if held:
        with manager._metadata_lock:
            manager._bind_locked("borrower", "sandbox-id", ("alice", "chat-a"), release_on_last=False)
    paths = ChatStoragePaths(Paths(tmp_path / "operational"), user_id="alice")
    try:
        if held:
            with pytest.raises(BackingUnavailable):
                async with provider.conversation_cleanup_scope(paths, "chat-a", user_id="alice"):
                    pytest.fail("Cleanup exposed files while a borrower retained the client")
            assert ("sandbox-id" in provider._sandboxes) == (tracked == "active")
            provider._backend.destroy.assert_not_called()
            assert client.close.call_count == int(tracked == "warm")
        else:
            async with provider.conversation_cleanup_scope(paths, "chat-a", user_id="alice"):
                assert not provider._sandboxes
                provider._backend.destroy.assert_called_once()
                assert provider._backend.list_running.call_count == 2
    finally:
        discard_sandbox_lease_manager(provider)
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("node", ["directory", "file"])
async def test_conversation_removal_refuses_nested_mounts_even_on_the_same_device(tmp_path, monkeypatch, retry, node):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_cleanup
    from deerflow.spaces import filesystem as filesystem_module
    from deerflow.spaces.contract import PrincipalRef
    from deerflow.spaces.filesystem import UnsafeSpacePath

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "operational"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    external = paths.sandbox_outputs_dir("chat-a", user_id="alice") / "external"
    if node == "directory":
        external.mkdir()
        preserved = external / "keep.txt"
    else:
        preserved = external
    preserved.write_bytes(b"external bytes")
    cleanup = chat_cleanup.ConversationCleanup(None, PrincipalRef("human", "alice"), "chat-a", volume, None)
    monkeypatch.setattr(cleanup, "refresh", AsyncMock(return_value=volume))
    if retry:
        original = chat_cleanup.purge_private_tree
        monkeypatch.setattr(chat_cleanup, "purge_private_tree", lambda *_: (_ for _ in ()).throw(OSError("Interrupted")))
        with pytest.raises(OSError):
            await cleanup.remove_files()
        monkeypatch.setattr(chat_cleanup, "purge_private_tree", original)
    original_open = filesystem_module._openat2

    def kernel_boundary(root, path, flags, **kwargs):
        if path.endswith("/outputs/external"):
            raise UnsafeSpacePath("Kernel NO_XDEV rejects a nested mount")
        return original_open(root, path, flags, **kwargs)

    monkeypatch.setattr(filesystem_module, "_openat2", kernel_boundary)
    with pytest.raises(UnsafeSpacePath):
        await cleanup.remove_files()
    ticket = next(volume.control_path.iterdir())
    remaining = ticket / "user-data/outputs/external"
    if node == "directory":
        remaining /= "keep.txt"
    assert remaining.read_bytes() == b"external bytes"


@pytest.mark.parametrize("identity", ["partial", "old-open", "unknown-role"])
def test_strict_inventory_refuses_ambiguous_candidates_instead_of_proving_absence(monkeypatch, identity):
    from unittest.mock import Mock

    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend, _ContainerInspection

    backend = LocalContainerBackend.__new__(LocalContainerBackend)
    backend._runtime, backend._container_prefix, backend._network_mode, backend._image = "docker", "sandbox", "blocked", "new-image"
    labels = {"deerflow.owner_user_id": "alice", "deerflow.thread_id": "chat-a"}
    if identity == "partial":
        labels["deerflow.sandbox_id"] = "known"
    elif identity == "unknown-role":
        labels.update({"deerflow.sandbox_id": "known", "deerflow.role": "unknown"})
    data = _ContainerInspection(0, None, labels, "old-image", frozenset(), container_id="a" * 64)
    backend._batch_inspect = Mock(return_value={"sandbox-known": data})
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.subprocess.run", Mock(return_value=SimpleNamespace(returncode=0, stdout="sandbox-known", stderr="")))
    with pytest.raises(RuntimeError, match="ambiguous"):
        backend.list_running(strict=True)


@pytest.mark.asyncio
async def test_private_cleanup_unlinks_symlink_without_touching_its_target(tmp_path, monkeypatch):
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_cleanup
    from deerflow.spaces.contract import PrincipalRef

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "operational"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"preserved")
    (paths.sandbox_outputs_dir("chat-a", user_id="alice") / "link").symlink_to(outside)
    cleanup = chat_cleanup.ConversationCleanup(None, PrincipalRef("human", "alice"), "chat-a", volume, None)
    monkeypatch.setattr(cleanup, "refresh", AsyncMock(return_value=volume))
    assert await cleanup.remove_files()
    assert outside.read_bytes() == b"preserved"
    assert list(volume.control_path.iterdir()) == []

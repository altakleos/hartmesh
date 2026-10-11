"""Storage preparation precedes every AIO acquisition and retained-client door."""

import asyncio
import threading
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.gateway.chat_storage import PreparedChatStorage
from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.config.paths import Paths, get_paths, paths_scope
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_bridge import ChatStorageHost
from deerflow.spaces.chat_paths import ChatStoragePaths


@pytest.mark.asyncio
async def test_preexisting_signal_handler_hands_bound_host_shutdown_to_gateway(tmp_path, monkeypatch):
    import signal

    provider, prepared = provider_fixture(tmp_path)
    host_pointer = [None]
    handlers = {}
    original = Mock()
    monkeypatch.setattr(signal, "getsignal", lambda signum: original)
    monkeypatch.setattr(signal, "signal", lambda signum, callback: handlers.update({signum: callback}))
    monkeypatch.setattr(provider, "_storage_host", lambda: host_pointer[0])
    provider.shutdown = Mock(side_effect=AssertionError("Owner loop joined a prewarm worker"))
    provider._register_signal_handlers()

    async def prepare(*args):
        return prepared

    host_pointer[0] = host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        original.assert_called_once_with(signal.SIGTERM, None)
        provider.shutdown.assert_not_called()
    finally:
        await host.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["shutdown", "reset"])
async def test_sync_lifecycle_refuses_owner_loop_before_joining_prewarm(tmp_path, operation):
    from deerflow_extension_api.storage import StorageUnsupported

    provider, prepared = provider_fixture(tmp_path)
    provider._shutdown_called = False
    provider._stop_prewarm_worker = Mock(side_effect=AssertionError("Owner loop joined its admission worker"))

    async def prepare(*args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        with pytest.raises(StorageUnsupported):
            getattr(provider, operation)()
        provider._stop_prewarm_worker.assert_not_called()
        assert provider._shutdown_called is False
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_reset_keeps_singleton_attached_while_queued_prewarm_needs_owner_loop(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from deerflow_extension_api.storage import StorageUnsupported
    from test_aio_sandbox_provider import _make_provider

    from deerflow.sandbox import sandbox_provider

    provider = _make_provider(tmp_path)
    provider._lock = threading.RLock()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def prepare(*args):
        entered.set()
        await finish.wait()
        return "prepared"

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    executor = provider._prewarm_executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(host.prepare_sync, "alice", "chat-a")
    monkeypatch.setattr(sandbox_provider, "_default_sandbox_provider", provider)
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(StorageUnsupported):
            sandbox_provider.reset_sandbox_provider()
        assert sandbox_provider.get_initialized_sandbox_provider() is provider
        assert not future.done()
        finish.set()
        assert await asyncio.to_thread(future.result, 2) == "prepared"
    finally:
        finish.set()
        await asyncio.to_thread(provider.reset)
        await host.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [False, True])
@pytest.mark.parametrize("replacement", ["profile", "identity"])
async def test_inprocess_wrong_mount_profile_is_replaced_only_without_holders(tmp_path, monkeypatch, held, replacement):
    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.aio_sandbox_provider import _ChatCandidate
    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.community.aio_sandbox.local_backend import ChatMountMismatch
    from deerflow.sandbox.lease import discard_sandbox_lease_manager, get_sandbox_lease_manager

    provider = _make_provider(tmp_path)
    provider._lock = threading.Lock()
    _, prepared = provider_fixture(tmp_path)
    client = Mock()
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.AioSandbox", Mock(return_value=client))
    info = SandboxInfo("sandbox-id", "http://localhost:8080", container_id="a" * 64)
    provider._register_created_sandbox("chat-a", "sandbox-id", info, user_id="alice")
    provider._thread_skill_projection_active = Mock(return_value=replacement == "identity")
    provider._sandbox_id_for_thread = Mock(return_value="new-sandbox-id" if replacement == "identity" else "sandbox-id")
    provider._backend = Mock(spec=LocalContainerBackend)
    provider._backend.is_alive.return_value = True
    provider._backend.destroy.return_value = DestroyOutcome.ABSENT
    provider._backend.validate_thread_mounts.side_effect = ChatMountMismatch("This chat is now mountless")
    manager = get_sandbox_lease_manager(provider)
    if held:
        with manager._metadata_lock:
            manager._bind_locked("holder", "sandbox-id", ("alice", "chat-a"), release_on_last=False, storage_candidate=_ChatCandidate(provider, "sandbox-id", client, info, "a" * 64, ("alice", "chat-a")))

    async def prepare(*args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        with paths_scope(prepared.paths):
            if held:
                with pytest.raises(BackingUnavailable):
                    await asyncio.to_thread(provider._reuse_in_process_sandbox, "chat-a", user_id="alice")
                assert provider._sandboxes["sandbox-id"] is client
                assert provider._cleanup_pending_for("sandbox-id") is None
                client.close.assert_not_called()
                provider._backend.destroy.assert_not_called()
            else:
                assert await asyncio.to_thread(provider._reuse_in_process_sandbox, "chat-a", user_id="alice") is None
                assert provider._counted_slots_locked() == (3, 0, 0, 0)
                client.close.assert_called_once()
                provider._backend.destroy.assert_called_once()
    finally:
        await host.close()
        discard_sandbox_lease_manager(provider)
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_newly_adopted_wrong_profile_is_cleaned_before_one_reacquire(tmp_path, monkeypatch):
    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.community.aio_sandbox.local_backend import ChatMountMismatch
    from deerflow.sandbox.lease import discard_sandbox_lease_manager

    provider = _make_provider(tmp_path)
    provider._lock = threading.Lock()
    _, prepared = provider_fixture(tmp_path)
    first, replacement = Mock(), Mock()
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.AioSandbox", Mock(side_effect=[first, replacement]))
    provider._backend = Mock(spec=LocalContainerBackend)
    provider._backend.destroy.return_value = DestroyOutcome.ABSENT

    async def prepare(*args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    registrations = []

    async def register(thread, *, user_id):
        registrations.append(thread)
        return provider._register_created_sandbox(thread, "sandbox-id", SandboxInfo("sandbox-id", "http://localhost:8080"), user_id=user_id)

    provider._acquire_internal_async = register
    provider.validate_storage_candidate_async = AsyncMock(side_effect=[ChatMountMismatch("Old mounts"), None])
    try:
        assert await provider.acquire_async("chat-a", user_id="alice") == "sandbox-id"
        assert registrations == ["chat-a", "chat-a"]
        assert provider._sandboxes["sandbox-id"] is replacement
        assert provider._counted_slots_locked() == (3, 1, 0, 0)
        first.close.assert_called_once()
        replacement.close.assert_not_called()
        provider._backend.destroy.assert_called_once()
    finally:
        await host.close()
        discard_sandbox_lease_manager(provider)
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_manager", [False, True])
async def test_cleanup_reservation_blocks_binding_without_blocking_gateway_loop(existing_manager):
    from deerflow.sandbox.lease import cleanup_unheld_sandbox, discard_sandbox_lease_manager, get_sandbox_lease_manager

    provider = Mock()
    if existing_manager:
        get_sandbox_lease_manager(provider)
    started, finish = threading.Event(), threading.Event()

    def destroy():
        started.set()
        assert finish.wait(5)

    task = asyncio.create_task(asyncio.to_thread(cleanup_unheld_sandbox, provider, "refused", destroy))
    try:
        assert await asyncio.to_thread(started.wait, 3)
        manager = get_sandbox_lease_manager(provider)
        await asyncio.wait_for(manager.retain_async("other-owner", "other-client", thread_id="other-chat", user_id="bob"), 1)
        with pytest.raises(RuntimeError, match="cleanup"):
            await asyncio.wait_for(manager.retain_async("new-owner", "refused", thread_id="chat-a", user_id="alice"), 1)
        assert not task.done()
    finally:
        finish.set()
        await task
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["refusal", "cancellation", "borrowed"])
async def test_failed_admission_cleans_real_registered_client(tmp_path, monkeypatch, failure):
    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.sandbox.lease import discard_sandbox_lease_manager, get_sandbox_lease_manager

    provider = _make_provider(tmp_path)
    provider._lock = threading.RLock()
    _, prepared = provider_fixture(tmp_path)
    provider._backend = Mock(spec=LocalContainerBackend)
    provider._backend.destroy.return_value = DestroyOutcome.ABSENT
    client = Mock()
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.AioSandbox", Mock(return_value=client))

    async def prepare(*args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)

    async def register(thread, *, user_id):
        return provider._register_created_sandbox(thread, "sandbox-id", SandboxInfo("sandbox-id", "http://localhost:8080"), user_id=user_id)

    provider._acquire_internal_async = register
    exception = asyncio.CancelledError() if failure == "cancellation" else BackingUnavailable("Admission changed")
    if failure == "borrowed":
        provider._backend.validate_thread_mounts.return_value = "a" * 64
        validate = provider.validate_storage_candidate_async

        async def bind_then_refuse(*args, **kwargs):
            token = await validate(*args, **kwargs)
            manager = get_sandbox_lease_manager(provider)
            with manager._metadata_lock:
                manager._bind_locked("borrower", "sandbox-id", ("alice", "child-chat"), release_on_last=False, storage_candidate=token)
            raise exception

        provider.validate_storage_candidate_async = bind_then_refuse
    else:
        provider.validate_storage_candidate_async = AsyncMock(side_effect=exception)
    try:
        with pytest.raises(type(exception)):
            await provider.acquire_async("chat-a", user_id="alice")
        if failure == "borrowed":
            client.close.assert_not_called()
            provider._backend.destroy.assert_not_called()
            await get_sandbox_lease_manager(provider).release_async("borrower")
        assert not provider._sandboxes and not provider._sandbox_infos and not provider._thread_sandboxes
        assert provider._counted_slots_locked() == (3, 0, 0, 0)
        client.close.assert_called_once()
        provider._backend.destroy.assert_called_once()
    finally:
        await host.close()
        discard_sandbox_lease_manager(provider)
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.parametrize("role", ["files", "shared"])
@pytest.mark.parametrize("target", ["external", "internal"])
def test_invalid_data_mount_never_reaches_container_create(tmp_path, monkeypatch, role, target):
    from test_aio_sandbox_provider import _make_provider
    from test_compose_chat_storage import pinned_chat_volume

    from deerflow.spaces.filesystem import UnsafeSpacePath

    user = pinned_chat_volume(tmp_path / "user")
    company = pinned_chat_volume(tmp_path / "company")
    (user.data_path / "files").mkdir()
    (company.data_path / "shared").mkdir()
    selected = user if role == "files" else company
    source = selected.data_path / role
    source.rmdir()
    destination = tmp_path / "outside" if target == "external" else selected.data_path / "other"
    destination.mkdir()
    source.symlink_to(destination, target_is_directory=True)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=user, company=company)
    provider = _make_provider(tmp_path)
    provider._backend = Mock(spec=LocalContainerBackend)
    provider._configured_skills_container_path = Mock(return_value="/mnt/skills")
    provider._backend.create.side_effect = AssertionError("Unsafe source reached container creation")
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(storage_spaces=SimpleNamespace(enabled=True)))
    try:
        with paths_scope(paths), pytest.raises((UnsafeSpacePath, OSError)):
            provider._create_sandbox("chat-a", "sandbox-id", user_id="alice")
        provider._backend.create.assert_not_called()
        assert list(destination.iterdir()) == []
    finally:
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


class Serial:
    def hold(self, *_args):
        return nullcontext()

    @asynccontextmanager
    async def hold_async(self, *_args):
        yield


def provider_fixture(tmp_path):
    provider = AioSandboxProvider.__new__(AioSandboxProvider)
    provider._acquire_serializer = Serial()
    provider._lock = threading.RLock()
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._active_sandbox_identity = {}
    provider._config = {}
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    prepared = PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"), "space-not-assigned")
    return provider, prepared


@pytest.mark.asyncio
async def test_async_acquire_prepares_current_scope_before_any_cache_fastpath(tmp_path):
    provider, prepared = provider_fixture(tmp_path)
    calls = []

    async def prepare(user, thread):
        calls.append((user, thread))
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)

    async def cached(thread, *, user_id):
        assert get_paths() is prepared.paths
        return "sandbox-id"

    provider._acquire_internal_async = cached
    provider.validate_storage_candidate_async = AsyncMock(return_value=None)
    try:
        assert await provider.acquire_async("chat-a", user_id="alice") == "sandbox-id"
        assert calls == [("alice", "chat-a")]
        assert not (tmp_path / "legacy").exists()
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_prewarm_prepares_before_submission(tmp_path):
    provider, prepared = provider_fixture(tmp_path)
    calls = []

    async def prepare(*args):
        calls.append("prepared")
        return prepared

    def submit(*args, **kwargs):
        assert get_paths() is prepared.paths
        calls.append("submitted")
        return None

    provider._submit_prewarm = submit
    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        assert await provider.prewarm_async("chat-a", user_id="alice") is None
        assert calls == ["prepared", "submitted"]
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_prewarm_refusal_cleans_created_client_before_parking(tmp_path, monkeypatch):
    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.sandbox.lease import discard_sandbox_lease_manager

    provider = _make_provider(tmp_path)
    provider._lock = threading.RLock()
    _, prepared = provider_fixture(tmp_path)
    provider._backend = Mock(spec=LocalContainerBackend)
    provider._backend.discover.return_value = None
    provider._backend.destroy.return_value = DestroyOutcome.ABSENT
    provider._ensure_skills_projection = Mock()
    provider._sandbox_id_for_thread = Mock(return_value="sandbox-id")
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.AioSandbox", Mock(return_value=Mock()))

    def create(thread, sandbox_id, *, user_id, **kwargs):
        return provider._register_created_sandbox(thread, sandbox_id, SandboxInfo(sandbox_id, "http://localhost:8080"), user_id=user_id)

    provider._create_sandbox = create
    provider.validate_storage_candidate = Mock(side_effect=BackingUnavailable("Mounts no longer admitted"))

    async def prepare(*args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        with paths_scope(prepared.paths), pytest.raises(BackingUnavailable):
            await asyncio.to_thread(provider._prewarm, "chat-a", user_id="alice")
        assert not provider._sandboxes and not provider._warm_pool
        assert provider._counted_slots_locked() == (3, 0, 0, 0)
        provider._backend.destroy.assert_called_once()
    finally:
        await host.close()
        discard_sandbox_lease_manager(provider)
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_sync_worker_acquire_marshals_preparation_before_cache_fastpath(tmp_path):
    provider, prepared = provider_fixture(tmp_path)
    calls = []

    async def prepare(user, thread):
        calls.append((user, thread))
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)

    def cached(thread, *, user_id):
        assert get_paths() is prepared.paths
        return "sandbox-id"

    provider._acquire_internal = cached
    provider.validate_storage_candidate = Mock(return_value=None)
    try:
        assert await asyncio.to_thread(provider.acquire, "chat-a", user_id="alice") == "sandbox-id"
        assert calls == [("alice", "chat-a")]
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_retained_candidate_validation_detects_same_id_client_replacement(tmp_path):
    provider, prepared = provider_fixture(tmp_path)
    first, replacement = object(), object()
    info = SandboxInfo("sandbox-id", "http://localhost:8080", container_name="sandbox-id", container_id="a" * 64)
    provider._sandboxes["sandbox-id"] = first
    provider._sandbox_infos["sandbox-id"] = info
    provider._active_sandbox_identity["sandbox-id"] = ("alice", "chat-a")
    backend = provider._backend = Mock(spec=LocalContainerBackend)
    backend.validate_thread_mounts.return_value = "a" * 64

    async def prepare(*args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        token = await provider.validate_storage_candidate_async("sandbox-id", thread_id="chat-a", user_id="alice")
        assert provider.storage_candidate_is_current(token)
        provider._sandboxes["sandbox-id"] = replacement
        assert not provider.storage_candidate_is_current(token)
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_trusted_fork_validates_parent_identity_without_destroying_it(tmp_path):
    provider, prepared = provider_fixture(tmp_path)
    parent = object()
    provider._sandboxes["sandbox-id"] = parent
    provider._sandbox_infos["sandbox-id"] = SandboxInfo("sandbox-id", "http://localhost:8080", container_name="sandbox-id", container_id="a" * 64)
    provider._active_sandbox_identity["sandbox-id"] = ("alice", "chat-a")
    backend = provider._backend = Mock(spec=LocalContainerBackend)
    backend.validate_thread_mounts.return_value = "a" * 64
    calls = []

    async def prepare(user, thread):
        calls.append((user, thread))
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        token = await provider.validate_storage_candidate_async("sandbox-id", thread_id="child-chat", user_id="alice", allow_unscoped_borrow=True)
        assert calls == [("alice", "chat-a"), ("alice", "chat-a")] and provider.storage_candidate_is_current(token)
        assert provider._sandboxes["sandbox-id"] is parent
        assert backend.validate_thread_mounts.call_args.kwargs["mounts"].thread_id == "chat-a"
        with pytest.raises(BackingUnavailable):
            await provider.validate_storage_candidate_async("sandbox-id", thread_id="child-chat", user_id="bob", allow_unscoped_borrow=True)
        assert provider._sandboxes["sandbox-id"] is parent
    finally:
        await host.close()


def test_mountless_chat_uses_operational_lock_area_without_legacy_data(tmp_path):
    provider, prepared = provider_fixture(tmp_path)
    with paths_scope(prepared.paths):
        lock = provider._thread_lock_path(prepared.paths, "chat-a", "sandbox-id", user_id="alice")
    assert lock == tmp_path / "legacy/chat-runtime/alice/chat-a/conversation.lock"
    assert lock.parent.is_dir()
    assert not (tmp_path / "legacy/users").exists()


def test_storage_mount_renderer_omits_every_data_bind_without_user_space(tmp_path, monkeypatch):
    _provider, prepared = provider_fixture(tmp_path)
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(storage_spaces=SimpleNamespace(enabled=True)))
    with paths_scope(prepared.paths):
        assert AioSandboxProvider._get_thread_mounts("chat-a", user_id="alice") == []
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
async def test_retained_validation_rechecks_identity_after_slow_docker_inspection(tmp_path):
    from deerflow.spaces.contract import InvalidPrincipal

    provider, prepared = provider_fixture(tmp_path)
    provider._sandboxes["sandbox-id"] = object()
    provider._sandbox_infos["sandbox-id"] = SandboxInfo("sandbox-id", "http://localhost:8080", container_name="sandbox-id", container_id="a" * 64)
    provider._active_sandbox_identity["sandbox-id"] = ("alice", "chat-a")
    backend = provider._backend = Mock(spec=LocalContainerBackend)
    revoked = False

    def inspect(*args, **kwargs):
        nonlocal revoked
        revoked = True
        return "a" * 64

    backend.validate_thread_mounts.side_effect = inspect

    async def prepare(*args):
        if revoked:
            raise InvalidPrincipal("The account was disabled during Docker inspection")
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    try:
        with pytest.raises(InvalidPrincipal):
            await provider.validate_storage_candidate_async("sandbox-id", thread_id="chat-a", user_id="alice")
    finally:
        await host.close()

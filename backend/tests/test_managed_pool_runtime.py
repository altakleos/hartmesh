"""Private managed ownership precedes provider construction and native effects."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.runtime_owner import PoolRuntimeOwner


@pytest.fixture
def pool(tmp_path):
    root = tmp_path / "pool"
    root.mkdir(mode=0o750)
    (root / ".gateway-owner.lock").touch(mode=0o600)
    (root / ".native-creates").mkdir(mode=0o700)
    return root


def files(pool, owner):
    return SimpleNamespace(inventory_path=pool / "inventory.json", _managed_runtime_required=True, _pool_runtime_owner=owner)


def qualify_native(provider, owner):
    evidence = owner.qualify_native_evidence(provider, daemon_id="fixture-host")
    provider._install_native_creation(evidence, SimpleNamespace(host_id="fixture-host", _same_host=Mock()))


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_owner", [False, True])
async def test_managed_qualification_refuses_before_baseline_lookup(pool, monkeypatch, missing_owner):
    from deerflow.agent_instances.runtime import qualified_attachments

    held, peer = PoolRuntimeOwner(pool), PoolRuntimeOwner(pool)
    held.acquire()
    initialized = Mock(side_effect=AssertionError("Provider lookup preceded pool ownership"))
    getter = Mock(side_effect=AssertionError("Provider construction preceded pool ownership"))
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", initialized)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_baseline_sandbox_provider", getter)
    try:
        with pytest.raises(StorageHostUnqualified):
            await qualified_attachments(files(pool, None if missing_owner else peer))
        initialized.assert_not_called()
        getter.assert_not_called()
    finally:
        held.close()
        peer.close()


@pytest.mark.asyncio
async def test_managed_probe_retry_reuses_exact_provider_and_owned_runner(pool, monkeypatch):
    from deerflow.agent_instances import runtime
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend

    owner = PoolRuntimeOwner(pool)
    provider = AioSandboxProvider.__new__(AioSandboxProvider)
    provider._lock = threading.RLock()
    provider._backend = LocalContainerBackend.__new__(LocalContainerBackend)
    provider._reconcile_orphans = Mock()
    getter = Mock(return_value=provider)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_baseline_sandbox_provider", getter)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", lambda: None)
    calls = []
    adapter = SimpleNamespace(host_id="host", _same_host=Mock())

    def probe(backend, **kwargs):
        assert backend is provider._backend and owner.accepts(provider)
        assert backend._runtime_runner == owner.run
        calls.append(kwargs)
        if len(calls) == 1:
            raise StorageHostUnqualified("Probe not available yet")
        return adapter

    from deerflow.spaces.docker import DockerStorageAdapter

    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", probe)
    monkeypatch.setattr(runtime, "SpaceAttachments", lambda _files, _adapter: SimpleNamespace(provider=_adapter))
    storage = files(pool, owner)
    try:
        with pytest.raises(StorageHostUnqualified, match="Probe not available"):
            await runtime.qualified_attachments(storage)
        assert owner.provider is provider
        baseline, _mounts = await runtime.qualified_attachments(storage)
        assert baseline is provider
        getter.assert_called_once()
        with pytest.raises(StorageHostUnqualified):
            await runtime.qualified_attachments(storage, baseline_provider=AioSandboxProvider.__new__(AioSandboxProvider))
        assert len(calls) == 2
    finally:
        owner.close()


def test_unqualified_managed_reconciliation_is_effect_free(monkeypatch):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider

    provider = AioSandboxProvider.__new__(AioSandboxProvider)
    provider._chat_storage_host = SimpleNamespace(pool_runtime_owner=object())
    provider._backend = Mock()
    provider._backend.list_running.return_value = []
    provider._reconcile_orphans()
    provider._backend.list_running.assert_not_called()


@pytest.mark.asyncio
async def test_mountless_chat_remains_available_without_pool_ownership(tmp_path):
    from test_compose_chat_provider import provider_fixture

    from deerflow.spaces.chat_bridge import ChatStorageHost

    provider, prepared = provider_fixture(tmp_path)
    provider._managed_runtime_required = True

    async def prepare(*_args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare, pool_runtime_owner=PoolRuntimeOwner(tmp_path / "unavailable"))
    calls = []

    async def acquire(*_args, **_kwargs):
        calls.append("mountless")
        return "sandbox"

    from unittest.mock import AsyncMock

    provider._acquire_internal_async = acquire
    provider.validate_storage_candidate_async = AsyncMock(return_value=None)
    try:
        assert await provider.acquire_async("chat-a", user_id="alice") == "sandbox"
        assert calls == ["mountless"]
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_native_chat_refuses_unqualified_provider_before_cached_admission(tmp_path):
    from test_compose_chat_provider import provider_fixture

    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.chat_bridge import ChatStorageHost

    provider, prepared = provider_fixture(tmp_path)
    provider._managed_runtime_required = True
    prepared.paths._chat_user = object()

    async def prepare(*_args):
        return prepared

    host = provider._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), prepare, pool_runtime_owner=PoolRuntimeOwner(tmp_path / "unavailable"))
    provider._acquire_internal_async = Mock(side_effect=AssertionError("Unqualified provider reached cache"))
    try:
        with pytest.raises((StorageHostUnqualified, BackingUnavailable)):
            await provider.acquire_async("chat-a", user_id="alice")
        provider._acquire_internal_async.assert_not_called()
    finally:
        await host.close()


@pytest.mark.parametrize("replacement", [False, True])
def test_unqualified_peer_cannot_adopt_or_replace_native_container(tmp_path, replacement):
    from test_compose_chat_provider import provider_fixture

    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
    from deerflow.spaces.backings import BackingUnavailable

    provider, _prepared = provider_fixture(tmp_path)
    provider._managed_runtime_required = True
    provider._chat_storage_host = SimpleNamespace(pool_runtime_owner=PoolRuntimeOwner(tmp_path / "unavailable"))
    native = SandboxInfo("native", "http://localhost", container_id="a" * 64, requires_replacement=replacement, chat_storage_enabled=True)
    provider._backend = Mock()
    provider._publish_ownership = Mock(side_effect=AssertionError("Peer adopted a native container"))
    if replacement:
        assert provider._replace_incompatible_sandbox(native, 0) is False
    else:
        with pytest.raises((StorageHostUnqualified, BackingUnavailable)):
            provider._register_discovered_sandbox("chat-a", native, user_id="alice")
    provider._backend.destroy.assert_not_called()
    provider._publish_ownership.assert_not_called()


def test_mountless_names_cannot_collide_with_native_or_peer_names(tmp_path):
    from test_compose_chat_provider import provider_fixture

    from deerflow.config.paths import paths_scope

    first, prepared = provider_fixture(tmp_path)
    second, _ = provider_fixture(tmp_path)
    for provider, owner_id in ((first, "first"), (second, "second")):
        provider._managed_runtime_required = True
        provider._owner_id = owner_id
        provider._effective_acquire_user_id = lambda _user: "alice"
        provider._configured_skills_container_path = lambda: "/mnt/skills"
        provider._thread_skill_projection_active = lambda *_args: False
    with paths_scope(prepared.paths):
        mountless = first._sandbox_id_for_thread("chat-a", "alice")
        peer = second._sandbox_id_for_thread("chat-a", "alice")
    native = first._sandbox_id_for_thread("chat-a", "alice")
    assert len({mountless, peer, native}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("phase", ["created", "ready", "publication"])
async def test_post_create_proof_failure_keeps_exact_unknown_container(pool, tmp_path, monkeypatch, asynchronous, phase):
    import importlib
    import threading

    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.community.aio_sandbox.sandbox_info import PROVENANCE_CREATED, SandboxInfo
    from deerflow.config.paths import Paths, paths_scope

    module = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = _make_provider(tmp_path)
    provider._lock = threading.RLock()
    provider._managed_runtime_required = True
    runtime = PoolRuntimeOwner(pool)
    runtime.resolve_provider(lambda: provider)
    provider._pool_runtime_owner = runtime
    info = SandboxInfo("native", "http://native", container_id="a" * 64, provenance=PROVENANCE_CREATED)
    provider._backend = SimpleNamespace(create=Mock(), destroy=Mock())
    qualify_native(provider, runtime)
    for method in ("_lark_integration_active", "_lark_broker_active"):
        monkeypatch.setattr(provider, method, lambda *_args: False)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(provider, "_local_config_mount_exclusion_root", lambda *_args, **_kwargs: None)

    def invalidate():
        (pool / ".gateway-owner.lock").rename(pool / ".retained-owner")
        (pool / ".gateway-owner.lock").touch(mode=0o600)

    def create(*_args, **_kwargs):
        if phase == "created":
            invalidate()
        return info

    def ready(*_args, **_kwargs):
        if phase == "ready":
            invalidate()
        return True

    async def ready_async(*args, **kwargs):
        return ready(*args, **kwargs)

    provider._backend.create.side_effect = create
    if phase == "publication":
        publish = provider._publish_ownership
        publications = []

        def publish_then_invalidate(sandbox_id):
            publish(sandbox_id)
            publications.append(sandbox_id)
            if len(publications) == 2:
                invalidate()

        monkeypatch.setattr(provider, "_publish_ownership", publish_then_invalidate)
    monkeypatch.setattr(module, "wait_for_sandbox_ready", ready)
    monkeypatch.setattr(module, "wait_for_sandbox_ready_async", ready_async)
    try:
        with paths_scope(Paths(tmp_path / "legacy")), pytest.raises(StorageHostUnqualified):
            if asynchronous:
                await provider._create_sandbox_async("chat-a", "native", user_id="alice")
            else:
                await asyncio.to_thread(provider._create_sandbox, "chat-a", "native", user_id="alice")
        assert provider._warm_pool["native"][0] is info
        assert provider._cleanup_pending["native"] is DestroyOutcome.UNKNOWN
        assert "native" not in provider._starting and "native" not in provider._sandboxes
        provider._backend.destroy.assert_not_called()
    finally:
        runtime.close()
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.parametrize("closing", [False, True])
def test_closing_during_warm_ownership_roundtrip_prevents_promotion(pool, tmp_path, monkeypatch, closing):
    import threading

    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

    provider = _make_provider(tmp_path)
    provider._lock = threading.RLock()
    provider._managed_runtime_required = True
    runtime = PoolRuntimeOwner(pool)
    runtime.resolve_provider(lambda: provider)
    provider._pool_runtime_owner = runtime
    info = SandboxInfo("native", "http://native", container_id="a" * 64)
    provider._backend = SimpleNamespace()
    qualify_native(provider, runtime)
    provider._warm_pool["native"] = (info, 0)
    provider._warm_pool_identity["native"] = ("alice", "chat-a")
    monkeypatch.setattr(provider, "_check_tracked_sandbox_alive", lambda *_args: True)

    publications = []

    def changed(*_args):
        publications.append("native")
        if closing:
            runtime.mark_closing()
        else:
            (pool / ".gateway-owner.lock").rename(pool / ".retained-owner")
            (pool / ".gateway-owner.lock").touch(mode=0o600)

    monkeypatch.setattr(provider, "_publish_ownership", changed)
    try:
        with pytest.raises(StorageHostUnqualified):
            provider._reclaim_warm_pool_sandbox("chat-a", "native", user_id="alice")
        assert provider._warm_pool["native"][0] is info
        assert not provider._sandboxes
        assert publications == ["native"]
    finally:
        runtime.close()
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


def test_unqualified_conversation_cleanup_does_not_adopt_observed_native(tmp_path):
    import threading

    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

    provider = _make_provider(tmp_path)
    provider._lock = threading.RLock()
    provider._managed_runtime_required = True
    provider._backend = LocalContainerBackend.__new__(LocalContainerBackend)
    native = SandboxInfo("native", "http://native", container_id="a" * 64, owner=("alice", "chat-a"))
    provider._backend.list_running = Mock(return_value=[native])
    provider._claim_ownership = Mock(side_effect=AssertionError("Unqualified cleanup adopted native"))
    try:
        with pytest.raises(StorageHostUnqualified):
            provider._retire_conversation_containers(("alice", "chat-a"))
        provider._claim_ownership.assert_not_called()
        assert not provider._warm_pool
    finally:
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_outer_lifetime_joins_late_constructor_and_owned_shutdown_worker(pool):
    from threading import Event

    from app.gateway.pool_runtime import managed_pool_runtime

    app = SimpleNamespace(state=SimpleNamespace())
    config = SimpleNamespace(enabled=True, plan=object(), inventory_path=str(pool / "inventory.json"))
    entered, resume, stopping, stopped = Event(), Event(), Event(), Event()
    peer = PoolRuntimeOwner(pool)
    worker = None
    runtime = None
    observed = []

    def shutdown():
        assert runtime.owns(provider) and runtime.closing
        with pytest.raises(StorageHostUnqualified):
            peer.acquire()
        stopping.set()
        assert stopped.wait(5)
        observed.append("worker-drained")

    provider = SimpleNamespace(shutdown=shutdown)

    def construct():
        entered.set()
        assert resume.wait(5)
        return provider

    async def scenario():
        nonlocal worker, runtime
        async with managed_pool_runtime(app, config):
            runtime = app.state.pool_runtime_owner
            worker = asyncio.create_task(asyncio.to_thread(runtime.resolve_provider, construct))
            assert await asyncio.to_thread(entered.wait, 5)
        observed.append("owner-closed")

    task = asyncio.create_task(scenario())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(StorageHostUnqualified):
            peer.acquire()
        resume.set()
        assert await asyncio.to_thread(stopping.wait, 5)
        assert not task.done()
        with pytest.raises(StorageHostUnqualified):
            peer.acquire()
        stopped.set()
        await asyncio.wait_for(task, 5)
        with pytest.raises(StorageHostUnqualified):
            await worker
        peer.acquire()
        assert observed == ["worker-drained", "owner-closed"]
    finally:
        resume.set()
        stopped.set()
        await asyncio.gather(task, return_exceptions=True)
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
        peer.close()


@pytest.mark.asyncio
async def test_mountless_peer_leaves_native_untouched_while_owner_prepare_is_paused(pool, tmp_path):
    from test_compose_chat_provider import provider_fixture

    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
    from deerflow.spaces.chat_bridge import ChatStorageHost

    first, _ = provider_fixture(tmp_path)
    peer, prepared = provider_fixture(tmp_path)
    runtime = PoolRuntimeOwner(pool)
    runtime.resolve_provider(lambda: first)
    first._managed_runtime_required = peer._managed_runtime_required = True
    entered, resume = asyncio.Event(), asyncio.Event()
    native = SandboxInfo("native", "http://native", container_id="a" * 64, chat_storage_enabled=True)

    async def owner_prepare(*_args):
        entered.set()
        await resume.wait()
        return prepared

    async def peer_prepare(*_args):
        return prepared

    first_host = first._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), owner_prepare, pool_runtime_owner=runtime)
    peer_owner = PoolRuntimeOwner(pool)
    peer_host = peer._chat_storage_host = ChatStorageHost(asyncio.get_running_loop(), peer_prepare, pool_runtime_owner=peer_owner)
    peer._backend = Mock()
    peer._backend.discover.return_value = native
    peer._thread_lock_path = lambda *_args, **_kwargs: tmp_path / "peer.lock"
    peer._recheck_cached_sandbox = Mock(return_value=None)
    peer._publish_ownership = Mock(side_effect=AssertionError("Peer adopted native container"))
    paused = asyncio.create_task(first_host.prepare_async("alice", "chat-a"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(StorageHostUnqualified):
            peer_owner.acquire()
        async with peer._chat_scope_async("chat-a", "alice") as projection:
            assert not projection.paths.has_personal_files
            with pytest.raises(StorageHostUnqualified):
                await asyncio.to_thread(peer._discover_or_create_with_lock, "chat-a", "native", user_id="alice")
        assert native.container_id == "a" * 64 and not paused.done()
        peer._publish_ownership.assert_not_called()
        peer._backend.create.assert_not_called()
        peer._backend.destroy.assert_not_called()
        assert not peer._sandboxes
    finally:
        resume.set()
        await asyncio.gather(paused, return_exceptions=True)
        await first_host.close()
        await peer_host.close()
        runtime.close()
        peer_owner.close()

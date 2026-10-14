"""Gateway native qualification installs evidence/accounting before reconciliation."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_agent_instances import instances as instances
from test_aio_sandbox_provider import _make_provider
from test_storage_spaces_startup import startup as startup

from deerflow.spaces.docker import DockerStorageAdapter, StorageHostUnqualified
from deerflow.spaces.native_create_evidence import NativeCreateEvidence
from deerflow.spaces.runtime_owner import PoolRuntimeOwner


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    from threading import Lock

    from deerflow.agent_instances import runtime
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend

    pool = tmp_path / "pool"
    pool.mkdir(mode=0o750)
    (pool / ".gateway-owner.lock").touch(mode=0o600)
    (pool / ".native-creates").mkdir(mode=0o700)
    provider = _make_provider(tmp_path)
    provider._lock = Lock()
    provider._backend = LocalContainerBackend.__new__(LocalContainerBackend)
    provider._reconcile_orphans = Mock()
    owner = PoolRuntimeOwner(pool)
    storage = SimpleNamespace(inventory_path=pool / "inventory.json", _managed_runtime_required=True, _pool_runtime_owner=owner, attachments=None)
    getter = Mock(return_value=provider)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_baseline_sandbox_provider", getter)

    def attachments(files, adapter):
        mounts = SimpleNamespace(provider=adapter)
        files.attachments = mounts
        return mounts

    monkeypatch.setattr(runtime, "SpaceAttachments", attachments)
    adapter = SimpleNamespace(host_id="daemon-A", _same_host=Mock())
    try:
        yield pool, provider, owner, storage, adapter, getter
    finally:
        provider._acquire_serializer.close()
        provider._acquire_worker_executor.shutdown(wait=True)
        owner.close()


@pytest.mark.asyncio
async def test_pending_loaded_and_accounted_before_deliberate_reconciliation(fixture, monkeypatch):
    from deerflow.agent_instances.runtime import qualified_attachments

    pool, provider, owner, storage, adapter, getter = fixture
    journal = NativeCreateEvidence(pool, daemon_id="daemon-A")
    attempt = journal.begin(owner_id="alice", thread_id="chat", name="native-a", slots=("a" * 32,))
    journal.close()

    def probe(backend, **kwargs):
        assert owner.accepts(provider) and backend._managed_native_required
        assert not provider._pool_runtime_current()
        return adapter

    def reconciled():
        assert provider._pool_runtime_current()
        assert provider._native_creation.evidence is owner.native_create_evidence
        assert provider._native_creation.evidence.pending() == (attempt,)
        with provider._lock:
            assert provider._counted_slots_locked() == (3, 0, 0, 1)

    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", probe)
    provider._reconcile_orphans.side_effect = reconciled
    baseline, mounts = await qualified_attachments(storage)
    assert baseline is provider and mounts.provider is adapter
    getter.assert_called_once()
    provider._reconcile_orphans.assert_called_once()


@pytest.mark.asyncio
async def test_missing_journal_refuses_then_retry_keeps_provider_and_exact_helper(fixture, monkeypatch):
    from deerflow.agent_instances.runtime import qualified_attachments

    pool, provider, owner, storage, adapter, getter = fixture
    (pool / ".native-creates").rmdir()
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", lambda *args, **kwargs: adapter)
    with pytest.raises(StorageHostUnqualified):
        await qualified_attachments(storage)
    assert owner.provider is provider and owner.native_create_evidence is None
    assert not provider._pool_runtime_current()
    provider._reconcile_orphans.assert_not_called()
    (pool / ".native-creates").mkdir(mode=0o700)
    first, mounts = await qualified_attachments(storage)
    storage.attachments = mounts
    evidence, bridge = owner.native_create_evidence, provider._native_creation
    second, repeated = await qualified_attachments(storage)
    assert first is second is provider and repeated is mounts
    assert owner.native_create_evidence is evidence and provider._native_creation is bridge
    getter.assert_called_once()


@pytest.mark.asyncio
async def test_managed_qualification_retains_the_exact_native_containment_factory(fixture, monkeypatch):
    from deerflow.agent_instances.runtime import qualified_attachments

    _pool, provider, _owner, storage, adapter, _getter = fixture
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", lambda *_args, **_kwargs: adapter)
    _, mounts = await qualified_attachments(storage)
    factory = getattr(adapter, "_containment_scope", None)
    assert factory is not None
    assert factory.provider is provider and factory.files is storage and factory.adapter is adapter
    _, repeated = await qualified_attachments(storage)
    assert repeated is mounts and adapter._containment_scope is factory


@pytest.mark.asyncio
async def test_failed_first_journal_keeps_exact_attachments_and_qualified_adapter_on_retry(fixture, monkeypatch):
    from deerflow.agent_instances.runtime import qualified_attachments

    pool, provider, _owner, storage, adapter, _getter = fixture
    alternate = SimpleNamespace(host_id="daemon-A", _same_host=Mock())
    factory = Mock(side_effect=[adapter, alternate])
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", factory)
    (pool / ".native-creates").rmdir()
    with pytest.raises(StorageHostUnqualified):
        await qualified_attachments(storage)
    retained = storage.attachments
    assert retained is not None and retained.provider is adapter
    (pool / ".native-creates").mkdir(mode=0o700)
    _baseline, reused = await qualified_attachments(storage)
    assert reused is retained and storage._qualified_adapter is adapter
    assert provider._native_creation._check_daemon == adapter._same_host
    factory.assert_called_once()


@pytest.mark.asyncio
async def test_failed_requalification_disables_publication_without_switching_evidence(fixture, monkeypatch):
    from deerflow.agent_instances.runtime import qualified_attachments

    pool, provider, owner, storage, adapter, _getter = fixture
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", lambda *args, **kwargs: adapter)
    _baseline, mounts = await qualified_attachments(storage)
    storage.attachments = mounts
    evidence = owner.native_create_evidence
    (pool / ".native-creates").rename(pool / "retained-evidence")
    (pool / ".native-creates").mkdir(mode=0o700)
    with pytest.raises(StorageHostUnqualified):
        await qualified_attachments(storage)
    assert owner.native_create_evidence is evidence and evidence.ready
    assert not provider._pool_runtime_current()


@pytest.mark.asyncio
async def test_sql_startup_retains_pending_before_native_runtime_becomes_ready(fixture, startup, monkeypatch):
    from deerflow.agent_instances.runtime import qualified_attachments

    pool, provider, owner, storage, adapter, _getter = fixture
    ss, app, config, sf, catalog, _old_adapter, _agents = startup
    config.inventory_path = str(storage.inventory_path)
    catalog.data_disk = pool
    app.state.pool_runtime_owner = owner
    journal = NativeCreateEvidence(pool, daemon_id=adapter.host_id)
    attempt = journal.begin(owner_id="alice", thread_id="chat", name="native-a", slots=(next(iter(catalog.volumes)),))
    journal.close()
    monkeypatch.setattr(ss, "qualified_attachments", qualified_attachments)
    monkeypatch.setattr(DockerStorageAdapter, "from_local_backend", lambda *_args, **_kwargs: adapter)
    await ss.initialize_storage_spaces(app, config, session_factory=sf)
    assert app.state.agent_execution.host_id == adapter.host_id
    assert app.state.agent_execution.storage_reason is None
    assert provider._pool_runtime_current() and owner.native_create_evidence.pending() == (attempt,)
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("startup_failure", [False, True])
async def test_sql_shutdown_drains_held_ack_before_closing_journal(fixture, startup, tmp_path, monkeypatch, startup_failure):
    import asyncio
    from threading import Event

    from test_native_create_provider_cancellation import CONTAINER, LABELS, prepare, runner

    from app.gateway.pool_runtime import managed_pool_runtime, mark_pool_runtime_closing
    from deerflow.community.aio_sandbox.backend import DestroyOutcome
    from deerflow.community.aio_sandbox.sandbox_info import PROVENANCE_CREATED, SandboxInfo
    from deerflow.config.paths import Paths, paths_scope

    _pool, provider, _idle_owner, storage, _adapter, _getter = fixture
    _ss, app, config, sf, _catalog, _old_adapter, _agents = startup
    config.inventory_path = str(storage.inventory_path)
    entered, release, stopping = Event(), Event(), Event()
    peer = PoolRuntimeOwner(storage.inventory_path.parent)
    events, tasks, helpers = [], [], []
    provider._backend.destroy = Mock(return_value=DestroyOutcome.UNKNOWN)
    provider._backend.discover = Mock(return_value=None)
    monkeypatch.setattr(provider, "_ensure_skills_projection", lambda *_args: None)
    monkeypatch.setattr(provider, "_sandbox_id_for_thread", lambda *_args: "native")
    monkeypatch.setattr(provider, "_thread_lock_path", lambda *_args, **_kwargs: tmp_path / "native-shutdown.lock")

    def shutdown(*, expected_provider):
        assert expected_provider is provider and helpers[0].ready
        assert events == ["sql-disposed"]
        stopping.set()
        provider._stop_prewarm_worker()
        assert provider._prewarm_future.done()
        events.append("worker-drained")

    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.shutdown_sandbox_provider", shutdown)

    async def scenario():
        async with managed_pool_runtime(app, config):
            owner = app.state.pool_runtime_owner
            await asyncio.to_thread(owner.resolve_provider, lambda: provider)
            bridge = await asyncio.to_thread(prepare, provider, owner, monkeypatch)
            helpers.append(bridge.evidence)
            acknowledgment, close = bridge.evidence.acknowledge, bridge.evidence.close

            def held_ack(*args):
                assert bridge.evidence.ready
                entered.set()
                assert release.wait(5)
                assert bridge.evidence.ready
                return acknowledgment(*args)

            def closed():
                assert events == ["sql-disposed", "worker-drained"]
                events.append("journal-closed")
                close()

            monkeypatch.setattr(bridge.evidence, "acknowledge", held_ack)
            monkeypatch.setattr(bridge.evidence, "close", closed)
            run = runner(bridge)

            def create(*args, **kwargs):
                bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=run)
                return SandboxInfo("native", "http://native", container_name="native-a", container_id=CONTAINER, owner=("alice", "chat"), provenance=PROVENANCE_CREATED, chat_storage_enabled=True)

            provider._backend.create = create
            try:
                with paths_scope(Paths(tmp_path / "legacy")):
                    tasks.append(asyncio.create_task(provider.prewarm_async("chat", user_id="alice")))
                assert await asyncio.to_thread(entered.wait, 5)
                if startup_failure:
                    raise RuntimeError("partial startup")
            finally:
                mark_pool_runtime_closing(app)
                await sf.kw["bind"].dispose()
                events.append("sql-disposed")

    task = asyncio.create_task(scenario())
    try:
        assert await asyncio.to_thread(stopping.wait, 5)
        assert not task.done() and helpers[0].ready
        with pytest.raises(StorageHostUnqualified):
            await asyncio.to_thread(peer.acquire)
        release.set()
        if startup_failure:
            with pytest.raises(RuntimeError, match="partial startup"):
                await asyncio.wait_for(task, 5)
        else:
            await asyncio.wait_for(task, 5)
        assert events == ["sql-disposed", "worker-drained", "journal-closed"]
        assert not helpers[0].ready
        await asyncio.to_thread(peer.acquire)
    finally:
        release.set()
        await asyncio.gather(task, *tasks, return_exceptions=True)
        await asyncio.to_thread(peer.close)

"""Actual provider entry points reserve before preparation and drain owned work."""

import asyncio
import contextvars
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install

from deerflow.spaces.docker import StorageHostUnqualified

SLOT = "a" * 32


def prepared(provider, owner, monkeypatch):
    from deerflow.config.paths import Paths

    paths = Paths(owner.pool.parent / "test-paths")
    paths.check_thread_capacity = Mock()
    value = SimpleNamespace(paths=paths, mounts=SimpleNamespace(labels={}, user_id="alice", thread_id="chat"))
    host = SimpleNamespace(pool_runtime_owner=owner, prepare_sync=Mock(return_value=value))
    provider._chat_storage_host = host
    monkeypatch.setattr(provider, "_fresh_projection", lambda result, *_args: result)
    monkeypatch.setattr(provider, "_assert_chat_runtime", lambda *_args: None)
    return host, value


def test_sync_host_preparation_is_already_reserved_without_provider_lock(fixture, monkeypatch):
    provider, owner = fixture
    install(provider, owner)
    host, value = prepared(provider, owner, monkeypatch)
    gate = provider._owner_admission_gate()

    def prepare(*args):
        assert not provider._lock.locked()
        with pytest.raises(StorageHostUnqualified):
            gate.reserve("alice", SLOT)
        return value

    host.prepare_sync.side_effect = prepare
    with provider._chat_scope("chat", "alice"):
        with pytest.raises(StorageHostUnqualified):
            gate.reserve("alice", SLOT)
    token = gate.reserve("alice", SLOT)
    gate.end_retirement(token)


def test_reserved_owner_refuses_before_host_preparation(fixture, monkeypatch):
    provider, owner = fixture
    install(provider, owner)
    host, _value = prepared(provider, owner, monkeypatch)
    gate = provider._owner_admission_gate()
    token = gate.reserve("alice", SLOT)
    try:
        with pytest.raises(StorageHostUnqualified):
            with provider._chat_scope("chat", "alice"):
                pytest.fail("Reserved native owner admitted")
        host.prepare_sync.assert_not_called()
    finally:
        gate.end_retirement(token)


def test_copied_expired_owner_context_refuses_before_host_preparation(fixture, monkeypatch):
    provider, owner = fixture
    install(provider, owner)
    host, _value = prepared(provider, owner, monkeypatch)
    with provider._owner_admission_scope("alice"):
        copied = contextvars.copy_context()

    def call():
        with provider._chat_scope("chat", "alice"):
            pytest.fail("Expired copied admission admitted")

    with pytest.raises(StorageHostUnqualified):
        copied.run(call)
    host.prepare_sync.assert_not_called()


@pytest.mark.asyncio
async def test_async_preparation_cancellation_drains_before_owner_reference_exit(fixture, monkeypatch):
    provider, owner = fixture
    install(provider, owner)
    host, value = prepared(provider, owner, monkeypatch)
    gate = provider._owner_admission_gate()
    entered, release = asyncio.Event(), asyncio.Event()

    async def prepare(*args):
        assert not provider._lock.locked()
        entered.set()
        await release.wait()
        return value

    host.prepare_async = prepare

    async def call():
        async with provider._chat_scope_async("chat", "alice"):
            pytest.fail("Cancelled preparation entered its caller")

    task = asyncio.create_task(call())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(StorageHostUnqualified):
            gate.reserve("alice", SLOT)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        token = gate.reserve("alice", SLOT)
        gate.end_retirement(token)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_foreign_owner_cannot_submit_to_reserved_slot_before_journal_or_cli(fixture):
    provider, owner = fixture
    bridge = install(provider, owner)
    gate = provider._owner_admission_gate()
    token = gate.reserve("alice", SLOT)
    labels = {"deerflow.owner_user_id": "bob", "deerflow.thread_id": "chat", "deerflow.sandbox_id": "native", "deerflow.role": "sandbox", "hartmesh.chat.storage": "on", "hartmesh.storage.space." + SLOT: "1"}
    runner = Mock(side_effect=AssertionError("Reserved slot submitted"))
    try:
        with provider._owner_admission_scope("bob"), pytest.raises(StorageHostUnqualified):
            bridge.submit(name="native-b", labels=labels, command=["docker", "run", "--name", "native-b", "image"], runner=runner)
        assert bridge.evidence.pending() == ()
        runner.assert_not_called()
    finally:
        gate.end_retirement(token)


def test_immediate_prewarm_completion_releases_reference_outside_provider_lock(fixture):
    from concurrent.futures import Future

    provider, owner = fixture
    install(provider, owner)
    future = Future()
    future.set_result(None)
    original = future.add_done_callback

    def register(callback):
        assert not provider._lock.locked()
        return original(callback)

    future.add_done_callback = register
    provider._prewarm_executor = SimpleNamespace(submit=lambda *_args, **_kwargs: future)
    admitted = provider._submit_prewarm("chat", user_id="alice")
    assert admitted[0] is future
    gate = provider._owner_admission_gate()
    token = gate.reserve("alice", SLOT)
    gate.end_retirement(token)


@pytest.mark.parametrize("completion", ["result", "queued_cancel", "submit_error"])
def test_prewarm_future_reference_lasts_until_actual_completion(fixture, completion):
    from concurrent.futures import Future

    provider, owner = fixture
    install(provider, owner)
    future = Future()

    def submit(*args, **kwargs):
        if completion == "submit_error":
            raise RuntimeError("Executor closed")
        return future

    provider._prewarm_executor = SimpleNamespace(submit=submit)
    with provider._owner_admission_scope("alice"):
        if completion == "submit_error":
            with pytest.raises(RuntimeError, match="Executor closed"):
                provider._submit_prewarm("chat", user_id="alice")
        else:
            provider._submit_prewarm("chat", user_id="alice")
    gate = provider._owner_admission_gate()
    if completion != "submit_error":
        with pytest.raises(StorageHostUnqualified):
            gate.reserve("alice", SLOT)
        if completion == "result":
            future.set_result(None)
        else:
            assert future.cancel()
    token = gate.reserve("alice", SLOT)
    gate.end_retirement(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_direct_create_retains_owner_through_ack_and_backend_return(fixture, tmp_path, monkeypatch, asynchronous):
    from threading import Event

    from test_native_create_provider_cancellation import CONTAINER, LABELS, prepare, runner

    from deerflow.community.aio_sandbox.sandbox_info import PROVENANCE_CREATED, SandboxInfo
    from deerflow.config.paths import Paths, paths_scope
    from deerflow.sandbox.lease import run_sync_lifecycle_operation

    provider, owner = fixture
    bridge = prepare(provider, owner, monkeypatch)
    gate = provider._owner_admission_gate()
    entered, release = Event(), Event()
    run = runner(bridge)

    def create(*args, **kwargs):
        bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=run)
        entered.set()
        assert release.wait(5)
        return SandboxInfo("native", "http://native", container_name="native-a", container_id=CONTAINER, owner=("alice", "chat"), provenance=PROVENANCE_CREATED, chat_storage_enabled=True)

    provider._backend.create = create
    with paths_scope(Paths(tmp_path / "legacy")):
        operation = provider._create_sandbox_async("chat", "native", user_id="alice") if asynchronous else run_sync_lifecycle_operation(provider._create_sandbox, "chat", "native", user_id="alice")
        task = asyncio.create_task(operation)
    reservation = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert bridge.evidence.pending() == ()
        with pytest.raises(StorageHostUnqualified):
            reservation = gate.reserve("alice", SLOT)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    finally:
        if reservation is not None:
            gate.end_retirement(reservation)
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

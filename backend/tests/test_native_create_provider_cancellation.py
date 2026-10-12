"""Actual async creation drain preserves native identity across caller cancellation."""

import asyncio
import json
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install

from deerflow.community.aio_sandbox.backend import DestroyOutcome
from deerflow.community.aio_sandbox.sandbox_info import PROVENANCE_CREATED, SandboxInfo
from deerflow.config.paths import Paths, paths_scope
from deerflow.spaces.native_create_evidence import ATTEMPT_LABEL

CONTAINER = "c" * 64
LABELS = {
    "deerflow.owner_user_id": "alice",
    "deerflow.thread_id": "chat",
    "deerflow.sandbox_id": "native",
    "deerflow.role": "sandbox",
    "deerflow.network_mode": "open",
    "hartmesh.chat.storage": "on",
    "hartmesh.storage.space." + "b" * 32: "1",
}


def prepare(provider, owner, monkeypatch):
    bridge = install(provider, owner)
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.wait_for_sandbox_ready", lambda *args, **kwargs: True)
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.wait_for_sandbox_ready_async", AsyncMock(return_value=True))
    for method in ("_lark_integration_active", "_lark_broker_active"):
        monkeypatch.setattr(provider, method, lambda *_args: False)
    monkeypatch.setattr(provider, "_get_extra_mounts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(provider, "_local_config_mount_exclusion_root", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(provider, "_validate_planned_chat_mounts", lambda *_args, **_kwargs: None)
    return bridge


def runner(bridge):
    attempts = []

    def run(cmd, **kwargs):
        if cmd[1] == "run":
            (attempt,) = bridge.evidence.pending()
            attempts.append(attempt)
            return SimpleNamespace(stdout=CONTAINER, stderr="", returncode=0)
        entry = {"Id": CONTAINER, "Name": "/native-a", "Config": {"Labels": {**LABELS, ATTEMPT_LABEL: attempts[0].id}}}
        return SimpleNamespace(stdout=json.dumps([entry]), stderr="", returncode=0)

    return run


@pytest.mark.asyncio
@pytest.mark.parametrize("barrier", ["verified", "acknowledged", "before_return"])
async def test_cancelled_creation_worker_retains_actual_identity_and_capacity(fixture, tmp_path, monkeypatch, barrier):
    provider, owner = fixture
    bridge = prepare(provider, owner, monkeypatch)
    entered, release = Event(), Event()
    run = runner(bridge)
    acknowledgment = bridge.evidence.acknowledge

    def stopped_ack(*args):
        if barrier == "verified":
            entered.set()
            assert release.wait(5)
        result = acknowledgment(*args)
        if barrier == "acknowledged":
            entered.set()
            assert release.wait(5)
        return result

    monkeypatch.setattr(bridge.evidence, "acknowledge", stopped_ack)

    def create(*args, **kwargs):
        bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=run)
        if barrier == "before_return":
            entered.set()
            assert release.wait(5)
        return SandboxInfo("native", "http://native", container_name="native-a", container_id=CONTAINER, owner=("alice", "chat"), provenance=PROVENANCE_CREATED, chat_storage_enabled=True)

    provider._backend.create = create
    with paths_scope(Paths(tmp_path / "legacy")):
        task = asyncio.create_task(provider._create_sandbox_async("chat", "native", user_id="alice"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        with provider._lock:
            assert provider._counted_slots_locked() == (3, 0, 0, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert provider._warm_pool["native"][0].container_id == CONTAINER
        assert provider._cleanup_pending["native"] is DestroyOutcome.UNKNOWN
        assert not provider._sandboxes and "native" not in provider._starting
        assert bridge.evidence.pending() == ()
        with provider._lock:
            assert provider._counted_slots_locked() == (3, 0, 1, 0)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_identityless_failed_async_create_remains_counted(fixture, tmp_path, monkeypatch):
    provider, owner = fixture
    bridge = prepare(provider, owner, monkeypatch)

    def fail(cmd, **kwargs):
        raise RuntimeError("Lost Docker response")

    def create(*args, **kwargs):
        bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=fail)

    provider._backend.create = create
    with paths_scope(Paths(tmp_path / "legacy")), pytest.raises(RuntimeError):
        await provider._create_sandbox_async("chat", "native", user_id="alice")
    assert "native" not in provider._starting and not provider._warm_pool and not provider._sandboxes
    assert len(bridge.evidence.pending()) == 1
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_confirmed_readiness_cleanup_does_not_requarantine_acknowledged_container(fixture, tmp_path, monkeypatch, asynchronous):
    provider, owner = fixture
    bridge = prepare(provider, owner, monkeypatch)
    run = runner(bridge)

    def create(*args, **kwargs):
        bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=run)
        return SandboxInfo("native", "http://native", container_name="native-a", container_id=CONTAINER, owner=("alice", "chat"), provenance=PROVENANCE_CREATED, chat_storage_enabled=True)

    provider._backend.create = create
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.wait_for_sandbox_ready", lambda *args, **kwargs: False)
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.wait_for_sandbox_ready_async", AsyncMock(return_value=False))
    with paths_scope(Paths(tmp_path / "legacy")), pytest.raises(RuntimeError, match="failed to become ready"):
        if asynchronous:
            await provider._create_sandbox_async("chat", "native", user_id="alice")
        else:
            await asyncio.to_thread(provider._create_sandbox, "chat", "native", user_id="alice")
    provider._backend.destroy.assert_called_once()
    assert bridge.evidence.pending() == () and not provider._starting
    assert not provider._warm_pool and not provider._sandboxes and not getattr(provider, "_cleanup_pending", {})
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [DestroyOutcome.ABSENT, DestroyOutcome.UNKNOWN])
async def test_cancelled_prewarm_drains_verified_acknowledgment_worker(fixture, tmp_path, monkeypatch, outcome):
    from unittest.mock import Mock

    provider, owner = fixture
    bridge = prepare(provider, owner, monkeypatch)
    entered, release = Event(), Event()
    run = runner(bridge)
    acknowledge = bridge.evidence.acknowledge

    def held_ack(*args):
        result = acknowledge(*args)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(bridge.evidence, "acknowledge", held_ack)
    provider._backend.destroy.return_value = outcome
    provider._backend.discover = Mock(return_value=None)
    monkeypatch.setattr(provider, "_ensure_skills_projection", lambda *_args: None)
    monkeypatch.setattr(provider, "_sandbox_id_for_thread", lambda *_args: "native")
    monkeypatch.setattr(provider, "_thread_lock_path", lambda *_args, **_kwargs: tmp_path / "native-pwarm.lock")
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.wait_for_sandbox_ready", lambda *args, **kwargs: not kwargs["cancelled"].is_set())

    def create(*args, **kwargs):
        bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=run)
        return SandboxInfo("native", "http://native", container_name="native-a", container_id=CONTAINER, owner=("alice", "chat"), provenance=PROVENANCE_CREATED, chat_storage_enabled=True)

    provider._backend.create = create
    with paths_scope(Paths(tmp_path / "legacy")):
        task = asyncio.create_task(provider.prewarm_async("chat", user_id="alice"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not provider._prewarm_future.done()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert provider._prewarm_future.done() and not provider._starting
        provider._backend.destroy.assert_called_once()
        assert bridge.evidence.pending() == ()
        assert not provider._sandboxes
        with provider._lock:
            assert provider._counted_slots_locked() == (3, 0, 0 if outcome is DestroyOutcome.ABSENT else 1, 0)
        if outcome is DestroyOutcome.UNKNOWN:
            assert provider._warm_pool["native"][0].container_id == CONTAINER
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.to_thread(provider._stop_prewarm_worker)


@pytest.mark.asyncio
async def test_cancelled_readiness_does_not_requarantine_confirmed_absence(fixture, tmp_path, monkeypatch):
    provider, owner = fixture
    bridge = prepare(provider, owner, monkeypatch)
    entered = asyncio.Event()
    run = runner(bridge)

    def create(*args, **kwargs):
        bridge.submit(name="native-a", labels=LABELS, command=["docker", "run", "--name", "native-a", "image"], runner=run)
        return SandboxInfo("native", "http://native", container_name="native-a", container_id=CONTAINER, owner=("alice", "chat"), provenance=PROVENANCE_CREATED, chat_storage_enabled=True)

    async def ready(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    provider._backend.create = create
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.wait_for_sandbox_ready_async", ready)
    with paths_scope(Paths(tmp_path / "legacy")):
        task = asyncio.create_task(provider._create_sandbox_async("chat", "native", user_id="alice"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        provider._backend.destroy.assert_called_once()
        assert not provider._warm_pool and not provider._starting and bridge.evidence.pending() == ()
        with provider._lock:
            assert provider._counted_slots_locked() == (3, 0, 0, 0)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

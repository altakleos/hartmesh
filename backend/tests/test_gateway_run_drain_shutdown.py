"""Regression tests for graceful run-task drain on Gateway shutdown.

Guards bytedance/deer-flow issue #3373:

    psycopg_pool.PoolClosed: the pool 'pool-1' is already closed

Root cause: chat runs are fire-and-forget background ``asyncio`` tasks
(``app/gateway/services.py`` -> ``asyncio.create_task(run_agent(...))``) owned
by nobody. On shutdown, ``langgraph_runtime``'s ``AsyncExitStack`` tore down the
checkpointer's postgres pool while those tasks were still mid-graph. langgraph's
``AsyncPregelLoop._checkpointer_put_after_previous`` then ran its
``finally: await checkpointer.aput(...)`` against the already-closed pool.

Fix: ``RunManager.shutdown()`` cancels and *bounded*-awaits every in-flight run,
and ``langgraph_runtime`` calls it BEFORE the ``AsyncExitStack`` closes the
checkpointer — so the final checkpoint write lands while the pool is still open.
The drain must stay bounded (a stuck run must not hang the worker, the
precondition for the signal-reentrancy deadlock guarded by
``app.gateway.app._SHUTDOWN_HOOK_TIMEOUT_SECONDS``).
"""

from __future__ import annotations

import asyncio
import operator
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from typing import Annotated, TypedDict
from unittest.mock import AsyncMock

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from deerflow.runtime import RunManager, RunStatus


# Module-level so langgraph's get_type_hints (which resolves annotations against
# module globals under `from __future__ import annotations`) can see Annotated.
class _CountState(TypedDict):
    count: Annotated[int, operator.add]


class _CloseableSaver(InMemorySaver):
    """InMemorySaver that fails writes once closed, like a closed pool."""

    def __init__(self) -> None:
        super().__init__()
        self._closed = False
        self.writes_after_close: list[str] = []

    def close(self) -> None:
        self._closed = True

    async def aput(self, *args, **kwargs):
        if self._closed:
            self.writes_after_close.append("aput")
            raise RuntimeError("checkpointer is closed")
        return await super().aput(*args, **kwargs)

    async def aput_writes(self, *args, **kwargs):
        if self._closed:
            self.writes_after_close.append("aput_writes")
            raise RuntimeError("checkpointer is closed")
        return await super().aput_writes(*args, **kwargs)


@pytest.mark.asyncio
async def test_shutdown_cancels_and_awaits_inflight_run():
    """shutdown() cancels the in-flight task, waits for it, marks it interrupted."""
    rm = RunManager()
    record = await rm.create("t-drain")
    await rm.set_status(record.run_id, RunStatus.running)

    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def worker() -> None:
        try:
            started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    record.task = asyncio.create_task(worker())
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)

        await rm.shutdown(timeout=5.0)

        assert record.task.done()
        assert cancelled.is_set()
        assert record.status == RunStatus.interrupted
    finally:
        if not record.task.done():
            record.task.cancel()
            with suppress(asyncio.CancelledError):
                await record.task


@pytest.mark.asyncio
async def test_shutdown_is_bounded_when_run_ignores_cancellation():
    """A run that swallows cancellation must not make shutdown() hang."""
    rm = RunManager()
    record = await rm.create("t-stubborn")
    await rm.set_status(record.run_id, RunStatus.running)

    started = asyncio.Event()
    stop = asyncio.Event()

    async def stubborn() -> None:
        started.set()
        while not stop.is_set():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                if stop.is_set():
                    raise
                # else: swallow — simulates a run stuck in slow cleanup

    record.task = asyncio.create_task(stubborn())
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)

        loop = asyncio.get_running_loop()
        t0 = loop.time()
        await rm.shutdown(timeout=0.3)
        elapsed = loop.time() - t0

        assert elapsed < 2.0, f"shutdown took {elapsed:.2f}s; drain is not bounded"
    finally:
        # cleanup the deliberately-stubborn task
        stop.set()
        record.task.cancel()
        with suppress(asyncio.CancelledError):
            await record.task


@pytest.mark.asyncio
async def test_shutdown_is_noop_without_inflight_runs():
    """shutdown() on an idle manager completes cleanly and is idempotent."""
    rm = RunManager()
    await rm.shutdown(timeout=1.0)
    # already-finished runs must not be re-cancelled or error out
    record = await rm.create("t-done")
    await rm.set_status(record.run_id, RunStatus.success)
    await rm.shutdown(timeout=1.0)


@pytest.mark.asyncio
async def test_langgraph_shutdown_drains_late_admission_before_manager_snapshot(monkeypatch):
    from fastapi import FastAPI

    from app.gateway.deps import langgraph_runtime
    from app.gateway.resident_runtime import ResidentRuntime
    from deerflow.extensions.registry import ExtensionRegistry
    from deerflow.sandbox.admission import ProviderAdmission

    @asynccontextmanager
    async def stand_in(_config):
        yield object()

    monkeypatch.setattr("deerflow.runtime.checkpointer.async_provider.make_checkpointer", stand_in)
    monkeypatch.setattr("deerflow.runtime.make_stream_bridge", stand_in)
    monkeypatch.setattr("deerflow.runtime.make_store", stand_in)
    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", AsyncMock())
    monkeypatch.setattr("deerflow.persistence.engine.close_engine", AsyncMock())
    monkeypatch.setattr("deerflow.persistence.engine.get_session_factory", lambda: None)
    monkeypatch.setattr("deerflow.runtime.events.store.make_run_event_store", lambda _cfg: object())
    monkeypatch.setattr("deerflow.persistence.thread_meta.make_thread_store", lambda *_: object())
    monkeypatch.setattr("app.gateway.chat_storage.close_chat_storage_host", AsyncMock())
    app = FastAPI()
    app.state.extensions = ExtensionRegistry().build()
    config = SimpleNamespace(database=SimpleNamespace(backend="memory", checkpoint_channel_mode="full", checkpoint_delta=SimpleNamespace(snapshot_frequency=10)), run_events=None)
    entered, release, sealed, cancelled = (asyncio.Event() for _ in range(4))
    launched = []

    async def scenario():
        async with langgraph_runtime(app, config):
            manager = app.state.run_manager
            host = ResidentRuntime.__new__(ResidentRuntime)
            host._qualification_revision = 0
            host.app = app
            host.provider = SimpleNamespace(external_changed=lambda change: change())
            host.admission = ProviderAdmission()
            host.admission.bind(host.provider)
            host.admission.open(host.provider)
            host.registry = SimpleNamespace(run_manager=manager)
            host._closing_tasks, host._closing, host._operations = None, False, set()
            host.drain_producers = AsyncMock()
            host.coordinator = SimpleNamespace(close=host.drain_workers)
            app.state.resident_runtime = host
            seal = host.seal

            def signal_seal():
                seal()
                sealed.set()

            host.seal = signal_seal

            async def worker():
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            async def late_launch():
                entered.set()
                await release.wait()
                record = await manager.create("late", metadata={"agent_instance_id": "agent"})
                await manager.set_status(record.run_id, RunStatus.running)
                record.task = asyncio.create_task(worker())
                launched.append(record)

            app.state.pending_launch = asyncio.create_task(host.own_operation(late_launch()))
            await entered.wait()

    closing = asyncio.create_task(scenario())
    try:
        await asyncio.wait_for(sealed.wait(), 2)
        release.set()
        done, _pending = await asyncio.wait({closing}, timeout=2)
        assert done, "Canonical shutdown must cancel a worker attached by the drained admission"
        await closing
        assert cancelled.is_set() and launched[0].task.done()
        assert launched[0].status == RunStatus.interrupted
        assert launched[0].stop_reason == "gateway_stopped"
    finally:
        release.set()
        for record in launched:
            if not record.task.done():
                record.task.cancel()
        await asyncio.gather(closing, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("startup_failure", [False, True, "cancel"])
@pytest.mark.parametrize("managed", [False, True])
async def test_langgraph_runtime_drains_runs_before_closing_checkpointer(monkeypatch, startup_failure, managed, tmp_path):
    """Drain runs before services, then close runtime resources in stack order.

    Patches every ``langgraph_runtime`` collaborator down to trivial stand-ins so
    only the bootstrap/teardown ordering runs. The checkpointer probe records when
    its context manager exits (pool close); a ``RunManager.shutdown`` spy records
    when the drain happens. The drain MUST come first.
    """
    from fastapi import FastAPI

    from app.gateway.deps import langgraph_runtime
    from app.gateway.storage_spaces import ExecutionStatus
    from deerflow.extensions.registry import ExtensionRegistry

    events: list[str] = []
    metadata_entered, metadata_release = asyncio.Event(), asyncio.Event()
    maintenance_entered, maintenance_release = asyncio.Event(), asyncio.Event()
    metadata_finished = asyncio.Event()

    async def held_metadata():
        metadata_entered.set()
        await metadata_release.wait()
        assert "engine_closed" not in events
        events.append("metadata_drained")
        metadata_finished.set()

    async def held_maintenance():
        maintenance_entered.set()
        await maintenance_release.wait()
        assert "engine_closed" not in events
        events.append("maintenance_drained")

    async def recheck():
        try:
            await asyncio.Event().wait()
        finally:
            assert "engine_closed" not in events
            events.append("recheck_drained")

    def start_owned_storage():
        app.state.agent_execution = ExecutionStatus(metadata_task=asyncio.create_task(held_metadata()), maintenance_task=asyncio.create_task(held_maintenance()))
        app.state.storage_recheck_task = asyncio.create_task(recheck())

    @asynccontextmanager
    async def probe_checkpointer(_config):
        try:
            yield object()
        finally:
            events.append("checkpointer_closed")

    @asynccontextmanager
    async def fake_stream_bridge(_config):
        try:
            yield object()
        finally:
            events.append("stream_bridge_closed")

    @asynccontextmanager
    async def fake_store(_config):
        try:
            yield object()
        finally:
            events.append("store_closed")

    async def fake_init_engine(_db):
        events.append("engine_initialized")
        if startup_failure:
            start_owned_storage()
            if startup_failure == "cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("partial engine startup failed")

    def fake_session_factory():
        events.append("session_factory_resolved")
        return None

    async def fake_close_engine():
        if managed:
            assert app.state.pool_runtime_owner._closing
            with pytest.raises(StorageHostUnqualified):
                peer.acquire()
        events.append("engine_closed")

    async def spy_shutdown(self, *, timeout):  # noqa: ANN001
        events.append("runs_drained")

    async def spy_chat_storage_close(_app):
        events.append("chat_storage_closed")

    def spy_set_extension_notify_loop(loop):  # noqa: ANN001
        assert loop is asyncio.get_running_loop()
        events.append("extension_loop_set")

    def spy_reset_extension_notify_loop():
        events.append("extension_loop_reset")

    monkeypatch.setattr("deerflow.runtime.checkpointer.async_provider.make_checkpointer", probe_checkpointer)
    monkeypatch.setattr("deerflow.runtime.make_stream_bridge", fake_stream_bridge)
    monkeypatch.setattr("deerflow.runtime.make_store", fake_store)
    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", fake_init_engine)
    monkeypatch.setattr("deerflow.persistence.engine.close_engine", fake_close_engine)
    monkeypatch.setattr("deerflow.persistence.engine.get_session_factory", fake_session_factory)
    monkeypatch.setattr("deerflow.runtime.events.store.make_run_event_store", lambda _cfg: object())
    monkeypatch.setattr("deerflow.persistence.thread_meta.make_thread_store", lambda _sf, _store: object())
    monkeypatch.setattr(RunManager, "shutdown", spy_shutdown, raising=False)
    monkeypatch.setattr("app.gateway.chat_storage.close_chat_storage_host", spy_chat_storage_close)
    monkeypatch.setattr("deerflow.extensions.notify.set_extension_notify_loop", spy_set_extension_notify_loop)
    monkeypatch.setattr("deerflow.extensions.notify.reset_extension_notify_loop", spy_reset_extension_notify_loop)

    app = FastAPI()
    registry = ExtensionRegistry()

    class _Service:
        async def start(self, _deps):
            events.append("service_started")

        async def stop(self):
            events.append("service_stopped")

    with registry.attributed_to("service:install"):
        registry.service(_Service())
    app.state.extensions = registry.build()
    startup_config = SimpleNamespace(database=SimpleNamespace(backend="memory", checkpoint_channel_mode="full", checkpoint_delta=SimpleNamespace(snapshot_frequency=10)), run_events=None)

    if managed:
        from app.gateway.pool_runtime import managed_pool_runtime
        from deerflow.spaces.docker import StorageHostUnqualified
        from deerflow.spaces.runtime_owner import PoolRuntimeOwner

        pool = tmp_path / "pool"
        pool.mkdir(mode=0o750)
        (pool / ".gateway-owner.lock").touch(mode=0o600)
        startup_config.storage_spaces = SimpleNamespace(enabled=True, plan=object(), inventory_path=str(pool / "inventory.json"))
        peer = PoolRuntimeOwner(pool)

    async def run_runtime():
        if startup_failure:
            error = asyncio.CancelledError if startup_failure == "cancel" else RuntimeError
            with pytest.raises(error):
                async with langgraph_runtime(app, startup_config):
                    pytest.fail("Failed startup admitted runtime work")
        else:
            async with langgraph_runtime(app, startup_config):
                start_owned_storage()
                await metadata_entered.wait()

    async def scenario():
        if managed:
            async with managed_pool_runtime(app, startup_config.storage_spaces):
                await asyncio.to_thread(app.state.pool_runtime_owner.acquire)
                await run_runtime()
        else:
            await run_runtime()

    task = asyncio.create_task(scenario())
    try:
        await asyncio.wait_for(metadata_entered.wait(), 2)
        await asyncio.wait_for(maintenance_entered.wait(), 2)
        await asyncio.sleep(0.03)
        assert not task.done() and "engine_closed" not in events
        if managed:
            with pytest.raises(StorageHostUnqualified):
                peer.acquire()
        metadata_release.set()
        await asyncio.wait_for(metadata_finished.wait(), 2)
        assert not task.done() and "engine_closed" not in events
        maintenance_release.set()
        await asyncio.wait_for(task, 2)
        if managed:
            peer.acquire()
            peer.close()
    finally:
        metadata_release.set()
        maintenance_release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert events.index("metadata_drained") < events.index("engine_closed")
    assert events.index("maintenance_drained") < events.index("engine_closed")
    assert events.index("recheck_drained") < events.index("engine_closed")
    if startup_failure:
        assert "service_started" not in events
        return

    assert "runs_drained" in events, "langgraph_runtime never drained in-flight runs on shutdown"
    assert "service_started" in events
    assert "service_stopped" in events
    assert "checkpointer_closed" in events
    assert events.index("engine_initialized") < events.index("session_factory_resolved")
    assert events.index("session_factory_resolved") < events.index("service_started")
    assert events.index("runs_drained") < events.index("service_stopped")
    assert events.index("runs_drained") < events.index("chat_storage_closed") < events.index("checkpointer_closed")
    assert events.index("service_stopped") < events.index("store_closed")
    assert events.index("store_closed") < events.index("checkpointer_closed")
    assert events.index("checkpointer_closed") < events.index("engine_closed")
    assert events.index("engine_closed") < events.index("stream_bridge_closed")
    assert events[0] == "extension_loop_set"
    assert events.index("stream_bridge_closed") < events.index("extension_loop_reset"), f"extension loop reset must be the final runtime teardown; got order {events}"


@pytest.mark.asyncio
@pytest.mark.parametrize("startup_error", [RuntimeError("startup failed"), asyncio.CancelledError()])
async def test_langgraph_runtime_resets_extension_loop_when_startup_exits_early(monkeypatch, startup_error):
    """A partial startup must not leave a stale process-wide loop binding."""
    from fastapi import FastAPI

    from app.gateway.deps import langgraph_runtime

    events: list[str] = []

    @asynccontextmanager
    async def failing_stream_bridge(_config):
        raise startup_error
        yield  # pragma: no cover - makes this an async context manager

    def spy_set_extension_notify_loop(loop):  # noqa: ANN001
        assert loop is asyncio.get_running_loop()
        events.append("extension_loop_set")

    def spy_reset_extension_notify_loop():
        events.append("extension_loop_reset")

    monkeypatch.setattr("deerflow.runtime.make_stream_bridge", failing_stream_bridge)
    monkeypatch.setattr("deerflow.extensions.notify.set_extension_notify_loop", spy_set_extension_notify_loop)
    monkeypatch.setattr("deerflow.extensions.notify.reset_extension_notify_loop", spy_reset_extension_notify_loop)

    app = FastAPI()
    startup_config = SimpleNamespace(
        database=SimpleNamespace(
            backend="memory",
            checkpoint_channel_mode="full",
            checkpoint_delta=SimpleNamespace(snapshot_frequency=10),
        ),
    )

    with pytest.raises(type(startup_error)):
        async with langgraph_runtime(app, startup_config):
            pass

    assert events == ["extension_loop_set", "extension_loop_reset"]


@pytest.mark.asyncio
async def test_drain_flushes_real_graph_checkpoint_before_close():
    """End-to-end #3373 guard with a REAL langgraph graph + checkpointer.

    A real run is driven through ``graph.astream`` in a background task, then
    ``RunManager.shutdown()`` drains it. The checkpointer raises once closed
    (mirroring ``psycopg_pool.PoolClosed``). Closing only happens AFTER the
    drain — as the gateway's AsyncExitStack does. The drain must let langgraph
    flush its final checkpoint while the checkpointer is still open, so no write
    lands against a closed checkpointer.

    Unlike the unit/spy tests above, this exercises the real langgraph
    checkpoint-put machinery, so a future langgraph change that cancels (rather
    than awaits) its checkpoint-put task on executor exit would fail this test
    instead of silently regressing #3373.
    """
    from langgraph.graph import END, START, StateGraph

    async def slow(_state: _CountState) -> dict:
        await asyncio.sleep(0.1)
        return {"count": 1}

    saver = _CloseableSaver()
    builder = StateGraph(_CountState)
    for name in ("a", "b", "c"):
        builder.add_node(name, slow)
    builder.add_edge(START, "a")
    builder.add_edge("a", "b")
    builder.add_edge("b", "c")
    builder.add_edge("c", END)
    graph = builder.compile(checkpointer=saver)

    rm = RunManager()
    record = await rm.create("t-e2e")
    await rm.set_status(record.run_id, RunStatus.running)
    thread_cfg = {"configurable": {"thread_id": "t-e2e"}}

    started = asyncio.Event()

    async def run() -> None:
        started.set()
        async for _ in graph.astream({"count": 0}, config=thread_cfg):
            pass

    record.task = asyncio.create_task(run())
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # Deterministically wait until the run is genuinely in-flight — poll for
        # the first persisted checkpoint instead of a fixed sleep (avoids CI
        # flakiness on slow runners / under event-loop contention).
        async def _await_first_checkpoint() -> None:
            while (await saver.aget_tuple(thread_cfg)) is None:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(_await_first_checkpoint(), timeout=5.0)

        # The fix: drain while the checkpointer is still open ...
        await rm.shutdown(timeout=5.0)
        # ... and only then close it (mirrors langgraph_runtime's ExitStack).
        saver.close()

        assert saver.writes_after_close == [], f"a checkpoint write raced a closed checkpointer: {saver.writes_after_close}"
        # The final checkpoint landed before close.
        snapshot = await saver.aget_tuple(thread_cfg)
        assert snapshot is not None
    finally:
        if not record.task.done():
            record.task.cancel()
            with suppress(asyncio.CancelledError):
                await record.task


@pytest.mark.asyncio
async def test_shutdown_preserves_status_of_run_completed_during_drain():
    """A run that finishes (e.g. success) during the drain window must keep its
    real terminal status — shutdown must not blanket-overwrite it to
    ``interrupted`` in memory or in the store (Copilot review on PR #3381)."""
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    rm = RunManager(store=store)
    record = await rm.create("t-complete")
    await rm.set_status(record.run_id, RunStatus.running)

    async def worker() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # The run had effectively finished; swallow the cancellation and
            # record success, like a run that completed in the same tick the
            # shutdown cancelled it.
            pass
        await rm.set_status(record.run_id, RunStatus.success)

    record.task = asyncio.create_task(worker())
    try:
        await asyncio.sleep(0)  # let the task reach its await point

        await rm.shutdown(timeout=5.0)

        assert record.status == RunStatus.success, f"shutdown overwrote in-memory status: {record.status}"
        persisted = await store.get(record.run_id)
        assert persisted is not None and persisted["status"] == "success", f"shutdown overwrote persisted status: {persisted}"
    finally:
        if not record.task.done():
            record.task.cancel()
            with suppress(asyncio.CancelledError):
                await record.task


@pytest.mark.asyncio
async def test_shutdown_surfaces_failed_interrupted_persist(caplog):
    """A failed interrupted-status persist during the drain must be surfaced (with
    the run_id), not silently swallowed by the gather (maintainer review on
    PR #3381)."""
    import logging

    from deerflow.runtime.runs.store.memory import MemoryRunStore

    class _FailingStore(MemoryRunStore):
        async def update_status(self, *args, **kwargs):
            raise RuntimeError("store unavailable")

    rm = RunManager(store=_FailingStore())
    record = await rm.create("t-failpersist")
    record.status = RunStatus.running  # set in memory; the failing store is exercised by the drain

    started = asyncio.Event()

    async def worker() -> None:
        started.set()
        await asyncio.Event().wait()  # blocks until cancelled by the drain

    record.task = asyncio.create_task(worker())
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)
        with caplog.at_level(logging.WARNING, logger="deerflow.runtime.runs.manager"):
            await rm.shutdown(timeout=5.0)
        assert "Could not persist interrupted status for run" in caplog.text, caplog.text
    finally:
        if not record.task.done():
            record.task.cancel()
            with suppress(asyncio.CancelledError):
                await record.task

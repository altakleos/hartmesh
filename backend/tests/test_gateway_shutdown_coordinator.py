"""Contract tests for the Gateway's single bounded shutdown owner."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI


@pytest.mark.asyncio
async def test_shutdown_orders_producers_runs_memory_and_dependencies() -> None:
    from app.gateway.shutdown import GracefulShutdownCoordinator, ShutdownBudgets

    calls: list[str] = []

    async def phase(name: str) -> None:
        calls.append(name)

    async def drain_runs(_timeout: float) -> bool:
        calls.append("runs")
        # A run finishing during drain may enqueue its last memory update.
        calls.append("late-memory-write")
        return True

    def flush_memory(_timeout: float) -> bool:
        assert calls[-1] == "late-memory-write"
        calls.append("memory-flush")
        return True

    coordinator = GracefulShutdownCoordinator(
        budgets=ShutdownBudgets(
            admission_seconds=0.1,
            channel_seconds=0.1,
            scheduler_seconds=0.1,
            run_seconds=0.1,
            memory_seconds=0.1,
            dependencies_seconds=0.1,
        ),
        begin_admission_drain=lambda: phase("admission"),
        stop_channels=lambda: phase("channels"),
        stop_scheduler=lambda: phase("scheduler"),
        drain_runs=drain_runs,
        flush_memory=flush_memory,
        close_memory=lambda: calls.append("memory-close"),
        close_browser=lambda: phase("browser"),
        close_oidc=lambda: phase("oidc"),
        close_runtime=lambda: phase("runtime-dependencies"),
    )

    first, second = await asyncio.gather(coordinator.shutdown(), coordinator.shutdown())

    assert first is second
    assert calls == [
        "admission",
        "channels",
        "scheduler",
        "runs",
        "late-memory-write",
        "memory-flush",
        "memory-close",
        "browser",
        "oidc",
        "runtime-dependencies",
    ]
    assert first.memory_flushed is True


@pytest.mark.asyncio
async def test_hung_subsystems_cannot_extend_global_deadline() -> None:
    from app.gateway.shutdown import GracefulShutdownCoordinator, ShutdownBudgets

    async def hang() -> None:
        await asyncio.Future()

    budgets = ShutdownBudgets(
        admission_seconds=0.01,
        channel_seconds=0.02,
        scheduler_seconds=0.01,
        run_seconds=0.01,
        memory_seconds=0.02,
        dependencies_seconds=0.02,
    )
    coordinator = GracefulShutdownCoordinator(
        budgets=budgets,
        begin_admission_drain=lambda: asyncio.sleep(0),
        stop_channels=hang,
        stop_scheduler=lambda: asyncio.sleep(0),
        drain_runs=lambda _timeout: asyncio.sleep(0, result=True),
        flush_memory=lambda _timeout: True,
        close_memory=lambda: None,
        close_browser=hang,
        close_oidc=lambda: asyncio.sleep(0),
    )

    started = asyncio.get_running_loop().time()
    report = await coordinator.shutdown()
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < budgets.total_seconds + 0.1
    assert {item.phase for item in report.diagnostics} >= {"channels", "browser"}
    assert all(item.code == "shutdown_phase_timeout" for item in report.diagnostics)


@pytest.mark.asyncio
async def test_memory_is_not_flushed_when_admissions_or_runs_are_not_quiescent() -> None:
    from app.gateway.shutdown import GracefulShutdownCoordinator, ShutdownBudgets

    flushed = False

    def flush(_timeout: float) -> bool:
        nonlocal flushed
        flushed = True
        return True

    coordinator = GracefulShutdownCoordinator(
        budgets=ShutdownBudgets.uniform(0.01),
        begin_admission_drain=lambda: asyncio.sleep(0, result=False),
        stop_channels=lambda: asyncio.sleep(0),
        stop_scheduler=lambda: asyncio.sleep(0),
        drain_runs=lambda _timeout: asyncio.sleep(0, result=False),
        flush_memory=flush,
        close_memory=lambda: None,
        close_browser=lambda: asyncio.sleep(0),
        close_oidc=lambda: asyncio.sleep(0),
    )

    report = await coordinator.shutdown()

    assert flushed is False
    assert report.memory_flushed is False
    assert "memory_flush_skipped_active_writers" in {item.code for item in report.diagnostics}


@pytest.mark.asyncio
async def test_shutdown_diagnostics_are_bounded_and_redact_exception_text(caplog) -> None:
    from app.gateway.shutdown import GracefulShutdownCoordinator, ShutdownBudgets

    async def malicious_channel() -> None:
        raise RuntimeError("credential=must-not-appear")

    coordinator = GracefulShutdownCoordinator(
        budgets=ShutdownBudgets.uniform(0.01),
        begin_admission_drain=lambda: asyncio.sleep(0, result=True),
        stop_channels=malicious_channel,
        stop_scheduler=lambda: asyncio.sleep(0),
        drain_runs=lambda _timeout: asyncio.sleep(0, result=True),
        flush_memory=lambda _timeout: True,
        close_memory=lambda: None,
        close_browser=lambda: asyncio.sleep(0),
        close_oidc=lambda: asyncio.sleep(0),
    )
    caplog.set_level(logging.WARNING, logger="app.gateway.shutdown")

    report = await coordinator.shutdown()

    diagnostic = next(item for item in report.diagnostics if item.phase == "channels")
    assert diagnostic.error_class == "RuntimeError"
    assert "must-not-appear" not in repr(report)
    assert "must-not-appear" not in caplog.text


def test_hung_memory_flush_does_not_extend_asyncio_run_or_global_deadline() -> None:
    from app.gateway.shutdown import GracefulShutdownCoordinator, ShutdownBudgets

    release = threading.Event()

    def hung_flush(_timeout: float) -> bool:
        release.wait(5)
        return True

    memory_closed = False

    def close_memory() -> None:
        nonlocal memory_closed
        memory_closed = True

    async def drive():
        coordinator = GracefulShutdownCoordinator(
            budgets=ShutdownBudgets.uniform(0.01),
            begin_admission_drain=lambda: asyncio.sleep(0, result=True),
            stop_channels=lambda: asyncio.sleep(0),
            stop_scheduler=lambda: asyncio.sleep(0),
            drain_runs=lambda _timeout: asyncio.sleep(0, result=True),
            flush_memory=hung_flush,
            close_memory=close_memory,
            close_browser=lambda: asyncio.sleep(0),
            close_oidc=lambda: asyncio.sleep(0),
        )
        return await coordinator.shutdown()

    started = time.monotonic()
    try:
        report = asyncio.run(drive())
    finally:
        release.set()

    assert time.monotonic() - started < 0.15
    assert "shutdown_phase_timeout" in {item.code for item in report.diagnostics}
    assert memory_closed is False


@pytest.mark.parametrize(
    "run_state",
    ["idle", "accepted-before-start", "active-run", "terminal-commit", "channel-delivery"],
)
def test_real_lifespan_drives_every_shutdown_state_in_order(run_state: str) -> None:
    """Exercise the FastAPI lifespan with the production coordinator wiring."""

    from app.gateway.app import lifespan
    from deerflow.runtime import RunManager, RunStatus

    events: list[str] = []
    observed: dict[str, object] = {}
    delivery_release = asyncio.Event()

    class Readiness:
        async def begin_draining(self) -> bool:
            events.append("admission")
            return True

    @asynccontextmanager
    async def runtime(app, _config):
        manager = RunManager()
        record = None
        if run_state in {"accepted-before-start", "active-run", "terminal-commit"}:
            record = await manager.create(f"thread-{run_state}")
        if run_state == "active-run":
            await manager.set_status(record.run_id, RunStatus.running)

            async def active_worker() -> None:
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    events.append("late-memory-write")
                    raise

            record.task = asyncio.create_task(active_worker())
            await asyncio.sleep(0)
        elif run_state == "terminal-commit":
            await manager.set_status(record.run_id, RunStatus.success)

        original_shutdown = manager.shutdown

        async def tracked_shutdown(*, timeout: float) -> bool:
            events.append(f"runs:{run_state}")
            return await original_shutdown(timeout=timeout)

        manager.shutdown = tracked_shutdown  # type: ignore[method-assign]
        observed["record"] = record
        app.state.runtime_readiness = Readiness()
        app.state.run_manager = manager
        app.state.scheduled_task_repo = None
        app.state.scheduled_task_run_repo = None
        yield

    async def start_channels(*_args, **_kwargs):
        return SimpleNamespace(get_status=lambda: {})

    async def stop_channels() -> None:
        events.append("channels")
        if run_state == "channel-delivery":
            delivery_release.set()
            await observed["delivery_task"]

    async def ensure_admin(_app) -> None:
        return None

    async def close_oidc() -> None:
        events.append("oidc")

    browser = SimpleNamespace(
        close_all_sessions=lambda: asyncio.sleep(0, result=events.append("browser")),
    )
    memory = MagicMock()
    memory.warm.return_value = None

    def flush_memory(_timeout: float) -> bool:
        events.append("memory-flush")
        return True

    memory.shutdown_flush.side_effect = flush_memory
    config = SimpleNamespace(
        log_level="INFO",
        memory=SimpleNamespace(enabled=True, shutdown_flush_timeout_seconds=0.1),
        deployment=SimpleNamespace(
            shutdown=SimpleNamespace(
                admission_seconds=0.1,
                channel_seconds=0.1,
                scheduler_seconds=0.1,
                run_seconds=0.1,
                dependencies_seconds=0.1,
            )
        ),
    )

    async def drive() -> None:
        app = FastAPI()
        if run_state == "channel-delivery":

            async def finish_delivery() -> None:
                await delivery_release.wait()
                events.append("delivery-settled")

            observed["delivery_task"] = asyncio.create_task(finish_delivery())
        with (
            patch("app.gateway.app.get_app_config", return_value=config),
            patch("app.gateway.app.get_gateway_config", return_value=SimpleNamespace(host="x", port=0)),
            patch("app.gateway.app.langgraph_runtime", runtime),
            patch("app.gateway.app._ensure_admin_user", side_effect=ensure_admin),
            patch("app.gateway.app.ensure_browser_runtime_available"),
            patch("app.gateway.app.cleanup_stale_upload_staging_files", return_value=0),
            patch("deerflow.skills.projection.ensure_public_skill_projection", return_value=False),
            patch("deerflow.agents.memory.get_memory_manager", return_value=memory),
            patch("app.channels.service.start_channel_service", side_effect=start_channels),
            patch("app.channels.service.stop_channel_service", side_effect=stop_channels),
            patch("app.gateway.services.build_channel_invocation_runtime", return_value=object()),
            patch("deerflow.community.browser_automation.get_browser_session_manager", return_value=browser),
            patch("app.gateway.app.auth.close_oidc_service", side_effect=close_oidc),
        ):
            async with lifespan(app):
                pass

    asyncio.run(drive())

    assert events.index("admission") < events.index("channels")
    if run_state == "channel-delivery":
        assert events.index("delivery-settled") < events.index(f"runs:{run_state}")
    assert events.index("channels") < events.index(f"runs:{run_state}")
    assert events.index(f"runs:{run_state}") < events.index("memory-flush")
    assert events.index("memory-flush") < events.index("browser") < events.index("oidc")
    record = observed["record"]
    if run_state == "active-run":
        assert record.status is RunStatus.interrupted
        assert events.index("late-memory-write") < events.index("memory-flush")
    elif run_state == "accepted-before-start":
        assert record.status is RunStatus.pending
    elif run_state == "terminal-commit":
        assert record.status is RunStatus.success

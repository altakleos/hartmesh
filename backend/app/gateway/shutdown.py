"""Single-owner, deadline-driven Gateway shutdown orchestration.

The coordinator freezes admission, stops producers, settles locally owned runs,
flushes memory, and only then closes dependencies.  It is deliberately an
application-layer module: portable runtime contracts do not know about process
lifecycle or deployment budgets.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


async def _run_sync_daemon(operation: Callable[..., Any], *args: Any) -> Any:
    """Await synchronous cleanup without letting a wedged call own process exit."""

    loop = asyncio.get_running_loop()
    result: asyncio.Future[Any] = loop.create_future()

    def complete(value: Any = None, error: BaseException | None = None) -> None:
        if result.done():
            return
        if error is None:
            result.set_result(value)
        else:
            result.set_exception(error)

    def invoke() -> None:
        try:
            value = operation(*args)
        except BaseException as exc:
            try:
                loop.call_soon_threadsafe(complete, None, exc)
            except RuntimeError:
                pass
        else:
            try:
                loop.call_soon_threadsafe(complete, value, None)
            except RuntimeError:
                pass

    threading.Thread(
        target=invoke,
        name="gateway-bounded-shutdown-sync",
        daemon=True,
    ).start()
    return await result


@dataclass(frozen=True)
class ShutdownBudgets:
    """Bounded phase budgets whose sum is the absolute application deadline."""

    admission_seconds: float
    channel_seconds: float
    scheduler_seconds: float
    run_seconds: float
    memory_seconds: float
    dependencies_seconds: float

    def __post_init__(self) -> None:
        if any(value <= 0 for value in self.values):
            raise ValueError("shutdown phase budgets must be positive")

    @classmethod
    def uniform(cls, seconds: float) -> ShutdownBudgets:
        return cls(*(seconds for _ in range(6)))

    @property
    def values(self) -> tuple[float, ...]:
        return (
            self.admission_seconds,
            self.channel_seconds,
            self.scheduler_seconds,
            self.run_seconds,
            self.memory_seconds,
            self.dependencies_seconds,
        )

    @property
    def total_seconds(self) -> float:
        return sum(self.values)


@dataclass(frozen=True)
class ShutdownDiagnostic:
    """Bounded operator diagnostic; raw dependency errors are logged only."""

    phase: str
    code: str
    correlation_id: str
    error_class: str | None = None


@dataclass(frozen=True)
class ShutdownReport:
    """Immutable result shared by every repeated shutdown caller."""

    admissions_quiescent: bool
    channels_quiescent: bool
    scheduler_quiescent: bool
    runs_quiescent: bool
    memory_flushed: bool
    diagnostics: tuple[ShutdownDiagnostic, ...]


class GracefulShutdownCoordinator:
    """Own shutdown ordering, deadline accounting, and idempotent single-flight."""

    def __init__(
        self,
        *,
        budgets: ShutdownBudgets,
        begin_admission_drain: Callable[[], Awaitable[bool | None]],
        stop_channels: Callable[[], Awaitable[None]],
        stop_scheduler: Callable[[], Awaitable[None]],
        drain_runs: Callable[[float], Awaitable[bool]],
        flush_memory: Callable[[float], bool],
        close_memory: Callable[[], None],
        close_browser: Callable[[], Awaitable[Any]],
        close_oidc: Callable[[], Awaitable[None]],
        stop_retrieval: Callable[[], Awaitable[bool | None]] | None = None,
        close_runtime: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._budgets = budgets
        self._begin_admission_drain = begin_admission_drain
        self._stop_channels = stop_channels
        self._stop_scheduler = stop_scheduler
        self._drain_runs = drain_runs
        self._flush_memory = flush_memory
        self._close_memory = close_memory
        self._close_browser = close_browser
        self._close_oidc = close_oidc
        self._stop_retrieval = stop_retrieval
        self._close_runtime = close_runtime
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[ShutdownReport] | None = None

    async def shutdown(self) -> ShutdownReport:
        """Run the sequence once; concurrent/repeated callers share its report."""

        async with self._lock:
            if self._task is None:
                self._task = asyncio.create_task(self._run(), name="gateway-graceful-shutdown")
            task = self._task
        return await asyncio.shield(task)

    async def _run(self) -> ShutdownReport:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._budgets.total_seconds
        diagnostics: list[ShutdownDiagnostic] = []

        async def bounded(
            phase: str,
            budget: float,
            operation: Callable[[], Awaitable[Any]],
            *,
            default: Any = None,
        ) -> Any:
            timeout = min(budget, max(0.0, deadline - loop.time()))
            if timeout <= 0:
                diagnostics.append(self._diagnostic(phase, "shutdown_deadline_exhausted"))
                return default
            try:
                task = asyncio.create_task(operation(), name=f"gateway-shutdown-{phase}")
                done, _ = await asyncio.wait((task,), timeout=timeout)
                if not done:
                    task.cancel()
                    task.add_done_callback(self._consume_abandoned_task)
                    diagnostics.append(self._diagnostic(phase, "shutdown_phase_timeout"))
                    return default
                return task.result()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                diagnostics.append(self._diagnostic(phase, "shutdown_phase_failed", exc))
                return default

        admissions = await bounded(
            "admission",
            self._budgets.admission_seconds,
            self._begin_admission_drain,
            default=False,
        )
        admissions_quiescent = admissions is not False
        channels = await bounded(
            "channels",
            self._budgets.channel_seconds,
            self._stop_channels,
            default=False,
        )
        channels_quiescent = channels is not False
        scheduler = await bounded(
            "scheduler",
            self._budgets.scheduler_seconds,
            self._stop_scheduler,
            default=False,
        )
        scheduler_quiescent = scheduler is not False
        run_budget = min(self._budgets.run_seconds, max(0.0, deadline - loop.time()))
        runs_quiescent = bool(
            await bounded(
                "runs",
                self._budgets.run_seconds,
                lambda: self._drain_runs(run_budget),
                default=False,
            )
        )

        memory_flushed = False
        memory_attempt_complete = False
        writers_quiescent = admissions_quiescent and channels_quiescent and scheduler_quiescent and runs_quiescent
        if writers_quiescent:
            memory_budget = min(self._budgets.memory_seconds, max(0.0, deadline - loop.time()))
            incomplete = object()

            async def flush_memory() -> bool:
                try:
                    return await _run_sync_daemon(self._flush_memory, memory_budget)
                except Exception as exc:
                    diagnostics.append(self._diagnostic("memory", "shutdown_phase_failed", exc))
                    return False

            memory_result = await bounded(
                "memory",
                self._budgets.memory_seconds,
                flush_memory,
                default=incomplete,
            )
            memory_attempt_complete = memory_result is not incomplete
            memory_flushed = memory_attempt_complete and bool(memory_result)
            if not memory_flushed:
                diagnostics.append(self._diagnostic("memory", "memory_flush_incomplete"))
        else:
            diagnostics.append(self._diagnostic("memory", "memory_flush_skipped_active_writers"))

        dependency_deadline = min(
            deadline,
            loop.time() + self._budgets.dependencies_seconds,
        )
        dependency_count = 3 + (self._stop_retrieval is not None) + (self._close_runtime is not None)

        async def dependency(phase: str, operation: Callable[[], Awaitable[Any]]) -> Any:
            nonlocal dependency_count
            remaining = max(0.0, dependency_deadline - loop.time())
            share = remaining / max(1, dependency_count)
            dependency_count -= 1
            return await bounded(phase, share, operation, default=False)

        retrieval_stopped = True
        if self._stop_retrieval is not None:
            retrieval_stopped = (await dependency("retrieval", self._stop_retrieval)) is not False
        if writers_quiescent and memory_attempt_complete and retrieval_stopped:
            await dependency(
                "memory_close",
                lambda: _run_sync_daemon(self._close_memory),
            )
        else:
            dependency_count -= 1
        await dependency("browser", self._close_browser)
        await dependency("oidc", self._close_oidc)
        if self._close_runtime is not None and writers_quiescent:
            await dependency("runtime_dependencies", self._close_runtime)
        elif self._close_runtime is not None:
            diagnostics.append(
                self._diagnostic(
                    "runtime_dependencies",
                    "runtime_dependencies_close_skipped_active_users",
                )
            )
        return ShutdownReport(
            admissions_quiescent=admissions_quiescent,
            channels_quiescent=channels_quiescent,
            scheduler_quiescent=scheduler_quiescent,
            runs_quiescent=runs_quiescent,
            memory_flushed=memory_flushed,
            diagnostics=tuple(diagnostics),
        )

    @staticmethod
    def _consume_abandoned_task(task: asyncio.Task[Any]) -> None:
        if not task.done() or task.cancelled():
            return
        try:
            task.exception()
        except BaseException:
            # The phase already emitted a safe correlated diagnostic.
            pass

    @staticmethod
    def _diagnostic(
        phase: str,
        code: str,
        error: BaseException | None = None,
    ) -> ShutdownDiagnostic:
        correlation_id = uuid4().hex
        error_class = type(error).__name__ if error is not None else None
        logger.warning(
            "Gateway shutdown phase did not complete",
            extra={
                "correlation_id": correlation_id,
                "shutdown_phase": phase,
                "diagnostic_code": code,
                "error_class": error_class,
            },
        )
        return ShutdownDiagnostic(
            phase=phase,
            code=code,
            correlation_id=correlation_id,
            error_class=error_class,
        )


__all__ = [
    "GracefulShutdownCoordinator",
    "ShutdownBudgets",
    "ShutdownDiagnostic",
    "ShutdownReport",
]

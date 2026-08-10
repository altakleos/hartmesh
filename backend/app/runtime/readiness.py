"""Bounded deployment readiness shared by probes and invocation admission."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from deerflow.extensions.capabilities import (
    CapabilityHealthSnapshot,
    CapabilityReadinessSnapshot,
)
from deerflow.runtime.runs.store.base import LifecycleReadiness

logger = logging.getLogger(__name__)


class AdmissionHealthMonitor(Protocol):
    async def admission_readiness(
        self,
        *,
        expected_generation: int,
    ) -> CapabilityReadinessSnapshot: ...


class LifecycleReadinessStore(Protocol):
    async def lifecycle_readiness(self) -> LifecycleReadiness: ...


@dataclass(frozen=True)
class RuntimeReadinessSnapshot:
    """Safe current proof that this process can admit a durable invocation."""

    status: Literal["ready", "not_ready"]
    reason_codes: tuple[str, ...]
    checked_at: datetime
    correlation_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "checked_at": self.checked_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "correlation_id": self.correlation_id,
        }


def _capability_reason(
    snapshots: tuple[CapabilityHealthSnapshot, ...],
) -> str:
    codes = {item.diagnostic_code for item in snapshots if item.status != "healthy"}
    if "generation_mismatch" in codes:
        return "required_capability_generation_mismatch"
    if "snapshot_stale" in codes or "refresh_in_progress" in codes:
        return "required_capability_stale"
    if codes & {
        "not_registered",
        "initialization_failed",
        "duplicate_registration",
    }:
        return "required_capability_unavailable"
    return "required_capability_unhealthy"


class RuntimeReadinessCoordinator:
    """Evaluate one bounded fail-closed admission-readiness proof."""

    def __init__(
        self,
        *,
        health_monitor: AdmissionHealthMonitor,
        lifecycle_store: Callable[[], LifecycleReadinessStore | None],
        persistence_ready: Callable[[], bool],
        extension_generation: Callable[[], int],
        overall_timeout_seconds: float,
        sandbox_projection_ready: Callable[[], Awaitable[bool]] | None = None,
        admission_compensations_ready: Callable[[], bool] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if overall_timeout_seconds <= 0:
            raise ValueError("overall_timeout_seconds must be positive")
        self._health_monitor = health_monitor
        self._lifecycle_store = lifecycle_store
        self._persistence_ready = persistence_ready
        self._extension_generation = extension_generation
        self._overall_timeout_seconds = overall_timeout_seconds
        self._sandbox_projection_ready = sandbox_projection_ready
        self._admission_compensations_ready = admission_compensations_ready
        self._clock = clock or (lambda: datetime.now(UTC))
        self._last_snapshot: RuntimeReadinessSnapshot | None = None
        self._admission_condition = asyncio.Condition()
        self._draining = False
        self._active_admissions = 0

    @property
    def last_snapshot(self) -> RuntimeReadinessSnapshot | None:
        return self._last_snapshot

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("readiness clock must return an aware datetime")
        return now

    def _store(self) -> LifecycleReadinessStore | None:
        return self._lifecycle_store()

    @staticmethod
    def _log_dependency_failure(
        message: str,
        *,
        component: str,
        error: BaseException,
    ) -> str:
        from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

        diagnostic = bounded_diagnostic(
            code="readiness_dependency_failure",
            operation="readiness_check",
            error=error,
            capability_id=component,
        )
        log_bounded_failure(logger, diagnostic, level=logging.ERROR)
        return diagnostic.correlation_id

    async def _evaluate(self) -> RuntimeReadinessSnapshot:
        reasons: list[str] = []
        correlation_id: str | None = None
        if not self._persistence_ready():
            reasons.append("deployment_profile_unsatisfied")
        if self._admission_compensations_ready is not None:
            try:
                compensations_ready = self._admission_compensations_ready()
            except Exception as exc:
                compensations_ready = False
                correlation_id = self._log_dependency_failure(
                    "Admission compensation readiness check failed",
                    component="admission_compensation",
                    error=exc,
                )
            if not compensations_ready:
                reasons.append("admission_compensation_pending")

        expected_generation = self._extension_generation()
        store = self._store()

        async def lifecycle_check() -> LifecycleReadiness:
            if store is None:
                return LifecycleReadiness(
                    ready=False,
                    reason_code="lifecycle_store_unavailable",
                )
            return await store.lifecycle_readiness()

        async def sandbox_projection_check() -> bool:
            if self._sandbox_projection_ready is None:
                return True
            return await self._sandbox_projection_ready()

        health_result, lifecycle_result, sandbox_projection_result = await asyncio.gather(
            self._health_monitor.admission_readiness(
                expected_generation=expected_generation,
            ),
            lifecycle_check(),
            sandbox_projection_check(),
            return_exceptions=True,
        )
        if isinstance(health_result, BaseException):
            if isinstance(health_result, asyncio.CancelledError):
                raise health_result
            reasons.append("required_capability_check_failed")
            correlation_id = self._log_dependency_failure(
                "Required capability readiness check failed",
                component="capability_health",
                error=health_result,
            )
        elif health_result.status != "ready":
            reasons.append(_capability_reason(health_result.health))
            correlation_id = next(
                (item.correlation_id for item in health_result.health if item.status != "healthy" and item.correlation_id is not None),
                correlation_id,
            )

        if isinstance(lifecycle_result, BaseException):
            if isinstance(lifecycle_result, asyncio.CancelledError):
                raise lifecycle_result
            reasons.append("lifecycle_store_unavailable")
            correlation_id = self._log_dependency_failure(
                "Lifecycle readiness check failed",
                component="lifecycle_store",
                error=lifecycle_result,
            )
        elif not lifecycle_result.ready:
            reasons.append(lifecycle_result.reason_code)

        if isinstance(sandbox_projection_result, BaseException):
            if isinstance(sandbox_projection_result, asyncio.CancelledError):
                raise sandbox_projection_result
            reasons.append("sandbox_projection_unavailable")
            correlation_id = self._log_dependency_failure(
                "Accepted sandbox projection readiness check failed",
                component="sandbox_projection",
                error=sandbox_projection_result,
            )
        elif sandbox_projection_result is not True:
            reasons.append("sandbox_projection_unavailable")

        unique_reasons = tuple(dict.fromkeys(reasons))
        return RuntimeReadinessSnapshot(
            status="not_ready" if unique_reasons else "ready",
            reason_codes=unique_reasons,
            checked_at=self._now(),
            correlation_id=correlation_id,
        )

    async def readiness(self) -> RuntimeReadinessSnapshot:
        if self._draining:
            snapshot = RuntimeReadinessSnapshot(
                status="not_ready",
                reason_codes=("shutdown_draining",),
                checked_at=self._now(),
            )
            self._last_snapshot = snapshot
            return snapshot
        try:
            async with asyncio.timeout(self._overall_timeout_seconds):
                snapshot = await self._evaluate()
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            snapshot = RuntimeReadinessSnapshot(
                status="not_ready",
                reason_codes=("readiness_timeout",),
                checked_at=self._now(),
            )
        except Exception as exc:
            correlation_id = self._log_dependency_failure(
                "Unexpected readiness evaluation failure",
                component="coordinator",
                error=exc,
            )
            snapshot = RuntimeReadinessSnapshot(
                status="not_ready",
                reason_codes=("readiness_check_failed",),
                checked_at=self._now(),
                correlation_id=correlation_id,
            )
        self._last_snapshot = snapshot
        return snapshot

    async def ready_for_admission(self) -> bool:
        """Return only whether a genuinely new invocation may be admitted."""

        return (await self.readiness()).status == "ready"

    @asynccontextmanager
    async def admission_permit(self) -> AsyncIterator[bool]:
        """Fence a new admission atomically against graceful shutdown."""

        async with self._admission_condition:
            rejected = self._draining
            if not rejected:
                self._active_admissions += 1
        if rejected:
            yield False
            return
        try:
            yield await self.ready_for_admission()
        finally:
            async with self._admission_condition:
                self._active_admissions -= 1
                self._admission_condition.notify_all()

    async def begin_draining(self) -> bool:
        """Reject new permits and wait until every admitted launch leaves its seam."""

        async with self._admission_condition:
            self._draining = True
            await self._admission_condition.wait_for(lambda: self._active_admissions == 0)
        return True


__all__ = [
    "RuntimeReadinessCoordinator",
    "RuntimeReadinessSnapshot",
]

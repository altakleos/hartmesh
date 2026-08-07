"""Capability Host execution for restrictive invocation constraints."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
    ConstraintIndeterminate,
    ConstraintProjectionRequestV1,
    ConstraintProjectionV1,
    ConstraintRejected,
    InvocationConstraintsProvider,
)

from deerflow.extensions.registry import LoadedExtensions

_DEFAULT_TIMEOUT_SECONDS = 2.0
_MAX_FUTURE_SKEW = timedelta(seconds=30)


class ConstraintStartupError(RuntimeError):
    """The operator-required constraints capability cannot be initialized."""


def _bounded_failure(exc: BaseException) -> str:
    return type(exc).__name__[:128]


class InvocationConstraintsHost:
    """Startup-initialized direct host for the process's singular provider."""

    def __init__(
        self,
        extensions: LoadedExtensions,
        *,
        required_capabilities: Iterable[str] = (),
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        required = INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY in frozenset(required_capabilities)
        registration = extensions.invocation_constraints_provider_factory
        self._provider: InvocationConstraintsProvider | None = None
        self._timeout_seconds = float(timeout_seconds)
        if self._timeout_seconds <= 0:
            raise ValueError("invocation constraints timeout_seconds must be positive")
        self._clock = clock or (lambda: datetime.now(UTC))
        diagnostics: list[str] = []
        if registration is None:
            if required:
                raise ConstraintStartupError(f"required capability {INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY} is not registered")
        else:
            try:
                provider = registration.factory()
                if not isinstance(provider, InvocationConstraintsProvider):
                    raise TypeError("factory returned an object that does not implement InvocationConstraintsProvider")
                self._provider = provider
            except Exception as exc:
                if required:
                    raise ConstraintStartupError(f"required capability {INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY} failed to initialize: {_bounded_failure(exc)}") from exc
                diagnostics.append(_bounded_failure(exc))
        self.startup_diagnostics = tuple(diagnostics)

    @property
    def initialized_capability_ids(self) -> frozenset[str]:
        """Return the singular initialized capability, when present."""

        if self._provider is None:
            return frozenset()
        return frozenset({INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY})

    @property
    def clock(self) -> Callable[[], datetime]:
        return self._clock

    async def project(
        self,
        request: ConstraintProjectionRequestV1,
        *,
        host_max_total_subagents: int | None,
        runtime_enforceable: bool = True,
    ) -> ConstraintProjectionV1 | ConstraintRejected | ConstraintIndeterminate | None:
        provider = self._provider
        if provider is None:
            return None
        try:
            async with asyncio.timeout(self._timeout_seconds):
                result = await provider.project(request)
            if type(result) is ConstraintRejected:
                return result
            if type(result) is ConstraintIndeterminate:
                return result
            if type(result) is not ConstraintProjectionV1:
                raise TypeError("InvocationConstraintsProvider.project must return the v1 constraint union")
            projection = ConstraintProjectionV1(
                request_digest=result.request_digest,
                agent_revision_digest=result.agent_revision_digest,
                projection_revision=result.projection_revision,
                issued_at=result.issued_at,
                valid_until=result.valid_until,
                evidence_id=result.evidence_id,
                evidence_digest=result.evidence_digest,
                max_total_subagents=result.max_total_subagents,
            )
            now = self._clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise ValueError("the host constraint clock must return a timezone-aware datetime")
            if projection.request_digest != request.request_digest:
                raise ValueError("constraint request digest mismatch")
            if projection.agent_revision_digest != request.agent_revision_digest:
                raise ValueError("constraint agent revision digest mismatch")
            if projection.issued_at > now + _MAX_FUTURE_SKEW:
                raise ValueError("constraint projection issued_at exceeds future skew")
            if projection.valid_until <= now:
                raise ValueError("constraint projection is already expired")
            limit = projection.max_total_subagents
            if limit is not None:
                if not runtime_enforceable:
                    raise ValueError("the active runtime cannot enforce max_total_subagents")
                if host_max_total_subagents is not None:
                    limit = min(limit, host_max_total_subagents)
                projection = replace(projection, max_total_subagents=limit)
            return projection
        except asyncio.CancelledError:
            raise
        except Exception:
            return ConstraintIndeterminate()


__all__ = [
    "ConstraintStartupError",
    "InvocationConstraintsHost",
]

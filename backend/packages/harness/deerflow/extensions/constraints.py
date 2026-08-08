"""Capability Host execution for restrictive invocation constraints."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
    INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS,
    ConstraintIndeterminate,
    ConstraintProjectionRequestV1,
    ConstraintProjectionRequestV2,
    ConstraintProjectionV1,
    ConstraintProjectionV2,
    ConstraintRejected,
)

from deerflow.extensions.registry import LoadedExtensions

_DEFAULT_TIMEOUT_SECONDS = 2.0
_MAX_FUTURE_SKEW = timedelta(seconds=30)
_CAPABILITY_ID_BY_API_VERSION = {
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION: INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY,
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2: INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
}


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
        required_ids = frozenset(required_capabilities)
        required_constraints = required_ids.intersection(_CAPABILITY_ID_BY_API_VERSION.values())
        if len(required_constraints) > 1:
            raise ConstraintStartupError("only one invocation-constraints contract version may be required")
        self._required_capability_id = next(iter(required_constraints), None)
        registration = extensions.invocation_constraints_provider_factory
        self._provider: object | None = None
        self._capability_api_version: str | None = None
        self._timeout_seconds = float(timeout_seconds)
        if self._timeout_seconds <= 0:
            raise ValueError("invocation constraints timeout_seconds must be positive")
        self._clock = clock or (lambda: datetime.now(UTC))
        diagnostics: list[str] = []
        if extensions.invocation_constraints_provider_conflict:
            if required_constraints:
                capability_id = next(iter(required_constraints))
                raise ConstraintStartupError(f"required capability {capability_id} has duplicate provider registrations")
            diagnostics.append("duplicate_registration")
        elif registration is None:
            if required_constraints:
                capability_id = next(iter(required_constraints))
                raise ConstraintStartupError(f"required capability {capability_id} is not registered")
        else:
            capability_id = _CAPABILITY_ID_BY_API_VERSION[registration.capability_api_version]
            if required_constraints and capability_id not in required_constraints:
                required_id = next(iter(required_constraints))
                raise ConstraintStartupError(f"required capability {required_id} is not registered; found {capability_id}")
            try:
                provider = registration.factory()
                if not callable(getattr(provider, "project", None)):
                    raise TypeError("factory returned an object without an async project operation")
                self._provider = provider
                self._capability_api_version = registration.capability_api_version
            except Exception as exc:
                if capability_id in required_constraints:
                    raise ConstraintStartupError(f"required capability {capability_id} failed to initialize: {_bounded_failure(exc)}") from exc
                diagnostics.append(_bounded_failure(exc))
        self.startup_diagnostics = tuple(diagnostics)

    @property
    def initialized_capability_ids(self) -> frozenset[str]:
        """Return the singular initialized capability, when present."""

        if self._provider is None:
            return frozenset()
        return frozenset({_CAPABILITY_ID_BY_API_VERSION[self._capability_api_version]})

    @property
    def capability_api_version(self) -> str | None:
        """Return the declared contract version of the initialized provider."""

        return self._capability_api_version

    @property
    def required_capability_id(self) -> str | None:
        """Return the operator-required contract identity, when configured."""

        return self._required_capability_id

    @property
    def clock(self) -> Callable[[], datetime]:
        return self._clock

    async def project(
        self,
        request: ConstraintProjectionRequestV1 | ConstraintProjectionRequestV2,
        *,
        host_max_total_subagents: int | None = None,
        runtime_enforceable: bool = True,
    ) -> ConstraintProjectionV1 | ConstraintProjectionV2 | ConstraintRejected | ConstraintIndeterminate | None:
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
            if self._capability_api_version == INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION:
                if type(request) is not ConstraintProjectionRequestV1 or type(result) is not ConstraintProjectionV1:
                    raise TypeError("a v1 provider must receive and return the v1 constraint union")
                projection: ConstraintProjectionV1 | ConstraintProjectionV2 = ConstraintProjectionV1(
                    request_digest=result.request_digest,
                    agent_revision_digest=result.agent_revision_digest,
                    projection_revision=result.projection_revision,
                    issued_at=result.issued_at,
                    valid_until=result.valid_until,
                    evidence_id=result.evidence_id,
                    evidence_digest=result.evidence_digest,
                    max_total_subagents=result.max_total_subagents,
                )
                if projection.request_digest != request.request_digest:
                    raise ValueError("constraint request digest mismatch")
                if projection.agent_revision_digest != request.agent_revision_digest:
                    raise ValueError("constraint agent revision digest mismatch")
                host_ceiling = host_max_total_subagents
            elif self._capability_api_version == INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2:
                if type(request) is not ConstraintProjectionRequestV2 or type(result) is not ConstraintProjectionV2:
                    raise TypeError("a v2 provider must receive and return the v2 constraint union")
                projection = ConstraintProjectionV2(
                    request_digest=result.request_digest,
                    trusted_context_digest=result.trusted_context_digest,
                    thread_id=result.thread_id,
                    agent_revision_digest=result.agent_revision_digest,
                    profile_revision_digest=result.profile_revision_digest,
                    extension_manifest_digest=result.extension_manifest_digest,
                    extension_generation=result.extension_generation,
                    projection_revision=result.projection_revision,
                    issued_at=result.issued_at,
                    valid_until=result.valid_until,
                    evidence_id=result.evidence_id,
                    evidence_digest=result.evidence_digest,
                    mandatory_obligations=result.mandatory_obligations,
                    max_total_subagents=result.max_total_subagents,
                )
                bindings = (
                    (projection.request_digest, request.request_digest),
                    (projection.trusted_context_digest, request.trusted_context_digest),
                    (projection.thread_id, request.thread_id),
                    (projection.agent_revision_digest, request.agent_revision.digest),
                    (projection.profile_revision_digest, request.profile_revision.digest),
                    (projection.extension_manifest_digest, request.extension_manifest_digest),
                    (projection.extension_generation, request.extension_generation),
                )
                if any(actual != expected for actual, expected in bindings):
                    raise ValueError("constraint v2 projection binding mismatch")
                if not set(projection.mandatory_obligations).issubset(INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS):
                    raise ValueError("constraint v2 projection contains an unsupported mandatory obligation")
                if host_max_total_subagents is not None and host_max_total_subagents != request.host_max_total_subagents:
                    raise ValueError("constraint v2 host ceiling disagrees with its request")
                host_ceiling = request.host_max_total_subagents
            else:  # pragma: no cover - descriptor validation owns this invariant
                raise TypeError("unsupported invocation constraints contract version")
            now = self._clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise ValueError("the host constraint clock must return a timezone-aware datetime")
            if projection.issued_at > now + _MAX_FUTURE_SKEW:
                raise ValueError("constraint projection issued_at exceeds future skew")
            if projection.valid_until <= now:
                raise ValueError("constraint projection is already expired")
            limit = projection.max_total_subagents
            if limit is not None:
                if not runtime_enforceable:
                    raise ValueError("the active runtime cannot enforce max_total_subagents")
                if host_ceiling is not None:
                    limit = min(limit, host_ceiling)
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

"""Finite host-owned visibility grants for durable invocation observation."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from deerflow.runtime.accepted_invocation import AcceptedInvocation
from deerflow.runtime.runs.lifecycle_query import (
    INVOCATION_SOURCE_KINDS,
    LifecycleVisibilityScope,
)

if TYPE_CHECKING:
    from app.runtime.invocation import InvocationAuthorizationOutcome, InvocationPrincipal
    from deerflow.runtime import RunRecord

logger = logging.getLogger(__name__)

_MAX_SELECTOR_VALUES = 128
_MAX_SELECTOR_BYTES = 256
_MAX_GRANT_LIFETIME = timedelta(seconds=30)


def _validate_selector_values(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    if len(values) > _MAX_SELECTOR_VALUES:
        raise ValueError(f"{field_name} exceeds the visibility grant bound")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{field_name} must contain non-empty strings")
        if len(value.encode("utf-8")) > _MAX_SELECTOR_BYTES:
            raise ValueError(f"{field_name} contains an oversized identifier")
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError(f"{field_name} contains a control character")
        normalized.append(value)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} contains a duplicate identifier")
    return tuple(sorted(normalized))


def _source_kind(record: RunRecord) -> str | None:
    accepted = record.accepted_invocation
    if not isinstance(accepted, AcceptedInvocation):
        return None
    value = accepted.origin.source_kind
    return value.value if hasattr(value, "value") else str(value)


@dataclass(frozen=True)
class ServiceObservationGrant:
    """One short-lived finite search scope for an authenticated service.

    Selectors are OR-composed. The runtime still requires the coherent
    ``invocation:observe`` authorization decision before returning data.
    """

    service_id: str
    issued_at: datetime
    valid_until: datetime
    run_ids: tuple[str, ...] = ()
    thread_ids: tuple[str, ...] = ()
    owner_ids: tuple[str, ...] = ()
    source_kinds: tuple[str, ...] = ()
    evidence_digest: str = field(init=False)

    def __post_init__(self) -> None:
        service_id = _validate_selector_values((self.service_id,), field_name="service_id")[0]
        run_ids = _validate_selector_values(self.run_ids, field_name="run_ids")
        thread_ids = _validate_selector_values(self.thread_ids, field_name="thread_ids")
        owner_ids = _validate_selector_values(self.owner_ids, field_name="owner_ids")
        source_kinds = _validate_selector_values(self.source_kinds, field_name="source_kinds")
        if any(value not in INVOCATION_SOURCE_KINDS for value in source_kinds):
            raise ValueError("source_kinds contains an unsupported invocation source kind")
        if len(run_ids) + len(thread_ids) + len(owner_ids) + len(source_kinds) > _MAX_SELECTOR_VALUES:
            raise ValueError("visibility grant exceeds the aggregate selector bound")
        if not (run_ids or thread_ids or owner_ids or source_kinds):
            raise ValueError("visibility grant must contain at least one finite selector")
        if self.issued_at.tzinfo is None or self.valid_until.tzinfo is None:
            raise ValueError("visibility grant timestamps must be timezone-aware")
        if self.valid_until <= self.issued_at:
            raise ValueError("visibility grant must expire after it is issued")
        if self.valid_until - self.issued_at > _MAX_GRANT_LIFETIME:
            raise ValueError("visibility grant lifetime exceeds 30 seconds")
        object.__setattr__(self, "service_id", service_id)
        object.__setattr__(self, "run_ids", run_ids)
        object.__setattr__(self, "thread_ids", thread_ids)
        object.__setattr__(self, "owner_ids", owner_ids)
        object.__setattr__(self, "source_kinds", source_kinds)
        payload = {
            "version": 1,
            "service_id": service_id,
            "run_ids": run_ids,
            "thread_ids": thread_ids,
            "owner_ids": owner_ids,
            "source_kinds": source_kinds,
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        object.__setattr__(self, "evidence_digest", hashlib.sha256(encoded).hexdigest())

    def is_current(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        return self.issued_at <= current < self.valid_until

    def permits(self, record: RunRecord) -> bool:
        return bool(record.run_id in self.run_ids or record.thread_id in self.thread_ids or (record.user_id is not None and record.user_id in self.owner_ids) or _source_kind(record) in self.source_kinds)

    def lifecycle_scope(self, thread_id: str) -> LifecycleVisibilityScope | None:
        """Project only selectors usable by one exact-context store query."""

        allow_context = thread_id in self.thread_ids
        if not (allow_context or self.run_ids or self.owner_ids or self.source_kinds):
            return None
        return LifecycleVisibilityScope(
            thread_id=thread_id,
            allow_context=allow_context,
            run_ids=self.run_ids,
            owner_ids=self.owner_ids,
            source_kinds=self.source_kinds,
        )


class ObservationVisibilityResolver(Protocol):
    """Resolve current operator-established visibility for one principal."""

    async def resolve(self, principal: InvocationPrincipal) -> ServiceObservationGrant | None: ...


def is_authenticated_service(principal: InvocationPrincipal) -> bool:
    identity = principal.identity
    return bool(identity is not None and identity.effective_subject.kind == "service" and identity.effective_subject.subject_id == principal.user_id)


def audit_service_observation(
    grant: ServiceObservationGrant,
    *,
    target_kind: str,
    authorization: InvocationAuthorizationOutcome,
) -> str:
    """Emit bounded audit correlation without target content or provider data."""

    correlation_id = uuid4().hex
    logger.info(
        "Scoped service invocation observation evaluated",
        extra={
            "correlation_id": correlation_id,
            "observer_service_id": grant.service_id,
            "visibility_evidence_digest": grant.evidence_digest,
            "observation_target_kind": target_kind,
            "authorization_outcome": authorization.value,
        },
    )
    return correlation_id


def diagnose_visibility_resolution_failure(
    principal: InvocationPrincipal,
    error: Exception,
) -> str:
    """Record a bounded resolver failure without retaining exception text."""

    correlation_id = uuid4().hex
    logger.warning(
        "Service observation visibility resolution failed",
        extra={
            "correlation_id": correlation_id,
            "observer_service_id": principal.user_id,
            "diagnostic_code": "visibility_resolution_failed",
            "error_class": type(error).__name__[:96],
        },
    )
    return correlation_id


class ConfiguredServiceObservationGrantResolver:
    """Resolve a fresh immutable grant from hot-reloaded operator config."""

    def __init__(
        self,
        grants: Callable[[], tuple[object, ...]],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        validity_seconds: float = 5.0,
    ) -> None:
        if not 0 < validity_seconds <= _MAX_GRANT_LIFETIME.total_seconds():
            raise ValueError("visibility grant validity must be between zero and 30 seconds")
        self._grants = grants
        self._clock = clock
        self._validity_seconds = validity_seconds

    async def resolve(self, principal: InvocationPrincipal) -> ServiceObservationGrant | None:
        if not is_authenticated_service(principal):
            return None
        configured = self._grants()
        matching = [item for item in configured if getattr(item, "service_id", None) == principal.user_id]
        if not matching:
            return None
        if len(matching) != 1:
            raise ValueError("service observation grants contain a duplicate service")
        item = matching[0]
        now = self._clock()
        return ServiceObservationGrant(
            service_id=principal.user_id or "",
            run_ids=tuple(getattr(item, "run_ids", ())),
            thread_ids=tuple(getattr(item, "thread_ids", ())),
            owner_ids=tuple(getattr(item, "owner_ids", ())),
            source_kinds=tuple(getattr(item, "source_kinds", ())),
            issued_at=now,
            valid_until=now + timedelta(seconds=self._validity_seconds),
        )


__all__ = [
    "ConfiguredServiceObservationGrantResolver",
    "ObservationVisibilityResolver",
    "ServiceObservationGrant",
    "audit_service_observation",
    "diagnose_visibility_resolution_failure",
    "is_authenticated_service",
]

"""Host-independent restrictive invocation-constraint contracts."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol, runtime_checkable

from deerflow_extension_api.contributors import SealedOriginV1, TrustedRunContextV1
from deerflow_extension_api.health import CapabilityHealthProbe
from deerflow_extension_api.identity import InvocationIdentityV1

INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION = "1.0"
INVOCATION_CONSTRAINTS_KIND = "invocation_constraints"
INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY = "invocation_constraints.v1"

_DIGEST = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,127}$", re.ASCII)
_MAX_VALIDITY = timedelta(minutes=15)
_MAX_SUBAGENTS = 2_147_483_647


def _validate_digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 64-character lowercase SHA-256 digest")
    return value


def _validate_identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 1-128 character ASCII identifier")
    return value


def _validate_aware(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime")
    return value


@dataclass(frozen=True)
class ConstraintProjectionRequestV1:
    """The two immutable identities to which a projection must bind."""

    request_digest: str
    agent_revision_digest: str
    identity: InvocationIdentityV1 | None = None
    origin: SealedOriginV1 | None = None
    trusted_context: TrustedRunContextV1 | None = None

    def __post_init__(self) -> None:
        _validate_digest(self.request_digest, field_name="request_digest")
        _validate_digest(self.agent_revision_digest, field_name="agent_revision_digest")
        if self.identity is not None and not isinstance(self.identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1 or None")
        if self.origin is not None and not isinstance(self.origin, SealedOriginV1):
            raise TypeError("origin must be SealedOriginV1 or None")
        if self.trusted_context is not None and not isinstance(self.trusted_context, TrustedRunContextV1):
            raise TypeError("trusted_context must be TrustedRunContextV1 or None")


@dataclass(frozen=True)
class ConstraintProjectionV1:
    """A restrictive, short-lived projection returned by one provider."""

    request_digest: str
    agent_revision_digest: str
    projection_revision: str
    issued_at: datetime
    valid_until: datetime
    evidence_id: str
    evidence_digest: str
    max_total_subagents: int | None = None

    def __post_init__(self) -> None:
        _validate_digest(self.request_digest, field_name="request_digest")
        _validate_digest(self.agent_revision_digest, field_name="agent_revision_digest")
        _validate_identifier(self.projection_revision, field_name="projection_revision")
        issued_at = _validate_aware(self.issued_at, field_name="issued_at")
        valid_until = _validate_aware(self.valid_until, field_name="valid_until")
        if valid_until <= issued_at:
            raise ValueError("valid_until must be later than issued_at")
        if valid_until - issued_at > _MAX_VALIDITY:
            raise ValueError("constraint projections may be valid for at most 15 minutes")
        _validate_identifier(self.evidence_id, field_name="evidence_id")
        _validate_digest(self.evidence_digest, field_name="evidence_digest")
        limit = self.max_total_subagents
        if limit is not None and (type(limit) is not int or limit <= 0 or limit > _MAX_SUBAGENTS):
            raise ValueError("max_total_subagents must be a possible positive integer")


@dataclass(frozen=True)
class ConstraintRejected:
    """The provider authoritatively rejected this invocation."""


@dataclass(frozen=True)
class ConstraintIndeterminate:
    """The provider could not safely project constraints."""


@runtime_checkable
class InvocationConstraintsProvider(Protocol):
    async def project(
        self,
        request: ConstraintProjectionRequestV1,
    ) -> ConstraintProjectionV1 | ConstraintRejected | ConstraintIndeterminate:
        return ConstraintIndeterminate()


@dataclass(frozen=True)
class InvocationConstraintsProviderFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], InvocationConstraintsProvider]
    kind: Literal["invocation_constraints"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.contribution_id, field_name="constraint contribution_id")
        if self.capability_api_version != INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported invocation-constraints capability API version {self.capability_api_version!r}; expected {INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION!r}")
        if self.kind != INVOCATION_CONSTRAINTS_KIND:
            raise ValueError(f"invocation-constraints kind must be {INVOCATION_CONSTRAINTS_KIND!r}")
        if not callable(self.factory):
            raise TypeError("invocation-constraints factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("invocation-constraints health_probe must be callable")


__all__ = [
    "INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION",
    "INVOCATION_CONSTRAINTS_KIND",
    "INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY",
    "ConstraintIndeterminate",
    "ConstraintProjectionRequestV1",
    "ConstraintProjectionV1",
    "ConstraintRejected",
    "InvocationConstraintsProvider",
    "InvocationConstraintsProviderFactory",
]

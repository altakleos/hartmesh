"""Host-independent restrictive invocation-constraint contracts."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol, runtime_checkable

from deerflow_extension_api.contributors import (
    NamespacedContextReferenceV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)
from deerflow_extension_api.health import CapabilityHealthProbe
from deerflow_extension_api.identifiers import validate_thread_identifier
from deerflow_extension_api.identity import InvocationIdentityV1

INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION = "1.0"
INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2 = "2.0"
INVOCATION_CONSTRAINTS_KIND = "invocation_constraints"
INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY = "invocation_constraints.v1"
INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2 = "invocation_constraints.v2"
INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS = frozenset({"max_total_subagents"})

_DIGEST = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,127}$", re.ASCII)
_MAX_VALIDITY = timedelta(minutes=15)
_MAX_SUBAGENTS = 2_147_483_647
_SUPPORTED_CAPABILITY_API_VERSIONS = frozenset(
    {
        INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
        INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    }
)


def _validate_digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 64-character lowercase SHA-256 digest")
    return value


def _validate_policy_identifier(value: object, *, field_name: str) -> str:
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
        _validate_policy_identifier(self.projection_revision, field_name="projection_revision")
        issued_at = _validate_aware(self.issued_at, field_name="issued_at")
        valid_until = _validate_aware(self.valid_until, field_name="valid_until")
        if valid_until <= issued_at:
            raise ValueError("valid_until must be later than issued_at")
        if valid_until - issued_at > _MAX_VALIDITY:
            raise ValueError("constraint projections may be valid for at most 15 minutes")
        _validate_policy_identifier(self.evidence_id, field_name="evidence_id")
        _validate_digest(self.evidence_digest, field_name="evidence_digest")
        limit = self.max_total_subagents
        if limit is not None and (type(limit) is not int or limit <= 0 or limit > _MAX_SUBAGENTS):
            raise ValueError("max_total_subagents must be a possible positive integer")


@dataclass(frozen=True)
class ConstraintProjectionRequestV2:
    """Safe policy lookup and binding facts for the v2 projection."""

    identity: InvocationIdentityV1
    origin: SealedOriginV1
    policy_lookup_references: tuple[NamespacedContextReferenceV1, ...]
    thread_id: str
    external_key_reference: str | None
    agent_revision: ResolvedAgentRevisionReferenceV1
    profile_revision: ResolvedProfileRevisionReferenceV1
    request_digest: str
    trusted_context_digest: str
    extension_manifest_digest: str
    extension_generation: int
    host_max_total_subagents: int

    def __post_init__(self) -> None:
        if not isinstance(self.identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1")
        if not isinstance(self.origin, SealedOriginV1):
            raise TypeError("origin must be SealedOriginV1")
        references = tuple(self.policy_lookup_references)
        object.__setattr__(self, "policy_lookup_references", references)
        if len(references) > 32:
            raise ValueError("policy_lookup_references accepts at most 32 references")
        if any(not isinstance(reference, NamespacedContextReferenceV1) for reference in references):
            raise TypeError("policy_lookup_references must contain NamespacedContextReferenceV1 values")
        if any(reference.reference.purpose != "correlation" for reference in references):
            raise ValueError("policy lookup references must be correlation references")
        keys = [reference.fully_qualified_key for reference in references]
        if len(keys) != len(set(keys)):
            raise ValueError("policy lookup references reject duplicate fully qualified keys")
        canonical_references = json.dumps(
            [reference.to_json() for reference in references],
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(canonical_references) > 8192:
            raise ValueError("canonical policy lookup references are limited to 8 KiB")
        validate_thread_identifier(self.thread_id, field_name="thread_id")
        if self.external_key_reference is not None:
            if not isinstance(self.external_key_reference, str) or not self.external_key_reference or len(self.external_key_reference.encode("utf-8")) > 384:
                raise ValueError("external_key_reference must be a bounded non-empty string or None")
        if not isinstance(self.agent_revision, ResolvedAgentRevisionReferenceV1):
            raise TypeError("agent_revision must be ResolvedAgentRevisionReferenceV1")
        if not isinstance(self.profile_revision, ResolvedProfileRevisionReferenceV1):
            raise TypeError("profile_revision must be ResolvedProfileRevisionReferenceV1")
        _validate_digest(self.request_digest, field_name="request_digest")
        _validate_digest(self.trusted_context_digest, field_name="trusted_context_digest")
        _validate_digest(self.extension_manifest_digest, field_name="extension_manifest_digest")
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            raise ValueError("extension_generation must be a non-negative integer")
        if type(self.host_max_total_subagents) is not int or not 0 <= self.host_max_total_subagents <= _MAX_SUBAGENTS:
            raise ValueError("host_max_total_subagents must be a possible non-negative integer")


@dataclass(frozen=True)
class ConstraintProjectionV2:
    """A short-lived projection bound to v2 request and execution facts."""

    request_digest: str
    trusted_context_digest: str
    thread_id: str
    agent_revision_digest: str
    profile_revision_digest: str
    extension_manifest_digest: str
    extension_generation: int
    projection_revision: str
    issued_at: datetime
    valid_until: datetime
    evidence_id: str
    evidence_digest: str
    mandatory_obligations: tuple[str, ...] = ()
    max_total_subagents: int | None = None

    def __post_init__(self) -> None:
        _validate_digest(self.request_digest, field_name="request_digest")
        _validate_digest(self.trusted_context_digest, field_name="trusted_context_digest")
        validate_thread_identifier(self.thread_id, field_name="thread_id")
        _validate_digest(self.agent_revision_digest, field_name="agent_revision_digest")
        _validate_digest(self.profile_revision_digest, field_name="profile_revision_digest")
        _validate_digest(self.extension_manifest_digest, field_name="extension_manifest_digest")
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            raise ValueError("extension_generation must be a non-negative integer")
        _validate_policy_identifier(self.projection_revision, field_name="projection_revision")
        issued_at = _validate_aware(self.issued_at, field_name="issued_at")
        valid_until = _validate_aware(self.valid_until, field_name="valid_until")
        if valid_until <= issued_at:
            raise ValueError("valid_until must be later than issued_at")
        if valid_until - issued_at > _MAX_VALIDITY:
            raise ValueError("constraint projections may be valid for at most 15 minutes")
        _validate_policy_identifier(self.evidence_id, field_name="evidence_id")
        _validate_digest(self.evidence_digest, field_name="evidence_digest")
        obligations = tuple(self.mandatory_obligations)
        if len(obligations) > 16:
            raise ValueError("mandatory_obligations accepts at most 16 identifiers")
        if len(obligations) != len(set(obligations)):
            raise ValueError("mandatory_obligations must not contain duplicates")
        for obligation in obligations:
            _validate_policy_identifier(obligation, field_name="mandatory obligation")
        object.__setattr__(self, "mandatory_obligations", tuple(sorted(obligations)))
        limit = self.max_total_subagents
        if limit is not None and (type(limit) is not int or not 0 <= limit <= _MAX_SUBAGENTS):
            raise ValueError("max_total_subagents must be a possible non-negative integer")
        if (limit is not None) != ("max_total_subagents" in obligations):
            raise ValueError("max_total_subagents and its mandatory obligation must be supplied together")


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


@runtime_checkable
class InvocationConstraintsProviderV2(Protocol):
    async def project(
        self,
        request: ConstraintProjectionRequestV2,
    ) -> ConstraintProjectionV2 | ConstraintRejected | ConstraintIndeterminate:
        return ConstraintIndeterminate()


@dataclass(frozen=True)
class InvocationConstraintsProviderFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], InvocationConstraintsProvider | InvocationConstraintsProviderV2]
    kind: Literal["invocation_constraints"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_policy_identifier(self.contribution_id, field_name="constraint contribution_id")
        if self.capability_api_version not in _SUPPORTED_CAPABILITY_API_VERSIONS:
            raise ValueError(f"unsupported invocation-constraints capability API version {self.capability_api_version!r}; expected one of {sorted(_SUPPORTED_CAPABILITY_API_VERSIONS)!r}")
        if self.kind != INVOCATION_CONSTRAINTS_KIND:
            raise ValueError(f"invocation-constraints kind must be {INVOCATION_CONSTRAINTS_KIND!r}")
        if not callable(self.factory):
            raise TypeError("invocation-constraints factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("invocation-constraints health_probe must be callable")


__all__ = [
    "INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION",
    "INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2",
    "INVOCATION_CONSTRAINTS_KIND",
    "INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY",
    "INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2",
    "INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS",
    "ConstraintIndeterminate",
    "ConstraintProjectionRequestV1",
    "ConstraintProjectionRequestV2",
    "ConstraintProjectionV1",
    "ConstraintProjectionV2",
    "ConstraintRejected",
    "InvocationConstraintsProvider",
    "InvocationConstraintsProviderV2",
    "InvocationConstraintsProviderFactory",
]

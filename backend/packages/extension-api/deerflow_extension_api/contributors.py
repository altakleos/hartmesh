"""Host-independent invocation contributor contracts.

Contributors receive narrow, immutable host projections and may return only
bounded scalar references in their own namespace.  Credentials and arbitrary
host objects never cross this contract boundary.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from deerflow_extension_api.health import CapabilityHealthProbe
from deerflow_extension_api.identity import InvocationIdentityV1

ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION = "1.0"
RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION = "1.0"
ORIGIN_CONTRIBUTOR_KIND = "origin_contributor"
RUN_CONTEXT_CONTRIBUTOR_KIND = "run_context_contributor"

type StorageClass = Literal["persistable", "runtime_only"]
type ReferencePurpose = Literal["execution", "correlation", "secret_handle"]
type SafeScalarV1 = str | int | bool | None
type SafeValueV1 = SafeScalarV1 | tuple[SafeScalarV1, ...] | list[SafeScalarV1]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,63}$", re.ASCII)
_MAX_REFERENCES = 32
_MAX_STRING_BYTES = 1024
_MAX_CANONICAL_BYTES = 8192


def _validate_identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 1-64 character ASCII identifier")
    return value


def _validate_scalar(value: object) -> None:
    # bool must be checked before int because it is an int subclass.
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("safe context references reject non-finite numbers")
        raise TypeError("safe context references accept integers, not floating-point values")
    if isinstance(value, str):
        if len(value.encode("utf-8")) > _MAX_STRING_BYTES:
            raise ValueError("safe context reference strings are limited to 1 KiB UTF-8")
        return
    raise TypeError("safe context references accept only strings, integers, booleans, null, or lists of those values")


@dataclass(frozen=True)
class SafeContextReferenceV1:
    key: str
    value: SafeValueV1
    storage_class: StorageClass
    purpose: ReferencePurpose

    def __post_init__(self) -> None:
        _validate_identifier(self.key, field_name="reference key")
        if self.storage_class not in ("persistable", "runtime_only"):
            raise ValueError("storage_class must be 'persistable' or 'runtime_only'")
        if self.purpose not in ("execution", "correlation", "secret_handle"):
            raise ValueError("purpose must be 'execution', 'correlation', or 'secret_handle'")
        value = self.value
        if isinstance(value, (list, tuple)):
            for item in value:
                _validate_scalar(item)
            # Freeze caller-owned lists so post-validation mutation cannot alter
            # an accepted contribution or its digest.
            object.__setattr__(self, "value", tuple(value))
        else:
            _validate_scalar(value)
        if self.purpose == "secret_handle" and not isinstance(self.value, str):
            raise TypeError("a secret_handle reference must contain one stable string identifier")


def _validate_contribution(namespace: str, references: tuple[SafeContextReferenceV1, ...]) -> None:
    _validate_identifier(namespace, field_name="contribution namespace")
    if len(references) > _MAX_REFERENCES:
        raise ValueError("a contributor may return at most 32 references")
    seen: set[str] = set()
    for reference in references:
        if not isinstance(reference, SafeContextReferenceV1):
            raise TypeError("contribution references must be SafeContextReferenceV1 values")
        if reference.key in seen:
            raise ValueError(f"duplicate reference key {reference.key!r}")
        seen.add(reference.key)
    canonical = json.dumps(
        {
            "namespace": namespace,
            "references": [
                {
                    "key": reference.key,
                    "purpose": reference.purpose,
                    "storage_class": reference.storage_class,
                    "value": reference.value,
                }
                for reference in references
            ],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(canonical) > _MAX_CANONICAL_BYTES:
        raise ValueError("a canonical contributor result is limited to 8 KiB")


@dataclass(frozen=True)
class OriginContributionRequestV1:
    source_kind: str
    authenticated_subject_reference: str | None = None
    source_references: tuple[SafeContextReferenceV1, ...] = ()
    identity: InvocationIdentityV1 | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_references", tuple(self.source_references))
        if self.identity is not None and not isinstance(self.identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1 or None")


@dataclass(frozen=True)
class OriginContributionV1:
    namespace: str
    references: tuple[SafeContextReferenceV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "references", tuple(self.references))
        _validate_contribution(self.namespace, self.references)


@dataclass(frozen=True)
class PrincipalProjectionV1:
    user_id: str | None = None
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    channel_user_id: str | None = None
    is_internal: bool = False
    identity: InvocationIdentityV1 | None = None

    def __post_init__(self) -> None:
        identity = self.identity
        if identity is None:
            if self.is_internal and (self.channel_user_id is not None or self.role not in {"internal", "service"}):
                object.__setattr__(self, "is_internal", False)
            return
        if not isinstance(identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1 or None")
        subject = identity.effective_subject
        object.__setattr__(self, "user_id", subject.subject_id)
        object.__setattr__(self, "role", subject.role)
        object.__setattr__(self, "oauth_provider", subject.oauth_provider)
        object.__setattr__(self, "oauth_id", subject.oauth_id)
        object.__setattr__(self, "is_internal", subject.kind == "service")


@dataclass(frozen=True)
class SealedOriginV1:
    source_kind: str
    references: tuple[SafeContextReferenceV1, ...] = ()
    digest: str = ""


@dataclass(frozen=True)
class ResolvedAgentRevisionReferenceV1:
    agent_id: str
    digest: str


@dataclass(frozen=True)
class RunContextContributionRequestV1:
    principal: PrincipalProjectionV1
    origin: SealedOriginV1
    thread_id: str
    agent_revision: ResolvedAgentRevisionReferenceV1
    external_key_reference: str | None = None


@dataclass(frozen=True)
class RunContextContributionV1:
    namespace: str
    references: tuple[SafeContextReferenceV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "references", tuple(self.references))
        _validate_contribution(self.namespace, self.references)


@runtime_checkable
class OriginContributor(Protocol):
    async def contribute(self, request: OriginContributionRequestV1) -> OriginContributionV1 | None:
        return None


@runtime_checkable
class RunContextContributor(Protocol):
    async def contribute(self, request: RunContextContributionRequestV1) -> RunContextContributionV1 | None:
        return None


@dataclass(frozen=True)
class OriginContributorFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], OriginContributor]
    kind: Literal["origin_contributor"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.contribution_id, field_name="origin contributor contribution_id")
        if self.capability_api_version != ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported origin contributor capability API version {self.capability_api_version!r}; expected {ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION!r}")
        if self.kind != ORIGIN_CONTRIBUTOR_KIND:
            raise ValueError(f"origin contributor kind must be {ORIGIN_CONTRIBUTOR_KIND!r}")
        if not callable(self.factory):
            raise TypeError("origin contributor factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("origin contributor health_probe must be callable")


@dataclass(frozen=True)
class RunContextContributorFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], RunContextContributor]
    kind: Literal["run_context_contributor"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.contribution_id, field_name="run-context contributor contribution_id")
        if self.capability_api_version != RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported run-context contributor capability API version {self.capability_api_version!r}; expected {RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION!r}")
        if self.kind != RUN_CONTEXT_CONTRIBUTOR_KIND:
            raise ValueError(f"run-context contributor kind must be {RUN_CONTEXT_CONTRIBUTOR_KIND!r}")
        if not callable(self.factory):
            raise TypeError("run-context contributor factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("run-context contributor health_probe must be callable")

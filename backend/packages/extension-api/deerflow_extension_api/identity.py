"""Host-independent immutable invocation identity projections."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

_MAX_IDENTITY_BYTES = 8192
_MAX_IDENTIFIER_BYTES = 256
_MAX_ROLE_BYTES = 128
_MAX_DEPTH = 8


def _require_fields(value: Mapping[str, Any], expected: frozenset[str], *, record_name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{record_name} has unknown or missing fields")


def _bounded_text(value: object, *, field_name: str, max_bytes: int, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field_name} must be a non-empty string of at most {max_bytes} UTF-8 bytes")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field_name} must not contain control characters")
    return value


def _freeze_json(value: Any, *, depth: int = 0) -> Any:
    if depth > _MAX_DEPTH:
        raise ValueError("identity attributes are too deeply nested")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("identity attributes reject non-finite numbers")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("identity attribute keys must be strings")
        return MappingProxyType({key: _freeze_json(item, depth=depth + 1) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, depth=depth + 1) for item in value)
    raise TypeError("identity attributes must contain only JSON-safe values")


def thaw_identity_value(value: Any) -> Any:
    """Return a JSON-safe copy of a frozen identity value."""
    if isinstance(value, Mapping):
        return {str(key): thaw_identity_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_identity_value(item) for item in value]
    return value


def _freeze_attributes(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise TypeError("identity attributes must be a mapping")
    frozen = _freeze_json(value)
    encoded = json.dumps(
        thaw_identity_value(frozen),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > _MAX_IDENTITY_BYTES:
        raise ValueError("canonical identity attributes are limited to 8 KiB")
    return frozen


@dataclass(frozen=True)
class EffectiveSubjectV1:
    """The human or service whose authority the invocation exercises."""

    kind: Literal["human", "service"]
    subject_id: str
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in ("human", "service"):
            raise ValueError("effective subject kind must be 'human' or 'service'")
        _bounded_text(self.subject_id, field_name="effective subject id", max_bytes=_MAX_IDENTIFIER_BYTES)
        _bounded_text(self.role, field_name="effective subject role", max_bytes=_MAX_ROLE_BYTES, optional=True)
        _bounded_text(self.oauth_provider, field_name="OAuth provider", max_bytes=_MAX_IDENTIFIER_BYTES, optional=True)
        _bounded_text(self.oauth_id, field_name="OAuth id", max_bytes=_MAX_IDENTIFIER_BYTES, optional=True)
        object.__setattr__(self, "attributes", _freeze_attributes(self.attributes))

    def to_json(self) -> dict[str, Any]:
        return {
            "version": 1,
            "kind": self.kind,
            "subject_id": self.subject_id,
            "role": self.role,
            "oauth_provider": self.oauth_provider,
            "oauth_id": self.oauth_id,
            "attributes": thaw_identity_value(self.attributes),
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> EffectiveSubjectV1:
        if not isinstance(value, Mapping) or value.get("version") != 1:
            raise ValueError("effective subject must use identity version 1")
        _require_fields(
            value,
            frozenset(
                {
                    "version",
                    "kind",
                    "subject_id",
                    "role",
                    "oauth_provider",
                    "oauth_id",
                    "attributes",
                }
            ),
            record_name="effective subject",
        )
        return cls(
            kind=value.get("kind"),
            subject_id=value.get("subject_id"),
            role=value.get("role"),
            oauth_provider=value.get("oauth_provider"),
            oauth_id=value.get("oauth_id"),
            attributes=value.get("attributes") or {},
        )


@dataclass(frozen=True)
class ActingServiceV1:
    """The authenticated service representing or delegating to the subject."""

    service_id: str
    role: str | None = "service"
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _bounded_text(self.service_id, field_name="acting service id", max_bytes=_MAX_IDENTIFIER_BYTES)
        _bounded_text(self.role, field_name="acting service role", max_bytes=_MAX_ROLE_BYTES, optional=True)
        object.__setattr__(self, "attributes", _freeze_attributes(self.attributes))

    def to_json(self) -> dict[str, Any]:
        return {
            "version": 1,
            "service_id": self.service_id,
            "role": self.role,
            "attributes": thaw_identity_value(self.attributes),
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> ActingServiceV1:
        if not isinstance(value, Mapping) or value.get("version") != 1:
            raise ValueError("acting service must use identity version 1")
        _require_fields(
            value,
            frozenset({"version", "service_id", "role", "attributes"}),
            record_name="acting service",
        )
        return cls(
            service_id=value.get("service_id"),
            role=value.get("role"),
            attributes=value.get("attributes") or {},
        )


@dataclass(frozen=True)
class InvocationIdentityV1:
    """One immutable effective subject plus an optional acting service."""

    effective_subject: EffectiveSubjectV1
    acting_service: ActingServiceV1 | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.effective_subject, EffectiveSubjectV1):
            raise TypeError("effective_subject must be EffectiveSubjectV1")
        if self.acting_service is not None and not isinstance(self.acting_service, ActingServiceV1):
            raise TypeError("acting_service must be ActingServiceV1 or None")
        encoded = json.dumps(
            self.to_json(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > _MAX_IDENTITY_BYTES:
            raise ValueError("canonical invocation identity is limited to 8 KiB")

    def to_json(self) -> dict[str, Any]:
        return {
            "version": 1,
            "effective_subject": self.effective_subject.to_json(),
            "acting_service": None if self.acting_service is None else self.acting_service.to_json(),
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> InvocationIdentityV1:
        if not isinstance(value, Mapping) or value.get("version") != 1:
            raise ValueError("invocation identity must use version 1")
        _require_fields(
            value,
            frozenset({"version", "effective_subject", "acting_service"}),
            record_name="invocation identity",
        )
        acting = value.get("acting_service")
        return cls(
            effective_subject=EffectiveSubjectV1.from_json(value.get("effective_subject")),
            acting_service=None if acting is None else ActingServiceV1.from_json(acting),
        )


__all__ = [
    "ActingServiceV1",
    "EffectiveSubjectV1",
    "InvocationIdentityV1",
    "thaw_identity_value",
]

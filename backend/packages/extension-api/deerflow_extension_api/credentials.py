"""Host-independent, secret-free authentication evidence contracts."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Literal

from deerflow_extension_api.identity import InvocationIdentityV1
from deerflow_extension_api.tenant import TenantReferenceV1

AuthenticationMethod = Literal[
    "session",
    "personal_access_token",
    "internal_service",
    "channel",
    "development_bypass",
]

AUTHORITY_CANONICALIZATION_VERSION = 1
AUTHORITY_ALIASES_V1: Mapping[str, str] = MappingProxyType(
    {
        "run:cancel": "runs:cancel",
        "run:create": "runs:create",
        "run:read": "runs:read",
        "thread:delete": "threads:delete",
        "thread:read": "threads:read",
        "thread:write": "threads:write",
    }
)
_AUTHORITY_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}:[a-z][a-z0-9_-]{0,63}$")
_CATEGORY_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_METHODS = frozenset(
    {
        "session",
        "personal_access_token",
        "internal_service",
        "channel",
        "development_bypass",
    }
)
_MAX_AUTHORITIES = 64
_MAX_CATEGORIES = 16


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def canonicalize_authority_v1(
    authorities: Iterable[str],
    *,
    aliases: Mapping[str, str] | None = None,
    allowed: frozenset[str] | set[str] | None = None,
) -> tuple[str, ...]:
    """Return the version-1 authority set in canonical order.

    Aliases are resolved before allowlist validation.  The function deliberately
    rejects malformed and unknown values instead of letting evidence silently
    describe broader or different authority than the host evaluated.
    """

    if isinstance(authorities, (str, bytes)):
        raise TypeError("authorities must be an iterable of identifiers")
    alias_map = dict(AUTHORITY_ALIASES_V1)
    for alias, target in dict(aliases or {}).items():
        existing = alias_map.get(alias)
        if existing is not None and existing != target:
            raise ValueError("authority alias conflicts with canonical version 1")
        alias_map[alias] = target
    for alias, target in alias_map.items():
        if not isinstance(alias, str) or _AUTHORITY_IDENTIFIER.fullmatch(alias) is None:
            raise ValueError("authority alias must be a canonical authority identifier")
        if not isinstance(target, str) or _AUTHORITY_IDENTIFIER.fullmatch(target) is None:
            raise ValueError("authority alias target must be a canonical authority identifier")
    normalized: set[str] = set()
    for authority in authorities:
        if not isinstance(authority, str) or _AUTHORITY_IDENTIFIER.fullmatch(authority) is None:
            raise ValueError("authority identifier must use lowercase resource:action syntax")
        target = alias_map.get(authority, authority)
        if target in alias_map:
            raise ValueError("authority aliases must resolve directly to a canonical identifier")
        if allowed is not None and target not in allowed:
            raise ValueError(f"unknown authority: {target}")
        normalized.add(target)
    if len(normalized) > _MAX_AUTHORITIES:
        raise ValueError(f"effective authority accepts at most {_MAX_AUTHORITIES} identifiers")
    return tuple(sorted(normalized))


def effective_authority_digest_v1(authorities: Iterable[str]) -> str:
    """Digest one canonical authority set without persisting the scope list."""

    canonical = canonicalize_authority_v1(authorities)
    return hashlib.sha256(
        _canonical_json(
            {
                "version": AUTHORITY_CANONICALIZATION_VERSION,
                "authorities": list(canonical),
            }
        )
    ).hexdigest()


def authority_categories_v1(authorities: Iterable[str]) -> tuple[str, ...]:
    """Project a bounded coarse resource list from canonical authority."""

    canonical = canonicalize_authority_v1(authorities)
    categories = tuple(sorted({authority.partition(":")[0] for authority in canonical}))
    if len(categories) > _MAX_CATEGORIES:
        raise ValueError(f"authority evidence accepts at most {_MAX_CATEGORIES} categories")
    return categories


def _normalize_datetime(value: datetime | None, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime or None")
    return value.astimezone(UTC)


def _datetime_json(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _datetime_from_json(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"{field_name} must be a bounded ISO-8601 timestamp or null")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    return _normalize_datetime(parsed, field_name=field_name)


@dataclass(frozen=True)
class CredentialEvidenceV1:
    """Immutable, safe evidence for the credential used at admission.

    The authority list itself remains management data.  Durable evidence holds
    only its canonical digest and coarse resource categories.
    """

    method: AuthenticationMethod
    credential_ref: str | None
    effective_authority_digest: str
    authority_categories: tuple[str, ...] = ()
    issued_at: datetime | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.method not in _METHODS:
            raise ValueError("credential method is unsupported")
        if self.credential_ref is not None:
            if not isinstance(self.credential_ref, str) or _REFERENCE.fullmatch(self.credential_ref) is None:
                raise ValueError("credential_ref must be an opaque URL-safe identifier")
            if self.credential_ref.startswith("dfp_"):
                raise ValueError("credential_ref must not contain bearer-token material")
        if self.method == "personal_access_token":
            if self.credential_ref is None:
                raise ValueError("personal access token evidence requires credential_ref")
            try:
                parsed_reference = uuid.UUID(self.credential_ref)
            except (ValueError, AttributeError) as exc:
                raise ValueError("personal access token credential_ref must be a UUID4") from exc
            if parsed_reference.version != 4 or str(parsed_reference) != self.credential_ref:
                raise ValueError("personal access token credential_ref must be a canonical UUID4")
        if not isinstance(self.effective_authority_digest, str) or _DIGEST.fullmatch(self.effective_authority_digest) is None:
            raise ValueError("effective_authority_digest must be a lowercase SHA-256 digest")
        categories = tuple(self.authority_categories)
        if len(categories) > _MAX_CATEGORIES:
            raise ValueError(f"authority evidence accepts at most {_MAX_CATEGORIES} categories")
        if any(not isinstance(item, str) or _CATEGORY_IDENTIFIER.fullmatch(item) is None for item in categories):
            raise ValueError("authority categories must be lowercase bounded identifiers")
        if tuple(sorted(set(categories))) != categories:
            raise ValueError("authority categories must be sorted and deduplicated")
        object.__setattr__(self, "authority_categories", categories)
        issued_at = _normalize_datetime(self.issued_at, field_name="issued_at")
        expires_at = _normalize_datetime(self.expires_at, field_name="expires_at")
        if issued_at is not None and expires_at is not None and expires_at < issued_at:
            raise ValueError("expires_at must not precede issued_at")
        object.__setattr__(self, "issued_at", issued_at)
        object.__setattr__(self, "expires_at", expires_at)
        if len(_canonical_json(self.to_json())) > 2048:
            raise ValueError("canonical credential evidence is limited to 2 KiB")

    def to_json(self) -> dict[str, object]:
        return {
            "version": 1,
            "method": self.method,
            "credential_ref": self.credential_ref,
            "effective_authority_digest": self.effective_authority_digest,
            "authority_categories": list(self.authority_categories),
            "issued_at": _datetime_json(self.issued_at),
            "expires_at": _datetime_json(self.expires_at),
        }

    @classmethod
    def from_json(cls, value: object) -> CredentialEvidenceV1:
        expected = {
            "version",
            "method",
            "credential_ref",
            "effective_authority_digest",
            "authority_categories",
            "issued_at",
            "expires_at",
        }
        if not isinstance(value, Mapping) or value.get("version") != 1 or set(value) != expected:
            raise ValueError("credential evidence has unknown fields or an unsupported version")
        categories = value["authority_categories"]
        if not isinstance(categories, list):
            raise ValueError("credential authority_categories must be a list")
        return cls(
            method=value["method"],  # type: ignore[arg-type]
            credential_ref=value["credential_ref"],  # type: ignore[arg-type]
            effective_authority_digest=value["effective_authority_digest"],  # type: ignore[arg-type]
            authority_categories=tuple(categories),  # type: ignore[arg-type]
            issued_at=_datetime_from_json(value["issued_at"], field_name="issued_at"),
            expires_at=_datetime_from_json(value["expires_at"], field_name="expires_at"),
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_json())).hexdigest()


@dataclass(frozen=True)
class VerifiedActorContextV1:
    """One verified principal, credential projection, and tenant reference."""

    identity: InvocationIdentityV1
    credential: CredentialEvidenceV1
    tenant: TenantReferenceV1

    def __post_init__(self) -> None:
        if not isinstance(self.identity, InvocationIdentityV1):
            raise TypeError("verified actor identity must be InvocationIdentityV1")
        if not isinstance(self.credential, CredentialEvidenceV1):
            raise TypeError("verified actor credential must be CredentialEvidenceV1")
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("verified actor tenant must be TenantReferenceV1")
        if self.credential.method == "personal_access_token" and (self.identity.effective_subject.kind != "human" or self.identity.acting_service is not None):
            raise ValueError("a PAT is a user credential and cannot be an acting service")
        if len(_canonical_json(self.to_json())) > 12 * 1024:
            raise ValueError("canonical verified actor context is limited to 12 KiB")

    def to_json(self) -> dict[str, object]:
        return {
            "version": 1,
            "identity": self.identity.to_json(),
            "credential": self.credential.to_json(),
            "tenant": self.tenant.to_json(),
        }

    @classmethod
    def from_json(cls, value: object) -> VerifiedActorContextV1:
        if not isinstance(value, Mapping) or value.get("version") != 1 or set(value) != {"version", "identity", "credential", "tenant"}:
            raise ValueError("verified actor context has unknown fields or an unsupported version")
        return cls(
            identity=InvocationIdentityV1.from_json(value["identity"]),  # type: ignore[arg-type]
            credential=CredentialEvidenceV1.from_json(value["credential"]),
            tenant=TenantReferenceV1.from_json(value["tenant"]),  # type: ignore[arg-type]
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_json())).hexdigest()


__all__ = [
    "AUTHORITY_CANONICALIZATION_VERSION",
    "AUTHORITY_ALIASES_V1",
    "AuthenticationMethod",
    "CredentialEvidenceV1",
    "VerifiedActorContextV1",
    "authority_categories_v1",
    "canonicalize_authority_v1",
    "effective_authority_digest_v1",
]

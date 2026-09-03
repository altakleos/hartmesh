"""Shared validation for secret-free credential audit adapters."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_SAFE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

AUDIT_METHODS = frozenset(
    {
        "session",
        "personal_access_token",
        "internal_service",
        "channel",
        "development_bypass",
    }
)
AUDIT_ACTIONS = frozenset(
    {
        "created",
        "authenticated",
        "authentication_failed",
        "admission",
        "control",
        "expired",
        "revoked",
        "scope_changed",
    }
)
AUDIT_REASON_CODES = frozenset(
    {
        "credential_invalid",
        "credential_expired",
        "credential_revoked",
        "credential_tenant_mismatch",
        "scope_required",
        "credential_evidence_unavailable",
        "authority_digest_mismatch",
        "credential_reference_conflict",
        "audit_record_unavailable",
    }
)


def validate_credential_reference(
    value: str | None,
    *,
    method: str | None = None,
) -> None:
    if value is None:
        return
    if not isinstance(value, str) or _SAFE_REFERENCE.fullmatch(value) is None or value.startswith("dfp_"):
        raise ValueError("credential_ref must be a safe public reference or None")
    if method == "personal_access_token":
        try:
            parsed = uuid.UUID(value)
        except (AttributeError, ValueError) as exc:
            raise ValueError("personal access token credential_ref must be a UUID4") from exc
        if parsed.version != 4 or str(parsed) != value:
            raise ValueError("personal access token credential_ref must be a canonical UUID4")


def validate_credential_audit_fields(
    *,
    method: str,
    action: str,
    credential_ref: str | None,
    actor_digest: str | None,
    authority_digest: str | None,
    route_category: str,
    reason_code: str | None,
) -> None:
    if method not in AUDIT_METHODS:
        raise ValueError("unsupported credential audit method")
    if action not in AUDIT_ACTIONS:
        raise ValueError("unsupported credential audit action")
    validate_credential_reference(credential_ref, method=method)
    for value, field_name in (
        (actor_digest, "actor_digest"),
        (authority_digest, "authority_digest"),
    ):
        if value is not None and (not isinstance(value, str) or _DIGEST.fullmatch(value) is None):
            raise ValueError(f"{field_name} must be a lowercase SHA-256 digest or None")
    if not isinstance(route_category, str) or _SAFE_IDENTIFIER.fullmatch(route_category) is None:
        raise ValueError("route_category must be a bounded lowercase identifier")
    if reason_code is not None and reason_code not in AUDIT_REASON_CODES:
        raise ValueError("unsupported credential audit reason_code")


def normalize_audit_timestamp(value: datetime | None) -> datetime:
    resolved = value or datetime.now(UTC)
    if not isinstance(resolved, datetime) or resolved.tzinfo is None or resolved.utcoffset() is None:
        raise ValueError("audit timestamps must be timezone-aware")
    return resolved.astimezone(UTC)


__all__ = [
    "AUDIT_ACTIONS",
    "AUDIT_METHODS",
    "AUDIT_REASON_CODES",
    "normalize_audit_timestamp",
    "validate_credential_audit_fields",
    "validate_credential_reference",
]

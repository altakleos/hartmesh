"""Personal Access Token (PAT) credentials for programmatic API access.

Tokens are ``dfp_`` + base62(32 CSPRNG bytes), shown exactly once in the
create response and persisted only as a SHA-256 digest. Validation is a
digest-indexed lookup plus a constant-time re-comparison, with a single
generic failure surface so a 401 never reveals which check failed.

v1 scopes are exactly the route-permission strings owned by
``app.gateway.authz`` — a PAT can only narrow its owning user's
permissions, never widen them.
"""

from __future__ import annotations

import functools
import hashlib
import hmac
import re
import secrets
from typing import Any

from deerflow_extension_api import (
    AUTHORITY_ALIASES_V1,
    canonicalize_authority_v1,
)

PAT_TOKEN_PREFIX = "dfp_"
PAT_RANDOM_BYTES = 32
# Best-effort ``last_used_at`` writes are throttled per token so high-volume
# automation does not turn every request into a database write.
PAT_LAST_USED_WRITE_INTERVAL_SECONDS = 300.0

PAT_ALLOWED_SCOPES: frozenset[str] = frozenset(
    {
        "threads:read",
        "threads:write",
        "threads:delete",
        "runs:create",
        "runs:read",
        "runs:cancel",
    }
)

PAT_MAX_NAME_LENGTH = 128

# Default-deny route boundary for PAT callers (#5041 review P1-1): scope
# intersection in AuthMiddleware only constrains routes that consult
# ``request.state.auth.permissions`` (``@require_permission``). Authenticated
# mutation routes without that decorator — memory deletion, agent creation,
# credential switching, channel configuration — would otherwise accept a
# PAT holding a single read scope. A route is reachable by PAT only when it
# is explicitly listed here, and only together with the thread/run lifecycle
# the v1 scopes govern; everything else answers 403 regardless of scopes.
PAT_ROUTE_SCOPE_RULES: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("POST", re.compile(r"^/api/threads$"), "threads:write"),
    ("POST", re.compile(r"^/api/threads/search$"), "threads:read"),
    ("GET", re.compile(r"^/api/threads/[^/]+$"), "threads:read"),
    ("PATCH", re.compile(r"^/api/threads/[^/]+$"), "threads:write"),
    ("DELETE", re.compile(r"^/api/threads/[^/]+$"), "threads:delete"),
    ("GET", re.compile(r"^/api/threads/[^/]+/goal$"), "threads:read"),
    ("PUT", re.compile(r"^/api/threads/[^/]+/goal$"), "threads:write"),
    ("DELETE", re.compile(r"^/api/threads/[^/]+/goal$"), "threads:write"),
    ("GET", re.compile(r"^/api/threads/[^/]+/state$"), "threads:read"),
    ("POST", re.compile(r"^/api/threads/[^/]+/state$"), "threads:write"),
    ("POST", re.compile(r"^/api/threads/[^/]+/compact$"), "threads:write"),
    ("POST", re.compile(r"^/api/threads/[^/]+/history$"), "threads:read"),
    ("POST", re.compile(r"^/api/threads/[^/]+/branches$"), "threads:write"),
    # Runs subtree: enumerated per implemented subroute instead of a
    # ``runs(/.*)?`` wildcard, so a route added under /runs is default-denied
    # until explicitly listed — the same no-dead-methods precision the
    # threads collection rule enforces. The ``{run_id}`` slot necessarily
    # matches any single segment; the POST-only collection endpoints sharing
    # that depth (stream, wait, regenerate, edit-regenerate) are excluded
    # from the GET run-id rule so no unimplemented method is pre-authorized.
    ("GET", re.compile(r"^/api/threads/[^/]+/runs$"), "runs:read"),
    ("POST", re.compile(r"^/api/threads/[^/]+/runs$"), "runs:create"),
    (
        "POST",
        re.compile(r"^/api/threads/[^/]+/runs/(stream|wait|regenerate/prepare|edit-regenerate/prepare)$"),
        "runs:create",
    ),
    (
        "GET",
        re.compile(r"^/api/threads/[^/]+/runs/(?!stream$|wait$|regenerate$|edit-regenerate$)[^/]+$"),
        "runs:read",
    ),
    ("POST", re.compile(r"^/api/threads/[^/]+/runs/[^/]+/cancel$"), "runs:cancel"),
    (
        "GET",
        re.compile(r"^/api/threads/[^/]+/runs/[^/]+/(join|messages|events|retrieval-observations|workspace-changes)$"),
        "runs:read",
    ),
    ("GET", re.compile(r"^/api/threads/[^/]+/runs/[^/]+/artifacts/archive$"), "runs:read"),
    ("POST", re.compile(r"^/api/threads/[^/]+/runs/[^/]+/artifacts/archive$"), "runs:read"),
    ("GET", re.compile(r"^/api/threads/[^/]+/runs/[^/]+/stream$"), "runs:read"),
    ("POST", re.compile(r"^/api/threads/[^/]+/runs/[^/]+/stream$"), "runs:read"),
    ("POST", re.compile(r"^/api/runs/(stream|wait)$"), "runs:create"),
    ("GET", re.compile(r"^/api/runs/[^/]+/(messages|feedback)$"), "runs:read"),
)

PAT_AUTHORITY_ALIASES_V1 = AUTHORITY_ALIASES_V1

_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def is_pat_allowed_route(method: str, path: str) -> bool:
    """Return whether the PAT route policy admits *method* + *path*.

    Trailing slashes are normalized away so the mounted route and its
    redirect-style twin resolve identically.
    """
    return required_pat_scope(method, path) is not None


def required_pat_scope(method: str, path: str) -> str | None:
    """Return the one checked-in scope required by a PAT-enabled route."""

    normalized = path.rstrip("/") or "/"
    normalized_method = method.upper()
    for rule_method, pattern, scope in PAT_ROUTE_SCOPE_RULES:
        if normalized_method == rule_method and pattern.match(normalized):
            return scope
    return None


@functools.cache
def _base62_width(byte_length: int) -> int:
    """Digits sufficient for any *byte_length*-byte value (exact integer math)."""
    width = 1
    limit = 1 << (8 * byte_length)
    while 62**width < limit:
        width += 1
    return width


def _base62(data: bytes) -> str:
    """Fixed-width big-endian base62 of *data*, ``0``-padded on the left.

    ``int.from_bytes`` discards leading zero bytes, so an unpadded encoding
    would be variable-length (and empty for all-zero input) — any draw below
    62**39 would have produced a shorter-than-expected token. The fixed width
    keeps every token body exactly ``_base62_width(len(data))`` characters
    and makes the format test deterministic.
    """
    value = int.from_bytes(data, "big")
    digits: list[str] = []
    while value:
        value, remainder = divmod(value, 62)
        digits.append(_BASE62_ALPHABET[remainder])
    body = "".join(reversed(digits))
    return body.rjust(_base62_width(len(data)), "0")


def generate_pat_token() -> str:
    """Generate a show-once raw token: ``dfp_`` + base62(CSPRNG bytes)."""
    return PAT_TOKEN_PREFIX + _base62(secrets.token_bytes(PAT_RANDOM_BYTES))


def pat_token_digest(token: str) -> str:
    """Return the hex SHA-256 digest persisted for *token*."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def digest_matches(stored_digest: str | None, token: str) -> bool:
    """Constant-time comparison of *token* against a stored digest."""
    if not isinstance(stored_digest, str) or not stored_digest:
        return False
    return hmac.compare_digest(stored_digest, pat_token_digest(token))


def extract_bearer_token(authorization: str | None) -> str | None:
    """Return the Bearer credential from an Authorization header value.

    ``None`` means the request carries no Authorization header at all, so the
    caller should fall through to the session-cookie path. Any other unusable
    value (non-Bearer scheme, empty credential) returns ``""`` so callers
    treat it as an invalid credential rather than an absent one.
    """
    if authorization is None:
        return None
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return ""
    return value.strip()


async def authenticate_pat(
    app: Any,
    authorization: str | None,
    *,
    route_category: str = "other",
) -> tuple[Any, frozenset[str], dict[str, Any]]:
    """Validate the Bearer credential and resolve its owning user.

    Returns ``(user, scopes, record)``. The bounded record carries the stable
    PAT UUID and safe timestamps needed for credential evidence; it never
    contains the bearer value. Every token-verdict failure mode — malformed
    token, unknown/revoked/expired token, PAT store not configured, missing
    owning user — raises the same generic 401 so responses cannot serve as an
    oracle on which check failed. Infrastructure errors (store I/O failures)
    propagate and fail closed; they are not part of the token verdict.
    """
    from fastapi import HTTPException

    pat_repo = getattr(app.state, "pat_repo", None)
    token = extract_bearer_token(authorization)
    if not token or not token.startswith(PAT_TOKEN_PREFIX):
        if pat_repo is not None:
            await pat_repo.record_audit_best_effort(
                method="personal_access_token",
                action="authentication_failed",
                credential_ref=None,
                actor_digest=None,
                authority_digest=None,
                route_category=route_category,
                reason_code="credential_invalid",
            )
        raise HTTPException(status_code=401, detail="Invalid token")
    if pat_repo is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    lookup = await pat_repo.resolve_for_authentication(pat_token_digest(token))
    record = lookup.record
    if lookup.failure_reason is not None or record is None:
        from deerflow_extension_api import effective_authority_digest_v1

        audit_identity = None if record is None else pat_repo.audit_identity_for_record(record)
        credential_ref = None if audit_identity is None else audit_identity.credential_ref
        actor_digest = None if audit_identity is None else audit_identity.actor_digest
        authority_digest = None
        if record is not None:
            try:
                authority_digest = effective_authority_digest_v1(validate_scopes(record.get("scopes")))
            except (TypeError, ValueError):
                pass
        if lookup.failure_reason == "credential_expired":
            await pat_repo.record_audit_best_effort(
                method="personal_access_token",
                action="expired",
                credential_ref=credential_ref,
                actor_digest=actor_digest,
                authority_digest=authority_digest,
                route_category=route_category,
                reason_code="credential_expired",
            )
        await pat_repo.record_audit_best_effort(
            method="personal_access_token",
            action="authentication_failed",
            credential_ref=credential_ref,
            actor_digest=actor_digest,
            authority_digest=authority_digest,
            route_category=route_category,
            reason_code=lookup.failure_reason or "credential_invalid",
        )
        raise HTTPException(status_code=401, detail="Invalid token")
    if not digest_matches(record.get("token_digest"), token):
        await pat_repo.record_audit_best_effort(
            method="personal_access_token",
            action="authentication_failed",
            credential_ref=None,
            actor_digest=None,
            authority_digest=None,
            route_category=route_category,
            reason_code="credential_invalid",
        )
        raise HTTPException(status_code=401, detail="Invalid token")
    try:
        canonical_scopes = validate_scopes(record.get("scopes"))
    except (TypeError, ValueError):
        audit_identity = pat_repo.audit_identity_for_record(record)
        await pat_repo.record_audit_best_effort(
            method="personal_access_token",
            action="authentication_failed",
            credential_ref=audit_identity.credential_ref,
            actor_digest=audit_identity.actor_digest,
            authority_digest=None,
            route_category=route_category,
            reason_code="credential_invalid",
        )
        raise HTTPException(status_code=401, detail="Invalid token") from None
    from app.gateway.deps import get_local_provider

    user = await get_local_provider().get_user(str(record["user_id"]))
    if user is None:
        # The owning user was deleted or became unresolvable; the token is
        # dead even though its row survives (deleting a user revokes their
        # PATs, without needing a FK cascade).
        from deerflow_extension_api import effective_authority_digest_v1

        audit_identity = pat_repo.audit_identity_for_record(record)

        await pat_repo.record_audit_best_effort(
            method="personal_access_token",
            action="authentication_failed",
            credential_ref=audit_identity.credential_ref,
            actor_digest=audit_identity.actor_digest,
            authority_digest=effective_authority_digest_v1(canonical_scopes),
            route_category=route_category,
            reason_code="credential_invalid",
        )
        raise HTTPException(status_code=401, detail="Invalid token")
    await pat_repo.touch_last_used(str(record["id"]))
    return user, frozenset(canonical_scopes), record


def validate_scopes(scopes: list[str]) -> list[str]:
    """Validate a creation-time scope list; returns the deduplicated order."""
    if not isinstance(scopes, list) or any(not isinstance(scope, str) for scope in scopes):
        raise ValueError("PAT scopes must be a list of identifiers")
    try:
        deduplicated = list(
            canonicalize_authority_v1(
                scopes,
                aliases=PAT_AUTHORITY_ALIASES_V1,
                allowed=PAT_ALLOWED_SCOPES,
            )
        )
    except (TypeError, ValueError) as exc:
        unknown = sorted(scope for scope in set(scopes) if scope not in PAT_ALLOWED_SCOPES and scope not in PAT_AUTHORITY_ALIASES_V1)
        if unknown:
            raise ValueError(f"Unknown PAT scopes: {', '.join(unknown)}") from exc
        raise
    if not deduplicated:
        raise ValueError("A PAT must request at least one scope")
    return deduplicated

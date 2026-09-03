"""Gateway adapters for host-sealed, secret-free credential evidence."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from deerflow_extension_api import (
    CredentialEvidenceV1,
    VerifiedActorContextV1,
    authority_categories_v1,
    canonicalize_authority_v1,
    effective_authority_digest_v1,
)

from app.gateway.auth_disabled import (
    AUTH_SOURCE_AUTH_DISABLED,
    AUTH_SOURCE_INTERNAL,
    AUTH_SOURCE_PAT,
    AUTH_SOURCE_SESSION,
)
from app.runtime.invocation import InternalLaunchIntent, InternalSourceKind

CREDENTIAL_EVIDENCE_STATE_ATTR = "credential_evidence"


class CredentialEvidenceError(RuntimeError):
    """Typed failure to project already-authenticated request state."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _validated_evidence(**values: Any) -> CredentialEvidenceV1:
    try:
        return CredentialEvidenceV1(**values)
    except (TypeError, ValueError) as exc:
        raise CredentialEvidenceError("credential_evidence_unavailable") from exc


def credential_route_category(path: str) -> str:
    normalized = path.rstrip("/") or "/"
    if normalized.startswith("/api/v1/auth/pats"):
        return "credential_management"
    if normalized.startswith("/api/runs") or "/runs" in normalized:
        return "runs"
    if normalized.startswith("/api/threads"):
        return "threads"
    if normalized.startswith("/api/runtime"):
        return "runtime"
    return "other"


def _coerce_record_time(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CredentialEvidenceError("credential_evidence_unavailable") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    raise CredentialEvidenceError("credential_evidence_unavailable")


def build_boundary_credential_evidence(
    *,
    auth_source: str,
    permissions: list[str] | tuple[str, ...] | frozenset[str],
    pat_record: dict[str, Any] | None = None,
    session_payload: Any | None = None,
) -> CredentialEvidenceV1:
    """Project only server-resolved auth state into the neutral contract."""

    from app.gateway.auth.pat import PAT_ALLOWED_SCOPES

    if auth_source == AUTH_SOURCE_PAT:
        allowed_authorities = PAT_ALLOWED_SCOPES
    else:
        # Session/internal authority also covers authenticated management
        # surfaces that are intentionally unavailable to PAT v1.
        from app.gateway.authz import _ALL_PERMISSIONS

        allowed_authorities = frozenset(_ALL_PERMISSIONS)

    try:
        canonical = canonicalize_authority_v1(
            permissions,
            allowed=allowed_authorities,
        )
    except (TypeError, ValueError) as exc:
        raise CredentialEvidenceError("credential_evidence_unavailable") from exc
    digest = effective_authority_digest_v1(canonical)
    categories = authority_categories_v1(canonical)
    if auth_source == AUTH_SOURCE_PAT:
        if not isinstance(pat_record, dict):
            raise CredentialEvidenceError("credential_evidence_unavailable")
        return _validated_evidence(
            method="personal_access_token",
            credential_ref=str(pat_record.get("id") or ""),
            effective_authority_digest=digest,
            authority_categories=categories,
            issued_at=_coerce_record_time(pat_record.get("created_at")),
            expires_at=_coerce_record_time(pat_record.get("expires_at")),
        )
    if auth_source == AUTH_SOURCE_SESSION:
        return _validated_evidence(
            method="session",
            credential_ref=None,
            effective_authority_digest=digest,
            authority_categories=categories,
            issued_at=_coerce_record_time(getattr(session_payload, "iat", None)),
            expires_at=_coerce_record_time(getattr(session_payload, "exp", None)),
        )
    if auth_source in {AUTH_SOURCE_INTERNAL, "service"}:
        return _validated_evidence(
            method="internal_service",
            credential_ref=None,
            effective_authority_digest=digest,
            authority_categories=categories,
        )
    if auth_source == AUTH_SOURCE_AUTH_DISABLED:
        return _validated_evidence(
            method="development_bypass",
            credential_ref=None,
            effective_authority_digest=digest,
            authority_categories=categories,
        )
    raise CredentialEvidenceError("credential_evidence_unavailable")


def credential_evidence_for_admission(
    request: Any,
    intent: InternalLaunchIntent,
) -> CredentialEvidenceV1:
    """Return verified request evidence, adapting trusted channel launches."""

    state = getattr(request, "state", None)
    evidence = getattr(state, CREDENTIAL_EVIDENCE_STATE_ATTR, None)
    auth_source = getattr(state, "auth_source", None)
    auth = getattr(state, "auth", None)
    permissions = tuple(getattr(auth, "permissions", ()) or ())
    if isinstance(evidence, CredentialEvidenceV1):
        if permissions:
            try:
                expected = effective_authority_digest_v1(canonicalize_authority_v1(permissions))
            except (TypeError, ValueError) as exc:
                raise CredentialEvidenceError("credential_evidence_unavailable") from exc
            if expected != evidence.effective_authority_digest:
                raise CredentialEvidenceError("authority_digest_mismatch")
            if authority_categories_v1(canonicalize_authority_v1(permissions)) != evidence.authority_categories:
                raise CredentialEvidenceError("authority_digest_mismatch")
        expected_method = {
            AUTH_SOURCE_PAT: "personal_access_token",
            AUTH_SOURCE_SESSION: "session",
            AUTH_SOURCE_INTERNAL: "internal_service",
            AUTH_SOURCE_AUTH_DISABLED: "development_bypass",
            "service": "internal_service",
        }.get(auth_source)
        if expected_method is None or evidence.method != expected_method:
            raise CredentialEvidenceError("credential_evidence_unavailable")
    elif auth_source == AUTH_SOURCE_PAT:
        raise CredentialEvidenceError("credential_evidence_unavailable")
    elif auth_source in {
        AUTH_SOURCE_SESSION,
        AUTH_SOURCE_INTERNAL,
        AUTH_SOURCE_AUTH_DISABLED,
        "service",
    }:
        # Direct scheduler/channel launchers are authenticated before they
        # construct their request-like host object and do not traverse ASGI
        # middleware. The private Gateway seal remains the trust boundary.
        from app.gateway.authz import _ALL_PERMISSIONS

        evidence = build_boundary_credential_evidence(
            auth_source=auth_source,
            permissions=list(permissions or _ALL_PERMISSIONS),
        )
    else:
        raise CredentialEvidenceError("credential_evidence_unavailable")

    if intent.source_kind is InternalSourceKind.native_channel:
        if auth_source not in {AUTH_SOURCE_INTERNAL, None}:
            raise CredentialEvidenceError("credential_evidence_unavailable")
        evidence = replace(evidence, method="channel")
    return evidence


def _tenant_for_request(request: Any) -> Any:
    app_state = getattr(getattr(request, "app", None), "state", None)
    pat_repo = getattr(app_state, "pat_repo", None)
    tenant = getattr(pat_repo, "tenant", None)
    if tenant is None:
        tenant_identity = getattr(app_state, "tenant_identity", None)
        to_reference = getattr(
            tenant_identity,
            "to_persisted_reference",
            None,
        )
        if callable(to_reference):
            tenant = to_reference()
    if tenant is None:
        raise CredentialEvidenceError("credential_evidence_unavailable")
    return tenant


def _audit_repository_for_request(request: Any) -> Any:
    app_state = getattr(getattr(request, "app", None), "state", None)
    pat_repo = getattr(app_state, "pat_repo", None)
    audit_repo = getattr(app_state, "credential_audit_repo", None)
    if audit_repo is None and pat_repo is not None:
        audit_repo = getattr(pat_repo, "audit_repository", None)
    if audit_repo is None:
        raise CredentialEvidenceError("credential_evidence_unavailable")
    return audit_repo


async def verified_actor_context_for_request(
    request: Any,
) -> VerifiedActorContextV1:
    """Compose current server-resolved principal, credential, and tenant."""

    evidence = getattr(
        getattr(request, "state", None),
        CREDENTIAL_EVIDENCE_STATE_ATTR,
        None,
    )
    if not isinstance(evidence, CredentialEvidenceV1):
        raise CredentialEvidenceError("credential_evidence_unavailable")
    tenant = _tenant_for_request(request)

    from app.gateway.services import invocation_principal_from_request

    principal = await invocation_principal_from_request(request)
    if principal.identity is None:
        raise CredentialEvidenceError("credential_evidence_unavailable")
    try:
        return VerifiedActorContextV1(
            identity=principal.identity,
            credential=evidence,
            tenant=tenant,
        )
    except (TypeError, ValueError) as exc:
        raise CredentialEvidenceError("credential_evidence_unavailable") from exc


async def _record_credential_action(
    request: Any,
    *,
    action: str,
    route_category: str,
    reason_code: str | None = None,
) -> VerifiedActorContextV1:
    """Persist one current actor observation and return its exact evidence."""

    audit_repo = _audit_repository_for_request(request)
    actor = await verified_actor_context_for_request(request)
    await audit_repo.record(
        method=actor.credential.method,
        action=action,
        credential_ref=actor.credential.credential_ref,
        actor_digest=actor.digest,
        authority_digest=(actor.credential.effective_authority_digest),
        route_category=route_category,
        reason_code=reason_code,
    )
    return actor


async def record_required_credential_action(
    request: Any,
    *,
    action: str,
    route_category: str,
) -> VerifiedActorContextV1:
    """Persist current actor evidence before a required audited action.

    The returned neutral actor contract is the handoff seam for deep services:
    the authorization check and durable audit refer to the same immutable
    projection, without those services importing Gateway authentication code.
    """

    from deerflow.persistence.credential_audit import (
        CredentialAuditUnavailable,
    )

    try:
        return await _record_credential_action(
            request,
            action=action,
            route_category=route_category,
        )
    except CredentialAuditUnavailable:
        raise
    except Exception as exc:
        raise CredentialAuditUnavailable() from exc


async def record_credential_action_best_effort(
    request: Any,
    *,
    action: str,
    route_category: str,
    reason_code: str | None = None,
) -> None:
    """Record a low-value aggregate without exposing failure details."""

    try:
        await _record_credential_action(
            request,
            action=action,
            route_category=route_category,
            reason_code=reason_code,
        )
    except Exception:
        # This path is explicitly availability-preserving. Never log the
        # exception: third-party drivers may include query parameters or
        # credential-shaped values in exception text.
        return


__all__ = [
    "CREDENTIAL_EVIDENCE_STATE_ATTR",
    "CredentialEvidenceError",
    "build_boundary_credential_evidence",
    "credential_evidence_for_admission",
    "credential_route_category",
    "record_credential_action_best_effort",
    "record_required_credential_action",
    "verified_actor_context_for_request",
]

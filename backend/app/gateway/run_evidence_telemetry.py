"""Bounded request telemetry shared by evidence handlers and authorization."""

from __future__ import annotations

import logging
import re
from typing import Any, Literal

from app.gateway.request_path import get_request_route_path

logger = logging.getLogger(__name__)

RunEvidenceOutcome = Literal[
    "requested",
    "completed",
    "refused",
    "cancelled",
    "failed",
]

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_REF_RE = re.compile(r"^bundle-[a-z0-9]{16,32}$")
_EVIDENCE_ROUTE_RE = re.compile(r"^/api/threads/[^/]+/runs/[^/]+/artifacts/evidence-bundle/?$")


def is_run_evidence_request(request: Any) -> bool:
    """Recognize only the additive GET/POST evidence bundle surface."""

    scope = getattr(request, "scope", None)
    method = scope.get("method") if isinstance(scope, dict) else None
    if method not in {"GET", "POST"}:
        return False
    path = get_request_route_path(request)
    return _EVIDENCE_ROUTE_RE.fullmatch(path) is not None


def actor_digest_for_evidence_request(request: Any) -> str | None:
    value = getattr(
        getattr(request, "state", None),
        "run_evidence_actor_digest",
        None,
    )
    return value if isinstance(value, str) and _DIGEST_RE.fullmatch(value) else None


def set_run_evidence_actor_digest(request: Any, actor_digest: str | None) -> None:
    if actor_digest is not None and (not isinstance(actor_digest, str) or _DIGEST_RE.fullmatch(actor_digest) is None):
        actor_digest = None
    request.state.run_evidence_actor_digest = actor_digest


def record_run_evidence_outcome(
    request: Any,
    outcome: RunEvidenceOutcome,
    *,
    actor_digest: str | None = None,
    bundle_ref: str | None = None,
) -> None:
    """Emit a closed, identifier-free counter and log event."""

    if not is_run_evidence_request(request):
        return
    if outcome not in {
        "requested",
        "completed",
        "refused",
        "cancelled",
        "failed",
    }:
        raise ValueError("run evidence telemetry outcome is invalid")
    if actor_digest is None:
        actor_digest = actor_digest_for_evidence_request(request)
    if actor_digest is not None and _DIGEST_RE.fullmatch(actor_digest) is None:
        actor_digest = None
    if bundle_ref is not None and _BUNDLE_REF_RE.fullmatch(bundle_ref) is None:
        bundle_ref = None
    app_state = getattr(getattr(request, "app", None), "state", None)
    if app_state is None:
        return
    metrics = getattr(app_state, "run_evidence_bundle_metrics", None)
    if not isinstance(metrics, dict):
        metrics = {}
        app_state.run_evidence_bundle_metrics = metrics
    metrics[outcome] = int(metrics.get(outcome, 0)) + 1
    logger.info(
        "run_evidence_bundle outcome=%s actor_digest=%s bundle_ref=%s",
        outcome,
        actor_digest or "unavailable",
        bundle_ref or "unavailable",
    )


def ensure_run_evidence_requested(request: Any) -> str | None:
    """Record the request once, including when authorization later refuses it."""

    if not is_run_evidence_request(request):
        return None
    if getattr(request.state, "run_evidence_requested", False):
        return actor_digest_for_evidence_request(request)
    request.state.run_evidence_requested = True
    actor_digest = actor_digest_for_evidence_request(request)
    record_run_evidence_outcome(
        request,
        "requested",
        actor_digest=actor_digest,
    )
    return actor_digest

"""Authenticated HTTP adapter for governed skill and MCP revisions."""

from __future__ import annotations

from typing import Any, Literal

from deerflow_extension_api import VerifiedActorContextV1
from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, ConfigDict

from app.gateway.authz import require_audited_permission
from deerflow.tool_plane import (
    ScopedStageRevisionRequest,
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    ToolPlaneRevisionService,
    user_scope_reference,
)

read_router = APIRouter(prefix="/api/tool-plane", tags=["tool-plane"])
mutation_router = APIRouter(prefix="/api/tool-plane", tags=["tool-plane"])


class StageToolPlaneRevisionRequest(BaseModel):
    """Authenticated request to stage one scoped immutable candidate."""

    scope_kind: Literal["deployment_base", "user_overlay"]
    candidate: dict[str, Any]
    model_config = ConfigDict(extra="forbid")


class ToolPlaneRevisionActionRequest(BaseModel):
    """Scope discriminator required before a revision action is authorized."""

    scope_kind: Literal["deployment_base", "user_overlay"]
    model_config = ConfigDict(extra="forbid")


def _service(request: Request) -> ToolPlaneRevisionService:
    service = getattr(request.app.state, "tool_plane_revision_service", None)
    if service is None:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "tool_plane_unavailable",
                "message": "Governed tool-plane revisions are not enabled.",
            },
        )
    return service


def _raise_http(error: ToolPlaneRevisionError) -> None:
    status = {
        "revision_not_found": 404,
        "promotion_not_authorized": 403,
        "immutable_deployment": 409,
        "revision_conflict": 409,
        "base_revision_changed": 409,
        "active_overlay_set_changed": 409,
        "validation_failed": 422,
        "validation_stale": 409,
        "secret_value_present": 422,
        "unsafe_archive": 422,
        "tool_plane_bootstrap_required": 409,
        "bootstrap_inventory_changed": 409,
        "overlay_preflight_failed": 422,
        "overlay_preflight_incomplete": 503,
        "projection_failed": 503,
        "projection_digest_mismatch": 503,
        "recovery_required": 503,
        "unmanaged_drift": 503,
        "validator_unavailable": 503,
        "skill_artifact_not_staged": 422,
    }.get(error.code, 400)
    raise HTTPException(
        status_code=status,
        detail={"code": error.code, "safe_details": dict(error.safe_details)},
    ) from error


async def _actor(
    request: Request,
    action: Literal["read", "mutate", "admin"],
) -> VerifiedActorContextV1:
    actor = await require_audited_permission(
        request,
        "tool_plane",
        action,
        route_category="tool_plane",
    )
    if actor is None:  # pragma: no cover - only direct handler unit stubs
        raise HTTPException(status_code=401, detail="Authentication required")
    return actor


def _scope(
    kind: Literal["deployment_base", "user_overlay"],
    actor: VerifiedActorContextV1,
) -> ToolPlaneRevisionScopeV1:
    if kind == "deployment_base":
        return ToolPlaneRevisionScopeV1(kind="deployment_base")
    return ToolPlaneRevisionScopeV1(
        kind="user_overlay",
        user_ref=user_scope_reference(actor),
    )


def _admin_scope(
    kind: Literal["deployment_base", "user_overlay"],
    user_ref: str | None,
) -> ToolPlaneRevisionScopeV1:
    if kind == "deployment_base":
        if user_ref is not None:
            raise ToolPlaneRevisionError("revision_conflict")
        return ToolPlaneRevisionScopeV1(kind="deployment_base")
    if user_ref is None:
        raise ToolPlaneRevisionError("revision_conflict")
    try:
        return ToolPlaneRevisionScopeV1(
            kind="user_overlay",
            user_ref=user_ref,
        )
    except ValueError as exc:
        raise ToolPlaneRevisionError("revision_conflict") from exc


async def _action_actor(
    request: Request,
    scope_kind: Literal["deployment_base", "user_overlay"],
) -> VerifiedActorContextV1:
    return await _actor(
        request,
        "admin" if scope_kind == "deployment_base" else "mutate",
    )


@read_router.get("/status")
async def get_tool_plane_status(
    request: Request,
    scope_kind: Literal["deployment_base", "user_overlay"] = Query(default="user_overlay"),
) -> dict[str, object]:
    """Return governance status for the caller's overlay or deployment base."""

    actor = await _actor(
        request,
        "admin" if scope_kind == "deployment_base" else "read",
    )
    service = _service(request)
    try:
        if scope_kind == "deployment_base":
            status = await service.admin_status(
                ToolPlaneRevisionScopeV1(kind="deployment_base"),
                actor,
            )
        else:
            status = await service.status_for_actor(actor)
        return {
            **status.to_json(),
            "immutable": bool(service.immutable),
            "durable": bool(service.durable),
            "validation_policy_digest": service.validation_policy_digest,
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@read_router.get("/revisions")
async def list_tool_plane_revisions(
    request: Request,
    scope_kind: Literal["deployment_base", "user_overlay"] = Query(default="user_overlay"),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, object]:
    """List bounded revision history visible in the caller-selected scope."""

    actor = await _actor(
        request,
        "admin" if scope_kind == "deployment_base" else "read",
    )
    service = _service(request)
    try:
        if scope_kind == "deployment_base":
            records = await service.admin_list(
                ToolPlaneRevisionScopeV1(kind="deployment_base"),
                actor,
                limit=limit,
            )
        else:
            records = await service.list_for_actor(actor, limit=limit)
        return {
            "version": 1,
            "scope": _scope(scope_kind, actor).to_json(),
            "revisions": [record.to_safe_json(include_manifest=False) for record in records],
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@read_router.get("/admin/status")
async def get_admin_tool_plane_status(
    request: Request,
    scope_kind: Literal["deployment_base", "user_overlay"] = Query(...),
    user_ref: str | None = Query(default=None, min_length=1, max_length=256),
) -> dict[str, object]:
    """Return status for an explicitly administrator-selected scope."""

    actor = await _actor(request, "admin")
    service = _service(request)
    try:
        status = await service.admin_status(
            _admin_scope(scope_kind, user_ref),
            actor,
        )
        return {
            **status.to_json(),
            "immutable": bool(service.immutable),
            "durable": bool(service.durable),
            "validation_policy_digest": service.validation_policy_digest,
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@read_router.get("/admin/revisions")
async def list_admin_tool_plane_revisions(
    request: Request,
    scope_kind: Literal["deployment_base", "user_overlay"] = Query(...),
    user_ref: str | None = Query(default=None, min_length=1, max_length=256),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, object]:
    """List history for an explicitly administrator-selected scope."""

    actor = await _actor(request, "admin")
    service = _service(request)
    try:
        scope = _admin_scope(scope_kind, user_ref)
        records = await service.admin_list(scope, actor, limit=limit)
        return {
            "version": 1,
            "scope": scope.to_json(),
            "revisions": [record.to_safe_json(include_manifest=False) for record in records],
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/revisions", status_code=201)
async def stage_tool_plane_revision(
    request: Request,
    body: StageToolPlaneRevisionRequest,
) -> dict[str, object]:
    """Stage inert canonical material in the caller-authorized scope."""

    actor = await _action_actor(request, body.scope_kind)
    service = _service(request)
    try:
        staged = await service.stage(
            ScopedStageRevisionRequest(
                scope=_scope(body.scope_kind, actor),
                candidate=body.candidate,
            ),
            actor,
        )
        return {
            "version": 1,
            "revision_id": staged.revision_id,
            "revision_digest": staged.revision_digest,
            "content_digest": staged.content_digest,
            "scope": staged.scope.to_json(),
            "state": staged.state,
            "staged_at": staged.staged_at.isoformat(),
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/skill-artifacts", status_code=201)
async def stage_tool_plane_skill_artifact(
    request: Request,
    archive: UploadFile = File(...),
    scope_kind: Literal["deployment_base", "user_overlay"] = Form(...),
) -> dict[str, object]:
    """Safely stage archive bytes without installing or activating them."""

    actor = await _action_actor(request, scope_kind)
    try:
        staged = await _service(request).stage_skill_archive(
            _scope(scope_kind, actor),
            archive.file,
            actor,
        )
        return staged.to_safe_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)
    finally:
        await archive.close()


@mutation_router.post("/bootstrap/stage-current", status_code=201)
async def stage_current_tool_plane_projection(request: Request) -> dict[str, object]:
    """Capture current mutable installation state as inert bootstrap revisions."""

    actor = await _actor(request, "admin")
    try:
        staged = await _service(request).stage_current_projection(actor)
        return {
            "version": 1,
            "revision_id": staged.revision_id,
            "revision_digest": staged.revision_digest,
            "content_digest": staged.content_digest,
            "scope": staged.scope.to_json(),
            "state": staged.state,
            "staged_at": staged.staged_at.isoformat(),
            "inventory_digest": staged.inventory_digest,
            "overlay_revisions": [
                {
                    "revision_id": overlay.revision_id,
                    "revision_digest": overlay.revision_digest,
                    "content_digest": overlay.content_digest,
                    "scope": overlay.scope.to_json(),
                    "state": overlay.state,
                    "staged_at": overlay.staged_at.isoformat(),
                }
                for overlay in staged.overlay_revisions
            ],
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@read_router.get("/revisions/{revision_id}")
async def inspect_tool_plane_revision(
    request: Request,
    revision_id: str,
) -> dict[str, object]:
    """Inspect safe manifest and evidence for a caller-owned revision."""

    actor = await _actor(request, "read")
    try:
        record = await _service(request).inspect_for_actor(revision_id, actor)
        return record.to_safe_json(include_manifest=True)
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@read_router.get("/admin/revisions/{revision_id}")
async def admin_inspect_tool_plane_revision(
    request: Request,
    revision_id: str,
) -> dict[str, object]:
    """Inspect safe material for an administrator-authorized revision."""

    actor = await _actor(request, "admin")
    try:
        record = await _service(request).admin_inspect(revision_id, actor)
        return record.to_safe_json(include_manifest=True)
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@read_router.get("/revisions/{revision_id}/diff")
async def diff_tool_plane_revision(
    request: Request,
    revision_id: str,
    against: str = Query(..., min_length=1, max_length=64),
) -> dict[str, object]:
    """Return a bounded top-level field diff between same-scope revisions."""

    actor = await _actor(request, "read")
    service = _service(request)
    try:
        candidate = await service.inspect_for_actor(revision_id, actor)
        baseline = await service.inspect_for_actor(against, actor)
        if candidate.scope != baseline.scope:
            raise ToolPlaneRevisionError("revision_conflict")
        candidate_manifest = candidate.manifest
        baseline_manifest = baseline.manifest
        keys = sorted(set(candidate_manifest) | set(baseline_manifest))
        changed = [key for key in keys if candidate_manifest.get(key) != baseline_manifest.get(key)][:128]
        return {
            "version": 1,
            "revision_digest": candidate.revision_digest,
            "against_revision_digest": baseline.revision_digest,
            "changed_fields": changed,
            "truncated": len(changed) == 128 and len(keys) > 128,
        }
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


async def _verified_action_record(
    request: Request,
    revision_id: str,
    claimed_scope_kind: Literal["deployment_base", "user_overlay"],
) -> tuple[Any, VerifiedActorContextV1]:
    service = _service(request)
    actor = await _action_actor(request, claimed_scope_kind)
    try:
        record = await service.inspect_for_actor(revision_id, actor)
        if record.scope.kind != claimed_scope_kind:
            raise ToolPlaneRevisionError("revision_conflict")
        # The actual revision scope, not only the request discriminator,
        # decides the dedicated deployment-administrator check.
        if record.scope.kind == "deployment_base":
            admin_actor = await _actor(request, "admin")
            if admin_actor.digest != actor.digest:
                raise ToolPlaneRevisionError("promotion_not_authorized")
        return service, actor
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/revisions/{revision_id}/validate")
async def validate_tool_plane_revision(
    request: Request,
    revision_id: str,
    body: ToolPlaneRevisionActionRequest,
) -> dict[str, object]:
    """Validate one caller-owned revision without activating it."""

    service, actor = await _verified_action_record(
        request,
        revision_id,
        body.scope_kind,
    )
    try:
        return (await service.validate(revision_id, actor)).to_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/revisions/{revision_id}/promote")
async def promote_tool_plane_revision(
    request: Request,
    revision_id: str,
    body: ToolPlaneRevisionActionRequest,
) -> dict[str, object]:
    """Promote one validated caller-owned revision."""

    service, actor = await _verified_action_record(
        request,
        revision_id,
        body.scope_kind,
    )
    try:
        return (await service.promote(revision_id, actor)).to_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/revisions/{revision_id}/rollback")
async def rollback_tool_plane_revision(
    request: Request,
    revision_id: str,
    body: ToolPlaneRevisionActionRequest,
) -> dict[str, object]:
    """Create and promote a caller-attributed rollback revision."""

    service, actor = await _verified_action_record(
        request,
        revision_id,
        body.scope_kind,
    )
    try:
        return (await service.rollback(revision_id, actor)).to_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/admin/revisions/{revision_id}/validate")
async def admin_validate_tool_plane_revision(
    request: Request,
    revision_id: str,
) -> dict[str, object]:
    """Validate an administrator-selected revision."""

    actor = await _actor(request, "admin")
    try:
        return (await _service(request).admin_validate(revision_id, actor)).to_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/admin/revisions/{revision_id}/promote")
async def admin_promote_tool_plane_revision(
    request: Request,
    revision_id: str,
) -> dict[str, object]:
    """Promote an administrator-selected validated revision."""

    actor = await _actor(request, "admin")
    try:
        return (await _service(request).admin_promote(revision_id, actor)).to_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


@mutation_router.post("/admin/revisions/{revision_id}/rollback")
async def admin_rollback_tool_plane_revision(
    request: Request,
    revision_id: str,
) -> dict[str, object]:
    """Create and promote an administrator-attributed rollback revision."""

    actor = await _actor(request, "admin")
    try:
        return (await _service(request).admin_rollback(revision_id, actor)).to_json()
    except ToolPlaneRevisionError as exc:
        _raise_http(exc)


# Aggregate router retained for single-process/local consumers. The shipped
# Gateway mounts these surfaces separately so immutable exact-two deployments
# never publish mutation or bootstrap operations in routing or OpenAPI.
router = APIRouter()
router.include_router(read_router)
router.include_router(mutation_router)

__all__ = ["mutation_router", "read_router", "router"]

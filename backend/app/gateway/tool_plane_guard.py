"""Compatibility guard for legacy direct skill/MCP mutation routes."""

from __future__ import annotations

from fastapi import HTTPException, Request


def reject_direct_tool_plane_mutation(
    request: Request,
    *,
    surface: str,
) -> None:
    """Disable direct writes whenever the governed service is installed."""

    app_state = getattr(getattr(request, "app", None), "state", None)
    service = getattr(app_state, "tool_plane_revision_service", None)
    if service is None:
        return
    immutable = bool(getattr(service, "immutable", False))
    code = "immutable_deployment" if immutable else "governed_revision_required"
    message = "This deployment is immutable; skill and MCP changes must be supplied through its deployment revision." if immutable else "Direct skill and MCP writes are disabled. Stage, validate, and promote a tool-plane revision instead."
    raise HTTPException(
        status_code=409,
        detail={"code": code, "surface": surface, "message": message},
    )


__all__ = ["reject_direct_tool_plane_mutation"]

"""Thread-scoped read API for durable MCP background tasks."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field

from app.gateway.authz import require_permission
from app.gateway.deps import get_current_user, get_mcp_task_repo, get_mcp_task_service
from app.gateway.services import invocation_principal_from_request
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.mcp.tasks import (
    ORDINARY_MCP_TASK_DRIVER,
    McpTaskLineageBinder,
    McpTaskLineageError,
    TaskSubmitRequest,
    configured_credential_selector,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import build_request_projection, canonical_digest
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/threads/{thread_id}/mcp-tasks", tags=["mcp-tasks"])

_MAX_PUBLIC_ERROR_CHARS = 500


class CreateMcpTaskBody(BaseModel):
    """Strict standalone input; provenance-shaped extras are ignored."""

    server_name: str = Field(min_length=1, max_length=128)
    task_name: str = Field(min_length=1, max_length=255)
    arguments: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(min_length=1, max_length=256)
    model_config = ConfigDict(extra="ignore")


def _short_error(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)[:_MAX_PUBLIC_ERROR_CHARS]


def _tracking_degraded(record: dict[str, Any], *, threshold: int) -> bool:
    return int(record.get("consecutive_poll_error_count") or 0) >= threshold


def _list_item(record: dict[str, Any], *, threshold: int) -> dict[str, Any]:
    return {
        "task_id": record["id"],
        "task_name": record["task_name"],
        "status": record["status"],
        "created_at": record["created_at"],
        "updated_at": record["updated_at"],
        "error": _short_error(record.get("error")),
        "tracking_degraded": _tracking_degraded(record, threshold=threshold),
        "cancel_requested": record.get("cancel_requested_at") is not None,
        "lineage": _lineage_summary(record),
    }


def _lineage_summary(record: dict[str, Any]) -> dict[str, Any]:
    status_value = str(record.get("lineage_status") or "legacy_unavailable")
    raw = record.get("lineage")
    if status_value != "verified" or not isinstance(raw, dict):
        return {"status": "legacy_unavailable"}
    return {
        "status": "verified",
        "kind": raw.get("kind"),
        "digest": raw.get("digest"),
        "principal_ref": raw.get("principal_ref"),
        "parent_execution_task_id": raw.get("parent_execution_task_id"),
        "parent_execution_kind": raw.get("parent_execution_kind"),
        "parent_subagent_name": raw.get("parent_subagent_name"),
        "parent_tool_receipt_id": raw.get("parent_tool_receipt_id"),
        "agent_revision_digest": raw.get("agent_revision_digest"),
        "assembly_fingerprint": raw.get("assembly_fingerprint"),
        "subagent_catalog_digest": raw.get("subagent_catalog_digest"),
        "subagent_definition_digest": raw.get("subagent_definition_digest"),
        "extension_generation": raw.get("extension_generation"),
        "extension_manifest_digest": raw.get("extension_manifest_digest"),
        "accepted_origin_digest": raw.get("accepted_origin_digest"),
        "mcp_server_name": raw.get("mcp_server_name"),
        "mcp_tool_name": raw.get("mcp_tool_name"),
        "request_projection_digest": raw.get("request_projection_digest"),
        "credential_selector_ref": raw.get("credential_selector_ref"),
        "credential_selector_version": raw.get("credential_selector_version"),
    }


async def _authorized_links(
    request: Request,
    record: dict[str, Any],
    *,
    user_id: str,
) -> dict[str, str]:
    manager = getattr(request.app.state, "run_manager", None)
    get_run = getattr(manager, "get", None)
    if not callable(get_run):
        return {}
    links: dict[str, str] = {}
    for key, response_key in (
        ("parent_run_id", "parent_run_id"),
        ("notification_run_id", "notification_run_id"),
    ):
        run_id = record.get(key)
        if not isinstance(run_id, str) or not run_id:
            continue
        try:
            run = await get_run(
                run_id,
                user_id=user_id,
                raise_on_store_error=True,
            )
        except Exception:
            run = None
        if run is not None:
            links[response_key] = run_id
    return links


def _detail(
    record: dict[str, Any],
    *,
    threshold: int,
    links: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        **_list_item(record, threshold=threshold),
        "last_polled_at": record.get("last_polled_at"),
        "last_poll_error": _short_error(record.get("last_poll_error")),
        "last_cancel_error": _short_error(record.get("last_cancel_error")),
        "cancel_attempt_count": int(record.get("cancel_attempt_count") or 0),
        "notification_status": record.get("notification_status"),
        "notification_error": _short_error(record.get("notification_error")),
        "notification_attempt_count": int(record.get("notification_attempt_count") or 0),
        "result": record.get("result"),
        "result_preview": record.get("result_preview"),
        "result_truncated": bool(record.get("result_truncated")),
        "result_artifact": record.get("result_artifact"),
        "input_required": record.get("input_required"),
        "lineage": _lineage_summary(record),
        "links": dict(links or {}),
    }


async def _current_user_id(request: Request) -> str:
    user_id = await get_current_user(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user_id


@router.get("")
@require_permission("threads", "read", owner_check=True)
async def list_mcp_tasks(
    thread_id: ThreadId,
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
) -> list[dict[str, Any]]:
    repository = get_mcp_task_repo(request)
    service = get_mcp_task_service(request)
    user_id = await _current_user_id(request)
    records = await repository.list_by_thread(
        thread_id,
        user_id=user_id,
        limit=limit,
        tenant_digest=repository.tenant.digest,
    )
    threshold = service.tracking_degraded_after_errors
    return [_list_item(record, threshold=threshold) for record in records]


@router.post("", status_code=status.HTTP_201_CREATED)
@require_permission("threads", "write", owner_check=True)
async def create_mcp_task(
    thread_id: ThreadId,
    body: CreateMcpTaskBody,
    request: Request,
) -> dict[str, Any]:
    """Submit a standalone task using only authenticated server provenance."""

    service = get_mcp_task_service(request)
    user_id = await _current_user_id(request)
    if not getattr(request.app.state, "mcp_tasks_available", False):
        raise HTTPException(
            status_code=503,
            detail="MCP task worker is not running",
        )
    tenant_identity = getattr(request.app.state, "tenant_identity", None)
    if not isinstance(tenant_identity, TenantIdentityV1):
        raise HTTPException(status_code=503, detail="MCP task tenant is unavailable")
    config = getattr(request.app.state, "mcp_task_extensions_config", None)
    if not isinstance(config, ExtensionsConfig):
        raise HTTPException(status_code=503, detail="MCP task configuration is unavailable")
    server = config.get_enabled_mcp_servers().get(body.server_name)
    if server is None:
        raise HTTPException(status_code=404, detail="MCP task toolset not found")
    toolset = next(
        (item for item in server.task_toolsets if item.name == body.task_name),
        None,
    )
    if toolset is None:
        raise HTTPException(status_code=404, detail="MCP task toolset not found")
    principal = await invocation_principal_from_request(
        request,
        user_id=user_id,
    )
    if principal.identity is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    tenant = tenant_identity.to_persisted_reference()
    projection = build_request_projection(
        toolset.submit_tool,
        body.arguments,
    )
    projection["task_mode"] = {
        "notification_requested": True,
        "task_name": toolset.name,
    }
    manifest = getattr(request.app.state, "capability_manifest", None)
    try:
        lineage = McpTaskLineageBinder().for_standalone_api(
            tenant=tenant,
            principal_identity=principal.identity,
            extension_generation=int(getattr(manifest, "extension_generation")),
            extension_manifest_digest=getattr(manifest, "digest", None),
            artifact_manifest_digest=getattr(
                manifest,
                "artifact_manifest_digest",
                None,
            ),
            extension_configuration_digest=getattr(
                manifest,
                "extension_configuration_digest",
                None,
            ),
            accepted_origin_digest=canonical_digest(
                {
                    "version": 1,
                    "source_kind": "standalone_mcp_task_api",
                    "tenant_digest": tenant.digest,
                    "principal_user_ref": canonical_digest(
                        {
                            "version": 1,
                            "user_id": user_id,
                        }
                    ),
                    "thread_id": thread_id,
                    "idempotency_key": body.idempotency_key,
                }
            ),
            server_name=body.server_name,
            tool_name=toolset.submit_tool,
            safe_request_projection=projection,
            credential_selector=configured_credential_selector(
                body.server_name,
                server,
            ),
        )
        local_task_id = (
            "mcp-task-"
            + canonical_digest(
                {
                    "version": 1,
                    "tenant_digest": tenant.digest,
                    "principal_ref": lineage.principal_ref,
                    "thread_id": thread_id,
                    "idempotency_key": body.idempotency_key,
                }
            )[:48]
        )
        created = await service.submit(
            driver_name=ORDINARY_MCP_TASK_DRIVER,
            request=TaskSubmitRequest(
                user_id=user_id,
                thread_id=thread_id,
                lineage=lineage,
                task_name=toolset.name,
                arguments=dict(body.arguments),
                driver_data={
                    "submit_tool": toolset.submit_tool,
                    "status_tool": toolset.status_tool,
                    "cancel_tool": toolset.cancel_tool,
                },
                local_task_id=local_task_id,
            ),
        )
    except McpTaskLineageError as exc:
        raise HTTPException(status_code=409, detail=exc.code) from exc
    links = await _authorized_links(request, created, user_id=user_id)
    return _detail(
        created,
        threshold=service.tracking_degraded_after_errors,
        links=links,
    )


@router.get("/{task_id}")
@require_permission("threads", "read", owner_check=True)
async def get_mcp_task(
    thread_id: ThreadId,
    task_id: str,
    request: Request,
) -> dict[str, Any]:
    repository = get_mcp_task_repo(request)
    service = get_mcp_task_service(request)
    user_id = await _current_user_id(request)
    record = await repository.get(
        task_id,
        user_id=user_id,
        tenant_digest=repository.tenant.digest,
    )
    if record is None or record["thread_id"] != thread_id:
        raise HTTPException(status_code=404, detail="MCP task not found")
    links = await _authorized_links(request, record, user_id=user_id)
    return _detail(
        record,
        threshold=service.tracking_degraded_after_errors,
        links=links,
    )


@router.post("/{task_id}/cancel")
@require_permission("threads", "write", owner_check=True)
async def cancel_mcp_task(
    thread_id: ThreadId,
    task_id: str,
    request: Request,
) -> dict[str, Any]:
    service = get_mcp_task_service(request)
    user_id = await _current_user_id(request)
    if not getattr(request.app.state, "mcp_tasks_available", False):
        # The service exists whenever SQL persistence is configured, but the
        # background loop that owns the remote cancel call only runs when
        # mcp_tasks.enabled=true. Recording cancel_requested_at without a
        # worker would acknowledge a cancellation nobody will ever perform.
        raise HTTPException(status_code=503, detail="MCP task cancellation worker is not running")
    record = await service.cancel_task(
        task_id=task_id,
        thread_id=thread_id,
        user_id=user_id,
        reason_code="user_api",
    )
    if record is None:
        raise HTTPException(status_code=404, detail="MCP task not found")
    links = await _authorized_links(request, record, user_id=user_id)
    return _detail(
        record,
        threshold=service.tracking_degraded_after_errors,
        links=links,
    )

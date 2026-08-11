"""Versioned HTTP transport for the durable invocation runtime API."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Annotated, Any

from deerflow_runtime_api import (
    CancelInvocationRequest,
    ContextInvocationsQuery,
    ControlDisposition,
    DurableInvocationPort,
    EnsureDisposition,
    FailureCode,
    InvocationControlReceipt,
    InvocationEnsureReceipt,
    InvocationEnsureRequest,
    InvocationObservation,
    InvocationQuery,
    RuntimeCapabilities,
    RuntimeFailure,
)
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from app.gateway.authz import get_auth_context
from app.gateway.deps import get_current_user, require_admin_user
from app.gateway.runtime_http import runtime_error_response
from app.gateway.services import (
    build_invocation_runtime,
    invocation_principal_from_request,
)
from app.runtime.api import InvocationRuntimeAPI, unexpected_adapter_failure
from app.runtime.deployment import DeploymentReportPort
from app.runtime.invocation import InternalSourceKind
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/runtime/v1", tags=["runtime"])
logger = logging.getLogger(__name__)


async def get_runtime_api(request: Request) -> DurableInvocationPort:
    """Bind the transport to the current authenticated Gateway principal."""

    principal = await invocation_principal_from_request(
        request,
        user_id=await get_current_user(request),
    )
    return InvocationRuntimeAPI(
        build_invocation_runtime(request),
        principal=principal,
        source_kind=InternalSourceKind.http,
    )


def _permission_error(request: Request, resource: str, action: str) -> JSONResponse | None:
    auth = get_auth_context(request)
    if auth is None or not auth.is_authenticated:
        return runtime_error_response(401, FailureCode.denied)
    if not auth.has_permission(resource, action):
        return runtime_error_response(403, FailureCode.denied)
    return None


async def _read_record(request: Request, record_type: Any) -> Any | JSONResponse:
    try:
        payload = await request.json()
        return record_type.from_dict(payload)
    except Exception:
        return runtime_error_response(422, FailureCode.invalid_request)


def _runtime_failure_response(failure: RuntimeFailure) -> JSONResponse:
    status_by_code = {
        FailureCode.invalid_request: 422,
        FailureCode.denied: 403,
        FailureCode.indeterminate: 503,
        FailureCode.not_found_or_invisible: 404,
        FailureCode.conflict: 409,
        FailureCode.thread_busy: 409,
        FailureCode.stale: 409,
        FailureCode.cursor_gap: 410,
        FailureCode.cursor_ahead: 422,
    }
    details = {key: str(value) for key, value in failure.detail.items() if key != "version"}
    return runtime_error_response(
        status_by_code[failure.code],
        failure.code,
        details=details or None,
    )


async def _invoke_runtime_operation(
    operation: str,
    invoke: Callable[[], Any],
) -> Any | RuntimeFailure:
    """Translate every portable-port failure through one bounded boundary."""

    try:
        result = invoke()
        if inspect.isawaitable(result):
            result = await result
        return result
    except Exception as exc:
        return unexpected_adapter_failure(operation, exception=exc)


def _paging_values(
    request: Request,
    *,
    allow_source_kind: bool = False,
) -> tuple[str | None, int, str | None] | JSONResponse:
    items = list(request.query_params.multi_items())
    allowed = {"cursor", "limit"}
    if allow_source_kind:
        allowed.add("source_kind")
    if any(key not in allowed for key, _value in items):
        return runtime_error_response(422, FailureCode.invalid_request)
    if any(sum(key == name for key, _value in items) > 1 for name in allowed):
        return runtime_error_response(422, FailureCode.invalid_request)
    cursor = request.query_params.get("cursor")
    if cursor == "":
        return runtime_error_response(422, FailureCode.invalid_request)
    raw_limit = request.query_params.get("limit", "100")
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        return runtime_error_response(422, FailureCode.invalid_request)
    if str(limit) != raw_limit or not 1 <= limit <= 500:
        return runtime_error_response(422, FailureCode.invalid_request)
    return cursor, limit, request.query_params.get("source_kind")


@router.get("/capabilities")
async def runtime_capabilities(
    request: Request,
    runtime: Annotated[DurableInvocationPort, Depends(get_runtime_api)],
) -> JSONResponse:
    """Return portable runtime capabilities to an administrator."""

    try:
        await require_admin_user(
            request,
            detail="Runtime capabilities require administrator access",
        )
    except HTTPException as exc:
        return runtime_error_response(exc.status_code, FailureCode.denied)
    capabilities = await _invoke_runtime_operation("capabilities", runtime.capabilities)
    if isinstance(capabilities, RuntimeFailure):
        return _runtime_failure_response(capabilities)
    if not isinstance(capabilities, RuntimeCapabilities):
        return _runtime_failure_response(unexpected_adapter_failure("capabilities"))
    return JSONResponse(content=capabilities.to_dict())


@router.get("/deployment")
async def runtime_deployment_report(request: Request) -> JSONResponse:
    """Return deployment-only capability and durability facts to admins."""

    try:
        await require_admin_user(
            request,
            detail="Runtime deployment reports require administrator access",
        )
    except HTTPException as exc:
        return runtime_error_response(exc.status_code, FailureCode.denied)
    reporter = getattr(request.app.state, "deployment_reporter", None)
    if not isinstance(reporter, DeploymentReportPort):
        return runtime_error_response(503, FailureCode.indeterminate)
    try:
        report = await reporter.deployment_report()
    except Exception as exc:
        failure = unexpected_adapter_failure(
            "deployment_report",
            exception=exc,
        )
        return _runtime_failure_response(failure)
    return JSONResponse(content=report.to_dict())


@router.post("/invocations/ensure")
async def ensure_invocation(
    request: Request,
    runtime: Annotated[DurableInvocationPort, Depends(get_runtime_api)],
) -> JSONResponse:
    """Ensure one invocation through the portable durable runtime contract."""

    if error := _permission_error(request, "runs", "create"):
        return error
    parsed = await _read_record(request, InvocationEnsureRequest)
    if isinstance(parsed, JSONResponse):
        return parsed
    result = await _invoke_runtime_operation("ensure", lambda: runtime.ensure(parsed))
    if isinstance(result, RuntimeFailure):
        return _runtime_failure_response(result)
    status_by_disposition = {
        EnsureDisposition.created: 201,
        EnsureDisposition.known: 200,
        EnsureDisposition.conflict: 409,
        EnsureDisposition.denied: 403,
        EnsureDisposition.indeterminate: 503,
        EnsureDisposition.thread_busy: 409,
    }
    if not isinstance(result, InvocationEnsureReceipt):
        return _runtime_failure_response(unexpected_adapter_failure("ensure"))
    if result.disposition in {EnsureDisposition.created, EnsureDisposition.known}:
        return JSONResponse(
            status_code=status_by_disposition[result.disposition],
            content=result.to_dict(),
        )
    return runtime_error_response(
        status_by_disposition[result.disposition],
        FailureCode(result.disposition.value),
    )


@router.get("/invocations/{run_id}")
async def observe_invocation(
    run_id: str,
    request: Request,
    runtime: Annotated[DurableInvocationPort, Depends(get_runtime_api)],
) -> JSONResponse:
    """Observe one authorized invocation and its lifecycle page."""

    if error := _permission_error(request, "runs", "read"):
        return error
    paging = _paging_values(request)
    if isinstance(paging, JSONResponse):
        return paging
    cursor, limit, _source_kind = paging
    try:
        query = InvocationQuery(run_id=run_id, cursor=cursor, limit=limit)
    except (TypeError, ValueError):
        return runtime_error_response(422, FailureCode.invalid_request)
    result = await _invoke_runtime_operation("observe", lambda: runtime.observe(query))
    if isinstance(result, RuntimeFailure):
        return _runtime_failure_response(result)
    if not isinstance(result, InvocationObservation):
        return _runtime_failure_response(unexpected_adapter_failure("observe"))
    return JSONResponse(content=result.to_dict())


@router.get("/contexts/{thread_id}/invocations")
async def observe_context_invocations(
    thread_id: ThreadId,
    request: Request,
    runtime: Annotated[DurableInvocationPort, Depends(get_runtime_api)],
) -> JSONResponse:
    """Observe an authorized bounded invocation page for one context."""

    if error := _permission_error(request, "runs", "read"):
        return error
    paging = _paging_values(request, allow_source_kind=True)
    if isinstance(paging, JSONResponse):
        return paging
    cursor, limit, source_kind = paging
    try:
        query = ContextInvocationsQuery(
            thread_id=thread_id,
            cursor=cursor,
            limit=limit,
            source_kind=source_kind,
        )
    except (TypeError, ValueError):
        return runtime_error_response(422, FailureCode.invalid_request)
    result = await _invoke_runtime_operation("observe", lambda: runtime.observe(query))
    if isinstance(result, RuntimeFailure):
        return _runtime_failure_response(result)
    if not isinstance(result, InvocationObservation):
        return _runtime_failure_response(unexpected_adapter_failure("observe"))
    return JSONResponse(content=result.to_dict())


@router.post("/invocations/{run_id}/control")
async def control_invocation(
    run_id: str,
    request: Request,
    runtime: Annotated[DurableInvocationPort, Depends(get_runtime_api)],
) -> JSONResponse:
    """Apply one authorized version-fenced invocation control request."""

    if error := _permission_error(request, "runs", "cancel"):
        return error
    parsed = await _read_record(request, CancelInvocationRequest)
    if isinstance(parsed, JSONResponse):
        return parsed
    if parsed.run_id != run_id:
        return runtime_error_response(422, FailureCode.invalid_request)
    result = await _invoke_runtime_operation("control", lambda: runtime.control(parsed))
    if isinstance(result, RuntimeFailure):
        return _runtime_failure_response(result)
    if not isinstance(result, InvocationControlReceipt):
        return _runtime_failure_response(unexpected_adapter_failure("control"))
    status_by_disposition = {
        ControlDisposition.requested: 202,
        ControlDisposition.already_requested: 200,
        ControlDisposition.already_terminal: 200,
        ControlDisposition.stale: 409,
        ControlDisposition.not_found_or_invisible: 404,
        ControlDisposition.denied: 403,
        ControlDisposition.indeterminate: 503,
    }
    if result.disposition in {
        ControlDisposition.requested,
        ControlDisposition.already_requested,
        ControlDisposition.already_terminal,
    }:
        return JSONResponse(
            status_code=status_by_disposition[result.disposition],
            content=result.to_dict(),
        )
    return runtime_error_response(
        status_by_disposition[result.disposition],
        FailureCode(result.disposition.value),
    )


__all__ = ["get_runtime_api", "router"]

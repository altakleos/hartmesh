"""Gateway router for IM channel management."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.channels.inbound_receipt_operations import (
    MAX_RECEIPT_STATE_COUNT,
    InboundDeadLetterRequeueDisposition,
    InboundDeadLetterRequeueRequest,
    InboundReceiptOperations,
    inbound_receipt_operator_ref,
    validate_inbound_receipt_id,
)
from app.gateway.deps import require_admin_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/channels", tags=["channels"])

_ADMIN_REQUIRED_DETAIL = "Admin privileges required to manage channel runtime workers."
_RECEIPT_ADMIN_REQUIRED_DETAIL = "Admin privileges required to inspect inbound receipts."


class _ReceiptDeadLetterNotFound(Exception):
    """Host-authored exact lookup miss."""


class _ReceiptRequeueConflict(Exception):
    """Host-authored compare-and-set miss."""


class ChannelStatusResponse(BaseModel):
    service_running: bool
    channels: dict[str, dict]


class ChannelRestartResponse(BaseModel):
    success: bool
    message: str


class InboundReceiptRequeueBody(BaseModel):
    """Strict optimistic fence for one exact dead-letter receipt."""

    model_config = ConfigDict(extra="forbid")

    expected_fencing_token: int = Field(ge=0)
    expected_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_provider_event_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


def _receipt_operations() -> InboundReceiptOperations:
    from app.channels.service import get_channel_service

    service = get_channel_service()
    operations = getattr(service, "inbound_receipt_operations", None)
    if not isinstance(operations, InboundReceiptOperations):
        raise RuntimeError("inbound receipt operations are unavailable")
    return operations


async def _run_receipt_operation[ReceiptResult](
    *,
    operation: str,
    actor_ref: str,
    call: Callable[[], Awaitable[ReceiptResult]],
) -> ReceiptResult:
    """Translate unexpected operator failures without exposing their messages."""

    try:
        return await call()
    except _ReceiptDeadLetterNotFound:
        raise HTTPException(status_code=404, detail="Inbound dead letter not found") from None
    except _ReceiptRequeueConflict:
        raise HTTPException(status_code=409, detail="Inbound receipt is not eligible for requeue") from None
    except Exception as exc:
        correlation_id = str(uuid.uuid4())
        logger.warning(
            "inbound receipt operation unavailable code=inbound_receipt_operations_unavailable operation=%s actor_ref=%s correlation_id=%s exception_class=%s",
            operation,
            actor_ref,
            correlation_id,
            exc.__class__.__name__,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": "inbound_receipt_operations_unavailable",
                "correlation_id": correlation_id,
            },
        ) from None


@router.get("/inbound-receipts/summary")
async def inbound_receipt_summary(
    request: Request,
    per_state_cap: int = Query(default=MAX_RECEIPT_STATE_COUNT, ge=1, le=MAX_RECEIPT_STATE_COUNT),
) -> dict:
    """Return bounded per-state receipt health without enumerating rows."""

    user = await require_admin_user(request, detail=_RECEIPT_ADMIN_REQUIRED_DETAIL)
    actor_ref = inbound_receipt_operator_ref(user.id)

    async def summarize():
        result = await _receipt_operations().summary(per_state_cap=per_state_cap)
        return result.to_dict()

    return await _run_receipt_operation(
        operation="summary",
        actor_ref=actor_ref,
        call=summarize,
    )


@router.get("/inbound-receipts/{receipt_id}")
async def inspect_inbound_dead_letter(receipt_id: str, request: Request) -> dict:
    """Inspect one exact unresolved dead letter through a safe projection."""

    user = await require_admin_user(request, detail=_RECEIPT_ADMIN_REQUIRED_DETAIL)
    actor_ref = inbound_receipt_operator_ref(user.id)
    try:
        validate_inbound_receipt_id(receipt_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid inbound receipt identity") from None

    async def inspect():
        result = await _receipt_operations().inspect_dead_letter(receipt_id)
        if result is None:
            raise _ReceiptDeadLetterNotFound
        return result.to_dict()

    return await _run_receipt_operation(
        operation="inspect",
        actor_ref=actor_ref,
        call=inspect,
    )


@router.post("/inbound-receipts/{receipt_id}/requeue")
async def requeue_inbound_dead_letter(
    receipt_id: str,
    body: InboundReceiptRequeueBody,
    request: Request,
) -> dict:
    """CAS-requeue one exact unresolved dead letter and wake processing."""

    user = await require_admin_user(request, detail=_RECEIPT_ADMIN_REQUIRED_DETAIL)
    actor_ref = inbound_receipt_operator_ref(user.id)
    try:
        requested = InboundDeadLetterRequeueRequest(
            receipt_id=receipt_id,
            expected_fencing_token=body.expected_fencing_token,
            expected_payload_digest=body.expected_payload_digest,
            expected_provider_event_digest=body.expected_provider_event_digest,
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid inbound receipt requeue request") from None

    async def requeue():
        result = await _receipt_operations().requeue_dead_letter(
            requested,
            actor_ref=actor_ref,
        )
        if result.disposition is InboundDeadLetterRequeueDisposition.not_requeued:
            raise _ReceiptRequeueConflict
        return result.to_dict()

    return await _run_receipt_operation(
        operation="requeue",
        actor_ref=actor_ref,
        call=requeue,
    )


@router.get("/", response_model=ChannelStatusResponse)
async def get_channels_status() -> ChannelStatusResponse:
    """Get the status of all IM channels."""
    from app.channels.service import get_channel_service

    service = get_channel_service()
    if service is None:
        return ChannelStatusResponse(service_running=False, channels={})
    status = service.get_status()
    return ChannelStatusResponse(**status)


@router.post("/{name}/restart", response_model=ChannelRestartResponse)
async def restart_channel(name: str, request: Request) -> ChannelRestartResponse:
    """Restart a specific IM channel."""
    await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)

    from app.channels.service import get_channel_service

    service = get_channel_service()
    if service is None:
        raise HTTPException(status_code=503, detail="Channel service is not running")

    success = await service.restart_channel(name)
    if success:
        logger.info("Channel %s restarted successfully", name)
        return ChannelRestartResponse(success=True, message=f"Channel {name} restarted successfully")
    else:
        logger.warning("Failed to restart channel %s", name)
        return ChannelRestartResponse(success=False, message=f"Failed to restart channel {name}")

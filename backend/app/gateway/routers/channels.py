"""Gateway router for IM channel management."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.channels.inbound_receipt_operations import (
    MAX_RECEIPT_STATE_COUNT,
    InboundDeadLetterDiscardDisposition,
    InboundDeadLetterDiscardRequest,
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
_RECEIPT_ADMIN_REQUIRED_DETAIL = "Admin privileges required to manage inbound receipts."


class _ReceiptDeadLetterNotFound(Exception):
    """Host-authored exact lookup miss."""


class _ReceiptRequeueConflict(Exception):
    """Host-authored compare-and-set miss."""


class _ReceiptDiscardConflict(Exception):
    """Host-authored logical-discard compare-and-set miss."""


class ChannelStatusResponse(BaseModel):
    service_running: bool
    channels: dict[str, dict]


class ChannelRestartResponse(BaseModel):
    success: bool
    message: str


class InboundReceiptRequeueBody(BaseModel):
    """Strict optimistic fence for one exact dead-letter receipt."""

    model_config = ConfigDict(extra="forbid", strict=True)

    expected_fencing_token: int = Field(ge=0, le=2_147_483_647)
    expected_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_provider_event_digest: str | None = Field(
        pattern=r"^[0-9a-f]{64}$",
    )


class InboundReceiptDiscardBody(InboundReceiptRequeueBody):
    """Strict optimistic fence for one logical dead-letter discard."""


class InboundReceiptStateSummaryResponse(BaseModel):
    """Strict wire projection of one bounded receipt-state aggregate."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    state: Literal["received", "claimed", "admitted", "deferred", "completed", "dead_letter"]
    count: int = Field(ge=0, le=MAX_RECEIPT_STATE_COUNT)
    capped: bool
    oldest_due_age_seconds: int | None = Field(default=None, ge=0, le=2_147_483_647)


class InboundReceiptOperationsSummaryResponse(BaseModel):
    """Strict bounded wire response for receipt operational health."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    generated_at: datetime
    per_state_cap: int = Field(ge=1, le=MAX_RECEIPT_STATE_COUNT)
    states: tuple[InboundReceiptStateSummaryResponse, ...] = Field(
        min_length=1,
        max_length=6,
    )


class InboundDeadLetterInspectionResponse(BaseModel):
    """Strict safe wire projection for one exact unresolved dead letter."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    receipt_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    state: Literal["dead_letter"]
    provider: str = Field(min_length=1, max_length=64)
    binding_kind: str = Field(min_length=1, max_length=32)
    thread_id: str = Field(min_length=1, max_length=64)
    payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_event_digest: str | None = Field(pattern=r"^[0-9a-f]{64}$")
    fencing_token: int = Field(ge=0, le=2_147_483_647)
    attempt_count: int = Field(ge=0, le=2_147_483_647)
    failure_count: int = Field(ge=0, le=2_147_483_647)
    outcome_code: str | None = Field(default=None, max_length=64)
    received_at: datetime
    updated_at: datetime
    completed_at: datetime | None


class InboundDeadLetterRequeueResponse(BaseModel):
    """Strict bounded result of one exact dead-letter requeue."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    receipt_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    disposition: Literal["requeued"]
    correlation_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    fencing_token: int = Field(ge=0, le=2_147_483_647)
    wakeup_published: bool


class InboundDeadLetterDiscardResponse(BaseModel):
    """Strict bounded result of one exact logical dead-letter discard."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    receipt_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    disposition: Literal["discarded"]
    correlation_id: str = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    fencing_token: int = Field(ge=0, le=2_147_483_647)


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
    except _ReceiptDiscardConflict:
        raise HTTPException(status_code=409, detail="Inbound receipt is not eligible for discard") from None
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


@router.get(
    "/inbound-receipts/summary",
    response_model=InboundReceiptOperationsSummaryResponse,
)
async def inbound_receipt_summary(
    request: Request,
    per_state_cap: int = Query(default=MAX_RECEIPT_STATE_COUNT, ge=1, le=MAX_RECEIPT_STATE_COUNT),
) -> InboundReceiptOperationsSummaryResponse:
    """Return bounded per-state receipt health without enumerating rows."""

    user = await require_admin_user(request, detail=_RECEIPT_ADMIN_REQUIRED_DETAIL)
    actor_ref = inbound_receipt_operator_ref(user.id)

    async def summarize() -> InboundReceiptOperationsSummaryResponse:
        result = await _receipt_operations().summary(per_state_cap=per_state_cap)
        return InboundReceiptOperationsSummaryResponse(
            generated_at=result.generated_at,
            per_state_cap=result.per_state_cap,
            states=tuple(
                InboundReceiptStateSummaryResponse(
                    state=item.state,
                    count=item.count,
                    capped=item.capped,
                    oldest_due_age_seconds=item.oldest_due_age_seconds,
                )
                for item in result.states
            ),
        )

    return await _run_receipt_operation(
        operation="summary",
        actor_ref=actor_ref,
        call=summarize,
    )


@router.get(
    "/inbound-receipts/{receipt_id}",
    response_model=InboundDeadLetterInspectionResponse,
)
async def inspect_inbound_dead_letter(
    receipt_id: str,
    request: Request,
) -> InboundDeadLetterInspectionResponse:
    """Inspect one exact unresolved dead letter through a safe projection."""

    user = await require_admin_user(request, detail=_RECEIPT_ADMIN_REQUIRED_DETAIL)
    actor_ref = inbound_receipt_operator_ref(user.id)
    try:
        validate_inbound_receipt_id(receipt_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid inbound receipt identity") from None

    async def inspect() -> InboundDeadLetterInspectionResponse:
        result = await _receipt_operations().inspect_dead_letter(receipt_id)
        if result is None:
            raise _ReceiptDeadLetterNotFound
        return InboundDeadLetterInspectionResponse(
            receipt_id=result.receipt_id,
            state=result.state,
            provider=result.provider,
            binding_kind=result.binding_kind,
            thread_id=result.thread_id,
            payload_digest=result.payload_digest,
            provider_event_digest=result.provider_event_digest,
            fencing_token=result.fencing_token,
            attempt_count=result.attempt_count,
            failure_count=result.failure_count,
            outcome_code=result.outcome_code,
            received_at=result.received_at,
            updated_at=result.updated_at,
            completed_at=result.completed_at,
        )

    return await _run_receipt_operation(
        operation="inspect",
        actor_ref=actor_ref,
        call=inspect,
    )


@router.post(
    "/inbound-receipts/{receipt_id}/requeue",
    response_model=InboundDeadLetterRequeueResponse,
)
async def requeue_inbound_dead_letter(
    receipt_id: str,
    body: InboundReceiptRequeueBody,
    request: Request,
) -> InboundDeadLetterRequeueResponse:
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

    async def requeue() -> InboundDeadLetterRequeueResponse:
        result = await _receipt_operations().requeue_dead_letter(
            requested,
            actor_ref=actor_ref,
        )
        if result.disposition is InboundDeadLetterRequeueDisposition.not_requeued:
            raise _ReceiptRequeueConflict
        if result.fencing_token is None:
            raise RuntimeError("successful requeue omitted its fencing token")
        return InboundDeadLetterRequeueResponse(
            receipt_id=result.receipt_id,
            disposition=result.disposition.value,
            correlation_id=result.correlation_id,
            fencing_token=result.fencing_token,
            wakeup_published=result.wakeup_published,
        )

    return await _run_receipt_operation(
        operation="requeue",
        actor_ref=actor_ref,
        call=requeue,
    )


@router.post(
    "/inbound-receipts/{receipt_id}/discard",
    response_model=InboundDeadLetterDiscardResponse,
)
async def discard_inbound_dead_letter(
    receipt_id: str,
    body: InboundReceiptDiscardBody,
    request: Request,
) -> InboundDeadLetterDiscardResponse:
    """CAS-discard one exact unresolved dead letter without waking processing."""

    user = await require_admin_user(request, detail=_RECEIPT_ADMIN_REQUIRED_DETAIL)
    actor_ref = inbound_receipt_operator_ref(user.id)
    try:
        requested = InboundDeadLetterDiscardRequest(
            receipt_id=receipt_id,
            expected_fencing_token=body.expected_fencing_token,
            expected_payload_digest=body.expected_payload_digest,
            expected_provider_event_digest=body.expected_provider_event_digest,
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid inbound receipt discard request") from None

    async def discard() -> InboundDeadLetterDiscardResponse:
        result = await _receipt_operations().discard_dead_letter(
            requested,
            actor_ref=actor_ref,
        )
        if result.disposition is InboundDeadLetterDiscardDisposition.not_discarded:
            raise _ReceiptDiscardConflict
        if result.fencing_token is None:
            raise RuntimeError("successful discard omitted its fencing token")
        return InboundDeadLetterDiscardResponse(
            receipt_id=result.receipt_id,
            disposition=result.disposition.value,
            correlation_id=result.correlation_id,
            fencing_token=result.fencing_token,
        )

    return await _run_receipt_operation(
        operation="discard",
        actor_ref=actor_ref,
        call=discard,
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

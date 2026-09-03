"""Authenticated in-process facade for the durable invocation runtime."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from deerflow_extension_api import EffectiveSubjectV1, InvocationIdentityV1
from deerflow_runtime_api import (
    CancelInvocationRequest,
    ContextInvocationsQuery,
    ControlDisposition,
    DurableInvocationPort,
    EnsureDisposition,
    FailureCode,
    GraphInputV1,
    InvocationControlReceipt,
    InvocationCorrelationReferenceV1,
    InvocationEnsureReceipt,
    InvocationEnsureRequest,
    InvocationObservation,
    InvocationQuery,
    InvocationSummaryV1,
    RuntimeCapabilities,
    RuntimeFailure,
)

from app.runtime.invocation import (
    InternalCancelReceipt,
    InternalCancelRequest,
    InternalContextLifecycleQuery,
    InternalInvocationLifecycleQuery,
    InternalLaunchIntent,
    InternalLaunchReceipt,
    InternalLifecycleObservation,
    InternalSourceKind,
    InvocationAuthorizationOutcome,
    InvocationPrincipal,
    InvocationRuntime,
    NotFoundOrInvisible,
)
from app.runtime.service_identity import validate_persisted_service_id
from deerflow.runtime.runs.lifecycle_query import (
    CursorAhead,
    CursorGap,
    InvalidLifecycleCursor,
    encode_lifecycle_cursor,
)
from deerflow.runtime.runs.manager import ConflictError, IdempotencyConflictError

logger = logging.getLogger(__name__)


def _status_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _failure(code: FailureCode, **detail: Any) -> RuntimeFailure:
    return RuntimeFailure(code=code, detail={"version": 1, **detail})


def unexpected_adapter_failure(
    operation: str,
    *,
    exception: BaseException | None = None,
    exception_class: str | None = None,
    exc_info: bool | None = None,
) -> RuntimeFailure:
    """Correlate an internal diagnostic without exposing exception text."""

    correlation_id = uuid4().hex
    if exception_class is None:
        exception_class = exception.__class__.__name__ if exception is not None else "RuntimeProtocolError"
    logger.error(
        "durable invocation adapter failure code=runtime_adapter_indeterminate operation=%s exception_class=%s correlation_id=%s",
        operation,
        exception_class,
        correlation_id,
        extra={
            "correlation_id": correlation_id,
            "runtime_operation": operation,
            "exception_class": exception_class,
            "diagnostic_code": "runtime_adapter_indeterminate",
        },
    )
    return _failure(FailureCode.indeterminate, correlation_id=correlation_id)


class InvocationRuntimeAPI(DurableInvocationPort):
    """Transport-neutral adapter bound to one host-authenticated principal."""

    def __init__(
        self,
        runtime: InvocationRuntime,
        *,
        principal: InvocationPrincipal,
        source_kind: InternalSourceKind,
        trusted_service_id: str | None = None,
    ) -> None:
        if source_kind is InternalSourceKind.service and not trusted_service_id:
            raise ValueError("service runtime adapters require an authenticated service id")
        if source_kind is not InternalSourceKind.service and trusted_service_id is not None:
            raise ValueError("trusted_service_id is valid only for service runtime adapters")
        self._runtime = runtime
        self._authenticated_principal = principal
        self._source_kind = source_kind
        self._trusted_service_id = trusted_service_id

    def _principal(self) -> InvocationPrincipal:
        return self._authenticated_principal

    async def ensure(
        self,
        request: InvocationEnsureRequest,
    ) -> InvocationEnsureReceipt | RuntimeFailure:
        options = request.options
        context: dict[str, Any] = {}
        if options.model_name is not None:
            context["model_name"] = options.model_name
        if options.thinking_enabled is not None:
            context["thinking_enabled"] = options.thinking_enabled
        is_graph_input = isinstance(request.input, GraphInputV1)
        try:
            result = await self._runtime.launch(
                InternalLaunchIntent(
                    thread_id=request.thread_id,
                    assistant_id=request.agent_hint,
                    input=request.input.value if is_graph_input else None,
                    command=None if is_graph_input else {"resume": request.input.value},
                    context=context,
                    checkpoint_id=options.checkpoint_id,
                    interrupt_before=(list(options.interrupt_before) if isinstance(options.interrupt_before, tuple) else options.interrupt_before),
                    interrupt_after=(list(options.interrupt_after) if isinstance(options.interrupt_after, tuple) else options.interrupt_after),
                    multitask_strategy=options.multitask_strategy,
                    source_kind=self._source_kind,
                    trusted_service_id=self._trusted_service_id,
                    external_key=request.external_key,
                )
            )
        except IdempotencyConflictError:
            return InvocationEnsureReceipt(disposition=EnsureDisposition.conflict)
        except ConflictError:
            return InvocationEnsureReceipt(disposition=EnsureDisposition.thread_busy)
        except (TypeError, ValueError):
            return _failure(FailureCode.invalid_request)
        except Exception as exc:
            if getattr(exc, "status_code", None) == 422:
                return _failure(FailureCode.invalid_request)
            return unexpected_adapter_failure("ensure")
        if isinstance(result, InternalLaunchReceipt):
            record = result.record
            return InvocationEnsureReceipt(
                disposition=(EnsureDisposition.created if result.created else EnsureDisposition.known),
                run_id=record.run_id,
                thread_id=record.thread_id,
                status=_status_value(record.status),
                state_version=record.state_version,
            )
        if result is InvocationAuthorizationOutcome.denied:
            return InvocationEnsureReceipt(disposition=EnsureDisposition.denied)
        if result is InvocationAuthorizationOutcome.indeterminate:
            return InvocationEnsureReceipt(disposition=EnsureDisposition.indeterminate)
        if result is NotFoundOrInvisible.not_found_or_invisible:
            return InvocationEnsureReceipt(disposition=EnsureDisposition.conflict)
        return unexpected_adapter_failure("ensure", exc_info=False)

    async def observe(
        self,
        request: InvocationQuery | ContextInvocationsQuery,
    ) -> InvocationObservation | RuntimeFailure:
        try:
            if isinstance(request, InvocationQuery):
                result = await self._runtime.observe_invocation_lifecycle(
                    InternalInvocationLifecycleQuery(
                        run_id=request.run_id,
                        principal=self._principal(),
                        cursor=request.cursor,
                        limit=request.limit,
                        include_snapshot=request.include_snapshot,
                        include_tool_receipts=request.include_tool_receipts,
                        tool_receipt_cursor=request.tool_receipt_cursor,
                        tool_receipt_limit=request.tool_receipt_limit,
                        include_mcp_tasks=request.include_mcp_tasks,
                        mcp_task_cursor=request.mcp_task_cursor,
                        mcp_task_limit=request.mcp_task_limit,
                        include_subagent_batches=(request.include_subagent_batches),
                        subagent_batch_cursor=request.subagent_batch_cursor,
                        subagent_batch_limit=request.subagent_batch_limit,
                    )
                )
                thread_id = None
            elif isinstance(request, ContextInvocationsQuery):
                result = await self._runtime.observe_context_lifecycle(
                    InternalContextLifecycleQuery(
                        thread_id=request.thread_id,
                        principal=self._principal(),
                        cursor=request.cursor,
                        limit=request.limit,
                        include_snapshot=request.include_snapshot,
                        source_kind=request.source_kind,
                    )
                )
                thread_id = request.thread_id
            else:
                return _failure(FailureCode.invalid_request)
        except CursorGap as exc:
            return _failure(
                FailureCode.cursor_gap,
                minimum_available_cursor=exc.minimum_available_cursor,
            )
        except CursorAhead as exc:
            return _failure(
                FailureCode.cursor_ahead,
                read_fence_cursor=exc.read_fence_cursor,
            )
        except (InvalidLifecycleCursor, ValueError, TypeError):
            return _failure(FailureCode.invalid_request)
        except Exception:
            return unexpected_adapter_failure("observe")

        if result is NotFoundOrInvisible.not_found_or_invisible:
            return _failure(FailureCode.not_found_or_invisible)
        if result is InvocationAuthorizationOutcome.denied:
            return _failure(FailureCode.denied)
        if result is InvocationAuthorizationOutcome.indeterminate:
            return _failure(FailureCode.indeterminate)
        if not isinstance(result, InternalLifecycleObservation):
            return unexpected_adapter_failure("observe", exc_info=False)

        record = result.record
        authoritative_snapshot = result.authoritative_snapshot
        if authoritative_snapshot is None and record is not None:
            authoritative_snapshot = next(
                (snapshot for snapshot in result.page.snapshots if snapshot.get("run_id") == record.run_id),
                None,
            )
        try:
            if isinstance(request, InvocationQuery):
                if record is None or record.run_id != request.run_id:
                    return unexpected_adapter_failure("observe", exc_info=False)
                thread_id = record.thread_id
                if authoritative_snapshot is not None and (str(authoritative_snapshot["run_id"]) != request.run_id or str(authoritative_snapshot["thread_id"]) != thread_id):
                    return unexpected_adapter_failure("observe", exc_info=False)
            snapshots = tuple(
                {
                    "run_id": str(row["run_id"]),
                    "thread_id": str(row["thread_id"]),
                    "status": _status_value(row["status"]),
                    "state_version": int(row["state_version"]),
                }
                for row in result.page.snapshots
            )
            summaries = tuple(
                InvocationSummaryV1(
                    run_id=str(summary["run_id"]),
                    thread_id=str(summary["thread_id"]),
                    status=_status_value(summary["status"]),
                    state_version=int(summary["state_version"]),
                    source_kind=str(summary["source_kind"]),
                    correlation_references=tuple(
                        InvocationCorrelationReferenceV1(
                            namespace=str(reference["namespace"]),
                            key=str(reference["key"]),
                            value=reference["value"],
                        )
                        for reference in summary["correlation_references"]
                    ),
                    agent_revision_digest=summary.get("agent_revision_digest"),
                    extension_generation=summary.get("extension_generation"),
                    extension_manifest_digest=summary.get("extension_manifest_digest"),
                    extension_artifact_manifest_digest=summary.get("extension_artifact_manifest_digest"),
                    extension_configuration_digest=summary.get("extension_configuration_digest"),
                    caller_intent_digest=summary.get("caller_intent_digest"),
                    accepted_context_digest=summary.get("accepted_context_digest"),
                    authorization_evidence_digests=tuple(summary.get("authorization_evidence_digests", ())),
                    constraint_evidence_digest=summary.get("constraint_evidence_digest"),
                    assembly_evidence=summary.get("assembly_evidence"),
                    assembly_evidence_status=summary.get("assembly_evidence_status"),
                    subagent_catalog=summary.get("subagent_catalog"),
                    subagent_catalog_status=summary.get("subagent_catalog_status"),
                )
                for summary in result.page.summaries
            )
            events = tuple(
                {
                    "event_id": str(event["event_id"]),
                    "cursor": encode_lifecycle_cursor(int(event["cursor"])),
                    "run_id": str(event["run_id"]),
                    "thread_id": str(event["thread_id"]),
                    "lifecycle_type": _status_value(event["lifecycle_type"]),
                    "state_version": int(event["state_version"]),
                    "status": _status_value(event["status"]),
                    "created_at": str(event["created_at"]),
                    "payload": dict(event["payload"]),
                }
                for event in result.page.events
            )
            return InvocationObservation(
                run_id=(request.run_id if isinstance(request, InvocationQuery) else None),
                thread_id=thread_id or "",
                status=(_status_value(authoritative_snapshot["status"]) if authoritative_snapshot is not None else (_status_value(record.status) if record is not None else None)),
                state_version=(int(authoritative_snapshot["state_version"]) if authoritative_snapshot is not None else (record.state_version if record is not None else None)),
                snapshots=snapshots,
                events=events,
                next_cursor=result.page.next_cursor,
                minimum_available_cursor=result.page.minimum_available_cursor,
                read_fence_cursor=result.page.read_fence_cursor,
                summaries=summaries,
                tool_receipts=(result.page.tool_receipts.to_dict() if result.page.tool_receipts is not None else None),
                mcp_tasks=(dict(result.mcp_tasks) if result.mcp_tasks is not None else None),
                subagent_batches=(dict(result.subagent_batches) if result.subagent_batches is not None else None),
            )
        except (KeyError, TypeError, ValueError):
            return unexpected_adapter_failure("observe", exc_info=False)

    async def control(
        self,
        request: CancelInvocationRequest,
    ) -> InvocationControlReceipt | RuntimeFailure:
        if not isinstance(request, CancelInvocationRequest):
            return _failure(FailureCode.invalid_request)
        try:
            result = await self._runtime.cancel_run(
                InternalCancelRequest(
                    run_id=request.run_id,
                    action=request.action,
                    expected_state_version=request.expected_state_version,
                    principal=self._principal(),
                )
            )
        except Exception:
            return unexpected_adapter_failure("control")
        if result is NotFoundOrInvisible.not_found_or_invisible:
            return InvocationControlReceipt(disposition=ControlDisposition.not_found_or_invisible)
        if result is InvocationAuthorizationOutcome.denied:
            return InvocationControlReceipt(disposition=ControlDisposition.denied)
        if result is InvocationAuthorizationOutcome.indeterminate:
            return InvocationControlReceipt(disposition=ControlDisposition.indeterminate)
        if not isinstance(result, InternalCancelReceipt):
            return unexpected_adapter_failure("control", exc_info=False)
        disposition = ControlDisposition(result.outcome.value)
        hidden = disposition is ControlDisposition.not_found_or_invisible
        record = result.record
        if hidden or record is None:
            return InvocationControlReceipt(disposition=disposition)
        return InvocationControlReceipt(
            disposition=disposition,
            run_id=record.run_id,
            thread_id=record.thread_id,
            status=_status_value(record.status),
            state_version=record.state_version,
        )

    @staticmethod
    def capabilities() -> RuntimeCapabilities:
        return RuntimeCapabilities()


class InProcessInvocationRuntime(InvocationRuntimeAPI):
    """Supported embedded facade bound to one authenticated service identity."""

    def __init__(
        self,
        runtime: InvocationRuntime,
        *,
        authenticated_service_id: str,
    ) -> None:
        authenticated_service_id = validate_persisted_service_id(authenticated_service_id)
        super().__init__(
            runtime,
            principal=InvocationPrincipal(
                user_id=authenticated_service_id,
                role="service",
                is_internal=True,
                identity=InvocationIdentityV1(
                    effective_subject=EffectiveSubjectV1(
                        kind="service",
                        subject_id=authenticated_service_id,
                        role="service",
                    )
                ),
            ),
            source_kind=InternalSourceKind.service,
            trusted_service_id=authenticated_service_id,
        )


def build_in_process_runtime_api(
    app: Any,
    *,
    authenticated_service_id: str,
) -> InProcessInvocationRuntime:
    """Bind the supported in-process facade to a Gateway application host."""

    from app.gateway.services import build_service_invocation_runtime

    return InProcessInvocationRuntime(
        build_service_invocation_runtime(
            app,
            authenticated_service_id=authenticated_service_id,
        ),
        authenticated_service_id=authenticated_service_id,
    )


__all__ = [
    "InProcessInvocationRuntime",
    "InvocationRuntimeAPI",
    "build_in_process_runtime_api",
    "unexpected_adapter_failure",
]

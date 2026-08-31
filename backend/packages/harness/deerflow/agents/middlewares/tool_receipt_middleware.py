"""Observe durable tool attempts and render compact receipts to the model.

Ordering contract (enforced by the build-time constraints in
``deerflow.extensions.ordering.core_ordering_constraints``): this is the
outermost ``wrap_tool_call`` layer — Guardrail, SandboxAudit, ReadBeforeWrite,
and ToolProgress can short-circuit or rebuild results, and an inner receipt
layer would silently gap the ledger on those. It also sits outside result
sanitization and output budgeting, so its return path digests/stamps the exact
final model-visible projection. Normal results still carry a normalized
``deerflow_tool_meta`` status when observed (ToolErrorHandling runs on the
inner return path); short-circuit messages either self-stamp the meta or fall
back to ``message.status`` in ``make_tool_receipt``.

Durable evidence activates only through typed server-owned runtime context and
is independent from display configuration. The display-ledger injection mirrors
DurableContextMiddleware: derived from the
in-flight messages on every model call, appended as a hidden HumanMessage,
never written back to state.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelCallResult, ModelRequest, ModelResponse
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.runtime import ExecutionInfo
from langgraph.types import Command

from deerflow.agents.middlewares.message_utils import insert_after_leading_system_messages, is_genuine_user_message
from deerflow.agents.middlewares.tool_receipt import TOOL_RECEIPT_KEY, extract_tool_receipts, make_tool_receipt, render_tool_receipts
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.authz.outcome import peek_policy_outcomes, pop_policy_outcomes
from deerflow.runtime.tool_evidence import (
    ToolAttemptReservation,
    ToolDispatchObservationV1,
    ToolEvidenceError,
    build_request_projection,
    digest_request_projection,
    digest_result_projection,
    observe_tool_dispatch,
    resolve_tool_evidence_context,
)

logger = logging.getLogger(__name__)

_RECEIPT_CONTEXT_KEY = "deerflow_tool_receipt_context"


class ToolReceiptMiddleware(AgentMiddleware[AgentState]):
    """Receipt layer: zero-LLM provenance for every tool call.

    render_mode: 'always' renders the ledger on every model call (subagent
    chains — citations are produced there; without the ledger the subagent
    cannot cite and Layer 1 goes inert). 'delegation_only' renders only when
    the message stream contains a completed subagent result (lead chain —
    the one place the lead needs citation context), avoiding the always-on
    token tax in ordinary conversation turns.
    """

    state_schema = AgentState

    def __init__(self, *, render_mode: str = "always", display_enabled: bool = True) -> None:
        super().__init__()
        if render_mode not in {"always", "delegation_only"}:
            raise ValueError(f"Unknown render_mode: {render_mode}")
        self._render_mode = render_mode
        self._display_enabled = display_enabled

    def release_policy_parameters(self) -> dict[str, object]:
        return {
            "render_mode": self._render_mode,
            "display_enabled": self._display_enabled,
        }

    def _stamp_message(self, message: ToolMessage, request: ToolCallRequest) -> None:
        try:
            kwargs = dict(message.additional_kwargs or {})
            # The receipt key is runtime-owned: always overwrite, never preserve
            # a pre-existing value — a tool could otherwise forge its own
            # "evidence" and have it rendered as runtime-stamped provenance.
            kwargs[TOOL_RECEIPT_KEY] = make_tool_receipt(request.tool_call, message)
            message.additional_kwargs = kwargs
        except Exception:
            # Never block tool execution — but a systematic stamping failure must
            # be visible, or the ledger silently goes incomplete and citations lie.
            logger.warning("Failed to stamp tool receipt", exc_info=True)

    def _stamp(self, result: ToolMessage | Command, request: ToolCallRequest) -> ToolMessage | Command:
        if isinstance(result, ToolMessage):
            self._stamp_message(result, request)
            return result

        update = result.update
        if not isinstance(update, dict):
            return result
        messages = update.get("messages", [])
        if isinstance(messages, ToolMessage):
            messages = [messages]
        if not isinstance(messages, (list, tuple)):
            return result

        tool_call_id = str(request.tool_call.get("id") or "")
        for message in messages:
            if isinstance(message, ToolMessage) and str(message.tool_call_id) == tool_call_id:
                self._stamp_message(message, request)
        return result

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        context = self._runtime_context(request)
        binding, _sink = resolve_tool_evidence_context(context)
        if binding is not None:
            # The durable boundary requires an acknowledged async store write
            # before dispatch. There is no safe synchronous bridge inside the
            # running Gateway, so fail before invoking the tool.
            raise ToolEvidenceError("durable_sync_tool_unsupported")
        result = handler(request)
        return self._stamp(result, request) if self._display_enabled else result

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        context = self._runtime_context(request)
        binding, sink = resolve_tool_evidence_context(context)
        if binding is None:
            result = await handler(request)
            return self._stamp(result, request) if self._display_enabled else result

        tool_call_id = str(request.tool_call.get("id") or "")
        dispatch = self._dispatch_observation(request)
        async with binding.serialize_dispatch(tool_call_id):
            tool_name = str(request.tool_call.get("name") or "")
            arguments = request.tool_call.get("args")
            if not isinstance(arguments, Mapping):
                raise ToolEvidenceError("arguments_not_object")
            projection = build_request_projection(
                tool_name,
                arguments,
                evidence_safe_fields=self._evidence_safe_fields(request),
            )
            # Reservation and append are one fenced store operation. This await
            # is the side-effect boundary: no inner authorization, provider,
            # guardrail, or tool code runs before the start is durable.
            reservation = await sink.reserve_started(
                binding=binding,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                request_projection_digest=digest_request_projection(projection),
                dispatch=dispatch,
            )
            if not isinstance(reservation, ToolAttemptReservation):
                raise ToolEvidenceError("tool_attempt_reservation_invalid")
            started = reservation.started
            replayed_outcome = reservation.replayed_outcome
            if replayed_outcome is not None:
                result = ToolMessage(
                    content=(f"This tool call already reached durable status '{replayed_outcome.phase}' before recovery, but its prior result is unavailable. The tool was not executed again."),
                    tool_call_id=tool_call_id,
                    name=tool_name,
                    status="error",
                    additional_kwargs={
                        TOOL_META_KEY: {
                            "status": "error",
                            "error_type": "internal_error",
                        }
                    },
                )
                return self._stamp(result, request) if self._display_enabled else result
            try:
                result = await handler(request)
            except asyncio.CancelledError:
                policy = self._policy_references(context, tool_call_id)
                try:
                    await sink.record_outcome(
                        started.outcome(
                            phase="cancelled",
                            result_projection_digest=None,
                            result_kind=None,
                            safe_error_code="cancelled",
                            **policy,
                        )
                    )
                except asyncio.CancelledError:
                    pass
                except Exception:
                    # Cancellation is the caller's primary control signal. A
                    # failed best-effort terminal write must never replace it.
                    logger.warning("Failed to record durable tool cancellation outcome")
                raise
            except Exception:
                policy = self._policy_references(context, tool_call_id)
                await sink.record_outcome(
                    started.outcome(
                        phase="failed",
                        result_projection_digest=None,
                        result_kind=None,
                        safe_error_code="internal_error",
                        **policy,
                    )
                )
                raise

            phase, safe_error_code, result_kind, result_status, model_visible = self._classify_result(
                result,
                request,
                context,
            )
            result_digest = digest_result_projection(
                model_visible,
                result_kind=result_kind,
                status=result_status,
            )
            policy = self._policy_references(context, tool_call_id)
            await sink.record_outcome(
                started.outcome(
                    phase=phase,
                    result_projection_digest=result_digest,
                    result_kind=result_kind,
                    safe_error_code=safe_error_code,
                    **policy,
                )
            )
            return self._stamp(result, request) if self._display_enabled else result

    @staticmethod
    def _runtime_context(request: ToolCallRequest) -> dict:
        runtime = getattr(request, "runtime", None)
        context = getattr(runtime, "context", None) if runtime is not None else None
        return context if isinstance(context, dict) else {}

    @staticmethod
    def _dispatch_observation(
        request: ToolCallRequest,
    ) -> ToolDispatchObservationV1:
        runtime = getattr(request, "runtime", None)
        execution_info = getattr(runtime, "execution_info", None)
        if not isinstance(execution_info, ExecutionInfo):
            raise ToolEvidenceError("tool_dispatch_generation_unavailable")
        return observe_tool_dispatch(
            checkpoint_id=execution_info.checkpoint_id,
            checkpoint_ns=execution_info.checkpoint_ns,
            task_id=execution_info.task_id,
            node_attempt=execution_info.node_attempt,
        )

    @staticmethod
    def _evidence_safe_fields(request: ToolCallRequest) -> frozenset[str]:
        """Read only server-registered schema markers; arguments cannot opt in."""

        tool = getattr(request, "tool", None)
        schema = getattr(tool, "args_schema", None)
        json_schema = getattr(schema, "model_json_schema", None)
        if not callable(json_schema):
            return frozenset()
        try:
            declared = json_schema()
        except Exception:
            # Some valid LangChain tools contain injected callable fields that
            # Pydantic deliberately cannot represent as JSON Schema. Evidence
            # projection must remain fail-closed: an unreadable schema opts no
            # arguments into plaintext evidence, but does not block the tool.
            return frozenset()
        properties = declared.get("properties") if isinstance(declared, dict) else None
        if not isinstance(properties, dict):
            return frozenset()
        safe: set[str] = set()
        for name, field_schema in properties.items():
            if not isinstance(name, str) or not isinstance(field_schema, dict):
                continue
            marker = field_schema.get("x-deerflow-evidence-safe", field_schema.get("evidence_safe"))
            if marker is True:
                safe.add(name)
            elif marker not in (None, False):
                raise ToolEvidenceError("evidence_safe_policy_invalid")
        return frozenset(safe)

    @staticmethod
    def _matching_messages(result: ToolMessage | Command, request: ToolCallRequest) -> list[ToolMessage]:
        if isinstance(result, ToolMessage):
            return [result]
        update = result.update
        if not isinstance(update, dict):
            return []
        messages = update.get("messages", [])
        if isinstance(messages, ToolMessage):
            messages = [messages]
        if not isinstance(messages, (list, tuple)):
            return []
        call_id = str(request.tool_call.get("id") or "")
        return [message for message in messages if isinstance(message, ToolMessage) and str(message.tool_call_id) == call_id]

    @classmethod
    def _classify_result(
        cls,
        result: ToolMessage | Command,
        request: ToolCallRequest,
        context: dict,
    ) -> tuple[str, str | None, str, str, object]:
        messages = cls._matching_messages(result, request)
        statuses: list[str] = []
        error_types: list[str] = []
        for message in messages:
            meta = (message.additional_kwargs or {}).get(TOOL_META_KEY)
            meta = meta if isinstance(meta, dict) else {}
            statuses.append(str(meta.get("status") or message.status or "success"))
            if isinstance(meta.get("error_type"), str):
                error_types.append(meta["error_type"])
        outcomes = peek_policy_outcomes(
            context,
            str(request.tool_call.get("id") or ""),
        )
        denied = [item for item in outcomes if item.decision == "denied"]
        result_kind = "tool_message" if isinstance(result, ToolMessage) else "command"
        model_visible: object
        if len(messages) == 1:
            model_visible = messages[0].content
        else:
            model_visible = [message.content for message in messages]
        status = "error" if "error" in statuses else (statuses[0] if statuses else "success")
        if denied:
            code = "authorization_denied" if any(getattr(item, "kind", None) == "authorization" for item in denied) else "guardrail_denied"
            return "denied", code, result_kind, status, model_visible
        if status == "error":
            allowed = {
                "rate_limited",
                "permission_denied",
                "transient_error",
                "configuration_error",
                "not_found",
                "no_results",
                "internal_error",
                "unknown_error",
            }
            code = next((item for item in error_types if item in allowed), "tool_error")
            return "failed", code, result_kind, status, model_visible
        return "succeeded", None, result_kind, status, model_visible

    @staticmethod
    def _policy_references(context: dict, tool_call_id: str) -> dict[str, object]:
        outcomes = pop_policy_outcomes(context, tool_call_id)
        authz = next((item.decision_ref for item in reversed(outcomes) if item.kind == "authorization"), None)
        guardrails = tuple(dict.fromkeys(item.decision_ref for item in outcomes if item.kind == "guardrail"))
        return {
            "authz_decision_ref": authz,
            "guardrail_decision_refs": guardrails,
        }

    def _should_render(self, request: ModelRequest) -> bool:
        if self._render_mode == "always":
            return True
        # delegation_only: render only while a subagent result is being
        # processed (a task ToolMessage carries subagent_status in its
        # additional_kwargs — see subagents/status_contract.py). Scoped to the
        # current turn: only messages after the latest genuine user message
        # count, otherwise one completed delegation would keep the ledger
        # rendering on every later ordinary turn and defeat the token saving.
        # Without any genuine user message there is no turn boundary (e.g.
        # scheduled/internal invocations), so the whole stream is in scope.
        messages = list(request.messages)
        latest_user_index = -1
        for index, message in enumerate(messages):
            if is_genuine_user_message(message):
                latest_user_index = index
        turn_messages = messages[latest_user_index + 1 :] if latest_user_index >= 0 else messages
        for message in turn_messages:
            if isinstance(message, ToolMessage) and (message.additional_kwargs or {}).get("subagent_status"):
                return True
        return False

    def _inject(self, request: ModelRequest) -> ModelRequest:
        if not self._display_enabled:
            return request
        if not self._should_render(request):
            return request
        receipts = extract_tool_receipts(list(request.messages))
        ledger = render_tool_receipts(receipts)
        if not ledger:
            return request
        ledger_message = HumanMessage(
            content=ledger,
            additional_kwargs={"hide_from_ui": True, _RECEIPT_CONTEXT_KEY: True},
        )
        messages = insert_after_leading_system_messages(list(request.messages), [ledger_message])
        return request.override(messages=messages)

    @override
    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        return handler(self._inject(request))

    @override
    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        return await handler(self._inject(request))

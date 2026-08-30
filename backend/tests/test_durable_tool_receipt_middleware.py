from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from langchain_core.messages import ToolMessage

from deerflow.agents.middlewares.tool_receipt import TOOL_RECEIPT_KEY, extract_tool_receipts
from deerflow.agents.middlewares.tool_receipt_middleware import ToolReceiptMiddleware
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.authz.outcome import AuthorizationOutcome, put_authorization_outcome
from deerflow.runtime.tool_evidence import (
    TOOL_EVIDENCE_CONTEXT_KEY,
    TOOL_EVIDENCE_SINK_KEY,
    ToolEvidenceError,
    ToolEvidenceRuntimeBinding,
    ToolReceiptOwnershipLost,
    digest_result_projection,
)


class _RecordingSink:
    def __init__(self, order: list[str] | None = None) -> None:
        self.started = []
        self.outcomes = []
        self.order = order if order is not None else []

    async def record_started(self, receipt) -> None:
        self.order.append("started")
        self.started.append(receipt)

    async def reserve_started(
        self,
        *,
        binding,
        tool_call_id: str,
        tool_name: str,
        request_projection_digest: str,
    ):
        receipt = binding.make_attempt(tool_call_id, len(self.started) + 1)
        from deerflow.runtime.tool_evidence import DurableToolReceiptV1

        started = DurableToolReceiptV1.started(
            context=receipt,
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
        )
        await self.record_started(started)
        return started

    async def record_outcome(self, receipt) -> None:
        self.order.append(receipt.phase)
        self.outcomes.append(receipt)


def _binding(**changes: object) -> ToolEvidenceRuntimeBinding:
    values: dict[str, object] = {
        "run_id": "run-1",
        "execution_task_id": "run-1",
        "execution_kind": "lead",
        "subagent_name": None,
        "owner_id": "worker-1",
        "lease_epoch": 5,
        "agent_revision_digest": "a" * 64,
        "assembly_fingerprint": "b" * 64,
        "extension_generation": 3,
        "subagent_catalog_digest": "c" * 64,
        "subagent_definition_digest": None,
    }
    values.update(changes)
    return ToolEvidenceRuntimeBinding(**values)  # type: ignore[arg-type]


def _request(sink: _RecordingSink, binding: ToolEvidenceRuntimeBinding | None = None, *, call_id: str = "call-1"):
    context = {
        TOOL_EVIDENCE_CONTEXT_KEY: binding or _binding(),
        TOOL_EVIDENCE_SINK_KEY: sink,
    }
    return SimpleNamespace(
        tool_call={"name": "web_search", "id": call_id, "args": {"query": "private query", "api_token": "secret"}},
        tool=None,
        runtime=SimpleNamespace(context=context),
    )


def _success(request, content: str = "sanitized result") -> ToolMessage:
    return ToolMessage(
        content=content,
        tool_call_id=request.tool_call["id"],
        name=request.tool_call["name"],
        additional_kwargs={TOOL_META_KEY: {"status": "success"}},
    )


@pytest.mark.anyio
async def test_start_is_acknowledged_before_tool_side_effect_and_success_is_terminal() -> None:
    order: list[str] = []
    sink = _RecordingSink(order)
    request = _request(sink)
    middleware = ToolReceiptMiddleware()

    async def handler(req):
        assert order == ["started"]
        order.append("side_effect")
        return _success(req)

    result = await middleware.awrap_tool_call(request, handler)

    assert order == ["started", "side_effect", "succeeded"]
    assert sink.started[0].receipt_id == sink.outcomes[0].receipt_id
    assert sink.outcomes[0].result_projection_digest == digest_result_projection("sanitized result", result_kind="tool_message", status="success")
    assert TOOL_RECEIPT_KEY in result.additional_kwargs
    serialized = str(sink.started[0].to_event_body())
    assert "private query" not in serialized and "secret" not in serialized


@pytest.mark.anyio
async def test_start_storage_failure_prevents_tool_dispatch() -> None:
    class _UnavailableSink(_RecordingSink):
        async def reserve_started(self, **_kwargs):
            raise RuntimeError("event store unavailable")

    request = _request(_UnavailableSink())
    called = False

    async def handler(req):
        nonlocal called
        called = True
        return _success(req)

    with pytest.raises(RuntimeError, match="event store unavailable"):
        await ToolReceiptMiddleware().awrap_tool_call(request, handler)

    assert called is False


@pytest.mark.anyio
async def test_display_renumbering_cannot_change_durable_identity() -> None:
    sink = _RecordingSink()
    request = _request(sink)
    result = await ToolReceiptMiddleware().awrap_tool_call(request, lambda req: asyncio.sleep(0, result=_success(req)))
    older = ToolMessage(
        content="older",
        tool_call_id="older-call",
        name="older_tool",
        additional_kwargs={
            TOOL_RECEIPT_KEY: {
                "tool_call_id": "older-call",
                "tool_name": "older_tool",
                "status": "success",
                "args_sha256": "a" * 16,
                "output_sha256": "b" * 16,
                "output_bytes": 5,
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        },
    )

    assert [item["id"] for item in extract_tool_receipts([older, result])] == ["r1", "r2"]
    assert [item["id"] for item in extract_tool_receipts([result])] == ["r1"]
    assert sink.started[0].receipt_id == sink.outcomes[0].receipt_id


@pytest.mark.anyio
async def test_denied_call_records_policy_reference_and_no_success() -> None:
    sink = _RecordingSink()
    request = _request(sink)
    middleware = ToolReceiptMiddleware()

    async def handler(req):
        put_authorization_outcome(
            req.runtime.context,
            req.tool_call["id"],
            AuthorizationOutcome(
                decision="denied",
                policy_id="authz.main",
                policy_version="2",
                reason_codes=("denied",),
                kind="authorization",
            ),
        )
        return ToolMessage(content="Denied", tool_call_id="call-1", name="web_search", status="error")

    await middleware.awrap_tool_call(request, handler)

    outcome = sink.outcomes[0]
    assert outcome.phase == "denied"
    assert outcome.safe_error_code == "authorization_denied"
    assert outcome.authz_decision_ref and outcome.guardrail_decision_refs == ()


@pytest.mark.anyio
async def test_failed_result_uses_safe_code_without_exception_text() -> None:
    sink = _RecordingSink()
    request = _request(sink)
    middleware = ToolReceiptMiddleware()

    async def handler(_req):
        return ToolMessage(
            content="provider leaked arbitrary secret text",
            tool_call_id="call-1",
            name="web_search",
            status="error",
            additional_kwargs={TOOL_META_KEY: {"status": "error", "error_type": "rate_limited"}},
        )

    await middleware.awrap_tool_call(request, handler)
    body = sink.outcomes[0].to_event_body()

    assert sink.outcomes[0].phase == "failed"
    assert sink.outcomes[0].safe_error_code == "rate_limited"
    assert "provider leaked" not in str(body)


@pytest.mark.anyio
async def test_host_cancellation_is_recorded_then_propagated() -> None:
    sink = _RecordingSink()
    request = _request(sink)
    middleware = ToolReceiptMiddleware()

    async def handler(_req):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await middleware.awrap_tool_call(request, handler)
    assert sink.outcomes[0].phase == "cancelled"
    assert sink.outcomes[0].safe_error_code == "cancelled"


@pytest.mark.anyio
async def test_host_cancellation_still_propagates_after_ownership_loss() -> None:
    class _StaleSink(_RecordingSink):
        async def record_outcome(self, receipt) -> None:
            raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")

    sink = _StaleSink()
    request = _request(sink)

    async def handler(_req):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await ToolReceiptMiddleware().awrap_tool_call(request, handler)


def test_sync_durable_call_fails_before_side_effect() -> None:
    sink = _RecordingSink()
    request = _request(sink)
    called = False

    def handler(_req):
        nonlocal called
        called = True
        return _success(request)

    with pytest.raises(ToolEvidenceError, match="durable_sync_tool_unsupported"):
        ToolReceiptMiddleware().wrap_tool_call(request, handler)
    assert called is False


@pytest.mark.anyio
async def test_forged_untyped_context_falls_back_to_ordinary_display_receipt() -> None:
    request = SimpleNamespace(
        tool_call={"name": "bash", "id": "call-1", "args": {}},
        tool=None,
        runtime=SimpleNamespace(
            context={
                TOOL_EVIDENCE_CONTEXT_KEY: {"owner_id": "attacker"},
                TOOL_EVIDENCE_SINK_KEY: _RecordingSink(),
            }
        ),
    )
    result = await ToolReceiptMiddleware().awrap_tool_call(request, lambda req: asyncio.sleep(0, result=_success(req)))
    assert TOOL_RECEIPT_KEY in result.additional_kwargs


def test_lead_and_subagent_with_same_model_call_id_do_not_collide() -> None:
    lead = _binding()
    child = lead.for_subagent(
        execution_task_id="task-1",
        subagent_name="researcher",
        subagent_definition_digest="d" * 64,
    )
    assert lead.make_attempt("same-call", 1).execution_task_id == "run-1"
    assert child.make_attempt("same-call", 1).execution_task_id == "task-1"
    assert lead.make_attempt("same-call", 2).attempt == 2
    assert child.make_attempt("same-call", 2).attempt == 2

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    ToolEvidenceError,
    ToolEvidenceRuntimeBinding,
    build_request_projection,
    digest_request_projection,
    digest_result_projection,
    stable_receipt_id,
    stable_subagent_task_id,
)


def _context(**changes: object) -> ToolAttemptContextV1:
    values: dict[str, object] = {
        "run_id": "run-123",
        "execution_task_id": "run-123",
        "execution_kind": "lead",
        "subagent_name": None,
        "tool_call_id": "call-abc",
        "attempt": 1,
        "owner_id": "worker-1",
        "lease_epoch": 7,
        "agent_revision_digest": "a" * 64,
        "assembly_fingerprint": "b" * 64,
        "extension_generation": 4,
        "subagent_catalog_digest": "c" * 64,
        "subagent_definition_digest": None,
    }
    values.update(changes)
    return ToolAttemptContextV1(**values)  # type: ignore[arg-type]


def test_stable_receipt_id_has_full_digest_and_is_order_independent() -> None:
    context = _context()

    assert stable_receipt_id(context) == "tr_349e9d349ee45de8e46f6b250b966d5f49a3af138fc4ec20fd64f01a9bfcef52"
    assert stable_receipt_id(ToolAttemptContextV1.from_dict(dict(reversed(list(context.to_dict().items()))))) == stable_receipt_id(context)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "run-456"),
        ("execution_task_id", "task-456"),
        ("tool_call_id", "call-def"),
        ("attempt", 2),
    ],
)
def test_receipt_identity_changes_for_each_identity_component(field: str, value: object) -> None:
    assert stable_receipt_id(_context(**{field: value})) != stable_receipt_id(_context())


def test_request_projection_redacts_secrets_and_classifies_unsafe_strings_by_shape() -> None:
    first = {
        "query": "private alpha",
        "api_token": "secret-one",
        "nested": {"enabled": True, "count": 11},
    }
    second = {
        "nested": {"count": 99, "enabled": False},
        "api_token": "secret-two",
        "query": "private bravo",
    }

    first_projection = build_request_projection("web_search", first)
    second_projection = build_request_projection("web_search", second)
    serialized = json.dumps(first_projection, sort_keys=True)

    assert "private alpha" not in serialized
    assert "secret-one" not in serialized
    assert first_projection["arguments"]["api_token"] == {
        "classification": "secret_handle",
        "type": "string",
    }
    # Same field/type/string-byte-length shapes produce the same commitment;
    # raw unclassified values are not hashed into durable evidence.
    assert digest_request_projection(first_projection) == digest_request_projection(second_projection)


def test_evidence_safe_values_are_explicit_bounded_policy() -> None:
    projection = build_request_projection(
        "weather",
        {"units": "metric", "location": "private place"},
        evidence_safe_fields=frozenset({"units"}),
    )

    assert projection["arguments"]["units"] == {
        "classification": "evidence_safe",
        "type": "string",
        "value": "metric",
    }
    assert "private place" not in json.dumps(projection)
    with pytest.raises(ToolEvidenceError, match="evidence_safe_field_unknown"):
        build_request_projection("weather", {"units": "metric"}, evidence_safe_fields=frozenset({"missing"}))


def test_subagent_task_identity_is_stable_and_parent_scoped() -> None:
    binding = ToolEvidenceRuntimeBinding(
        run_id="run-123",
        execution_task_id="run-123",
        execution_kind="lead",
        subagent_name=None,
        owner_id="worker-1",
        lease_epoch=7,
        agent_revision_digest="a" * 64,
        assembly_fingerprint="b" * 64,
        extension_generation=4,
        subagent_catalog_digest="c" * 64,
        subagent_definition_digest=None,
    )

    first = stable_subagent_task_id(
        binding,
        parent_tool_call_id="task-call-1",
        subagent_name="researcher",
    )
    assert first == stable_subagent_task_id(
        binding,
        parent_tool_call_id="task-call-1",
        subagent_name="researcher",
    )
    assert first.startswith("st_") and len(first) == 67
    assert first != stable_subagent_task_id(
        binding,
        parent_tool_call_id="task-call-2",
        subagent_name="researcher",
    )


def test_projection_bounds_fail_closed() -> None:
    with pytest.raises(ToolEvidenceError, match="tool_name_too_long"):
        build_request_projection("x" * 129, {})
    with pytest.raises(ToolEvidenceError, match="projection_too_deep"):
        build_request_projection("tool", {"a": {"b": {"c": {"d": {"e": 1}}}}})
    with pytest.raises(ToolEvidenceError, match="projection_too_many_fields"):
        build_request_projection("tool", {f"f{i}": i for i in range(65)})


def test_result_digest_commits_to_exact_sanitized_model_visible_result() -> None:
    assert digest_result_projection("safe result", result_kind="tool_message", status="success") != digest_result_projection("different result", result_kind="tool_message", status="success")
    assert digest_result_projection({"b": 2, "a": 1}, result_kind="command", status="success") == digest_result_projection({"a": 1, "b": 2}, result_kind="command", status="success")


def test_receipt_validation_and_terminal_idempotency_key() -> None:
    context = _context()
    receipt_id = stable_receipt_id(context)
    started = DurableToolReceiptV1(
        version=1,
        receipt_id=receipt_id,
        idempotency_key=f"{receipt_id}:start",
        phase="started",
        tool_name="web_search",
        request_projection_digest="d" * 64,
        result_projection_digest=None,
        result_kind=None,
        safe_error_code=None,
        authz_decision_ref=None,
        guardrail_decision_refs=(),
        occurred_at=datetime.now(UTC),
        context=context,
    )

    assert started.to_event_body()["context"]["attempt"] == 1
    assert "occurred_at" not in started.to_event_body()
    for phase in ("succeeded", "failed", "denied", "cancelled"):
        terminal = started.outcome(
            phase=phase,
            result_projection_digest="e" * 64 if phase == "succeeded" else None,
            result_kind="tool_message" if phase == "succeeded" else None,
            safe_error_code={
                "succeeded": None,
                "failed": "tool_error",
                "denied": "authorization_denied",
                "cancelled": "cancelled",
            }[phase],
        )
        assert terminal.idempotency_key == f"{receipt_id}:terminal"


def test_receipt_rejects_raw_or_unbounded_evidence_fields() -> None:
    context = _context()
    receipt_id = stable_receipt_id(context)
    with pytest.raises(ToolEvidenceError, match="safe_error_code_invalid"):
        DurableToolReceiptV1(
            version=1,
            receipt_id=receipt_id,
            idempotency_key=f"{receipt_id}:terminal",
            phase="failed",
            tool_name="tool",
            request_projection_digest="d" * 64,
            result_projection_digest=None,
            result_kind=None,
            safe_error_code="provider said the password was hunter2",
            authz_decision_ref=None,
            guardrail_decision_refs=(),
            occurred_at=datetime.now(UTC),
            context=context,
        )

    with pytest.raises(ToolEvidenceError, match="authz_decision_ref_invalid"):
        DurableToolReceiptV1.started(
            context=context,
            tool_name="tool",
            request_projection_digest="d" * 64,
        ).outcome(
            phase="denied",
            result_projection_digest=None,
            result_kind=None,
            safe_error_code="authorization_denied",
            authz_decision_ref="provider returned secret policy text",
        )

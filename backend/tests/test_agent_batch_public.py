from types import SimpleNamespace

import pytest
from test_batch_scope_repository import KNOWN

from deerflow.persistence.subagent_batches.public import bound_followup_projection


@pytest.mark.parametrize(
    ("status", "reason", "state"),
    [("pending", None, "waiting"), ("dispatching", None, "waiting"), ("dispatched", None, "working"), ("dispatched", "completed", "done"), ("cancelled", "unavailable", "ended"), ("cancelled", "interrupted", "ended")],
)
def test_scoped_followup_contains_only_safe_current_disposition(status, reason, state):
    row = SimpleNamespace(
        execution_spec={"scope": KNOWN}, total_items=5, continuation_status=status, continuation_json={"prompt": "private", "carry": {"version": 1, "replacements": 0}, "outcome": {"version": 1, "reason": reason} if reason else None}
    )
    result = bound_followup_projection(row, {"succeeded": 1, "failed": 1, "cancelled": 3})
    assert result["follow_up"] == {"state": state, "finished": 2, "total": 5, **({"reason": reason} if state == "ended" else {})}
    assert result["controls"] == (["cancel"] if state in ("waiting", "working") else [])


def test_unknown_reason_hidden_and_unknown_scope_unmodified():
    row = SimpleNamespace(
        execution_spec={"scope": KNOWN}, total_items=1, continuation_status="cancelled", continuation_json={"carry": {"version": 1, "replacements": 0}, "outcome": {"version": 1, "reason": "private unknown failure detail"}}
    )
    assert "reason" not in bound_followup_projection(row, {})["follow_up"]
    for spec in ({}, {"scope": None}, {"scope": KNOWN | {"version": 2}}):
        row.execution_spec = spec
        assert bound_followup_projection(row, {}) == {}


def test_no_controls_for_a_future_carry_or_an_unrecognized_principal():
    row = SimpleNamespace(execution_spec={"scope": KNOWN}, total_items=1, continuation_status="pending", continuation_json={"carry": {"version": 2, "replacements": 0}})
    assert bound_followup_projection(row, {}) == {}
    row.continuation_json = {"carry": {"version": 1, "replacements": 0, "wait_reason": "waiting-for-answer"}}
    assert bound_followup_projection(row, {})["follow_up"]["reason"] == "waiting-for-answer"


@pytest.mark.parametrize("document", [None, {}, {"outcome": {"version": 2, "reason": "unavailable"}}, {"outcome": {"version": 1, "reason": "other"}}, {"carry": {"version": 2}, "outcome": {"version": 1, "reason": "unavailable"}}])
def test_unknown_legacy_disposition_does_not_claim_ended(document):
    row = SimpleNamespace(execution_spec={"scope": KNOWN}, total_items=2, continuation_status="cancelled", continuation_json=document)
    assert bound_followup_projection(row, {}) == {}
    row.continuation_json = {"outcome": {"version": 1, "reason": "unavailable"}}
    assert bound_followup_projection(row, {}) == {"follow_up": {"state": "ended", "reason": "unavailable", "finished": 0, "total": 2}, "controls": []}


@pytest.mark.parametrize("reason", ["agent-paused", "agent-archived"])
def test_public_agent_lifecycle_reasons_survive_projection(reason):
    row = SimpleNamespace(execution_spec={"scope": KNOWN}, total_items=1, continuation_status="cancelled", continuation_json={"carry": {"version": 1, "replacements": 0}, "outcome": {"version": 1, "reason": reason}})
    assert bound_followup_projection(row, {})["follow_up"]["reason"] == reason

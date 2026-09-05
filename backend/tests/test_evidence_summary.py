"""Bounded public evidence-summary projection."""

from __future__ import annotations

import json

from deerflow.runtime.evidence_summary import build_evidence_summary_v1
from deerflow.runtime.execution_policy import ExecutionBudgetV1, ExecutionPolicyStateV1


def test_evidence_summary_is_versioned_bounded_and_excludes_private_state() -> None:
    budget = ExecutionBudgetV1.build(profile="interactive")
    state = ExecutionPolicyStateV1(
        budget_digest=budget.digest,
        turns=4,
        total_tool_attempts=3,
        recent_tool_commitments=("f" * 64,),
        emitted_decisions=("warn:repeated_tool_loop:3",),
    )
    summary = build_evidence_summary_v1(
        run_ref="run-public",
        thread_ref="thread-public",
        status="running",
        accepted_at="2026-09-04T12:00:00Z",
        updated_at="2026-09-04T12:01:00Z",
        terminal_reason=None,
        budget=budget,
        policy_state=state,
        admission={"agent_revision_digest": "a" * 64},
        assembly={"fingerprint": "b" * 64},
        decision_events=[
            {
                "seq": 9,
                "created_at": "2026-09-04T12:00:10Z",
                "content": {
                    "version": 1,
                    "decision": "warn",
                    "reason_code": "repeated_tool_loop",
                    "current": 3,
                    "limit": 3,
                    "budget_digest": budget.digest,
                    "state_digest": state.digest,
                    "summary_key": "execution_policy.repeated_tool_loop",
                    "prompt": "must never escape",
                },
            }
        ],
        event_counts={"tools": 3, "sandbox": 1, "sandbox_diagnostics": 4, "sandbox_refusals": 1, "retrieval": 2, "mcp": 0},
        batches=[{"status": "running", "total_items": 8}],
        artifacts={"file_count": 2, "bundle_state": "available"},
        qualification="unverified",
    )

    assert summary["schema"] == "hartmesh.run-evidence-summary"
    assert summary["sections"]["sandbox"] == {"state": "available", "data": {"observation_count": 1, "diagnostic_count": 4, "refusal_count": 1, "diagnostics_shown": 0, "diagnostics_pruned": False}}
    assert summary["sandbox_diagnostics"] == []
    assert summary["schema_version"] == 1
    assert summary["overview"]["policy"]["digest"] == budget.digest
    assert summary["timeline"][0]["reason_code"] == "repeated_tool_loop"
    assert summary["sections"]["policy"]["data"]["counters"]["turns"] == 4
    encoded = json.dumps(summary, sort_keys=True)
    assert "must never escape" not in encoded
    assert "recent_tool_commitments" not in encoded
    assert "f" * 64 not in encoded
    assert len(encoded.encode()) < 64 * 1024


def test_legacy_and_unknown_sections_are_explicit() -> None:
    summary = build_evidence_summary_v1(
        run_ref="run-public",
        thread_ref="thread-public",
        status="success",
        accepted_at="2026-09-04T12:00:00Z",
        updated_at="2026-09-04T12:01:00Z",
        terminal_reason=None,
        budget=None,
        policy_state=None,
        admission=None,
        assembly=None,
        decision_events=[],
        event_counts={},
        batches=[],
        artifacts={"file_count": 0, "bundle_state": "unsupported"},
        qualification="legacy",
    )

    assert summary["sections"]["admission"]["state"] == "legacy"
    assert summary["sections"]["policy"]["state"] == "legacy"
    assert summary["sections"]["batches"]["state"] == "not_applicable"
    assert summary["qualification"]["state"] == "legacy"


def test_bounded_event_history_reports_pruned_instead_of_false_completeness() -> None:
    budget = ExecutionBudgetV1.build()
    state = ExecutionPolicyStateV1.initial(budget)
    summary = build_evidence_summary_v1(
        run_ref="run-public",
        thread_ref="thread-public",
        status="success",
        accepted_at="2026-09-04T12:00:00Z",
        updated_at="2026-09-04T12:01:00Z",
        terminal_reason=None,
        budget=budget,
        policy_state=state,
        admission={"agent_revision_digest": "a" * 64},
        assembly={"fingerprint": "b" * 64},
        decision_events=[],
        event_counts={"tools": 500, "pruned": True},
        batches=[],
        artifacts={"file_count": 0, "bundle_state": "not_applicable"},
        qualification="unqualified",
    )

    assert summary["overview"]["completeness"] == "partial"
    assert summary["sections"]["policy"]["state"] == "pruned"
    assert summary["sections"]["tools"]["state"] == "pruned"


def _summary(**overrides):
    arguments = {
        "run_ref": "run-public",
        "thread_ref": "thread-public",
        "status": "success",
        "accepted_at": "2026-09-04T12:00:00Z",
        "updated_at": "2026-09-04T12:01:00Z",
        "terminal_reason": None,
        "budget": None,
        "policy_state": None,
        "admission": None,
        "assembly": None,
        "decision_events": [],
        "event_counts": {"sandbox_diagnostics": 3},
        "batches": [],
        "artifacts": {"file_count": 0, "bundle_state": "not_applicable"},
        "qualification": "legacy",
    }
    arguments.update(overrides)
    return build_evidence_summary_v1(**arguments)


def _diagnostic(seq: int, kind: str, **facts):
    return {
        "seq": seq,
        "event_type": "sandbox.diagnostic.v1",
        "metadata": {"sequence": seq, "dropped": 0},
        "content": {
            "version": 1,
            "kind": kind,
            "session_kind": "accepted",
            "run_id": "run-private",
            "thread_id": "thread-private",
            "sandbox_ref": "accepted-execution-" + "a" * 64,
            "attempt_ref": "attempt-1",
            "batch_child_attempt_ref": None,
            "execution_evidence_digest": "b" * 64,
            "observed_at": "2026-09-04T12:00:30Z",
            "facts": facts,
            "digest": "c" * 64,
        },
    }


def test_sandbox_diagnostics_are_projected_to_kind_session_time_and_facts() -> None:
    refused = _diagnostic(7, "session.refused", requester="gateway:upload", reason="sandbox_session_conflict")
    blocked = _diagnostic(3, "egress.blocked", request_ref="req-1", host="example.com", port=443)
    blocked["content"]["session_kind"] = "ordinary"
    blocked["content"]["sandbox_ref"] = "box-1"
    unsafe = _diagnostic(5, "egress.decided", decision="allow")
    unsafe["content"]["facts"] = {"decision": "allow", "Bad Key": "x"}
    malformed = _diagnostic(6, "not-namespaced")

    summary = _summary(diagnostic_events=[refused, blocked, unsafe, malformed])

    assert summary["sandbox_diagnostics"] == [
        {"seq": 3, "at": "2026-09-04T12:00:30Z", "kind": "egress.blocked", "session_kind": "ordinary", "facts": {"host": "example.com", "port": 443, "request_ref": "req-1"}, "dropped": 0},
        {"seq": 7, "at": "2026-09-04T12:00:30Z", "kind": "session.refused", "session_kind": "accepted", "facts": {"reason": "sandbox_session_conflict", "requester": "gateway:upload"}, "dropped": 0},
    ]
    serialized = json.dumps(summary)
    for private in ("run-private", "thread-private", "box-1", "a" * 64, "b" * 64, "c" * 64, "attempt-1", "Bad Key"):
        assert private not in serialized
    assert summary["sections"]["sandbox"]["data"]["diagnostics_shown"] == 2
    assert summary["sections"]["sandbox"]["data"]["diagnostics_pruned"] is False


def test_sandbox_diagnostics_beyond_the_cap_are_reported_as_pruned() -> None:
    events = [_diagnostic(seq, "scope.opened", scope_ref=f"scope-{seq}") for seq in range(200)]

    summary = _summary(diagnostic_events=events)

    assert len(summary["sandbox_diagnostics"]) == 128
    assert summary["sections"]["sandbox"]["data"]["diagnostics_pruned"] is True

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
    assert summary["sections"]["sandbox"] == {"state": "available", "data": {"observation_count": 1, "diagnostic_count": 4, "refusal_count": 1}}
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

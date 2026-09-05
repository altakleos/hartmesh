"""Bounded browser-facing run evidence projection.

This module accepts already-authorized runtime facts and emits the only shape
the evidence UI consumes. It intentionally cannot serialize raw journal rows,
tool arguments/results, private policy commitments, or deployment provenance.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Literal

from deerflow.runtime.execution_policy import ExecutionBudgetV1, ExecutionPolicyStateV1

EVIDENCE_SUMMARY_SCHEMA = "hartmesh.run-evidence-summary"
EVIDENCE_SUMMARY_SCHEMA_VERSION = 1
MAX_EVIDENCE_SUMMARY_BYTES = 64 * 1024
MAX_TIMELINE_ITEMS = 100
MAX_BATCHES = 100

EvidenceState = Literal[
    "available",
    "not_applicable",
    "unsupported",
    "legacy",
    "pruned",
    "unqualified",
    "error",
]
QualificationState = Literal[
    "qualified",
    "unqualified",
    "unverified",
    "legacy",
    "unsupported",
]

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _section(state: EvidenceState, data: Mapping[str, object] | None = None) -> dict[str, object]:
    return {"state": state, "data": dict(data or {})}


def _safe_digest(value: object) -> str | None:
    return value if isinstance(value, str) and _DIGEST.fullmatch(value) else None


def _safe_policy_events(
    events: Sequence[Mapping[str, object]],
    *,
    budget_digest: str | None,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for event in events[:MAX_TIMELINE_ITEMS]:
        content = event.get("content")
        if not isinstance(content, Mapping) or content.get("version") != 1:
            continue
        decision = content.get("decision")
        reason = content.get("reason_code")
        current = content.get("current")
        limit = content.get("limit")
        event_budget = _safe_digest(content.get("budget_digest"))
        state_digest = _safe_digest(content.get("state_digest"))
        seq = event.get("seq")
        if (
            decision not in {"warn", "stop"}
            or not isinstance(reason, str)
            or _SAFE_REASON.fullmatch(reason) is None
            or type(seq) is not int
            or seq < 0
            or type(current) is not int
            or current < 0
            or type(limit) is not int
            or limit < 0
            or event_budget is None
            or state_digest is None
            or (budget_digest is not None and event_budget != budget_digest)
        ):
            continue
        items.append(
            {
                "seq": seq,
                "at": event.get("created_at") if isinstance(event.get("created_at"), str) else None,
                "kind": "policy_decision",
                "decision": decision,
                "reason_code": reason,
                "current": current,
                "limit": limit,
                "state_digest": state_digest,
            }
        )
    return sorted(items, key=lambda item: int(item["seq"]))


def _policy_counters(state: ExecutionPolicyStateV1) -> dict[str, object]:
    """Project safe counters only; omit commitments and dedupe internals."""

    return {
        "turns": state.turns,
        "total_tool_attempts": state.total_tool_attempts,
        "tool_category_attempts": dict(state.tool_category_attempts),
        "no_progress_observations": state.no_progress_observations,
        "batches": state.batches,
        "batch_items": state.batch_items,
        "batch_attempts": state.batch_attempts,
        "batch_runtime_seconds": state.batch_runtime_seconds,
        "retrieval_calls": state.retrieval_calls,
        "retrieval_results": state.retrieval_results,
        "retrieval_sources": state.retrieval_sources,
        "retrieval_bytes": state.retrieval_bytes,
        "sandbox_operations": state.sandbox_operations,
        "sandbox_runtime_seconds": state.sandbox_runtime_seconds,
    }


def _safe_count(value: object) -> int:
    return value if type(value) is int and 0 <= value <= 1_000_000_000 else 0


def build_evidence_summary_v1(
    *,
    run_ref: str,
    thread_ref: str,
    status: str,
    accepted_at: str,
    updated_at: str,
    terminal_reason: str | None,
    budget: ExecutionBudgetV1 | None,
    policy_state: ExecutionPolicyStateV1 | None,
    admission: Mapping[str, object] | None,
    assembly: Mapping[str, object] | None,
    decision_events: Sequence[Mapping[str, object]],
    event_counts: Mapping[str, object],
    batches: Sequence[Mapping[str, object]],
    artifacts: Mapping[str, object],
    qualification: QualificationState,
) -> dict[str, object]:
    """Build one strict, finite public projection from authorized facts."""

    if _SAFE_REF.fullmatch(run_ref) is None or _SAFE_REF.fullmatch(thread_ref) is None:
        raise ValueError("evidence_summary_reference_invalid")
    if qualification not in {"qualified", "unqualified", "unverified", "legacy", "unsupported"}:
        raise ValueError("evidence_summary_qualification_invalid")
    if policy_state is not None and (budget is None or policy_state.budget_digest != budget.digest):
        raise ValueError("policy_state_inconsistent")

    budget_digest = None if budget is None else budget.digest
    timeline = _safe_policy_events(decision_events, budget_digest=budget_digest)
    events_pruned = event_counts.get("pruned") is True
    policy_pruned = events_pruned or event_counts.get("policy_pruned") is True
    safe_batches = [
        {
            "status": batch.get("status") if isinstance(batch.get("status"), str) else "unknown",
            "total_items": _safe_count(batch.get("total_items")),
        }
        for batch in batches[:MAX_BATCHES]
    ]
    sections: dict[str, object] = {
        "admission": (
            _section("legacy")
            if admission is None
            else _section(
                "available",
                {
                    "agent_revision_digest": _safe_digest(admission.get("agent_revision_digest")),
                    "actor_evidence": "verified" if admission.get("actor_evidence") is True else "bounded",
                },
            )
        ),
        "assembly": (
            _section("legacy")
            if admission is None
            else _section(
                "available" if assembly else "error",
                {
                    "fingerprint": _safe_digest((assembly or {}).get("fingerprint")),
                    "tool_plane_digest": _safe_digest((assembly or {}).get("tool_plane_digest")),
                },
            )
        ),
        "policy": (
            _section("legacy")
            if budget is None
            else _section(
                ("pruned" if policy_state is not None and policy_pruned else "available" if policy_state is not None else "error"),
                {
                    "profile": budget.profile,
                    "budget_digest": budget.digest,
                    "counters": {} if policy_state is None else _policy_counters(policy_state),
                    "decision_count": len(timeline),
                },
            )
        ),
        "tools": _section(
            "pruned" if events_pruned else "available" if _safe_count(event_counts.get("tools")) else "not_applicable",
            {"receipt_count": _safe_count(event_counts.get("tools"))},
        ),
        "batches": _section(
            "available" if safe_batches else "not_applicable",
            {"count": len(safe_batches), "items": safe_batches},
        ),
        "sandbox": _section(
            "pruned" if events_pruned else "available" if _safe_count(event_counts.get("sandbox")) or _safe_count(event_counts.get("sandbox_diagnostics")) else "not_applicable",
            {
                "observation_count": _safe_count(event_counts.get("sandbox")),
                "diagnostic_count": _safe_count(event_counts.get("sandbox_diagnostics")),
            },
        ),
        "retrieval": _section(
            "pruned" if events_pruned else "available" if _safe_count(event_counts.get("retrieval")) else "not_applicable",
            {"observation_count": _safe_count(event_counts.get("retrieval"))},
        ),
        "mcp": _section(
            "pruned" if events_pruned else "available" if _safe_count(event_counts.get("mcp")) else "not_applicable",
            {"observation_count": _safe_count(event_counts.get("mcp"))},
        ),
        "artifacts": _section(
            "available" if _safe_count(artifacts.get("file_count")) else "not_applicable",
            {
                "file_count": _safe_count(artifacts.get("file_count")),
                "bundle_state": (artifacts.get("bundle_state") if artifacts.get("bundle_state") in {"available", "not_applicable", "unsupported", "legacy", "pruned", "unqualified", "error"} else "error"),
            },
        ),
    }
    completeness = "complete"
    if status in {"pending", "running"}:
        completeness = "in_progress"
    elif any(section["state"] in {"legacy", "pruned", "error", "unqualified"} for section in sections.values()):
        completeness = "partial"

    summary: dict[str, object] = {
        "schema": EVIDENCE_SUMMARY_SCHEMA,
        "schema_version": EVIDENCE_SUMMARY_SCHEMA_VERSION,
        "overview": {
            "run_ref": run_ref,
            "thread_ref": thread_ref,
            "status": status,
            "accepted_at": accepted_at,
            "updated_at": updated_at,
            "terminal_reason": (terminal_reason if terminal_reason is None or _SAFE_REASON.fullmatch(terminal_reason) else "runtime_error"),
            "policy": None if budget is None else {"profile": budget.profile, "digest": budget.digest},
            "completeness": completeness,
        },
        "timeline": timeline,
        "sections": sections,
        "qualification": {"state": qualification},
    }
    if len(json.dumps(summary, separators=(",", ":"), sort_keys=True).encode("utf-8")) > MAX_EVIDENCE_SUMMARY_BYTES:
        raise ValueError("evidence_summary_limit_exceeded")
    return summary


__all__ = [
    "EVIDENCE_SUMMARY_SCHEMA",
    "EVIDENCE_SUMMARY_SCHEMA_VERSION",
    "MAX_EVIDENCE_SUMMARY_BYTES",
    "build_evidence_summary_v1",
]

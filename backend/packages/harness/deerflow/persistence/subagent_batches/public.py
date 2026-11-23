"""Owner-facing large-job state. Stored authority and diagnostic details stay private."""

from collections.abc import Mapping
from typing import Any

from deerflow.persistence.subagent_batches.bound import carry
from deerflow.persistence.subagent_batches.model import SubagentBatchRow
from deerflow.subagents.batch_scope import known_agent_batch_scope

# Unknown reasons keep their general state and never become user-facing text.
_PUBLIC_REASONS = frozenset(
    {
        "unavailable",
        "interrupted",
        "cancelled",
        "source_ended",
        "start_timeout",
        "account-changed",
        "access-lost",
        "agent-paused",
        "agent-archived",
        "files-full",
        "waiting-for-answer",
        "goal-active",
        "busy",
        "run-limit",
        "restarting",
        "starting-elsewhere",
        "start-failed",
        "storage-busy",
        "execution-unavailable",
        "cannot-run",
    }
)


def bound_followup_projection(row: SubagentBatchRow, counts: Mapping[str, int]) -> dict[str, Any]:
    if known_agent_batch_scope(row.execution_spec) is None:
        return {}
    stored = row.continuation_json if isinstance(row.continuation_json, dict) else {}
    outcome = stored.get("outcome")
    reason = outcome.get("reason") if isinstance(outcome, dict) and type(outcome.get("version")) is int and outcome["version"] == 1 else None
    phase = carry(row)
    if phase is None:
        # Older writers recorded a startup cancellation without a carry document.
        if "carry" in stored or row.continuation_status != "cancelled" or reason != "unavailable":
            return {}
    if outcome and row.continuation_status in ("pending", "dispatching", "dispatched") and reason != "completed":
        return {}
    if row.continuation_status in ("pending", "dispatching"):
        state = "waiting"
        reason = phase.get("wait_reason")
    elif row.continuation_status == "dispatched":
        state = "done" if reason == "completed" else "working"
        reason = None
    else:
        state = "ended"
    follow_up = {"state": state, "finished": counts.get("succeeded", 0) + counts.get("failed", 0), "total": row.total_items}
    if isinstance(reason, str) and reason in _PUBLIC_REASONS:
        follow_up["reason"] = reason
    return {"follow_up": follow_up, "controls": ["cancel"] if state in ("waiting", "working") else []}

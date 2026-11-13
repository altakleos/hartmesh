"""Pure wording choices for source-qualified Start and original-attempt Stop."""

from dataclasses import dataclass

from deerflow.agent_instances.work import UNRESOLVED


@dataclass(frozen=True)
class ExecutionNotice:
    key: str
    restart: bool = False
    retry: bool = False
    cancel: bool = False
    preview: bool = False


_START_REASONS = {
    "work-changed": "start.changed",
    "sources-changed": "start.changed",
    "attempt-running": "start.already",
    "attempt-unsettled": "start.not_settled",
    "awaiting-review": "start.needs_review",
    "work-closed": "start.closed",
    "brief-changed": "start.brief_changed",
    "lifecycle-pending": "start.settings_changing",
    "source-unavailable": "start.source_missing",
    "source-not-shared": "start.source_missing",
    "work-off": "assign.no_work",
    "run-limit": "start.refused.run-limit",
    "restarting": "start.refused.restarting",
    "storage-busy": "start.refused.storage-busy",
}


def execution_notice(act, *, original=None, dispatch=None):
    """No authority or receipt is inferred here; the source owner supplies them."""
    if act.kind == "activate":
        if act.state == "done":
            # The single canonical status root already owns this answer.
            return None
        if act.cancel_requested_at is not None:
            return ExecutionNotice("start.cancelled" if act.state == "refused" and act.reason == "cancelled" else "start.cancel_unconfirmed")
        if original is not None and original.get("status") in UNRESOLVED and original.get("run_id") is None and (act.state in {"refused", "conflict"} or act.state == "waiting" and act.reason == "work-limit"):
            return ExecutionNotice("act.unconfirmed", retry=True)
        if act.state == "waiting":
            if act.reason == "busy":
                return ExecutionNotice("start.busy_wait", cancel=True)
            if act.reason == "work-limit":
                return ExecutionNotice("busy.work_limit")
        if act.state == "uncertain":
            return ExecutionNotice("act.unconfirmed", retry=True)
        if act.state == "pending":
            return None
        if act.state in {"refused", "conflict"}:
            if act.reason == "busy":
                return ExecutionNotice("start.busy_gave_up", restart=True, preview=True)
            key = _START_REASONS.get(act.reason, "denied.start" if act.reason in {"denied", "grant-required", "not-visible"} else "unavailable.fix" if act.reason == "execution-unavailable" else "tap.failed")
            return ExecutionNotice(
                key, restart=act.reason in {"run-limit", "restarting", "storage-busy", "lifecycle-pending"}, preview=key == "start.changed" or act.reason in {"run-limit", "restarting", "storage-busy", "lifecycle-pending"}
            )
    elif act.kind == "stop":
        if act.state == "done":
            if original is not None and original.get("status") in {"succeeded", "failed", "cancelled"} and original.get("settled_at"):
                return ExecutionNotice("status.stopped" if original["status"] == "cancelled" else "status.ended")
            if dispatch is not None and dispatch.get("state") == "ended":
                return ExecutionNotice("stop.unconfirmed")
            if dispatch is None or dispatch.get("state") == "uncertain":
                return ExecutionNotice("act.unconfirmed", retry=True)
            return ExecutionNotice("status.stopping")
        if act.state == "uncertain":
            return ExecutionNotice("act.unconfirmed", retry=True)
        if act.state in {"pending", "waiting"}:
            return None
        if act.state in {"refused", "conflict"}:
            return ExecutionNotice("stop.nothing" if act.reason == "no-running-attempt" else "stop.not_yours" if act.reason in {"denied", "grant-required"} else "stop.failed")
    raise ValueError("unsupported-execution-outcome")

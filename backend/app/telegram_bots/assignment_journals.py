"""Canonical delegation evidence for a private draft's own saved presentation."""

from datetime import timedelta

from app.telegram_bots.assignment_sources import private_facts, private_snapshot
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import request_for, same_body, thaw
from app.telegram_bots.work_delegate_actions import delegate_source, draft_body_signature, qualify_delegate_receipt, qualify_no_due, require_delegate_scope

RETRY_REASONS = frozenset({"agent-not-active", "work-off", "lifecycle-pending", "denied", "grant-required", "not-visible", "storage-busy"})


def action_facts(row):
    if row is None:
        return None
    return {**private_facts(row), "continuation": thaw(row.continuation), "stop_dispatch": thaw(row.stop_dispatch)}


def event_facts(row):
    return {key: getattr(row, key) for key in ("id", "instance_id", "work_id", "operation_id", "action", "actor_kind", "actor_id", "request", "result", "revision")} if row is not None else None


def exact_delegation(link, root, parent, child):
    try:
        require_delegate_scope(link, parent)
        source = delegate_source(parent)
        request = request_for("delegate", parent.body, identity=parent.id)
        draft = root.body["draft"]
        if (
            list(root.operation_ids) != [parent.id]
            or parent.stop_dispatch is not None
            or child is not None
            and child.stop_dispatch is not None
            or source["parent_id"] is not None
            or source["draft_id"] != root.id
            or source["draft_signature"] != draft_body_signature(link, root)
            or not same_body(source["due_text"], root.body["original"]["due_text"])
            or (request.objective, request.success_criteria) != (draft["objective"], draft["success_criteria"])
            or not same_body(request.model_dump(mode="json").get("due_at"), draft["due_at"])
            or not same_body(request.model_dump(mode="json", exclude_unset=True), parent.body)
        ):
            raise ValueError()
        if parent.operation_ids:
            if child is None:
                raise ValueError()
            require_delegate_scope(link, child)
            qualify_no_due(parent, child)
        elif child is not None:
            raise ValueError()
        return child or parent
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ProductSourceRetired() from None


def saved_receipt(link, act, event):
    """A done journal alone never proves that a Work record was created."""
    try:
        if act.state != "done" or (act.instance_id, act.owner_user_id) != (link.agent_ref, link.owner_user_id):
            return None
        return qualify_delegate_receipt(act, event, act.result)
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def journal_state(link, root, parent, child, original_event, child_event, *, now, config):
    effective = exact_delegation(link, root, parent, child)
    if child is not None and original_event is not None:
        raise ProductSourceRetired()
    receipt = saved_receipt(link, effective, child_event if child is not None else original_event)
    if receipt is not None and parent.state == "done":
        if child is not None and not same_body(parent.result, {"delegate_child_id": child.id, "receipt": child.result}):
            raise ProductSourceRetired()
        return "saved", None, receipt
    # A cancellation fence stops future effects; it cannot undo an earlier save.
    if parent.cancel_requested_at is not None or effective.cancel_requested_at is not None:
        return "unconfirmed", {"key": "assign.save_cancelled", "cancel": False, "retry": False}, None
    event = child_event if child is not None else original_event
    if parent.state in {"refused", "conflict"} and effective.state in {"refused", "conflict"} and event is None:
        return "refused", {"key": "assign.not_saved", "cancel": False, "retry": effective.reason in RETRY_REASONS}, None
    if effective.state == "pending" and effective.first_attempt_at is not None and now - effective.first_attempt_at < timedelta(hours=1) and effective.attempts < config.acts.max_attempts:
        return "saving", {"key": "assign.saving", "cancel": True, "retry": False}, None
    return "unconfirmed", {"key": "act.unconfirmed" if parent.state != "done" else "assign.save_unconfirmed", "cancel": False, "retry": parent.state != "done"}, None


def journal_rows(value):
    return tuple(private_snapshot(value[key]) if value.get(key) is not None else None for key in ("act", "child"))


def created_work(source, link):
    """Identify the canonical Work behind an acknowledged saved-card preview."""
    from types import SimpleNamespace

    try:
        value = source["work"]
        if source["key"] != "assignment.card" or value["phase"] != "saved":
            raise ValueError()
        root, (parent, child) = private_snapshot(value["root"]), journal_rows(value)
        effective = exact_delegation(link, root, parent, child)
        event = value["child_event"] if child is not None else value["original_event"]
        receipt = saved_receipt(link, effective, SimpleNamespace(**event) if event is not None else None)
        if (
            parent.state != "done"
            or receipt is None
            or child is not None
            and not same_body(parent.result, {"delegate_child_id": child.id, "receipt": child.result})
            or value["detail"]["record"]["id"] != receipt["id"]
            or not same_body(root.continuation, {"version": 1, "assignment_admission": parent.continuation.get("delegate_origin")})
        ):
            raise ValueError()
        return receipt["id"]
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ProductSourceRetired() from None

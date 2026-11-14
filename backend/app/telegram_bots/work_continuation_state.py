"""Closed composite authorization and exact child identity; no effects."""

from collections.abc import Mapping
from datetime import datetime, timedelta
from hashlib import sha256

from app.telegram_bots.notifications import aware


def activation_child_id(identity):
    return sha256((identity + ":activate").encode()).hexdigest()[:32]


def continuation_state(act):
    from app.telegram_bots.work_actions import request_for, thaw

    value = thaw(act.continuation)
    if type(value) is not dict or type(value.get("version")) is not int or value["version"] != 1:
        raise ValueError("invalid-activation-continuation")
    data = value.get("activation_continuation")
    if (
        type(data) is not dict
        or set(data) - {"refusal"} != {"version", "phase", "authorized_at", "child_id", "work_id", "assignment_revision", "child_body"}
        or type(data["version"]) is not int
        or data["version"] != 1
        or data["phase"] not in {"awaiting_primary", "ready", "child_planned", "done", "refused", "cancelled", "expired"}
        or data["child_id"] != activation_child_id(act.id)
        or type(data["authorized_at"]) is not str
    ):
        raise ValueError("invalid-activation-continuation")
    stamp = datetime.fromisoformat(data["authorized_at"])
    if stamp.utcoffset() is None:
        raise ValueError("invalid-activation-continuation")
    empty_target = data["work_id"] is None and data["assignment_revision"] is None
    if data["phase"] == "awaiting_primary" and not empty_target or not empty_target and (type(data["work_id"]) is not str or not data["work_id"] or type(data["assignment_revision"]) is not int or data["assignment_revision"] < 1):
        raise ValueError("invalid-activation-continuation")
    if empty_target and data["phase"] not in {"awaiting_primary", "refused", "cancelled", "expired"}:
        raise ValueError("invalid-activation-continuation")
    if data["child_body"] is not None:
        request = request_for("activate", data["child_body"], identity=data["child_id"])
        if request.expected_assignment_revision != data["assignment_revision"]:
            raise ValueError("invalid-activation-continuation")
    if data["phase"] in {"child_planned", "done"} and data["child_body"] is None or data["phase"] in {"awaiting_primary", "ready"} and data["child_body"] is not None:
        raise ValueError("invalid-activation-continuation")
    if "refusal" in data:
        refusal = data["refusal"]
        if (
            data["phase"] != "refused"
            or data["child_body"] is not None
            or type(refusal) is not dict
            or set(refusal) != {"kind", "reason"}
            or refusal["kind"] not in {"conflict", "denied", "refused"}
            or type(refusal["reason"]) is not str
            or not 1 <= len(refusal["reason"]) <= 128
        ):
            raise ValueError("invalid-activation-continuation")
    return data


def child_marker(parent):
    from app.telegram_bots.work_actions import _ACT_SCOPE, thaw
    from app.telegram_bots.work_result_identity import digest

    return {"version": 1, "act_id": parent.id, "source_signature": digest([*[getattr(parent, key) for key in _ACT_SCOPE], thaw(parent.body)]), "authorized_at": continuation_state(parent)["authorized_at"]}


def require_child(parent, child):
    from app.telegram_bots.work_actions import same_body

    data = continuation_state(parent)
    keys = ("link_id", "owner_user_id", "bot_id", "binding_generation", "telegram_user_id", "chat_id", "instance_id")
    if (
        child is None
        or any(getattr(parent, key) != getattr(child, key) for key in keys)
        or (child.id, child.kind, child.work_id, child.request_id, child.message_id) != (data["child_id"], "activate", data["work_id"], None, None)
        or list(parent.operation_ids).count(child.id) != 1
        or not same_body(child.body, data["child_body"])
        or not isinstance(child.continuation, Mapping)
        or not same_body(child.continuation.get("activation_parent"), child_marker(parent))
        or child.first_attempt_at is None
        or aware(child.first_attempt_at) >= aware(datetime.fromisoformat(data["authorized_at"])) + timedelta(hours=1)
    ):
        raise ValueError("invalid-activation-child")

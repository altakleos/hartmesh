"""Closed original request consent and durable activation readiness."""

from collections.abc import Mapping

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_continuation_state import activation_child_id, continuation_state
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.work import _date


def request_signature(parent):
    from copy import deepcopy

    keyboard = deepcopy(parent.payload["product_keyboard_basis"])
    for line in keyboard:
        for control in line:
            control["payload"].pop("transcript", None)
    return digest(
        [
            *[getattr(parent, key) for key in ("id", "link_id", "owner_user_id", "binding_generation", "chat_id", "message_thread_id", "instance_id", "work_id", "request_id")],
            parent.payload["presentation_version"],
            parent.payload["work_request_basis"] if "work_request_basis" in parent.payload else parent.payload["product_basis"],
            keyboard,
            parent.parts,
        ]
    )


def request_input(mark, *, kind):
    if mark is None or len(mark.operation_ids) != 1:
        raise CallbackRefused()
    return {
        "kind": kind,
        "link_id": mark.link_id,
        "chat_id": mark.chat_id,
        "message_id": mark.message_id,
        "update_id": mark.update_id,
        "binding_generation": mark.binding_generation,
        "operation_id": mark.operation_ids[0],
        "signature": digest(thaw([_date(mark.accepted_at), mark.sanitized_input, {key: mark.route_basis.get(key) for key in ("work_input", "work_answer", "voice_note")}, list(mark.operation_ids)])),
    }


def outcome_signature(row):
    return digest([row.id, row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.message_thread_id, row.instance_id, row.work_id, row.request_id, row.payload["work_outcome_basis"], row.payload["product_keyboard_basis"]])


def request_activation_value(value):
    """Parse closed private metadata; this alone never proves source authority."""
    from types import SimpleNamespace

    value = thaw(value)
    if type(value) is not dict or set(value) - {"work_outcome", "work_outcome_transfer"} != {"version", "request_activation", "activation_continuation"} or type(value["version"]) is not int or value["version"] not in {1, 2}:
        raise ValueError("invalid-request-activation")
    source = value["request_activation"]
    if (
        type(source) is not dict
        or set(source) != {"version", "origin", "assignment_revision", "input", "outcome_id", "outcome_signature"}
        or type(source["version"]) is not int
        or source["version"] != 1
        or type(source["assignment_revision"]) is not int
        or source["assignment_revision"] < 1
    ):
        raise ValueError("invalid-request-activation")
    if source["outcome_id"] is not None and (type(source["outcome_id"]) is not str or len(source["outcome_id"]) != 32 or any(char not in "0123456789abcdef" for char in source["outcome_id"])):
        raise ValueError("invalid-request-activation")
    identity = source["input"]
    if identity is not None:
        if type(identity) is not dict or set(identity) != {"kind", "link_id", "chat_id", "message_id", "update_id", "binding_generation", "operation_id", "signature"} or identity["kind"] not in {"voice", "typed"}:
            raise ValueError("invalid-request-input")
        for key, size in (("link_id", 32), ("operation_id", 32), ("signature", 64)):
            if type(identity[key]) is not str or len(identity[key]) != size or any(char not in "0123456789abcdef" for char in identity[key]):
                raise ValueError("invalid-request-input")
        for key in ("chat_id", "message_id", "update_id", "binding_generation"):
            if type(identity[key]) is not int or identity[key] < (1 if key in {"chat_id", "message_id"} else 0):
                raise ValueError("invalid-request-input")
    signature = source["outcome_signature"]
    if (source["outcome_id"] is None) != (signature is None) or signature is not None and (type(signature) is not str or len(signature) != 64 or any(char not in "0123456789abcdef" for char in signature)):
        raise ValueError("invalid-request-activation")
    origin = source["origin"]
    if type(origin) is not dict or set(origin) != {"callback_id", "anchor_outbox_id", "presentation_version", "signature"} or type(origin["presentation_version"]) is not int or origin["presentation_version"] < 0:
        raise ValueError("invalid-request-activation")
    for key, size in (("callback_id", 32), ("anchor_outbox_id", 32), ("signature", 64)):
        if type(origin[key]) is not str or len(origin[key]) != size or any(char not in "0123456789abcdef" for char in origin[key]):
            raise ValueError("invalid-request-activation")
    if (value["version"] == 2) != ("work_outcome_transfer" in value):
        raise ValueError("invalid-request-activation")
    continuation_state(SimpleNamespace(id=origin["callback_id"], continuation={**value, "version": 1}))
    return source


def request_activation(act):
    source = request_activation_value(act.continuation)
    if (
        act.kind not in {"decide", "respond"}
        or source["origin"]["callback_id"] != act.id
        or source["assignment_revision"] != act.body["expected_assignment_revision"]
        or act.continuation["version"] != 1
        or act.first_attempt_at is None
        or continuation_state(act)["authorized_at"] != _date(act.first_attempt_at)
    ):
        raise ValueError("invalid-request-activation")
    identity = source["input"]
    if act.kind == "respond" and (identity is None or identity["kind"] != "voice") or act.kind == "decide" and identity is not None and identity["kind"] != "typed":
        raise ValueError("invalid-request-input")
    return source


def stage_request_activation(act, callback, parent, source, *, mark=None):
    if (
        (callback.kind, act.kind) not in {("decide_confirm_resume", "decide"), ("voice_save_resume", "respond")}
        or source.request.get("can_resume") is not True
        or type(callback.payload.get("assignment_revision")) is not int
        or callback.payload["assignment_revision"] != source.seen["assignment_revision"]
    ):
        raise CallbackRefused()
    value = {
        "version": 1,
        "request_activation": {
            "version": 1,
            "origin": {"callback_id": callback.id, "anchor_outbox_id": parent.id, "presentation_version": callback.payload["presentation_version"], "signature": request_signature(parent)},
            "assignment_revision": source.seen["assignment_revision"],
            "input": request_input(mark, kind="voice" if act.kind == "respond" else "typed") if mark is not None else None,
            "outcome_id": None,
            "outcome_signature": None,
        },
        "activation_continuation": {"version": 1, "phase": "awaiting_primary", "authorized_at": _date(act.first_attempt_at), "child_id": activation_child_id(act.id), "work_id": None, "assignment_revision": None, "child_body": None},
    }
    if act.continuation is not None and not same_body(act.continuation, value):
        # Replays retain their already advanced phase and original consent.
        old = request_activation(act)
        if not same_body({**old, "outcome_id": None, "outcome_signature": None}, value["request_activation"]):
            raise CallbackRefused()
        return
    act.continuation = value


def ready_request_activation(act, row):
    if not isinstance(act.continuation, Mapping) or "request_activation" not in act.continuation:
        return False
    source = request_activation(act)
    state = continuation_state(act)
    if act.state != "done" or act.cancel_requested_at is not None or "work_outcome_transfer" in act.continuation or state["phase"] != "awaiting_primary":
        return False
    act.continuation = {
        **thaw(act.continuation),
        "request_activation": {**source, "outcome_id": row.id, "outcome_signature": outcome_signature(row)},
        "activation_continuation": {**state, "phase": "ready", "work_id": act.work_id, "assignment_revision": source["assignment_revision"]},
    }
    return True

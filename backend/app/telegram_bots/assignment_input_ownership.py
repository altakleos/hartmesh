"""Retain a typed prompt's original custody until atomic private admission."""

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_view_origins import anchor_signature


def typed_basis(mark):
    value = thaw(mark.route_basis.get("assignment_owner"))
    if value is None:
        return None
    if (
        type(value) is not dict
        or set(value) != {"version", "owner", "purpose", "prompt_id", "source_outbox_id", "signature", "origin"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["owner"] not in {"assignment", "management"}
        or value["purpose"] not in ({"work_edit"} if value["owner"] == "management" else {"assign_words", "assign_done", "assign_edit"})
        or any(type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]) for key, size in (("prompt_id", 32), ("source_outbox_id", 32), ("signature", 64)))
        or not same_body(value["origin"], origin_facts(mark))
    ):
        raise ProductSourceRetired()
    return value


def capture_basis(mark, prompt, parent):
    value = {
        "version": 1,
        "owner": "management" if prompt.payload["purpose"] == "work_edit" else "assignment",
        "purpose": prompt.payload["purpose"],
        "prompt_id": prompt.id,
        "source_outbox_id": parent.id,
        "signature": anchor_signature(_item(parent)),
        "origin": origin_facts(mark),
    }
    return value


def require_owner(link, mark, prompt, parent, *, expected=None):
    value = typed_basis(mark)
    if (
        value is None
        or expected is not None
        and not same_body(value, expected)
        or prompt is None
        or parent is None
        or (prompt.id, parent.id) != (value["prompt_id"], value["source_outbox_id"])
        or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) != (link.id, link.owner_user_id, link.binding_generation, mark.chat_id, mark.message_thread_id)
        or (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (link.id, mark.chat_id, link.binding_generation, mark.message_thread_id)
        or prompt.payload.get("purpose") != value["purpose"]
        or anchor_signature(_item(parent)) != value["signature"]
        or mark.route_kind != "assignment_hold"
        or mark.route_target_id != parent.id
        or mark.operation_ids
        or mark.route_complete
    ):
        raise ProductSourceRetired()
    return value


def release_hold(mark):
    if mark.route_kind != "assignment_hold" or typed_basis(mark) is None:
        raise ProductSourceRetired()
    mark.route_kind, mark.route_target_id = None, None
    mark.route_basis = {key: value for key, value in mark.route_basis.items() if key != "assignment_owner"}

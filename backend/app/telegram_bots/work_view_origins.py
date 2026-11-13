"""Durable explicit gestures behind unconcealed Work list/detail replies."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import _basis
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news_delivery import frozen_news
from app.telegram_bots.work_result_identity import digest


def acknowledged_view(link, parent, version):
    """Reconstruct an acknowledged original even after a source refresh."""
    item = _item(parent)
    current = item.payload["presentation_version"]
    if type(version) is not int or not 0 <= version <= current:
        raise ProductSourceRetired()
    if version != current:
        try:
            old = item.payload["work_news_basis"]["history"][version]
            basis = deepcopy(old["basis"])
            if basis.get("version") == 3:
                basis["history"] = deepcopy(item.payload["work_news_basis"]["history"][:version])
            item = replace(
                item,
                class_=old["class"],
                parts=[part for part in item.parts if part["presentation_version"] < version] + old["parts"],
                payload={**item.payload, "presentation_version": version, "work_news_basis": basis, "product_keyboard_basis": basis["keyboard"]},
            )
        except (KeyError, IndexError, TypeError):
            raise ProductSourceRetired() from None
    frozen_news(SimpleNamespace(link=link, item=item, part=None), acknowledged=True)
    return item


def anchor_signature(item):
    return digest([item.id, item.payload["presentation_version"], item.payload["work_news_basis"], item.payload["product_keyboard_basis"], item.parts])


def qualify_origin(link, item, origin, rows, column, clock):
    mark, gesture, parent, _ = rows[0][column : column + 4]
    if type(origin) is not dict:
        raise ProductSourceRetired()
    if origin.get("kind") == "inbox":
        if (
            set(origin) != {"kind", "message_id", "update_id"}
            or type(origin["message_id"]) is not int
            or type(origin["update_id"]) is not int
            or mark is None
            or (mark.link_id, mark.chat_id, mark.binding_generation, mark.update_id, mark.message_thread_id) != (link.id, link.chat_id, link.binding_generation, origin["update_id"], item.message_thread_id)
            or mark.sanitized_input.get("command") != "work"
            or mark.route_kind != "work_view"
            or mark.route_target_id != item.payload["work_view_intent"]["cursor"]
            or not same_body(_basis(mark), {"cursor": item.payload["work_view_intent"]["cursor"]})
            or not mark.route_complete
            or mark.outbox_id != item.id
            or item.id not in mark.delivery_ids
        ):
            raise ProductSourceRetired()
        return
    if origin.get("kind") != "callback" or set(origin) != {"kind", "gesture_id", "callback_id", "anchor_outbox_id", "presentation_version", "anchor_signature"}:
        raise ProductSourceRetired()
    if (
        gesture is None
        or parent is None
        or (gesture.link_id, gesture.chat_id, gesture.binding_generation, gesture.kind, gesture.target_kind, gesture.target_id, gesture.message_thread_id)
        != (link.id, link.chat_id, link.binding_generation, "work_view_gesture", "outbox", item.id, item.message_thread_id)
        or not same_body(gesture.payload, origin)
        or gesture.consumed_at is not None
        or gesture.expires_at is not None
        and aware(gesture.expires_at) <= clock()
        or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation) != (link.id, link.owner_user_id, link.chat_id, link.binding_generation)
    ):
        raise ProductSourceRetired()
    original = acknowledged_view(link, parent, origin["presentation_version"])
    if anchor_signature(original) != origin["anchor_signature"]:
        raise ProductSourceRetired()
    controls = {control.id: control for row in rows if (control := row[column + 3]) is not None}
    try:
        _validate_rows(original, list(controls.values()))
    except CallbackRefused:
        raise ProductSourceRetired() from None
    selected = controls.get(origin["callback_id"])
    final = [part for part in original.parts if part["presentation_version"] == origin["presentation_version"]][-1]
    if selected is None or gesture.message_id != selected.message_id or selected.message_id != final["message_ids"][0] or selected.group_id != final.get("callback_group_id") or selected.kind not in {"work_details", "work_list_page"}:
        raise ProductSourceRetired()
    expected = {"key": "view.detail", "work_id": selected.payload.get("work_id")} if selected.kind == "work_details" else {"key": "view.list", "cursor": selected.payload.get("cursor")}
    if not same_body({key: value for key, value in item.payload["work_view_intent"].items() if key not in {"version", "origin"}}, expected):
        raise ProductSourceRetired()

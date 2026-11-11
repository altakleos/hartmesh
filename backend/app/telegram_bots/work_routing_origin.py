"""A consumed routing choice owns subsequent direct presentation recovery."""

from datetime import datetime

from app.telegram_bots.producer import ProductSourceRetired


def require_choice(callback, parent):
    try:
        if (
            callback is None
            or callback.kind != "route_to"
            or callback.consumed_at is None
            or callback.closed_reason != "routing-refresh"
            or (callback.link_id, callback.chat_id, callback.binding_generation, callback.message_thread_id, callback.target_kind, callback.target_id, callback.payload.get("request_id"), callback.payload.get("anchor_outbox_id"))
            != (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id, "request", parent.request_id, parent.request_id, parent.id)
            or type(callback.payload.get("request_revision")) is not int
            or callback.payload["request_revision"] < 1
            or type(callback.payload.get("recipient_id")) is not str
            or not callback.payload["recipient_id"]
        ):
            raise ValueError()
        version = callback.payload["presentation_version"]
        current = parent.payload["presentation_version"]
        if type(version) is not int or type(current) is not int or not 0 <= version < current:
            raise ValueError()
        parts = [part for part in parent.parts if part.get("presentation_version") == version]
        if not parts:
            raise ValueError()
        messages, identities = set(), set()
        for index, part in enumerate(parts):
            identity = part["part_id"]
            if (
                type(part["presentation_version"]) is not int
                or type(part["ordinal"]) is not int
                or part["ordinal"] != index
                or type(identity) is not str
                or len(identity) != 32
                or any(char not in "0123456789abcdef" for char in identity)
                or identity in identities
                or part["state"] != "sent"
                or len(part["message_ids"]) != 1
                or type(part["message_ids"][0]) is not int
                or part["message_ids"][0] <= 0
                or part["message_ids"][0] in messages
                or datetime.fromisoformat(part["sent_at"]).utcoffset() is None
                or type(part["source_ref"]["values"]["part"]) is not int
                or part["source_ref"] != {"key": "work.request.presentation", "values": {"request_id": parent.request_id, "part": index}}
            ):
                raise ValueError()
            identities.add(identity)
            messages.add(part["message_ids"][0])
        if parts[-1].get("callback_group_id") != callback.group_id or parts[-1].get("stored_keyboard") is not True or type(callback.message_id) is not int or parts[-1]["message_ids"] != [callback.message_id]:
            raise ValueError()
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ProductSourceRetired() from None

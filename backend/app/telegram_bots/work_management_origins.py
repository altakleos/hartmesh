"""Saved-work previews retain the actual acknowledged opening gesture."""

from collections.abc import Mapping

from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


def origin_for(original, control):
    return {"callback_id": control.id, "anchor_outbox_id": original.id, "presentation_version": control.payload["presentation_version"], "signature": anchor_signature(original)}


def displayed_management(link, parent, control):
    original = acknowledged_view(link, parent, control.payload.get("presentation_version"))
    source = original.payload["work_news_basis"]["source"]
    data, final = source["work"], original.parts[-1]
    editing = source["key"] == "management.edit" and data["phase"] in {"draft", "unchanged"}
    withdrawing = source["key"] == "management.withdraw" and data["phase"] == "confirm"
    expected = ("draft", data.get("draft_id")) if editing else ("work", source["work_id"])
    if (
        not (editing or withdrawing)
        or original.payload["work_news_basis"]["concealed"]
        or (control.target_kind, control.target_id) != expected
        or control.payload.get("draft_id" if editing else "work_id") != expected[1]
        or control.kind not in ({"work_change_save", "assign_edit", "assign_discard"} if editing else {"work_withdraw_confirm", "back"})
        or final.get("callback_group_id") != control.group_id
        or final.get("message_ids") != [control.message_id]
        or control.message_thread_id != original.message_thread_id
    ):
        raise CallbackRefused()
    return original, source


class ManagementOpening:
    def __init__(self, link, item, intent):
        self.link, self.item, self.intent = link, item, intent
        self.origin = intent["origin"]

    def extend(self, query):
        self.column = len(query.column_descriptions)
        gesture, parent, control = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return (
            query.add_columns(gesture, parent, control)
            .join(gesture, gesture.id == self.origin["gesture_id"])
            .join(parent, parent.id == self.origin["anchor_outbox_id"])
            .outerjoin(control, control.group_id == gesture.payload["group_id"].as_string())
        )

    def check(self, rows):
        try:
            gesture, parent = rows[0][self.column : self.column + 2]
            members = list({row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}.values())
            selected = next(member for member in members if member.id == self.origin["callback_id"])
            original = acknowledged_view(self.link, parent, self.origin["presentation_version"])
            _validate_rows(original, members)
            expected = "work_change" if self.intent["kind"] == "edit" else "work_withdraw"
            if (
                not isinstance(self.origin, Mapping)
                or set(self.origin) != {"gesture_id", "callback_id", "anchor_outbox_id", "presentation_version", "signature", "group_id"}
                or origin_for(original, selected) != {key: self.origin[key] for key in ("callback_id", "anchor_outbox_id", "presentation_version", "signature")}
                or (gesture.link_id, gesture.chat_id, gesture.binding_generation, gesture.kind, gesture.target_kind, gesture.target_id, gesture.message_thread_id)
                != (self.link.id, self.link.chat_id, self.link.binding_generation, "work_management_gesture", "outbox", self.item.id, self.item.message_thread_id)
                or not same_body(gesture.payload, self.origin)
                or gesture.consumed_at is not None
                or (selected.kind, selected.target_kind, selected.target_id, selected.payload.get("work_id")) != (expected, "work", self.intent["work_id"], self.intent["work_id"])
                or selected.message_id != gesture.message_id
                or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id) != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref)
                or original.payload["work_news_basis"]["concealed"]
            ):
                raise ValueError()
        except (ValueError, TypeError, KeyError, AttributeError, StopIteration, CallbackRefused):
            raise ProductSourceRetired() from None

"""Original acknowledged Save consent remains qualified at domain commit."""

from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment
from app.telegram_bots.assignment_journals import exact_delegation
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_delegate_actions import delegate_source
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


def delegate_origin(act):
    try:
        source = delegate_source(act)
        value = thaw(act.continuation["delegate_origin"])
        if (
            source["origin_kind"] != "callback"
            or type(value) is not dict
            or set(value) != {"callback_id", "anchor_outbox_id", "presentation_version", "signature"}
            or value["callback_id"] != (source["parent_id"] or act.id)
            or any(type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]) for key, size in (("callback_id", 32), ("anchor_outbox_id", 32), ("signature", 64)))
            or type(value["presentation_version"]) is not int
            or value["presentation_version"] < 0
        ):
            raise ValueError()
        return value
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ProductSourceRetired() from None


class DelegationConsent:
    def __init__(self, link, act):
        self.link, self.act, self.origin = link, act, delegate_origin(act)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        control, parent, member = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(control, parent, member).join(control, control.id == self.origin["callback_id"]).join(parent, parent.id == self.origin["anchor_outbox_id"]).outerjoin(member, member.group_id == control.group_id)

    def check(self, rows, root, act, child=None):
        control, parent = rows[0][self.column : self.column + 2]
        members = list({row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}.values())
        try:
            exact_delegation(self.link, root, act, child)
            original, source = displayed_assignment(self.link, parent, control)
            _validate_rows(original, members)
            if (
                (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.instance_id, parent.work_id)
                != (self.link.id, self.link.owner_user_id, self.link.chat_id, self.link.binding_generation, self.link.agent_ref, None)
                or source["work"]["draft_id"] != root.id
                or not same_body(source["work"]["root"]["body"], root.body)
                or (control.id, control.kind) != (act.id, "assign_start" if delegate_source(act)["start"] else "assign_save")
                or consent_origin(original, control) != self.origin
                or not same_body(act.continuation.get("delegate_origin"), self.origin)
                or not same_body(root.continuation, {"version": 1, "assignment_admission": self.origin})
                or child is not None
                and not same_body(child.continuation.get("delegate_origin"), self.origin)
                or not members
                or any(member.message_id != control.message_id or member.payload.get("work_action_id") != act.id for member in members)
            ):
                raise ValueError()
        except (CallbackRefused, ValueError, KeyError, TypeError, AttributeError):
            raise ProductSourceRetired() from None

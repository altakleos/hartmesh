"""A real full-preview callback owns each saved-work management command."""

from collections.abc import Mapping

from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body, thaw
from app.telegram_bots.work_management_actions import command_source, same_binding
from app.telegram_bots.work_management_origins import displayed_management, origin_for
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


def management_origin(act):
    try:
        value = thaw(act.continuation["management_origin"])
        if (
            command_source(act)["origin_kind"] != "callback"
            or type(value) is not dict
            or set(value) != {"callback_id", "anchor_outbox_id", "presentation_version", "signature"}
            or value["callback_id"] != act.id
            or any(type(value[key]) is not str or len(value[key]) != size or any(ch not in "0123456789abcdef" for ch in value[key]) for key, size in (("callback_id", 32), ("anchor_outbox_id", 32), ("signature", 64)))
            or type(value["presentation_version"]) is not int
            or value["presentation_version"] < 0
        ):
            raise ValueError()
        return value
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ProductSourceRetired() from None


def edit_admission_for(act, reference):
    witness = {"version": 1, "act_id": act.id, "signature": reference["signature"]}
    if command_source(act)["origin_kind"] == "callback":
        witness.update(origin_kind="callback", origin=management_origin(act))
    return witness


def require_management_scope(link, act, intent):
    source = command_source(act)
    body = request_for(act.kind, act.body, identity=act.id)
    if (
        not same_binding(link, act)
        or (act.id, act.kind, act.work_id, act.request_id, act.message_id) != (intent["act_id"], intent["kind"], intent["work_id"], None, None)
        or (body.expected_revision, body.expected_assignment_revision) != (intent["expected_revision"], intent["expected_assignment_revision"])
        or act.operation_ids
        or source["origin_kind"] != "callback"
        or (source["draft_ref"] is None) != (intent["draft_id"] is None)
        or source["draft_ref"] is not None
        and source["draft_ref"]["id"] != intent["draft_id"]
    ):
        raise ProductSourceRetired()
    management_origin(act)


def management_receipt(act, event):
    body = request_for(act.kind, act.body, identity=act.id)
    if (
        act.state != "done"
        or act.reason is not None
        or event is None
        or (event.instance_id, event.work_id, event.operation_id, event.action, event.actor_kind, event.actor_id) != (act.instance_id, act.work_id, act.id, act.kind, "human", act.owner_user_id)
        or not same_body(event.request, act.body)
        or (event.revision, event.assignment_revision) != (body.expected_revision + 1, body.expected_assignment_revision + 1)
        or not isinstance(act.result, Mapping)
        or not isinstance(event.result, Mapping)
        or (event.result.get("id"), event.result.get("instance_id"), event.result.get("revision"), event.result.get("assignment_revision")) != (act.work_id, act.instance_id, event.revision, event.assignment_revision)
        or any(not same_body(act.result.get(key), event.result.get(key)) for key in ("id", "instance_id", "revision", "assignment_revision", "status", "objective", "success_criteria", "due_at"))
    ):
        raise ProductSourceRetired()


class ManagementConsent:
    def __init__(self, act, *, admitting=False):
        self.act, self.admitting, self.origin = act, admitting, management_origin(act)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        control, parent, member = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(control, parent, member).join(control, control.id == self.act.id).join(parent, parent.id == self.origin["anchor_outbox_id"]).outerjoin(member, member.group_id == control.group_id)

    def check(self, rows, *, allow_closed=False):
        try:
            link = rows[0][0]
            control, parent = rows[0][self.column : self.column + 2]
            members = list({row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}.values())
            original, source = displayed_management(link, parent, control)
            _validate_rows(original, members)
            data = source["work"]
            expected_intent = {**data["intent"], "act_id": self.act.id}
            require_management_scope(link, self.act, expected_intent)
            expected = "work_change_save" if self.act.kind == "edit" else "work_withdraw_confirm"
            if (
                control.kind != expected
                or origin_for(original, control) != self.origin
                or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.instance_id, parent.work_id)
                != (link.id, link.owner_user_id, link.chat_id, link.binding_generation, link.agent_ref, self.act.work_id)
                or parent.message_thread_id != control.message_thread_id
                or not members
                or any(member.message_id != control.message_id or member.payload.get("work_action_id") != (None if self.admitting else self.act.id) for member in members)
                or not allow_closed
                and any(member.consumed_at is not None for member in members)
            ):
                raise ValueError()
            if self.act.kind == "edit":
                from app.telegram_bots.assignment_sources import private_snapshot
                from app.telegram_bots.work_edit_drafts import snapshot_edit
                from deerflow.agent_instances.work_contract import WorkCommand

                root, ref = data["root"], command_source(self.act)["draft_ref"]
                snapshot_edit(link, private_snapshot(root))
                draft = root["body"]["draft"]
                expected_body = WorkCommand(
                    operation_id=self.act.id,
                    action="edit",
                    expected_revision=root["body"]["expected_revision"],
                    expected_assignment_revision=root["body"]["expected_assignment_revision"],
                    objective=draft["objective"],
                    success_criteria=draft["success_criteria"],
                    due_at=draft["due_at"],
                ).model_dump(mode="json", exclude_unset=True)
                if (
                    any(root[key] != getattr(self.act, key) for key in _ACT_SCOPE if key != "kind")
                    or (root["id"], root["continuation"]["edit_draft_source"]["signature"]) != (ref["id"], ref["signature"])
                    or root["operation_ids"]
                    or root["state"] != "draft"
                    or data["phase"] != "draft"
                    or not same_body(root["body"]["draft"], data["draft"])
                    or not same_body(expected_body, self.act.body)
                ):
                    raise ValueError()
        except (ValueError, TypeError, KeyError, AttributeError, CallbackRefused):
            raise ProductSourceRetired() from None


class ManagementModeWitness:
    """An independent preview reference survives loss of an actual callback."""

    def __init__(self, act, *, admitting=False):
        self.act, self.admitting = act, admitting
        self.origin = management_origin(act) if command_source(act)["origin_kind"] == "callback" else None

    def extend(self, query):
        self.column = len(query.column_descriptions)
        witness = aliased(TelegramOutboxRow)
        return query.add_columns(witness).outerjoin(witness, witness.payload["work_management_intent"]["act_id"].as_string() == self.act.id)

    def check(self, rows):
        expected = {self.origin["anchor_outbox_id"]} if self.origin is not None and not self.admitting else set()
        actual = {row[self.column].id for row in rows if row[self.column] is not None}
        if actual != expected:
            raise ProductSourceRetired()

"""Canonical stored suggestions and actual result receipts behind private drafts."""

from types import SimpleNamespace

from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_origins import CallbackAssignmentInput
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.result_notice_delivery import frozen_notice
from app.telegram_bots.work_actions import thaw
from app.telegram_bots.work_outcome_proof import event_facts
from app.telegram_bots.work_request_source import cleanup_ready, require_show
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


def acknowledged_result(link, parent):
    item = _item(parent)
    basis, source, _ = frozen_notice(SimpleNamespace(link=link, item=item, part=item.parts[-1]), acknowledged=True)
    if basis["concealed"] or basis.get("suggestion_controls") != 1:
        raise ProductSourceRetired()
    return item, source


def result_signature(item):
    return digest([item.id, item.payload["presentation_version"], item.payload["result_notice_basis"], item.payload["product_keyboard_basis"], item.parts])


def validate_origin(origin):
    if type(origin) is not dict or set(origin) != {
        "version",
        "kind",
        "link_id",
        "chat_id",
        "binding_generation",
        "message_id",
        "message_thread_id",
        "callback_id",
        "anchor_outbox_id",
        "presentation_version",
        "anchor_signature",
        "work_id",
        "event_id",
        "event_signature",
        "words",
    }:
        raise ProductSourceRetired()
    if type(origin["version"]) is not int or origin["version"] != 1 or origin["kind"] != "result_suggestion" or type(origin["words"]) is not str or not origin["words"].strip():
        raise ProductSourceRetired()
    for key in ("link_id", "callback_id", "anchor_outbox_id", "work_id", "event_id", "anchor_signature", "event_signature"):
        value = origin[key]
        if type(value) is not str or len(value) != (64 if key.endswith("signature") else 32) or any(char not in "0123456789abcdef" for char in value):
            raise ProductSourceRetired()
    if any(type(origin[key]) is not int for key in ("chat_id", "binding_generation", "message_id", "presentation_version")) or origin["message_thread_id"] is not None and type(origin["message_thread_id"]) is not int:
        raise ProductSourceRetired()
    if origin["message_id"] <= 0 or origin["presentation_version"] < 0 or origin["binding_generation"] < 0 or origin["message_thread_id"] is not None and origin["message_thread_id"] <= 0:
        raise ProductSourceRetired()
    return origin


class SuggestionOriginProjection:
    def __init__(self, link, origin, *, saved=None):
        self.link, self.origin, self.saved = link, validate_origin(thaw(origin)), saved

    def extend(self, query):
        parent, selected, control, event, attempt, work = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramCallbackRow), aliased(AgentWorkEventRow), aliased(AgentWorkAttemptRow), aliased(AgentWorkRow)
        show, cleanup = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        self.column = len(query.column_descriptions)
        return (
            query.add_columns(parent, selected, control, event, attempt, work, show, cleanup)
            .join(parent, parent.id == self.origin["anchor_outbox_id"])
            .join(selected, selected.id == self.origin["callback_id"])
            .outerjoin(control, control.group_id == selected.group_id)
            .join(event, event.id == self.origin["event_id"])
            .join(attempt, attempt.id == event.request["attempt_id"].as_string())
            .join(work, work.id == self.origin["work_id"])
            .outerjoin(show, show.id == parent.payload["result_notice_basis"]["reveal"].as_string())
            .outerjoin(cleanup, cleanup.id == parent.payload["result_notice_basis"]["cleanup_id"].as_string())
        )

    def check_with_permissions(self, rows, held):
        from app.telegram_bots.assignment_drafts import draft_identity

        parent, selected, _control, event, attempt, work = rows[0][self.column : self.column + 6]
        instance, permissions = rows[0][1:3]
        origin = self.origin
        if permissions & 3 != 3 or "agents:read" not in held:
            raise ProductSourceRetired()
        if (
            (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id, parent.work_id, parent.message_thread_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, origin["work_id"], origin["message_thread_id"])
            or (origin["link_id"], origin["chat_id"], origin["binding_generation"]) != (self.link.id, self.link.chat_id, self.link.binding_generation)
            or (event.instance_id, event.work_id, event.actor_kind, event.actor_id, event.action) != (self.link.agent_ref, work.id, "nonhuman", instance.principal_id, "suggest")
            or work.instance_id != self.link.agent_ref
            or attempt.work_id != work.id
            or event.assignment_revision != attempt.assignment_revision
            or event.request.get("statement") != origin["words"]
            or event.request.get("action") != "suggest"
            or event.result.get("id") != work.id
            or event.result.get("instance_id") != work.instance_id
            or digest(event_facts(event)) != origin["event_signature"]
        ):
            raise ProductSourceRetired()
        original, source = acknowledged_result(self.link, parent)
        parts = [part for part in original.parts if part["presentation_version"] == original.payload["presentation_version"]]
        if not parts or parts[-1].get("callback_group_id") != selected.group_id or parts[-1]["message_ids"] != [origin["message_id"]]:
            raise ProductSourceRetired()
        if original.payload["result_notice_basis"].get("reveal"):
            show, cleanup = rows[0][self.column + 6 : self.column + 8]
            require_show(show, parent)
            if not cleanup_ready(parent, cleanup) or cleanup.payload["target_message_id"] != show.message_id:
                raise ProductSourceRetired()
        suggestion = source.work.get("suggestion")
        if result_signature(original) != origin["anchor_signature"] or original.payload["presentation_version"] != origin["presentation_version"] or suggestion != {"id": origin["event_id"], "statement": origin["words"]}:
            raise ProductSourceRetired()
        controls = {row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}
        try:
            _validate_rows(original, list(controls.values()))
        except CallbackRefused:
            raise ProductSourceRetired() from None
        if (selected.kind, selected.target_kind, selected.target_id, selected.message_id, selected.message_thread_id, selected.payload.get("event_id")) != (
            "assign_from_suggestion",
            "work",
            work.id,
            origin["message_id"],
            origin["message_thread_id"],
            event.id,
        ):
            raise ProductSourceRetired()
        expected = draft_identity(self.link, CallbackAssignmentInput(origin))
        marker = selected.payload.get("suggestion_draft_id")
        if marker is not None and marker != expected or self.saved is not None and marker != expected:
            raise ProductSourceRetired()

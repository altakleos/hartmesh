"""A replacement preserves the actual edit gesture, prompt and typed input."""

from dataclasses import replace

from sqlalchemy import and_, case
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_edit_drafts import snapshot_edit
from app.telegram_bots.work_input_capture import accepted_text
from app.telegram_bots.work_management_edit_records import read_editor, read_incoming, read_input
from app.telegram_bots.work_management_origins import displayed_management, origin_for
from app.telegram_bots.work_management_prompt_sources import editor_from, prompt_intent
from app.telegram_bots.work_management_retention import closed_edit
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def replacements(item):
    values = item.payload.get("work_management_replacements", [])
    if type(values) is not list:
        raise ProductSourceRetired()
    previous, seen = None, set()
    for value in values:
        try:
            if type(value) is not dict or set(value) != {"version", "editor", "input", "prompt_outbox_id", "draft_id"} or type(value["version"]) is not int or value["version"] != 1:
                raise ValueError()
            editor = read_editor(value["editor"])
            read_incoming(value["input"])
            if type(editor) is not dict or set(editor) != {"version", "prompt_id", "draft_id", "origin"} or type(editor["version"]) is not int or editor["version"] != 1:
                raise ValueError()
            for identity in (value["draft_id"], value["prompt_outbox_id"], editor["draft_id"], editor["prompt_id"], editor["origin"]["callback_id"]):
                if type(identity) is not str or len(identity) != 32 or any(ch not in "0123456789abcdef" for ch in identity):
                    raise ValueError()
            if value["draft_id"] in seen or editor["draft_id"] == value["draft_id"] or previous is not None and editor["draft_id"] != previous:
                raise ValueError()
            seen.add(editor["draft_id"])
            previous = value["draft_id"]
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ProductSourceRetired() from None
    return values


class ManagementEditLineage:
    def __init__(self, link, item, intent):
        self.link, self.item, self.intent = link, item, intent
        self.values = replacements(item)

    def extend(self, query):
        if not self.values:
            return query
        self.column = len(query.column_descriptions)
        old, fresh, native, event, member, prompt, delivery, mark, selected = (
            aliased(TelegramActRow),
            aliased(TelegramActRow),
            aliased(TelegramActRow),
            aliased(AgentWorkEventRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramInboundMarkRow),
            aliased(TelegramCallbackRow),
        )

        def for_root(get):
            return case({value["editor"]["draft_id"]: get(value) for value in self.values}, value=old.id)

        return (
            query.add_columns(old, fresh, native, event, member, prompt, delivery, mark)
            .outerjoin(old, old.id.in_([value["editor"]["draft_id"] for value in self.values]))
            .outerjoin(fresh, fresh.id == for_root(lambda value: value["draft_id"]))
            .outerjoin(native, native.id == old.operation_ids[0].as_string())
            .outerjoin(event, and_(event.instance_id == self.link.agent_ref, event.operation_id == native.id))
            .outerjoin(selected, selected.id == for_root(lambda value: value["editor"]["origin"]["callback_id"]))
            .outerjoin(member, member.group_id == selected.group_id)
            .outerjoin(prompt, prompt.id == for_root(lambda value: value["editor"]["prompt_id"]))
            .outerjoin(delivery, delivery.id == for_root(lambda value: value["prompt_outbox_id"]))
            .outerjoin(mark, and_(mark.link_id == self.link.id, mark.chat_id == self.link.chat_id, mark.message_id == for_root(lambda value: value["input"]["message_id"])))
        )

    def check(self, rows, parent, root):
        initial = digest([self.item.dedupe_key, "draft"])[:32]
        self.closed_native_ids = set()
        if not self.values:
            if self.intent["kind"] == "edit" and self.intent["draft_id"] != initial:
                raise ProductSourceRetired()
            return
        try:
            if self.intent["kind"] != "edit" or self.values[0]["editor"]["draft_id"] != initial or self.values[-1]["draft_id"] != root.id:
                raise ValueError()
            by_id = {}
            for row in rows:
                old = row[self.column]
                if old is not None:
                    by_id.setdefault(old.id, []).append(row)
            if set(by_id) != {value["editor"]["draft_id"] for value in self.values}:
                raise ValueError()
            for value in self.values:
                joined = by_id[value["editor"]["draft_id"]]
                old, fresh, native, event, _member, prompt, delivery, mark = joined[0][self.column : self.column + 8]
                snapshot_edit(self.link, old)
                snapshot_edit(self.link, fresh)
                normalized = replace(_snapshot(old), state="draft", reason=None, result=None)
                closed = not old.operation_ids and set(old.continuation) == {"edit_draft_source"}
                if native is not None:
                    closed = (
                        native.state == "refused" and native.reason == "unchanged" and native.attempts == 0 and closed_edit(normalized, _snapshot(native), {(event.instance_id, event.operation_id): event} if event is not None else {}, {})
                    )
                    self.closed_native_ids.add(native.id)
                members = list({row[self.column + 4].id: row[self.column + 4] for row in joined if row[self.column + 4] is not None}.values())
                control = next(member for member in members if member.id == value["editor"]["origin"]["callback_id"])
                original, source = displayed_management(self.link, parent, control)
                _validate_rows(original, members)
                intent = prompt_intent(delivery)
                acknowledged, messages = shown_prompt(delivery, prompt, accepted_at=mark.accepted_at)
                incoming = InboxRow.from_row(mark)
                basis = mark.route_basis.get("management_edit")
                marker = read_input(prompt.payload.get("management_input"))
                if (
                    not closed
                    or old.state != "refused"
                    or old.reason != "replaced"
                    or not same_body(old.result, value)
                    or any(getattr(old, key) != getattr(fresh, key) for key in _ACT_SCOPE)
                    or any(not same_body(old.body[key], fresh.body[key]) for key in ("version", "original", "expected_revision", "expected_assignment_revision"))
                    or aware(old.created_at) != aware(fresh.created_at)
                    or control.kind != "assign_edit"
                    or control.target_id != old.id
                    or not same_body(origin_for(original, control), value["editor"]["origin"])
                    or not same_body(source["work"]["root"]["body"], old.body)
                    or any(member.consumed_at is None or member.closed_reason != "editing" for member in members)
                    or any(member.payload.get("work_action_id") != (native.id if native is not None and source["work"]["intent"]["act_id"] is None else None) for member in members)
                    or not same_body(editor_from(intent), value["editor"])
                    or intent["parent_id"] != parent.id
                    or mark.state == "interrupted"
                    or mark.route_kind != "management_edit"
                    or mark.route_target_id != fresh.id
                    or mark.operation_ids
                    or not same_body(origin_facts(incoming), value["input"])
                    or not same_body(basis, {"version": 1, "input": value["input"], "prompt_outbox_id": delivery.id, "intent": intent, "words": accepted_text(mark, prompt, acknowledged, message_ids=messages), "draft_id": fresh.id})
                    or digest(["management-edit", value["input"]])[:32] != fresh.id
                    or type(marker) is not dict
                    or set(marker) != {"version", "input_id", "phase", "claim_id"}
                    or marker["version"] != 1
                    or marker["input_id"] != fresh.id
                    or marker["phase"] != "completed"
                    or type(marker["claim_id"]) is not str
                    or len(marker["claim_id"]) != 32
                    or prompt.consumed_at is None
                    or prompt.closed_reason != "answered"
                ):
                    raise ValueError()
        except (ValueError, TypeError, KeyError, AttributeError, StopIteration, CallbackRefused):
            raise ProductSourceRetired() from None

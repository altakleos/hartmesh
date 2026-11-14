"""An edit prompt retains the actual saved-work preview and private copy."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_edit_drafts import snapshot_edit
from app.telegram_bots.work_management_edit_records import read_editor, read_input
from app.telegram_bots.work_management_origins import ManagementOpening, displayed_management, origin_for
from app.telegram_bots.work_management_retention import closed_edit
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def editor_for(item):
    return read_editor(item.payload.get("work_management_editor"))


class ManagementEditFrame:
    def __init__(self, reader, link, parent_id, editor, detail, *, opening_intent, topic, history, dedupe_key, opening=False, replacement=None):
        self.reader, self.link, self.parent_id, self.editor = reader, link, parent_id, editor
        self.opening, self.replacement = opening, replacement
        self.detail = WorkDetailProjection(reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail)
        self.origin = ManagementOpening(link, SimpleNamespace(id=parent_id, message_thread_id=topic), opening_intent)
        from app.telegram_bots.work_management_edit_lineage import ManagementEditLineage

        self.history = history
        self.lineage = ManagementEditLineage(link, SimpleNamespace(id=parent_id, dedupe_key=dedupe_key, payload={"work_management_replacements": history}), opening_intent)

    def extend(self, query):
        result = self.detail.extend(query)
        self.column = len(result.column_descriptions)
        parent, root, control, member, native, event, previous = (
            aliased(TelegramOutboxRow),
            aliased(TelegramActRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramActRow),
            aliased(AgentWorkEventRow),
            aliased(TelegramCallbackRow),
        )
        result = (
            result.add_columns(parent, root, control, member, native, event, previous)
            .join(parent, parent.id == self.parent_id)
            .join(root, root.id == self.editor["draft_id"])
            .join(control, control.id == self.editor["origin"]["callback_id"])
            .outerjoin(member, member.group_id == control.group_id)
            .outerjoin(native, native.id == root.operation_ids[0].as_string())
            .outerjoin(event, and_(event.instance_id == self.link.agent_ref, event.operation_id == native.id))
            .outerjoin(previous, previous.id == parent.payload["work_management_editor"]["prompt_id"].as_string())
        )
        return self.lineage.extend(self.origin.extend(result))

    def check_with_permissions(self, rows, held):
        self.detail.check_with_permissions(rows, held)
        self.origin.check(rows)
        parent, root, control, _member, native, event = rows[0][self.column : self.column + 6]
        try:
            original, source = displayed_management(self.link, parent, control)
            members = list({row[self.column + 3].id: row[self.column + 3] for row in rows if row[self.column + 3] is not None}.values())
            _validate_rows(original, members)
            captured = source["work"]["root"]
            snapshot_edit(self.link, root)
            replaced = self.replacement is not None and root.state == "refused" and root.reason == "replaced" and same_body(root.result, self.replacement)
            normalized = replace(_snapshot(root), state="draft", reason=None, result=None) if replaced else _snapshot(root)
            self.lineage.check(rows, parent, root)
            if not same_body(parent.payload.get("work_management_replacements", []), [*self.history, *([self.replacement] if replaced else [])]):
                raise ValueError()
            allowed = not root.operation_ids and set(root.continuation) == {"edit_draft_source"}
            if native is not None:
                allowed = closed_edit(normalized, _snapshot(native), {(event.instance_id, event.operation_id): event} if event is not None else {}, {}) and native.state == "refused" and native.reason == "unchanged" and native.attempts == 0
            active = editor_for(parent)
            if (
                rows[0][2] & 6 != 6
                or "agents:write" not in held
                or control.kind != "assign_edit"
                or source["key"] != "management.edit"
                or control.target_id != root.id
                or origin_for(original, control) != self.editor["origin"]
                or any(getattr(root, key) != captured[key] for key in _ACT_SCOPE)
                or not same_body(root.body, captured["body"])
                or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id, parent.work_id, parent.class_)
                != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, root.work_id, "reply")
                or control.message_thread_id != parent.message_thread_id
                or not allowed
                or not (root.state == "draft" or replaced)
                or aware(self.reader.clock()) >= aware(root.created_at) + timedelta(hours=self.reader.config.callbacks.draft_ttl_hours)
                or any(member.payload.get("work_action_id") != (native.id if native is not None and source["work"]["intent"]["act_id"] is None else None) for member in members)
            ):
                raise ValueError()
            previous = rows[0][self.column + 6]
            marker = read_input(previous.payload.get("management_input")) if previous is not None else None
            settled_previous = (
                self.opening
                and active is not None
                and not same_body(active, self.editor)
                and same_body(source["work"].get("editor"), active)
                and previous is not None
                and previous.id == active["prompt_id"]
                and previous.kind == "prompt"
                and previous.payload.get("purpose") == "work_edit"
                and previous.payload.get("draft_id") == active["draft_id"]
                and isinstance(marker, Mapping)
                and marker.get("phase") == ("failed" if active["draft_id"] == root.id else "completed")
                and previous.consumed_at is not None
                and previous.closed_reason == "answered"
            )
            if self.opening and (active is None or settled_previous):
                if any(member.consumed_at is not None for member in members):
                    raise ValueError()
            elif not same_body(active, self.editor) or any(member.consumed_at is None or member.closed_reason != "editing" for member in members):
                if not replaced or not any(same_body(value, self.replacement) for value in parent.payload.get("work_management_replacements", [])):
                    raise ValueError()
            self.root, self.parent, self.members = root, parent, members
        except (ValueError, TypeError, KeyError, AttributeError, CallbackRefused):
            raise ProductSourceRetired() from None

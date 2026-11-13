"""A preference choice belongs to the actual delivered control-owning part."""

from datetime import datetime

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def require_settings_group(writer, group, *, acknowledged=True):
    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, with_for_update=True, populate_existing=True)
    rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.selected.group_id).with_for_update().execution_options(populate_existing=True)))
    validate_settings_group(parent, rows, group, acknowledged=acknowledged)


def validate_settings_group(parent, rows, group, *, acknowledged=True):
    if parent is None or type(parent.payload.get("presentation_version", 0)) is not int or parent.payload.get("presentation_version", 0) != group.presentation_version:
        raise CallbackRefused()
    parts = [part for part in parent.parts if type(part.get("presentation_version")) is int and part.get("presentation_version") == group.presentation_version and part.get("callback_group_id") == group.selected.group_id]
    if len(parts) != 1:
        raise CallbackRefused()
    part = parts[0]
    _validate_rows(parent, rows)
    originals = {row.id: row for row in group.rows}
    fields = ("link_id", "chat_id", "binding_generation", "message_thread_id", "message_id", "kind", "target_kind", "target_id")
    if set(originals) != {row.id for row in rows} or any(any(getattr(row, name) != getattr(originals[row.id], name) for name in fields) or not same_body(row.payload, originals[row.id].payload) for row in rows):
        raise CallbackRefused()
    if any(row.message_id != group.selected.message_id for row in rows):
        raise CallbackRefused()
    if not acknowledged:
        return
    try:
        if (
            parent.state != "sent"
            or claim_of(parent) != (None, None, None)
            or part["state"] != "sent"
            or type(part["message_ids"]) is not list
            or len(part["message_ids"]) != 1
            or type(part["message_ids"][0]) is not int
            or part["message_ids"][0] <= 0
            or part["message_ids"] != [group.selected.message_id]
            or datetime.fromisoformat(part["sent_at"]).utcoffset() is None
            or part.get("stored_keyboard") is not True
            or part.get("source_ref", {}).get("key") not in {"settings.state", "settings.state_basic", "settings.messages_choose", "settings.quiet_choose", "settings.summary_choose", "settings.language_choose"}
        ):
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None


class SettingsGroupProjection:
    def __init__(self, group):
        self.group = group

    def extend(self, query):
        return (
            query.add_columns(TelegramOutboxRow, TelegramCallbackRow)
            .outerjoin(TelegramOutboxRow, TelegramOutboxRow.id == self.group.anchor_outbox_id)
            .outerjoin(TelegramCallbackRow, TelegramCallbackRow.group_id == self.group.selected.group_id)
        )

    def check(self, rows):
        controls = {row[6].id: row[6] for row in rows if row[6] is not None}
        validate_settings_group(rows[0][5], list(controls.values()), self.group)


class SettingsGroupGuard:
    def __init__(self, base, callbacks):
        self.base, self.group, self.callbacks = base, None, callbacks
        self.captured_link = base.captured_link

    async def prepare(self):
        await self.base.prepare()

    def current(self):
        self.base.current()

    async def check(self, writer):
        if self.group is None:
            return await self.base.check(writer)
        await require_settings_group(writer, self.group)
        current = await self.base.check_projection(writer, SettingsGroupProjection(self.group))
        self.callbacks._unexpired(self.group.selected)
        self.current()
        return current

"""Actual callback groups attached to coalesced status update receipts."""

from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


def presentation(source):
    from app.telegram_bots.work_news import present_news
    from app.telegram_bots.work_status import WorkStatusRead
    from app.telegram_bots.work_status_controls import present_status_controls
    from app.telegram_bots.work_status_delivery import notice
    from app.telegram_bots.work_view_sources import detail_read

    status = WorkStatusRead(**source["status"])
    view = present_news(status.key, values=status.values, locale=source["locale"], protected=status.protected)
    if "detail" not in source:
        return view
    current = notice(status, SimpleNamespace(work_id=source["work_id"]), detail=detail_read(source["detail"]), dates=source["detail_dates"])
    return present_status_controls(current, {"locale": source["locale"], "concealed": False}, SimpleNamespace(owner_user_id=source["owner_user_id"]), view)


async def stage(writer, link, row, callbacks):
    source = row.payload["work_status_update"]["source"]
    keyboard = canonical_keyboard(presentation(source).keyboard)
    row.payload = {**row.payload, "product_keyboard_basis": keyboard}
    if keyboard:
        version = row.payload["presentation_version"]
        group = await callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version, chat_id=row.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=writer)
        row.parts = [*row.parts[:-1], {**row.parts[-1], "callback_group_id": group.group_id}]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
            control.payload = {**control.payload, "presentation_pending": True}


async def retire(writer, row, now, *, groups=None):
    groups = {part.get("callback_group_id") for part in row.parts} - {None} if groups is None else groups
    if not groups:
        return
    for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups), TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == row.id)):
        if (control.link_id, control.chat_id, control.binding_generation, control.message_thread_id) != (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id):
            raise ProductSourceRetired()
        if control.consumed_at is None:
            control.consumed_at, control.closed_reason = now, "superseded"

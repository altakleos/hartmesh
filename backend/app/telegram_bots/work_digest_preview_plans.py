"""Persist source-owned digest previews using their existing family recipes."""

from dataclasses import asdict

from sqlalchemy import select

from app.telegram_bots.producer import ProductSourceRetired, _safe_json
from app.telegram_bots.work_navigation_origin import validate_origin
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


async def freeze_notice(owner, writer, link, row, source, settings, origin, *, family):
    basis = {"version": 1, "source": asdict(source), "locale": settings.locale, "concealed": False, "navigation_origin": validate_origin(origin)}
    if family == "work_news_basis":
        from app.telegram_bots.work_news_delivery import recipes
        from app.telegram_bots.work_result_navigation import NavigationOrigin

        basis["public_base_url"] = NavigationOrigin().base
        parts, keyboard = recipes(source, basis, row.id, link)
    elif family == "result_notice_basis":
        from app.telegram_bots.result_notice_delivery import recipes

        basis["result_files"] = {"version": 1, "capacity": owner.outbox.config.files.in_memory_max_bytes, "max_bytes": owner.outbox.config.files.max_outbound_bytes}
        parts, keyboard = recipes(source, basis, row.id)
        row.payload = {**row.payload, "work_result_identity": source.identity, "work_result_navigation": source.navigation}
    else:
        raise ProductSourceRetired()
    row.payload = _safe_json({**row.payload, family: {**basis, "parts": parts, "keyboard": keyboard}, "product_keyboard_basis": keyboard})
    row.parts = owner.outbox._parts(parts, 0)
    if keyboard:
        group = await owner.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
        row.parts = [*row.parts[:-1], {**row.parts[-1], "callback_group_id": group.group_id}]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
            control.payload = {**control.payload, "presentation_pending": True}
    if family == "result_notice_basis":
        from app.telegram_bots.work_result_ledger import initial_ledger

        row.payload = {**row.payload, "work_result_ledger": initial_ledger(row.parts)}
    await writer.flush()


def frozen_preview(captured, *, pending=False, acknowledged=False, replay=False):
    families = {"work_request_basis", "work_news_basis", "result_notice_basis"}.intersection(captured.item.payload)
    if len(families) != 1:
        raise ProductSourceRetired()
    family = next(iter(families))
    if family == "work_request_basis":
        from app.telegram_bots.work_request_source import frozen

        return frozen(captured, pending=pending, acknowledged=acknowledged, replay=replay)
    if family == "work_news_basis":
        from app.telegram_bots.work_news_delivery import frozen_news

        return frozen_news(captured, pending=pending, acknowledged=acknowledged)
    from app.telegram_bots.result_notice_delivery import frozen_notice

    return frozen_notice(captured, pending=pending, acknowledged=acknowledged)

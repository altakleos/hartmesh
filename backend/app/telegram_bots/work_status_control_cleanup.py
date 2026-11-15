"""A complete replacement ACK owns cleanup of its actual prior keyboard."""

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import BotLink
from app.telegram_bots.producer import ProductSourceRetired, keyboard_cleanup_row
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


def scope(row):
    return row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.message_thread_id


async def plan(writer, row, root, prior, basis, now):
    from app.telegram_bots.work_status_updates import anchor

    if prior.id == row.id or not any(part["kind"] == "text" and part["presentation_version"] == row.payload["presentation_version"] for part in row.parts):
        return
    binding = await writer.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    link = BotLink.from_row(binding)
    target = anchor(link, root, basis["source"], prior if prior.id != root.id else None, replacement=basis["anchor"]["replace"])
    if not same_body(target, basis["anchor"]) or scope(prior) != scope(row):
        raise ProductSourceRetired()
    original = acknowledged_view(link, prior, prior.payload["presentation_version"])
    final = original.parts[-1]
    group = final.get("callback_group_id")
    if not group:
        return
    controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
    try:
        _validate_rows(original, controls)
    except CallbackRefused:
        raise ProductSourceRetired() from None
    if not controls or any(control.message_id != final["message_ids"][0] for control in controls):
        raise ProductSourceRetired()
    if "status_control_cleanup" in row.payload:
        raise ProductSourceRetired()
    cleanup = keyboard_cleanup_row(controls[0], owner_user_id=row.owner_user_id, now=now, dedupe_key=f"notice:status-keyboard:{row.id}:{row.payload['presentation_version']}")
    previous = {"outbox_id": prior.id, "presentation_version": original.payload["presentation_version"], "signature": anchor_signature(original)}
    cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id, "status_previous": previous}
    writer.add(cleanup)
    row.payload = {**row.payload, "status_control_cleanup": {"version": 1, "outbox_id": cleanup.id, "previous": previous, "target_message_id": controls[0].message_id}}


async def dependencies(writer):
    parent, cleanup = TelegramOutboxRow, TelegramOutboxRow.__table__.alias("status_keyboard_cleanup")
    # Keep the actual own-scope plan independently of optional prior proof;
    # malformed locators cannot fan out to another owner's delivery graph.
    pairs = (await writer.execute(select(parent, cleanup.c.id).join(cleanup, cleanup.c.id == parent.payload["status_control_cleanup"]["outbox_id"].as_string()).where(cleanup.c.state.in_(("pending", "sending", "failed"))))).all()
    held = set()
    for row, cleanup_id in pairs:
        item = await writer.get(TelegramOutboxRow, cleanup_id)
        if item is None or scope(item) != scope(row) or item.payload.get("kind") != "keyboard_cleanup" or item.payload.get("work_request_parent_id") != row.id:
            continue
        held.update((row.id, item.id))
        try:
            saved = row.payload["status_control_cleanup"]
            prior = await writer.get(TelegramOutboxRow, saved["previous"]["outbox_id"])
            binding = await writer.get(TelegramBotLinkRow, row.link_id)
            if prior is None or binding is None or scope(prior) != scope(row) or prior.work_id != row.work_id or not same_body(item.payload.get("status_previous"), saved["previous"]):
                continue
            original = acknowledged_view(BotLink.from_row(binding), prior, saved["previous"]["presentation_version"])
            if anchor_signature(original) == saved["previous"]["signature"] and original.parts[-1]["message_ids"] == [saved["target_message_id"]] and item.payload.get("target_message_id") == saved["target_message_id"]:
                held.add(prior.id)
        except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
            continue
    return held

"""Known unselected Stop choices retire without creating a native action."""

from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.callbacks import _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_command_stop_origins import command_basis, operation_id, source_marker
from app.telegram_bots.work_news_delivery import frozen_news
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def unselected_closed(mark, basis, now, max_age_days):
    value = mark.route_basis.get("stop_command_closed")
    try:
        return (
            basis["selection"] is None
            and not mark.operation_ids
            and mark.route_complete
            and type(value) is dict
            and set(value) == {"version", "reason", "at"}
            and type(value["version"]) is int
            and value["version"] == 1
            and value["reason"] == "expired"
            and datetime.fromisoformat(value["at"]).utcoffset() is not None
            and aware(mark.accepted_at) + timedelta(days=max_age_days) <= datetime.fromisoformat(value["at"]) <= now
        )
    except (TypeError, ValueError, KeyError):
        return False


async def expire_unselected(owner, link, origin, *, guard):
    now, maximum = owner.outbox.clock(), owner.outbox.config.callbacks.max_age_days
    if now < aware(origin.accepted_at) + timedelta(days=maximum):
        return False
    async with owner.callbacks.writer(link, guard) as writer:
        mark = (await owner.outbox.inbox._rows(writer, [origin]))[0]
        owner.outbox.inbox._current(link, mark)
        basis = command_basis(mark)
        if basis["selection"] is not None:
            return False
        candidates = list(
            await writer.scalars(
                select(TelegramActRow).where(
                    TelegramActRow.link_id == link.id, TelegramActRow.chat_id == origin.chat_id, TelegramActRow.binding_generation == link.binding_generation, TelegramActRow.continuation["command_stop_source"].as_string().is_not(None)
                )
            )
        )
        for act in candidates:
            marker = source_marker(act)
            if marker is None or not isinstance(marker.get("basis"), dict) or "input" not in marker["basis"]:
                raise ProductSourceRetired()
            if same_body(marker["basis"]["input"], basis["input"]):
                raise ProductSourceRetired()
        notices = list(
            await writer.scalars(
                select(TelegramOutboxRow).where(
                    TelegramOutboxRow.link_id == link.id,
                    TelegramOutboxRow.owner_user_id == link.owner_user_id,
                    TelegramOutboxRow.binding_generation == link.binding_generation,
                    TelegramOutboxRow.chat_id == origin.chat_id,
                    TelegramOutboxRow.message_thread_id == origin.message_thread_id,
                    TelegramOutboxRow.payload["work_execution_intent"]["origin"]["kind"].as_string() == "command_stop",
                )
            )
        )
        if any(same_body(row.payload["work_execution_intent"]["origin"].get("input"), basis["input"]) for row in notices):
            raise ProductSourceRetired()
        parent = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:command-stop:" + digest(basis["input"])).with_for_update())
        if parent is not None:
            if (parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id, parent.instance_id) != (link.owner_user_id, link.binding_generation, origin.chat_id, origin.message_thread_id, basis["instance_id"]):
                raise ProductSourceRetired()
            if any(getattr(parent, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
                return False
            if parent.parts:
                item = _item(parent)
                _frozen, source, _part = frozen_news(SimpleNamespace(link=link, item=item, part=None), acknowledged=parent.state == "sent")
                if source.key != "command.stop" or not same_body(source.work["basis"], basis):
                    raise ProductSourceRetired()
                group_id = parent.parts[-1].get("callback_group_id")
                controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id).order_by(TelegramCallbackRow.id).with_for_update()))
                _validate_rows(item, controls, allow_pending=parent.state != "sent")
                if any(row.consumed_at is not None for row in controls):
                    raise ProductSourceRetired()
                shown = next((row for row in controls if row.message_id is not None), None)
                if shown is not None:
                    cleanup = keyboard_cleanup_row(shown, owner_user_id=link.owner_user_id, now=now)
                    writer.add(cleanup)
                    await writer.flush()
                    await owner.outbox.inbox.register_delivery([InboxRow.from_row(mark)], outbox_id=cleanup.id, finish=False, session=writer)
                for row in controls:
                    row.consumed_at, row.closed_reason = now, "command-expired"
            if parent.state != "sent":
                parent.parts = [{**part, "state": "retired"} if part["state"] in {"pending", "sending"} else part for part in parent.parts]
                parent.state, parent.skip_reason, parent.closed_at = "skipped", "command-expired", now
        mark.route_basis = {**mark.route_basis, "stop_command_closed": {"version": 1, "reason": "expired", "at": now.isoformat()}}
        await owner.outbox.inbox.finish_route(InboxRow.from_row(mark), session=writer)
        await writer.flush()
        guard.current()
    return True


async def expire_retired_unselected(session, now, *, max_age_days):
    """Retire proven no-effect expired choices without requiring live authority."""
    from app.telegram_bots.work_command_stop_choices import choice_intent
    from app.telegram_bots.work_command_stop_retention import _scope

    candidates = list(
        await session.execute(
            select(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
            .where(TelegramInboundMarkRow.route_kind == "command_stop", TelegramInboundMarkRow.route_complete.is_(False), TelegramInboundMarkRow.accepted_at <= now - timedelta(days=max_age_days))
            .order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
        )
    )
    for identity in candidates:
        try:
            # Match sender/ACK order: Link, Outbox, then Input. Discovery
            # owns no row locks, including when several retired links exist.
            link = await session.get(TelegramBotLinkRow, identity[0], with_for_update=True, populate_existing=True)
            mark = await session.get(TelegramInboundMarkRow, tuple(identity), populate_existing=True)
            if mark is None:
                continue
            basis = command_basis(mark)
            if basis["selection"] is not None or mark.operation_ids:
                continue
            if link is not None and (link.state, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref) == ("connected", basis["owner_user_id"], mark.binding_generation, mark.chat_id, basis["instance_id"]):
                continue
            identities = [operation_id(basis["input"], target) for target in basis["targets"]]
            if await session.scalar(select(TelegramActRow.id).where(TelegramActRow.id.in_(identities)).limit(1)) is not None:
                continue
            # A command-origin notice without its journal remains unknown.
            notices = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == mark.link_id, TelegramOutboxRow.payload["work_execution_intent"]["origin"]["kind"].as_string() == "command_stop")))
            if any(same_body(row.payload["work_execution_intent"]["origin"].get("input"), basis["input"]) for row in notices):
                continue
            parent = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == mark.link_id, TelegramOutboxRow.dedupe_key == "notice:command-stop:" + digest(basis["input"])).with_for_update())
            mark = await session.get(TelegramInboundMarkRow, tuple(identity), with_for_update=True, populate_existing=True)
            if mark is None or mark.route_complete or not same_body(command_basis(mark), basis):
                continue
            if set(mark.delivery_ids) != ({parent.id} if parent is not None else set()):
                continue
            if parent is not None:
                if not _scope(parent, mark, basis) or any(getattr(parent, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
                    continue
                item = _item(parent)
                if not same_body(choice_intent(item), {"version": 1, "message_id": mark.message_id, "chat_id": mark.chat_id, "signature": digest(basis)}):
                    continue
                controls = []
                if parent.parts:
                    # This scope renders historical recipes only; it cannot
                    # authorize an SDK call or a native mutation.
                    _frozen, source, _part = frozen_news(SimpleNamespace(link=SimpleNamespace(id=mark.link_id), item=item, part=None), acknowledged=parent.state == "sent")
                    if source.key != "command.stop" or not same_body(source.work["basis"], basis):
                        continue
                    controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1].get("callback_group_id")).with_for_update()))
                    _validate_rows(item, controls, allow_pending=parent.state != "sent")
                    if any(not _scope(row, mark, basis) or row.consumed_at is not None for row in controls):
                        continue
                for control in controls:
                    control.consumed_at, control.closed_reason = now, "command-expired"
                if parent.state != "sent":
                    parent.parts = [{**part, "state": "retired"} if part["state"] in {"pending", "sending"} else part for part in parent.parts]
                    parent.state, parent.skip_reason, parent.closed_at = "skipped", "command-expired", now
            mark.route_basis = {**mark.route_basis, "stop_command_closed": {"version": 1, "reason": "expired", "at": now.isoformat()}}
            mark.route_complete, mark.state, mark.closed_at = True, "done", now
        except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
            continue
    await session.flush()

"""Retain actual ownership receipts even while damaged source proof is repaired."""

from sqlalchemy import or_, select

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def scope(row):
    return row.link_id, row.chat_id, row.binding_generation, row.message_thread_id


def family(parent):
    if not isinstance(parent.payload, dict):
        return None
    values = [value for key in ("assignment_prompt_intent", "management_prompt_intent") if isinstance((value := parent.payload.get(key)), dict)]
    return values[0] if len(values) == 1 else None


async def source_history(session, root, original, typing, closed, result):
    source_id = typing.get("source_outbox_id")
    if not isinstance(source_id, str) or source_id != closed.get("source_outbox_id") or source_id not in original.delivery_ids:
        return None
    source = await session.get(TelegramOutboxRow, source_id)
    intent = family(source) if source is not None else None
    if source is None or intent is None or scope(source) != scope(root) or source.owner_user_id != root.owner_user_id or source.instance_id != root.instance_id or intent.get("prompt_id") != typing.get("prompt_id"):
        return None
    result.deliveries.add(source.id)
    prompt = await session.get(TelegramCallbackRow, typing.get("prompt_id")) if isinstance(typing.get("prompt_id"), str) else None
    if prompt is not None and scope(prompt) == scope(root) and prompt.kind == "prompt" and prompt.payload.get("anchor_outbox_id") == source.id:
        result.callbacks.add(prompt.id)
    ancestor = intent.get("origin")
    if isinstance(ancestor, dict) and (ancestor.get("link_id"), ancestor.get("chat_id"), ancestor.get("binding_generation"), ancestor.get("message_thread_id")) == scope(root):
        command = await session.get(TelegramInboundMarkRow, (root.link_id, ancestor.get("message_id"), root.chat_id)) if type(ancestor.get("message_id")) is int else None
        if command is not None and source.id in command.delivery_ids and same_body(origin_facts(command), ancestor):
            result.inputs.add((command.link_id, command.message_id, command.chat_id))
    return source.id


async def active_owner(session, parent, now):
    prompts = list(
        await session.scalars(
            select(TelegramCallbackRow).where(
                TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent.id,
                TelegramCallbackRow.kind == "prompt",
                TelegramCallbackRow.link_id == parent.link_id,
                TelegramCallbackRow.chat_id == parent.chat_id,
                TelegramCallbackRow.binding_generation == parent.binding_generation,
                TelegramCallbackRow.message_thread_id.is_not_distinct_from(parent.message_thread_id),
            )
        )
    )
    active = parent.state in {"pending", "sending", "failed"} or any(prompt.consumed_at is None and (prompt.expires_at is None or aware(prompt.expires_at) > aware(now)) for prompt in prompts)
    if not active:
        for mark in await session.scalars(
            select(TelegramInboundMarkRow).where(
                TelegramInboundMarkRow.link_id == parent.link_id,
                TelegramInboundMarkRow.chat_id == parent.chat_id,
                TelegramInboundMarkRow.binding_generation == parent.binding_generation,
                TelegramInboundMarkRow.message_thread_id.is_not_distinct_from(parent.message_thread_id),
                TelegramInboundMarkRow.route_complete.is_(False),
                TelegramInboundMarkRow.state != "done",
            )
        ):
            if parent.id not in mark.delivery_ids:
                continue
            if mark.route_kind in {"assignment_hold", "work_card_hold"} and mark.route_target_id == parent.id:
                active = True
    return active, prompts


async def pin_owned_history(session, now, result):
    """Primary receipts and registered deliveries are independent of Reopen JSON."""
    mark, parent = TelegramInboundMarkRow, TelegramOutboxRow
    pairs = list(
        (
            await session.execute(
                select(parent, mark)
                .join(mark, mark.outbox_id == parent.id)
                .where(
                    mark.route_kind == "work_card_hold",
                    mark.route_complete.is_(True),
                    mark.thread_id.is_(None),
                    mark.turn_id.is_(None),
                    or_(parent.payload["assignment_prompt_intent"].is_not(None), parent.payload["management_prompt_intent"].is_not(None)),
                )
            )
        ).all()
    )
    by_parent = {parent.id: (parent, mark) for parent, mark in pairs}

    async def hold_family(root, receipt):
        if not isinstance(receipt.route_basis.get("closed_typing"), dict):
            return
        link = await session.get(TelegramBotLinkRow, root.link_id)
        if link is None or (root.owner_user_id, root.binding_generation, root.chat_id, root.instance_id) != (link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref):
            return
        active, prompts = await active_owner(session, root, now)
        if not active:
            return
        result.callbacks.update(prompt.id for prompt in prompts)
        pending, visited = [(root, receipt)], set()
        while pending:
            current, original = pending.pop()
            if current.id in visited or scope(current) != scope(root) or scope(original) != scope(root) or current.owner_user_id != link.owner_user_id or current.instance_id != link.agent_ref or original.operation_ids:
                continue
            visited.add(current.id)
            result.deliveries.add(current.id)
            result.inputs.add((original.link_id, original.message_id, original.chat_id))
            typing, closed = original.route_basis.get("closed_typing"), original.route_basis.get("work_closed")
            if not isinstance(typing, dict) or typing.get("version") != 1 or not isinstance(closed, dict):
                continue
            source_id = await source_history(session, root, original, typing, closed, result)
            if source_id in by_parent:
                pending.append(by_parent[source_id])
            cleanup = await session.get(TelegramOutboxRow, current.payload.get("reopen_cleanup_id")) if isinstance(current.payload.get("reopen_cleanup_id"), str) else None
            if cleanup is not None and scope(cleanup) == scope(root) and cleanup.owner_user_id == link.owner_user_id and cleanup.payload.get("kind") == "keyboard_cleanup" and cleanup.payload.get("work_request_parent_id") == current.id:
                result.deliveries.add(cleanup.id)
            if "voice_note" in original.route_basis:
                echo = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == original.link_id, TelegramOutboxRow.dedupe_key == f"notice:voice-heard:{original.chat_id}:{original.message_id}"))
                if echo is not None and echo.id in original.delivery_ids and scope(echo) == scope(root) and echo.owner_user_id == link.owner_user_id and echo.class_ == "reply":
                    result.deliveries.add(echo.id)
            context = await session.get(TelegramCallbackRow, closed.get("prompt_id")) if isinstance(closed.get("prompt_id"), str) else None
            if (
                context is not None
                and scope(context) == scope(root)
                and context.kind == "prompt"
                and context.payload.get("purpose") == "retired_presentation"
                and context.payload.get("anchor_outbox_id") == typing.get("source_outbox_id")
                and context.payload.get("closed_input") == {"message_id": original.message_id, "update_id": original.update_id}
            ):
                result.callbacks.add(context.id)
            notice = await session.get(TelegramOutboxRow, closed.get("notice_id")) if isinstance(closed.get("notice_id"), str) else None
            if (
                notice is None
                or notice.id not in original.delivery_ids
                or scope(notice) != scope(root)
                or notice.owner_user_id != link.owner_user_id
                or notice.payload.get("product_basis", {}).get("origins") != [[original.link_id, original.message_id, original.chat_id]]
            ):
                continue
            result.deliveries.add(notice.id)
            if type(notice.message_id) is int and notice.message_id > 0 and type(notice.payload.get("presentation_version")) is int:
                rows = await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == notice.id))
                result.callbacks.update(
                    row.id
                    for row in rows
                    if scope(row) == scope(root) and row.message_id == notice.message_id and row.payload.get("presentation_version") == notice.payload["presentation_version"] and row.payload.get("presentation_pending") is False
                )

    for root, receipt in pairs:
        try:
            await hold_family(root, receipt)
        except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
            continue

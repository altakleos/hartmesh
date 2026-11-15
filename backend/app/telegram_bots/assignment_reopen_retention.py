"""Keep only verified history behind an unresolved reopened typing prompt."""

from dataclasses import dataclass, field

from sqlalchemy import or_, select

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.assignment_prompt_reopen import AssignmentReopenProof, stored_prompt_intent
from app.telegram_bots.assignment_typing import validate_prompt
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import BotLink
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_request_source import _RequestProjection
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@dataclass
class ReopenDependencies:
    callbacks: set = field(default_factory=set)
    deliveries: set = field(default_factory=set)
    inputs: set = field(default_factory=set)


async def reopen_dependencies(session, now):
    result = ReopenDependencies()
    from app.telegram_bots.closed_prompt_history import pin_owned_history

    await pin_owned_history(session, now, result)
    candidates = await session.scalars(
        select(TelegramOutboxRow).where(or_(TelegramOutboxRow.payload["assignment_prompt_intent"]["version"].as_integer() == 2, TelegramOutboxRow.payload["management_prompt_intent"]["version"].as_integer() == 2))
    )
    for parent in candidates:
        try:
            intent = stored_prompt_intent(_item(parent))
            raw_link = await session.get(TelegramBotLinkRow, parent.link_id)
            if raw_link is None:
                continue
            link = BotLink.from_row(raw_link)
            if (parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.instance_id) != (link.owner_user_id, link.chat_id, link.binding_generation, link.agent_ref):
                continue
            prompt = await session.get(TelegramCallbackRow, intent["prompt_id"])
            active = parent.state in {"pending", "sending", "failed"}
            if prompt is not None:
                validate_prompt(_item(parent), prompt, attached=True)
                active |= prompt.consumed_at is None and (prompt.expires_at is None or aware(prompt.expires_at) > aware(now))
            marks = list(
                await session.scalars(
                    select(TelegramInboundMarkRow).where(
                        TelegramInboundMarkRow.link_id == link.id,
                        TelegramInboundMarkRow.chat_id == link.chat_id,
                        TelegramInboundMarkRow.binding_generation == link.binding_generation,
                        TelegramInboundMarkRow.message_thread_id.is_not_distinct_from(parent.message_thread_id),
                        TelegramInboundMarkRow.state != "done",
                    )
                )
            )
            for mark in marks:
                if mark.route_complete or parent.id not in mark.delivery_ids:
                    continue
                if mark.route_kind == "assignment_hold":
                    from app.telegram_bots.assignment_input_ownership import typed_basis

                    held = typed_basis(mark)
                elif mark.route_kind == "work_card_hold":
                    held = mark.route_basis.get("closed_typing", {})
                    if held.get("version") != 1 or held.get("ambiguous") is not False:
                        continue
                else:
                    continue
                if held is not None and (held.get("source_outbox_id"), held.get("prompt_id")) == (parent.id, intent["prompt_id"]):
                    active = True
            if not active:
                continue
            # A damaged locator may hold its own publication, never arbitrary
            # records named by unqualified JSON.
            result.deliveries.add(parent.id)
            if prompt is not None:
                result.callbacks.add(prompt.id)
            proof = AssignmentReopenProof(link, intent["reopen"], intent, completed=parent.id, sqlite=session.bind.dialect.name == "sqlite")
            rows = (await session.execute(proof.extend(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id)))).all()
            proof.check(rows)
            if intent["purpose"] == "work_edit":
                target = await session.get(TelegramOutboxRow, intent["parent_id"])
                if target is None or (target.link_id, target.owner_user_id, target.binding_generation, target.chat_id, target.message_thread_id, target.instance_id, target.work_id) != (
                    link.id,
                    link.owner_user_id,
                    link.binding_generation,
                    link.chat_id,
                    parent.message_thread_id,
                    link.agent_ref,
                    parent.work_id,
                ):
                    continue
                result.deliveries.add(target.id)
            else:
                origin = intent["origin"]
                original = await session.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"]))
                if (
                    original is None
                    or (original.link_id, original.chat_id, original.binding_generation, original.message_thread_id) != (link.id, link.chat_id, link.binding_generation, parent.message_thread_id)
                    or not same_body(origin_facts(InboxRow.from_row(original)), origin)
                ):
                    continue
                result.inputs.add((origin["link_id"], origin["message_id"], origin["chat_id"]))
            value = intent["reopen"]
            result.deliveries.update((value["source_outbox_id"], value["notice_id"]))
            result.deliveries.add(parent.payload["reopen_cleanup_id"])
            if proof.echo is not None:
                result.deliveries.add(proof.echo.id)
            result.callbacks.update(row.id for row in _RequestProjection.objects(rows[0][proof.column + 3]))
            held = value["input"]
            result.inputs.add((held["link_id"], held["message_id"], held["chat_id"]))
        except (ProductSourceRetired, CallbackRefused, PermanentDeliveryError, ValueError, KeyError, TypeError, AttributeError):
            continue
    return result

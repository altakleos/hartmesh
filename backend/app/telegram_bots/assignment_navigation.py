"""Explicit draft navigation preserves full consent and creates no Work effect."""

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts, DraftProjection, snapshot_draft
from app.telegram_bots.assignment_lineage import require_fresh_draft
from app.telegram_bots.assignment_sources import private_facts
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class AssignmentDraftNavigation:
    def __init__(self, plans):
        self.plans, self.acts = plans, plans.reader.acts
        self.outbox, self.callbacks = plans.outbox, plans.callbacks
        self.drafts = AssignmentDrafts(self.acts)

    async def discard(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            current = await prepare_attention(self.plans.reader, link)
            async with self.callbacks.writer(link, guard) as writer:
                control = await writer.get(TelegramCallbackRow, callback_id)
                parent = await writer.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id"), with_for_update=True, populate_existing=True) if control else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = displayed_assignment(link, parent, control)
                if control.kind != "assign_discard" or source["work"]["source"] == "tool":
                    raise CallbackRefused()
                root = await writer.get(TelegramActRow, control.target_id, with_for_update=True, populate_existing=True)
                shown = snapshot_draft(link, root)
                witness = {"version": 1, "assignment_navigation": {"kind": "discard", "origin": consent_origin(original, control)}}
                members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id).order_by(TelegramCallbackRow.id).with_for_update().execution_options(populate_existing=True)))
                _validate_rows(original, members)
                frozen = source["work"]["root"]
                if (
                    any(getattr(root, key) != frozen[key] for key in _ACT_SCOPE)
                    or not same_body(root.body, frozen["body"])
                    or root.operation_ids
                    or root.continuation is not None
                    or any(member.payload.get("work_action_id") is not None for member in members)
                ):
                    raise CallbackRefused()
                projection = DraftProjection(link, root.body["origin"], saved=_snapshot(root))
                await current(session=writer, projection=projection)
                if root.state == "refused":
                    if root.reason != "discarded" or not same_body(root.result, witness) or any(member.consumed_at is None or member.closed_reason != "discarded" for member in members):
                        raise CallbackRefused()
                else:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    self.drafts._age(shown)
                    if not same_body(private_facts(root), frozen):
                        raise CallbackRefused()
                    await require_fresh_draft(writer, link, root.id)
                    root.state, root.reason, root.result, root.updated_at = "refused", "discarded", witness, self.outbox.clock()
                    await self.callbacks.consume_group(group, reason="discarded", session=writer)
                result = _snapshot(root)
                await writer.flush()
                await current(session=writer, projection=DraftProjection(link, root.body["origin"], saved=result))
                guard.current()
            await self.plans.observe(link, result.id, guard=guard)
            return result

        return await await_drained(perform())

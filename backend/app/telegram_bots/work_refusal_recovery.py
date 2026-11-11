"""Refresh a changed, conclusively refused confirmation without retrying its body."""

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_control_admission import _ActionGuard, command
from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans, acknowledged_source
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkRefusalRecovery:
    def __init__(self, controls):
        self.controls = controls
        self.plans, self.acts = controls.plans, controls.acts
        self.callbacks, self.outbox = controls.callbacks, controls.outbox
        self.interactions = WorkInteractionPlans(self.plans)

    async def refresh(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind not in {"decide_confirm", "accept_confirm"}:
                    raise CallbackRefused()
                request_id = callback.target_id
            source, settings = await self.controls.recovery._source(link, request_id)
            admission = await prepare_attention(self.plans.reader, link)
            kind = "decide" if callback.kind == "decide_confirm" else "accept"
            composed = _ActionGuard(guard, admission, source, callback_id, kind)
            key = "notice:work-refusal-refresh:" + callback_id
            async with self.callbacks.writer(link, composed) as writer:
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if existing is not None:
                    origin = existing.payload.get("work_request_basis", {}).get("origin", {})
                    if (existing.binding_generation, existing.chat_id, existing.message_thread_id, origin.get("callback_id"), origin.get("message_id")) != (link.binding_generation, chat_id, message_thread_id, callback_id, message_id):
                        raise CallbackRefused()
                    return existing.id
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                captured = acknowledged_source(link, parent)
                kind, request = command(self.acts, group.selected, captured)
                saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                if (
                    saved is None
                    or (saved.link_id, saved.owner_user_id, saved.binding_generation, saved.chat_id, saved.instance_id, saved.work_id, saved.request_id, saved.kind)
                    != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref, source.work["id"], request_id, kind)
                    or saved.state != "pending"
                    or saved.reason != "attempt-unresolved"
                    or saved.cancel_requested_at is not None
                    or not same_body(saved.body, request.model_dump(mode="json", exclude_unset=True))
                    or any(row.payload.get("work_action_id") != saved.id for row in group.rows)
                ):
                    raise CallbackRefused()
                if same_body(captured.seen, source.seen):
                    return None
                # The bot/Act locks serialize this against the worker's fresh
                # attempt admission. Never cancel a pending unknown outcome.
                # The cancellation fence also stops an explicit retry already
                # reading this old operation outside the writer.
                saved.state, saved.reason = "conflict", "changed"
                saved.cancel_requested_at = saved.updated_at = self.outbox.clock()
                await writer.flush()
                return await self.interactions._publish(
                    writer,
                    link,
                    group,
                    parent,
                    source,
                    settings,
                    {"view": "current"},
                    key=key,
                    chat_id=chat_id,
                    message_id=message_id,
                    message_thread_id=message_thread_id,
                    guard=composed,
                    reason="work-refresh",
                )

        return await await_drained(perform())

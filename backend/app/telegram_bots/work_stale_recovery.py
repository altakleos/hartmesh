"""Show the complete current request after a conclusively stale callback."""

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_control_admission import command
from app.telegram_bots.work_interaction_delivery import CURRENT_REQUEST_KINDS, acknowledged_source
from app.telegram_bots.work_request_recovery import _RecoveryGuard
from app.telegram_bots.work_request_source import same_displayed_basis
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkStaleRecovery:
    def __init__(self, controls, interactions):
        self.controls, self.interactions = controls, interactions
        self.callbacks, self.outbox = controls.callbacks, controls.outbox

    async def refresh(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind not in CURRENT_REQUEST_KINDS or callback.target_kind != "request":
                    raise CallbackRefused()
                request_id = callback.target_id
            source, settings = await self.controls.recovery._source(link, request_id)
            admission = await prepare_attention(self.controls.plans.reader, link)
            composed = _RecoveryGuard(self.controls.recovery, guard, admission, source)
            key = "notice:work-stale-refresh:" + callback_id
            async with self.callbacks.writer(link, composed) as writer:
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if existing is not None:
                    origin = existing.payload.get("work_request_basis", {}).get("origin", {})
                    if (existing.binding_generation, existing.chat_id, existing.message_thread_id, origin.get("callback_id"), origin.get("message_id")) != (link.binding_generation, chat_id, message_thread_id, callback_id, message_id):
                        raise CallbackRefused()
                    return existing.id
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                shown = acknowledged_source(link, parent)
                if parent.payload["work_request_basis"]["version"] not in {1, 2, 3} or (shown.work["id"], shown.request["id"]) != (source.work["id"], request_id):
                    # Input-owned drafts have their own preservation/review flow.
                    raise CallbackRefused()
                saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                if saved is None:
                    if any("work_action_id" in row.payload for row in group.rows):
                        raise CallbackRefused()
                else:
                    kind, request = command(self.controls.acts, group.selected, shown)
                    if (
                        (saved.link_id, saved.owner_user_id, saved.binding_generation, saved.chat_id, saved.instance_id, saved.work_id, saved.request_id, saved.kind)
                        != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref, source.work["id"], request_id, kind)
                        or saved.state != "conflict"
                        or not same_body(saved.body, request.model_dump(mode="json", exclude_unset=True))
                        or any(row.payload.get("work_action_id") != saved.id for row in group.rows)
                    ):
                        raise CallbackRefused()
                if same_displayed_basis(shown, source):
                    return None
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

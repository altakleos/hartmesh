"""Explicit navigation retires an unsubmitted review note atomically."""

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_input_capture import WorkInputCaptures
from app.telegram_bots.work_input_previews import WorkInputPreviews, lock_input_origin
from app.telegram_bots.work_interaction_delivery import acknowledged_source, transition
from app.telegram_bots.work_request_recovery import _RecoveryGuard
from app.telegram_bots.work_request_source import same_displayed_basis
from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkRetainedDrafts:
    def __init__(self, plans):
        self.plans, self.inbox = plans, plans.outbox.inbox
        self.previews = WorkInputPreviews(plans, WorkInputCaptures(self.inbox, plans.callbacks))

    async def navigate(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.plans.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind not in {"changes_open", "dismiss"}:
                    raise CallbackRefused()
                editing, request_id = callback.kind == "changes_open", callback.target_id
            source = settings = None
            composed = guard
            if editing:
                source, settings = await self.previews.recovery._source(link, request_id)
                admission = await prepare_attention(self.plans.reader, link)
                composed = _RecoveryGuard(self.previews.recovery, guard, admission, source)
            async with self.plans.callbacks.writer(link, composed) as writer:
                group = await self.plans.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                basis = parent.payload.get("work_request_basis", {})
                if basis.get("version") != 4 or basis.get("interaction", {}).get("view") != "changes":
                    raise CallbackRefused()
                captured = acknowledged_source(link, parent)
                await lock_input_origin(writer, link, parent, captured, draft_ttl_hours=self.plans.callbacks.config.callbacks.draft_ttl_hours)
                identity = basis["input"]
                mark = await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), populate_existing=True)
                result = None
                if editing:
                    if not same_displayed_basis(captured, source):
                        raise AgentConflict("Review the current request and all responses")
                    result = await self.previews.interactions._publish(
                        writer,
                        link,
                        group,
                        parent,
                        source,
                        settings,
                        transition(group.selected, source),
                        key="notice:work-interaction:" + callback_id,
                        chat_id=chat_id,
                        message_id=message_id,
                        message_thread_id=message_thread_id,
                        guard=composed,
                    )
                await self.previews._retire_previous(writer, link, identity)
                await self.inbox.finish_route(InboxRow.from_row(mark), session=writer)
                return result

        return await await_drained(perform())

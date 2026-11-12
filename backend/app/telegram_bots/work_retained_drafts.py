"""Explicit navigation retires an unsubmitted decision or review input atomically."""

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_input_capture import WorkInputCaptures
from app.telegram_bots.work_input_previews import WorkInputPreviews, _InputGuard, _InputProjection, lock_input_origin
from app.telegram_bots.work_interaction_delivery import acknowledged_source, transition
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from app.telegram_bots.work_request_source import same_displayed_basis
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
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
                if callback is None or callback.link_id != link.id or callback.kind not in {"changes_open", "dismiss", "back", "decide_note"}:
                    raise CallbackRefused()
                editing, request_id = callback.kind != "dismiss", callback.target_id
            source = settings = None
            if editing:
                source, settings = await self.previews.recovery._source(link, request_id)
            admission = await prepare_attention(self.plans.reader, link)
            composed = _NavigationGuard(guard, admission, source, editing)
            async with self.plans.callbacks.writer(link, composed) as writer:
                group = await self.plans.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                basis = parent.payload.get("work_request_basis", {})
                view = "decision" if group.selected.kind in {"back", "decide_note"} else "changes"
                if basis.get("version") != 4 or basis.get("interaction", {}).get("view") != view:
                    raise CallbackRefused()
                captured = acknowledged_source(link, parent)
                await lock_input_origin(writer, link, parent, captured, draft_ttl_hours=self.plans.callbacks.config.callbacks.draft_ttl_hours)
                identity = basis["input"]
                mark = await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), populate_existing=True)
                composed.captured = InboxRow.from_row(mark)
                composed.source = source or captured
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
                await self.inbox.finish_route(composed.captured, session=writer)
                composed.complete = True
                return result

        return await await_drained(perform())


class _NavigationGuard(_InputGuard):
    def __init__(self, base, admission, source, editing):
        super().__init__(base, admission, source, None)
        self.editing, self.complete = editing, False

    async def check(self, writer):
        current = await self.base.check(writer)
        projection = None
        if self.captured is not None:
            projection = _NavigationProjection(self.source, self.captured, self.editing, self.complete)
        elif self.editing:
            projection = _RecoveryProjection(self.source)
        held, _target = await self.admission(session=writer, projection=projection)
        if self.editing and "agents:write" not in held:
            raise AgentDenied("The preview is unavailable")
        self.base.current()
        return current


class _NavigationProjection(_InputProjection):
    def __init__(self, source, captured, editing, complete):
        super().__init__(source, captured)
        self.editing, self.complete = editing, complete

    def check(self, rows):
        if self.editing:
            _RecoveryProjection.check(self, rows)
        self._check_mark(rows, complete=self.complete)

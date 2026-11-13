"""Start consent binds a complete acknowledged assignment to one saved Act."""

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from app.telegram_bots.work_views import work_buttons
from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def displayed_start(link, parent, callback):
    original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
    source = original.payload["work_news_basis"]["source"]
    preview = source["key"] in {"execution.activate", "execution.stop"} and source.get("work", {}).get("notice", {}).get("preview") is True
    if (source["key"] != "view.detail" and not preview) or callback.kind != "work_start" or callback.target_kind != "work":
        raise CallbackRefused()
    detail = source["work"]["detail"]["record"]
    if (callback.target_id, callback.payload.get("work_id"), callback.payload.get("assignment_revision")) != (detail["id"], detail["id"], detail["assignment_revision"]) or type(callback.payload.get("assignment_revision")) is not int:
        raise CallbackRefused()
    return original, detail


class WorkStartAdmissions:
    def __init__(self, plans, acts):
        self.plans, self.acts = plans, acts
        self.callbacks, self.outbox = plans.callbacks, plans.outbox
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("work-start-requires-shared-storage")

    async def stage(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                saved = await reader.get(TelegramActRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id")) if callback else None
                if (
                    callback is None
                    or parent is None
                    or (callback.link_id, callback.chat_id, callback.binding_generation, callback.message_id, callback.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, displayed = displayed_start(link, parent, callback)
                signature = anchor_signature(original)
            if saved is not None:
                # Only its original immutable intent is observed. No expired
                # callback becomes fresh permission to launch another attempt.
                async with self.callbacks.writer(link, guard) as writer:
                    current = await writer.get(TelegramCallbackRow, callback_id, populate_existing=True)
                    parent = await writer.get(TelegramOutboxRow, current.payload.get("anchor_outbox_id"), populate_existing=True)
                    original, _ = displayed_start(link, parent, current)
                    rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == current.group_id)))
                    _validate_rows(original, rows)
                    if anchor_signature(original) != signature or any(row.payload.get("work_action_id") != callback_id for row in rows):
                        raise CallbackRefused()
                return await self.acts.stage_activation(link, operation_id=callback_id, work_id=displayed["id"], assignment_revision=displayed["assignment_revision"])
            fresh = await self.plans.reader.detail(link, displayed["id"])
            if any(not same_body(fresh.record.get(key), displayed.get(key)) for key in ("id", "objective", "success_criteria", "assignment_revision", "due_at")):
                raise AgentConflict("Review the current assignment", reason="work-changed")
            controls = work_buttons(
                fresh.record, owner_id=link.owner_user_id, permissions=fresh.permissions, held=fresh.held, request=fresh.request, lifecycle_pending=fresh.lifecycle_pending, can_reuse_conversation=fresh.can_reuse_conversation
            )
            if not any(item.get("kind") == "work_start" for line in controls for item in line):
                raise CallbackRefused()
            detail_current = await self.plans.reader.prepare_detail(link, fresh, ignore_activation=callback_id)

            async def admit(writer, act):
                await guard.check(writer)
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                actual, _ = displayed_start(link, parent, group.selected)
                if anchor_signature(actual) != signature:
                    raise CallbackRefused()
                rows = [await writer.get(TelegramCallbackRow, row.id, populate_existing=True) for row in group.rows]
                if any(row.payload.get("work_action_id") is not None for row in rows):
                    raise CallbackRefused(None)
                if act is not None:
                    for row in rows:
                        row.payload = {**row.payload, "work_action_id": act.id}
                    await writer.flush()
                await detail_current(session=writer)
                guard.current()

            return await self.acts.stage_activation(link, operation_id=callback_id, work_id=displayed["id"], assignment_revision=displayed["assignment_revision"], admission=admit)

        return await await_drained(perform())

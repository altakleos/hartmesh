"""One full acknowledged management card admits one immutable command."""

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_management_consent import management_origin, require_management_scope
from app.telegram_bots.work_management_origins import displayed_management, origin_for
from app.telegram_bots.work_management_sources import management_intent
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkManagementAdmissions:
    def __init__(self, plans):
        self.plans, self.acts = plans, plans.acts
        self.callbacks, self.outbox = plans.callbacks, plans.outbox

    async def stage(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                control = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control is not None else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = displayed_management(link, parent, control)
                intent = source["work"]["intent"]
                expected_kind = "work_change_save" if intent["kind"] == "edit" else "work_withdraw_confirm"
                if control.kind != expected_kind:
                    raise CallbackRefused()
                origin = origin_for(original, control)
                native = await reader.get(TelegramActRow, callback_id)
                saved = _snapshot(native) if native is not None else None
                item = _item(parent)
            if saved is not None:
                await self.acts._work_link(link)
                async with self.callbacks.writer(link, guard) as writer:
                    control = await writer.get(TelegramCallbackRow, callback_id, populate_existing=True)
                    parent = await writer.get(TelegramOutboxRow, origin["anchor_outbox_id"], populate_existing=True)
                    native = await writer.get(TelegramActRow, callback_id, populate_existing=True)
                    if native is None:
                        raise CallbackRefused()
                    original, _source = displayed_management(link, parent, control)
                    members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)))
                    _validate_rows(original, members)
                    actual = _snapshot(native)
                    require_management_scope(link, actual, {**intent, "act_id": actual.id})
                    if origin_for(original, control) != origin or management_origin(actual) != origin or any(member.payload.get("work_action_id") != actual.id for member in members) or not same_body(actual.body, saved.body):
                        raise CallbackRefused()
                    guard.current()
                    return actual
            await self.acts._work_link(link, kind=intent["kind"], writable=True)
            current = await self.plans.reader.read(link, item)
            if current.work["phase"] != ("draft" if intent["kind"] == "edit" else "confirm"):
                raise CallbackRefused("stale.changed_resend")

            async def admission(writer, act):
                await guard.check(writer)
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, with_for_update=True, populate_existing=True)
                original, _source = displayed_management(link, parent, group.selected)
                if origin_for(original, group.selected) != origin or not same_body(management_intent(_item(parent)), intent):
                    raise CallbackRefused()
                members = [await writer.get(TelegramCallbackRow, member.id, populate_existing=True) for member in group.rows]
                if any(member.payload.get("work_action_id") is not None for member in members):
                    raise CallbackRefused()
                if act is not None:
                    for member in members:
                        member.payload = {**member.payload, "work_action_id": act.id}
                    parent.payload = {**parent.payload, "work_management_intent": {**intent, "act_id": act.id}}
                    await writer.flush()
                guard.current()

            def confirmation_current():
                guard.current()
                self.callbacks._unexpired(control)

            if intent["kind"] == "edit":
                return await self.acts.stage_edit(link, intent["draft_id"], operation_id=callback_id, admission=admission, origin=origin, confirmation_current=confirmation_current)
            return await self.acts.stage_cancel(
                link,
                operation_id=callback_id,
                work_id=intent["work_id"],
                expected_revision=intent["expected_revision"],
                expected_assignment_revision=intent["expected_assignment_revision"],
                admission=admission,
                origin=origin,
                confirmation_current=confirmation_current,
            )

        return await await_drained(perform())

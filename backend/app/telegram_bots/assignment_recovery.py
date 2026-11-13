"""Own Save outcomes admit cancellation or one explicit retry of the shown intent."""

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_delegate_actions import require_delegate_scope
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def shown_save(link, parent, control, *, cancel):
    original = acknowledged_view(link, parent, control.payload.get("presentation_version"))
    source = original.payload["work_news_basis"]["source"]
    final = original.parts[-1]
    try:
        if (
            source["key"] != "assignment.card"
            or not source["work"]["notice"]["cancel" if cancel else "retry"]
            or control.kind != ("cancel_wait" if cancel else "act_check")
            or control.target_kind != "act"
            or (control.target_id, control.payload.get("act_id")) != (source["work"]["act"]["id"], source["work"]["act"]["id"])
            or final.get("callback_group_id") != control.group_id
            or final.get("message_ids") != [control.message_id]
        ):
            raise ValueError()
        return original, source
    except (ValueError, KeyError, TypeError):
        raise CallbackRefused() from None


def require_save(link, saved, expected):
    if saved is None:
        raise CallbackRefused()
    require_delegate_scope(link, _snapshot(saved))
    if (
        any(getattr(saved, key) != expected[key] for key in _ACT_SCOPE)
        or not same_body(saved.body, expected["body"])
        or not same_body(saved.continuation.get("delegate_source"), expected["continuation"].get("delegate_source"))
        or not same_body(saved.continuation.get("delegate_origin"), expected["continuation"].get("delegate_origin"))
    ):
        raise CallbackRefused()


class AssignmentRecovery:
    def __init__(self, plans):
        self.plans, self.acts, self.outbox, self.callbacks = plans, plans.reader.acts, plans.outbox, plans.callbacks

    async def operate(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard, cancel):
        async def perform():
            async with self.outbox.sessions() as reader:
                control = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = shown_save(link, parent, control, cancel=cancel)
                signature, expected, draft_id = anchor_signature(original), source["work"]["act"], source["work"]["draft_id"]
                replay = control.payload.get("work_action_id") == expected["id"]

            async def admit(writer, act):
                await guard.check(writer)
                if replay:
                    selected = await writer.get(TelegramCallbackRow, callback_id, populate_existing=True)
                    row = await writer.get(TelegramOutboxRow, selected.payload.get("anchor_outbox_id"), populate_existing=True)
                    members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == selected.group_id)))
                else:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    selected = group.selected
                    row = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                    members = [await writer.get(TelegramCallbackRow, member.id, populate_existing=True) for member in group.rows]
                actual, _source = shown_save(link, row, selected, cancel=cancel)
                _validate_rows(actual, members)
                target = await writer.get(TelegramActRow, expected["id"], with_for_update=True, populate_existing=True)
                require_save(link, target, expected)
                if (
                    anchor_signature(actual) != signature
                    or (selected.message_id, selected.message_thread_id) != (message_id, message_thread_id)
                    or any(member.message_id != message_id or member.payload.get("work_action_id") != (expected["id"] if replay else None) for member in members)
                    or replay
                    and target.cancel_requested_at is None
                ):
                    raise CallbackRefused()
                if act is not None and not replay:
                    for member in members:
                        member.payload = {**member.payload, "work_action_id": expected["id"]}
                    await writer.flush()
                guard.current()

            if cancel:
                if replay:
                    async with self.callbacks.writer(link, guard) as writer:
                        await admit(writer, None)
                        fenced = _snapshot(await writer.get(TelegramActRow, expected["id"], populate_existing=True))
                    try:
                        result = await self.acts.reconcile_receipt(expected["id"])
                    except Exception:
                        # A missing receipt cannot undo the original durable
                        # fence or turn its repeated observation into failure.
                        result = fenced
                else:
                    result = await self.acts.cancel_delegation(expected["id"], admission=admit)
            else:
                async with self.callbacks.writer(link, guard) as writer:
                    await admit(writer, None)

                def current():
                    try:
                        guard.current()
                        return True
                    except Exception:
                        return False

                result = await self.acts.check(expected["id"], expected={**expected, "effective": source["work"].get("child") or expected}, attempt_current=current)
            await self.plans.observe(link, draft_id)
            return result

        return await await_drained(perform())

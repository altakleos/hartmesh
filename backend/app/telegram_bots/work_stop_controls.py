"""One acknowledged Stop gesture owns one immutable original-attempt intent."""

from sqlalchemy import select

from app.telegram_bots.authority import AuthorityUnavailable, CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.work_actions import _ACT_SCOPE, request_for
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from app.telegram_bots.work_views import work_buttons
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


def displayed_stop(link, parent, callback):
    original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
    source = original.payload["work_news_basis"]["source"]
    if source["key"] != "view.detail" or callback.kind != "stop_attempt" or callback.target_kind != "attempt":
        raise CallbackRefused()
    record = source["work"]["detail"]["record"]
    attempt = record.get("attempt") or {}
    if (callback.target_id, callback.payload.get("attempt_id"), callback.payload.get("work_id")) != (attempt.get("id"), attempt.get("id"), record["id"]):
        raise CallbackRefused()
    return original, record["id"], attempt["id"]


class WorkStopAdmissions:
    def __init__(self, plans, acts):
        self.plans, self.acts = plans, acts
        self.callbacks, self.outbox = plans.callbacks, plans.outbox
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("work-stop-requires-shared-storage")

    async def stage(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            await self.acts._work_link(link)
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
                original, work_id, attempt_id = displayed_stop(link, parent, callback)
                signature = anchor_signature(original)
            if saved is not None:
                async with self.callbacks.writer(link, guard) as writer:
                    current = await writer.get(TelegramCallbackRow, callback_id, populate_existing=True)
                    parent = await writer.get(TelegramOutboxRow, current.payload.get("anchor_outbox_id"), populate_existing=True)
                    original, _work, _attempt = displayed_stop(link, parent, current)
                    rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == current.group_id)))
                    _validate_rows(original, rows)
                    from app.telegram_bots.acts import _snapshot

                    journal = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                    if journal is None:
                        raise CallbackRefused()
                    act = _snapshot(journal)
                    expected = dict(
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        bot_id=link.bot_id,
                        binding_generation=link.binding_generation,
                        telegram_user_id=link.telegram_user_id,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        work_id=work_id,
                        request_id=None,
                        kind="stop",
                        message_id=None,
                    )
                    if (
                        anchor_signature(original) != signature
                        or any(row.payload.get("work_action_id") != act.id for row in rows)
                        or any(getattr(act, key) != expected[key] for key in _ACT_SCOPE)
                        or request_for("stop", act.body, identity=act.id).attempt_id != attempt_id
                    ):
                        raise CallbackRefused()
                    return act
            # A progress update can invalidate preparation before admission.
            # Retrying this read never changes an admitted operation or target.
            for _ in range(3):
                fresh = await self.plans.reader.detail(link, work_id)
                if not {"agents:write", "runs:cancel"}.issubset(fresh.held):
                    raise CeilingRefused()
                original_attempt = await self.acts.work.attempt_for_stop(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=work_id, attempt_id=attempt_id)
                latest = fresh.record.get("attempt") or {}
                if original_attempt and original_attempt["status"] in {"starting", "running"} and latest.get("id") == attempt_id:
                    controls = work_buttons(fresh.record, owner_id=link.owner_user_id, permissions=fresh.permissions, held=fresh.held, request=fresh.request)
                    if not any(item.get("kind") == "stop_attempt" and item["target_id"] == attempt_id for line in controls for item in line):
                        raise CallbackRefused()
                detail_current = await self.plans.reader.prepare_detail(link, fresh)

                async def admit(writer, act):
                    await guard.check(writer)
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                    actual, _work, _attempt = displayed_stop(link, parent, group.selected)
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

                try:
                    return await self.acts.stage_stop(link, operation_id=callback_id, work_id=work_id, attempt_id=attempt_id, admission=admit)
                except AuthorityUnavailable:
                    continue
            raise CallbackRefused("stop.failed")

        return await await_drained(perform())

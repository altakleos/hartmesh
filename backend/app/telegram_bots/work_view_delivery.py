"""Durable explicit Work views; publication does not execute a Work action."""

from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, Prompts
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_news_delivery import WorkNewsPlans
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from app.telegram_bots.work_view_sources import WorkViewProjection
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkViewPlans(WorkNewsPlans):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, projection=WorkViewProjection)

    async def command(self, link, inbox, *, guard):
        async def perform():
            key = "notice:work-view:inbox:" + str(inbox.update_id)
            # A replay reads its saved cursor instead of allocating a new list.
            async with self.outbox.sessions() as reader:
                current = await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
                if (
                    current is None
                    or current.sanitized_input.get("command") != "work"
                    or (current.link_id, current.chat_id, current.binding_generation, current.update_id) != (link.id, link.chat_id, link.binding_generation, inbox.update_id)
                ):
                    raise ProductSourceRetired()
                cursor = current.route_basis.get("cursor") if current.route_kind == "work_view" else None
            if cursor is None:
                cursor = await self.reader.open_list(link)
            async with self.callbacks.writer(link, guard) as writer:
                routed = await self.outbox.inbox.route(inbox, kind="work_view", target_id=cursor, basis={"cursor": cursor}, operation_ids=[], session=writer)
                row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if row is None:
                    await Prompts(self.callbacks).close(link, inbox.chat_id, inbox.message_thread_id, reason="command", guard=guard, session=writer, shown_only=True)
                    row = self._row(link, key, {"version": 1, "key": "view.list", "cursor": cursor, "origin": {"kind": "inbox", "message_id": inbox.message_id, "update_id": inbox.update_id}}, topic=inbox.message_thread_id)
                    writer.add(row)
                    await writer.flush()
                await self.outbox.inbox.register_delivery([routed], outbox_id=row.id, primary=True, finish=True, session=writer)
                identity = row.id
            await self.materialize(link, identity, guard=guard)
            return identity

        return await await_drained(perform())

    async def open(self, link, callback_id, *, query_id, chat_id, message_id, message_thread_id, guard):
        if type(query_id) is not str or not 1 <= len(query_id) <= 256:
            raise CallbackRefused()
        query_key = digest([link.id, link.binding_generation, callback_id, query_id])

        async def perform():
            key = "notice:work-view:callback:" + query_key
            async with self.callbacks.writer(link, guard) as writer:
                row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if row is not None:
                    origin = row.payload.get("work_view_intent", {}).get("origin", {})
                    gesture = await writer.get(TelegramCallbackRow, origin.get("gesture_id"))
                    if gesture is None or gesture.message_id != message_id or gesture.target_id != row.id or gesture.payload != origin:
                        raise CallbackRefused()
                    if (row.owner_user_id, row.chat_id, row.binding_generation, row.message_thread_id) != (link.owner_user_id, chat_id, link.binding_generation, message_thread_id) or origin.get("callback_id") != callback_id:
                        raise CallbackRefused()
                else:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                    selected = group.selected
                    if selected.kind not in {"work_details", "work_list_page"}:
                        raise CallbackRefused()
                    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
                    original = acknowledged_view(link, parent, group.presentation_version)
                    if original.payload["work_news_basis"]["source"]["key"] not in {"view.list", "view.detail"}:
                        raise CallbackRefused()
                    origin = {"kind": "callback", "gesture_id": uuid4().hex, "callback_id": selected.id, "anchor_outbox_id": parent.id, "presentation_version": group.presentation_version, "anchor_signature": anchor_signature(original)}
                    intent = {"key": "view.detail", "work_id": selected.payload["work_id"]} if selected.kind == "work_details" else {"key": "view.list", "cursor": selected.payload["cursor"]}
                    row = self._row(link, key, {"version": 1, **intent, "origin": origin}, topic=message_thread_id)
                    writer.add(row)
                    expires = aware(selected.created_at) + timedelta(days=self.callbacks.config.callbacks.max_age_days)
                    if selected.expires_at is not None:
                        expires = min(expires, aware(selected.expires_at))
                    writer.add(
                        TelegramCallbackRow(
                            id=origin["gesture_id"],
                            group_id=origin["gesture_id"],
                            link_id=link.id,
                            chat_id=link.chat_id,
                            binding_generation=link.binding_generation,
                            message_id=message_id,
                            message_thread_id=message_thread_id,
                            kind="work_view_gesture",
                            target_kind="outbox",
                            target_id=row.id,
                            payload=origin,
                            expires_at=expires,
                            created_at=self.outbox.clock(),
                        )
                    )
                    await writer.flush()
                identity = row.id
            await self.materialize(link, identity, guard=guard)
            return identity

        return await await_drained(perform())

    def _row(self, link, key, intent, *, topic):
        now = self.outbox.clock()
        return TelegramOutboxRow(
            id=uuid4().hex,
            link_id=link.id,
            owner_user_id=link.owner_user_id,
            binding_generation=link.binding_generation,
            chat_id=link.chat_id,
            instance_id=link.agent_ref,
            work_id=intent.get("work_id"),
            dedupe_key=key,
            class_="reply",
            payload={"presentation_version": 0, "work_view_intent": deepcopy(intent)},
            parts=[],
            state="pending",
            silent=True,
            message_thread_id=topic,
            not_before=now,
            created_at=now,
        )

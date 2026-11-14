"""Durable saved-work preview plans precede every explicit effect control."""

from dataclasses import asdict
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_edit_drafts import WorkEditDrafts
from app.telegram_bots.work_management_origins import origin_for
from app.telegram_bots.work_management_sources import WorkManagementProjection, management_intent
from app.telegram_bots.work_news_delivery import WorkNewsPlans, frozen_news
from app.telegram_bots.work_news_recovery import WorkNewsRecovery
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_result_navigation import NavigationOrigin
from app.telegram_bots.work_view_origins import acknowledged_view
from app.telegram_bots.work_views import work_buttons
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkManagementPlans(WorkNewsPlans):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, projection=WorkManagementProjection)
        self.acts = self.reader.acts

    async def confirm(self, link, callback_id, **kwargs):
        from app.telegram_bots.work_management_controls import WorkManagementAdmissions

        return await WorkManagementAdmissions(self).stage(link, callback_id, **kwargs)

    async def back(self, link, callback_id, **kwargs):
        from app.telegram_bots.work_management_navigation import WorkManagementNavigation

        return await WorkManagementNavigation(self).operate(link, callback_id, discard=False, **kwargs)

    async def discard(self, link, callback_id, **kwargs):
        from app.telegram_bots.work_management_navigation import WorkManagementNavigation

        return await WorkManagementNavigation(self).operate(link, callback_id, discard=True, **kwargs)

    async def cancel(self, link, callback_id, **kwargs):
        from app.telegram_bots.work_management_recovery import WorkManagementRecovery

        return await WorkManagementRecovery(self).operate(link, callback_id, retry=False, **kwargs)

    async def retry(self, link, callback_id, **kwargs):
        from app.telegram_bots.work_management_recovery import WorkManagementRecovery

        return await WorkManagementRecovery(self).operate(link, callback_id, retry=True, **kwargs)

    async def open(self, link, callback_id, *, query_id, chat_id, message_id, message_thread_id, guard):
        if type(query_id) is not str or not 1 <= len(query_id) <= 256:
            raise CallbackRefused()

        async def perform():
            await prepare_publication_guard(guard)
            key = "notice:work-management:" + digest([link.id, link.binding_generation, callback_id, query_id])
            async with self.outbox.sessions() as reader:
                saved = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
                if saved is not None:
                    intent = management_intent(_item(saved))
                    gesture = await reader.get(TelegramCallbackRow, intent["origin"]["gesture_id"])
                    if (
                        gesture is None
                        or intent["origin"]["callback_id"] != callback_id
                        or (saved.owner_user_id, saved.chat_id, saved.binding_generation, saved.message_thread_id, gesture.message_id) != (link.owner_user_id, chat_id, link.binding_generation, message_thread_id, message_id)
                    ):
                        raise CallbackRefused()
                    identity = saved.id
                else:
                    identity = None
            if identity is not None:
                await self.observe(link, identity)
                return identity
            async with self.callbacks.writer(link, guard) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                selected = group.selected
                if selected.kind not in {"work_change", "work_withdraw"} or selected.target_kind != "work" or selected.target_id != selected.payload.get("work_id"):
                    raise CallbackRefused()
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
                original = acknowledged_view(link, parent, group.presentation_version)
                if original.payload["work_news_basis"]["concealed"]:
                    raise CallbackRefused()
                origin = {**origin_for(original, selected), "gesture_id": uuid4().hex, "group_id": selected.group_id}
                work_id, kind = selected.target_id, "edit" if selected.kind == "work_change" else "cancel"
            fresh = await self.reader.detail(link, work_id)
            controls = work_buttons(fresh.record, owner_id=link.owner_user_id, permissions=fresh.permissions, held=fresh.held, request=fresh.request)
            if not any(button.get("kind") == selected.kind for line in controls for button in line):
                raise CallbackRefused()
            detail_current = await self.reader.prepare_detail(link, fresh)
            identity = uuid4().hex

            async def validate(writer):
                await guard.check(writer)
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                actual = acknowledged_view(link, parent, group.presentation_version)
                if origin_for(actual, group.selected) != {key: origin[key] for key in ("callback_id", "anchor_outbox_id", "presentation_version", "signature")}:
                    raise CallbackRefused()
                await detail_current(session=writer)
                guard.current()

            async def stage_row(writer, root):
                now = self.outbox.clock()
                intent = {
                    "version": 1,
                    "kind": kind,
                    "work_id": work_id,
                    "draft_id": root.id if root is not None else None,
                    "expected_revision": fresh.record["revision"],
                    "expected_assignment_revision": fresh.record["assignment_revision"],
                    "origin": origin,
                    "act_id": None,
                }
                writer.add(
                    TelegramOutboxRow(
                        id=identity,
                        link_id=link.id,
                        owner_user_id=link.owner_user_id,
                        binding_generation=link.binding_generation,
                        chat_id=link.chat_id,
                        instance_id=link.agent_ref,
                        work_id=work_id,
                        dedupe_key=key,
                        class_="reply",
                        payload={"presentation_version": 0, "work_management_intent": intent},
                        parts=[],
                        state="pending",
                        silent=True,
                        message_thread_id=message_thread_id,
                        not_before=now,
                        created_at=now,
                    )
                )
                writer.add(
                    TelegramCallbackRow(
                        id=origin["gesture_id"],
                        group_id=origin["gesture_id"],
                        link_id=link.id,
                        chat_id=link.chat_id,
                        binding_generation=link.binding_generation,
                        message_id=message_id,
                        message_thread_id=message_thread_id,
                        kind="work_management_gesture",
                        target_kind="outbox",
                        target_id=identity,
                        payload=origin,
                        expires_at=now + timedelta(hours=self.outbox.config.callbacks.draft_ttl_hours),
                        created_at=now,
                    )
                )
                await writer.flush()

            if kind == "edit":

                async def admission(writer, root):
                    await validate(writer)
                    if root is not None:
                        await stage_row(writer, root)
                    await validate(writer)

                await WorkEditDrafts(self.acts).create(link, operation_id=digest([key, "draft"])[:32], work_id=work_id, admission=admission, confirmation_current=guard.current)
            else:
                async with self.callbacks.writer(link, guard) as writer:
                    await validate(writer)
                    await stage_row(writer, None)
                    await validate(writer)
            await self.materialize(link, identity)
            return identity

        return await await_drained(perform())

    async def observe(self, link, identity):
        """Refresh only this saved preview; never issue a Work command."""

        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, identity)
                if row is None:
                    raise ProductSourceRetired()
                item = _item(row)
                if any(getattr(row, field) is not None for field in ("claim_owner", "claim_id", "claimed_at")):
                    return identity
            source = await self.reader.read(link, item)
            if not item.parts:
                await self.materialize(link, identity)
                return identity
            basis, prior, _part = frozen_news(SimpleNamespace(link=link, item=item, part=None))
            settings = await self.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            origin = NavigationOrigin()
            if same_body(asdict(source), asdict(prior)) and settings.locale == basis["locale"] and origin.base == basis["public_base_url"]:
                return identity
            current = await prepare_attention(self.reader, link)
            clock = await self.reader.prepare_clock(link, item)
            async with self.outbox._writer(link.id) as (writer, _binding):
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if row is None or _item(row) != item or any(getattr(row, field) is not None for field in ("claim_owner", "claim_id", "claimed_at")):
                    return identity
                await current(session=writer, projection=WorkManagementProjection(self.reader, link, item, source, clock=clock))
                await WorkNewsRecovery(self)._replace(writer, link, row, source, settings, origin, direct_edit=True, unclaimed=True)
                await current(session=writer, projection=WorkManagementProjection(self.reader, link, _item(row), source, clock=clock))
                origin.check(row.payload["work_news_basis"])
            return identity

        return await await_drained(perform())

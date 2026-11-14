"""Full source-owned previews from reusable acknowledged digest controls."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_preview_plans import freeze_notice, frozen_preview
from app.telegram_bots.work_navigation_origin import NavigationOriginProjection, WorkNavigation
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class DigestItems:
    def __init__(self, *, digests, requests, reviews=None, routing=None, news=None, results=None, summaries=None, views=None):
        self.navigation = WorkNavigation(digests, summaries=summaries, requests=requests, reviews=reviews, views=views)
        self.digests, self.requests = digests, requests
        self.outbox, self.callbacks = digests.outbox, digests.callbacks
        self.reviews, self.routing, self.news, self.results = reviews, routing, news, results
        if any(owner is not None and (owner.outbox is not self.outbox or owner.callbacks is not self.callbacks) for owner in (requests, reviews, routing, news, results)):
            raise ValueError("digest-items-require-shared-owners")

    async def execute_callback(self, channel, row, *, query_id=None, **_options):
        if row.kind != "digest_item":
            return False
        link = deepcopy(channel.link)
        guard = channel._services.commands._guard(channel, link)
        try:
            await self.open(link, callback_id=row.id, query_id=query_id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard)
        except ProductSourceRetired:
            raise CallbackRefused("stale.not_waiting") from None
        return True

    def _owner(self, target):
        if target.family == "work_request_basis":
            owner = self.routing if target.routing else self.reviews if target.review else self.requests
        elif target.family == "work_news_basis":
            owner = self.news
        else:
            owner = self.results
        if owner is None:
            raise ProductSourceRetired()
        return target.family, owner

    async def open(self, link, *, callback_id, query_id, chat_id, message_id, message_thread_id, guard):
        async def perform():
            origin = await self.navigation.read(link, callback_id=callback_id, query_id=query_id)
            if type(chat_id) is not int or type(message_id) is not int or (chat_id, message_id, message_thread_id) != (link.chat_id, origin.value["message_id"], origin.anchor.message_thread_id):
                raise CallbackRefused()
            source = origin.target.source
            family, planner = self._owner(origin.target)
            settings = await self.digests.settings.get(link.owner_user_id)
            if settings is None or settings.locale != origin.snapshot.locale:
                raise ProductSourceRetired()
            composed = _PreviewGuard(guard, origin)
            key = "notice:work-navigation:" + origin.value["query_key"]
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if existing is not None:
                    current_parts = [part for part in existing.parts if part["presentation_version"] == existing.payload["presentation_version"]]
                    if not current_parts:
                        raise ProductSourceRetired()
                    selected = next((part for part in current_parts if part["state"] in {"pending", "sending"}), current_parts[-1])
                    saved, _source, _part = frozen_preview(SimpleNamespace(link=link, item=_item(existing), part=selected), pending=selected["state"] == "pending", replay=True, acknowledged=selected["state"] == "sent")
                    if not same_body(saved.get("navigation_origin"), origin.value):
                        raise ProductSourceRetired()
                    return existing.id
                row = TelegramOutboxRow(
                    id=uuid4().hex,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    chat_id=link.chat_id,
                    instance_id=link.agent_ref,
                    work_id=origin.target.work_id,
                    request_id=origin.target.request_id,
                    event_id=origin.target.event_id,
                    dedupe_key=key,
                    class_="reply",
                    message_thread_id=message_thread_id,
                    payload={"presentation_version": 0, **deepcopy(origin.target.intent or {})},
                    parts=[],
                    state="pending",
                    silent=True,
                    not_before=self.outbox.clock(),
                    created_at=self.outbox.clock(),
                )
                writer.add(row)
                await writer.flush()
                if family != "work_request_basis":
                    await freeze_notice(planner, writer, link, row, source, settings, origin.value, family=family)
                elif planner is self.routing:
                    await planner.freeze_routing(writer, link, row, source.value, settings, source.public_base_url, navigation_origin=origin.value)
                else:
                    await planner._freeze(writer, link, row, source, settings, pending=True, navigation_origin=origin.value)
                for part in row.parts:
                    if group_id := part.get("callback_group_id"):
                        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id)):
                            control.created_at = group.selected.created_at
                frozen_preview(SimpleNamespace(link=link, item=_item(row), part=row.parts[0]), pending=True)
                composed.preview, composed.created_at = _item(row), group.selected.created_at
                return row.id

        return await await_drained(perform())


class _PreviewGuard:
    def __init__(self, base, origin):
        self.base, self.origin = base, origin
        self.preview, self.created_at = None, None

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        admission = await prepare_attention(self.origin.owner.plans.reader, self.origin.snapshot.link)
        projection = NavigationOriginProjection(self.origin) if self.preview is None else _PreviewProjection(self)
        await admission(session=writer, projection=projection)
        self.base.current()
        return current


class _PreviewProjection(NavigationOriginProjection):
    def __init__(self, guard):
        super().__init__(guard.origin)
        self.guard = guard

    def extend(self, query):
        query = super().extend(query)
        self.preview_column = len(query.column_descriptions)
        row, callback = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        groups = [part["callback_group_id"] for part in self.guard.preview.parts if "callback_group_id" in part]
        return query.add_columns(row, callback).join(row, row.id == self.guard.preview.id).outerjoin(callback, callback.group_id.in_(groups))

    def check_with_permissions(self, rows, held):
        # The anchor projection must see its own last-column callback, not
        # the newly appended preview controls.
        super().check_with_permissions([row[: self.preview_column] for row in rows], held)
        row = rows[0][self.preview_column]
        if not _same_item(row, self.guard.preview) or claim_of(row) != (None, None, None):
            raise ProductSourceRetired()
        controls = {value.id: value for result in rows if (value := result[self.preview_column + 1]) is not None}
        try:
            _validate_rows(row, list(controls.values()), allow_pending=True)
            for control in controls.values():
                self.guard.origin.owner.plans.callbacks._usable(control)
                if control.message_id is not None or control.payload.get("presentation_pending") is not True or aware(control.created_at) != aware(self.guard.created_at):
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None

"""Review previews retain the real Work-detail gesture and fresh request basis."""

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_detail_cards import detail_card_owner, full_detail
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_news_delivery import prepare_news_projection
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from app.telegram_bots.work_view_origins import acknowledged_view
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def review_transition(link, parent, selected, source):
    original = acknowledged_view(link, parent, selected.payload.get("presentation_version"))
    basis = original.payload["work_news_basis"]
    detail = full_detail(basis["source"])
    request = detail.get("request") or {}
    if (
        basis["concealed"]
        or selected.kind not in {"accept_open", "changes_open"}
        or selected.target_kind != "request"
        or selected.target_id != source.request["id"]
        or selected.payload.get("request_id") != source.request["id"]
        or selected.payload.get("work_id") != source.work["id"]
        or request.get("id") != source.request["id"]
        or request.get("purpose") != "review"
        or source.request["purpose"] != "review"
        or detail.get("record", {}).get("id") != source.work["id"]
        or original.parts[-1].get("callback_group_id") != selected.group_id
        or original.parts[-1].get("message_ids") != [selected.message_id]
        or original.message_thread_id != selected.message_thread_id
    ):
        raise ProductSourceRetired()
    return {"view": "accept"} if selected.kind == "accept_open" else {"view": "prompt", "purpose": "changes"}


def check_review_origin(link, parent, callbacks, basis, source):
    origin = basis["origin"]
    selected = next((row for row in callbacks if row.id == origin["callback_id"]), None)
    if selected is None or parent.id != origin["anchor_outbox_id"] or parent.payload["presentation_version"] != origin["presentation_version"]:
        raise ProductSourceRetired()
    original = acknowledged_view(link, parent, origin["presentation_version"])
    group = [row for row in callbacks if row.group_id == selected.group_id]
    _validate_rows(original, group)
    if (
        review_transition(link, parent, selected, source) != basis["interaction"]
        or selected.message_id != origin["message_id"]
        or any(row.consumed_at is None or row.closed_reason != "work-preview" or row.message_id != origin["message_id"] for row in group)
    ):
        raise ProductSourceRetired()


class WorkViewReviews:
    def __init__(self, interactions, views):
        self.interactions, self.views = interactions, views
        self.outbox, self.callbacks = interactions.outbox, interactions.callbacks
        if not views or any(owner.outbox is not self.outbox or owner.callbacks is not self.callbacks for owner in views.values()):
            raise ValueError("work-detail-reviews-require-shared-owners")

    async def open(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id")) if callback else None
                if callback is None or parent is None:
                    raise CallbackRefused()
                item = _item(parent)
                plans = detail_card_owner(item, self.views)
            source, settings = await self.interactions.recovery._source(link, callback.target_id)
            state = review_transition(link, parent, callback, source)
            view = await plans.reader.read(link, item)
            full_detail(view)
            clock = await plans.reader.prepare_clock(link, item)
            prepared = await prepare_news_projection(plans.projection(plans.reader, link, item, view, clock=clock))
            projection = _ReviewProjection(prepared, link, item, callback_id, source)
            current = await prepare_attention(self.interactions.reader, link)
            composed = _ReviewGuard(guard, current, DigestProjection(link, [_RecoveryProjection(source), projection]))
            key = "notice:work-interaction:" + callback_id
            async with self.callbacks.writer(link, composed) as writer:
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if existing is not None:
                    origin = existing.payload.get("work_request_basis", {}).get("origin", {})
                    if (existing.binding_generation, existing.request_id, existing.chat_id, existing.message_thread_id, origin.get("callback_id"), origin.get("message_id")) != (
                        link.binding_generation,
                        source.request["id"],
                        chat_id,
                        message_thread_id,
                        callback_id,
                        message_id,
                    ):
                        raise CallbackRefused()
                    return existing.id
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                if review_transition(link, parent, group.selected, source) != state:
                    raise ProductSourceRetired()
                return await self.interactions._publish(writer, link, group, parent, source, settings, state, key=key, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed)

        return await await_drained(perform())


class _ReviewProjection:
    def __init__(self, delegate, link, item, callback_id, source):
        self.delegate, self.link, self.item = delegate, link, item
        self.callback_id, self.request_source = callback_id, source

    def extend(self, query):
        query = self.delegate.extend(query)
        self.delivery_column = self.delegate.delivery_column
        self.controls_column = len(query.column_descriptions)
        control = aliased(TelegramCallbackRow)
        return query.add_columns(control).outerjoin(control, control.group_id == self.item.parts[-1]["callback_group_id"])

    def check_with_permissions(self, rows, held):
        self.delegate.check_with_permissions(rows, held)
        parent = rows[0][self.delivery_column]
        controls = list({row[self.controls_column].id: row[self.controls_column] for row in rows if row[self.controls_column] is not None}.values())
        _validate_rows(parent, controls)
        selected = next((row for row in controls if row.id == self.callback_id), None)
        if selected is None:
            raise ProductSourceRetired()
        review_transition(self.link, parent, selected, self.request_source)
        if not (all(row.consumed_at is None for row in controls) or all(row.consumed_at is not None and row.closed_reason == "work-preview" for row in controls)):
            raise ProductSourceRetired()


class _ReviewGuard:
    def __init__(self, base, admission, projection):
        self.base, self.admission, self.projection = base, admission, projection

    def current(self):
        return self.base.current()

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.admission(session=writer, projection=self.projection)
        self.base.current()
        return current

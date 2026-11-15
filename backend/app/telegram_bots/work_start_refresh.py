"""A stale, unadmitted Start can only open a fresh read-only assignment preview."""

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_news_carriers import news_witness_id
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def require_unreserved(controls):
    if any(control.payload.get("work_action_id") is not None for control in controls):
        raise ProductSourceRetired()


async def unadmitted_start(writer, link, callback_id, work_id):
    if await writer.get(TelegramActRow, callback_id) is not None or await writer.get(TelegramCallbackRow, news_witness_id(callback_id)) is not None:
        raise ProductSourceRetired()
    if await writer.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == callback_id)) is not None:
        raise ProductSourceRetired()
    if await writer.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == callback_id)) is not None:
        raise ProductSourceRetired()


class UnadmittedStartProjection:
    def __init__(self, link, origin, item):
        self.link, self.origin, self.item = link, origin, item

    def extend(self, query):
        cleanup = aliased(TelegramOutboxRow)
        selected = aliased(TelegramCallbackRow)
        identity = self.origin["callback_id"]
        self.column = len(query.column_descriptions)
        return (
            query.add_columns(
                select(TelegramActRow.id).where(TelegramActRow.id == identity).exists(),
                select(TelegramCallbackRow.id).where(TelegramCallbackRow.id == news_witness_id(identity)).exists(),
                select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == identity).exists(),
                select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == identity).exists(),
                cleanup,
                selected,
            )
            .outerjoin(cleanup, cleanup.id == self.origin.get("cleanup_id"))
            .outerjoin(selected, selected.id == identity)
        )

    def check(self, rows):
        if any(value is not False for row in rows for value in row[self.column : self.column + 4]):
            raise ProductSourceRetired()
        from app.telegram_bots.work_request_source import cleanup_ready

        identity = self.origin.get("cleanup_id")
        if type(identity) is not str or len(identity) != 32 or any(char not in "0123456789abcdef" for char in identity):
            raise ProductSourceRetired()
        cleanup, selected = rows[0][self.column + 4 : self.column + 6]
        if (
            cleanup is None
            or selected is None
            or (cleanup.link_id, cleanup.owner_user_id, cleanup.binding_generation, cleanup.chat_id, cleanup.message_thread_id)
            != (self.item.link_id, self.item.owner_user_id, self.item.binding_generation, self.item.chat_id, self.item.message_thread_id)
            or (selected.link_id, selected.chat_id, selected.binding_generation, selected.message_thread_id) != (self.item.link_id, self.item.chat_id, self.item.binding_generation, self.item.message_thread_id)
            or cleanup.payload.get("target_message_id") != selected.message_id
            or (cleanup.work_id, cleanup.request_id, cleanup.event_id) != (None, None, None)
        ):
            raise ProductSourceRetired()
        ready = cleanup_ready(self.item, cleanup, identity=identity)
        if not ready and any(part.get("state") == "sending" for part in self.item.parts):
            from datetime import timedelta

            from app.telegram_bots.producer import DeliveryRetry

            raise DeliveryRetry(timedelta(seconds=1))


async def refresh_origin_projection(reader, link, item, *, trail=(), activation_id=None, origin=None):
    """Use the real original carrier, never a Work-view-shaped substitute."""
    from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature

    origin = item.payload["work_view_intent"]["origin"] if origin is None else origin
    async with reader.links.session_factory() as session:
        parent = await session.get(TelegramOutboxRow, origin.get("anchor_outbox_id"))
        if parent is None or parent.id in trail or len(trail) >= 32:
            raise ProductSourceRetired()
        original = acknowledged_view(link, parent, origin.get("presentation_version"), readonly_start_refresh=True)
        if anchor_signature(original) != origin.get("anchor_signature"):
            raise ProductSourceRetired()
    return await prepare_start_carrier(reader, link, parent, original, trail=(*trail, item.id), activation_id=activation_id)


async def prepare_start_carrier(reader, link, parent, original, *, trail=(), activation_id=None):
    """Prepare the actual acknowledged carrier before a new read-only write."""
    from app.telegram_bots.producer import _item
    from app.telegram_bots.work_news_carriers import news_origin_projection
    from app.telegram_bots.work_news_sources import WorkNewsRead
    from app.telegram_bots.work_status_control_origins import carrier_source
    from app.telegram_bots.work_view_sources import WorkViewProjection

    current = _item(parent)
    source = WorkNewsRead(**carrier_source(original, readonly_start_refresh=True))
    if source.key == "view.detail":
        projection = WorkViewProjection(reader, link, current, source, clock=await reader.prepare_clock(link, current), origin_only=True)
        await projection.prepare_source(trail=trail, activation_id=activation_id)
        return projection
    clock = await reader.prepare_clock(link, current)
    if source.key == "capacity.offer":
        from app.telegram_bots.work_capacity_sources import WorkCapacityOfferProjection

        projection = WorkCapacityOfferProjection(reader, link, current, source, clock=clock, origin_only=True, activation_id=activation_id)
        await projection.prepare_source()
        return projection
    if source.key == "assignment.card":
        from app.telegram_bots.assignment_sources import AssignmentProjection, AssignmentReader

        return AssignmentProjection(AssignmentReader(reader.acts), link, current, source, clock=clock, origin_only=True)
    if source.key in {"management.edit", "management.withdraw"}:
        from app.telegram_bots.work_management_sources import WorkManagementProjection, WorkManagementReader

        return WorkManagementProjection(WorkManagementReader(reader.acts), link, current, source, clock=clock, origin_only=True)
    if source.key in {"execution.activate", "execution.stop"}:
        from app.telegram_bots.work_execution_sources import WorkExecutionProjection, WorkExecutionReader

        owner = WorkExecutionReader(work=reader.work, authority=reader.authority, links=reader.links, clock=reader.clock)
        owner.acts, owner.product = reader.acts, source.work["product"]
        projection = WorkExecutionProjection(owner, link, current, source, clock=await owner.prepare_clock(link, current), origin_only=True)
        await projection.prepare_source()
        return projection
    projection = await news_origin_projection(reader, link, parent, original, activation_id=activation_id, historical=True, readonly_start_refresh=True)
    if projection is None:
        raise ProductSourceRetired()
    return projection


def waiting_act(source):
    if source["key"] == "capacity.offer":
        return source["work"]["waiting_act"]["id"]
    if source["key"] == "view.detail" and source["work"].get("origin", {}).get("refusal") == "start.changed":
        return source["work"]["origin"].get("waiting_act_id")
    return None


async def original_capacity_offer(writer, link, original):
    from app.telegram_bots.work_status_control_origins import carrier_source
    from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature

    seen = set()
    expected = waiting_act(carrier_source(original))
    while carrier_source(original)["key"] == "view.detail":
        if original.id in seen or len(seen) >= 32:
            raise ProductSourceRetired()
        seen.add(original.id)
        origin = carrier_source(original)["work"].get("origin", {})
        if origin.get("refusal") != "start.changed" or origin.get("waiting_act_id") != expected:
            raise ProductSourceRetired()
        parent = await writer.get(TelegramOutboxRow, origin.get("anchor_outbox_id"), populate_existing=True)
        if parent is None:
            raise ProductSourceRetired()
        original = acknowledged_view(link, parent, origin.get("presentation_version"), readonly_start_refresh=True)
        if anchor_signature(original) != origin.get("anchor_signature"):
            raise ProductSourceRetired()
    if carrier_source(original)["key"] != "capacity.offer" or waiting_act(carrier_source(original)) != expected:
        raise ProductSourceRetired()
    return original

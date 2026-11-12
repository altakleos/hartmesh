"""Join whole-cohort ownership to the final account, grant and policy snapshot."""

from copy import deepcopy
from datetime import UTC, datetime

from sqlalchemy import JSON, Boolean, DateTime, func, literal, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRepository, InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_album_inputs import input_packet, inputs_digest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def normalized(value):
    if isinstance(value, datetime):
        return (value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)).isoformat()
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalized(item) for item in value]
    return value


def _rows(packets, model):
    if packets is None:
        return []
    if type(packets) is not list:
        raise CallbackRefused()
    for packet in packets:
        for column in model.__table__.columns:
            value = packet.get(column.name)
            if value is not None and isinstance(column.type, DateTime):
                packet[column.name] = datetime.fromisoformat(value) if type(value) is str else value
            elif value is not None and isinstance(column.type, Boolean):
                packet[column.name] = bool(value)
    return packets


class AlbumGuard:
    def __init__(self, base, origins, *, reader, callback_id=None):
        self.base, self.captured_link, self.reader = base, base.captured_link, reader
        self.origins = tuple(origins)
        self.inputs = inputs_digest([input_packet(mark) for mark in origins])
        self.callback_id = callback_id
        self.expected = {mark.message_id: mark for mark in origins}
        self.allow_finish, self.admission = False, None

    def current(self):
        self.base.current()

    async def prepare(self):
        await prepare_publication_guard(self.base)
        self.admission = await prepare_attention(self.reader, self.captured_link, require_inspect=False)
        self.current()

    def expect(self, origins):
        if inputs_digest([input_packet(mark) for mark in origins]) != self.inputs:
            raise CallbackRefused()
        self.expected = {mark.message_id: mark for mark in origins}

    async def check(self, writer):
        projection = AlbumProjection(self, sqlite=writer.get_bind().dialect.name == "sqlite")
        # Capture this phase's exact controls and delivery bookkeeping before
        # any provider await. Final authority rereads the same compact facts.
        projection.snapshot = projection.decode((await writer.execute(projection.extend(select(literal(1))))).one())
        projection.validate(projection.snapshot)
        current = await self.base.check(writer)
        if self.admission is None:
            raise CallbackRefused()
        await self.admission(session=writer, projection=projection)
        self.current()
        return current


class AlbumProjection:
    def __init__(self, guard, *, sqlite):
        self.guard, self.sqlite, self.snapshot = guard, sqlite, None

    def aggregate(self, row, model, names=None):
        args = []
        for column in model.__table__.columns:
            if names is not None and column.name not in names:
                continue
            field = getattr(row, "class_" if column.name == "class" else column.name)
            args.extend((column.name, func.json(field) if self.sqlite and isinstance(column.type, JSON) else field))
        packet = func.json_object(*args) if self.sqlite else func.json_build_object(*args)
        return func.json_group_array(packet, type_=JSON) if self.sqlite else func.json_agg(packet, type_=JSON)

    def extend(self, query):
        guard, first = self.guard, self.guard.origins[0]
        mark, callback, parent = aliased(TelegramInboundMarkRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        marks = select(self.aggregate(mark, TelegramInboundMarkRow, InboxRow.__dataclass_fields__)).where(mark.link_id == first.link_id, mark.chat_id == first.chat_id, mark.message_id.in_(guard.expected)).scalar_subquery()
        group_id = select(TelegramCallbackRow.group_id).where(TelegramCallbackRow.id == guard.callback_id).scalar_subquery()
        controls = select(self.aggregate(callback, TelegramCallbackRow)).where(callback.group_id == group_id).scalar_subquery()
        sources, operations = set(), set()
        for origin in guard.origins:
            operations.update(origin.operation_ids)
        for origin in guard.expected.values():
            context = origin.route_basis.get("work_album_recovery", {})
            sources.update(identity for identity in (context.get("notice_id"), context.get("source_outbox_id")) if identity)
            operations.update(context.get("operation_ids", []))
            operations.update(origin.operation_ids)
        parents = select(self.aggregate(parent, TelegramOutboxRow)).where(parent.id.in_(sources)).scalar_subquery()
        blocked = select(func.count()).select_from(TelegramActRow).where(TelegramActRow.id.in_(operations)).scalar_subquery()
        return query.add_columns(marks, controls, parents, blocked)

    def decode(self, row):
        marks, controls, parents, blocked = row[-4:]
        return normalized(
            {
                "inputs": sorted(_rows(marks, TelegramInboundMarkRow), key=lambda item: item["message_id"]),
                "controls": sorted(_rows(controls, TelegramCallbackRow), key=lambda item: item["id"]),
                "parents": sorted(_rows(parents, TelegramOutboxRow), key=lambda item: item["id"]),
                "blocked": blocked,
            }
        )

    def validate(self, snapshot):
        guard = self.guard
        indexed = {packet["message_id"]: packet for packet in snapshot["inputs"]}
        if snapshot["blocked"] or set(indexed) != set(guard.expected):
            raise CallbackRefused()
        for original in guard.origins:
            packet = deepcopy(indexed[original.message_id])
            for key in ("accepted_at", "closed_at"):
                if packet[key] is not None:
                    packet[key] = datetime.fromisoformat(packet[key])
            mark = InboxRow(**packet)
            expected = guard.expected[original.message_id]
            if not same_body(input_packet(mark), input_packet(original)) or normalized(mark.accepted_at) != normalized(original.accepted_at):
                raise CallbackRefused()
            InboxRepository._publication_snapshot_matches(mark, expected)
            if (mark.route_complete != expected.route_complete and not guard.allow_finish) or "work_action_id" in mark.route_basis or any(getattr(mark, key) is not None for key in ("turn_id", "thread_id", "run_id", "assistant_message_id")):
                raise CallbackRefused()

    def check(self, rows):
        snapshot = self.decode(rows[0])
        self.validate(snapshot)
        if self.snapshot is not None and not same_body(snapshot, self.snapshot):
            raise CallbackRefused()

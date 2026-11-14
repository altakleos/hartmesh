"""Stop choices retain the command while current Work sources fence delivery."""

from dataclasses import asdict
from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_command_stop_origins import command_basis
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_news_delivery import WorkNewsPlans
from app.telegram_bots.work_news_recovery import WorkNewsRecovery
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_sources import WorkViewReader, detail_read
from app.telegram_bots.work_views import WorkViewPresentation
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def choice_intent(item):
    intent = item.payload.get("command_stop_intent")
    if (
        type(intent) is not dict
        or set(intent) != {"version", "message_id", "chat_id", "signature"}
        or type(intent["version"]) is not int
        or intent["version"] != 1
        or type(intent["message_id"]) is not int
        or intent["message_id"] < 1
        or intent["chat_id"] != item.chat_id
    ):
        raise ProductSourceRetired()
    return intent


def present_stop_choices(source, basis, link):
    keyboard = []
    if source.work["basis"]["answer"] is not None:
        keyboard.append([{"label": "label.stop_answer", "kind": "stop_which", "target_kind": "link", "target_id": link.id, "payload": {"choice": "answer"}, "expires_at": source.work["expires_at"]}])
    for target, detail in zip(source.work["basis"]["targets"], source.work["details"], strict=True):
        objective = " ".join(detail["record"]["objective"].split())
        title = objective if len(objective) <= 24 else objective[:24] + "…"
        keyboard.append(
            [
                {
                    "label": "label.stop_assignment",
                    "label_values": {"objective": title},
                    "kind": "stop_which",
                    "target_kind": "link",
                    "target_id": link.id,
                    "payload": {"choice": "attempt:" + target["attempt_id"]},
                    "expires_at": source.work["expires_at"],
                }
            ]
        )
    keyboard.append([{"label": "label.stop_everything", "kind": "stop_which", "target_kind": "link", "target_id": link.id, "payload": {"choice": "all"}, "expires_at": source.work["expires_at"]}])
    parts = tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(render("stop.which", basis["locale"])))
    return WorkViewPresentation(parts, tuple(tuple(line) for line in canonical_keyboard(keyboard)))


class CommandStopChoiceReader(WorkViewReader):
    async def read(self, link, item):
        async def perform():
            intent = choice_intent(item)
            async with self.links.session_factory() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (link.id, intent["message_id"], link.chat_id))
                if mark is None:
                    raise ProductSourceRetired()
                basis = command_basis(mark)
            details = [await self.detail(link, target["work_id"]) for target in basis["targets"]]
            clock = await self.prepare_clock(link, item)
            projection = CommandStopChoiceProjection(self, link, item, basis=basis, details=details, clock=clock)
            prepared = await prepare_attention(self, link)
            await prepared(projection=projection)
            return projection.source

        return await await_drained(perform())


class CommandStopChoiceProjection:
    def __init__(self, reader, link, item, source=None, *, basis=None, details=None, clock):
        self.reader, self.link, self.item, self.expected, self.clock = reader, link, item, source, clock
        self.intent = choice_intent(item)
        self.basis = thaw(source.work["basis"]) if source is not None else basis
        details = [detail_read(value) for value in source.work["details"]] if source is not None else details
        self.details = [WorkDetailProjection(reader, link, value.record, value.request, reusable=value.can_reuse_conversation, expected=value) for value in details]

    def extend(self, query):
        first = self.details[0]
        result = first.extend(query, work_ids=[value.record["id"] for value in self.details])
        for value in self.details:
            value.column = first.column
        self.column = len(result.column_descriptions)
        self.delivery_column = self.column
        delivery, mark, settings = aliased(TelegramOutboxRow), aliased(TelegramInboundMarkRow), aliased(TelegramPersonSettingsRow)
        return (
            result.add_columns(delivery, mark, settings)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(mark, and_(mark.link_id == self.link.id, mark.chat_id == self.link.chat_id, mark.message_id == self.intent["message_id"]))
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
        )

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        delivery, mark, settings = rows[0][self.column : self.column + 3]
        if (
            mark is None
            or not same_body(command_basis(mark), self.basis)
            or self.basis["selection"] is not None
            or self.intent["signature"] != digest(self.basis)
            or _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.message_thread_id, delivery.instance_id, delivery.work_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, mark.message_thread_id, self.link.agent_ref, None, "reply")
            or settings.timezone != self.clock.settings.timezone
        ):
            raise ProductSourceRetired()
        details = []
        for value in self.details:
            matching = [row for row in rows if row[value.column].id == value.record["id"]]
            if not matching:
                raise ProductSourceRetired()
            value.check_with_permissions(matching, held)
            details.append(value.source)
        expires = aware(mark.accepted_at) + timedelta(days=self.reader.links.config.callbacks.max_age_days)
        data = thaw({"intent": self.intent, "basis": self.basis, "details": tuple(asdict(value) for value in details), "timezone": settings.timezone, "expires_at": expires.isoformat()})
        source = WorkNewsRead(None, "command.stop", {}, any(value.protected for value in details), data, digest([self.link.id, self.link.binding_generation, self.intent]), digest(data))
        if self.expected is not None and not same_body(asdict(source), asdict(self.expected)):
            raise ProductSourceRetired()
        self.source = source


class CommandStopChoicePlans(WorkNewsPlans):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, projection=CommandStopChoiceProjection)
        self.navigation = None
        self.recovery = WorkNewsRecovery(self)

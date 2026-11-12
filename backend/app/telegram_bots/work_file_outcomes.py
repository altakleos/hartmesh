"""Frozen, admitted file-answer evidence for the canonical response delivery."""

from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import JSON, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_card_inputs import response
from app.telegram_bots.work_file_inputs import saved_file_text
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

_FIELDS = ("route_basis", "sanitized_input", "operation_ids", "route_kind", "route_target_id", "message_id", "update_id", "binding_generation", "message_thread_id")


@dataclass(frozen=True)
class FileAnswerProof:
    sessions: object
    mark: object
    packet: dict
    shown: dict

    def expression(self):
        row = aliased(TelegramInboundMarkRow)
        sqlite = self.sessions.kw["bind"].dialect.name == "sqlite"
        args = []
        for name in _FIELDS:
            column = getattr(row, name)
            args.extend((name, func.json(column) if sqlite and isinstance(column.type, JSON) else column))
        value = func.json_object(*args, type_=JSON) if sqlite else func.json_build_object(*args, type_=JSON)
        return select(value).where(row.link_id == self.mark.link_id, row.chat_id == self.mark.chat_id, row.message_id == self.mark.message_id).scalar_subquery().label("work_file_evidence")

    def check(self, packet):
        if not same_body(packet, self.packet):
            raise ProductSourceRetired()


async def file_answer_proof(host, link, act):
    if act.kind != "respond" or act.state != "done":
        return None
    row = TelegramInboundMarkRow
    async with host.requests.links.session_factory() as reader:
        marks = list(await reader.scalars(select(row).where(row.link_id == link.id, row.chat_id == link.chat_id, row.binding_generation == link.binding_generation, row.route_basis["work_action_id"].as_string() == act.id).limit(2)))
    if not marks or marks[0].route_basis.get("work_input", {}).get("version") != 4:
        if act.message_id is not None:
            raise ProductSourceRetired()
        return None
    if len(marks) != 1:
        raise ProductSourceRetired()
    mark = marks[0]
    data = mark.route_basis["work_input"]
    saved_file_text(mark)
    if (
        act.message_id != mark.message_id
        or mark.route_basis["work_file"]["target"]["instance_id"] != act.instance_id
        or mark.route_kind != "work_input"
        or data["request_id"] != act.request_id
        or data["work_id"] != act.work_id
        or mark.route_target_id != act.request_id
        or mark.operation_ids != [act.id]
        or not same_body(response(mark).model_dump(mode="json", exclude_unset=True), act.body)
    ):
        raise ProductSourceRetired()
    frozen = mark.route_basis["work_file"]
    shown = {"file": frozen["result"]["path"].rsplit("/", 1)[-1], "caption": data["text"]}
    return FileAnswerProof(host.requests.links.session_factory, InboxRow.from_row(mark), {name: deepcopy(getattr(mark, name)) for name in _FIELDS}, shown)

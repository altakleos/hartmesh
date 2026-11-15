"""Distinct actual callback inputs for private drafts; never synthetic inboxes."""

from collections.abc import Mapping
from dataclasses import dataclass

from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow


def callback_origin(origin):
    return isinstance(origin, Mapping) and origin.get("kind") == "result_suggestion"


@dataclass(frozen=True)
class CallbackAssignmentInput:
    origin: dict

    @property
    def link_id(self):
        return self.origin["link_id"]

    @property
    def chat_id(self):
        return self.origin["chat_id"]

    @property
    def binding_generation(self):
        return self.origin["binding_generation"]

    @property
    def message_id(self):
        return self.origin["message_id"]

    @property
    def message_thread_id(self):
        return self.origin["message_thread_id"]


async def load_assignment_input(session, origin):
    if callback_origin(origin):
        return CallbackAssignmentInput(origin)
    row = await session.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"]))
    if row is None:
        raise ProductSourceRetired()
    return InboxRow.from_row(row)

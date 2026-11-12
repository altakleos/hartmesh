"""Locate the latest acknowledged Work question before an untied human input."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select

from app.telegram_bots.callbacks import CallbackRefused, _aware, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_requests import WorkRequestRead
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class UntiedQuestion:
    outbox_id: str
    presentation_version: int
    group_id: str
    source: WorkRequestRead
    origin: InboxRow


class WorkUntiedSource:
    def __init__(self, plans):
        self.plans, self.inbox, self.callbacks = plans, plans.outbox.inbox, plans.callbacks

    async def find(self, link, origin, *, guard, session=None):
        """Return a locator, never route text or infer consent to save it."""

        async def perform():
            async with self.callbacks.writer(link, guard, session=session) as writer:
                mark = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, mark)
                raw = mark.sanitized_input
                if link.agent_kind != "instance" or mark.route_kind is not None or type(raw) is not dict or set(raw) - {"type", "text"} or raw.get("type", "text") != "text" or type(raw.get("text")) is not str or not raw["text"].strip():
                    return None
                previous = await writer.scalar(
                    select(func.max(TelegramInboundMarkRow.message_id)).where(
                        TelegramInboundMarkRow.link_id == link.id,
                        TelegramInboundMarkRow.chat_id == mark.chat_id,
                        TelegramInboundMarkRow.message_thread_id == mark.message_thread_id,
                        TelegramInboundMarkRow.binding_generation == link.binding_generation,
                        TelegramInboundMarkRow.message_id < mark.message_id,
                    )
                )
                after, latest, parent, latest_part = None, None, None, None
                while True:
                    query = select(TelegramOutboxRow).where(
                        TelegramOutboxRow.link_id == link.id,
                        TelegramOutboxRow.owner_user_id == link.owner_user_id,
                        TelegramOutboxRow.binding_generation == link.binding_generation,
                        TelegramOutboxRow.chat_id == mark.chat_id,
                        TelegramOutboxRow.message_thread_id == mark.message_thread_id,
                    )
                    if after is not None:
                        query = query.where(TelegramOutboxRow.id > after)
                    page = list(await writer.scalars(query.order_by(TelegramOutboxRow.id).limit(100)))
                    for row in page:
                        if row.payload.get("kind") == "keyboard_cleanup":
                            continue
                        for part in row.parts:
                            # Removing only markup does not replace the question.
                            if part.get("kind") == "keyboard" or part.get("state") != "sent":
                                continue
                            for message_id in part.get("message_ids", []):
                                if type(message_id) is not int or message_id <= 0:
                                    raise CallbackRefused()
                                if message_id >= mark.message_id:
                                    continue
                                stamp = datetime.fromisoformat(part["sent_at"])
                                if stamp.utcoffset() is None:
                                    raise CallbackRefused()
                                if stamp > _aware(mark.accepted_at):
                                    continue
                                position = (message_id, stamp)
                                if latest is None or position > latest:
                                    latest, parent, latest_part = position, row, part
                                elif position == latest and parent.id != row.id:
                                    raise CallbackRefused()
                    if len(page) < 100:
                        break
                    after = page[-1].id
                if parent is None or previous is not None and previous >= latest[0]:
                    return None
                basis = parent.payload.get("work_request_basis", {})
                comment = parent.payload.get("work_outcome_basis", {}).get("version") == 5
                if (not comment and basis.get("version") not in {1, 2, 3, 4}) or basis.get("interaction", {}).get("view") == "prompt" or parent.instance_id != link.agent_ref:
                    return None
                if latest_part["presentation_version"] != parent.payload["presentation_version"] or parent.state != "sent":
                    return None
                if basis.get("concealed") is True:
                    return None
                source = acknowledged_source(link, parent)
                parts = [part for part in parent.parts if part["presentation_version"] == parent.payload["presentation_version"]]
                if any(_aware(datetime.fromisoformat(part["sent_at"])) > _aware(mark.accepted_at) for part in parts):
                    return None
                groups = {part.get("callback_group_id") for part in parts} - {None}
                if len(groups) != 1:
                    return None
                controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups))))
                _validate_rows(parent, controls)
                if any(row.consumed_at is not None or "work_action_id" in row.payload for row in controls):
                    return None
                try:
                    for control in controls:
                        self.callbacks._usable(control)
                except CallbackRefused:
                    return None
                return UntiedQuestion(parent.id, parent.payload["presentation_version"], next(iter(groups)), source, InboxRow.from_row(mark))

        return await await_drained(perform())

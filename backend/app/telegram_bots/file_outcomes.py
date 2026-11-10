"""Fixed direct-file replies and retry controls over original journal evidence."""

import asyncio

from sqlalchemy import select

from app.telegram_bots.callbacks import _topic
from app.telegram_bots.inbox import chat_input_parts, validate_chat_file
from app.telegram_bots.pages import page_address
from app.telegram_bots.publication import failure_button, fixed_source
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def _key(row, file):
    if row.state == "done":
        if row.result["deduplicated"]:
            return "file.same"
        return "file.saved_as" if row.result["path"].rsplit("/", 1)[-1] != file["filename"] else None
    if row.state not in {"refused", "uncertain"}:
        raise ValueError("file-outcome-unsettled") from None
    if row.state == "uncertain":
        return "file.unconfirmed"
    return {"files-full": "file.files_full", "storage-busy": "file.files_busy", "agent-not-active": "file.not_active", "invalid-name": "file.bad_name", "file-too-large": "file.too_large_in"}.get(row.reason, "file.not_saved")


def _basis(row):
    return dict(version=1, act_id=row.id, operation_id=row.operation_ids[-1], state=row.state, reason=row.reason, result=dict(row.result) if row.result is not None else None)


def _retry_source(row, callback, link):
    if callback.kind != "file_retry" or callback.target_kind != "act" or callback.target_id != row.id or callback.chat_id != link.chat_id:
        raise ValueError("file-retry-source-conflict") from None
    presses = row.continuation["presses"]
    press = dict(callback_id=callback.id, message_id=callback.message_id, topic=_topic(callback.message_thread_id) if row.continuation["version"] == 2 else callback.message_thread_id)
    if not presses or press not in presses:
        raise ValueError("file-retry-source-conflict") from None
    return press == presses[-1]


class FileOutcomes:
    def __init__(self, services):
        self.services = services

    async def plan(self, channel, link, origin, file, *, current=None, callback=None):
        file = validate_chat_file(file)
        if (origin is None) != (callback is not None):
            raise ValueError("file-outcome-source-conflict") from None

        def live():
            return channel._callback_current(link) and (current is None or current() is True)

        async def perform():
            reader = await self.services.file_puts.receipt_reader(link, authority=self.services.authority, current=live)
            captured = await reader.outcome(file)
            if callback is not None and not _retry_source(captured, callback, link):
                return None  # An old press never republishes a later child's outcome.
            topic_id = origin.message_thread_id if origin is not None else callback.message_thread_id
            key = _key(captured, file)
            if callback is not None and key is None:
                key = "file.saved_ask" if captured.continuation.get("input", {}).get("captionless") is True else "file.saved"
            if key is None:
                return None
            source = await fixed_source(self.services, link, key)
            source["values"].update(file=captured.result["path"].rsplit("/", 1)[-1] if captured.result else captured.body["name"], product=self.services.product_name)
            button = await failure_button(self.services, link, key)
            if key == "file.too_large_in" and button is None:
                topic = str(topic_id) if topic_id not in (None, 1) else None
                # A size refusal never creates a conversation just for a link.
                thread = await self.services.authority.connections.get_thread_id(link.connection_id, str(link.chat_id), topic)
                if thread is not None:
                    address = await await_drained(asyncio.to_thread(page_address, "web.conversation", link, thread_id=thread))
                    button = {"page": "web.conversation", "url": address} if address else None
            basis = _basis(captured)
            if callback is not None:
                basis["callback_id"] = callback.id
            identity = f"file-put:{captured.operation_ids[-1]}"
            guard = reader.guard()
            async with self.services.callbacks.writer(link, guard) as writer:
                actual = await reader.outcome(file, session=writer)
                if actual != captured:
                    raise ValueError("file-outcome-changed") from None
                if callback is not None:
                    if not _retry_source(actual, callback, link):
                        raise ValueError("file-retry-source-conflict") from None
                else:
                    mark = await writer.get(TelegramInboundMarkRow, (link.id, origin.message_id, link.chat_id), with_for_update=True, populate_existing=True)
                    if mark is None:
                        raise ValueError("file-origin-unavailable") from None
                    self.services.file_puts.inbox._publication_snapshot_matches(mark, origin)
                    _text, descriptors = chat_input_parts(origin, bot_id=link.bot_id)
                    if file not in descriptors or file["message_id"] != mark.message_id:
                        raise ValueError("file-origin-conflict") from None
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:" + identity).with_for_update())
                if existing is not None:
                    if existing.owner_user_id != link.owner_user_id or existing.binding_generation != link.binding_generation or existing.chat_id != link.chat_id or existing.payload.get("file_put_basis") != basis:
                        raise ValueError("file-outcome-conflict") from None
                    # Frozen wording and controls survive display-name/grant
                    # changes; delivery still checks their actual authority.
                    return existing.id
                keyboard = []
                if key in {"file.not_saved", "file.files_busy", "file.unconfirmed"}:
                    keyboard = [[dict(label="label.send_again", kind="file_retry", target_kind="act", target_id=actual.id, payload={"act_id": actual.id})]]
                return await self.services.outbox._plan_product_keyboard(
                    link,
                    source_id=identity,
                    parts=[{"kind": "text", "source_ref": source, "reply_to_message_id": file["message_id"]}],
                    keyboard=keyboard,
                    callbacks=self.services.callbacks,
                    guard=guard,
                    inbox_rows=[origin] if origin is not None else [],
                    message_thread_id=topic_id,
                    session=writer,
                    _url_button=button,
                    _extra={"file_put_basis": basis},
                )

        return await await_drained(perform())

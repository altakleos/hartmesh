"""Held-startup file reconciliation: metadata only, never resumed file effects."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

from sqlalchemy import String, cast, select

from app.telegram_bots.file_outcomes import FileOutcomes
from app.telegram_bots.inbox import chat_input_parts
from app.telegram_bots.instance_file_authority import _SCOPE
from app.telegram_bots.turns import _chat_line, _Guard
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.utils.file_io import await_drained

_ISSUER = object()


class _Recovery:
    def __init__(self, issuer, host, channel, link, phase):
        if issuer is not _ISSUER:
            raise ValueError("file-recovery-unavailable") from None
        self.host, self.channel, self.link, self.phase = host, channel, deepcopy(link), phase
        self.turns, self.loop, self.active = host.turns, asyncio.get_running_loop(), True

    def current(self):
        return (
            self.active
            and asyncio.get_running_loop() is self.loop
            and self.phase() is True
            and self.host.turns is self.turns
            and self.host.channel(self.link.id) is self.channel
            and self.channel._same_scope(self.link, self.channel.link)
            and self.channel.transport_revision == self.link.token_revision
            and not self.channel._stopping
            and not self.channel._file_tasks
            and not any(turn.link.id == self.link.id for turn in self.turns._turns.values())
        )


def check_recovery(ticket, host, channel, link):
    if type(ticket) is not _Recovery or ticket.host is not host or ticket.channel is not channel or any(getattr(ticket.link, key) != getattr(link, key) for key in _SCOPE):
        raise ValueError("file-recovery-unavailable") from None
    return ticket.current()


async def recover_input(host, channel, link, entry, phase):
    """Recover original file outcomes before the generic interrupted chat reply."""
    if entry.input_owner_callback_id is not None or link.agent_kind != "instance":
        return False
    pairs, texts = [], []
    for origin in entry.origins:
        text, files = chat_input_parts(origin, bot_id=link.bot_id)
        texts.append(text)
        pairs.extend((origin, file) for file in files if file["message_id"] == origin.message_id)
    if not pairs:
        return False
    ticket = _Recovery(_ISSUER, host, channel, link, phase)
    outcomes, saved = FileOutcomes(host), []
    try:
        for origin, file in pairs:
            row = await host.file_puts.reconcile_input(link, origin, file, origins=entry.origins, authority=host.authority, current=ticket.current)
            await outcomes.plan(channel, link, origin, file, _recovery=ticket)
            if row.state == "done":
                saved.append((origin, file))
        if any(text.strip() for text in texts):
            return False  # The caption keeps the ordinary explicit chat retry.
        if saved:
            await outcomes.question(channel, link, saved, _recovery=ticket)
        guard = _Guard(host, channel, link, ticket.current, repair=True)
        await host.outbox.inbox.close_unpublished_input(link, entry.origins, guard=guard)
        return True
    finally:
        ticket.active = False


async def recover_retries(host, phase, *, link_id=None):
    """A recorded press owns its result even after input/control collection."""
    after = ""
    while phase():

        async def read_page():
            async with host.links.session_factory() as reader:
                statement = select(TelegramActRow.id, TelegramActRow.link_id).where(
                    TelegramActRow.kind == "file_put",
                    TelegramActRow.id > after,
                    cast(TelegramActRow.continuation["version"].as_string(), String).in_(("3", "4")),
                    TelegramActRow.continuation["delivery"]["operation_id"].as_string().is_(None),
                )
                if link_id is not None:
                    statement = statement.where(TelegramActRow.link_id == link_id)
                return list((await reader.execute(statement.order_by(TelegramActRow.id).limit(32))).all())

        try:
            page = await await_drained(read_page())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _chat_line("file_restart_page_held", error=error)
            return
        for identity, owner_link_id in page:
            if not phase():
                return
            ticket = None
            try:
                link = await await_drained(host.links.get(owner_link_id))
                if link is None or link.agent_kind != "instance" or (channel := host.channel(link.id)) is None:
                    continue
                ticket = _Recovery(_ISSUER, host, channel, link, phase)
                reader = await host.file_puts.receipt_reader(link, authority=host.authority, current=ticket.current)
                row = await reader.act(identity)
                if row.continuation["version"] not in {3, 4} or row.continuation["delivery"] is not None or not row.continuation["presses"]:
                    continue
                if row.continuation["version"] == 4 and row.state == "done":
                    # The retained Work input owns Respond and its named-file
                    # acknowledgement. File recovery never substitutes chat UI.
                    continue
                press = row.continuation["presses"][-1]
                callback = SimpleNamespace(kind="file_retry", target_kind="act", target_id=row.id, chat_id=link.chat_id, id=press["callback_id"], message_id=press["message_id"], message_thread_id=press["topic"])
                await host.file_puts.settle_failed_retry(link, row.id, callback_id=callback.id, authority=host.authority, current=ticket.current)
                await FileOutcomes(host).plan(channel, link, None, dict(row.continuation["file"]), callback=callback, _recovery=ticket)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                _chat_line("file_restart_held", error=error, link=owner_link_id)
            finally:
                if ticket is not None:
                    ticket.active = False
        if len(page) < 32:
            return
        after = page[-1][0]

"""In-memory waiting order, without model, publication or authorization ownership.

The controller owns each turn's single completion and durable input receipts.
Transport delivery never keeps this queue occupied after final planning.
"""

from __future__ import annotations

import re
from copy import deepcopy
from dataclasses import dataclass, field, replace

from app.channels.message_bus import InboundMessage, InboundMessageType

_SCOPE = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
_SOURCES = "telegram_source_message_ids"


def _refuse():
    raise ValueError("managed-queue-refused") from None


def _scope(link):
    try:
        scope = tuple(getattr(link, name) for name in _SCOPE)
        if any(type(getattr(link, name)) is not str or not getattr(link, name) for name in ("id", "owner_user_id", "agent_kind", "agent_ref", "connection_id")):
            _refuse()
        if any(type(getattr(link, name)) is not int or getattr(link, name) <= 0 for name in ("bot_id", "telegram_user_id", "chat_id")) or link.chat_id != link.telegram_user_id:
            _refuse()
        if any(type(getattr(link, name)) is not int or getattr(link, name) < 0 for name in ("binding_generation", "token_revision")):
            _refuse()
        return scope
    except AttributeError:
        _refuse()


def _topic(value, *, string=False):
    if value is None:
        return None
    if string and type(value) is str and value.isascii() and value.isdecimal() and value == str(int(value)):
        value = int(value)
    if type(value) is not int or value <= 0:
        _refuse()
    return None if value == 1 else value


def _message_topic(message):
    explicit = "message_thread_id" in message.metadata
    topic = _topic(message.metadata.get("message_thread_id"))
    if message.topic_id is not None:
        inherited = _topic(message.topic_id, string=True)
        if explicit and inherited != topic:
            _refuse()
        topic = inherited
    return topic


def _sources(message):
    identity = message.metadata.get("message_id")
    if type(identity) is not int or identity <= 0 or message.thread_ts != str(identity):
        _refuse()
    identities = message.metadata.get(_SOURCES, [identity])
    if type(identities) is not list or not identities or identities[0] != identity or any(type(value) is not int or value <= 0 for value in identities) or len(set(identities)) != len(identities):
        _refuse()
    return tuple(identities)


def _copy(message):
    return replace(message, files=deepcopy(message.files), metadata=deepcopy(message.metadata))


@dataclass
class QueuedInput:
    inbound: InboundMessage
    callback_id: str | None = None
    separate: bool = False


@dataclass
class _Turn:
    scope: tuple
    current: InboundMessage
    waiting: list[QueuedInput] = field(default_factory=list)
    callback_id: str | None = None
    retrying: bool = False
    cancelled: bool = False


class ChatQueue:
    def __init__(self, config):
        self.depth = config.queue.depth
        self._turns: dict[tuple[str, int | None], _Turn] = {}
        self._closed = False

    @staticmethod
    def _input(link, inbound):
        scope = _scope(link)
        if (
            type(inbound) is not InboundMessage
            or inbound.msg_type != InboundMessageType.CHAT
            or inbound.channel_name != "telegram"
            or (inbound.owner_user_id, inbound.connection_id, inbound.chat_id, inbound.user_id, inbound.workspace_id) != (link.owner_user_id, link.connection_id, str(link.chat_id), str(link.telegram_user_id), str(link.bot_id))
            or type(inbound.text) is not str
            or type(inbound.files) is not list
            or any(type(value) is not dict for value in inbound.files)
            or type(inbound.metadata) is not dict
            or inbound.execution_required is not False
            or inbound.execution_context is not None
        ):
            _refuse()
        _sources(inbound)
        return scope, (link.id, _message_topic(inbound))

    def admit(self, link, inbound):
        return self._admit(link, inbound, callback_id=None)

    def admit_submission(self, link, inbound):
        """Keep one durably staged typed response separate in FIFO order.

        The controller, rather than message metadata, selects this boundary.
        The queue neither validates nor grants authority to a response.
        """
        return self._admit(link, inbound, callback_id=None, separate=True)

    def admit_callback(self, link, inbound, *, callback_id):
        """Queue an already-staged host callback as an indivisible input unit.

        The id is a locator only. The controller must freshly read the actual
        SQL callback owner before creating a publication from its input.
        """
        if type(callback_id) is not str or re.fullmatch(r"[0-9a-f]{32}", callback_id) is None:
            _refuse()
        return self._admit(link, inbound, callback_id=callback_id)

    def _admit(self, link, inbound, *, callback_id, separate=False):
        if self._closed:
            _refuse()
        scope, key = self._input(link, inbound)
        turn = self._turns.get(key)
        if turn is None:
            self._turns[key] = _Turn(scope, _copy(inbound), callback_id=callback_id)
            return "run"
        if turn.scope != scope:
            _refuse()
        if callback_id is not None:
            # A callback owns a new input unit. Its retained human origins may
            # still belong to the question-producing turn being released.
            if callback_id == turn.callback_id or any(entry.callback_id == callback_id for entry in turn.waiting):
                _refuse()
        else:
            known = {identity for item in [turn.current, *(entry.inbound for entry in turn.waiting)] for identity in _sources(item)}
            if known.intersection(_sources(inbound)):
                _refuse()
        if len(turn.waiting) >= self.depth:
            return "full"
        # Stop dropped the then-current tail. A later actual input waits for
        # that same turn's one completion; it cannot resurrect dropped units.
        turn.cancelled = False
        turn.waiting.append(QueuedInput(_copy(inbound), callback_id, separate))
        return "first-waiting" if len(turn.waiting) == 1 else "waiting"

    def _find(self, link, topic):
        try:
            turn = self._turns.get((link.id, _topic(topic)))
            return turn if turn is not None and turn.scope == _scope(link) else None
        except (AttributeError, ValueError):
            return None

    def waiting_count(self, link, topic):
        turn = self._find(link, topic)
        return len(turn.waiting) if turn is not None else 0

    def current_callback(self, link, topic):
        turn = self._find(link, topic)
        return turn.callback_id if turn is not None else None

    def waiting_inputs(self, link, topic):
        turn = self._find(link, topic)
        return tuple(QueuedInput(_copy(entry.inbound), entry.callback_id, entry.separate) for entry in turn.waiting) if turn is not None else ()

    def retry_later(self, link, inbound):
        scope, key = self._input(link, inbound)
        turn = self._turns.get(key)
        if self._closed or turn is None or turn.scope != scope or turn.cancelled or (inbound.text, inbound.files, _sources(inbound)) != (turn.current.text, turn.current.files, _sources(turn.current)):
            _refuse()
        # The controller owns retry timing and authorized successor creation.
        # Holding a refused current turn never releases its following messages.
        turn.retrying = True

    def cancel_wait(self, link, topic):
        turn = self._find(link, topic)
        if turn is None:
            return False
        turn.cancelled, turn.retrying = True, False
        turn.waiting.clear()
        return True

    def turn_ended(self, link, topic):
        turn = self._find(link, topic)
        if turn is None:
            return None
        key = (link.id, _topic(topic))
        del self._turns[key]
        if turn.cancelled or not turn.waiting:
            return None
        entry = turn.waiting[0]
        count = 1
        if entry.callback_id is None and not entry.separate:
            while count < len(turn.waiting) and turn.waiting[count].callback_id is None and not turn.waiting[count].separate:
                count += 1
        selected = [value.inbound for value in turn.waiting[:count]]
        first = entry.inbound
        joined = replace(
            first,
            text="\n\n".join(item.text for item in selected),
            files=[deepcopy(value) for item in selected for value in item.files],
            metadata={**deepcopy(first.metadata), _SOURCES: [identity for item in selected for identity in _sources(item)]},
        )
        self._turns[key] = _Turn(turn.scope, _copy(joined), waiting=turn.waiting[count:], callback_id=entry.callback_id)
        return joined

    def close(self):
        self._closed = True
        held = tuple(_copy(item) for turn in self._turns.values() for item in [turn.current, *(entry.inbound for entry in turn.waiting)])
        self._turns.clear()
        return held

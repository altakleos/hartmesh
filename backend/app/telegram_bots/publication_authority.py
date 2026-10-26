"""Original host attempt with separate creating and retained-read authority."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.channels.execution import ChannelExecutionProtocolError
from app.telegram_bots import authority as live_authority
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.channels.execution import ChannelOperation
    from app.telegram_bots.links import BotLink, PublicationGuard

_IDENTITY = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
_PERMISSIONS = {
    "thread_read": ("threads:read",),
    "run_create": ("threads:read", "threads:write", "runs:create"),
    "run_read": ("threads:read", "runs:read"),
    "source_read": ("threads:read", "runs:read"),
}


def _same(actual, expected) -> bool:
    return all(type(getattr(actual, name, None)) is type(getattr(expected, name)) and getattr(actual, name, None) == getattr(expected, name) for name in _IDENTITY)


@dataclass
class _Admission:
    host: PublicationAttemptAuthority
    link: BotLink
    channel: object
    client: object
    endpoint_id: str
    retired: bool = False

    def current(self, operation: ChannelOperation) -> bool:
        if type(operation) is not str or operation not in _PERMISSIONS or self.retired:
            return False
        host = self.host
        try:
            valid = (
                not host._reads_closed
                and host._read_allowed() is True
                and host._client is self.client
                and self.client.endpoint_id == self.endpoint_id
                and host._channel(self.link.id) is self.channel
                and _same(self.channel.link, self.link)
                and type(self.channel.transport_revision) is int
                and self.channel.transport_revision == self.link.token_revision
                and self.channel.link.state == "connected"
                and self.channel.link.health == "ok"
            )
            if not valid:
                self.retired = True
                return False
            if operation == "run_create":
                # Phase one closes creation without withdrawing an already
                # dispatched run's source and final-plan authority.
                return not host._creation_closed and host._create_allowed() is True and self.channel.application.running is True
            return True
        except Exception:
            self.retired = True
            return False

    def _check(self, operation: ChannelOperation) -> None:
        if self.current(operation) is not True:
            raise ChannelExecutionProtocolError() from None

    async def authorize(self, operation: ChannelOperation) -> None:
        self._check(operation)
        try:
            await await_drained(live_authority.require(self.link.owner_user_id, *_PERMISSIONS[operation]))
            self._check(operation)
            actual = await await_drained(self.host._links.update_if_current(self.link.id, token_revision=self.link.token_revision))
            self._check(operation)
            if actual is None or not _same(actual, self.link) or actual.state != "connected" or actual.health != "ok":
                raise ChannelExecutionProtocolError()
            target = await await_drained(self.host._authority.target(actual))
            self._check(operation)
            if target != "ok":
                raise ChannelExecutionProtocolError()
        except asyncio.CancelledError:
            self.retired = True
            raise
        except Exception:
            self.retired = True
            raise ChannelExecutionProtocolError() from None


@dataclass(frozen=True)
class _Guard:
    admission: _Admission
    operation: str
    borrowed: PublicationGuard

    @property
    def captured_link(self) -> BotLink:
        return self.borrowed.captured_link

    async def prepare(self) -> None:
        await self.admission.authorize(self.operation)

    async def check(self, writer):
        return await self.borrowed.check(writer)


@dataclass(frozen=True)
class PublicationAttempt:
    """Process-local capabilities; never serialized into an input or receipt."""

    admission: _Admission
    create_guard: PublicationGuard
    read_guard: PublicationGuard
    thread_id: str
    endpoint_id: str


class PublicationAttemptAuthority:
    def __init__(self, *, links, authority, client, channel: Callable, create_allowed: Callable[[], bool], read_allowed: Callable[[], bool]):
        self._links, self._authority, self._client = links, authority, client
        self._channel, self._create_allowed, self._read_allowed = channel, create_allowed, read_allowed
        self._creation_closed = self._reads_closed = False

    def close_creation(self) -> None:
        self._creation_closed = True

    def close_reads(self) -> None:
        self._creation_closed = self._reads_closed = True

    async def capture(self, link: BotLink, *, thread_id: str, endpoint_id: str) -> PublicationAttempt:
        from app.telegram_bots.links import BotLink

        try:
            if type(link) is not BotLink or type(thread_id) is not str or not thread_id or len(thread_id) > 64 or thread_id != thread_id.strip() or not thread_id.isprintable():
                raise ChannelExecutionProtocolError()
            if type(endpoint_id) is not str or re.fullmatch(r"[0-9a-f]{64}", endpoint_id) is None or endpoint_id != self._client.endpoint_id:
                raise ChannelExecutionProtocolError()
            captured = deepcopy(link)
            if (
                captured.agent_kind not in {"lead_agent", "custom_agent"}
                or captured.agent_kind == "lead_agent"
                and captured.agent_ref != "lead_agent"
                or captured.connection_id is None
                or any(type(value) is not int or value <= 0 for value in (captured.bot_id, captured.telegram_user_id, captured.chat_id))
                or captured.chat_id != captured.telegram_user_id
                or any(type(value) is not int or value < 0 for value in (captured.token_revision, captured.binding_generation))
            ):
                raise ChannelExecutionProtocolError()
            channel = self._channel(captured.id)
            admission = _Admission(self, captured, channel, self._client, endpoint_id)
            admission._check("thread_read")
            owner = await await_drained(self._authority.prepare_live_owner(captured.owner_user_id))
            admission._check("thread_read")
            return PublicationAttempt(
                admission=admission,
                create_guard=_Guard(admission, "run_create", self._links.publication_guard(captured, check_live_owner=owner, check_attempt_current=lambda: admission.current("run_create"))),
                read_guard=_Guard(admission, "source_read", self._links.publication_guard(captured, check_live_owner=owner, check_attempt_current=lambda: admission.current("source_read"))),
                thread_id=thread_id,
                endpoint_id=endpoint_id,
            )
        except Exception:
            raise ChannelExecutionProtocolError() from None

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
    "run_cancel": ("threads:read", "runs:cancel"),
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
    thread_id: str
    conversations: object | None = None
    live_owner: object | None = None
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
            if self.link.agent_kind == "instance":
                state = getattr(getattr(host._authority, "app", None), "state", None)
                valid = valid and self.conversations is not None and getattr(state, "agent_conversations", None) is self.conversations and getattr(state, "agent_instances", None) is self.conversations.instances
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

    async def instance_rows(self, operation, *, writer=None):
        if self.link.agent_kind != "instance":
            return
        from sqlalchemy import and_, or_, select

        from app.telegram_bots.policy import POLICIES, pending_discard
        from deerflow.agent_instances.contract import AgentPermission
        from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow, AgentProtectedContextRow
        from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
        from deerflow.persistence.thread_meta.model import ThreadMetaRow
        from deerflow.persistence.user.access import DisabledIdentityRow
        from deerflow.persistence.user.model import UserRow

        async def read(session):
            bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
            conversation, thread, protected = AgentConversationRow, ThreadMetaRow, AgentProtectedContextRow
            result = (
                await session.execute(
                    select(bot, instance.status, instance.custody, grant.permissions, policy.policy, protected.thread_id, UserRow, DisabledIdentityRow)
                    .join(instance, instance.id == bot.agent_ref)
                    .join(conversation, and_(conversation.instance_id == instance.id, conversation.thread_id == self.thread_id, conversation.requester_id == bot.owner_user_id))
                    .join(thread, and_(thread.thread_id == conversation.thread_id, thread.user_id == bot.owner_user_id))
                    .join(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
                    .join(UserRow, UserRow.id == bot.owner_user_id)
                    .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
                    .outerjoin(policy, policy.instance_id == instance.id)
                    .outerjoin(protected, protected.thread_id == thread.thread_id)
                    .where(bot.id == self.link.id)
                    .execution_options(populate_existing=True)
                )
            ).all()
            if not result or self.live_owner is None:
                raise ChannelExecutionProtocolError()
            current, status, custody, permissions, value, protected_thread, user, _ = result[0]
            self.live_owner.check_snapshot(user, [row[-1] for row in result])
            value = "allowed" if custody == "personal" or value is None else value
            if (
                not _same(current, self.link)
                or current.state != "connected"
                or current.health != "ok"
                or status != "active"
                or custody not in {"personal", "company"}
                or type(permissions) is not int
                or not permissions & int(AgentPermission.USE)
                or protected_thread is not None
                and not permissions & int(AgentPermission.INSPECT)
                or value not in POLICIES
                or value == "off"
                or pending_discard(current) is not None
            ):
                raise ChannelExecutionProtocolError()
            self._check(operation)

        if writer is not None:
            await read(writer)
        else:
            async with self.host._links.session_factory() as session:
                await read(session)

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
            if actual.agent_kind == "instance":
                from deerflow.agent_instances.contract import AgentPermission
                from deerflow.spaces.contract import PrincipalRef

                if operation == "run_create":
                    await await_drained(self.host._authority.admit_instance(actual))
                if not await await_drained(self.conversations.allowed(actor=PrincipalRef("human", actual.owner_user_id), thread_id=self.thread_id, permission=AgentPermission.INSPECT)):
                    raise ChannelExecutionProtocolError()
                await await_drained(self.instance_rows(operation))
                self._check(operation)
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
        async def check():
            result = await self.borrowed.check(writer)
            await self.admission.instance_rows(self.operation, writer=writer)
            self.admission._check(self.operation)
            return result

        return await await_drained(check())


@dataclass(frozen=True)
class PublicationAttempt:
    """Process-local capabilities; never serialized into an input or receipt."""

    admission: _Admission
    create_guard: PublicationGuard
    read_guard: PublicationGuard
    thread_id: str
    endpoint_id: str
    cancel_guard: PublicationGuard | None = None


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
                captured.agent_kind not in {"lead_agent", "custom_agent", "instance"}
                or captured.agent_kind == "lead_agent"
                and captured.agent_ref != "lead_agent"
                or captured.connection_id is None
                or any(type(value) is not int or value <= 0 for value in (captured.bot_id, captured.telegram_user_id, captured.chat_id))
                or captured.chat_id != captured.telegram_user_id
                or any(type(value) is not int or value < 0 for value in (captured.token_revision, captured.binding_generation))
            ):
                raise ChannelExecutionProtocolError()
            channel = self._channel(captured.id)
            conversations = None
            if captured.agent_kind == "instance":
                from deerflow.agent_instances.conversations import AgentConversations

                conversations = getattr(getattr(self._authority.app, "state", None), "agent_conversations", None)
                if type(conversations) is not AgentConversations or conversations._sf.kw.get("bind") is not self._links.session_factory.kw.get("bind"):
                    raise ChannelExecutionProtocolError()
            admission = _Admission(self, captured, channel, self._client, endpoint_id, thread_id, conversations)
            admission._check("thread_read")
            owner = await await_drained(self._authority.prepare_live_owner(captured.owner_user_id))
            admission.live_owner = owner
            admission._check("thread_read")
            return PublicationAttempt(
                admission=admission,
                create_guard=_Guard(admission, "run_create", self._links.publication_guard(captured, check_live_owner=owner, check_attempt_current=lambda: admission.current("run_create"))),
                read_guard=_Guard(admission, "source_read", self._links.publication_guard(captured, check_live_owner=owner, check_attempt_current=lambda: admission.current("source_read"))),
                thread_id=thread_id,
                endpoint_id=endpoint_id,
                cancel_guard=_Guard(admission, "run_cancel", self._links.publication_guard(captured, check_live_owner=owner, check_attempt_current=lambda: admission.current("run_cancel"))),
            )
        except Exception:
            raise ChannelExecutionProtocolError() from None

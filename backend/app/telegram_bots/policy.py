"""Stored employee Telegram policy, independent of running bot transports."""

import re
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
from deerflow.utils.file_io import await_drained

POLICIES = frozenset({"allowed", "protected", "off"})


def pending_discard(link) -> str | None:
    if link.agent_kind != "instance":
        return None
    if not isinstance(link.notices, dict):
        raise ValueError("Invalid persisted Telegram notices")
    value = link.notices.get("policy_discard")
    if value is not None and (type(value) is not str or re.fullmatch(r"[0-9a-f]{32}", value) is None):
        raise ValueError("Invalid persisted Telegram discard intent")
    return value


def mark_discard(row, *, replace=False):
    if replace or pending_discard(row) is None:
        row.notices = {**row.notices, "policy_discard": uuid4().hex}


class InstancePolicy:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def get(self, instance_id: str) -> str:
        async def read():
            async with self.session_factory() as session:
                row = await session.get(TelegramInstancePolicyRow, instance_id)
                value = row.policy if row is not None else "allowed"
                if value not in POLICIES:
                    raise ValueError("Invalid persisted Telegram policy")
                return value

        return await await_drained(read())

    async def set(self, instance_id: str, policy: str, *, updated_by: str, check_current) -> None:
        if type(policy) is not str or policy not in POLICIES:
            raise ValueError("Invalid Telegram policy")

        async def write():
            async with self.session_factory.begin() as session:
                if session.get_bind().dialect.name == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                await check_current(session)
                # Link -> policy matches the discard acknowledgement order.
                # A short off/on period must survive without a live supervisor.
                if policy == "off":
                    links = await session.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.agent_kind == "instance", TelegramBotLinkRow.agent_ref == instance_id).order_by(TelegramBotLinkRow.id).with_for_update())
                    for link in links:
                        mark_discard(link, replace=True)
                    await session.flush()
                insert = sqlite_insert if session.get_bind().dialect.name == "sqlite" else postgres_insert
                values = {"policy": policy, "updated_by": updated_by, "updated_at": datetime.now(UTC)}
                await session.execute(insert(TelegramInstancePolicyRow).values(instance_id=instance_id, **values).on_conflict_do_update(index_elements=["instance_id"], set_=values))

        await await_drained(write())

    async def complete_discard(self, captured, intent: str, *, check_current) -> bool:
        """Acknowledge only the exact successful discard, never a later off epoch."""

        async def write():
            async with self.session_factory.begin() as session:
                if session.get_bind().dialect.name == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                row = await session.get(TelegramBotLinkRow, captured.id, with_for_update=True)
                basis = ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "chat_id", "telegram_user_id", "binding_generation", "token_revision")
                if row is None or any(getattr(row, key) != getattr(captured, key) for key in basis) or pending_discard(row) != intent:
                    return False
                policy = await session.get(TelegramInstancePolicyRow, row.agent_ref, with_for_update=True)
                if policy is not None and policy.policy not in {"allowed", "protected"} or check_current() is not True:
                    return False
                row.notices = {key: value for key, value in row.notices.items() if key != "policy_discard"}
                return True

        return await await_drained(write())

    async def counts(self, instance_id: str) -> tuple[int, int]:
        async def read():
            async with self.session_factory() as session:
                grant, link = AgentInstanceGrantRow, TelegramBotLinkRow
                connected = select(link.id).where(link.agent_kind == "instance", link.agent_ref == grant.instance_id, link.owner_user_id == grant.user_id, link.state == "connected").exists()
                query = select(func.count(), func.count().filter(connected)).where(grant.instance_id == instance_id, grant.permissions.bitwise_and(int(AgentPermission.USE)) != 0)
                return tuple((await session.execute(query)).one())

        return await await_drained(read())

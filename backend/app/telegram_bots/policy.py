"""Stored employee Telegram policy, independent of running bot transports."""

from datetime import UTC, datetime

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
from deerflow.utils.file_io import await_drained

POLICIES = frozenset({"allowed", "protected", "off"})


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
                insert = sqlite_insert if session.get_bind().dialect.name == "sqlite" else postgres_insert
                values = {"policy": policy, "updated_by": updated_by, "updated_at": datetime.now(UTC)}
                await session.execute(insert(TelegramInstancePolicyRow).values(instance_id=instance_id, **values).on_conflict_do_update(index_elements=["instance_id"], set_=values))

        await await_drained(write())

    async def counts(self, instance_id: str) -> tuple[int, int]:
        async def read():
            async with self.session_factory() as session:
                grant, link = AgentInstanceGrantRow, TelegramBotLinkRow
                connected = select(link.id).where(link.agent_kind == "instance", link.agent_ref == grant.instance_id, link.owner_user_id == grant.user_id, link.state == "connected").exists()
                query = select(func.count(), func.count().filter(connected)).where(grant.instance_id == instance_id, grant.permissions.bitwise_and(int(AgentPermission.USE)) != 0)
                return tuple((await session.execute(query)).one())

        return await await_drained(read())

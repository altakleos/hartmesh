"""Read capacity as its owner over HTTP, then qualify displayed foreign facts."""

from dataclasses import dataclass

from sqlalchemy import and_, false
from sqlalchemy.orm import aliased

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.attention_authority import current_attention, prepare_attention
from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.gateway_client import ChannelExecutionProtocolError
from app.telegram_bots.work_capacity_text import Capacity
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class CapacityRead:
    capacity: Capacity
    person: str | None
    signature: str
    scope: tuple


def capacity_scope(link):
    """An owner-redacted response belongs to exactly this captured binding."""
    return tuple(getattr(link, key) for key in ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "telegram_user_id", "chat_id", "binding_generation", "connection_id", "token_revision", "connect_conflict_epoch"))


class WorkCapacityReader(AttentionReader):
    def __init__(self, *, gateway, **kwargs):
        super().__init__(**kwargs)
        self.gateway = gateway

    async def read(self, link):
        async def perform():
            async def current():
                await self._current(link)

            try:
                value = await self.gateway.get_work_capacity(link.owner_user_id, instance_id=link.agent_ref, before_dispatch=current)
                capacity = Capacity.parse(value)
            except (ChannelExecutionProtocolError, ValueError):
                raise AuthorityUnavailable() from None
            projection = CapacityProjection(link, capacity)
            await current_attention(self, link, projection=projection)
            return projection.source

        return await await_drained(perform())

    async def prepare(self, link, source):
        """Borrowed proof of displayed facts; the HTTP budget stays advisory."""
        if type(source) is not CapacityRead or source.scope != capacity_scope(link):
            raise AuthorityUnavailable()
        admission = await prepare_attention(self, link)
        projection = CapacityProjection(link, source.capacity, expected=source)

        async def current(*, session=None):
            return await admission(session=session, projection=projection)

        return current


class CapacityProjection:
    """Revalidate the route's displayed record, never select extra occupants."""

    def __init__(self, link, capacity, *, expected=None):
        self.link, self.capacity, self.expected = link, capacity, expected
        self.occupant = capacity.running[0] if capacity.running else None
        self.source = None

    def extend(self, query):
        self.column = len(query.column_descriptions)
        work, instance, grant, person = aliased(AgentWorkRow), aliased(AgentInstanceRow), aliased(AgentInstanceGrantRow), aliased(UserRow)
        shown = self.occupant
        return (
            query.add_columns(work, instance, grant.permissions, person.email)
            .outerjoin(work, work.id == shown.work_id if shown else false())
            .outerjoin(instance, instance.id == work.instance_id)
            .outerjoin(grant, and_(grant.instance_id == instance.id, grant.user_id == self.link.owner_user_id))
            .outerjoin(person, person.id == shown.requester_id if shown and shown.requester_id else false())
        )

    def check(self, rows):
        if capacity_scope(rows[0][0]) != capacity_scope(self.link):
            raise AuthorityUnavailable()
        work, instance, permissions, person = rows[0][self.column : self.column + 4]
        shown = self.occupant
        if shown is not None and (
            work is None
            or instance is None
            or (work.id, work.instance_id, work.objective, instance.name) != (shown.work_id, shown.instance_id, shown.objective, shown.instance_name)
            or type(permissions) is not int
            or not 1 <= permissions <= 7
            or permissions & 2 != 2
        ):
            raise AuthorityUnavailable()
        scope = capacity_scope(self.link)
        source = CapacityRead(self.capacity, person, digest([scope, self.capacity.signature, person]), scope)
        if self.expected is not None and self.expected != source:
            raise AuthorityUnavailable()
        self.source = source

"""Bound-human list reads; publication must retain and recheck each target fact."""

from dataclasses import dataclass

from app.channels.execution import ChannelExecutionProtocolError
from app.telegram_bots.agent_lists import AgentList, AgentListDetails, select_agents
from app.telegram_bots.authority import AuthorityUnavailable, ceilings, is_administrator
from app.telegram_bots.roles import role_of
from deerflow.agent_instances.contract import AgentPermission
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class AgentListRead:
    snapshot: AgentList
    details: dict[str, AgentListDetails]
    facts: tuple[dict, ...]
    bots: tuple
    administrator: bool


def _fact(view, *, required):
    return {"id": view.id, "owner_id": view.owner_id, "name": view.name, "custody": view.custody, "status": view.status, "generation": view.generation, "definition_revision": view.definition_revision, "permissions": int(required)}


async def read_list(host, link, *, custody, before_read) -> AgentListRead:
    return await await_drained(_read_list(host, link, custody=custody, before_read=before_read))


async def _read_list(host, link, *, custody, before_read) -> AgentListRead:
    if link.agent_kind != "lead_agent" or link.agent_ref != "lead_agent" or not callable(before_read):
        raise ChannelExecutionProtocolError()
    app = host.authority.app
    agents = getattr(app.state, "agent_instances", None)
    if agents is None:
        raise AuthorityUnavailable()
    actor = PrincipalRef("human", link.owner_user_id)

    async def check():
        await await_drained(before_read())
        if getattr(app.state, "agent_instances", None) is not agents:
            raise AuthorityUnavailable()

    await check()
    permissions = await ceilings(link.owner_user_id)
    if "agents:read" not in permissions:
        raise AuthorityUnavailable()
    body = await host.thread_client.get_agent_instances(link.owner_user_id, custody=custody, before_dispatch=check)
    snapshot = select_agents(body, owner_user_id=link.owner_user_id, custody=custody)
    work, details, facts = AgentWork(agents), {}, []
    for entry in snapshot.entries:
        await check()
        required = AgentPermission.USE | (AgentPermission.INSPECT if entry.permissions & int(AgentPermission.INSPECT) else AgentPermission(0))
        view = await await_drained(agents.get(actor=actor, instance_id=entry.id, permission=required))
        if view.name != entry.name or view.custody != custody or view.status == "provisioning" or (view.owner_id == link.owner_user_id) != entry.own:
            raise ChannelExecutionProtocolError()
        fact = _fact(view, required=required)
        if required & AgentPermission.INSPECT:
            role = await role_of(app, actor, entry.id)
            await check()
            working = await await_drained(work.count_active(actor=actor, instance_id=entry.id))
            await check()
            attention = await await_drained(HumanInput(work).list(actor=actor, instance_id=entry.id, view="pending", limit=1, can_write="agents:write" in permissions))
            details[entry.id] = AgentListDetails(role["name"] if role else None, working, attention["counts"]["pending"])
        facts.append(fact)
    bots = tuple(await await_drained(host.links.list_for_owner(link.owner_user_id)))
    administrator = await is_administrator(link.owner_user_id)
    for fact in facts:
        await check()
        current = await await_drained(agents.get(actor=actor, instance_id=fact["id"], permission=AgentPermission(fact["permissions"])))
        if _fact(current, required=fact["permissions"]) != fact:
            raise ChannelExecutionProtocolError()
    current_permissions = await ceilings(link.owner_user_id)
    if "agents:read" not in current_permissions or ("agents:write" in permissions) != ("agents:write" in current_permissions):
        raise AuthorityUnavailable()
    await check()
    return AgentListRead(snapshot, details, tuple(facts), bots, administrator)

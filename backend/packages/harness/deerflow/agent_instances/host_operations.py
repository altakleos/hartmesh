"""Host authority for orphaned identities; physical cleanup stays with Gateway."""

from datetime import UTC, datetime

from sqlalchemy import select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission, Refusal
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.recovery import SpaceRecovery


async def human(agents, reference, session):
    lookup = agents.directory.human
    borrowed = getattr(lookup, "in_session", None)
    return await borrowed(reference, session) if callable(borrowed) else await lookup(reference)


async def require_orphaned(agents, session, row):
    if row is None:
        raise AgentDenied("Agent instance is unavailable")
    if row.custody == "personal":
        if await human(agents, PrincipalRef("human", row.owner_id), session) is not None:
            raise Refusal(f"{row.id} is an AI assistant and its owner's account is active: the owner deletes it in the product", reason="owner-active")
    else:
        grants = (await session.scalars(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == row.id))).all()
        managers = {row.supervisor_id} | {grant.user_id for grant in grants if grant.permissions & int(AgentPermission.MANAGE)}
        for identifier in sorted(managers):
            if await human(agents, PrincipalRef("human", identifier), session) is not None:
                raise Refusal(f"{row.id} is an AI employee with a manager who can sign in: a manager deletes it in the product", reason="managers-active")


async def host_delete(agents, instance_id, *, host_id, dry_run):
    if not isinstance(host_id, str) or not host_id or len(host_id) > 128 or type(dry_run) is not bool:
        raise ValueError("Host deletion requires a Docker host identity and a boolean dry run")
    for _ in range(3):
        async with agents._sf() as session:
            original = await session.get(AgentInstanceRow, instance_id)
            if original is None:
                raise AgentDenied("Agent instance is unavailable")
            home_id = original.home_id
        if home_id is None:
            async with agents._sf() as session, session.begin():
                await agents._reserve_writer(session)
                row = (await session.scalars(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).one_or_none()
                if row is not None and row.home_id is not None:
                    continue
                await require_orphaned(agents, session, row)
                raise AgentConflict("The agent is not set up; the host removes an unfinished creation after 24 hours")
        async with agents.files.registry.host_locked([home_id]) as (session, homes):
            row = (await session.scalars(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).one_or_none()
            if row is not None and row.home_id != home_id:
                continue
            await require_orphaned(agents, session, row)
            if row.status == "provisioning":
                raise AgentConflict("The agent is not set up; the host removes an unfinished creation after 24 hours")
            if row.status == "deleted" or dry_run:
                return home_id
            home = homes.get(home_id)
            if home is None:
                raise AgentConflict("The agent's Home is unavailable")
            await SpaceRecovery._require_fenced(session, home_id)
            if max(row.generation, home.generation) >= 2**31 - 1:
                raise AgentConflict("The generation is exhausted; the agent cannot be deleted")
            actor = PrincipalRef("human", row.owner_id if row.custody == "personal" else row.supervisor_id)
            SpaceRecovery._advance(session, home, actor, "delete", {"retained_backups": False, "retained_binding": True, "by": "host", "cause": "operator-deleted", "operator_host_id": host_id})
            home.status = row.status = "deleted"
            row.generation += 1
            row.updated_at = datetime.now(UTC)
            return home_id
    raise AgentConflict("The agent's Home changed; retry the deletion")

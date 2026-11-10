"""Public role labels, read through the person's adopted-definition authority."""

import re

from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.roles import role_templates
from deerflow.utils.file_io import await_drained, run_file_io


async def roles(app) -> list[dict]:
    return await await_drained(run_file_io(role_templates))


async def role_of(app, actor, instance_id: str) -> dict | None:
    agents = getattr(app.state, "agent_instances", None)
    if agents is None:
        return None
    try:
        definition = await await_drained(agents.definition(actor=actor, instance_id=instance_id))
    except (AgentDenied, AgentConflict):
        # A missing/invalid optional definition cannot suppress fixed help or
        # privacy. Unexpected service/storage failures still propagate.
        return None
    name = definition.config.get("name")
    matched = re.fullmatch(r"(.+)-[0-9a-fA-F]{8}", name) if isinstance(name, str) else None
    if matched is None:
        return None
    return next((role for role in await roles(app) if role["id"] == matched[1]), None)

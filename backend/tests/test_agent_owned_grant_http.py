"""Generic file grants cannot bypass live agent membership management."""

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_storage_spaces_routes import request_app

from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Permission


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["new", "change", "remove"])
async def test_live_agent_home_requires_agent_management_even_with_generic_spaces_enabled(instances, change):
    agents, files, sf, _catalog = instances
    agent = await create(agents)
    space = await files.registry.get(actor=ALICE, space_id=agent.home_id)
    if change != "new":
        space = await files.registry.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ, expected_generation=space.generation, acknowledge_existing_data=True)
    files.user_spaces = True
    app = request_app(files)
    app.state.agent_instances = agents
    value = {"new": 1, "change": 3, "remove": 0}[change]
    body = {"subject_kind": "human", "subject_id": "bob", "permissions": value, "generation": space.generation, "acknowledge_existing_data": True}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(f"/api/spaces/{space.id}/grants", json=body)
        assert response.status_code == 409 and response.json()["reason"] == "owned-space", response.text
    async with sf() as session:
        grant = await session.get(SpaceGrantRow, (space.id, "human", "bob"))
        assert (grant.permissions if grant else 0) == (0 if change == "new" else 1)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, agent.id)).status = "deleted"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(f"/api/spaces/{space.id}/grants", json=body)
        assert response.status_code == 200, response.text

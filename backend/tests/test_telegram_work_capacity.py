"""Work capacity comes from the authenticated route, including its redaction."""

from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import startable as startable

from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.spaces.model import SpaceRow


@pytest.mark.asyncio
async def test_work_capacity_is_read_over_http_as_the_person(startable):
    from app.gateway.routers.agent_work import router

    e = startable
    e.http_app.include_router(router)
    hidden_instance, hidden_work, hidden_home = operation(), operation(), operation()
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(AgentInstanceRow, e.instance.id)
        home = await writer.get(SpaceRow, e.instance.home_id)
        record = await writer.get(AgentWorkRow, e.work["id"])
        writer.add(SpaceRow(**{**{col.name: getattr(home, col.name) for col in SpaceRow.__table__.columns}, "id": hidden_home, "backing_handle": operation()}))
        await writer.flush()
        writer.add(
            AgentInstanceRow(
                **{
                    **{col.name: getattr(parent, col.name) for col in AgentInstanceRow.__table__.columns},
                    "id": hidden_instance,
                    "home_id": hidden_home,
                    "principal_id": "agent:" + hidden_instance,
                    "creation_id": operation(),
                    "name": "Hidden employee",
                }
            )
        )
        await writer.flush()
        writer.add(AgentWorkRow(**{**{col.name: getattr(record, col.name) for col in AgentWorkRow.__table__.columns}, "id": hidden_work, "instance_id": hidden_instance, "objective": "Hidden assignment"}))
    e.human.agents.residents = SimpleNamespace(
        work_max=lambda: 3,
        attempts_in_flight=lambda: [
            {"instance_id": e.instance.id, "work_id": e.work["id"], "run_id": "visible-run", "requester_id": e.actor.subject_id, "started_at": None},
            {"instance_id": hidden_instance, "work_id": hidden_work, "run_id": "hidden-run", "requester_id": "another-person", "started_at": None},
        ],
    )
    checks = []

    async def current():
        checks.append(True)

    capacity = await e.http_client.get_work_capacity(e.link.owner_user_id, instance_id=e.instance.id, before_dispatch=current)
    assert capacity["max"] == 3 and capacity["in_use"] == 2
    assert [row["work_id"] for row in capacity["running"]] == [e.work["id"]]
    assert "Hidden" not in str(capacity) and len(checks) >= 2
    assert e.observed[-1][:3] == ("GET", f"/api/agent-instances/{e.instance.id}/work", e.link.owner_user_id)
    assert not e.launches

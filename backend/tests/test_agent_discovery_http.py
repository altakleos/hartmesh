"""Agent discovery reads durable authority; client metadata never supplies a binding."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import event, update
from test_agent_conversation_http import app_for
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_agent_work_records import delegate
from test_agent_work_records import work as work

from deerflow.agent_instances.conversations import AgentConversations
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentProtectedContextRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow


def checkpoint_reader(monkeypatch):
    from app.gateway.routers import threads

    snapshot = SimpleNamespace(config=None, metadata={}, values={}, created_at=None)
    accessor = SimpleNamespace(aget=AsyncMock(return_value=snapshot))
    monkeypatch.setattr(threads, "get_checkpointer", lambda _request: None)
    monkeypatch.setattr(threads, "abuild_checkpoint_state_accessor", AsyncMock(return_value=(accessor, {})))


@pytest.mark.asyncio
async def test_bindings_need_only_the_factory_and_read_the_whole_page_once(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    one = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    two = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    ordinary = await threads.create(operation(), user_id="alice")
    standalone = AgentConversations(None, session_factory=sf)
    statements = []

    def record(_connection, _cursor, statement, *_args):
        statements.append(statement)

    engine = sf.kw["bind"].sync_engine
    event.listen(engine, "before_cursor_execute", record)
    try:
        rows = await standalone.bindings([one["thread_id"], two["thread_id"], ordinary["thread_id"]])
        assert rows == {thread["thread_id"]: {"id": agent.id, "name": agent.name, "custody": "personal", "status": "active", "requester_id": "alice"} for thread in (one, two)}
        assert len(statements) == 1
        assert await standalone.bindings([]) == {} and len(statements) == 1
    finally:
        event.remove(engine, "before_cursor_execute", record)


@pytest.mark.asyncio
async def test_thread_rows_carry_the_binding_from_the_binding_table_not_from_metadata(instances, monkeypatch):
    agents, authority, threads, _sf = await conversations(instances)
    agent = await create(agents, custody="company")
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    forged = await threads.create(operation(), user_id="alice", metadata={"agent_instance": {"id": "forged", "custody": "company"}})
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    checkpoint_reader(monkeypatch)
    expected = {"id": agent.id, "name": agent.name, "custody": "company", "status": "active", "requester_id": "alice"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.get("/api/threads/" + chat["thread_id"])
        assert response.status_code == 200, response.text
        assert response.json()["agent_instance"] == expected
        response = await client.get("/api/threads/" + forged["thread_id"])
        assert response.status_code == 200 and response.json()["agent_instance"] is None
        response = await client.post("/api/threads/search", json={})
        assert response.status_code == 200, response.text
        rows = {row["thread_id"]: row for row in response.json()}
        assert rows[chat["thread_id"]]["agent_instance"] == expected
        assert rows[forged["thread_id"]]["agent_instance"] is None


@pytest.mark.asyncio
async def test_bound_thread_row_is_marked_with_the_agents_api_off(instances, monkeypatch):
    from app.gateway.routers import agent_instances

    agents, authority, threads, _sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    checkpoint_reader(monkeypatch)
    monkeypatch.setattr(agent_instances, "get_agents_api_config", lambda: SimpleNamespace(enabled=False))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.get("/api/threads/" + chat["thread_id"])
        assert response.status_code == 200, response.text
        assert response.json()["agent_instance"]["id"] == agent.id


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", [1, 2, 4])
async def test_conversation_listing_filters_before_pagination_and_keeps_protected_context_private(instances, monkeypatch, permission):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    stamp = datetime(2026, 10, 12, 9, 14, 2, tzinfo=UTC)
    for number, actor in ((1, ALICE), (2, BOB), (3, ALICE)):
        await authority.create(actor=actor, instance_id=agent.id, thread_id=str(number) * 32, display_name=f"Conversation {number}")
    async with sf() as session, session.begin():
        await session.execute(update(ThreadMetaRow).values(updated_at=stamp))
        (await session.get(AgentInstanceGrantRow, (agent.id, "alice"))).permissions = permission
        session.add(AgentProtectedContextRow(thread_id="3" * 32))
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    await app.state.run_manager.create("2" * 32, user_id="bob")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/agent-instances/{agent.id}/conversations"
        response = await client.get(endpoint)
        if permission == 4:
            assert response.status_code == 404, response.text
            return
        assert response.status_code == 200, response.text
        rows = response.json()["conversations"]
        assert [row["thread_id"] for row in rows] == [str(number) * 32 for number in ([1] if permission == 1 else [1, 2, 3])]
        assert rows[0] == {"thread_id": "1" * 32, "requester_id": "alice", "title": "Conversation 1", "updated_at": stamp.isoformat(), "running": False}
        response = await client.get(endpoint, params={"limit": 1, "offset": 1})
        assert response.status_code == 200, response.text
        assert response.json()["conversations"] == ([] if permission == 1 else [{"thread_id": "2" * 32, "requester_id": "bob", "title": "Conversation 2", "updated_at": stamp.isoformat(), "running": True}])
        assert (await client.get(endpoint, params={"limit": 101})).status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", [1, 2])
async def test_detail_activity_separates_totals_from_inspect_evidence_and_projects_execution(work, instances, monkeypatch, permission):
    work_service, agent, sf = work
    agents, authority, threads, _sf = await conversations(instances)
    assignment = await delegate(work_service, agent)
    chats = [await authority.create(actor=actor, instance_id=agent.id, creation_id=operation()) for actor in (BOB, ALICE)]
    stamp = datetime(2026, 10, 12, 9, 14, 2, tzinfo=UTC)
    async with sf() as session, session.begin():
        await session.execute(update(ThreadMetaRow).values(updated_at=stamp))
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = permission
    app = app_for(agents, authority, threads, monkeypatch)
    manager = app.state.run_manager
    release = asyncio.Event()
    records = []
    for chat, work_id in zip(chats, (None, assignment["id"]), strict=True):
        metadata = {"agent_instance_id": agent.id, "requester_id": chat["user_id"]}
        if work_id is not None:
            metadata["agent_work_id"] = work_id
        record = await manager.create(chat["thread_id"], user_id=chat["user_id"], metadata=metadata)
        record.task = asyncio.create_task(release.wait())
        records.append(record)
    agents.residents = SimpleNamespace(run_manager=manager, in_flight=lambda instance_id: tuple(run_id for run_id, row in manager.task_runs("agent_instance_id").items() if row["value"] == instance_id))
    agents.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
    quota = AsyncMock(return_value={"available_bytes": 9999, "max_bytes": 99999})
    monkeypatch.setattr(agents.files, "quota", quota)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
            response = await client.get(f"/api/agent-instances/{agent.id}")
            assert response.status_code == 200, response.text
            body = response.json()
            activity = body["activity"]
            assert activity["running_runs"] == 2 and activity["last_active_at"] == stamp.isoformat()
            assert activity["open_conversations"] == (1 if permission == 1 else 2)
            expected = [
                {"run_id": record.run_id, "thread_id": record.thread_id, "requester_id": record.user_id, "work_id": None if i == 0 else assignment["id"], "objective": None if i == 0 else assignment["objective"]}
                for i, record in enumerate(records)
            ]
            assert activity["running"] == ([] if permission == 1 else expected)
            assert body["home"] == (None if permission == 1 else {"state": "available", "workspace": "absent"})
            assert body["execution"] == {"execution": "unavailable", "reason": "operator-hold"}
            agents.execution_probe.assert_awaited_once_with(agent.id)
            assert quota.await_count == (0 if permission == 1 else 1)
            agents.residents = None
            response = await client.get(f"/api/agent-instances/{agent.id}")
            assert response.json()["activity"]["running_runs"] == 0 and response.json()["activity"]["running"] == []
    finally:
        release.set()
        await asyncio.gather(*(record.task for record in records))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,state,workspace",
    [
        (None, "available", "absent"),
        ("active", "available", "running"),
        ("pending", "available", "starting"),
        ("fence_pending", "available", "starting"),
        ("fenced", "available", "absent"),
        ("busy", "busy", "absent"),
        ("unverified", "unverified", "absent"),
        ("attachment-pending", "available", "starting"),
        ("denied", None, None),
    ],
)
async def test_detail_home_exposes_state_without_quota_sizes(instances, monkeypatch, phase, state, workspace):
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
    from deerflow.spaces.attachments import AttachmentPending
    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.contract import SpaceDenied
    from deerflow.spaces.service import SpaceOperationPending

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    quota = AsyncMock(return_value={"max_bytes": 9999, "available_bytes": 8888})
    errors = {"busy": SpaceOperationPending, "unverified": BackingUnavailable, "attachment-pending": AttachmentPending, "denied": SpaceDenied}
    if phase in errors:
        quota.side_effect = errors[phase]("private diagnostic")
    elif phase is not None:
        attachment_id = operation()
        async with sf() as session, session.begin():
            session.add(SpaceAttachmentRow(id=attachment_id, incarnation=operation(), actor_kind="nonhuman", actor_id=agent.principal.subject_id, host_id="host", phase=phase))
            await session.flush()
            session.add(SpaceMountRow(attachment_id=attachment_id, space_id=agent.home_id, generation=1, alias="home", writable=1))
    monkeypatch.setattr(agents.files, "quota", quota)
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.get(f"/api/agent-instances/{agent.id}")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["home"] == (None if state is None else {"state": state, "workspace": workspace})
        assert body["activity"] == {"running_runs": 0, "running": [], "last_active_at": None, "open_conversations": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["unassigned", "deleted", "no-space-grant", "manage-only"])
async def test_detail_hides_a_missing_or_unreadable_home_without_querying_its_quota(instances, monkeypatch, cause):
    from test_agent_instances import pending_creation

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow

    agents, authority, threads, sf = await conversations(instances)
    agent = await pending_creation(agents) if cause == "unassigned" else await create(agents)
    async with sf() as session, session.begin():
        if cause == "deleted":
            (await session.get(AgentInstanceRow, agent.id)).status = "deleted"
            (await session.get(SpaceRow, agent.home_id)).status = "deleted"
        elif cause == "no-space-grant":
            await session.delete(await session.get(SpaceGrantRow, (agent.home_id, "human", "alice")))
        elif cause == "manage-only":
            (await session.get(AgentInstanceGrantRow, (agent.id, "alice"))).permissions = 4
    quota = AsyncMock(side_effect=AssertionError("Hidden Home quota must not be read"))
    monkeypatch.setattr(agents.files, "quota", quota)
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.get(f"/api/agent-instances/{agent.id}", params={"include_deleted": True})
        assert response.status_code == 200, response.text
        assert response.json()["home"] is None
        quota.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", [["agents:read"], ["threads:read"]])
async def test_conversation_listing_requires_both_route_ceilings(instances, monkeypatch, permissions):
    agents, authority, threads, _sf = await conversations(instances)
    agent = await create(agents)
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE, permissions=permissions)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        assert (await client.get(f"/api/agent-instances/{agent.id}/conversations")).status_code == 403

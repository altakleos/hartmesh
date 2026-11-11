"""Actual owner-authenticated discovery and SQL details; no live model/Telegram."""

from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_repository_test_support import code_of
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_bot_adapter import build, start
from test_telegram_instance_presentation import definition
from test_telegram_instance_presentation import presentation as presentation

from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.base import Base

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def lists(presentation, monkeypatch):
    from app.gateway.routers import agent_instances
    from deerflow.config import agents_api_config

    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e = presentation
    models = (AgentLifecycleRow, AgentWorkRow, AgentWorkAttemptRow, AgentWorkEventRow, HumanInputRequestRow, HumanInputResponseRow, HumanInputEventRow, HumanInputReadRow)
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=[model.__table__ for model in models]))
    e.http_app.state.agent_instances.files.catalog = SimpleNamespace(volumes=[])
    e.http_app.include_router(agent_instances.router)
    e.services.thread_client = e.http_client
    e.instance_link, e.instance_id = e.link, e.link.agent_ref
    await e.channel.stop()
    link = await e.db.links.add(owner_user_id=e.link.owner_user_id, agent_kind="lead_agent", agent_ref="lead_agent", bot_id=9_000_000_003, bot_username="list_fixture_bot", encrypted_token="wrapped-fixture", actor_id=e.link.owner_user_id)
    link = await e.db.links.bind(
        link,
        telegram_user_id=e.link.telegram_user_id,
        chat_id=e.link.chat_id,
        first_name="Fixture",
        username=None,
        code=code_of(await e.db.links.mint_code(link)),
        start_identity={"update_id": 203, "message_id": 103, "chat_id": e.link.chat_id},
        hello_parts=[{"source_ref": {"key": "hello.chat", "values": {"product": "HartMesh", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
    )
    old_services = e.services
    transport = await build(e.db, connected=False, link=link, construct_off_loop=True)
    await start(transport)
    e.services, e.channel, e.link = transport.services, transport.channel, link
    e.request, e.clock = transport.request, transport.clock
    e.current[0] = e.channel
    e.services.authority = e.live.authority
    e.services.prepare_live_owner = e.live.authority.prepare_live_owner
    e.services.prepare_ingress_values = old_services.prepare_ingress_values
    e.services._app_config = e.live.config
    e.services.callbacks, e.services.prompts = old_services.callbacks, old_services.prompts
    e.services.thread_client = e.http_client
    e.list_link = e.link
    e.checks = []

    async def current():
        e.checks.append(True)
        assert e.active[0]

    e.list_current = current
    return e


async def test_actual_http_owner_discovery_keeps_use_only_details_private(lists):
    from app.telegram_bots.agent_list_source import read_list

    e = lists
    result = await read_list(e.services, e.list_link, custody="company", before_read=e.list_current)
    assert [entry.id for entry in result.snapshot.entries] == [e.instance_id]
    assert result.details == {}
    assert result.facts[0]["permissions"] == 1
    assert e.observed == [("GET", "/api/agent-instances", e.link.owner_user_id, True)]
    assert len(e.checks) >= 3


async def test_inspect_reads_adopted_role_and_durable_counts(lists):
    from app.telegram_bots.agent_list_source import read_list
    from deerflow.config.ui_config import RoleConfig

    e = lists
    e.live.config.ui.roles = [RoleConfig(id="analyst", name="Reporting analyst", description="Explains reports", soul="Public role")]
    await definition(SimpleNamespace(db=e.db, link=e.instance_link), "analyst-1234abcd", 3)
    result = await read_list(e.services, e.list_link, custody="company", before_read=e.list_current)
    detail = result.details[e.instance_id]
    assert detail.role == "Reporting analyst"
    assert (detail.working, detail.waiting) == (0, 0)
    assert result.facts[0]["permissions"] == 3
    assert result.facts[0]["definition_revision"]
    assert {bot.id for bot in result.bots} == {e.link.id, e.instance_link.id}


async def test_loss_of_use_during_discovery_cannot_return_private_list(lists, monkeypatch):
    from app.telegram_bots.agent_list_source import read_list
    from deerflow.agent_instances.contract import AgentDenied

    e = lists
    original = e.http_client.get_agent_instances

    async def changed(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return result

    monkeypatch.setattr(e.http_client, "get_agent_instances", changed)
    with pytest.raises(AgentDenied):
        await read_list(e.services, e.list_link, custody="company", before_read=e.list_current)


async def test_waiting_count_uses_actual_recipient_eligibility_and_write_ceiling(lists, monkeypatch):
    from uuid import uuid4

    from app.telegram_bots import agent_list_source
    from deerflow.agent_instances.contract import DefinitionSnapshot
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work import AgentWork
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow
    from deerflow.spaces.contract import PrincipalRef

    e = lists
    snapshot = DefinitionSnapshot.capture(owner_id=e.link.owner_user_id, config={"name": "analyst-1234abcd", "work_policy": {"enabled": True}}, soul="Work on reports")
    async with e.db.sessions.begin() as writer:
        writer.add(AgentDefinitionRevisionRow(revision=snapshot.revision, owner_id=snapshot.owner_id, config=snapshot.config, soul=snapshot.soul))
        await writer.flush()
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.instance_id).values(definition_revision=snapshot.revision))
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))
    work = AgentWork(e.http_app.state.agent_instances)
    actor = PrincipalRef("human", e.link.owner_user_id)
    record = await work.delegate(actor=actor, instance_id=e.instance_id, request=DelegateWork(operation_id=uuid4().hex, objective="Check a report", success_criteria="Explain gaps"))
    question = await HumanInput(work).create(
        actor=actor,
        instance_id=e.instance_id,
        work_id=record["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=record["revision"],
            expected_assignment_revision=record["assignment_revision"],
            purpose="information",
            question="Which report?",
            reason="Need a source",
            expected_response="The report name",
        ),
    )
    assert question["recipient_id"] == e.link.owner_user_id
    result = await agent_list_source.read_list(e.services, e.link, custody="company", before_read=e.list_current)
    assert result.details[e.instance_id].waiting == 1

    async def read_only(owner):
        assert owner == e.link.owner_user_id
        return frozenset({"agents:read"})

    monkeypatch.setattr(agent_list_source, "ceilings", read_only)
    result = await agent_list_source.read_list(e.services, e.link, custody="company", before_read=e.list_current)
    assert result.details[e.instance_id].waiting == 0


async def test_cancellation_at_final_owner_read_retains_complete_list_unit(lists, monkeypatch):
    import asyncio

    from sqlalchemy import event
    from sqlalchemy.util.concurrency import await_only

    from app.telegram_bots import agent_list_source

    e = lists
    entered, release = asyncio.Event(), asyncio.Event()
    armed = []
    original = agent_list_source.is_administrator

    async def owner_read(owner):
        armed.append(True)
        return await original(owner)

    def hold(_connection, _cursor, statement, _parameters, _context, _many):
        if armed and "FROM users" in statement:
            entered.set()
            await_only(release.wait())

    monkeypatch.setattr(agent_list_source, "is_administrator", owner_read)
    event.listen(e.db.engine.sync_engine, "before_cursor_execute", hold)
    task = asyncio.create_task(agent_list_source.read_list(e.services, e.link, custody="company", before_read=e.list_current))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", hold)

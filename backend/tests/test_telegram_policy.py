"""Employee Telegram settings over the real instance grants and SQL repositories."""

from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import code_of
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_router import api as api

from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.base import Base
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow
from deerflow.spaces.contract import ResolvedPrincipal

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def policy_api(api):
    tables = [SpaceRow.__table__, AgentDefinitionRevisionRow.__table__, AgentInstanceRow.__table__, AgentInstanceGrantRow.__table__]
    async with api.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))

    async def human(reference):
        return ResolvedPrincipal(reference) if reference.subject_id in {api.state.owner, "colleague"} else None

    async def in_session(reference, session):
        assert session.in_transaction() and session.get_bind() is api.db.engine.sync_engine
        return await human(reference)

    human.in_session = in_session
    api.app.state.agent_instances = AgentInstances(None, InstanceDirectory(api.db.sessions, human=human))
    await api.db.settings.ensure(api.state.owner, timezone="UTC", locale="en-US")
    api.instance_id, home = uuid4().hex, uuid4().hex
    api.path = f"/api/telegram-bots/instances/{api.instance_id}/policy"
    async with api.db.sessions.begin() as session:
        session.add(SpaceRow(id=home, backing_handle=uuid4().hex, name="Fixture Home", custody_kind="company", custodian_kind=None, custodian_id=None, mode="native", status="active", generation=1))
        session.add(AgentDefinitionRevisionRow(revision="b" * 64, owner_id=api.state.owner, config={}, soul=""))
        await session.flush()
        session.add(
            AgentInstanceRow(
                id=api.instance_id,
                principal_id="agent:" + api.instance_id,
                name="Fixture employee",
                custody="company",
                owner_id=None,
                creator_id=api.state.owner,
                supervisor_id=api.state.owner,
                definition_revision="b" * 64,
                home_id=home,
                status="active",
                generation=1,
                creation_id=uuid4().hex,
                creation_request={},
            )
        )
        await session.flush()
        session.add(AgentInstanceGrantRow(instance_id=api.instance_id, user_id=api.state.owner, permissions=7))
    return api


async def test_default_and_durable_policy_counts_use_grants_and_connected_links(policy_api):
    api = policy_api
    assert (await api.client.get(api.path)).json() == {"policy": "allowed", "people": 1, "connected": 0}
    async with api.db.sessions.begin() as session:
        session.add_all([AgentInstanceGrantRow(instance_id=api.instance_id, user_id="colleague", permissions=1), AgentInstanceGrantRow(instance_id=api.instance_id, user_id="reader", permissions=2)])
    link = await api.db.links.add(owner_user_id=api.state.owner, agent_kind="instance", agent_ref=api.instance_id, bot_id=9_000_000_001, bot_username="policy_fixture_bot", encrypted_token="wrapped-fixture", actor_id=api.state.owner)
    await api.db.links.bind(
        link,
        telegram_user_id=7_000_000_001,
        chat_id=7_000_000_001,
        first_name="Fixture",
        username=None,
        code=code_of(await api.db.links.mint_code(link)),
        start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_001},
        hello_parts=[{"source_ref": {"key": "hello.employee", "values": {"agent": "Fixture employee", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
    )
    response = await api.client.put(api.path, json={"policy": "protected"})
    assert response.status_code == 200 and response.json() == {"policy": "protected", "people": 2, "connected": 1}
    assert (await api.client.get(api.path)).json() == response.json()
    async with api.db.sessions() as session:
        row = await session.get(TelegramInstancePolicyRow, api.instance_id)
        assert row.updated_by == api.state.owner
    async with api.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.user_id == api.state.owner).values(permissions=6))
    assert (await api.client.get(api.path)).json() == {"policy": "protected", "people": 1, "connected": 0}


@pytest.mark.parametrize("permissions", [1, 2, 3, 4, 5])
async def test_both_manage_and_inspect_are_required(policy_api, permissions):
    api = policy_api
    async with api.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceGrantRow).values(permissions=permissions))
    for response in [await api.client.get(api.path), await api.client.put(api.path, json={"policy": "off"})]:
        assert response.status_code == 403 and response.json()["reason"] == "manage-required"
    async with api.db.sessions() as session:
        assert await session.scalar(select(TelegramInstancePolicyRow)) is None


async def test_personal_instance_has_no_policy_even_for_its_owner(policy_api):
    api = policy_api
    async with api.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceRow).values(custody="personal", owner_id=api.state.owner))
    for response in [await api.client.get(api.path), await api.client.put(api.path, json={"policy": "off"})]:
        assert response.status_code == 409 and response.json()["reason"] == "agent-personal"


async def test_session_and_route_ceilings_are_checked_before_policy(policy_api):
    api = policy_api
    api.state.source = "internal"
    for response in [await api.client.get(api.path), await api.client.put(api.path, json={"policy": "off"})]:
        assert response.status_code == 403 and response.json()["reason"] == "session-required"
    from app.gateway.auth_disabled import AUTH_SOURCE_SESSION

    api.state.source, api.state.allowed = AUTH_SOURCE_SESSION, False
    for response in [await api.client.get(api.path), await api.client.put(api.path, json={"policy": "off"})]:
        assert response.status_code == 403 and response.json()["reason"] == "ceiling"


@pytest.mark.parametrize("body", [{"policy": "unknown"}, {"policy": True}, {"policy": "off", "extra": 1}])
async def test_invalid_policy_leaves_no_row(policy_api, body):
    api = policy_api
    response = await api.client.put(api.path, json=body)
    assert response.status_code == 400 and response.json()["reason"] == "invalid-request"
    assert (await api.client.get(api.path)).json()["policy"] == "allowed"


async def test_list_policy_and_privacy_include_visible_instances_without_bots(policy_api):
    api = policy_api
    await api.client.put(api.path, json={"policy": "protected"})
    body = (await api.client.get("/api/telegram-bots")).json()
    assert body["bots"] == []
    assert body["policies"] == [{"instance_id": api.instance_id, "policy": "protected"}]
    assert body["read_by_others"] == []
    async with api.db.sessions.begin() as session:
        session.add(AgentInstanceGrantRow(instance_id=api.instance_id, user_id="colleague", permissions=2))
    assert (await api.client.get("/api/telegram-bots")).json()["read_by_others"] == [api.instance_id]
    async with api.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.user_id == api.state.owner).values(permissions=1))
    assert (await api.client.get("/api/telegram-bots")).json()["read_by_others"] == [api.instance_id]
    async with api.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.user_id == api.state.owner).values(permissions=6))
    body = (await api.client.get("/api/telegram-bots")).json()
    assert body["policies"] == body["read_by_others"] == []


async def test_instance_bot_label_and_policy_require_current_use(policy_api):
    api = policy_api
    await api.db.links.add(owner_user_id=api.state.owner, agent_kind="instance", agent_ref=api.instance_id, bot_id=9_000_000_001, bot_username="policy_fixture_bot", encrypted_token="wrapped-fixture", actor_id=api.state.owner)
    await api.client.put(api.path, json={"policy": "off"})
    bot = (await api.client.get("/api/telegram-bots")).json()["bots"][0]
    assert bot["agent_name"] == "Fixture employee" and bot["policy"] == "off"
    async with api.db.sessions.begin() as session:
        await session.execute(update(AgentInstanceGrantRow).values(permissions=6))
    bot = (await api.client.get("/api/telegram-bots")).json()["bots"][0]
    assert bot["agent_name"] is bot["policy"] is None


@pytest.mark.parametrize("change", ["grant", "deleted", "personal"])
async def test_policy_writer_rechecks_authority_after_preliminary_view(policy_api, monkeypatch, change):
    from app.telegram_bots.policy import InstancePolicy

    api = policy_api
    assert (await api.client.put(api.path, json={"policy": "protected"})).status_code == 200
    original = InstancePolicy.set

    async def changed(self, instance_id, policy, **kwargs):
        async with api.db.sessions.begin() as session:
            if change == "grant":
                await session.execute(update(AgentInstanceGrantRow).values(permissions=1))
            else:
                await session.execute(update(AgentInstanceRow).values(**({"status": "deleted"} if change == "deleted" else {"custody": "personal", "owner_id": api.state.owner})))
        return await original(self, instance_id, policy, **kwargs)

    monkeypatch.setattr(InstancePolicy, "set", changed)
    response = await api.client.put(api.path, json={"policy": "allowed"})
    assert response.status_code == (409 if change == "personal" else 403)
    async with api.db.sessions() as session:
        assert (await session.get(TelegramInstancePolicyRow, api.instance_id)).policy == "protected"


async def test_policy_write_requires_current_human_in_its_borrowed_session(policy_api):
    api = policy_api
    calls = []

    async def disabled(reference, session):
        calls.append(session.get_bind())
        return None

    api.app.state.agent_instances.directory.human.in_session = disabled
    response = await api.client.put(api.path, json={"policy": "off"})
    assert response.status_code == 403 and response.json()["reason"] == "manage-required"
    assert calls == [api.db.engine.sync_engine]
    async with api.db.sessions() as session:
        assert await session.get(TelegramInstancePolicyRow, api.instance_id) is None


async def test_policy_writer_refuses_authority_from_another_database(policy_api, tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    api = policy_api
    foreign = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'foreign-policy.sqlite'}")
    try:
        api.app.state.agent_instances.directory.session_factory = async_sessionmaker(foreign)
        response = await api.client.put(api.path, json={"policy": "off"})
        assert response.status_code == 403 and response.json()["reason"] == "manage-required"
        async with api.db.sessions() as session:
            assert await session.get(TelegramInstancePolicyRow, api.instance_id) is None
    finally:
        await foreign.dispose()


async def test_current_human_is_checked_after_the_writer_acquires_grant_locks(policy_api):
    from sqlalchemy import event

    api = policy_api
    disabled = False

    def acquired(connection, cursor, statement, parameters, context, executemany):
        nonlocal disabled
        if "FROM agent_instance_grants " in statement and "JOIN" not in statement:
            disabled = True

    async def human(reference, session):
        return None if disabled else ResolvedPrincipal(reference)

    api.app.state.agent_instances.directory.human.in_session = human
    event.listen(api.db.engine.sync_engine, "after_cursor_execute", acquired)
    try:
        response = await api.client.put(api.path, json={"policy": "off"})
    finally:
        event.remove(api.db.engine.sync_engine, "after_cursor_execute", acquired)
    assert disabled and response.status_code == 403
    async with api.db.sessions() as session:
        assert await session.get(TelegramInstancePolicyRow, api.instance_id) is None


async def test_off_records_discard_intent_even_if_restored_before_reconcile(policy_api):
    api = policy_api
    link = await api.db.links.add(owner_user_id=api.state.owner, agent_kind="instance", agent_ref=api.instance_id, bot_id=9_000_000_001, bot_username="policy_fixture_bot", encrypted_token="wrapped-fixture", actor_id=api.state.owner)
    assert (await api.client.put(api.path, json={"policy": "off"})).status_code == 200
    intent = (await api.db.links.get(link.id)).notices.get("policy_discard")
    assert isinstance(intent, str) and len(intent) == 32
    assert (await api.client.put(api.path, json={"policy": "protected"})).status_code == 200
    assert (await api.db.links.get(link.id)).notices["policy_discard"] == intent
    assert (await api.client.put(api.path, json={"policy": "off"})).status_code == 200
    assert (await api.db.links.get(link.id)).notices["policy_discard"] != intent

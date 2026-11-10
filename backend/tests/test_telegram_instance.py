"""Persistent conversation preparation through real SQL and Gateway reads."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from _telegram_repository_test_support import code_of
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_bot_adapter import build, start
from test_telegram_threads import close, inbound, origin
from test_telegram_threads_http import http_environment

from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentProtectedContextRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef

pytestmark = pytest.mark.asyncio


async def instance_environment(db, monkeypatch):
    e = await http_environment(db, monkeypatch)
    async with db.engine.begin() as connection:
        await connection.run_sync(lambda sync: AgentProtectedContextRow.__table__.create(sync, checkfirst=True))
    instance_id, home = uuid4().hex, uuid4().hex
    owner = e.link.owner_user_id
    async with db.sessions.begin() as writer:
        writer.add(SpaceRow(id=home, backing_handle=uuid4().hex, name="Fixture Home", custody_kind="company", custodian_kind=None, custodian_id=None, mode="native", status="active", generation=1))
        writer.add(AgentDefinitionRevisionRow(revision="b" * 64, owner_id=owner, config={}, soul=""))
        await writer.flush()
        writer.add(
            AgentInstanceRow(
                id=instance_id,
                principal_id="agent:" + instance_id,
                name="Fixture employee",
                custody="company",
                owner_id=None,
                creator_id=owner,
                supervisor_id=owner,
                definition_revision="b" * 64,
                home_id=home,
                status="active",
                generation=1,
                creation_id=uuid4().hex,
                creation_request={},
            )
        )
        await writer.flush()
        writer.add(AgentInstanceGrantRow(instance_id=instance_id, user_id=owner, permissions=1))
    await e.channel.stop()
    await db.links.remove(e.link.id, actor_id=owner)
    link = await db.links.add(owner_user_id=owner, agent_kind="instance", agent_ref=instance_id, bot_id=9_000_000_002, bot_username="instance_fixture_bot", encrypted_token="wrapped-fixture", actor_id=owner)
    link = await db.links.bind(
        link,
        telegram_user_id=7_000_000_001,
        chat_id=7_000_000_001,
        first_name="Fixture",
        username=None,
        code=code_of(await db.links.mint_code(link)),
        start_identity={"update_id": 202, "message_id": 102, "chat_id": 7_000_000_001},
        hello_parts=[{"source_ref": {"key": "hello.employee", "values": {"agent": "Fixture employee", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
    )
    transport = await build(db, connected=False, link=link)
    transport.services.send_allowed = lambda: e.active[0]
    await start(transport)
    e.channel, e.link, e.current[0] = transport.channel, link, transport.channel
    e.live.authority.app = e.http_app
    agents = e.http_app.state.agent_conversations.instances
    agents.files = SimpleNamespace(_operation_id=lambda value: value)
    e.http_app.state.agent_instances = agents
    e.admissions = []

    async def execution(app, identity):
        e.admissions.append(("execution", identity))

    async def admit(view, *, work):
        e.admissions.append(("resident", view.id, work))

    from app.gateway import storage_spaces

    monkeypatch.setattr(storage_spaces, "require_execution", execution)
    agents.residents = SimpleNamespace(admit=admit)
    return e


async def test_instance_link_creates_conversation_as_requester_and_reuses_binding(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    try:
        row = await origin(e)
        identity = await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        authority = e.http_app.state.agent_conversations
        assert await authority.binding(identity) == e.link.agent_ref
        thread = await e.http_app.state.thread_store.get(identity, user_id=None)
        assert thread["user_id"] == e.link.owner_user_id
        assert thread["metadata"] == {"channel_source": {"type": "im_channel", "provider": "telegram", "connection_id": e.link.connection_id}}
        assert await e.threads.ensure_thread(e.link, inbound(e), origin=row) == identity
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == identity
        assert e.observed and all(method == "GET" for method, *_ in e.observed)
    finally:
        await close(e)


async def test_use_is_checked_before_execution_or_conversation_creation(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    try:
        async with db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=2))
        with pytest.raises(AgentDenied):
            await e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e))
        assert e.admissions == []
        assert not e.observed
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) is None
    finally:
        await close(e)


@pytest.mark.parametrize("status", ["suspended", "archived"])
async def test_inactive_instance_keeps_link_and_refuses_preparation(db, monkeypatch, status):
    e = await instance_environment(db, monkeypatch)
    try:
        async with db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status=status))
        assert await e.live.authority.target(e.link) == "agent_unavailable"
        with pytest.raises(AgentDenied):
            await e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e))
        assert not e.admissions
        assert await db.links.get(e.link.id) is not None
    finally:
        await close(e)


async def test_lost_instance_service_is_uncertain_and_preserves_link(db, monkeypatch):
    from app.telegram_bots.authority import AuthorityUnavailable

    e = await instance_environment(db, monkeypatch)
    try:
        e.http_app.state.agent_instances = None
        with pytest.raises(AuthorityUnavailable):
            await e.live.authority.target(e.link)
        assert await db.links.get(e.link.id) is not None
    finally:
        await close(e)


async def test_live_instance_delivery_uses_current_policy_and_notice_context(db, monkeypatch):
    from app.telegram_bots.adapter import SendContext
    from app.telegram_bots.producer import DeliveryClaimRetired, OutboxItem

    e = await instance_environment(db, monkeypatch)
    try:
        assert await e.live.authority.target(e.link) == "ok"
        async with db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id))
            item = OutboxItem(**{name: getattr(row, name) for name in OutboxItem.__dataclass_fields__})
        assert (await e.live.authority.delivery(e.link, item)).decision == "allow"
        assert await e.live.authority.protect_notice(SendContext(e.link.id, e.link.chat_id, e.link.binding_generation, e.link.token_revision, "bound_message")) is False
        with pytest.raises(DeliveryClaimRetired):
            await e.live.authority.protect_notice(SendContext(e.link.id, e.link.chat_id, e.link.binding_generation + 1, e.link.token_revision, "bound_message"))
    finally:
        await close(e)


async def test_instance_admission_runs_use_execution_and_resident_checks_in_order(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    try:
        view = await e.live.authority.admit_instance(e.link)
        assert view.permissions == AgentPermission.USE
        assert e.admissions == [("execution", e.link.agent_ref), ("resident", e.link.agent_ref, False)]
        assert (await e.http_app.state.agent_instances.get(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.link.agent_ref, permission=AgentPermission.USE)).id == view.id
    finally:
        await close(e)


async def test_instance_unavailable_keeps_target_and_never_creates_a_conversation(db, monkeypatch):
    from deerflow.agent_instances.contract import ExecutionUnavailable

    e = await instance_environment(db, monkeypatch)
    try:

        async def refused(app, instance_id):
            raise ExecutionUnavailable("operator-hold")

        monkeypatch.setattr("app.gateway.storage_spaces.require_execution", refused)
        assert await e.live.authority.target(e.link) == "ok"
        with pytest.raises(ExecutionUnavailable):
            await e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e))
        assert await db.links.get(e.link.id) is not None
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) is None
    finally:
        await close(e)


async def test_wrong_stored_binding_is_replaced_without_reusing_conversation(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    try:
        previous = uuid4().hex
        await e.http_app.state.thread_store.create(previous, user_id=e.link.owner_user_id, metadata={"channel_source": {"provider": "telegram"}})
        await db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id=previous)
        identity = await e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e))
        assert identity != previous
        assert await e.http_app.state.agent_conversations.binding(identity) == e.link.agent_ref
        assert await e.http_app.state.agent_conversations.binding(previous) is None
    finally:
        await close(e)


async def test_cancellation_drains_instance_creation_and_reuses_saved_identity(db, monkeypatch):
    import asyncio

    e = await instance_environment(db, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    create = e.http_app.state.agent_conversations.create
    identities = []

    async def held(**kwargs):
        identities.append(kwargs["creation_id"])
        result = await create(**kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(e.http_app.state.agent_conversations, "create", held)
    task = None
    try:
        row = await origin(e)
        task = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=row))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        identity = await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        assert set(identities) == {identity}
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)

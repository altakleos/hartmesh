"""Browser setup over actual instance grants, policy and the atomic bot writer."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_policy import policy_api as policy_api
from test_telegram_router import added
from test_telegram_router import api as api

from app.gateway.storage_spaces import ExecutionState
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def setup_api(policy_api):
    e = policy_api
    e.probe = AsyncMock(return_value=ExecutionState(True, "ready"))
    e.app.state.agent_instances.execution_probe = e.probe
    return e


async def post(e, **changes):
    return await e.client.post("/api/telegram-bots", json=added(agent_kind="instance", agent_ref=e.instance_id, **changes))


@pytest.mark.parametrize("custody,owner,status", [("company", None, "active"), ("personal", "owner-a", "active"), ("personal", "colleague", "active"), ("company", None, "suspended")])
async def test_current_use_connects_company_own_and_colleague_assistants(setup_api, custody, owner, status):
    e = setup_api
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).values(custody=custody, owner_id=owner, status=status))
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=1))
    response = await post(e)
    assert response.status_code == 201, response.text
    assert response.json()["bot"]["agent_ref"] == e.instance_id
    assert response.json()["connect"]["url"].startswith("https://t.me/")
    assert len(await e.db.links.list_all()) == 1
    e.supervisor.inspect_token.assert_awaited_once()
    assert all(call.args == (e.instance_id,) for call in e.probe.await_args_list)


@pytest.mark.parametrize("cause", ["missing", "use", "deleted", "provisioning", "archived"])
async def test_absent_or_unavailable_target_precedes_token_validation(setup_api, cause):
    e = setup_api
    async with e.db.sessions.begin() as writer:
        if cause == "use":
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=6))
        elif cause != "missing":
            await writer.execute(update(AgentInstanceRow).values(status=cause))
    if cause == "missing":
        e.instance_id = "missing"
    response = await post(e, token="not a token")
    assert response.status_code == 404 and response.json()["reason"] == "agent-not-found"
    e.supervisor.inspect_token.assert_not_awaited()
    e.probe.assert_not_awaited()
    assert not await e.db.links.list_all()


async def test_policy_off_refuses_before_token_or_execution_probe(setup_api):
    e = setup_api
    assert (await e.client.put(e.path, json={"policy": "off"})).status_code == 200
    response = await post(e, token="not a token")
    assert response.status_code == 409 and response.json()["reason"] == "policy-off"
    e.supervisor.inspect_token.assert_not_awaited()
    e.probe.assert_not_awaited()
    assert not await e.db.links.list_all()


@pytest.mark.parametrize("reason", ["storage-unavailable", "agents-none", "operator-hold", "space-unverified", "space-not-assigned"])
async def test_execution_refusal_precedes_token_and_carries_the_reason(setup_api, reason):
    e = setup_api
    e.probe.return_value = ExecutionState(True, "unavailable", reason)
    response = await post(e, token="not a token")
    assert response.status_code == 409 and response.json()["reason"] == "execution-unavailable"
    assert response.json()["execution_reason"] == reason
    e.supervisor.inspect_token.assert_not_awaited()
    assert not await e.db.links.list_all()


@pytest.mark.parametrize("change", ["use", "policy", "execution", "ceiling", "service"])
async def test_token_roundtrip_cannot_publish_a_retired_target(setup_api, change):
    e = setup_api

    async def changed(token):
        if change == "use":
            async with e.db.sessions.begin() as writer:
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=6))
        elif change == "policy":
            await e.client.put(e.path, json={"policy": "off"})
        elif change == "execution":
            e.probe.return_value = ExecutionState(True, "unavailable", "operator-hold")
        elif change == "ceiling":
            e.state.allowed = False
        elif change == "service":
            e.app.state.agent_instances = None
        return e.state.identity

    e.supervisor.inspect_token.side_effect = changed
    response = await post(e)
    assert response.status_code in {403, 404, 409}, response.text
    assert not await e.db.links.list_all()
    e.supervisor.add.assert_not_awaited()


@pytest.mark.parametrize("change", ["use", "policy"])
async def test_target_is_rechecked_in_the_final_capacity_writer(setup_api, monkeypatch, change):
    e = setup_api
    original = e.db.links.capacity_writer

    @asynccontextmanager
    async def changed(*args, **kwargs):
        if change == "use":
            async with e.db.sessions.begin() as writer:
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=6))
        else:
            await e.client.put(e.path, json={"policy": "off"})
        async with original(*args, **kwargs) as writer:
            yield writer

    monkeypatch.setattr(e.db.links, "capacity_writer", changed)
    response = await post(e)
    assert response.status_code == (404 if change == "use" else 409), response.text
    assert not await e.db.links.list_all()
    e.supervisor.add.assert_not_awaited()


@pytest.mark.parametrize("change", ["execution", "policy", "archived"])
async def test_same_bot_token_repair_is_available_while_execution_is_held(setup_api, change):
    from test_telegram_router import SECOND

    e = setup_api
    first = await post(e)
    link = await e.db.links.get(first.json()["bot"]["id"])
    if change == "execution":
        e.probe.return_value = ExecutionState(True, "unavailable", "operator-hold")
    elif change == "policy":
        await e.client.put(e.path, json={"policy": "off"})
    else:
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceRow).values(status="archived"))
    response = await e.client.post(f"/api/telegram-bots/{link.id}/token", json={"token": SECOND, "operation_id": "a" * 32})
    assert response.status_code == 200, response.text
    assert (await e.db.links.get(link.id)).token_revision == link.token_revision + 1


@pytest.mark.parametrize("different_bot", [False, True])
@pytest.mark.parametrize("change", ["use", "service", "ceiling", "policy", "execution"])
async def test_instance_token_roundtrip_cannot_bypass_current_authority(setup_api, different_bot, change):
    from test_telegram_router import SECOND

    e = setup_api
    first = await post(e)
    link = await e.db.links.get(first.json()["bot"]["id"])

    async def changed(token):
        if change == "use":
            async with e.db.sessions.begin() as writer:
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=6))
        elif change == "service":
            e.app.state.agent_instances = None
        elif change == "ceiling":
            e.state.allowed = False
        elif change == "policy":
            await e.client.put(e.path, json={"policy": "off"})
        else:
            e.probe.return_value = ExecutionState(True, "unavailable", "operator-hold")
        return link.bot_id + int(different_bot), "fixture_bot", None

    e.supervisor.inspect_token.side_effect = changed
    response = await e.client.post(f"/api/telegram-bots/{link.id}/token", json={"token": SECOND, "operation_id": "a" * 32})
    if not different_bot and change in {"execution", "policy"}:
        assert response.status_code == 200, response.text
    else:
        assert response.status_code in {403, 404, 409}, response.text
        saved = await e.db.links.get(link.id)
        assert (saved.id, saved.bot_id, saved.token_revision, saved.binding_generation) == (link.id, link.bot_id, link.token_revision, link.binding_generation)
        assert len(await e.db.links.list_all()) == 1


async def test_setup_locks_instance_before_owner_links_like_the_policy_writer(setup_api, monkeypatch):
    from sqlalchemy import event

    e = setup_api
    original = e.db.links.capacity_writer
    statements = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(" ".join(statement.lower().split()))

    @asynccontextmanager
    async def observed(*args, **kwargs):
        event.listen(e.db.engine.sync_engine, "before_cursor_execute", capture)
        try:
            async with original(*args, **kwargs) as writer:
                yield writer
        finally:
            event.remove(e.db.engine.sync_engine, "before_cursor_execute", capture)

    monkeypatch.setattr(e.db.links, "capacity_writer", observed)
    assert (await post(e)).status_code == 201
    target = next(i for i, sql in enumerate(statements) if sql.startswith("select ") and "from agent_instances " in sql)
    links = next(i for i, sql in enumerate(statements) if sql.startswith("select ") and "from telegram_bot_links " in sql)
    assert target < links, statements


@pytest.mark.parametrize("boundary", ["policy", "owner", "capacity"])
async def test_final_writer_rechecks_human_after_earlier_awaits(setup_api, monkeypatch, boundary):
    from sqlalchemy.ext.asyncio import AsyncSession

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = setup_api
    human = e.app.state.agent_instances.directory.human
    original_human = human.in_session
    retired = False

    async def resolve(reference, writer):
        return None if retired else await original_human(reference, writer)

    monkeypatch.setattr(human, "in_session", resolve)
    if boundary == "policy":
        original = AsyncSession.get

        async def policy_read(writer, entity, *args, **kwargs):
            nonlocal retired
            result = await original(writer, entity, *args, **kwargs)
            if entity is TelegramInstancePolicyRow and writer.in_transaction():
                # Only after the target row was explicitly locked, not preflight.
                if writer.info.get("fixture_target_locked"):
                    retired = True
            return result

        original_scalar = AsyncSession.scalar

        async def scalar(writer, statement, *args, **kwargs):
            result = await original_scalar(writer, statement, *args, **kwargs)
            if isinstance(result, AgentInstanceRow) and getattr(statement, "_for_update_arg", None) is not None:
                writer.info["fixture_target_locked"] = True
            return result

        monkeypatch.setattr(AsyncSession, "get", policy_read)
        monkeypatch.setattr(AsyncSession, "scalar", scalar)
    else:
        method = "_owner_lock" if boundary == "owner" else "_counts"
        original = getattr(e.db.links, method)
        calls = 0

        async def later(*args, **kwargs):
            nonlocal retired, calls
            result = await original(*args, **kwargs)
            calls += 1
            if boundary == "owner" or calls == 2:
                retired = True
            return result

        monkeypatch.setattr(e.db.links, method, later)
    response = await post(e)
    assert retired
    assert response.status_code == 404, response.text
    assert not await e.db.links.list_all()


async def test_committed_instance_repair_receipt_settles_after_use_is_withdrawn(setup_api):
    from test_telegram_router import SECOND

    e = setup_api
    first = await post(e)
    link_id = first.json()["bot"]["id"]
    body = {"token": SECOND, "operation_id": "a" * 32}
    assert (await e.client.post(f"/api/telegram-bots/{link_id}/token", json=body)).status_code == 200
    revision = (await e.db.links.get(link_id)).token_revision
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=6))
    e.supervisor.inspect_token.reset_mock()
    replay = await e.client.post(f"/api/telegram-bots/{link_id}/token", json=body)
    assert replay.status_code == 200 and replay.json()["replacement_status"] == "committed"
    assert (await e.db.links.get(link_id)).token_revision == revision
    e.supervisor.inspect_token.assert_not_awaited()


async def test_repair_rolls_back_token_and_receipt_on_final_human_retirement(setup_api, monkeypatch):
    from test_telegram_router import SECOND

    e = setup_api
    first = await post(e)
    link = await e.db.links.get(first.json()["bot"]["id"])
    original = e.db.links._counts
    retired, calls = False, 0
    human = e.app.state.agent_instances.directory.human
    lookup = human.in_session

    async def resolve(reference, writer):
        return None if retired else await lookup(reference, writer)

    async def counts(*args, **kwargs):
        nonlocal retired, calls
        result = await original(*args, **kwargs)
        calls += 1
        if calls == 2:
            retired = True
        return result

    monkeypatch.setattr(human, "in_session", resolve)
    monkeypatch.setattr(e.db.links, "_counts", counts)
    response = await e.client.post(f"/api/telegram-bots/{link.id}/token", json={"token": SECOND, "operation_id": "a" * 32})
    assert response.status_code == 404 and retired
    assert (await e.db.links.get(link.id)).token_revision == link.token_revision
    async with e.db.sessions() as reader:
        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow

        assert await reader.scalar(select(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.operation_id == "a" * 32)) is None

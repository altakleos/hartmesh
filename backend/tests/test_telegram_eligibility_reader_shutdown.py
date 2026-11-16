"""Actual eligibility repositories retain their own SQL cleanup on shutdown."""

import asyncio
from uuid import uuid4

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy.ext.asyncio import AsyncSession
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.authority import get_local_provider
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["account", "connection", "instance"])
@pytest.mark.parametrize("phase", ["body", "close"])
async def test_real_eligibility_lookup_drains_close(capture_environment, monkeypatch, tmp_path, reader, phase):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        provider = get_local_provider()
        operation = {
            "account": lambda: provider.get_user(e.link.owner_user_id),
            "connection": lambda: ChannelConnectionRepository(e.e.sf).find_connection_by_external_identity(provider="telegram", external_account_id=str(e.link.telegram_user_id), workspace_id=str(e.link.bot_id)),
            "instance": lambda: e.app.state.agent_instances.get(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id),
        }[reader]
        table = {"account": UserRow, "connection": ChannelConnectionRow, "instance": AgentInstanceRow}[reader]
        mark = "eligibility-reader-" + uuid4().hex
        entered, release, queried, release_query = (asyncio.Event() for _ in range(4))
        actual_close, actual_get, actual_execute = AsyncSession.close, AsyncSession.get, AsyncSession.execute
        retained, sessions = [], []
        owner = stopping = None

        async def selected(session, entity):
            if session.bind is e.e.engine and asyncio.current_task() is owner and entity is table:
                session.info[mark] = True
                if phase == "body" and not queried.is_set():
                    queried.set()
                    await release_query.wait()

        async def get(session, entity, *args, **kwargs):
            value = await actual_get(session, entity, *args, **kwargs)
            await selected(session, entity)
            return value

        async def execute(session, statement, *args, **kwargs):
            value = await actual_execute(session, statement, *args, **kwargs)
            descriptions = getattr(statement, "column_descriptions", ())
            await selected(session, descriptions[0].get("entity") if descriptions else None)
            return value

        async def close(session):
            if session.bind is e.e.engine and session.info.get(mark) and session.in_transaction() and not retained:
                retained.append(asyncio.current_task())
                sessions.append(session)
                entered.set()
                await release.wait()
            await actual_close(session)

        monkeypatch.setattr(AsyncSession, "get", get)
        monkeypatch.setattr(AsyncSession, "execute", execute)
        monkeypatch.setattr(AsyncSession, "close", close)
        owner = asyncio.create_task(operation(), name="actual-eligibility-lookup")
        e.host._background.add(owner)
        owner.add_done_callback(e.host._background.discard)
        try:
            await asyncio.wait_for((queried if phase == "body" else entered).wait(), 10)
            stopping = asyncio.create_task(e.host.begin_shutdown())
            await asyncio.wait_for(entered.wait(), 10)
            done, _ = await asyncio.wait({stopping}, timeout=0.1)
            assert owner.cancelling()
            assert not owner.done(), "cancelled eligibility owner abandoned its own SQL close"
            assert not done, "shutdown abandoned actual eligibility reader cleanup"
            owner.cancel()
            done, _ = await asyncio.wait({stopping}, timeout=0.1)
            assert not owner.done(), "repeated cancellation released the eligibility owner early"
            assert not done, "repeated cancellation abandoned the same actual SQL close"
            release.set()
            await asyncio.wait_for(stopping, 10)
            assert owner.cancelled() and not sessions[0].in_transaction()
        finally:
            release.set()
            release_query.set()
            await asyncio.gather(owner, *([stopping] if stopping is not None else []), return_exceptions=True)
            # The exact detached close is also drained on a red regression.
            if retained:
                await asyncio.gather(*retained, return_exceptions=True)


@pytest.mark.asyncio
async def test_account_lookup_keeps_borrowed_transaction_owned(capture_environment, monkeypatch, tmp_path):
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        closed, actual_close = [], AsyncSession.close

        async def close(session):
            closed.append(session)
            await actual_close(session)

        monkeypatch.setattr(AsyncSession, "close", close)
        async with e.e.sf.begin() as borrowed:
            user = await SQLiteUserRepository(e.e.sf).get_user_by_id(e.link.owner_user_id, session=borrowed)
            assert str(user.id) == e.link.owner_user_id
            assert borrowed.in_transaction() and borrowed not in closed
        assert closed.count(borrowed) == 1

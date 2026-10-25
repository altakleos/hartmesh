"""Owned ordinary-thread Gateway admission with real binding SQL, no model."""

import asyncio
import os
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from langgraph.store.memory import InMemoryStore
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_gateway_services import _make_start_run_request

from app.gateway import channel_execution_reads, deps, services
from app.gateway.auth.models import User
from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
from app.gateway.internal_auth import INTERNAL_OWNER_USER_ID_HEADER_NAME, get_internal_user
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.config import app_config as app_config_module
from deerflow.config.app_config import AppConfig, set_app_config
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceRow
from deerflow.persistence.base import Base
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime import MemoryStreamBridge, RunManager, RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore

OWNER_ID = "00000000-0000-0000-0000-00000000000a"
THREAD_ID = "channel-execution-owned-thread"
TABLES = [AgentDefinitionRevisionRow.__table__, SpaceRow.__table__, AgentInstanceRow.__table__, AgentConversationRow.__table__]


class ControlledProvider:
    """Fresh reads return an actual User; tests may set .user to None/disabled."""

    def __init__(self, user):
        self.user = user
        self.lookups = []

    async def get_user(self, user_id):
        self.lookups.append(user_id)
        return self.user if self.user is not None and str(self.user.id) == user_id else None


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def gateway_environment(request, tmp_path, monkeypatch):
    schema = "channel_execution_" + uuid4().hex
    admin = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        uri = make_url(uri).set(drivername="postgresql+asyncpg")
        ssl = {"ssl": False} if uri.query.get("sslmode") == "disable" else {}
        uri = uri.difference_update_query(["sslmode"])
        admin = create_async_engine(uri, connect_args=ssl)
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        engine = create_async_engine(uri, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'channel-execution.sqlite'}", connect_args={"timeout": 30})

        @event.listens_for(engine.sync_engine, "connect")
        def sqlite_pragmas(connection, _record):
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")

    manager = None
    bridge = None
    workers = set()
    try:
        async with engine.begin() as connection:
            if request.param == "sqlite":
                await connection.execute(text("BEGIN"))
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=TABLES))
        sf = async_sessionmaker(engine, expire_on_commit=False)
        config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
        # set_app_config changes only these cache fields; preserve ambient
        # state rather than resetting a prior suite's owned configuration.
        for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
            monkeypatch.setattr(app_config_module, name, getattr(app_config_module, name))
        set_app_config(config)
        owner = User(id=UUID(OWNER_ID), email="channel-execution@example.com", system_role="user")
        provider = ControlledProvider(owner)
        for module in (deps, services, channel_execution_reads):
            monkeypatch.setattr(module, "get_local_provider", lambda: provider)
        thread_store = MemoryThreadMetaStore(InMemoryStore())
        await thread_store.create(THREAD_ID, assistant_id="lead_agent", user_id=OWNER_ID)
        store = MemoryRunStore()
        manager = RunManager(store=store)
        gateway_request = _make_start_run_request(manager, thread_store=thread_store, auth_source=AUTH_SOURCE_INTERNAL)
        gateway_request.headers = httpx.Headers({INTERNAL_OWNER_USER_ID_HEADER_NAME: OWNER_ID})
        gateway_request.state.user = get_internal_user(owner_user_id=OWNER_ID)
        bindings = AgentConversations(None, session_factory=sf)
        gateway_request.app.state.agent_conversations = bindings
        bridge = MemoryStreamBridge()
        gateway_request.app.state.stream_bridge = bridge
        worker_inputs = []

        async def capture_worker(bridge, run_manager, record, *, graph_input, config, **kwargs):
            workers.add(asyncio.current_task())
            worker_inputs.append({"graph_input": graph_input, "config": config, "record": record, "kwargs": kwargs})
            await run_manager.set_status(record.run_id, RunStatus.running)
            await run_manager.set_status(record.run_id, RunStatus.success)
            await bridge.publish_end(record.run_id)

        monkeypatch.setattr(services, "run_agent", capture_worker)
        yield SimpleNamespace(
            request=gateway_request,
            manager=manager,
            store=store,
            thread_store=thread_store,
            owner=owner,
            owner_id=OWNER_ID,
            thread_id=THREAD_ID,
            bindings=bindings,
            worker_inputs=worker_inputs,
            sf=sf,
            checkpointer=gateway_request.app.state.checkpointer,
            provider=provider,
            config=config,
            engine=engine,
            dialect=request.param,
        )
    finally:
        if manager is not None:
            await manager.shutdown(timeout=2)
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        if bridge is not None:
            await bridge.close()
        await engine.dispose()
        if admin is not None:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await admin.dispose()

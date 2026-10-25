"""Actual creating service retains native metadata I/O through run shutdown."""

import asyncio
import threading
from contextvars import ContextVar
from uuid import UUID

import aiosqlite
import httpx
import pytest
from _channel_execution_test_support import TABLES, ControlledProvider
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway import channel_execution_reads, deps, services
from app.gateway.auth.models import User
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import thread_runs
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.config import app_config
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.checkpointer.async_provider import make_checkpointer

OWNER = "00000000-0000-0000-0000-00000000000a"
THREAD = "captured-native-metadata"
PUBLICATION = "dc" * 16


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
async def test_prestart_abort_shutdown_retains_executed_metadata_query(capture_environment, output_store, monkeypatch):
    e = capture_environment
    assert e.dialect == "sqlite"
    async with e.engine.connect() as connection:
        mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        assert mode.scalar().upper() == "WAL"
        await connection.commit()
    async with e.engine.begin() as connection:
        await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: TABLES[0].metadata.create_all(sync, tables=TABLES))

    config = app_config.AppConfig.model_validate({"database": {"backend": "memory"}, "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config, name, getattr(app_config, name))
    app_config.set_app_config(config)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    provider = ControlledProvider(User(id=UUID(OWNER), email="capture-metadata@example.com", system_role="user"))
    for module in (deps, services, channel_execution_reads):
        monkeypatch.setattr(module, "get_local_provider", lambda: provider)
    await e.thread_store.create(THREAD, assistant_id="lead_agent", user_id=OWNER)

    marker = ContextVar("native_metadata_observation", default=False)
    native_started, release, native_returned, native_closed = (threading.Event() for _ in range(4))
    metadata_tasks, held = [], {}
    original_metadata = services._ensure_thread_metadata
    original_execute = aiosqlite.Connection._execute

    async def observe_metadata(*args, **kwargs):
        metadata_tasks.append(asyncio.current_task())
        token = marker.set(True)
        try:
            return await original_metadata(*args, **kwargs)
        finally:
            marker.reset(token)

    async def observe_execute(connection, function, *args, **kwargs):
        name = getattr(function, "__name__", "")
        if marker.get() and not held and name == "execute" and args and "FROM THREADS_META" in str(args[0]).upper():
            held["cursor"] = function.__self__

            def execute_then_hold(*native_args, **native_kwargs):
                result = function(*native_args, **native_kwargs)
                native_started.set()
                try:
                    assert release.wait(10), "Controlled native metadata query was not released"
                    return result
                finally:
                    native_returned.set()

            return await original_execute(connection, execute_then_hold, *args, **kwargs)
        result = await original_execute(connection, function, *args, **kwargs)
        cursor = held.get("cursor")
        if cursor is not None and name == "close" and function.__self__ in (cursor, cursor.connection):
            native_closed.set()
        return result

    monkeypatch.setattr(services, "_ensure_thread_metadata", observe_metadata)
    monkeypatch.setattr(aiosqlite.Connection, "_execute", observe_execute)
    factory_calls = []

    def factory(**kwargs):
        factory_calls.append(kwargs)
        raise AssertionError("Aborted prestart metadata reached graph construction")

    monkeypatch.setattr(services, "resolve_agent_factory", lambda _assistant: factory)
    bridge = MemoryStreamBridge()
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)
    app.add_middleware(AuthMiddleware)
    app.state.stream_bridge = bridge
    app.state.run_manager = e.manager
    app.state.store = None
    app.state.run_event_store = e.event_store
    app.state.run_events_config = None
    app.state.thread_store = e.thread_store
    app.state.run_store = e.repository
    app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
    app.include_router(thread_runs.router)
    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=httpx.ASGITransport(app=app, raise_app_exceptions=False))

    async def before_fixture_dispatch():
        # The actual Gateway still enforces its authenticated creating route.
        return None

    record, closing = None, None
    async with make_checkpointer(config) as checkpointer:
        app.state.checkpointer = checkpointer
        try:
            response = await owner.request_json(
                operation="run_create",
                owner_user_id=OWNER,
                method="POST",
                path=f"threads/{THREAD}/runs",
                body={
                    "assistant_id": "lead_agent",
                    "input": {"messages": [{"role": "human", "content": "Never execute a model"}]},
                    "output_capture": {"version": 1},
                    "channel_execution": {"version": 1, "publication_id": PUBLICATION},
                    "on_disconnect": "continue",
                    "on_completion": None,
                    "multitask_strategy": "reject",
                },
                headers={"Idempotency-Key": PUBLICATION},
                before_dispatch=before_fixture_dispatch,
            )
            assert response.status_code == 200, response.body
            record = await e.manager.get(response.body["run_id"], user_id=OWNER)
            assert record is not None and record.task is not None
            assert await asyncio.to_thread(native_started.wait, 3), "Actual metadata SELECT did not execute"
            await e.manager.cancel(record.run_id)
            stored = await e.repository.get(record.run_id, user_id=OWNER)
            assert stored["status"] == "interrupted"
            assert stored["kwargs"]["output_evidence"]["reason"] == "cancelled"
            closing = asyncio.create_task(e.manager.shutdown(timeout=0.02))
            done, _ = await asyncio.wait({closing}, timeout=0.05)
            assert not native_returned.is_set() and not native_closed.is_set()
            assert factory_calls == []
            assert not done, "Run shutdown must retain the already executed native metadata query"
        finally:
            release.set()
            if closing is None:
                closing = asyncio.create_task(e.manager.shutdown(timeout=0.02))
            await asyncio.wait_for(asyncio.shield(closing), 5)
            await asyncio.gather(*metadata_tasks, return_exceptions=True)
            if record is not None and record.task is not None:
                if not record.task.done():
                    record.task.cancel()
                await asyncio.gather(record.task, return_exceptions=True)
            await owner.stop()
            await bridge.close()
    assert native_returned.is_set() and native_closed.is_set()

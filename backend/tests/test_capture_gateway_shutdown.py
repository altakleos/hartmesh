"""Real Gateway lifespan keeps native SQL and resident preparation within one grace."""

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiosqlite
import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _run_output_store_test_support import RAW_OWNER
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway import deps
from deerflow.config.app_config import AppConfig
from deerflow.runtime.runs.capture_io import CaptureIOCleanupError


@pytest.mark.asyncio
async def test_inflight_drain_reports_failed_owned_resource_cleanup():
    class Manager:
        async def shutdown(self, *, timeout):
            raise CaptureIOCleanupError()

    with pytest.raises(CaptureIOCleanupError):
        await deps._drain_inflight_runs(Manager())


@pytest.mark.asyncio
@pytest.mark.parametrize("absolute", [True, False])
async def test_queued_shutdown_start_does_not_receive_an_expired_grace_again(absolute):
    loop, calls = asyncio.get_running_loop(), []
    deadline, blocked_at = loop.time() + 0.03, []

    class Manager:
        async def shutdown(self, *, timeout):
            calls.append((loop.time(), timeout))

    def delay_queued_start():
        blocked_at.append(loop.time())
        # Exercise the scheduling delay between the host helper and its child.
        time.sleep(0.06)

    closing = asyncio.create_task(deps._drain_inflight_runs(Manager(), deadline=deadline if absolute else None))
    loop.call_soon(delay_queued_start)
    await closing
    assert blocked_at[0] < deadline < calls[0][0], "Controlled queue delay must span the original deadline"
    assert calls[0][1] == (0 if absolute else deps._RUN_DRAIN_TIMEOUT_SECONDS)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
async def test_real_lifespan_retires_capture_during_resident_wait_and_closes_sql_after_native_return(capture_environment, monkeypatch):
    e, app = capture_environment, FastAPI()
    config = AppConfig.model_validate({"database": {"backend": "memory"}, "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", AsyncMock())
    monkeypatch.setattr("deerflow.persistence.engine.get_session_factory", lambda: None)
    monkeypatch.setattr(deps, "_RUN_DRAIN_TIMEOUT_SECONDS", 0.03)
    native_started, release_native, native_returned = threading.Event(), threading.Event(), threading.Event()
    resident_entered, release_resident, sealed, engine_closed = (asyncio.Event() for _ in range(4))
    original_execute = aiosqlite.Connection._execute

    async def observed(connection, function, *args, **kwargs):
        if getattr(function, "__name__", "") == "execute" and args and "capture_native_lifespan" in str(args[0]):

            def hold(*native_args, **native_kwargs):
                result = function(*native_args, **native_kwargs)
                native_started.set()
                try:
                    assert release_native.wait(10)
                    return result
                finally:
                    native_returned.set()

            return await original_execute(connection, hold, *args, **kwargs)
        return await original_execute(connection, function, *args, **kwargs)

    async def close_engine():
        assert native_returned.is_set()
        assert e.engine.sync_engine.pool.checkedout() == 0
        await e.engine.dispose()
        engine_closed.set()

    async def resident_drain():
        assert sealed.is_set()
        resident_entered.set()
        await release_resident.wait()

    monkeypatch.setattr(aiosqlite.Connection, "_execute", observed)
    monkeypatch.setattr("deerflow.persistence.engine.close_engine", close_engine)
    context = deps.langgraph_runtime(app, config)
    await context.__aenter__()
    app.state.run_manager = e.manager
    app.state.resident_runtime = SimpleNamespace(seal=sealed.set, drain_admissions=resident_drain, close=AsyncMock())
    record = await e.manager.create_or_reject("native-lifespan", "lead_agent", user_id=RAW_OWNER, kwargs={"output_capture": {"version": 1}, "input": {"messages": []}})
    owner = e.manager.get_capture_io_owner(record)

    async def read():
        async with e.sf() as session:
            return (await session.execute(text("SELECT 37 AS capture_native_lifespan"))).scalar_one()

    pending, closing = asyncio.create_task(owner.run(read)), None
    try:
        assert await asyncio.to_thread(native_started.wait, 3)
        closing = asyncio.create_task(context.__aexit__(None, None, None))
        await asyncio.wait_for(resident_entered.wait(), 3)
        await asyncio.wait_for(owner.wait_retired(), 0.3)
        assert not release_resident.is_set() and not engine_closed.is_set()
        assert not closing.done() and not native_returned.is_set()
        for _ in range(2):
            closing.cancel()
            await asyncio.wait({closing}, timeout=0.01)
        release_resident.set()
        assert not engine_closed.is_set()
        release_native.set()
        assert await pending == 37
        result = (await asyncio.gather(closing, return_exceptions=True))[0]
        assert isinstance(result, asyncio.CancelledError) and engine_closed.is_set()
    finally:
        release_resident.set()
        release_native.set()
        await asyncio.gather(pending, return_exceptions=True)
        if closing is None:
            await context.__aexit__(None, None, None)
        else:
            await asyncio.gather(closing, return_exceptions=True)

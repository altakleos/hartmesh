"""The optional supervisor shares the existing channel lifetime, including failures."""

import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from test_gateway_lifespan_shutdown import _gateway_lifespan_patches

from app.gateway import app as gateway
from deerflow.config.app_config import AppConfig

pytestmark = pytest.mark.asyncio


def configuration(*, present=True):
    return AppConfig(sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"}, **({"telegram_bots": {"enabled": False}} if present else {}))


def supervisor(events, *, begin_error=None, stop_error=None):
    sup = MagicMock()

    async def check():
        events.append("check")

    async def start():
        events.append("start")

    async def begin():
        events.append("begin")
        if begin_error:
            raise begin_error

    async def stop():
        events.append("stop")
        if stop_error:
            raise stop_error

    sup.check = AsyncMock(side_effect=check)
    sup.start = AsyncMock(side_effect=start)
    sup.begin_shutdown = AsyncMock(side_effect=begin)
    sup.stop = AsyncMock(side_effect=stop)
    sup.drain_after_stop_timeout = AsyncMock()
    return sup


async def test_absent_section_creates_no_supervisor_or_telegram_state(monkeypatch):
    events = []
    app = FastAPI()
    monkeypatch.setattr("app.channels.service.start_channel_service", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr("app.channels.service.stop_channel_service", AsyncMock())
    ctor = MagicMock(side_effect=AssertionError("absent integration must not construct"))
    monkeypatch.setattr("app.telegram_bots.supervisor.TelegramSupervisor", ctor)
    async with gateway._channel_services_lifetime(app, configuration(present=False)):
        events.append("serve")
    assert not hasattr(app.state, "telegram_bots") and not hasattr(app.state, "telegram_bots_start")
    ctor.assert_not_called()


@pytest.mark.parametrize("failure_stage", [None, "mcp", "batch", "export", "request", "cancel"])
async def test_cleanup_spans_later_startup_and_serving_and_preserves_original_failure(monkeypatch, failure_stage):
    events = []
    app = FastAPI()
    sup = supervisor(events, begin_error=RuntimeError("cleanup begin failed"), stop_error=RuntimeError("cleanup stop failed"))
    channel = MagicMock()
    monkeypatch.setattr("app.channels.service.start_channel_service", AsyncMock(return_value=channel))

    async def stop_channels():
        events.append("channels-stop")

    monkeypatch.setattr("app.channels.service.stop_channel_service", stop_channels)
    monkeypatch.setattr("app.telegram_bots.supervisor.TelegramSupervisor", MagicMock(return_value=sup))
    original = asyncio.CancelledError() if failure_stage == "cancel" else RuntimeError(failure_stage)

    async def run():
        async with gateway._channel_services_lifetime(app, configuration()):
            assert events == ["check"]
            assert app.state.telegram_bots is sup
            await asyncio.sleep(0)
            events.append("yield")
            if failure_stage is not None:
                raise original

    if failure_stage is None:
        await run()
    else:
        with pytest.raises(type(original)) as caught:
            await run()
        assert caught.value is original
    assert events[-3:] == ["begin", "channels-stop", "stop"]
    assert app.state.telegram_bots_start.done()


@pytest.mark.parametrize("stage", ["mcp", "batch", "export"])
async def test_actual_gateway_lifespan_cleans_channels_when_later_startup_fails(stage, monkeypatch):
    app = FastAPI()
    config = configuration(present=False)
    stopped = AsyncMock()
    error = RuntimeError("later-startup-fixture")
    with ExitStack() as stack:
        for patcher in _gateway_lifespan_patches(config):
            stack.enter_context(patcher)
        stack.enter_context(patch("app.channels.service.stop_channel_service", stopped))
        if stage == "mcp":
            stack.enter_context(patch("deerflow.mcp.tasks.runtime.validate_mcp_task_runtime_configuration", side_effect=error))
        elif stage == "batch":
            stack.enter_context(patch("deerflow.subagents.batch_runtime.set_subagent_batch_submitter", side_effect=error))
        else:
            stack.enter_context(patch("app.gateway.account_export.runs_in_this_process", side_effect=error))
        with pytest.raises(RuntimeError) as caught:
            async with gateway.lifespan(app):
                raise AssertionError("must not serve")
        assert caught.value is error
    stopped.assert_awaited_once()


async def test_repeated_caller_cancellation_does_not_detach_channel_cleanup(monkeypatch):
    events = []
    app = FastAPI()
    sup = supervisor(events)
    entered, release = asyncio.Event(), asyncio.Event()

    async def begin():
        events.append("begin")
        entered.set()
        await release.wait()

    async def stop_channels():
        events.append("channels-stop")

    sup.begin_shutdown = AsyncMock(side_effect=begin)
    monkeypatch.setattr("app.channels.service.start_channel_service", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr("app.channels.service.stop_channel_service", stop_channels)
    monkeypatch.setattr("app.telegram_bots.supervisor.TelegramSupervisor", MagicMock(return_value=sup))

    async def run():
        async with gateway._channel_services_lifetime(app, configuration()):
            await asyncio.sleep(0)

    task = asyncio.create_task(run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert events[-3:] == ["begin", "channels-stop", "stop"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_supervisor_stop_uses_one_gateway_timeout_and_reaches_cleanup(monkeypatch):
    app, events = FastAPI(), []
    sup = supervisor(events)
    cancelled = asyncio.Event()

    async def hanging_stop():
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    sup.stop = AsyncMock(side_effect=hanging_stop)
    monkeypatch.setattr(gateway, "_SHUTDOWN_HOOK_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr("app.channels.service.start_channel_service", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr("app.channels.service.stop_channel_service", AsyncMock())
    monkeypatch.setattr("app.telegram_bots.supervisor.TelegramSupervisor", MagicMock(return_value=sup))
    async with gateway._channel_services_lifetime(app, configuration()):
        await asyncio.sleep(0)
    assert cancelled.is_set()
    sup.stop.assert_awaited_once()
    sup.drain_after_stop_timeout.assert_awaited_once()


async def test_internal_timeout_error_does_not_retire_retry_intent(monkeypatch):
    app, events = FastAPI(), []
    sup = supervisor(events, stop_error=TimeoutError("internal fixture timeout"))
    monkeypatch.setattr("app.channels.service.start_channel_service", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr("app.channels.service.stop_channel_service", AsyncMock())
    monkeypatch.setattr("app.telegram_bots.supervisor.TelegramSupervisor", MagicMock(return_value=sup))
    async with gateway._channel_services_lifetime(app, configuration()):
        await asyncio.sleep(0)
    sup.stop.assert_awaited_once()
    sup.drain_after_stop_timeout.assert_not_awaited()


@pytest.mark.parametrize("cancel_waiter", [False, True])
async def test_expired_stop_grace_drains_same_owner_before_runtime_exit(monkeypatch, cancel_waiter):
    app, events = FastAPI(), []
    sup = supervisor(events)
    grace_expired, release_cleanup, sdk_closed, runtime_exited = (asyncio.Event() for _ in range(4))
    owned = None

    async def cleanup():
        events.append("cleanup-start")
        await release_cleanup.wait()
        sdk_closed.set()
        events.append("clients-closed")

    async def stop():
        nonlocal owned
        assert owned is None, "Gateway must start stop only once"
        owned = asyncio.create_task(cleanup())
        await asyncio.shield(owned)

    async def retire_and_drain():
        events.append("grace-retirement")
        grace_expired.set()
        await asyncio.shield(owned)

    sup.stop = AsyncMock(side_effect=stop)
    sup.drain_after_stop_timeout = AsyncMock(side_effect=retire_and_drain)
    monkeypatch.setattr(gateway, "_SHUTDOWN_HOOK_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr("app.channels.service.start_channel_service", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr("app.channels.service.stop_channel_service", AsyncMock())
    monkeypatch.setattr("app.telegram_bots.supervisor.TelegramSupervisor", MagicMock(return_value=sup))

    async def lifetime():
        try:
            async with gateway._channel_services_lifetime(app, configuration()):
                await asyncio.sleep(0)
        finally:
            events.append("runtime-exit")
            runtime_exited.set()

    exiting = asyncio.create_task(lifetime())
    try:
        await asyncio.wait_for(grace_expired.wait(), 1)
        assert not exiting.done() and not runtime_exited.is_set() and not sdk_closed.is_set()
        if cancel_waiter:
            for _ in range(2):
                exiting.cancel()
                await asyncio.sleep(0)
            assert not exiting.done() and not runtime_exited.is_set()
        release_cleanup.set()
        if cancel_waiter:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(exiting, 1)
        else:
            await asyncio.wait_for(exiting, 1)
        assert sdk_closed.is_set() and runtime_exited.is_set()
        assert events.index("clients-closed") < events.index("runtime-exit")
        sup.stop.assert_awaited_once()
        sup.drain_after_stop_timeout.assert_awaited_once()
    finally:
        release_cleanup.set()
        await asyncio.gather(exiting, *(task for task in (owned,) if task is not None), return_exceptions=True)

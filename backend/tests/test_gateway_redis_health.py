"""Required Redis probes share work and borrow the clients the application uses."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.config.auth_config import LocalAuthConfig


@pytest.fixture
def health(monkeypatch):
    import app.gateway.redis_health as module

    monkeypatch.setattr(module, "_CACHE_SECONDS", 0)
    return module


@pytest.mark.asyncio
async def test_probe_uses_startup_bridge_and_current_login_store_and_recovers(health, monkeypatch, caplog):
    from app.gateway.auth.login_throttle import RedisLoginThrottleStore
    from deerflow.runtime.stream_bridge.redis import RedisStreamBridge

    stream_client = SimpleNamespace(ping=AsyncMock(return_value=True))
    first_client = SimpleNamespace(ping=AsyncMock(side_effect=ConnectionError("redis://secret@old:6379")))
    second_client = SimpleNamespace(ping=AsyncMock(return_value=True))
    bridge = RedisStreamBridge(redis_url="redis://startup", client=stream_client)
    stores = {name: RedisLoginThrottleStore(name, key_prefix="test", client=client) for name, client in (("old", first_client), ("new", second_client))}
    local = LocalAuthConfig(lockout_store="redis", lockout_store_redis_url="old")
    namespace = object()
    selected = []

    def select(config, *, tenant_namespace):
        selected.append((config.lockout_store_redis_url, tenant_namespace))
        return stores[config.lockout_store_redis_url]

    monkeypatch.setattr(health, "get_login_throttle_store", select)
    probe = health.RequiredRedisReadiness(bridge, local_config=lambda: local, tenant_namespace=namespace)
    try:
        assert await probe.check() == "unreachable"
        first_client.ping.side_effect = None
        first_client.ping.return_value = True
        assert await probe.check() == "ok"
        local = LocalAuthConfig(lockout_store="redis", lockout_store_redis_url="new")
        assert await probe.check() == "ok"
        assert selected == [("old", namespace), ("old", namespace), ("new", namespace)]
        assert stream_client.ping.await_count == 3
        assert second_client.ping.await_count == 1
        assert "secret" not in caplog.text and "redis://" not in caplog.text
        stream_client.ping.side_effect = ConnectionError("private URL")
        assert await probe.check() == "unreachable"
    finally:
        await probe.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("local", [LocalAuthConfig(), LocalAuthConfig(enabled=False, lockout_store="redis")])
async def test_optional_memory_and_disabled_local_login_open_no_redis(health, monkeypatch, local):
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    monkeypatch.setattr(health, "get_login_throttle_store", lambda *_a, **_k: pytest.fail("unexpected store"))
    probe = health.RequiredRedisReadiness(MemoryStreamBridge(), local_config=lambda: local)
    try:
        assert await probe.check() == "not_configured"
    finally:
        await probe.close()


@pytest.mark.asyncio
async def test_public_probe_burst_coalesces_and_waiter_cancellation_keeps_owner(health):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def ping():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return True

    probe = health.RequiredRedisReadiness(SimpleNamespace(check_health=ping), local_config=LocalAuthConfig)
    requests = [asyncio.create_task(probe.check()) for _ in range(30)]
    try:
        await asyncio.wait_for(entered.wait(), 2)
        requests[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await requests[0]
        requests.append(asyncio.create_task(probe.check()))
        release.set()
        assert await asyncio.gather(*requests[1:]) == ["ok"] * 30
        assert calls == 1
    finally:
        release.set()
        await asyncio.gather(*requests, return_exceptions=True)
        await probe.close()


@pytest.mark.asyncio
async def test_timeout_does_not_enqueue_more_config_workers_or_select_stale_store(health, monkeypatch):
    started, release = threading.Event(), threading.Event()
    calls = 0

    def config():
        nonlocal calls
        calls += 1
        started.set()
        assert release.wait(5)
        return LocalAuthConfig(lockout_store="redis")

    monkeypatch.setattr(health, "_TIMEOUT_SECONDS", 0.03)
    monkeypatch.setattr(health, "get_login_throttle_store", lambda *_a, **_k: pytest.fail("stale config selected store"))
    probe = health.RequiredRedisReadiness(object(), local_config=config)
    try:
        assert await probe.check() == "unreachable"
        assert started.is_set()
        assert await asyncio.gather(*(probe.check() for _ in range(10))) == ["unreachable"] * 10
        assert calls == 1
        closing = asyncio.create_task(probe.close())
        await asyncio.sleep(0)
        assert not closing.done()
        release.set()
        await asyncio.wait_for(closing, 2)
        assert await probe.check() == "unreachable"
    finally:
        release.set()
        await probe.close()


@pytest.mark.asyncio
async def test_redis_failure_degrades_readiness_without_changing_liveness(health, monkeypatch):
    import httpx

    from app.gateway.app import create_app
    from deerflow.config.checkpointer_config import CheckpointerConfig

    app = create_app()
    app.state.checkpointer_config = CheckpointerConfig(type="memory")
    app.state.redis_readiness = SimpleNamespace(check=AsyncMock(return_value="unreachable"))
    monkeypatch.setattr("app.gateway.health.get_engine", lambda: None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        ready = await client.get("/health/ready")
        assert ready.status_code == 503
        assert ready.json()["redis"] == "unreachable"
        assert (await client.get("/health")).status_code == 200
        app.state.redis_readiness.check.return_value = "ok"
        assert (await client.get("/health/ready")).status_code == 200


@pytest.mark.asyncio
async def test_hanging_ping_is_cancelled_and_next_attempt_recovers(health, monkeypatch):
    stopped = asyncio.Event()

    async def hang():
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    bridge = SimpleNamespace(check_health=hang)
    monkeypatch.setattr(health, "_TIMEOUT_SECONDS", 0.03)
    probe = health.RequiredRedisReadiness(bridge, local_config=LocalAuthConfig)
    try:
        assert await probe.check() == "unreachable"
        await asyncio.wait_for(stopped.wait(), 1)
        await asyncio.wait_for(asyncio.shield(probe._task), 1)
        bridge.check_health = AsyncMock(return_value=True)
        assert await probe.check() == "ok"
    finally:
        await probe.close()


@pytest.mark.asyncio
async def test_completed_result_cache_expires_and_observes_recovery():
    import app.gateway.redis_health as module

    ping = AsyncMock(return_value=False)
    probe = module.RequiredRedisReadiness(SimpleNamespace(check_health=ping), local_config=LocalAuthConfig)
    try:
        assert module._CACHE_SECONDS == 1.0
        assert await probe.check() == "unreachable"
        ping.return_value = True
        assert await asyncio.gather(*(probe.check() for _ in range(20))) == ["unreachable"] * 20
        assert ping.await_count == 1
        await asyncio.sleep(1.05)
        assert await probe.check() == "ok"
        assert ping.await_count == 2
    finally:
        await probe.close()


@pytest.mark.asyncio
async def test_shutdown_drains_readiness_before_clients_across_repeated_cancellation(monkeypatch):
    from contextlib import asynccontextmanager
    from importlib import import_module

    from fastapi import FastAPI

    app_module = import_module("app.gateway.app")
    module = import_module("app.gateway.redis_health")
    events = []
    started, release = asyncio.Event(), asyncio.Event()
    app = FastAPI()
    bridge = object()

    @asynccontextmanager
    async def runtime(app, _config):
        app.state.stream_bridge = bridge
        try:
            yield
        finally:
            events.append("runtime")

    class Probe:
        def __init__(self, actual, **_kwargs):
            assert actual is bridge

        async def close(self):
            events.append("readiness-start")
            started.set()
            await release.wait()
            events.append("readiness-end")

    async def auth_close():
        events.append("auth")

    async def mcp_close():
        events.append("mcp")

    monkeypatch.setattr(app_module, "langgraph_runtime", runtime)
    monkeypatch.setattr(module, "RequiredRedisReadiness", Probe)
    monkeypatch.setattr(app_module.auth, "close_auth_clients", auth_close)
    monkeypatch.setattr("deerflow.mcp.session_pool.get_session_pool", lambda: SimpleNamespace(close_all=mcp_close))
    context = app_module._runtime_with_mcp_pool_shutdown(app, object())
    await context.__aenter__()
    closing = asyncio.create_task(context.__aexit__(None, None, None))
    try:
        await asyncio.wait_for(started.wait(), 1)
        closing.cancel()
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        assert events == ["readiness-start"]
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert events == ["readiness-start", "readiness-end", "auth", "runtime", "mcp"]
    finally:
        release.set()
        await asyncio.gather(closing, return_exceptions=True)

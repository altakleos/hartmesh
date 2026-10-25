"""Shared bridge shutdown fences later effects without owning borrowed clients."""

import asyncio

import pytest
from test_stream_bridge import _FakeRedis

from deerflow.runtime.stream_bridge import redis as redis_module


class ObservedRedis(_FakeRedis):
    def __init__(self):
        super().__init__()
        self.calls = []

    def pipeline(self, **kwargs):
        self.calls.append("pipeline")
        return super().pipeline(**kwargs)

    async def delete(self, name):
        self.calls.append("delete")
        return await super().delete(name)

    async def exists(self, name):
        self.calls.append("exists")
        return await super().exists(name)

    async def xread(self, *args, **kwargs):
        self.calls.append("xread")
        return await super().xread(*args, **kwargs)

    async def ping(self):
        self.calls.append("ping")
        return True

    async def aclose(self):
        self.calls.append("close")
        await super().aclose()


def bridge_for_client(monkeypatch, *, owns_client):
    client = ObservedRedis()
    if owns_client:
        monkeypatch.setattr(redis_module.Redis, "from_url", staticmethod(lambda *args, **kwargs: client))
    bridge = redis_module.RedisStreamBridge(redis_url="redis://fixture.invalid", client=None if owns_client else client)
    return bridge, client


@pytest.mark.asyncio
@pytest.mark.parametrize("owns_client", [False, True])
@pytest.mark.parametrize("operation", ["publish", "end", "cleanup", "exists", "subscribe", "health"])
async def test_closed_bridge_refuses_late_client_effects(monkeypatch, owns_client, operation):
    bridge, client = bridge_for_client(monkeypatch, owns_client=owns_client)
    await bridge.publish("kept", "values", {"text": "before close"})
    await bridge.publish_end("kept")
    await bridge.close()
    before = list(client.calls)
    caught, subscription = None, None
    try:
        if operation == "publish":
            await bridge.publish("kept", "values", {"text": "late"})
        elif operation == "end":
            await bridge.publish_end("kept")
        elif operation == "cleanup":
            await bridge.cleanup("kept")
        elif operation == "exists":
            await bridge.stream_exists("kept")
        elif operation == "health":
            await bridge.check_health()
        else:
            subscription = bridge.subscribe("kept")
            await anext(subscription)
    except RuntimeError as error:
        caught = error
    finally:
        if subscription is not None:
            await subscription.aclose()
    # Observe actual calls before testing the desired fixed refusal. The
    # offline client approximates reconnect-capable Redis; no live service ran.
    assert client.calls == before, "Closed bridge reached its shared client"
    assert caught is not None and str(caught) == "Stream bridge is closed"
    assert client.closed is owns_client


@pytest.mark.asyncio
@pytest.mark.parametrize("owns_client", [False, True])
async def test_bridge_close_is_idempotent_and_borrowed_client_stays_usable(monkeypatch, owns_client):
    bridge, client = bridge_for_client(monkeypatch, owns_client=owns_client)
    await bridge.publish("kept", "values", {"text": "before close"})
    await bridge.close()
    await bridge.close()
    assert client.calls.count("close") == int(owns_client)
    if not owns_client:
        assert not client.closed
        assert await client.exists("deerflow:stream_bridge:kept") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("owns_client", [False, True])
async def test_delayed_cleanup_rechecks_closed_bridge(monkeypatch, owns_client):
    bridge, client = bridge_for_client(monkeypatch, owns_client=owns_client)
    entered, release = asyncio.Event(), asyncio.Event()

    class Clock:
        def __getattr__(self, name):
            return getattr(asyncio, name)

        async def sleep(self, seconds):
            assert seconds == 60
            entered.set()
            await release.wait()

    monkeypatch.setattr(redis_module, "asyncio", Clock())
    pending = asyncio.create_task(bridge.cleanup("kept", delay=60))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await bridge.close()
        before = list(client.calls)
        release.set()
        result = (await asyncio.gather(pending, return_exceptions=True))[0]
        assert client.calls == before, "A previously delayed cleanup reached the closed client"
        assert isinstance(result, RuntimeError) and str(result) == "Stream bridge is closed"
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("owns_client", [False, True])
async def test_started_subscription_rechecks_close_before_next_redis_read(monkeypatch, owns_client):
    bridge, client = bridge_for_client(monkeypatch, owns_client=owns_client)
    await bridge.publish("kept", "values", {"text": "before close"})
    subscription = bridge.subscribe("kept", heartbeat_interval=0.01)
    try:
        assert (await anext(subscription)).event == "values"
        await bridge.close()
        before = list(client.calls)
        result = (await asyncio.gather(anext(subscription), return_exceptions=True))[0]
        assert client.calls == before, "An existing subscription read again after bridge close"
        assert isinstance(result, RuntimeError) and str(result) == "Stream bridge is closed"
    finally:
        await subscription.aclose()


@pytest.mark.asyncio
async def test_owned_close_is_retained_through_repeated_waiter_cancellation(monkeypatch):
    bridge, client = bridge_for_client(monkeypatch, owns_client=True)
    entered, release, closed, delegate_cancelled = (asyncio.Event() for _ in range(4))

    async def close_client():
        client.calls.append("close")
        entered.set()
        try:
            await release.wait()
            client.closed = True
            closed.set()
        except asyncio.CancelledError:
            delegate_cancelled.set()
            raise

    monkeypatch.setattr(client, "aclose", close_client)
    pending = asyncio.create_task(bridge.close())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        pending.cancel()
        await asyncio.wait({pending}, timeout=0.01)
        pending.cancel()
        done, _ = await asyncio.wait({pending}, timeout=0.01)
        premature = bool(done)
        was_cancelled = delegate_cancelled.is_set()
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await bridge.close()
        assert not premature and not was_cancelled, "Started client close escaped its cancelled waiter"
        assert closed.is_set() and client.calls.count("close") == 1
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)

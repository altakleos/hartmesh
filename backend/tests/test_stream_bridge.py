"""Tests for StreamBridge implementations."""

import asyncio
import os
import re
import uuid
from collections import defaultdict
from types import MappingProxyType

import anyio
import pytest
from pydantic import ValidationError

from deerflow.config.deployment_config import DeploymentConfig
from deerflow.config.stream_bridge_config import MAX_HEARTBEAT_INTERVAL_SECONDS, StreamBridgeConfig, set_stream_bridge_config
from deerflow.runtime import END_SENTINEL, HEARTBEAT_SENTINEL, MemoryStreamBridge, StreamGap, make_stream_bridge

# RedisStreamBridge is no longer re-exported from deerflow.runtime (redis is an
# optional extra; see the NOTE in runtime/stream_bridge/__init__.py). Import it
# directly from the submodule.
from deerflow.runtime.stream_bridge.redis import RedisStreamBridge
from deerflow.runtime.tenant_identity import (
    RedisTenantComponent,
    TenantIdentityV1,
    TenantSubsystem,
    redis_component_key_prefix,
)


def _stream_id_gt(left: str, right: str) -> bool:
    left_ms, left_seq = left.split("-", 1)
    right_ms, right_seq = right.split("-", 1)
    return (int(left_ms), int(left_seq)) > (int(right_ms), int(right_seq))


class _FakeRedis:
    def __init__(self) -> None:
        self.streams = defaultdict(list)
        self.conditions = defaultdict(asyncio.Condition)
        self.counters = defaultdict(int)
        self.deleted = []
        self.expirations = []
        self.closed = False

    async def xadd(self, name, fields, maxlen=None, approximate=True):
        self.counters[name] += 1
        event_id = f"{self.counters[name]}-0"
        async with self.conditions[name]:
            self.streams[name].append((event_id, dict(fields)))
            if maxlen is not None and len(self.streams[name]) > maxlen:
                del self.streams[name][: len(self.streams[name]) - maxlen]
            self.conditions[name].notify_all()
        return event_id

    async def xread(self, streams, count=None, block=None):
        [(name, last_id)] = list(streams.items())
        timeout = None if block is None else block / 1000
        while True:
            async with self.conditions[name]:
                entries = [(event_id, fields) for event_id, fields in self.streams.get(name, []) if _stream_id_gt(event_id, last_id)]
                if entries:
                    return [(name, entries[:count] if count is not None else entries)]
                if timeout is None:
                    return []
                try:
                    await asyncio.wait_for(self.conditions[name].wait(), timeout=timeout)
                except TimeoutError:
                    return []

    async def xrevrange(self, name, max="+", min="-", count=None):
        entries = list(reversed(self.streams.get(name, [])))
        return entries[:count] if count is not None else entries

    async def xrange(self, name, min="-", max="+", count=None):
        entries = list(self.streams.get(name, []))
        return entries[:count] if count is not None else entries

    async def delete(self, name):
        self.deleted.append(name)
        self.streams.pop(name, None)
        return 1

    async def exists(self, name):
        return 1 if name in self.streams else 0

    async def expire(self, name, seconds):
        self.expirations.append((name, seconds))
        return True

    def pipeline(self, *, transaction=True):
        return _FakeRedisPipeline(self)

    async def aclose(self):
        self.closed = True


class _RecordingRedis(_FakeRedis):
    def __init__(self) -> None:
        super().__init__()
        self.redis_calls: list[tuple[str, str]] = []

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        self.redis_calls.append(("xadd", name))
        return await super().xadd(
            name,
            fields,
            maxlen=maxlen,
            approximate=approximate,
        )

    async def xread(
        self,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        for name in streams:
            self.redis_calls.append(("xread", name))
        return await super().xread(streams, count=count, block=block)

    async def xrevrange(
        self,
        name: str,
        max: str = "+",
        min: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        self.redis_calls.append(("xrevrange", name))
        return await super().xrevrange(name, max=max, min=min, count=count)

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        self.redis_calls.append(("xrange", name))
        return await super().xrange(name, min=min, max=max, count=count)

    async def delete(self, name: str) -> int:
        self.redis_calls.append(("delete", name))
        return await super().delete(name)

    async def exists(self, name: str) -> int:
        self.redis_calls.append(("exists", name))
        return await super().exists(name)

    async def expire(self, name: str, seconds: int) -> bool:
        self.redis_calls.append(("expire", name))
        return await super().expire(name, seconds)


class _DelayedBlockingReadRedis:
    """Hold the first blocking XREAD response while the stream is trimmed."""

    def __init__(self, redis) -> None:
        self._delegate = redis
        self.blocking_read_started = asyncio.Event()
        self.wake_response_captured = asyncio.Event()
        self.release_wake_response = asyncio.Event()

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    async def xread(self, streams, count=None, block=None):
        if block is None:
            return await self._delegate.xread(streams, count=count, block=block)

        self.blocking_read_started.set()
        response = await self._delegate.xread(streams, count=count, block=block)
        if response:
            self.wake_response_captured.set()
            await self.release_wake_response.wait()
        return response


class _FakeRedisPipeline:
    def __init__(self, redis: _FakeRedis) -> None:
        self.redis = redis
        self.ops = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def xadd(self, name, fields, maxlen=None, approximate=True):
        self.ops.append(("xadd", name, fields, maxlen, approximate))
        return self

    def expire(self, name, seconds):
        self.ops.append(("expire", name, seconds))
        return self

    def xrange(self, name, min="-", max="+", count=None):
        self.ops.append(("xrange", name, min, max, count))
        return self

    def xrevrange(self, name, max="+", min="-", count=None):
        self.ops.append(("xrevrange", name, max, min, count))
        return self

    def xread(self, streams, count=None, block=None):
        self.ops.append(("xread", streams, count, block))
        return self

    async def execute(self):
        results = []
        for op in self.ops:
            if op[0] == "xadd":
                _, name, fields, maxlen, approximate = op
                results.append(await self.redis.xadd(name, fields, maxlen=maxlen, approximate=approximate))
            elif op[0] == "expire":
                _, name, seconds = op
                results.append(await self.redis.expire(name, seconds))
            elif op[0] == "xrange":
                _, name, min_id, max_id, count = op
                results.append(await self.redis.xrange(name, min=min_id, max=max_id, count=count))
            elif op[0] == "xrevrange":
                _, name, max_id, min_id, count = op
                results.append(await self.redis.xrevrange(name, max=max_id, min=min_id, count=count))
            elif op[0] == "xread":
                _, streams, count, block = op
                results.append(await self.redis.xread(streams, count=count, block=block))
        return results


class _FakeReadinessPubSub:
    def __init__(self, redis: "_FakeReadinessRedis") -> None:
        self.redis = redis
        self.channel: str | None = None
        self.messages: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        self.closed = False

    async def subscribe(self, channel: str) -> None:
        self.channel = channel
        self.redis.subscribers[channel] = self
        await self.messages.put({"type": "subscribe", "channel": channel, "data": 1})

    async def get_message(self, *, timeout: float = 0) -> dict[str, object] | None:
        try:
            return await asyncio.wait_for(self.messages.get(), timeout=timeout)
        except TimeoutError:
            return None

    async def unsubscribe(self, channel: str) -> None:
        self.redis.subscribers.pop(channel, None)

    async def aclose(self) -> None:
        self.closed = True


class _FakeReadinessRedis:
    def __init__(
        self,
        *,
        deny_publish: bool = False,
        deny_prefix: str | None = None,
    ) -> None:
        self.values: dict[str, str] = {}
        self.subscribers: dict[str, _FakeReadinessPubSub] = {}
        self.names: list[str] = []
        self.deny_publish = deny_publish
        self.deny_prefix = deny_prefix

    async def ping(self) -> bool:
        return True

    async def set(self, name: str, value: str, *, ex: int) -> bool:
        if self.deny_prefix and name.startswith(self.deny_prefix):
            raise RuntimeError("NOPERM")
        assert ex == 5
        self.names.append(name)
        self.values[name] = value
        return True

    async def get(self, name: str) -> str | None:
        self.names.append(name)
        return self.values.get(name)

    async def delete(self, name: str) -> int:
        self.names.append(name)
        return int(self.values.pop(name, None) is not None)

    def pubsub(self) -> _FakeReadinessPubSub:
        return _FakeReadinessPubSub(self)

    async def publish(self, channel: str, data: str) -> int:
        if self.deny_prefix and channel.startswith(self.deny_prefix):
            raise RuntimeError("NOPERM")
        self.names.append(channel)
        if self.deny_publish:
            return 0
        subscriber = self.subscribers.get(channel)
        if subscriber is None:
            return 0
        await subscriber.messages.put({"type": "message", "channel": channel, "data": data})
        return 1


# ---------------------------------------------------------------------------
# Unit tests for MemoryStreamBridge
# ---------------------------------------------------------------------------


@pytest.fixture
def bridge() -> MemoryStreamBridge:
    return MemoryStreamBridge(queue_maxsize=256)


@pytest.mark.anyio
async def test_publish_subscribe(bridge: MemoryStreamBridge):
    """Three events followed by end should be received in order."""
    run_id = "run-1"

    await bridge.publish(run_id, "metadata", {"run_id": run_id})
    await bridge.publish(run_id, "values", {"messages": []})
    await bridge.publish(run_id, "updates", {"step": 1})
    await bridge.publish_end(run_id)

    received = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert len(received) == 4
    assert received[0].event == "metadata"
    assert received[1].event == "values"
    assert received[2].event == "updates"
    assert received[3] is END_SENTINEL


@pytest.mark.anyio
async def test_heartbeat(bridge: MemoryStreamBridge):
    """When no events arrive within the heartbeat interval, yield a heartbeat."""
    run_id = "run-heartbeat"
    bridge._get_or_create_stream(run_id)  # ensure stream exists

    received = []

    async def consumer():
        async for entry in bridge.subscribe(run_id, heartbeat_interval=0.1):
            received.append(entry)
            if entry is HEARTBEAT_SENTINEL:
                break

    await asyncio.wait_for(consumer(), timeout=2.0)
    assert len(received) == 1
    assert received[0] is HEARTBEAT_SENTINEL


@pytest.mark.anyio
async def test_memory_bridge_uses_configured_default_heartbeat():
    """A subscriber may omit its override and inherit the bridge setting."""
    bridge = MemoryStreamBridge(queue_maxsize=256, heartbeat_interval=0.01)
    run_id = "run-configured-heartbeat"
    bridge._get_or_create_stream(run_id)

    entry = await asyncio.wait_for(anext(bridge.subscribe(run_id)), timeout=1.0)

    assert entry is HEARTBEAT_SENTINEL
    assert bridge.heartbeat_interval == 0.01


@pytest.mark.anyio
async def test_cleanup(bridge: MemoryStreamBridge):
    """After cleanup, the run's stream/event log is removed."""
    run_id = "run-cleanup"
    await bridge.publish(run_id, "test", {})
    assert run_id in bridge._streams

    await bridge.cleanup(run_id)
    assert run_id not in bridge._streams
    assert run_id not in bridge._counters


@pytest.mark.anyio
async def test_stream_exists_reports_cleanup(bridge: MemoryStreamBridge):
    """Callers can detect when the in-process event log has been cleaned up.

    Before cleanup a completed run's retained history still exists; after
    cleanup ``stream_exists`` reports False so a reconnecting subscriber does
    not hang waiting on a stream whose data is already gone.
    """
    run_id = "run-post-cleanup"
    await bridge.publish(run_id, "event-1", {"n": 1})
    await bridge.publish_end(run_id)

    assert await bridge.stream_exists(run_id) is True
    await bridge.cleanup(run_id)
    assert await bridge.stream_exists(run_id) is False


@pytest.mark.anyio
async def test_history_is_bounded():
    """Retained history should be bounded by queue_maxsize."""
    bridge = MemoryStreamBridge(queue_maxsize=1)
    run_id = "run-bp"

    await bridge.publish(run_id, "first", {})
    await bridge.publish(run_id, "second", {})
    await bridge.publish_end(run_id)

    received = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert len(received) == 2
    assert received[0].event == "second"
    assert received[1] is END_SENTINEL


@pytest.mark.anyio
async def test_multiple_runs(bridge: MemoryStreamBridge):
    """Two different run_ids should not interfere with each other."""
    await bridge.publish("run-a", "event-a", {"a": 1})
    await bridge.publish("run-b", "event-b", {"b": 2})
    await bridge.publish_end("run-a")
    await bridge.publish_end("run-b")

    events_a = []
    async for entry in bridge.subscribe("run-a", heartbeat_interval=1.0):
        events_a.append(entry)
        if entry is END_SENTINEL:
            break

    events_b = []
    async for entry in bridge.subscribe("run-b", heartbeat_interval=1.0):
        events_b.append(entry)
        if entry is END_SENTINEL:
            break

    assert len(events_a) == 2
    assert events_a[0].event == "event-a"
    assert events_a[0].data == {"a": 1}

    assert len(events_b) == 2
    assert events_b[0].event == "event-b"
    assert events_b[0].data == {"b": 2}


@pytest.mark.anyio
async def test_event_id_format(bridge: MemoryStreamBridge):
    """Event IDs should use timestamp-sequence format."""
    run_id = "run-id-format"
    await bridge.publish(run_id, "test", {"key": "value"})
    await bridge.publish_end(run_id)

    received = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    event = received[0]
    assert re.match(r"^\d+-\d+$", event.id), f"Expected timestamp-seq format, got {event.id}"


@pytest.mark.anyio
async def test_subscribe_replays_after_last_event_id(bridge: MemoryStreamBridge):
    """Reconnect should replay buffered events after the provided Last-Event-ID."""
    run_id = "run-replay"
    await bridge.publish(run_id, "metadata", {"run_id": run_id})
    await bridge.publish(run_id, "values", {"step": 1})
    await bridge.publish(run_id, "updates", {"step": 2})
    await bridge.publish_end(run_id)

    first_pass = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        first_pass.append(entry)
        if entry is END_SENTINEL:
            break

    received = []
    async for entry in bridge.subscribe(
        run_id,
        last_event_id=first_pass[0].id,
        heartbeat_interval=1.0,
    ):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in received[:-1]] == ["values", "updates"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_evicted_last_event_id_yields_gap_before_partial_replay():
    """A valid cursor older than retained history must not silently partial-replay."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-evicted-cursor"
    await bridge.publish(run_id, "e1", {"step": 1})
    await bridge.publish(run_id, "e2", {"step": 2})

    stream = bridge._streams[run_id]
    e1_id = stream.events[0].id
    await bridge.publish(run_id, "e3", {"step": 3})  # trims e1
    await bridge.publish_end(run_id)

    received = []
    async for entry in bridge.subscribe(
        run_id,
        last_event_id=e1_id,
        heartbeat_interval=1.0,
    ):
        received.append(entry)

    assert received == [
        StreamGap(
            requested_event_id=e1_id,
            earliest_available_event_id=stream.events[0].id,
            latest_available_event_id=stream.events[-1].id,
        )
    ]


@pytest.mark.anyio
async def test_slow_subscriber_yields_gap_after_buffer_trim():
    """A live subscriber that falls behind must not silently jump forward."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-slow-subscriber"
    await bridge.publish(run_id, "e1", {"step": 1})
    await bridge.publish(run_id, "e2", {"step": 2})

    subscriber = bridge.subscribe(run_id, heartbeat_interval=1.0)
    first = await anext(subscriber)
    assert first.event == "e1"

    await bridge.publish(run_id, "e3", {"step": 3})
    await bridge.publish(run_id, "e4", {"step": 4})

    stream = bridge._streams[run_id]
    assert await anext(subscriber) == StreamGap(
        requested_event_id=first.id,
        earliest_available_event_id=stream.events[0].id,
        latest_available_event_id=stream.events[-1].id,
    )
    with pytest.raises(StopAsyncIteration):
        await anext(subscriber)


# ---------------------------------------------------------------------------
# Stream termination tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_publish_end_terminates_even_when_history_is_full():
    """publish_end() should terminate subscribers without mutating retained history."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-end-history-full"

    await bridge.publish(run_id, "event-1", {"n": 1})
    await bridge.publish(run_id, "event-2", {"n": 2})
    stream = bridge._streams[run_id]
    assert [entry.event for entry in stream.events] == ["event-1", "event-2"]

    await bridge.publish_end(run_id)
    assert [entry.event for entry in stream.events] == ["event-1", "event-2"]

    events = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=0.1):
        events.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in events[:-1]] == ["event-1", "event-2"]
    assert events[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_publish_end_without_history_yields_end_immediately():
    """Subscribers should still receive END when a run completes without events."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-end-empty"
    await bridge.publish_end(run_id)

    events = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=0.1):
        events.append(entry)
        if entry is END_SENTINEL:
            break

    assert len(events) == 1
    assert events[0] is END_SENTINEL


@pytest.mark.anyio
async def test_publish_end_preserves_history_when_space_available():
    """When history has spare capacity, publish_end should preserve prior events."""
    bridge = MemoryStreamBridge(queue_maxsize=10)
    run_id = "run-no-evict"

    await bridge.publish(run_id, "event-1", {"n": 1})
    await bridge.publish(run_id, "event-2", {"n": 2})
    await bridge.publish_end(run_id)

    events = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=0.1):
        events.append(entry)
        if entry is END_SENTINEL:
            break

    # All events plus END should be present
    assert len(events) == 3
    assert events[0].event == "event-1"
    assert events[1].event == "event-2"
    assert events[2] is END_SENTINEL


@pytest.mark.anyio
async def test_concurrent_slow_consumers_receive_gap():
    """Concurrent consumers must all receive a gap when producers outrun retention.

    Each producer fills a four-entry bridge without yielding, so subscribers
    that started at offset zero cannot observe the first six events.
    """
    bridge = MemoryStreamBridge(queue_maxsize=4)
    num_runs = 4

    async def producer(run_id: str):
        for i in range(10):  # More events than queue capacity
            await bridge.publish(run_id, f"event-{i}", {"i": i})
        await bridge.publish_end(run_id)

    async def consumer(run_id: str) -> list:
        events = []
        async for entry in bridge.subscribe(run_id, heartbeat_interval=0.1):
            events.append(entry)
            if entry is END_SENTINEL:
                return events
        return events  # pragma: no cover

    run_ids = [f"concurrent-{i}" for i in range(num_runs)]
    results: dict[str, list] = {}

    async def consume_into(run_id: str) -> None:
        results[run_id] = await consumer(run_id)

    with anyio.fail_after(10):
        async with anyio.create_task_group() as task_group:
            for run_id in run_ids:
                task_group.start_soon(consume_into, run_id)
            await anyio.sleep(0)
            for run_id in run_ids:
                task_group.start_soon(producer, run_id)

    for run_id in run_ids:
        events = results[run_id]
        assert len(events) == 1
        assert isinstance(events[0], StreamGap), f"Run {run_id} did not receive a gap"
        assert events[0].requested_event_id is None


# ---------------------------------------------------------------------------
# Unit tests for RedisStreamBridge
# ---------------------------------------------------------------------------


@pytest.fixture
def redis_bridge() -> RedisStreamBridge:
    return RedisStreamBridge(redis_url="redis://fake", queue_maxsize=2, client=_FakeRedis())


@pytest.mark.parametrize(
    ("namespace_prefix", "expected_key"),
    [
        ("tA", "tA:deerflow:stream_bridge:inventory-run"),
        ("", "deerflow:stream_bridge:inventory-run"),
    ],
    ids=["tenant-prefix", "legacy-unprefixed"],
)
@pytest.mark.anyio
async def test_redis_bridge_routes_every_emitted_name_through_configurable_prefix(
    namespace_prefix: str,
    expected_key: str,
) -> None:
    fake = _RecordingRedis()
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        namespace_prefix=namespace_prefix,
        client=fake,
    )

    await bridge.publish("inventory-run", "metadata", {"run_id": "inventory-run"})
    await bridge.publish_end("inventory-run")
    assert await bridge.stream_exists("inventory-run") is True
    received = [entry async for entry in bridge.subscribe("inventory-run")]
    assert received[0].event == "metadata"
    assert received[1] is END_SENTINEL
    await bridge.cleanup("inventory-run")

    assert {command for command, _name in fake.redis_calls} == {
        "delete",
        "exists",
        "expire",
        "xadd",
        "xrange",
        "xread",
        "xrevrange",
    }
    emitted_names = [name for _command, name in fake.redis_calls]
    assert set(emitted_names) == {expected_key}
    assert all(re.fullmatch(r"tA:.*", name) for name in emitted_names) is bool(
        namespace_prefix,
    )


@pytest.mark.anyio
async def test_redis_topology_readiness_proves_tenant_key_and_channel_acl() -> None:
    fake = _FakeReadinessRedis()
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        namespace_prefix="hm:v1:tenant-abcd:redis:stream",
        client=fake,
    )

    assert await bridge.topology_readiness_probe(
        replica_id="gateway-0",
        timeout_seconds=1,
        additional_key_prefixes=(
            "hm:v1:tenant-abcd:redis:ckpt-hist:v1",
            "hm:v1:tenant-abcd:redis:deerflow:sandbox:owner",
        ),
    )

    assert fake.values == {}
    assert fake.subscribers == {}
    assert fake.names
    assert all(name.startswith("hm:v1:tenant-abcd:redis:") for name in fake.names)


@pytest.mark.anyio
async def test_redis_topology_readiness_fails_closed_when_channel_acl_denies_publish() -> None:
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        namespace_prefix="hm:v1:tenant-abcd:redis:stream",
        client=_FakeReadinessRedis(deny_publish=True),
    )

    with pytest.raises(RuntimeError, match="redis_topology_channel_probe_failed"):
        await bridge.topology_readiness_probe(
            replica_id="gateway-0",
            timeout_seconds=1,
        )


@pytest.mark.anyio
async def test_redis_topology_readiness_requires_cross_tenant_acl_denial() -> None:
    foreign = "hm:v1:tenant-foreign:redis"
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        namespace_prefix="hm:v1:tenant-abcd:redis:stream",
        client=_FakeReadinessRedis(),
    )

    with pytest.raises(RuntimeError, match="redis_topology_acl_isolation_failed"):
        await bridge.topology_readiness_probe(
            replica_id="gateway-0",
            timeout_seconds=1,
            forbidden_key_prefix=foreign,
        )

    isolated = RedisStreamBridge(
        redis_url="redis://fake",
        namespace_prefix="hm:v1:tenant-abcd:redis:stream",
        client=_FakeReadinessRedis(deny_prefix=foreign),
    )
    assert await isolated.topology_readiness_probe(
        replica_id="gateway-0",
        timeout_seconds=1,
        forbidden_key_prefix=foreign,
    )


@pytest.mark.anyio
async def test_explicit_storage_prefix_keeps_constructor_compatibility() -> None:
    fake = _FakeRedis()
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        key_prefix="custom",
        client=fake,
    )

    await bridge.publish("run", "metadata", {})

    assert set(fake.streams) == {"custom:run"}


@pytest.mark.anyio
async def test_redis_publish_subscribe(redis_bridge: RedisStreamBridge):
    """Redis bridge should deliver events in order and terminate on end."""
    run_id = "redis-run-1"

    await redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    await redis_bridge.publish(run_id, "values", {"messages": []})
    await redis_bridge.publish_end(run_id)

    received = []
    async for entry in redis_bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in received[:-1]] == ["metadata", "values"]
    assert received[0].data == {"run_id": run_id}
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_replays_after_last_event_id(redis_bridge: RedisStreamBridge):
    """Redis XREAD should resume after Last-Event-ID."""
    run_id = "redis-run-replay"

    await redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    await redis_bridge.publish(run_id, "values", {"step": 1})
    await redis_bridge.publish_end(run_id)

    first_pass = []
    async for entry in redis_bridge.subscribe(run_id, heartbeat_interval=1.0):
        first_pass.append(entry)
        if entry is END_SENTINEL:
            break

    received = []
    async for entry in redis_bridge.subscribe(run_id, last_event_id=first_pass[0].id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in received[:-1]] == ["values"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_evicted_last_event_id_yields_gap_before_partial_replay(
    redis_bridge: RedisStreamBridge,
):
    """Redis must distinguish a trimmed cursor from a complete replay."""
    run_id = "redis-run-evicted-cursor"
    await redis_bridge.publish(run_id, "e1", {"step": 1})
    key = redis_bridge._key(run_id)
    e1_id = redis_bridge._redis.streams[key][0][0]
    await redis_bridge.publish(run_id, "e2", {"step": 2})
    await redis_bridge.publish(run_id, "e3", {"step": 3})

    retained_ids = [event_id for event_id, _fields in redis_bridge._redis.streams[key]]
    received = [
        entry
        async for entry in redis_bridge.subscribe(
            run_id,
            last_event_id=e1_id,
            heartbeat_interval=1.0,
        )
    ]

    assert received == [
        StreamGap(
            requested_event_id=e1_id,
            earliest_available_event_id=retained_ids[0],
            latest_available_event_id=retained_ids[-1],
        )
    ]


@pytest.mark.anyio
async def test_redis_slow_subscriber_yields_gap_after_buffer_trim(
    redis_bridge: RedisStreamBridge,
):
    """A live Redis subscriber must detect trimming after its last batch."""
    run_id = "redis-run-slow-subscriber"
    await redis_bridge.publish(run_id, "e1", {"step": 1})
    subscriber = redis_bridge.subscribe(run_id, heartbeat_interval=1.0)
    first = await anext(subscriber)
    assert first.event == "e1"

    await redis_bridge.publish(run_id, "e2", {"step": 2})
    await redis_bridge.publish(run_id, "e3", {"step": 3})
    await redis_bridge.publish(run_id, "e4", {"step": 4})
    key = redis_bridge._key(run_id)
    retained_ids = [event_id for event_id, _fields in redis_bridge._redis.streams[key]]

    assert await anext(subscriber) == StreamGap(
        requested_event_id=first.id,
        earliest_available_event_id=retained_ids[0],
        latest_available_event_id=retained_ids[-1],
    )
    with pytest.raises(StopAsyncIteration):
        await anext(subscriber)


@pytest.mark.anyio
async def test_redis_initial_subscriber_yields_gap_when_first_wake_falls_behind():
    """An established no-cursor wait must not silently replay a trimmed tail."""
    fake = _FakeRedis()
    delayed = _DelayedBlockingReadRedis(fake)
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        queue_maxsize=2,
        client=delayed,
    )
    run_id = "redis-run-initial-subscriber-gap"
    subscriber = bridge.subscribe(run_id, heartbeat_interval=1.0)
    first_item = asyncio.create_task(anext(subscriber))

    with anyio.fail_after(2):
        await delayed.blocking_read_started.wait()
        await bridge.publish(run_id, "e1", {"step": 1})
        await delayed.wake_response_captured.wait()
        await bridge.publish(run_id, "e2", {"step": 2})
        await bridge.publish(run_id, "e3", {"step": 3})
        await bridge.publish(run_id, "e4", {"step": 4})
        delayed.release_wake_response.set()
        gap = await first_item

    key = bridge._key(run_id)
    retained_ids = [event_id for event_id, _fields in fake.streams[key]]
    assert gap == StreamGap(
        requested_event_id=None,
        earliest_available_event_id=retained_ids[0],
        latest_available_event_id=retained_ids[-1],
    )
    with pytest.raises(StopAsyncIteration):
        await anext(subscriber)


@pytest.mark.anyio
async def test_redis_recovery_cursor_at_end_yields_end_immediately(
    redis_bridge: RedisStreamBridge,
):
    """The latest gap cursor may be the internal end marker."""
    run_id = "redis-run-end-cursor"
    await redis_bridge.publish(run_id, "event", {})
    await redis_bridge.publish_end(run_id)
    key = redis_bridge._key(run_id)
    end_id = redis_bridge._redis.streams[key][-1][0]

    received = [
        entry
        async for entry in redis_bridge.subscribe(
            run_id,
            last_event_id=end_id,
            heartbeat_interval=0.01,
        )
    ]

    assert received == [END_SENTINEL]


@pytest.mark.anyio
async def test_redis_invalid_last_event_id_tails_live_events(redis_bridge: RedisStreamBridge):
    """Malformed reconnect ids should not replay retained Redis events."""
    run_id = "redis-run-invalid-last-event-id"

    await redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    received = []

    async def publish_later() -> None:
        await anyio.sleep(0.05)
        await redis_bridge.publish(run_id, "values", {"step": 1})
        await redis_bridge.publish_end(run_id)

    with anyio.fail_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_later)
            async for entry in redis_bridge.subscribe(run_id, last_event_id="-1", heartbeat_interval=0.01):
                if entry is HEARTBEAT_SENTINEL:
                    continue
                received.append(entry)
                if entry is END_SENTINEL:
                    break

    assert [entry.event for entry in received[:-1]] == ["values"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_invalid_last_event_id_tails_empty_stream(redis_bridge: RedisStreamBridge):
    """Malformed reconnect ids should still wait for the first Redis event."""
    run_id = "redis-run-invalid-empty"
    received = []

    async def publish_later() -> None:
        await anyio.sleep(0.05)
        await redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
        await redis_bridge.publish_end(run_id)

    with anyio.fail_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_later)
            async for entry in redis_bridge.subscribe(run_id, last_event_id="-1", heartbeat_interval=0.01):
                if entry is HEARTBEAT_SENTINEL:
                    continue
                received.append(entry)
                if entry is END_SENTINEL:
                    break

    assert [entry.event for entry in received[:-1]] == ["metadata"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_invalid_last_event_id_on_terminal_run_replays_end(redis_bridge: RedisStreamBridge):
    """Malformed reconnect ids on terminal streams should drain END instead of hanging."""
    run_id = "redis-run-invalid-terminal"

    await redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    await redis_bridge.publish_end(run_id)

    received = []
    async for entry in redis_bridge.subscribe(run_id, last_event_id="-1", heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in received[:-1]] == ["metadata"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_heartbeat(redis_bridge: RedisStreamBridge):
    """Redis bridge should yield heartbeats when XREAD times out on an existing stream."""
    run_id = "redis-run-heartbeat"
    await redis_bridge.publish(run_id, "init", {})

    received = []
    async for entry in redis_bridge.subscribe(run_id, heartbeat_interval=0.01):
        received.append(entry)
        if entry is HEARTBEAT_SENTINEL:
            break

    assert len(received) == 2
    assert received[0].event == "init"
    assert received[1] is HEARTBEAT_SENTINEL


@pytest.mark.anyio
async def test_redis_publish_end_preserves_data_history_capacity(redis_bridge: RedisStreamBridge):
    """The internal end marker should not evict the configured data history."""
    run_id = "redis-run-end-capacity"

    await redis_bridge.publish(run_id, "event-1", {"n": 1})
    await redis_bridge.publish(run_id, "event-2", {"n": 2})
    await redis_bridge.publish_end(run_id)

    received = []
    async for entry in redis_bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in received[:-1]] == ["event-1", "event-2"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_cleanup_deletes_stream(redis_bridge: RedisStreamBridge):
    fake = redis_bridge._redis
    run_id = "redis-run-cleanup"

    await redis_bridge.publish(run_id, "event", {})
    await redis_bridge.cleanup(run_id)

    assert fake.deleted == ["deerflow:stream_bridge:redis-run-cleanup"]


@pytest.mark.anyio
async def test_redis_publish_refreshes_stream_ttl():
    """Redis stream TTL should be rolling on publish and publish_end."""
    fake = _FakeRedis()
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        queue_maxsize=2,
        stream_ttl_seconds=42,
        client=fake,
    )
    run_id = "redis-run-ttl"
    key = "deerflow:stream_bridge:redis-run-ttl"

    await bridge.publish(run_id, "event-1", {"n": 1})
    await bridge.publish(run_id, "event-2", {"n": 2})
    await bridge.publish_end(run_id)

    assert fake.expirations == [(key, 42), (key, 42), (key, 42)]


@pytest.mark.anyio
async def test_redis_stream_ttl_can_be_disabled():
    """A zero TTL disables the Redis leak safety net for installations that need it."""
    fake = _FakeRedis()
    bridge = RedisStreamBridge(
        redis_url="redis://fake",
        queue_maxsize=2,
        stream_ttl_seconds=0,
        client=fake,
    )

    await bridge.publish("redis-run-no-ttl", "event", {})
    await bridge.publish_end("redis-run-no-ttl")

    assert fake.expirations == []


@pytest.mark.anyio
async def test_redis_subscribe_waits_for_first_publish(redis_bridge: RedisStreamBridge):
    """A subscriber that starts before the first XADD must not receive END."""
    run_id = "redis-run-first-publish"
    received = []

    async def publish_later() -> None:
        await anyio.sleep(0.05)
        await redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
        await redis_bridge.publish_end(run_id)

    with anyio.fail_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_later)
            async for entry in redis_bridge.subscribe(run_id, heartbeat_interval=0.01):
                if entry is HEARTBEAT_SENTINEL:
                    continue
                received.append(entry)
                if entry is END_SENTINEL:
                    break

    assert [entry.event for entry in received[:-1]] == ["metadata"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_stream_exists_reports_cleanup(redis_bridge: RedisStreamBridge):
    """Callers can detect when retained Redis stream data has been cleaned up."""
    run_id = "redis-run-post-cleanup"
    await redis_bridge.publish(run_id, "event-1", {"n": 1})
    await redis_bridge.publish_end(run_id)

    assert await redis_bridge.stream_exists(run_id) is True
    await redis_bridge.cleanup(run_id)
    assert await redis_bridge.stream_exists(run_id) is False


@pytest.mark.anyio
async def test_redis_transient_error_retries():
    """Transient RedisError during XREAD should be retried, not propagated."""
    from redis.exceptions import RedisError

    fake = _FakeRedis()
    call_count = 0
    original_xread = fake.xread

    async def flaky_xread(streams, count=None, block=None):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RedisError("Transient connection error")
        return await original_xread(streams, count=count, block=block)

    fake.xread = flaky_xread
    bridge = RedisStreamBridge(redis_url="redis://fake", queue_maxsize=2, client=fake)

    run_id = "redis-run-retry"
    await bridge.publish(run_id, "event-1", {"n": 1})
    await bridge.publish_end(run_id)

    received = []
    with anyio.fail_after(5):
        async for entry in bridge.subscribe(run_id, heartbeat_interval=0.01):
            received.append(entry)
            if entry is END_SENTINEL:
                break

    assert call_count > 2
    assert [e.event for e in received[:-1]] == ["event-1"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_redis_transient_error_gives_up_after_max_retries():
    """After exceeding max consecutive errors, RedisError should propagate."""
    from redis.exceptions import RedisError

    fake = _FakeRedis()

    async def always_fail_xread(streams, count=None, block=None):
        raise RedisError("Persistent connection error")

    fake.xread = always_fail_xread
    bridge = RedisStreamBridge(redis_url="redis://fake", queue_maxsize=2, client=fake)

    with pytest.raises(RedisError, match="Persistent connection error"):
        async for _ in bridge.subscribe("redis-run-fail", heartbeat_interval=0.01):
            pass


@pytest.mark.anyio
async def test_redis_blocking_wakeup_error_gives_up_after_max_retries():
    """Successful snapshots must not hide a permanently failing blocking XREAD."""
    from redis.exceptions import RedisError

    fake = _FakeRedis()
    original_xread = fake.xread

    async def fail_blocking_xread(streams, count=None, block=None):
        if block is not None:
            raise RedisError("Persistent blocking connection error")
        return await original_xread(streams, count=count, block=block)

    fake.xread = fail_blocking_xread
    bridge = RedisStreamBridge(redis_url="redis://fake", queue_maxsize=2, client=fake)

    with pytest.raises(RedisError, match="Persistent blocking connection error"):
        async for _ in bridge.subscribe("redis-run-blocking-fail", heartbeat_interval=0.01):
            pass


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------


def test_stream_bridge_key_prefix_defaults_to_legacy_names() -> None:
    config = StreamBridgeConfig()

    assert config.key_prefix == ""


@pytest.mark.parametrize(
    "heartbeat_interval",
    [
        True,
        False,
        0,
        -1,
        float("inf"),
        float("-inf"),
        float("nan"),
        MAX_HEARTBEAT_INTERVAL_SECONDS + 1,
    ],
)
def test_stream_bridge_config_rejects_invalid_heartbeat_interval(heartbeat_interval):
    with pytest.raises(ValidationError):
        StreamBridgeConfig(heartbeat_interval_seconds=heartbeat_interval)


def test_stream_bridge_config_accepts_numeric_heartbeat_string():
    config = StreamBridgeConfig(heartbeat_interval_seconds="2.5")

    assert config.heartbeat_interval_seconds == 2.5


@pytest.mark.parametrize("heartbeat_interval", [True, MAX_HEARTBEAT_INTERVAL_SECONDS + 1])
def test_memory_bridge_rejects_invalid_default_heartbeat(heartbeat_interval):
    with pytest.raises(ValueError, match="heartbeat_interval"):
        MemoryStreamBridge(heartbeat_interval=heartbeat_interval)


@pytest.mark.anyio
async def test_redis_bridge_rejects_oversized_subscription_heartbeat_before_io():
    fake = _FakeRedis()

    async def fail_if_called(*_args, **_kwargs):
        pytest.fail("Redis I/O must not start for an invalid heartbeat interval")

    fake.xrange = fail_if_called
    bridge = RedisStreamBridge(redis_url="redis://fake", client=fake)

    with pytest.raises(ValueError, match="heartbeat_interval"):
        await anext(
            bridge.subscribe(
                "redis-run-oversized-heartbeat",
                heartbeat_interval=MAX_HEARTBEAT_INTERVAL_SECONDS + 1,
            )
        )


@pytest.mark.anyio
async def test_make_stream_bridge_defaults():
    """make_stream_bridge() with no config yields a MemoryStreamBridge."""
    async with make_stream_bridge() as bridge:
        assert isinstance(bridge, MemoryStreamBridge)
        assert bridge.heartbeat_interval == 15.0


@pytest.mark.anyio
async def test_make_stream_bridge_passes_memory_heartbeat():
    set_stream_bridge_config(
        StreamBridgeConfig(
            type="memory",
            heartbeat_interval_seconds=2.5,
        )
    )
    try:
        async with make_stream_bridge() as bridge:
            assert isinstance(bridge, MemoryStreamBridge)
            assert bridge.heartbeat_interval == 2.5
    finally:
        set_stream_bridge_config(None)


# ---------------------------------------------------------------------------
# _resolve_start_offset: O(1) seq-indexed resolution
# ---------------------------------------------------------------------------


def _linear_resolve(stream, last_event_id):
    """The original linear-scan resolver, kept as a parity reference."""
    if last_event_id is None:
        return stream.start_offset
    for index, entry in enumerate(stream.events):
        if entry.id == last_event_id:
            return stream.start_offset + index + 1
    return stream.start_offset


@pytest.mark.parametrize(
    "event_id,expected",
    [
        ("1718000000000-0", 0),
        ("1718000000000-42", 42),
        ("-1", None),  # malformed live-tail sentinel, not a valid event id
        ("garbage", None),  # no separator
        ("1718000000000-x", None),  # non-integer seq
        ("", None),
    ],
)
def test_parse_event_seq(event_id, expected):
    assert MemoryStreamBridge._parse_event_seq(event_id) == expected


@pytest.mark.anyio
async def test_resolve_start_offset_matches_linear_scan():
    """Retained and unknown cursors preserve the previous linear-scan behavior."""
    bridge = MemoryStreamBridge(queue_maxsize=4)
    run_id = "run-parity"
    ids = []
    for i in range(10):
        await bridge.publish(run_id, f"e{i}", {"i": i})
        ids.append(bridge._streams[run_id].events[-1].id)  # includes ids that later evict
    stream = bridge._streams[run_id]
    assert stream.start_offset == 6  # 10 published, buffer of 4 retains seq 6..9

    # A foreign id: a retained event's seq but a different timestamp -> must NOT match.
    ts, _, seq_text = stream.events[0].id.rpartition("-")
    foreign_id = f"{int(ts) + 1}-{seq_text}"

    candidates = [None, "garbage", "1718000000000-x", "999999-999999", foreign_id, *ids[6:]]
    for eid in candidates:
        assert bridge._resolve_start_offset(stream, eid) == _linear_resolve(stream, eid), eid

    for evicted_id in ids[:6]:
        assert bridge._resolve_start_offset(stream, evicted_id) == StreamGap(
            requested_event_id=evicted_id,
            earliest_available_event_id=stream.events[0].id,
            latest_available_event_id=stream.events[-1].id,
        )


@pytest.mark.anyio
async def test_memory_low_numeric_foreign_cursor_conservatively_yields_gap():
    """An unverifiable numeric cursor below the watermark takes the safe path."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-low-foreign-cursor"
    for index in range(3):
        await bridge.publish(run_id, f"e{index}", {"index": index})

    stream = bridge._streams[run_id]
    timestamp, _, _sequence = stream.events[0].id.rpartition("-")
    foreign_evicted_id = f"{int(timestamp) + 1}-0"

    assert bridge._resolve_start_offset(stream, foreign_evicted_id) == StreamGap(
        requested_event_id=foreign_evicted_id,
        earliest_available_event_id=stream.events[0].id,
        latest_available_event_id=stream.events[-1].id,
    )


@pytest.mark.anyio
async def test_subscribe_with_unknown_last_event_id_replays_from_earliest():
    """A foreign/garbage Last-Event-ID falls back to replaying retained events."""
    bridge = MemoryStreamBridge(queue_maxsize=10)
    run_id = "run-unknown-id"
    await bridge.publish(run_id, "first", {})
    await bridge.publish(run_id, "second", {})
    await bridge.publish_end(run_id)

    received = []
    async for entry in bridge.subscribe(run_id, last_event_id="not-a-real-id", heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [entry.event for entry in received[:-1]] == ["first", "second"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_memory_malformed_last_event_id_is_not_reported_as_gap():
    """Malformed cursor policy stays separate from valid evicted cursors."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-malformed-id"
    await bridge.publish(run_id, "first", {})
    await bridge.publish(run_id, "second", {})
    await bridge.publish_end(run_id)

    received = [
        entry
        async for entry in bridge.subscribe(
            run_id,
            last_event_id="-1",
            heartbeat_interval=1.0,
        )
    ]

    assert [entry.event for entry in received[:-1]] == ["first", "second"]
    assert received[-1] is END_SENTINEL
    assert all(not isinstance(entry, StreamGap) for entry in received)


@pytest.mark.anyio
async def test_memory_make_gap_handles_empty_events_buffer():
    """_make_gap returns StreamGap with None bounds when events deque is empty."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-empty-stream-gap"
    stream = bridge._get_or_create_stream(run_id)
    assert len(stream.events) == 0

    gap = bridge._make_gap(stream, "100-0")
    assert gap == StreamGap(
        requested_event_id="100-0",
        earliest_available_event_id=None,
        latest_available_event_id=None,
    )


def test_memory_stream_bridge_clamps_queue_maxsize():
    """queue_maxsize <= 0 is clamped to at least 1."""
    bridge = MemoryStreamBridge(queue_maxsize=0)
    assert bridge._maxsize == 1
    bridge_neg = MemoryStreamBridge(queue_maxsize=-5)
    assert bridge_neg._maxsize == 1


def test_stream_bridge_config_validates_queue_maxsize():
    """StreamBridgeConfig rejects queue_maxsize < 1."""
    with pytest.raises(ValidationError):
        StreamBridgeConfig(queue_maxsize=0)
    with pytest.raises(ValidationError):
        StreamBridgeConfig(queue_maxsize=-1)


@pytest.mark.anyio
async def test_memory_subscribe_replays_gap_when_buffer_empty_and_cursor_behind_watermark():
    """Subscribing with a cursor behind start_offset when buffer is empty yields StreamGap with None bounds."""
    bridge = MemoryStreamBridge(queue_maxsize=2)
    run_id = "run-empty-retained-gap"
    stream = bridge._get_or_create_stream(run_id)
    # Simulate a stream whose retained buffer was cleared / advanced
    stream.start_offset = 10
    stream.events.clear()

    received = [
        entry
        async for entry in bridge.subscribe(
            run_id,
            last_event_id="5-0",
            heartbeat_interval=0.1,
        )
    ]
    assert len(received) == 1
    assert received[0] == StreamGap(
        requested_event_id="5-0",
        earliest_available_event_id=None,
        latest_available_event_id=None,
    )


@pytest.mark.anyio
async def test_make_stream_bridge_uses_docker_redis_env(monkeypatch):
    """Docker can enable Redis bridge without editing config.yaml."""
    set_stream_bridge_config(None)
    monkeypatch.setenv("DEER_FLOW_STREAM_BRIDGE_REDIS_URL", "redis://redis:6379/0")
    try:
        async with make_stream_bridge() as bridge:
            assert isinstance(bridge, RedisStreamBridge)
            assert bridge._redis_url == "redis://redis:6379/0"
    finally:
        set_stream_bridge_config(None)


@pytest.mark.anyio
async def test_make_stream_bridge_passes_redis_options(monkeypatch):
    """Redis options from config should be forwarded to Redis bridge setup."""
    import deerflow.runtime.stream_bridge.redis as redis_module

    captured: dict = {}
    fake = _FakeRedis()

    def fake_from_url(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(redis_module.Redis, "from_url", staticmethod(fake_from_url))
    set_stream_bridge_config(
        StreamBridgeConfig(
            type="redis",
            redis_url="redis://fake:6379/0",
            key_prefix="configured",
            heartbeat_interval_seconds=2.5,
            max_connections=50,
            stream_ttl_seconds=42,
        )
    )
    try:
        async with make_stream_bridge() as bridge:
            assert isinstance(bridge, RedisStreamBridge)
            assert bridge.heartbeat_interval == 2.5
            assert bridge._stream_ttl_seconds == 42
            await bridge.publish("factory-run", "metadata", {})
        assert set(fake.streams) == {
            "configured:deerflow:stream_bridge:factory-run",
        }
        assert captured["max_connections"] == 50
        assert captured["decode_responses"] is True
    finally:
        set_stream_bridge_config(None)


@pytest.mark.anyio
async def test_make_stream_bridge_key_prefix_env_overrides_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deerflow.runtime.stream_bridge.redis as redis_module

    fake = _FakeRedis()
    monkeypatch.setattr(
        redis_module.Redis,
        "from_url",
        staticmethod(lambda _url, **_kwargs: fake),
    )
    monkeypatch.setenv("DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX", "from-env")
    set_stream_bridge_config(
        StreamBridgeConfig(
            type="redis",
            redis_url="redis://fake:6379/0",
            key_prefix="from-config",
        )
    )
    try:
        async with make_stream_bridge() as bridge:
            await bridge.publish("env-run", "metadata", {})
    finally:
        set_stream_bridge_config(None)

    assert set(fake.streams) == {
        "from-env:deerflow:stream_bridge:env-run",
    }


@pytest.mark.anyio
async def test_make_stream_bridge_uses_server_tenant_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deerflow.runtime.stream_bridge.redis as redis_module

    fake = _FakeRedis()
    monkeypatch.setattr(
        redis_module.Redis,
        "from_url",
        staticmethod(lambda _url, **_kwargs: fake),
    )
    identity = TenantIdentityV1.resolve(
        deployment_config=DeploymentConfig(tenant_id="tenant-a"),
        environ=MappingProxyType({}),
    )
    set_stream_bridge_config(StreamBridgeConfig(type="redis", redis_url="redis://fake:6379/0"))
    try:
        async with make_stream_bridge(
            tenant_namespace=identity.namespace(TenantSubsystem.REDIS),
        ) as bridge:
            await bridge.publish("same-run", "metadata", {})
    finally:
        set_stream_bridge_config(None)

    assert set(fake.streams) == {f"{identity.namespace(TenantSubsystem.REDIS).key_prefix.rstrip(':')}:deerflow:stream_bridge:same-run"}


# ---------------------------------------------------------------------------
# Integration tests against a real Redis server
# ---------------------------------------------------------------------------
#
# Opt-in and self-skipping: when no Redis is reachable these are skipped so
# `make test` stays green without Redis. Point at a server with
# DEER_FLOW_TEST_REDIS_URL (defaults to redis://localhost:6379/15 — DB 15 to
# avoid clobbering real data) and select with `pytest -m integration`. They
# cover what _FakeRedis only approximates: real XADD/XREAD semantics, live-tail
# reconnects for malformed Last-Event-ID values, the server <ms>-<seq> ID
# format, and MAXLEN trimming.

REDIS_TEST_URL = os.environ.get("DEER_FLOW_TEST_REDIS_URL", "redis://localhost:6379/15")


def _redis_available() -> bool:
    try:
        import redis  # sync client, used only for the connectivity probe
    except ImportError:
        return False
    try:
        client = redis.Redis.from_url(REDIS_TEST_URL, socket_connect_timeout=0.5)
        try:
            client.ping()
        finally:
            client.close()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(not _redis_available(), reason=f"Redis not reachable at {REDIS_TEST_URL}")


@pytest.fixture
async def real_redis_bridge():
    from redis.asyncio import Redis

    client = Redis.from_url(REDIS_TEST_URL, decode_responses=True)
    key_prefix = f"deerflow:test:{uuid.uuid4().hex}"
    bridge = RedisStreamBridge(redis_url=REDIS_TEST_URL, queue_maxsize=2, key_prefix=key_prefix, client=client)
    try:
        yield bridge
    finally:
        async for key in client.scan_iter(f"{key_prefix}:*"):
            await client.delete(key)
        await client.aclose()


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_publish_subscribe_and_id_format(real_redis_bridge):
    run_id = "integ-basic"
    await real_redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    await real_redis_bridge.publish(run_id, "values", {"step": 1})
    await real_redis_bridge.publish_end(run_id)

    received = []
    async for entry in real_redis_bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [e.event for e in received[:-1]] == ["metadata", "values"]
    assert received[0].data == {"run_id": run_id}
    assert received[-1] is END_SENTINEL
    # Real Redis stream IDs use the <ms>-<seq> format the fake only approximates.
    assert re.match(r"^\d+-\d+$", received[0].id), received[0].id


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_acl_confines_bridge_and_denies_cross_tenant_commands():
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    from redis import Redis as SyncRedis
    from redis.asyncio import Redis
    from redis.exceptions import NoPermissionError

    from deerflow.community.aio_sandbox.ownership.base import RenewOutcome
    from deerflow.community.aio_sandbox.ownership.redis import RedisOwnershipStore
    from deerflow.community.e2b_sandbox.capacity.redis import (
        RedisE2BCapacityStore,
        ReserveStatus,
    )
    from deerflow.runtime.checkpoint_cache.base import make_history_key
    from deerflow.runtime.checkpoint_cache.redis import RedisCheckpointHistoryCache

    tenant_a_namespace = TenantIdentityV1.from_canonical_id("tenant-a").namespace(TenantSubsystem.REDIS)
    tenant_b_namespace = TenantIdentityV1.from_canonical_id("tenant-b").namespace(TenantSubsystem.REDIS)
    components = (
        RedisTenantComponent.STREAM_BRIDGE,
        RedisTenantComponent.CHECKPOINT_CACHE,
        RedisTenantComponent.SANDBOX_OWNERSHIP,
    )
    tenant_a_prefixes = {component: redis_component_key_prefix(tenant_a_namespace, component) for component in components}
    tenant_b_prefixes = {component: redis_component_key_prefix(tenant_b_namespace, component) for component in components}
    tenant_a_prefix = tenant_a_prefixes[RedisTenantComponent.STREAM_BRIDGE]
    run_id = f"acl-{uuid.uuid4().hex}"
    username = f"deerflow-test-{uuid.uuid4().hex}"
    password = uuid.uuid4().hex
    admin = Redis.from_url(REDIS_TEST_URL, decode_responses=True)
    tenant = None
    checkpoint_tenant = None
    sync_tenant = None

    try:
        await admin.execute_command(
            "ACL",
            "SETUSER",
            username,
            "reset",
            "on",
            f">{password}",
            f"~{tenant_a_prefix}*",
            f"&{tenant_a_prefix}*",
            "+@read",
            "+@write",
            "+@transaction",
            "+@scripting",
            "+time",
            "+publish",
            "+select",
        )
        tenant = Redis.from_url(
            REDIS_TEST_URL,
            username=username,
            password=password,
            decode_responses=True,
        )
        bridge = RedisStreamBridge(
            redis_url=REDIS_TEST_URL,
            namespace_prefix=tenant_a_prefix,
            client=tenant,
        )

        await bridge.publish(run_id, "metadata", {"run_id": run_id})
        await bridge.publish_end(run_id)
        received = [entry async for entry in bridge.subscribe(run_id)]

        assert received[0].event == "metadata"
        assert received[1] is END_SENTINEL
        assert await tenant.publish(f"{tenant_a_prefix}:events", "ok") == 0

        checkpoint_tenant = Redis.from_url(
            REDIS_TEST_URL,
            username=username,
            password=password,
            decode_responses=False,
        )
        checkpoint_prefix = tenant_a_prefixes[RedisTenantComponent.CHECKPOINT_CACHE]
        checkpoint_key = make_history_key(
            checkpoint_prefix,
            "thread-acl",
            "",
            "checkpoint-acl",
            "messages",
        )
        checkpoint_cache = RedisCheckpointHistoryCache(
            REDIS_TEST_URL,
            serde=JsonPlusSerializer(),
            ttl_seconds=60,
            client=checkpoint_tenant,
        )
        await checkpoint_cache.aset_many({checkpoint_key: {"writes": []}})
        assert checkpoint_key in await checkpoint_cache.aget_many([checkpoint_key])
        await checkpoint_cache.adelete_thread(checkpoint_prefix, "thread-acl")
        assert await checkpoint_tenant.exists(checkpoint_key) == 0

        sync_tenant = SyncRedis.from_url(
            REDIS_TEST_URL,
            username=username,
            password=password,
            decode_responses=True,
        )
        ownership_prefix = tenant_a_prefixes[RedisTenantComponent.SANDBOX_OWNERSHIP]
        ownership = RedisOwnershipStore(
            owner_id="acl-owner",
            redis_url=REDIS_TEST_URL,
            ttl_seconds=60,
            key_prefix=ownership_prefix,
            client=sync_tenant,
        )
        assert await asyncio.to_thread(ownership.take, "sandbox-acl") is True
        assert await asyncio.to_thread(ownership.owner, "sandbox-acl") == "acl-owner"
        assert await asyncio.to_thread(ownership.renew, "sandbox-acl") is RenewOutcome.RENEWED
        await asyncio.to_thread(ownership.release, "sandbox-acl")

        capacity = RedisE2BCapacityStore(
            redis_url=REDIS_TEST_URL,
            hard_limit=1,
            key_prefix=ownership_prefix,
            client=sync_tenant,
        )
        assert await asyncio.to_thread(
            capacity.reconcile,
            expected_revision=0,
            remote_sandboxes={},
            complete=True,
            reservation_max_age_ms=60_000,
        )
        assert await asyncio.to_thread(capacity.reserve, "reservation-acl") is ReserveStatus.GRANTED
        await asyncio.to_thread(
            capacity.track,
            "sandbox-capacity-acl",
            reservation_token="reservation-acl",
        )
        await asyncio.to_thread(capacity.release, "sandbox-capacity-acl")

        with pytest.raises(NoPermissionError):
            await tenant.xadd(
                f"{tenant_b_prefixes[RedisTenantComponent.STREAM_BRIDGE]}:stream",
                {"kind": "event"},
            )
        with pytest.raises(NoPermissionError):
            await tenant.xread({f"{tenant_b_prefixes[RedisTenantComponent.STREAM_BRIDGE]}:stream": "0-0"})
        with pytest.raises(NoPermissionError):
            await tenant.set(
                f"{tenant_b_prefixes[RedisTenantComponent.CHECKPOINT_CACHE]}:value",
                "denied",
            )
        with pytest.raises(NoPermissionError):
            await tenant.set(
                f"{tenant_b_prefixes[RedisTenantComponent.SANDBOX_OWNERSHIP]}:value",
                "denied",
            )
        with pytest.raises(NoPermissionError):
            await tenant.publish(
                f"{tenant_b_prefixes[RedisTenantComponent.STREAM_BRIDGE]}:events",
                "denied",
            )
    finally:
        if tenant is not None:
            await tenant.aclose()
        if checkpoint_tenant is not None:
            await checkpoint_tenant.aclose()
        if sync_tenant is not None:
            await asyncio.to_thread(sync_tenant.close)
        try:
            keys = [key async for key in admin.scan_iter(f"{tenant_a_namespace.key_prefix.rstrip(':')}*")]
            if keys:
                await admin.delete(*keys)
        finally:
            try:
                await admin.execute_command("ACL", "DELUSER", username)
            finally:
                await admin.aclose()


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_replays_after_last_event_id(real_redis_bridge):
    run_id = "integ-replay"
    await real_redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    await real_redis_bridge.publish(run_id, "values", {"step": 1})
    await real_redis_bridge.publish_end(run_id)

    first_pass = []
    async for entry in real_redis_bridge.subscribe(run_id, heartbeat_interval=1.0):
        first_pass.append(entry)
        if entry is END_SENTINEL:
            break

    received = []
    async for entry in real_redis_bridge.subscribe(run_id, last_event_id=first_pass[0].id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [e.event for e in received[:-1]] == ["values"]
    assert received[-1] is END_SENTINEL


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_invalid_last_event_id_tails_live_events(real_redis_bridge):
    """A malformed Last-Event-ID should wait at the live tail."""
    run_id = "integ-bad-leid"
    await real_redis_bridge.publish(run_id, "metadata", {"run_id": run_id})
    received = []

    async def publish_later() -> None:
        await anyio.sleep(0.05)
        await real_redis_bridge.publish(run_id, "values", {"step": 1})
        await real_redis_bridge.publish_end(run_id)

    with anyio.fail_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(publish_later)
            async for entry in real_redis_bridge.subscribe(run_id, last_event_id="not-a-valid-id", heartbeat_interval=0.01):
                if entry is HEARTBEAT_SENTINEL:
                    continue
                received.append(entry)
                if entry is END_SENTINEL:
                    break

    assert [e.event for e in received[:-1]] == ["values"]
    assert received[-1] is END_SENTINEL


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_maxlen_trims_history(real_redis_bridge):
    """queue_maxsize should bound the retained stream via XADD MAXLEN (exact)."""
    run_id = "integ-maxlen"
    # Fixture sets queue_maxsize=2; publish more data events than that.
    for i in range(6):
        await real_redis_bridge.publish(run_id, f"event-{i}", {"i": i})

    key = real_redis_bridge._key(run_id)
    length = await real_redis_bridge._redis.xlen(key)
    assert length == 2


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_evicted_cursor_yields_gap(real_redis_bridge):
    """Real Redis MAXLEN trimming must produce the same gap contract."""
    run_id = "integ-gap"
    await real_redis_bridge.publish(run_id, "event-1", {"i": 1})
    key = real_redis_bridge._key(run_id)
    first_id = (await real_redis_bridge._redis.xrange(key, count=1))[0][0]
    await real_redis_bridge.publish(run_id, "event-2", {"i": 2})
    await real_redis_bridge.publish(run_id, "event-3", {"i": 3})

    retained = await real_redis_bridge._redis.xrange(key)
    received = [
        entry
        async for entry in real_redis_bridge.subscribe(
            run_id,
            last_event_id=first_id,
            heartbeat_interval=1.0,
        )
    ]

    assert received == [
        StreamGap(
            requested_event_id=first_id,
            earliest_available_event_id=retained[0][0],
            latest_available_event_id=retained[-1][0],
        )
    ]


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_initial_subscriber_yields_gap_when_first_wake_falls_behind(
    real_redis_bridge,
):
    """A real blocking XREAD wake must be validated before first delivery."""
    raw_redis = real_redis_bridge._redis
    delayed = _DelayedBlockingReadRedis(raw_redis)
    real_redis_bridge._redis = delayed
    run_id = "integ-initial-subscriber-gap"
    subscriber = real_redis_bridge.subscribe(run_id, heartbeat_interval=1.0)
    first_item = asyncio.create_task(anext(subscriber))

    with anyio.fail_after(2):
        await delayed.blocking_read_started.wait()
        await real_redis_bridge.publish(run_id, "e1", {"step": 1})
        await delayed.wake_response_captured.wait()
        await real_redis_bridge.publish(run_id, "e2", {"step": 2})
        await real_redis_bridge.publish(run_id, "e3", {"step": 3})
        await real_redis_bridge.publish(run_id, "e4", {"step": 4})
        delayed.release_wake_response.set()
        gap = await first_item

    key = real_redis_bridge._key(run_id)
    retained = await raw_redis.xrange(key)
    assert gap == StreamGap(
        requested_event_id=None,
        earliest_available_event_id=retained[0][0],
        latest_available_event_id=retained[-1][0],
    )
    with pytest.raises(StopAsyncIteration):
        await anext(subscriber)


@pytest.mark.integration
@requires_redis
@pytest.mark.anyio
async def test_redis_integration_stream_ttl_reclaims_key():
    """Redis should reclaim retained stream data when cleanup never runs."""
    from redis.asyncio import Redis

    client = Redis.from_url(REDIS_TEST_URL, decode_responses=True)
    key_prefix = f"deerflow:test:{uuid.uuid4().hex}"
    bridge = RedisStreamBridge(
        redis_url=REDIS_TEST_URL,
        queue_maxsize=2,
        key_prefix=key_prefix,
        stream_ttl_seconds=1,
        client=client,
    )
    run_id = "integ-ttl"
    key = bridge._key(run_id)
    try:
        await bridge.publish(run_id, "metadata", {"run_id": run_id})
        assert await client.exists(key) == 1
        assert await client.ttl(key) >= 0
        await anyio.sleep(2.0)

        assert await client.exists(key) == 0
    finally:
        async for existing_key in client.scan_iter(f"{key_prefix}:*"):
            await client.delete(existing_key)
        await client.aclose()

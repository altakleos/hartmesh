"""Configured resource compatibility and real native fixtures for capture facades."""

from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from typing import Annotated, TypedDict
from uuid import uuid4

import pytest
import pytest_asyncio
from langgraph.channels import DeltaChannel
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.graph import StateGraph

from deerflow.config.checkpointer_config import CheckpointerConfig
from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.postgres_schema import dsn_with_search_path, normalize_libpq_dsn
from deerflow.runtime.checkpointer.async_provider import make_checkpointer
from deerflow.runtime.store.async_provider import make_store


def config_for(thread_id):
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def merge_values(current, updates):
    """DeltaChannel reducers receive a batch of updates, each a list here."""
    return current + [item for batch in updates for item in batch]


async def seed_checkpoint(saver, thread_id, *, value="retained", step=0):
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"value": value}
    checkpoint["channel_versions"] = {"value": step + 1}
    return await saver.aput(config_for(thread_id), checkpoint, {"source": "input", "step": step, "parents": {}}, {"value": step + 1})


@pytest_asyncio.fixture(params=["memory", "sqlite", "postgres"])
async def resource_backend(request, tmp_path, monkeypatch):
    """One actual backend; PostgreSQL schemas and all native lifetimes are owned."""
    kind = request.param
    schema = "capture_resources_" + uuid4().hex
    uri = None
    if kind == "postgres":
        raw_uri = os.environ.get("TEST_POSTGRES_URI")
        if not raw_uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        psycopg = pytest.importorskip("psycopg", reason="postgres extra is not installed", exc_type=ModuleNotFoundError)
        uri = normalize_libpq_dsn(raw_uri)
        async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as admin:
            await admin.execute(f'CREATE SCHEMA "{schema}"')
    path = tmp_path / "capture.sqlite"
    legacy = CheckpointerConfig(type=kind, connection_string=uri if kind == "postgres" else str(path) if kind == "sqlite" else None, postgres_schema=schema if kind == "postgres" else "")
    database = DatabaseConfig(backend="memory")
    app_config = SimpleNamespace(checkpointer=legacy, database=database)
    from deerflow.runtime import checkpoint_mode

    monkeypatch.setattr(checkpoint_mode, "frozen_checkpoint_channel_mode", lambda: "full")
    env = SimpleNamespace(kind=kind, schema=schema, uri=uri, path=path, config=app_config, dsn=dsn_with_search_path(uri, schema) if uri else None)
    try:
        yield env
    finally:
        if kind == "postgres":
            async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as admin:
                await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest_asyncio.fixture
async def native_resources(resource_backend):
    """Raw adapters are explicit test-owned instrumentation targets, not an escape."""
    env = resource_backend
    async with AsyncExitStack() as stack:
        if env.kind == "memory":
            from langgraph.checkpoint.memory import InMemorySaver
            from langgraph.store.memory import InMemoryStore

            saver, store = InMemorySaver(), InMemoryStore()
        elif env.kind == "sqlite":
            from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
            from langgraph.store.sqlite.aio import AsyncSqliteStore

            saver = await stack.enter_async_context(AsyncSqliteSaver.from_conn_string(str(env.path)))
            store = await stack.enter_async_context(AsyncSqliteStore.from_conn_string(str(env.path)))
            await saver.setup()
            await store.setup()
        else:
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
            from langgraph.store.postgres.aio import AsyncPostgresStore

            saver = await stack.enter_async_context(AsyncPostgresSaver.from_conn_string(env.dsn))
            store = await stack.enter_async_context(AsyncPostgresStore.from_conn_string(env.dsn))
            await saver.setup()
            await store.setup()
        yield SimpleNamespace(**vars(env), saver=saver, store=store)


@pytest_asyncio.fixture
async def provided_saver(resource_backend, monkeypatch):
    """Saver-only physical probes may change journal mode before another opener."""
    from deerflow.runtime.checkpointer import async_provider as savers

    original = savers._select_inner_checkpointer
    captured = []

    @asynccontextmanager
    async def capture(config):
        async with original(config) as inner:
            captured.append(inner)
            yield inner

    monkeypatch.setattr(savers, "_select_inner_checkpointer", capture)
    async with make_checkpointer(resource_backend.config) as saver:
        yield SimpleNamespace(**vars(resource_backend), saver=saver, native_saver=captured[0])


@pytest_asyncio.fixture
async def provided_resources(resource_backend, monkeypatch):
    """Observe real native factories without exposing raw clients in production."""
    from deerflow.runtime.checkpointer import async_provider as savers
    from deerflow.runtime.store import async_provider as stores

    captured = {}
    original_saver, original_store = savers._select_inner_checkpointer, stores._async_store

    @asynccontextmanager
    async def capture_saver(config):
        async with original_saver(config) as inner:
            captured["saver"] = inner
            yield inner

    @asynccontextmanager
    async def capture_store(config):
        async with original_store(config) as inner:
            captured["store"] = inner
            yield inner

    monkeypatch.setattr(savers, "_select_inner_checkpointer", capture_saver)
    monkeypatch.setattr(stores, "_async_store", capture_store)
    async with make_checkpointer(resource_backend.config) as saver, make_store(resource_backend.config) as store:
        yield SimpleNamespace(**vars(resource_backend), saver=saver, store=store, native_saver=captured["saver"], native_store=captured["store"])


@pytest.mark.asyncio
async def test_current_factories_keep_ordinary_checkpoint_and_store_data(resource_backend):
    """Behavioral compatibility control, independent of any missing capture API."""
    async with make_checkpointer(resource_backend.config) as saver, make_store(resource_backend.config) as store:
        saved = await seed_checkpoint(saver, "ordinary")
        checkpoint = await saver.aget(saved)
        assert checkpoint["channel_values"] == {"value": "retained"}
        await saver.aput_writes(saved, [("note", "durable write")], "task", task_path="node/0")
        assert (await saver.aget_tuple(saved)).pending_writes == [("task", "note", "durable write")]
        await store.aput(("ordinary",), "item", {"value": "retained"}, index=False)
        assert (await store.aget(("ordinary",), "item")).value == {"value": "retained"}
        assert [item.key for item in await store.asearch(("ordinary",), limit=1)] == ["item"]
        assert await store.alist_namespaces(prefix=("ordinary",), max_depth=1) == [("ordinary",)]
        await store.adelete(("ordinary",), "item")
        assert await store.aget(("ordinary",), "item") is None


@pytest.mark.asyncio
async def test_capture_facade_api_scaffold_is_explicit(resource_backend):
    """Missing type is API scaffolding, never counted as a native behavioral red."""
    from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    assert callable(OwnedCheckpointSaver) and callable(OwnedStore)
    owner = CaptureIOOwner()
    with capture_io_scope(owner):
        assert owner.loop is asyncio.get_running_loop()
    owner.retire()
    await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["saver", "store"])
async def test_retired_capture_cannot_mutate_actual_factory_backend(provided_resources, operation):
    from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope

    env = provided_resources
    owner = CaptureIOOwner()
    owner.retire()
    refused = False
    try:
        with capture_io_scope(owner):
            try:
                if operation == "saver":
                    await seed_checkpoint(env.saver, "retired-capture", value="must-not-write")
                else:
                    await env.store.aput(("retired-capture",), "item", {"must_not_write": True})
            except CaptureIOClosed:
                refused = True
        # Observe the actual durable bytes outside the retired caller scope.
        observed = await env.native_saver.aget_tuple(config_for("retired-capture")) if operation == "saver" else await env.native_store.aget(("retired-capture",), "item")
        assert observed is None, "retired operation reached the configured backend"
        assert refused, "retired capture must explicitly refuse, never silently skip"
        # The shared configured resource still serves an ordinary neighbor.
        await env.store.aput(("ordinary-neighbor",), "item", {"ok": True})
        assert (await env.store.aget(("ordinary-neighbor",), "item")).value == {"ok": True}
    finally:
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["saver", "store"])
@pytest.mark.parametrize("resource_backend", ["sqlite", "postgres"], indirect=True)
@pytest.mark.parametrize("caller", ["owner-loop", "worker-thread"])
async def test_async_backed_capture_sync_refuses_before_actual_delegate(provided_resources, monkeypatch, operation, caller):
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, CaptureIOUnsupported, capture_io_scope

    env = provided_resources
    owner = CaptureIOOwner()
    raw = env.native_saver if operation == "saver" else env.native_store
    method = "get_tuple" if operation == "saver" else "get"
    original = getattr(type(raw), method)
    entered = []

    def observe(instance, *args, **kwargs):
        if instance is raw:
            entered.append(True)
        return original(instance, *args, **kwargs)

    monkeypatch.setattr(type(raw), method, observe)

    def call():
        if operation == "saver":
            return env.saver.get_tuple(config_for("sync-refusal"))
        return env.store.get(("sync-refusal",), "item")

    try:
        with capture_io_scope(owner):
            with pytest.raises(CaptureIOUnsupported):
                if caller == "owner-loop":
                    call()
                else:
                    await asyncio.to_thread(call)
        assert entered == [], "refusal must precede native bridge/codec submission"
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
@pytest.mark.parametrize("caller", ["owner-loop", "worker-thread"])
async def test_atomic_memory_capture_sync_is_guarded_and_ordinary_unchanged(provided_resources, caller):
    from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope

    env = provided_resources
    owner = CaptureIOOwner()

    async def call(method, *args):
        if caller == "worker-thread":
            return await asyncio.to_thread(method, *args)
        return method(*args)

    try:
        with capture_io_scope(owner):
            await call(env.store.put, ("memory-capture",), "item", {"value": 1})
            assert (await call(env.store.get, ("memory-capture",), "item")).value == {"value": 1}
            owner.retire()
            with pytest.raises(CaptureIOClosed):
                await call(env.store.put, ("memory-capture",), "item", {"value": 2})
        assert env.store.get(("memory-capture",), "item").value == {"value": 1}
        env.store.put(("memory-capture",), "item", {"value": 3})
        assert env.store.get(("memory-capture",), "item").value == {"value": 3}
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
async def test_actual_delta_graph_and_allowlist_clone_keep_existing_cache_semantics(resource_backend, monkeypatch, strict):
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

    from deerflow.runtime import checkpoint_mode

    monkeypatch.setattr(checkpoint_mode, "frozen_checkpoint_channel_mode", lambda: "delta")

    class State(TypedDict):
        values: Annotated[list[str], DeltaChannel(merge_values, snapshot_frequency=2)]

    async def step(state):
        return {"values": ["node"]}

    async with make_checkpointer(resource_backend.config) as saver:
        saver.serde = JsonPlusSerializer(allowed_msgpack_modules=None if strict else True)
        original_serde = saver.serde
        clone = saver.with_allowlist([("capture_test", "Allowed")])
        if strict:
            assert clone is not saver and clone.serde is not original_serde
        else:
            assert clone is saver and clone.serde is original_serde
        assert saver.serde is original_serde
        assert clone.config_specs == saver.config_specs
        builder = StateGraph(State)
        builder.add_node("step", step)
        builder.set_entry_point("step")
        builder.set_finish_point("step")
        graph = builder.compile(checkpointer=clone)
        config = config_for("cached-allowlist")
        assert await graph.ainvoke({"values": ["first"]}, config) == {"values": ["first", "node"]}
        assert await graph.ainvoke({"values": ["second"]}, config) == {"values": ["first", "node", "second", "node"]}
        assert (await graph.aget_state(config)).values == {"values": ["first", "node", "second", "node"]}
        assert (await graph.aget_state(config)).values == {"values": ["first", "node", "second", "node"]}
        assert saver.serde is original_serde


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["memory"], indirect=True)
async def test_actual_async_only_cache_prevents_atomic_memory_sync_qualification(resource_backend, monkeypatch):
    from deerflow.runtime import checkpoint_mode
    from deerflow.runtime.checkpoint_cache import provider
    from deerflow.runtime.checkpoint_cache.redis import RedisCheckpointHistoryCache
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, CaptureIOUnsupported, capture_io_scope

    # The actual Redis cache client remains lazy/unconnected: this test must
    # refuse before a cache request. Its real async-only API is the evidence.
    cache = RedisCheckpointHistoryCache("redis://127.0.0.1:1/0", serde=None, ttl_seconds=60)

    @asynccontextmanager
    async def selected(config, *, serde):
        yield cache

    monkeypatch.setattr(checkpoint_mode, "frozen_checkpoint_channel_mode", lambda: "delta")
    monkeypatch.setattr(provider, "make_checkpoint_cache", selected)
    owner = CaptureIOOwner()
    try:
        async with make_checkpointer(resource_backend.config) as saver:
            saved = await seed_checkpoint(saver, "async-only-cache")
            with capture_io_scope(owner):
                with pytest.raises(CaptureIOUnsupported):
                    saver.get_tuple(saved)
            assert (await saver.aget_tuple(saved)).checkpoint["channel_values"] == {"value": "retained"}
    finally:
        owner.retire()
        await owner.drain()
        await cache.aclose()


@pytest.mark.parametrize("kind", ["saver", "store"])
def test_sync_singleton_wrapper_failure_closes_before_publishing(monkeypatch, kind):
    from contextlib import contextmanager

    from deerflow.runtime import capture_resources
    from deerflow.runtime.checkpointer import provider as savers
    from deerflow.runtime.store import provider as stores

    module = savers if kind == "saver" else stores
    value_name, context_name = ("_checkpointer", "_checkpointer_ctx") if kind == "saver" else ("_store", "_store_ctx")
    monkeypatch.setattr(module, value_name, None)
    monkeypatch.setattr(module, context_name, None)
    config = SimpleNamespace(checkpointer=CheckpointerConfig(type="memory"), database=DatabaseConfig(backend="memory"))
    monkeypatch.setattr(module, "get_app_config", lambda: config)
    selector_name = "_sync_checkpointer_cm" if kind == "saver" else "_sync_store_cm"
    original = getattr(module, selector_name)
    exited = []

    @contextmanager
    def selected(settings):
        try:
            with original(settings) as inner:
                yield inner
        finally:
            exited.append(True)

    def refused(*args, **kwargs):
        raise RuntimeError("test-only constructor failure")

    monkeypatch.setattr(module, selector_name, selected)
    monkeypatch.setattr(module, "_get_checkpointer_config" if kind == "saver" else "_get_store_config", lambda: config.checkpointer)
    monkeypatch.setattr(capture_resources, "OwnedCheckpointSaver" if kind == "saver" else "OwnedStore", refused)
    try:
        with pytest.raises(RuntimeError, match="test-only constructor failure"):
            (module.get_checkpointer if kind == "saver" else module.get_store)()
        assert exited == [True], "failed wrapper construction must close its actual provider context"
        assert getattr(module, value_name) is None and getattr(module, context_name) is None
    finally:
        (module.reset_checkpointer if kind == "saver" else module.reset_store)()


def test_all_public_sdk_methods_preserve_parameter_names_kinds_and_defaults():
    import inspect

    from langgraph.checkpoint.base import BaseCheckpointSaver
    from langgraph.store.base import BaseStore

    from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore

    for base, facade in [(BaseCheckpointSaver, OwnedCheckpointSaver), (BaseStore, OwnedStore)]:
        for name, method in vars(base).items():
            if name.startswith("_") or not inspect.isfunction(method):
                continue
            assert name in vars(facade), f"public {base.__name__}.{name} needs explicit forwarding"
            expected = inspect.signature(method).parameters
            actual = inspect.signature(getattr(facade, name)).parameters
            assert list(actual) == list(expected), name
            assert [(value.kind, value.default) for value in actual.values()] == [(value.kind, value.default) for value in expected.values()], name


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_backend", ["sqlite", "postgres"], indirect=True)
async def test_real_store_ttl_defaults_and_refresh_reach_existing_sdk_batch(provided_resources, monkeypatch):
    from langgraph.store.base import GetOp, PutOp

    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope

    env = provided_resources
    original = type(env.native_store).abatch
    ops = []

    async def observe(store, values):
        values = list(values)
        if store is env.native_store:
            ops.extend(values)
        return await original(store, values)

    monkeypatch.setattr(type(env.native_store), "abatch", observe)
    settings = {"default_ttl": 5.0, "refresh_on_read": False}
    env.store.ttl_config = settings
    assert env.store.ttl_config is settings and env.native_store.ttl_config is settings
    assert env.store.supports_ttl is env.native_store.supports_ttl
    owner = CaptureIOOwner()
    try:
        with capture_io_scope(owner):
            await env.store.aput(("ttl-capture",), "default", {"value": 1}, index=False)
            await env.store.aput(("ttl-capture",), "unlimited", {"value": 2}, ttl=None)
            await env.store.aput(("ttl-capture",), "explicit", {"value": 3}, ttl=2.0)
            assert (await env.store.aget(("ttl-capture",), "default")).value == {"value": 1}
            assert (await env.store.aget(("ttl-capture",), "explicit", refresh_ttl=True)).value == {"value": 3}
        assert [op.ttl for op in ops if isinstance(op, PutOp)] == [5.0, None, 2.0]
        assert [op.refresh_ttl for op in ops if isinstance(op, GetOp)] == [False, True]
    finally:
        owner.retire()
        await owner.drain()

"""Checkpoint retention reaches storage through HartMesh's capture facade.

The Gateway's checkpointer is always an ``OwnedCheckpointSaver`` (the capture
facade), wrapping either the raw saver or, in delta mode, a
``CachedHistorySaver``. Upstream retention resolves storage through the
public ``source_saver`` contract and purges caches through
``ainvalidate_history_cache``; without both on the facade, every admission
logged "requires a supported storage saver" and no trailing duration leaf
was ever removed.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, TypedDict
from uuid import uuid4

import pytest
from langchain_core.messages import AnyMessage, HumanMessage
from langgraph.channels import DeltaChannel
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import StateGraph
from langgraph.graph.message import add_messages

from app.gateway.checkpoint_retention import enforce_completed_run_retention
from deerflow.agents.thread_state import merge_message_writes
from deerflow.runtime.capture_resources import OwnedCheckpointSaver, checkpoint_cache_capture_profile, checkpoint_capture_profile
from deerflow.runtime.checkpoint_cache.base import make_history_key
from deerflow.runtime.checkpoint_cache.memory import MemoryCheckpointHistoryCache
from deerflow.runtime.checkpointer.cached_saver import CachedHistorySaver
from deerflow.runtime.runs.worker import persist_run_durations, persist_run_history_metadata


class FullState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


class DeltaState(TypedDict):
    messages: Annotated[list[AnyMessage], DeltaChannel(merge_message_writes, snapshot_frequency=2)]


def _config(thread_id: str, checkpoint_id: str | None = None) -> dict[str, Any]:
    configurable = {"thread_id": thread_id}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def _graph(schema: Any, checkpointer: Any) -> Any:
    builder = StateGraph(schema)
    builder.add_node("noop", lambda state: {})
    builder.set_entry_point("noop")
    builder.set_finish_point("noop")
    return builder.compile(checkpointer=checkpointer)


@asynccontextmanager
async def _native(kind: str, tmp_path) -> AsyncIterator[Any]:
    if kind == "memory":
        yield InMemorySaver()
        return
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "owned-retention.sqlite")) as saver:
        await saver.setup()
        yield saver


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_retention_removes_the_duration_leaf_through_the_facade(kind, tmp_path) -> None:
    async with _native(kind, tmp_path) as native:
        saver = OwnedCheckpointSaver(native, profile=checkpoint_capture_profile(native, asynchronous=True), loop=asyncio.get_running_loop())
        thread_id = f"owned-retention-{uuid4().hex}"
        await _graph(FullState, saver).ainvoke({"messages": [HumanMessage(content="hello")]}, _config(thread_id))
        resumable = (await saver.aget_tuple(_config(thread_id))).checkpoint["id"]
        assert await persist_run_durations(checkpointer=saver, thread_id=thread_id, durations={"run-1": 3})
        leaf = (await saver.aget_tuple(_config(thread_id))).checkpoint["id"]
        assert leaf != resumable

        report = await enforce_completed_run_retention(checkpointer=saver, thread_id=thread_id)

        assert report.deleted_checkpoint_ids == [leaf]
        assert (await saver.aget_tuple(_config(thread_id))).checkpoint["id"] == resumable


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_retention_purges_the_wrapped_history_cache(kind, tmp_path) -> None:
    async with _native(kind, tmp_path) as native:
        cache = MemoryCheckpointHistoryCache(max_entries=16)
        profile = checkpoint_cache_capture_profile(checkpoint_capture_profile(native, asynchronous=True), cache)
        saver = OwnedCheckpointSaver(CachedHistorySaver(native, cache, key_prefix="owned-retention"), profile=profile, loop=asyncio.get_running_loop())
        assert saver.source_saver is native
        thread_id = f"owned-retention-{uuid4().hex}"
        await _graph(DeltaState, saver).ainvoke({"messages": [HumanMessage(content="hello")]}, _config(thread_id))
        assert await persist_run_history_metadata(checkpointer=saver, thread_id=thread_id, durations={"run-1": 3}, message_run_ids={"message-1": "run-1"})
        leaf = (await saver.aget_tuple(_config(thread_id))).checkpoint["id"]
        await cache.aset_many({make_history_key("owned-retention", thread_id, "", leaf, "messages"): {"writes": []}})
        assert cache.stats().entries == 1

        report = await enforce_completed_run_retention(checkpointer=saver, thread_id=thread_id)

        assert report.deleted_checkpoint_ids == [leaf]
        assert cache.stats().entries == 0
        assert await native.aget_tuple(_config(thread_id, leaf)) is None


@pytest.mark.anyio
async def test_cacheless_facade_invalidation_is_a_no_op() -> None:
    native = InMemorySaver()
    saver = OwnedCheckpointSaver(native, profile="memory", loop=asyncio.get_running_loop())

    assert saver.source_saver is native
    await saver.ainvalidate_history_cache("any-thread")

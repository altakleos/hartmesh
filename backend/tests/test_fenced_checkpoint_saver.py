from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from langgraph.checkpoint.base import BaseCheckpointSaver

from deerflow.runtime.checkpointer.fenced_saver import (
    CheckpointOwnershipLost,
    FencedCheckpointSaver,
)


class _Saver(BaseCheckpointSaver):
    def __init__(self) -> None:
        super().__init__()
        self.puts = 0
        self.writes = 0

    async def aput(self, config, checkpoint, metadata, new_versions):
        self.puts += 1
        return config

    async def aput_writes(
        self,
        config,
        writes,
        task_id,
        task_path="",
    ) -> None:
        self.writes += 1


@pytest.mark.anyio
async def test_async_checkpoint_mutations_require_current_run_fence() -> None:
    active = True
    rejected = AsyncMock()
    inner = _Saver()

    @asynccontextmanager
    async def fence():
        yield active

    saver = FencedCheckpointSaver(
        inner,
        fence=fence,
        on_rejected=rejected,
    )
    config = {"configurable": {"thread_id": "thread-1"}}

    await saver.aput(config, {}, {}, {})
    active = False
    with pytest.raises(CheckpointOwnershipLost):
        await saver.aput_writes(config, [], "task-1")

    assert inner.puts == 1
    assert inner.writes == 0
    rejected.assert_awaited_once_with("put_writes")


def test_sync_checkpoint_mutations_fail_closed() -> None:
    @asynccontextmanager
    async def fence():
        yield True

    saver = FencedCheckpointSaver(_Saver(), fence=fence)

    with pytest.raises(CheckpointOwnershipLost, match="async"):
        saver.put({"configurable": {"thread_id": "thread-1"}}, {}, {}, {})

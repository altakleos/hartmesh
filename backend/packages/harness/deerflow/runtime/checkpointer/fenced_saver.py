"""Run-owner fencing wrapper for durable checkpoint mutations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple


class CheckpointOwnershipLost(RuntimeError):
    """The writer no longer holds the durable run execution fence."""


class FencedCheckpointSaver(BaseCheckpointSaver):
    """Delegate reads while gating every mutation on the current run fence.

    A wrapper instance belongs to one admitted run. The durable store-backed
    callback is evaluated immediately before each async saver mutation, so a
    process that returns after lease takeover cannot checkpoint with its stale
    owner/epoch tuple. Durable Gateway execution is async; synchronous writes
    fail closed because they cannot evaluate an async database-time fence.
    """

    def __init__(
        self,
        inner: BaseCheckpointSaver,
        *,
        fence: Callable[[], AbstractAsyncContextManager[bool]],
        on_rejected: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self.serde = inner.serde
        self._inner = inner
        self._fence = fence
        self._on_rejected = on_rejected

    def __getattr__(self, name: str) -> Any:
        inner = self.__dict__.get("_inner")
        if inner is None:
            raise AttributeError(name)
        return getattr(inner, name)

    async def _mutate(
        self,
        operation: str,
        mutation: Callable[[], Awaitable[Any]],
    ) -> Any:
        async with self._fence() as active:
            if active:
                return await mutation()
        if self._on_rejected is not None:
            await self._on_rejected(operation)
        raise CheckpointOwnershipLost("run_checkpoint_ownership_lost")

    @staticmethod
    def _reject_sync() -> None:
        raise CheckpointOwnershipLost(
            "durable run checkpoint fencing requires async mutation",
        )

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self._inner.get_tuple(config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        return self._inner.list(
            config,
            filter=filter,
            before=before,
            limit=limit,
        )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
    ) -> RunnableConfig:
        del config, checkpoint, metadata, new_versions
        self._reject_sync()

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        del config, writes, task_id, task_path
        self._reject_sync()

    def delete_thread(self, thread_id: str) -> None:
        del thread_id
        self._reject_sync()

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        del run_ids
        self._reject_sync()

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        del source_thread_id, target_thread_id
        self._reject_sync()

    def prune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        del thread_ids, strategy
        self._reject_sync()

    async def aget_tuple(
        self,
        config: RunnableConfig,
    ) -> CheckpointTuple | None:
        return await self._inner.aget_tuple(config)

    def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._inner.alist(
            config,
            filter=filter,
            before=before,
            limit=limit,
        )

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
    ) -> RunnableConfig:
        return await self._mutate(
            "put",
            lambda: self._inner.aput(
                config,
                checkpoint,
                metadata,
                new_versions,
            ),
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await self._mutate(
            "put_writes",
            lambda: self._inner.aput_writes(
                config,
                writes,
                task_id,
                task_path,
            ),
        )

    async def adelete_thread(self, thread_id: str) -> None:
        await self._mutate(
            "delete_thread",
            lambda: self._inner.adelete_thread(thread_id),
        )

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        await self._mutate(
            "delete_for_runs",
            lambda: self._inner.adelete_for_runs(run_ids),
        )

    async def acopy_thread(
        self,
        source_thread_id: str,
        target_thread_id: str,
    ) -> None:
        await self._mutate(
            "copy_thread",
            lambda: self._inner.acopy_thread(source_thread_id, target_thread_id),
        )

    async def aprune(
        self,
        thread_ids: Sequence[str],
        *,
        strategy: str = "keep_latest",
    ) -> None:
        await self._mutate(
            "prune",
            lambda: self._inner.aprune(thread_ids, strategy=strategy),
        )

    def get_next_version(self, current: Any, channel: Any) -> Any:
        return self._inner.get_next_version(current, channel)


__all__ = ["CheckpointOwnershipLost", "FencedCheckpointSaver"]

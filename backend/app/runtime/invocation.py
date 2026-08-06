"""Application-layer ownership of durable invocation sequencing."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, Protocol

from deerflow.runtime import CancelOutcome, DisconnectMode, RunRecord

WorkerCoroutine = Coroutine[Any, Any, None]
WorkerFactory = Callable[[RunRecord], WorkerCoroutine]
TaskFactory = Callable[[WorkerCoroutine], asyncio.Task[None]]


class InternalSourceKind(StrEnum):
    http = "http"
    scheduled_task = "scheduled_task"


@dataclass(frozen=True)
class InternalLaunchIntent:
    """Finite, host-internal request for one invocation."""

    thread_id: str
    assistant_id: str | None = None
    input: dict[str, Any] | None = None
    command: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    checkpoint_id: str | None = None
    checkpoint: dict[str, Any] | None = None
    interrupt_before: list[str] | Literal["*"] | None = None
    interrupt_after: list[str] | Literal["*"] | None = None
    stream_mode: list[str] | str | None = None
    stream_subgraphs: bool = False
    on_disconnect: Literal["cancel", "continue"] = "cancel"
    multitask_strategy: Literal["reject", "rollback", "interrupt"] = "reject"
    source_kind: InternalSourceKind = InternalSourceKind.http
    trusted_task_id: str | None = None
    task_run_id: str | None = None
    scheduled_trigger: Literal["scheduled", "manual"] | None = None
    owner_user_id: str | None = None


@dataclass(frozen=True)
class PreparedLaunch:
    """Normalized admission data plus the deferred worker factory."""

    thread_id: str
    assistant_id: str | None
    on_disconnect: DisconnectMode
    metadata: dict[str, Any]
    kwargs: dict[str, Any]
    multitask_strategy: str
    model_name: str | None
    user_id: str | None
    worker: WorkerFactory = field(repr=False)


@dataclass(frozen=True)
class InternalLaunchReceipt:
    record: RunRecord


@dataclass(frozen=True)
class InvocationPrincipal:
    user_id: str | None


class NotFoundOrInvisible(StrEnum):
    not_found_or_invisible = "not_found_or_invisible"


@dataclass(frozen=True)
class InternalCancelRequest:
    run_id: str
    action: Literal["interrupt", "rollback"] = "interrupt"


@dataclass(frozen=True)
class InternalCancelReceipt:
    outcome: CancelOutcome


class LaunchNormalizer(Protocol):
    def scope(self, intent: InternalLaunchIntent) -> AbstractContextManager[None]: ...

    async def normalize(self, intent: InternalLaunchIntent) -> PreparedLaunch: ...


class DurableRuns(Protocol):
    def admission_scope(self, thread_id: str) -> AbstractAsyncContextManager[None]: ...

    async def prepare_admission(self, launch: PreparedLaunch) -> None: ...

    async def admit(self, launch: PreparedLaunch) -> RunRecord: ...

    async def fail_start(self, record: RunRecord, error: str) -> None: ...

    async def observe(self, run_id: str, principal: InvocationPrincipal) -> RunRecord | None: ...

    async def cancel(self, request: InternalCancelRequest) -> CancelOutcome: ...


class InvocationRuntime:
    """Deep application module for launch, observation, and cancellation."""

    def __init__(
        self,
        *,
        normalizer: LaunchNormalizer,
        runs: DurableRuns,
        task_factory: TaskFactory = asyncio.create_task,
    ) -> None:
        self._normalizer = normalizer
        self._runs = runs
        self._task_factory = task_factory

    async def launch(self, intent: InternalLaunchIntent) -> InternalLaunchReceipt:
        with self._normalizer.scope(intent):
            launch = await self._normalizer.normalize(intent)
            async with self._runs.admission_scope(launch.thread_id):
                await self._runs.prepare_admission(launch)
                record = await self._runs.admit(launch)
                # Keep attachment adjacent to durable admission: no await may
                # separate a successful admit from installing its worker task.
                worker = launch.worker(record)
                try:
                    record.task = self._task_factory(worker)
                except Exception as exc:
                    close = getattr(worker, "close", None)
                    if callable(close):
                        close()
                    await self._runs.fail_start(
                        record,
                        f"Failed to attach run worker: {exc}",
                    )
                    raise
        return InternalLaunchReceipt(record=record)

    async def observe_run(
        self,
        run_id: str,
        principal: InvocationPrincipal,
    ) -> RunRecord | NotFoundOrInvisible:
        record = await self._runs.observe(run_id, principal)
        if record is None:
            return NotFoundOrInvisible.not_found_or_invisible
        return record

    async def cancel_run(
        self,
        request: InternalCancelRequest,
    ) -> InternalCancelReceipt:
        outcome = await self._runs.cancel(request)
        return InternalCancelReceipt(outcome=outcome)

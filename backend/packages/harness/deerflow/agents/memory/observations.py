"""Host binding from portable memory callbacks to the durable run journal."""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol

from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.memory_observation import (
    MemoryObservationStatus,
    MemoryObservationV1,
    MemoryOperation,
)


class MemoryObservationSink(Protocol):
    """Host sink that accepts one validated durable memory observation."""

    async def persist_memory_observations(
        self,
        observations: Sequence[MemoryObservationV1],
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class _Binding:
    sink: MemoryObservationSink
    tenant: TenantReferenceV1
    owner_loop: asyncio.AbstractEventLoop | None


_CURRENT_BINDING: ContextVar[_Binding | None] = ContextVar(
    "deerflow_memory_observation_binding",
    default=None,
)


class _StageState(Enum):
    OPEN = "open"
    DISCARDED = "discarded"
    SEALED = "sealed"


@dataclass(slots=True)
class MemoryObservationStage:
    """Thread-safe two-phase collection for one candidate context injection."""

    _binding: _Binding | None
    _observations: list[MemoryObservationV1] = field(default_factory=list)
    _state: _StageState = _StageState.OPEN
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def append(
        self,
        binding: _Binding,
        observation: MemoryObservationV1,
    ) -> bool:
        """Stage an observation, or reject it after timeout/discard."""

        with self._lock:
            if self._state is not _StageState.OPEN:
                return False
            if self._binding is not binding:
                raise RuntimeError("memory observation binding changed during staging")
            self._observations.append(observation)
            return True

    def discard(self) -> None:
        """Atomically prevent late worker completion from emitting evidence."""

        with self._lock:
            if self._state is _StageState.OPEN:
                self._state = _StageState.DISCARDED
                self._observations.clear()

    def seal(self) -> tuple[_Binding | None, tuple[MemoryObservationV1, ...]]:
        """Close the stage and return its immutable persistence batch."""

        with self._lock:
            if self._state is not _StageState.OPEN:
                return self._binding, ()
            self._state = _StageState.SEALED
            return self._binding, tuple(self._observations)


_CURRENT_STAGE: ContextVar[MemoryObservationStage | None] = ContextVar(
    "deerflow_memory_observation_stage",
    default=None,
)


@contextmanager
def bind_memory_observation_sink(
    sink: MemoryObservationSink,
    tenant: TenantReferenceV1,
) -> Iterator[None]:
    """Bind one trusted durable sink for the current run execution context."""

    if not isinstance(tenant, TenantReferenceV1):
        raise TypeError("tenant must be TenantReferenceV1")
    try:
        owner_loop = asyncio.get_running_loop()
    except RuntimeError:
        owner_loop = None
    token = _CURRENT_BINDING.set(
        _Binding(
            sink=sink,
            tenant=tenant,
            owner_loop=owner_loop,
        )
    )
    try:
        yield
    finally:
        _CURRENT_BINDING.reset(token)


@contextmanager
def stage_memory_observations() -> Iterator[MemoryObservationStage]:
    """Stage observations until the caller confirms context will be injected."""

    stage = MemoryObservationStage(_CURRENT_BINDING.get())
    token = _CURRENT_STAGE.set(stage)
    try:
        yield stage
    finally:
        _CURRENT_STAGE.reset(token)


async def commit_memory_observations(stage: MemoryObservationStage) -> None:
    """Persist a completed injection's staged observations before it is used."""

    if not isinstance(stage, MemoryObservationStage):
        raise TypeError("stage must be MemoryObservationStage")
    binding, observations = stage.seal()
    if binding is None or not observations:
        return
    await binding.sink.persist_memory_observations(observations)


def _projection_digest(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        encoded = value.encode("utf-8")
    else:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _persist_on_owner_loop(
    binding: _Binding,
    observation: MemoryObservationV1,
) -> None:
    """Wait for actual event-store persistence on the run's owner loop."""

    owner_loop = binding.owner_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
    if owner_loop is None:
        asyncio.run(binding.sink.persist_memory_observations((observation,)))
        return
    if current_loop is owner_loop:
        raise RuntimeError("durable memory reads must run off the owner event loop")
    if owner_loop.is_closed():
        raise RuntimeError("memory observation owner loop is closed")
    acknowledged = asyncio.run_coroutine_threadsafe(
        binding.sink.persist_memory_observations((observation,)),
        owner_loop,
    )
    acknowledged.result()


def observe_honcho_memory(
    *,
    workspace: str,
    operation: MemoryOperation,
    status: MemoryObservationStatus,
    safe_projection: Any | None,
    item_count: int | None,
    truncated: bool,
) -> bool:
    """Append one redacted observation when a trusted run binding exists.

    Raw workspace names and projections are reduced to digests here and never
    cross the journal boundary. ``False`` means an ordinary non-durable call,
    not an append failure.
    """

    binding = _CURRENT_BINDING.get()
    if binding is None:
        return False
    workspace_digest = hashlib.sha256(workspace.encode("utf-8")).hexdigest()
    observation = MemoryObservationV1(
        version=1,
        backend="honcho",
        tenant=binding.tenant,
        workspace_ref=f"honcho-workspace-{workspace_digest[:24]}",
        operation=operation,
        status=status,
        safe_projection_digest=_projection_digest(safe_projection),
        item_count=item_count,
        truncated=truncated,
        occurred_at=datetime.now(UTC),
    )
    stage = _CURRENT_STAGE.get()
    if stage is not None:
        return stage.append(binding, observation)
    _persist_on_owner_loop(binding, observation)
    return True


__all__ = [
    "MemoryObservationSink",
    "MemoryObservationStage",
    "bind_memory_observation_sink",
    "commit_memory_observations",
    "observe_honcho_memory",
    "stage_memory_observations",
]

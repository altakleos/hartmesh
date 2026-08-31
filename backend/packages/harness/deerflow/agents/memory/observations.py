"""Host binding from portable memory callbacks to the durable run journal."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.memory_observation import MemoryObservationV1


class MemoryObservationSink(Protocol):
    def record_memory_observation(self, observation: MemoryObservationV1) -> None: ...


@dataclass(frozen=True, slots=True)
class _Binding:
    sink: MemoryObservationSink
    tenant: TenantReferenceV1
    owner_loop: asyncio.AbstractEventLoop | None


_CURRENT_BINDING: ContextVar[_Binding | None] = ContextVar(
    "deerflow_memory_observation_binding",
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


def _record_on_owner_loop(
    binding: _Binding,
    observation: MemoryObservationV1,
) -> None:
    """Serialize journal mutation back onto the run's event-loop thread."""

    owner_loop = binding.owner_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
    if owner_loop is None or current_loop is owner_loop:
        binding.sink.record_memory_observation(observation)
        return
    if owner_loop.is_closed():
        raise RuntimeError("memory observation owner loop is closed")

    acknowledged: Future[None] = Future()

    def append() -> None:
        try:
            binding.sink.record_memory_observation(observation)
        except BaseException as exc:
            acknowledged.set_exception(exc)
        else:
            acknowledged.set_result(None)

    owner_loop.call_soon_threadsafe(append)
    # The owner loop is awaiting the asyncio.to_thread operation that reached
    # this callback, so it remains able to run ``append`` while this worker
    # waits for an honest success/failure acknowledgement.
    acknowledged.result()


def observe_honcho_memory(
    *,
    workspace: str,
    operation: str,
    status: str,
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
        operation=operation,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        safe_projection_digest=_projection_digest(safe_projection),
        item_count=item_count,
        truncated=truncated,
        occurred_at=datetime.now(UTC),
    )
    _record_on_owner_loop(binding, observation)
    return True


__all__ = [
    "MemoryObservationSink",
    "bind_memory_observation_sink",
    "observe_honcho_memory",
]

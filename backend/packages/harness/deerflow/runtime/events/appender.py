"""Authority-bound append ports for the rich run-event stream.

Live workers receive :class:`FencedRunEventAppender`; their callers cannot
select another tenant, run, thread, owner, or epoch. Recovery, migration, and
history-seeding code must opt into the visibly separate administrative port.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from deerflow_extension_api import TenantReferenceV1

if TYPE_CHECKING:
    from deerflow.runtime.events.store.base import RunEventStore


class RuntimeEventOwnershipLost(RuntimeError):
    """The supplied run-event writer authority is no longer current."""


@dataclass(frozen=True, slots=True)
class RuntimeEventAuthority:
    """One server-owned tenant/run/worker/epoch write capability."""

    tenant: TenantReferenceV1 | None
    thread_id: str
    run_id: str
    owner_id: str
    lease_epoch: int

    def __post_init__(self) -> None:
        if self.tenant is not None and not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")
        for label, value, limit in (
            ("thread_id", self.thread_id, 64),
            ("run_id", self.run_id, 64),
            ("owner_id", self.owner_id, 128),
        ):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit:
                raise ValueError(f"runtime event authority {label} is invalid")
        if type(self.lease_epoch) is not int or self.lease_epoch < 0:
            raise ValueError("runtime event authority lease_epoch is invalid")

    @property
    def tenant_digest(self) -> str | None:
        return None if self.tenant is None else self.tenant.digest

    def require_event_identity(self, event: dict[str, Any]) -> None:
        if event.get("thread_id") != self.thread_id or event.get("run_id") != self.run_id:
            raise ValueError("runtime event identity differs from writer authority")


class FencedRunEventAppender:
    """RunEventStore-shaped live-worker port that always supplies authority."""

    def __init__(
        self,
        store: RunEventStore,
        authority: RuntimeEventAuthority,
        *,
        process_local_validator: Callable[[RuntimeEventAuthority], Awaitable[bool]] | None = None,
        authority_provider: Callable[[], Awaitable[RuntimeEventAuthority]] | None = None,
    ) -> None:
        self._store = store
        self.authority = authority
        self._process_local_validator = process_local_validator
        self._authority_provider = authority_provider

    async def _current_authority(self) -> RuntimeEventAuthority:
        provider = self._authority_provider
        if provider is None:
            return self.authority
        candidate = await provider()
        if (
            candidate.tenant != self.authority.tenant
            or candidate.thread_id != self.authority.thread_id
            or candidate.run_id != self.authority.run_id
            or candidate.owner_id != self.authority.owner_id
            or candidate.lease_epoch < self.authority.lease_epoch
        ):
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")
        return candidate

    async def _require_process_local_authority(
        self,
        authority: RuntimeEventAuthority,
    ) -> None:
        validator = self._process_local_validator
        if validator is None:
            return
        if not await validator(authority):
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")

    async def put(
        self,
        *,
        event_type: str,
        category: str,
        content: str | dict = "",
        metadata: dict | None = None,
        created_at: str | None = None,
        thread_id: str | None = None,
        run_id: str | None = None,
    ) -> dict:
        event = {
            "thread_id": self.authority.thread_id if thread_id is None else thread_id,
            "run_id": self.authority.run_id if run_id is None else run_id,
            "event_type": event_type,
            "category": category,
            "content": content,
            "metadata": metadata or {},
        }
        if created_at is not None:
            event["created_at"] = created_at
        return (await self.put_batch([event]))[0]

    async def put_batch(self, events: list[dict]) -> list[dict]:
        detached = [dict(event) for event in events]
        authority = await self._current_authority()
        for event in detached:
            authority.require_event_identity(event)
        if self._process_local_validator is not None:
            await self._require_process_local_authority(authority)
            return await self._store.put_batch(detached)
        return await self._store.append_fenced_batch(authority, detached)

    async def put_if_absent(
        self,
        *,
        event_type: str,
        category: str,
        content: str | dict = "",
        metadata: dict | None = None,
        created_at: str | None = None,
        thread_id: str | None = None,
        run_id: str | None = None,
    ) -> tuple[dict, bool]:
        event = {
            "thread_id": self.authority.thread_id if thread_id is None else thread_id,
            "run_id": self.authority.run_id if run_id is None else run_id,
            "event_type": event_type,
            "category": category,
            "content": content,
            "metadata": metadata or {},
        }
        if created_at is not None:
            event["created_at"] = created_at
        authority = await self._current_authority()
        authority.require_event_identity(event)
        if self._process_local_validator is not None:
            await self._require_process_local_authority(authority)
            return await self._store.put_if_absent(**event)
        return await self._store.append_fenced_if_absent(authority, event)


class AdministrativeRunEventAppender:
    """Explicitly unfenced port for recovery, migration, and history seeding."""

    def __init__(self, store: RunEventStore) -> None:
        self._store = store

    async def put(self, **event: Any) -> dict:
        return await self._store.put(**event)

    async def put_batch(self, events: list[dict]) -> list[dict]:
        return await self._store.put_batch(events)

    async def put_if_absent(self, **event: Any) -> tuple[dict, bool]:
        return await self._store.put_if_absent(**event)


__all__ = [
    "AdministrativeRunEventAppender",
    "FencedRunEventAppender",
    "RuntimeEventAuthority",
    "RuntimeEventOwnershipLost",
]

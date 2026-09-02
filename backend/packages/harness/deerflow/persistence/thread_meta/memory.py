"""In-memory ThreadMetaStore backed by LangGraph BaseStore.

Used when database.backend=memory. Delegates to the LangGraph Store's
``("threads",)`` namespace — the same namespace used by the Gateway
router for thread records.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from langgraph.store.base import BaseStore

from deerflow.persistence.json_compat import json_value_matches
from deerflow.persistence.thread_meta.base import THREAD_PINNED_METADATA_KEY, ThreadMetaAlreadyExistsError, ThreadMetaRunProjection, ThreadMetaStore
from deerflow.runtime.user_context import AUTO, _AutoSentinel, resolve_user_id
from deerflow.utils.time import coerce_iso, now_iso

THREADS_NS: tuple[str, ...] = ("threads",)
SEARCH_PAGE_SIZE = 500

if TYPE_CHECKING:
    from deerflow.runtime.runs.store.base import RunStore


class MemoryThreadMetaStore(ThreadMetaStore):
    def __init__(self, store: BaseStore, *, run_store: RunStore | None = None) -> None:
        self._store = store
        self._run_store = run_store
        self._ownership_lock = asyncio.Lock()

    async def _get_owned_record(
        self,
        thread_id: str,
        user_id: str | None | _AutoSentinel,
        method_name: str,
    ) -> dict | None:
        """Fetch a record and verify ownership. Returns a mutable copy, or None."""
        resolved = resolve_user_id(user_id, method_name=method_name)
        item = await self._store.aget(THREADS_NS, thread_id)
        if item is None:
            return None
        record = dict(item.value)
        if resolved is not None and record.get("user_id") != resolved:
            return None
        return record

    async def create(
        self,
        thread_id: str,
        *,
        assistant_id: str | None = None,
        user_id: str | None | _AutoSentinel = AUTO,
        display_name: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        resolved_user_id = resolve_user_id(user_id, method_name="MemoryThreadMetaStore.create")
        async with self._ownership_lock:
            if await self._store.aget(THREADS_NS, thread_id) is not None:
                raise ThreadMetaAlreadyExistsError("thread metadata already exists")
            now = now_iso()
            record: dict[str, Any] = {
                "thread_id": thread_id,
                "assistant_id": assistant_id,
                "user_id": resolved_user_id,
                "display_name": display_name,
                "status": "idle",
                "metadata": metadata or {},
                "values": {},
                "created_at": now,
                "updated_at": now,
            }
            await self._store.aput(THREADS_NS, thread_id, record)
            return record

    async def get(self, thread_id: str, *, user_id: str | None | _AutoSentinel = AUTO) -> dict | None:
        return await self._get_owned_record(thread_id, user_id, "MemoryThreadMetaStore.get")

    async def search(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> list[dict[str, Any]]:
        """Search threads by materializing matches, then sorting in Python.

        The memory backend loads all matching rows in chunks before slicing so
        it can mirror SQL's pinned-first ordering. Use the SQL store for
        scalable paginated I/O.
        """
        resolved_user_id = resolve_user_id(user_id, method_name="MemoryThreadMetaStore.search")
        filter_dict: dict[str, Any] = {}
        if status:
            filter_dict["status"] = status
        if resolved_user_id is not None:
            filter_dict["user_id"] = resolved_user_id

        items = []
        search_offset = 0
        while True:
            page = await self._store.asearch(
                THREADS_NS,
                filter=filter_dict or None,
                limit=SEARCH_PAGE_SIZE,
                offset=search_offset,
            )
            if not page:
                break
            items.extend(page)
            if len(page) < SEARCH_PAGE_SIZE:
                break
            search_offset += len(page)

        records = [self._item_to_dict(item) for item in items]
        if metadata:
            records = [record for record in records if isinstance(record.get("metadata"), dict) and all(json_value_matches(record["metadata"], key, value) for key, value in metadata.items())]
        records.sort(key=self._sort_key, reverse=True)
        return records[offset : offset + limit]

    async def check_access(self, thread_id: str, user_id: str, *, require_existing: bool = False) -> bool:
        item = await self._store.aget(THREADS_NS, thread_id)
        if item is None:
            return not require_existing
        record_user_id = item.value.get("user_id")
        if record_user_id is None:
            return True
        return record_user_id == user_id

    async def update_display_name(
        self,
        thread_id: str,
        display_name: str,
        *,
        remove_metadata_keys: tuple[str, ...] = (),
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> None:
        async with self._ownership_lock:
            record = await self._get_owned_record(thread_id, user_id, "MemoryThreadMetaStore.update_display_name")
            if record is None:
                return
            record["display_name"] = display_name
            metadata = dict(record.get("metadata") or {})
            for key in remove_metadata_keys:
                metadata.pop(key, None)
            record["metadata"] = metadata
            record["updated_at"] = now_iso()
            await self._store.aput(THREADS_NS, thread_id, record)

    async def update_status(self, thread_id: str, status: str, *, user_id: str | None | _AutoSentinel = AUTO) -> None:
        async with self._ownership_lock:
            record = await self._get_owned_record(thread_id, user_id, "MemoryThreadMetaStore.update_status")
            if record is None:
                return
            record["status"] = status
            record["updated_at"] = now_iso()
            await self._store.aput(THREADS_NS, thread_id, record)

    async def project_run(
        self,
        projection: ThreadMetaRunProjection,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> bool:
        """Apply a process-local projection only under durable run authority."""

        if self._run_store is None:
            return False
        async with self._ownership_lock:
            async with self._run_store.hold_thread_projection_authority(
                run_id=projection.run_id,
                thread_id=projection.thread_id,
                owner_worker_id=projection.owner_worker_id,
                active_state_version=projection.active_state_version,
                terminal_state_version=projection.terminal_state_version,
                run_status=projection.run_status,
            ) as authorized:
                if not authorized:
                    return False
                record = await self._get_owned_record(
                    projection.thread_id,
                    user_id,
                    "MemoryThreadMetaStore.project_run",
                )
                if record is None:
                    return False
                if projection.display_name is not None:
                    record["display_name"] = projection.display_name
                record["status"] = projection.status
                record["updated_at"] = now_iso()
                await self._store.aput(
                    THREADS_NS,
                    projection.thread_id,
                    record,
                )
                return True

    async def update_metadata(self, thread_id: str, metadata: dict, *, touch: bool = True, user_id: str | None | _AutoSentinel = AUTO) -> None:
        async with self._ownership_lock:
            record = await self._get_owned_record(thread_id, user_id, "MemoryThreadMetaStore.update_metadata")
            if record is None:
                return
            merged = dict(record.get("metadata") or {})
            merged.update(metadata)
            record["metadata"] = merged
            if touch:
                record["updated_at"] = now_iso()
            await self._store.aput(THREADS_NS, thread_id, record)

    async def update_owner(self, thread_id: str, owner_user_id: str, *, user_id: str | None | _AutoSentinel = AUTO) -> None:
        async with self._ownership_lock:
            record = await self._get_owned_record(thread_id, user_id, "MemoryThreadMetaStore.update_owner")
            if record is None:
                return
            record["user_id"] = owner_user_id
            record["updated_at"] = now_iso()
            await self._store.aput(THREADS_NS, thread_id, record)

    async def claim_unowned(self, thread_id: str, owner_user_id: str) -> bool:
        """Assign an owner only while the process-local row is unowned."""
        async with self._ownership_lock:
            item = await self._store.aget(THREADS_NS, thread_id)
            if item is None or item.value.get("user_id") is not None:
                return False
            record = dict(item.value)
            record["user_id"] = owner_user_id
            record["updated_at"] = now_iso()
            await self._store.aput(THREADS_NS, thread_id, record)
            return True

    async def delete(self, thread_id: str, *, user_id: str | None | _AutoSentinel = AUTO) -> None:
        async with self._ownership_lock:
            record = await self._get_owned_record(thread_id, user_id, "MemoryThreadMetaStore.delete")
            if record is None:
                return
            await self._store.adelete(THREADS_NS, thread_id)

    @staticmethod
    def _item_to_dict(item) -> dict[str, Any]:
        """Convert a Store SearchItem to the dict format expected by callers."""
        val = item.value
        return {
            "thread_id": item.key,
            "assistant_id": val.get("assistant_id"),
            "user_id": val.get("user_id"),
            "display_name": val.get("display_name"),
            "status": val.get("status", "idle"),
            "metadata": val.get("metadata", {}),
            # ``coerce_iso`` heals legacy unix-second values written by
            # earlier Gateway versions that called ``str(time.time())``.
            "created_at": coerce_iso(val.get("created_at", "")),
            "updated_at": coerce_iso(val.get("updated_at", "")),
        }

    @staticmethod
    def _sort_key(record: dict[str, Any]) -> tuple[bool, str, str]:
        metadata = record.get("metadata")
        pinned = isinstance(metadata, dict) and metadata.get(THREAD_PINNED_METADATA_KEY) is True
        return (pinned, str(record.get("updated_at") or ""), str(record.get("thread_id") or ""))

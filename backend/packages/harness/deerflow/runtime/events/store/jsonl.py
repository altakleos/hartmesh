"""JSONL file-backed RunEventStore implementation.

Each run's events are stored in a single file:
``.deer-flow/threads/{thread_id}/runs/{run_id}.jsonl``

All categories (message, trace, lifecycle) are in the same file.
This backend is suitable for lightweight single-node deployments.

**Single-process guarantee**: the in-memory seq counter is process-local.
Multi-process deployments sharing the same directory will produce duplicate
or non-monotonic seq values. Use ``DbRunEventStore`` for multi-process or
high-concurrency deployments.

Bulk file I/O is offloaded to a thread pool via ``asyncio.to_thread``.
Per-thread ``asyncio.Lock`` objects serialise writes within a single process
to prevent interleaved JSONL lines. Fenced writes prepare a complete
replacement off-loop, then hold the run-store execution fence until the
off-loop atomic rename has finished, including when the caller is cancelled.

Known trade-off: ``list_messages()`` must scan all run files for a
thread since messages from multiple runs need unified seq ordering.
``list_events()`` reads only one file -- the fast path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.events.appender import RuntimeEventAuthority, RuntimeEventOwnershipLost
from deerflow.runtime.events.store.base import AppendOutcome, RunEventStore, resolve_owned_run, validate_idempotent_append
from deerflow.runtime.tool_evidence import (
    TOOL_RECEIPT_CATEGORY,
    TOOL_RECEIPT_STARTED_EVENT,
    DurableToolReceiptV1,
    ToolReceiptIntegrityError,
    ToolReceiptOwnershipLost,
    canonical_digest,
    parse_tool_receipt_event,
    receipt_event_metadata,
    require_started_transition,
    require_tool_attempt_binding_fence,
    reserve_attempt_from_events,
)
from deerflow.runtime.user_context import AUTO, _AutoSentinel
from deerflow.utils.thread_id import validate_thread_id

logger = logging.getLogger(__name__)

_SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
MAX_JSONL_RECEIPT_DEDUPE_ENTRIES = 1024


class JsonlRunEventStore(RunEventStore):
    def __init__(
        self,
        base_dir: str | Path | None = None,
        *,
        run_store: object | None = None,
        tenant: TenantReferenceV1 | None = None,
    ):
        if tenant is not None and not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")
        self._base_dir = Path(base_dir) if base_dir else Path(".deer-flow")
        self._run_store = run_store
        self._tenant = tenant
        self._seq_counters: dict[str, int] = {}  # thread_id -> current max seq
        # Per-thread asyncio.Lock — serialises concurrent writes within one process.
        self._write_locks: dict[str, asyncio.Lock] = {}
        # Best-effort LRU acceleration only. Correctness always falls back to
        # scanning the run file while holding its thread write lock.
        self._dedupe_index: OrderedDict[tuple[str, str, str], dict] = OrderedDict()

    def _tenant_visible(self, event: dict) -> bool:
        return self._tenant is None or event.get("tenant_digest") == self._tenant.digest

    def _cache_dedupe(self, key: tuple[str, str, str], event: dict) -> None:
        self._dedupe_index[key] = event
        self._dedupe_index.move_to_end(key)
        while len(self._dedupe_index) > MAX_JSONL_RECEIPT_DEDUPE_ENTRIES:
            self._dedupe_index.popitem(last=False)

    def _get_write_lock(self, thread_id: str) -> asyncio.Lock:
        return self._write_locks.setdefault(thread_id, asyncio.Lock())

    @staticmethod
    def _validate_id(value: str, label: str) -> str:
        """Validate that an ID is safe for use in filesystem paths."""
        if not value or not _SAFE_ID_PATTERN.match(value):
            raise ValueError(f"Invalid {label}: must be alphanumeric/dash/underscore, got {value!r}")
        return value

    def _thread_dir(self, thread_id: str) -> Path:
        validate_thread_id(thread_id)
        return self._base_dir / "threads" / thread_id / "runs"

    def _run_file(self, thread_id: str, run_id: str) -> Path:
        self._validate_id(run_id, "run_id")
        return self._thread_dir(thread_id) / f"{run_id}.jsonl"

    def _next_seq(self, thread_id: str) -> int:
        self._seq_counters[thread_id] = self._seq_counters.get(thread_id, 0) + 1
        return self._seq_counters[thread_id]

    def _compute_max_seq(self, thread_id: str) -> int:
        """Scan all run files for a thread and return the current max seq (blocking I/O)."""
        max_seq = 0
        thread_dir = self._thread_dir(thread_id)
        if thread_dir.exists():
            for f in thread_dir.glob("*.jsonl"):
                for line in f.read_text(encoding="utf-8").strip().splitlines():
                    try:
                        record = json.loads(line)
                        max_seq = max(max_seq, record.get("seq", 0))
                    except json.JSONDecodeError:
                        logger.debug("Skipping malformed JSONL line in %s", f)
        return max_seq

    async def _ensure_seq_loaded(self, thread_id: str) -> None:
        """Load max seq from existing files into the in-memory counter (non-blocking)."""
        if thread_id in self._seq_counters:
            return
        max_seq = await asyncio.to_thread(self._compute_max_seq, thread_id)
        self._seq_counters[thread_id] = max_seq

    def _write_record(self, record: dict) -> None:
        path = self._run_file(record["thread_id"], record["run_id"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")

    def _prepare_record_replace(self, record: dict) -> tuple[Path, Path]:
        """Prepare an fsynced whole-file replacement without committing it."""

        return self._prepare_records_replace([record])

    def _prepare_records_replace(self, records: list[dict]) -> tuple[Path, Path]:
        """Prepare an fsynced whole-file replacement for one run."""

        if not records:
            raise ValueError("records must not be empty")
        identity = {(record["thread_id"], record["run_id"]) for record in records}
        if len(identity) != 1:
            raise ValueError("prepared records must belong to one run")
        record = records[0]
        target = self._run_file(record["thread_id"], record["run_id"])
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
        )
        temp_path = Path(temp_name)
        try:
            with os.fdopen(descriptor, "wb") as destination:
                if target.exists():
                    with target.open("rb") as source:
                        shutil.copyfileobj(source, destination)
                for item in records:
                    destination.write((json.dumps(item, default=str, ensure_ascii=False) + "\n").encode("utf-8"))
                destination.flush()
                os.fsync(destination.fileno())
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temp_path.unlink(missing_ok=True)
            raise
        return target, temp_path

    @staticmethod
    def _commit_prepared_record(target: Path, temp_path: Path) -> None:
        """Atomically publish one already-prepared receipt record."""

        os.replace(temp_path, target)

    @staticmethod
    async def _finish_off_thread(
        function,
        /,
        *args,
        _on_success=None,
        **kwargs,
    ):
        """Finish one blocking mutation before propagating cancellation.

        Cancelling ``asyncio.to_thread`` only cancels the awaiter; its worker
        thread keeps running.  A fenced publication therefore must drain the
        worker while the database execution fence remains held.
        """

        worker = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        cancellation: asyncio.CancelledError | None = None
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError as exc:
                cancellation = exc
        result = worker.result()
        if _on_success is not None:
            _on_success()
        if cancellation is not None:
            raise cancellation
        return result

    @staticmethod
    async def _prepare_replace_off_thread(
        function,
        /,
        *args,
    ) -> tuple[Path, Path]:
        """Drain preparation and remove its temp file before cancellation.

        Preparation returns the temp-file identity needed for cleanup. A raw
        ``asyncio.to_thread`` await loses that result when its caller is
        cancelled even though the worker thread continues running.
        """

        worker = asyncio.create_task(asyncio.to_thread(function, *args))
        cancellation: asyncio.CancelledError | None = None
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError as exc:
                cancellation = exc
        try:
            target, temp_path = worker.result()
        except BaseException:
            if cancellation is not None:
                raise cancellation
            raise
        if cancellation is not None:
            try:
                await JsonlRunEventStore._finish_off_thread(
                    temp_path.unlink,
                    missing_ok=True,
                )
            except asyncio.CancelledError as exc:
                cancellation = exc
            raise cancellation
        return target, temp_path

    async def _commit_under_execution_fence(
        self,
        target: Path,
        temp_path: Path,
        *,
        run_id: str,
        owner_id: str,
        lease_epoch: int,
        ownership_error: type[Exception],
        committed_thread_id: str,
        committed_seq: int,
        allowed_active_statuses: tuple[str, ...] = ("running",),
    ) -> None:
        holder = getattr(self._run_store, "hold_execution_fence", None)
        if not callable(holder):
            raise ownership_error("runtime_event_store_unfenced")
        async with holder(
            run_id,
            owner_worker_id=owner_id,
            state_version=lease_epoch,
            allowed_active_statuses=allowed_active_statuses,
        ) as active:
            if not active:
                raise ownership_error("runtime_event_ownership_lost")

            def advance_committed_sequence() -> None:
                self._seq_counters[committed_thread_id] = max(
                    self._seq_counters.get(committed_thread_id, 0),
                    committed_seq,
                )

            await self._finish_off_thread(
                self._commit_prepared_record,
                target,
                temp_path,
                _on_success=advance_committed_sequence,
            )

    async def _publish_fenced_record(
        self,
        record: dict,
        *,
        owner_id: str,
        lease_epoch: int,
    ) -> None:
        """Prepare and publish off-loop while retaining the execution fence."""

        target, temp_path = await self._prepare_replace_off_thread(
            self._prepare_record_replace,
            record,
        )
        try:
            run = await resolve_owned_run(
                self._run_store,
                record["run_id"],
                owner_id=owner_id,
                lease_epoch=lease_epoch,
            )
            if run["thread_id"] != record["thread_id"]:
                raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")
            await self._commit_under_execution_fence(
                target,
                temp_path,
                run_id=record["run_id"],
                owner_id=owner_id,
                lease_epoch=lease_epoch,
                ownership_error=ToolReceiptOwnershipLost,
                committed_thread_id=record["thread_id"],
                committed_seq=record["seq"],
            )
        except BaseException:
            await self._finish_off_thread(temp_path.unlink, missing_ok=True)
            raise

    def _read_thread_events(self, thread_id: str) -> list[dict]:
        """Read all events for a thread, sorted by seq (blocking I/O)."""
        events = []
        thread_dir = self._thread_dir(thread_id)
        if not thread_dir.exists():
            return events
        for f in sorted(thread_dir.glob("*.jsonl")):
            for line in f.read_text(encoding="utf-8").strip().splitlines():
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.debug("Skipping malformed JSONL line in %s", f)
        events.sort(key=lambda e: e.get("seq", 0))
        return events

    def _read_run_events(self, thread_id: str, run_id: str) -> list[dict]:
        """Read events for a specific run file (blocking I/O)."""
        path = self._run_file(thread_id, run_id)
        if not path.exists():
            return []
        events = []
        for line in path.read_text(encoding="utf-8").strip().splitlines():
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                logger.debug("Skipping malformed JSONL line in %s", path)
        events.sort(key=lambda e: e.get("seq", 0))
        return events

    def _delete_thread_files(self, thread_id: str) -> None:
        thread_dir = self._thread_dir(thread_id)
        if thread_dir.exists():
            for f in thread_dir.glob("*.jsonl"):
                f.unlink()

    def _delete_run_file(self, thread_id: str, run_id: str) -> None:
        path = self._run_file(thread_id, run_id)
        if path.exists():
            path.unlink()

    def _replace_run_events(self, thread_id: str, run_id: str, events: list[dict]) -> None:
        path = self._run_file(thread_id, run_id)
        if not events:
            path.unlink(missing_ok=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temp_path = Path(temp_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
                for event in events:
                    destination.write(json.dumps(event, default=str, ensure_ascii=False) + "\n")
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temp_path, path)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise

    async def put(self, *, thread_id, run_id, event_type, category, content="", metadata=None, created_at=None):
        async with self._get_write_lock(thread_id):
            await self._ensure_seq_loaded(thread_id)
            seq = self._next_seq(thread_id)
            record = {
                "thread_id": thread_id,
                "run_id": run_id,
                "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
                "tenant_digest": None if self._tenant is None else self._tenant.digest,
                "event_type": event_type,
                "category": category,
                "content": content,
                "metadata": metadata or {},
                "seq": seq,
                "created_at": created_at or datetime.now(UTC).isoformat(),
            }
            await asyncio.to_thread(self._write_record, record)
            return record

    async def put_batch(self, events):
        """Persist a batch of events under a per-thread write lock.

        All seq numbers for the batch are reserved under a single per-thread
        write lock. Records are grouped by run_id and appended to their own
        run files while that lock is held. If a write fails and rollback
        succeeds, already-appended groups for the current thread are restored
        so callers (e.g. worker.py's flush-retry path) may safely re-buffer
        that thread's batch. When a batch contains multiple thread IDs, thread
        groups are processed sequentially, so a later failure does not roll
        back earlier thread groups. This rollback does not make a multi-file
        batch crash-atomic.
        """
        if not events:
            return []

        # Group by thread_id; each thread has its own write lock and seq counter.
        by_thread: dict[str, list[dict[str, Any]]] = {}
        for ev in events:
            by_thread.setdefault(ev["thread_id"], []).append(ev)

        results: list[dict[str, Any]] = []
        for thread_id, batch in by_thread.items():
            records = await self._write_batch_async(thread_id, batch)
            results.extend(records)
        return results

    async def put_if_absent(
        self,
        *,
        thread_id,
        run_id,
        event_type,
        category,
        content="",
        metadata=None,
        created_at=None,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        async with self._get_write_lock(thread_id):
            existing = await asyncio.to_thread(self._read_run_events, thread_id, run_id)
            for event in existing:
                if self._tenant_visible(event) and event.get("event_type") == event_type:
                    return event, False
            await self._ensure_seq_loaded(thread_id)
            record = {
                "thread_id": thread_id,
                "run_id": run_id,
                "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
                "tenant_digest": None if self._tenant is None else self._tenant.digest,
                "event_type": event_type,
                "category": category,
                "content": content,
                "metadata": metadata or {},
                "seq": self._next_seq(thread_id),
                "created_at": created_at or datetime.now(UTC).isoformat(),
            }
            if not isinstance(user_id, _AutoSentinel):
                record["user_id"] = user_id
            await asyncio.to_thread(self._write_record, record)
            return record, True

    async def _require_runtime_authority(
        self,
        authority: RuntimeEventAuthority,
    ) -> dict:
        configured_digest = None if self._tenant is None else self._tenant.digest
        if authority.tenant_digest != configured_digest:
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")
        try:
            run = await resolve_owned_run(
                self._run_store,
                authority.run_id,
                owner_id=authority.owner_id,
                lease_epoch=authority.lease_epoch,
                allowed_statuses=("pending", "running"),
            )
        except Exception:
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost") from None
        if run.get("thread_id") != authority.thread_id or run.get("tenant_digest") != authority.tenant_digest:
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")
        return run

    def _runtime_records(
        self,
        authority: RuntimeEventAuthority,
        events: list[dict],
    ) -> list[dict]:
        next_seq = self._seq_counters[authority.thread_id]
        records: list[dict] = []
        for event in events:
            authority.require_event_identity(event)
            next_seq += 1
            records.append(
                {
                    "thread_id": authority.thread_id,
                    "run_id": authority.run_id,
                    "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
                    "tenant_digest": None if self._tenant is None else self._tenant.digest,
                    "event_type": event["event_type"],
                    "category": event.get("category", "trace"),
                    "content": event.get("content", ""),
                    "metadata": event.get("metadata") or {},
                    "seq": next_seq,
                    "created_at": event.get("created_at") or datetime.now(UTC).isoformat(),
                }
            )
        return records

    async def _publish_runtime_records(
        self,
        authority: RuntimeEventAuthority,
        records: list[dict],
    ) -> None:
        target, temp_path = await self._prepare_replace_off_thread(
            self._prepare_records_replace,
            records,
        )
        try:
            await self._require_runtime_authority(authority)
            await self._commit_under_execution_fence(
                target,
                temp_path,
                run_id=authority.run_id,
                owner_id=authority.owner_id,
                lease_epoch=authority.lease_epoch,
                ownership_error=RuntimeEventOwnershipLost,
                committed_thread_id=authority.thread_id,
                committed_seq=records[-1]["seq"],
                allowed_active_statuses=("pending", "running"),
            )
        except BaseException:
            await self._finish_off_thread(temp_path.unlink, missing_ok=True)
            raise

    async def append_fenced_batch(
        self,
        authority: RuntimeEventAuthority,
        events: list[dict],
    ) -> list[dict]:
        if not events:
            return []
        async with self._get_write_lock(authority.thread_id):
            await self._require_runtime_authority(authority)
            await self._ensure_seq_loaded(authority.thread_id)
            records = self._runtime_records(authority, events)
            await self._publish_runtime_records(authority, records)
            return records

    async def append_fenced_if_absent(
        self,
        authority: RuntimeEventAuthority,
        event: dict,
    ) -> tuple[dict, bool]:
        authority.require_event_identity(event)
        async with self._get_write_lock(authority.thread_id):
            await self._require_runtime_authority(authority)
            persisted = [
                item
                for item in await asyncio.to_thread(
                    self._read_run_events,
                    authority.thread_id,
                    authority.run_id,
                )
                if self._tenant_visible(item)
            ]
            existing = next(
                (item for item in persisted if item.get("event_type") == event["event_type"]),
                None,
            )
            if existing is not None:
                await self._require_runtime_authority(authority)
                return existing, False
            await self._ensure_seq_loaded(authority.thread_id)
            [record] = self._runtime_records(authority, [event])
            await self._publish_runtime_records(authority, [record])
            return record, True

    async def append_idempotent(
        self,
        run_id,
        *,
        event_type,
        idempotency_key,
        body,
        owner_id,
        lease_epoch,
    ) -> AppendOutcome:
        detached = validate_idempotent_append(
            event_type=event_type,
            idempotency_key=idempotency_key,
            body=body,
        )
        # Resolve once to find the lock, then verify again while holding it so
        # a local ownership transition cannot race the append boundary.
        run = await resolve_owned_run(
            self._run_store,
            run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        thread_id = run["thread_id"]
        async with self._get_write_lock(thread_id):
            await resolve_owned_run(
                self._run_store,
                run_id,
                owner_id=owner_id,
                lease_epoch=lease_epoch,
            )
            persisted = [event for event in await asyncio.to_thread(self._read_run_events, thread_id, run_id) if self._tenant_visible(event)]
            key = (run_id, event_type, idempotency_key)
            existing = self._dedupe_index.get(key)
            if existing is None:
                existing = next(
                    (event for event in persisted if event.get("event_type") == event_type and event.get("idempotency_key") == idempotency_key),
                    None,
                )
            if existing is not None and not self._tenant_visible(existing):
                existing = None
            if existing is not None:
                if canonical_digest(existing.get("content")) != canonical_digest(detached):
                    raise ToolReceiptIntegrityError("receipt_idempotency_conflict")
                parse_tool_receipt_event(existing)
                await resolve_owned_run(
                    self._run_store,
                    run_id,
                    owner_id=owner_id,
                    lease_epoch=lease_epoch,
                )
                self._cache_dedupe(key, existing)
                return AppendOutcome(event=existing, created=False)
            await self._ensure_seq_loaded(thread_id)
            receipt = DurableToolReceiptV1.from_event_body(detached, occurred_at=datetime.now(UTC))
            if receipt.phase != "started":
                require_started_transition(
                    persisted,
                    receipt,
                )
            next_seq = self._seq_counters[thread_id] + 1
            record = {
                "thread_id": thread_id,
                "run_id": run_id,
                "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
                "tenant_digest": None if self._tenant is None else self._tenant.digest,
                "event_type": event_type,
                "category": TOOL_RECEIPT_CATEGORY,
                "content": detached,
                "metadata": receipt_event_metadata(
                    receipt,
                    writer_owner_id=owner_id,
                    writer_lease_epoch=lease_epoch,
                ),
                "idempotency_key": idempotency_key,
                "seq": next_seq,
                "created_at": datetime.now(UTC).isoformat(),
            }
            await self._publish_fenced_record(
                record,
                owner_id=owner_id,
                lease_epoch=lease_epoch,
            )
            self._cache_dedupe(key, record)
            return AppendOutcome(event=record, created=True)

    async def reserve_tool_attempt(
        self,
        run_id,
        *,
        binding,
        tool_call_id,
        tool_name,
        request_projection_digest,
        observed_node_attempt,
        expected_attempt,
        owner_id,
        lease_epoch,
    ) -> AppendOutcome:
        binding = require_tool_attempt_binding_fence(
            binding,
            run_id=run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        run = await resolve_owned_run(
            self._run_store,
            run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        thread_id = run["thread_id"]
        async with self._get_write_lock(thread_id):
            await resolve_owned_run(
                self._run_store,
                run_id,
                owner_id=owner_id,
                lease_epoch=lease_epoch,
            )
            events = [event for event in await asyncio.to_thread(self._read_run_events, thread_id, run_id) if self._tenant_visible(event)]
            receipt, existing, terminal = reserve_attempt_from_events(
                events,
                binding=binding,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                request_projection_digest=request_projection_digest,
                observed_node_attempt=observed_node_attempt,
                expected_attempt=expected_attempt,
            )
            if existing is not None:
                await resolve_owned_run(
                    self._run_store,
                    run_id,
                    owner_id=owner_id,
                    lease_epoch=lease_epoch,
                )
                return AppendOutcome(
                    event=dict(existing),
                    created=False,
                    terminal_event=(dict(terminal) if terminal is not None else None),
                )
            body = validate_idempotent_append(
                event_type=TOOL_RECEIPT_STARTED_EVENT,
                idempotency_key=receipt.idempotency_key,
                body=receipt.to_event_body(),
            )
            await self._ensure_seq_loaded(thread_id)
            next_seq = self._seq_counters[thread_id] + 1
            record = {
                "thread_id": thread_id,
                "run_id": run_id,
                "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
                "tenant_digest": None if self._tenant is None else self._tenant.digest,
                "event_type": TOOL_RECEIPT_STARTED_EVENT,
                "category": TOOL_RECEIPT_CATEGORY,
                "content": body,
                "metadata": receipt_event_metadata(
                    receipt,
                    writer_owner_id=owner_id,
                    writer_lease_epoch=lease_epoch,
                ),
                "idempotency_key": receipt.idempotency_key,
                "seq": next_seq,
                "created_at": datetime.now(UTC).isoformat(),
            }
            await self._publish_fenced_record(
                record,
                owner_id=owner_id,
                lease_epoch=lease_epoch,
            )
            self._cache_dedupe(
                (run_id, TOOL_RECEIPT_STARTED_EVENT, receipt.idempotency_key),
                record,
            )
            return AppendOutcome(event=record, created=True)

    async def _write_batch_async(self, thread_id: str, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        async with self._get_write_lock(thread_id):
            await self._ensure_seq_loaded(thread_id)
            records: list[dict[str, Any]] = []
            for ev in batch:
                seq = self._next_seq(thread_id)
                record = {
                    "thread_id": thread_id,
                    "run_id": ev["run_id"],
                    "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
                    "tenant_digest": None if self._tenant is None else self._tenant.digest,
                    "event_type": ev["event_type"],
                    "category": ev["category"],
                    "content": ev.get("content", ""),
                    "metadata": ev.get("metadata") or {},
                    "seq": seq,
                    "created_at": ev.get("created_at") or datetime.now(UTC).isoformat(),
                }
                records.append(record)
            records_by_run: dict[str, list[dict[str, Any]]] = {}
            for record in records:
                records_by_run.setdefault(record["run_id"], []).append(record)
            run_batches = [(self._run_file(thread_id, run_id), run_records) for run_id, run_records in records_by_run.items()]
            await asyncio.to_thread(self._append_record_groups, run_batches)
            return records

    def _append_records(self, path: Path, records: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = "".join(json.dumps(r, default=str, ensure_ascii=False) + "\n" for r in records)
        with open(path, "a", encoding="utf-8") as f:
            f.write(lines)

    def _append_record_groups(self, groups: list[tuple[Path, list[dict[str, Any]]]]) -> None:
        """Append run groups and restore their original sizes if one fails."""
        original_sizes: dict[Path, int | None] = {}
        try:
            for path, records in groups:
                original_sizes[path] = path.stat().st_size if path.exists() else None
                self._append_records(path, records)
        except Exception:
            for path, original_size in original_sizes.items():
                try:
                    if original_size is None:
                        if path.exists():
                            path.unlink()
                    else:
                        with open(path, "r+b") as f:
                            f.truncate(original_size)
                except OSError:
                    logger.error(
                        "Failed to roll back JSONL batch append for %s; retrying the batch may create duplicate records",
                        path,
                        exc_info=True,
                    )
            raise

    async def list_messages(self, thread_id, *, limit=50, before_seq=None, after_seq=None, user_id: str | None | _AutoSentinel = AUTO):
        all_events = await asyncio.to_thread(self._read_thread_events, thread_id)
        messages = [event for event in all_events if self._tenant_visible(event) and event.get("category") == "message"]

        if before_seq is not None:
            messages = [e for e in messages if e["seq"] < before_seq]
            return messages[-limit:]
        elif after_seq is not None:
            messages = [e for e in messages if e["seq"] > after_seq]
            return messages[:limit]
        else:
            return messages[-limit:]

    async def list_events(self, thread_id, run_id, *, event_types=None, task_id=None, limit=500, after_seq=None, user_id: str | None | _AutoSentinel = AUTO):
        events = [event for event in await asyncio.to_thread(self._read_run_events, thread_id, run_id) if self._tenant_visible(event)]
        if event_types is not None:
            events = [e for e in events if e.get("event_type") in event_types]
        if task_id is not None:
            events = [e for e in events if (e.get("metadata") or {}).get("task_id") == task_id]
        if after_seq is not None:
            events = [e for e in events if e.get("seq", 0) > after_seq]
        return events[:limit]

    async def list_messages_by_run(self, thread_id, run_id, *, limit=50, before_seq=None, after_seq=None):
        events = await asyncio.to_thread(self._read_run_events, thread_id, run_id)
        filtered = [event for event in events if self._tenant_visible(event) and event.get("category") == "message"]
        if before_seq is not None:
            filtered = [e for e in filtered if e.get("seq", 0) < before_seq]
        if after_seq is not None:
            filtered = [e for e in filtered if e.get("seq", 0) > after_seq]
        if after_seq is not None:
            return filtered[:limit]
        else:
            return filtered[-limit:] if len(filtered) > limit else filtered

    async def get_last_visible_ai_seq_by_run(self, thread_id, run_ids, *, user_id: str | None | _AutoSentinel = AUTO):
        def _scan() -> dict[str, int]:
            result: dict[str, int] = {}
            for run_id in run_ids:
                for event in reversed(self._read_run_events(thread_id, run_id)):
                    if not self._tenant_visible(event):
                        continue
                    caller = str((event.get("metadata") or {}).get("caller", ""))
                    if event.get("category") == "message" and event.get("event_type") in {"llm.ai.response", "ai_message"} and not caller.startswith("middleware:"):
                        result[run_id] = event["seq"]
                        break
            return result

        return await asyncio.to_thread(_scan)

    async def count_messages(self, thread_id):
        all_events = await asyncio.to_thread(self._read_thread_events, thread_id)
        return sum(1 for event in all_events if self._tenant_visible(event) and event.get("category") == "message")

    async def delete_by_thread(self, thread_id):
        async with self._get_write_lock(thread_id):
            all_events = await asyncio.to_thread(self._read_thread_events, thread_id)
            visible = [event for event in all_events if self._tenant_visible(event)]
            count = len(visible)
            run_ids = {event.get("run_id") for event in visible}
            for run_id in {event.get("run_id") for event in all_events}:
                if not isinstance(run_id, str):
                    continue
                run_events = [event for event in all_events if event.get("run_id") == run_id]
                remaining = [event for event in run_events if not self._tenant_visible(event)]
                await asyncio.to_thread(self._replace_run_events, thread_id, run_id, remaining)
            if not any(not self._tenant_visible(event) for event in all_events):
                self._seq_counters.pop(thread_id, None)
            self._dedupe_index = OrderedDict((key, value) for key, value in self._dedupe_index.items() if not (key[0] in run_ids and self._tenant_visible(value)))
            # Pop the lock inside the held scope to minimise the window where a new caller
            # could obtain a fresh lock while a waiting coroutine still holds the old one.
            # Note: coroutines that already acquired a reference to this lock before the
            # delete will still proceed after we release — this is an accepted narrow race.
            self._write_locks.pop(thread_id, None)
            return count

    async def delete_by_run(self, thread_id, run_id):
        async with self._get_write_lock(thread_id):
            events = await asyncio.to_thread(self._read_run_events, thread_id, run_id)
            visible = [event for event in events if self._tenant_visible(event)]
            remaining = [event for event in events if not self._tenant_visible(event)]
            count = len(visible)
            await asyncio.to_thread(self._replace_run_events, thread_id, run_id, remaining)
            self._dedupe_index = OrderedDict((key, value) for key, value in self._dedupe_index.items() if not (key[0] == run_id and self._tenant_visible(value)))
            return count

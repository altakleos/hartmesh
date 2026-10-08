"""SQL platform memory through DeerMem's portable storage port.

This is not a Home filesystem writer. Resource READ admission checks the
current audience and lifecycle; native Home writes keep their existing fence.
Model calls occur between transactions. Queued storage objects retain their
original epoch even when the updater reloads after a revision conflict.
"""

import asyncio
import copy
import json
import math
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import select

from deerflow.agent_instances.contract import AgentDenied, AgentPermission, InstanceIdentity
from deerflow.agent_instances.conversations import AgentExecution
from deerflow.agents.memory.backends.deermem.deermem.core.storage import (
    MemoryFactRevisionConflict,
    MemoryManifestRevisionConflict,
    MemoryStorage,
    MemoryStorageCorruption,
    _normalize_fact,
    create_empty_memory,
    normalize_memory_data,
    utc_now_iso_z,
)
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow, AgentMemoryRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.spaces.contract import Permission, SpaceDenied
from deerflow.utils.file_io import await_drained

MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
MAX_FACT_BYTES = 64 * 1024
MAX_FACTS = 10000
_replacement = ContextVar("instance_memory_replacement", default=None)


def _document(value, instance_id, *, incoming=False):
    """Validate canonical persisted data, including scope, without repairing it."""
    try:
        if not isinstance(value, dict) or len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")) > MAX_DOCUMENT_BYTES:
            raise ValueError("document size")
        if type(value.get("revision")) is not int or value["revision"] < 0:
            raise ValueError("revision")
        facts = value.get("facts")
        if not isinstance(facts, list) or len(facts) > MAX_FACTS:
            raise ValueError("facts")
        ids = set()
        for fact in facts:
            if not isinstance(fact, dict) or fact.get("scope") != {"instanceId": instance_id} or type(fact.get("revision")) is not int or fact["revision"] < 1:
                raise ValueError("fact scope or revision")
            if not isinstance(fact.get("content"), str) or not fact["content"].strip() or len(fact["content"].encode("utf-8")) > MAX_FACT_BYTES or fact["id"] in ids:
                raise ValueError("fact content or identity")
            _normalize_fact(fact, scope={"instanceId": instance_id})
            ids.add(fact["id"])
        for section in ("user", "history"):
            if not isinstance(value.get(section), dict):
                raise ValueError("summaries")
            for entry in value[section].values():
                if not isinstance(entry, dict) or not isinstance(entry.get("summary"), str) or len(entry["summary"].encode("utf-8")) > MAX_FACT_BYTES:
                    raise ValueError("summary")
        metadata = value.get("_meta", {})
        if not isinstance(metadata, dict) or not isinstance(metadata.get("usage", {}), dict) or not isinstance(metadata.get("evictions", []), list):
            raise ValueError("memory metadata")
        for key, entry in metadata.get("usage", {}).items():
            if (
                key not in ids
                or not isinstance(entry, dict)
                or type(entry.get("accessCount")) is not int
                or entry["accessCount"] < 0
                or not isinstance(entry.get("accessHeat"), (int, float))
                or not math.isfinite(entry["accessHeat"])
                or entry["accessHeat"] < 0
                or not isinstance(entry.get("lastAccessedAt"), str)
            ):
                raise ValueError("memory access metadata")
        return copy.deepcopy(value)
    except (ValueError, KeyError, TypeError, OverflowError) as exc:
        if incoming:
            raise ValueError("Invalid or oversized instance memory document") from exc
        raise MemoryStorageCorruption("Invalid instance memory document") from exc


@dataclass(frozen=True)
class MemoryScope:
    actor: object
    instance: InstanceIdentity
    home_generation: int
    epoch: int
    owner_loop: object
    agent_name: str
    execution: AgentExecution | None = None
    management_write: bool = False


class InstanceMemory:
    def __init__(self, agents):
        self.agents = agents
        self._managers = {}
        self._lock = threading.RLock()
        self._closed = False
        self._finished = set()
        self._building = {}
        self._closing = False

    async def bind(self, *, actor, instance_id, execution=None, management_write=False):
        needed = AgentPermission.INSPECT | (AgentPermission.MANAGE if management_write else AgentPermission(0))
        if execution is not None:
            if not isinstance(execution, AgentExecution) or execution.requester != actor or execution.instance.id != instance_id:
                raise AgentDenied("Memory requires its admitted instance execution")
            await execution.validate()
            needed |= AgentPermission.USE
        view = await self.agents.get(actor=actor, instance_id=instance_id, permission=needed)
        home = await self.agents.files.registry.get(actor=actor, space_id=view.home_id)
        identity = InstanceIdentity(**{key: getattr(view, key) for key in InstanceIdentity.__dataclass_fields__})
        async with self.agents._sf() as session:
            definition = await self.agents._stored_definition(session, view.definition_revision)
        scope = MemoryScope(actor, identity, home.generation, 0, asyncio.get_running_loop(), definition.config["name"].lower(), execution, management_write)
        async with self._admitted(scope, write=False, check_epoch=False) as (_session, row):
            scope = MemoryScope(actor, identity, home.generation, row.epoch, scope.owner_loop, scope.agent_name, execution, management_write)
        return SqlMemoryStorage(self, scope)

    @asynccontextmanager
    async def _admitted(self, scope, *, write, check_epoch=True):
        from deerflow.config.agents_config import AgentConfig

        agents = self.agents
        if self._closed:
            raise AgentDenied("The instance memory service is closing")
        # No filesystem WRITE/ADMIN claim: native memory is SQL state.
        try:
            async with agents.files.registry.admitted(actor=scope.actor, requests={scope.instance.home_id: (Permission.READ, scope.home_generation)}) as (session, homes):
                if scope.execution is not None:
                    thread = (await session.execute(select(ThreadMetaRow).where(ThreadMetaRow.thread_id == scope.execution.thread_id).with_for_update())).scalar_one_or_none()
                    binding = await session.get(AgentConversationRow, scope.execution.thread_id)
                    if thread is None or thread.incarnation != scope.execution.thread_incarnation or binding is None or binding.instance_id != scope.instance.id:
                        raise AgentDenied("Memory conversation authority changed")
                instance = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == scope.instance.id).with_for_update())).scalar_one_or_none()
                grant = await session.get(AgentInstanceGrantRow, (scope.instance.id, scope.actor.subject_id))
                needed = AgentPermission.INSPECT
                if scope.execution is not None:
                    needed |= AgentPermission.USE
                elif write:
                    needed |= AgentPermission.MANAGE
                try:
                    identity = InstanceIdentity.from_row(instance)
                except (AttributeError, TypeError, ValueError):
                    raise AgentDenied("Instance memory is unavailable") from None
                if identity != scope.instance or identity.status not in {"active", "suspended", "archived"}:
                    raise AgentDenied("Instance memory generation changed")
                if grant is None or not 1 <= grant.permissions <= 7 or grant.permissions & int(needed) != int(needed):
                    raise AgentDenied("Instance memory audience changed")
                await agents._human(scope.actor)
                if identity.custody == "personal":
                    await agents._human(type(scope.actor)("human", identity.owner_id))
                if scope.execution is not None:
                    if identity.status != "active" or homes[identity.home_id][0].status != "active" or identity.definition_revision != scope.execution.definition.revision:
                        raise AgentDenied("Instance memory execution is inactive")
                    if AgentConfig.model_validate(scope.execution.definition.config).memory_enabled is False:
                        raise AgentDenied("Instance memory is disabled by its definition")
                elif write and (not scope.management_write or identity.status != "active"):
                    raise AgentDenied("Instance memory mutation requires active management admission")
                row = await session.get(AgentMemoryRow, identity.id)
                if row is None:
                    row = AgentMemoryRow(instance_id=identity.id, epoch=1, document=create_empty_memory())
                    session.add(row)
                    await session.flush()
                if type(row.epoch) is not int or not 1 <= row.epoch <= 2**31 - 1:
                    raise MemoryStorageCorruption("Invalid instance memory epoch")
                _document(row.document, identity.id)
                if write and check_epoch and row.epoch != scope.epoch:
                    raise AgentDenied("Queued memory belongs to a retired epoch")
                yield session, row
        except SpaceDenied:
            raise AgentDenied("Instance memory Home audience is unavailable") from None

    def manager(self, execution, base):
        """Called off-loop. Keep a bounded set of independent run-bound queues."""
        from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem

        if type(base) is not DeerMem:
            raise NotImplementedError("The configured memory backend has no qualified instance scope")
        retiring = []
        key = id(execution)
        with self._lock:
            if self._closed or self._closing:
                raise AgentDenied("The instance memory service is closing")
            if key in self._managers:
                return self._managers[key][1]
            for old_key, (_execution, manager) in list(self._managers.items()):
                if old_key in self._finished and not manager._queue.pending_count and not manager._queue.is_processing:
                    del self._managers[old_key]
                    self._finished.discard(old_key)
                    retiring.append(manager)
            pending = self._building.get(key)
            creator = pending is None
            if creator and len(self._managers) + len(self._building) >= 1024:
                raise AgentDenied("Instance memory queue capacity is exhausted")
            if creator:
                pending = Future()
                self._building[key] = pending
        # Closing ports, host-loop admission and peer waits can never own the
        # mutex taken by synchronous finish() on the host event loop.
        for manager in retiring:
            manager.close()
        if not creator:
            return pending.result()
        try:
            storage = _bridge(execution.owner_loop, self.bind(actor=execution.requester, instance_id=execution.instance.id, execution=execution))
            manager = base.with_storage(storage)
            with self._lock:
                if self._closed:
                    raise AgentDenied("The instance memory service is closed")
                self._managers[key] = (execution, manager)
                self._building.pop(key)
            pending.set_result(manager)
            return manager
        except BaseException as exc:
            with self._lock:
                self._building.pop(key, None)
                self._finished.discard(key)
            pending.set_exception(exc)
            raise

    def finish(self, execution):
        with self._lock:
            if id(execution) in self._managers or id(execution) in self._building:
                self._finished.add(id(execution))

    def shutdown_flush(self, timeout):
        deadline = time.monotonic() + timeout
        with self._lock:
            self._closing = True
            building = list(self._building.values())
        completed = True
        for pending in building:
            try:
                pending.result(timeout=max(0, deadline - time.monotonic()))
            except FutureTimeoutError:
                completed = False
            except Exception:
                # A rejected construction owns no queue to flush.
                pass
        with self._lock:
            managers = list(self._managers.values())
        for _execution, manager in managers:
            completed = manager.shutdown_flush(max(0, deadline - time.monotonic())) and completed
        with self._lock:
            self._closed = True
        return completed


def _bridge(loop, coroutine):
    try:
        current = asyncio.get_running_loop()
    except RuntimeError:
        current = None
    if loop is None or not loop.is_running() or current is loop:
        coroutine.close()
        raise AgentDenied("Synchronous memory requires a live host loop and a worker thread")
    return asyncio.run_coroutine_threadsafe(coroutine, loop).result()


class SqlMemoryStorage(MemoryStorage):
    isolated_summaries = True

    def __init__(self, service, scope):
        self.service, self.scope = service, scope
        self._config = None

    def configure(self, config):
        self._config = config

    def _scope_args(self, agent_name, user_id):
        # These compatibility parameters cannot select another storage domain.
        if user_id not in (None, self.scope.actor.subject_id) or agent_name not in (None, self.scope.agent_name):
            raise AgentDenied("Memory arguments cannot change the admitted instance scope")

    def _call(self, *, write, operation):
        if self.scope.execution is not None:
            from deerflow.config.memory_config import get_memory_config

            if not execution_memory_enabled(self.scope.execution, get_memory_config()):
                raise AgentDenied("Instance memory is disabled")

        async def run():
            async with self.service._admitted(self.scope, write=write) as (_session, row):
                result = operation(row)
                if write:
                    _document(row.document, self.scope.instance.id)
                return copy.deepcopy(result)

        async def owned():
            return await await_drained(run())

        return _bridge(self.scope.owner_loop, owned())

    def before_extraction(self):
        self._call(write=True, operation=lambda _row: None)

    def load(self, agent_name=None, *, user_id=None):
        self._scope_args(agent_name, user_id)
        document = self._call(write=False, operation=lambda row: row.document)
        document.pop("_meta", None)
        if agent_name is None:
            document["facts"] = []
        return document

    def reload(self, agent_name=None, *, user_id=None):
        return self.load(agent_name, user_id=user_id)

    def apply_changes(self, change_set, *, agent_name=None, user_id=None, expected_manifest_revision=None, allow_manifest_rebase=False):
        self._scope_args(agent_name, user_id)
        changes = copy.deepcopy(change_set)

        def apply(row):
            document = copy.deepcopy(row.document)
            upserts, deletes = changes.get("upserts", []), changes.get("deletes", [])
            summaries = changes.get("summaries")
            if not isinstance(upserts, list) or not isinstance(deletes, list) or (upserts or deletes) and agent_name is None:
                raise ValueError("Invalid fact changes")
            if expected_manifest_revision is None or type(expected_manifest_revision) is not int:
                raise ValueError("A memory document revision is required")
            if document["revision"] != expected_manifest_revision and not (allow_manifest_rebase and summaries is None and (upserts or deletes)):
                raise MemoryManifestRevisionConflict("Instance memory revision changed")
            facts = {f["id"]: f for f in document["facts"]}
            for key in deletes:
                expected = changes.get("deleteRevisions", {}).get(key)
                if key not in facts or type(expected) is not int or facts[key]["revision"] != expected:
                    raise MemoryFactRevisionConflict("Fact deletion revision changed")
                del facts[key]
            for value in upserts:
                incoming = copy.deepcopy(value)
                key = incoming.get("id")
                expected = changes.get("upsertRevisions", {}).get(key)
                existing = facts.get(key)
                if (existing is None) != (expected is None) or existing is not None and existing["revision"] != expected:
                    raise MemoryFactRevisionConflict("Fact update revision changed")
                incoming["revision"] = expected or 1
                facts[key] = _normalize_fact(incoming, scope={"instanceId": self.scope.instance.id}, existing=existing)
            document["facts"] = list(facts.values())
            metadata = document.get("_meta", {})
            metadata["usage"] = {key: entry for key, entry in metadata.get("usage", {}).items() if key in facts}
            document["_meta"] = metadata
            if summaries is not None:
                document.update({key: copy.deepcopy(summaries[key]) for key in ("user", "history") if key in summaries})
            document["revision"] += 1
            document["lastUpdated"] = utc_now_iso_z()
            if _replacement.get() is self:
                if not self.scope.management_write or row.epoch >= 2**31 - 1:
                    raise AgentDenied("Memory replacement requires current management admission")
                row.epoch += 1
                document["_meta"] = {}
            row.document = _document(document, self.scope.instance.id, incoming=True)
            return document

        return self._call(write=True, operation=apply)

    def save(self, memory_data, agent_name=None, *, user_id=None, expected_revision=None):
        current = self.load(agent_name, user_id=user_id)
        if expected_revision is None:
            expected_revision = current["revision"]
        facts = current["facts"]
        incoming = normalize_memory_data(memory_data)
        self.apply_changes(
            {
                "upserts": incoming["facts"],
                "upsertRevisions": {f["id"]: next((old["revision"] for old in facts if old["id"] == f["id"]), None) for f in incoming["facts"]},
                "deletes": [f["id"] for f in facts if f["id"] not in {n["id"] for n in incoming["facts"]}],
                "deleteRevisions": {f["id"]: f["revision"] for f in facts},
                "summaries": {key: incoming[key] for key in ("user", "history")},
            },
            agent_name=agent_name,
            user_id=user_id,
            expected_manifest_revision=expected_revision,
        )
        return True

    def clear_all(self, *, user_id=None):
        self._scope_args(None, user_id)

        def clear(row):
            if row.epoch >= 2**31 - 1:
                raise AgentDenied("Memory epoch is exhausted")
            document = create_empty_memory()
            document["revision"] = row.document["revision"] + 1
            row.epoch += 1
            row.document = document
            return document

        return self._call(write=True, operation=clear)

    def replace_from_import(self, memory_data, *, agent_name, user_id, updater):
        """Validate/cap through the existing updater, then retire older jobs."""
        if len(json.dumps(memory_data, ensure_ascii=False, allow_nan=False).encode("utf-8")) > MAX_DOCUMENT_BYTES:
            raise ValueError("The instance memory import exceeds 4 MiB")
        token = _replacement.set(self)
        try:
            return updater.import_memory_data(memory_data, agent_name=agent_name, user_id=user_id, replace_shared_summaries=True)
        finally:
            _replacement.reset(token)

    def get_fact_usage(self, *, agent_name, user_id=None):
        self._scope_args(agent_name, user_id)
        return self._call(write=False, operation=lambda row: row.document.get("_meta", {}).get("usage", {}))

    def record_fact_accesses(self, fact_ids, *, agent_name, user_id=None, accessed_at=None):
        from datetime import UTC, datetime

        self._scope_args(agent_name, user_id)

        def record(row):
            document = copy.deepcopy(row.document)
            metadata = document.setdefault("_meta", {})
            usage = metadata.setdefault("usage", {})
            existing = {f["id"] for f in document["facts"]}
            now = accessed_at or datetime.now(UTC)
            for key in set(fact_ids) & existing:
                previous = usage.get(key, {})
                count = previous.get("accessCount", 0)
                heat = float(previous.get("accessHeat", 0))
                try:
                    last = datetime.fromisoformat(previous["lastAccessedAt"].replace("Z", "+00:00"))
                    elapsed = max(0, (now.astimezone(UTC) - last.astimezone(UTC)).total_seconds() / 86400)
                    heat *= 2 ** (-elapsed / self._config.eviction_access_half_life_days)
                except (KeyError, ValueError, TypeError):
                    heat = 0
                usage[key] = {"accessCount": count + 1, "accessHeat": heat + 1, "lastAccessedAt": now.isoformat()}
            row.document = document

        self._call(write=True, operation=record)

    def record_capacity_eviction(self, decision, *, max_facts, agent_name, user_id=None, occurred_at=None, shadow_decision=None):
        self._scope_args(agent_name, user_id)
        if not decision.evicted or self._config.eviction_audit_max_entries == 0:
            return

        def record(row):
            document = copy.deepcopy(row.document)
            metadata = document.setdefault("_meta", {})
            events = metadata.setdefault("evictions", [])
            event = {
                "occurredAt": utc_now_iso_z(),
                "reason": "capacity",
                "maxFacts": max_facts,
                "policyVersion": decision.policy,
                "reservedCorrectionSlots": decision.reserved_correction_slots,
                "evicted": [{"factId": item.fact_id, "category": item.category, "score": item.score, "components": copy.deepcopy(item.components)} for item in decision.evicted],
            }
            if shadow_decision is not None:
                shadow_ids = {item.fact_id for item in shadow_decision.evicted}
                event["shadow"] = {"policyVersion": shadow_decision.policy, "wouldEvict": sorted(shadow_ids), "disagrees": {item.fact_id for item in decision.evicted} != shadow_ids}
            events.append(event)
            metadata["evictions"] = events[-min(10000, self._config.eviction_audit_max_entries) :]
            existing = {f["id"] for f in document["facts"]}
            metadata["usage"] = {k: v for k, v in metadata.get("usage", {}).items() if k in existing}
            row.document = document

        self._call(write=True, operation=record)

    def clear_fact_metadata(self, *, agent_name, user_id=None, fact_ids=None):
        self._scope_args(agent_name, user_id)

        def clear(row):
            document = copy.deepcopy(row.document)
            metadata = document.get("_meta", {})
            existing = {f["id"] for f in document["facts"]}
            metadata["usage"] = {k: v for k, v in metadata.get("usage", {}).items() if k in existing and fact_ids is not None and k not in fact_ids}
            document["_meta"] = metadata
            row.document = document

        self._call(write=True, operation=clear)


def instance_memory_service(agents):
    service = getattr(agents, "memory_service", None)
    if service is None:
        service = agents.memory_service = InstanceMemory(agents)
    return service


def execution_memory_enabled(execution, config):
    """Cheap class capability resolution during off-loop graph assembly."""
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem
    from deerflow.agents.memory.manager import _resolve_manager_class

    if not isinstance(execution, AgentExecution) or not execution.execution_allowed or not config.enabled or execution.definition.config.get("memory_enabled", True) is False:
        return False
    if getattr(execution.instance, "permissions", AgentPermission(0)) & AgentPermission.INSPECT != AgentPermission.INSPECT:
        return False
    if config.backend_config.get("storage_class", "") not in ("", "file", "markdown") or config.backend_config.get("retrieval_adapter", "fts5") not in ("", "fts5"):
        return False
    return _resolve_manager_class(config.manager_class) is DeerMem


def get_instance_memory_manager(execution, host_factory):
    from deerflow.config.memory_config import get_memory_config

    if not execution_memory_enabled(execution, get_memory_config()):
        raise NotImplementedError("Instance memory is disabled or the configured backend scope is unsupported")
    execution.validate_sync()
    return instance_memory_service(execution.authority.instances).manager(execution, host_factory())


def runtime_memory_manager(runtime, legacy_factory):
    """An explicit host runtime binding also covers compaction and children."""
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY

    context = getattr(runtime, "context", None)
    execution = context.get(AGENT_EXECUTION_CONTEXT_KEY) if isinstance(context, dict) else None
    if isinstance(execution, AgentExecution):
        from deerflow.agents.memory.manager import _get_host_memory_manager

        return get_instance_memory_manager(execution, _get_host_memory_manager)
    return legacy_factory()


async def validate_memory_audience(execution):
    """Retire an already-injected context when its whole-memory audience changes."""
    from deerflow.config.memory_config import get_memory_config

    if not await asyncio.to_thread(lambda: execution_memory_enabled(execution, get_memory_config())):
        raise AgentDenied("The injected instance memory capability is no longer available")

    async def admit():
        await instance_memory_service(execution.authority.instances).bind(actor=execution.requester, instance_id=execution.instance.id, execution=execution)

    if asyncio.get_running_loop() is execution.owner_loop:
        await admit()
    elif execution.owner_loop is not None and execution.owner_loop.is_running():
        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(admit(), execution.owner_loop))
    else:
        raise AgentDenied("The instance memory authority loop is unavailable")


def finish_execution_memory(execution):
    service = getattr(getattr(execution.authority, "instances", None), "memory_service", None)
    if service is not None:
        service.finish(execution)

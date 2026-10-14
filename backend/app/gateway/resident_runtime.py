"""Gateway ownership of resident startup, native accounting and final drainage."""

import asyncio
import logging
import time
from datetime import UTC, datetime

from sqlalchemy import select

from deerflow.agent_instances.bootstrap import NativeAccountingProof, ResidentBootstrapCoordinator
from deerflow.agent_instances.contract import InstanceIdentity
from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.agent_instances.work import UNRESOLVED, AgentWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.config.paths import get_paths, paths_scope
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.attachments import ResourceMount
from deerflow.spaces.chat_paths import ChatStoragePaths
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class ResidentRuntime:
    def __init__(self, app, *, registry, admission, entered):
        self.app, self.registry, self.admission = app, registry, admission
        self.provider, self.attachments = registry.provider, registry.attachments
        self.files = app.state.storage_spaces
        self.foreign_run_blocks = frozenset()
        self.workspace_qualified = False
        self._closing_tasks = None
        self._closing = False
        self._operations = set()
        self.accounting_retry = False
        self._startup_claims = None
        self._startup_settled = False
        self._projection_roots = tuple(str(path) for spec in self.files.catalog.volumes.values() for name in ("mount_path", "image_path") if (path := getattr(spec, name, None)) is not None)
        self.provider._backend._native_mountless_binds = self._operational_projection
        self.provider._backend._native_protected_roots = self._projection_roots
        self.coordinator = ResidentBootstrapCoordinator(
            registry=registry,
            admission=admission,
            entered=entered,
            drain_producers=self.drain_producers,
            account_native=self.account_native,
            reconcile=self.reconcile,
            drain_workers=self.drain_workers,
        )

    def _operational_projection(self, user_id, thread_id):
        """Use the production operational projection without a User/Home bind."""
        paths = ChatStoragePaths(get_paths(), user_id=user_id, protected_roots=self._projection_roots)
        with paths_scope(paths):
            binds = list(self.provider._get_extra_mounts(thread_id, user_id=user_id))
            exclusion = self.provider._local_config_mount_exclusion_root(thread_id, user_id=user_id)
        from pathlib import PurePosixPath

        for mount in self.provider._backend._config_mounts:
            destination = str(mount.container_path)
            if exclusion is not None and PurePosixPath(destination).is_relative_to(PurePosixPath(exclusion)):
                continue
            if mount.read_only is True:
                binds.append((str(mount.host_path), destination, True))
        return tuple(binds)

    async def drain_producers(self):
        await await_drained(run_sync_lifecycle_operation(self.provider._drain_bootstrap_producers))

    @staticmethod
    def _pending_facts(bridge):
        return tuple(sorted((key, id(value), value.attempt, value.verified, value.acknowledged, value.submitted) for key, value in bridge.handoffs._items.items()))

    def invalidate_accounting(self):
        """Close immediately; retain any initializer until its outcome is read."""
        self.workspace_qualified = False
        self.provider.external_changed(self.admission.close)
        self.accounting_retry = True

    @staticmethod
    def _catalog_facts(catalog):
        names = ("kind", "mount_path", "image_path", "filesystem_uuid", "max_bytes", "max_inodes")
        return tuple(sorted((slot, id(spec), tuple(str(getattr(spec, name, None)) for name in names)) for slot, spec in catalog.volumes.items()))

    async def account_native(self, permit):
        """Retain a complete strict census, independently of workspace readiness."""
        from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

        provider, bridge, catalog = self.provider, self.provider._native_creation, self.files.catalog
        catalog_facts = self._catalog_facts(catalog)
        await self.inventory_runs()
        adapter = self.attachments.provider
        claims = dict(self.registry.retained)
        if any(claim.container_id is None for claim in claims.values()):
            raise StorageRuntimeUnavailable("A retained preparation lacks its original acknowledged container identity")
        expected = {}
        roots = await adapter.readonly_roots() if claims else ()
        configured = tuple(adapter._configured_mounts)
        if claims:
            async with self.app.state.agent_instances._sf() as session:
                space_ids = {resource.space_id for claim in claims.values() for resource in claim.resources}
                bindings = {row.space_id: row for row in (await session.scalars(select(SpaceBackingRow).where(SpaceBackingRow.space_id.in_(space_ids)))).all()}
                for identifier, claim in claims.items():
                    mounts, slots = [], set()
                    for resource in claim.resources:
                        binding = bindings.get(resource.space_id)
                        if binding is None:
                            raise StorageRuntimeUnavailable("Resident census lacks its exact durable backing")
                        spec = self.files._binding_spec(binding)
                        mounts.append((str(spec.mount_path / "data"), "/mnt/spaces/" + resource.alias, resource.writable))
                        slots.add(binding.slot_id)
                    mounts.extend((root.source, root.destination, False) for root in roots)
                    slots.update(root.slot_id for root in roots)
                    mounts.extend((str(source), str(destination), False) for source, destination, readonly in configured if readonly is True)
                    expected[identifier] = (tuple(sorted(mounts)), tuple(sorted(slots)))

        def account():
            from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet

            provider.external_changed(provider._require_bootstrap_producers_drained_locked)
            paired = NativeResourceSet(provider._backend, guard=bridge.assert_current).inventory_paired()
            residents = adapter.bootstrap_residents(known_slots=tuple(catalog.volumes), paired=paired)
            for actual in residents:
                claim = claims.get(actual.attachment_id)
                if claim is None or (claim.container_id, claim.host_id) != (actual.container_id, actual.host_id) or expected.get(actual.attachment_id) != (actual.mounts, actual.slots):
                    raise StorageRuntimeUnavailable("Resident census has an untracked or changed physical writer")
            actuals = provider._backend._bootstrap_primary_inventory(known_slots=tuple(catalog.volumes), mountless_binds=self._operational_projection)
            explained = {resource for actual in (*actuals, *residents) for resource in actual.resources}
            explained.update(resource for record in bridge.teardowns._items.values() for resource in record.resources)
            if any(item.fact not in explained for item in paired):
                raise StorageRuntimeUnavailable("Native census retains unexplained paired resources")
            for actual in actuals:
                info = SandboxInfo(
                    sandbox_id=actual.sandbox_id,
                    sandbox_url="",
                    container_name=actual.container_name,
                    container_id=actual.container_id,
                    owner=actual.owner,
                    native_slots=actual.slots,
                    native_container=actual,
                    chat_storage_enabled=True,
                )
                provider._park_bootstrap_accounting(permit, info)
            retained = {}
            pending = teardowns = None

            def capture():
                nonlocal pending, teardowns
                provider._require_bootstrap_producers_drained_locked()
                for actual in actuals:
                    parked = provider._warm_pool.get(actual.sandbox_id)
                    current = provider._sandbox_infos.get(actual.sandbox_id) or (parked[0] if parked is not None else None)
                    if current is None or (current.container_id, current.container_name, current.owner, current.native_slots) != (actual.container_id, actual.container_name, actual.owner, actual.slots):
                        raise StorageRuntimeUnavailable("Native census lost its exact retained primary")
                    retained[actual.sandbox_id] = (current, actual)
                pending = self._pending_facts(bridge)
                teardowns = frozenset(bridge.teardowns._items.values())

            provider.external_changed(capture)

            def validate():
                provider._require_bootstrap_producers_drained_locked()
                if self.foreign_run_blocks:
                    raise StorageRuntimeUnavailable("Durable inflight runs lack their completely drained local owner")
                if (
                    self.provider is not provider
                    or provider._native_creation is not bridge
                    or self.files.catalog is not catalog
                    or self._catalog_facts(catalog) != catalog_facts
                    or self._pending_facts(bridge) != pending
                    or frozenset(bridge.teardowns._items.values()) != teardowns
                ):
                    raise StorageRuntimeUnavailable("Native census or submitted request facts changed")
                if tuple(adapter._configured_mounts) != configured or any(self.registry.retained.get(actual.attachment_id) != claims[actual.attachment_id] for actual in residents):
                    raise StorageRuntimeUnavailable("Resident census changed before capacity publication")
                for sandbox_id, (original, actual) in retained.items():
                    parked = provider._warm_pool.get(sandbox_id)
                    current = provider._sandbox_infos.get(sandbox_id) or (parked[0] if parked is not None else None)
                    if current is not original or (current.container_id, current.container_name, current.owner, current.native_slots) != (actual.container_id, actual.container_name, actual.owner, actual.slots):
                        raise StorageRuntimeUnavailable("Native census primary changed before publication")

            return NativeAccountingProof.capture(provider, permit, validate_locked=validate)

        return await await_drained(run_sync_lifecycle_operation(account))

    async def inventory_runs(self):
        """Read all durable inflight rows; a census grants no lease takeover."""
        try:
            rows = await self.app.state.run_store.list_inflight()
            if not isinstance(rows, list) or len(rows) > 100000 or any(not isinstance(row, dict) or not isinstance(row.get("run_id"), str) or not row["run_id"] or not isinstance(row.get("metadata", {}), dict) for row in rows):
                raise ValueError("Invalid durable run inventory")
            if len({row["run_id"] for row in rows}) != len(rows):
                raise ValueError("Ambiguous durable run inventory")
        except Exception as exc:
            self.invalidate_accounting()
            raise StorageRuntimeUnavailable("The complete durable inflight run inventory is unavailable") from exc
        manager = self.registry.run_manager
        foreign = set()
        for row in rows:
            local = manager._runs.get(row["run_id"])
            if (
                local is None
                or local.task is None
                or local.task.done()
                or local.ownership_lost
                or local.metadata != row.get("metadata", {})
                or row.get("owner_worker_id") != local.owner_worker_id
                or row.get("thread_id") != local.thread_id
                or row.get("user_id") != local.user_id
            ):
                foreign.add(row["run_id"])
        self.foreign_run_blocks = frozenset(foreign)
        if foreign:
            self.invalidate_accounting()
        return rows

    async def owner_of(self, home_id):
        async with self.app.state.agent_instances._sf() as session:
            rows = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.home_id == home_id))).scalars().all()
            return InstanceIdentity.from_row(rows[0]) if len(rows) == 1 else None

    async def settle(self, *, instance_id, reason, attachment_id, live_run_ids=frozenset(), unstarted_after=None):
        rows = await self.inventory_runs()
        if self.foreign_run_blocks:
            return []
        live = frozenset(row["run_id"] for row in rows) | frozenset(live_run_ids) | frozenset(self.registry.in_flight(instance_id))

        def active_attempts():
            running = self.registry.run_manager.task_runs("agent_work_attempt_id")
            result = {record["value"] for run_id, record in running.items() if run_id not in self.registry.ended and record["metadata"].get("agent_instance_id") == instance_id}
            result.update(row.get("metadata", {}).get("agent_work_attempt_id") for row in rows if row.get("metadata", {}).get("agent_instance_id") == instance_id and row.get("metadata", {}).get("agent_work_attempt_id") is not None)
            result.update(reservation.attempt_id for reservation in self.registry.reservations.values() if reservation.instance_id == instance_id and reservation.attempt_id is not None and self.registry.launch_owned(reservation))
            return frozenset(result)

        result = await WorkExecution(AgentWork(self.app.state.agent_instances)).settle_contained(
            instance_id=instance_id,
            host_id=self.attachments.provider.host_id,
            reason=reason,
            attachment_id=attachment_id,
            unstarted_after=unstarted_after,
            live_run_ids=live,
            live_attempts=active_attempts,
        )
        self.app.state.agent_execution.attempts_settled += len(result)
        for reservation in tuple(self.registry.reservations.values()):
            if reservation.attempt_id in result and not self.registry.launch_owned(reservation):
                self.registry.cancel(reservation)
        return result

    async def reconcile_rows(self, *, attachment_ids=None, deadline):
        """Associate only unique qualified Homes; preserve foreign live owners."""
        runs = await self.inventory_runs()
        foreign_instances = {row.get("metadata", {}).get("agent_instance_id") for row in runs if row["run_id"] in self.foreign_run_blocks}
        foreign_instances.discard(None)
        claims = await self.attachments.retained_claims()
        if self._startup_claims is None:
            self._startup_claims = frozenset(claim.id for claim in claims)
        if attachment_ids is not None:
            selected = frozenset(attachment_ids)
            claims = tuple(claim for claim in claims if claim.id in selected)
        for claim in claims:
            self.registry.retain(claim)
        async with self.app.state.agent_instances._sf() as session:
            rows = (await session.execute(select(AgentInstanceRow, SpaceRow.generation).outerjoin(SpaceRow, SpaceRow.id == AgentInstanceRow.home_id))).all()
            by_home = {}
            for row, home_generation in rows:
                if row.home_id is not None:
                    by_home.setdefault(row.home_id, []).append((InstanceIdentity.from_row(row), home_generation))
            attempts = (
                await session.execute(
                    select(AgentWorkRow.instance_id, AgentWorkAttemptRow.id, AgentWorkAttemptRow.run_id).join(AgentWorkAttemptRow, AgentWorkAttemptRow.work_id == AgentWorkRow.id).where(AgentWorkAttemptRow.status.in_(UNRESOLVED))
                )
            ).all()
            live_ids = {record["run_id"] for record in runs}
            promised = {reservation.attempt_id for reservation in self.registry.reservations.values() if reservation.attempt_id is not None}
            orphaned = {identifier for identifier, attempt_id, run_id in attempts if run_id not in live_ids and attempt_id not in promised}
            replacements = set((await session.execute(select(StorageHostReplacementRow.old_host_id).where(StorageHostReplacementRow.new_host_id == self.attachments.provider.host_id))).scalars())
        live_instances = {row.get("metadata", {}).get("agent_instance_id") for row in runs}
        startup_orphaned = set()
        for record in getattr(self.app.state, "startup_recovered_runs", ()):
            identifier = (record.metadata or {}).get("agent_instance_id")
            if identifier is not None and identifier not in live_instances:
                startup_orphaned.add(identifier)
        from app.gateway.storage_spaces import get_agents_api_config

        enabled = (await run_sync_lifecycle_operation(get_agents_api_config)).enabled
        pending = []
        for claim in claims:
            claim_orphaned = orphaned | startup_orphaned if claim.id in self._startup_claims else orphaned
            if time.monotonic() >= deadline:
                pending.append(claim.id)
                continue
            if claim.container_id is None:
                # An empty daemon listing cannot drain an earlier unreturned
                # request. Retain the preparation until exact recovery exists.
                pending.append(claim.id)
                continue
            owners = [owner for resource in claim.resources for owner in by_home.get(resource.space_id, ())]
            if len(owners) > 1:
                pending.append(claim.id)
                continue
            identity, generation = owners[0] if owners else (None, None)
            if identity is not None and identity.id in foreign_instances:
                pending.append(claim.id)
                continue
            entry = self.registry.entry(identity) if identity is not None else None

            async def reconcile():
                nonlocal identity, generation, enabled
                if entry is not None:
                    busy = bool(self.registry.in_flight(identity.id) or self.registry._has_reservation(entry.home_id) or entry.preparation is not None)
                    if busy and (identity.id in claim_orphaned or identity.status != "active" or claim.phase != "active" or not enabled):
                        return False
                    current_runs = await self.inventory_runs()
                    if any(row.get("metadata", {}).get("agent_instance_id") == identity.id and row["run_id"] in self.foreign_run_blocks for row in current_runs):
                        return False
                    async with self.app.state.agent_instances._sf() as session:
                        current = (await session.execute(select(AgentInstanceRow, SpaceRow.generation).outerjoin(SpaceRow, SpaceRow.id == AgentInstanceRow.home_id).where(AgentInstanceRow.id == identity.id))).one_or_none()
                        if current is None or current[0].home_id != entry.home_id:
                            return False
                        identity, generation = InstanceIdentity.from_row(current[0]), current[1]
                    enabled = (await run_sync_lifecycle_operation(get_agents_api_config)).enabled
                if claim.host_id != self.attachments.provider.host_id and claim.host_id not in replacements:
                    return False
                if claim.host_id != self.attachments.provider.host_id:
                    reason = "host-replaced"
                elif not enabled:
                    reason = "operator"
                elif identity is None:
                    reason = "container-absent"
                elif identity.id in claim_orphaned:
                    reason = "run-orphaned"
                elif identity.status != "active":
                    reason = "operator"
                elif claim.phase == "active":
                    touching = [item for item in self.registry.retained.values() if any(resource.space_id == identity.home_id for resource in item.resources)]
                    if len(touching) != 1:
                        return False
                    expected = (ResourceMount(identity.home_id, generation, "home", True),)
                    if claim.actor != identity.principal or claim.resources != expected or await self.attachments.container_state(claim.id) != "running":
                        reason = "container-absent"
                    else:
                        resumed = await self.attachments.resume(actor=identity.principal, resources=expected, incarnation=claim.incarnation)
                        fresh = await self.attachments.retained_attachment(claim.id)
                        if resumed is None or resumed.id != claim.id or resumed.container_id != claim.container_id or fresh is None:
                            raise StorageRuntimeUnavailable("Workspace resume lost its exact retained identity")
                        self.registry.retain(fresh)
                        self.registry.associate(identity, self.registry.retained[claim.id], generation=generation)
                        return True
                else:
                    reason = "container-absent"
                # The Home fence stays installed through the awaited effect.
                # Recheck submitted activity after qualification/config awaits.
                if entry is not None and (self.registry.in_flight(identity.id) or self.registry._has_reservation(entry.home_id) or entry.preparation is not None):
                    return False
                if not await self.attachments.contain_host(claim.id, reason):
                    return False
                self.app.state.agent_execution.attachments_fenced += 1
                return True

            try:
                if entry is None:
                    confirmed = await reconcile()
                else:
                    async with entry.lock:
                        entry.reconciling = True
                        try:
                            confirmed = await reconcile()
                        finally:
                            entry.reconciling = False
                if not confirmed:
                    pending.append(claim.id)
            except Exception:
                logger.exception("agents: attachment reconciliation remains pending")
                pending.append(claim.id)
        return pending

    async def apply_hold(self):
        """Close current local Homes; a live foreign lease grants no takeover."""
        from app.gateway.storage_spaces import hold_present

        status = self.app.state.agent_execution
        if not await run_sync_lifecycle_operation(hold_present):
            if status.hold_since is not None:
                status.hold_since = None
                logger.info("agents: hold lifted")
            return
        first = status.hold_since is None
        if first:
            status.hold_since = datetime.now(UTC)
        rows = await self.inventory_runs()
        foreign = {row.get("metadata", {}).get("agent_instance_id") for row in rows if row["run_id"] in self.foreign_run_blocks}
        closed = 0
        for claim in await self.attachments.retained_claims():
            if claim.container_id is None:
                continue
            owners = [await self.owner_of(resource.space_id) for resource in claim.resources]
            owners = [owner for owner in owners if owner is not None]
            if len(owners) != 1 or owners[0].id in foreign or claim.actor != owners[0].principal:
                continue
            entry = self.registry.entry(owners[0])
            async with entry.lock:
                entry.reconciling = True
                try:
                    current_runs = await self.inventory_runs()
                    if any(row["run_id"] in self.foreign_run_blocks and row.get("metadata", {}).get("agent_instance_id") == entry.instance_id for row in current_runs):
                        continue
                    current_owner = await self.owner_of(entry.home_id)
                    if current_owner is None or current_owner.id != entry.instance_id or current_owner.principal != claim.actor or await self.attachments.retained_attachment(claim.id) != claim:
                        continue
                    if await self.attachments.contain_host(claim.id, "operator"):
                        closed += 1
                        status.attachments_fenced += 1
                finally:
                    entry.reconciling = False
        if first:
            logger.warning("agents: held (operator hold present since %s; %s workspaces closed)", status.hold_since.isoformat(), closed)

    async def reconcile(self):
        from app.gateway.storage_spaces import reconcile_residents, storage_state

        self.workspace_qualified = False
        await self.apply_hold()
        self.app.state.agent_execution.pending_reconcile = await reconcile_residents(self.app, deadline=time.monotonic() + 60)
        if not self._startup_settled:
            await self.settle_pending(startup=True)
            self._startup_settled = not self.foreign_run_blocks
        self.workspace_qualified = not self.app.state.agent_execution.pending_reconcile and (await storage_state(self.app)).state not in {"off", "unavailable"}
        return self.workspace_qualified

    async def settle_pending(self, *, startup=False):
        """Settle only drained attempts; an unknown commit keeps its charge."""
        async with self.app.state.agent_instances._sf() as session:
            instances = (await session.scalars(select(AgentWorkRow.instance_id).join(AgentWorkAttemptRow, AgentWorkAttemptRow.work_id == AgentWorkRow.id).where(AgentWorkAttemptRow.status.in_(UNRESOLVED)).distinct())).all()
        for instance_id in instances:
            await self.settle(instance_id=instance_id, reason="run-orphaned" if startup else "start-failed", attachment_id=None, unstarted_after=0 if startup else 120)

    def seal(self):
        """Seal before run drainage and keep the actual owned worker tasks."""
        self._closing = True
        self.provider.external_changed(self.admission.seal)
        if self._closing_tasks is None:
            manager = self.registry.run_manager
            self._closing_tasks = tuple(record.task for record in manager._runs.values() if record.task is not None and not record.task.done())

    async def own_operation(self, operation):
        """Register before yielding; caller cancellation drains the launch."""
        if self._closing:
            operation.close()
            raise StorageRuntimeUnavailable("The owning Gateway is closing")
        task = asyncio.create_task(operation, name="resident-run-admission")
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        return await await_drained(asyncio.shield(task))

    async def drain_workers(self):
        await self.drain_admissions()
        # An owned launch can attach a worker after seal's first snapshot.
        manager = self.registry.run_manager
        workers = set(self._closing_tasks or ()) | {record.task for record in manager._runs.values() if record.task is not None and not record.task.done()}
        if workers:
            await await_drained(asyncio.gather(*workers, return_exceptions=True))
        await self.drain_producers()

    async def drain_admissions(self):
        self.seal()
        operations = tuple(getattr(self, "_operations", ()))
        if operations:
            await await_drained(asyncio.gather(*operations, return_exceptions=True))

    async def close(self):
        self.seal()
        await self.coordinator.close()


def seal_resident_runtime(app):
    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is not None:
        runtime.seal()
        return
    host = getattr(app.state, "chat_storage_host", None)
    admission = getattr(host, "admission", None)
    if admission is not None:
        from deerflow.sandbox.sandbox_provider import get_initialized_sandbox_provider

        provider = get_initialized_sandbox_provider()
        if provider is not None and getattr(provider, "_runtime_admission", None) is admission:
            provider.external_changed(admission.seal)
        else:
            admission.seal()


async def close_resident_runtime(app):
    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is not None:
        await await_drained(runtime.close())


def install_resident_runtime(app, *, provider, attachments, entered):
    host = getattr(app.state, "chat_storage_host", None)
    admission = getattr(host, "admission", None)
    if admission is None:
        return None
    existing = getattr(app.state, "resident_runtime", None)
    agents = app.state.agent_instances
    if existing is not None:
        if existing.provider is not provider or existing.attachments is not attachments or existing.admission is not admission or agents.residents is not existing.registry:
            raise StorageRuntimeUnavailable("Resident startup lost its exact retained runtime")
        return existing
    if agents.residents is not None or attachments.on_contained is not None:
        raise StorageRuntimeUnavailable("Resident startup cannot replace another retained registry")
    provider._bind_runtime_admission(admission)

    async def settle(**kwargs):
        return await runtime.settle(**kwargs)

    async def owner_of(home_id):
        return await runtime.owner_of(home_id)

    registry = ResidentRegistry(
        attachments=attachments,
        provider=provider,
        run_manager=app.state.run_manager,
        settle=settle,
        owner_of=owner_of,
        replicas=provider._config.get("replicas", 3),
        idle_timeout=provider._config.get("idle_timeout", 600),
    )
    runtime = ResidentRuntime(app, registry=registry, admission=admission, entered=entered)
    agents.residents = registry
    attachments.on_contained = registry.contained
    app.state.resident_runtime = runtime
    return runtime

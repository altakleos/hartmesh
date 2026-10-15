"""Gateway ownership of resident startup, native accounting and final drainage."""

import asyncio
import logging
import time
from dataclasses import replace
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
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.attachments import AttachmentPending, ResourceMount
from deerflow.spaces.chat_paths import ChatStoragePaths
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class _ResidentSnapshotChanged(Exception):
    pass


class ResidentRuntime:
    def __init__(self, app, *, registry, admission, entered):
        self.app, self.registry, self.admission = app, registry, admission
        self.provider, self.attachments = registry.provider, registry.attachments
        self.files = app.state.storage_spaces
        self.foreign_run_blocks = frozenset()
        self.workspace_qualified = False
        self._qualification_revision = 0
        self._closing_tasks = None
        self._closing = False
        self._operations = set()
        self.accounting_retry = False
        self._startup_claims = None
        self._startup_settled = False
        self._startup_record_ids = None
        self._startup_originals = {}
        self._recovery_cursor = 0
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

    def _invalidate_workspace(self, source="accounting-invalidated"):
        self._qualification_revision += 1
        self.workspace_qualified = False
        logger.warning("agents: workspace unqualified (%s; revision=%s)", source, self._qualification_revision)

    def invalidate_accounting(self, source="accounting-invalidated"):
        """Close immediately; retain any initializer until its outcome is read."""
        self._invalidate_workspace(source)
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
        pending_recovery = await self.recover_originals(deadline=min(self.coordinator.recovery_deadline, time.monotonic() + 60))
        if pending_recovery:
            raise StorageRuntimeUnavailable("Original workspace recovery remains pending before the complete census")
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
            qualified_resident_records = frozenset(bridge.residents.records())
            for record in qualified_resident_records:
                if record.daemon_id != bridge.evidence.daemon_id:
                    raise StorageRuntimeUnavailable("Resident census retains a foreign original graph")
                NativeResourceSet(provider._backend, guard=bridge.assert_current).qualify_resident_record(record)
            residents = adapter.bootstrap_residents(known_slots=tuple(catalog.volumes), paired=paired)
            for actual in residents:
                claim = claims.get(actual.attachment_id)
                if claim is None or (claim.container_id, claim.host_id) != (actual.container_id, actual.host_id) or expected.get(actual.attachment_id) != (actual.mounts, actual.slots):
                    raise StorageRuntimeUnavailable("Resident census has an untracked or changed physical writer")
            actuals = provider._backend._bootstrap_primary_inventory(known_slots=tuple(catalog.volumes), mountless_binds=self._operational_projection)
            explained = {resource for actual in (*actuals, *residents) for resource in actual.resources}
            explained.update(resource for record in bridge.teardowns._items.values() for resource in record.resources)
            explained.update(resource for record in qualified_resident_records for resource in record.resources)
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
            pending = teardowns = resident_records = None

            def capture():
                nonlocal pending, teardowns, resident_records
                provider._require_bootstrap_producers_drained_locked()
                for actual in actuals:
                    parked = provider._warm_pool.get(actual.sandbox_id)
                    current = provider._sandbox_infos.get(actual.sandbox_id) or (parked[0] if parked is not None else None)
                    if current is None or (current.container_id, current.container_name, current.owner, current.native_slots) != (actual.container_id, actual.container_name, actual.owner, actual.slots):
                        raise StorageRuntimeUnavailable("Native census lost its exact retained primary")
                    retained[actual.sandbox_id] = (current, actual)
                pending = self._pending_facts(bridge)
                teardowns = frozenset(bridge.teardowns._items.values())
                resident_records = frozenset(bridge.residents._items.values())
                if resident_records != qualified_resident_records:
                    raise StorageRuntimeUnavailable("Resident original graph changed during census qualification")

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
                    or frozenset(bridge.residents._items.values()) != resident_records
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

    async def recover_originals(self, *, deadline):
        """Bounded recovery of the original startup set, before the complete census."""
        from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
        from deerflow.spaces.resident_containment import _fenced_facts
        from deerflow.spaces.resident_recovery import NativeResidentRecoveryScopes
        from deerflow.spaces.resident_restoration import RestoredResidentCompletionScopes

        factory = getattr(self.attachments.provider, "_containment_scope", None)
        if factory is None:
            return ()
        bridge = self.provider._native_creation
        if time.monotonic() >= deadline:
            return tuple(record.attachment_id for record in bridge.residents.records())
        await await_drained(run_sync_lifecycle_operation(bridge.assert_current, teardown=True))
        records = bridge.residents.records()
        if self._startup_record_ids is None:
            self._startup_record_ids = frozenset(record.id for record in records)
            self._startup_originals = {record.id: record for record in records if record.id in self._startup_record_ids}
        records = sorted((record for record in records if record.id in self._startup_record_ids), key=lambda record: record.id)
        if not records:
            return ()
        # Ordinary current-device active attachments belong to reconciliation,
        # not this four-original recovery budget. Classify before rotating so
        # healthy records cannot starve the final census.
        async with asyncio.timeout_at(deadline), self.files.registry._sf() as session:
            phases = dict((await session.execute(select(SpaceAttachmentRow.id, SpaceAttachmentRow.phase).where(SpaceAttachmentRow.id.in_([record.attachment_id for record in records])))).all())

        def candidates():
            selected = []
            for index, original in enumerate(records):
                if time.monotonic() >= deadline:
                    selected.extend(records[index:])
                    break
                if original.daemon_id != bridge.evidence.daemon_id or phases.get(original.attachment_id) in (None, "fenced"):
                    selected.append(original)
                    continue
                try:
                    changed = False
                    for root in original.roots:
                        if time.monotonic() >= deadline:
                            changed = True
                            break
                        if self.files.catalog.verify(root.slot_id).device != root.device:
                            changed = True
                            break
                except Exception:
                    changed = True  # Unknown roots retain evidence and retry.
                if changed or time.monotonic() >= deadline:
                    selected.append(original)
            return selected

        records = await await_drained(run_sync_lifecycle_operation(candidates))
        if not records:
            return ()
        offset = self._recovery_cursor % len(records)
        ordered = records[offset:] + records[:offset]
        self._recovery_cursor += min(4, len(records))
        pending = [record.attachment_id for record in ordered[4:]]
        for index, record in enumerate(ordered[:4]):
            if time.monotonic() >= deadline:
                pending.extend(item.attachment_id for item in ordered[index:4])
                break
            if replace(self._startup_originals[record.id], phase=record.phase) != record:
                pending.append(record.attachment_id)
                continue
            async with asyncio.timeout_at(deadline), self.files.registry._sf() as session:
                row = await session.get(SpaceAttachmentRow, record.attachment_id)
                initial_phase = None if row is None else row.phase
                expected = None if row is None else _fenced_facts(row)
                scoped = [root for root in record.roots if root.space_id is not None]
                home_id = scoped[0].space_id if len(scoped) == 1 and scoped[0].writable and scoped[0].destination == "/mnt/spaces/home" else None
                owners = (await session.scalars(select(AgentInstanceRow).where(AgentInstanceRow.home_id == home_id))).all() if home_id is not None else []
                identity = InstanceIdentity.from_row(owners[0]) if len(owners) == 1 else None
                replacement = None
                if record.daemon_id != bridge.evidence.daemon_id:
                    mapping = await session.get(StorageHostReplacementRow, (record.daemon_id, bridge.evidence.daemon_id))
                    if mapping is not None:
                        replacement = (mapping.old_host_id, mapping.new_host_id, mapping.attachments_named, mapping.created_at)
            foreign = record.daemon_id != bridge.evidence.daemon_id
            if time.monotonic() >= deadline or identity is None or (foreign and replacement is None):
                pending.append(record.attachment_id)
                continue

            def observe_absence():
                reader = NativeResourceSet(self.provider._backend, guard=lambda: bridge.assert_current(teardown=True))
                reader.deadline = deadline
                return reader.prove_resident_absent(record)

            try:
                absent = await await_drained(run_sync_lifecycle_operation(observe_absence))
            except Exception:
                pending.append(record.attachment_id)
                continue
            # This observation selects a path; the owned scope repeats every
            # proof under Home, provider-batch and original-parent exclusion.
            if not absent:
                if foreign:
                    pending.append(record.attachment_id)
                    continue
                if initial_phase not in (None, "fenced"):
                    pending.append(record.attachment_id)
                    continue
            restored_completion = foreign
            if absent and not foreign:
                try:
                    restored_completion = await await_drained(run_sync_lifecycle_operation(lambda: any(self.files.catalog.verify(root.slot_id).device != root.device for root in record.roots)))
                except Exception:
                    pending.append(record.attachment_id)
                    continue
            entry = self.registry.entry(identity)
            if entry.lock.locked():
                pending.append(record.attachment_id)
                continue

            def locally_idle():
                if (
                    not entry.lock.locked()
                    or not entry.reconciling
                    or self.registry.in_flight(identity.id)
                    or self.registry._has_reservation(entry.home_id)
                    or entry.preparation is not None
                    or entry.attachment_id not in (None, record.attachment_id)
                ):
                    raise AttachmentPending("Original resident recovery has a current local holder")

            async def eligible(session, original):
                locally_idle()
                current = await session.get(AgentInstanceRow, identity.id, populate_existing=True)
                if current is None or InstanceIdentity.from_row(current) != identity or entry.home_id != identity.home_id:
                    raise AttachmentPending("Original resident Home changed owner")
                current_row = await session.get(SpaceAttachmentRow, original.attachment_id, populate_existing=True)
                if (current_row is None) != (expected is None) or (current_row is not None and initial_phase == "fenced" and _fenced_facts(current_row) != expected):
                    raise AttachmentPending("Original orphan SQL eligibility changed")
                if current_row is not None:
                    self.attachments._closure_reason(current_row, None)
                    mounts = (await self.attachments._mount_scope(session, (original.attachment_id,)))[original.attachment_id]
                    if (current_row.host_id, current_row.incarnation, current_row.container_id, current_row.actor_kind, current_row.actor_id) != (
                        original.daemon_id,
                        original.incarnation,
                        original.container_id,
                        identity.principal.kind,
                        identity.principal.subject_id,
                    ) or set(mounts) != set(original.correlation[4]):
                        raise AttachmentPending("Original fenced resident correlation changed")
                    if initial_phase == "fence_pending" and _fenced_facts(current_row)[6:9] != expected[6:9]:
                        raise AttachmentPending("Original resident closure attribution changed")
                if foreign:
                    mapping = await session.get(StorageHostReplacementRow, (original.daemon_id, bridge.evidence.daemon_id), populate_existing=True)
                    if mapping is None or (mapping.old_host_id, mapping.new_host_id, mapping.attachments_named, mapping.created_at) != replacement:
                        raise AttachmentPending("Original resident host replacement changed")
                other = await session.scalar(select(SpaceAttachmentRow.id).join(SpaceMountRow).where(SpaceMountRow.space_id == entry.home_id, SpaceAttachmentRow.id != original.attachment_id, SpaceAttachmentRow.phase != "fenced").limit(1))
                if other is not None:
                    raise AttachmentPending("Original resident Home has another unfenced incarnation")
                snapshot = getattr(self.app.state.run_store, "_borrowed_inflight", None)
                if not callable(snapshot):
                    raise AttachmentPending("Original resident recovery lacks a borrowed complete run snapshot")
                rows = await snapshot(session)
                self._validate_run_snapshot(rows)
                if any(run.get("metadata", {}).get("agent_instance_id") == identity.id or run.get("metadata", {}).get("agent_attachment_id") == original.attachment_id for run in rows):
                    raise AttachmentPending("Original resident recovery retains a durable inflight run")
                locally_idle()

            async with entry.lock:
                entry.reconciling = True
                try:
                    locally_idle()
                    if absent and restored_completion:
                        restoration = RestoredResidentCompletionScopes(factory, record, eligible=eligible, deadline=deadline)
                        reason = "host-replaced" if foreign else "container-absent"
                        if expected is None:
                            async with restoration.missing(self.attachments) as scope:
                                try:
                                    await scope.recover_missing()
                                    await scope.finalize()
                                finally:
                                    if scope.terminal_completed:
                                        await await_drained(self.registry.contained(record.attachment_id, (entry.home_id,), reason))
                        else:
                            try:
                                if not await self.attachments._contain_restored_host(record.attachment_id, reason, scope_factory=restoration):
                                    if not restoration.terminal_completed:
                                        raise AttachmentPending("Restored resident completion remains pending")
                            finally:
                                if restoration.terminal_completed:
                                    self.attachments.uncontained.pop(record.attachment_id, None)
                                    if record.attachment_id in self.registry.retained or self.registry._associated.get(entry.home_id) == record.attachment_id or entry.attachment_id == record.attachment_id:
                                        await await_drained(self.registry.contained(record.attachment_id, (entry.home_id,), reason))
                            if initial_phase != "fenced":
                                self.app.state.agent_execution.attachments_fenced += 1
                    elif initial_phase not in (None, "fenced"):
                        # The SQL phase may have changed after classification.
                        # Normal reconciliation owns current-device open rows.
                        continue
                    else:
                        async with NativeResidentRecoveryScopes(factory)(record, eligible=eligible, deadline=deadline) as scope:
                            try:
                                await scope.contain()
                                await scope.finalize()
                            finally:
                                if scope.terminal_completed:
                                    await await_drained(self.registry.contained(record.attachment_id, (entry.home_id,), "container-absent"))
                    logger.info("agents: recovered original resident resources %s", record.attachment_id)
                except Exception as exc:
                    pending.append(record.attachment_id)
                    logger.warning("Original resident recovery remains pending for %s (%s)", record.attachment_id, type(exc).__name__)
                finally:
                    entry.reconciling = False
        return tuple(pending)

    @staticmethod
    def _validate_run_snapshot(rows):
        if not isinstance(rows, list) or len(rows) > 100000 or any(not isinstance(row, dict) or not isinstance(row.get("run_id"), str) or not row["run_id"] or not isinstance(row.get("metadata", {}), dict) for row in rows):
            raise ValueError("Invalid durable run inventory")
        if len({row["run_id"] for row in rows}) != len(rows):
            raise ValueError("Ambiguous durable run inventory")

    async def inventory_runs(self):
        """Read all durable inflight rows; a census grants no lease takeover."""
        try:
            rows = await self.app.state.run_store.list_inflight()
            self._validate_run_snapshot(rows)
        except Exception as exc:
            self.invalidate_accounting("run-inventory-error")
            raise StorageRuntimeUnavailable("The complete durable inflight run inventory is unavailable") from exc
        manager = self.registry.run_manager
        foreign = set()
        mismatches = set()
        for row in rows:
            local = manager._runs.get(row["run_id"])
            mismatch = None
            if local is None:
                mismatch = "local-missing"
            elif local.task is None:
                mismatch = "task-missing"
            elif local.task.done():
                mismatch = "task-done"
            elif local.ownership_lost:
                mismatch = "ownership-lost"
            elif local.metadata != row.get("metadata", {}):
                mismatch = "metadata"
            elif row.get("owner_worker_id") != local.owner_worker_id:
                mismatch = "worker"
            elif row.get("thread_id") != local.thread_id:
                mismatch = "thread"
            elif row.get("user_id") != local.user_id:
                mismatch = "user"
            if mismatch is not None:
                foreign.add(row["run_id"])
                mismatches.add(mismatch)
        self.foreign_run_blocks = frozenset(foreign)
        if foreign:
            logger.warning("agents: run inventory unqualified (foreign=%s; fields=%s)", len(foreign), ",".join(sorted(mismatches)))
            self.invalidate_accounting("run-inventory-foreign")
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
        for refresh in range(2):
            try:
                return await self._reconcile_rows(attachment_ids=attachment_ids, deadline=deadline)
            except _ResidentSnapshotChanged as changed:
                if refresh or attachment_ids is not None or time.monotonic() >= deadline:
                    self._invalidate_workspace("reconcile-snapshot")
                    return [changed.args[0]]
            except BaseException:
                self.invalidate_accounting("reconcile-scan-error")
                raise

    async def _reconcile_rows(self, *, attachment_ids=None, deadline):
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
            owners = [owner for resource in claim.resources for owner in by_home.get(resource.space_id, ())]
            if len(owners) != 1:
                self.registry.retain(claim)
            claim_orphaned = orphaned | startup_orphaned if claim.id in self._startup_claims else orphaned
            if time.monotonic() >= deadline:
                self.registry.retain(claim)
                self._invalidate_workspace("reconcile-deadline")
                pending.append(claim.id)
                continue
            if claim.container_id is None:
                self.registry.retain(claim)
                # An empty daemon listing cannot drain an earlier unreturned
                # request. Retain the preparation until exact recovery exists.
                self._invalidate_workspace("reconcile-preparing")
                pending.append(claim.id)
                continue
            if len(owners) > 1:
                self._invalidate_workspace("reconcile-owners")
                pending.append(claim.id)
                continue
            identity, generation = owners[0] if owners else (None, None)
            if identity is not None and identity.id in foreign_instances:
                self.registry.retain(claim)
                self._invalidate_workspace("reconcile-foreign")
                pending.append(claim.id)
                continue
            entry = self.registry.entry(identity) if identity is not None else None
            refusal = "containment-unconfirmed"

            def unconfirmed(source):
                nonlocal refusal
                refusal = source
                return False

            async def reconcile():
                nonlocal claim, identity, generation, enabled
                if entry is not None:
                    state = await self.attachments.inspect_preparation(actor=claim.actor, incarnation=claim.incarnation, resources=claim.resources, host_id=claim.host_id)
                    if state is None or (state.id, state.container_id) != (claim.id, claim.container_id):
                        raise StorageRuntimeUnavailable("Captured attachment correlation changed")
                    if state.phase == "fenced":
                        if claim.id not in self.registry.retained and claim.id not in self.registry._associated.values() and entry.attachment_id != claim.id and entry.preparation is None and claim.id not in self.attachments.uncontained:
                            raise _ResidentSnapshotChanged(claim.id)
                        return unconfirmed("containment-unconfirmed")
                    fresh = replace(claim, phase=state.phase)
                    if self.registry._merge_retained(claim, fresh) != fresh:
                        raise StorageRuntimeUnavailable("Captured attachment phase regressed")
                    claim = fresh
                    self.registry.retain(claim)
                    if time.monotonic() >= deadline:
                        return unconfirmed("deadline")
                    busy = bool(self.registry.in_flight(identity.id) or self.registry._has_reservation(entry.home_id) or entry.preparation is not None)
                    if busy and (identity.id in claim_orphaned or identity.status != "active" or claim.phase != "active" or not enabled):
                        return unconfirmed("busy-ineligible")
                    current_runs = await self.inventory_runs()
                    if any(row.get("metadata", {}).get("agent_instance_id") == identity.id and row["run_id"] in self.foreign_run_blocks for row in current_runs):
                        return unconfirmed("foreign-owner")
                    async with self.app.state.agent_instances._sf() as session:
                        current = (await session.execute(select(AgentInstanceRow, SpaceRow.generation).outerjoin(SpaceRow, SpaceRow.id == AgentInstanceRow.home_id).where(AgentInstanceRow.id == identity.id))).one_or_none()
                        if current is None or current[0].home_id != entry.home_id:
                            return unconfirmed("owner-changed")
                        identity, generation = InstanceIdentity.from_row(current[0]), current[1]
                    enabled = (await run_sync_lifecycle_operation(get_agents_api_config)).enabled
                if claim.host_id != self.attachments.provider.host_id and claim.host_id not in replacements:
                    return unconfirmed("host-unqualified")
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
                        return unconfirmed("ambiguous-association")
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
                    return unconfirmed("busy-containment")
                if not await self.attachments.contain_host(claim.id, reason):
                    return unconfirmed("containment-unconfirmed")
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
                    self._invalidate_workspace("reconcile-" + refusal)
                    logger.warning(
                        "agents: attachment unqualified (active=%s; cid_present=%s; retained_equal=%s; preparation=%s)",
                        claim.phase == "active",
                        claim.container_id is not None,
                        self.registry.retained.get(claim.id) == claim,
                        entry is not None and entry.preparation is not None,
                    )
                    pending.append(claim.id)
            except _ResidentSnapshotChanged:
                raise
            except Exception:
                self.registry.retain(claim)
                self._invalidate_workspace("reconcile-error")
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

        self.coordinator._owning_loop()
        revision = self._qualification_revision
        bound = self.registry, self.provider, self.attachments, self.files, self.admission, self.coordinator
        try:
            await self.apply_hold()
            pending = await reconcile_residents(self.app, deadline=time.monotonic() + 60)
            self.app.state.agent_execution.pending_reconcile = pending
            if pending:
                self._invalidate_workspace("reconcile-pending")
            if not self._startup_settled:
                await self.settle_pending(startup=True)
                self._startup_settled = not self.foreign_run_blocks
            qualified = not pending and (await storage_state(self.app)).state not in {"off", "unavailable"}
        except BaseException:
            self._invalidate_workspace("refresh-error")
            raise
        if revision != self._qualification_revision:
            return False
        current = self.registry, self.provider, self.attachments, self.files, self.admission, self.coordinator
        if any(original is not fresh for original, fresh in zip(bound, current)) or self.accounting_retry or self._closing or self.app.state.resident_runtime is not self:
            self._invalidate_workspace("refresh-binding")
            return False
        if not qualified:
            self._invalidate_workspace("refresh-storage")
            return False
        # One owning-loop publication after all awaited checks. Reconciliation
        # cannot reopen the provider lifecycle or certify fresh accounting.
        try:

            def publish():
                self.coordinator._require_bound_locked()
                self.workspace_qualified = True

            self.provider.external_changed(publish)
        except BaseException:
            self._invalidate_workspace("refresh-publication")
            raise
        return True

    async def settle_pending(self, *, startup=False):
        """Settle only drained attempts; an unknown commit keeps its charge."""
        async with self.app.state.agent_instances._sf() as session:
            instances = (await session.scalars(select(AgentWorkRow.instance_id).join(AgentWorkAttemptRow, AgentWorkAttemptRow.work_id == AgentWorkRow.id).where(AgentWorkAttemptRow.status.in_(UNRESOLVED)).distinct())).all()
        for instance_id in instances:
            await self.settle(instance_id=instance_id, reason="run-orphaned" if startup else "start-failed", attachment_id=None, unstarted_after=0 if startup else 120)

    def seal(self):
        """Seal before run drainage and keep the actual owned worker tasks."""
        self._invalidate_workspace("shutdown")
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

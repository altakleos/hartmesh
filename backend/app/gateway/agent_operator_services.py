"""Isolated, one-shot operator policy; no Gateway lifespan or runtime owner."""

import os
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, or_, select
from sqlalchemy.orm import load_only

from app.gateway.agent_operator_actions import OperatorActions
from app.gateway.agent_operator_contract import CommandRefused
from app.gateway.agent_operator_settlement import OperatorSettlement
from app.gateway.agent_operator_supervision import OperatorSupervision
from app.gateway.auth.mode import account_refusal
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.incarnations import verify_bound
from deerflow.spaces.provision import KINDS, plan_capacities, storage_fit
from deerflow.spaces.settlement import HOST_RESOLUTIONS, host_settlement_time
from deerflow.utils.file_io import await_drained, run_file_io

UNRESOLVED = ("starting", "running", "stopping", "uncertain")


def utc_isoformat(value):
    return value.replace(tzinfo=UTC).isoformat() if value.tzinfo is None else value.astimezone(UTC).isoformat()


def _hold():
    from app.gateway.storage_spaces import AGENTS_HOLD_PATH

    try:
        metadata = AGENTS_HOLD_PATH.stat()
    except FileNotFoundError:
        return None
    with AGENTS_HOLD_PATH.open(encoding="utf-8") as stream:
        reason = stream.readline(4096).strip() or None
    return {"since": datetime.fromtimestamp(metadata.st_mtime, UTC).isoformat(), "reason": reason}


def inspect_operator_volume(files, binding=None, *, slot=None):
    """Verify without publishing geometry, caches, markers or ownership."""
    if binding is not None:
        files._binding_spec(binding)
        slot = binding.slot_id
    volume = files.catalog.verify(slot, previous=files._verified.get(slot))
    if binding is not None:
        files._check_incarnation(binding, volume)
        volume = verify_bound(volume, binding.backing_handle)
    with volume.filesystem() as filesystem:
        usage = os.fstatvfs(filesystem._data_fd)
    empty = files._empty(volume) if binding is None else False
    return volume, (usage.f_blocks - usage.f_bfree) * usage.f_frsize, empty


class OperatorCommands(OperatorActions, OperatorSettlement, OperatorSupervision):
    def __init__(self, *, files, agents, repository, runs, config, adapter=None, adapter_error=None, probe_error=None, fit=None, storage_reason=None, storage_text=None, home):
        self.files, self.agents, self.repository, self.runs, self.config = files, agents, repository, runs, config
        self.adapter, self.adapter_error, self.probe_error = adapter, adapter_error, probe_error
        self.fit, self.storage_reason, self.storage_text, self.home = fit, storage_reason, storage_text, Path(home)
        self.attachments = self._attachment_factory = None

    async def run(self, command, **options):
        method = getattr(self, "_" + command.replace("-", "_"))
        return await method(**options)

    @staticmethod
    def _answer(command, note, *, returncode=0, **fields):
        return {"command": command, "returncode": returncode, "note": note, **fields}

    async def _require_daemon(self):
        try:
            if self.adapter is None:
                raise self.adapter_error or RuntimeError("Docker qualification is unavailable")
            await run_file_io(self.adapter._same_host)
        except Exception as exc:
            raise CommandRefused(f"the Docker daemon did not answer: {exc}") from None

    async def _snapshot(self, *, mode="list", since=None):
        """All durable facts are read without acquiring the application writer."""
        from app.gateway.storage_spaces import storage_allowance
        from deerflow.features.resources import StorageFeatureLinkRow

        async with self.files.registry._sf() as session, session.begin():
            result = {}
            for name, model in (("instances", AgentInstanceRow), ("spaces", SpaceRow), ("bindings", SpaceBackingRow), ("links", StorageFeatureLinkRow)):
                query = select(model)
                if model is AgentInstanceRow:
                    query = query.options(load_only(model.id, model.name, model.custody, model.owner_id, model.supervisor_id, model.home_id, model.status, model.generation, model.created_at, raiseload=True))
                result[name] = list((await session.scalars(query)).all())
            result["allowances"] = {kind: await storage_allowance(self.config.storage_spaces, self.files.catalog, self.files.registry._sf, kind, session=session) for kind in KINDS}
            if mode == "verify":
                return result
            active = SpaceAttachmentRow.phase != "fenced"
            result["attachments"] = list((await session.scalars(select(SpaceAttachmentRow).where(or_(active, SpaceAttachmentRow.fenced_at >= since) if since is not None else active))).all())
            if mode == "list":
                result["mounts"] = list((await session.scalars(select(SpaceMountRow).join(SpaceAttachmentRow, SpaceAttachmentRow.id == SpaceMountRow.attachment_id).where(active))).all())
                unresolved = select(AgentWorkRow.instance_id, func.count()).join(AgentWorkAttemptRow, AgentWorkAttemptRow.work_id == AgentWorkRow.id).where(AgentWorkAttemptRow.status.in_(UNRESOLVED)).group_by(AgentWorkRow.instance_id)
                result["attempt_counts"] = dict((await session.execute(unresolved)).all())
                instance = RunRow.metadata_json["agent_instance_id"].as_string()
                last = select(instance, func.max(RunRow.created_at)).where(RunRow.operation_kind == "run", instance.is_not(None)).group_by(instance)
                result["last_runs"] = {identifier: utc_isoformat(at) for identifier, at in (await session.execute(last)).all()}
            elif mode == "status":
                result["replacements"] = list((await session.scalars(select(StorageHostReplacementRow))).all())
                result["attempts_unresolved"] = await session.scalar(select(func.count()).select_from(AgentWorkAttemptRow).where(AgentWorkAttemptRow.status.in_(UNRESOLVED)))
                result["file_operations_pending"] = await session.scalar(select(func.count()).select_from(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))
                result["work_attempts_settled"] = await session.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "attempt_contained", AgentWorkEventRow.created_at >= since))
                resolution = SpaceFileOperationRow.result["resolution"].as_string()
                stamp = SpaceFileOperationRow.result["host_settled_at"].as_string()
                terminal = select(resolution, stamp).where(SpaceFileOperationRow.phase.in_(("complete", "failed")), resolution.in_(HOST_RESOLUTIONS), stamp >= utc_isoformat(since))
                result["file_operations_settled"] = sum(host_settlement_time({"resolution": value, "host_settled_at": at}) >= since for value, at in (await session.execute(terminal)).all())
        return result

    async def _slots(self, snapshot):
        bindings = {row.slot_id: row for row in snapshot["bindings"]}
        spaces = {row.id: row for row in snapshot["spaces"]}
        agents = {row.home_id: row for row in snapshot["instances"]}
        links = {row.space_id: row for row in snapshot["links"]}
        free = Counter()
        output = []
        allowances = snapshot["allowances"]
        bound_counts = Counter(getattr(self.files.catalog.volumes.get(row.slot_id), "kind", None) for row in bindings.values())
        for slot in sorted(set(self.files.catalog.volumes) | set(bindings)):
            spec, binding = self.files.catalog.volumes.get(slot), bindings.get(slot)
            space = spaces.get(binding.space_id) if binding is not None else None
            agent = agents.get(space.id) if space is not None else None
            link = links.get(space.id) if space is not None else None
            kind = getattr(spec, "kind", None)
            holder = agent.id if agent else "company" if link and link.namespace == "hm.company-space" else space.custodian_id if space else None
            entry = dict(
                slot_id=slot,
                kind=kind,
                listed=spec is not None,
                state="verified",
                reason=None,
                use=kind or ("assistant" if agent and agent.custody == "personal" else "employee" if agent else "company" if holder == "company" else "user"),
                holder_id=holder,
                space_id=space.id if space else None,
                space_status=space.status if space else None,
                used_bytes=None,
                max_bytes=spec.max_bytes if spec else binding.max_bytes,
                empty=None,
            )
            try:
                _proven, entry["used_bytes"], entry["empty"] = await run_file_io(inspect_operator_volume, self.files, binding, slot=slot)
            except Exception as exc:
                entry.update(state="unverified", reason=str(exc))
            if binding is not None:
                if space is None:
                    entry.update(state="unverified", reason="The durable binding has no Space")
                elif space.status == "deleted":
                    entry["use"] = "releasing"
            elif entry["state"] == "unverified" or not entry["empty"]:
                entry["use"] = "unclaimed"
            else:
                free[kind] += 1
                entry["use"] = "surplus" if kind in allowances and bound_counts[kind] + free[kind] > allowances[kind].allowed_key else "free"
            output.append(entry)
        return output, allowances

    async def _workspace(self, row):
        result = dict(attachment_id=row.id, phase=row.phase, container_id=row.container_id, host_id=row.host_id, container="unknown")
        if row.host_id != self.adapter.host_id:
            return result
        if row.container_id is not None:

            def inspect():
                self.adapter._same_host()
                entry = self.adapter._inspect(row.container_id)
                if entry is None:
                    result["container"] = "absent"
                else:
                    self.adapter._owned(entry, row.id)
                    running = (entry.get("State") or {}).get("Running")
                    if type(running) is not bool:
                        raise RuntimeError("Workspace container state is unconfirmed")
                    result["container"] = "running" if running else "exited"
                self.adapter._same_host()

            await run_file_io(inspect)
        return result

    async def _list(self, *, all=False):
        await self._require_daemon()
        snapshot = await self._snapshot()
        slots, _allowances = await self._slots(snapshot)
        by_space = {row["space_id"]: row for row in slots if row["space_id"] is not None}
        homes = {row.id: row for row in snapshot["spaces"]}
        unresolved = snapshot["attempt_counts"]
        live = Counter(row["metadata"].get("agent_instance_id") for row in await self.runs.list_inflight_by_metadata("agent_instance_id"))
        last = snapshot["last_runs"]
        hold, instances, deleted_released = await run_file_io(_hold), [], 0
        for row in sorted(snapshot["instances"], key=lambda row: (utc_isoformat(row.created_at), row.id)):
            home, slot = homes.get(row.home_id), by_space.get(row.home_id)
            released = row.status == "deleted" and slot is None
            deleted_released += int(released)
            if released and not all:
                continue
            supervisor = await self.repository.get_user_by_id(row.supervisor_id)
            owner = await self.repository.get_user_by_id(row.owner_id) if row.owner_id else None
            space = (
                None
                if home is None
                else dict(
                    space_id=home.id,
                    space_status=home.status,
                    space_generation=home.generation,
                    slot_id=slot["slot_id"] if slot else None,
                    max_bytes=slot["max_bytes"] if slot else None,
                    used_bytes=slot["used_bytes"] if slot else None,
                    verified=slot["state"] == "verified" if slot else None,
                    reason=slot["reason"] if slot else None,
                    release=("done" if released else "pending") if row.status == "deleted" else None,
                )
            )
            open_rows = [item for item in snapshot["attachments"] if item.phase != "fenced" and any(mount.space_id == row.home_id and mount.attachment_id == item.id for mount in snapshot["mounts"])]
            if len(open_rows) > 1:
                raise RuntimeError("The AI agent has multiple open workspace records")
            workspace = await self._workspace(open_rows[0]) if open_rows else None
            execution = reason = None
            if row.status not in ("provisioning", "deleted"):
                execution = "ready"
                if self.storage_reason:
                    reason = "storage-unavailable"
                elif open_rows and open_rows[0].host_id != self.adapter.host_id:
                    reason = "host-replaced"
                elif space is None or space["verified"] is not True:
                    reason = "space-unverified"
                elif hold is not None:
                    reason = "operator-hold"
                if reason:
                    execution = "unavailable"
            instances.append(
                dict(
                    id=row.id,
                    name=row.name,
                    custody=row.custody,
                    kind="assistant" if row.custody == "personal" else "employee",
                    status=row.status,
                    created_at=utc_isoformat(row.created_at),
                    owner_id=row.owner_id,
                    owner_live=None if row.custody == "company" else owner is not None and account_refusal(owner) is None,
                    supervisor={"id": row.supervisor_id, "email": supervisor.email if supervisor else None, "live": supervisor is not None and account_refusal(supervisor) is None},
                    space=space,
                    workspace=workspace,
                    execution=execution,
                    execution_reason=reason,
                    runs_in_flight=live[row.id],
                    attempts_unresolved=unresolved.get(row.id, 0),
                    last_run_at=last.get(row.id),
                )
            )
        await self._require_daemon()
        return self._answer("list", f"{len(instances)} AI agents listed; {deleted_released} deleted and released", count=len(instances), deleted_released=deleted_released, instances=instances)

    async def _verify(self):
        if self.storage_reason == "storage-pool-missing":
            raise CommandRefused(f"no inventory at {self.config.storage_spaces.inventory_path}; run: storage-init prepare")
        if self.storage_reason == "inventory-unreadable":
            raise CommandRefused(f"the inventory cannot be loaded: {self.storage_text}")
        snapshot = await self._snapshot(mode="verify")
        slots, allowances = await self._slots(snapshot)
        error = self.probe_error or self.adapter_error
        try:
            await self._require_daemon()
        except CommandRefused as exc:
            error = exc
        verified = sum(row["listed"] and row["state"] == "verified" for row in slots)
        total = len(self.files.catalog.volumes)
        unclaimed, surplus = (sum(row["use"] == use for row in slots) for use in ("unclaimed", "surplus"))
        code = 2 if error is not None or any(row["state"] != "verified" for row in slots) else 0
        return self._answer(
            "verify",
            f"{verified} of {total} Spaces verified; {unclaimed} unclaimed; {surplus} surplus",
            returncode=code,
            pool={"path": str(Path(self.config.storage_spaces.inventory_path).parent), "probe": "failed" if error else "ok", "probe_error": str(error) if error else None},
            plan={kind: {key: getattr(value, key) for key in ("allowed_key", "prepared", "in_use")} for kind, value in allowances.items()},
            spaces=slots,
            verified=verified,
            total=total,
            unclaimed=unclaimed,
            surplus=surplus,
        )

    async def _status(self):
        from app.gateway.gateway_observation import read
        from deerflow.spaces.docker import StorageHostUnqualified

        observed = await run_file_io(read, self.home)
        snapshot = await self._snapshot(mode="status", since=observed.started_at)
        slots, allowances = await self._slots(snapshot)
        reason, text = self.storage_reason, self.storage_text
        host, workspaces, chats = None, None, None
        try:
            await self._require_daemon()

            def running_names():
                reply = self.adapter._call("ps", "--no-trunc", "--format", "{{.Names}}")
                if reply.returncode:
                    raise RuntimeError(reply.stderr or "Docker container census failed")
                names = reply.stdout.splitlines()
                if len(names) != len(set(names)) or any(not name or name.strip() != name or any(c.isspace() for c in name) for name in names):
                    raise ValueError("Docker container census is malformed")
                self.adapter._same_host()
                return names

            names = await run_file_io(running_names)
            host = self.adapter.host_id
            workspaces = sum(name.startswith("hartmesh-space-") for name in names)
            chats = sum(name.startswith("deer-flow-sandbox-") for name in names)
        except Exception as exc:
            if reason is None:
                reason = "host-unqualified" if isinstance(self.adapter_error, StorageHostUnqualified) else "admission-failed"
                text = str(exc)
        if self.probe_error is not None and reason is None:
            reason, text = "admission-failed", str(self.probe_error)
        fit = self.fit or await run_file_io(storage_fit, Path(self.config.storage_spaces.inventory_path).parent, self.config.storage_spaces.plan, self.files.catalog)
        storage = "unavailable" if reason else "incomplete" if any(fit.missing.values()) else "ready"
        hold = await run_file_io(_hold)
        plan = self.config.storage_spaces.plan
        execution, execution_reason = (
            ("unavailable", "storage-unavailable") if storage == "unavailable" else ("none", "agents-none") if plan.ai_assistants_per_user == plan.ai_employees == 0 else ("held", "operator-hold") if hold else ("ready", None)
        )
        instances = {kind: dict.fromkeys(("provisioning", "active", "suspended", "archived", "deleted"), 0) for kind in ("assistant", "employee")}
        for row in snapshot["instances"]:
            instances["assistant" if row.custody == "personal" else "employee"][row.status] += 1
        active = [row for row in snapshot["attachments"] if row.phase != "fenced"]
        foreign = Counter(row.host_id for row in active if host is not None and row.host_id != host)
        adopted = {(row.old_host_id, row.new_host_id) for row in snapshot["replacements"]}
        fenced = Counter(row.fence_reason for row in snapshot["attachments"] if row.phase == "fenced" and row.fenced_by_kind == "host" and row.fenced_at is not None and utc_isoformat(row.fenced_at) >= utc_isoformat(observed.started_at))
        settled_files = snapshot["file_operations_settled"]
        settled_work = snapshot["work_attempts_settled"]
        runs = await self.runs.list_inflight_by_metadata("agent_instance_id")
        capacities = plan_capacities(plan)
        await run_file_io(observed.require_current, self.home)
        return self._answer(
            "status",
            f"storage {storage}; AI agents {execution}; {len(runs)} runs in flight",
            storage=storage,
            storage_reason=reason,
            execution=execution,
            reason=execution_reason,
            reason_text=text,
            spaces={kind: {**asdict(value), "unverified": sum(row["kind"] == kind and row["state"] == "unverified" for row in slots), "capacity_bytes": capacities[kind]} for kind, value in allowances.items()},
            per_user=plan.ai_assistants_per_user,
            unclaimed={kind: sum(row["kind"] == kind and row["use"] == "unclaimed" for row in slots) for kind in KINDS},
            surplus={kind: sum(row["kind"] == kind and row["use"] == "surplus" for row in slots) for kind in KINDS},
            reserved_bytes=sum(spec.max_bytes for spec in self.files.catalog.volumes.values()),
            required_bytes=sum(fit.required[kind] * capacities[kind] for kind in KINDS),
            headroom_bytes=fit.headroom_bytes,
            data_disk={"size_bytes": fit.disk_bytes, "free_bytes": fit.free_bytes, "free_inodes": fit.free_inodes},
            shortfall={"missing": fit.missing, "missing_bytes": fit.missing_bytes, "fits": fit.fits} if any(fit.missing.values()) else None,
            hold=hold,
            instances=instances,
            releases={"pending": sum(row["use"] == "releasing" for row in slots)},
            sandbox_slots={"limit": self.config.sandbox.replicas, "workspaces_running": workspaces, "chat_sandboxes_running": chats},
            runs_in_flight=len(runs),
            attachments_active=len(active),
            attempts_unresolved=snapshot["attempts_unresolved"],
            file_operations_pending=snapshot["file_operations_pending"],
            since_gateway_start={"at": utc_isoformat(observed.started_at), "attachments_fenced": dict(fenced), "work_attempts_settled": settled_work, "file_operations_settled": settled_files},
            host_id=host,
            foreign_hosts=[{"host_id": old, "attachments": count, "adopted": (old, host) in adopted} for old, count in sorted(foreign.items())],
            runtime=os.environ.get("SANDBOX_RUNTIME") or "default",
        )

    async def _adopt_host(self, *, old_host, acknowledge_old_host_gone=False):
        from sqlalchemy.exc import IntegrityError

        if not old_host:
            raise CommandRefused("give --from <Docker engine ID>")
        await self._require_daemon()
        host = self.adapter.host_id
        if old_host == host:
            raise CommandRefused(f"{old_host} is this Docker host; there is nothing to adopt")
        if len(old_host) > 128:
            raise CommandRefused("give --from <Docker engine ID>")
        for retry in range(2):
            try:
                async with self.files.registry._sf() as session, session.begin():
                    await self.agents._reserve_writer(session)
                    existing = await session.get(StorageHostReplacementRow, (old_host, host))
                    rows = list((await session.scalars(select(SpaceAttachmentRow.host_id).where(SpaceAttachmentRow.phase != "fenced"))).all())
                    count = rows.count(old_host)
                    if existing is not None:
                        count, verdict = existing.attachments_named, "already_adopted"
                    else:
                        if not count:
                            raise CommandRefused(f"no attachment names Docker host {old_host}; hosts on record: {', '.join(sorted(set(rows)))}")
                        if acknowledge_old_host_gone is not True:
                            raise CommandRefused(f"{count} attachments are held by Docker host {old_host}; confirm that VM is powered off or destroyed, then pass --acknowledge-old-host-gone", attachments_named=count)
                        await run_file_io(self.adapter._same_host)
                        session.add(StorageHostReplacementRow(old_host_id=old_host, new_host_id=host, attachments_named=count))
                        verdict = "adopted"
                break
            except IntegrityError:
                if retry:
                    raise
        return self._answer(
            "adopt-host",
            f"Docker host {old_host} recorded as replaced by {host}; {count} attachments are closed at the next Gateway start",
            **{"from": old_host},
            host_id=host,
            acknowledged=acknowledge_old_host_gone,
            attachments_named=count,
            verdict=verdict,
        )


def _operator_adapter(config, *, probe):
    from deerflow.community.aio_sandbox.aio_sandbox_provider import DEFAULT_CONTAINER_PREFIX, DEFAULT_IMAGE, DEFAULT_PORT
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.spaces.docker import DockerStorageAdapter

    sandbox = config.sandbox
    backend = LocalContainerBackend(
        image=sandbox.image or DEFAULT_IMAGE,
        base_port=sandbox.port or DEFAULT_PORT,
        container_prefix=sandbox.container_prefix or DEFAULT_CONTAINER_PREFIX,
        config_mounts=sandbox.mounts or [],
        environment=sandbox.environment or {},
        network_config=sandbox.network.model_dump(),
    )
    pool = Path(config.storage_spaces.inventory_path).parent
    adapter = DockerStorageAdapter.for_operator(backend, pool=pool)
    if probe:
        try:
            return DockerStorageAdapter.for_operator(backend, pool=pool, probe_pool=True), None
        except Exception as exc:
            return adapter, exc
    return adapter, None


@asynccontextmanager
async def operator_services(config, *, command, session_factory, discard=False):
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from app.gateway.storage_spaces import build_storage_services, read_storage_pool, storage_readonly_roots
    from deerflow.config.paths import get_paths
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.spaces.attachments import SpaceAttachments
    from deerflow.spaces.native_create_evidence import NativeEvidenceReader
    from deerflow.spaces.operator_containment import OperatorResidentContainmentScopes

    catalog, _errors, fit, reason, text = await await_drained(run_file_io(read_storage_pool, config.storage_spaces))
    repository = SQLiteUserRepository(session_factory)
    files, agents = build_storage_services(config.storage_spaces, catalog, session_factory, repository)
    adapter = adapter_error = probe_error = None
    if command != "settle" or discard:
        try:
            adapter, probe_error = await await_drained(run_file_io(_operator_adapter, config, probe=command in ("status", "verify")))
        except Exception as exc:
            adapter_error = exc
    owned = []
    try:
        service = OperatorCommands(
            files=files,
            agents=agents,
            repository=repository,
            runs=RunRepository(session_factory),
            config=config,
            adapter=adapter,
            adapter_error=adapter_error,
            probe_error=probe_error,
            fit=fit,
            storage_reason=reason,
            storage_text=text,
            home=get_paths().base_dir,
        )
        if adapter is not None and command in ("close-workspace", "export", "supervise", "delete"):

            @asynccontextmanager
            async def containment(attachments, captured):
                if not owned:

                    def begin():
                        owned.append(NativeEvidenceReader(Path(config.storage_spaces.inventory_path).parent, daemon_id=adapter.host_id))

                    await await_drained(run_file_io(begin))
                factory = OperatorResidentContainmentScopes(files=files, adapter=adapter, evidence=owned[0])
                async with factory(attachments, captured) as scope:
                    yield scope

            adapter._readonly_roots = lambda: storage_readonly_roots(files)
            adapter._containment_scope = containment

            async def attach_scope():
                service.attachments = SpaceAttachments(files, adapter)

            service._attachment_factory = attach_scope
        yield service
    finally:
        if owned:
            await await_drained(run_file_io(owned[0].close))


async def run(command, **options):
    from deerflow.config import get_app_config
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config

    try:
        config = await run_file_io(get_app_config)
    except Exception as exc:
        try:
            path = AppConfig.resolve_config_path()
        except Exception:
            path = os.environ.get("DEER_FLOW_CONFIG_PATH", "/srv/hartmesh/home/config.yaml")
        raise CommandRefused(f"the configuration {path} cannot be loaded: {exc}") from None
    if not config.storage_spaces.enabled:
        raise CommandRefused("Storage Spaces are off in the rendered configuration (HARTMESH_USERS absent); there is nothing to act on")
    try:
        try:
            if config.database.backend == "memory":
                raise RuntimeError("the memory backend has no durable operator records")
            await init_engine_from_config(config.database)
            session_factory = get_session_factory()
            if session_factory is None:
                raise RuntimeError("persistence engine unavailable")
        except Exception as exc:
            raise CommandRefused(f"the database cannot be reached: {exc}") from None
        async with operator_services(config, command=command, session_factory=session_factory, discard=command == "settle" and options.get("discard", False)) as service:
            return await service.run(command, **options)
    finally:
        await await_drained(close_engine())

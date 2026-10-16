"""Explicit operator effects; the Gateway retains run and Work ownership."""

import asyncio
import os
import stat
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import load_only

from app.gateway.agent_operator_contract import CommandFailed, CommandRefused
from deerflow.agent_instances.contract import AgentConflict, Refusal
from deerflow.agent_instances.host_operations import require_orphaned
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.spaces.contract import SpaceConflict
from deerflow.utils.file_io import await_drained, run_file_io

EXPORT_ROOT = Path("/srv/hartmesh/home/agent-exports")

_UNRESOLVED = ("starting", "running", "stopping", "uncertain")
_ENDED = ("success", "error", "timeout", "interrupted")


async def _run_action(operation, effects):
    try:
        return await operation()
    except CommandRefused as exc:
        if effects["changed"]:
            raise CommandFailed(str(exc), **exc.fields) from exc
        raise


def empty_closure():
    return dict(attachments=[], attachments_unconfirmed=[], runs_found=0, runs_ended=0, runs_unconfirmed=[], attempts_settled=[], attempts_unconfirmed=[], waited_seconds=0.0)


def _archive_headroom(pool, used):
    capacity = os.statvfs(pool)
    if capacity.f_bavail * capacity.f_frsize - used < 1 << 30:
        raise CommandRefused(f"the archive needs {used / (1 << 30):.1f} GiB and would leave the data disk under 1 GiB free")


def _external_archive(files, volume, instance_id):
    from deerflow.files.store import open_directory_source
    from deerflow.spaces import snapshots

    root = EXPORT_ROOT
    protected = [files.inventory_path.parent, volume.data_path, volume.control_path]
    protected.extend(spec.mount_path for spec in files.catalog.volumes.values() if hasattr(spec, "mount_path"))
    resolved = root.resolve()
    if any(resolved.is_relative_to(path.resolve()) or path.resolve().is_relative_to(resolved) for path in protected):
        raise ValueError("The export directory overlaps native storage or private control")
    parent = open_directory_source(root.parent)
    descriptor = None
    try:
        try:
            os.mkdir(root.name, 0o700, dir_fd=parent)
        except FileExistsError:
            pass
        descriptor = os.open(root.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        metadata = os.fstat(descriptor)
        if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("The export directory is not private to the Gateway user")
        name = instance_id + "-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + ".tar"
        digest, size = snapshots.backup(volume, instance_id, target=root / name, target_directory_fd=descriptor)
        archive_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
        try:
            os.fchmod(archive_fd, 0o600)
            current = os.fstat(archive_fd)
            if (current.st_uid, current.st_gid) != (1000, 1000):
                os.fchown(archive_fd, 1000, 1000)
            with os.fdopen(os.dup(archive_fd), "rb") as stream, tarfile.open(fileobj=stream) as archive:
                count = sum(1 for _member in archive)
            os.fsync(archive_fd)
        finally:
            os.close(archive_fd)
        os.fsync(descriptor)
        named = os.stat(root.name, dir_fd=parent, follow_symlinks=False)
        if (named.st_dev, named.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise RuntimeError("The export directory changed after archiving; retain the completed archive")
        return dict(path=str(root / name), sha256=digest, bytes=size, files=count)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


class OperatorActions:
    async def _agent(self, instance_id):
        async with self.files.registry._sf() as session:
            row = await session.get(AgentInstanceRow, instance_id)
        if row is None:
            raise CommandRefused(f"no AI assistant or AI employee has id {instance_id}")
        return row

    async def _open_workspace(self, instance):
        async with self.files.registry._sf() as session:
            query = (
                select(SpaceAttachmentRow)
                .join(SpaceMountRow, SpaceMountRow.attachment_id == SpaceAttachmentRow.id)
                .where(SpaceMountRow.space_id == instance.home_id, SpaceAttachmentRow.phase != "fenced")
                .order_by(SpaceAttachmentRow.created_at, SpaceAttachmentRow.id)
            )
            rows = list((await session.scalars(query)).all())
            replacements = set((await session.execute(select(StorageHostReplacementRow.old_host_id, StorageHostReplacementRow.new_host_id))).all())
        for row in rows:
            if row.host_id != self.adapter.host_id and (row.host_id, self.adapter.host_id) not in replacements:
                raise CommandRefused(f"attachment {row.id} was started by Docker host {row.host_id}, not this one; see adopt-host")
        return rows

    async def _unfinished_attempts(self, instance_id):
        async with self.files.registry._sf() as session:
            query = (
                select(AgentWorkAttemptRow.id)
                .join(AgentWorkRow, AgentWorkRow.id == AgentWorkAttemptRow.work_id)
                .where(AgentWorkRow.instance_id == instance_id, AgentWorkAttemptRow.status.in_(_UNRESOLVED))
                .order_by(AgentWorkAttemptRow.created_at, AgentWorkAttemptRow.id)
            )
            return list((await session.scalars(query)).all())

    async def _wait_closed_work(self, run_ids, attempt_ids, *, wait_seconds):
        if not run_ids and not attempt_ids:
            return dict(runs_found=0, runs_ended=0, runs_unconfirmed=[], attempts_settled=[], attempts_unconfirmed=[], waited_seconds=0.0)
        started = time.monotonic()
        deadline = asyncio.get_running_loop().time() + wait_seconds
        ended, settled = set(), []

        async def observe():
            async with self.files.registry._sf() as session:
                current_ended = set((await session.scalars(select(RunRow.run_id).where(RunRow.run_id.in_(run_ids), RunRow.operation_kind == "run", RunRow.status.in_(_ENDED)))).all()) if run_ids else set()
                attempts = (
                    list(
                        (
                            await session.scalars(
                                select(AgentWorkAttemptRow)
                                .options(
                                    load_only(AgentWorkAttemptRow.id, AgentWorkAttemptRow.work_id, AgentWorkAttemptRow.run_id, AgentWorkAttemptRow.status, AgentWorkAttemptRow.contained_reason, AgentWorkAttemptRow.settled_at, raiseload=True)
                                )
                                .where(AgentWorkAttemptRow.id.in_(attempt_ids))
                            )
                        ).all()
                    )
                    if attempt_ids
                    else []
                )
            current_settled = [dict(attempt_id=row.id, work_id=row.work_id, run_id=row.run_id, reason=row.contained_reason) for row in attempts if row.status in ("succeeded", "failed", "cancelled") and row.settled_at is not None]
            return current_ended, current_settled

        while True:
            try:
                async with asyncio.timeout_at(deadline):
                    ended, settled = await observe()
            except TimeoutError:
                break
            settled_ids = {row["attempt_id"] for row in settled}
            runs_unconfirmed, attempts_unconfirmed = [identifier for identifier in run_ids if identifier not in ended], [identifier for identifier in attempt_ids if identifier not in settled_ids]
            elapsed = time.monotonic() - started
            if not runs_unconfirmed and not attempts_unconfirmed or elapsed >= wait_seconds:
                break
            await asyncio.sleep(min(0.2, wait_seconds - elapsed))
        settled_ids = {row["attempt_id"] for row in settled}
        return dict(
            runs_found=len(run_ids),
            runs_ended=len(ended),
            runs_unconfirmed=[identifier for identifier in run_ids if identifier not in ended],
            attempts_settled=settled,
            attempts_unconfirmed=[identifier for identifier in attempt_ids if identifier not in settled_ids],
            waited_seconds=round(time.monotonic() - started, 3),
        )

    async def _close(self, instance, *, end_running_work, wait_seconds, wait_if_closed=False, domain_admission=None):
        from app.gateway.agent_operator_services import utc_isoformat

        await self._require_daemon()
        rows = await self._open_workspace(instance)
        if not rows and not wait_if_closed:
            return empty_closure()
        run_ids = [row["run_id"] for row in await self.runs.list_inflight_by_metadata("agent_instance_id", instance.id)]
        if run_ids and end_running_work is not True:
            count = len(run_ids)
            raise CommandRefused(f"{instance.id} has {count} run{'s' if count != 1 else ''} in flight; wait for {'them' if count != 1 else 'it'} to end or pass --end-running-work")
        attempt_ids = await self._unfinished_attempts(instance.id)

        async def perform():
            result = empty_closure()
            if rows and self.attachments is None:
                await self._attachment_factory()

            async def admission(session, identities):
                # Native proof can await filesystem/daemon I/O. Re-read the
                # durable run set under the actual Home containment transaction.
                live = list(
                    (
                        await session.scalars(
                            select(RunRow.run_id).where(
                                RunRow.operation_kind == "run", RunRow.status.in_(("pending", "running")), RunRow.lease_expires_at > datetime.now(UTC), RunRow.metadata_json["agent_instance_id"].as_string() == instance.id
                            )
                        )
                    ).all()
                )
                if live and end_running_work is not True:
                    count = len(live)
                    message = f"{instance.id} has {count} run{'s' if count != 1 else ''} in flight; wait for {'them' if count != 1 else 'it'} to end or pass --end-running-work"
                    if result["attachments"] or result["attachments_unconfirmed"]:
                        raise CommandFailed(message)
                    raise CommandRefused(message)
                run_ids.extend(identifier for identifier in live if identifier not in run_ids)
                current_attempts = list(
                    (await session.scalars(select(AgentWorkAttemptRow.id).join(AgentWorkRow, AgentWorkRow.id == AgentWorkAttemptRow.work_id).where(AgentWorkRow.instance_id == instance.id, AgentWorkAttemptRow.status.in_(_UNRESOLVED)))).all()
                )
                attempt_ids.extend(identifier for identifier in current_attempts if identifier not in attempt_ids)
                if domain_admission is not None:
                    try:
                        await domain_admission(session)
                    except CommandRefused as exc:
                        if result["attachments"] or result["attachments_unconfirmed"]:
                            raise CommandFailed(str(exc), **exc.fields) from exc
                        raise

            for original in rows:
                reason = "host-replaced" if original.host_id != self.adapter.host_id else "operator"
                confirmed = await self.attachments.contain_host(original.id, reason, admission=admission)
                async with self.files.registry._sf() as session:
                    row = await session.get(SpaceAttachmentRow, original.id)
                    if confirmed and row is not None and row.phase == "fenced" and row.fenced_at is not None:
                        result["attachments"].append(dict(attachment_id=row.id, container_id=row.container_id, fence_reason=row.fence_reason, fenced_at=utc_isoformat(row.fenced_at)))
                    else:
                        result["attachments_unconfirmed"].append(original.id)
            result.update(await self._wait_closed_work(run_ids, attempt_ids, wait_seconds=0 if result["attachments_unconfirmed"] else wait_seconds))
            return result

        return await await_drained(perform())

    async def _close_workspace(self, *, instance_id, end_running_work=False, wait_seconds=120):
        instance = await self._agent(instance_id)
        result = await self._close(instance, end_running_work=end_running_work, wait_seconds=wait_seconds)
        if result["attachments_unconfirmed"]:
            async with self.files.registry._sf() as session:
                row = await session.get(SpaceAttachmentRow, result["attachments_unconfirmed"][0])
            name = row.container_id[:12] if row and row.container_id else result["attachments_unconfirmed"][0]
            verdict, code = "unconfirmed", 2
            note = f"the Docker daemon did not confirm that container {name} is gone; its record is not marked closed; run the command again"
        elif result["attachments"]:
            verdict, code = "closed", 0
            note = f"workspace of {instance_id} closed: {len(result['attachments'])} container removed; {result['runs_ended']} run ended; the next message starts a new workspace"
        else:
            verdict, code = "already_closed", 0
            note = f"workspace of {instance_id} is already closed; the next message starts a new workspace"
        if result["runs_unconfirmed"] or result["attempts_unconfirmed"]:
            code = 2
            note += f"; {len(result['runs_unconfirmed'])} run had not ended after {wait_seconds:g} s; the Gateway settles its attempt when it ends; run the command again to see"
        return self._answer("close-workspace", note, returncode=code, instance_id=instance_id, verdict=verdict, **result)

    async def _delete(self, *, instance_id, end_running_work=False, wait_seconds=120):
        instance = await self._agent(instance_id)
        await self._require_daemon()

        async def remove(*, dry_run):
            try:
                return await self.agents.host_delete(instance_id, host_id=self.adapter.host_id, dry_run=dry_run)
            except Refusal as exc:
                raise CommandRefused(str(exc), reason=exc.reason) from None
            except (AgentConflict, SpaceConflict) as exc:
                if instance.status == "provisioning":
                    raise CommandRefused(f"{instance_id} is not set up yet (status provisioning); the Gateway removes it 24 hours after it was created") from None
                raise CommandRefused(f"{instance_id} was not deleted: {exc}") from None

        await remove(dry_run=True)
        async with self.files.registry._sf() as session:
            original = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == instance.home_id))
        slot = original.slot_id if original else None
        owner = await self.repository.get_user_by_id(instance.owner_id) if instance.owner_id else None
        owner_document = {"id": instance.owner_id, "email": owner.email if owner else None} if instance.custody == "personal" else None
        kind = "assistant" if instance.custody == "personal" else "employee"

        async def admission(session):
            current = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update().execution_options(populate_existing=True))
            if current is None or current.home_id != instance.home_id:
                raise RuntimeError("The AI agent's Home changed before deletion containment")
            try:
                await require_orphaned(self.agents, session, current)
            except Refusal as exc:
                raise CommandRefused(str(exc), reason=exc.reason) from None

        effects = {"changed": False}

        async def perform():
            closure = empty_closure() if instance.status == "deleted" else await self._close(instance, end_running_work=end_running_work, wait_seconds=wait_seconds, wait_if_closed=True, domain_admission=admission)
            effects["changed"] = bool(closure["attachments"] or closure["attachments_unconfirmed"])
            workspace_closed = bool(closure["attachments"])
            if closure["attachments_unconfirmed"]:
                async with self.files.registry._sf() as session:
                    row = await session.get(SpaceAttachmentRow, closure["attachments_unconfirmed"][0])
                name = row.container_id[:12] if row and row.container_id else closure["attachments_unconfirmed"][0]
                raise CommandRefused(f"the Docker daemon did not confirm that container {name} is gone; nothing was deleted; run the command again")
            if closure["runs_unconfirmed"] or closure["attempts_unconfirmed"]:
                return self._answer(
                    "delete",
                    f"workspace of {instance_id} closed; {len(closure['runs_unconfirmed'])} run had not ended after {wait_seconds:g} s; nothing was deleted; run the command again",
                    returncode=2,
                    instance_id=instance_id,
                    kind=kind,
                    verdict="workspace_closed",
                    owner=owner_document,
                    space={"space_id": instance.home_id, "slot_id": slot, "release": None, "use": kind if slot else None},
                    workspace_closed=workspace_closed,
                    **closure,
                )
            if instance.status != "deleted":
                await remove(dry_run=False)
                effects["changed"] = True
            released = original is None
            started = time.monotonic()
            deadline = asyncio.get_running_loop().time() + wait_seconds
            while not released and asyncio.get_running_loop().time() < deadline:
                try:
                    async with asyncio.timeout_at(deadline):
                        async with self.files.registry._sf() as session:
                            released = await session.scalar(select(SpaceBackingRow.space_id).where(SpaceBackingRow.space_id == instance.home_id)) is None
                    if not released:
                        await asyncio.sleep(min(0.2, max(0, deadline - asyncio.get_running_loop().time())))
                except TimeoutError:
                    break
            closure["waited_seconds"] += round(time.monotonic() - started, 3)
            use = None if slot is None else "releasing"
            if released and slot is not None:
                snapshot = await self._snapshot(mode="verify")
                slots, _allowances = await self._slots(snapshot)
                use = next((row["use"] for row in slots if row["slot_id"] == slot), "unclaimed")
            code = 2 if not released or use == "unclaimed" else 0
            note = f"AI {kind} {instance_id} deleted"
            if not released:
                note += f"; the Gateway had not freed slot {slot} after {wait_seconds:g} s; agent_instances list shows when it has"
            elif use == "unclaimed":
                note += f"; slot {slot} could not be verified and was released without being emptied; reclaim it with: storage-init forget {slot} --discard"
            else:
                note += f"; slot {slot} released" if slot else "; its Space holds no slot"
            return self._answer(
                "delete",
                note,
                returncode=code,
                instance_id=instance_id,
                kind=kind,
                verdict="already_deleted" if instance.status == "deleted" else "deleted",
                owner=owner_document,
                space={"space_id": instance.home_id, "slot_id": slot, "release": "done" if released else "pending", "use": use},
                workspace_closed=workspace_closed,
                **closure,
            )

        return await await_drained(_run_action(perform, effects))

    async def _export(self, *, instance_id, end_running_work=False, wait_seconds=120):
        from app.gateway.agent_operator_services import inspect_operator_volume
        from deerflow.persistence.spaces.model import SpaceRow
        from deerflow.spaces.recovery import SpaceRecovery

        instance = await self._agent(instance_id)
        if instance.status == "deleted":
            raise CommandRefused(f"{instance_id} is deleted; its files were deleted with it")
        if instance.home_id is None or instance.status == "provisioning":
            raise CommandRefused(f"{instance_id} has no files yet (status provisioning)")

        async def verified():
            async with self.files.registry._sf() as session:
                row = await session.get(SpaceRow, instance.home_id)
                binding = await session.get(SpaceBackingRow, row.backing_handle) if row else None
            try:
                if binding is None or binding.space_id != instance.home_id:
                    raise ValueError("The resource has no qualified filesystem binding")
                volume, used, _empty = await run_file_io(inspect_operator_volume, self.files, binding)
                return binding, volume, used
            except Exception as exc:
                slot = binding.slot_id if binding is not None else "unassigned"
                raise CommandRefused(f"space {instance.home_id} (slot {slot}) is unverified: {exc}; see storage-init verify") from None

        binding, _initial_volume, used = await verified()
        await run_file_io(_archive_headroom, self.files.inventory_path.parent, used)

        effects = {"changed": False}

        async def perform():
            closure = await self._close(instance, end_running_work=end_running_work, wait_seconds=wait_seconds, wait_if_closed=True)
            effects["changed"] = bool(closure["attachments"] or closure["attachments_unconfirmed"])
            if closure["attachments_unconfirmed"]:
                async with self.files.registry._sf() as session:
                    record = await session.get(SpaceAttachmentRow, closure["attachments_unconfirmed"][0])
                name = record.container_id[:12] if record and record.container_id else closure["attachments_unconfirmed"][0]
                raise CommandRefused(f"the Docker daemon did not confirm that container {name} is gone; nothing was exported; run the command again")
            async with self.files.registry.host_locked([instance.home_id]) as (session, rows):
                current = await session.get(AgentInstanceRow, instance_id, populate_existing=True)
                row = rows.get(instance.home_id)
                actual = await session.get(SpaceBackingRow, row.backing_handle) if row else None
                if current is None or current.home_id != instance.home_id or current.status in ("deleted", "provisioning") or actual is None or actual.backing_handle != binding.backing_handle or actual.slot_id != binding.slot_id:
                    raise RuntimeError("The AI agent's Home changed before export; run the command again")
                await SpaceRecovery._require_fenced(session, instance.home_id)
                volume, used, _empty = await run_file_io(inspect_operator_volume, self.files, actual)
                await run_file_io(_archive_headroom, self.files.inventory_path.parent, used)
                archive = await run_file_io(_external_archive, self.files, volume, instance_id)
                effects["changed"] = True
            code = 2 if closure["runs_unconfirmed"] or closure["attempts_unconfirmed"] else 0
            return self._answer(
                "export",
                f"files of {instance_id} exported: {archive['files']} entries, {archive['bytes'] / (1 << 20):.1f} MiB; nothing deletes this file",
                returncode=code,
                instance_id=instance_id,
                verdict="exported",
                space={"space_id": instance.home_id, "slot_id": binding.slot_id, "kind": "assistant" if instance.custody == "personal" else "employee"},
                workspace_closed=bool(closure["attachments"]),
                **archive,
                **closure,
            )

        return await await_drained(_run_action(perform, effects))

"""Explicit quiesced resource lifecycle; current grants are never restored."""

import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceBackupRow
from deerflow.persistence.spaces.model import SpaceEventRow
from deerflow.spaces import snapshots
from deerflow.spaces.contract import MutationMode, Permission, SpaceConflict, SpaceDenied, validate_generation
from deerflow.spaces.filesystem import FileConflict
from deerflow.spaces.service import SpaceOperationPending, _reservation
from deerflow.utils.file_io import await_drained, run_file_io


@dataclass(frozen=True)
class Backup:
    id: str
    space_id: str
    generation: int
    size_bytes: int
    sha256: str
    consistency: str = "quiesced-filesystem"


class SpaceRecovery:
    def __init__(self, files, *, attachments=None):
        self.files, self.registry = files, files.registry
        self.attachments = attachments or getattr(files, "attachments", None)

    async def _quiesce(self, actor, space_id, generation, operation_id=None, attachment_ids=None):
        # The installed service denies ADMIN while any native attachment
        # remains. The explicit provider closes RO handles as well as writers.
        if self.attachments is not None and attachment_ids != []:
            await self.attachments.retire(actor=actor, space_id=space_id, expected_generation=generation, operation_id=operation_id, attachment_ids=attachment_ids)

    @staticmethod
    def _advance(session, row, actor, action, details):
        if row.generation >= 2**31 - 1:
            raise SpaceConflict("Resource generation is exhausted")
        row.generation += 1
        row.updated_at = datetime.now(UTC)
        session.add(SpaceEventRow(space_id=row.id, generation=row.generation, action=action, actor_kind=actor.kind, actor_id=actor.subject_id, details=details))

    async def _run(self, *, actor, space_id, expected_generation, operation_id, request, execute):
        validate_generation(expected_generation)
        self.files._operation_id(operation_id)
        requests = {space_id: (Permission.ADMIN, expected_generation)}

        async def perform():
            view = await self.registry.get(actor=actor, space_id=space_id, permission=Permission.ADMIN, expected_generation=expected_generation)
            if view.mode != MutationMode.NATIVE:
                raise SpaceDenied("A mediated resource requires its bound recovery controller")
            # A retry/conflicting ID must not stop an environment created
            # after the original outcome. Check it before external effects.
            async with self.registry._sf() as session:
                existing = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                if existing is not None:
                    self.files._same_operation(existing, actor, expected_generation, request)
                    if request["action"] == "backup" and existing.phase == "complete":
                        return Backup(**existing.result["value"])
                    raise SpaceOperationPending("Lifecycle operation already has a durable outcome; inspect it before continuing")
            token = _reservation.set(space_id)
            try:
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(space_id, operation_id)) as (session, rows):
                    row, _ = rows[space_id]
                    if row.mode != MutationMode.NATIVE:
                        raise SpaceDenied("A mediated resource requires its bound recovery controller")
                    if row.generation >= 2**31 - 1:
                        raise SpaceConflict("Resource generation is exhausted")
                    existing = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                    if existing is not None:
                        self.files._same_operation(existing, actor, expected_generation, request)
                        if request["action"] == "backup" and existing.phase == "complete":
                            return Backup(**existing.result["value"])
                        raise SpaceOperationPending("Lifecycle operation already has a durable outcome; inspect it before continuing")
                    await self.files._volume(session, row)
                    from deerflow.spaces.attachments import SpaceAttachments

                    attachment_ids = sorted({a.id for a, _ in await SpaceAttachments._current(session, [space_id], lock=True)})
                    session.add(
                        SpaceFileOperationRow(
                            space_id=space_id, operation_id=operation_id, actor_kind=actor.kind, actor_id=actor.subject_id, generation=expected_generation, phase="pending", request=request, result={"attachment_ids": attachment_ids}
                        )
                    )
            finally:
                _reservation.reset(token)
            try:
                await self._quiesce(actor, space_id, expected_generation, operation_id, attachment_ids)
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(space_id, operation_id)) as (session, rows):
                    row, grant = rows[space_id]
                    volume = await self.files._volume(session, row)
                    operation = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                    self.files._same_operation(operation, actor, expected_generation, request)
                    result = await execute(session, row, grant, volume)
                    operation.phase = "complete"
                    operation.result = {**(operation.result or {}), "value": asdict(result)}
                    if request["action"] == "restore":
                        operation.result["cleanup"] = "pending"
            except FileConflict:
                # Snapshot rejection precedes public namespace mutation.
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(space_id, operation_id)) as (session, _):
                    operation = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                    operation.phase = "failed"
                raise
            except Exception as exc:
                raise SpaceOperationPending("Lifecycle outcome remains pending; preserve private recovery state") from exc
            if request["action"] == "restore":
                await self._cleanup_restore(actor, space_id, operation_id)
            return result

        return await await_drained(perform())

    async def backup(self, *, actor, space_id, expected_generation, operation_id):
        backup_id = operation_id

        async def execute(session, row, _grant, volume):
            checksum, size = await run_file_io(snapshots.backup, volume, backup_id)
            record = SpaceBackupRow(space_id=space_id, id=backup_id, generation=row.generation, size_bytes=size, sha256=checksum, consistency="quiesced-filesystem", actor_kind=actor.kind, actor_id=actor.subject_id)
            session.add(record)
            return Backup(backup_id, space_id, row.generation, size, checksum)

        return await self._run(actor=actor, space_id=space_id, expected_generation=expected_generation, operation_id=operation_id, request={"action": "backup", "backup_id": backup_id}, execute=execute)

    async def restore(self, *, actor, space_id, expected_generation, operation_id, backup_id):
        if not isinstance(backup_id, str) or not re.fullmatch(r"[0-9a-f]{32}", backup_id):
            raise ValueError("A durable resource backup identity is required")

        async def execute(session, row, grant, volume):
            backup = await session.get(SpaceBackupRow, (space_id, backup_id))
            if backup is None or backup.consistency != "quiesced-filesystem":
                raise FileConflict("Qualified resource backup is unavailable")
            await run_file_io(snapshots.restore, volume, backup_id, backup.sha256, backup.size_bytes, operation_id)
            self._advance(session, row, actor, "restore", {"backup_id": backup_id})
            return self.registry._view(row, grant.permissions)

        return await self._run(actor=actor, space_id=space_id, expected_generation=expected_generation, operation_id=operation_id, request={"action": "restore", "backup_id": backup_id}, execute=execute)

    async def archive(self, *, actor, space_id, expected_generation, operation_id):
        async def execute(session, row, grant, _volume):
            if row.status != "active":
                raise FileConflict("Resource is already archived")
            row.status = "archived"
            self._advance(session, row, actor, "archive", {})
            return self.registry._view(row, grant.permissions)

        return await self._run(actor=actor, space_id=space_id, expected_generation=expected_generation, operation_id=operation_id, request={"action": "archive"}, execute=execute)

    async def _cleanup_restore(self, actor, space_id, operation_id):
        try:
            async with self.registry.admitted(actor=actor, requests={space_id: (Permission.ADMIN, None)}) as (session, rows):
                operation = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                if operation.phase != "complete" or operation.result.get("cleanup") != "pending":
                    return
                volume = await self.files._volume(session, rows[space_id][0])
                await run_file_io(snapshots.cleanup_restore, volume, operation_id)
                operation.result = {**operation.result, "cleanup": "complete"}
        except Exception:
            # Main restore committed. Failed reclamation retains an explicit
            # cleanup fact and consumes capacity; never replay public restore.
            return

    async def delete(self, *, actor, space_id, expected_generation, operation_id):
        async def execute(session, row, grant, volume):
            await run_file_io(snapshots.delete_visible, volume)
            row.status = "deleted"
            self._advance(session, row, actor, "delete", {"retained_backups": True, "retained_binding": True})
            return self.registry._view(row, grant.permissions)

        return await self._run(actor=actor, space_id=space_id, expected_generation=expected_generation, operation_id=operation_id, request={"action": "delete"}, execute=execute)

    async def status(self, *, actor, space_id):
        # Reading durable recovery facts must work while file access is pending.
        await self.registry.get(actor=actor, space_id=space_id, permission=Permission.ADMIN)
        async with self.registry._sf() as session:
            operations = (
                (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space_id).order_by((SpaceFileOperationRow.phase == "pending").desc(), SpaceFileOperationRow.created_at.desc()).limit(200)))
                .scalars()
                .all()
            )
            backups = (await session.execute(select(SpaceBackupRow).where(SpaceBackupRow.space_id == space_id).order_by(SpaceBackupRow.created_at.desc()).limit(200))).scalars().all()
            from deerflow.spaces.attachments import SpaceAttachments

            attachments = {a.id: a for a, _ in await SpaceAttachments._current(session, [space_id])}
            return {
                "operations": [{"operation_id": op.operation_id, "generation": op.generation, "phase": op.phase, "request": op.request, "result": op.result} for op in operations],
                "backups": [asdict(Backup(b.id, b.space_id, b.generation, b.size_bytes, b.sha256, b.consistency)) for b in backups],
                "attachments": [{"id": a.id, "incarnation": a.incarnation, "phase": a.phase, "actor_kind": a.actor_kind, "actor_id": a.actor_id} for a in attachments.values()],
                "can_fence": self.attachments is not None,
            }

    async def accept_current_state(self, *, actor, space_id, expected_generation, operation_id, acknowledge_uncertain_outcome):
        """Owner accepts observed bytes, without asserting or replaying an intent.

        Recovery of partial restore/delete advances the lifecycle generation.
        Private displaced trees are retained for deliberate operator inspection.
        """
        if acknowledge_uncertain_outcome is not True:
            raise ValueError("Explicit acknowledgement of uncertain file state is required")
        self.files._operation_id(operation_id)
        validate_generation(expected_generation)
        requests = {space_id: (Permission.ADMIN, expected_generation)}

        async def perform():
            token = _reservation.set(space_id)
            try:
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(space_id, operation_id)) as (session, rows):
                    operation = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                    if operation is None or operation.phase != "pending":
                        raise SpaceConflict("No matching pending operation exists")
                    if rows[space_id][0].mode != MutationMode.NATIVE:
                        raise SpaceDenied("Required mediated recovery controller is unavailable")
                    from deerflow.spaces.attachments import SpaceAttachments

                    attachment_ids = sorted({a.id for a, _ in await SpaceAttachments._current(session, [space_id], lock=True)})
            finally:
                _reservation.reset(token)
            await self._quiesce(actor, space_id, expected_generation, operation_id, attachment_ids)
            async with self.registry.admitted(actor=actor, requests=requests, operation_id=(space_id, operation_id)) as (session, rows):
                row, _grant = rows[space_id]
                if row.mode != MutationMode.NATIVE:
                    raise SpaceDenied("Required mediated recovery controller is unavailable")
                operation = await session.get(SpaceFileOperationRow, (space_id, operation_id))
                if operation is None or operation.phase != "pending":
                    raise SpaceConflict("No matching pending operation exists")
                await self.files._volume(session, row)
                if operation.request.get("action") in ("restore", "delete", "archive"):
                    self._advance(session, row, actor, "recovery", {"operation_id": operation_id, "resolution": "owner-accepted-current-state"})
                operation.phase = "failed"
                operation.result = {"resolution": "owner-accepted-current-state", "actor_kind": actor.kind, "actor_id": actor.subject_id}

        await await_drained(perform())

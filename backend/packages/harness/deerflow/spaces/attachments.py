"""Host-owned native views and durable single-host containment admission.

The caller supplies an already authenticated actor and an execution incarnation,
not a chat ID. A trusted provider prepares a stopped environment; only this
service starts it after recording its exact identity. No lease can release it.
"""

import re
import uuid
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import MutationMode, Permission, PrincipalRef, SpaceConflict, SpaceDenied, validate_generation
from deerflow.spaces.registry import _stored_permissions
from deerflow.spaces.service import _reservation
from deerflow.utils.file_io import await_drained, run_file_io

_operation = ContextVar("storage_attachment_operation", default=None)


class AttachmentPending(SpaceConflict):
    """Containment is unconfirmed; preserve attachment and resource state."""


@dataclass(frozen=True)
class ResourceMount:
    space_id: str
    generation: int
    alias: str
    writable: bool = False

    def __post_init__(self):
        if not isinstance(self.space_id, str) or not re.fullmatch(r"[0-9a-f]{32}", self.space_id):
            raise ValueError("A stable resource identity is required")
        if type(self.generation) is not int or not 1 <= self.generation <= 2**31 - 1:
            raise ValueError("A current resource generation is required")
        if not isinstance(self.alias, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", self.alias) or type(self.writable) is not bool:
            raise ValueError("Mount aliases must be single safe names with an explicit mode")


@dataclass(frozen=True)
class NativeView:
    space_id: str
    generation: int
    source: str
    destination: str
    writable: bool


@dataclass(frozen=True)
class AttachmentPlan:
    id: str
    incarnation: str
    actor: PrincipalRef
    host_id: str
    views: tuple[NativeView, ...]


@dataclass(frozen=True)
class Attachment:
    id: str
    incarnation: str
    host_id: str
    container_id: str


class SpaceAttachments:
    def __init__(self, files, provider):
        self.files, self.registry, self.provider = files, files.registry, provider
        files.attachment_guard = self._guard
        files.attachments = self

    @staticmethod
    async def _current(session, resource_ids, *, lock=False):
        query = (
            select(SpaceAttachmentRow, SpaceMountRow)
            .join(SpaceMountRow, SpaceMountRow.attachment_id == SpaceAttachmentRow.id)
            .where(SpaceMountRow.space_id.in_(resource_ids), SpaceAttachmentRow.phase != "fenced")
            .order_by(SpaceAttachmentRow.id)
        )
        if lock:
            query = query.with_for_update(of=SpaceAttachmentRow)
        return (await session.execute(query)).all()

    async def _guard(self, session, admitted, requests):
        action = _operation.get()
        for attachment, mount in await self._current(session, admitted):
            if action == ("retire", None) or action == ("attach", attachment.id):
                continue
            permission = requests[mount.space_id][0]
            if _reservation.get() == mount.space_id and permission == Permission.ADMIN:
                # Host lifecycle reservation records intent/scope only. It
                # performs no filesystem or provider operation in this context.
                continue
            if attachment.phase not in ("active", "pending", "fence_pending"):
                raise AttachmentPending("Invalid persisted attachment state")
            if attachment.phase != "active":
                raise AttachmentPending("A storage attachment has an unconfirmed outcome")
            if permission & Permission.ADMIN or (permission & Permission.WRITE and (action is None or mount.writable)):
                raise SpaceConflict("The storage attachment must be fenced before this operation")

    async def _audience(self, session, rows, resources, acknowledgement):
        destinations = [r.space_id for r in resources if r.writable]
        if not destinations:
            return
        for resource in resources:
            if any(key != resource.space_id for key in destinations) and not rows[resource.space_id][1].permissions & Permission.EXPORT:
                raise SpaceDenied("Joint views require source EXPORT for cross-resource copying")
        audiences = {}
        for key, (row, _) in rows.items():
            grants = (await session.execute(select(SpaceGrantRow).where(SpaceGrantRow.space_id == key))).scalars().all()
            audiences[key] = {(g.principal_kind, g.subject_id) for g in grants if _stored_permissions(g.permissions, MutationMode(row.mode)) & (Permission.READ | Permission.EXPORT)}
        destination_readers = set().union(*(audiences[key] for key in destinations))
        for key, (_, grant) in rows.items():
            if destination_readers - audiences[key] and (not grant.permissions & Permission.ADMIN or acknowledgement is not True):
                raise SpaceDenied("Joint mount audience requires source disclosure authority and acknowledgement")

    async def resume(self, *, actor, resources, incarnation=None):
        """Revalidate an exact durable active view without preparing or starting.

        Absence returns None. Pending containment never means absence. A host
        may select its sole existing resource view after restart; it may not
        shrink, widen, relabel or change the actor of that environment.
        """
        if not isinstance(resources, (tuple, list)) or not 1 <= len(resources) <= 32 or any(not isinstance(r, ResourceMount) for r in resources):
            raise ValueError("Request bounded typed resource mounts")
        if len({r.space_id for r in resources}) != len(resources) or len({r.alias for r in resources}) != len(resources):
            raise ValueError("Duplicate resource roots or aliases are unsupported")
        if incarnation is not None and (not isinstance(incarnation, str) or not re.fullmatch(r"[0-9a-f]{32}", incarnation)):
            raise ValueError("An exact execution incarnation is required")
        if not isinstance(actor, PrincipalRef):
            raise SpaceDenied("An authenticated resource actor is required")
        async with self.registry._sf() as session:
            query = select(SpaceAttachmentRow).where(SpaceAttachmentRow.actor_kind == actor.kind, SpaceAttachmentRow.actor_id == actor.subject_id)
            if incarnation is not None:
                query = query.where(SpaceAttachmentRow.incarnation == incarnation)
            else:
                query = query.join(SpaceMountRow).where(SpaceMountRow.space_id.in_([r.space_id for r in resources]), SpaceAttachmentRow.phase != "fenced").distinct()
            candidates = (await session.execute(query)).scalars().all()
        if not candidates:
            # Still authenticate/read the requested resources before returning
            # absence, so a foreign caller cannot use this as a discovery API.
            async with self.registry.admitted(actor=actor, requests={r.space_id: (Permission.READ, r.generation) for r in resources}):
                return None
        if len(candidates) != 1:
            raise SpaceConflict("The requested resources do not identify one attachment")
        candidate = candidates[0]
        token = _operation.set(("attach", candidate.id))
        try:
            requests = {r.space_id: (Permission.READ | (Permission.WRITE if r.writable else Permission(0)), r.generation) for r in resources}
            async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                record = (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.id == candidate.id).with_for_update())).scalar_one()
                if record.phase != "active" or record.host_id != self.provider.host_id or record.container_id is None:
                    raise AttachmentPending("The recorded environment is not a confirmed active attachment")
                mounted = (await session.execute(select(SpaceMountRow).where(SpaceMountRow.attachment_id == record.id))).scalars().all()
                if {(m.space_id, m.generation, m.alias, bool(m.writable)) for m in mounted} != {(r.space_id, r.generation, r.alias, r.writable) for r in resources}:
                    raise SpaceConflict("The durable attachment differs from the requested view")
                await self._audience(session, rows, resources, False)
                volumes = {key: await self.files._volume(session, row) for key, (row, _) in rows.items()}
                plan = AttachmentPlan(record.id, record.incarnation, actor, self.provider.host_id, tuple(NativeView(r.space_id, r.generation, str(volumes[r.space_id].data_path), "/mnt/spaces/" + r.alias, r.writable) for r in resources))
                await run_file_io(self.provider.verify_active, record.container_id, plan)
                return Attachment(record.id, record.incarnation, record.host_id, record.container_id)
        except (SpaceDenied, SpaceConflict, ValueError):
            raise
        except Exception as exc:
            raise AttachmentPending("Attachment reuse is unconfirmed; preserve its containment record") from exc
        finally:
            _operation.reset(token)

    async def attach(self, *, actor, incarnation, resources, acknowledge_disclosure=False):
        if not isinstance(incarnation, str) or not re.fullmatch(r"[0-9a-f]{32}", incarnation):
            raise ValueError("A distinct execution incarnation UUID is required")
        if not isinstance(resources, (list, tuple)) or not 1 <= len(resources) <= 32 or any(not isinstance(r, ResourceMount) for r in resources):
            raise ValueError("Request one to32 typed resource mounts")
        if len({r.space_id for r in resources}) != len(resources) or len({r.alias for r in resources}) != len(resources):
            raise ValueError("Duplicate resource roots or aliases are unsupported")
        attachment_id = uuid.uuid4().hex
        requests = {r.space_id: (Permission.READ | (Permission.WRITE if r.writable else Permission(0)), r.generation) for r in resources}

        async def perform():
            token = _operation.set(("attach", attachment_id))
            try:
                async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                    await self._audience(session, rows, resources, acknowledge_disclosure)
                    current = await self._current(session, rows)
                    if any(sum(m.space_id == resource.space_id for _a, m in current) >= 32 for resource in resources):
                        raise SpaceConflict("Resource attachment capacity is exhausted")
                    if (await session.execute(select(SpaceAttachmentRow.id).where(SpaceAttachmentRow.incarnation == incarnation))).first():
                        raise SpaceConflict("Execution incarnation already has an attachment; use its durable outcome")
                    for resource in resources:
                        row, _ = rows[resource.space_id]
                        if row.status != "active" and resource.writable:
                            raise SpaceDenied("Archived resources cannot be writable")
                        await self.files._volume(session, row)
                    session.add(SpaceAttachmentRow(id=attachment_id, incarnation=incarnation, actor_kind=actor.kind, actor_id=actor.subject_id, host_id=self.provider.host_id, phase="pending"))
                    await session.flush()
                    session.add_all([SpaceMountRow(attachment_id=attachment_id, space_id=r.space_id, generation=r.generation, alias=r.alias, writable=int(r.writable)) for r in resources])
                # Persist intent before any external environment work. The
                # prepared environment must remain stopped until start below.
                async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                    record = (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.id == attachment_id).with_for_update())).scalar_one()
                    if record.phase != "pending":
                        raise AttachmentPending("Attachment preparation was retired")
                    volumes = {key: await self.files._volume(session, row) for key, (row, _) in rows.items()}
                    plan = AttachmentPlan(attachment_id, incarnation, actor, self.provider.host_id, tuple(NativeView(r.space_id, r.generation, str(volumes[r.space_id].data_path), "/mnt/spaces/" + r.alias, r.writable) for r in resources))
                    container = await run_file_io(self.provider.prepare, plan)
                    if not isinstance(container, str) or not re.fullmatch(r"[0-9a-f]{64}", container):
                        raise AttachmentPending("Provider did not return an exact Docker identity")
                    record.container_id = container
                # The immutable ID commits before start. If start/commit loses
                # its reply, pending never becomes a second writer invitation.
                async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                    record = (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.id == attachment_id).with_for_update())).scalar_one()
                    if record.phase != "pending" or record.host_id != self.provider.host_id:
                        raise AttachmentPending("Attachment is no longer eligible for activation")
                    for row, _ in rows.values():
                        await self.files._volume(session, row)
                    await run_file_io(self.provider.start, record.container_id, plan)
                    record.phase = "active"
                return Attachment(attachment_id, incarnation, self.provider.host_id, container)
            except IntegrityError as exc:
                raise SpaceConflict("Execution incarnation was claimed concurrently") from exc
            except (SpaceDenied, SpaceConflict, ValueError):
                raise
            except Exception as exc:
                raise AttachmentPending("Attachment outcome is pending; inspect and fence the environment") from exc
            finally:
                _operation.reset(token)

        return await await_drained(perform())

    async def retire(self, *, actor, space_id, expected_generation, operation_id=None, attachment_ids=None, admission=None):
        """ADMIN authorizes containment, including RO handles, on this resource.

        Removing the entire environment also closes its other resource handles.
        It changes no membership or custody. Never erase attachment records.
        A host-only admission callback adds domain conditions/intent in the first
        qualified ADMIN transaction, before any containment effect.
        """
        validate_generation(expected_generation)
        if attachment_ids is not None and (not isinstance(attachment_ids, (tuple, list)) or not 1 <= len(attachment_ids) <= 32 or any(not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{32}", v) for v in attachment_ids)):
            raise ValueError("Explicit retirement requires bounded attachment identities")
        if operation_id is not None:
            self.files._operation_id(operation_id)
        exemption = (space_id, operation_id) if operation_id is not None else None

        async def perform():
            token = _operation.set(("retire", None))
            requests = {space_id: (Permission.ADMIN, expected_generation)}
            callback_rejected = False
            effects_possible = False
            try:
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=exemption) as (session, _):
                    records = {a.id: a for a, _mount in await self._current(session, [space_id], lock=True)}
                    if attachment_ids is not None:
                        bound = set((await session.execute(select(SpaceMountRow.attachment_id).where(SpaceMountRow.space_id == space_id, SpaceMountRow.attachment_id.in_(attachment_ids)))).scalars())
                        if set(attachment_ids) != bound:
                            raise SpaceConflict("Retirement scope does not belong to the resource")
                        records = {key: value for key, value in records.items() if key in bound}
                    identities = [(a.id, a.container_id) for a in records.values()]
                    if admission is not None:
                        try:
                            await admission(session, tuple(identities))
                        except Exception:
                            callback_rejected = True
                            raise
                    # From here, transaction commit/physical outcomes may be uncertain.
                    effects_possible = True
                    for record in records.values():
                        if record.host_id != self.provider.host_id:
                            raise AttachmentPending("Cross-host containment is unsupported")
                        record.phase = "fence_pending"
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=exemption) as (session, _):
                    for attachment_id, container_id in identities:
                        record = (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.id == attachment_id).with_for_update())).scalar_one()
                        if record.phase == "fenced":
                            continue
                        if record.phase != "fence_pending":
                            raise AttachmentPending("Attachment containment state changed")
                        containers = set(await run_file_io(self.provider.discover, attachment_id))
                        if container_id:
                            containers.add(container_id)
                        if not containers:
                            raise AttachmentPending("Preparation identity is unknown; absence of a label is not containment proof")
                        for container in sorted(containers):
                            await run_file_io(self.provider.fence, container, attachment_id)
                        # A late prepare can only create a stopped container;
                        # activation still requires this recorded pending phase.
                        record.phase = "fenced"
            except (SpaceDenied, AttachmentPending):
                raise
            except Exception as exc:
                if callback_rejected or (isinstance(exc, SpaceConflict) and not effects_possible):
                    raise
                raise AttachmentPending("Storage attachment containment remains pending") from exc
            finally:
                _operation.reset(token)

        await await_drained(perform())

"""Host-owned native views and durable single-host containment admission.

The caller supplies an already authenticated actor and an execution incarnation,
not a chat ID. A trusted provider prepares a stopped environment; only this
service starts it after recording its exact identity. No lease can release it.
"""

import asyncio
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, ExitStack
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import MutationMode, Permission, PrincipalRef, SpaceConflict, SpaceDenied, validate_generation
from deerflow.spaces.preparation import preparation_scope
from deerflow.spaces.registry import _stored_permissions
from deerflow.spaces.service import _reservation
from deerflow.utils.file_io import await_drained, run_file_io

_operation = ContextVar("storage_attachment_operation", default=None)
logger = logging.getLogger(__name__)

HOST_FENCE_REASONS = frozenset({"container-absent", "run-orphaned", "start-failed", "outcome-uncertain", "idle", "operator", "host-replaced"})
HUMAN_FENCE_REASONS = frozenset({"lifecycle", "storage-recovery"})
FENCE_REASONS = HOST_FENCE_REASONS | HUMAN_FENCE_REASONS
MAX_RETAINED_ATTACHMENTS = 4096


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
    slot_id: str | None = None
    native_root: object | None = None


@dataclass(frozen=True)
class AttachmentPlan:
    id: str
    incarnation: str
    actor: PrincipalRef
    host_id: str
    views: tuple[NativeView, ...]
    readonly_roots: tuple = ()


@dataclass(frozen=True)
class Attachment:
    id: str
    incarnation: str
    host_id: str
    container_id: str


@dataclass(frozen=True)
class AttachmentState:
    id: str
    phase: str
    host_id: str
    container_id: str | None


@dataclass(frozen=True)
class RetainedAttachment:
    """Detached accounting facts; neither permission nor containment proof."""

    id: str
    incarnation: str
    actor: PrincipalRef
    host_id: str
    resources: tuple[ResourceMount, ...]
    phase: str
    container_id: str | None

    def __post_init__(self):
        for identifier in (self.id, self.incarnation):
            if not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{32}", identifier):
                raise ValueError("Retained attachment requires exact durable identities")
        if not isinstance(self.actor, PrincipalRef) or not isinstance(self.host_id, str) or not 1 <= len(self.host_id) <= 128:
            raise ValueError("Retained attachment requires exact actor and host")
        if not isinstance(self.resources, tuple) or not 1 <= len(self.resources) <= 32 or any(not isinstance(resource, ResourceMount) for resource in self.resources):
            raise ValueError("Retained attachment requires complete typed mounts")
        if len({resource.space_id for resource in self.resources}) != len(self.resources) or len({resource.alias for resource in self.resources}) != len(self.resources):
            raise ValueError("Retained attachment mounts must be unique")
        object.__setattr__(self, "resources", tuple(sorted(self.resources, key=lambda resource: resource.space_id)))
        if self.phase not in {"pending", "active", "fence_pending"} or (self.phase == "active" and self.container_id is None):
            raise ValueError("Retained attachment phase is invalid")
        if self.container_id is not None and (not isinstance(self.container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", self.container_id)):
            raise ValueError("Retained attachment requires an immutable container identity")

    @property
    def identity(self):
        return self.id, self.incarnation, self.actor, self.host_id, self.resources


class SpaceAttachments:
    def __init__(self, files, provider):
        self.files, self.registry, self.provider = files, files.registry, provider
        files.attachment_guard = self._guard
        files.attachments = self
        self.on_contained: Callable[[str, Sequence[str], str], Awaitable[None]] | None = None
        self.uncontained: dict[str, str] = {}

    def _views(self, resources, volumes):
        from deerflow.spaces.native_resident_evidence import NativeResidentRoot

        managed = getattr(getattr(self.provider, "_backend", None), "_managed_native_required", False)
        return tuple(
            NativeView(
                resource.space_id,
                resource.generation,
                str(volumes[resource.space_id].data_path),
                "/mnt/spaces/" + resource.alias,
                resource.writable,
                slot_id=getattr(volumes[resource.space_id].spec, "slot_id", None),
                native_root=NativeResidentRoot.from_view(resource, volumes[resource.space_id]) if managed else None,
            )
            for resource in resources
        )

    async def _containment(self, stack, scope, space_ids):
        """Private host exclusion precedes every expanded SQL parent phase."""
        factory = getattr(self.provider, "_containment_scope", None)
        return await self._enter_containment(stack, scope, space_ids, factory)

    async def _enter_containment(self, stack, scope, space_ids, factory):
        if factory is None or not scope:
            return None, space_ids
        native = await stack.enter_async_context(factory(self, scope))
        parents = sorted(set(native.parent_ids))
        if not set(space_ids).issubset(parents):
            raise AttachmentPending("Containment omitted an original resource parent")
        return native, parents

    @staticmethod
    def _closure_reason(record, fallback):
        previous = (record.fenced_by_kind, record.fenced_by_id, record.fence_reason)
        if previous == (None, None, None):
            if record.fenced_at is not None:
                raise AttachmentPending("Attachment closure attribution is missing")
            # Pre-attribution, fully fenced rows are historical facts. A retry
            # may notify consumers without inventing a past closer or time.
            return fallback
        valid_reasons = HUMAN_FENCE_REASONS if record.fenced_by_kind == "human" else HOST_FENCE_REASONS if record.fenced_by_kind == "host" else frozenset()
        if not isinstance(record.fenced_by_id, str) or not record.fenced_by_id or len(record.fenced_by_id) > 128 or record.fence_reason not in valid_reasons:
            raise AttachmentPending("Attachment closure attribution is invalid")
        if (record.phase == "fenced") != (record.fenced_at is not None):
            raise AttachmentPending("Attachment closure timestamp is invalid")
        return record.fence_reason

    @classmethod
    def _record_closure(cls, record, *, kind, subject_id, reason):
        cls._closure_reason(record, reason)
        if (record.fenced_by_kind, record.fenced_by_id, record.fence_reason) == (None, None, None):
            if not isinstance(subject_id, str) or not subject_id or len(subject_id) > 128:
                raise AttachmentPending("Attachment closer identity is invalid")
            record.fenced_by_kind, record.fenced_by_id, record.fence_reason = kind, subject_id, reason

    @staticmethod
    def _identifier(identifier):
        if not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{32}", identifier):
            raise ValueError("An exact attachment or space identity is required")

    @staticmethod
    async def _mount_scope(session, identifiers):
        mounts = (await session.execute(select(SpaceMountRow).where(SpaceMountRow.attachment_id.in_(identifiers)))).scalars().all()
        scope = {identifier: set() for identifier in identifiers}
        for mount in mounts:
            scope[mount.attachment_id].add((mount.space_id, mount.generation, mount.alias, mount.writable))
        if any(not facts for facts in scope.values()):
            raise AttachmentPending("Attachment mount scope is unavailable")
        return scope

    async def _locked_records(self, session, scope):
        records = {row.id: row for row in (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.id.in_(scope)).order_by(SpaceAttachmentRow.id).with_for_update())).scalars()}
        if set(records) != set(scope) or await self._mount_scope(session, scope) != scope:
            raise AttachmentPending("Attachment mount scope changed")
        if any(row.phase not in ("active", "pending", "fence_pending", "fenced") for row in records.values()):
            raise AttachmentPending("Invalid persisted attachment state")
        return records

    async def _fence_physical(self, record, native=None):
        if native is not None:
            await native.contain(record)
            return
        containers = set(await run_file_io(self.provider.discover, record.id))
        if record.container_id is not None:
            containers.add(record.container_id)
        if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in containers):
            raise AttachmentPending("Container containment identity is invalid")
        for container in sorted(containers):
            await run_file_io(self.provider.fence, container, record.id)
        if record.container_id is None:
            # A lost preparation reply can still create a stopped container
            # after this census. Removing observed IDs does not settle it.
            raise AttachmentPending("Preparation identity is unknown; containment remains unconfirmed")
        await run_file_io(self.provider.cleanup, record.id)

    async def _fence_record(self, record, native=None):
        await self._fence_physical(record, native)
        record.phase = "fenced"
        record.fenced_at = datetime.now(UTC)

    async def unfenced(self, space_id: str, *, actor=None, resources=None, host_id=None) -> AttachmentState | None:
        """Trusted host read; ambiguity and pending containment are not absence."""
        self._identifier(space_id)
        scoped = any(value is not None for value in (actor, resources, host_id))
        if scoped:
            if not isinstance(actor, PrincipalRef) or not isinstance(host_id, str) or not host_id or not isinstance(resources, (list, tuple)) or not resources or any(not isinstance(resource, ResourceMount) for resource in resources):
                raise ValueError("Scoped inspection requires exact actor, host and mounts")
            expected = {(resource.space_id, resource.generation, resource.alias, resource.writable) for resource in resources}
            if len({resource.space_id for resource in resources}) != len(resources) or len({resource.alias for resource in resources}) != len(resources) or space_id not in {resource.space_id for resource in resources}:
                raise ValueError("Scoped inspection requires a complete unique resource view")
        async with self.registry._sf() as session:
            records = {row.id: row for row, _mount in await self._current(session, [space_id])}
            if len(records) > 1:
                raise SpaceConflict("The resource does not identify one attachment")
            if not records:
                return None
            row = next(iter(records.values()))
            if scoped:
                scope = (await self._mount_scope(session, (row.id,)))[row.id]
                if (row.actor_kind, row.actor_id) != (actor.kind, actor.subject_id) or row.host_id != host_id or scope != expected:
                    raise AttachmentPending("Resident attachment differs from its exact actor, host and mount scope")
                self._identifier(row.id)
                if row.phase not in {"pending", "active", "fence_pending"} or (row.phase == "active" and row.container_id is None) or (row.container_id is not None and not re.fullmatch(r"[0-9a-f]{64}", row.container_id)):
                    raise AttachmentPending("Resident attachment facts are invalid")
                self._closure_reason(row, None)
        return AttachmentState(row.id, row.phase, row.host_id, row.container_id)

    async def inspect_preparation(self, *, actor: PrincipalRef, incarnation: str, resources: Sequence[ResourceMount], host_id: str) -> AttachmentState | None:
        """Read exact durable correlation, without admission or physical proof.

        No row for this incarnation is distinct from a matching fenced row.
        Any mismatch or unreadable metadata raises; neither is absence. The
        host caller independently retains its Home lock, drains preparation
        and freshly qualifies physical containment before releasing a claim.
        Current grants are deliberately irrelevant to cleanup correlation.
        """
        self._identifier(incarnation)
        resources = tuple(resources)
        if not isinstance(actor, PrincipalRef) or not isinstance(host_id, str) or not host_id or len(host_id) > 128:
            raise ValueError("Preparation inspection requires exact actor and host facts")
        if not resources or any(not isinstance(resource, ResourceMount) for resource in resources):
            raise ValueError("Preparation inspection requires its complete resource scope")
        if len({resource.space_id for resource in resources}) != len(resources) or len({resource.alias for resource in resources}) != len(resources):
            raise ValueError("Preparation resource identities and aliases must be unique")
        expected = {(resource.space_id, resource.generation, resource.alias, resource.writable) for resource in resources}
        async with self.registry._sf() as session:
            record = await session.scalar(select(SpaceAttachmentRow).where(SpaceAttachmentRow.incarnation == incarnation))
            if record is None:
                return None
            try:
                self._identifier(record.id)
            except ValueError as exc:
                raise AttachmentPending("Preparation attachment identity is invalid") from exc
            if record.phase not in ("pending", "active", "fence_pending", "fenced"):
                raise AttachmentPending("Preparation attachment phase is invalid")
            if record.container_id is not None and (not isinstance(record.container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", record.container_id)):
                raise AttachmentPending("Preparation container identity is invalid")
            if record.phase == "active" and record.container_id is None:
                raise AttachmentPending("Active preparation container identity is unavailable")
            self._closure_reason(record, None)
            if record.phase in ("pending", "active") and any(value is not None for value in (record.fenced_by_kind, record.fenced_by_id, record.fence_reason, record.fenced_at)):
                raise AttachmentPending("Open preparation carries closure metadata")
            scope = (await self._mount_scope(session, (record.id,)))[record.id]
            if (record.actor_kind, record.actor_id) != (actor.kind, actor.subject_id) or record.host_id != host_id or scope != expected:
                raise AttachmentPending("Preparation does not match its captured actor, host and mount scope")
            facts = AttachmentState(record.id, record.phase, record.host_id, record.container_id)
        return facts

    async def confirm_fenced_preparation(self, *, actor, incarnation, resources, host_id, attachment_id, container_id):
        """Fresh exact physical confirmation for an already drained host start.

        Historical phase alone never releases an in-process preparation. The
        trusted caller retains its Home lock and has drained every invoked
        worker. Reprove the original immutable resource set under SQL parent
        locks without changing its historical closure attribution or time.
        """
        state = await self.inspect_preparation(actor=actor, incarnation=incarnation, resources=resources, host_id=host_id)
        if state is None or (state.id, state.phase, state.host_id, state.container_id) != (attachment_id, "fenced", host_id, container_id):
            raise AttachmentPending("Fenced preparation lost its exact historical identity")
        if host_id != self.provider.host_id:
            raise AttachmentPending("Fenced preparation belongs to another physical host")
        expected = {attachment_id: {(resource.space_id, resource.generation, resource.alias, resource.writable) for resource in resources}}
        space_ids = sorted({resource.space_id for resource in resources})

        async def perform():
            async with AsyncExitStack() as stack:
                native, parent_ids = await self._containment(stack, expected, space_ids)
                async with self.registry.host_locked(parent_ids) as (session, parents):
                    if set(parents) != set(parent_ids):
                        raise AttachmentPending("Fenced preparation resources are unavailable")
                    record = (await self._locked_records(session, expected))[attachment_id]
                    if (record.incarnation, record.actor_kind, record.actor_id, record.host_id, record.container_id, record.phase) != (incarnation, actor.kind, actor.subject_id, host_id, container_id, "fenced"):
                        raise AttachmentPending("Fenced preparation changed before physical confirmation")
                    if native is not None:
                        await native.admit(session, {record.id: record})
                    self._closure_reason(record, None)
                    await self._fence_physical(record, native)
                if native is not None:
                    await native.finalize()
            return True

        return await await_drained(perform())

    @classmethod
    def _retained_claim(cls, record, scope):
        cls._closure_reason(record, None)
        if record.phase in {"pending", "active"} and any(value is not None for value in (record.fenced_by_kind, record.fenced_by_id, record.fence_reason, record.fenced_at)):
            raise AttachmentPending("Open retained attachment carries closure metadata")
        if any(type(writable) not in (int, bool) or writable not in (0, 1) for _space, _generation, _alias, writable in scope):
            raise AttachmentPending("Retained mount has an invalid write mode")
        return RetainedAttachment(
            record.id,
            record.incarnation,
            PrincipalRef(record.actor_kind, record.actor_id),
            record.host_id,
            tuple(ResourceMount(space, generation, alias, bool(writable)) for space, generation, alias, writable in scope),
            record.phase,
            record.container_id,
        )

    async def retained_attachment(self, attachment_id: str) -> RetainedAttachment | None:
        """Exact detached accounting read; None confers no containment proof."""
        self._identifier(attachment_id)
        async with self.registry._sf() as session:
            record = await session.get(SpaceAttachmentRow, attachment_id)
            if record is None:
                return None
            if record.phase == "fenced":
                self._identifier(record.incarnation)
                self._closure_reason(record, None)
                return None
            scope = (await self._mount_scope(session, (record.id,)))[record.id]
            return self._retained_claim(record, scope)

    async def retained_claims(self, *, limit=MAX_RETAINED_ATTACHMENTS) -> tuple[RetainedAttachment, ...]:
        """Read a complete globally bounded snapshot, without effects or grants."""
        if type(limit) is not int or not 1 <= limit <= MAX_RETAINED_ATTACHMENTS:
            raise ValueError("A bounded positive attachment snapshot limit is required")
        async with self.registry._sf() as session:
            records = (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.phase != "fenced").order_by(SpaceAttachmentRow.id).limit(limit + 1))).scalars().all()
            if len(records) > limit:
                raise AttachmentPending("Retained attachment snapshot exceeds its global limit")
            identifiers = tuple(record.id for record in records)
            if len(set(identifiers)) != len(identifiers):
                raise AttachmentPending("Retained attachment snapshot has duplicate identities")
            scope = await self._mount_scope(session, identifiers) if identifiers else {}
            claims = []
            for record in records:
                claims.append(self._retained_claim(record, scope[record.id]))
        return tuple(claims)

    async def fence_reason(self, attachment_id: str) -> str | None:
        self._identifier(attachment_id)
        async with self.registry._sf() as session:
            row = await session.get(SpaceAttachmentRow, attachment_id)
            if row is None or row.phase not in ("fence_pending", "fenced"):
                return None
            return self._closure_reason(row, None)

    async def container_state(self, attachment_id: str) -> Literal["gone", "running", "unknown"]:
        """Read liveness only; a stopped container still needs containment."""
        self._identifier(attachment_id)
        try:
            async with self.registry._sf() as session:
                row = await session.get(SpaceAttachmentRow, attachment_id)
                if row is None or row.phase != "active" or row.host_id != self.provider.host_id or row.container_id is None:
                    return "unknown"
                container = row.container_id
            state = await run_file_io(self.provider.container_state, container, attachment_id)
            return state if state in ("gone", "running", "unknown") else "unknown"
        except Exception:
            return "unknown"

    async def container_gone(self, attachment_id: str) -> bool:
        return await self.container_state(attachment_id) == "gone"

    async def _host_eligible(self, session, record):
        if record.host_id == self.provider.host_id:
            return False
        replacement = await session.get(StorageHostReplacementRow, (record.host_id, self.provider.host_id))
        if replacement is None:
            raise AttachmentPending("Cross-host containment requires a recorded host replacement")
        return True

    async def contain_host(self, attachment_id: str, reason: str, *, admission=None) -> bool:
        """Host containment with optional domain admission before any closure intent."""
        if admission is not None and not callable(admission):
            raise ValueError("Host containment admission must be callable")
        return await self._contain_host(attachment_id, reason, getattr(self.provider, "_containment_scope", None), admission=admission)

    async def _contain_restored_host(self, attachment_id, reason, *, scope_factory):
        """An owning Gateway supplies one exact absence-only recovery scope."""
        if not callable(scope_factory):
            raise ValueError("Restored containment requires its explicit owned scope")
        return await self._contain_host(attachment_id, reason, scope_factory)

    async def _contain_host(self, attachment_id, reason, scope_factory, *, admission=None):
        self._identifier(attachment_id)
        if not isinstance(reason, str) or reason not in HOST_FENCE_REASONS:
            raise ValueError("Host containment requires a host closure reason")

        async def perform():
            stored_reason = reason
            retry_reason = reason
            unknown_attachment = False
            callback_rejected = False
            try:
                async with self.registry._sf() as session:
                    row = await session.get(SpaceAttachmentRow, attachment_id)
                    if row is None:
                        unknown_attachment = True
                        raise ValueError("Unknown attachment")
                    scope = await self._mount_scope(session, [attachment_id])
                space_ids = sorted(fact[0] for fact in scope[attachment_id])
                async with AsyncExitStack() as stack:
                    native, parent_ids = await self._enter_containment(stack, scope, space_ids, scope_factory)
                    async with self.registry.host_locked(parent_ids) as (session, parents):
                        if set(parents) != set(parent_ids):
                            raise AttachmentPending("Attachment resources are unavailable")
                        records = await self._locked_records(session, scope)
                        if native is not None:
                            await native.admit(session, records)
                        record = records[attachment_id]
                        if admission is not None:
                            try:
                                await admission(session, ((record.id, record.container_id),))
                            except Exception:
                                callback_rejected = True
                                raise
                        if record.phase != "fenced":
                            replaced = await self._host_eligible(session, record)
                            retry_reason = stored_reason = "host-replaced" if replaced else reason
                            self._record_closure(record, kind="host", subject_id=self.provider.host_id, reason=stored_reason)
                            record.phase = "fence_pending"
                        stored_reason = self._closure_reason(record, stored_reason)
                    async with self.registry.host_locked(parent_ids) as (session, parents):
                        if set(parents) != set(parent_ids):
                            raise AttachmentPending("Attachment resources are unavailable")
                        records = await self._locked_records(session, scope)
                        if native is not None:
                            await native.admit(session, records)
                        record = records[attachment_id]
                        stored_reason = self._closure_reason(record, stored_reason)
                        if record.phase != "fenced":
                            await self._host_eligible(session, record)
                            if record.phase != "fence_pending":
                                raise AttachmentPending("Attachment containment state changed")
                            await self._fence_record(record, native)
                    if native is not None:
                        await native.finalize()
            except Exception as exc:
                if unknown_attachment or callback_rejected:
                    raise
                # Inputs for host replay, rather than historical attribution:
                # a first human closure reason is not a valid host API input.
                self.uncontained[attachment_id] = retry_reason
                logger.warning("Storage host containment remains pending for %s (%s)", attachment_id, type(exc).__name__)
                return False
            self.uncontained.pop(attachment_id, None)
            await self._notify_contained(attachment_id, space_ids, stored_reason)
            return True

        return await await_drained(perform())

    async def _notify_contained(self, attachment_id, space_ids, reason):
        if self.on_contained is None:
            return
        try:
            await self.on_contained(attachment_id, space_ids, reason)
        except (Exception, asyncio.CancelledError) as exc:
            logger.warning("Storage containment observer failed for %s (%s)", attachment_id, type(exc).__name__)

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
        factory = getattr(self.provider, "_containment_scope", None)
        blocks_admin = getattr(factory, "blocks_admin", None)
        if callable(blocks_admin) and action != ("retire", None):
            for identifier, (row, _grant) in admitted.items():
                if requests[identifier][0] & Permission.ADMIN and _reservation.get() != identifier and blocks_admin(row.backing_handle):
                    raise AttachmentPending("Resident durable cleanup still owns this backing")
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
            if attachment.phase != "active" and permission & Permission.ADMIN:
                raise AttachmentPending("A storage attachment has an unconfirmed outcome")

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
        refresh = getattr(self.provider, "readonly_roots", None)
        readonly_roots = await refresh() if callable(refresh) else ()
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
                plan = AttachmentPlan(
                    record.id,
                    record.incarnation,
                    actor,
                    self.provider.host_id,
                    self._views(resources, volumes),
                    readonly_roots,
                )
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
        refresh = getattr(self.provider, "readonly_roots", None)
        readonly_roots = await refresh() if callable(refresh) else ()
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
                with ExitStack() as preparation_lifetime:
                    async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                        record = (await session.execute(select(SpaceAttachmentRow).where(SpaceAttachmentRow.id == attachment_id).with_for_update())).scalar_one()
                        if record.phase != "pending" or record.host_id != self.provider.host_id:
                            raise AttachmentPending("Attachment preparation was retired")
                        volumes = {key: await self.files._volume(session, row) for key, (row, _) in rows.items()}
                        plan = AttachmentPlan(
                            attachment_id,
                            incarnation,
                            actor,
                            self.provider.host_id,
                            self._views(resources, volumes),
                            readonly_roots,
                        )
                        proof = preparation_lifetime.enter_context(preparation_scope(self.provider, plan))
                        try:
                            container = await await_drained(run_file_io(self.provider.prepare, plan))
                        finally:
                            proof.finish_worker()
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

    async def retire(self, *, actor, space_id, expected_generation, reason, operation_id=None, attachment_ids=None, admission=None):
        """ADMIN authorizes containment, including RO handles, on this resource.

        Removing the entire environment also closes its other resource handles.
        It changes no membership or custody. Never erase attachment records.
        A host-only admission callback adds domain conditions/intent in the first
        qualified ADMIN transaction, before any containment effect.
        """
        if not isinstance(reason, str) or reason not in HUMAN_FENCE_REASONS:
            raise ValueError("Human retirement requires a human closure reason")
        if not isinstance(actor, PrincipalRef) or actor.kind != "human":
            raise SpaceDenied("Human retirement requires an authenticated human")
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
                await self.registry.get(actor=actor, space_id=space_id, permission=Permission.ADMIN, expected_generation=expected_generation)
                async with self.registry._sf() as session:
                    identifiers = {a.id for a, _mount in await self._current(session, [space_id])}
                    if attachment_ids is not None:
                        bound = set((await session.execute(select(SpaceMountRow.attachment_id).where(SpaceMountRow.space_id == space_id, SpaceMountRow.attachment_id.in_(attachment_ids)))).scalars())
                        if set(attachment_ids) != bound:
                            raise SpaceConflict("Retirement scope does not belong to the resource")
                        identifiers = bound
                    scope = await self._mount_scope(session, identifiers)
                # Capture all roots before reacquiring them in sorted order.
                # ADMIN on one root closes the entire native environment; it
                # does not require new grants on its other captured roots.
                space_ids = sorted({space_id} | {fact[0] for facts in scope.values() for fact in facts})
                async with AsyncExitStack() as stack:
                    native, parent_ids = await self._containment(stack, scope, space_ids)
                    async with self.registry.host_locked(parent_ids) as (session, parents):
                        if set(parents) != set(parent_ids):
                            raise AttachmentPending("Attachment resources are unavailable")
                        await self.registry.admit_in_session(session, actor=actor, requests=requests, operation_id=exemption)
                        if attachment_ids is None and {a.id for a, _mount in await self._current(session, [space_id])} != identifiers:
                            raise SpaceConflict("Retirement scope changed; retry against current attachments")
                        records = await self._locked_records(session, scope)
                        if native is not None:
                            await native.admit(session, records)
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
                            if record.phase == "fenced":
                                self._closure_reason(record, reason)
                                continue
                            self._record_closure(record, kind="human", subject_id=actor.subject_id, reason=reason)
                            record.phase = "fence_pending"
                    confirmed = []
                    async with self.registry.host_locked(parent_ids) as (session, parents):
                        if set(parents) != set(parent_ids):
                            raise AttachmentPending("Attachment resources are unavailable")
                        await self.registry.admit_in_session(session, actor=actor, requests=requests, operation_id=exemption)
                        records = await self._locked_records(session, scope)
                        if native is not None:
                            await native.admit(session, records)
                        for attachment_id, _container_id in identities:
                            record = records[attachment_id]
                            if record.host_id != self.provider.host_id:
                                raise AttachmentPending("Cross-host containment is unsupported")
                            stored_reason = self._closure_reason(record, reason)
                            if record.phase == "fenced":
                                confirmed.append((attachment_id, stored_reason))
                                continue
                            if record.phase != "fence_pending":
                                raise AttachmentPending("Attachment containment state changed")
                            await self._fence_record(record, native)
                            confirmed.append((attachment_id, stored_reason))
                    if native is not None:
                        await native.finalize()
            except (SpaceDenied, AttachmentPending):
                raise
            except Exception as exc:
                if callback_rejected or (isinstance(exc, SpaceConflict) and not effects_possible):
                    raise
                raise AttachmentPending("Storage attachment containment remains pending") from exc
            finally:
                _operation.reset(token)
            # SQL and the retirement guard are released before consumer code.
            for attachment_id, stored_reason in confirmed:
                await self._notify_contained(attachment_id, sorted(fact[0] for fact in scope[attachment_id]), stored_reason)

        await await_drained(perform())

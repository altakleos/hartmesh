"""Owning-loop workspace accounting; installation belongs to the Gateway."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING
from uuid import uuid4

from deerflow.agent_instances.contract import ResidentBusy, ResidentRestarting, ResidentRunLimit, ResidentStartingElsewhere, ResidentWorkLimit
from deerflow.sandbox.exceptions import SandboxCapacityExceededError
from deerflow.spaces.attachments import MAX_RETAINED_ATTACHMENTS, ResourceMount, RetainedAttachment
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from deerflow.agent_instances.contract import InstanceIdentity
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.spaces.attachments import SpaceAttachments

logger = logging.getLogger(__name__)
MAX_RUNS_PER_WORKSPACE = 2
RECYCLE_GRACE_SECONDS = {"outcome-uncertain": 300}


@dataclass(frozen=True, eq=False)
class PreparationClaim:
    """Exact in-process preparation identity, never evidence of containment."""

    incarnation: str
    home_id: str
    generation: int
    host_id: str


@dataclass
class ResidentEntry:
    instance_id: str
    home_id: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    attachment_id: str | None = None
    preparation: PreparationClaim | None = None
    last_release: float = 0.0
    recycle_due_since: float | None = None
    recycle_reason: str | None = None


@dataclass
class WorkReservation:
    id: str
    instance_id: str
    home_id: str
    expires_at: float
    attempt_id: str | None = None


@dataclass(eq=False)
class _RestoreToken:
    updates: dict[str, RetainedAttachment | None] = field(default_factory=dict)
    overflowed: bool = False
    uncorrelated_creation: bool = False


class ResidentRegistry:
    """Share slots without a second run ledger or authority from Docker census.

    Every decision runs on the creating loop. Only ``slots`` is exposed to
    the provider, whose metadata lock guards all slot mutations and reads.
    Physical and SQL work never runs under that lock. This foundation does
    not install itself: startup must first restore durable resident claims.
    """

    eviction_timeout = 60.0

    def __init__(
        self,
        *,
        attachments: SpaceAttachments,
        provider: AioSandboxProvider,
        run_manager: RunManager,
        settle: Callable[..., Awaitable[list[str]]],
        owner_of: Callable[[str], Awaitable[InstanceIdentity | None]],
        replicas: int,
        idle_timeout: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.attachments, self.provider, self.run_manager = attachments, provider, run_manager
        self.settle, self.owner_of = settle, owner_of
        self.replicas, self.idle_timeout, self.clock = replicas, idle_timeout, clock
        self.homes: dict[str, ResidentEntry] = {}
        self.slots: set[str] = set()
        self.reservations: dict[str, WorkReservation] = {}
        self.ended: set[str] = set()
        self.loop = asyncio.get_running_loop()
        self._thread_id = threading.get_ident()
        self._evictions: set[asyncio.Task] = set()
        self._closing = False
        self.retained: dict[str, RetainedAttachment] = {}
        self._associated: dict[str, str] = {}
        self._restore: _RestoreToken | None = None
        self._restoration_started = False
        self.restoration_complete = False

    def begin_restore(self):
        self._owning_loop()
        token = _RestoreToken()

        def begin():
            if self._restore is not None or self._closing:
                raise RuntimeError("Resident restoration is already owned or closing")
            self._restoration_started = True
            self.restoration_complete = False
            self._restore = token

        self.provider.external_changed(begin)
        return token

    def _touch_retained(self, identifier, claim):
        if self._restore is not None:
            updates = self._restore.updates
            if identifier in updates or len(updates) < MAX_RETAINED_ATTACHMENTS:
                updates[identifier] = claim
            else:
                self._restore.overflowed = True
                self.restoration_complete = False

    @staticmethod
    def _merge_retained(previous, incoming):
        if previous.identity != incoming.identity or (previous.container_id is not None and incoming.container_id is not None and previous.container_id != incoming.container_id):
            raise ValueError("Retained attachment changed immutable identity")
        phases = {"pending": 0, "active": 1, "fence_pending": 2}
        phase = max((previous.phase, incoming.phase), key=phases.__getitem__)
        return replace(incoming, phase=phase, container_id=incoming.container_id or previous.container_id)

    def finish_restore(self, token, claims):
        self._owning_loop()
        if not isinstance(claims, (list, tuple)) or len(claims) > MAX_RETAINED_ATTACHMENTS or any(not isinstance(claim, RetainedAttachment) for claim in claims):
            raise ValueError("Restoration requires a complete bounded typed snapshot")
        if len({claim.id for claim in claims}) != len(claims) or len({claim.incarnation for claim in claims}) != len(claims):
            raise ValueError("Restoration snapshot contains duplicate durable identities")

        def publish():
            if token is not self._restore or token.overflowed or token.uncorrelated_creation:
                raise RuntimeError("Restoration snapshot lost its bounded ownership")
            merged = dict(self.retained)
            for claim in claims:
                previous = merged.get(claim.id)
                merged[claim.id] = self._merge_retained(previous, claim) if previous is not None else claim
            for identifier, updated in token.updates.items():
                if updated is None:
                    merged.pop(identifier, None)
                else:
                    previous = merged.get(identifier)
                    merged[identifier] = self._merge_retained(previous, updated) if previous is not None else updated
            if len(merged) > MAX_RETAINED_ATTACHMENTS:
                raise ValueError("Retained attachment snapshot exceeds its global limit")
            if len({claim.incarnation for claim in merged.values()}) != len(merged):
                raise ValueError("Merged attachment accounting reuses an incarnation")
            self.retained.clear()
            self.retained.update(merged)
            self._restore = None
            self.restoration_complete = True

        self.provider.external_changed(publish)

    def abort_restore(self, token):
        self._owning_loop()

        def abort():
            if self._restore is token:
                self._restore = None
                self.restoration_complete = False

        self.provider.external_changed(abort)

    def _retain_locked(self, claim):
        try:
            previous = self.retained.get(claim.id)
            if previous is None and len(self.retained) >= MAX_RETAINED_ATTACHMENTS:
                raise ValueError("Retained attachment index exhausted its bound")
            if any(other.id != claim.id and other.incarnation == claim.incarnation for other in self.retained.values()):
                raise ValueError("Retained attachment reuses another incarnation")
            current = self._merge_retained(previous, claim) if previous is not None else claim
            self.retained[claim.id] = current
            self._touch_retained(claim.id, current)
        except ValueError:
            self._restoration_started = True
            self.restoration_complete = False
            if self._restore is not None:
                self._restore.overflowed = True
            raise
        return current

    def retain(self, claim):
        self._owning_loop()
        if not isinstance(claim, RetainedAttachment):
            raise ValueError("Retained accounting requires exact typed facts")
        self.provider.external_changed(lambda: self._retain_locked(claim))

    def preparation_retained(self, instance, preparation, claim, *, attempt_id=None):
        """Transfer the exact in-process preparation and one capacity charge."""
        entry = self.entry(instance)
        if not entry.lock.locked():
            raise RuntimeError("Preparation transfer requires its Home lock")
        if not isinstance(claim, RetainedAttachment) or (claim.incarnation, claim.host_id, claim.actor, claim.resources) != (
            preparation.incarnation,
            preparation.host_id,
            instance.principal,
            (ResourceMount(preparation.home_id, preparation.generation, "home", True),),
        ):
            raise ValueError("Durable claim does not match its exact preparation")
        transferred = False

        def publish():
            nonlocal transferred
            if entry.preparation is not preparation:
                return
            if entry.attachment_id not in (None, claim.id):
                raise RuntimeError("Previous resident attachment is not contained")
            self._retain_locked(claim)
            entry.attachment_id, entry.last_release = claim.id, self.clock()
            entry.preparation = None
            self._associated[entry.home_id] = claim.id
            self.slots.add(entry.home_id)
            self._forget_attempt(instance.id, attempt_id)
            transferred = True

        self.provider.external_changed(publish)
        return transferred

    def _require_restored(self, instance):
        if self._restoration_started and not self.restoration_complete:
            raise ResidentRestarting(instance.name)
        touching = [claim.id for claim in self.retained.values() if any(resource.space_id == instance.home_id for resource in claim.resources)]
        if touching and (len(touching) != 1 or self._associated.get(instance.home_id) != touching[0] or self.retained[touching[0]].phase != "active"):
            raise ResidentStartingElsewhere(instance.name)

    def associate(self, instance, claim, *, generation):
        self._owning_loop()
        entry = self.entry(instance)
        if not entry.lock.locked():
            raise RuntimeError("Resident association requires its Home lock")
        expected = (ResourceMount(instance.home_id, generation, "home", True),)
        if (
            not self.restoration_complete
            or self.retained.get(claim.id) is not claim
            or claim.actor != instance.principal
            or claim.host_id != self.attachments.provider.host_id
            or claim.resources != expected
            or claim.phase != "active"
            or claim.container_id is None
            or entry.preparation is not None
            or entry.attachment_id not in (None, claim.id)
        ):
            raise ValueError("Retained claim cannot become this resident's usable identity")
        if sum(any(resource.space_id == instance.home_id for resource in retained.resources) for retained in self.retained.values()) != 1:
            raise ValueError("Resident Home has ambiguous retained attachments")

        def associate():
            entry.attachment_id, entry.last_release = claim.id, self.clock()
            self._associated[instance.home_id] = claim.id
            self.slots.add(instance.home_id)
            self._touch_retained(claim.id, claim)

        self.provider.external_changed(associate)

    def _owning_loop(self) -> None:
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("Resident decisions require their owning loop")

    def entry(self, instance: InstanceIdentity) -> ResidentEntry:
        self._owning_loop()
        if instance.home_id is None:
            raise ValueError("Resident has no Home")
        entry = self.homes.get(instance.home_id)
        if entry is None:
            entry = self.homes[instance.home_id] = ResidentEntry(instance.id, instance.home_id, last_release=self.clock())
        elif entry.instance_id != instance.id:
            raise RuntimeError("Resident Home changed owner")
        return entry

    def in_flight(self, instance_id: str) -> tuple[str, ...]:
        self._owning_loop()
        return tuple(run_id for run_id, record in self.run_manager.task_runs("agent_instance_id").items() if record["value"] == instance_id and run_id not in self.ended)

    def attempts_in_flight(self) -> list[dict]:
        self._owning_loop()
        expired = [reservation for reservation in self.reservations.values() if reservation.expires_at <= self.clock()]
        for reservation in expired:
            self.cancel(reservation)
        result = []
        for run_id, record in self.run_manager.task_runs("agent_work_attempt_id").items():
            if run_id in self.ended:
                continue
            metadata = record["metadata"]
            result.append(
                {"attempt_id": record["value"], "run_id": run_id, "instance_id": metadata.get("agent_instance_id"), "work_id": metadata.get("agent_work_id"), "requester_id": metadata.get("requester_id"), "started_at": record["created_at"]}
            )
        attempts = {item["attempt_id"] for item in result}
        result.extend(
            {"attempt_id": reservation.attempt_id, "run_id": None, "instance_id": reservation.instance_id, "work_id": None, "requester_id": None, "started_at": None}
            for reservation in self.reservations.values()
            if reservation.attempt_id is None or reservation.attempt_id not in attempts
        )
        return result

    def work_max(self) -> int:
        return max(1, self.replicas - 1)

    async def admit(self, instance: InstanceIdentity, *, work: bool, run_id: str | None = None) -> WorkReservation | None:
        self._require_restored(instance)
        entry = self.entry(instance)
        attempts = self.attempts_in_flight()
        if self._closing or (entry.recycle_due_since is not None and self.clock() - entry.recycle_due_since >= RECYCLE_GRACE_SECONDS.get(entry.recycle_reason, 0)):
            raise ResidentRestarting(instance.name)
        runs = self.in_flight(instance.id)
        active_attempts = {item["attempt_id"] for item in attempts if item["run_id"] is not None}
        promised = sum(1 for reservation in self.reservations.values() if reservation.instance_id == instance.id and (reservation.attempt_id is None or reservation.attempt_id not in active_attempts))
        if (run_id is None and len(runs) + promised >= MAX_RUNS_PER_WORKSPACE) or (run_id in runs and runs.index(run_id) >= MAX_RUNS_PER_WORKSPACE):
            raise ResidentRunLimit(instance.name)
        if not work:
            return None
        if len(attempts) >= self.work_max():
            raise ResidentWorkLimit(instance.name)
        reservation = WorkReservation(uuid4().hex, instance.id, entry.home_id, self.clock() + self.provider.sandbox_ready_timeout())
        self.reservations[reservation.id] = reservation
        try:
            await self.take_slot(instance, wait=False, reservation=reservation)
            if self.reservations.get(reservation.id) is not reservation or reservation.expires_at <= self.clock():
                raise ResidentBusy(instance.name)
        except BaseException:
            self.cancel(reservation)
            self._drop_slot(entry.home_id)
            raise
        return reservation

    def bind(self, reservation: WorkReservation, attempt_id: str) -> None:
        self._owning_loop()
        if self.reservations.get(reservation.id) is not reservation:
            raise RuntimeError("Work reservation is no longer current")
        if reservation.expires_at <= self.clock():
            self.cancel(reservation)
            raise RuntimeError("Work reservation expired")
        if reservation.attempt_id is not None and reservation.attempt_id != attempt_id:
            raise RuntimeError("Work reservation already binds another attempt")
        reservation.attempt_id = attempt_id

    def cancel(self, reservation: WorkReservation | None) -> None:
        self._owning_loop()
        if reservation is not None and self.reservations.get(reservation.id) is reservation:
            del self.reservations[reservation.id]
            self._drop_slot(reservation.home_id)

    async def take_slot(self, instance: InstanceIdentity, *, wait: bool, reservation: WorkReservation | None = None) -> None:
        entry = self.entry(instance)

        def check_current():
            self._require_restored(instance)
            if self._closing:
                raise ResidentRestarting(instance.name)
            if reservation is not None and (self.reservations.get(reservation.id) is not reservation or reservation.expires_at <= self.clock()):
                raise ResidentBusy(instance.name)

        def reserved():
            check_current()
            return entry.home_id in self.slots

        def reserve():
            check_current()
            self.slots.add(entry.home_id)

        check_current()
        try:
            await self.provider.admit_external_async(entry.home_id, reserve, wait=wait, is_reserved=reserved)
            check_current()
        except (ResidentRestarting, ResidentBusy):
            self._drop_slot(entry.home_id)
            raise
        except SandboxCapacityExceededError as exc:
            raise ResidentBusy(instance.name) from exc

    def claimed(self, instance: InstanceIdentity, attachment_id: str, *, attempt_id: str | None = None) -> None:
        entry = self.entry(instance)
        if entry.attachment_id is not None and entry.attachment_id != attachment_id:
            raise RuntimeError("Previous resident attachment is not contained")
        entry.attachment_id, entry.last_release = attachment_id, self.clock()
        self._forget_attempt(instance.id, attempt_id)
        self.provider.external_changed(lambda: self.slots.add(entry.home_id))
        if self._restore is not None:
            # A legacy caller supplied no detached immutable facts. Its Home
            # charge remains, but the concurrent snapshot cannot claim readiness.
            self.provider.external_changed(lambda: setattr(self._restore, "uncorrelated_creation", True))

    def preparing(self, instance: InstanceIdentity, *, incarnation: str, generation: int, host_id: str) -> PreparationClaim:
        entry = self.entry(instance)
        if not entry.lock.locked() or entry.preparation is not None or entry.attachment_id is not None:
            raise RuntimeError("Preparation requires an empty Home under its lock")
        claim = entry.preparation = PreparationClaim(incarnation, entry.home_id, generation, host_id)
        return claim

    def preparation_claimed(self, instance: InstanceIdentity, claim: PreparationClaim, attachment_id: str, *, attempt_id: str | None = None) -> bool:
        entry = self.entry(instance)
        if not entry.lock.locked():
            raise RuntimeError("Preparation transition requires the Home lock")
        if entry.preparation is not claim:
            return False
        self.claimed(instance, attachment_id, attempt_id=attempt_id)
        entry.preparation = None
        return True

    def preparation_absent(self, home_id: str, claim: PreparationClaim) -> bool:
        """Host-only transition after pre-invocation failure or qualified absence.

        Caller retains the Home lock and proves any invoked worker drained.
        This method performs no inspection and cannot establish that proof.
        """
        self._owning_loop()
        entry = self.homes[home_id]
        if not entry.lock.locked():
            raise RuntimeError("Preparation transition requires the Home lock")
        if entry.preparation is not claim:
            return False
        entry.preparation = None
        self._drop_slot(home_id)
        return True

    def _forget_attempt(self, instance_id: str, attempt_id: str | None) -> None:
        if attempt_id is not None:
            for reservation in list(self.reservations.values()):
                if reservation.instance_id == instance_id and reservation.attempt_id == attempt_id:
                    del self.reservations[reservation.id]

    def _has_reservation(self, home_id: str) -> bool:
        return any(reservation.home_id == home_id for reservation in self.reservations.values())

    def _drop_slot(self, home_id: str) -> None:
        self._owning_loop()
        entry = self.homes.get(home_id)
        can_drop = entry is not None and entry.attachment_id is None and entry.preparation is None and not self.in_flight(entry.instance_id) and not self._has_reservation(home_id)
        # Becoming idle is also a capacity event: a waiter can now evict the
        # retained attachment even though its slot has not yet been released.
        self.provider.external_changed(lambda: self.slots.discard(home_id) if can_drop else None)

    async def request_recycle(self, instance_id: str, home_id: str, attachment_id: str, reason: str, *, exclude_run_id: str | None = None) -> bool:
        self._owning_loop()
        entry = self.homes.get(home_id)
        if entry is None or entry.instance_id != instance_id or entry.attachment_id != attachment_id:
            return False
        async with entry.lock:
            if entry.attachment_id != attachment_id:
                return False
            others = tuple(run_id for run_id in self.in_flight(instance_id) if run_id != exclude_run_id)
            if not others and not self._has_reservation(home_id) and entry.preparation is None:
                return await self.attachments.contain_host(attachment_id, reason)
            if entry.recycle_due_since is None:
                entry.recycle_due_since, entry.recycle_reason = self.clock(), reason
            return False

    async def _recycle_if_idle(self, entry: ResidentEntry) -> None:
        if entry.recycle_due_since is not None and entry.attachment_id is not None:
            await self.request_recycle(entry.instance_id, entry.home_id, entry.attachment_id, entry.recycle_reason or "idle")

    async def _contain_if_gone(self, entry: ResidentEntry, attachment_id: str) -> bool:
        if entry.attachment_id != attachment_id or not await self.attachments.container_gone(attachment_id):
            return False
        async with entry.lock:
            if entry.attachment_id == attachment_id and await self.attachments.container_gone(attachment_id):
                return await self.attachments.contain_host(attachment_id, "container-absent")
        return False

    async def run_ended(self, instance_id: str, home_id: str, run_id: str, attachment_id: str | None, recycle: bool, *, attempt_id: str | None = None) -> None:
        self._owning_loop()
        try:
            await await_drained(self._run_ended(instance_id, home_id, run_id, attachment_id, recycle, attempt_id=attempt_id))
        except asyncio.CancelledError:
            logger.warning("Resident completion drained despite cancellation")
        except Exception:
            logger.exception("Resident completion bookkeeping failed")

    async def _run_ended(self, instance_id: str, home_id: str, run_id: str, attachment_id: str | None, recycle: bool, *, attempt_id: str | None) -> None:
        entry = None
        try:
            self.ended.add(run_id)
            self.ended.intersection_update(self.run_manager.task_runs("agent_instance_id"))
            self._forget_attempt(instance_id, attempt_id)
            entry = self.homes.get(home_id)
            if entry is not None and entry.instance_id == instance_id:
                entry.last_release = self.clock()
                if recycle and attachment_id is not None and not await self._contain_if_gone(entry, attachment_id):
                    await self.request_recycle(instance_id, home_id, attachment_id, "outcome-uncertain")
            if recycle:
                await self.settle(instance_id=instance_id, reason="start-failed", attachment_id=attachment_id, live_run_ids=frozenset(self.in_flight(instance_id)))
        finally:
            try:
                if entry is not None:
                    await self._recycle_if_idle(entry)
            finally:
                self._drop_slot(home_id)

    async def contained(self, attachment_id: str, space_ids: Sequence[str], reason: str) -> None:
        self._owning_loop()

        def clear_exact():
            self.retained.pop(attachment_id, None)
            self._touch_retained(attachment_id, None)
            for home_id, identifier in list(self._associated.items()):
                if identifier == attachment_id:
                    del self._associated[home_id]

        self.provider.external_changed(clear_exact)
        # Caller may already hold the Home lock. Never acquire it here.
        for home_id in space_ids:
            entry = self.homes.get(home_id)
            if entry is not None and entry.attachment_id == attachment_id:
                entry.attachment_id, entry.recycle_due_since, entry.recycle_reason = None, None, None
                self._drop_slot(home_id)
        for home_id in space_ids:
            owner = await self.owner_of(home_id)
            if owner is not None:
                logger.info("agents: attachment %s of %s contained (%s)", attachment_id, owner.id, reason)
                await self.settle(instance_id=owner.id, reason=reason, attachment_id=attachment_id, live_run_ids=frozenset(self.in_flight(owner.id)))

    def external_slots(self) -> int:
        """Memory-only read called by the provider under its metadata lock."""
        if self._restoration_started and not self.restoration_complete:
            raise StorageRuntimeUnavailable("Resident accounting restoration is incomplete")
        return len(self.retained) + sum(self._associated.get(home_id) not in self.retained for home_id in self.slots)

    def _evictable(self, entry: ResidentEntry) -> bool:
        return entry.attachment_id is not None and entry.preparation is None and entry.recycle_due_since is None and not self.in_flight(entry.instance_id) and not self._has_reservation(entry.home_id)

    async def _evict_idle(self) -> bool:
        self._owning_loop()
        task = asyncio.current_task()
        self._evictions.add(task)
        try:
            if self._closing:
                return False
            entries = [entry for entry in self.homes.values() if self._evictable(entry)]
            if not entries:
                return False
            entry = min(entries, key=lambda candidate: candidate.last_release)
            attachment_id = entry.attachment_id
            async with entry.lock:
                if self._closing or entry.attachment_id != attachment_id or not self._evictable(entry):
                    return False
                return await self.attachments.contain_host(attachment_id, "idle")
        finally:
            self._evictions.discard(task)

    def evict_external(self) -> bool:
        """Schedule every decision on the owning loop; timeout grants no credit."""
        if threading.get_ident() == self._thread_id or self.loop.is_closed():
            return False
        coroutine = self._evict_idle()
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        except RuntimeError:
            coroutine.close()
            return False
        try:
            return future.result(timeout=self.eviction_timeout)
        except TimeoutError:
            # Keep the owning task alive and registered until physical drainage.
            return False
        except Exception:
            logger.exception("Resident eviction failed")
            return False

    async def reap(self) -> None:
        self._owning_loop()
        for entry in list(self.homes.values()):
            attachment_id = entry.attachment_id
            if attachment_id is not None:
                reason = self.attachments.uncontained.get(attachment_id)
                if reason is not None:
                    async with entry.lock:
                        if entry.attachment_id == attachment_id:
                            await self.attachments.contain_host(attachment_id, reason)
                if entry.attachment_id == attachment_id:
                    await self._contain_if_gone(entry, attachment_id)
                if entry.attachment_id == attachment_id and await self.attachments.fence_reason(attachment_id) is not None:
                    async with entry.lock:
                        if entry.attachment_id == attachment_id:
                            await self.attachments.contain_host(attachment_id, "idle")
            await self._recycle_if_idle(entry)
            if self.idle_timeout > 0 and self.clock() - entry.last_release >= self.idle_timeout and self._evictable(entry):
                async with entry.lock:
                    if self.clock() - entry.last_release >= self.idle_timeout and self._evictable(entry):
                        await self.attachments.contain_host(entry.attachment_id, "idle")
        self.attempts_in_flight()

    async def close(self) -> None:
        """Close new eviction admission and drain all owned physical effects."""
        self._owning_loop()
        self._closing = True
        if self._evictions:
            await await_drained(asyncio.gather(*self._evictions, return_exceptions=True))

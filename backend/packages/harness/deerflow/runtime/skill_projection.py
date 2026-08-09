"""Invocation-fenced ownership for accepted skill sandbox projections.

The coordinator is deliberately process-local: the supported production
topology has one Gateway replica and a lost worker is terminalized rather than
resumed.  Its narrow job is to prevent a cached sandbox from exposing material
owned by another invocation while lead and background consumers overlap.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field

from deerflow.runtime.skill_snapshot import AcceptedSkillSnapshot, SkillSnapshotProjection

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
SKILL_PROJECTION_TOKEN_CONTEXT_KEY = "__deerflow_skill_projection_token_v1"


class SkillProjectionBusyError(RuntimeError):
    """A prior invocation still owns this thread's process-local projection."""

    def __init__(self) -> None:
        super().__init__("skill_projection_thread_busy")


@dataclass(frozen=True, slots=True)
class SkillProjectionEvidence:
    """Accepted immutable manifest used to verify every sandbox copy."""

    snapshot_id: str | None
    content_digest: str | None
    projections: tuple[SkillSnapshotProjection, ...]
    file_count: int
    total_bytes: int

    def __post_init__(self) -> None:
        _validate_snapshot_id(self.snapshot_id)
        if self.snapshot_id is None:
            if self.content_digest is not None or self.projections or self.file_count != 0 or self.total_bytes != 0:
                raise ValueError("empty skill projection evidence must contain no material")
            return
        if self.content_digest != self.snapshot_id:
            raise ValueError("skill projection content digest must match snapshot_id")
        if not self.projections or self.file_count < 1 or self.total_bytes < 1:
            raise ValueError("non-empty skill projection evidence is incomplete")
        if sum(item.file_count for item in self.projections) != self.file_count:
            raise ValueError("skill projection file count does not match projections")
        if sum(item.total_bytes for item in self.projections) != self.total_bytes:
            raise ValueError("skill projection byte count does not match projections")

    @classmethod
    def from_snapshot(
        cls,
        snapshot: AcceptedSkillSnapshot | None,
    ) -> SkillProjectionEvidence:
        if snapshot is None:
            return cls(
                snapshot_id=None,
                content_digest=None,
                projections=(),
                file_count=0,
                total_bytes=0,
            )
        return cls(
            snapshot_id=snapshot.snapshot_id,
            content_digest=snapshot.content_digest,
            projections=tuple(snapshot.projections),
            file_count=snapshot.file_count,
            total_bytes=snapshot.total_bytes,
        )


@dataclass(frozen=True, slots=True)
class SkillProjectionAdmissionReservation:
    """Opaque pre-admission reservation for one user/thread."""

    user_id: str
    thread_id: str
    reservation_id: str
    generation: int
    snapshot_id: str | None
    evidence: SkillProjectionEvidence | None = None


@dataclass(frozen=True, slots=True)
class SkillProjectionSupersessionFence:
    """Exact committed owner observed before an atomic replacement batch."""

    user_id: str
    thread_id: str
    run_id: str
    generation: int
    snapshot_id: str | None
    evidence: SkillProjectionEvidence | None = None


@dataclass(frozen=True, slots=True)
class SkillProjectionConsumerToken:
    """Exact invocation/generation/consumer ownership proof."""

    user_id: str
    thread_id: str
    sandbox_id: str
    run_id: str
    generation: int
    consumer_id: str
    snapshot_id: str | None
    evidence: SkillProjectionEvidence | None = None


@dataclass(frozen=True, slots=True)
class SkillProjectionClear:
    """Proof authorizing a provider to clear the last released projection."""

    user_id: str
    thread_id: str
    sandbox_id: str
    run_id: str
    generation: int
    snapshot_id: str | None
    evidence: SkillProjectionEvidence | None = None


@dataclass(slots=True)
class _ProjectionState:
    reservation_id: str
    generation: int
    snapshot_id: str | None
    evidence: SkillProjectionEvidence | None = None
    run_id: str | None = None
    sandbox_id: str | None = None
    consumers: dict[str, SkillProjectionConsumerToken] = field(default_factory=dict)
    clearing: SkillProjectionClear | None = None
    clearing_token: SkillProjectionConsumerToken | None = None


def _validate_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 512 or any(ord(character) < 32 for character in value):
        raise ValueError(f"{field_name} must be a bounded non-control string")


def _validate_snapshot_id(snapshot_id: str | None) -> None:
    if snapshot_id is not None and _DIGEST.fullmatch(snapshot_id) is None:
        raise ValueError("snapshot_id must be a lowercase SHA-256 digest or None")


class SkillProjectionCoordinator:
    """Serialize projection ownership and release it with exact CAS tokens."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._next_generation = 1
        self._states: dict[tuple[str, str], _ProjectionState] = {}

    @staticmethod
    def _key(user_id: str, thread_id: str) -> tuple[str, str]:
        _validate_text(user_id, "user_id")
        _validate_text(thread_id, "thread_id")
        return user_id, thread_id

    def reserve_admission(
        self,
        *,
        user_id: str,
        thread_id: str,
        reservation_id: str,
        snapshot_id: str | None,
        evidence: SkillProjectionEvidence | None = None,
    ) -> SkillProjectionAdmissionReservation:
        key = self._key(user_id, thread_id)
        _validate_text(reservation_id, "reservation_id")
        _validate_snapshot_id(snapshot_id)
        if evidence is not None and evidence.snapshot_id != snapshot_id:
            raise ValueError("skill projection evidence does not match snapshot_id")
        with self._lock:
            current = self._states.get(key)
            if current is not None:
                if current.run_id is None and current.reservation_id == reservation_id and current.snapshot_id == snapshot_id and current.evidence == evidence:
                    return SkillProjectionAdmissionReservation(
                        user_id=user_id,
                        thread_id=thread_id,
                        reservation_id=reservation_id,
                        generation=current.generation,
                        snapshot_id=snapshot_id,
                        evidence=current.evidence,
                    )
                raise SkillProjectionBusyError()
            generation = self._next_generation
            self._next_generation += 1
            self._states[key] = _ProjectionState(
                reservation_id=reservation_id,
                generation=generation,
                snapshot_id=snapshot_id,
                evidence=evidence,
            )
            return SkillProjectionAdmissionReservation(
                user_id=user_id,
                thread_id=thread_id,
                reservation_id=reservation_id,
                generation=generation,
                snapshot_id=snapshot_id,
                evidence=evidence,
            )

    def promote_admission(
        self,
        reservation: SkillProjectionAdmissionReservation,
        *,
        run_id: str,
    ) -> None:
        _validate_text(run_id, "run_id")
        key = self._key(reservation.user_id, reservation.thread_id)
        with self._lock:
            state = self._states.get(key)
            if not self._matches_reservation(state, reservation):
                raise SkillProjectionBusyError()
            if state.run_id is not None and state.run_id != run_id:
                raise SkillProjectionBusyError()
            state.run_id = run_id

    def claim_committed_run(
        self,
        *,
        user_id: str,
        thread_id: str,
        run_id: str,
        snapshot_id: str | None,
        evidence: SkillProjectionEvidence | None = None,
    ) -> None:
        """Compatibility claim for direct harness runs without app admission."""
        if not self.try_claim_committed_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id=run_id,
            snapshot_id=snapshot_id,
            evidence=evidence,
        ):
            raise SkillProjectionBusyError()

    def binding_for_committed_run(
        self,
        *,
        user_id: str,
        thread_id: str,
        run_id: str,
    ) -> tuple[int, str | None, SkillProjectionEvidence]:
        """Return the exact accepted projection already owned by ``run_id``."""

        key = self._key(user_id, thread_id)
        _validate_text(run_id, "run_id")
        with self._lock:
            state = self._states.get(key)
            if state is None or state.run_id != run_id or state.clearing is not None or not isinstance(state.evidence, SkillProjectionEvidence):
                raise SkillProjectionBusyError()
            return state.generation, state.snapshot_id, state.evidence

    def try_claim_committed_run(
        self,
        *,
        user_id: str,
        thread_id: str,
        run_id: str,
        snapshot_id: str | None,
        evidence: SkillProjectionEvidence | None = None,
    ) -> bool:
        """Try one non-blocking claim without replacing another run's owner."""
        key = self._key(user_id, thread_id)
        _validate_text(run_id, "run_id")
        _validate_snapshot_id(snapshot_id)
        if evidence is not None and evidence.snapshot_id != snapshot_id:
            raise ValueError("skill projection evidence does not match snapshot_id")
        with self._lock:
            state = self._states.get(key)
            if state is not None:
                if state.clearing is not None:
                    return False
                if state.run_id == run_id and state.snapshot_id == snapshot_id and state.evidence == evidence:
                    return True
                return False
            generation = self._next_generation
            self._next_generation += 1
            self._states[key] = _ProjectionState(
                reservation_id=f"committed:{run_id}",
                generation=generation,
                snapshot_id=snapshot_id,
                evidence=evidence,
                run_id=run_id,
            )
            return True

    def fence_committed_owner(
        self,
        *,
        user_id: str,
        thread_id: str,
    ) -> SkillProjectionSupersessionFence:
        """Capture the exact old owner an atomic SQL batch may supersede."""
        key = self._key(user_id, thread_id)
        with self._lock:
            state = self._states.get(key)
            if state is None or state.run_id is None or state.clearing is not None:
                raise SkillProjectionBusyError()
            return SkillProjectionSupersessionFence(
                user_id=user_id,
                thread_id=thread_id,
                run_id=state.run_id,
                generation=state.generation,
                snapshot_id=state.snapshot_id,
                evidence=state.evidence,
            )

    def promote_supersession(
        self,
        fence: SkillProjectionSupersessionFence,
        *,
        run_id: str,
        snapshot_id: str | None,
        evidence: SkillProjectionEvidence | None = None,
    ) -> bool:
        """Install the replacement only if the fenced old owner has no consumers."""
        key = self._key(fence.user_id, fence.thread_id)
        _validate_text(run_id, "run_id")
        _validate_snapshot_id(snapshot_id)
        if evidence is not None and evidence.snapshot_id != snapshot_id:
            raise ValueError("skill projection evidence does not match snapshot_id")
        with self._lock:
            state = self._states.get(key)
            if state is not None:
                matches_fence = state.run_id == fence.run_id and state.generation == fence.generation and state.snapshot_id == fence.snapshot_id and state.evidence == fence.evidence
                if not matches_fence or state.consumers or state.clearing is not None:
                    return False
            generation = self._next_generation
            self._next_generation += 1
            self._states[key] = _ProjectionState(
                reservation_id=f"replacement:{run_id}",
                generation=generation,
                snapshot_id=snapshot_id,
                evidence=evidence,
                run_id=run_id,
            )
            return True

    def abort_admission(self, reservation: SkillProjectionAdmissionReservation) -> bool:
        """Remove only the exact unactivated reservation."""
        key = self._key(reservation.user_id, reservation.thread_id)
        with self._lock:
            state = self._states.get(key)
            if not self._matches_reservation(state, reservation) or state.run_id is not None or state.consumers:
                return False
            self._states.pop(key, None)
            return True

    @staticmethod
    def _matches_reservation(
        state: _ProjectionState | None,
        reservation: SkillProjectionAdmissionReservation,
    ) -> bool:
        return bool(state is not None and state.reservation_id == reservation.reservation_id and state.generation == reservation.generation and state.snapshot_id == reservation.snapshot_id and state.evidence == reservation.evidence)

    def activate(
        self,
        *,
        user_id: str,
        thread_id: str,
        sandbox_id: str,
        run_id: str,
        snapshot_id: str | None,
        consumer_id: str,
    ) -> SkillProjectionConsumerToken:
        key = self._key(user_id, thread_id)
        for value, name in (
            (sandbox_id, "sandbox_id"),
            (run_id, "run_id"),
            (consumer_id, "consumer_id"),
        ):
            _validate_text(value, name)
        _validate_snapshot_id(snapshot_id)
        with self._lock:
            state = self._states.get(key)
            if state is None or state.clearing is not None or state.run_id != run_id or state.snapshot_id != snapshot_id:
                raise SkillProjectionBusyError()
            if state.sandbox_id is None:
                state.sandbox_id = sandbox_id
            elif state.sandbox_id != sandbox_id:
                raise SkillProjectionBusyError()
            existing = state.consumers.get(consumer_id)
            if existing is not None:
                return existing
            token = SkillProjectionConsumerToken(
                user_id=user_id,
                thread_id=thread_id,
                sandbox_id=sandbox_id,
                run_id=run_id,
                generation=state.generation,
                consumer_id=consumer_id,
                snapshot_id=snapshot_id,
                evidence=state.evidence,
            )
            state.consumers[consumer_id] = token
            return token

    def retain(
        self,
        token: SkillProjectionConsumerToken,
        *,
        consumer_id: str,
    ) -> SkillProjectionConsumerToken:
        _validate_text(consumer_id, "consumer_id")
        key = self._key(token.user_id, token.thread_id)
        with self._lock:
            state = self._states.get(key)
            if state is None or state.clearing is not None or not self._matches_token_state(state, token) or token.consumer_id not in state.consumers:
                raise SkillProjectionBusyError()
            existing = state.consumers.get(consumer_id)
            if existing is not None:
                return existing
            retained = SkillProjectionConsumerToken(
                user_id=token.user_id,
                thread_id=token.thread_id,
                sandbox_id=token.sandbox_id,
                run_id=token.run_id,
                generation=token.generation,
                consumer_id=consumer_id,
                snapshot_id=token.snapshot_id,
                evidence=token.evidence,
            )
            state.consumers[consumer_id] = retained
            return retained

    def owns(self, token: SkillProjectionConsumerToken) -> bool:
        """Return whether this exact consumer still belongs to live state."""
        key = self._key(token.user_id, token.thread_id)
        with self._lock:
            state = self._states.get(key)
            return bool(self._matches_token_state(state, token) and state.consumers.get(token.consumer_id) == token)

    @staticmethod
    def _matches_token_state(
        state: _ProjectionState | None,
        token: SkillProjectionConsumerToken,
    ) -> bool:
        return bool(state is not None and state.run_id == token.run_id and state.generation == token.generation and state.snapshot_id == token.snapshot_id and state.sandbox_id == token.sandbox_id and state.evidence == token.evidence)

    def release(self, token: SkillProjectionConsumerToken) -> SkillProjectionClear | None:
        """Begin last-consumer cleanup while retaining exclusive ownership."""
        key = self._key(token.user_id, token.thread_id)
        with self._lock:
            state = self._states.get(key)
            if state is not None and state.clearing is not None:
                return state.clearing if state.clearing_token == token else None
            if state is None or not self._matches_token_state(state, token):
                return None
            if state.consumers.get(token.consumer_id) != token:
                return None
            state.consumers.pop(token.consumer_id, None)
            if state.consumers:
                return None
            clear = SkillProjectionClear(
                user_id=token.user_id,
                thread_id=token.thread_id,
                sandbox_id=token.sandbox_id,
                run_id=token.run_id,
                generation=token.generation,
                snapshot_id=token.snapshot_id,
                evidence=token.evidence,
            )
            state.clearing = clear
            state.clearing_token = token
            return clear

    def finalize_release(self, clear: SkillProjectionClear) -> bool:
        """Remove ownership only after provider cleanup completed successfully."""
        key = self._key(clear.user_id, clear.thread_id)
        with self._lock:
            state = self._states.get(key)
            if (
                state is None
                or state.clearing != clear
                or state.clearing_token is None
                or state.consumers
                or state.run_id != clear.run_id
                or state.generation != clear.generation
                or state.snapshot_id != clear.snapshot_id
                or state.sandbox_id != clear.sandbox_id
                or state.evidence != clear.evidence
            ):
                return False
            self._states.pop(key, None)
            return True

    def release_unactivated_run(self, *, user_id: str, thread_id: str, run_id: str) -> bool:
        """Release a committed owner only when no sandbox consumer ever activated."""
        key = self._key(user_id, thread_id)
        with self._lock:
            state = self._states.get(key)
            if state is None or state.clearing is not None or state.run_id != run_id or state.sandbox_id is not None or state.consumers:
                return False
            self._states.pop(key, None)
            return True

    def current_token(self, *, user_id: str, thread_id: str) -> SkillProjectionConsumerToken | None:
        key = self._key(user_id, thread_id)
        with self._lock:
            state = self._states.get(key)
            if state is None or not state.consumers:
                return None
            return state.consumers[sorted(state.consumers)[0]]

    def token_for_consumer(
        self,
        *,
        user_id: str,
        thread_id: str,
        run_id: str,
        consumer_id: str,
    ) -> SkillProjectionConsumerToken | None:
        key = self._key(user_id, thread_id)
        with self._lock:
            state = self._states.get(key)
            if state is None or state.run_id != run_id:
                return None
            token = state.consumers.get(consumer_id)
            if token is not None:
                return token
            if state.clearing_token is not None and state.clearing_token.consumer_id == consumer_id:
                return state.clearing_token
            return None

    def is_busy(self, *, user_id: str, thread_id: str) -> bool:
        key = self._key(user_id, thread_id)
        with self._lock:
            return key in self._states


_coordinator = SkillProjectionCoordinator()


def get_skill_projection_coordinator() -> SkillProjectionCoordinator:
    """Return the process-wide coordinator used by admission and workers."""
    return _coordinator


__all__ = [
    "SkillProjectionAdmissionReservation",
    "SkillProjectionBusyError",
    "SkillProjectionClear",
    "SkillProjectionConsumerToken",
    "SkillProjectionCoordinator",
    "SkillProjectionEvidence",
    "SkillProjectionSupersessionFence",
    "SKILL_PROJECTION_TOKEN_CONTEXT_KEY",
    "get_skill_projection_coordinator",
]

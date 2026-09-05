"""The bounded sandbox diagnostic stream, for both session kinds.

Authority-relevant lifecycle stays closed: the five
:class:`~deerflow.sandbox.accepted_material.AcceptedSandboxLifecycleKind` states
are digest-bound to accepted execution evidence, capped at eight per durable
attempt, and an overflow is refused rather than truncated. Everything else
worth knowing about a sandbox session is a diagnostic: an open, namespaced
kind (``egress.blocked``, ``egress.decided``, ``scope.released``) with bounded
scalar facts, recorded for the ordinary and the accepted kind alike and
dropped oldest-first when a run's stream is full. A diagnostic explains; it
never authorizes execution or cleanup, never carries a provider handle, and a
failure to record never obscures the operation that was being observed.

An ordinary session's diagnostics are thread-scoped: they carry the thread and
the provider's own sandbox id, which is the public ref for that kind. An
accepted session's diagnostics are run-bound: they carry the public ref, the
attempt, and the execution evidence digest, and never the container id. The
worker publishes each run's stream to the run event store as
``sandbox.diagnostic.v1`` events beside the lifecycle events.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from collections import OrderedDict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Self

from deerflow.sandbox.session import SandboxSessionKind, current_sandbox_session

logger = logging.getLogger(__name__)

SANDBOX_DIAGNOSTIC_STREAM_CAPACITY = 64
_MAX_TRACKED_RUNS = 1024
_MAX_FACTS = 16
_MAX_FACT_TEXT = 256
_MAX_FACT_INT = 2**53
_MAX_OBSERVATION_BYTES = 4096
_KIND = re.compile(r"^[a-z][a-z0-9_]{0,31}(?:\.[a-z][a-z0-9_]{0,31}){1,2}$")
_FACT_KEY = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()


def _canonical_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("sandbox diagnostic observed_at must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _reference(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 512 or any(ch in value for ch in "\x00\r\n"):
        raise ValueError(f"sandbox diagnostic {field_name} is invalid")
    return value


def _facts(value: object) -> dict[str, str | int | bool]:
    if not isinstance(value, Mapping) or len(value) > _MAX_FACTS:
        raise ValueError("sandbox diagnostic facts must be a mapping of at most 16 scalars")
    facts: dict[str, str | int | bool] = {}
    for key, fact in value.items():
        if not isinstance(key, str) or _FACT_KEY.fullmatch(key) is None:
            raise ValueError("sandbox diagnostic facts key is invalid")
        if isinstance(fact, bool):
            facts[key] = fact
        elif isinstance(fact, int):
            if abs(fact) > _MAX_FACT_INT:
                raise ValueError("sandbox diagnostic facts integer is out of range")
            facts[key] = fact
        elif isinstance(fact, str):
            if len(fact) > _MAX_FACT_TEXT or any(ch in fact for ch in "\x00\r\n"):
                raise ValueError("sandbox diagnostic facts text is invalid")
            facts[key] = fact
        else:
            raise ValueError("sandbox diagnostic facts must be strings, integers or booleans")
    return dict(sorted(facts.items()))


@dataclass(frozen=True, slots=True)
class SandboxDiagnosticObservationV1:
    """One bounded, handle-free fact about a sandbox session.

    ``kind`` is open but namespaced (``area.event``); the closed lifecycle
    names can never be spelled here. Facts are a small mapping of scalars.
    """

    version: Literal[1]
    kind: str
    session_kind: SandboxSessionKind
    run_id: str
    thread_id: str
    sandbox_ref: str
    attempt_ref: str | None
    batch_child_attempt_ref: str | None
    execution_evidence_digest: str | None
    observed_at: datetime
    facts: Mapping[str, str | int | bool]
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("sandbox diagnostic version must be 1")
        if not isinstance(self.kind, str) or _KIND.fullmatch(self.kind) is None:
            raise ValueError("sandbox diagnostic kind must be namespaced as area.event")
        try:
            session_kind = SandboxSessionKind(self.session_kind)
        except (TypeError, ValueError) as exc:
            raise ValueError("sandbox diagnostic session kind is invalid") from exc
        object.__setattr__(self, "session_kind", session_kind)
        for field_name in ("run_id", "thread_id", "sandbox_ref"):
            _reference(getattr(self, field_name), field_name)
        for field_name in ("attempt_ref", "batch_child_attempt_ref"):
            value = getattr(self, field_name)
            if value is not None:
                _reference(value, field_name)
        if self.execution_evidence_digest is not None and (not isinstance(self.execution_evidence_digest, str) or _DIGEST.fullmatch(self.execution_evidence_digest) is None):
            raise ValueError("sandbox diagnostic execution evidence digest is invalid")
        if session_kind is SandboxSessionKind.ACCEPTED:
            if self.execution_evidence_digest is None:
                raise ValueError("an accepted sandbox diagnostic must carry its execution evidence digest")
            if self.attempt_ref is None:
                raise ValueError("an accepted sandbox diagnostic must carry its attempt reference")
        elif self.execution_evidence_digest is not None or self.attempt_ref is not None or self.batch_child_attempt_ref is not None:
            raise ValueError("an ordinary sandbox diagnostic is thread-scoped and carries no accepted references")
        object.__setattr__(self, "facts", _facts(self.facts))
        _canonical_timestamp(self.observed_at)
        if not isinstance(self.digest, str) or _DIGEST.fullmatch(self.digest) is None:
            raise ValueError("sandbox diagnostic digest is invalid")
        payload = self._digest_payload()
        if self.digest != _canonical_digest(payload):
            raise ValueError("sandbox diagnostic digest does not match its content")
        if len(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > _MAX_OBSERVATION_BYTES:
            raise ValueError("sandbox diagnostic observation is too large")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "kind": self.kind,
            "session_kind": self.session_kind.value,
            "run_id": self.run_id,
            "thread_id": self.thread_id,
            "sandbox_ref": self.sandbox_ref,
            "attempt_ref": self.attempt_ref,
            "batch_child_attempt_ref": self.batch_child_attempt_ref,
            "execution_evidence_digest": self.execution_evidence_digest,
            "observed_at": _canonical_timestamp(self.observed_at),
            "facts": dict(self.facts),
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        kind: str,
        session_kind: SandboxSessionKind,
        run_id: str,
        thread_id: str,
        sandbox_ref: str,
        observed_at: datetime,
        facts: Mapping[str, str | int | bool],
        attempt_ref: str | None = None,
        batch_child_attempt_ref: str | None = None,
        execution_evidence_digest: str | None = None,
    ) -> Self:
        if not isinstance(kind, str) or _KIND.fullmatch(kind) is None:
            raise ValueError("sandbox diagnostic kind must be namespaced as area.event")
        payload = {
            "version": 1,
            "kind": kind,
            "session_kind": SandboxSessionKind(session_kind).value,
            "run_id": run_id,
            "thread_id": thread_id,
            "sandbox_ref": sandbox_ref,
            "attempt_ref": attempt_ref,
            "batch_child_attempt_ref": batch_child_attempt_ref,
            "execution_evidence_digest": execution_evidence_digest,
            "observed_at": _canonical_timestamp(observed_at),
            "facts": _facts(facts),
        }
        return cls(
            version=1,
            kind=kind,
            session_kind=SandboxSessionKind(session_kind),
            run_id=run_id,
            thread_id=thread_id,
            sandbox_ref=sandbox_ref,
            attempt_ref=attempt_ref,
            batch_child_attempt_ref=batch_child_attempt_ref,
            execution_evidence_digest=execution_evidence_digest,
            observed_at=observed_at,
            facts=payload["facts"],  # type: ignore[arg-type]
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "kind",
            "session_kind",
            "run_id",
            "thread_id",
            "sandbox_ref",
            "attempt_ref",
            "batch_child_attempt_ref",
            "execution_evidence_digest",
            "observed_at",
            "facts",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("sandbox diagnostic observation has unknown or missing fields")
        observed_at = value["observed_at"]
        if not isinstance(observed_at, str):
            raise ValueError("sandbox diagnostic observed_at is invalid")
        try:
            parsed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("sandbox diagnostic observed_at is invalid") from exc
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            kind=value["kind"],  # type: ignore[arg-type]
            session_kind=value["session_kind"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            thread_id=value["thread_id"],  # type: ignore[arg-type]
            sandbox_ref=value["sandbox_ref"],  # type: ignore[arg-type]
            attempt_ref=value["attempt_ref"],  # type: ignore[arg-type]
            batch_child_attempt_ref=value["batch_child_attempt_ref"],  # type: ignore[arg-type]
            execution_evidence_digest=value["execution_evidence_digest"],  # type: ignore[arg-type]
            observed_at=parsed,
            facts=value["facts"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


class SandboxDiagnosticStream:
    """A run's diagnostics: append-only up to capacity, then drop oldest.

    Every observation gets a monotonic sequence number so a publisher can
    drain what it has not seen yet; ``dropped`` counts what the ring let go.
    Dropping oldest is deliberate: a diagnostic stream that refused writes
    would report silence exactly when something is going wrong.
    """

    def __init__(self, capacity: int = SANDBOX_DIAGNOSTIC_STREAM_CAPACITY) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self._lock = threading.Lock()
        self._entries: deque[tuple[int, SandboxDiagnosticObservationV1]] = deque(maxlen=capacity)
        self._next_sequence = 0
        self._dropped = 0
        self._once: set[tuple[object, ...]] = set()

    def record(self, observation: SandboxDiagnosticObservationV1) -> int:
        if not isinstance(observation, SandboxDiagnosticObservationV1):
            raise TypeError("observation must be SandboxDiagnosticObservationV1")
        with self._lock:
            sequence = self._next_sequence
            self._next_sequence += 1
            if len(self._entries) == self._entries.maxlen:
                self._dropped += 1
            self._entries.append((sequence, observation))
            return sequence

    def record_once(self, key: tuple[object, ...], observation: SandboxDiagnosticObservationV1) -> int | None:
        """Record unless ``key`` was already recorded on this stream."""
        with self._lock:
            if key in self._once:
                return None
            self._once.add(key)
        return self.record(observation)

    def since(self, sequence: int) -> tuple[tuple[int, SandboxDiagnosticObservationV1], ...]:
        with self._lock:
            return tuple(entry for entry in self._entries if entry[0] >= sequence)

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


_streams_lock = threading.Lock()
# One stream per run for the runs this process is executing. The worker
# discards a run's stream after its final publication; the bound protects the
# process from runs that never drain (embedders without a worker fence).
_streams: OrderedDict[str, SandboxDiagnosticStream] = OrderedDict()


def sandbox_diagnostics(run_id: str) -> SandboxDiagnosticStream:
    """The diagnostic stream for ``run_id``, created on first use."""
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("run_id must be a non-empty string")
    with _streams_lock:
        stream = _streams.get(run_id)
        if stream is None:
            stream = SandboxDiagnosticStream()
            _streams[run_id] = stream
            while len(_streams) > _MAX_TRACKED_RUNS:
                _streams.popitem(last=False)
        else:
            _streams.move_to_end(run_id)
        return stream


def discard_sandbox_diagnostics(run_id: str) -> None:
    """Forget ``run_id``'s stream; a later record starts a fresh one."""
    with _streams_lock:
        _streams.pop(run_id, None)


def current_accepted_sandbox_bridge():  # pragma: no cover - thin indirection for monkeypatching
    from deerflow.sandbox.accepted_material import current_accepted_sandbox_bridge as resolve

    return resolve()


def record_sandbox_diagnostic(
    context: object,
    kind: str,
    *,
    facts: Mapping[str, str | int | bool],
    sandbox_ref: str | None = None,
    once: bool = False,
) -> SandboxDiagnosticObservationV1 | None:
    """Record a fact about the executing context's sandbox session.

    The run and thread come from the runtime context; the session kind, public
    ref and accepted anchors come from the executing session declaration, or
    from the context's ``sandbox_id`` for the ordinary kind. Returns ``None``,
    and never raises, when there is no run to attach the fact to, when the
    fact is malformed, or when ``once`` finds the same fact already recorded.
    """
    try:
        if not isinstance(context, Mapping):
            return None
        run_id = context.get("run_id")
        thread_id = context.get("thread_id")
        if not isinstance(run_id, str) or not run_id or not isinstance(thread_id, str) or not thread_id:
            return None
        session_kind = SandboxSessionKind.ORDINARY
        attempt_ref = None
        batch_child_attempt_ref = None
        execution_evidence_digest = None
        declaration = current_sandbox_session()
        if declaration is not None and declaration.kind is SandboxSessionKind.ACCEPTED:
            bridge = current_accepted_sandbox_bridge()
            if bridge is None:
                return None
            session_kind = SandboxSessionKind.ACCEPTED
            sandbox_ref = declaration.public_ref
            attempt_ref = bridge.attempt_ref
            batch_child_attempt_ref = bridge.batch_child_attempt_ref
            execution_evidence_digest = bridge.execution_evidence_digest
        elif sandbox_ref is None:
            candidate = context.get("sandbox_id")
            sandbox_ref = candidate if isinstance(candidate, str) and candidate else None
        if sandbox_ref is None:
            return None
        observation = SandboxDiagnosticObservationV1.build(
            kind=kind,
            session_kind=session_kind,
            run_id=run_id,
            thread_id=thread_id,
            sandbox_ref=sandbox_ref,
            observed_at=datetime.now(UTC),
            facts=facts,
            attempt_ref=attempt_ref,
            batch_child_attempt_ref=batch_child_attempt_ref,
            execution_evidence_digest=execution_evidence_digest,
        )
        stream = sandbox_diagnostics(run_id)
        if once:
            key = (kind, sandbox_ref, tuple(sorted(observation.facts.items())))
            if stream.record_once(key, observation) is None:
                return None
        else:
            stream.record(observation)
        return observation
    except Exception:
        logger.debug("Sandbox diagnostic %s was not recorded", kind, exc_info=True)
        return None


__all__ = [
    "SANDBOX_DIAGNOSTIC_STREAM_CAPACITY",
    "SandboxDiagnosticObservationV1",
    "SandboxDiagnosticStream",
    "discard_sandbox_diagnostics",
    "record_sandbox_diagnostic",
    "sandbox_diagnostics",
]

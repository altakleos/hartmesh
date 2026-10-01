"""Bounded, correlated phase timings for one interactive turn.

Why this exists
---------------
A turn that takes thirty seconds to reach its first model call and one second
to finish it is not a slow model, but nothing in the run record said so. This
module records *where* a turn's wall time went, along the path the turn
actually took, so application overhead can be told apart from provider latency
without guessing from unsynchronized clocks.

What it is not
--------------
Not a telemetry service and not a metrics backend. A journal is an in-memory
object with a fixed field set; it is written by appends, read by whoever opened
it, and emitted once as a single structured log record. Nothing here opens a
socket, touches the filesystem, or blocks the event loop, so instrumentation
cannot become the latency it measures.

Timing
------
Every offset is a monotonic duration from the journal's own start
(:func:`time.monotonic`), never a difference between two wall clocks. Phases
observed in a different task or thread from the one that opened the journal --
the SSE consumer runs in the Gateway request task while the run executes in its
own -- correlate through the run id, which is why the registry exists beside the
context variable.

Disclosure
----------
The recorded vocabulary is closed: phase names, acquisition sources and outcome
labels are enum members, and every free-form field passes through
:func:`_bounded_label`. Model requests, answers, tool output, credentials,
relay headers, provider handles and deployment identities are never recorded --
only that a phase happened and when. Correlation ids live in the record's own
fields, never in a metric label.

One exception, taken deliberately: a tool's *registered name* is recorded, so
``tools=14/308000ms(execute_command=9/270000ms,...)`` names the call that spent
the turn instead of leaving an operator to instrument again and re-run. Its
arguments and its output are not, and neither is anything about the call beyond
its name and how long it took. Note what that admits: built-in tool names are
first-party vocabulary, but MCP and skill-provided tools are named by the
operator's configuration, so those names now reach the Gateway's logs. Names
are bounded and the distinct set is capped
(:data:`MAX_TRACKED_TOOL_NAMES`, then :data:`OTHER_TOOLS_LABEL`).

Acquisition
-----------
``acquisition_source`` is the turn's own *origin* -- how the container it used
came to be. A turn acquires in stages, and the later ones can only observe that
the container is already in hand, so ``IN_PROCESS`` and ``ACCEPTED_ACTIVE`` are
classified as observations (:attr:`AcquisitionSource.observes_active_reuse`):
they never replace a recorded origin, riding beside it as ``acquisition_reuse``
instead. A new source belongs on one side of that line, deliberately. When no
stage reported an origin, the observation is what the turn observed and is
reported as the source; nothing is inferred to fill the slot.

Counting
--------
The unit of every resource counter is one *resource set*: the sandbox
container together with its network sidecar and networks on the local
restricted backend, the Pod together with its Service on the remote backend.
A ``create`` call is an *attempt*; it is counted as a new resource set only
when the backend says it started one (``SandboxInfo.provenance == "created"``),
as a *rediscovery* when the backend returned one that already existed, and as
*unknown* when the backend did not say. A teardown is likewise an attempt until
the backend establishes the set *absent* (``DestroyOutcome.ABSENT``); an
ownership-fenced refusal is counted as a refusal, and a stop or remove that
failed, left part of the set behind or could not be observed is counted as a
failure -- neither is a disappearance, and a set that takes several retries to
go is counted absent once, on the retry that confirmed it. The journal's
counters are what this process observed through its own calls; the backend's
own counters (Docker, the provisioner) are the independent record to
reconcile them against.

A turn's own work is counted the same way but as *occupancy*, not as spans:
``tool_calls``/``tool_ms`` and ``model_calls``/``model_ms`` say how many calls
of each kind the turn made and how much of its wall clock the completed ones
covered. Overlap counts once, so each figure is inside ``total_ms`` -- but
their *sum* need not be, because a tool and a model call can run at the same
time. ``busy_ms`` is the two merged together and is the one to subtract from
the total; the difference from ``tool_ms + model_ms`` is the overlap. A call
whose end was never seen is reported as open (``tool_open``) rather than
folded into the measured figure, which is what keeps one lost event from
turning the rest of the turn into tool time.

Scope: this journal hears the lead agent's own calls. A subagent is invoked
with its own ``callbacks`` list, which replaces the inherited one rather than
extending it, so its tool and model calls are not counted here -- a blocking
delegation appears as the one long tool call it is, and a backgrounded one
appears only as the short call that launched it.

Honesty
-------
A phase that could not be observed is recorded as such with a reason
(:meth:`TurnPhaseJournal.unobservable`) rather than inferred from a neighbouring
event. Browser submit-to-first-rendered-text is not observable from here at all:
it needs a browser measuring through public ingress, and no server timestamp
substitutes for it. The first outgoing SSE text is observable only by a consumer
in the same process during the run's live window: a join stream served by
another Gateway replica, or a ``Last-Event-ID`` replay after the run ended,
cannot reach the journal, and the run wrapper declares the phase unobservable
when the provider produced text and no consumer here marked it.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from deerflow.diagnostics import bounded_label as _bounded_label

logger = logging.getLogger(__name__)

# One turn cannot legitimately produce more than this many phase records; the
# cap is what keeps a pathological loop from turning the journal into a leak.
MAX_PHASE_RECORDS = 256
# How many finished-but-unclosed run journals the registry keeps. Bounded so a
# run that never reaches its terminal cannot retain memory indefinitely.
MAX_TRACKED_RUNS = 64
# How much of the journal the emitted *message* carries. The structured record
# is complete; the message is what a deployment's formatter actually prints, so
# it is rendered for an operator reading one line per turn and is bounded
# independently of the record cap above.
MAX_RENDERED_PHASES = 24
# Of that budget, how many of the *last* records are always kept. A turn with
# goal continuations records a graph start and a sandbox binding per attempt,
# so head-only truncation drops model_completion and terminal -- the end of the
# arithmetic this line exists for -- before it drops a repeated early span.
MAX_RENDERED_PHASE_TAIL = 6
MAX_RENDERED_UNOBSERVABLE = 4
MAX_RENDERED_REASON = 80
# How many calls of one kind may be tracked open at once. A framework that
# loses an end event leaves one open forever, so the set that holds them is
# capped rather than trusted to drain.
MAX_OPEN_CALLS = 64
# How many closed intervals are kept before they are merged down to the
# disjoint stretches they cover. Merging loses nothing: the union is the
# only thing ever read back from them.
MAX_TRACKED_INTERVALS = 256
# How many distinct tool names are broken out before the rest are pooled
# into one bucket. A turn uses a handful; the cap is for a tool plane that
# mints names, so the journal cannot grow with the configuration.
MAX_TRACKED_TOOL_NAMES = 16
#: Where calls beyond that cap are counted.
OTHER_TOOLS_LABEL = "other"
#: How long a tool label may be, digest suffix included.
TOOL_LABEL_LIMIT = 48
# How many of them the emitted *message* names, largest first. The
# structured record carries all of them.
MAX_RENDERED_TOOL_NAMES = 4


class TurnPhase(StrEnum):
    """The closed set of phases a turn is measured in.

    Closed to callers, not frozen: members are added as a phase is broken into
    the steps it is made of. ``to_wire``'s version stamps the record *shape*,
    which is why it does not move when a member is added.
    """

    ADMISSION = "admission"
    ASSEMBLY = "assembly"
    # The accepted preparation before the model. On a durable profile it is the
    # whole materialization, so the sandbox phases *it records* nest inside this
    # one (``SANDBOX_LOOKUP``, and on a cold turn ``SANDBOX_CREATE`` and
    # ``SANDBOX_READINESS``). On the projection profile it holds only the
    # authorization: the first sandbox-backed tool call acquires the sandbox
    # and records ``SKILL_PROJECTION`` and ``SKILL_SNAPSHOT_BIND`` there. The
    # middleware's ``SANDBOX_BINDING`` and ``SANDBOX_ACQUIRE`` run after the
    # graph starts and never nest here.
    SKILL_MATERIALIZATION = "skill_materialization"
    # The steps that phase is made of, so its own figure is attributed rather
    # than being one unexplained block. Tenant-class .17 measured 5 to 6 s
    # before the first model request on a *warm* turn, which is most of a
    # one-sentence revision's budget, and named none of it.
    #
    #   ACCEPTED_AUTHORIZATION     authorizing the execution and choosing who
    #                              materializes it. Not free and not local: it
    #                              resolves the provider, and on a durable
    #                              profile the selection asks the sandbox
    #                              backend for its pinned runtime digest.
    #   ACCEPTED_MATERIAL_VERIFY   re-digesting the published snapshot and
    #                              capturing its file manifest -- three walks
    #                              of the same tree. Durable profiles only.
    #   SKILL_PROJECTION           the provider putting the accepted material
    #                              in a sandbox (``SANDBOX_LOOKUP`` and, on a
    #                              cold turn, ``SANDBOX_CREATE`` and
    #                              ``SANDBOX_READINESS`` nest inside this one).
    #                              A provider that binds while it provisions
    #                              publishes the snapshot in here too.
    #   SKILL_SNAPSHOT_BIND        binding that snapshot as the thread's only
    #                              skills mount. Against a provider that has
    #                              already bound, this is the idempotent
    #                              receipt check, which verifies the published view in place and stages a copy only when that fails.
    #
    # Each span is read back by its *first* record, so one name must not be
    # opened twice in a turn: the second is measured and then dropped, and its
    # cost reappears as residual nobody can account for.
    #
    # What is left inside ``SKILL_MATERIALIZATION`` on the released projection
    # profile is the binding lookup and the isolation assertions, and a turn
    # where that residual is not small is itself the finding. A durable profile
    # leaves more there: the materialization validation and two execution-fence
    # round trips.
    ACCEPTED_AUTHORIZATION = "accepted_authorization"
    ACCEPTED_MATERIAL_VERIFY = "accepted_material_verify"
    SKILL_PROJECTION = "skill_projection"
    SKILL_SNAPSHOT_BIND = "skill_snapshot_bind"
    # The window between assembly and the model request is most of a warm
    # turn's wait. Tenant-class .15 measured 2.6 to 3.4 s of it per turn with
    # nothing named, so these three say where the rest of it goes: building the
    # graph, the checkpoint preflight that loads the thread's state, and the
    # worker entering the stream attempt.
    AGENT_BUILD = "agent_build"
    CHECKPOINT_PREFLIGHT = "checkpoint_preflight"
    GRAPH_START = "graph_start"
    SANDBOX_ACQUIRE = "sandbox_acquire"
    SANDBOX_LOOKUP = "sandbox_lookup"
    # Waiting for a replica slot before any container work begins. The budget
    # is hard, so a turn that arrives while every slot is in active use waits
    # here and is then refused -- and without this member that wait was
    # residual nobody could account for, which is the whole reason the phase
    # exists. Opened once per turn even when the admission loop goes round
    # several times (evict, recheck, wait again): a name opened twice has its
    # second span measured and dropped, per the note above.
    SANDBOX_CAPACITY_WAIT = "sandbox_capacity_wait"
    SANDBOX_EVICTION = "sandbox_eviction"
    SANDBOX_CREATE = "sandbox_create"
    SANDBOX_READINESS = "sandbox_readiness"
    SANDBOX_BINDING = "sandbox_binding"
    MODEL_REQUEST = "model_request"
    FIRST_PROVIDER_TEXT = "first_provider_text"
    FIRST_STREAM_TEXT = "first_stream_text"
    MODEL_COMPLETION = "model_completion"
    # Where the turn's tool work began. The phases above end at the first
    # answer, so on a turn that then goes to work -- a report is minutes of
    # commands after a few seconds of talking -- everything after
    # ``model_completion`` was one unexplained block. This names the start of
    # it; ``tool_calls``/``tool_ms`` and ``model_calls``/``model_ms`` say how
    # much of the block was each. Marked once, like the first-text phases: a
    # turn makes many tool calls and a span name may only be opened once.
    TOOL_EXECUTION = "tool_execution"
    TERMINAL = "terminal"


class AcquisitionSource(StrEnum):
    """Where a turn's sandbox came from.

    The three reuse sources are the ones that cost no container creation. They
    are distinguished rather than merged because "reused" alone cannot tell a
    warm reclaim (the repair's subject) from an in-process cache hit.
    """

    IN_PROCESS = "in_process"
    WARM_RECLAIM = "warm_reclaim"
    ACCEPTED_ACTIVE = "accepted_active"
    ACCEPTED_WARM_RECLAIM = "accepted_warm_reclaim"
    DISCOVERED = "discovered"
    # The backend's ``create`` returned a resource that already existed
    # instead of starting one. Distinct from ``DISCOVERED`` (found before
    # create was attempted) because the create attempt was paid for.
    REDISCOVERED = "rediscovered"
    CREATED = "created"
    # The backend answered ``create`` without saying whether it started or
    # found the resource. Not a reuse source and not a creation: unknown
    # provenance is never reported as either.
    UNKNOWN_PROVENANCE = "unknown_provenance"

    @property
    def reuses_existing_resource(self) -> bool:
        return self in _REUSE_SOURCES

    @property
    def observes_active_reuse(self) -> bool:
        """This source says the container was already in hand, not how it came to be.

        A turn acquires in stages: the worker projects the accepted skills
        before the graph runs, and the sandbox middleware binds later against
        whatever is now active. The later stage can only observe that the
        container is there, which is true of a turn that just created it, a
        turn that reclaimed one, and a turn that did neither -- so it must
        never be allowed to answer *where the container came from*.
        """
        return self in _ACTIVE_REUSE_OBSERVATIONS


# Sources that report an already-held container rather than this turn's own
# acquisition. Distinct from ``_REUSE_SOURCES`` below, which answers the wider
# "did this turn pay for a container creation" question: a warm reclaim reuses
# an existing resource *and* is an origin.
_ACTIVE_REUSE_OBSERVATIONS = frozenset(
    {
        AcquisitionSource.IN_PROCESS,
        AcquisitionSource.ACCEPTED_ACTIVE,
    },
)


_REUSE_SOURCES = frozenset(
    {
        AcquisitionSource.IN_PROCESS,
        AcquisitionSource.WARM_RECLAIM,
        AcquisitionSource.ACCEPTED_ACTIVE,
        AcquisitionSource.ACCEPTED_WARM_RECLAIM,
        AcquisitionSource.DISCOVERED,
        AcquisitionSource.REDISCOVERED,
    },
)


@dataclass(frozen=True, slots=True)
class LaunchTimings:
    """What the route did before the worker opened the journal.

    The journal starts at worker admission, so everything the launch does
    first -- sealing the accepted invocation, authorizing it, persisting the
    row, handing the record to a worker -- would otherwise be the unnamed
    interval between the request and the first phase. The launch records its
    awaited steps against the request's own monotonic stamp; the worker hands
    the result to the journal, which reports the interval as a whole and the
    handoff it cannot see from either side alone. Steps are bounded labels in
    the order they ran, each with its own duration; a name is never free text.
    """

    received_at: float
    persisted_at: float
    steps: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        # Diagnostics never fail a run: a stamp behind the request (two clocks
        # that should have been one) is clamped, not refused.
        object.__setattr__(self, "received_at", float(self.received_at))
        object.__setattr__(self, "persisted_at", max(float(self.received_at), float(self.persisted_at)))
        object.__setattr__(
            self,
            "steps",
            tuple((_bounded_label(name, fallback="unspecified", limit=32), max(0.0, float(ms))) for name, ms in self.steps),
        )


@dataclass(frozen=True, slots=True)
class PhaseRecord:
    """One observed phase: when it started and, if it ended, how long it took."""

    phase: TurnPhase
    started_ms: float
    duration_ms: float | None = None
    detail: str | None = None

    def to_wire(self) -> dict[str, object]:
        wire: dict[str, object] = {"phase": str(self.phase), "at_ms": round(self.started_ms, 3)}
        if self.duration_ms is not None:
            wire["duration_ms"] = round(self.duration_ms, 3)
        if self.detail is not None:
            wire["detail"] = self.detail
        return wire


@dataclass(frozen=True, slots=True)
class TurnPhaseSnapshot:
    """An immutable read of one journal, safe to log and to assert against."""

    correlation_id: str
    run_id: str | None
    total_ms: float
    phases: tuple[PhaseRecord, ...]
    acquisition_source: AcquisitionSource | None
    acquisition_reuse: AcquisitionSource | None
    acquire_reason: str | None
    session_kind: str | None
    snapshot_present: bool | None
    snapshot_package_count: int | None
    mandatory_materialization: bool | None
    create_attempts: int
    resource_creates: int
    resource_rediscoveries: int
    unknown_create_results: int
    teardown_attempts: int
    resource_teardowns: int
    teardown_refusals: int
    teardown_failures: int
    evictions: int
    #: Turns that had to wait for a replica slot, and turns the hard budget
    #: refused. Kept apart because they say different things to an operator: a
    #: tenant that waits routinely is sized close to its limit, one that is
    #: refused routinely is sized under it.
    capacity_waits: int
    capacity_refusals: int
    queue_ms: float
    failed_attempts: int
    #: How many tool calls the turn made, and how much of its wall clock the
    #: *completed* ones covered between them (overlap counted once).
    tool_calls: int
    tool_ms: float
    #: Calls whose end was never seen, and how long they have been open. A
    #: cancelled async tool ends up here; kept beside the measured figure
    #: rather than inside it, so an unfinished call cannot inflate the answer.
    tool_open: int
    tool_open_ms: float
    #: The same for model calls. ``MODEL_REQUEST`` names only the first, so
    #: without these a multi-step turn's later calls are residual too.
    model_calls: int
    model_ms: float
    model_open: int
    model_open_ms: float
    #: What the tool time was spent on: ``(name, calls, ms)`` largest first,
    #: capped at :data:`MAX_TRACKED_TOOL_NAMES` distinct names with the rest
    #: pooled under :data:`OTHER_TOOLS_LABEL`. Only completed calls, merged
    #: per name exactly as ``tool_ms`` is across all of them. The parts can
    #: total less than the whole, never more: two tools that overlap are one
    #: stretch of the turn, and a call opened past :data:`MAX_OPEN_CALLS` is
    #: counted but not timed -- by then ``tool_ms`` has dropped it too.
    tool_names: tuple[tuple[str, int, float], ...]
    #: Both kinds together, overlap counted once across them. Each figure
    #: above is individually inside ``total_ms``, but their sum need not be --
    #: a tool and a model call can run at the same time. This is the one to
    #: subtract from the total, and ``tool_ms + model_ms - busy_ms`` is how
    #: much of the turn was spent doing both at once.
    busy_ms: float
    unobservable: tuple[tuple[str, str], ...]
    outcome: str | None
    dropped_records: int
    #: Request received to the journal's start, when the route stamped it.
    launch_ms: float | None = None
    #: Run row persisted to the journal's start: the worker handoff.
    launch_handoff_ms: float | None = None
    launch_steps: tuple[tuple[str, float], ...] = ()

    def phase_ms(self, phase: TurnPhase) -> float | None:
        """Duration of the first record for *phase*, or ``None`` if unmeasured."""
        for record in self.phases:
            if record.phase is phase and record.duration_ms is not None:
                return record.duration_ms
        return None

    def phase_at_ms(self, phase: TurnPhase) -> float | None:
        """Offset at which *phase* was first observed, or ``None``."""
        for record in self.phases:
            if record.phase is phase:
                return record.started_ms
        return None

    def to_log_line(self) -> str:
        """Render the journal as one bounded line a text formatter will print.

        The structured record is the complete one; this carries the reading an
        operator needs from a deployment's log alone. ``@`` is an offset from
        the turn's start and ``+`` the phase's own measured duration, both in
        milliseconds, so a phase carrying both *ends* at ``@ + duration``:
        ``first_stream_text@1904ms`` minus the end of
        ``sandbox_acquire@145ms+1600ms`` (1745 ms) is the 159 ms wait a reader
        is usually after, with no second log line to correlate.
        """
        parts = [f"turn phase timings run={self.run_id or '-'}", f"correlation={self.correlation_id}", f"total={round(self.total_ms)}ms"]
        if self.outcome:
            parts.append(f"outcome={self.outcome}")
        if self.session_kind:
            parts.append(f"kind={self.session_kind}")
        if self.acquisition_source is not None:
            parts.append(f"acquisition={self.acquisition_source}")
        if self.acquisition_reuse is not None:
            parts.append(f"reused={self.acquisition_reuse}")
        if self.acquire_reason:
            parts.append(f"acquire_reason={self.acquire_reason}")
        if self.snapshot_present is not None:
            snapshot = "present" if self.snapshot_present else "absent"
            if self.snapshot_package_count is not None:
                snapshot = f"{snapshot}/{self.snapshot_package_count}pkg"
            if self.mandatory_materialization:
                snapshot = f"{snapshot}/mandatory"
            parts.append(f"snapshot={snapshot}")
        if self.queue_ms:
            parts.append(f"queue={round(self.queue_ms)}ms")
        # Rendered as calls/time so one field answers both questions a slow
        # working turn raises: how much of it was tools rather than the
        # provider, and whether that was a few long calls or very many. An
        # unfinished call gets its own field rather than joining the measured
        # one, because the two mean different things to a reader.
        for label, calls, occupied_ms, open_calls, open_ms in (
            ("tools", self.tool_calls, self.tool_ms, self.tool_open, self.tool_open_ms),
            ("model", self.model_calls, self.model_ms, self.model_open, self.model_open_ms),
        ):
            if calls:
                # The breakdown rides inside the field rather than beside it,
                # comma-joined, so a reader splitting the line on whitespace
                # keeps "which tool" attached to "how much tool".
                named = ""
                if label == "tools" and self.tool_names:
                    shown = [f"{name}={name_calls}/{round(name_ms)}ms" for name, name_calls, name_ms in self.tool_names[:MAX_RENDERED_TOOL_NAMES]]
                    omitted = len(self.tool_names) - len(shown)
                    if omitted > 0:
                        shown.append(f"+{omitted} more")
                    named = f"({','.join(shown)})"
                parts.append(f"{label}={calls}/{round(occupied_ms)}ms{named}")
            if open_calls:
                parts.append(f"{label}_open={open_calls}/{round(open_ms)}ms")
        if self.busy_ms:
            parts.append(f"busy={round(self.busy_ms)}ms")
        if self.launch_ms is not None:
            steps = [f"{name}={round(ms)}ms" for name, ms in self.launch_steps]
            if self.launch_handoff_ms is not None:
                steps.append(f"handoff={round(self.launch_handoff_ms)}ms")
            launch = f"launch={round(self.launch_ms)}ms"
            # Comma-joined so a reader that splits the line on whitespace
            # keeps the group as one field, unlike the top-level ones.
            parts.append(f"{launch}({','.join(steps)})" if steps else launch)
        for label, value in (
            ("creates", self.resource_creates),
            ("rediscoveries", self.resource_rediscoveries),
            ("teardowns", self.resource_teardowns),
            ("teardown_refusals", self.teardown_refusals),
            ("teardown_failures", self.teardown_failures),
            ("evictions", self.evictions),
            ("capacity_waits", self.capacity_waits),
            ("capacity_refusals", self.capacity_refusals),
            ("failed_attempts", self.failed_attempts),
            ("dropped_records", self.dropped_records),
        ):
            if value:
                parts.append(f"{label}={value}")

        def _entry(record: PhaseRecord) -> str:
            entry = f"{record.phase}@{round(record.started_ms)}ms"
            return entry if record.duration_ms is None else f"{entry}+{round(record.duration_ms)}ms"

        records = self.phases
        if len(records) > MAX_RENDERED_PHASES:
            head = records[: MAX_RENDERED_PHASES - MAX_RENDERED_PHASE_TAIL]
            tail = records[-MAX_RENDERED_PHASE_TAIL:]
        else:
            head, tail = records, ()
        rendered = [_entry(record) for record in head]
        omitted = len(records) - len(head) - len(tail)
        if omitted > 0:
            rendered.append(f"+{omitted} more")
        rendered.extend(_entry(record) for record in tail)
        if rendered:
            parts.append("phases=" + " ".join(rendered))
        for phase, reason in self.unobservable[:MAX_RENDERED_UNOBSERVABLE]:
            parts.append(f"unobservable={phase}({reason[:MAX_RENDERED_REASON]})")
        unobservable_omitted = len(self.unobservable) - MAX_RENDERED_UNOBSERVABLE
        if unobservable_omitted > 0:
            parts.append(f"unobservable=+{unobservable_omitted} more")
        return " ".join(parts)

    def to_wire(self) -> dict[str, object]:
        launch = None
        if self.launch_ms is not None:
            launch = {
                "total_ms": round(self.launch_ms, 3),
                "handoff_ms": None if self.launch_handoff_ms is None else round(self.launch_handoff_ms, 3),
                "steps": [{"step": name, "ms": round(ms, 3)} for name, ms in self.launch_steps],
            }
        return {
            "version": 7,
            "correlation_id": self.correlation_id,
            "launch": launch,
            "run_id": self.run_id,
            "total_ms": round(self.total_ms, 3),
            "acquisition_source": None if self.acquisition_source is None else str(self.acquisition_source),
            "acquisition_reuse": None if self.acquisition_reuse is None else str(self.acquisition_reuse),
            "acquire_reason": self.acquire_reason,
            "session_kind": self.session_kind,
            "snapshot_present": self.snapshot_present,
            "snapshot_package_count": self.snapshot_package_count,
            "mandatory_materialization": self.mandatory_materialization,
            "create_attempts": self.create_attempts,
            "resource_creates": self.resource_creates,
            "resource_rediscoveries": self.resource_rediscoveries,
            "unknown_create_results": self.unknown_create_results,
            "teardown_attempts": self.teardown_attempts,
            "resource_teardowns": self.resource_teardowns,
            "teardown_refusals": self.teardown_refusals,
            "teardown_failures": self.teardown_failures,
            "evictions": self.evictions,
            "capacity_waits": self.capacity_waits,
            "capacity_refusals": self.capacity_refusals,
            "queue_ms": round(self.queue_ms, 3),
            "tool_calls": self.tool_calls,
            "tool_ms": round(self.tool_ms, 3),
            "tool_open": self.tool_open,
            "tool_open_ms": round(self.tool_open_ms, 3),
            "model_calls": self.model_calls,
            "model_ms": round(self.model_ms, 3),
            "model_open": self.model_open,
            "model_open_ms": round(self.model_open_ms, 3),
            "tool_names": [{"name": name, "calls": calls, "ms": round(occupied_ms, 3)} for name, calls, occupied_ms in self.tool_names],
            "busy_ms": round(self.busy_ms, 3),
            "failed_attempts": self.failed_attempts,
            "outcome": self.outcome,
            "dropped_records": self.dropped_records,
            "phases": [record.to_wire() for record in self.phases],
            "unobservable": [{"phase": phase, "reason": reason} for phase, reason in self.unobservable],
        }


def _merged(intervals: Iterable[tuple[float, float]]) -> list[list[float]]:
    """*intervals* as the disjoint stretches they cover, in order."""
    merged: list[list[float]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def _covered_ms(intervals: Iterable[tuple[float, float]]) -> float:
    """How much wall clock *intervals* cover between them, overlap counted once."""
    return sum(end - start for start, end in _merged(intervals))


def _tool_label(name: str | None) -> str:
    """*name* as a log-safe label that still tells two tools apart.

    ``bounded_label`` keeps the log line parseable -- it strips the delimiters
    the line is built from, so a tool named ``evil=9/99999ms,fake`` cannot
    forge a field -- and truncates. Both of those merge names: every tool
    named in a non-Latin script sanitizes to the same run of underscores, and
    two long MCP names sharing a 48-character prefix become one. Merging is
    the one thing this breakdown must not do, so when sanitizing changed the
    name, a short digest of the original is appended. Ordinary first-party
    names pass through untouched and unadorned.
    """
    if not isinstance(name, str) or not name.strip():
        return "unnamed"
    label = _bounded_label(name, fallback="unnamed", limit=TOOL_LABEL_LIMIT)
    if label == name:
        return label
    return f"{label[: TOOL_LABEL_LIMIT - 7]}.{hashlib.sha256(name.encode('utf-8')).hexdigest()[:6]}"


class _Occupancy:
    """How much of the turn had a call of one kind open, and how many there were.

    Two rules, and the second is the one that cost a rewrite.

    *Overlap counts once.* A working turn runs calls repeatedly and sometimes
    at the same time; adding their durations up would report more time than
    the turn took. What is kept is the stretches of the turn's own wall clock
    each call covered, merged. That keeps the figure inside ``total_ms``, which
    is what makes the leftover a residual a reader can reason about.

    *A call that never ends must not take the rest of the turn with it.* It
    happens: ``asyncio.CancelledError`` is a ``BaseException``, LangChain's
    tool base catches ``Exception``, and a cancelled async tool -- a subagent
    that hit its timeout, a run that was aborted -- therefore reaches neither
    ``on_tool_end`` nor ``on_tool_error``. Counting depth alone, that one lost
    event froze the accounting and then reported every later second, tool or
    not, as time inside a tool: a measured 100 ms of work rendered as
    ``tools=1/601ms`` on a 602 ms turn. So each call is closed by its own
    identity and only *completed* calls are ever added up. One that is still
    open is reported as open (``open_calls``), never folded into the figure --
    the journal says what it saw, and an end it never saw is not a duration.

    Not a span vocabulary: one name per turn is the journal's rule and a turn
    has many calls, so these are counters beside the phases, not records
    among them.
    """

    __slots__ = ("_closed", "_open", "calls")

    def __init__(self) -> None:
        self.calls = 0
        self._open: dict[object, float] = {}
        self._closed: list[tuple[float, float]] = []

    def enter(self, key: object, now_ms: float) -> bool:
        """Open a call under *key*; ``True`` when it is the turn's first of this kind."""
        self.calls += 1
        if key not in self._open and len(self._open) < MAX_OPEN_CALLS:
            self._open[key] = now_ms
        return self.calls == 1

    def leave(self, key: object, now_ms: float) -> None:
        """Close the call opened under *key*. An end for nothing open is dropped."""
        started = self._open.pop(key, None)
        if started is None:
            return
        self._closed.append((started, now_ms))
        if len(self._closed) >= MAX_TRACKED_INTERVALS:
            self._closed = [(start, end) for start, end in _merged(self._closed)]

    @property
    def open_calls(self) -> int:
        """Calls still open: at the end of a turn, ones whose end was never seen."""
        return len(self._open)

    def closed(self) -> list[tuple[float, float]]:
        return list(self._closed)

    def open(self, now_ms: float) -> list[tuple[float, float]]:
        return [(started, now_ms) for started in self._open.values()]


class TurnPhaseJournal:
    """Append-only phase timings for one turn.

    Thread-safe by a plain lock: the acquisition path crosses
    ``asyncio.to_thread`` boundaries, so the same journal is written from the
    event loop and from worker threads within one turn.
    """

    __slots__ = (
        "_observers",
        "_acquire_reason",
        "_acquisition_reuse",
        "_acquisition_source",
        "_correlation_id",
        "_create_attempts",
        "_dropped",
        "_capacity_refusals",
        "_capacity_waits",
        "_evictions",
        "_failed_attempts",
        "_launch",
        "_lock",
        "_mandatory_materialization",
        "_model",
        "_outcome",
        "_queue_ms",
        "_records",
        "_resource_creates",
        "_resource_rediscoveries",
        "_resource_teardowns",
        "_run_id",
        "_teardown_attempts",
        "_teardown_failures",
        "_teardown_refusals",
        "_unknown_create_results",
        "_session_kind",
        "_snapshot_package_count",
        "_snapshot_present",
        "_start",
        "_tool_by_name",
        "_tool_call_labels",
        "_tools",
        "_unobservable",
    )

    def __init__(self, *, correlation_id: str, run_id: str | None = None) -> None:
        self._correlation_id = _bounded_label(correlation_id, fallback="unknown", limit=128)
        self._run_id = None if run_id is None else _bounded_label(run_id, fallback="unknown", limit=128)
        self._start = time.monotonic()
        self._lock = threading.Lock()
        self._records: list[PhaseRecord] = []
        self._dropped = 0
        self._acquisition_source: AcquisitionSource | None = None
        self._acquisition_reuse: AcquisitionSource | None = None
        self._acquire_reason: str | None = None
        self._session_kind: str | None = None
        self._snapshot_present: bool | None = None
        self._snapshot_package_count: int | None = None
        self._mandatory_materialization: bool | None = None
        self._create_attempts = 0
        self._resource_creates = 0
        self._resource_rediscoveries = 0
        self._unknown_create_results = 0
        self._teardown_attempts = 0
        self._resource_teardowns = 0
        self._teardown_refusals = 0
        self._teardown_failures = 0
        self._evictions = 0
        self._capacity_waits = 0
        self._capacity_refusals = 0
        self._queue_ms = 0.0
        self._failed_attempts = 0
        self._tools = _Occupancy()
        self._model = _Occupancy()
        self._tool_by_name: dict[str, _Occupancy] = {}
        self._tool_call_labels: dict[object, str] = {}
        self._unobservable: list[tuple[str, str]] = []
        self._outcome: str | None = None
        self._launch: LaunchTimings | None = None
        self._observers: list[Callable[[TurnPhase, float], None]] = []

    # ── Clock ────────────────────────────────────────────────────────────

    def _elapsed_ms(self) -> float:
        return (time.monotonic() - self._start) * 1000.0

    # ── Recording ────────────────────────────────────────────────────────

    def _append(self, record: PhaseRecord) -> None:
        with self._lock:
            if len(self._records) >= MAX_PHASE_RECORDS:
                self._dropped += 1
                return
            self._records.append(record)

    def observe(self, callback: Callable[[TurnPhase, float], None]) -> None:
        """Hear every phase as it begins: ``callback(phase, offset_ms)``.

        Called synchronously from whichever thread opens the phase -- a
        provider's ``to_thread`` worker included -- so a callback must be
        cheap and must not block; a callback that raises is dropped for that
        phase and the journal keeps recording. The live progress a client is
        shown rides on this (``turn_progress.py``); nothing in the journal
        depends on an observer being present.
        """
        self._observers.append(callback)

    def _notify(self, phase: TurnPhase, at_ms: float) -> None:
        for callback in self._observers:
            try:
                callback(phase, at_ms)
            except Exception:
                logger.debug("turn phase observer failed for %s", phase, exc_info=True)

    def mark(self, phase: TurnPhase, *, detail: str | None = None) -> None:
        """Record that *phase* happened now, with no duration of its own."""
        at_ms = self._elapsed_ms()
        self._append(PhaseRecord(phase=phase, started_ms=at_ms, detail=_detail(detail)))
        self._notify(phase, at_ms)

    def mark_once(self, phase: TurnPhase, *, detail: str | None = None) -> bool:
        """Record *phase* only if it has not been recorded yet.

        First-text phases are the reason this exists: the tenth token is not a
        first token, and a phase that re-marks on every chunk would report the
        last one.
        """
        with self._lock:
            if any(record.phase is phase for record in self._records):
                return False
            if len(self._records) >= MAX_PHASE_RECORDS:
                self._dropped += 1
                return False
            at_ms = self._elapsed_ms()
            self._records.append(PhaseRecord(phase=phase, started_ms=at_ms, detail=_detail(detail)))
        self._notify(phase, at_ms)
        return True

    @contextmanager
    def span(self, phase: TurnPhase, *, detail: str | None = None) -> Iterator[None]:
        """Time *phase*, recording it whether the body succeeds or raises."""
        started = self._elapsed_ms()
        self._notify(phase, started)
        try:
            yield
        finally:
            self._append(
                PhaseRecord(
                    phase=phase,
                    started_ms=started,
                    duration_ms=self._elapsed_ms() - started,
                    detail=_detail(detail),
                ),
            )

    def unobservable(self, phase: TurnPhase | str, reason: str) -> None:
        """Record that *phase* was not measured, and why.

        Reporting the limitation is the contract: a phase that cannot be seen
        here must never be inferred from a different event that can.
        """
        label = str(phase) if isinstance(phase, TurnPhase) else _bounded_label(phase, fallback="unknown_phase", limit=64)
        with self._lock:
            entry = (label, _bounded_label(reason, fallback="unspecified", limit=96))
            if entry not in self._unobservable and len(self._unobservable) < 16:
                self._unobservable.append(entry)

    # ── Attributes ───────────────────────────────────────────────────────

    def set_acquisition_source(self, source: AcquisitionSource, *, reason: str | None = None) -> None:
        """Record where this turn's sandbox came from, origin first.

        Order is not precedence. A turn acquires in stages and the later stage
        sees only that the container is active, so an active-reuse observation
        is kept beside a recorded origin (``reused=``) rather than replacing
        it; an origin always wins, whenever it arrives. Without this the cold
        turn that measured its own ``sandbox_create`` reported
        ``acquisition=accepted_active``, and so did every warm reclaim.
        """
        with self._lock:
            if source.observes_active_reuse and self._acquisition_source is not None and not self._acquisition_source.observes_active_reuse:
                self._acquisition_reuse = source
            else:
                self._acquisition_source = source
                if not source.observes_active_reuse:
                    # A newly known origin describes the whole turn: a reuse
                    # noted before it was an observation of the same container.
                    self._acquisition_reuse = None
            if reason is not None:
                self._acquire_reason = _bounded_label(reason, fallback="unspecified", limit=64)

    def set_acquire_reason(self, reason: str) -> None:
        """Record *why* the turn acquired eagerly (or did not)."""
        with self._lock:
            self._acquire_reason = _bounded_label(reason, fallback="unspecified", limit=64)

    def set_session_kind(self, kind: str) -> None:
        with self._lock:
            self._session_kind = _bounded_label(kind, fallback="unknown", limit=32)

    def set_snapshot_facts(self, *, present: bool, package_count: int | None, mandatory_materialization: bool | None = None) -> None:
        """Record the verified snapshot presence and count.

        Evidence for a later optimization, never a licence to skip work now:
        zero packages alone does not establish a deferrable state, which is why
        presence and count are recorded separately.
        """
        with self._lock:
            self._snapshot_present = bool(present)
            self._snapshot_package_count = None if package_count is None else max(0, int(package_count))
            if mandatory_materialization is not None:
                self._mandatory_materialization = bool(mandatory_materialization)

    def record_create_attempt(self) -> None:
        """A backend ``create`` call was made. Says nothing about its result."""
        with self._lock:
            self._create_attempts += 1

    def record_resource_create(self) -> None:
        """The backend confirmed it started a new resource set."""
        with self._lock:
            self._resource_creates += 1

    def record_resource_rediscovery(self) -> None:
        """The backend answered ``create`` with a resource set that already existed."""
        with self._lock:
            self._resource_rediscoveries += 1

    def record_unknown_create_result(self) -> None:
        """The backend answered ``create`` without saying which of the two it was."""
        with self._lock:
            self._unknown_create_results += 1

    def record_teardown_attempt(self) -> None:
        """A destroy was decided on. Says nothing about whether the resource is gone."""
        with self._lock:
            self._teardown_attempts += 1

    def record_resource_teardown(self) -> None:
        """The backend's destroy returned: the resource set is gone."""
        with self._lock:
            self._resource_teardowns += 1

    def record_teardown_refusal(self) -> None:
        """A destroy was refused by an ownership or teardown fence; the resource remains."""
        with self._lock:
            self._teardown_refusals += 1

    def record_teardown_failure(self) -> None:
        """A destroy ran but the set is not confirmed absent: failed, partial or unobservable."""
        with self._lock:
            self._teardown_failures += 1

    def record_eviction(self) -> None:
        with self._lock:
            self._evictions += 1

    def record_capacity_wait(self) -> None:
        """This turn had to wait for a replica slot before it could create one.

        Counted once per turn, on the first wait, for the same reason
        ``SANDBOX_CAPACITY_WAIT`` is one span: a turn that goes round the
        admission loop several times waited once, and counting each pass would
        report a busier deployment than the one that exists.
        """
        with self._lock:
            self._capacity_waits += 1

    def record_capacity_refusal(self) -> None:
        """The budget refused this turn a slot after its wait ran out."""
        with self._lock:
            self._capacity_refusals += 1

    def record_tool_start(self, call_id: object, *, name: str | None = None) -> None:
        """A tool call began: *call_id* identifies it, *name* says which tool.

        *call_id* is the framework's own handle for this call (LangChain's
        ``run_id``), used only to pair the end with the start and never
        recorded. Pairing is what makes a lost end event cost one call's
        measurement instead of the whole turn's. *name* is recorded, bounded;
        its arguments and its output are not (see the module's Disclosure).

        The phase is offered exactly once per turn, on the first call, rather
        than on every transition out of idle: a turn that had already filled
        the record cap never appends it, so ``mark_once`` would find nothing
        to dedupe against and count every later call as a dropped record --
        reporting far more loss than actually happened.
        """
        label = _tool_label(name)
        with self._lock:
            at_ms = self._elapsed_ms()
            opened = self._tools.enter(call_id, at_ms)
            if label not in self._tool_by_name and len(self._tool_by_name) >= MAX_TRACKED_TOOL_NAMES:
                label = OTHER_TOOLS_LABEL
            by_name = self._tool_by_name.setdefault(label, _Occupancy())
            by_name.enter(call_id, at_ms)
            # Bounded by the same cap as the occupancy's own open set, so a
            # framework that loses ends cannot grow this either.
            if len(self._tool_call_labels) < MAX_OPEN_CALLS:
                self._tool_call_labels[call_id] = label
        if opened:
            self.mark_once(TurnPhase.TOOL_EXECUTION)

    def record_tool_end(self, call_id: object) -> None:
        """A tool call returned or raised; either way the turn is out of it."""
        with self._lock:
            at_ms = self._elapsed_ms()
            self._tools.leave(call_id, at_ms)
            label = self._tool_call_labels.pop(call_id, None)
            if label is not None:
                self._tool_by_name[label].leave(call_id, at_ms)

    def record_model_start(self, call_id: object) -> None:
        """A model call began, and the first one is also ``MODEL_REQUEST``."""
        with self._lock:
            self._model.enter(call_id, self._elapsed_ms())
        self.mark_once(TurnPhase.MODEL_REQUEST)

    def record_model_end(self, call_id: object, *, detail: str | None = None) -> None:
        """A model call finished, and the first one is also ``MODEL_COMPLETION``."""
        with self._lock:
            self._model.leave(call_id, self._elapsed_ms())
        self.mark_once(TurnPhase.MODEL_COMPLETION, detail=detail)

    def add_queue_ms(self, milliseconds: float) -> None:
        """Add time spent waiting for a serializer or lock, not doing work."""
        with self._lock:
            self._queue_ms += max(0.0, float(milliseconds))

    def record_failed_attempt(self) -> None:
        with self._lock:
            self._failed_attempts += 1

    def set_launch(self, timings: LaunchTimings) -> None:
        """Carry what the launch measured before this journal opened.

        Re-bounded here as well as in the record's constructor: the journal
        trusts no caller to have spelled a label, whatever object it was
        handed.
        """
        bounded = LaunchTimings(
            received_at=float(timings.received_at),
            persisted_at=float(timings.persisted_at),
            steps=tuple(timings.steps),
        )
        with self._lock:
            self._launch = bounded

    def set_outcome(self, outcome: str) -> None:
        with self._lock:
            self._outcome = _bounded_label(outcome, fallback="unknown", limit=48)

    def bind_run_id(self, run_id: str) -> None:
        with self._lock:
            self._run_id = _bounded_label(run_id, fallback="unknown", limit=128)

    # ── Reading ──────────────────────────────────────────────────────────

    @property
    def correlation_id(self) -> str:
        return self._correlation_id

    def snapshot(self) -> TurnPhaseSnapshot:
        with self._lock:
            # One read of the clock for the whole snapshot: the total and the
            # occupancy of a still-open call must be consistent with each
            # other, or a tool running right now can be reported as having
            # cost more than the turn.
            now_ms = self._elapsed_ms()
            launch = self._launch
            launch_ms = None if launch is None else max(0.0, (self._start - launch.received_at) * 1000.0)
            handoff_ms = None if launch is None else max(0.0, (self._start - launch.persisted_at) * 1000.0)
            return TurnPhaseSnapshot(
                launch_ms=launch_ms,
                launch_handoff_ms=handoff_ms,
                launch_steps=() if launch is None else launch.steps,
                correlation_id=self._correlation_id,
                run_id=self._run_id,
                total_ms=now_ms,
                phases=tuple(self._records),
                acquisition_source=self._acquisition_source,
                acquisition_reuse=self._acquisition_reuse,
                acquire_reason=self._acquire_reason,
                session_kind=self._session_kind,
                snapshot_present=self._snapshot_present,
                snapshot_package_count=self._snapshot_package_count,
                mandatory_materialization=self._mandatory_materialization,
                create_attempts=self._create_attempts,
                resource_creates=self._resource_creates,
                resource_rediscoveries=self._resource_rediscoveries,
                unknown_create_results=self._unknown_create_results,
                teardown_attempts=self._teardown_attempts,
                resource_teardowns=self._resource_teardowns,
                teardown_refusals=self._teardown_refusals,
                teardown_failures=self._teardown_failures,
                evictions=self._evictions,
                capacity_waits=self._capacity_waits,
                capacity_refusals=self._capacity_refusals,
                queue_ms=self._queue_ms,
                failed_attempts=self._failed_attempts,
                tool_calls=self._tools.calls,
                tool_ms=_covered_ms(self._tools.closed()),
                tool_open=self._tools.open_calls,
                tool_open_ms=_covered_ms(self._tools.open(now_ms)),
                model_calls=self._model.calls,
                model_ms=_covered_ms(self._model.closed()),
                model_open=self._model.open_calls,
                model_open_ms=_covered_ms(self._model.open(now_ms)),
                tool_names=tuple(
                    sorted(
                        ((label, occupancy.calls, _covered_ms(occupancy.closed())) for label, occupancy in self._tool_by_name.items()),
                        key=lambda entry: (-entry[2], -entry[1], entry[0]),
                    )
                ),
                busy_ms=_covered_ms(self._tools.closed() + self._model.closed()),
                unobservable=tuple(self._unobservable),
                outcome=self._outcome,
                dropped_records=self._dropped,
            )

    def emit(self, target: logging.Logger | None = None) -> TurnPhaseSnapshot:
        """Log the journal once, as one record, and return it.

        The message carries the reading (``TurnPhaseSnapshot.to_log_line``)
        because that is all a deployment's formatter prints; the structured
        payload rides along for anything that reads fields. A new field that an
        operator needs belongs in both: the record alone reaches nobody running
        the default text format.
        """
        snapshot = self.snapshot()
        (target or logger).info("%s", snapshot.to_log_line(), extra={"turn_phases": snapshot.to_wire()})
        return snapshot


def _detail(detail: str | None) -> str | None:
    return None if detail is None else _bounded_label(detail, fallback="unspecified", limit=64)


# ── Binding: context variable plus a run-keyed registry ──────────────────
#
# The context variable covers everything inside one task (and the worker
# threads it hands work to, since ``asyncio.to_thread`` copies the context).
# The registry covers what is not in that task at all: the SSE consumer runs in
# the Gateway request task and can only find the run's journal by its id.

_current_journal: ContextVar[TurnPhaseJournal | None] = ContextVar("deerflow_turn_phase_journal", default=None)
_registry: OrderedDict[str, TurnPhaseJournal] = OrderedDict()
_registry_lock = threading.Lock()


def current_turn_phases() -> TurnPhaseJournal | None:
    """The journal bound to this task, or ``None`` when nothing is measuring."""
    return _current_journal.get()


def turn_phases_for_run(run_id: str | None) -> TurnPhaseJournal | None:
    """The journal for *run_id*, for observers outside the run's own task."""
    if not run_id:
        return None
    with _registry_lock:
        return _registry.get(run_id)


def register_turn_phases(journal: TurnPhaseJournal, run_id: str) -> None:
    """Make *journal* findable by *run_id*, evicting the oldest past the cap."""
    with _registry_lock:
        _registry[run_id] = journal
        _registry.move_to_end(run_id)
        while len(_registry) > MAX_TRACKED_RUNS:
            _registry.popitem(last=False)


def unregister_turn_phases(run_id: str | None) -> None:
    if not run_id:
        return
    with _registry_lock:
        _registry.pop(run_id, None)


@contextmanager
def turn_phases(*, correlation_id: str, run_id: str | None = None) -> Iterator[TurnPhaseJournal]:
    """Open a journal for one turn, bound to this task and to *run_id*."""
    journal = TurnPhaseJournal(correlation_id=correlation_id, run_id=run_id)
    if run_id:
        register_turn_phases(journal, run_id)
    token = _current_journal.set(journal)
    try:
        yield journal
    finally:
        _current_journal.reset(token)
        unregister_turn_phases(run_id)


def reset_turn_phase_registry() -> None:
    """Drop every tracked journal. For tests and process shutdown only."""
    with _registry_lock:
        _registry.clear()


# ── Convenience recorders ────────────────────────────────────────────────
#
# Call sites deep in the provider should not have to care whether anything is
# measuring, so every recorder is a no-op when no journal is bound.


def record_acquisition_source(source: AcquisitionSource, *, reason: str | None = None) -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.set_acquisition_source(source, reason=reason)


def record_create_attempt() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_create_attempt()


def record_resource_create() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_resource_create()


def record_resource_rediscovery() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_resource_rediscovery()


def record_unknown_create_result() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_unknown_create_result()


def record_create_result(provenance: str) -> None:
    """Classify one ``create`` result by the backend's own word for it.

    ``created`` counts a new resource set and ``rediscovered`` an existing one;
    anything else is an unknown result. Unknown is a category of its own so a
    backend that stays silent can never inflate the create count.
    """
    if provenance == "created":
        record_resource_create()
    elif provenance == "rediscovered":
        record_resource_rediscovery()
    else:
        record_unknown_create_result()


def record_teardown_attempt() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_teardown_attempt()


def record_resource_teardown() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_resource_teardown()


def record_teardown_refusal() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_teardown_refusal()


def record_teardown_failure() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_teardown_failure()


def record_eviction() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_eviction()


def record_capacity_wait() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_capacity_wait()


def record_capacity_refusal() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_capacity_refusal()


def record_failed_attempt() -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.record_failed_attempt()


def mark_phase(phase: TurnPhase, *, detail: str | None = None) -> None:
    """Record *phase* on the bound journal, or do nothing if none is bound."""
    journal = _current_journal.get()
    if journal is not None:
        journal.mark(phase, detail=detail)


@contextmanager
def phase_span(phase: TurnPhase, *, detail: str | None = None) -> Iterator[None]:
    """Time *phase* on the bound journal, or do nothing if none is bound."""
    journal = _current_journal.get()
    if journal is None:
        yield
        return
    with journal.span(phase, detail=detail):
        yield


class TurnPhaseCallbackHandler:
    """Model-side phases, taken from the callback seam the run already has.

    Attached beside ``RunJournal`` on the graph root so the model request, the
    provider's first streamed text and the completion are timed on the same
    monotonic clock as the sandbox phases. It records timing only: no prompt,
    no completion, no token text, no provider handle.

    First text means first *text*. Reasoning tokens, empty chunks and tool-call
    fragments carry no assistant answer and do not mark it, which is what keeps
    the figure comparable with what a reader actually sees.

    Tool calls are timed here for the same reason and with the same restraint.
    ``ignore_agent`` is what LangChain gates every tool callback on, so it is
    ``False`` -- the agent-action hooks it also lets through are no-ops. The
    hooks are handed the tool's name, its arguments and its output; the name
    is recorded so the time can be attributed, the other two never are.
    """

    raise_error = False
    run_inline = True
    ignore_llm = False
    ignore_chain = True
    # Not "we want agent callbacks": this is the flag the tool hooks below
    # are gated on. Left ``True``, they are never called at all.
    ignore_agent = False
    ignore_retriever = True
    ignore_chat_model = False
    ignore_custom_event = True
    ignore_retry = True

    def __init__(self, journal: TurnPhaseJournal) -> None:
        self._journal = journal

    def __getattr__(self, name: str) -> Any:
        # LangChain probes a wide handler surface; anything this class does not
        # implement is an ignored no-op rather than an AttributeError.
        if name.startswith("on_"):
            return _ignore
        raise AttributeError(name)

    def on_chat_model_start(self, *args: Any, **kwargs: Any) -> None:
        self._journal.record_model_start(kwargs.get("run_id"))

    def on_llm_start(self, *args: Any, **kwargs: Any) -> None:
        self._journal.record_model_start(kwargs.get("run_id"))

    def on_llm_new_token(self, token: Any = "", *args: Any, chunk: Any = None, **kwargs: Any) -> None:
        # When the chunk carries structured content, judge it by the same
        # block rule as the outgoing frames, so a thinking-only chunk whose
        # ``token`` string happens to be non-empty does not count as text.
        content = getattr(getattr(chunk, "message", None), "content", None)
        if content is None:
            content = getattr(chunk, "content", None)
        if isinstance(content, list | tuple):
            if any(_block_carries_answer_text(block) for block in content):
                self._journal.mark_once(TurnPhase.FIRST_PROVIDER_TEXT)
            return
        if isinstance(token, str) and token.strip():
            self._journal.mark_once(TurnPhase.FIRST_PROVIDER_TEXT)

    def on_llm_end(self, *args: Any, **kwargs: Any) -> None:
        self._journal.record_model_end(kwargs.get("run_id"))

    def on_llm_error(self, *args: Any, **kwargs: Any) -> None:
        self._journal.record_model_end(kwargs.get("run_id"), detail="error")
        self._journal.record_failed_attempt()

    def on_tool_start(self, serialized: Any = None, *args: Any, **kwargs: Any) -> None:
        # ``serialized`` is the tool's own descriptor; only its name is read,
        # and ``input_str``/``inputs`` -- the arguments -- are left untouched.
        name = serialized.get("name") if isinstance(serialized, dict) else None
        self._journal.record_tool_start(kwargs.get("run_id"), name=name if isinstance(name, str) else None)

    def on_tool_end(self, *args: Any, **kwargs: Any) -> None:
        self._journal.record_tool_end(kwargs.get("run_id"))

    def on_tool_error(self, *args: Any, **kwargs: Any) -> None:
        # A tool that raised still occupied the turn; the exception says why
        # and is the agent's to handle, so nothing of it is recorded here.
        self._journal.record_tool_end(kwargs.get("run_id"))


def _ignore(*_args: Any, **_kwargs: Any) -> None:
    return None


def record_acquire_reason(reason: str) -> None:
    journal = _current_journal.get()
    if journal is not None:
        journal.set_acquire_reason(reason)


# ── What counts as the first assistant text ──────────────────────────────

# The stream carries far more than the answer. Lifecycle frames, run values,
# heartbeats, progress and gap notices all arrive before or beside it, and none
# of them is what a reader sees as the assistant starting to speak.
_ASSISTANT_MESSAGE_TYPES = frozenset({"ai", "AIMessage", "AIMessageChunk"})
# Hidden reasoning is text, but it is not the answer: it is not rendered, so
# counting it would report a first-text time no reader ever experienced.
_NON_ANSWER_BLOCK_TYPES = frozenset({"thinking", "reasoning", "redacted_thinking", "reasoning_content"})


def _block_carries_answer_text(block: object) -> bool:
    if isinstance(block, str):
        return bool(block.strip())
    if not isinstance(block, dict):
        return False
    if str(block.get("type", "text")) in _NON_ANSWER_BLOCK_TYPES:
        return False
    return bool(str(block.get("text", "")).strip())


def sse_frame_carries_assistant_text(event: object, data: object) -> bool:
    """Whether one outgoing SSE frame is the assistant's own visible text.

    First response bytes are not first text: a lifecycle frame, a progress
    update, a heartbeat or a tool-call fragment all put bytes on the wire while
    the reader still sees nothing. Only a ``messages`` frame carrying an AI
    message whose content has renderable text answers yes.
    """
    if event != "messages":
        return False
    if isinstance(data, dict):
        message: object = data
    elif isinstance(data, list | tuple) and data:
        message = data[0]
    else:
        return False
    if not isinstance(message, dict):
        return False
    if str(message.get("type", "")) not in _ASSISTANT_MESSAGE_TYPES:
        return False
    content = message.get("content")
    if isinstance(content, str):
        return bool(content.strip())
    if isinstance(content, list | tuple):
        return any(_block_carries_answer_text(block) for block in content)
    return False


def mark_first_stream_text(run_id: str | None, event: object, data: object) -> None:
    """Mark the run's first outgoing assistant text, if this frame is it.

    Called from the SSE consumer, which runs in the Gateway request task rather
    than the run's own, so the journal is found by run id. A no-op for every
    other frame and for any run nothing is measuring.
    """
    journal = turn_phases_for_run(run_id)
    if journal is None:
        return
    if sse_frame_carries_assistant_text(event, data):
        journal.mark_once(TurnPhase.FIRST_STREAM_TEXT)


def record_queue_ms(milliseconds: float) -> None:
    """Add serializer or lock wait time to the bound journal, if any."""
    journal = _current_journal.get()
    if journal is not None:
        journal.add_queue_ms(milliseconds)

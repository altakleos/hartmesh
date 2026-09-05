"""The bounded sandbox diagnostic stream, split from the closed lifecycle set."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from deerflow.runtime.events.catalog import SANDBOX_DIAGNOSTIC_EVENT, SANDBOX_LIFECYCLE_EVENT
from deerflow.sandbox.accepted_material import AcceptedSandboxLifecycleKind
from deerflow.sandbox.diagnostics import (
    SANDBOX_DIAGNOSTIC_STREAM_CAPACITY,
    SandboxDiagnosticObservationV1,
    SandboxDiagnosticStream,
    discard_sandbox_diagnostics,
    record_sandbox_diagnostic,
    sandbox_diagnostics,
)
from deerflow.sandbox.session import SandboxSessionKind

_NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def _ordinary(**overrides):
    fields = dict(
        kind="egress.blocked",
        session_kind=SandboxSessionKind.ORDINARY,
        run_id="run-1",
        thread_id="thread-1",
        sandbox_ref="box:user-1:thread-1",
        observed_at=_NOW,
        facts={"request_ref": "req-1", "host": "example.com", "port": 443},
    )
    fields.update(overrides)
    return SandboxDiagnosticObservationV1.build(**fields)


def test_closed_lifecycle_set_is_unchanged_and_separate_from_diagnostics() -> None:
    assert [kind.value for kind in AcceptedSandboxLifecycleKind] == ["acquired", "authority_lost", "released", "cleanup_pending", "orphaned"]
    assert SANDBOX_LIFECYCLE_EVENT.event_type == "sandbox.lifecycle.v1"
    assert SANDBOX_DIAGNOSTIC_EVENT.event_type == "sandbox.diagnostic.v1"
    assert SANDBOX_DIAGNOSTIC_EVENT.category == "trace"
    # An authority-relevant kind is never a diagnostic kind: the stream is open
    # but namespaced, and the closed set's names are single words.
    for kind in AcceptedSandboxLifecycleKind:
        with pytest.raises(ValueError, match="diagnostic kind"):
            _ordinary(kind=kind.value)


def test_ordinary_observation_is_thread_scoped_and_digest_bound() -> None:
    observation = _ordinary()
    persisted = observation.to_persisted()
    assert persisted["session_kind"] == "ordinary"
    assert persisted["execution_evidence_digest"] is None
    assert persisted["attempt_ref"] is None
    assert persisted["facts"] == {"host": "example.com", "port": 443, "request_ref": "req-1"}
    assert SandboxDiagnosticObservationV1.from_persisted(persisted) == observation
    tampered = {**persisted, "facts": {**persisted["facts"], "port": 80}}
    with pytest.raises(ValueError, match="digest"):
        SandboxDiagnosticObservationV1.from_persisted(tampered)
    with pytest.raises(ValueError, match="unknown or missing"):
        SandboxDiagnosticObservationV1.from_persisted({**persisted, "extra": 1})


def test_accepted_observation_is_run_bound_to_its_evidence() -> None:
    observation = _ordinary(
        session_kind=SandboxSessionKind.ACCEPTED,
        sandbox_ref="accepted-execution-" + "a" * 64,
        attempt_ref="attempt-1",
        batch_child_attempt_ref="child-1",
        execution_evidence_digest="b" * 64,
    )
    assert observation.to_persisted()["execution_evidence_digest"] == "b" * 64
    with pytest.raises(ValueError, match="evidence"):
        _ordinary(session_kind=SandboxSessionKind.ACCEPTED, attempt_ref="attempt-1")
    with pytest.raises(ValueError, match="attempt"):
        _ordinary(session_kind=SandboxSessionKind.ACCEPTED, execution_evidence_digest="b" * 64)
    with pytest.raises(ValueError, match="ordinary"):
        _ordinary(execution_evidence_digest="b" * 64)


@pytest.mark.parametrize(
    "kind",
    ["Egress.blocked", "egress", "egress..blocked", "egress.blocked.", "a" * 40 + ".b", "egress.blocked.x.y.z"],
)
def test_diagnostic_kind_must_be_namespaced(kind: str) -> None:
    with pytest.raises(ValueError, match="diagnostic kind"):
        _ordinary(kind=kind)


def test_facts_are_bounded_scalars() -> None:
    with pytest.raises(ValueError, match="facts"):
        _ordinary(facts={"nested": {"a": 1}})
    with pytest.raises(ValueError, match="facts"):
        _ordinary(facts={"Bad Key": 1})
    with pytest.raises(ValueError, match="facts"):
        _ordinary(facts={"long": "x" * 257})
    with pytest.raises(ValueError, match="facts"):
        _ordinary(facts={f"k{i}": i for i in range(17)})
    with pytest.raises(ValueError, match="facts"):
        _ordinary(facts={"big": 2**63})
    assert _ordinary(facts={}).facts == {}
    observation = _ordinary(facts={"flag": True, "count": 3, "name": "x"})
    assert json.dumps(observation.to_persisted()["facts"]) == '{"count": 3, "flag": true, "name": "x"}'


def test_stream_drops_oldest_and_counts_the_drop() -> None:
    stream = SandboxDiagnosticStream(capacity=3)
    sequences = [stream.record(_ordinary(facts={"n": n})) for n in range(5)]
    assert sequences == [0, 1, 2, 3, 4]
    assert stream.dropped == 2
    assert [seq for seq, _ in stream.since(0)] == [2, 3, 4]
    assert [obs.facts["n"] for _, obs in stream.since(3)] == [3, 4]
    assert stream.since(5) == ()
    assert len(stream) == 3
    with pytest.raises(TypeError):
        stream.record(object())
    assert SANDBOX_DIAGNOSTIC_STREAM_CAPACITY == 64
    assert len(SandboxDiagnosticStream()) == 0


def test_run_registry_hands_out_one_stream_per_run_and_discards_it() -> None:
    discard_sandbox_diagnostics("run-registry")
    stream = sandbox_diagnostics("run-registry")
    assert sandbox_diagnostics("run-registry") is stream
    stream.record(_ordinary(run_id="run-registry"))
    assert len(sandbox_diagnostics("run-registry")) == 1
    discard_sandbox_diagnostics("run-registry")
    assert len(sandbox_diagnostics("run-registry")) == 0
    discard_sandbox_diagnostics("run-registry")


def test_record_helper_reads_the_ordinary_context_and_never_raises() -> None:
    discard_sandbox_diagnostics("run-helper")
    context = {"run_id": "run-helper", "thread_id": "thread-1", "sandbox_id": "box-1"}
    observation = record_sandbox_diagnostic(context, "scope.opened", facts={"scope_ref": "owner-1"})
    assert observation is not None
    assert observation.session_kind is SandboxSessionKind.ORDINARY
    assert observation.sandbox_ref == "box-1"
    assert observation.thread_id == "thread-1"
    assert [obs.kind for _, obs in sandbox_diagnostics("run-helper").since(0)] == ["scope.opened"]
    # No run, no sandbox, a malformed fact: nothing recorded, nothing raised.
    assert record_sandbox_diagnostic({"thread_id": "thread-1", "sandbox_id": "box-1"}, "scope.opened", facts={}) is None
    assert record_sandbox_diagnostic({"run_id": "run-helper", "thread_id": "thread-1"}, "scope.opened", facts={}) is None
    assert record_sandbox_diagnostic(context, "scope.opened", facts={"bad": object()}) is None
    assert record_sandbox_diagnostic(None, "scope.opened", facts={}) is None
    assert record_sandbox_diagnostic(context, "scope.opened", facts={"scope_ref": "owner-1"}, once=True) is not None
    assert record_sandbox_diagnostic(context, "scope.opened", facts={"scope_ref": "owner-1"}, once=True) is None
    assert len(sandbox_diagnostics("run-helper")) == 2
    discard_sandbox_diagnostics("run-helper")


def test_record_helper_binds_an_accepted_session_to_its_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from deerflow.sandbox import diagnostics as diagnostics_module

    discard_sandbox_diagnostics("run-accepted")
    declaration = SimpleNamespace(kind=SandboxSessionKind.ACCEPTED, public_ref="accepted-execution-" + "c" * 64)
    bridge = SimpleNamespace(execution_evidence_digest="d" * 64, attempt_ref="attempt-9", batch_child_attempt_ref=None)
    monkeypatch.setattr(diagnostics_module, "current_sandbox_session", lambda: declaration)
    monkeypatch.setattr(diagnostics_module, "current_accepted_sandbox_bridge", lambda: bridge)
    observation = record_sandbox_diagnostic({"run_id": "run-accepted", "thread_id": "thread-1", "sandbox_id": "raw-id"}, "egress.denied", facts={"reason": "accepted_session"})
    assert observation is not None
    assert observation.session_kind is SandboxSessionKind.ACCEPTED
    assert observation.sandbox_ref == declaration.public_ref
    assert observation.execution_evidence_digest == "d" * 64
    assert observation.attempt_ref == "attempt-9"
    assert "raw-id" not in json.dumps(observation.to_persisted())
    discard_sandbox_diagnostics("run-accepted")

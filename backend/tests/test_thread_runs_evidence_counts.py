"""The evidence endpoint counts sandbox refusals beside the other diagnostics."""

from __future__ import annotations

from app.gateway.routers.thread_runs import _evidence_event_counts
from deerflow.runtime.events.catalog import SANDBOX_DIAGNOSTIC_EVENT, SANDBOX_LIFECYCLE_EVENT


def test_refusals_are_counted_from_the_diagnostic_kind() -> None:
    events = [
        {"event_type": SANDBOX_LIFECYCLE_EVENT.event_type, "content": {"kind": "acquired"}},
        {"event_type": SANDBOX_DIAGNOSTIC_EVENT.event_type, "content": {"kind": "egress.denied"}},
        {"event_type": SANDBOX_DIAGNOSTIC_EVENT.event_type, "content": {"kind": "session.refused"}},
        {"event_type": SANDBOX_DIAGNOSTIC_EVENT.event_type, "content": "not a mapping"},
        {"event_type": "run.delivery", "content": {"kind": "session.refused"}},
    ]

    counts = _evidence_event_counts(events, events_pruned=False, policy_pruned=True)

    assert counts["sandbox"] == 1
    assert counts["sandbox_diagnostics"] == 3
    assert counts["sandbox_refusals"] == 1
    assert counts["pruned"] is False
    assert counts["policy_pruned"] is True

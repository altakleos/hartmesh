"""Execution wording distinguishes a command receipt from settled execution."""

from types import SimpleNamespace

import pytest


def act(kind="activate", state="pending", reason=None, **values):
    return SimpleNamespace(kind=kind, state=state, reason=reason, attempts=values.pop("attempts", 1), cancel_requested_at=values.pop("cancel_requested_at", None), **values)


@pytest.mark.parametrize(
    "reason,key",
    [
        ("run-limit", "start.refused.run-limit"),
        ("restarting", "start.refused.restarting"),
        ("storage-busy", "start.refused.storage-busy"),
        ("brief-changed", "start.brief_changed"),
        ("work-closed", "start.closed"),
        ("attempt-running", "start.already"),
        ("attempt-unsettled", "start.not_settled"),
    ],
)
def test_start_refusals_keep_their_own_reason(reason, key):
    from app.telegram_bots.execution_notices import execution_notice

    shown = execution_notice(act(state="refused", reason=reason))
    assert shown.key == key
    assert shown.restart is (reason in {"run-limit", "restarting", "storage-busy"})


def test_busy_wait_cancel_and_retry_controls_are_separate_from_fresh_start():
    from app.telegram_bots.execution_notices import execution_notice

    waiting = execution_notice(act(state="waiting", reason="busy"))
    assert waiting.key == "start.busy_wait" and waiting.cancel and not waiting.restart
    unknown = execution_notice(act(state="uncertain"))
    assert unknown.key == "act.unconfirmed" and unknown.retry and not unknown.restart
    cancelled = execution_notice(act(state="uncertain", reason="cancel-unconfirmed", cancel_requested_at="saved fence"))
    assert cancelled.key == "start.cancel_unconfirmed" and not cancelled.retry and not cancelled.restart
    assert execution_notice(act(state="done")) is None


@pytest.mark.parametrize("dispatch", ["ready", "requested", "awaiting_command"])
def test_successful_stop_command_is_still_stopping_until_original_settlement(dispatch):
    from app.telegram_bots.execution_notices import execution_notice

    shown = execution_notice(act(kind="stop", state="done"), original={"status": "stopping", "settled_at": None}, dispatch={"state": dispatch})
    assert shown.key == "status.stopping" and not shown.retry


def test_cancelled_original_attempt_needs_a_settlement_timestamp_for_stopped_copy():
    from app.telegram_bots.execution_notices import execution_notice

    stopped = act(kind="stop", state="done")
    assert execution_notice(stopped, original={"status": "cancelled", "settled_at": None}, dispatch={"state": "requested"}).key == "status.stopping"
    assert execution_notice(stopped, original={"status": "cancelled", "settled_at": "2026-11-13T12:00:00+00:00"}, dispatch={"state": "settled"}).key == "status.stopped"
    uncertain = execution_notice(stopped, original={"status": "stopping", "settled_at": None}, dispatch={"state": "uncertain"})
    assert uncertain.key == "act.unconfirmed" and uncertain.retry


@pytest.mark.parametrize("status", ["starting", "running", "stopping", "uncertain"])
def test_stale_settlement_time_does_not_turn_active_stop_into_an_ended_notice(status):
    from app.telegram_bots.execution_notices import execution_notice

    shown = execution_notice(act(kind="stop", state="done"), original={"status": status, "settled_at": "2026-11-13T12:00:00+00:00"}, dispatch={"state": "requested"})
    assert shown.key == "status.stopping"


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_execution_notice_catalog_renders_all_current_outcomes(locale):
    from app.telegram_bots.execution_notices import execution_notice
    from app.telegram_bots.render import render_plain

    values = dict(name="Analyst", objective="Review the guide", product="HartMesh", person="A colleague", supervisor="a manager", wait="two minutes")
    states = [
        act(state="refused", reason=reason)
        for reason in [
            "run-limit",
            "restarting",
            "storage-busy",
            "brief-changed",
            "work-closed",
            "attempt-running",
            "attempt-unsettled",
            "awaiting-review",
            "work-off",
            "lifecycle-pending",
            "source-unavailable",
            "execution-unavailable",
            "denied",
            "busy",
        ]
    ]
    states += [act(state="waiting", reason="busy"), act(state="waiting", reason="work-limit"), act(state="refused", reason="cancelled", cancel_requested_at="fence"), act(state="uncertain", cancel_requested_at="fence")]
    states += [act(kind="stop", state="refused", reason="denied")]
    for record in states:
        shown = execution_notice(record)
        assert render_plain(shown.key, locale, **values)


@pytest.mark.parametrize("status", ["starting", "running", "stopping", "uncertain"])
@pytest.mark.parametrize("state,reason", [("refused", "restarting"), ("refused", "busy"), ("waiting", "work-limit")])
def test_original_runless_promise_never_offers_a_fresh_start_after_refusal(state, reason, status):
    from app.telegram_bots.execution_notices import execution_notice

    shown = execution_notice(act(state=state, reason=reason), original={"status": status, "run_id": None, "settled_at": None})
    assert shown.key == "act.unconfirmed" and shown.retry and not shown.restart


def test_stop_dispatch_ended_without_settlement_never_claims_stopping_or_failure():
    from app.telegram_bots.execution_notices import execution_notice

    shown = execution_notice(act(kind="stop", state="done"), original={"status": "stopping", "settled_at": None}, dispatch={"state": "ended"})
    assert shown.key == "stop.unconfirmed" and not shown.retry and not shown.restart
    settled = execution_notice(act(kind="stop", state="done"), original={"status": "cancelled", "settled_at": "2026-11-13T12:00:00+00:00"}, dispatch={"state": "ended"})
    assert settled.key == "status.stopped"

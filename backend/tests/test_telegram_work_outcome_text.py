"""Product wording distinguishes conclusive refusals from unknown actions."""

from types import SimpleNamespace

import pytest

from app.telegram_bots import errors
from app.telegram_bots.authority import CeilingRefused
from deerflow.agent_instances.contract import AgentConflict, AgentDenied


class UnprintableDenied(AgentDenied):
    def __str__(self):
        raise AssertionError("exception wording must not be inspected")


@pytest.mark.parametrize("kind,key", [("respond", "denied.respond"), ("decide", "denied.decide"), ("accept", "denied.accept"), ("changes_requested", "denied.changes"), ("route", "denied.route"), ("other", "denied.other")])
def test_denial_uses_kind_and_class_without_exception_wording(kind, key):
    assert errors.text_key_for(kind, UnprintableDenied("private words")) == key
    assert errors.text_key_for(kind, CeilingRefused()) == key


@pytest.mark.parametrize("kind", ["decide", "accept"])
def test_unresolved_attempt_is_a_conclusive_refusal_without_automatic_retry(kind):
    assert errors.text_key_for(kind, AgentConflict("arbitrary wording", reason="attempt-unresolved")) == "decide.wait"
    assert errors.text_key_for(kind, AgentConflict("attempt-unresolved in text only")) == "stale.changed_resend"


def test_unknown_exception_is_never_called_a_failed_action():
    assert errors.text_key_for("respond", TimeoutError("denied")) == "act.unconfirmed"
    assert errors.text_key_for("route", AgentConflict("private")) == "tap.failed"


def act(kind="respond", state="done", reason=None, **body):
    return SimpleNamespace(kind=kind, state=state, reason=reason, body=body)


@pytest.mark.parametrize("permissions,expected", [(3, "ask.cant_saved"), (7, "ask.cant_saved_manager")])
def test_cannot_reason_uses_manager_wording_but_wrong_recipient_does_not(permissions, expected):
    from app.telegram_bots.work_outcomes import outcome_text

    cannot = outcome_text(act(disposition="cannot_provide"), purpose="information", permissions=permissions, attempt_status=None)
    assert cannot.key == expected and cannot.open_assignment == (permissions == 7)
    wrong = outcome_text(act(disposition="wrong_recipient"), purpose="information", permissions=permissions, attempt_status=None)
    assert wrong.key == "ask.cant_saved" and not wrong.open_assignment
    assert wrong.route_question == (permissions == 7)


@pytest.mark.parametrize("running", [True, False])
def test_supplied_answer_distinguishes_running_from_saved_for_later(running):
    from app.telegram_bots.work_outcomes import outcome_text

    shown = outcome_text(act(disposition="supplied", text="Exact <answer>"), purpose="information", permissions=3, attempt_status="running" if running else None)
    assert shown.key == ("ask.saved_running" if running else "ask.saved_next")
    assert shown.open_assignment != running and shown.echo == "Exact <answer>"


@pytest.mark.parametrize("purpose", ["decision", "review"])
def test_plain_reply_to_management_request_is_only_a_comment(purpose):
    from app.telegram_bots.work_outcomes import outcome_text

    shown = outcome_text(act(disposition="supplied", text="A comment"), purpose=purpose, permissions=7, attempt_status=None)
    assert shown.key == "decide.comment_saved" and not shown.open_assignment and shown.echo is None


def test_unknown_pending_keeps_waiting_and_uncertain_offers_exact_action_retry():
    from app.telegram_bots.work_outcomes import outcome_text

    options = dict(purpose="information", permissions=7, attempt_status=None)
    assert outcome_text(act(state="pending"), **options) is None
    uncertain = outcome_text(act(state="uncertain"), **options)
    assert uncertain.key == "act.unconfirmed" and uncertain.retry_action and not uncertain.open_assignment
    waiting = outcome_text(act("accept", state="pending", reason="attempt-unresolved"), **options)
    assert waiting.key == "decide.wait" and not waiting.retry_action


def test_route_conflict_requires_current_basis_to_select_refresh():
    from app.telegram_bots.work_outcomes import outcome_text

    options = dict(purpose="information", permissions=7, attempt_status=None)
    assert outcome_text(act("route", state="conflict"), **options).key == "tap.failed"
    shown = outcome_text(act("route", state="conflict"), request_changed=True, **options)
    assert shown.key == "stale.changed_resend" and shown.refresh_request


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_outcome_previews_escape_full_typed_content_and_keep_navigation_web_only(locale):
    from app.telegram_bots.work_outcomes import present_outcome

    note = "<unsafe & complete> " * 100 + "😀" * 2000
    result = present_outcome(act("changes_requested", note=note), purpose="review", permissions=7, attempt_status=None, locale=locale, values={"name": "Taylor"})
    assert len(result.parts) > 1
    assert "".join(part["visible_text"] for part in result.parts).count("<unsafe & complete>") == 100
    assert all("<unsafe" not in part["html"] for part in result.parts)
    assert result.text.open_assignment and not result.text.retry_action


@pytest.mark.parametrize("kind,key", [("decide", "decide.saved_next"), ("accept", "review.done_line"), ("changes_requested", "review.changes_saved"), ("route", "request.routed")])
def test_success_kind_selects_fixed_outcome(kind, key):
    from app.telegram_bots.work_outcomes import present_outcome

    shown = present_outcome(act(kind, note="Preserved request"), purpose="review", permissions=7, attempt_status=None, locale="en", values={"name": "Taylor", "time": "10:20", "person": "Casey"})
    assert shown.text.key == key
    assert shown.parts


@pytest.mark.parametrize("status", ["starting", "stopping", "uncertain"])
def test_unsettled_attempt_does_not_promise_that_the_answer_will_be_read(status):
    from app.telegram_bots.work_outcomes import present_outcome

    shown = present_outcome(act(disposition="supplied", text="Answer"), purpose="information", permissions=3, attempt_status=status, locale="en", values={"name": "Taylor"})
    assert shown.text.key == "ask.saved_waiting" and not shown.text.open_assignment
    assert "will read" not in "".join(part["visible_text"] for part in shown.parts)


@pytest.mark.parametrize("kind,key", [("respond", "ask.saved"), ("decide", "decide.saved"), ("changes_requested", "review.changes_recorded")])
def test_later_web_activation_uses_only_confirmed_saved_effect_wording(kind, key):
    from app.telegram_bots.work_outcomes import present_outcome

    shown = present_outcome(act(kind, disposition="supplied", text="Answer", note="Note"), purpose="information", permissions=7, attempt_status="running", continued=True, locale="en", values={"name": "Taylor"})
    assert shown.text.key == key
    assert "has not" not in "".join(part["visible_text"] for part in shown.parts)

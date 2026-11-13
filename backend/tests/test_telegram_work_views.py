"""Complete Work previews and capability-derived descriptions; no execution."""

import pytest


def record(**updates):
    return {
        "id": "a" * 32,
        "instance_id": "b" * 32,
        "revision": 4,
        "assignment_revision": 2,
        "objective": "Review the guide",
        "success_criteria": "Report verified gaps",
        "status": "open",
        "work_enabled": True,
        "execution_available": True,
        "needs_mandate_reconciliation": False,
        "attempt": None,
        "progress": "",
        "due_at": None,
        **updates,
    }


HELD = frozenset({"agents:read", "agents:write", "runs:create", "runs:cancel", "threads:write"})


def kinds(keyboard):
    return [button["kind"] for row in keyboard for button in row if "kind" in button]


@pytest.mark.parametrize(
    "attempt,status,expected",
    [
        (None, "open", ["work_start"]),
        ({"id": "c" * 32, "status": "failed", "requester_id": "owner"}, "open", ["work_start"]),
        ({"id": "c" * 32, "status": "cancelled", "requester_id": "owner"}, "blocked", ["work_start"]),
        ({"id": "c" * 32, "status": "starting", "requester_id": "owner"}, "open", ["stop_attempt"]),
        ({"id": "c" * 32, "status": "running", "requester_id": "peer"}, "open", []),
        ({"id": "c" * 32, "status": "stopping", "requester_id": "owner"}, "open", []),
        ({"id": "c" * 32, "status": "uncertain", "requester_id": "owner"}, "open", []),
        (None, "completed", []),
        (None, "cancelled", []),
        (None, "submitted", []),
    ],
)
def test_execution_description_uses_current_attempt_and_assignment(attempt, status, expected):
    from app.telegram_bots.work_views import work_buttons

    shown = record(attempt=attempt, status=status)
    keyboard = work_buttons(shown, owner_id="owner", permissions=3, held=HELD)
    assert kinds(keyboard) == expected
    if expected == ["work_start"]:
        assert keyboard[0][0]["payload"] == {"work_id": shown["id"], "assignment_revision": 2}
    if expected == ["stop_attempt"]:
        assert keyboard[0][0]["payload"] == {"work_id": shown["id"], "attempt_id": attempt["id"]}


@pytest.mark.parametrize("removed", ["agents:write", "runs:create"])
def test_no_create_ceiling_has_no_start_control(removed):
    from app.telegram_bots.work_views import work_buttons

    assert "work_start" not in kinds(work_buttons(record(), owner_id="owner", permissions=3, held=HELD - {removed}))


@pytest.mark.parametrize("status,expected", [("open", ["work_start", "work_change", "work_withdraw"]), ("submitted", ["work_withdraw"]), ("completed", []), ("cancelled", [])])
def test_manager_changes_match_open_review_and_closed_states(status, expected):
    from app.telegram_bots.work_views import work_buttons

    assert kinds(work_buttons(record(status=status), owner_id="owner", permissions=7, held=HELD)) == expected


def test_question_waits_for_explicit_answer_instead_of_starting():
    from app.telegram_bots.work_views import work_buttons

    request = {"id": "d" * 32, "purpose": "information", "state": "pending", "recipient_id": "owner", "can_respond": True, "can_manage": False, "needs_routing": False}
    shown = record(status="blocked")
    assert kinds(work_buttons(shown, owner_id="owner", permissions=3, held=HELD, request=request)) == ["digest_item"]
    answered = {**request, "state": "answered"}
    assert kinds(work_buttons(shown, owner_id="owner", permissions=3, held=HELD, request=answered)) == ["work_start"]


@pytest.mark.parametrize("permissions,held,changed", [(3, HELD, False), (7, HELD - {"agents:write"}, False), (7, HELD, True)])
def test_old_review_eligibility_cannot_grant_current_management(permissions, held, changed):
    from app.telegram_bots.work_views import work_buttons

    request = {"id": "d" * 32, "purpose": "review", "state": "pending", "recipient_id": "owner", "can_respond": False, "can_manage": True, "needs_routing": False}
    keyboard = work_buttons(record(status="submitted", needs_mandate_reconciliation=changed), owner_id="owner", permissions=permissions, held=held, request=request)
    assert not {"accept_open", "changes_open"} & set(kinds(keyboard))


@pytest.mark.parametrize("change", [{"needs_mandate_reconciliation": True}, {"work_enabled": False}, {"execution_available": False}])
def test_unstartable_record_keeps_no_start_description(change):
    from app.telegram_bots.work_views import work_buttons

    assert "work_start" not in kinds(work_buttons(record(**change), owner_id="owner", permissions=3, held=HELD))


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_detail_is_complete_escaped_and_puts_controls_after_all_parts(locale):
    from app.telegram_bots.work_views import present_work_detail

    objective = "<objective>& " + "a" * 4080
    criteria = "<criteria>& " + "b" * 4080
    shown = record(objective=objective, success_criteria=criteria)
    view = present_work_detail(shown, owner_id="owner", permissions=3, held=HELD, locale=locale, person="A <person>", date="2026-11-13")
    visible = "".join(part["visible_text"] for part in view.parts)
    assert objective in visible and criteria in visible
    assert "&lt;objective&gt;" in "".join(part["html"] for part in view.parts)
    assert all(not part.get("stored_keyboard") for part in view.parts[:-1])
    assert view.parts[-1]["stored_keyboard"] is True
    assert kinds(view.keyboard) == ["work_start"]


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_work_page_renders_current_rows_and_snapshot_cursors(locale):
    from types import SimpleNamespace

    from app.telegram_bots.work_views import present_work_page

    row = record(objective="<current> assignment", progress="A long progress note " * 20, attempt={"id": "c" * 32, "status": "running", "requester_id": "owner"})
    frame = {"work": [row], "requests": [], "pending": [], "routing": [], "page": 2, "pages": 3, "previous": "e" * 32, "next": "f" * 32, "expires_at": "2026-11-14T09:00:00+00:00"}
    view = present_work_page(frame, link=SimpleNamespace(id="d" * 32), owner_id="owner", name="Analyst", locale=locale)
    text = "\n".join(part["visible_text"] for part in view.parts)
    assert "11." in text and "<current> assignment" in text
    assert text.count("A long progress note") < 5
    navigation = [button for line in view.keyboard for button in line if button["kind"] == "work_list_page"]
    assert [button["payload"] for button in navigation] == [{"cursor": frame["previous"]}, {"cursor": frame["next"]}]
    assert all(button["expires_at"] == frame["expires_at"] for line in view.keyboard for button in line)
    assert view.keyboard[0][0]["payload"] == {"work_id": row["id"]}


def test_fresh_start_without_conversation_write_has_no_impossible_control():
    from app.telegram_bots.work_views import work_buttons

    shown = work_buttons(record(), owner_id="owner", permissions=3, held=HELD - {"threads:write"})
    assert "work_start" not in kinds(shown)


def test_old_review_flag_cannot_offer_accept_on_current_open_work():
    from app.telegram_bots.work_views import work_buttons

    request = {"id": "d" * 32, "purpose": "review", "state": "pending", "recipient_id": "owner", "can_respond": False, "can_manage": True, "needs_routing": False}
    shown = work_buttons(record(status="open"), owner_id="owner", permissions=7, held=HELD, request=request)
    assert not {"accept_open", "changes_open"} & set(kinds(shown))


def test_running_work_waiting_for_other_person_renders_without_progress():
    from types import SimpleNamespace

    from app.telegram_bots.work_views import present_work_page

    row = record(status="blocked", attempt={"id": "c" * 32, "status": "running", "requester_id": "owner"}, progress="")
    request = {"id": "d" * 32, "work_id": row["id"], "purpose": "information", "state": "pending", "recipient_id": "peer", "can_respond": True, "can_manage": False, "needs_routing": False}
    frame = {"work": [row], "requests": [request], "pending": [], "routing": [], "page": 1, "pages": 1, "previous": None, "next": None, "expires_at": "2026-11-14T09:00:00+00:00"}
    view = present_work_page(frame, link=SimpleNamespace(id="e" * 32), owner_id="owner", name="Analyst", locale="en-US")
    assert "Waiting for an answer" in "\n".join(p["visible_text"] for p in view.parts)

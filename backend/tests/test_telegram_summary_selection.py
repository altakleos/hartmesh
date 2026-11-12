"""Summary scheduling and ranking operate on the complete authorized set."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest


def settings():
    return SimpleNamespace(summary_enabled=True, summary_minute=480, quiet_enabled=True, quiet_start_minute=1260, quiet_end_minute=420)


@pytest.mark.parametrize("zone,hour,minute,expected", [("UTC", 7, 59, None), ("UTC", 8, 0, "2030-01-02"), ("America/New_York", 12, 59, None), ("America/New_York", 13, 0, "2030-01-02")])
def test_summary_is_due_by_local_calendar_day(zone, hour, minute, expected):
    from app.telegram_bots.summary import due_date

    assert due_date(datetime(2030, 1, 2, hour, minute, tzinfo=UTC), settings(), ZoneInfo(zone)) == expected


def test_summary_waits_for_quiet_end_without_starting_any_work():
    from app.telegram_bots.summary import due_date

    preferences = settings()
    preferences.quiet_end_minute = 540
    assert due_date(datetime(2030, 1, 2, 8, 59, tzinfo=UTC), preferences, ZoneInfo("UTC")) is None
    assert due_date(datetime(2030, 1, 2, 9, tzinfo=UTC), preferences, ZoneInfo("UTC")) == "2030-01-02"
    preferences.summary_enabled = False
    assert due_date(datetime(2030, 1, 2, 10, tzinfo=UTC), preferences, ZoneInfo("UTC")) is None


def work(index, *, status="open", attempt=None, creator="owner", created=None):
    return {
        "id": f"{index:032x}",
        "objective": f"Assignment {index}",
        "status": status,
        "assignment_revision": 1,
        "blocker": {"id": f"{index:032x}", "revision": 1},
        "attempt": attempt,
        "creator_id": creator,
        "created_at": created or "2030-01-01T00:00:00+00:00",
    }


def request(index, record, *, purpose="information", state="pending", created=None):
    return {
        "id": f"{index:032x}",
        "work_id": record["id"],
        "assignment_revision": 1,
        "basis_id": record["blocker"]["id"],
        "basis_revision": 1,
        "purpose": purpose,
        "state": state,
        "recipient_id": "owner",
        "needs_routing": False,
        "created_at": created or "2030-01-01T00:00:00+00:00",
    }


def test_complete_selection_ranks_old_requests_before_recent_startable_work():
    from app.telegram_bots.summary import sections

    recent = [work(index, created="2030-01-02T00:00:00+00:00") for index in range(1, 151)]
    old = work(1001, status="blocked", creator="someone-else", created="2029-01-01T00:00:00+00:00")
    pending = request(2001, old, created=old["created_at"])
    complete = {"work": recent + [old], "requests": [pending], "pending": [pending], "routing": []}
    result = sections(complete, owner_user_id="owner", since=datetime(2030, 1, 1, tzinfo=UTC))
    assert len(result["waiting"]) == 151
    assert result["waiting"][0]["request_id"] == pending["id"]
    assert result["waiting"][1]["work_id"] == recent[0]["id"]


def test_summary_categories_use_canonical_attempt_and_answer_state():
    from app.telegram_bots.summary import sections

    since = datetime(2030, 1, 1, tzinfo=UTC)
    settled = {"status": "succeeded", "assignment_revision": 1, "settled_at": since.isoformat()}
    records = [
        work(1, status="completed", attempt=settled),
        work(2, status="submitted", attempt={**settled, "settled_at": (since - timedelta(seconds=1)).isoformat()}),
        work(3, attempt={"status": "running"}),
        work(4, attempt={"status": "uncertain"}),
        work(5, status="blocked", attempt=settled),
        work(6, creator="someone-else"),
    ]
    answer = request(100, records[4], state="answered")
    result = sections({"work": records, "requests": [answer], "pending": [], "routing": []}, owner_user_id="owner", since=since)
    assert [row["work_id"] for row in result["finished"]] == [records[0]["id"]]
    assert [row["work_id"] for row in result["working"]] == [records[2]["id"]]
    assert [row["work_id"] for row in result["interrupted"]] == [records[3]["id"]]
    assert [(row["work_id"], row["kind"]) for row in result["waiting"]] == [(records[4]["id"], "resume")]


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_summary_caps_only_display_and_keeps_request_buttons_and_web_handover(locale):
    from app.telegram_bots.summary import sections
    from app.telegram_bots.summary_text import present_summary

    records = [work(index, status="blocked") for index in range(1, 109)]
    pending = [request(index + 1000, row) for index, row in enumerate(records)]
    data = sections({"work": records, "requests": pending, "pending": pending, "routing": []}, owner_user_id="owner", since=datetime(2030, 1, 1, tzinfo=UTC))
    shown = present_summary(data, locale=locale, protected=False, concealed=False, outbox_id="a" * 32, link_id="b" * 32, time="08:00", work_url="https://example.test/work")
    assert len(shown.parts) == 1 and "103" in shown.parts[0]["visible_text"]
    buttons = [button for line in shown.keyboard for button in line]
    assert len([button for button in buttons if button.get("kind") == "digest_item"]) == 5
    assert all(len(button["label_values"]["label"]) <= 32 for button in buttons if button.get("kind") == "digest_item")
    assert any(button.get("url") == "https://example.test/work" for button in buttons)
    assert buttons[-1]["kind"] == "summary_off"
    assert not any(button.get("kind") in {"work_details", "work_start", "stop_attempt"} for button in buttons)


def test_concealed_summary_has_no_private_titles_or_item_targets():
    from app.telegram_bots.summary_text import present_summary

    shown = present_summary(
        {"finished": [], "working": [], "interrupted": [], "waiting": [{"kind": "question", "objective": "Private title", "work_id": "c" * 32, "request_id": "d" * 32}]},
        locale="en-US",
        protected=True,
        concealed=True,
        outbox_id="a" * 32,
        link_id="b" * 32,
        time="08:00",
        work_url=None,
    )
    assert "Private" not in str(shown) and "d" * 32 not in str(shown)
    assert shown.parts[0]["protect_content"] is True
    assert [row[0]["kind"] for row in shown.keyboard] == ["reveal"]


def test_late_summary_deferred_across_midnight_keeps_its_scheduled_date():
    from app.telegram_bots.summary import due_date

    preferences = settings()
    preferences.summary_minute = 22 * 60
    assert due_date(datetime(2030, 1, 2, 6, 59, tzinfo=UTC), preferences, ZoneInfo("UTC")) is None
    assert due_date(datetime(2030, 1, 2, 7, tzinfo=UTC), preferences, ZoneInfo("UTC")) == "2030-01-01"


def test_daylight_saving_gap_and_fold_keep_one_local_date_key():
    from app.telegram_bots.summary import due_date

    preferences = settings()
    preferences.quiet_enabled = False
    preferences.summary_minute = 150
    zone = ZoneInfo("America/New_York")
    assert due_date(datetime(2030, 3, 10, 6, 59, tzinfo=UTC), preferences, zone) is None
    assert due_date(datetime(2030, 3, 10, 7, tzinfo=UTC), preferences, zone) == "2030-03-10"
    preferences.summary_minute = 90
    assert due_date(datetime(2030, 11, 3, 5, 30, tzinfo=UTC), preferences, zone) == "2030-11-03"
    assert due_date(datetime(2030, 11, 3, 6, 30, tzinfo=UTC), preferences, zone) == "2030-11-03"


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_every_summary_section_stays_within_one_message_at_display_caps(locale):
    from app.telegram_bots.split import utf16_units
    from app.telegram_bots.summary_text import present_summary

    data = {
        key: [{"kind": "question" if key == "waiting" else key, "objective": "🙂" * 1000, "work_id": f"{index:032x}", "request_id": f"{index + 100:032x}"} for index in range(10)] for key in ("finished", "working", "interrupted", "waiting")
    }
    shown = present_summary(data, locale=locale, protected=False, concealed=False, outbox_id="a" * 32, link_id="b" * 32, time="08:00", work_url=None)
    assert utf16_units(shown.parts[0]["visible_text"]) <= 4096


@pytest.mark.parametrize("damage", ["assignment", "basis", "basis_revision", "attempt_assignment", "unsettled"])
def test_summary_does_not_treat_stale_answer_as_ready_to_resume(damage):
    from app.telegram_bots.summary import sections

    now = datetime(2030, 1, 2, tzinfo=UTC)
    record = work(1, status="blocked", attempt={"status": "succeeded", "assignment_revision": 1, "settled_at": now.isoformat()})
    reply = request(2, record, state="answered")
    if damage == "assignment":
        reply["assignment_revision"] = 2
    elif damage == "basis":
        reply["basis_id"] = "f" * 32
    elif damage == "basis_revision":
        reply["basis_revision"] = 2
    elif damage == "attempt_assignment":
        record["attempt"]["assignment_revision"] = 2
    else:
        record["attempt"]["status"] = "stopping"
    result = sections({"work": [record], "requests": [reply], "pending": [], "routing": []}, owner_user_id="owner", since=now - timedelta(days=1))
    assert not result["waiting"]

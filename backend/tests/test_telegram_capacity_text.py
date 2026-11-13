"""Capacity copy preserves the owner-filtered HTTP facts without guessing."""

from datetime import UTC, datetime

import pytest


def occupant(**values):
    return dict(instance_id="a" * 32, instance_name="Reviewer", work_id="b" * 32, objective="Check the previous guide", started_at="2026-11-13T09:11:00+00:00", requester_id="person-a", **values)


@pytest.mark.parametrize("maximum,count,phrase", [(1, 1, "only one"), (2, 2, "2 assignments"), (1, 2, "above the current limit")])
def test_capacity_copy_uses_global_counts_without_inventing_visible_occupants(maximum, count, phrase):
    from app.telegram_bots.work_capacity_text import Capacity, waiting_html

    source = Capacity.parse({"max": maximum, "in_use": count, "running": []})
    text = waiting_html(source, objective="Review this guide", locale="en", people={}, time=lambda value: "09:11")
    assert phrase in text and "Reviewer" not in text


def test_room_available_does_not_claim_another_assignment_is_running():
    from app.telegram_bots.work_capacity_text import Capacity, waiting_html

    source = Capacity.parse({"max": 1, "in_use": 0, "running": []})
    assert source.available and waiting_html(source, objective="Review this guide", locale="en", people={}, time=lambda value: "09:11") is None


@pytest.mark.parametrize("started,requester,suffix", [(None, None, '"Check the previous guide".'), (None, "person-a", "started by Taylor."), ("2026-11-13T09:11:00+00:00", None, "started at 09:11.")])
def test_partial_task_metadata_never_falls_back_to_assignment_creator_or_date(started, requester, suffix):
    from app.telegram_bots.work_capacity_text import Capacity, waiting_html

    row = occupant()
    row.update(started_at=started, requester_id=requester)
    source = Capacity.parse({"max": 1, "in_use": 1, "running": [row]})
    text = waiting_html(source, objective="Review this guide", locale="en", people={"person-a": "Taylor"}, time=lambda value: "09:11")
    assert text.endswith(suffix)
    if started is None:
        assert "09:11" not in text


def test_visible_metadata_is_escaped_and_aware_dates_are_normalized():
    from app.telegram_bots.work_capacity_text import Capacity, waiting_html

    row = occupant()
    row.update(instance_name="<Reviewer>", started_at="2026-11-13T10:11:00+01:00")
    source = Capacity.parse({"max": 1, "in_use": 1, "running": [row]})
    seen = []

    def time(value):
        seen.append(value)
        return "09:11"

    text = waiting_html(source, objective="Read <guide>", locale="en", people={"person-a": "<Taylor>"}, time=time)
    assert "&lt;Reviewer&gt;" in text and "&lt;Taylor&gt;" in text and "&lt;guide&gt;" in text
    assert seen == [datetime(2026, 11, 13, 9, 11, tzinfo=UTC)]


@pytest.mark.parametrize("patch", [{"started_at": False}, {"requester_id": False}])
def test_falsey_task_facts_are_unknown_instead_of_borrowed_database_facts(patch):
    from app.telegram_bots.work_capacity_text import Capacity

    row = occupant()
    row.update(patch)
    source = Capacity.parse({"max": 1, "in_use": 1, "running": [row]})
    assert len(source.running) == 1 and getattr(source.running[0], next(iter(patch))) is None


@pytest.mark.parametrize("patch", [{"max": True}, {"in_use": False}, {"max": 0}, {"in_use": -1}, {"running": "private text"}])
def test_invalid_capacity_counts_cannot_support_a_free_place(patch):
    from app.telegram_bots.work_capacity_text import Capacity

    with pytest.raises(ValueError):
        Capacity.parse({**{"max": 1, "in_use": 0, "running": []}, **patch})

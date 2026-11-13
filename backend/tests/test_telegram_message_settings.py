"""Message settings choices are closed, localized and independent of Work actions."""

from types import SimpleNamespace

import pytest


def preferences(**values):
    return SimpleNamespace(locale="en-US", timezone="Europe/Lisbon", notify="all", quiet_enabled=True, quiet_start_minute=1260, quiet_end_minute=420, summary_enabled=True, summary_minute=480, **values)


@pytest.mark.parametrize("text,expected", [("22:30-06:30", (1350, 390)), (" 00:00-23:59 ", (0, 1439))])
def test_custom_quiet_times_accept_two_distinct_24_hour_times(text, expected):
    from app.telegram_bots.message_settings import parse_quiet

    assert parse_quiet(text) == expected


@pytest.mark.parametrize("text", ["22:30-22:30", "24:00-08:00", "22:60-06:30", "22-6", "22:30-06:30 extra", "２２:３０-０６:３０", "", None])
def test_custom_quiet_times_refuse_ambiguous_or_invalid_values(text):
    from app.telegram_bots.message_settings import parse_quiet

    with pytest.raises(ValueError):
        parse_quiet(text)


@pytest.mark.parametrize(
    "field,value,changes",
    [
        ("notify", "none", {"notify": "none"}),
        ("quiet", "off", {"quiet_enabled": False, "quiet_start_minute": 1260, "quiet_end_minute": 420}),
        ("quiet", "22:00-08:00", {"quiet_enabled": True, "quiet_start_minute": 1320, "quiet_end_minute": 480}),
        ("summary", "07:00", {"summary_enabled": True, "summary_minute": 420}),
        ("summary", "off", {"summary_enabled": False, "summary_minute": 480}),
        ("locale", "zh-CN", {"locale": "zh-CN"}),
    ],
)
def test_setting_choices_change_only_the_selected_preference(field, value, changes):
    from app.telegram_bots.message_settings import choose, setting_changes

    body = choose(preferences(), field, value)
    assert body["field"] == field
    assert setting_changes(body) == changes


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_message_settings_pages_have_scoped_controls_and_web_handover(locale):
    from app.telegram_bots.message_settings import present

    settings = preferences()
    settings.locale = locale
    link = SimpleNamespace(id="a" * 32)
    state = present(settings, link=link, product="Product", settings_url="https://example.test/settings")
    assert len(state["keyboard"]) == 3
    assert {button["payload"]["field"] for line in state["keyboard"][:2] for button in line} == {"notify", "quiet", "summary", "locale"}
    assert "/work" not in state["values"]["body"]
    for field in ("notify", "quiet", "summary", "locale"):
        page = present(settings, link=link, product="Product", page=field)
        controls = [button for line in page["keyboard"] for button in line if "kind" in button]
        assert all(control["target_id"] == link.id for control in controls)
        assert controls[-1]["kind"] == "back" and controls[-1]["payload"] == {"settings_page": "state"}


@pytest.mark.parametrize(
    "field,value",
    [
        ("quiet", {"enabled": 1, "start": 1320, "end": 480}),
        ("quiet", {"enabled": True, "start": 480, "end": 480}),
        ("summary", {"enabled": True, "minute": True}),
        ("summary", {"enabled": False, "minute": 1440}),
        ("notify", True),
        ("timezone", "UTC"),
        ("locale", "unknown"),
    ],
)
def test_settings_receipt_validation_rejects_untyped_or_unrecognized_values(field, value):
    from app.telegram_bots.message_settings import setting_body

    with pytest.raises(ValueError):
        setting_body({"field": field, "old": value, "new": value})

"""Notice timestamps use the reader's day and timezone, including DST."""

from datetime import datetime
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize(
    "zone,locale,moment,now,expected",
    [
        ("UTC", "en-US", "2026-10-16T18:00:00+00:00", "2026-10-16T20:00:00+00:00", "18:00"),
        ("America/New_York", "en-US", "2026-10-16T18:00:00+00:00", "2026-10-17T18:00:00+00:00", "Fri 16 Oct, 14:00"),
        ("America/New_York", "en-US", "2026-11-01T05:30:00+00:00", "2026-11-01T07:00:00+00:00", "01:30"),
        ("America/New_York", "en-US", "2026-11-01T06:30:00+00:00", "2026-11-01T07:00:00+00:00", "01:30"),
        ("Asia/Shanghai", "zh-CN", "2026-10-16T18:00:00+00:00", "2026-10-18T18:00:00+00:00", "周六 10月17日，02:00"),
    ],
)
def test_notice_time_uses_local_calendar(zone, locale, moment, now, expected):
    from app.telegram_bots.settings import format_time

    assert format_time(datetime.fromisoformat(moment), SimpleNamespace(timezone=zone, locale=locale), now=datetime.fromisoformat(now)) == expected


def test_notice_time_refuses_naive_timestamp():
    from app.telegram_bots.settings import format_time

    with pytest.raises(ValueError):
        format_time(datetime(2026, 10, 16, 18), SimpleNamespace(timezone="UTC", locale="en-US"))

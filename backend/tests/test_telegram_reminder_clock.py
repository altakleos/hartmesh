"""The final minute of a local day remains an explicit quiet test precondition."""

from datetime import UTC, timedelta
from zoneinfo import ZoneInfo

import pytest
import test_telegram_resume_reminders as module
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_quiet_drift_fixture_at_the_final_minute(work_delivery, monkeypatch):
    actual = module.ManualClock

    class FinalMinuteClock(actual):
        def __init__(self, now=None):
            local = now.astimezone(ZoneInfo("America/New_York"))
            future = local.replace(hour=22, minute=58, second=50, microsecond=0) + timedelta(days=1)
            super().__init__(future.astimezone(UTC))

    monkeypatch.setattr(module, "ManualClock", FinalMinuteClock)
    await module.test_reminder_final_settings_fence_observes_summary_enable(work_delivery, monkeypatch)

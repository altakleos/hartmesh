"""Question cleanup keeps source checks and cold timezone reads off the loop."""

from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_question_edits import test_actual_assessment_edits_the_acknowledged_question as assessed
from test_telegram_question_edits import test_late_protection_upgrade_repairs_only_the_retained_delivery as repair
from test_telegram_question_edits import test_question_update_rechecks_access_to_original_references as references
from test_telegram_question_edits import test_question_update_retries_a_late_language_change as language
from test_telegram_question_edits import test_web_question_change_edits_original_acknowledged_message as edited
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_question_cleanup_cold_timezone(work_delivery):
    ZoneInfo.clear_cache()
    await edited(work_delivery, "answer", "Answered by you on the web")


@pytest.mark.asyncio
async def test_question_cleanup_assessment(work_delivery):
    await assessed(work_delivery)


@pytest.mark.asyncio
async def test_question_cleanup_protection_repair(work_delivery):
    await repair(work_delivery)


@pytest.mark.asyncio
async def test_question_cleanup_final_references(work_delivery, monkeypatch):
    await references(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_question_cleanup_late_language(work_delivery, monkeypatch):
    await language(work_delivery, monkeypatch)

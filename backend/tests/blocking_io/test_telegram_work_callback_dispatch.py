"""Actual Work callback admission and retry retain strict I/O boundaries."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_callback_dispatch import test_actual_callback_dispatch_retries_and_plans_the_canonical_result as retry
from test_telegram_work_callback_dispatch import test_actual_first_taps_create_a_shown_prompt_without_an_action as prompt
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_work_retry_callback(work_delivery):
    await retry(work_delivery)


@pytest.mark.asyncio
async def test_work_prompt_callbacks(work_delivery):
    await prompt(work_delivery)

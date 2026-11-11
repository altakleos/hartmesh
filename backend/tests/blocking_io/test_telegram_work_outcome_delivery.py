"""Result delivery exercises real SQL and SDK guards without event-loop I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_outcome_delivery import test_completed_acceptance_delivers_once_and_retires_original_controls as completed
from test_telegram_work_outcome_delivery import test_input_completion_waits_for_outcome_ack as input_completion
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_completed_delivery(work_delivery):
    await completed(work_delivery)


@pytest.mark.asyncio
async def test_input_delivery(work_delivery):
    await input_completion(work_delivery)

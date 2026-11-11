"""Work prompt publication and final SDK proof under strict I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import test_prompt_requires_full_delivery_and_preserves_request_basis as typing_prompt
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_typing_prompt_delivery(work_delivery):
    await typing_prompt(work_delivery, "information", "hi_answer", "answer")

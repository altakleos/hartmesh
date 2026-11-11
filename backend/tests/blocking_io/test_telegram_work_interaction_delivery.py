"""Actual Work interaction publication and SDK authority under strict I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import test_acknowledged_first_tap_creates_reply_and_never_an_act as first_tap
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_confirmation_delivery(work_delivery):
    await first_tap(work_delivery, "review", "accept_open", "accept")

"""Cold timezone/config drafting and cancellation stay off the Gateway loop."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from test_telegram_assignment_drafts import draft_configuration as draft_configuration
from test_telegram_assignment_drafts import test_caller_cancellation_retains_the_owned_draft_call_until_drained as cancelled
from test_telegram_assignment_drafts import test_extraction_uses_one_bounded_non_graph_call_with_untrusted_data as extraction


@pytest.mark.asyncio
async def test_cold_proposal_extraction_keeps_the_loop_clear(monkeypatch, draft_configuration):
    await asyncio.to_thread(ZoneInfo.clear_cache)
    await extraction(monkeypatch, draft_configuration)


@pytest.mark.asyncio
async def test_proposal_cancellation_keeps_the_loop_clear(monkeypatch):
    await cancelled(monkeypatch)


@pytest.mark.asyncio
async def test_supported_proposal_agent_loop_keeps_the_loop_clear():
    from test_assignment_proposal_runtime import test_async_actual_agent_also_ends_after_supported_proposal

    await test_async_actual_agent_also_ends_after_supported_proposal()

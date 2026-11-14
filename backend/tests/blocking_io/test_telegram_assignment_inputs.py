"""Assignment prompts, captures and replacement proofs keep the loop clear."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_assignment_chat_fallthrough import test_installed_assignment_probe_preserves_ordinary_chat_custody as ordinary_chat
from test_telegram_assignment_chat_fallthrough import test_legacy_unpublished_probe_capture_gets_one_resend_notice as legacy_chat
from test_telegram_assignment_editing import test_actual_edit_reply_replaces_one_draft_and_extracts_once as edit
from test_telegram_assignment_prompt_delivery import test_prompt_visibility_begins_at_its_actual_ack_without_a_model_or_work as prompt
from test_telegram_assignment_prompt_inputs import test_typed_prompt_answer_extracts_once_and_preserves_one_private_card as capture
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_assignment_prompt_keeps_the_loop_clear(work_delivery):
    await prompt(work_delivery)


@pytest.mark.asyncio
async def test_assignment_capture_keeps_the_loop_clear(work_delivery, monkeypatch):
    await capture(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_assignment_edit_keeps_the_loop_clear(work_delivery, monkeypatch):
    await edit(work_delivery, monkeypatch)


@pytest.mark.asyncio
async def test_assignment_probe_chat_fallthrough_keeps_the_loop_clear(work_delivery, monkeypatch):
    await ordinary_chat(work_delivery, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["replay", "recovery"])
async def test_legacy_assignment_probe_closure_keeps_the_loop_clear(work_delivery, monkeypatch, entry):
    await legacy_chat(work_delivery, monkeypatch, True, entry)

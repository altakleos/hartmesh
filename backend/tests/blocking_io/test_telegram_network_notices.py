"""Owned denial reads and actual SQL/SDK trailers under the strict I/O gate."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
import test_telegram_network_denial_source as sources
import test_telegram_network_denials as notices
import test_telegram_network_notice_questions as questions
from _telegram_repository_test_support import db as db
from test_telegram_instance_network_cards import network as network
from test_telegram_instance_publication import publication as publication


@pytest_asyncio.fixture(autouse=True)
async def timezone_precondition():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_denial_source_and_three_trailers_stay_off_loop(network):
    await notices.test_turn_delivers_three_distinct_blocked_sites_after_answer_and_no_act(network)


@pytest.mark.asyncio
async def test_denial_source_drains_held_http_on_repeated_cancel(network, monkeypatch):
    await sources.test_cancelled_source_read_owns_original_http_until_it_settles(network, monkeypatch)


@pytest.mark.asyncio
async def test_question_controls_and_denial_notice_stay_off_loop(network):
    await questions.test_answered_question_does_not_cancel_the_separate_denial_notice(network)


@pytest.mark.asyncio
async def test_question_writer_drains_repeated_cancel(network, monkeypatch):
    await questions.test_cancelled_question_writer_keeps_both_plans_owned_until_settlement(network, monkeypatch)

"""Work file ingress, native storage and canonical acknowledgement avoid loop I/O."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_work_file_inputs import test_file_answer_acknowledgement_names_the_file_and_finishes_only_after_delivery as exercise_answer
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest_asyncio.fixture(autouse=True)
async def fixture_timezone():
    await asyncio.to_thread(ZoneInfo, "America/New_York")


@pytest.mark.asyncio
async def test_actual_file_answer_keeps_native_io_off_loop(work_delivery, monkeypatch, tmp_path):
    await exercise_answer(work_delivery, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_captionless_file_handoff_keeps_native_io_off_loop(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_file_inputs import test_closed_saved_file_send_new_reaches_actual_chat_publication

    await test_closed_saved_file_send_new_reaches_actual_chat_publication(work_delivery, monkeypatch, tmp_path, "unsaved", "")


@pytest.mark.asyncio
async def test_album_collection_and_restart_keep_io_off_loop(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_file_albums import test_restart_classifies_whole_durable_album_without_file_or_chat_effects

    await test_restart_classifies_whole_durable_album_without_file_or_chat_effects(work_delivery, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_actual_album_ingress_keeps_native_and_response_io_off_loop(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_album_answers import test_actual_album_ingress_reaches_one_complete_work_response

    await test_actual_album_ingress_reaches_one_complete_work_response(work_delivery, monkeypatch, tmp_path, False)


@pytest.mark.asyncio
async def test_album_full_ack_keeps_io_off_loop(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_album_answers import test_multipart_album_ack_settles_nothing_before_last_part

    await test_multipart_album_ack_settles_nothing_before_last_part(work_delivery, monkeypatch, tmp_path)

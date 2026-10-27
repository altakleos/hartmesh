"""Whole command/press items use actual SQL, SDK and recorded Gateway HTTP."""

import asyncio
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_callback_commands import test_new_command_choice_uses_actual_sdk_gateway_and_original_input as new_flow
from test_telegram_callback_commands import test_settings_command_press_changes_one_setting_and_edits_with_new_controls as settings_flow
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
async def test_actual_settings_command_and_press_keep_the_gateway_loop_clear(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await settings_flow(db, monkeypatch, off_loop=True)


@pytest.mark.asyncio
async def test_actual_new_command_confirm_and_gateway_reads_keep_the_loop_clear(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    await new_flow(db, monkeypatch, "new_confirm", "New conversation started.", 1, off_loop=True)

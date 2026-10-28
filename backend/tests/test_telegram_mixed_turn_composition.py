"""Actual worker file plus human-input source, upload barrier and continuation."""

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_turn_composition import test_actual_managed_turn_creates_once_and_delivers_only_verified_output as exercise_turn


@pytest.mark.asyncio
@pytest.mark.parametrize("dialogue,case", [("choice", "text"), ("form", "custom_text")])
async def test_actual_file_ack_precedes_question_controls_and_original_response(capture_environment, monkeypatch, tmp_path, dialogue, case):
    await exercise_turn(capture_environment, monkeypatch, tmp_path, case, managed_ingress=True, dialogue=dialogue, mixed_file=True)

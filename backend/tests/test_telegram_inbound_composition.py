"""Real SDK/controller/manager/upload/worker composition with offline transports."""

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_turn_composition import test_actual_managed_turn_creates_once_and_delivers_only_verified_output as run_managed_turn


@pytest.mark.asyncio
async def test_actual_sdk_caption_file_is_uploaded_before_same_worker_input(capture_environment, monkeypatch, tmp_path):
    await run_managed_turn(capture_environment, monkeypatch, tmp_path, "text", managed_ingress=True, inbound_file=True)

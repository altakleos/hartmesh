"""Actual account, connection and instance reader cleanup keeps the loop clear."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_eligibility_reader_shutdown import test_real_eligibility_lookup_drains_close as eligibility_reader
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["account", "connection", "instance"])
@pytest.mark.parametrize("phase", ["body", "close"])
async def test_actual_eligibility_reader_cleanup(capture_environment, monkeypatch, tmp_path, reader, phase):
    await eligibility_reader(capture_environment, monkeypatch, tmp_path, reader, phase)

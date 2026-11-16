"""Real lifecycle admission, inactive receipts and final roles keep the loop clear."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_lifecycle_role_snapshot import test_actual_notice_rechecks_role_limit_after_last_require as role_snapshot
from test_telegram_retry_lifecycle import test_inactive_receipt_retains_its_actual_authority as receipt_authority
from test_telegram_retry_lifecycle import test_known_receipt_priority_respects_current_read_access as known_receipt
from test_telegram_retry_lifecycle import test_restored_agent_retires_the_global_notice_before_sdk as restored_notice
from test_telegram_retry_lifecycle import test_unavailable_retry_still_needs_its_real_custody as custody
from test_telegram_retry_lifecycle import test_unknown_command_retry_explains_current_lifecycle as management
from test_telegram_retry_lifecycle import test_unknown_save_retry_keeps_original_on_pause as assignment
from test_telegram_retry_lifecycle import test_unknown_start_retry_keeps_native_on_pause as execution
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["paused", "retired", "use", "read-capability"])
async def test_current_lifecycle(capture_environment, monkeypatch, tmp_path, change):
    await management(capture_environment, monkeypatch, tmp_path, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["paused", "read-capability"])
async def test_readable_receipt_priority(capture_environment, monkeypatch, tmp_path, change):
    await known_receipt(capture_environment, monkeypatch, tmp_path, change)


@pytest.mark.asyncio
async def test_final_role_snapshot(capture_environment, monkeypatch, tmp_path):
    await role_snapshot(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_paused_start(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_paused_save(capture_environment, monkeypatch, tmp_path):
    await assignment(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["consumed", "ack"])
async def test_unavailable_custody(capture_environment, monkeypatch, tmp_path, invalid):
    await custody(capture_environment, monkeypatch, tmp_path, invalid)


@pytest.mark.asyncio
async def test_restored_lifecycle(capture_environment, monkeypatch, tmp_path):
    await restored_notice(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "service", "account"])
async def test_receipt_authority(capture_environment, monkeypatch, tmp_path, change):
    await receipt_authority(capture_environment, monkeypatch, tmp_path, change)

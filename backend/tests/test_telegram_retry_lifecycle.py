"""Current lifecycle replies belong to a new Retry, never its unknown command."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_assignment_retry_refusals import test_current_save_retry_preserves_unknown_delegation as assignment
from test_telegram_execution_retry_refusals import test_current_execution_retry_preserves_actual_native as execution
from test_telegram_management_retry_refusals import test_current_management_retry_denial_preserves_unknown_owner as management
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["paused", "retired", "use", "read-capability"])
async def test_unknown_command_retry_explains_current_lifecycle(capture_environment, monkeypatch, tmp_path, change):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", change=change)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["paused", "read-capability"])
async def test_known_receipt_priority_respects_current_read_access(capture_environment, monkeypatch, tmp_path, change):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", change=change, known=True)


@pytest.mark.asyncio
async def test_unknown_start_retry_keeps_native_on_pause(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path, "activate", "paused")


@pytest.mark.asyncio
async def test_retry_expiry_waits_for_real_receipt_owner(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path, "activate", "paused", contend=True)


@pytest.mark.asyncio
async def test_unknown_save_retry_keeps_original_on_pause(capture_environment, monkeypatch, tmp_path):
    await assignment(capture_environment, monkeypatch, tmp_path, "paused")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["consumed", "ack"])
async def test_unavailable_retry_still_needs_its_real_custody(capture_environment, monkeypatch, tmp_path, invalid):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", change="use", lifecycle_invalid=invalid)


@pytest.mark.asyncio
async def test_restored_agent_retires_the_global_notice_before_sdk(capture_environment, monkeypatch, tmp_path):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", change="paused", restore_notice=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "service", "account"])
async def test_inactive_receipt_retains_its_actual_authority(capture_environment, monkeypatch, tmp_path, change):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", change="paused", inactive_rejection=change)


@pytest.mark.asyncio
async def test_inactive_save_recovers_its_actual_no_due_child(capture_environment, monkeypatch, tmp_path):
    await assignment(capture_environment, monkeypatch, tmp_path, "paused", no_due=True)


@pytest.mark.asyncio
async def test_inactive_stop_receipt_never_dispatches(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path, "stop", "paused", known_stop=True)

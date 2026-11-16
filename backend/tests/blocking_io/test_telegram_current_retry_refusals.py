"""Real Retry custody, current admission and remaining dispatch keep the loop clear."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_assignment_retry_refusals import test_current_save_retry_preserves_unknown_delegation as assignment
from test_telegram_execution_retry_refusals import test_current_execution_retry_preserves_actual_native as execution
from test_telegram_management_retry_refusals import test_current_management_retry_denial_preserves_unknown_owner as management
from test_telegram_retry_dispatch_permissions import test_original_requester_can_retry_known_command_dispatch_without_manage as dispatch
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["edit", "cancel"])
async def test_management_owner(capture_environment, monkeypatch, tmp_path, kind):
    await management(capture_environment, monkeypatch, tmp_path, kind)


@pytest.mark.asyncio
async def test_management_expiry_receipt_owner(capture_environment, monkeypatch, tmp_path):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", contend=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inspect", "capability", "cancel-capability"])
async def test_management_admission(capture_environment, monkeypatch, tmp_path, change):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", change=change)


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["restore", "receipt", "age"])
async def test_final_authority_and_retention(capture_environment, monkeypatch, tmp_path, race):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", race=race)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["consumed", "expired", "target", "ack"])
async def test_known_receipt_requires_retry_custody(capture_environment, monkeypatch, tmp_path, invalid):
    await management(capture_environment, monkeypatch, tmp_path, "cancel", invalid=invalid)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inspect", "capability"])
async def test_unknown_save(capture_environment, monkeypatch, tmp_path, change):
    await assignment(capture_environment, monkeypatch, tmp_path, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["activate", "stop"])
@pytest.mark.parametrize("change", ["inspect", "capability"])
async def test_native_execution_owner(capture_environment, monkeypatch, tmp_path, kind, change):
    await execution(capture_environment, monkeypatch, tmp_path, kind, change)


@pytest.mark.asyncio
async def test_context_capability(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path, "activate", "capability", context=True)


@pytest.mark.asyncio
async def test_observer_source_retirement(capture_environment, monkeypatch, tmp_path):
    await execution(capture_environment, monkeypatch, tmp_path, "activate", "capability", observer_race=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["manage", "capability"])
async def test_foreign_stop_priority(capture_environment, monkeypatch, tmp_path, change):
    await execution(capture_environment, monkeypatch, tmp_path, "stop", change, foreign=True)


@pytest.mark.asyncio
async def test_requester_dispatch_authority(capture_environment, monkeypatch, tmp_path):
    await dispatch(capture_environment, monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_remaining_scope(capture_environment, monkeypatch, tmp_path):
    await dispatch(capture_environment, monkeypatch, tmp_path, changed_scope=True)

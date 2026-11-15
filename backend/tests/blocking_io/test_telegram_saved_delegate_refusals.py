"""Saved delegation refusals preserve actual SQL owners off the event loop."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_saved_delegate_ownership import OWNERSHIP_CASES
from test_telegram_saved_delegate_ownership import test_saved_delegate_keeps_late_owners as late_owners
from test_telegram_saved_delegate_refusals import DELEGATE_CASES
from test_telegram_saved_delegate_refusals import test_saved_delegate_keeps_original_owner_on_permission_loss as current_refusal
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,change", DELEGATE_CASES)
async def test_current_saved_delegate_refusal(capture_environment, monkeypatch, tmp_path, kind, change):
    await current_refusal(capture_environment, monkeypatch, tmp_path, kind, change)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", OWNERSHIP_CASES)
async def test_late_saved_delegate_owner(capture_environment, monkeypatch, tmp_path, change):
    await late_owners(capture_environment, monkeypatch, tmp_path, change)

"""Real receipt-owned previews retain SQL and Native ownership under strict I/O."""

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_telegram_continuation_resume_views import test_continuation_preview_preserves_independent_receipt_proof as independent_proof
from test_telegram_continuation_resume_views import test_fresh_resume_reaches_native_with_retained_primary as native_resume
from test_telegram_continuation_resume_views import test_saved_request_opens_fresh_resume_without_replaying_primary as saved_resume
from test_telegram_continuation_resume_views import test_updated_assignment_has_complete_multipart_resume as multipart_resume
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
@pytest.mark.parametrize("ending", ["refused", "expired"])
async def test_current_resume_after_ended_start(capture_environment, monkeypatch, tmp_path, kind, ending):
    await saved_resume(capture_environment, monkeypatch, tmp_path, kind, ending)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
async def test_actual_native_reservation(capture_environment, monkeypatch, tmp_path, kind):
    await native_resume(capture_environment, monkeypatch, tmp_path, kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["changed", "marker", "retention", "retention-corrupt"])
async def test_actual_primary_proof_and_retention(capture_environment, monkeypatch, tmp_path, mode):
    await independent_proof(capture_environment, monkeypatch, tmp_path, mode)


@pytest.mark.asyncio
async def test_complete_current_assignment(capture_environment, monkeypatch, tmp_path):
    await multipart_resume(capture_environment, monkeypatch, tmp_path)

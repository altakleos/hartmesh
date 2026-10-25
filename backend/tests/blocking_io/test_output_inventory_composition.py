"""Actual creating HTTP, first-party SDK worker, scans and strict owned reads."""

import pytest
import test_output_inventory_composition as composition
from _run_output_capture_test_support import capture_environment as capture_environment  # noqa: F401
from _run_output_capture_test_support import output_store as output_store  # noqa: F401

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("case", ["normal", "runtime_ai_mutation", "early_rollback", "late_rollback"])
async def test_real_worker_inventory_protocol_never_blocks_loop(capture_environment, monkeypatch, tmp_path, case):
    await composition.test_actual_firstparty_worker_inventory_and_real_cancellation_through_owned_http(capture_environment, monkeypatch, tmp_path, case)

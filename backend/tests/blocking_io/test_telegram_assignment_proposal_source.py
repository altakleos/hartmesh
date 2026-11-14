"""Proposal capture uses the retained owner with whole-operation I/O checks."""

import pytest
from support.detectors.blocking_io_runtime import detect_blocking_io_strict
from test_telegram_assignment_proposal_reader import reader_environment as reader_environment
from test_telegram_assignment_proposal_reader import test_guarded_read_retains_the_original_attempt_for_every_http_operation as exercise_read


@pytest.mark.asyncio
async def test_proposal_read_and_final_guard_keep_blocking_work_off_loop(reader_environment):
    with detect_blocking_io_strict():
        await exercise_read(reader_environment)

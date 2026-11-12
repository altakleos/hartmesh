"""The excerpt consumes a real offline worker seal, not a seeded receipt."""

import pytest
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import select
from test_run_output_capture_worker import _environment, _execute

from app.telegram_bots.attempt_output import attempt_output
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow


@pytest.mark.asyncio
async def test_attempt_excerpt_consumes_actual_worker_output(output_store, monkeypatch):
    env = await _environment(output_store, monkeypatch)
    await _execute(env)
    async with output_store.sf() as reader:
        run = await reader.get(RunRow, env.record.run_id)
        receipt = run.kwargs_json["output_evidence"]
        source = await reader.scalar(select(RunEventRow).where(RunEventRow.thread_id == run.thread_id, RunEventRow.seq == receipt["event_seq"]))
        assert receipt["assistant_message_id"] == "genuine-model-id"
        assert attempt_output(run, source) == "  Exact answer\nα"

"""Actual proof route/source SQL keeps cold account checks off the Gateway loop."""

import asyncio
import threading

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from test_run_output_route import get, seed
from test_run_output_route import route_environment as route_environment

from app.gateway import channel_execution_reads

pytestmark = pytest.mark.asyncio


async def test_actual_sql_projection_owns_cold_account_preparation(route_environment, monkeypatch, tmp_path):
    path = tmp_path / "small-account-fixture.txt"
    await asyncio.to_thread(path.write_text, "prepared", encoding="utf-8")
    original = channel_execution_reads.account_refusal
    main = threading.get_ident()
    workers = []

    def cold(*args, **kwargs):
        assert path.read_text(encoding="utf-8") == "prepared"
        workers.append(threading.get_ident())
        return original(*args, **kwargs)

    monkeypatch.setattr(channel_execution_reads, "account_refusal", cold)
    record, _ = await seed(route_environment)
    response = await get(route_environment, record.run_id)
    assert response.status_code == 200 and response.json()["state"] == "complete"
    assert len(workers) == 2 and all(worker != main for worker in workers)

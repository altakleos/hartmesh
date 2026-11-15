"""Real synchronous preparation retains request context and cancellation ownership.

The borrowed admission fixture uses SQL and a worker double: these cases prove
pre-admission ownership, not worker execution or output provenance.
"""

import asyncio
import threading

import pytest
from _run_output_store_test_support import output_store as output_store
from test_run_output_capture_admission import admission as admission
from test_run_output_capture_admission import captured, ordinary

from app.gateway import services
from deerflow.config.app_config import get_app_config, pop_current_app_config, push_current_app_config


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize("boundary", ["get_run_context", "build_run_config"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_start_preparation_keeps_context_and_drains_cancellation(admission, monkeypatch, capture, boundary, cancel):
    e = admission
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = getattr(services, boundary)
    loop_thread = threading.get_ident()
    override = e.config.model_copy(update={"recursion_limit": 37})
    observed = []

    def held(*args, **kwargs):
        observed.append((threading.get_ident(), get_app_config()))
        started.set()
        try:
            assert release.wait(10), "preparation owner failed to release its worker"
            return original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(services, boundary, held)
    push_current_app_config(override)
    task = asyncio.create_task(services.start_run(captured() if capture else ordinary(), e.thread_id, e.request))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        if cancel:
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not finished.is_set()
        release.set()
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await task
            assert finished.is_set()
            assert not e.inputs
            assert await e.repository.list_by_thread(e.thread_id, user_id=str(e.owner.id)) == []
        else:
            record = await task
            await record.task
            assert e.inputs[0]["ctx"].app_config is override
            assert e.inputs[0]["config"]["recursion_limit"] == 37
        assert len(observed) == 1
        assert observed[0][0] != loop_thread and observed[0][1] is override
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        pop_current_app_config()

"""The optional manager path borrows one HTTP owner and drains file work."""

import asyncio
import threading

import pytest
from test_channel_execution_manager import setup as setup
from test_channel_execution_manager import test_actual_facade_and_one_http_owner_receive_the_managers_canonical_creating_body as test_actual_owned_facade_manager_wire  # noqa: F401

from app.channels.execution import ChannelExecutionProtocolError


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_output_preparation_runs_off_loop_and_retains_its_worker_before_late_refusal(setup, tmp_path, monkeypatch, cancel):
    from app.channels import manager as module

    e = setup
    path = tmp_path / "output-proof.txt"
    await asyncio.to_thread(path.write_text, "synthetic output", encoding="utf-8")
    loop = asyncio.get_running_loop()
    owner_thread = threading.get_ident()
    entered, release = asyncio.Event(), threading.Event()
    observed = []
    actual = module._prepare_artifact_delivery

    def prepare(*args, **kwargs):
        observed.append(threading.get_ident())
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        assert path.read_text(encoding="utf-8") == "synthetic output"
        return actual(*args, **kwargs)

    monkeypatch.setattr(module, "_prepare_artifact_delivery", prepare)
    task = asyncio.create_task(e.manager._handle_chat(e.message))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        if cancel:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        else:
            e.admission.allowed = False
        release.set()
        with pytest.raises(asyncio.CancelledError if cancel else ChannelExecutionProtocolError):
            await task
        assert not e.outbounds and len(observed) == 1 and observed[0] != owner_thread
        assert e.client.runs.wait.await_count == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)

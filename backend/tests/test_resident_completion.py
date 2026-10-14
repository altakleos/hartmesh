"""Mandatory worker completion after SDK, durable status and Work settlement."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_agent_execution_runtime import execution as make_execution

from deerflow.agent_instances import runtime
from deerflow.runtime.runs.schemas import RunStatus


def setup():
    order = []
    residents = SimpleNamespace(
        attachments=SimpleNamespace(fence_reason=AsyncMock(return_value=None)),
        run_ended=AsyncMock(side_effect=lambda *args, **kwargs: order.append("ended")),
    )
    work = SimpleNamespace(attempt_id="attempt", owned_cleanup_confirmed=True, finish=AsyncMock(side_effect=lambda *args, **kwargs: order.append(("finish", kwargs["settled"])) or ("succeeded" if kwargs["settled"] else "uncertain")))
    execution = replace(make_execution(), authority=SimpleNamespace(instances=SimpleNamespace(residents=residents)), work=work)
    sandbox = SimpleNamespace(id="d" * 32, requires_container_recycle=False, execution_outcome_uncertain=False)
    provider = SimpleNamespace(sandbox=sandbox, close=Mock(side_effect=lambda: order.append("close")))
    environment = runtime.ExecutionEnvironment(execution, provider, attachment_id=sandbox.id)
    record = SimpleNamespace(run_id="run", user_id="requester", status=RunStatus.success)
    stored = {"status": "success", "stop_reason": None}
    manager = SimpleNamespace(_store=SimpleNamespace(get=AsyncMock(side_effect=lambda *args, **kwargs: order.append("store") or stored)))
    return execution, environment, manager, record, residents, order


@pytest.mark.anyio
async def test_completion_orders_client_close_durable_status_work_finish_then_run_ended():
    execution, environment, manager, record, residents, order = setup()
    await runtime.complete_environment(execution, environment, manager, record)
    assert order == ["close", "store", ("finish", True), "ended"]
    residents.run_ended.assert_awaited_once_with(execution.instance.id, execution.instance.home_id, "run", "d" * 32, recycle=False, attempt_id="attempt")


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["close", "store", "finish", "fence"])
async def test_failed_completion_step_cannot_skip_work_or_run_ended(failure):
    execution, environment, manager, record, residents, _order = setup()
    if failure == "close":
        environment.provider.close.side_effect = RuntimeError("SDK close failed")
    elif failure == "store":
        manager._store.get.side_effect = RuntimeError("status unavailable")
    elif failure == "finish":
        execution.work.finish.side_effect = RuntimeError("settlement failed")
    else:
        residents.attachments.fence_reason.side_effect = RuntimeError("fence diagnostics unavailable")
    await runtime.complete_environment(execution, environment, manager, record)
    execution.work.finish.assert_awaited_once()
    residents.run_ended.assert_awaited_once()
    assert residents.run_ended.call_args.kwargs["recycle"]
    if failure != "finish":
        assert not execution.work.finish.call_args.kwargs["settled"]


@pytest.mark.anyio
async def test_fenced_attachment_never_promotes_success_and_unresolved_attempt_requests_recycle():
    execution, environment, manager, record, residents, _order = setup()
    residents.attachments.fence_reason.return_value = "lifecycle"
    execution.work.finish.return_value = "uncertain"
    execution.work.finish.side_effect = None
    await runtime.complete_environment(execution, environment, manager, record)
    assert not execution.work.finish.call_args.kwargs["settled"]
    assert residents.run_ended.call_args.kwargs["recycle"]


@pytest.mark.anyio
async def test_preparation_without_client_still_retains_actual_attachment_in_completion():
    execution, environment, manager, record, residents, _order = setup()
    environment.provider = None
    environment.recycle_required = True
    await runtime.complete_environment(execution, environment, manager, record)
    assert residents.run_ended.call_args.args[3] == "d" * 32
    assert residents.run_ended.call_args.kwargs["recycle"]


@pytest.mark.anyio
async def test_repeated_cancel_of_settlement_drains_it_before_unavoidable_run_ended():
    execution, environment, manager, record, residents, order = setup()
    entered, release = asyncio.Event(), asyncio.Event()

    async def finish(*_args, **_kwargs):
        entered.set()
        await release.wait()
        order.append("settled")
        return "succeeded"

    execution.work.finish.side_effect = finish
    task = asyncio.create_task(runtime.complete_environment(execution, environment, manager, record))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        residents.run_ended.assert_not_awaited()
        assert not task.done()
        task.cancel()
        release.set()
        await task
        assert order[-2:] == ["settled", "ended"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_conversation_completion_without_work_still_notifies_registry():
    execution, environment, manager, record, residents, order = setup()
    execution = replace(execution, work=None)
    environment.execution = execution
    await runtime.complete_environment(execution, environment, manager, record)
    assert order == ["close", "ended"]
    manager._store.get.assert_not_awaited()
    assert residents.run_ended.call_args.kwargs["attempt_id"] is None

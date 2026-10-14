"""Captured workspace validation and error mapping, with no native effects."""

import asyncio
import queue
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_agent_execution_runtime import execution as make_execution

from deerflow.agent_instances import runtime
from deerflow.agent_instances.contract import AgentDenied, ResidentBusy
from deerflow.spaces.service import SpaceOperationPending


def scope(*, installed=True, failed=False):
    execution = make_execution()
    attachments = SimpleNamespace(fence_reason=AsyncMock(return_value=None), container_gone=AsyncMock(return_value=False), contain_host=AsyncMock(return_value=False))
    entry = SimpleNamespace(lock=asyncio.Lock(), attachment_id="b" * 32)
    residents = SimpleNamespace(attachments=attachments, entry=Mock(return_value=entry))
    authority = SimpleNamespace(instances=SimpleNamespace(residents=residents if installed else None), validate=AsyncMock())
    execution = replace(execution, authority=authority)
    environment = runtime.ExecutionEnvironment(execution)
    environment.provider = SimpleNamespace(sandbox=SimpleNamespace(id="a" * 32, execution_outcome_uncertain=failed, requires_container_recycle=False))
    return execution, environment, residents


@pytest.mark.anyio
async def test_no_registry_optional_environment_keeps_existing_validation():
    execution, environment, residents = scope(installed=False)
    await execution.validate(environment)
    execution.authority.validate.assert_awaited_once_with(execution)
    residents.attachments.fence_reason.assert_not_awaited()


@pytest.mark.anyio
async def test_captured_attachment_refuses_even_after_registry_replacement():
    execution, environment, residents = scope()
    residents.attachments.fence_reason.return_value = "operator"
    with pytest.raises(AgentDenied, match="closed"):
        await execution.validate(environment)
    residents.attachments.fence_reason.assert_awaited_once_with("a" * 32)
    residents.attachments.contain_host.assert_not_awaited()


@pytest.mark.anyio
async def test_workspace_validation_rejects_mismatched_execution_environment():
    execution, environment, residents = scope()
    environment.execution = make_execution()
    with pytest.raises(AgentDenied):
        await execution.validate(environment)
    residents.attachments.fence_reason.assert_not_awaited()


@pytest.mark.anyio
async def test_uncertain_client_contains_only_after_double_gone_proof_under_home_lock():
    execution, environment, residents = scope(failed=True)
    residents.attachments.container_gone.side_effect = [True, True]
    residents.attachments.fence_reason.side_effect = [None, "container-absent"]

    async def contain(identifier, reason):
        assert residents.entry(execution.instance).lock.locked()
        assert (identifier, reason) == ("a" * 32, "container-absent")
        return False

    residents.attachments.contain_host.side_effect = contain
    with pytest.raises(AgentDenied, match="closed"):
        await execution.validate(environment)
    assert residents.attachments.container_gone.await_count == 2
    residents.attachments.contain_host.assert_awaited_once_with("a" * 32, "container-absent")


@pytest.mark.anyio
async def test_healthy_client_skips_physical_liveness_inspection():
    execution, environment, residents = scope()
    await execution.validate(environment)
    residents.attachments.container_gone.assert_not_awaited()


@pytest.mark.anyio
async def test_child_loop_workspace_decisions_run_on_original_authority_loop():
    execution, environment, residents = scope()
    execution = replace(execution, owner_loop=asyncio.get_running_loop())
    environment.execution = execution
    owner_thread = threading.get_ident()

    async def reason(_identifier):
        assert threading.get_ident() == owner_thread
        return None

    residents.attachments.fence_reason.side_effect = reason
    await asyncio.to_thread(lambda: asyncio.run(execution.validate(environment)))
    residents.attachments.fence_reason.assert_awaited_once_with("a" * 32)


@pytest.mark.anyio
async def test_refusal_mapping_keeps_reason_and_stop_reason():
    execution, _environment, _residents = scope()
    record = SimpleNamespace(run_id="run")
    result = await runtime.run_error(execution, record, ResidentBusy("Worker"))
    assert result == {"name": "ResidentBusy", "reason": "busy", "stop_reason": "busy"}


@pytest.mark.anyio
async def test_error_of_captured_fenced_attachment_reports_interruption_without_new_stop_reason(monkeypatch):
    execution, environment, residents = scope()
    residents.attachments.fence_reason.return_value = "operator"
    monkeypatch.setattr(runtime, "current_environment", lambda: environment)
    result = await runtime.run_error(execution, SimpleNamespace(run_id="run"), AgentDenied("closed"))
    assert result == {"name": "ResidentInterrupted", "reason": "operator", "stop_reason": None}
    residents.attachments.fence_reason.assert_awaited_once_with("a" * 32)


@pytest.mark.anyio
async def test_error_mapping_retains_attachment_before_client_handoff(monkeypatch):
    execution, environment, residents = scope()
    environment.provider = None
    environment.attachment_id = "a" * 32
    residents.attachments.fence_reason.return_value = "start-failed"
    monkeypatch.setattr(runtime, "current_environment", lambda: environment)
    result = await runtime.run_error(execution, SimpleNamespace(run_id="run"), RuntimeError("preparation"))
    assert result == {"name": "ResidentInterrupted", "reason": "start-failed", "stop_reason": None}


@pytest.mark.anyio
async def test_pending_operation_mapping_and_diagnostic_failure_preserve_original_error(monkeypatch):
    execution, environment, residents = scope()
    monkeypatch.setattr(runtime, "current_environment", lambda: environment)
    result = await runtime.run_error(execution, SimpleNamespace(run_id="run"), SpaceOperationPending("busy", cause="lifecycle"))
    assert result == {"name": "ResidentFilesBusy", "reason": "storage-busy", "stop_reason": "storage-busy", "cause": "lifecycle"}
    residents.attachments.fence_reason.side_effect = RuntimeError("unavailable diagnostics")
    result = await runtime.run_error(execution, SimpleNamespace(run_id="run"), AgentDenied("original"))
    assert result == {"name": "AgentDenied", "reason": None, "stop_reason": None}


@pytest.mark.anyio
async def test_unbound_error_mapping_stays_plain():
    assert await runtime.run_error(None, SimpleNamespace(run_id="run"), RuntimeError("plain")) == {"name": "RuntimeError", "reason": None, "stop_reason": None}


@pytest.mark.anyio
async def test_authority_middleware_checks_captured_workspace_before_model_or_tool(monkeypatch):
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agent_instances.middleware import InstanceAuthorityMiddleware

    execution, environment, residents = scope()
    residents.attachments.fence_reason.return_value = "operator"
    monkeypatch.setattr(runtime, "current_environment", lambda: environment)
    request_runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution})
    middleware = InstanceAuthorityMiddleware()
    with pytest.raises(AgentDenied, match="closed"):
        await middleware.abefore_model({}, request_runtime)
    handler = AsyncMock()
    with pytest.raises(AgentDenied, match="closed"):
        await middleware.awrap_tool_call(SimpleNamespace(runtime=request_runtime), handler)
    handler.assert_not_awaited()


@pytest.mark.anyio
async def test_pending_mapping_survives_failed_diagnostics_and_keeps_its_cause(monkeypatch):
    execution, environment, residents = scope()
    monkeypatch.setattr(runtime, "current_environment", lambda: environment)
    residents.attachments.fence_reason.side_effect = RuntimeError("diagnostics unavailable")
    assert await runtime.run_error(execution, SimpleNamespace(run_id="run"), SpaceOperationPending("busy", cause="lifecycle")) == {"name": "ResidentFilesBusy", "reason": "storage-busy", "stop_reason": "storage-busy", "cause": "lifecycle"}


@pytest.mark.parametrize("child_loop", [False, True])
@pytest.mark.anyio
async def test_workspace_validation_drains_held_containment_before_cancellation(child_loop):
    execution, environment, residents = scope(failed=True)
    execution = replace(execution, owner_loop=asyncio.get_running_loop())
    environment.execution = execution
    entered, release = asyncio.Event(), asyncio.Event()
    handles = queue.Queue()
    residents.attachments.container_gone.return_value = True

    async def contain(_identifier, _reason):
        entered.set()
        await release.wait()
        return True

    residents.attachments.contain_host.side_effect = contain
    if child_loop:

        async def child():
            task = asyncio.create_task(execution.validate(environment))
            handles.put((asyncio.get_running_loop(), task))
            await task

        running = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(child())))
    else:
        running = asyncio.create_task(execution.validate(environment))
        handles.put((asyncio.get_running_loop(), running))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        loop, validation = await asyncio.to_thread(handles.get, True, 2)
        loop.call_soon_threadsafe(validation.cancel)
        await asyncio.sleep(0.02)
        assert not running.done() and residents.entry(execution.instance).lock.locked()
        loop.call_soon_threadsafe(validation.cancel)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(running, 2)
        assert not residents.entry(execution.instance).lock.locked()
    finally:
        release.set()
        await asyncio.gather(running, return_exceptions=True)

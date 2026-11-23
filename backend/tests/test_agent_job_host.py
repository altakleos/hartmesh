from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from deerflow_extension_api import AgentRunError
from test_extension_agent_runs import host as host
from test_extension_agent_runs import runtime as runtime

from app.subagent_batches.continuation import BatchContinuations
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.subagents.batch_runtime import BatchSubmitRequest
from deerflow.subagents.batch_service import SubagentBatchService


async def employee_submission(runtime):
    runs, state, http_request, _, _ = runtime
    owner = str(http_request.state.user.id)
    thread = await runs.create_thread(thread_id="agent-job")
    row = await state.thread_store.get(thread, user_id=owner)
    record = await state.run_manager.create_or_reject(thread, "lead_agent", user_id=owner)
    record.metadata["agent_instance_id"] = "employee"
    record.metadata["agent_requester_id"] = owner
    state.agent_conversations = SimpleNamespace(binding=AsyncMock(return_value="employee"))
    controller = BatchContinuations(http_request.app, agent_enabled=True)
    repo = SimpleNamespace(create_batch=AsyncMock(return_value={"id": "job"}))
    service = SubagentBatchService(
        repository=repo, config=SubagentBatchesConfig(enabled=True), runtime_config=SubagentRuntimeConfig(), classify_submission=controller.admit_submission, prepare_continuation=controller.prepare, dispatch_continuation=controller.dispatch
    )
    original = {"thread_incarnation": row["incarnation"], "user_role": "admin", "oauth_id": "invented", "mcp_agent_auth": {"untrusted": True}}
    request = BatchSubmitRequest(
        owner,
        thread,
        record.run_id,
        "call",
        "submission",
        "Compare the results",
        "general-purpose",
        [{"key": "one", "prompt": "Read the local inventory"}],
        None,
        None,
        original,
        continuation="Summarize the inventory",
        continuation_authority=runs,
        continuation_context={"context": {}, "goal": None},
    )
    from deerflow.agent_instances.conversations import AgentExecution

    execution = Mock(
        spec=AgentExecution,
        batch_mode="submit",
        work=None,
        thread_id=thread,
        thread_incarnation=row["incarnation"],
        requester=SimpleNamespace(subject_id=owner),
        instance=SimpleNamespace(id="employee"),
        validate=AsyncMock(),
    )
    return service, repo, replace(request, agent_execution=execution), record, http_request


@pytest.mark.asyncio
async def test_host_stores_only_its_current_bound_session(runtime):
    service, repo, submission, _, http_request = await employee_submission(runtime)
    await service.submit(submission)
    values = repo.create_batch.await_args.kwargs
    spec = values["execution_spec"]
    assert spec["scope"] == {
        "kind": "agent_conversation",
        "version": 1,
        "instance_id": "employee",
        "principal": {"source": "session", "token_version": http_request.state.user.token_version, "system_role": http_request.state.user.system_role, "oauth_provider": None, "oauth_id": None},
    }
    assert "user_role" not in spec and "oauth_id" not in spec and "mcp_agent_auth" not in spec
    assert values["continuation"]["thread_incarnation"] == submission.execution_spec["thread_incarnation"]
    assert "scope" not in submission.execution_spec


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["no-final", "missing-session", "nested", "channel", "goal", "too-many", "wrong-instance", "injected-scope", "stale-thread", "source-ended", "assignment", "schedule", "helper"])
async def test_bound_submission_refuses_before_creating_any_items(runtime, case):
    service, repo, request, record, _ = await employee_submission(runtime)
    if case == "no-final":
        request = replace(request, continuation=None)
    elif case == "helper":
        request.agent_execution.batch_mode = None
    elif case == "missing-session":
        request = replace(request, continuation_authority=None)
    elif case == "nested":
        record.metadata["deerflow_origin"] = {"kind": "subagent_batch"}
    elif case == "channel":
        record.kwargs["channel_execution"] = {"version": 1}
    elif case == "assignment":
        record.metadata["agent_work_attempt_id"] = "attempt"
    elif case == "schedule":
        record.metadata["scheduled_task_id"] = "schedule"
    elif case == "goal":
        request = replace(request, continuation_context={"goal": {"status": "active"}})
    elif case == "too-many":
        request = replace(request, items=request.items * 51)
    elif case == "wrong-instance":
        record.metadata["agent_instance_id"] = "other"
    elif case == "injected-scope":
        request = replace(request, execution_spec={"scope": None})
    elif case == "stale-thread":
        request = replace(request, execution_spec={"thread_incarnation": "stale"})
    elif case == "source-ended":
        from deerflow.runtime.runs.schemas import RunStatus

        record.status = RunStatus.success
    with pytest.raises((AgentRunError, ValueError)):
        await service.submit(request)
    repo.create_batch.assert_not_awaited()


def test_followup_waits_on_the_serialized_assignment_card_until_a_new_user_turn():
    from langchain_core.messages import HumanMessage
    from test_agent_chat_tools import context
    from test_assignment_proposal_runtime import build

    from app.subagent_batches.agent_jobs import waiting_for_response
    from deerflow.runtime.serialization import serialize_channel_values_for_api

    graph, _, _, incoming = build({"shows_cards": ["assignment_card"]})
    result = graph.invoke(incoming, context=context("company"))
    wire = serialize_channel_values_for_api(result)["messages"]
    assert waiting_for_response(wire)
    wire += serialize_channel_values_for_api({"messages": [HumanMessage(content="Answer here")]})["messages"]
    assert not waiting_for_response(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped", [True, False])
async def test_personal_batch_tool_uses_the_target_access_mode(scoped):
    from deerflow.tools.builtins.batch_task_tool import _outside_agent_thread

    submitter = SimpleNamespace(accepts_continuation_authority=True, get_batch=AsyncMock(return_value={"id": "job", "thread_id": "employee", "read_only": scoped}))
    runtime = SimpleNamespace(context={}, config={"configurable": {"thread_id": "my-chat"}})
    assert await _outside_agent_thread(submitter, runtime, "job") == scoped


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "expected"), [("active", "access-lost"), ("suspended", "agent-paused"), ("archived", "agent-archived")])
async def test_refusal_explains_lifecycle_only_with_current_conversation_access(runtime, status, expected):
    from app.subagent_batches.agent_jobs import access_loss_reason
    from deerflow.agent_instances.contract import AgentPermission

    runs, state, _, _, _ = runtime
    thread = await runs.create_thread(thread_id="lifecycle")
    view = SimpleNamespace(status=status)
    authority = SimpleNamespace(binding=AsyncMock(return_value="employee"), allowed=AsyncMock(return_value=True), instances=SimpleNamespace(get=AsyncMock(return_value=view)))
    assert await access_loss_reason(runs, authority, thread, "employee") == expected
    assert {c.kwargs["permission"] for c in authority.allowed.await_args_list} == {AgentPermission.USE, AgentPermission.INSPECT}
    authority.allowed.return_value = False
    authority.instances.get.reset_mock()
    assert await access_loss_reason(runs, authority, thread, "employee") == "access-lost"
    authority.instances.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_agent_job_page_preserves_personal_continuations_and_item_polling(runtime):
    import asyncio
    from datetime import UTC, datetime

    service, repo, _, _, http = await employee_submission(runtime)
    host = BatchContinuations(http.app, agent_enabled=True)
    http.app.state.subagent_batch_repo = repo
    repo.list_bound_jobs = AsyncMock(side_effect=RuntimeError("unavailable"))
    repo.claim_continuations = AsyncMock(return_value=[{"id": "personal"}])
    repo.claim_items = AsyncMock(return_value=[])
    service._continue_batch = AsyncMock()
    service._claim_agent_jobs = host.claim_agent_jobs
    await service.run_once(now=datetime.now(UTC))
    await asyncio.gather(*service._continuations.values())
    assert service._continue_batch.await_args.args[0]["id"] == "personal"
    repo.claim_items.assert_awaited_once()


def test_live_agent_and_final_tool_inventories_accept_iterables():
    from deerflow.agent_instances.runtime import instance_tools
    from deerflow.tools.builtins.batch_task_tool import batch_status, batch_task, cancel_batch, read_batch_result

    inventory = (batch_status, batch_task, cancel_batch, read_batch_result)
    assert instance_tools(iter(inventory), batch_mode="submit") == list(inventory)
    assert instance_tools(iter(inventory), batch_mode="results") == [batch_status, read_batch_result]
    assert instance_tools(inventory) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("values", "expected"), [({}, ("dispatched", None)), ({"goal": {"status": "active"}}, ("ended", "goal-active"))])
async def test_bound_dispatch_carries_only_the_claimed_thread_generation(runtime, values, expected):
    from app.subagent_batches.agent_jobs import dispatch_agent_job, require_job_execution
    from deerflow.subagents.batch_runtime import BATCH_CONTINUATION_NAMESPACE, batch_continuation_key

    service, repo, request, _, http = await employee_submission(runtime)
    await service.submit(request)
    stored = repo.create_batch.await_args.kwargs
    key = batch_continuation_key("job")
    batch = {**stored, "id": "job", "idempotency_key": key, "thread_generation": "observed-generation", "execution_fence": {"namespace": BATCH_CONTINUATION_NAMESPACE, "resource_id": "job", "holder_id": "host", "epoch": 1}}
    operations = SimpleNamespace(get_state=AsyncMock(return_value={"values": values}), start_batch_followup=AsyncMock())
    http.app.state.agent_runs_host.restore_batch_authority = AsyncMock(return_value=operations)
    assert await dispatch_agent_job(http.app, batch) == expected
    if expected[0] == "ended":
        operations.start_batch_followup.assert_not_awaited()
        return
    admitted = operations.start_batch_followup.await_args.kwargs
    fence = admitted["execution_fence"]
    assert fence.thread_generation == batch["thread_generation"]
    assert admitted["thread_id"] == request.thread_id and admitted["idempotency_key"] == key
    http.state.extension_execution_fence = fence
    assert require_job_execution(http, request.agent_execution, origin={"kind": "subagent_batch"}, thread_id=request.thread_id, idempotency_key=key) is fence

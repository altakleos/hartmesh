"""Durable followups retain their originating chat and principal boundaries."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import AgentRunError
from test_extension_agent_runs import host as host
from test_extension_agent_runs import runtime as runtime


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["promotion", "demotion", "oauth", "disabled", "released", "token"])
async def test_saved_batch_grant_refuses_changed_principal(host, change):
    from datetime import UTC, datetime

    control, request, load = host
    if change == "demotion":
        request.state.user = request.state.user.model_copy(update={"system_role": "admin"})
        request.state.auth.user = request.state.user
    runs = control.bind(request)
    grant = control.capture_batch_authority(runs, user_id=str(request.state.user.id))
    changes = {
        "promotion": {"system_role": "admin"},
        "demotion": {"system_role": "user"},
        "oauth": {"oauth_provider": "changed", "oauth_id": "new-account"},
        "disabled": {"disabled_at": datetime.now(UTC)},
        "released": {"released_at": datetime.now(UTC)},
        "token": {"token_version": request.state.user.token_version + 1},
    }
    load.return_value = request.state.user.model_copy(update=changes[change])
    with pytest.raises(AgentRunError) as refused:
        await control.restore_batch_authority(user_id=str(request.state.user.id), grant=grant)
    assert refused.value.status_code == 403


@pytest.mark.asyncio
async def test_restored_batch_rechecks_principal_and_denies_management(host):
    from app.gateway.customer_administration import request_customer_administration_policy, resolve_customer_management_actor
    from deerflow.runtime.customer_administration import DENIED_CUSTOMER_ADMINISTRATION, CustomerManagementActor

    control, request, load = host
    runs = control.bind(request)
    owner = str(request.state.user.id)
    grant = control.capture_batch_authority(runs, user_id=owner)
    restored = await control.restore_batch_authority(user_id=owner, grant=grant)
    async with restored._request("runs:create") as followup:
        assert request_customer_administration_policy(followup) == DENIED_CUSTOMER_ADMINISTRATION
        assert await resolve_customer_management_actor(followup) == CustomerManagementActor()
        descendant = control.bind(followup).for_plugin("fixture")
        async with descendant._request("runs:create") as child:
            assert child.state.run_origin == {"kind": "extension", "namespace": "fixture"}
            assert request_customer_administration_policy(child) == DENIED_CUSTOMER_ADMINISTRATION
            assert await resolve_customer_management_actor(child) == CustomerManagementActor()
            grandchild = control.bind(child).for_plugin("nested")
            async with grandchild._request("runs:create") as nested:
                assert request_customer_administration_policy(nested) == DENIED_CUSTOMER_ADMINISTRATION
                assert await resolve_customer_management_actor(nested) == CustomerManagementActor()
            assert control.capture_batch_authority(grandchild, user_id=owner)["durable_followup"] is True
    load.return_value = request.state.user.model_copy(update={"system_role": "admin"})
    with pytest.raises(AgentRunError):
        async with restored._request("runs:create"):
            pytest.fail("A promotion after restoration must not expand automatic authority")


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["work", "instance", "capture", "channel", "scheduled", "late-instance"])
async def test_unsupported_followup_refused_before_batch_persistence(runtime, scope):
    from app.subagent_batches.continuation import BatchContinuations
    from deerflow.config.subagent_batches_config import SubagentBatchesConfig
    from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
    from deerflow.subagents.batch_runtime import BatchSubmitRequest
    from deerflow.subagents.batch_service import SubagentBatchService

    runs, state, request, _, _ = runtime
    owner = str(request.state.user.id)
    thread = await runs.create_thread(thread_id="scope-boundary")
    record = await state.run_manager.create_or_reject(thread, "lead_agent", user_id=owner)
    if scope == "work":
        record.metadata["agent_work_attempt_id"] = "attempt"
    elif scope == "instance":
        record.metadata["agent_instance_id"] = "instance"
    elif scope == "capture":
        record.kwargs["output_capture"] = {"version": 1}
    elif scope == "channel":
        record.kwargs["channel_execution"] = {"version": 1}
    elif scope == "scheduled":
        record.metadata["deerflow_origin"] = {"kind": "schedule"}
    else:
        state.agent_conversations = SimpleNamespace(binding=AsyncMock(return_value="instance"))
    repo = SimpleNamespace(create_batch=AsyncMock())
    controller = BatchContinuations(request.app)
    service = SubagentBatchService(repository=repo, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), prepare_continuation=controller.prepare, dispatch_continuation=controller.dispatch)
    submission = BatchSubmitRequest(owner, thread, record.run_id, "tool", "submission", "Read records", "general-purpose", [{"key": "one", "prompt": "Read source"}], 1, 1, {}, continuation="Combine results", continuation_authority=runs)
    with pytest.raises(AgentRunError) as refused:
        await service.submit(submission)
    assert refused.value.status_code == 422
    repo.create_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_refusal_suppresses_followup_before_dispatch():
    from deerflow.config.subagent_batches_config import SubagentBatchesConfig
    from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
    from deerflow.subagents.batch_service import SubagentBatchService

    repo = SimpleNamespace(finish_continuation=AsyncMock(return_value=True))
    dispatch = AsyncMock()
    service = SubagentBatchService(repository=repo, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), owner_access=AsyncMock(return_value=False), dispatch_continuation=dispatch)
    await service._continue_batch({"id": "batch"}, "worker")
    dispatch.assert_not_awaited()
    repo.finish_continuation.assert_awaited_once_with("batch", lease_owner="worker", status="suppressed", run_id=None)

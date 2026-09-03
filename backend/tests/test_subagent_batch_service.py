import asyncio
from dataclasses import replace
from enum import Enum
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _subagent_batch_helpers import (
    make_claimed_item,
    make_parent_batch_request,
)

from deerflow.config.app_config import AppConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.runtime.skill_projection import SkillProjectionConsumerToken
from deerflow.runtime.tool_evidence import (
    NullDurableToolReceiptSink,
    ToolReceiptOwnershipLost,
    active_tool_receipt_context,
)
from deerflow.subagents import batch_service as service_module
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchV1,
    BatchAdmissionError,
    BatchItemRequestV1,
)
from deerflow.subagents.batch_service import SubagentBatchService
from deerflow.subagents.capacity import SubagentExecutionCapacity


class FakeStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in {FakeStatus.COMPLETED, FakeStatus.FAILED}


def _app_config():
    return SimpleNamespace(get_model_config=lambda _name: {})


async def _accept_active(service, request):
    with active_tool_receipt_context(request.parent_tool_receipt):
        return await service.accept(request)


@pytest.mark.asyncio
async def test_accept_requires_the_live_parent_tool_receipt_scope() -> None:
    repository = SimpleNamespace(accept_batch=AsyncMock(return_value={"id": "batch-1"}))
    request = make_parent_batch_request(app_config=_app_config())
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    with pytest.raises(BatchAdmissionError, match="tool_attempt_not_active"):
        await service.accept(request)

    repository.accept_batch.assert_not_awaited()


def test_live_capability_manifest_drift_fails_closed() -> None:
    request = make_parent_batch_request(app_config=_app_config())
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="batch-capability-drift",
    )
    service = SubagentBatchService(
        repository=SimpleNamespace(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        capability_manifest_digest="f" * 64,
    )

    with pytest.raises(BatchAdmissionError, match="provider_not_qualified"):
        service._validated_extensions(accepted, {})


def test_live_app_execution_policy_drift_fails_closed() -> None:
    accepted_app = AppConfig(sandbox=SandboxConfig(use="test"))
    changed_app = accepted_app.model_copy(update={"authorization": accepted_app.authorization.model_copy(update={"fail_closed": not accepted_app.authorization.fail_closed})})
    execution = SimpleNamespace(model_profile={"app_execution_digest": service_module.app_config_execution_digest(accepted_app)})
    service = SubagentBatchService(
        repository=SimpleNamespace(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    service._validate_app_execution_digest(execution, accepted_app)
    with pytest.raises(BatchAdmissionError, match="provider_not_qualified"):
        service._validate_app_execution_digest(execution, changed_app)


@pytest.mark.asyncio
async def test_accept_revalidates_the_active_parent_receipt_fence() -> None:
    class StaleParentSink(NullDurableToolReceiptSink):
        async def record_started(self, _receipt) -> None:
            raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")

    repository = SimpleNamespace(accept_batch=AsyncMock())
    request = replace(
        make_parent_batch_request(app_config=_app_config()),
        parent_tool_receipt_sink=StaleParentSink(),
    )
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    with pytest.raises(BatchAdmissionError, match="tool_attempt_not_active"):
        await _accept_active(service, request)

    repository.accept_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_accept_rejects_nondurable_parent_receipt_sink() -> None:
    repository = SimpleNamespace(accept_batch=AsyncMock())
    request = replace(
        make_parent_batch_request(app_config=_app_config()),
        parent_tool_receipt_sink=NullDurableToolReceiptSink(),
    )
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    with pytest.raises(BatchAdmissionError, match="tool_attempt_not_active"):
        await _accept_active(service, request)

    repository.accept_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_keeps_batch_running_limit_separate_from_one_process_capacity() -> None:
    repository = SimpleNamespace(accept_batch=AsyncMock(return_value={"id": "batch-1"}))
    request = make_parent_batch_request(app_config=_app_config())
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(max_running_items_per_batch=32),
        runtime_config=SubagentRuntimeConfig(max_running=3),
    )

    result = await _accept_active(service, request)

    assert result == {"id": "batch-1"}
    accepted = repository.accept_batch.await_args.kwargs["accepted"]
    assert accepted.limits.max_running_items == 10


@pytest.mark.asyncio
async def test_idempotent_accept_keeps_the_original_process_material_lease(
    monkeypatch,
) -> None:
    class Lease:
        def __init__(self, ordinal: int) -> None:
            self.ordinal = ordinal
            self.released = False

        def release_process_material(self) -> None:
            self.released = True

    leases: list[Lease] = []

    def retain_process_material(_material) -> Lease:
        lease = Lease(len(leases) + 1)
        leases.append(lease)
        return lease

    monkeypatch.setattr(
        service_module.ResolvedAgentMaterialV1,
        "retain_process_material",
        retain_process_material,
    )
    repository = SimpleNamespace(
        accept_batch=AsyncMock(side_effect=lambda **kwargs: {"id": kwargs["accepted"].batch_id}),
    )
    request = make_parent_batch_request(app_config=_app_config())
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    first = await _accept_active(service, request)
    service._item_batches["active-item"] = first["id"]
    await _accept_active(service, request)

    assert len(leases) == 1
    assert service._accepted_material[first["id"]] is leases[0]
    assert leases[0].released is False


@pytest.mark.asyncio
async def test_accept_installs_process_material_before_committing_claimable_rows() -> None:
    request = make_parent_batch_request(app_config=_app_config())
    service = None

    class Repository:
        async def accept_batch(self, **kwargs):
            batch_id = kwargs["accepted"].batch_id
            assert service is not None
            assert batch_id in service._accepted_material
            assert service._batch_owners[batch_id] == request.user_id
            return {"id": batch_id, "status": "queued"}

    service = SubagentBatchService(
        repository=Repository(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    await _accept_active(service, request)


@pytest.mark.asyncio
async def test_terminal_pruning_waits_for_batch_acceptance_outcome() -> None:
    request = make_parent_batch_request(app_config=_app_config())
    acceptance_started = asyncio.Event()
    allow_commit = asyncio.Event()
    committed = False

    class Repository:
        async def accept_batch(self, **kwargs):
            nonlocal committed
            acceptance_started.set()
            await allow_commit.wait()
            committed = True
            return {
                "id": kwargs["accepted"].batch_id,
                "status": "queued",
            }

        async def get_batch(self, batch_id: str, *, user_id: str):
            assert user_id == request.user_id
            return {"id": batch_id, "status": "queued"} if committed else None

    service = SubagentBatchService(
        repository=Repository(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    acceptance_task = asyncio.create_task(_accept_active(service, request))
    await acceptance_started.wait()
    prune_task = asyncio.create_task(service._prune_terminal_material())
    await asyncio.sleep(0)
    allow_commit.set()
    batch = await acceptance_task
    await prune_task

    assert batch["id"] in service._accepted_material


@pytest.mark.asyncio
async def test_accept_retains_skill_projection_until_batch_material_release(
    monkeypatch,
) -> None:
    parent_token = SkillProjectionConsumerToken(
        user_id="user-1",
        thread_id="thread-1",
        sandbox_id="sandbox-1",
        run_id="run-1",
        generation=1,
        consumer_id="lead",
        snapshot_id=None,
    )
    retained_token = replace(
        parent_token,
        consumer_id="subagent-batch:retained",
    )
    released: list[object] = []

    class Coordinator:
        def retain(self, token, *, consumer_id: str):
            assert token is parent_token
            assert consumer_id.startswith("subagent-batch:")
            return retained_token

    monkeypatch.setattr(
        service_module,
        "get_skill_projection_coordinator",
        lambda: Coordinator(),
        raising=False,
    )
    monkeypatch.setattr(
        service_module,
        "release_accepted_skill_consumer",
        released.append,
        raising=False,
    )
    repository = SimpleNamespace(
        accept_batch=AsyncMock(
            side_effect=lambda **kwargs: {
                "id": kwargs["accepted"].batch_id,
                "status": "queued",
            }
        ),
    )
    request = replace(
        make_parent_batch_request(app_config=_app_config()),
        skill_projection_token=parent_token,
    )
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    batch = await _accept_active(service, request)

    assert service._runtime_adapters[batch["id"]]["skill_projection_token"] is retained_token
    await service._release_batch_material(batch["id"])
    assert released == [retained_token]


@pytest.mark.asyncio
async def test_material_cleanup_retry_does_not_release_skill_projection_twice(
    monkeypatch,
) -> None:
    token = SkillProjectionConsumerToken(
        user_id="user-1",
        thread_id="thread-1",
        sandbox_id="sandbox-1",
        run_id="run-1",
        generation=1,
        consumer_id="subagent-batch:batch-1",
        snapshot_id=None,
    )
    released: list[object] = []

    class Lease:
        attempts = 0

        def release_process_material(self) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("transient cleanup failure")

    monkeypatch.setattr(
        service_module,
        "release_accepted_skill_consumer",
        released.append,
    )
    lease = Lease()
    service = SubagentBatchService(
        repository=SimpleNamespace(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )
    service._accepted_material["batch-1"] = lease
    service._runtime_adapters["batch-1"] = {"skill_projection_token": token}
    service._batch_owners["batch-1"] = "user-1"

    await service._release_batch_material("batch-1")
    await service._release_batch_material("batch-1")

    assert released == [token]
    assert lease.attempts == 2
    assert "batch-1" not in service._accepted_material
    assert "batch-1" not in service._runtime_adapters


@pytest.mark.asyncio
async def test_scheduler_releases_process_material_for_remotely_terminal_batch() -> None:
    class Lease:
        released = False

        def release_process_material(self) -> None:
            self.released = True

    class Repository:
        async def get_batch(self, batch_id: str, *, user_id: str):
            assert batch_id == "batch-1"
            assert user_id == "user-1"
            return {"id": batch_id, "status": "completed"}

        async def claim_items(self, **_kwargs):
            return []

    lease = Lease()
    service = SubagentBatchService(
        repository=Repository(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )
    service._accepted_material["batch-1"] = lease
    service._runtime_adapters["batch-1"] = {"private": object()}
    service._batch_owners["batch-1"] = "user-1"

    await service.run_once(now=service_module.datetime.now(service_module.UTC))

    assert lease.released is True
    assert "batch-1" not in service._accepted_material
    assert "batch-1" not in service._runtime_adapters
    assert "batch-1" not in service._batch_owners


@pytest.mark.asyncio
async def test_execute_item_marks_real_running_then_persists_terminal_result(monkeypatch) -> None:
    result = SimpleNamespace(
        status=FakeStatus.RUNNING,
        result=None,
        error=None,
        stop_reason=None,
        token_usage_records=None,
    )

    class Repository:
        def __init__(self) -> None:
            self.marked_running = False
            self.finalized = None
            self.request = make_parent_batch_request(app_config=_app_config())

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(self.request)]

        async def mark_item_running(self, *_args, **_kwargs):
            self.marked_running = True
            result.status = FakeStatus.COMPLETED
            result.result = "done"
            return True

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    execution_capacity = SubagentExecutionCapacity(SubagentRuntimeConfig(max_running=1))
    executor_kwargs = {}

    class Executor:
        def __init__(self, **kwargs) -> None:
            executor_kwargs.update(kwargs)

        def execute_async(self, _prompt, task_id=None):
            assert task_id.startswith("bi_")
            asyncio.create_task(executor_kwargs["execution_admitted_callback"]())
            return "execution-1"

    repository = Repository()
    monkeypatch.setattr(service_module, "resolve_subagent_model_name", lambda *_args, **_kwargs: "model-a")
    monkeypatch.setattr(service_module, "SubagentExecutor", Executor)
    monkeypatch.setattr(service_module, "SubagentStatus", FakeStatus)
    monkeypatch.setattr(service_module, "get_background_task_result", lambda _execution_id: result)
    monkeypatch.setattr(service_module, "cleanup_background_task", lambda _execution_id: None)
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        execution_capacity=execution_capacity,
        app_config=_app_config(),
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.gather(*list(service._executions.values()))

    assert repository.marked_running is True
    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is True
    assert repository.finalized["result"] == "done"
    assert executor_kwargs["execution_capacity"] is execution_capacity
    restored_material = executor_kwargs["resolved_agent_material"]
    accepted = make_claimed_item(repository.request)["batch"]["acceptance"]
    assert restored_material.subagent_catalog.digest == (accepted.subagent_catalog_digest)


@pytest.mark.asyncio
async def test_execute_item_polls_completion_without_waiting_for_lease_renewal(monkeypatch) -> None:
    result = SimpleNamespace(
        status=FakeStatus.PENDING,
        result=None,
        error=None,
        stop_reason=None,
        token_usage_records=None,
    )
    reads = 0

    class Repository:
        def __init__(self) -> None:
            self.finalized = None
            self.request = make_parent_batch_request(app_config=_app_config())

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(self.request)]

        async def mark_item_running(self, *_args, **_kwargs):
            return True

        async def renew_item_lease(self, *_args, **_kwargs):
            raise AssertionError("short completion must not wait for lease renewal")

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    callback = None

    class Executor:
        def __init__(self, **kwargs) -> None:
            nonlocal callback
            callback = kwargs["execution_admitted_callback"]

        def execute_async(self, _prompt, task_id=None):
            assert task_id.startswith("bi_")
            assert callback is not None
            asyncio.create_task(callback())
            return "execution-1"

    def read_result(_execution_id):
        nonlocal reads
        reads += 1
        if reads > 1:
            result.status = FakeStatus.COMPLETED
            result.result = "fast result"
        return result

    repository = Repository()
    monkeypatch.setattr(service_module, "resolve_subagent_model_name", lambda *_args, **_kwargs: "model-a")
    monkeypatch.setattr(service_module, "SubagentExecutor", Executor)
    monkeypatch.setattr(service_module, "SubagentStatus", FakeStatus)
    monkeypatch.setattr(service_module, "get_background_task_result", read_result)
    monkeypatch.setattr(service_module, "cleanup_background_task", lambda _execution_id: None)
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(poll_interval_seconds=0.1, lease_seconds=120),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.wait_for(
        asyncio.gather(*list(service._executions.values())),
        timeout=1,
    )

    assert repository.finalized is not None
    assert repository.finalized["result"] == "fast result"


@pytest.mark.asyncio
async def test_executor_admission_failure_requeues_instead_of_finalizing(monkeypatch) -> None:
    result = SimpleNamespace(
        status=FakeStatus.FAILED,
        result=None,
        error="Process-wide subagent capacity is full",
        stop_reason=None,
        token_usage_records=None,
        admission_failure=True,
    )

    class Repository:
        def __init__(self) -> None:
            self.requeued = None
            self.finalized = False
            self.request = make_parent_batch_request(app_config=_app_config())

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(self.request)]

        async def requeue_item_after_admission_failure(self, item_id, **kwargs):
            self.requeued = (item_id, kwargs)
            return True

        async def finalize_item(self, *_args, **_kwargs):
            self.finalized = True
            return True

    class Executor:
        def __init__(self, **_kwargs) -> None:
            pass

        def execute_async(self, _prompt, task_id=None):
            assert task_id.startswith("bi_")
            return "execution-1"

    repository = Repository()
    monkeypatch.setattr(service_module, "resolve_subagent_model_name", lambda *_args, **_kwargs: "model-a")
    monkeypatch.setattr(service_module, "SubagentExecutor", Executor)
    monkeypatch.setattr(service_module, "SubagentStatus", FakeStatus)
    monkeypatch.setattr(service_module, "get_background_task_result", lambda _execution_id: result)
    monkeypatch.setattr(service_module, "cleanup_background_task", lambda _execution_id: None)
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.gather(*list(service._executions.values()))

    assert repository.requeued is not None
    assert repository.requeued[0].startswith("bi_")
    assert repository.requeued[1]["error"] == "queue_rejected"
    assert "capacity is full" not in str(repository.requeued)
    assert repository.finalized is False


@pytest.mark.asyncio
async def test_executor_failure_persists_only_safe_status_codes(monkeypatch) -> None:
    result = SimpleNamespace(
        status=FakeStatus.PENDING,
        result=None,
        error="provider secret: credential-shaped diagnostic",
        stop_reason="provider secret stop detail",
        token_usage_records=None,
        admission_failure=False,
    )
    request = make_parent_batch_request(app_config=_app_config())

    class Repository:
        finalized = None

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(request)]

        async def mark_item_running(self, *_args, **_kwargs):
            result.status = FakeStatus.FAILED
            return True

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    callback = None

    class Executor:
        def __init__(self, **kwargs) -> None:
            nonlocal callback
            callback = kwargs["execution_admitted_callback"]

        def execute_async(self, _prompt, task_id=None):
            assert task_id.startswith("bi_")
            assert callback is not None
            asyncio.create_task(callback())
            return "execution-1"

    repository = Repository()
    monkeypatch.setattr(
        service_module,
        "resolve_subagent_model_name",
        lambda *_args, **_kwargs: "model-a",
    )
    monkeypatch.setattr(service_module, "SubagentExecutor", Executor)
    monkeypatch.setattr(service_module, "SubagentStatus", FakeStatus)
    monkeypatch.setattr(
        service_module,
        "get_background_task_result",
        lambda _execution_id: result,
    )
    monkeypatch.setattr(
        service_module,
        "cleanup_background_task",
        lambda _execution_id: None,
    )
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(poll_interval_seconds=0.1),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.gather(*list(service._executions.values()))

    assert repository.finalized is not None
    assert repository.finalized["error"] == "execution_failed"
    assert repository.finalized["stop_reason"] is None
    assert "provider secret" not in str(repository.finalized)


@pytest.mark.asyncio
async def test_accept_rejects_acceptance_over_configured_evidence_bound() -> None:
    repository = SimpleNamespace(accept_batch=AsyncMock())
    request = make_parent_batch_request(
        app_config=_app_config(),
        items=tuple(
            BatchItemRequestV1(
                key=f"record-{index}",
                prompt=f"private input {index}",
            )
            for index in range(20)
        ),
    )
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(
            max_items_per_batch=100,
            max_evidence_bytes=1_024,
        ),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    with pytest.raises(BatchAdmissionError, match="batch_acceptance_too_large"):
        await _accept_active(service, request)

    repository.accept_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_result_over_accepted_limit_fails_without_persisting_payload(
    monkeypatch,
) -> None:
    result = SimpleNamespace(
        status=FakeStatus.PENDING,
        result=None,
        error=None,
        stop_reason=None,
        token_usage_records=None,
    )
    request = make_parent_batch_request(app_config=_app_config())
    request = replace(
        request,
        limits=replace(request.limits, max_result_chars=5),
    )

    class Repository:
        finalized = None

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(request)]

        async def mark_item_running(self, *_args, **_kwargs):
            return True

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    callback = None

    class Executor:
        def __init__(self, **kwargs) -> None:
            nonlocal callback
            callback = kwargs["execution_admitted_callback"]

        def execute_async(self, _prompt, task_id=None):
            async def complete() -> None:
                assert callback is not None
                await callback()
                result.status = FakeStatus.COMPLETED
                result.result = "private-result-that-is-too-large"

            asyncio.create_task(complete())
            return "execution-1"

    repository = Repository()
    monkeypatch.setattr(
        service_module,
        "resolve_subagent_model_name",
        lambda *_args, **_kwargs: "model-a",
    )
    monkeypatch.setattr(service_module, "SubagentExecutor", Executor)
    monkeypatch.setattr(service_module, "SubagentStatus", FakeStatus)
    monkeypatch.setattr(
        service_module,
        "get_background_task_result",
        lambda _execution_id: result,
    )
    monkeypatch.setattr(
        service_module,
        "cleanup_background_task",
        lambda _execution_id: None,
    )
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(poll_interval_seconds=0.1),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.gather(*list(service._executions.values()))

    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is False
    assert repository.finalized["terminal_code"] == "result_too_large"
    assert repository.finalized["result"] is None
    assert repository.finalized["result_preview"] is None
    assert "private-result" not in str(repository.finalized)


@pytest.mark.asyncio
async def test_live_model_setting_drift_fails_closed_as_unqualified() -> None:
    class DriftedAppConfig:
        def get_model_config(self, _name):
            return {"temperature": 0.9}

    request = make_parent_batch_request(app_config=DriftedAppConfig())

    class Repository:
        finalized = None

        async def claim_items(self, **_kwargs):
            return [make_claimed_item(request)]

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    repository = Repository()
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=request.app_config,
    )

    await service.run_once(now=service_module.datetime.now(service_module.UTC))
    await asyncio.gather(*list(service._executions.values()))

    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is False
    assert repository.finalized["terminal_code"] == "provider_not_qualified"

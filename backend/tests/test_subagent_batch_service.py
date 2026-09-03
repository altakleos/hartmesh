import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from enum import Enum
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from _subagent_batch_helpers import (
    make_claimed_item,
    make_parent_batch_request,
)
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.config.app_config import AppConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.persistence.base import Base
from deerflow.persistence.subagent_batches import (
    SubagentBatchItemRow,
    SubagentBatchRepository,
)
from deerflow.runtime.skill_projection import SkillProjectionConsumerToken
from deerflow.runtime.subagent_snapshot import resolved_subagent_definition
from deerflow.runtime.tool_evidence import (
    NullDurableToolReceiptSink,
    ToolReceiptOwnershipLost,
    active_tool_receipt_context,
)
from deerflow.subagents import batch_service as service_module
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchV1,
    BatchAdmissionError,
    BatchCancelled,
    BatchItemRequestV1,
    BatchStaleAttempt,
    ParentBoundBatchExecutionV1,
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


def _install_successful_executor(monkeypatch, captured: dict[str, object]) -> None:
    result = SimpleNamespace(
        status=FakeStatus.PENDING,
        result=None,
        error=None,
        stop_reason=None,
        token_usage_records=None,
    )

    class Executor:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def execute_async(self, _prompt, task_id=None):
            async def complete() -> None:
                callback = captured["execution_admitted_callback"]
                await callback()
                result.status = FakeStatus.COMPLETED
                result.result = "accepted result"

            asyncio.create_task(complete())
            return "execution-1"

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


def _persisted_claimed_item(request) -> dict:
    item = make_claimed_item(request)
    accepted = item["batch"]["acceptance"]
    execution = item["batch"]["execution"]
    item["batch"]["acceptance"] = AcceptedBatchV1.from_persisted_json(accepted.to_persisted_json())
    item["batch"]["execution"] = ParentBoundBatchExecutionV1.from_persisted_json(execution.to_persisted_json())
    return item


class _TerminalRepository:
    def __init__(self) -> None:
        self.finalized = None

    async def mark_item_running(self, *_args, **_kwargs):
        return True

    async def finalize_item(self, *_args, **kwargs):
        self.finalized = kwargs
        return True


class _SkillProjection:
    name = "worker-skill"
    content_digest = "d" * 64

    @staticmethod
    def to_json() -> dict[str, object]:
        return {
            "name": "worker-skill",
            "content_digest": "d" * 64,
        }


class _SkillSnapshot:
    snapshot_id = "e" * 64
    content_digest = "e" * 64
    skills = (SimpleNamespace(name="worker-skill"),)
    projections = (_SkillProjection(),)
    file_count = 1
    total_bytes = 1

    def __init__(self) -> None:
        self.drifted = False
        self.verify_calls = 0

    def verify(self) -> None:
        self.verify_calls += 1
        if self.drifted:
            raise RuntimeError("changed accepted skill bytes")

    def retain(self):
        return self

    def release(self) -> None:
        return None


def _skill_bound_request(
    snapshot: _SkillSnapshot,
    *,
    tool_plane_revision=None,
):
    definition = resolved_subagent_definition(
        name="general-purpose",
        source_kind="builtin",
        source_version="skill-bound-v1",
        description="Skill-bound worker",
        system_prompt="Use only accepted skills.",
        model=None,
        model_settings={},
        tool_names=(),
        skill_names=("worker-skill",),
        max_turns=20,
        timeout_seconds=300,
    )
    return make_parent_batch_request(
        app_config=_app_config(),
        definition=definition,
        skill_snapshot=snapshot,
        skill_scope_digests=("d" * 64,),
        tool_plane_revision=tool_plane_revision,
    )


async def _accept_active(service, request):
    with active_tool_receipt_context(request.parent_tool_receipt):
        return await service.accept(request)


@pytest.mark.asyncio
async def test_rejected_attempt_mutation_distinguishes_cancellation_from_stale_fence() -> None:
    repository = SimpleNamespace(
        get_batch=AsyncMock(
            side_effect=[
                {"status": "cancelled"},
                {"status": "running"},
            ]
        )
    )
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )

    cancelled = await service._attempt_mutation_rejection(
        batch_id="batch-1",
        user_id="user-1",
    )
    stale = await service._attempt_mutation_rejection(
        batch_id="batch-1",
        user_id="user-1",
    )

    assert isinstance(cancelled, BatchCancelled)
    assert isinstance(stale, BatchStaleAttempt)


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
async def test_restart_reuses_persisted_catalog_instead_of_live_catalog(
    monkeypatch,
) -> None:
    from deerflow.runtime import subagent_snapshot as snapshot_module

    request = make_parent_batch_request(app_config=_app_config())
    item = _persisted_claimed_item(request)
    changed_definition = resolved_subagent_definition(
        name="general-purpose",
        source_kind="managed",
        source_version="changed-live-v2",
        description="Changed live definition",
        system_prompt="MUTATED LIVE CATALOG PROMPT",
        model=None,
        model_settings={},
        tool_names=(),
        skill_names=(),
        max_turns=1,
        timeout_seconds=1,
    )
    live_resolver = MagicMock(return_value=changed_definition)
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_effective_subagents",
        live_resolver,
    )
    captured: dict[str, object] = {}
    _install_successful_executor(monkeypatch, captured)

    class Repository:
        finalized = None

        async def mark_item_running(self, *_args, **_kwargs):
            return True

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    repository = Repository()
    restarted_service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(poll_interval_seconds=0.1),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await restarted_service._execute_item(item)

    live_resolver.assert_not_called()
    assert captured["config"].system_prompt == "Work carefully."
    material = captured["resolved_agent_material"]
    assert material.subagent_catalog.digest == item["batch"]["acceptance"].subagent_catalog_digest
    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is True


@pytest.mark.asyncio
async def test_restart_does_not_replace_unavailable_accepted_skill_with_live_skill(
    monkeypatch,
) -> None:
    from deerflow.skills import storage as skill_storage_module

    request = _skill_bound_request(_SkillSnapshot())
    item = _persisted_claimed_item(request)
    live_storage = MagicMock()
    monkeypatch.setattr(
        skill_storage_module,
        "get_or_new_user_skill_storage",
        live_storage,
    )
    repository = _TerminalRepository()
    restarted_service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await restarted_service._execute_item(item)

    live_storage.assert_not_called()
    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is False
    assert repository.finalized["terminal_code"] == "execution_material_unavailable"


@pytest.mark.asyncio
async def test_retained_accepted_skill_drift_fails_closed_without_live_fallback(
    monkeypatch,
) -> None:
    from deerflow.skills import storage as skill_storage_module

    snapshot = _SkillSnapshot()
    request = _skill_bound_request(snapshot)
    item = _persisted_claimed_item(request)
    live_storage = MagicMock()
    monkeypatch.setattr(
        skill_storage_module,
        "get_or_new_user_skill_storage",
        live_storage,
    )
    repository = _TerminalRepository()
    service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )
    service._accepted_material[item["batch"]["id"]] = request.resolved_parent_material
    snapshot.drifted = True

    await service._execute_item(item)

    live_storage.assert_not_called()
    assert snapshot.verify_calls == 1
    assert repository.finalized is not None
    assert repository.finalized["terminal_code"] == "execution_material_unavailable"


@pytest.mark.parametrize(
    ("drift_field", "drift_value"),
    [
        ("generation", 8),
        ("artifact_manifest_digest", "sha256:" + "8" * 64),
        ("extension_configuration_digest", "sha256:" + "9" * 64),
    ],
)
@pytest.mark.asyncio
async def test_restart_fails_closed_on_live_extension_anchor_drift(
    monkeypatch,
    drift_field: str,
    drift_value: object,
) -> None:
    manifest_digest = "a" * 64
    artifact_digest = "sha256:" + "b" * 64
    configuration_digest = "sha256:" + "c" * 64
    request = make_parent_batch_request(
        app_config=_app_config(),
        extension_generation=7,
        capability_manifest_digest=manifest_digest,
        artifact_manifest_digest=artifact_digest,
        extension_configuration_digest=configuration_digest,
    )
    item = _persisted_claimed_item(request)
    live_anchors = {
        "generation": 7,
        "artifact_manifest_digest": artifact_digest,
        "extension_configuration_digest": configuration_digest,
    }
    live_anchors[drift_field] = drift_value
    monkeypatch.setattr(
        service_module,
        "resolve_subagent_model_name",
        lambda *_args, **_kwargs: "model-a",
    )
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    repository = _TerminalRepository()
    restarted_service = SubagentBatchService(
        repository=repository,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
        extensions=SimpleNamespace(**live_anchors),
        capability_manifest_digest=manifest_digest,
    )

    await restarted_service._execute_item(item)

    assert repository.finalized is not None
    assert repository.finalized["succeeded"] is False
    assert repository.finalized["terminal_code"] == "provider_not_qualified"


@pytest.mark.parametrize("restart_state", ["pending", "running"])
@pytest.mark.asyncio
async def test_batch_worker_restart_recovers_pending_and_running_items(
    monkeypatch,
    tmp_path,
    restart_state: str,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / f'batch-{restart_state}.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    request = make_parent_batch_request(app_config=_app_config())
    batch_id = f"batch-restart-{restart_state}"
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id=batch_id)
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    repository = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    await repository.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )

    started = asyncio.Event()
    execution_calls = 0
    results: dict[str, SimpleNamespace] = {}

    class Executor:
        def __init__(self, **kwargs) -> None:
            self._callback = kwargs["execution_admitted_callback"]

        def execute_async(self, _prompt, task_id=None):
            nonlocal execution_calls
            execution_calls += 1
            call_number = execution_calls
            execution_id = f"execution-{call_number}"
            result = SimpleNamespace(
                status=FakeStatus.PENDING,
                result=None,
                error=None,
                stop_reason=None,
                token_usage_records=None,
            )
            results[execution_id] = result

            async def execute() -> None:
                await self._callback()
                started.set()
                if restart_state == "running" and call_number == 1:
                    return
                result.status = FakeStatus.COMPLETED
                result.result = "recovered result"

            asyncio.create_task(execute())
            return execution_id

    async def no_barrier(**_kwargs) -> bool:
        return False

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
        results.get,
    )
    monkeypatch.setattr(
        service_module,
        "request_cancel_background_task",
        lambda _execution_id: None,
    )
    monkeypatch.setattr(
        service_module,
        "cleanup_background_task",
        lambda _execution_id: None,
    )
    monkeypatch.setattr(
        service_module,
        "qualification_service_barrier",
        no_barrier,
    )
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])

    first_config = SubagentBatchesConfig(
        poll_interval_seconds=0.1,
        lease_seconds=10,
    )
    if restart_state == "pending":
        claim_entered = asyncio.Event()

        class BlockedRepository:
            async def claim_items(self, **_kwargs):
                claim_entered.set()
                await asyncio.Event().wait()

        first_service = SubagentBatchService(
            repository=BlockedRepository(),
            config=first_config,
            runtime_config=SubagentRuntimeConfig(max_running=1),
            app_config=_app_config(),
        )
        await first_service.start()
        await asyncio.wait_for(claim_entered.wait(), timeout=1)
        await first_service.stop()
    else:
        first_service = SubagentBatchService(
            repository=repository,
            config=first_config,
            runtime_config=SubagentRuntimeConfig(max_running=1),
            app_config=_app_config(),
        )
        await first_service.run_once(now=datetime.now(UTC))
        await asyncio.wait_for(started.wait(), timeout=1)
        await first_service.stop()
        async with session_factory() as session:
            await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.batch_id == batch_id).values(lease_expires_at=datetime(2000, 1, 1, tzinfo=UTC)))
            await session.commit()

    restarted_service = SubagentBatchService(
        repository=repository,
        config=first_config,
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )
    try:
        await restarted_service.run_once(now=datetime.now(UTC))
        tasks = list(restarted_service._executions.values())
        assert tasks
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)

        items = await repository.list_items(
            batch_id,
            user_id=request.user_id,
            include_result=True,
        )
        attempts = await repository.list_attempts(
            batch_id,
            user_id=request.user_id,
        )
        assert items is not None
        assert items[0]["result"] == "recovered result"
        assert attempts is not None
        assert [attempt["terminal_code"] for attempt in attempts] == (["succeeded"] if restart_state == "pending" else ["lease_expired", "succeeded"])
    finally:
        await restarted_service.stop()
        await engine.dispose()


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
async def test_batch_item_uses_its_own_attempt_fence_for_accepted_sandbox(
    monkeypatch,
) -> None:
    from datetime import timedelta

    from deerflow.sandbox.accepted_material import (
        AcceptedExecutionEvidenceV2,
        AcceptedMaterialCapability,
        AcceptedMaterializerSelection,
        AcceptedMaterialLeaseV1,
        AcceptedSandboxCapabilityProfileV1,
        AcceptedSandboxIsolationFactsV1,
        AcceptedSandboxQualificationV1,
        AcceptedSandboxSessionBridge,
    )
    from deerflow.sandbox.sandbox import Sandbox
    from deerflow.tool_plane.contracts import EffectiveToolPlaneRevisionV1

    tool_plane = EffectiveToolPlaneRevisionV1(
        base_revision_digest="1" * 64,
        user_overlay_digest="2" * 64,
        base_generation=1,
        overlay_generation=2,
        projection_digest="3" * 64,
    )
    snapshot = _SkillSnapshot()
    snapshot.root = object()
    request = _skill_bound_request(
        snapshot,
        tool_plane_revision=tool_plane.to_json(),
    )
    token = SkillProjectionConsumerToken(
        user_id=request.user_id,
        thread_id=request.thread_id,
        sandbox_id="parent-sandbox",
        run_id=request.run_id,
        generation=7,
        consumer_id="subagent-batch:batch-1",
        snapshot_id=snapshot.snapshot_id,
    )
    item = make_claimed_item(request)
    result = SimpleNamespace(
        status=FakeStatus.PENDING,
        result=None,
        error=None,
        stop_reason=None,
        token_usage_records=None,
    )
    authority_calls: list[dict[str, object]] = []

    class Repository:
        finalized = None

        async def item_attempt_authorized(self, item_id, **kwargs):
            authority_calls.append({"item_id": item_id, **kwargs})
            return True

        async def mark_item_running(self, *_args, **_kwargs):
            return True

        async def finalize_item(self, *_args, **kwargs):
            self.finalized = kwargs
            return True

    class RecordingSandbox(Sandbox):
        persistent_shell_sessions = False

        def __init__(self) -> None:
            super().__init__("raw-provider-resource")
            self.calls: list[str] = []

        def execute_command(self, command, env=None, timeout=None):
            del env, timeout
            self.calls.append(command)
            return "sandbox-ok"

        def read_file(self, path, start_line=None, end_line=None):
            raise AssertionError((path, start_line, end_line))

        def download_file(self, path):
            raise AssertionError(path)

        def list_dir(self, path, max_depth=2):
            raise AssertionError((path, max_depth))

        def write_file(self, path, content, append=False):
            raise AssertionError((path, content, append))

        def glob(self, path, pattern, *, include_dirs=False, max_results=200):
            raise AssertionError((path, pattern, include_dirs, max_results))

        def grep(
            self,
            path,
            pattern,
            *,
            glob=None,
            literal=False,
            case_sensitive=False,
            max_results=100,
        ):
            raise AssertionError(
                (path, pattern, glob, literal, case_sensitive, max_results),
            )

        def update_file(self, path, content):
            raise AssertionError((path, content))

    now = datetime.now(UTC)
    profile = AcceptedSandboxCapabilityProfileV1.build(
        material_capability=AcceptedMaterialCapability.IMMUTABLE_READ_ONLY,
        atomic_provider_ownership_fencing=True,
        atomic_provider_operation_fencing=False,
        authoritative_shared_expiry=True,
        resolved_immutable_image=True,
        restricted_non_root_isolation=True,
        recoverable_resource_lookup=True,
        durable_one_replica=True,
        exact_two=False,
    )
    qualification = AcceptedSandboxQualificationV1.build(
        capability_profile_digest=profile.digest,
        qualification_scope="contract_test_only",
        artifact_digest="4" * 64,
        topology_digest="5" * 64,
        verified_at=now,
        expires_at=now + timedelta(hours=1),
    )
    isolation = AcceptedSandboxIsolationFactsV1.build(
        restricted_non_root=True,
        read_only_accepted_material=True,
        privilege_escalation_disabled=True,
        runtime_class_digest="6" * 64,
        network_policy_digest="7" * 64,
    )
    raw_sandbox = RecordingSandbox()

    class Materializer:
        acquired_request = None
        released = 0
        validate_calls = 0

        def capability(self):
            return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

        async def acquire_and_materialize(
            self,
            accepted_request,
            *,
            execution_claim=None,
        ):
            assert execution_claim is None
            self.acquired_request = accepted_request
            lease = AcceptedMaterialLeaseV1(
                version=1,
                provider_kind="qualified-test",
                provider_instance_ref=raw_sandbox.id,
                ownership_epoch=9,
                lease_expires_at=now + timedelta(minutes=5),
                opaque_renewal_handle=object(),
            )
            evidence = AcceptedExecutionEvidenceV2.build(
                request=accepted_request,
                lease=lease,
                materialization_digest="8" * 64,
                verifier_image_digest="9" * 64,
                verifier_contract_version="contract-test-v1",
                read_only_proof_digest="a" * 64,
                qualification=qualification,
                isolation=isolation,
            )
            return raw_sandbox, lease, evidence

        async def validate(self, lease, evidence):
            del lease, evidence
            self.validate_calls += 1
            return True

        async def renew(self, lease):
            return lease

        async def release(self, lease):
            del lease
            self.released += 1

    materializer = Materializer()
    selection = AcceptedMaterializerSelection(
        materializer=materializer,
        runtime_image_digest="b" * 64,
        lease_duration=timedelta(minutes=5),
        capability_profile=profile,
        qualification=qualification,
    )
    selection_calls: list[dict[str, object]] = []

    async def select(_provider, **kwargs):
        selection_calls.append(kwargs)
        return selection

    executor_kwargs: dict[str, object] = {}

    class Executor:
        def __init__(self, **kwargs) -> None:
            executor_kwargs.update(kwargs)

        def execute_async(self, _prompt, task_id=None):
            assert task_id == item["id"]

            async def complete() -> None:
                bridge = executor_kwargs["accepted_sandbox_session_bridge"]
                assert isinstance(bridge, AcceptedSandboxSessionBridge)
                assert (
                    await asyncio.to_thread(
                        bridge.sandbox.execute_command,
                        "echo child",
                    )
                    == "sandbox-ok"
                )
                callback = executor_kwargs["execution_admitted_callback"]
                await callback()
                result.status = FakeStatus.COMPLETED
                result.result = "done"

            asyncio.create_task(complete())
            return "execution-1"

    repository = Repository()
    monkeypatch.setattr(
        service_module,
        "resolve_accepted_materializer",
        select,
        raising=False,
    )
    monkeypatch.setattr(
        service_module,
        "capture_accepted_file_manifest",
        lambda _root: (),
        raising=False,
    )
    monkeypatch.setattr(
        service_module,
        "get_sandbox_provider",
        lambda: object(),
        raising=False,
    )
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
    service._accepted_material["batch-1"] = request.resolved_parent_material
    service._runtime_adapters["batch-1"] = {
        "app_config": request.app_config,
        "accepted_parent": request.accepted_parent,
        "skill_projection_token": token,
    }
    service._batch_owners["batch-1"] = request.user_id

    await service._execute_item(item)

    accepted_request = materializer.acquired_request
    assert accepted_request.batch_child_attempt_ref is not None
    assert accepted_request.accepted_invocation_digest == (request.accepted_parent.runtime_identity_digest)
    assert accepted_request.tool_plane_effective_digest == tool_plane.effective_digest
    assert selection_calls[0]["require_durable_one_replica"] is True
    assert selection_calls[0]["require_exact_two"] is False
    assert authority_calls
    assert authority_calls[0] == {
        "item_id": item["id"],
        "attempt_id": item["attempt_id"],
        "lease_epoch": item["lease_epoch"],
        "lease_owner": service._lease_owner,
    }
    assert raw_sandbox.calls == ["echo child"]
    assert materializer.validate_calls == 2
    assert materializer.released == 1
    assert repository.finalized["succeeded"] is True


@pytest.mark.asyncio
async def test_terminal_publication_crosses_the_service_fault_barrier(
    monkeypatch,
) -> None:
    request = make_parent_batch_request(app_config=_app_config())
    item = make_claimed_item(request)
    result = SimpleNamespace(
        status=FakeStatus.PENDING,
        result=None,
        error=None,
        stop_reason=None,
        token_usage_records=None,
    )
    transitions: list[str] = []

    class Repository:
        async def mark_item_running(self, *_args, **_kwargs):
            transitions.append("started")
            return True

        async def finalize_item(self, *_args, **_kwargs):
            transitions.append("published")
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
                result.result = "accepted result"

            asyncio.create_task(complete())
            return "execution-1"

    async def barrier(**kwargs) -> bool:
        assert kwargs == {
            "scenario": "subagent_batch",
            "point": "before_terminal_publication",
            "subject_id": item["id"],
        }
        transitions.append("barrier")
        return False

    monkeypatch.setattr(
        service_module,
        "qualification_service_barrier",
        barrier,
        raising=False,
    )
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
        repository=Repository(),
        config=SubagentBatchesConfig(poll_interval_seconds=0.1),
        runtime_config=SubagentRuntimeConfig(max_running=1),
        app_config=_app_config(),
    )

    await service._execute_item(item)

    assert transitions == ["started", "barrier", "published"]


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

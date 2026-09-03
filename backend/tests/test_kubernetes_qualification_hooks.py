"""Production fault hooks remain inert unless the Kubernetes gate is explicit."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from langchain_core.messages import ToolMessage

import deerflow.runtime.kubernetes_qualification as qualification_module
from deerflow.runtime.kubernetes_qualification import (
    KubernetesQualificationChatModel,
    KubernetesQualificationHooks,
    accepted_sandbox_qualification_candidate_enabled,
    qualification_barrier,
    qualification_service_barrier,
    scenario_from_external_key,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1, TenantSubsystem

_TENANT_NAMESPACE = TenantIdentityV1.from_canonical_id("qualification").namespace(TenantSubsystem.REDIS)
_QUALIFICATION_PREFIX = "hm:v1:tenant-e08e79269b9e0fde:redis:qualification"


class _RedisDouble:
    def __init__(self, *, released: bool = True) -> None:
        self.values: dict[str, bytes] = {}
        self.increments: list[str] = []
        self.released = released

    async def incr(self, key: str) -> int:
        self.increments.append(key)
        return len(self.increments)

    async def set(self, key: str, value: str) -> None:
        self.values[key] = value.encode()

    async def get(self, key: str):
        if key.endswith(":release") and self.released:
            return b"1"
        return self.values.get(key)

    async def getdel(self, key: str):
        return self.values.pop(key, None)


def _arm(redis: _RedisDouble, scenario: str) -> None:
    redis.values[f"{_QUALIFICATION_PREFIX}:qual-1:{scenario}:arm"] = b"1"


def test_fault_scenario_is_derived_only_from_the_normalized_external_key() -> None:
    assert scenario_from_external_key("raw:k8s-qual-v1:accepted_before_worker_start:delivery-1") == "accepted_before_worker_start"
    assert scenario_from_external_key("k8s-qual-v1:active_execution:delivery-1") is None
    assert scenario_from_external_key("raw:k8s-qual-v1:unknown:delivery-1") is None
    assert scenario_from_external_key("raw:k8s-qual-v1:owner_sigkill:delivery-1") == "owner_sigkill"


def test_accepted_sandbox_candidate_requires_every_disposable_test_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = (
        "DEER_FLOW_QUALIFICATION_CANDIDATE",
        "DEER_FLOW_QUALIFICATION_CANDIDATE_ID",
        "DEER_FLOW_QUALIFICATION_NAMESPACE",
        "DEERFLOW_TEST_KUBERNETES_RUNTIME",
        "DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION",
        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID",
    )
    for name in names:
        monkeypatch.delenv(name, raising=False)
    assert accepted_sandbox_qualification_candidate_enabled() is False

    monkeypatch.setenv("DEER_FLOW_QUALIFICATION_CANDIDATE", "1")
    monkeypatch.setenv("DEER_FLOW_QUALIFICATION_CANDIDATE_ID", "qual-1")
    monkeypatch.setenv(
        "DEER_FLOW_QUALIFICATION_NAMESPACE",
        "hartmesh-qualification-qual-1",
    )
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", "1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION", "1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID", "qual-1")
    assert accepted_sandbox_qualification_candidate_enabled() is True

    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID", "other")
    assert accepted_sandbox_qualification_candidate_enabled() is False


@pytest.mark.anyio
async def test_fault_hooks_and_model_are_unreachable_without_explicit_runtime_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", raising=False)
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:active_execution:delivery-1",
    )

    assert await qualification_barrier("during_model_execution", record) is False
    with pytest.raises(RuntimeError, match="qualification model is disabled"):
        KubernetesQualificationChatModel()


@pytest.mark.anyio
async def test_fault_barrier_requires_the_separate_fault_injection_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", "1")
    monkeypatch.delenv(
        "DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION",
        raising=False,
    )
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID", "qual-1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_REDIS_URL", "redis://fixture")
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:active_execution:delivery-1",
    )

    assert await qualification_barrier("during_model_execution", record) is False


@pytest.mark.anyio
async def test_subagent_batch_terminal_publication_is_a_valid_service_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        async def getdel(self, _key: str):
            return None

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        qualification_module,
        "_runtime_configuration",
        lambda **_kwargs: ("qual-1", "redis://fixture", 1.0),
    )
    monkeypatch.setenv("DEER_FLOW_TENANT_ID", "qualification")
    monkeypatch.setattr(
        "redis.asyncio.Redis.from_url",
        lambda *_args, **_kwargs: Client(),
    )

    assert (
        await qualification_service_barrier(
            scenario="subagent_batch",
            point="before_terminal_publication",
            subject_id="bi_" + "a" * 48,
        )
        is False
    )


@pytest.mark.anyio
async def test_fault_hook_records_and_releases_only_the_selected_barrier() -> None:
    redis = _RedisDouble()
    _arm(redis, "accepted_before_worker_start")
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=1,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:accepted_before_worker_start:delivery-1",
    )

    reached = await hooks.barrier("accepted_before_worker_start", record)
    ignored = await hooks.barrier("accepted_before_client_response", record)

    assert reached is True
    assert ignored is False
    assert redis.increments == [f"{_QUALIFICATION_PREFIX}:qual-1:accepted_before_worker_start:barrier_hits"]
    assert redis.values[f"{_QUALIFICATION_PREFIX}:qual-1:accepted_before_worker_start:reached"] == b"run-1"


@pytest.mark.anyio
async def test_fault_hook_requires_and_atomically_consumes_one_expiring_arm() -> None:
    redis = _RedisDouble()
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=1,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:active_execution:delivery-1",
    )

    assert await hooks.barrier("during_model_execution", record) is False
    _arm(redis, "active_execution")
    assert await hooks.barrier("during_model_execution", record) is True
    assert await hooks.barrier("during_model_execution", record) is False
    assert redis.increments == [
        f"{_QUALIFICATION_PREFIX}:qual-1:active_execution:barrier_hits",
    ]


@pytest.mark.anyio
async def test_accepted_sandbox_race_has_a_post_validation_barrier() -> None:
    redis = _RedisDouble()
    _arm(redis, "terminal_before_lifecycle_commit")
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=1,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key=("raw:k8s-qual-v1:terminal_before_lifecycle_commit:during-tool"),
    )

    assert await hooks.barrier("accepted_sandbox_after_validation", record) is True
    assert redis.values[f"{_QUALIFICATION_PREFIX}:qual-1:terminal_before_lifecycle_commit:reached"] == b"run-1"


@pytest.mark.anyio
async def test_live_qualification_tool_proves_raced_call_then_post_loss_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.sandbox.accepted_material import (
        AcceptedSandboxAuthorityLostError,
    )

    record = SimpleNamespace(
        external_key=("raw:k8s-qual-v1:terminal_before_lifecycle_commit:during-tool"),
        execution_lease_renewal=None,
    )
    record_token = qualification_module._ACTIVE_RECORD.set(record)
    calls = 0
    counters: list[str] = []

    async def bash(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise AcceptedSandboxAuthorityLostError(
                "accepted_sandbox_material_lease_lost",
            )
        return "raced-call-completed"

    async def counter(name, _record):
        counters.append(name)
        return True

    async def barrier(_point, _record):
        return True

    monkeypatch.setattr("deerflow.sandbox.tools._bash_tool_async", bash)
    monkeypatch.setattr(
        "deerflow.runtime.tool_evidence.get_active_tool_receipt",
        lambda: SimpleNamespace(
            tool_name="qualification_sandbox_operation",
            receipt_id="receipt-1",
        ),
    )
    monkeypatch.setattr(qualification_module, "qualification_counter", counter)
    monkeypatch.setattr(qualification_module, "qualification_barrier", barrier)
    try:
        result = await qualification_module.qualification_sandbox_operation.coroutine(
            runtime=object(),
            description="bounded",
        )
    finally:
        qualification_module._ACTIVE_RECORD.reset(record_token)

    assert result == "raced-call-completed"
    assert calls == 2
    assert counters == [
        "tool_starts",
        "tool_completions",
        "accepted_sandbox_raced_provider_calls",
        "accepted_sandbox_post_loss_rejections",
    ]


@pytest.mark.anyio
async def test_multi_gateway_owner_barrier_records_safe_replica_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEER_FLOW_REPLICA_ID", "gateway-1")
    redis = _RedisDouble()
    _arm(redis, "owner_sigkill")
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=1,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:owner_sigkill:delivery-1",
    )

    assert await hooks.barrier("during_model_execution", record) is True
    assert redis.values[f"{_QUALIFICATION_PREFIX}:qual-1:owner_sigkill:owner_replica_id"] == b"gateway-1"


@pytest.mark.anyio
async def test_multi_gateway_owner_barrier_honors_the_armed_fault_window() -> None:
    redis = _RedisDouble()
    prefix = f"{_QUALIFICATION_PREFIX}:qual-1:owner_sigkill"
    redis.values[f"{prefix}:selected_point"] = b"post_checkpoint_before_graph"
    _arm(redis, "owner_sigkill")
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=1,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:owner_sigkill:checkpoint",
    )

    assert await hooks.barrier("post_materialization_before_checkpoint", record) is False
    assert await hooks.barrier("post_checkpoint_before_graph", record) is True
    assert redis.values[f"{prefix}:reached_point"] == b"post_checkpoint_before_graph"
    assert redis.increments == [f"{prefix}:barrier_hits"]


@pytest.mark.anyio
async def test_qualification_model_supports_scheduler_runs_without_fault_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", "1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID", "qual-1")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_REDIS_URL", "redis://fixture")
    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_MODEL_DELAY_SECONDS", "0")
    model = KubernetesQualificationChatModel()

    result = await model._agenerate([])

    assert result.generations[0].message.content == ("Kubernetes qualification completed deterministically.")


def test_qualification_model_selects_one_long_sandbox_tool_call() -> None:
    record_token = qualification_module._ACTIVE_RECORD.set(
        SimpleNamespace(
            external_key="raw:k8s-qual-v1:owner_sigkill:during-tool",
        )
    )
    try:
        first = KubernetesQualificationChatModel._response([])
        second = KubernetesQualificationChatModel._response(
            [
                ToolMessage(
                    content="qualification-complete",
                    tool_call_id="qualification-sandbox-operation-1",
                )
            ]
        )
    finally:
        qualification_module._ACTIVE_RECORD.reset(record_token)

    assert first.tool_calls == [
        {
            "name": "qualification_sandbox_operation",
            "args": {
                "description": "exercise a bounded long sandbox operation",
            },
            "id": "qualification-sandbox-operation-1",
            "type": "tool_call",
        }
    ]
    assert second.tool_calls == []
    assert second.content == "Kubernetes qualification completed deterministically."


@pytest.mark.anyio
async def test_fault_hook_timeout_is_bounded_and_does_not_include_redis_details() -> None:
    redis = _RedisDouble(released=False)
    _arm(redis, "active_execution")
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=0.001,
        poll_seconds=0,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key="raw:k8s-qual-v1:active_execution:delivery-1",
    )

    with pytest.raises(RuntimeError, match="qualification_barrier_timeout:active_execution"):
        await hooks.barrier("during_model_execution", record)


@pytest.mark.anyio
async def test_forced_kill_barrier_survives_graceful_task_cancellation() -> None:
    redis = _RedisDouble(released=False)
    _arm(redis, "forced_kill_after_graceful_deadline")
    hooks = KubernetesQualificationHooks(
        qualification_id="qual-1",
        redis_client=redis,
        timeout_seconds=1,
        poll_seconds=0.001,
        tenant_namespace=_TENANT_NAMESPACE,
    )
    record = SimpleNamespace(
        run_id="run-1",
        external_key=("raw:k8s-qual-v1:forced_kill_after_graceful_deadline:delivery-1"),
    )

    task = asyncio.create_task(hooks.barrier("during_model_execution", record))
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0.01)

    assert not task.done()
    redis.released = True
    assert await task is True

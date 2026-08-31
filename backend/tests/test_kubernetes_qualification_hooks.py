"""Production fault hooks remain inert unless the Kubernetes gate is explicit."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from deerflow.runtime.kubernetes_qualification import (
    KubernetesQualificationChatModel,
    KubernetesQualificationHooks,
    qualification_barrier,
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


def test_fault_scenario_is_derived_only_from_the_normalized_external_key() -> None:
    assert scenario_from_external_key("raw:k8s-qual-v1:accepted_before_worker_start:delivery-1") == "accepted_before_worker_start"
    assert scenario_from_external_key("k8s-qual-v1:active_execution:delivery-1") is None
    assert scenario_from_external_key("raw:k8s-qual-v1:unknown:delivery-1") is None


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
async def test_fault_hook_records_and_releases_only_the_selected_barrier() -> None:
    redis = _RedisDouble()
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
async def test_fault_hook_timeout_is_bounded_and_does_not_include_redis_details() -> None:
    redis = _RedisDouble(released=False)
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

"""Opt-in real-process Kubernetes qualification hooks.

The hooks are inert unless ``DEERFLOW_TEST_KUBERNETES_RUNTIME=1`` is present.
They coordinate through the deployment's shared Redis so deleting the Gateway
pod cannot erase a reached fault point. No HTTP endpoint or production config
field exposes this module.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any, ClassVar

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from deerflow.qualification_evidence import QUALIFICATION_SCENARIOS
from deerflow.runtime.tenant_identity import (
    RedisTenantComponent,
    TenantNamespaceV1,
    TenantReferenceV1,
    TenantSubsystem,
    redis_component_key_prefix,
    tenant_namespace_from_reference,
)

_SCENARIOS = frozenset(QUALIFICATION_SCENARIOS)
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_EXTERNAL_KEY_PREFIX = "raw:k8s-qual-v1:"
_ACTIVE_SCENARIO: ContextVar[str | None] = ContextVar(
    "deerflow_kubernetes_qualification_scenario",
    default=None,
)
_ACTIVE_RECORD: ContextVar[object | None] = ContextVar(
    "deerflow_kubernetes_qualification_record",
    default=None,
)


def scenario_from_external_key(value: object) -> str | None:
    """Extract a known scenario from the normalized idempotency key only."""

    if not isinstance(value, str) or not value.startswith(_EXTERNAL_KEY_PREFIX):
        return None
    remainder = value[len(_EXTERNAL_KEY_PREFIX) :]
    scenario, separator, delivery_id = remainder.partition(":")
    if not separator or not delivery_id or scenario not in _SCENARIOS:
        return None
    return scenario


def _scenario_for_record(record: object) -> str | None:
    return scenario_from_external_key(getattr(record, "external_key", None))


def _point_matches(point: str, scenario: str) -> bool:
    if point == scenario:
        return True
    return point == "during_model_execution" and scenario in {
        "active_execution",
        "graceful_rollout_termination",
        "forced_kill_after_graceful_deadline",
    }


def _tenant_namespace_for_record(record: object) -> TenantNamespaceV1:
    accepted = getattr(record, "accepted_invocation", None)
    reference = getattr(accepted, "tenant", None)
    if not isinstance(reference, TenantReferenceV1):
        raise RuntimeError("kubernetes_qualification_tenant_missing")
    return tenant_namespace_from_reference(reference, TenantSubsystem.REDIS)


class KubernetesQualificationHooks:
    """Redis-backed bounded barriers for one qualification run."""

    def __init__(
        self,
        *,
        qualification_id: str,
        redis_client: Any,
        timeout_seconds: float,
        poll_seconds: float = 0.1,
        tenant_namespace: TenantNamespaceV1,
    ) -> None:
        if _SAFE_ID.fullmatch(qualification_id) is None:
            raise ValueError("qualification id is invalid")
        if not 0 < timeout_seconds <= 300 or not 0 <= poll_seconds <= 5:
            raise ValueError("qualification barrier timing is invalid")
        self._prefix = f"{redis_component_key_prefix(tenant_namespace, RedisTenantComponent.QUALIFICATION)}:{qualification_id}"
        self._redis = redis_client
        self._timeout_seconds = timeout_seconds
        self._poll_seconds = poll_seconds

    async def barrier(self, point: str, record: object) -> bool:
        """Record and wait at a selected point; unrelated runs pass through."""

        scenario = _scenario_for_record(record)
        if scenario is None or not _point_matches(point, scenario):
            return False
        run_id = getattr(record, "run_id", None)
        if not isinstance(run_id, str) or not run_id or len(run_id) > 128:
            raise RuntimeError("qualification_barrier_invalid_run")
        scenario_prefix = f"{self._prefix}:{scenario}"
        await self._redis.incr(f"{scenario_prefix}:barrier_hits")
        await self._redis.set(f"{scenario_prefix}:reached", run_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_seconds
        while True:
            try:
                if await self._redis.get(f"{scenario_prefix}:release") == b"1":
                    return True
                if loop.time() >= deadline:
                    raise RuntimeError(f"qualification_barrier_timeout:{scenario}")
                await asyncio.sleep(self._poll_seconds)
            except asyncio.CancelledError:
                await self._redis.incr(f"{scenario_prefix}:cancellation_observed")
                if scenario != "forced_kill_after_graceful_deadline":
                    raise
                # This one opt-in scenario must outlive the application's
                # graceful drain so Kubernetes, rather than an in-process
                # exception, applies the configured pod termination deadline.
                task = asyncio.current_task()
                if task is not None:
                    task.uncancel()

    async def counter(self, name: str, record: object) -> bool:
        """Increment a bounded scenario counter in shared evidence state."""

        if _SAFE_ID.fullmatch(name) is None:
            raise ValueError("qualification counter name is invalid")
        scenario = _scenario_for_record(record)
        if scenario is None:
            return False
        _ACTIVE_SCENARIO.set(scenario)
        _ACTIVE_RECORD.set(record)
        await self._redis.incr(f"{self._prefix}:{scenario}:{name}")
        return True


def _runtime_configuration() -> tuple[str, str, float] | None:
    if os.getenv("DEERFLOW_TEST_KUBERNETES_RUNTIME") != "1":
        return None
    qualification_id = os.getenv("DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID", "")
    redis_url = os.getenv("DEERFLOW_TEST_KUBERNETES_REDIS_URL") or os.getenv(
        "DEER_FLOW_STREAM_BRIDGE_REDIS_URL",
        "",
    )
    if _SAFE_ID.fullmatch(qualification_id) is None or not redis_url.startswith(("redis://", "rediss://")):
        raise RuntimeError("kubernetes_qualification_runtime_misconfigured")
    try:
        timeout_seconds = float(os.getenv("DEERFLOW_TEST_KUBERNETES_BARRIER_TIMEOUT_SECONDS", "120"))
    except ValueError as exc:
        raise RuntimeError("kubernetes_qualification_runtime_misconfigured") from exc
    if not 0 < timeout_seconds <= 300:
        raise RuntimeError("kubernetes_qualification_runtime_misconfigured")
    return qualification_id, redis_url, timeout_seconds


async def _with_async_hooks(
    record: object,
    operation: Callable[[KubernetesQualificationHooks], Awaitable[bool]],
) -> bool:
    configuration = _runtime_configuration()
    if configuration is None:
        return False
    qualification_id, redis_url, timeout_seconds = configuration
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url, decode_responses=False)
    try:
        hooks = KubernetesQualificationHooks(
            qualification_id=qualification_id,
            redis_client=client,
            timeout_seconds=timeout_seconds,
            tenant_namespace=_tenant_namespace_for_record(record),
        )
        return await operation(hooks)
    finally:
        await client.aclose()


async def qualification_barrier(point: str, record: object) -> bool:
    """Reach one runtime barrier only in the explicit qualification process."""

    return await _with_async_hooks(
        record,
        lambda hooks: hooks.barrier(point, record),
    )


async def qualification_counter(name: str, record: object) -> bool:
    """Increment shared execution evidence only in qualification mode."""

    return await _with_async_hooks(
        record,
        lambda hooks: hooks.counter(name, record),
    )


class KubernetesQualificationChatModel(BaseChatModel):
    """Deterministic no-network model double restricted to qualification pods."""

    response_text: str = "Kubernetes qualification completed deterministically."
    _model_name: ClassVar[str] = "kubernetes-qualification-double"

    def __init__(self, **kwargs: Any) -> None:
        if _runtime_configuration() is None:
            raise RuntimeError("qualification model is disabled")
        super().__init__(**kwargs)

    @property
    def _llm_type(self) -> str:
        return self._model_name

    async def _record_model_start_async(self) -> None:
        scenario = _ACTIVE_SCENARIO.get()
        record = _ACTIVE_RECORD.get()
        configuration = _runtime_configuration()
        if scenario is None or record is None or configuration is None:
            raise RuntimeError("qualification_model_missing_scenario")
        qualification_id, redis_url, _timeout = configuration
        from redis.asyncio import Redis

        client = Redis.from_url(redis_url, decode_responses=False)
        try:
            hooks = KubernetesQualificationHooks(
                qualification_id=qualification_id,
                redis_client=client,
                timeout_seconds=configuration[2],
                tenant_namespace=_tenant_namespace_for_record(record),
            )
            await client.incr(f"{hooks._prefix}:{scenario}:model_starts")
            await hooks.barrier("during_model_execution", record)
        finally:
            await client.aclose()

    def _record_model_start_sync(self) -> None:
        scenario = _ACTIVE_SCENARIO.get()
        record = _ACTIVE_RECORD.get()
        configuration = _runtime_configuration()
        if scenario is None or record is None or configuration is None:
            raise RuntimeError("qualification_model_missing_scenario")
        qualification_id, redis_url, _timeout = configuration
        from redis import Redis

        client = Redis.from_url(redis_url, decode_responses=False)
        try:
            hooks = KubernetesQualificationHooks(
                qualification_id=qualification_id,
                redis_client=client,
                timeout_seconds=configuration[2],
                tenant_namespace=_tenant_namespace_for_record(record),
            )
            client.incr(f"{hooks._prefix}:{scenario}:model_starts")
        finally:
            client.close()

    def _generate(
        self,
        messages: list[Any],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        del messages, stop, run_manager, kwargs
        self._record_model_start_sync()
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=self.response_text))])

    async def _agenerate(
        self,
        messages: list[Any],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        del messages, stop, run_manager, kwargs
        await self._record_model_start_async()
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=self.response_text))])


__all__ = [
    "QUALIFICATION_SCENARIOS",
    "KubernetesQualificationChatModel",
    "KubernetesQualificationHooks",
    "qualification_barrier",
    "qualification_counter",
    "scenario_from_external_key",
]

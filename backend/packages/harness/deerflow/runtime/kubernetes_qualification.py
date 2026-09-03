"""Opt-in real-process Kubernetes qualification hooks.

Fault hooks are inert unless both ``DEERFLOW_TEST_KUBERNETES_RUNTIME=1`` and
``DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION=1`` are present, and every barrier
also consumes one tenant-scoped Redis arm.
They coordinate through the deployment's shared Redis so deleting the Gateway
pod cannot erase a reached fault point. No HTTP endpoint or production config
field exposes this module.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import time
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any, ClassVar

from langchain.tools import tool
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from deerflow.agents.assembly_descriptor import _seal_host_tool_recovery
from deerflow.multi_gateway_qualification import (
    MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
)
from deerflow.qualification_evidence import QUALIFICATION_SCENARIOS
from deerflow.runtime.tenant_identity import (
    RedisTenantComponent,
    TenantIdentityV1,
    TenantNamespaceV1,
    TenantReferenceV1,
    TenantSubsystem,
    redis_component_key_prefix,
    tenant_namespace_from_reference,
)
from deerflow.tools.types import Runtime

_SCENARIOS = frozenset((*QUALIFICATION_SCENARIOS, *MULTI_GATEWAY_QUALIFICATION_SCENARIOS))
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


def qualification_reconciled_operation_command(
    receipt_id: str,
    *,
    base_dir: str = "/mnt/user-data/workspace/.hartmesh-qualification-operations",
    delay_seconds: float = 20.0,
) -> str:
    """Build one receipt-keyed sandbox operation with one execution body."""

    if not isinstance(receipt_id, str) or _SAFE_ID.fullmatch(receipt_id) is None:
        raise ValueError("qualification receipt id is invalid")
    if not isinstance(base_dir, str) or not base_dir.startswith("/") or "\x00" in base_dir or len(base_dir.encode("utf-8")) > 512:
        raise ValueError("qualification operation base directory is invalid")
    if not isinstance(delay_seconds, (int, float)) or isinstance(delay_seconds, bool) or not 0 <= float(delay_seconds) <= 60:
        raise ValueError("qualification operation delay is invalid")

    child_source = """
import os
import pathlib
import sys
import time

operation = pathlib.Path(sys.argv[1])
launched = operation / "launched"
try:
    descriptor = os.open(launched, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    raise SystemExit(0)
with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
    stream.write(str(os.getpid()) + "\\n")
(operation / "execution-count").write_text("1\\n", encoding="utf-8")
time.sleep(float(sys.argv[2]))
temporary = operation / ("result.tmp." + str(os.getpid()))
temporary.write_text("qualification-complete\\n", encoding="utf-8")
os.replace(temporary, operation / "result")
""".strip()
    coordinator_source = f"""
import fcntl
import pathlib
import subprocess
import sys
import time

operation = pathlib.Path({json.dumps(base_dir)}) / {json.dumps(receipt_id)}
operation.mkdir(parents=True, exist_ok=True)
result = operation / "result"
with (operation / "coordinator.lock").open("a+", encoding="utf-8") as lock:
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    if not result.exists() and not (operation / "launched").exists():
        subprocess.Popen(
            [sys.executable, "-c", {json.dumps(child_source)}, str(operation), {json.dumps(str(float(delay_seconds)))}],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        launch_deadline = time.monotonic() + 5
        while not (operation / "launched").exists() and time.monotonic() < launch_deadline:
            time.sleep(0.02)
        if not (operation / "launched").exists():
            raise RuntimeError("qualification operation did not launch")
deadline = time.monotonic() + 90
while not result.exists() and time.monotonic() < deadline:
    time.sleep(0.1)
if not result.exists():
    raise RuntimeError("qualification operation did not complete")
sys.stdout.write(result.read_text(encoding="utf-8"))
""".strip()
    return f"python -c {shlex.quote(coordinator_source)}"


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
    if point == "accepted_before_worker_start":
        return scenario == "concurrent_admission"
    if point == "accepted_before_client_response":
        return scenario == "sse_reconnect"
    if scenario == "owner_sigkill":
        return point in {
            "accepted_before_materialization",
            "post_materialization_before_checkpoint",
            "post_checkpoint_before_graph",
            "post_dispatch_marker_before_graph",
            "during_model_execution",
            "during_tool_execution",
            "terminal_before_lifecycle_commit",
        }
    if scenario == "sandbox_recovery":
        return point in {
            "post_materialization_before_checkpoint",
            "during_model_execution",
            "during_tool_execution",
        }
    if scenario == "cancellation_finalization":
        return point in {
            "during_model_execution",
            "terminal_before_lifecycle_commit",
        }
    if scenario == "postgresql_interruption":
        return point in {
            "post_checkpoint_before_graph",
            "during_tool_execution",
            "terminal_before_lifecycle_commit",
        }
    if scenario == "terminal_before_lifecycle_commit":
        return point == "accepted_sandbox_after_validation"
    return point == "during_model_execution" and scenario in {
        "active_execution",
        "graceful_rollout_termination",
        "forced_kill_after_graceful_deadline",
        "execution_ownership",
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
        selected_point = await self._redis.get(
            f"{scenario_prefix}:selected_point",
        )
        if selected_point is not None:
            if isinstance(selected_point, bytes):
                selected_point = selected_point.decode("utf-8", errors="strict")
            if selected_point != point:
                return False
        if await self._redis.getdel(f"{scenario_prefix}:arm") != b"1":
            return False
        await self._redis.incr(f"{scenario_prefix}:barrier_hits")
        await self._redis.set(f"{scenario_prefix}:reached_point", point)
        await self._redis.set(f"{scenario_prefix}:reached", run_id)
        replica_id = os.getenv("DEER_FLOW_REPLICA_ID")
        if isinstance(replica_id, str) and _SAFE_ID.fullmatch(replica_id):
            await self._redis.set(
                f"{scenario_prefix}:owner_replica_id",
                replica_id,
            )
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

    async def stale_external_renewal_probe(
        self,
        record: object,
        renewal: Callable[[], Awaitable[bool]],
    ) -> bool:
        """Exercise one returning stale owner's real accepted-material lease."""

        scenario = _scenario_for_record(record)
        if scenario != "postgresql_interruption":
            return False
        scenario_prefix = f"{self._prefix}:{scenario}"
        if (
            await self._redis.getdel(
                f"{scenario_prefix}:stale_external_renewal_arm",
            )
            != b"1"
        ):
            return False
        if await renewal():
            raise RuntimeError("stale_external_renewal_was_accepted")
        await self._redis.incr(
            f"{scenario_prefix}:sandbox_stale_renewal_rejections",
        )
        return True


def _runtime_configuration(
    *,
    require_fault_injection: bool = False,
) -> tuple[str, str, float] | None:
    if os.getenv("DEERFLOW_TEST_KUBERNETES_RUNTIME") != "1":
        return None
    if require_fault_injection and os.getenv("DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION") != "1":
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


def accepted_sandbox_qualification_candidate_enabled() -> bool:
    """Recognize only the disposable, fault-enabled live qualification mode.

    The candidate gate is not passing evidence.  It exists solely to let the
    external Kubernetes harness exercise the code that will produce such
    evidence without creating a circular admission dependency.
    """

    candidate_id = os.getenv("DEER_FLOW_QUALIFICATION_CANDIDATE_ID", "")
    qualification_id = os.getenv(
        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID",
        "",
    )
    namespace = os.getenv("DEER_FLOW_QUALIFICATION_NAMESPACE", "")
    return (
        os.getenv("DEER_FLOW_QUALIFICATION_CANDIDATE") == "1"
        and os.getenv("DEERFLOW_TEST_KUBERNETES_RUNTIME") == "1"
        and os.getenv("DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION") == "1"
        and _SAFE_ID.fullmatch(candidate_id) is not None
        and candidate_id == qualification_id
        and namespace.startswith("hartmesh-qualification-")
        and _SAFE_ID.fullmatch(namespace) is not None
    )


async def _with_async_hooks(
    record: object,
    operation: Callable[[KubernetesQualificationHooks], Awaitable[bool]],
    *,
    require_fault_injection: bool = False,
) -> bool:
    configuration = _runtime_configuration(
        require_fault_injection=require_fault_injection,
    )
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
        require_fault_injection=True,
    )


async def qualification_counter(name: str, record: object) -> bool:
    """Increment shared execution evidence only in qualification mode."""

    return await _with_async_hooks(
        record,
        lambda hooks: hooks.counter(name, record),
    )


async def qualification_stale_external_renewal_probe(
    record: object,
    renewal: Callable[[], Awaitable[bool]],
) -> bool:
    """Run an armed stale accepted-material renewal in the fault harness."""

    return await _with_async_hooks(
        record,
        lambda hooks: hooks.stale_external_renewal_probe(record, renewal),
        require_fault_injection=True,
    )


async def qualification_service_barrier(
    *,
    scenario: str,
    point: str,
    subject_id: str,
) -> bool:
    """Pause a scheduler, MCP, or batch lease owner at an armed live point.

    Unlike invocation barriers, these services do not own a ``RunRecord`` yet.
    The harness must atomically arm a named point in the tenant Redis prefix;
    production processes and unarmed qualification work return immediately.
    """

    configuration = _runtime_configuration(require_fault_injection=True)
    if configuration is None:
        return False
    if scenario not in {
        "scheduler_owner_loss",
        "mcp_task_notification",
        "subagent_batch",
    }:
        raise ValueError("qualification service scenario is invalid")
    if _SAFE_ID.fullmatch(point) is None or _SAFE_ID.fullmatch(subject_id) is None:
        raise ValueError("qualification service barrier identity is invalid")
    tenant_id = os.getenv("DEER_FLOW_TENANT_ID", "")
    if _SAFE_ID.fullmatch(tenant_id) is None:
        raise RuntimeError("kubernetes_qualification_tenant_missing")
    qualification_id, redis_url, timeout_seconds = configuration
    tenant_namespace = TenantIdentityV1.from_canonical_id(tenant_id).namespace(TenantSubsystem.REDIS)
    prefix = f"{redis_component_key_prefix(tenant_namespace, RedisTenantComponent.QUALIFICATION)}:{qualification_id}:{scenario}:{point}"
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url, decode_responses=False)
    try:
        if await client.getdel(f"{prefix}:arm") != b"1":
            return False
        await client.incr(f"{prefix}:barrier_hits")
        await client.set(f"{prefix}:reached", subject_id, ex=300)
        replica_id = os.getenv("DEER_FLOW_REPLICA_ID")
        if isinstance(replica_id, str) and _SAFE_ID.fullmatch(replica_id):
            await client.set(
                f"{prefix}:owner_replica_id",
                replica_id,
                ex=300,
            )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while await client.get(f"{prefix}:release") != b"1":
            if loop.time() >= deadline:
                raise RuntimeError(f"qualification_barrier_timeout:{scenario}:{point}")
            await asyncio.sleep(0.1)
        return True
    finally:
        await client.aclose()


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

    def bind_tools(self, tools: list[Any], **kwargs: Any) -> Any:
        """Advertise the deterministic qualification tool without provider IO."""

        del tools, kwargs
        return self

    @staticmethod
    def _delay_seconds() -> float:
        try:
            value = float(
                os.getenv(
                    "DEERFLOW_TEST_KUBERNETES_MODEL_DELAY_SECONDS",
                    "0",
                )
            )
        except ValueError as exc:
            raise RuntimeError("kubernetes_qualification_runtime_misconfigured") from exc
        if not 0 <= value <= 10:
            raise RuntimeError("kubernetes_qualification_runtime_misconfigured")
        return value

    async def _record_model_start_async(self) -> None:
        scenario = _ACTIVE_SCENARIO.get()
        record = _ACTIVE_RECORD.get()
        configuration = _runtime_configuration()
        if configuration is None:
            raise RuntimeError("qualification_model_missing_scenario")
        if scenario is None or record is None:
            await asyncio.sleep(self._delay_seconds())
            return
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

    @staticmethod
    def _raise_selected_failure() -> None:
        record = _ACTIVE_RECORD.get()
        external_key = getattr(record, "external_key", None)
        if isinstance(external_key, str) and external_key.endswith(":fail"):
            raise RuntimeError("qualification_deterministic_model_failure")

    @staticmethod
    def _response(messages: list[Any]) -> AIMessage:
        record = _ACTIVE_RECORD.get()
        external_key = getattr(record, "external_key", None)
        tool_selected = isinstance(external_key, str) and external_key.endswith(
            ":during-tool",
        )
        tool_finished = any(isinstance(message, ToolMessage) for message in messages)
        if tool_selected and not tool_finished:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "qualification_sandbox_operation",
                        "args": {
                            "description": ("exercise a bounded long sandbox operation"),
                        },
                        "id": "qualification-sandbox-operation-1",
                        "type": "tool_call",
                    }
                ],
            )
        return AIMessage(
            content="Kubernetes qualification completed deterministically.",
        )

    def _record_model_start_sync(self) -> None:
        scenario = _ACTIVE_SCENARIO.get()
        record = _ACTIVE_RECORD.get()
        configuration = _runtime_configuration()
        if configuration is None:
            raise RuntimeError("qualification_model_missing_scenario")
        if scenario is None or record is None:
            time.sleep(self._delay_seconds())
            return
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
        del stop, run_manager, kwargs
        self._record_model_start_sync()
        self._raise_selected_failure()
        return ChatResult(
            generations=[ChatGeneration(message=self._response(messages))],
        )

    async def _agenerate(
        self,
        messages: list[Any],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        await self._record_model_start_async()
        self._raise_selected_failure()
        return ChatResult(
            generations=[ChatGeneration(message=self._response(messages))],
        )


@tool("qualification_sandbox_operation", parse_docstring=True)
async def qualification_sandbox_operation(
    runtime: Runtime,
    description: str,
) -> str:
    """Run one bounded, side-effect-free sandbox operation for live qualification.

    Args:
        description: Explain the qualification operation in short words.
    """

    del description
    record = _ACTIVE_RECORD.get()
    if record is None:
        raise RuntimeError("qualification_tool_missing_run")
    from deerflow.runtime.tool_evidence import get_active_tool_receipt
    from deerflow.sandbox.tools import _bash_tool_async

    receipt = get_active_tool_receipt()
    if receipt is None or receipt.tool_name != "qualification_sandbox_operation":
        raise RuntimeError("qualification_tool_receipt_missing")

    operation = asyncio.create_task(
        _bash_tool_async(
            runtime,
            "run a bounded qualification delay",
            qualification_reconciled_operation_command(receipt.receipt_id),
        )
    )
    await asyncio.sleep(0.25)
    try:
        await qualification_counter("tool_starts", record)
        await qualification_barrier("during_tool_execution", record)
        renewal = getattr(record, "execution_lease_renewal", None)
        if callable(renewal):
            await qualification_stale_external_renewal_probe(
                record,
                renewal,
            )
        result = await operation
        await qualification_counter("tool_completions", record)
        if _scenario_for_record(record) == "terminal_before_lifecycle_commit":
            from deerflow.sandbox.accepted_material import (
                AcceptedSandboxAuthorityLostError,
            )

            await qualification_counter(
                "accepted_sandbox_raced_provider_calls",
                record,
            )
            try:
                await _bash_tool_async(
                    runtime,
                    "verify post-loss refusal",
                    qualification_reconciled_operation_command(
                        receipt.receipt_id,
                    ),
                )
            except AcceptedSandboxAuthorityLostError:
                await qualification_counter(
                    "accepted_sandbox_post_loss_rejections",
                    record,
                )
            else:
                raise RuntimeError(
                    "accepted_sandbox_post_loss_operation_was_accepted",
                )
        return result
    finally:
        if not operation.done():
            operation.cancel()


_seal_host_tool_recovery(
    qualification_sandbox_operation,
    "receipt_idempotent_reconcile_v1",
)


__all__ = [
    "QUALIFICATION_SCENARIOS",
    "KubernetesQualificationChatModel",
    "KubernetesQualificationHooks",
    "accepted_sandbox_qualification_candidate_enabled",
    "qualification_barrier",
    "qualification_counter",
    "qualification_reconciled_operation_command",
    "qualification_stale_external_renewal_probe",
    "qualification_sandbox_operation",
    "qualification_service_barrier",
    "scenario_from_external_key",
]

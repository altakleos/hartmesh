"""Canonical caller-intent equality at the durable HTTP launch boundary."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_runtime_api import GraphInputV1, InvocationEnsureRequest, InvocationOptionsV1
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.routers import runtime_api as runtime_api_router
from app.gateway.run_models import RunCreateRequest
from app.gateway.services import build_service_invocation_runtime, start_run
from app.runtime.api import InvocationRuntimeAPI
from app.runtime.idempotency import CanonicalCallerIntent, canonical_request_digest, normalize_external_key, scope_for_http
from app.runtime.invocation import InternalSourceKind, InvocationPrincipal
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime import RunManager, RunStatus
from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1, ResolvedAgentRevision
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.store.base import AdmissionOutcome
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.tenant_identity import TenantIdentityV1

_INPUT = {"messages": [{"role": "user", "content": "hello"}]}
_TEST_TENANT_IDENTITY = TenantIdentityV1.from_canonical_id("local")
_TEST_TENANT = _TEST_TENANT_IDENTITY.to_persisted_reference()


class _ReadyAdmissionFence:
    async def ready_for_admission(self) -> bool:
        return True


@pytest.fixture
def gateway_launch_harness():
    graph_store = InMemoryStore()
    run_store = MemoryRunStore()
    manager = RunManager(store=run_store, tenant=_TEST_TENANT)
    request = SimpleNamespace(
        headers={},
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
        ),
        app=SimpleNamespace(
            state=SimpleNamespace(
                runtime_readiness=_ReadyAdmissionFence(),
                stream_bridge=SimpleNamespace(),
                run_manager=manager,
                checkpointer=InMemorySaver(),
                store=graph_store,
                run_event_store=MemoryRunEventStore(),
                run_events_config=None,
                thread_store=MemoryThreadMetaStore(graph_store),
                tenant_identity=_TEST_TENANT_IDENTITY,
            )
        ),
    )
    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="default",
            storage_source="builtin",
            storage_version="v1",
            agent_config=None,
            soul="test",
            model_profile={},
        )
    )
    set_app_config(
        AppConfig.model_validate(
            {
                "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
                "models": [
                    {
                        "name": "fixture-model",
                        "use": "tests.fake:Model",
                        "model": "fixture-model",
                    }
                ],
            }
        )
    )
    with (
        patch("app.gateway.services.resolve_agent_revision", return_value=revision) as resolve_revision,
        patch("app.gateway.services.resolve_agent_factory", return_value=object()),
        patch("app.gateway.services.run_agent", new_callable=AsyncMock) as run_agent,
    ):
        yield request, manager, run_store, resolve_revision, run_agent
    reset_app_config()


async def _first_then_retry(request, original: RunCreateRequest, retry: RunCreateRequest, *, key: str, thread_id: str):
    request.headers = {"Idempotency-Key": key}
    first = await start_run(original, thread_id, request)
    assert first.task is not None
    await first.task
    return first, await start_run(retry, thread_id, request)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "original",
    [
        pytest.param(RunCreateRequest(input={**_INPUT, "payload": {"required": True}}), id="graph-input-field"),
        pytest.param(RunCreateRequest(input=_INPUT, command={"resume": {"answer": "yes"}}), id="resume-input"),
        pytest.param(RunCreateRequest(input=_INPUT, assistant_id="custom_agent"), id="agent-selection"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"model_name": "fixture-model"}), id="model-name"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"thinking_enabled": False}), id="thinking-enabled"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"mode": "fast"}), id="mode"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"reasoning_effort": "high"}), id="reasoning-effort"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"is_plan_mode": True}), id="plan-mode"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"subagent_enabled": True}), id="subagent-enabled"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"max_concurrent_subagents": 2}), id="subagent-concurrency"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"max_total_subagents": 4}), id="subagent-total"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"is_bootstrap": True, "agent_name": "bootstrap-agent"}), id="bootstrap-agent"),
        pytest.param(RunCreateRequest(input=_INPUT, config={"context": {"thinking_enabled": False}}), id="config-context"),
        pytest.param(RunCreateRequest(input=_INPUT, config={"configurable": {"checkpoint_id": "checkpoint-1"}}), id="checkpoint-selection"),
        pytest.param(RunCreateRequest(input=_INPUT, config={"recursion_limit": 150}), id="recursion-limit"),
        pytest.param(RunCreateRequest(input=_INPUT, interrupt_before=["agent"]), id="interrupt-before"),
        pytest.param(RunCreateRequest(input=_INPUT, interrupt_after=["tools"]), id="interrupt-after"),
        pytest.param(RunCreateRequest(input=_INPUT, multitask_strategy="interrupt"), id="multitask-strategy"),
    ],
)
async def test_http_replay_conflicts_when_original_execution_field_is_removed(gateway_launch_harness, original: RunCreateRequest) -> None:
    request, _manager, _store, _resolve_revision, _run_agent = gateway_launch_harness
    request.headers = {"Idempotency-Key": f"removed-{next(iter(original.model_fields_set - {'input'}), 'input')}"}
    first = await start_run(original, "thread-removal", request)
    assert first.task is not None
    await first.task

    with pytest.raises(Exception) as conflict:
        await start_run(RunCreateRequest(input=_INPUT), "thread-removal", request)

    assert getattr(conflict.value, "status_code", None) == 409


@pytest.mark.anyio
@pytest.mark.parametrize(
    "original",
    [
        pytest.param(RunCreateRequest(input=_INPUT, assistant_id=None), id="agent-selection"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"model_name": None}), id="model-name"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"thinking_enabled": None}), id="thinking-enabled"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"mode": None}), id="mode"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"reasoning_effort": None}), id="reasoning-effort"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"is_plan_mode": None}), id="plan-mode"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"subagent_enabled": None}), id="subagent-enabled"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"max_concurrent_subagents": None}), id="subagent-concurrency"),
        pytest.param(RunCreateRequest(input=_INPUT, context={"max_total_subagents": None}), id="subagent-total"),
        pytest.param(RunCreateRequest(input=_INPUT, config={"context": {"thinking_enabled": None}}), id="config-context"),
        pytest.param(RunCreateRequest(input=_INPUT, checkpoint_id=None), id="checkpoint-id"),
        pytest.param(RunCreateRequest(input=_INPUT, checkpoint=None), id="checkpoint-object"),
        pytest.param(RunCreateRequest(input=_INPUT, interrupt_before=None), id="interrupt-before"),
        pytest.param(RunCreateRequest(input=_INPUT, interrupt_after=None), id="interrupt-after"),
        pytest.param(RunCreateRequest(input=_INPUT, config={"recursion_limit": None}), id="recursion-limit"),
    ],
)
async def test_http_replay_treats_explicit_null_as_omission_for_nullable_execution_fields(gateway_launch_harness, original: RunCreateRequest) -> None:
    request, _manager, _store, resolve_revision, run_agent = gateway_launch_harness
    first, replay = await _first_then_retry(
        request,
        original,
        RunCreateRequest(input=_INPUT),
        key=f"null-equals-omitted-{next(iter(original.model_fields_set - {'input'}), 'context')}",
        thread_id="thread-null",
    )

    assert replay is first
    assert resolve_revision.call_count == 1
    assert run_agent.await_count == 1


@pytest.mark.anyio
async def test_http_replay_conflicts_when_omitted_option_becomes_explicit(gateway_launch_harness) -> None:
    request, _manager, _store, _resolve_revision, _run_agent = gateway_launch_harness
    request.headers = {"Idempotency-Key": "omitted-becomes-explicit"}
    first = await start_run(RunCreateRequest(input=_INPUT), "thread-addition", request)
    assert first.task is not None
    await first.task

    with pytest.raises(Exception) as conflict:
        await start_run(RunCreateRequest(input=_INPUT, context={"thinking_enabled": False}), "thread-addition", request)

    assert getattr(conflict.value, "status_code", None) == 409


@pytest.mark.anyio
async def test_http_replay_conflicts_when_bootstrap_agent_selection_changes(gateway_launch_harness) -> None:
    request, _manager, _store, _resolve_revision, _run_agent = gateway_launch_harness
    request.headers = {"Idempotency-Key": "bootstrap-agent-changes"}
    original = RunCreateRequest(
        input=_INPUT,
        context={"is_bootstrap": True, "agent_name": "bootstrap-one"},
    )
    first = await start_run(original, "thread-bootstrap-change", request)
    assert first.task is not None
    await first.task

    retry = RunCreateRequest(
        input=_INPUT,
        context={"is_bootstrap": True, "agent_name": "bootstrap-two"},
    )
    with pytest.raises(Exception) as conflict:
        await start_run(retry, "thread-bootstrap-change", request)

    assert getattr(conflict.value, "status_code", None) == 409


@pytest.mark.anyio
async def test_http_replay_ignores_mapping_order_and_transient_delivery_options(gateway_launch_harness) -> None:
    request, _manager, _store, resolve_revision, run_agent = gateway_launch_harness
    original = RunCreateRequest(
        input={"messages": _INPUT["messages"], "payload": {"alpha": 1, "beta": 2}},
        stream_mode="values",
        stream_subgraphs=True,
        on_disconnect="continue",
    )
    retry = RunCreateRequest(
        input={"payload": {"beta": 2, "alpha": 1}, "messages": _INPUT["messages"]},
        stream_mode="messages-tuple",
        stream_subgraphs=False,
        on_disconnect="cancel",
    )
    first, replay = await _first_then_retry(request, original, retry, key="mapping-order", thread_id="thread-mapping")

    assert replay is first
    assert resolve_revision.call_count == 1
    assert run_agent.await_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize("terminal", [RunStatus.success, RunStatus.error])
async def test_terminal_http_replay_reuses_accepted_execution_without_second_worker(gateway_launch_harness, terminal: RunStatus) -> None:
    request, manager, _store, resolve_revision, run_agent = gateway_launch_harness
    request.headers = {"Idempotency-Key": f"terminal-{terminal.value}"}
    body = RunCreateRequest(input=_INPUT, context={"thinking_enabled": False})
    first = await start_run(body, f"thread-{terminal.value}", request)
    assert first.task is not None
    await first.task
    await manager.set_status(first.run_id, terminal)

    replay = await start_run(body, f"thread-{terminal.value}", request)

    assert replay is first
    assert replay.status is terminal
    assert resolve_revision.call_count == 1
    assert run_agent.await_count == 1


@pytest.mark.anyio
async def test_legacy_keyed_row_without_caller_intent_conflicts_conservatively(gateway_launch_harness) -> None:
    request, _manager, _store, _resolve_revision, _run_agent = gateway_launch_harness
    request.headers = {"Idempotency-Key": "legacy-key"}
    body = RunCreateRequest(input=_INPUT)
    first = await start_run(body, "thread-legacy", request)
    assert first.task is not None
    await first.task
    first.caller_intent_json = None
    first.caller_intent_digest = None
    first.caller_intent_digest_version = None

    with pytest.raises(Exception) as conflict:
        await start_run(body, "thread-legacy", request)

    assert getattr(conflict.value, "status_code", None) == 409


@pytest.mark.anyio
async def test_concurrent_equal_and_unequal_caller_intents_have_one_atomic_winner() -> None:
    store = MemoryRunStore()
    common = {
        "thread_id": "thread-race",
        "owner_worker_id": "worker-1",
        "lease_expires_at": None,
        "external_scope": scope_for_http("user", "owner-1"),
        "external_key": normalize_external_key("race-key"),
        "user_id": "owner-1",
    }
    equal = CanonicalCallerIntent({"input": {"value": "same"}})

    async def admit(run_id: str, caller: CanonicalCallerIntent):
        return await store.ensure_run_atomic(
            run_id,
            request_digest=canonical_request_digest(caller.to_persisted()),
            request_digest_version="sha256-canonical-json-v1",
            caller_intent_json=caller.to_persisted(),
            caller_intent_digest=caller.digest,
            caller_intent_digest_version=caller.digest_version,
            **common,
        )

    equal_results = await asyncio.gather(admit("equal-left", equal), admit("equal-right", equal))
    assert {result.outcome for result in equal_results} == {AdmissionOutcome.created, AdmissionOutcome.known_same}
    assert len({result.row["run_id"] for result in equal_results}) == 1

    unequal_store = MemoryRunStore()
    store = unequal_store
    changed = CanonicalCallerIntent({"input": {"value": "changed"}})
    unequal_results = await asyncio.gather(admit("unequal-left", equal), admit("unequal-right", changed))
    assert {result.outcome for result in unequal_results} == {AdmissionOutcome.created, AdmissionOutcome.key_conflict}
    assert len({result.row["run_id"] for result in unequal_results}) == 1


@pytest.mark.anyio
async def test_in_process_and_http_runtime_adapters_persist_the_same_caller_intent(gateway_launch_harness) -> None:
    _request, _manager, _store, _resolve_revision, _run_agent = gateway_launch_harness

    def host(service_id: str):
        graph_store = InMemoryStore()
        manager = RunManager(
            store=MemoryRunStore(),
            tenant=_TEST_TENANT,
        )
        app = SimpleNamespace(
            state=SimpleNamespace(
                runtime_readiness=_ReadyAdmissionFence(),
                stream_bridge=SimpleNamespace(),
                run_manager=manager,
                checkpointer=InMemorySaver(),
                store=graph_store,
                run_event_store=MemoryRunEventStore(),
                run_events_config=None,
                thread_store=MemoryThreadMetaStore(graph_store),
                tenant_identity=_TEST_TENANT_IDENTITY,
            )
        )
        runtime = build_service_invocation_runtime(app, authenticated_service_id=service_id)
        adapter = InvocationRuntimeAPI(
            runtime,
            principal=InvocationPrincipal(user_id=service_id, role="service", is_internal=True),
            source_kind=InternalSourceKind.service,
            trusted_service_id=service_id,
        )
        return manager, adapter

    request = InvocationEnsureRequest(
        external_key="adapter-parity",
        thread_id="thread-adapter-parity",
        agent_hint=None,
        input=GraphInputV1(value={"payload": {"beta": 2, "alpha": 1}, **_INPUT}),
        options=InvocationOptionsV1(
            model_name="fixture-model",
            thinking_enabled=False,
            multitask_strategy="reject",
            interrupt_before=("agent",),
        ),
    )
    direct_manager, direct_adapter = host("service-direct")
    direct_result = await direct_adapter.ensure(request)
    assert direct_result.disposition.value == "created"

    http_manager, http_adapter = host("service-http")
    http_app = make_authed_test_app()
    http_app.include_router(runtime_api_router.router)
    http_app.dependency_overrides[runtime_api_router.get_runtime_api] = lambda: http_adapter
    with TestClient(http_app) as client:
        response = client.post("/api/runtime/v1/invocations/ensure", json=request.to_dict())
    assert response.status_code == 201

    direct_row = (await direct_manager.list_by_thread("thread-adapter-parity", user_id="service-direct"))[0]
    http_row = (await http_manager.list_by_thread("thread-adapter-parity", user_id="service-http"))[0]
    assert direct_row.caller_intent_json == http_row.caller_intent_json
    assert direct_row.caller_intent_digest == http_row.caller_intent_digest
    assert direct_row.request_digest != direct_row.caller_intent_digest

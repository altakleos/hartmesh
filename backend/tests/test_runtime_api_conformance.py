"""One semantic conformance suite exercised through both supported transports."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_runtime_api import (
    CancelInvocationRequest,
    ContextInvocationsQuery,
    DurableInvocationPort,
    FailureCode,
    GraphInputV1,
    InvocationEnsureRequest,
    InvocationOptionsV1,
    InvocationQuery,
    RuntimeFailure,
    record_from_dict,
)
from fastapi.testclient import TestClient
from support.runtime_api_conformance import assert_runtime_adapter_conformance

from app.gateway.auth.models import User
from app.gateway.routers import runtime_api
from app.runtime.api import InProcessInvocationRuntime
from app.runtime.deployment import GatewayDeploymentReporter
from app.runtime.invocation import (
    InternalCancelReceipt,
    InternalLaunchReceipt,
    InternalLifecycleObservation,
    InvocationAuthorizationOutcome,
    NotFoundOrInvisible,
)
from deerflow.extensions.capabilities import build_capability_manifest
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.runs.lifecycle_query import (
    CursorAhead,
    CursorGap,
    LifecyclePage,
    encode_lifecycle_cursor,
)
from deerflow.runtime.runs.manager import ConflictError, IdempotencyConflictError
from deerflow.runtime.runs.store.base import CancellationRequestOutcome


class _HealthyCapabilityMonitor:
    async def health(self):
        return ()


def _record(*, status: RunStatus = RunStatus.pending, state_version: int = 1) -> RunRecord:
    now = datetime.now(UTC).isoformat()
    return RunRecord(
        run_id="run-1",
        thread_id="thread-1",
        assistant_id="agent-1",
        status=status,
        on_disconnect=DisconnectMode.cancel,
        user_id="service-1",
        created_at=now,
        updated_at=now,
        state_version=state_version,
    )


def _empty_page() -> LifecyclePage:
    cursor = encode_lifecycle_cursor(0)
    return LifecyclePage(
        snapshots=(),
        events=(),
        next_cursor=cursor,
        minimum_available_cursor=cursor,
        read_fence_cursor=cursor,
    )


class _Runtime:
    async def launch(self, _intent):
        return InternalLaunchReceipt(_record(), created=True)

    async def observe_invocation_lifecycle(self, _query):
        return InternalLifecycleObservation(record=_record(), page=_empty_page())

    async def observe_context_lifecycle(self, _query):
        return InternalLifecycleObservation(record=None, page=_empty_page())

    async def cancel_run(self, _request):
        return InternalCancelReceipt(
            outcome=CancellationRequestOutcome.already_terminal,
            record=_record(status=RunStatus.success),
        )


def _admin_user() -> User:
    return User(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        email="runtime-conformance@example.com",
        password_hash="x",
        system_role="admin",
    )


class _HttpAdapter(DurableInvocationPort):
    """Client-side test adapter that decodes the public HTTP representation."""

    def __init__(self, delegate: InProcessInvocationRuntime) -> None:
        app = make_authed_test_app(user_factory=_admin_user)
        app.include_router(runtime_api.router)
        app.dependency_overrides[runtime_api.get_runtime_api] = lambda: delegate
        manifest = build_capability_manifest(ExtensionRegistry().build(generation=9))
        monitor = _HealthyCapabilityMonitor()
        app.state.capability_manifest = manifest
        app.state.capability_health_monitor = monitor
        app.state.deployment_reporter = GatewayDeploymentReporter(
            profile="durable_production",
            database_backend="postgres",
            atomic_lifecycle=True,
            manifest=manifest,
            health_monitor=monitor,
        )
        self._client = TestClient(app)
        self._client.__enter__()

    def close(self) -> None:
        self._client.__exit__(None, None, None)

    @staticmethod
    def _decode(response):
        payload = response.json()
        if payload.get("kind") == "runtime.error":
            return RuntimeFailure(
                code=FailureCode(payload["code"]),
                detail={"version": 1, **payload.get("details", {})},
            )
        return record_from_dict(payload)

    def capabilities(self):
        return self._decode(self._client.get("/api/runtime/v1/capabilities"))

    async def ensure(self, request):
        return self._decode(
            self._client.post(
                "/api/runtime/v1/invocations/ensure",
                json=request.to_dict(),
            )
        )

    async def observe(self, query):
        params = {"limit": query.limit}
        if query.cursor is not None:
            params["cursor"] = query.cursor
        if isinstance(query, InvocationQuery):
            path = f"/api/runtime/v1/invocations/{query.run_id}"
        else:
            path = f"/api/runtime/v1/contexts/{query.thread_id}/invocations"
        return self._decode(self._client.get(path, params=params))

    async def control(self, command):
        return self._decode(
            self._client.post(
                f"/api/runtime/v1/invocations/{command.run_id}/control",
                json=command.to_dict(),
            )
        )


def _semantic_outcome(result) -> str:
    if isinstance(result, RuntimeFailure):
        return result.code.value
    disposition = getattr(result, "disposition", None)
    if disposition is not None:
        return disposition.value
    return "observed"


def _transport_adapter(runtime, transport: str) -> DurableInvocationPort:
    in_process = InProcessInvocationRuntime(
        runtime,
        authenticated_service_id="service-1",
    )
    return in_process if transport == "in_process" else _HttpAdapter(in_process)


def _close_adapter(adapter) -> None:
    if isinstance(adapter, _HttpAdapter):
        adapter.close()


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["in_process", "http"])
async def test_runtime_transport_conformance(transport: str) -> None:
    adapter = _transport_adapter(_Runtime(), transport)
    try:
        assert isinstance(adapter, DurableInvocationPort)
        await assert_runtime_adapter_conformance(
            adapter,
            ensure=InvocationEnsureRequest(
                external_key="delivery-1",
                thread_id="thread-1",
                agent_hint="agent-1",
                input=GraphInputV1(value={"messages": []}),
                options=InvocationOptionsV1(),
            ),
            invocation_query=InvocationQuery(run_id="run-1"),
            context_query=ContextInvocationsQuery(thread_id="thread-1"),
            control=CancelInvocationRequest(
                run_id="run-1",
                expected_state_version=1,
            ),
        )
    finally:
        _close_adapter(adapter)


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["in_process", "http"])
@pytest.mark.parametrize(
    ("runtime_result", "expected"),
    [
        (InternalLaunchReceipt(_record(), created=False), "known"),
        (IdempotencyConflictError("private digest mismatch"), "conflict"),
        (ConflictError("private active thread"), "thread_busy"),
        (InvocationAuthorizationOutcome.denied, "denied"),
        (InvocationAuthorizationOutcome.indeterminate, "indeterminate"),
        (RuntimeError("ambiguous backend result"), "indeterminate"),
    ],
)
async def test_ensure_semantics_match_across_transports(
    transport: str,
    runtime_result,
    expected: str,
) -> None:
    class Runtime:
        async def launch(self, _intent):
            if isinstance(runtime_result, Exception):
                raise runtime_result
            return runtime_result

    adapter = _transport_adapter(Runtime(), transport)
    try:
        result = await adapter.ensure(
            InvocationEnsureRequest(
                external_key="delivery-1",
                thread_id="thread-1",
                agent_hint=None,
                input=GraphInputV1(value={}),
                options=InvocationOptionsV1(),
            )
        )
    finally:
        _close_adapter(adapter)

    assert _semantic_outcome(result) == expected


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["in_process", "http"])
@pytest.mark.parametrize(
    ("runtime_result", "expected"),
    [
        (NotFoundOrInvisible.not_found_or_invisible, "not_found_or_invisible"),
        (InvocationAuthorizationOutcome.denied, "denied"),
        (InvocationAuthorizationOutcome.indeterminate, "indeterminate"),
        (CursorGap(encode_lifecycle_cursor(5)), "cursor_gap"),
        (CursorAhead(encode_lifecycle_cursor(8)), "cursor_ahead"),
    ],
)
async def test_observe_semantics_match_across_transports(
    transport: str,
    runtime_result,
    expected: str,
) -> None:
    class Runtime:
        async def observe_invocation_lifecycle(self, _query):
            if isinstance(runtime_result, Exception):
                raise runtime_result
            return runtime_result

    adapter = _transport_adapter(Runtime(), transport)
    try:
        result = await adapter.observe(InvocationQuery(run_id="run-1"))
    finally:
        _close_adapter(adapter)

    assert _semantic_outcome(result) == expected


@pytest.mark.anyio
@pytest.mark.parametrize("transport", ["in_process", "http"])
@pytest.mark.parametrize(
    "outcome",
    list(CancellationRequestOutcome),
)
async def test_cancel_race_semantics_match_across_transports(
    transport: str,
    outcome: CancellationRequestOutcome,
) -> None:
    class Runtime:
        async def cancel_run(self, _request):
            record = None if outcome is CancellationRequestOutcome.not_found_or_invisible else _record(state_version=3)
            return InternalCancelReceipt(outcome=outcome, record=record)

    adapter = _transport_adapter(Runtime(), transport)
    try:
        result = await adapter.control(
            CancelInvocationRequest(
                run_id="run-1",
                expected_state_version=2,
            )
        )
    finally:
        _close_adapter(adapter)

    assert _semantic_outcome(result) == outcome.value

"""Conformance coverage for the authenticated in-process runtime adapter."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pytest
from deerflow_runtime_api import (
    CancelInvocationRequest,
    ContextInvocationsQuery,
    FailureCode,
    GraphInputV1,
    InvocationEnsureRequest,
    InvocationOptionsV1,
    InvocationQuery,
    RuntimeFailure,
)
from support.runtime_api_conformance import assert_runtime_adapter_conformance

from app.runtime.invocation import InternalLaunchReceipt
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.runs.store.base import CancellationRequestOutcome, LifecycleType


@pytest.mark.anyio
async def test_unexpected_adapter_failure_is_publicly_bounded_and_internally_correlated(
    caplog,
) -> None:
    from app.runtime.api import InProcessInvocationRuntime

    class ExplodingRuntime:
        async def observe_invocation_lifecycle(self, _query):
            raise RuntimeError("database password=never-return-this")

    adapter = InProcessInvocationRuntime(
        ExplodingRuntime(),
        authenticated_service_id="service-1",
    )

    with caplog.at_level(logging.ERROR, logger="app.runtime.api"):
        result = await adapter.observe(InvocationQuery(run_id="run-1"))

    assert isinstance(result, RuntimeFailure)
    assert result.code is FailureCode.indeterminate
    public = result.to_dict()
    assert "never-return-this" not in str(public)
    correlation_id = public["detail"]["correlation_id"]
    assert len(correlation_id) == 32
    matching = [record for record in caplog.records if getattr(record, "correlation_id", None) == correlation_id]
    assert len(matching) == 1
    assert matching[0].runtime_operation == "observe"


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


class _Runtime:
    def __init__(self) -> None:
        self.intent = None

    async def launch(self, intent):
        self.intent = intent
        return InternalLaunchReceipt(_record(), created=True)


@pytest.mark.anyio
async def test_ensure_builds_a_host_trusted_service_launch_intent() -> None:
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalSourceKind

    runtime = _Runtime()
    api = InProcessInvocationRuntime(
        runtime,
        authenticated_service_id="service-1",
    )

    receipt = await api.ensure(
        InvocationEnsureRequest(
            external_key="delivery-1",
            thread_id="thread-1",
            agent_hint="agent-1",
            input=GraphInputV1(value={"messages": [{"role": "user", "content": "hello"}]}),
            options=InvocationOptionsV1(
                model_name="fast",
                thinking_enabled=True,
                multitask_strategy="interrupt",
                checkpoint_id="checkpoint-1",
                interrupt_before=("tools",),
                interrupt_after="*",
            ),
        )
    )

    assert receipt.disposition == "created"
    assert receipt.run_id == "run-1"
    assert runtime.intent.source_kind is InternalSourceKind.service
    assert runtime.intent.trusted_service_id == "service-1"
    assert runtime.intent.external_key == "delivery-1"
    assert runtime.intent.input == {"messages": ({"role": "user", "content": "hello"},)}
    assert runtime.intent.context == {"model_name": "fast", "thinking_enabled": True}
    assert runtime.intent.multitask_strategy == "interrupt"
    assert runtime.intent.checkpoint_id == "checkpoint-1"


@pytest.mark.anyio
async def test_observe_maps_only_fixed_public_snapshot_and_event_fields() -> None:
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    class Runtime(_Runtime):
        async def observe_invocation_lifecycle(self, query):
            self.query = query
            return InternalLifecycleObservation(
                # Visibility/policy can race a later lifecycle commit. The
                # public singular state must come from the fenced page.
                record=_record(status=RunStatus.pending, state_version=1),
                page=LifecyclePage(
                    snapshots=(
                        {
                            "run_id": "run-1",
                            "thread_id": "thread-1",
                            "status": "running",
                            "state_version": 2,
                            "metadata": {"must_not_leak": True},
                            "accepted_invocation": {"must_not_leak": True},
                        },
                    ),
                    events=(
                        {
                            "event_id": "event-2",
                            "cursor": 2,
                            "run_id": "run-1",
                            "thread_id": "thread-1",
                            "owner_scope": "must-not-leak",
                            "lifecycle_type": LifecycleType.started,
                            "state_version": 2,
                            "status": "running",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "payload": {"version": 1},
                        },
                    ),
                    next_cursor=encode_lifecycle_cursor(2),
                    minimum_available_cursor=encode_lifecycle_cursor(0),
                    read_fence_cursor=encode_lifecycle_cursor(2),
                ),
            )

    runtime = Runtime()
    api = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")
    observation = await api.observe(InvocationQuery(run_id="run-1", limit=25))

    assert observation.run_id == "run-1"
    assert (observation.status, observation.state_version) == ("running", 2)
    assert observation.snapshots == (
        {
            "run_id": "run-1",
            "thread_id": "thread-1",
            "status": "running",
            "state_version": 2,
        },
    )
    assert observation.events[0]["cursor"] == encode_lifecycle_cursor(2)
    assert "owner_scope" not in observation.events[0]
    assert runtime.query.principal.user_id == "service-1"
    assert runtime.query.principal.identity.effective_subject.kind == "service"
    assert runtime.query.principal.identity.effective_subject.subject_id == "service-1"
    assert runtime.query.principal.identity.acting_service is None
    assert runtime.query.limit == 25


@pytest.mark.anyio
async def test_observe_fails_safely_when_internal_page_crosses_the_requested_run() -> None:
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    cross_context_snapshot = {
        "run_id": "run-2",
        "thread_id": "thread-2",
        "status": "running",
        "state_version": 2,
    }

    class Runtime(_Runtime):
        async def observe_invocation_lifecycle(self, _query):
            return InternalLifecycleObservation(
                record=_record(),
                page=LifecyclePage(
                    snapshots=(cross_context_snapshot,),
                    events=(),
                    next_cursor=encode_lifecycle_cursor(2),
                    minimum_available_cursor=encode_lifecycle_cursor(0),
                    read_fence_cursor=encode_lifecycle_cursor(2),
                ),
                authoritative_snapshot=cross_context_snapshot,
            )

    result = await InProcessInvocationRuntime(
        Runtime(),
        authenticated_service_id="service-1",
    ).observe(InvocationQuery(run_id="run-1"))

    assert isinstance(result, RuntimeFailure)
    assert result.code is FailureCode.indeterminate
    assert "run-2" not in str(result.to_dict())
    assert "thread-2" not in str(result.to_dict())


@pytest.mark.anyio
async def test_context_observe_uses_one_context_query_without_singular_state() -> None:
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    class Runtime(_Runtime):
        async def observe_context_lifecycle(self, query):
            self.query = query
            return InternalLifecycleObservation(
                record=None,
                page=LifecyclePage(
                    snapshots=(),
                    events=(),
                    next_cursor=encode_lifecycle_cursor(7),
                    minimum_available_cursor=encode_lifecycle_cursor(0),
                    read_fence_cursor=encode_lifecycle_cursor(7),
                ),
            )

    runtime = Runtime()
    api = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")
    observation = await api.observe(ContextInvocationsQuery(thread_id="thread-1", include_snapshot=False))

    assert observation.run_id is None
    assert observation.thread_id == "thread-1"
    assert observation.status is None
    assert observation.state_version is None
    assert runtime.query.principal.user_id == "service-1"


@pytest.mark.anyio
async def test_observe_maps_visibility_policy_and_cursor_failures_without_private_detail() -> None:
    from deerflow_runtime_api import FailureCode

    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InvocationAuthorizationOutcome, NotFoundOrInvisible
    from deerflow.runtime.runs.lifecycle_query import CursorAhead, CursorGap, InvalidLifecycleCursor, encode_lifecycle_cursor

    outcomes = [
        NotFoundOrInvisible.not_found_or_invisible,
        InvocationAuthorizationOutcome.denied,
        InvocationAuthorizationOutcome.indeterminate,
        CursorGap(encode_lifecycle_cursor(3)),
        CursorAhead(encode_lifecycle_cursor(8)),
        InvalidLifecycleCursor("private parser detail"),
    ]
    expected = [
        FailureCode.not_found_or_invisible,
        FailureCode.denied,
        FailureCode.indeterminate,
        FailureCode.cursor_gap,
        FailureCode.cursor_ahead,
        FailureCode.invalid_request,
    ]

    class Runtime(_Runtime):
        async def observe_invocation_lifecycle(self, _query):
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

    api = InProcessInvocationRuntime(Runtime(), authenticated_service_id="service-1")
    failures = [await api.observe(InvocationQuery(run_id="run-1")) for _ in expected]

    assert [failure.code for failure in failures] == expected
    assert failures[3].detail == {
        "version": 1,
        "minimum_available_cursor": encode_lifecycle_cursor(3),
    }
    assert failures[4].detail == {
        "version": 1,
        "read_fence_cursor": encode_lifecycle_cursor(8),
    }
    assert "private" not in str(failures[-1].detail)


@pytest.mark.anyio
async def test_control_is_version_fenced_and_maps_every_finite_outcome() -> None:
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalCancelReceipt

    outcomes = list(CancellationRequestOutcome)

    class Runtime(_Runtime):
        async def cancel_run(self, request):
            self.cancel_request = request
            return InternalCancelReceipt(outcome=outcomes.pop(0), record=_record(state_version=3))

    runtime = Runtime()
    api = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")
    receipts = [
        await api.control(
            CancelInvocationRequest(
                run_id="run-1",
                expected_state_version=2,
                action="rollback",
            )
        )
        for _ in CancellationRequestOutcome
    ]

    assert [receipt.disposition.value for receipt in receipts] == [outcome.value for outcome in CancellationRequestOutcome]
    assert runtime.cancel_request.expected_state_version == 2
    assert runtime.cancel_request.principal.user_id == "service-1"
    assert runtime.cancel_request.principal.identity.effective_subject.kind == "service"
    assert runtime.cancel_request.principal.identity.effective_subject.subject_id == "service-1"


def test_capabilities_are_exact_and_do_not_advertise_http_or_retirement() -> None:
    from app.runtime.api import InProcessInvocationRuntime

    api = InProcessInvocationRuntime(_Runtime(), authenticated_service_id="service-1")
    capabilities = api.capabilities()

    assert capabilities.ensure is True
    assert capabilities.observe_invocation is True
    assert capabilities.observe_context is True
    assert capabilities.controls == ("cancel",)
    assert capabilities.context_export is False
    assert capabilities.context_retirement is False


@pytest.mark.anyio
async def test_ensure_maps_replay_conflict_thread_busy_and_invalid_requests() -> None:
    from deerflow_runtime_api import FailureCode

    from app.runtime.api import InProcessInvocationRuntime
    from deerflow.runtime import ConflictError
    from deerflow.runtime.runs.manager import IdempotencyConflictError

    outcomes = [
        InternalLaunchReceipt(_record(), created=False),
        IdempotencyConflictError("private digest mismatch"),
        ConflictError("private active thread"),
        ValueError("private normalizer detail"),
    ]

    class Runtime(_Runtime):
        async def launch(self, intent):
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

    api = InProcessInvocationRuntime(Runtime(), authenticated_service_id="service-1")
    request = InvocationEnsureRequest(
        external_key="delivery-1",
        thread_id="thread-1",
        agent_hint=None,
        input=GraphInputV1(value={}),
        options=InvocationOptionsV1(),
    )

    known = await api.ensure(request)
    conflict = await api.ensure(request)
    busy = await api.ensure(request)
    invalid = await api.ensure(request)

    assert known.disposition == "known"
    assert conflict.disposition == "conflict"
    assert busy.disposition == "thread_busy"
    assert invalid.code is FailureCode.invalid_request
    assert "private" not in str(invalid.detail)


@pytest.mark.anyio
async def test_runtime_checks_visibility_then_policy_before_query_or_cancel() -> None:
    from contextlib import asynccontextmanager

    from app.runtime.invocation import (
        InternalAuthorizationDecision,
        InternalCancelRequest,
        InternalInvocationLifecycleQuery,
        InvocationPrincipal,
        InvocationRuntime,
        NotFoundOrInvisible,
    )

    calls: list[str] = []

    class Normalizer:
        def scope(self, _intent):
            raise AssertionError("launch is not part of this test")

    class Runs:
        @asynccontextmanager
        async def admission_scope(self, _thread_id):
            yield

        async def observe(self, _run_id, _principal):
            calls.append("visibility")
            return None

        async def query_lifecycle(self, _query):
            calls.append("query")
            raise AssertionError("invisible runs must not be queried")

        async def cancel(self, _request):
            calls.append("cancel")
            raise AssertionError("invisible runs must not be mutated")

    class Authorization:
        async def authorize_observe(self, *_args, **_kwargs):
            calls.append("policy")
            return InternalAuthorizationDecision.denied()

        async def authorize_cancel(self, *_args, **_kwargs):
            calls.append("cancel_policy")
            return InternalAuthorizationDecision.denied()

    runtime = InvocationRuntime(
        normalizer=Normalizer(),
        runs=Runs(),
        authorization=Authorization(),
    )
    principal = InvocationPrincipal(user_id="service-1", role="service")

    observation = await runtime.observe_invocation_lifecycle(InternalInvocationLifecycleQuery(run_id="missing", principal=principal))
    cancellation = await runtime.cancel_run(
        InternalCancelRequest(
            run_id="missing",
            principal=principal,
            expected_state_version=1,
        )
    )

    assert observation is NotFoundOrInvisible.not_found_or_invisible
    assert cancellation is NotFoundOrInvisible.not_found_or_invisible
    assert calls == ["visibility", "visibility"]


@pytest.mark.anyio
async def test_visible_policy_denial_precedes_lifecycle_read_and_cancel_mutation() -> None:
    from contextlib import asynccontextmanager

    from app.runtime.invocation import (
        InternalAuthorizationDecision,
        InternalCancelRequest,
        InternalContextLifecycleQuery,
        InternalInvocationLifecycleQuery,
        InvocationAuthorizationOutcome,
        InvocationPrincipal,
        InvocationRuntime,
    )

    calls: list[str] = []

    class Normalizer:
        def scope(self, _intent):
            raise AssertionError("launch is not part of this test")

    class Runs:
        @asynccontextmanager
        async def admission_scope(self, _thread_id):
            yield

        async def observe(self, _run_id, _principal):
            calls.append("visibility")
            return _record()

        async def context_visible(self, _thread_id, _principal):
            calls.append("context_visibility")
            return True

        async def query_lifecycle(self, _query):
            calls.append("query")
            raise AssertionError("denied observations must not read evidence")

        async def cancel(self, _request):
            calls.append("cancel")
            raise AssertionError("denied cancellations must not mutate")

    class Authorization:
        async def authorize_observe(self, *_args, **_kwargs):
            calls.append("run_policy")
            return InternalAuthorizationDecision.denied()

        async def authorize_context_observe(self, *_args, **_kwargs):
            calls.append("context_policy")
            return InternalAuthorizationDecision.denied()

        async def authorize_cancel(self, *_args, **_kwargs):
            calls.append("cancel_policy")
            return InternalAuthorizationDecision.denied()

    runtime = InvocationRuntime(
        normalizer=Normalizer(),
        runs=Runs(),
        authorization=Authorization(),
    )
    principal = InvocationPrincipal(user_id="service-1", role="service")

    run_result = await runtime.observe_invocation_lifecycle(InternalInvocationLifecycleQuery(run_id="run-1", principal=principal))
    context_result = await runtime.observe_context_lifecycle(InternalContextLifecycleQuery(thread_id="thread-1", principal=principal))
    cancel_result = await runtime.cancel_run(
        InternalCancelRequest(
            run_id="run-1",
            principal=principal,
            expected_state_version=1,
        )
    )

    assert run_result is InvocationAuthorizationOutcome.denied
    assert context_result is InvocationAuthorizationOutcome.denied
    assert cancel_result is InvocationAuthorizationOutcome.denied
    assert calls == [
        "visibility",
        "run_policy",
        "context_visibility",
        "context_policy",
        "visibility",
        "cancel_policy",
    ]


@pytest.mark.anyio
async def test_service_scope_is_derived_from_the_authenticated_host_identity() -> None:
    from types import SimpleNamespace

    from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
    from app.gateway.services import _GatewayLaunchNormalizer
    from app.runtime.idempotency import scope_for_service
    from app.runtime.invocation import InternalLaunchIntent, InternalSourceKind

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace()),
        headers={},
        state=SimpleNamespace(
            user=SimpleNamespace(
                id="service-1",
                system_role="service",
                oauth_provider=None,
                oauth_id=None,
            ),
            auth_source=AUTH_SOURCE_INTERNAL,
            principal_kind="service",
        ),
        cookies={},
    )
    normalizer = _GatewayLaunchNormalizer(
        request,
        trust_internal_launch_facts=True,
    )
    identity = await normalizer.identify(
        InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.service,
            trusted_service_id="service-1",
            external_key="delivery-1",
        )
    )

    assert identity is not None
    assert identity.external_scope == scope_for_service("service-1")
    assert identity.principal.user_id == "service-1"
    assert identity.principal.role == "service"
    with pytest.raises(ValueError, match="authenticated service identity"):
        await normalizer.identify(
            InternalLaunchIntent(
                thread_id="thread-1",
                source_kind=InternalSourceKind.service,
                trusted_service_id="forged-service",
                external_key="delivery-2",
            )
        )


@pytest.mark.anyio
async def test_in_process_adapter_passes_the_transport_neutral_conformance_suite() -> None:
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalCancelReceipt, InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    empty_page = LifecyclePage(
        snapshots=(),
        events=(),
        next_cursor=encode_lifecycle_cursor(0),
        minimum_available_cursor=encode_lifecycle_cursor(0),
        read_fence_cursor=encode_lifecycle_cursor(0),
    )

    class Runtime(_Runtime):
        async def observe_invocation_lifecycle(self, _query):
            return InternalLifecycleObservation(record=_record(), page=empty_page)

        async def observe_context_lifecycle(self, _query):
            return InternalLifecycleObservation(record=None, page=empty_page)

        async def cancel_run(self, _request):
            return InternalCancelReceipt(
                outcome=CancellationRequestOutcome.already_terminal,
                record=_record(status=RunStatus.success),
            )

    api = InProcessInvocationRuntime(Runtime(), authenticated_service_id="service-1")
    await assert_runtime_adapter_conformance(
        api,
        ensure=InvocationEnsureRequest(
            external_key="delivery-1",
            thread_id="thread-1",
            agent_hint=None,
            input=GraphInputV1(value={}),
            options=InvocationOptionsV1(),
        ),
        invocation_query=InvocationQuery(run_id="run-1"),
        context_query=ContextInvocationsQuery(thread_id="thread-1"),
        control=CancelInvocationRequest(
            run_id="run-1",
            expected_state_version=1,
        ),
    )


@pytest.mark.anyio
async def test_in_process_observe_and_cancel_use_the_durable_manager_ports() -> None:
    from types import SimpleNamespace

    from deerflow_runtime_api import FailureCode

    from app.gateway.services import _GatewayDurableRuns
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InvocationRuntime
    from deerflow.runtime import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    await store.put(
        "run-1",
        thread_id="thread-1",
        user_id="service-1",
    )
    await store.put(
        "other-run",
        thread_id="other-thread",
        user_id="other-service",
    )

    class ThreadStore:
        async def get(self, thread_id, *, user_id=None):
            return None

        async def check_access(self, thread_id, user_id, *, require_existing=False):
            return thread_id == "thread-1" and user_id == "service-1"

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                run_manager=RunManager(store=store),
                thread_store=ThreadStore(),
            )
        )
    )
    runtime = InvocationRuntime(
        normalizer=object(),
        runs=_GatewayDurableRuns(request),
    )
    api = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")

    observation = await api.observe(InvocationQuery(run_id="run-1", include_snapshot=False))
    hidden = await api.observe(InvocationQuery(run_id="other-run"))
    cancellation = await api.control(CancelInvocationRequest(run_id="run-1", expected_state_version=1))

    assert observation.run_id == "run-1"
    assert (observation.status, observation.state_version) == ("pending", 1)
    assert observation.snapshots == ()
    assert [event["lifecycle_type"] for event in observation.events] == ["accepted"]
    assert hidden.code is FailureCode.not_found_or_invisible
    assert cancellation.disposition == "requested"
    assert cancellation.state_version == 2
    assert [event["lifecycle_type"] for event in await store.list_lifecycle_events(run_id="run-1")] == [LifecycleType.accepted, LifecycleType.cancellation_requested]

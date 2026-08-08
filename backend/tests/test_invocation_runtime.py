"""Application-layer invocation lifecycle tests."""

from __future__ import annotations

import inspect
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import EffectiveSubjectV1, InvocationIdentityV1

from app.runtime.invocation import (
    InternalCancelReceipt,
    InternalCancelRequest,
    InternalLaunchIntent,
    InvocationAuthorizationOutcome,
    InvocationPrincipal,
    InvocationRuntime,
    NotFoundOrInvisible,
    PreparedLaunch,
)
from deerflow.runtime import CancelOutcome, ConflictError, DisconnectMode, RunRecord, RunStatus


def _record() -> RunRecord:
    now = datetime.now(UTC).isoformat()
    return RunRecord(
        run_id="run-1",
        thread_id="thread-1",
        assistant_id="lead_agent",
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.cancel,
        created_at=now,
        updated_at=now,
    )


def test_internal_launch_records_snapshot_and_freeze_nested_caller_values() -> None:
    from app.runtime.idempotency import canonical_request_digest

    caller_input = {"messages": [{"content": {"parts": ["original"]}}]}
    caller_callbacks = [lambda: None]
    caller_config = {
        "context": {"nested": ["original"]},
        "callbacks": caller_callbacks,
        "tags": {"one"},
    }
    caller_checkpoint = {"checkpoint_id": "checkpoint-1", "checkpoint_map": {"root": ["checkpoint-0"]}}
    caller_modes = ["values"]
    intent = InternalLaunchIntent(
        thread_id="thread-1",
        input=caller_input,
        config=caller_config,
        checkpoint=caller_checkpoint,
        interrupt_before=["tools"],
        stream_mode=caller_modes,
    )
    intent_digest = canonical_request_digest(intent.input)

    caller_input["messages"][0]["content"]["parts"][0] = "mutated"
    caller_callbacks.clear()
    caller_config["context"]["nested"].append("mutated")
    caller_config["tags"].add("mutated")
    caller_checkpoint["checkpoint_map"]["root"].append("mutated")
    caller_modes.append("debug")

    assert intent.input["messages"][0]["content"]["parts"] == ("original",)
    assert intent.config["callbacks"] != ()
    assert intent.config["context"]["nested"] == ("original",)
    assert intent.config["tags"] == frozenset({"one"})
    assert intent.checkpoint["checkpoint_map"]["root"] == ("checkpoint-0",)
    assert intent.stream_mode == ("values",)
    assert canonical_request_digest(intent.input) == intent_digest
    with pytest.raises(TypeError):
        intent.input["forged"] = True  # type: ignore[index]
    with pytest.raises(TypeError):
        intent.config["context"]["forged"] = True  # type: ignore[index]

    async def worker(_record: RunRecord) -> None:
        return None

    caller_metadata = {"labels": ["one"]}
    caller_kwargs = {"config": {"callbacks": [lambda: None]}}
    caller_intent = {"kind": "caller_intent", "version": 1, "value": {"input": ["one"]}}
    launch = PreparedLaunch(
        thread_id="thread-1",
        assistant_id=None,
        on_disconnect=DisconnectMode.cancel,
        metadata=caller_metadata,
        kwargs=caller_kwargs,
        multitask_strategy="reject",
        model_name=None,
        user_id=None,
        worker=worker,
        caller_intent_json=caller_intent,
    )
    caller_metadata["labels"].append("mutated")
    caller_kwargs["config"]["callbacks"].clear()
    caller_intent["value"]["input"].append("mutated")

    assert launch.metadata["labels"] == ("one",)
    assert len(launch.kwargs["config"]["callbacks"]) == 1
    assert launch.caller_intent_json["value"]["input"] == ("one",)
    with pytest.raises(TypeError):
        launch.kwargs["forged"] = True  # type: ignore[index]


class _Normalizer:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    @contextmanager
    def scope(self, _intent: InternalLaunchIntent):
        yield

    async def normalize(self, intent: InternalLaunchIntent) -> PreparedLaunch:
        async def worker(_record: RunRecord) -> None:
            self.events.append("execute")

        return PreparedLaunch(
            thread_id=intent.thread_id,
            assistant_id="lead_agent",
            on_disconnect=DisconnectMode.cancel,
            metadata={},
            kwargs={},
            multitask_strategy="reject",
            model_name=None,
            user_id=None,
            worker=worker,
        )


class _Runs:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.record = _record()
        self.failures: list[tuple[RunRecord, str]] = []
        self.observations: list[tuple[str, InvocationPrincipal]] = []
        self.cancellations: list[InternalCancelRequest] = []
        self.observed_record: RunRecord | None = self.record
        self.cancel_outcome = CancelOutcome.cancelled

    @asynccontextmanager
    async def admission_scope(self, _thread_id: str):
        yield

    async def prepare_admission(self, _launch: PreparedLaunch) -> None:
        self.events.append("prepare")

    async def admit(self, _launch: PreparedLaunch) -> RunRecord:
        self.events.append("admit")
        return self.record

    async def fail_start(self, _record: RunRecord, _error: str) -> None:
        self.failures.append((_record, _error))

    async def observe(
        self,
        run_id: str,
        principal: InvocationPrincipal,
    ) -> RunRecord | None:
        self.observations.append((run_id, principal))
        return self.observed_record

    async def cancel(self, request: InternalCancelRequest) -> CancelOutcome:
        self.cancellations.append(request)
        return self.cancel_outcome


class _AdmissionFence:
    def __init__(self, ready: bool) -> None:
        self.ready = ready
        self.calls = 0

    async def ready_for_admission(self) -> bool:
        self.calls += 1
        return self.ready


@pytest.mark.anyio
async def test_unready_deployment_blocks_new_invocation_before_normalization() -> None:
    events: list[str] = []
    fence = _AdmissionFence(ready=False)
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=_Runs(events),
        admission_fence=fence,
    )

    result = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert result is InvocationAuthorizationOutcome.indeterminate
    assert fence.calls == 1
    assert events == []


@pytest.mark.anyio
async def test_gateway_runtime_builder_installs_application_admission_fence() -> None:
    from app.gateway.services import (
        build_channel_invocation_runtime,
        build_invocation_runtime,
        build_scheduled_invocation_runtime,
        build_service_invocation_runtime,
    )

    fence = _AdmissionFence(ready=False)
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(runtime_readiness=fence),
        ),
        state=SimpleNamespace(user=None),
        headers={},
        cookies={},
    )

    runtimes = (
        build_invocation_runtime(request),
        build_scheduled_invocation_runtime(request.app),
        build_channel_invocation_runtime(request.app),
        build_service_invocation_runtime(
            request.app,
            authenticated_service_id="service-1",
        ),
    )

    for runtime in runtimes:
        result = await runtime.launch(
            InternalLaunchIntent(thread_id="thread-1"),
        )

        assert result is InvocationAuthorizationOutcome.indeterminate

    assert fence.calls == 4


def test_gateway_runtime_builders_require_application_admission_fence() -> None:
    from app.gateway.services import (
        build_channel_invocation_runtime,
        build_invocation_runtime,
        build_scheduled_invocation_runtime,
        build_service_invocation_runtime,
    )

    app = SimpleNamespace(state=SimpleNamespace())
    request = SimpleNamespace(
        app=app,
        state=SimpleNamespace(user=None),
        headers={},
        cookies={},
    )

    builders = (
        lambda: build_invocation_runtime(request),
        lambda: build_scheduled_invocation_runtime(app),
        lambda: build_channel_invocation_runtime(app),
        lambda: build_service_invocation_runtime(
            app,
            authenticated_service_id="service-1",
        ),
    )

    for build in builders:
        with pytest.raises(AttributeError, match="runtime_readiness"):
            build()


@pytest.mark.anyio
async def test_launch_admits_before_attaching_worker() -> None:
    events: list[str] = []
    runs = _Runs(events)
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=runs,
    )

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))
    assert receipt.record is runs.record
    assert receipt.record.task is not None
    assert events == ["prepare", "admit"]

    await receipt.record.task
    assert events == ["prepare", "admit", "execute"]
    assert runs.failures == []


@pytest.mark.anyio
async def test_attachment_failure_closes_worker_and_preserves_failure_semantics() -> None:
    events: list[str] = []
    runs = _Runs(events)
    failure = RuntimeError("task factory unavailable")

    def fail_to_attach(_worker):
        raise failure

    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=runs,
        task_factory=fail_to_attach,
    )

    with pytest.raises(RuntimeError) as exc_info:
        await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert exc_info.value is failure
    assert runs.record.task is None
    assert runs.failures == [(runs.record, "Failed to attach run worker: task factory unavailable")]


@pytest.mark.anyio
async def test_conflicting_admission_never_attaches_a_worker() -> None:
    events: list[str] = []
    conflict = ConflictError("thread is busy")

    class ConflictingRuns(_Runs):
        async def admit(self, _launch: PreparedLaunch) -> RunRecord:
            self.events.append("admit")
            raise conflict

    runs = ConflictingRuns(events)
    runtime = InvocationRuntime(normalizer=_Normalizer(events), runs=runs)

    with pytest.raises(ConflictError) as exc_info:
        await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert exc_info.value is conflict
    assert events == ["prepare", "admit"]
    assert runs.record.task is None
    assert runs.failures == []


@pytest.mark.anyio
async def test_observation_and_cancellation_delegate_with_finite_results() -> None:
    events: list[str] = []
    runs = _Runs(events)
    runtime = InvocationRuntime(normalizer=_Normalizer(events), runs=runs)
    principal = InvocationPrincipal(user_id="owner-1")

    observed = await runtime.observe_run("run-1", principal)
    cancellation = await runtime.cancel_run(InternalCancelRequest(run_id="run-1", action="rollback"))

    assert observed is runs.record
    assert runs.observations == [
        ("run-1", principal),
        ("run-1", InvocationPrincipal()),
    ]
    assert cancellation.outcome is CancelOutcome.cancelled
    assert runs.cancellations == [InternalCancelRequest(run_id="run-1", action="rollback")]

    runs.observed_record = None
    assert await runtime.observe_run("hidden-run", principal) is NotFoundOrInvisible.not_found_or_invisible


@pytest.mark.anyio
async def test_dependency_failures_propagate_without_runtime_translation() -> None:
    normalize_failure = RuntimeError("normalization failed")
    admit_failure = RuntimeError("admission failed")
    observe_failure = RuntimeError("observation failed")
    cancel_failure = RuntimeError("cancellation failed")

    class FailingNormalizer(_Normalizer):
        async def normalize(self, _intent: InternalLaunchIntent) -> PreparedLaunch:
            raise normalize_failure

    class FailingRuns(_Runs):
        async def admit(self, _launch: PreparedLaunch) -> RunRecord:
            raise admit_failure

        async def observe(
            self,
            _run_id: str,
            _principal: InvocationPrincipal,
        ) -> RunRecord | None:
            raise observe_failure

        async def cancel(self, _request: InternalCancelRequest) -> CancelOutcome:
            raise cancel_failure

    events: list[str] = []
    runs = FailingRuns(events)
    normalization_runtime = InvocationRuntime(
        normalizer=FailingNormalizer(events),
        runs=runs,
    )
    runtime = InvocationRuntime(normalizer=_Normalizer(events), runs=runs)

    with pytest.raises(RuntimeError) as exc_info:
        await normalization_runtime.launch(InternalLaunchIntent(thread_id="thread-1"))
    assert exc_info.value is normalize_failure

    with pytest.raises(RuntimeError) as exc_info:
        await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))
    assert exc_info.value is admit_failure

    with pytest.raises(RuntimeError) as exc_info:
        await runtime.observe_run("run-1", InvocationPrincipal(user_id=None))
    assert exc_info.value is observe_failure

    # Cancellation now applies the visibility boundary before mutating. Its
    # lookup failure therefore wins before the cancel dependency is reached.
    with pytest.raises(RuntimeError) as exc_info:
        await runtime.cancel_run(InternalCancelRequest(run_id="run-1"))
    assert exc_info.value is observe_failure


@pytest.mark.anyio
async def test_start_run_is_a_thin_gateway_compatibility_adapter(monkeypatch) -> None:
    from app.gateway import services
    from app.gateway.run_models import RunCreateRequest
    from app.runtime.invocation import InternalLaunchReceipt

    record = _record()
    launch = AsyncMock(return_value=InternalLaunchReceipt(record=record))
    runtime = SimpleNamespace(launch=launch)
    request = SimpleNamespace()
    monkeypatch.setattr(
        services,
        "build_invocation_runtime",
        lambda supplied_request: runtime if supplied_request is request else pytest.fail("request was not passed to the runtime factory"),
    )
    body = RunCreateRequest(
        assistant_id="lead_agent",
        input={"messages": [{"role": "user", "content": "hello"}]},
        metadata={"source": "http"},
        config={"configurable": {"checkpoint_ns": ""}},
        context={"model_name": "fast-model"},
        checkpoint_id="checkpoint-1",
        interrupt_before=["tools"],
        stream_mode=["values"],
        stream_subgraphs=True,
        on_disconnect="continue",
        multitask_strategy="rollback",
    )

    result = await services.start_run(body, "thread-1", request)

    assert result is record
    intent = launch.await_args.args[0]
    assert intent == InternalLaunchIntent(
        thread_id="thread-1",
        assistant_id="lead_agent",
        input=body.input,
        command=None,
        metadata={"source": "http"},
        config=body.config,
        context={"model_name": "fast-model"},
        checkpoint_id="checkpoint-1",
        checkpoint=None,
        interrupt_before=["tools"],
        interrupt_after=None,
        stream_mode=["values"],
        stream_subgraphs=True,
        on_disconnect="continue",
        multitask_strategy="rollback",
    )


@pytest.mark.anyio
async def test_thread_http_facade_observes_and_cancels_through_runtime(
    monkeypatch,
) -> None:
    from app.gateway.routers import thread_runs

    record = _record()
    observe_run = AsyncMock(return_value=record)
    cancel_run = AsyncMock(return_value=InternalCancelReceipt(outcome=CancelOutcome.cancelled, record=record))
    runtime = SimpleNamespace(observe_run=observe_run, cancel_run=cancel_run)
    request = SimpleNamespace()
    monkeypatch.setattr(
        thread_runs,
        "build_invocation_runtime",
        lambda supplied_request: runtime if supplied_request is request else pytest.fail("request was not passed to the runtime factory"),
    )
    monkeypatch.setattr(
        thread_runs,
        "get_current_user",
        AsyncMock(return_value="owner-1"),
    )

    response = await inspect.unwrap(thread_runs.get_run)(
        "thread-1",
        "run-1",
        request,
    )
    cancelled = await inspect.unwrap(thread_runs.cancel_run)(
        "thread-1",
        "run-1",
        request,
        wait=False,
        action="rollback",
    )

    assert response.run_id == "run-1"
    assert cancelled.status_code == 202
    assert observe_run.await_args_list[0].args == (
        "run-1",
        InvocationPrincipal(
            user_id="owner-1",
            visibility_prevalidated=True,
            identity=InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="human",
                    subject_id="owner-1",
                )
            ),
        ),
    )
    assert len(observe_run.await_args_list) == 1
    cancel_run.assert_awaited_once_with(
        InternalCancelRequest(
            run_id="run-1",
            action="rollback",
            principal=InvocationPrincipal(
                user_id="owner-1",
                visibility_prevalidated=True,
                identity=InvocationIdentityV1(
                    effective_subject=EffectiveSubjectV1(
                        kind="human",
                        subject_id="owner-1",
                    )
                ),
            ),
            thread_id="thread-1",
        )
    )

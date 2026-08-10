"""Bounded, source-aware durable invocation observation coverage."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def test_invocation_summary_is_strict_immutable_and_round_trips() -> None:
    from deerflow_runtime_api import (
        InvocationCorrelationReferenceV1,
        InvocationSummaryV1,
        record_from_dict,
    )

    summary = InvocationSummaryV1(
        run_id="run-1",
        thread_id="thread-1",
        status="running",
        state_version=2,
        source_kind="native_channel",
        correlation_references=(
            InvocationCorrelationReferenceV1(
                namespace="origin",
                key="provider_message_id",
                value="message-1",
            ),
        ),
        agent_revision_digest="a" * 64,
        extension_generation=7,
        extension_manifest_digest="b" * 64,
        caller_intent_digest="c" * 64,
        accepted_context_digest="d" * 64,
        authorization_evidence_digests=("e" * 64,),
        constraint_evidence_digest="f" * 64,
    )

    wire = summary.to_dict()
    assert record_from_dict(wire) == summary
    assert wire == {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
        "source_kind": "native_channel",
        "correlation_references": [
            {
                "namespace": "origin",
                "key": "provider_message_id",
                "value": "message-1",
                "api_version": "deerflow.runtime/v1",
                "kind": "invocation.correlation-reference.v1",
            }
        ],
        "agent_revision_digest": "a" * 64,
        "extension_generation": 7,
        "extension_manifest_digest": "b" * 64,
        "caller_intent_digest": "c" * 64,
        "accepted_context_digest": "d" * 64,
        "authorization_evidence_digests": ["e" * 64],
        "constraint_evidence_digest": "f" * 64,
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.summary.v1",
    }


@pytest.mark.parametrize("construction", ["direct", "wire"])
def test_invocation_summary_rejects_null_authorization_evidence_members(construction: str) -> None:
    from deerflow_runtime_api import InvocationSummaryV1, record_from_dict

    values = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "pending",
        "state_version": 1,
        "source_kind": "http",
        "authorization_evidence_digests": (None,),
    }
    with pytest.raises(ValueError, match="lowercase SHA-256 digest"):
        if construction == "direct":
            InvocationSummaryV1(**values)
        else:
            wire = InvocationSummaryV1(
                run_id="run-1",
                thread_id="thread-1",
                status="pending",
                state_version=1,
                source_kind="http",
            ).to_dict()
            wire["authorization_evidence_digests"] = [None]
            record_from_dict(wire)


@pytest.mark.parametrize(
    "digests",
    [
        ("a" * 64, "a" * 64),
        tuple(f"{index:064x}" for index in range(65)),
    ],
)
def test_invocation_summary_bounds_unique_authorization_evidence(digests: tuple[str, ...]) -> None:
    from deerflow_runtime_api import InvocationSummaryV1

    with pytest.raises(ValueError, match="authorization evidence"):
        InvocationSummaryV1(
            run_id="run-1",
            thread_id="thread-1",
            status="pending",
            state_version=1,
            source_kind="http",
            authorization_evidence_digests=digests,
        )


def test_correlation_reference_snapshots_nested_caller_values() -> None:
    from deerflow_runtime_api import InvocationCorrelationReferenceV1

    caller_value = ["message-1", 7, True, None]
    reference = InvocationCorrelationReferenceV1(
        namespace="origin",
        key="provider_identifiers",
        value=caller_value,
    )
    first_wire = reference.to_dict()

    caller_value.append("forged-after-construction")
    first_wire["value"].append("mutated-wire-copy")

    assert reference.value == ("message-1", 7, True, None)
    assert reference.to_dict()["value"] == ["message-1", 7, True, None]
    with pytest.raises(AttributeError):
        reference.value.append("cannot-mutate")


def test_observation_carries_typed_summaries_and_reads_legacy_wire() -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationSummaryV1, record_from_dict

    summary = InvocationSummaryV1(
        run_id="run-1",
        thread_id="thread-1",
        status="pending",
        state_version=1,
        source_kind="http",
    )
    observation = InvocationObservation(
        run_id="run-1",
        thread_id="thread-1",
        status="pending",
        state_version=1,
        snapshots=(
            {
                "run_id": "run-1",
                "thread_id": "thread-1",
                "status": "pending",
                "state_version": 1,
            },
        ),
        events=(),
        next_cursor="lc1.next",
        minimum_available_cursor="lc1.minimum",
        read_fence_cursor="lc1.fence",
        summaries=(summary,),
    )

    assert record_from_dict(observation.to_dict()) == observation
    legacy_wire = observation.to_dict()
    legacy_wire.pop("summaries")
    legacy = record_from_dict(legacy_wire)
    assert isinstance(legacy, InvocationObservation)
    assert legacy.summaries == ()


def test_context_source_filter_is_strict_and_backward_readable() -> None:
    from deerflow_runtime_api import ContextInvocationsQuery, record_from_dict

    query = ContextInvocationsQuery(
        thread_id="thread-1",
        source_kind="scheduled_task",
    )
    assert record_from_dict(query.to_dict()) == query
    with pytest.raises(ValueError, match="source kind"):
        ContextInvocationsQuery(thread_id="thread-1", source_kind="forged")

    legacy_wire = query.to_dict()
    legacy_wire.pop("source_kind")
    assert record_from_dict(legacy_wire) == ContextInvocationsQuery(thread_id="thread-1")


def test_observation_page_and_lifecycle_payload_limits_are_finite() -> None:
    from deerflow_runtime_api import ContextInvocationsQuery, InvocationObservation

    with pytest.raises(ValueError, match="between 1 and 500"):
        ContextInvocationsQuery(thread_id="thread-1", limit=501)
    with pytest.raises(ValueError, match="payload"):
        InvocationObservation(
            run_id=None,
            thread_id="thread-1",
            status=None,
            state_version=None,
            snapshots=(),
            events=(
                {
                    "event_id": "event-1",
                    "cursor": "lc1.cursor",
                    "run_id": "run-1",
                    "thread_id": "thread-1",
                    "lifecycle_type": "accepted",
                    "state_version": 1,
                    "status": "pending",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "payload": {"version": 1, "oversized": "x" * 5000},
                },
            ),
            next_cursor="lc1.next",
            minimum_available_cursor="lc1.minimum",
            read_fence_cursor="lc1.fence",
        )


def _accepted_fields(source_kind: str, source_id: str) -> dict[str, object]:
    return {
        "origin_json": {
            "version": 1,
            "source_kind": source_kind,
            "references": {"source_id": source_id},
            "contributor_references": [
                {
                    "contribution_id": "routing",
                    "namespace": "provider",
                    "key": "delivery_id",
                    "value": f"delivery-{source_id}",
                    "storage_class": "persistable",
                    "purpose": "correlation",
                },
                {
                    "contribution_id": "secrets",
                    "namespace": "provider",
                    "key": "credential",
                    "value": "must-not-be-public",
                    "storage_class": "persistable",
                    "purpose": "secret_handle",
                },
            ],
        },
        "agent_revision_digest": "a" * 64,
        "extension_generation": 4,
        "accepted_context_digest": "b" * 64,
        "caller_intent_json": {"kind": "caller_intent", "version": 1, "value": {}},
        "caller_intent_digest": "c" * 64,
        "caller_intent_digest_version": "caller-intent-canonical-json-v1",
        "decision_evidence_json": {
            "version": 1,
            "decisions": [{"evidence_digest": "d" * 64}],
            "constraints": {
                "evidence_digest": "e" * 64,
                "projection_digest": "f" * 64,
            },
            "capability_manifest": {"version": 1, "generation": 4, "digest": "1" * 64},
        },
    }


@pytest.mark.anyio
async def test_context_page_materializes_only_visible_filtered_event_run_summaries() -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
    from deerflow.runtime.runs.store.base import lifecycle_owner_scope
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    for index, source_kind in enumerate(("native_channel", "scheduled_task", "native_channel", "http", "service")):
        await store.put(
            f"run-{index}",
            thread_id="thread-1",
            user_id="owner-1",
            status="success",
            **_accepted_fields(source_kind, str(index)),
        )
    await store.put(
        "other-owner",
        thread_id="thread-1",
        user_id="owner-2",
        status="success",
        **_accepted_fields("native_channel", "hidden"),
    )

    page = await store.query_lifecycle(
        LifecycleQuery(
            thread_id="thread-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
            source_kind="native_channel",
            limit=3,
        )
    )

    page_run_ids = tuple(dict.fromkeys(event["run_id"] for event in page.events))
    assert page_run_ids == ("run-0", "run-2")
    assert tuple(summary["run_id"] for summary in page.summaries) == page_run_ids
    assert page.snapshots == tuple(
        {
            "run_id": summary["run_id"],
            "thread_id": summary["thread_id"],
            "status": summary["status"],
            "state_version": summary["state_version"],
        }
        for summary in page.summaries
    )
    first = page.summaries[0]
    assert first["source_kind"] == "native_channel"
    assert first["correlation_references"] == (
        {"namespace": "origin", "key": "source_id", "value": "0"},
        {
            "namespace": "routing:provider",
            "key": "delivery_id",
            "value": "delivery-0",
        },
    )
    assert first["extension_manifest_digest"] == "1" * 64
    assert first["authorization_evidence_digests"] == ("d" * 64,)
    assert first["constraint_evidence_digest"] == "e" * 64
    assert "must-not-be-public" not in str(first)

    all_visible = await store.query_lifecycle(
        LifecycleQuery(
            thread_id="thread-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
            source_kind="native_channel",
            limit=10,
        )
    )
    assert "other-owner" not in {event["run_id"] for event in all_visible.events}
    assert "other-owner" not in {summary["run_id"] for summary in all_visible.summaries}


@pytest.mark.anyio
async def test_legacy_row_without_accepted_origin_remains_observable_without_a_summary() -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    await store.put("legacy-run", thread_id="legacy-thread", user_id="owner-1")

    page = await store.query_lifecycle(LifecycleQuery(run_id="legacy-run"))

    assert page.snapshots == (
        {
            "run_id": "legacy-run",
            "thread_id": "legacy-thread",
            "status": "pending",
            "state_version": 1,
        },
    )
    assert page.summaries == ()


@pytest.mark.anyio
async def test_filtered_cursor_continues_across_insertions_and_reports_pruning_gap() -> None:
    from deerflow.runtime.runs.lifecycle_query import CursorGap, LifecycleQuery
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    store = MemoryRunStore()
    await store.put(
        "channel-1",
        thread_id="thread-1",
        user_id="owner-1",
        **_accepted_fields("native_channel", "message-1"),
    )
    await store.put(
        "http-1",
        thread_id="thread-1",
        user_id="owner-1",
        **_accepted_fields("http", "request-1"),
    )
    first = await store.query_lifecycle(LifecycleQuery(thread_id="thread-1", source_kind="native_channel", limit=1))
    assert [event["run_id"] for event in first.events] == ["channel-1"]
    assert first.next_cursor == first.read_fence_cursor

    await store.put(
        "channel-2",
        thread_id="thread-1",
        user_id="owner-1",
        **_accepted_fields("native_channel", "message-2"),
    )
    second = await store.query_lifecycle(
        LifecycleQuery(
            thread_id="thread-1",
            source_kind="native_channel",
            cursor=first.next_cursor,
            limit=1,
        )
    )
    assert [event["run_id"] for event in second.events] == ["channel-2"]
    assert [summary["run_id"] for summary in second.summaries] == ["channel-2"]

    await store.prune_lifecycle_through(second.next_cursor)
    with pytest.raises(CursorGap) as gap:
        await store.query_lifecycle(
            LifecycleQuery(
                thread_id="thread-1",
                source_kind="native_channel",
                cursor=first.next_cursor,
            )
        )
    assert gap.value.minimum_available_cursor == second.next_cursor


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("source_kind", "expected"),
    [
        (
            "native_channel",
            (
                ("channel_user_id", "sender-1"),
                ("chat_id", "chat-1"),
                ("connection_id", "connection-1"),
                ("provider", "telegram"),
                ("provider_message_id", "message-1"),
                ("topic_id", "topic-1"),
                ("workspace_id", "workspace-1"),
            ),
        ),
        (
            "scheduled_task",
            (
                ("task_id", "task-1"),
                ("task_run_id", "occurrence-1"),
                ("trigger", "scheduled"),
            ),
        ),
        ("http", ()),
        ("service", (("service_id", "embedded-1"),)),
    ],
)
async def test_summary_projects_each_production_source_mapping_and_safe_correlation(
    source_kind: str,
    expected: tuple[tuple[str, str], ...],
) -> None:
    from app.gateway.services import _base_origin_references
    from app.runtime.invocation import InternalLaunchIntent, InternalNativeChannelFacts, InternalSourceKind
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    if source_kind == "native_channel":
        intent = InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.native_channel,
            native_channel=InternalNativeChannelFacts(
                provider="telegram",
                connection_id="connection-1",
                workspace_id="workspace-1",
                chat_id="chat-1",
                topic_id="topic-1",
                provider_message_id="message-1",
                channel_user_id="sender-1",
                resolved_assistant_id="lead_agent",
                resolved_agent_name=None,
            ),
        )
    elif source_kind == "scheduled_task":
        intent = InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.scheduled_task,
            trusted_task_id="task-1",
            task_run_id="occurrence-1",
            scheduled_trigger="scheduled",
        )
    elif source_kind == "service":
        intent = InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.service,
            trusted_service_id="embedded-1",
        )
    else:
        intent = InternalLaunchIntent(thread_id="thread-1")

    fields = _accepted_fields(source_kind, "unused")
    fields["origin_json"] = {
        "version": 1,
        "source_kind": source_kind,
        "references": _base_origin_references(intent),
        "contributor_references": [],
    }
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="owner-1", **fields)

    page = await store.query_lifecycle(LifecycleQuery(run_id="run-1"))

    summary = page.summaries[0]
    assert summary["source_kind"] == source_kind
    assert tuple((reference["key"], reference["value"]) for reference in summary["correlation_references"]) == expected


@pytest.mark.anyio
async def test_sql_context_page_loads_summary_rows_only_for_bounded_page_ids(tmp_path) -> None:
    from deerflow.persistence.base import Base
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
    from deerflow.runtime.runs.store.base import lifecycle_owner_scope

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'summaries.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    class SummaryLoadSpy(RunRepository):
        def __init__(self, session_factory):
            super().__init__(session_factory)
            self.loaded_run_ids: list[tuple[str, ...]] = []

        async def _load_lifecycle_summary_rows(self, session, *, run_ids):
            self.loaded_run_ids.append(run_ids)
            return await super()._load_lifecycle_summary_rows(session, run_ids=run_ids)

    store = SummaryLoadSpy(factory)
    try:
        for index in range(12):
            source_kind = "native_channel" if index % 2 == 0 else "http"
            await store.put(
                f"run-{index:02d}",
                thread_id="thread-long",
                user_id="owner-1",
                status="success",
                **_accepted_fields(source_kind, str(index)),
            )

        page = await store.query_lifecycle(
            LifecycleQuery(
                thread_id="thread-long",
                owner_scope=lifecycle_owner_scope("owner-1"),
                source_kind="native_channel",
                limit=3,
            )
        )

        expected_ids = tuple(dict.fromkeys(event["run_id"] for event in page.events))
        assert expected_ids == ("run-00", "run-02")
        assert store.loaded_run_ids == [expected_ids]
        assert tuple(summary["run_id"] for summary in page.summaries) == expected_ids
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_in_process_observation_maps_source_filter_and_typed_summaries() -> None:
    from deerflow_runtime_api import ContextInvocationsQuery, InvocationObservation

    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    class Runtime:
        async def observe_context_lifecycle(self, query):
            self.query = query
            cursor = encode_lifecycle_cursor(1)
            return InternalLifecycleObservation(
                record=None,
                page=LifecyclePage(
                    snapshots=({"run_id": "run-1", "thread_id": "thread-1", "status": "pending", "state_version": 1},),
                    summaries=(
                        {
                            "run_id": "run-1",
                            "thread_id": "thread-1",
                            "status": "pending",
                            "state_version": 1,
                            "source_kind": "native_channel",
                            "correlation_references": ({"namespace": "origin", "key": "provider_message_id", "value": "message-1"},),
                            "agent_revision_digest": "a" * 64,
                            "extension_generation": 2,
                            "extension_manifest_digest": "b" * 64,
                            "caller_intent_digest": "c" * 64,
                            "accepted_context_digest": "d" * 64,
                            "authorization_evidence_digests": ("e" * 64,),
                            "constraint_evidence_digest": "f" * 64,
                        },
                    ),
                    events=(),
                    next_cursor=cursor,
                    minimum_available_cursor=encode_lifecycle_cursor(0),
                    read_fence_cursor=cursor,
                ),
            )

    runtime = Runtime()
    adapter = InProcessInvocationRuntime(runtime, authenticated_service_id="service-1")
    result = await adapter.observe(
        ContextInvocationsQuery(
            thread_id="thread-1",
            source_kind="native_channel",
        )
    )

    assert isinstance(result, InvocationObservation)
    assert result.summaries[0].source_kind == "native_channel"
    assert result.summaries[0].correlation_references[0].value == "message-1"
    assert runtime.query.source_kind == "native_channel"


@pytest.mark.anyio
async def test_in_process_observation_rejects_null_authorization_evidence_instead_of_coercing_it() -> None:
    from deerflow_runtime_api import ContextInvocationsQuery, FailureCode, RuntimeFailure

    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    class Runtime:
        async def observe_context_lifecycle(self, _query):
            cursor = encode_lifecycle_cursor(1)
            return InternalLifecycleObservation(
                record=None,
                page=LifecyclePage(
                    snapshots=({"run_id": "run-1", "thread_id": "thread-1", "status": "pending", "state_version": 1},),
                    summaries=(
                        {
                            "run_id": "run-1",
                            "thread_id": "thread-1",
                            "status": "pending",
                            "state_version": 1,
                            "source_kind": "http",
                            "correlation_references": (),
                            "authorization_evidence_digests": (None,),
                        },
                    ),
                    events=(),
                    next_cursor=cursor,
                    minimum_available_cursor=encode_lifecycle_cursor(0),
                    read_fence_cursor=cursor,
                ),
            )

    result = await InProcessInvocationRuntime(Runtime(), authenticated_service_id="service-1").observe(ContextInvocationsQuery(thread_id="thread-1"))

    assert isinstance(result, RuntimeFailure)
    assert result.code is FailureCode.indeterminate


@pytest.mark.anyio
@pytest.mark.parametrize("store_kind", ["memory", "sql"])
async def test_memory_and_sql_pages_satisfy_portable_observation_relationships(
    store_kind: str,
    tmp_path,
) -> None:
    from deerflow_runtime_api import ContextInvocationsQuery, InvocationObservation

    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery

    engine = None
    if store_kind == "memory":
        from deerflow.runtime.runs.store.memory import MemoryRunStore

        store = MemoryRunStore()
    else:
        from deerflow.persistence.base import Base
        from deerflow.persistence.run.sql import RunRepository

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'portable-observation.db'}")
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))

    try:
        await store.put(
            "run-1",
            thread_id="thread-1",
            user_id="owner-1",
            **_accepted_fields("http", "request-1"),
        )
        page = await store.query_lifecycle(LifecycleQuery(thread_id="thread-1"))

        class Runtime:
            async def observe_context_lifecycle(self, _query):
                return InternalLifecycleObservation(record=None, page=page)

        observation = await InProcessInvocationRuntime(
            Runtime(),
            authenticated_service_id="owner-1",
        ).observe(ContextInvocationsQuery(thread_id="thread-1"))

        assert isinstance(observation, InvocationObservation)
        assert tuple(snapshot["run_id"] for snapshot in observation.snapshots) == ("run-1",)
        assert tuple(summary.run_id for summary in observation.summaries) == ("run-1",)
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
async def test_http_and_in_process_observations_round_trip_identically() -> None:
    from _router_auth_helpers import make_authed_test_app
    from deerflow_runtime_api import (
        ContextInvocationsQuery,
        InvocationObservation,
        record_from_dict,
    )
    from fastapi.testclient import TestClient

    from app.gateway.routers import runtime_api
    from app.runtime.api import InProcessInvocationRuntime
    from app.runtime.invocation import InternalLifecycleObservation
    from deerflow.runtime.runs.lifecycle_query import LifecyclePage, encode_lifecycle_cursor

    class Runtime:
        async def observe_context_lifecycle(self, query):
            cursor = encode_lifecycle_cursor(1)
            return InternalLifecycleObservation(
                record=None,
                page=LifecyclePage(
                    snapshots=(
                        {
                            "run_id": "run-1",
                            "thread_id": "thread-1",
                            "status": "success",
                            "state_version": 3,
                        },
                    ),
                    summaries=(
                        {
                            "run_id": "run-1",
                            "thread_id": "thread-1",
                            "status": "success",
                            "state_version": 3,
                            "source_kind": "scheduled_task",
                            "correlation_references": (),
                            "agent_revision_digest": None,
                            "extension_generation": None,
                            "extension_manifest_digest": None,
                            "caller_intent_digest": None,
                            "accepted_context_digest": None,
                            "authorization_evidence_digests": (),
                            "constraint_evidence_digest": None,
                        },
                    ),
                    events=(),
                    next_cursor=cursor,
                    minimum_available_cursor=encode_lifecycle_cursor(0),
                    read_fence_cursor=cursor,
                ),
            )

    query = ContextInvocationsQuery(
        thread_id="thread-1",
        source_kind="scheduled_task",
        limit=1,
    )
    in_process = InProcessInvocationRuntime(Runtime(), authenticated_service_id="service-1")
    expected = await in_process.observe(query)
    assert isinstance(expected, InvocationObservation)

    class Adapter:
        async def observe(self, query):
            self.query = query
            return expected

    adapter = Adapter()
    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: adapter
    with TestClient(app) as client:
        response = client.get(
            "/api/runtime/v1/contexts/thread-1/invocations",
            params={"source_kind": "scheduled_task", "limit": 1},
        )

    assert response.status_code == 200
    assert record_from_dict(response.json()) == expected
    assert adapter.query == query


def test_http_rejects_an_unknown_source_filter_without_calling_the_adapter() -> None:
    from _router_auth_helpers import make_authed_test_app
    from fastapi.testclient import TestClient

    from app.gateway.routers import runtime_api

    class Adapter:
        async def observe(self, query):
            raise AssertionError("invalid source filters must stop at the transport boundary")

    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: Adapter()
    with TestClient(app) as client:
        response = client.get(
            "/api/runtime/v1/contexts/thread-1/invocations",
            params={"source_kind": "forged"},
        )

    assert response.status_code == 422
    assert response.json()["code"] == "invalid_request"

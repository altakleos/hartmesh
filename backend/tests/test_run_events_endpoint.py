"""The /events route forwards task_id + after_seq to the store (#3779).

The subtask card pages through one subagent task's persisted steps via these
query params; this locks the wiring so a rename/typo can't silently drop them
(which would make reload backfill fetch the whole run again, or nothing).
"""

import hashlib
from types import SimpleNamespace
from unittest import mock

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_extension_api import TenantReferenceV1
from fastapi.testclient import TestClient
from langchain_core.messages import HumanMessage

from deerflow.agents.middlewares.dynamic_context_middleware import DynamicContextMiddleware
from deerflow.retrieval import RetrievalObservationDraftV1, RetrievalObservationV1
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.journal import RunJournal
from deerflow.runtime.tool_evidence import DurableToolReceiptV1, ToolAttemptContextV1


@pytest.mark.anyio
async def test_list_run_events_forwards_task_id_and_after_seq():
    from app.gateway.routers.thread_runs import list_run_events

    calls: dict = {}

    class FakeStore:
        async def list_events(self, thread_id, run_id, *, event_types=None, task_id=None, limit=500, after_seq=None):
            calls.update(thread_id=thread_id, run_id=run_id, event_types=event_types, task_id=task_id, limit=limit, after_seq=after_seq)
            return [{"seq": 1, "event_type": "subagent.step"}]

    class FakeState:
        run_event_store = FakeStore()

    class FakeApp:
        state = FakeState()

    class FakeRequest:
        app = FakeApp()
        _deerflow_test_bypass_auth = True

    result = await list_run_events(
        thread_id="t1",
        run_id="r1",
        request=FakeRequest(),
        event_types="subagent.start,subagent.step,subagent.end",
        task_id="task-A",
        limit=500,
        after_seq=7,
    )

    assert result == [{"seq": 1, "event_type": "subagent.step"}]
    assert calls["task_id"] == "task-A"
    assert calls["after_seq"] == 7
    assert calls["event_types"] == ["subagent.start", "subagent.step", "subagent.end"]


@pytest.mark.anyio
async def test_retrieval_observation_endpoint_exposes_only_safe_projection():
    from app.gateway.routers.thread_runs import list_retrieval_observations

    tenant = TenantReferenceV1(
        version=1,
        public_ref="tenant-" + "d" * 16,
        digest="d" * 64,
    )
    started = DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id="run-1",
            execution_task_id="run-1",
            execution_kind="lead",
            subagent_name=None,
            tool_call_id="call-1",
            attempt=1,
            owner_id="worker-1",
            lease_epoch=1,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            extension_generation=1,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest=None,
            tenant=tenant,
        ),
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )
    terminal = started.outcome(
        phase="succeeded",
        result_projection_digest="f" * 64,
        result_kind="tool_message",
        safe_error_code=None,
    )
    observation = RetrievalObservationV1.finalize(
        terminal,
        RetrievalObservationDraftV1(
            tenant_ref=tenant.public_ref,
            tenant_digest=tenant.digest,
            run_id="run-1",
            receipt_id=started.receipt_id,
            attempt=1,
            provider_id="serply",
            tool_kind="web_search",
            adapter_capability_version="serply-http-v1",
            policy_digest="1" * 64,
            safe_constraints={
                "version": 1,
                "provider_id": "serply",
                "collection_public_refs": [],
                "domain_scope": "provider_default",
                "recency_days": None,
                "max_results": 2,
                "max_item_bytes": 1_024,
                "max_aggregate_bytes": 4_096,
                "timeout_ms": 2_000,
                "allow_redirects": False,
                "accept_partial": False,
                "source_schemes": ["https"],
                "policy_digest": "1" * 64,
            },
            started_at=started.occurred_at,
            provider_finished_at=started.occurred_at,
            provider_status="success",
            safe_reason=None,
            result_count=1,
            source_count=1,
            source_references=("https://example.com",),
            truncated=False,
            partial=False,
            safe_provider_request_ref=None,
            tool_plane_base_revision_digest="2" * 64,
            tool_plane_user_overlay_digest="3" * 64,
            tool_plane_projection_digest="4" * 64,
            tool_plane_effective_digest="5" * 64,
        ),
    )

    class FakeStore:
        async def list_events(self, *args, **kwargs):
            assert kwargs["event_types"] == ["retrieval.observation.v1"]
            return [
                {
                    "thread_id": "thread-1",
                    "run_id": "run-1",
                    "event_type": "retrieval.observation.v1",
                    "category": "tool",
                    "content": observation.to_event_body(),
                    "metadata": {},
                    "seq": 9,
                    "created_at": terminal.occurred_at.isoformat(),
                }
            ]

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(run_event_store=FakeStore())),
        _deerflow_test_bypass_auth=True,
    )
    response = await list_retrieval_observations(
        thread_id="thread-1",
        run_id="run-1",
        request=request,
        limit=20,
        after_seq=None,
    )

    encoded = str(response)
    assert response["items"][0]["result_projection_digest"] == "f" * 64
    assert response["items"][0]["source_references"] == ["https://example.com"]
    assert "query" not in encoded
    assert "result text" not in encoded


@pytest.mark.anyio
async def test_retrieval_observation_endpoint_pages_by_event_sequence():
    from app.gateway.routers import thread_runs

    calls: list[tuple[int, int | None]] = []

    class FakeStore:
        async def list_events(self, *args, **kwargs):
            calls.append((kwargs["limit"], kwargs["after_seq"]))
            after_seq = kwargs["after_seq"] or 0
            return [
                {
                    "thread_id": "thread-1",
                    "run_id": "run-1",
                    "event_type": "retrieval.observation.v1",
                    "content": {"test_seq": seq},
                    "seq": seq,
                }
                for seq in (11, 14)
                if seq > after_seq
            ][: kwargs["limit"]]

    class FakeObservation:
        def __init__(self, seq: int) -> None:
            self.draft = SimpleNamespace(
                run_id="run-1",
                tenant_digest="d" * 64,
            )
            self._seq = seq

        def to_public_projection(self):
            return {"observation_id": f"ro_{self._seq}"}

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(run_event_store=FakeStore())),
        _deerflow_test_bypass_auth=True,
    )

    def decode(content):
        return FakeObservation(content["test_seq"])

    with mock.patch.object(
        thread_runs.RetrievalObservationV1,
        "from_event_body",
        side_effect=decode,
    ):
        first = await thread_runs.list_retrieval_observations(
            thread_id="thread-1",
            run_id="run-1",
            request=request,
            limit=1,
            after_seq=None,
        )
        second = await thread_runs.list_retrieval_observations(
            thread_id="thread-1",
            run_id="run-1",
            request=request,
            limit=1,
            after_seq=first["next_after_seq"],
        )

    assert [item["event_seq"] for item in first["items"]] == [11]
    assert first["next_after_seq"] == 11
    assert [item["event_seq"] for item in second["items"]] == [14]
    assert second["next_after_seq"] is None
    assert calls == [(2, None), (2, 11)]


@pytest.mark.anyio
async def test_list_run_events_redacts_historical_run_start_metadata():
    from app.gateway.routers.thread_runs import list_run_events

    stored_row = {
        "seq": 1,
        "event_type": "run.start",
        "metadata": {
            "caller": "lead_agent",
            "auth_token": "legacy-secret",
            "token_usage": 7,
        },
    }

    class FakeStore:
        async def list_events(self, thread_id, run_id, *, event_types=None, task_id=None, limit=500, after_seq=None):
            return [stored_row]

    class FakeState:
        run_event_store = FakeStore()

    class FakeApp:
        state = FakeState()

    class FakeRequest:
        app = FakeApp()
        _deerflow_test_bypass_auth = True

    events = await list_run_events(
        thread_id="legacy-thread",
        run_id="legacy-run",
        request=FakeRequest(),
        event_types=None,
        task_id=None,
        limit=500,
        after_seq=None,
    )

    assert events[0]["metadata"] == {
        "caller": "lead_agent",
        "token_usage": 7,
    }
    assert stored_row["metadata"]["auth_token"] == "legacy-secret"
    assert events[0] is not stored_row


@pytest.mark.anyio
async def test_effective_memory_flows_from_injection_to_the_existing_debug_api():
    """The production run-events route is the field-level consumer for M1."""
    from app.gateway.routers.thread_runs import list_run_events

    store = MemoryRunEventStore()
    journal = RunJournal("r1", "t1", store, flush_threshold=100)
    runtime = SimpleNamespace(context={"__run_journal": journal})
    memory = "<memory>\nUser prefers Python.\n</memory>\n"

    with (
        mock.patch("deerflow.agents.lead_agent.prompt._get_memory_context", return_value=memory),
        mock.patch("deerflow.agents.middlewares.dynamic_context_middleware.datetime") as mock_dt,
    ):
        mock_dt.now.return_value.strftime.return_value = "2026-05-08, Friday"
        update = DynamicContextMiddleware().before_agent(
            {"messages": [HumanMessage(content="Hi", id="msg-1")]},
            runtime,
        )
    await journal.flush()

    class FakeState:
        run_event_store = store

    class FakeApp:
        state = FakeState()

    class FakeRequest:
        app = FakeApp()
        _deerflow_test_bypass_auth = True

    events = await list_run_events(
        thread_id="t1",
        run_id="r1",
        request=FakeRequest(),
        event_types="context:memory",
        task_id=None,
        limit=500,
        after_seq=None,
    )

    effective_content = update["messages"][1].content
    assert events[0]["content"] == {"content_sha256": hashlib.sha256(effective_content.encode("utf-8")).hexdigest()}


def test_memory_observation_metadata_requires_run_owner_access() -> None:
    from app.gateway.routers import thread_runs

    app = make_authed_test_app(owner_check_passes=False)
    app.include_router(thread_runs.router)

    with TestClient(app) as client:
        response = client.get(
            "/api/threads/another-users-thread/runs/run-1/events",
            params={"event_types": "memory.observation.v1"},
        )

    assert response.status_code == 404


def test_retrieval_observations_require_run_owner_access_and_bound_page_size() -> None:
    from app.gateway.routers import thread_runs

    app = make_authed_test_app(owner_check_passes=False)
    app.include_router(thread_runs.router)

    with TestClient(app) as client:
        denied = client.get("/api/threads/another-users-thread/runs/run-1/retrieval-observations")
        oversized = client.get(
            "/api/threads/another-users-thread/runs/run-1/retrieval-observations",
            params={"limit": 101},
        )

    assert denied.status_code == 404
    assert oversized.status_code == 422

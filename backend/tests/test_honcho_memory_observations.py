"""Bounded durable observations for mutable Honcho contextual memory."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from deerflow.agents.memory.backends.honcho.client import HonchoClient, HonchoRequestError
from deerflow.agents.memory.backends.honcho.config import HonchoConfig
from deerflow.agents.memory.backends.honcho.honcho_manager import HonchoMemoryManager
from deerflow.agents.memory.honcho_tenant import project_honcho_backend_config
from deerflow.agents.memory.manager import MemoryManagerError
from deerflow.agents.memory.observations import (
    bind_memory_observation_sink,
    observe_honcho_memory,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.journal import RunJournal
from deerflow.runtime.memory_observation import MemoryObservationV1
from deerflow.runtime.tenant_identity import TenantIdentityV1


class _Client:
    def __init__(self) -> None:
        self.representation = "Useful <memory> context"
        self.search_results: list[dict[str, object]] = [
            {
                "content": "A bounded <hit>",
                "peer_id": "peer-safe",
                "session_id": "session-safe",
                "created_at": "2026-08-31T00:00:00Z",
            }
        ]
        self.fail_representation: BaseException | None = None

    def working_representation(self, workspace: str, peer_id: str, *, max_conclusions: int = 25) -> str:
        if self.fail_representation is not None:
            raise self.fail_representation
        return self.representation

    def search(self, workspace: str, query: str, *, limit: int = 5) -> list[dict[str, object]]:
        return self.search_results

    def get_or_create_peer(self, workspace: str, peer_id: str) -> None:
        return None

    def get_or_create_session(self, workspace: str, session_id: str) -> None:
        return None

    def set_session_peers(self, workspace: str, session_id: str, peer_ids: list[str]) -> None:
        return None

    def add_messages(self, workspace: str, session_id: str, messages: list[dict[str, str]]) -> None:
        return None

    def close(self) -> None:
        return None


def _manager(
    *,
    failure_policy: str = "fail_open",
    max_injection_chars: int = 6000,
    message_char_limit: int = 8000,
) -> tuple[HonchoMemoryManager, _Client, TenantIdentityV1]:
    identity = TenantIdentityV1.from_canonical_id("customer-alpha")
    config = project_honcho_backend_config(
        {
            "base_url": "http://honcho.test",
            "failure_policy": {"read": failure_policy},
            "max_injection_chars": max_injection_chars,
            "message_char_limit": message_char_limit,
        },
        tenant_identity=identity,
        deployment_profile="durable_production",
    )
    manager = HonchoMemoryManager.from_config(
        config,
        memory_observer=observe_honcho_memory,
    )
    client = _Client()
    manager._client = client
    return manager, client, identity


def _journal(identity: TenantIdentityV1) -> tuple[RunJournal, MemoryRunEventStore]:
    store = MemoryRunEventStore(tenant=identity.to_persisted_reference())
    return RunJournal("run-1", "thread-1", store, flush_threshold=100), store


class _FailingObservationStore(MemoryRunEventStore):
    async def put_batch(self, events):  # type: ignore[no-untyped-def,override]
        if any(event.get("event_type") == "memory.observation.v1" for event in events):
            raise RuntimeError("event store unavailable")
        return await super().put_batch(events)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("failure_policy", "expected_error"),
    [("fail_open", False), ("fail_closed", True)],
)
async def test_read_waits_for_actual_observation_persistence(
    failure_policy: str,
    expected_error: bool,
) -> None:
    manager, _client, identity = _manager(failure_policy=failure_policy)
    store = _FailingObservationStore(tenant=identity.to_persisted_reference())
    journal = RunJournal("run-1", "thread-1", store, flush_threshold=100)

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        if expected_error:
            with pytest.raises(MemoryManagerError, match="honcho_memory_recall_failed"):
                await manager.aget_context("alice@example.com")
        else:
            assert await manager.aget_context("alice@example.com") == ""

    assert (
        await store.list_events(
            "thread-1",
            "run-1",
            event_types=["memory.observation.v1"],
        )
        == []
    )


@pytest.mark.anyio
async def test_success_observation_digests_exact_sanitized_projection() -> None:
    manager, client, identity = _manager()
    journal, store = _journal(identity)

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        projected = await manager.aget_context("alice@example.com")

    assert projected == "Useful &lt;memory&gt; context"
    events = await store.list_events("thread-1", "run-1", event_types=["memory.observation.v1"])
    assert len(events) == 1
    content = events[0]["content"]
    assert content["operation"] == "get_context"
    assert content["status"] == "succeeded"
    assert content["safe_projection_digest"] == hashlib.sha256(projected.encode("utf-8")).hexdigest()
    assert content["item_count"] == 1
    assert content["truncated"] is False
    assert content["tenant"] == identity.to_persisted_reference().to_json()
    assert re.fullmatch(r"honcho-workspace-[0-9a-f]{24}", content["workspace_ref"])
    rendered = repr(content)
    assert client.representation not in rendered
    assert "alice@example.com" not in rendered


@pytest.mark.anyio
async def test_legacy_run_has_no_synthetic_negative_memory_observation() -> None:
    identity = TenantIdentityV1.from_canonical_id("customer-alpha")
    journal, store = _journal(identity)
    journal.record_memory_context(content_sha256=hashlib.sha256(b"legacy memory was injected").hexdigest())
    await journal.flush()

    observations = await store.list_events(
        "thread-1",
        "run-1",
        event_types=["memory.observation.v1"],
    )
    legacy_context = await store.list_events(
        "thread-1",
        "run-1",
        event_types=["context:memory"],
    )
    assert observations == []
    assert len(legacy_context) == 1


@pytest.mark.anyio
async def test_empty_and_failed_reads_record_honest_status_without_provider_details() -> None:
    manager, client, identity = _manager()
    journal, store = _journal(identity)

    client.representation = ""
    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        assert await manager.aget_context("alice@example.com") == ""
    client.fail_representation = RuntimeError("provider body SECRET query=private")
    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        assert await manager.aget_context("alice@example.com") == ""

    events = await store.list_events("thread-1", "run-1", event_types=["memory.observation.v1"])
    assert [event["content"]["status"] for event in events] == ["empty", "failed_open"]
    assert all(event["content"]["safe_projection_digest"] is None for event in events)
    rendered = repr(events)
    assert "SECRET" not in rendered
    assert "private" not in rendered


@pytest.mark.anyio
async def test_search_with_only_empty_content_records_empty() -> None:
    manager, client, identity = _manager()
    journal, store = _journal(identity)
    client.search_results = [{"content": ""}]

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        assert await manager.asearch("query", user_id="alice@example.com") == []

    events = await store.list_events(
        "thread-1",
        "run-1",
        event_types=["memory.observation.v1"],
    )
    assert events[0]["content"]["status"] == "empty"
    assert events[0]["content"]["item_count"] == 0


@pytest.mark.anyio
async def test_fail_closed_records_status_then_raises_safe_contract_error() -> None:
    manager, client, identity = _manager(failure_policy="fail_closed")
    journal, store = _journal(identity)
    client.fail_representation = RuntimeError("raw provider response must disappear")

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        with pytest.raises(MemoryManagerError, match="honcho_memory_recall_failed") as raised:
            await manager.aget_context("alice@example.com")

    assert "raw provider" not in str(raised.value)
    assert raised.value.__cause__ is None
    events = await store.list_events("thread-1", "run-1", event_types=["memory.observation.v1"])
    assert [event["content"]["status"] for event in events] == ["failed_closed"]


@pytest.mark.anyio
async def test_truncation_and_retry_produce_separate_observations() -> None:
    manager, client, identity = _manager(max_injection_chars=12)
    journal, store = _journal(identity)
    client.representation = "<memory>" + "x" * 100

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        first = await manager.aget_context("alice@example.com")
        second = await manager.aget_context("alice@example.com")

    assert first == second
    assert len(first) == 12
    events = await store.list_events("thread-1", "run-1", event_types=["memory.observation.v1"])
    assert len(events) == 2
    assert all(event["content"]["truncated"] is True for event in events)
    assert all(event["content"]["safe_projection_digest"] == hashlib.sha256(first.encode("utf-8")).hexdigest() for event in events)
    assert events[0]["content"]["occurred_at"] != events[1]["content"]["occurred_at"]


def test_observation_append_failure_discards_read_under_fail_open() -> None:
    manager, _client, _identity = _manager()
    manager._memory_observer = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("sink unavailable"))
    assert manager.get_context("alice@example.com") == ""


def test_observation_append_failure_raises_under_fail_closed() -> None:
    manager, _client, _identity = _manager(failure_policy="fail_closed")
    manager._memory_observer = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("sink unavailable"))
    with pytest.raises(MemoryManagerError, match="honcho_memory_recall_failed"):
        manager.get_context("alice@example.com")


def test_ordinary_call_without_durable_binding_remains_unchanged() -> None:
    manager, _client, _identity = _manager()
    assert manager.get_context("alice@example.com") == "Useful &lt;memory&gt; context"


@pytest.mark.anyio
async def test_offloaded_observation_returns_to_the_owner_event_loop() -> None:
    manager, _client, identity = _manager()
    owner_thread = threading.get_ident()
    observed_threads: list[int] = []

    class Sink:
        async def persist_memory_observations(self, observations: object) -> None:
            observed_threads.append(threading.get_ident())

    with bind_memory_observation_sink(Sink(), identity.to_persisted_reference()):
        assert await manager.aget_context("alice@example.com")

    assert observed_threads == [owner_thread]


@pytest.mark.anyio
async def test_search_and_add_observations_never_persist_query_or_content() -> None:
    manager, _client, identity = _manager()
    journal, store = _journal(identity)

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        results = await manager.asearch("private customer query", user_id="alice@example.com")
        await manager.aadd(
            "thread-raw",
            [SimpleNamespace(type="human", content="private conversation content")],
            user_id="alice@example.com",
        )

    events = await store.list_events("thread-1", "run-1", event_types=["memory.observation.v1"])
    assert [event["content"]["operation"] for event in events] == ["search", "add"]
    canonical_results = json.dumps(results, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    assert events[0]["content"]["safe_projection_digest"] == hashlib.sha256(canonical_results).hexdigest()
    assert events[1]["content"]["safe_projection_digest"] is None
    rendered = repr(events)
    assert "private customer query" not in rendered
    assert "private conversation content" not in rendered
    assert "alice@example.com" not in rendered
    assert "thread-raw" not in rendered


@pytest.mark.anyio
async def test_exact_write_limit_is_not_reported_as_truncated() -> None:
    manager, _client, identity = _manager(message_char_limit=5)
    journal, store = _journal(identity)

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        await manager.aadd(
            "thread-raw",
            [SimpleNamespace(type="human", content="12345")],
            user_id="alice@example.com",
        )

    events = await store.list_events("thread-1", "run-1", event_types=["memory.observation.v1"])
    assert events[0]["content"]["truncated"] is False


def test_search_result_count_is_bounded_independently_of_requested_top_k() -> None:
    manager, client, _identity = _manager(max_injection_chars=10_000)
    client.search_results = [{"content": str(index)} for index in range(150)]

    results = manager.search("query", top_k=1000, user_id="alice@example.com")

    assert len(results) == 100


def test_observation_contract_rejects_unbounded_item_counts() -> None:
    identity = TenantIdentityV1.from_canonical_id("customer-alpha")
    journal, _store = _journal(identity)

    with bind_memory_observation_sink(journal, identity.to_persisted_reference()):
        with pytest.raises(ValueError, match="item_count"):
            observe_honcho_memory(
                workspace="workspace-safe",
                operation="search",
                status="succeeded",
                safe_projection=None,
                item_count=101,
                truncated=False,
            )


@pytest.mark.parametrize("forged_version", [True, 1.0, "1"])
def test_observation_contract_version_is_a_strict_integer(
    forged_version: object,
) -> None:
    tenant = TenantIdentityV1.from_canonical_id("customer-alpha").to_persisted_reference()

    with pytest.raises(ValueError, match="version"):
        MemoryObservationV1(
            version=forged_version,  # type: ignore[arg-type]
            backend="honcho",
            tenant=tenant,
            workspace_ref=f"honcho-workspace-{'a' * 24}",
            operation="get_context",
            status="empty",
            safe_projection_digest=None,
            item_count=0,
            truncated=False,
            occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
        )


def test_safe_diagnostics_report_optional_isolation_posture_and_probe_state() -> None:
    manager, client, identity = _manager()

    before = manager.safe_diagnostics()
    assert before["backend"] == "honcho"
    assert before["selected"] is True
    assert before["initialized"] is True
    assert before["durable_dependency"] is False
    assert before["dependency_role"] == "mutable_contextual_memory"
    assert before["tenant_public_ref"] == identity.public_ref
    assert before["isolation_mode"] == "tenant_user"
    assert before["transport_security"] == "http_without_credentials"
    assert before["read_failure_policy"] == "fail_open"
    assert before["last_successful_probe_at"] is None
    assert before["last_failed_probe_at"] is None
    assert before["operational_status"] == "not_observed"

    assert manager.get_context("alice@example.com")
    after_success = manager.safe_diagnostics()
    assert after_success["last_successful_probe_at"] is not None
    assert after_success["last_error_code"] is None
    assert after_success["operational_status"] == "available"

    client.fail_representation = RuntimeError("provider SECRET")
    assert manager.get_context("alice@example.com") == ""
    after_failure = manager.safe_diagnostics()
    assert after_failure["last_failed_probe_at"] is not None
    assert after_failure["last_error_code"] == "honcho_memory_recall_failed"
    assert after_failure["operational_status"] == "degraded"
    rendered = repr(after_failure)
    assert "honcho.test" not in rendered
    assert "SECRET" not in rendered


def test_client_errors_omit_provider_body_path_workspace_query_and_api_key() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="provider body SECRET")

    config = HonchoConfig.from_backend_config(
        {
            "base_url": "https://honcho.example.test",
            "api_key": "api-key-SECRET",
        }
    )
    client = HonchoClient(config, transport=httpx.MockTransport(handler))

    with pytest.raises(HonchoRequestError) as raised:
        client.search("workspace-private", "query-private")
    rendered = str(raised.value)
    assert rendered == "honcho_request_failed"
    assert "SECRET" not in rendered
    assert "workspace-private" not in rendered
    assert "query-private" not in rendered

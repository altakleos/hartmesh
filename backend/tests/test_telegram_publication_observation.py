"""Bounded exact observation over real SQL and actual recorded HTTP owner.

Remote protocol rows/proofs here are synthetic. They test matching, scope,
negotiation and custody, not remote SDK output provenance or actual scan facts.
"""

import asyncio
from dataclasses import FrozenInstanceError
from hashlib import sha256

import httpx
import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_coordinator import saved_marks

from app.telegram_bots import publication
from app.telegram_bots.publication import PublicationObservation, PublicationRefused
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, build_unavailable_inventory_evidence, output_receipt_digest

pytestmark = pytest.mark.asyncio
ACK = {"X-DeerFlow-Channel-Execution-Version": "1", "X-DeerFlow-Output-Inventory-Version": "2"}


def channel(record):
    return {"version": 1, "publication_id": record.publication_id, "owner_user_id": record.scope.owner_user_id, "thread_id": record.scope.thread_id, "assistant_id": record.scope.assistant_id, "agent_name": record.scope.agent_name}


def run_row(record, *, run_id="coordinator-run", publication_id=None, created="2026-10-25T12:00:00Z"):
    proof = channel(record)
    if publication_id is not None:
        proof["publication_id"] = publication_id
    return {"run_id": run_id, "thread_id": record.scope.thread_id, "assistant_id": record.scope.assistant_id, "status": "running", "created_at": created, "kwargs": {"channel_execution": proof}}


def output_projection(record, *, pending=False, incomplete=False, nonempty=False, failed=False, text="**Exact synthetic answer**"):
    scope = record.scope
    output = {
        "version": 1,
        "owner_user_id": scope.owner_user_id,
        "thread_id": scope.thread_id,
        "run_id": "coordinator-run",
        "run_status": "running" if pending else "error" if failed else "success",
        "state": "pending" if pending else "unavailable" if failed else "complete",
        "channel_execution": channel(record),
        "output_evidence": None,
        "source": None,
        "reason": "failed" if failed else None,
    }
    if pending:
        return {"version": 2, "output": output, "inventory": {"state": "pending", "evidence": None, "document": None, "reason": None}}
    receipt = {"version": 1, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "run_id": "coordinator-run", "input_message_id": "synthetic-human", "kind": "unavailable" if failed else "text"}
    if failed:
        receipt["reason"] = "failed"
        evidence = build_unavailable_inventory_evidence(receipt, status="error", reason="prestart_not_enumerated")
        inventory = {"state": "unavailable", "evidence": evidence.model_dump(mode="json"), "document": None, "reason": evidence.reason}
    else:
        receipt.update(assistant_message_id="synthetic-assistant", event_seq=3, text_sha256=sha256(text.encode("utf-8")).hexdigest())
        output["source"] = {"text": text}
        document = {
            "version": 1,
            "profile": "terminal-presentation-v1",
            "owner_user_id": scope.owner_user_id,
            "thread_id": scope.thread_id,
            "run_id": "coordinator-run",
            "input_message_id": "synthetic-human",
            "output_receipt_sha256": output_receipt_digest(receipt),
            "root_epoch": "44067627-570c-4c90-9503-48f14b80627f",
            "coverage": "incomplete" if incomplete else "complete",
            "reason": "scan_incomplete" if incomplete else None,
            "observed_items" if incomplete else "items": [],
            "scan": {
                "state": "complete",
                "source": "managed_outputs",
                "scope": {"version": 1, "kind": "managed_outputs_regular_changes", "virtual_root": "/mnt/user-data/outputs", "excluded_dir_names": [".git", "tool_output"]},
                "before_root": "opened",
                "after_root": "opened",
                "produced_paths": [],
            },
        }
        if nonempty:
            document["items"].append({"kind": "file", "path": "/mnt/user-data/outputs/file.txt", "surfaces": ["artifact_reference"], "witnesses": [{"kind": "callback", "callback_run_id": "77c52cff-c7f0-4c98-8df2-597b806f9781"}]})
        evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=4, document=document)
        inventory = {"state": "matched", "evidence": evidence.model_dump(mode="json"), "document": document, "reason": None}
    output["output_evidence"] = receipt
    return {"version": 2, "output": output, "inventory": inventory}


async def marked(e):
    return await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)


async def serve(e, record, *, pages=None, output=None, run=None):
    pages = pages if pages is not None else [{"data": [run_row(record)], "has_more": False, "next_before_created_at": None, "next_before_run_id": None}]
    observed_pages = []

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            observed_pages.append(dict(request.url.params))
            return httpx.Response(200, headers=ACK, json=pages[min(len(observed_pages) - 1, len(pages) - 1)])
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=output) if output is not None else httpx.Response(404, headers=ACK, json={})
        if "/runs/" in request.url.path:
            return httpx.Response(200, headers=ACK, json=run if run is not None else run_row(record))
        raise AssertionError("Unexpected controlled remote request")

    e.responder = respond
    return observed_pages


async def test_completed_empty_run_search_waits_without_another_handoff(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    await serve(e, record, pages=[{"data": [], "has_more": False, "next_before_created_at": None, "next_before_run_id": None}])
    before = e.bus.inbound_queue.qsize()
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert (result.state, result.reason) == ("pending", "awaiting_run")
    assert result.record.run_id is None and e.bus.inbound_queue.qsize() == before
    assert all(row.run_id is None for row in await saved_marks(e))
    assert not any(method == "POST" for method, _, _ in e.requests)
    await serve(e, record, output=output_projection(record, pending=True))
    later = await e.coordinator.observe(record, e.link, max_pages=1)
    assert later.state == "pending" and later.record.run_id == "coordinator-run"
    assert e.bus.inbound_queue.qsize() == before
    assert not any(method == "POST" for method, _, _ in e.requests)


async def test_seeded_protocol_complete_empty_result_uses_actual_v2_sql_primary(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    output = output_projection(record)
    await serve(e, record, output=output)
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert result.state == "planned" and result.record.run_id == "coordinator-run"
    async with e.db.sessions() as session:
        row = await session.get(TelegramOutboxRow, result.outbox_id)
        assert row.payload["reply_basis"]["version"] == 2
        assert row.payload["reply_basis"]["outcome"]["projection"] == output
        assert row.dedupe_key == "reply:" + record.turn_id
    assert all(row.route_complete and row.outbox_id == result.outbox_id for row in await saved_marks(e))
    replay = await e.coordinator.observe(result.record, e.link, max_pages=1)
    assert replay.outbox_id == result.outbox_id
    assert not any(method == "POST" for method, _, _ in e.requests)


async def test_partial_page_with_match_never_binds_before_uniqueness(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    candidate = run_row(record)
    await serve(e, record, pages=[{"data": [candidate], "has_more": True, "next_before_created_at": candidate["created_at"], "next_before_run_id": candidate["run_id"]}])
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert result.state == "budget_exhausted"
    assert all(row.run_id is None for row in await saved_marks(e))
    assert not any(path.endswith("/output") for _, path, _ in e.requests)


async def test_two_actual_protocol_matches_refuse_binding(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    first, second = run_row(record, run_id="newer"), run_row(record, run_id="older", created="2026-10-25T11:00:00Z")
    await serve(e, record, pages=[{"data": [first, second], "has_more": False, "next_before_created_at": None, "next_before_run_id": None}])
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert (result.state, result.reason) == ("held", "ambiguous_run")
    assert all(row.run_id is None for row in await saved_marks(e))


@pytest.mark.parametrize("case", ["pending", "incomplete", "nonempty", "failed", "missing"])
async def test_unsupported_or_unavailable_output_does_not_consume_primary(coordinator_environment, case):
    e = coordinator_environment
    record = await marked(e)
    output = None if case == "missing" else output_projection(record, **{case: True})
    await serve(e, record, output=output)
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert result.state == ("pending" if case == "pending" else "held")
    marks = await saved_marks(e)
    assert all(row.run_id == "coordinator-run" and not row.route_complete and row.outbox_id is None for row in marks)


async def test_exact_run_binding_is_saved_only_after_complete_traversal(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    other = run_row(record, run_id="other", publication_id="d" * 32)
    target = run_row(record, created="2026-10-25T11:00:00Z")
    pages = [{"data": [other], "has_more": True, "next_before_created_at": other["created_at"], "next_before_run_id": other["run_id"]}, {"data": [target], "has_more": False, "next_before_created_at": None, "next_before_run_id": None}]
    queried = await serve(e, record, pages=pages, output=output_projection(record, pending=True))
    result = await e.coordinator.observe(record, e.link, max_pages=2)
    assert result.state == "pending" and result.record.run_id == "coordinator-run"
    assert len(queried) == 2 and queried[0] == {"limit": "100"}
    assert queried[1]["before_run_id"] == "other"


async def test_bound_run_cannot_switch_to_another_publication(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    bound = await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)
    await serve(e, record, run=run_row(record, publication_id="d" * 32), output=output_projection(record))
    result = await e.coordinator.observe(bound, e.link, max_pages=1)
    assert result.state == "held" and not any(path.endswith("/output") for _, path, _ in e.requests)


async def test_same_borrowed_owner_reads_after_creating_shutdown(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)
    await serve(e, record, output=output_projection(record))
    e.coordinator.begin_shutdown()
    e.host.close_creation()
    e.creating[0] = False
    e.channel.application.running = False
    e.http.close_run_admission()
    result = await e.coordinator.read_output(record, e.link)
    assert result.version == 2 and result.output.source.text == "**Exact synthetic answer**"
    assert e.http.closed is False


async def test_only_acknowledged_inventory_miss_is_none(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)
    await serve(e, record)
    assert await e.coordinator.read_output(record, e.link) is None

    async def unacknowledged(request):
        return httpx.Response(404, json={})

    e.responder = unacknowledged
    with pytest.raises(PublicationRefused):
        await e.coordinator.read_output(record, e.link)


@pytest.mark.parametrize("pages", [True, 0, 9, 1.0, "1", None])
async def test_page_bounds_refuse_before_collaborators(coordinator_environment, pages):
    e = coordinator_environment
    record = await marked(e)
    before = len(e.requests)
    with pytest.raises(PublicationRefused):
        await e.coordinator.observe(record, e.link, max_pages=pages)
    assert len(e.requests) == before


async def test_observation_is_frozen_and_closed(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    result = PublicationObservation("pending", record, reason="awaiting_run")
    with pytest.raises(FrozenInstanceError):
        result.reason = "awaiting_output"
    with pytest.raises(PublicationRefused):
        PublicationObservation("held", record, outbox_id="f" * 32, reason="source_unavailable")


async def test_read_timeout_retires_prompt_network_and_leaves_owner_owned(coordinator_environment, monkeypatch):
    e = coordinator_environment
    record = await marked(e)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def held(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    e.responder = held
    monkeypatch.setattr(publication, "OUTPUT_READ_TIMEOUT_SECONDS", 0.05)
    with pytest.raises(PublicationRefused):
        await e.coordinator.read_output(record, e.link)
    assert entered.is_set() and cancelled.is_set() and e.http.closed is False


async def test_observation_deadline_drains_committed_bind_exit_before_return(coordinator_environment, monkeypatch):
    e = coordinator_environment
    record = await marked(e)
    await serve(e, record, output=output_projection(record, pending=True))
    entered, release = asyncio.Event(), asyncio.Event()
    binding = [False]
    actual_wait = asyncio.wait

    class HeldExitSession(AsyncSession):
        async def __aexit__(self, *args):
            result = await super().__aexit__(*args)
            if binding[0]:
                entered.set()
                await release.wait()
            return result

    e.inbox.session_factory = async_sessionmaker(e.db.engine, class_=HeldExitSession, expire_on_commit=False)
    actual_bind = e.inbox.bind_publication_run

    async def bind(*args, **kwargs):
        binding[0] = True
        try:
            return await actual_bind(*args, **kwargs)
        finally:
            binding[0] = False

    async def controlled_deadline(tasks, *, timeout=None, **kwargs):
        if timeout is not None and any(task.get_name() == "telegram-publication-operation" for task in tasks):
            # Deliver a short logical deadline only once actual SQL committed.
            # Native commit/close already happened; exit bookkeeping is held.
            await asyncio.wait_for(entered.wait(), 5)
            timeout = 0.03
        return await actual_wait(tasks, timeout=timeout, **kwargs)

    monkeypatch.setattr(e.inbox, "bind_publication_run", bind)
    monkeypatch.setattr(asyncio, "wait", controlled_deadline)
    operation = asyncio.create_task(e.coordinator.observe(record, e.link, max_pages=1))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        done, _ = await actual_wait({operation}, timeout=0.08)
        assert not done and e.http.closed is False
        assert all(row.run_id == "coordinator-run" for row in await saved_marks(e))
        assert not any(path.endswith("/output") for _, path, _ in e.requests)
        release.set()
        result = await operation
        assert result.state == "budget_exhausted"
        assert all(not row.route_complete and row.outbox_id is None for row in await saved_marks(e))
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.parametrize("retirement", ["coordinator_stop", "root_read_current"])
async def test_buffered_output_cannot_cross_final_public_handoff_retirement(coordinator_environment, monkeypatch, retirement):
    e = coordinator_environment
    record = await marked(e)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)
    await serve(e, record, output=output_projection(record))
    entered, release = asyncio.Event(), asyncio.Event()
    actual_wait = asyncio.wait

    async def after_child(tasks, *, timeout=None, **kwargs):
        result = await actual_wait(tasks, timeout=timeout, **kwargs)
        if timeout is not None and result[0] and any(task.get_name() == "telegram-publication-operation" for task in tasks):
            # Actual request/decoder/post-response authority work has settled.
            # Hold only the public waiter's final handoff; no native-leak claim.
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(asyncio, "wait", after_child)
    operation = asyncio.create_task(e.coordinator.read_output(record, e.link))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if retirement == "coordinator_stop":
            await e.coordinator.stop()
        else:
            e.reading[0] = False
        release.set()
        with pytest.raises(PublicationRefused):
            await operation
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.parametrize("retirement", ["coordinator_stop", "root_read_current"])
async def test_buffered_observation_refuses_after_retirement_but_keeps_primary(coordinator_environment, monkeypatch, retirement):
    e = coordinator_environment
    record = await marked(e)
    await serve(e, record, output=output_projection(record))
    entered, release = asyncio.Event(), asyncio.Event()
    actual_wait = asyncio.wait

    async def after_child(tasks, *, timeout=None, **kwargs):
        result = await actual_wait(tasks, timeout=timeout, **kwargs)
        if timeout is not None and result[0] and any(task.get_name() == "telegram-publication-operation" for task in tasks):
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(asyncio, "wait", after_child)
    operation = asyncio.create_task(e.coordinator.observe(record, e.link, max_pages=1))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        committed = await saved_marks(e)
        assert all(row.run_id == "coordinator-run" and row.route_complete and row.outbox_id for row in committed)
        if retirement == "coordinator_stop":
            await e.coordinator.stop()
        else:
            e.reading[0] = False
        release.set()
        with pytest.raises(PublicationRefused):
            await operation
        assert await saved_marks(e) == committed
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.parametrize("method", ["read_output", "observe"])
async def test_queued_operation_cannot_start_new_capture_after_absolute_deadline(coordinator_environment, monkeypatch, method):
    e = coordinator_environment
    record = await marked(e)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="coordinator-run", guard=attempt.read_guard)
    await serve(e, record, output=output_projection(record))
    queued, waiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    actual_create, actual_wait, actual_capture = asyncio.create_task, asyncio.wait, e.host.capture
    captured = []

    async def capture(*args, **kwargs):
        captured.append(True)
        return await actual_capture(*args, **kwargs)

    def delayed_child(coro, *, name=None, **kwargs):
        if name != "telegram-publication-operation":
            return actual_create(coro, name=name, **kwargs)

        async def start():
            queued.set()
            try:
                await release.wait()
                return await coro
            finally:
                coro.close()

        return actual_create(start(), name=name, **kwargs)

    async def delayed_parent_timer(tasks, *, timeout=None, **kwargs):
        if timeout is not None and any(task.get_name() == "telegram-publication-operation" for task in tasks):
            # Reproduce delayed parent scheduling without blocking the loop or
            # leaving a real provider/SQL operation unowned.
            waiting.set()
            await release.wait()
        return await actual_wait(tasks, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "create_task", delayed_child)
    monkeypatch.setattr(asyncio, "wait", delayed_parent_timer)
    monkeypatch.setattr(publication, "OUTPUT_READ_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(publication, "OBSERVATION_TIMEOUT_SECONDS", 0.02)
    coordinator = publication.PublicationCoordinator(inbox=e.inbox, outbox=e.outbox, bus=e.bus, http_owner=e.http, capture_attempt=capture)
    before = len(e.requests)
    operation = actual_create(coordinator.read_output(record, e.link) if method == "read_output" else coordinator.observe(record, e.link, max_pages=1))
    try:
        await asyncio.wait_for(queued.wait(), 5)
        await asyncio.wait_for(waiting.wait(), 5)
        await asyncio.sleep(0.05)
        assert not captured
        release.set()
        try:
            result = await operation
        except PublicationRefused:
            assert method == "read_output"
        else:
            if method == "observe":
                assert result.state == "budget_exhausted"
        assert not captured, "A queued child started fresh host capture after its logical budget already elapsed"
        assert len(e.requests) == before
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await coordinator.stop()

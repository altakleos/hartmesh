"""Real SQL inventory sources and strict route composition; seeded proof is labelled."""

import asyncio
import importlib
import json
from types import SimpleNamespace

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store  # noqa: F401
from _run_output_store_test_support import RAW_OWNER
from sqlalchemy import delete, update
from test_output_inventory import inventory_document, output_receipt

from deerflow.persistence.models.run_event import RunEventRow
from deerflow.runtime.runs.output import OutputEvidenceError, TextOutputReceipt
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, inventory_document_digest, output_receipt_digest
from deerflow.runtime.user_context import reset_current_user, set_current_user


def source_api():
    return importlib.import_module("deerflow.runtime.runs.output_inventory_source")


def source_pair():
    receipt = TextOutputReceipt.model_validate({**output_receipt().model_dump(mode="json"), "owner_user_id": RAW_OWNER})
    document = inventory_document(owner_user_id=RAW_OWNER, output_receipt_sha256=output_receipt_digest(receipt))
    return receipt, document


async def put_inventory(e, document, *, run_id="run", category="outputs", event_type="run.output_inventory", owner=RAW_OWNER):
    token = set_current_user(SimpleNamespace(id=owner))
    try:
        return await e.event_store.put_if_absent(thread_id="thread", run_id=run_id, event_type=event_type, category=category, content=document)
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_actual_sql_inventory_singleton_is_exact(capture_environment):
    e = capture_environment
    receipt, document = source_pair()
    row, created = await put_inventory(e, document)
    assert created
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=row["seq"], document=document)
    audit = await source_api().read_durable_inventory_source(e.event_store, evidence=evidence, output_receipt=receipt)
    assert audit.state == "matched" and inventory_document_digest(audit.document) == evidence.inventory_sha256
    assert audit.document.items == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "changed", "malformed", "wrong-category"])
async def test_successful_missing_differs_from_malformed_or_changed(capture_environment, change):
    e = capture_environment
    receipt, document = source_pair()
    row, _ = await put_inventory(e, document)
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=row["seq"], document=document)
    async with e.sf.begin() as session:
        if change == "missing":
            await session.execute(delete(RunEventRow).where(RunEventRow.seq == row["seq"]))
        elif change == "wrong-category":
            await session.execute(update(RunEventRow).where(RunEventRow.seq == row["seq"]).values(category="trace"))
        else:
            altered = {**document, "root_epoch": "855d424b-9c86-4ee6-ac93-1b2feefcff51"}
            await session.execute(update(RunEventRow).where(RunEventRow.seq == row["seq"]).values(content="{broken" if change == "malformed" else json.dumps(altered)))
    if change in {"malformed", "wrong-category"}:
        with pytest.raises(OutputEvidenceError):
            await source_api().read_durable_inventory_source(e.event_store, evidence=evidence, output_receipt=receipt)
    else:
        audit = await source_api().read_durable_inventory_source(e.event_store, evidence=evidence, output_receipt=receipt)
        assert audit.state == ("missing" if change == "missing" else "changed") and audit.document is None


@pytest.mark.asyncio
async def test_duplicate_actual_singletons_are_uncertainty(capture_environment):
    e = capture_environment
    receipt, document = source_pair()
    row, _ = await put_inventory(e, document)
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    try:
        await e.event_store.put(thread_id="thread", run_id="run", event_type="run.output_inventory", category="outputs", content=document)
    finally:
        reset_current_user(token)
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=row["seq"], document=document)
    with pytest.raises(OutputEvidenceError):
        await source_api().read_durable_inventory_source(e.event_store, evidence=evidence, output_receipt=receipt)


@pytest.mark.asyncio
async def test_source_uses_bounded_explicit_scope_and_drains_repeated_cancel(capture_environment, monkeypatch):
    e = capture_environment
    receipt, document = source_pair()
    row, _ = await put_inventory(e, document)
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=row["seq"], document=document)
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = e.event_store.list_events
    calls = []

    async def held(*args, **kwargs):
        calls.append((args, kwargs))
        entered.set()
        await release.wait()
        try:
            return await original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(e.event_store, "list_events", held)
    task = asyncio.create_task(source_api().read_durable_inventory_source(e.event_store, evidence=evidence, output_receipt=receipt))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.05))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.05))[0]
        assert not finished.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert finished.is_set()
    assert calls == [(("thread", "run"), {"event_types": ["run.output_inventory"], "limit": 2, "user_id": RAW_OWNER})]


@pytest.mark.asyncio
async def test_unsupported_source_is_not_supported_absence():
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    receipt, document = source_pair()
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=4, document=document)
    with pytest.raises(OutputEvidenceError):
        await source_api().read_durable_inventory_source(MemoryRunEventStore(), evidence=evidence, output_receipt=receipt)


from test_run_output_route import ACK, HEADER  # noqa: E402
from test_run_output_route import route_environment as route_environment  # noqa: E402, F401

INVENTORY_HEADER = "X-DeerFlow-Require-Output-Inventory"
INVENTORY_ACK = "X-DeerFlow-Output-Inventory-Version"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["3", "true", ""])
async def test_unknown_inventory_version_refuses_without_any_ack(route_environment, value):
    response = await route_environment.client.get("/api/threads/thread/runs/missing/output", headers={HEADER: "1", INVENTORY_HEADER: value})
    assert response.status_code == 422
    assert ACK not in response.headers and INVENTORY_ACK not in response.headers


@pytest.mark.asyncio
async def test_inventory_capability_is_required_before_supported_missing(route_environment, monkeypatch):
    e = route_environment
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    e.app.state.run_event_store = MemoryRunEventStore()
    response = await e.client.get("/api/threads/thread/runs/missing/output", headers={HEADER: "1", INVENTORY_HEADER: "1"})
    assert response.status_code == 503
    assert ACK not in response.headers and INVENTORY_ACK not in response.headers


@pytest.mark.asyncio
async def test_ordinary_v1_missing_contract_is_preserved(route_environment):
    response = await route_environment.client.get("/api/threads/thread/runs/missing/output", headers={HEADER: "1"})
    assert response.status_code == 404 and response.headers.get(ACK) == "1" and INVENTORY_ACK not in response.headers


async def seed_route_inventory(e, *, sealed=True):
    # Real stored pair and actual source rows, with deliberately seeded host
    # root/scan facts. This does not qualify the worker's collector provenance.
    from langchain_core.messages import AIMessage, HumanMessage
    from test_run_output_route import OWNER, PUBLICATION, TEXT

    from deerflow.runtime.runs.output import canonical_text_digest
    from deerflow.runtime.runs.schemas import RunStatus

    record = await e.manager.create_or_reject(
        "thread",
        "lead_agent",
        user_id=OWNER,
        kwargs={"output_capture": {"version": 2}, "channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": OWNER, "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}},
    )
    assert await e.manager.try_start(record.run_id)
    from _run_output_capture_test_support import put_messages

    rows = await put_messages(e, [HumanMessage(id="human", content="input", additional_kwargs={"channel_turn_id": PUBLICATION}).model_dump(), AIMessage(id="actual-sdk-id", content=TEXT).model_dump()], owner=OWNER, run_id=record.run_id)
    receipt = TextOutputReceipt(
        version=1, kind="text", owner_user_id=OWNER, thread_id="thread", run_id=record.run_id, input_message_id="human", assistant_message_id="actual-sdk-id", event_seq=rows[-1]["seq"], text_sha256=canonical_text_digest(TEXT)
    )
    document = inventory_document(owner_user_id=OWNER, run_id=record.run_id, output_receipt_sha256=output_receipt_digest(receipt))
    row, _ = await put_inventory(e, document, run_id=record.run_id, owner=OWNER)
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=row["seq"], document=document)
    if sealed:
        result = await e.manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt, inventory_evidence=evidence)
        assert result.output_receipt_confirmed and result.inventory_evidence_confirmed
    return record, receipt, evidence, row


async def get_inventory(e, run_id, *, extra=None):
    return await e.client.get(f"/api/threads/thread/runs/{run_id}/output", headers={HEADER: "1", INVENTORY_HEADER: "1", **(extra or {})})


@pytest.mark.asyncio
async def test_real_sql_terminal_pair_and_sources_return_nested_complete_empty(route_environment):
    e = route_environment
    record, receipt, evidence, _ = await seed_route_inventory(e)
    response = await get_inventory(e, record.run_id)
    assert response.status_code == 200 and response.headers.get(INVENTORY_ACK) == response.headers.get(ACK) == "1"
    body = response.json()
    assert body["version"] == 2 and body["output"]["state"] == "complete"
    assert body["output"]["output_evidence"] == receipt.model_dump(mode="json")
    assert body["inventory"]["state"] == "matched" and body["inventory"]["document"]["items"] == []
    assert body["inventory"]["evidence"] == evidence.model_dump(mode="json")
    legacy = await e.client.get(f"/api/threads/thread/runs/{record.run_id}/output", headers={HEADER: "1"})
    assert legacy.status_code == 200 and legacy.json()["version"] == 1 and INVENTORY_ACK not in legacy.headers


@pytest.mark.asyncio
async def test_real_active_pair_absence_is_pending_and_missing_is_acknowledged(route_environment):
    from deerflow.runtime.runs.schemas import RunStatus

    e = route_environment
    record, _, _, _ = await seed_route_inventory(e, sealed=False)
    record.status, record.finalizing = RunStatus.success, True
    response = await get_inventory(e, record.run_id)
    assert response.status_code == 200 and response.json()["inventory"] == {"state": "pending", "evidence": None, "document": None, "reason": None}
    missing = await get_inventory(e, "missing")
    assert missing.status_code == 404 and missing.headers.get(INVENTORY_ACK) == missing.headers.get(ACK) == "1"


@pytest.mark.asyncio
async def test_real_prestart_closure_is_unavailable_never_empty(route_environment):
    from test_run_output_route import OWNER, PUBLICATION

    e = route_environment
    record = await e.manager.create_or_reject(
        "thread",
        "lead_agent",
        user_id=OWNER,
        kwargs={"output_capture": {"version": 2}, "channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": OWNER, "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}},
    )
    assert await e.manager.fail_start_if_pending(record.run_id, error="Synthetic startup refusal")
    response = await get_inventory(e, record.run_id)
    assert response.status_code == 200
    assert response.json()["output"]["reason"] == "failed"
    assert response.json()["inventory"]["state"] == "unavailable" and response.json()["inventory"]["document"] is None
    assert response.json()["inventory"]["reason"] == "prestart_not_enumerated"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "changed", "malformed"])
async def test_route_inventory_source_loss_does_not_adopt_output_text(route_environment, change):
    e = route_environment
    record, _, _, row = await seed_route_inventory(e)
    async with e.sf.begin() as session:
        if change == "missing":
            await session.execute(delete(RunEventRow).where(RunEventRow.seq == row["seq"]))
        elif change == "malformed":
            await session.execute(update(RunEventRow).where(RunEventRow.seq == row["seq"]).values(content="{broken"))
        else:
            changed = {**row["content"], "root_epoch": "855d424b-9c86-4ee6-ac93-1b2feefcff51"}
            await session.execute(update(RunEventRow).where(RunEventRow.seq == row["seq"]).values(content=json.dumps(changed)))
    response = await get_inventory(e, record.run_id)
    if change == "malformed":
        assert response.status_code == 503 and ACK not in response.headers and INVENTORY_ACK not in response.headers
    else:
        body = response.json()
        assert response.status_code == 200 and body["output"]["state"] == "complete"
        assert body["inventory"]["state"] == "unavailable" and body["inventory"]["document"] is None
        assert body["inventory"]["reason"] == "source_missing" if change == "missing" else body["inventory"]["reason"] == "source_changed"


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["text", "inventory"])
@pytest.mark.parametrize("change", ["proof", "status", "thread-owner"])
async def test_final_read_after_both_sources_refuses_changed_authority(route_environment, monkeypatch, source, change):
    from test_run_output_route import OWNER

    from deerflow.persistence.run.model import RunRow
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = route_environment
    record, _, _, _ = await seed_route_inventory(e)
    method = "list_messages_by_run" if source == "text" else "list_events"
    original = getattr(e.event_store, method)
    entered, release = asyncio.Event(), asyncio.Event()

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(e.event_store, method, held)
    task = asyncio.create_task(get_inventory(e, record.run_id))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with e.sf.begin() as session:
            if change == "thread-owner":
                await session.execute(update(ThreadMetaRow).where(ThreadMetaRow.thread_id == "thread").values(user_id="different-owner"))
            elif change == "status":
                await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(status="error"))
            else:
                row = await e.repository.get(record.run_id, user_id=OWNER)
                kwargs = {**row["kwargs"], "inventory_evidence": {**row["kwargs"]["inventory_evidence"], "inventory_sha256": "0" * 64}}
                await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs))
        release.set()
        response = await task
        assert response.status_code in {404, 503} and ACK not in response.headers and INVENTORY_ACK not in response.headers
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_missing_inventory_lookup_refreshes_real_thread_owner(route_environment, monkeypatch):
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = route_environment
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.manager.get_output_snapshot

    async def held(*args, **kwargs):
        result = await original(*args, **kwargs)
        assert result is None
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(e.manager, "get_output_snapshot", held)
    task = asyncio.create_task(get_inventory(e, "missing"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with e.sf.begin() as session:
            await session.execute(update(ThreadMetaRow).where(ThreadMetaRow.thread_id == "thread").values(user_id="different-owner"))
        release.set()
        response = await task
        assert response.status_code == 404 and ACK not in response.headers and INVENTORY_ACK not in response.headers
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_initial_source_scope_refusal_does_not_chain_active_private_error():
    import traceback

    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    receipt, document = source_pair()
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=4, document=document)
    other = TextOutputReceipt.model_validate({**receipt.model_dump(mode="json"), "owner_user_id": "other"})
    caught = None
    try:
        raise ValueError("synthetic-private-proof-body")
    except ValueError:
        try:
            await source_api().read_durable_inventory_source(MemoryRunEventStore(), evidence=evidence, output_receipt=other)
        except OutputEvidenceError as error:
            caught = error
    assert caught is not None
    assert "synthetic-private-proof-body" not in "".join(traceback.format_exception(caught))


@pytest.mark.asyncio
async def test_duplicate_inventory_header_refuses_without_ack(route_environment):
    response = await route_environment.client.get("/api/threads/thread/runs/missing/output", headers=[(HEADER, "1"), (INVENTORY_HEADER, "1"), (INVENTORY_HEADER, "1")])
    assert response.status_code == 422 and ACK not in response.headers and INVENTORY_ACK not in response.headers


@pytest.mark.asyncio
async def test_unavailable_pair_is_reread_after_actual_initial_snapshot(route_environment, monkeypatch):
    from test_run_output_route import OWNER, PUBLICATION

    from deerflow.persistence.run.model import RunRow

    e = route_environment
    record = await e.manager.create_or_reject(
        "thread",
        "lead_agent",
        user_id=OWNER,
        kwargs={"output_capture": {"version": 2}, "channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": OWNER, "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}},
    )
    assert await e.manager.fail_start_if_pending(record.run_id, error="Synthetic startup refusal")
    original = e.manager.get_output_snapshot
    entered, release = asyncio.Event(), asyncio.Event()
    reads = 0

    async def held(*args, **kwargs):
        nonlocal reads
        snapshot = await original(*args, **kwargs)
        reads += 1
        if reads == 1:
            entered.set()
            await release.wait()
        return snapshot

    monkeypatch.setattr(e.manager, "get_output_snapshot", held)
    operation = asyncio.create_task(get_inventory(e, record.run_id))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with e.sf.begin() as session:
            # Both statuses fit the failed receipt: only the final exact status
            # comparison catches this actual post-snapshot change.
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(status="timeout"))
        release.set()
        response = await operation
        assert reads == 2 and response.status_code == 503 and ACK not in response.headers and INVENTORY_ACK not in response.headers
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)

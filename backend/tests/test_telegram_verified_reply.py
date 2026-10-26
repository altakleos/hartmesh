"""Actual SQL custody with synthetic V2 proof; no SDK provenance is claimed."""

import asyncio
import threading
import warnings
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select
from test_output_inventory import inventory_document
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_publication_primary import _managed_turn, _output, _primary

from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.producer import Outbox, OutboxItem, freeze_reply_presentation
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest


def projection(record, *, text="**synthetic answer**", coverage="complete"):
    output = _output(record, text=text)
    receipt = output.output_evidence
    document = inventory_document(owner_user_id=receipt.owner_user_id, thread_id=receipt.thread_id, run_id=receipt.run_id, input_message_id=receipt.input_message_id, output_receipt_sha256=output_receipt_digest(receipt))
    if coverage != "complete":
        document.update(coverage="incomplete", reason="scan_incomplete", observed_items=document.pop("items"))
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=9, document=document)
    return RunOutputProjectionV2.model_validate({"version": 2, "output": output, "inventory": {"state": "matched", "evidence": evidence, "document": document, "reason": None}})


def synthetic_item(record, output):
    frozen = freeze_reply_presentation(output.output.source.text, text_sha256=output.output.output_evidence.text_sha256)
    identity = asdict(record)
    identity.pop("dispatch_started")
    identity.pop("run_id")
    identity["origins"] = [asdict(origin) for origin in record.origins]
    parts = [
        {
            "kind": "text",
            "source_ref": {
                "assistant_message_id": output.output.output_evidence.assistant_message_id,
                "thread_id": record.scope.thread_id,
                "run_id": output.output.run_id,
                "range": {"basis": "telegram-visible-v1", "start": chunk.start, "end": chunk.end},
            },
            "html": chunk.html,
            "visible_text": chunk.visible_text,
            "reply_to_message_id": record.origins[0].message_id,
        }
        for chunk in frozen.chunks
    ]
    outcome = {"kind": "model_text", "projection": output.model_dump(mode="json")}
    basis = {"version": 2, "publication": identity, "outcome": outcome, "presentation": frozen.to_json(), "parts": deepcopy(parts), "finish_route": True}
    return OutboxItem(
        "f" * 32,
        record.scope.link_id,
        record.scope.owner_user_id,
        record.scope.binding_generation,
        "reply:" + record.turn_id,
        "reply",
        {"reply_basis": basis, "source": outcome, "presentation_version": 0},
        parts,
        record.scope.chat_id,
        record.scope.message_thread_id,
    )


@pytest.mark.asyncio
async def test_current_public_authority_must_allow_verified_v2_with_actual_read_ceiling(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", lambda *args, **kwargs: SimpleNamespace(name=e.link.agent_ref))
    item = synthetic_item(record, projection(record))
    assert (await e.live.authority.delivery(e.link, item, item.parts[0])).decision == "allow"


@pytest.mark.asyncio
async def test_legacy_fixed_public_authority_control(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", lambda *args, **kwargs: SimpleNamespace(name=e.link.agent_ref))
    item = synthetic_item(record, projection(record))
    item = replace(item, payload={}, parts=[{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}}])
    assert (await e.live.authority.delivery(e.link, item)).decision == "allow"


@pytest.mark.asyncio
async def test_verified_primary_seals_full_pair_and_exact_replay(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record)
    box = Outbox(e.sessions)
    identity = await box.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    assert saved.payload["reply_basis"]["version"] == 2
    assert saved.payload["reply_basis"]["outcome"]["projection"] == output.model_dump(mode="json")
    assert saved.parts[0]["reply_to_message_id"] == record.origins[0].message_id
    async with e.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        parts = deepcopy(row.parts)
        parts[0].update(state="sent", message_ids=[501])
        row.parts = parts
    saved = await _primary(e.db, identity)
    assert await box.plan_verified_publication_reply(record, output=output, guard=e.guard_factory()) == identity
    assert (await _primary(e.db, identity)).parts == saved.parts


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["incomplete", "source", "legacy"])
async def test_verified_primary_refuses_partial_or_changed_or_v1_adoption(publication_environment, change):
    e = publication_environment
    record = await _managed_turn(e)
    box = Outbox(e.sessions)
    output = projection(record)
    if change == "legacy":
        await box.plan_publication_reply(record, output=output.output, guard=e.guard_factory())
    elif change == "source":
        await box.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
        output = projection(record, text="different synthetic answer")
    else:
        output = projection(record, coverage="incomplete")
    with pytest.raises(ValueError):
        await box.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())


@pytest.mark.asyncio
async def test_v2_final_owner_guard_rolls_back_both_primary_and_group(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    calls = []

    async def live(owner, session):
        await e.live_owner_check(owner, session)
        calls.append(True)
        if len(calls) == 2:
            e.attempt_current[0] = False

    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory(check_live_owner=live))
    async with e.sessions() as session:
        assert await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id)) is None
        marks = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id)))
    assert len(calls) == 2 and all(not mark.route_complete and mark.outbox_id is None for mark in marks)


@pytest.mark.asyncio
async def test_v2_flushed_sql_is_owned_through_repeated_cancel(publication_environment, monkeypatch):
    """Executed SQL/commit orchestration, not a native cursor leak claim."""
    e = publication_environment
    record = await _managed_turn(e)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.inbox.seal_publication

    async def held(*args, **kwargs):
        await original(*args, **kwargs)
        await kwargs["session"].flush()
        entered.set()
        await release.wait()

    monkeypatch.setattr(e.inbox, "seal_publication", held)
    task = asyncio.create_task(Outbox(e.sessions, inbox=e.inbox).plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory()))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with e.sessions() as session:
        saved = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
    assert saved is not None and saved.payload["reply_basis"]["version"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["reserve", "marker", "bind"])
async def test_prepared_guard_refusal_precedes_owned_admission(publication_environment, stage):
    e = publication_environment
    base = e.guard_factory()
    record = None if stage == "reserve" else await e.reserve()
    if stage == "bind":
        assert await e.inbox.mark_dispatch_started(record, guard=base)
    calls = []

    class Refusing:
        captured_link = base.captured_link

        async def prepare(self):
            calls.append("prepare")
            raise ValueError("controlled policy withdrawal")

        async def check(self, session):
            calls.append("writer")
            return await base.check(session)

    with pytest.raises(ValueError):
        if stage == "reserve":
            await e.reserve(guard=Refusing())
        elif stage == "marker":
            await e.inbox.mark_dispatch_started(record, guard=Refusing())
        else:
            await e.inbox.bind_publication_run(record, run_id="must-not-bind", guard=Refusing())
    assert calls == ["prepare"]
    async with e.sessions() as session:
        marks = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([origin.message_id for origin in e.origins]))))
    if stage == "reserve":
        assert all(mark.turn_id is None for mark in marks)
    else:
        assert all(mark.run_id is None and mark.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] is (stage == "bind") for mark in marks)


@pytest.mark.asyncio
async def test_verified_guard_refreshes_after_the_actual_codec(publication_environment, monkeypatch):
    from app.telegram_bots import producer

    e = publication_environment
    record = await _managed_turn(e)
    base = e.guard_factory()
    entered, release = threading.Event(), threading.Event()
    current, calls = [True], []
    original = producer.freeze_reply_presentation

    def codec(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result

    class Guard:
        captured_link = base.captured_link

        async def prepare(self):
            calls.append("prepare")
            if not current[0]:
                raise ValueError("controlled policy withdrawal")

        async def check(self, session):
            calls.append("writer")
            return await base.check(session)

    monkeypatch.setattr(producer, "freeze_reply_presentation", codec)
    task = asyncio.create_task(Outbox(e.sessions).plan_verified_publication_reply(record, output=projection(record), guard=Guard()))
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 5), 6)
        current[0] = False
    finally:
        release.set()
        with pytest.raises(ValueError):
            await task
    assert calls == ["prepare"]
    async with e.sessions() as session:
        assert await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id)) is None


@pytest.mark.asyncio
async def test_constructed_invalid_proof_does_not_log_serializer_payload(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    invalid = projection(record).model_copy(update={"version": "private-proof-marker"})
    with warnings.catch_warnings(record=True) as logged:
        warnings.simplefilter("always")
        with pytest.raises(ValueError, match="publication-source-conflict"):
            await Outbox(e.sessions).plan_verified_publication_reply(record, output=invalid, guard=e.guard_factory())
    assert logged == [], "Fixed proof refusal must not emit a serializer payload warning"

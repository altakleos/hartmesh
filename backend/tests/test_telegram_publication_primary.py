"""Actual SQL managed-primary tests; synthetic proof DTOs establish no AI provenance.

Genuine worker/HTTP provenance is tested by the separately owned composition case.
"""

import asyncio
import hashlib
from copy import deepcopy
from dataclasses import replace
from html import escape
from types import MappingProxyType

import pytest
from _telegram_repository_test_support import db as db
from _telegram_repository_test_support import rows
from sqlalchemy import select, update
from test_output_inventory import inventory_document
from test_telegram_publication_custody import publication_environment as publication_environment

from app.channels.execution_client import ValidatedRun
from app.gateway.channel_execution import receipt_from_run_kwargs
from app.gateway.run_output import RunOutputProjection, RunOutputProjectionV2
from app.telegram_bots import producer
from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest

RUN_ID = "managed-primary-run"


def _part(key="error.generic", **values):
    return {"kind": "text", "source_ref": {"key": key, "values": {}}, **values}


async def _primary(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


async def _assert_unsealed(e, record):
    marks = [row for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id]
    assert len(marks) == 2
    assert all(not row.route_complete and row.outbox_id is None and row.assistant_message_id is None and row.delivery_ids == [] for row in marks)


async def _managed_turn(e, origins=None):
    original = await e.reserve(origins)
    assert await e.inbox.mark_dispatch_started(original, guard=e.guard_factory()) is True
    bound = await e.inbox.bind_publication_run(original, run_id=RUN_ID, guard=e.guard_factory())
    assert bound.run_id == RUN_ID and bound.dispatch_started is True
    # Deliberately return the stale pre-marker/pre-bind observation.
    return original


def _output(record, *, text="**synthetic answer**", reason=None, run_id=RUN_ID):
    scope = record.scope
    proof = {"version": 1, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "run_id": run_id, "input_message_id": "synthetic-human"}
    if reason is None:
        proof.update(kind="text", assistant_message_id="synthetic-assistant", event_seq=7, text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest())
    else:
        proof.update(kind="unavailable", reason=reason)
    return RunOutputProjection.model_validate(
        {
            "version": 1,
            "owner_user_id": scope.owner_user_id,
            "thread_id": scope.thread_id,
            "run_id": run_id,
            "run_status": "success" if reason is None or reason not in {"failed", "cancelled"} else "error" if reason == "failed" else "interrupted",
            "state": "complete" if reason is None else "unavailable",
            "channel_execution": {"version": 1, "publication_id": record.publication_id, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "assistant_id": "lead_agent", "agent_name": scope.agent_name},
            "output_evidence": proof,
            "source": {"text": text} if reason is None else None,
            "reason": reason,
        }
    )


def verified(output, *, coverage="complete"):
    """Pair an output with a matched inventory, leaving its receipt as it was sealed."""
    receipt = output.output_evidence
    document = inventory_document(owner_user_id=receipt.owner_user_id, thread_id=receipt.thread_id, run_id=receipt.run_id, input_message_id=receipt.input_message_id, output_receipt_sha256=output_receipt_digest(receipt))
    if coverage != "complete":
        document.update(coverage="incomplete", reason="scan_incomplete", observed_items=document.pop("items"))
    evidence = build_event_inventory_evidence(receipt, status=output.run_status, inventory_event_seq=getattr(receipt, "event_seq", 7) + 2, document=document)
    return RunOutputProjectionV2.model_validate({"version": 2, "output": output, "inventory": {"state": "matched", "evidence": evidence, "document": document, "reason": None}})


def projection(record, *, text="**synthetic answer**", coverage="complete", run_id=RUN_ID):
    return verified(_output(record, text=text, run_id=run_id), coverage=coverage)


def _terminal_run(record, *, status="error", reason=None):
    scope = record.scope
    proof = {"version": 1, "publication_id": record.publication_id, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "assistant_id": scope.assistant_id, "agent_name": scope.agent_name}
    row = {"run_id": RUN_ID, "thread_id": scope.thread_id, "assistant_id": scope.assistant_id, "status": status, "stop_reason": reason, "metadata": {"channel_turn_id": record.publication_id}, "kwargs": {"channel_execution": proof}}
    return ValidatedRun(MappingProxyType(row), receipt_from_run_kwargs(row["kwargs"]))


def _plan_fixed(e, outbox, record, *, status="error", key="error.generic"):
    bound = replace(record, dispatch_started=True, run_id=RUN_ID)
    return outbox.plan_publication_outcome(bound, run=_terminal_run(bound, status=status), source_ref={"key": key, "values": {}}, keyboard=[], callbacks=CallbackRepository(e.sessions), guard=e.guard_factory())


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("text_sha256", "f" * 64), ("event_seq", 8), ("endpoint_id", "f" * 64)], ids=["source-digest", "source-event", "source-endpoint"])
async def test_managed_primary_refuses_changed_source_basis(publication_environment, field, value):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    output = projection(record)
    identity = await outbox.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    changed_record = record
    if field == "text_sha256":
        changed = projection(record, text="a different synthetic answer")
    elif field == "event_seq":
        data = output.output.model_dump(mode="json")
        data["output_evidence"][field] = value
        changed = verified(RunOutputProjection.model_validate(data))
    else:
        changed_record = replace(record, scope=replace(record.scope, endpoint_id=value))
        changed = output
    with pytest.raises(ValueError):
        await outbox.plan_verified_publication_reply(changed_record, output=changed, guard=e.guard_factory())
    unchanged = await _primary(e.db, identity)
    assert unchanged.payload == saved.payload and unchanged.parts == saved.parts


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["html", "boundaries", "stored-basis"])
async def test_managed_primary_refuses_changed_frozen_presentation(publication_environment, monkeypatch, change):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    output = projection(record)
    identity = await outbox.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    if change == "stored-basis":
        async with e.sessions.begin() as session:
            row = await session.get(TelegramOutboxRow, identity)
            altered = deepcopy(row.payload)
            altered["reply_basis"]["version"] = True
            await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == identity).values(payload=altered).execution_options(synchronize_session=False))
        assert type((await _primary(e.db, identity)).payload["reply_basis"]["version"]) is bool
    else:
        frozen = producer.freeze_reply_presentation(output.output.source.text, text_sha256=output.output.output_evidence.text_sha256)
        visible = "".join(chunk.visible_text for chunk in frozen.chunks)
        spans = [visible] if change == "html" else [visible[:3], visible[3:]]
        chunks = []
        cursor = 0
        for span in spans:
            chunks.append(producer.FrozenReplyChunk("<i>" + escape(span) + "</i>", span, hashlib.sha256(span.encode()).hexdigest(), cursor, cursor + len(span)))
            cursor += len(span)
        altered = producer.FrozenReplyPresentation(frozen.renderer_schema, frozen.text_sha256, frozen.visible_sha256, tuple(chunks))
        monkeypatch.setattr(producer, "freeze_reply_presentation", lambda *args, **kwargs: altered)
    with pytest.raises(ValueError):
        await outbox.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    unchanged = await _primary(e.db, identity)
    assert unchanged.parts == saved.parts
    if change != "stored-basis":
        assert unchanged.payload == saved.payload


@pytest.mark.asyncio
async def test_managed_primary_generates_parts_without_caller_transport_receipts(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    identity = await Outbox(e.sessions).plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    assert "plain_fallback" not in saved.parts[0]
    assert saved.parts[0]["state"] == "pending" and saved.parts[0]["message_ids"] == []
    assert saved.parts[0]["source_ref"]["range"] == {"basis": "telegram-visible-v1", "start": 0, "end": len("synthetic answer")}


@pytest.mark.asyncio
async def test_existing_reply_exact_replay_preserves_actual_part_receipts(publication_environment):
    e = publication_environment
    await _managed_turn(e)
    bound = await e.inbox.bind_publication_run(await e.reserve(), run_id=RUN_ID, guard=e.guard_factory())
    outbox = Outbox(e.sessions)
    output = projection(bound, text="**synthetic answer**\n\n" + "second part " * 500)
    identity = await outbox.plan_verified_publication_reply(bound, output=output, guard=e.guard_factory())
    async with e.sessions.begin() as session:
        saved = await session.get(TelegramOutboxRow, identity)
        assert len(saved.parts) > 1
        saved.parts = [{**saved.parts[0], "state": "sent", "message_ids": [900], "protect_content": True}, *saved.parts[1:]]
    before = await _primary(e.db, identity)
    assert await outbox.plan_verified_publication_reply(bound, output=output, guard=e.guard_factory()) == identity
    after = await _primary(e.db, identity)
    assert after.payload == before.payload and after.parts == before.parts
    marks = [mark for mark in await rows(e.db, TelegramInboundMarkRow) if mark.turn_id == bound.turn_id]
    assert all(mark.route_complete and mark.outbox_id == identity and mark.delivery_ids == [identity] and mark.state == "reply_pending" for mark in marks)


@pytest.mark.asyncio
async def test_existing_reply_resets_supplied_part_identity_and_acknowledgements(publication_environment):
    """The verified writer builds its own parts; every plan shares this reset in `_add`."""
    e = publication_environment
    forged = _part(part_id="forged", presentation_version=51, ordinal=91, state="sent", message_ids=[991], sent_at="synthetic", dispatched_at="synthetic", url_button_fallback=True)
    await Outbox(e.sessions).add(e.link, dedupe_key="reply:supplied-receipts", class_="reply", payload={"parts": [forged]})
    part = next(row for row in await rows(e.db, TelegramOutboxRow) if row.dedupe_key == "reply:supplied-receipts").parts[0]
    assert part["part_id"] != "forged" and len(part["part_id"]) == 32
    assert (part["presentation_version"], part["ordinal"], part["state"], part["message_ids"]) == (0, 0, "pending", [])
    assert not {"sent_at", "dispatched_at", "url_button_fallback"}.intersection(part)


@pytest.mark.asyncio
async def test_existing_reply_partial_group_refuses_without_mutation(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    partial, output = replace(record, origins=record.origins[:1]), projection(record)
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_verified_publication_reply(partial, output=output, guard=e.guard_factory())
    await _assert_unsealed(e, record)
    assert len(await rows(e.db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_existing_reply_wrong_run_refuses_without_mutation(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record, run_id="other-run")
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    await _assert_unsealed(e, record)
    assert len(await rows(e.db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_existing_reply_borrowed_rollback_retains_whole_group(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    written = []

    async def failed_seal(*args, **kwargs):
        written.append(await kwargs["session"].scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.id == kwargs["outbox_id"])))
        raise RuntimeError("injected test failure")

    monkeypatch.setattr(e.inbox, "seal_publication", failed_seal)
    with pytest.raises(RuntimeError, match="injected test failure"):
        await Outbox(e.sessions, inbox=e.inbox).plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory())
    assert len(written) == 1 and written[0] is not None
    await _assert_unsealed(e, record)
    assert len(await rows(e.db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_existing_reply_concurrent_identical_frames_have_one_primary(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    identities = await asyncio.gather(*(outbox.plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory()) for _ in range(3)))
    assert len(set(identities)) == 1
    assert len(await rows(e.db, TelegramOutboxRow)) == 2
    assert all(mark.outbox_id == identities[0] and mark.route_complete for mark in await rows(e.db, TelegramInboundMarkRow) if mark.turn_id == record.turn_id)


@pytest.mark.asyncio
async def test_managed_exact_replay_uses_fresh_observations_and_preserves_acks(publication_environment):
    e = publication_environment
    original = await _managed_turn(e)
    assert (original.dispatch_started, original.run_id) == (False, None)
    outbox = Outbox(e.sessions)
    output = projection(original, text="**synthetic answer**\n\n" + "😀é " * 1400)
    identity = await outbox.plan_verified_publication_reply(original, output=output, guard=e.guard_factory())
    async with e.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        assert len(row.parts) > 1
        row.parts = [{**row.parts[0], "state": "sent", "message_ids": [987], "protect_content": True, "plain_fallback": True}, *row.parts[1:]]
    before = await _primary(e.db, identity)
    assert await outbox.plan_verified_publication_reply(original, output=output, guard=e.guard_factory()) == identity
    after = await _primary(e.db, identity)
    assert after.parts == before.parts and after.payload == before.payload
    marks = [mark for mark in await rows(e.db, TelegramInboundMarkRow) if mark.turn_id == original.turn_id]
    assert len(marks) == 2
    assert all(mark.route_complete and mark.outbox_id == identity and mark.assistant_message_id == "synthetic-assistant" and mark.state == "reply_pending" for mark in marks)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["error", "interrupted"])
async def test_managed_fixed_primary_has_no_fabricated_assistant_or_controls(publication_environment, status):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    identity = await _plan_fixed(e, outbox, record, status=status)
    assert await _plan_fixed(e, outbox, record, status=status) == identity
    saved = await _primary(e.db, identity)
    assert saved.dedupe_key == f"reply:{record.turn_id}"
    assert saved.payload.get("delivery_kind") is None
    assert saved.parts[0]["source_ref"] == {"key": "error.generic", "values": {}}
    assert saved.parts[0]["callback_group_id"] is None and "url_button" not in saved.parts[0]
    marks = [mark for mark in await rows(e.db, TelegramInboundMarkRow) if mark.turn_id == record.turn_id]
    assert len(marks) == 2 and all(mark.route_complete and mark.outbox_id == identity and mark.assistant_message_id is None for mark in marks)


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["model", "fixed"])
async def test_managed_model_and_fixed_primary_cannot_replace_each_other(publication_environment, first):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    output = projection(record)
    if first == "model":
        identity = await outbox.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
        with pytest.raises(ValueError):
            await _plan_fixed(e, outbox, record)
    else:
        identity = await _plan_fixed(e, outbox, record)
        with pytest.raises(ValueError):
            await outbox.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    assert len([row for row in await rows(e.db, TelegramOutboxRow) if row.dedupe_key == f"reply:{record.turn_id}"]) == 1
    assert ("turn_outcome_basis" in (await _primary(e.db, identity)).payload) is (first == "fixed")


@pytest.mark.asyncio
async def test_managed_started_unknown_execution_cannot_consume_primary_with_restart(publication_environment):
    e = publication_environment
    record = await e.reserve()
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    outbox, output = Outbox(e.sessions), projection(record)
    with pytest.raises(ValueError):
        await outbox.plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    with pytest.raises(ValueError):
        await outbox.plan_restart_before_dispatch(record, ticket=None, guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1
    assert all(not row.route_complete and row.outbox_id is None for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id)


@pytest.mark.asyncio
async def test_unproved_restart_fact_is_refused_without_writing_a_primary(publication_environment):
    e = publication_environment
    record = await e.reserve()
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_restart_before_dispatch(record, ticket=None, guard=e.guard_factory())
    primaries = [row.id for row in await rows(e.db, TelegramOutboxRow) if row.dedupe_key == f"reply:{record.turn_id}"]
    assert not primaries, "unproved restart must not reserve the primary"
    assert all(not row.route_complete and row.outbox_id is None for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id)


@pytest.mark.asyncio
async def test_managed_unmarked_or_unbound_model_is_not_a_primary(publication_environment):
    e = publication_environment
    record = await e.reserve()
    output = projection(record)
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_auxiliary_uncertainty_cannot_consume_the_later_proved_primary(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    auxiliary = await outbox.plan_product(e.link, source_id="auxiliary-uncertainty", parts=[_part("chat.restarted")], inbox_rows=e.origins, finish_route=False)
    async with e.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, auxiliary)
        row.state = "sent"
    before = [row for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id]
    assert all(not row.route_complete and row.outbox_id is None and row.delivery_ids == [auxiliary] for row in before)
    primary = await outbox.plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory())
    after = [row for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id]
    assert primary != auxiliary
    assert all(row.route_complete and row.outbox_id == primary and row.delivery_ids == [auxiliary, primary] and row.state == "reply_pending" for row in after)


@pytest.mark.asyncio
async def test_final_guard_after_last_live_owner_await_rolls_back_whole_primary(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)

    class PreparedOwner:
        email = e.live_owner_check.email
        calls = 0

        async def __call__(self, owner, writer):
            await e.live_owner_check(owner, writer)
            self.calls += 1
            if self.calls == 2:
                e.attempt_current[0] = False

    prepared, output = PreparedOwner(), projection(record)
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_verified_publication_reply(record, output=output, guard=e.guard_factory(check_live_owner=prepared))
    assert prepared.calls == 2
    assert len(await rows(e.db, TelegramOutboxRow)) == 1
    await _assert_unsealed(e, record)


@pytest.mark.asyncio
async def test_failed_borrowed_seal_rolls_back_outbox_and_all_origins(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    seal = e.inbox.seal_publication

    async def failed_seal(*args, **kwargs):
        await seal(*args, **kwargs)
        await kwargs["session"].flush()
        raise RuntimeError("injected test failure")

    monkeypatch.setattr(e.inbox, "seal_publication", failed_seal)
    with pytest.raises(RuntimeError, match="injected test failure"):
        await Outbox(e.sessions, inbox=e.inbox).plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1
    await _assert_unsealed(e, record)


@pytest.mark.asyncio
async def test_managed_writer_retains_admitted_unit_through_repeated_cancellation(publication_environment, monkeypatch):
    """Real flushed SQL plus an orchestration barrier; not a native cursor leak proof."""
    e = publication_environment
    record = await _managed_turn(e)
    entered, release = asyncio.Event(), asyncio.Event()
    seal = e.inbox.seal_publication

    async def paused_seal(*args, **kwargs):
        await seal(*args, **kwargs)
        await kwargs["session"].flush()
        entered.set()
        await release.wait()

    monkeypatch.setattr(e.inbox, "seal_publication", paused_seal)
    outbox = Outbox(e.sessions, inbox=e.inbox)
    task = asyncio.create_task(outbox.plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory()))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.04))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.04))[0]
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        primaries = [row for row in await rows(e.db, TelegramOutboxRow) if row.dedupe_key == f"reply:{record.turn_id}"]
        assert len(primaries) == 1
        assert await outbox.plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory()) == primaries[0].id
        assert all(row.route_complete and row.outbox_id == primaries[0].id for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["unsupported_input", "unsupported_output", "human_input_required", "ambiguous_source", "missing_message_identity"])
async def test_unsupported_success_dispositions_stay_held(publication_environment, reason):
    e = publication_environment
    record = await _managed_turn(e)
    output = verified(_output(record, reason=reason))
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_verified_publication_reply(record, output=output, guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1

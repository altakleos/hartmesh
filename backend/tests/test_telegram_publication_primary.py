"""Actual SQL managed-primary tests; synthetic proof DTOs establish no AI provenance.

The task retains the earlier legacy-contrast baseline snapshot. The strict
requirements now target the managed API; compatibility controls keep exercising
the unchanged legacy primitive. Genuine worker/HTTP provenance is tested by the
separately owned composition case.
"""

import asyncio
import hashlib
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from html import escape

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from telegram import Chat, Message, Update, User
from test_telegram_publication_custody import publication_environment as publication_environment

from app.gateway.run_output import RunOutputProjection
from app.telegram_bots import producer
from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

TURN_ID = "d" * 32


def _part(key="error.generic", **values):
    return {"kind": "text", "source_ref": {"key": key, "values": {}}, **values}


async def _legacy_turn(db):
    """Exercise the shipped primitive without inventing a new guard or output receipt."""
    link = await bind_link(db, await add_link(db))
    outbox = Outbox(db.sessions)
    origins = []
    for identity in (301, 302):
        message = Message(identity, datetime.now(UTC), Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="synthetic publication", message_thread_id=77)
        origin, _ = await outbox.inbox.accept(link, Update(identity + 100, message=message), sanitized_input={"text": "synthetic publication"})
        origins.append(await outbox.inbox.route(origin, kind="chat", target_id="original-thread", basis={"conversation_id": "original-thread"}, operation_ids=[]))
    await outbox.inbox.bind_turn(origins, turn_id=TURN_ID, thread_id="original-thread")
    publication = hashlib.sha256(f"{TURN_ID}:m301".encode()).hexdigest()[:32]
    await outbox.inbox.bind_run(turn_id=TURN_ID, publication_id=publication, run_id="original-run")
    source = {
        "thread_id": "original-thread",
        "run_id": "original-run",
        "publication_id": publication,
        "assistant_message_id": "synthetic-assistant",
        "message_ids": [301, 302],
        "text_sha256": hashlib.sha256(b"synthetic answer").hexdigest(),
        "event_seq": 7,
        "endpoint_id": "e" * 64,
    }
    return link, outbox, tuple(origins), source


async def _primary(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


async def _assert_unsealed(db):
    marks = [row for row in await rows(db, TelegramInboundMarkRow) if row.turn_id == TURN_ID]
    assert len(marks) == 2
    assert all(not row.route_complete and row.outbox_id is None and row.assistant_message_id is None and row.delivery_ids == [] for row in marks)


async def _managed_turn(e):
    original = await e.reserve()
    assert await e.inbox.mark_dispatch_started(original, guard=e.guard_factory()) is True
    bound = await e.inbox.bind_publication_run(original, run_id="managed-primary-run", guard=e.guard_factory())
    assert bound.run_id == "managed-primary-run" and bound.dispatch_started is True
    # Deliberately return the stale pre-marker/pre-bind observation.
    return original


def _output(record, *, text="**synthetic answer**", reason=None):
    scope = record.scope
    proof = {"version": 1, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "run_id": "managed-primary-run", "input_message_id": "synthetic-human"}
    if reason is None:
        proof.update(kind="text", assistant_message_id="synthetic-assistant", event_seq=7, text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest())
    else:
        proof.update(kind="unavailable", reason=reason)
    return RunOutputProjection.model_validate(
        {
            "version": 1,
            "owner_user_id": scope.owner_user_id,
            "thread_id": scope.thread_id,
            "run_id": "managed-primary-run",
            "run_status": "success" if reason is None or reason not in {"failed", "cancelled"} else "error" if reason == "failed" else "interrupted",
            "state": "complete" if reason is None else "unavailable",
            "channel_execution": {"version": 1, "publication_id": record.publication_id, "owner_user_id": scope.owner_user_id, "thread_id": scope.thread_id, "assistant_id": "lead_agent", "agent_name": scope.agent_name},
            "output_evidence": proof,
            "source": {"text": text} if reason is None else None,
            "reason": reason,
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("text_sha256", "f" * 64), ("event_seq", 8), ("endpoint_id", "f" * 64)], ids=["source-digest", "source-event", "source-endpoint"])
async def test_managed_primary_refuses_changed_source_basis(publication_environment, field, value):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    output = _output(record)
    identity = await outbox.plan_publication_reply(record, output=output, guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    changed_record = record
    if field == "text_sha256":
        changed = _output(record, text="a different synthetic answer")
    elif field == "event_seq":
        data = output.model_dump(mode="json")
        data["output_evidence"][field] = value
        changed = RunOutputProjection.model_validate(data)
    else:
        changed_record = replace(record, scope=replace(record.scope, endpoint_id=value))
        changed = output
    with pytest.raises(ValueError):
        await outbox.plan_publication_reply(changed_record, output=changed, guard=e.guard_factory())
    unchanged = await _primary(e.db, identity)
    assert unchanged.payload == saved.payload and unchanged.parts == saved.parts


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["html", "boundaries", "stored-basis"])
async def test_managed_primary_refuses_changed_frozen_presentation(publication_environment, monkeypatch, change):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    output = _output(record)
    identity = await outbox.plan_publication_reply(record, output=output, guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    if change == "stored-basis":
        async with e.sessions.begin() as session:
            row = await session.get(TelegramOutboxRow, identity)
            altered = deepcopy(row.payload)
            altered["reply_basis"]["version"] = True
            await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == identity).values(payload=altered).execution_options(synchronize_session=False))
        assert type((await _primary(e.db, identity)).payload["reply_basis"]["version"]) is bool
    else:
        frozen = producer.freeze_reply_presentation(output.source.text, text_sha256=output.output_evidence.text_sha256)
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
        await outbox.plan_publication_reply(record, output=output, guard=e.guard_factory())
    unchanged = await _primary(e.db, identity)
    assert unchanged.parts == saved.parts
    if change != "stored-basis":
        assert unchanged.payload == saved.payload


@pytest.mark.asyncio
async def test_managed_primary_generates_parts_without_caller_transport_receipts(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    identity = await Outbox(e.sessions).plan_publication_reply(record, output=_output(record), guard=e.guard_factory())
    saved = await _primary(e.db, identity)
    assert "plain_fallback" not in saved.parts[0]
    assert saved.parts[0]["state"] == "pending" and saved.parts[0]["message_ids"] == []
    assert saved.parts[0]["source_ref"]["range"] == {"basis": "telegram-visible-v1", "start": 0, "end": len("synthetic answer")}


@pytest.mark.asyncio
async def test_existing_reply_exact_replay_preserves_actual_part_receipts(db):
    link, outbox, origins, source = await _legacy_turn(db)
    parts = [_part(), _part("hello.privacy", silent=True)]
    identity = await outbox.plan_reply(link, turn_id=TURN_ID, source=source, parts=parts, inbox_rows=origins)
    async with db.sessions.begin() as session:
        saved = await session.get(TelegramOutboxRow, identity)
        saved.parts = [{**saved.parts[0], "state": "sent", "message_ids": [900], "protect_content": True}, saved.parts[1]]
    before = await _primary(db, identity)
    assert await outbox.plan_reply(link, turn_id=TURN_ID, source=deepcopy(source), parts=deepcopy(parts), inbox_rows=origins) == identity
    after = await _primary(db, identity)
    assert after.payload == before.payload and after.parts == before.parts
    marks = [mark for mark in await rows(db, TelegramInboundMarkRow) if mark.turn_id == TURN_ID]
    assert all(mark.route_complete and mark.outbox_id == identity and mark.delivery_ids == [identity] and mark.state == "reply_pending" for mark in marks)


@pytest.mark.asyncio
async def test_existing_reply_resets_supplied_part_identity_and_acknowledgements(db):
    link, outbox, origins, source = await _legacy_turn(db)
    forged = _part(part_id="forged", presentation_version=51, ordinal=91, state="sent", message_ids=[991], sent_at="synthetic", dispatched_at="synthetic", url_button_fallback=True)
    identity = await outbox.plan_reply(link, turn_id=TURN_ID, source=source, parts=[forged], inbox_rows=origins)
    part = (await _primary(db, identity)).parts[0]
    assert part["part_id"] != "forged" and len(part["part_id"]) == 32
    assert (part["presentation_version"], part["ordinal"], part["state"], part["message_ids"]) == (0, 0, "pending", [])
    assert not {"sent_at", "dispatched_at", "url_button_fallback"}.intersection(part)


@pytest.mark.asyncio
async def test_existing_reply_partial_group_refuses_without_mutation(db):
    link, outbox, origins, source = await _legacy_turn(db)
    with pytest.raises(ValueError):
        await outbox.plan_reply(link, turn_id=TURN_ID, source=source, parts=[_part()], inbox_rows=origins[:1])
    await _assert_unsealed(db)
    assert len(await rows(db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_existing_reply_wrong_run_refuses_without_mutation(db):
    link, outbox, origins, source = await _legacy_turn(db)
    with pytest.raises(ValueError):
        await outbox.plan_reply(link, turn_id=TURN_ID, source={**source, "run_id": "other-run"}, parts=[_part()], inbox_rows=origins)
    await _assert_unsealed(db)
    assert len(await rows(db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_existing_reply_borrowed_rollback_retains_whole_group(db):
    link, outbox, origins, source = await _legacy_turn(db)
    async with db.sessions() as session:
        async with session.begin():
            identity = await outbox.plan_reply(link, turn_id=TURN_ID, source=source, parts=[_part()], inbox_rows=origins, session=session)
            assert await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.id == identity)) == identity
            await session.rollback()
    await _assert_unsealed(db)
    assert len(await rows(db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_existing_reply_concurrent_identical_frames_have_one_primary(db):
    link, outbox, origins, source = await _legacy_turn(db)
    identities = await asyncio.gather(*(outbox.plan_reply(link, turn_id=TURN_ID, source=deepcopy(source), parts=[_part()], inbox_rows=origins) for _ in range(3)))
    assert len(set(identities)) == 1
    assert len(await rows(db, TelegramOutboxRow)) == 2
    assert all(mark.outbox_id == identities[0] and mark.route_complete for mark in await rows(db, TelegramInboundMarkRow) if mark.turn_id == TURN_ID)


@pytest.mark.asyncio
async def test_managed_exact_replay_uses_fresh_observations_and_preserves_acks(publication_environment):
    e = publication_environment
    original = await _managed_turn(e)
    assert (original.dispatch_started, original.run_id) == (False, None)
    outbox = Outbox(e.sessions)
    output = _output(original, text="**synthetic answer**\n\n" + "😀e\u0301 " * 1400)
    identity = await outbox.plan_publication_reply(original, output=output, guard=e.guard_factory())
    async with e.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        assert len(row.parts) > 1
        row.parts = [{**row.parts[0], "state": "sent", "message_ids": [987], "protect_content": True, "plain_fallback": True}, *row.parts[1:]]
    before = await _primary(e.db, identity)
    assert await outbox.plan_publication_reply(original, output=output, guard=e.guard_factory()) == identity
    after = await _primary(e.db, identity)
    assert after.parts == before.parts and after.payload == before.payload
    marks = [mark for mark in await rows(e.db, TelegramInboundMarkRow) if mark.turn_id == original.turn_id]
    assert len(marks) == 2
    assert all(mark.route_complete and mark.outbox_id == identity and mark.assistant_message_id == "synthetic-assistant" and mark.state == "reply_pending" for mark in marks)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,key", [("sealed_failed", "error.generic"), ("sealed_cancelled", "run.cancelled")])
async def test_managed_fixed_primary_has_no_fabricated_assistant_or_controls(publication_environment, kind, key):
    e = publication_environment
    record = await _managed_turn(e)
    output = _output(record, reason="failed" if kind == "sealed_failed" else "cancelled")
    notice = producer.PrimaryTurnNotice(kind, output)
    outbox = Outbox(e.sessions)
    identity = await outbox.plan_publication_notice(record, notice=notice, guard=e.guard_factory())
    assert await outbox.plan_publication_notice(record, notice=notice, guard=e.guard_factory()) == identity
    saved = await _primary(e.db, identity)
    assert saved.dedupe_key == f"reply:{record.turn_id}"
    assert saved.payload.get("delivery_kind") is None
    assert saved.parts[0]["source_ref"] == {"key": key, "values": {}}
    assert saved.parts[0]["callback_group_id"] is None and "url_button" not in saved.parts[0]
    marks = [mark for mark in await rows(e.db, TelegramInboundMarkRow) if mark.turn_id == record.turn_id]
    assert len(marks) == 2 and all(mark.route_complete and mark.outbox_id == identity and mark.assistant_message_id is None for mark in marks)


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["model", "fixed"])
async def test_managed_model_and_fixed_primary_cannot_replace_each_other(publication_environment, first):
    e = publication_environment
    record = await _managed_turn(e)
    outbox = Outbox(e.sessions)
    notice = producer.PrimaryTurnNotice("sealed_failed", _output(record, reason="failed"))
    if first == "model":
        identity = await outbox.plan_publication_reply(record, output=_output(record), guard=e.guard_factory())
        with pytest.raises(ValueError):
            await outbox.plan_publication_notice(record, notice=notice, guard=e.guard_factory())
    else:
        identity = await outbox.plan_publication_notice(record, notice=notice, guard=e.guard_factory())
        with pytest.raises(ValueError):
            await outbox.plan_publication_reply(record, output=_output(record), guard=e.guard_factory())
    assert len([row for row in await rows(e.db, TelegramOutboxRow) if row.dedupe_key == f"reply:{record.turn_id}"]) == 1
    assert (await _primary(e.db, identity)).payload["reply_basis"]["outcome"]["kind"] == ("model_text" if first == "model" else "sealed_failed")


@pytest.mark.asyncio
async def test_managed_started_unknown_execution_cannot_consume_primary_with_restart(publication_environment):
    e = publication_environment
    record = await e.reserve()
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_publication_notice(record, notice=producer.PrimaryTurnNotice("restart_before_dispatch", None), guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1
    assert all(not row.route_complete and row.outbox_id is None for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id)


@pytest.mark.asyncio
async def test_unproved_restart_fact_is_refused_without_writing_a_primary(publication_environment):
    e = publication_environment
    record = await e.reserve()
    refused = False
    try:
        notice = producer.PrimaryTurnNotice("restart_before_dispatch", None)
        await Outbox(e.sessions).plan_publication_notice(record, notice=notice, guard=e.guard_factory())
    except ValueError:
        refused = True
    primaries = [row.id for row in await rows(e.db, TelegramOutboxRow) if row.dedupe_key == f"reply:{record.turn_id}"]
    assert refused and not primaries, "unproved restart must not reserve the primary"
    assert all(not row.route_complete and row.outbox_id is None for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id)


@pytest.mark.asyncio
async def test_managed_unmarked_or_unbound_model_is_not_a_primary(publication_environment):
    e = publication_environment
    record = await e.reserve()
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_publication_reply(record, output=_output(record), guard=e.guard_factory())
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
    primary = await outbox.plan_publication_reply(record, output=_output(record), guard=e.guard_factory())
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

    prepared = PreparedOwner()
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_publication_reply(record, output=_output(record), guard=e.guard_factory(check_live_owner=prepared))
    assert prepared.calls == 2
    assert len(await rows(e.db, TelegramOutboxRow)) == 1
    marks = [row for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id]
    assert all(not row.route_complete and row.outbox_id is None and row.assistant_message_id is None and row.delivery_ids == [] for row in marks)


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
        await Outbox(e.sessions, inbox=e.inbox).plan_publication_reply(record, output=_output(record), guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1
    marks = [row for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id]
    assert all(not row.route_complete and row.outbox_id is None and row.delivery_ids == [] and row.assistant_message_id is None for row in marks)


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
    task = asyncio.create_task(outbox.plan_publication_reply(record, output=_output(record), guard=e.guard_factory()))
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
        assert await outbox.plan_publication_reply(record, output=_output(record), guard=e.guard_factory()) == primaries[0].id
        assert all(row.route_complete and row.outbox_id == primaries[0].id for row in await rows(e.db, TelegramInboundMarkRow) if row.turn_id == record.turn_id)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["unsupported_input", "unsupported_output", "human_input_required", "ambiguous_source", "missing_message_identity"])
async def test_unsupported_success_dispositions_stay_held(publication_environment, reason):
    e = publication_environment
    record = await _managed_turn(e)
    output = _output(record, reason=reason)
    with pytest.raises(ValueError):
        producer.PrimaryTurnNotice("sealed_failed", output)
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_publication_reply(record, output=output, guard=e.guard_factory())
    assert len(await rows(e.db, TelegramOutboxRow)) == 1

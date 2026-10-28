"""Real SQL planners with explicitly trusted-host shown fixtures/protocol proof.

Actual callback-to-SSE provenance is tested by test_channel_preview_capture.
"""

from dataclasses import replace

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401
from test_telegram_publication_primary import _managed_turn, _output, _primary
from test_telegram_verified_reply import projection

from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.inbox import publication_identity
from app.telegram_bots.producer import Outbox, SendingPart
from app.telegram_bots.stopped_preview import StoppedSegment, _issue_shown_preview, _issue_stopped_preview
from deerflow.runtime.runs.output_inventory import build_unavailable_inventory_evidence
from deerflow.runtime.runs.preview import text_digest


def shown(record, *, stopped=False, empty=False, active=lambda: True):
    text = "synthetic"
    segments = () if empty else (StoppedSegment("model", "synthetic-assistant", 1, text, text_digest(text), "edit", 701, None, 0, "shown-first", 0, len(text)),)
    args = dict(publication_identity=publication_identity(record), run_id=record.run_id, segments=segments, check_current=active)
    return _issue_stopped_preview(**args, stop_id="original-stop") if stopped else _issue_shown_preview(**args)


def cancelled(record):
    output = _output(record, reason="cancelled")
    evidence = build_unavailable_inventory_evidence(output.output_evidence, status="interrupted", reason="cancellation_after_inventory_not_enumerated")
    return RunOutputProjectionV2.model_validate({"version": 2, "output": output, "inventory": {"state": "unavailable", "evidence": evidence, "document": None, "reason": evidence.reason}})


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped,empty", [(False, False), (True, False), (True, True)])
async def test_display_primary_uses_real_anchors_and_exact_source_before_delivery(publication_environment, stopped, empty):
    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    box = Outbox(e.sessions)
    output = cancelled(record) if stopped else projection(record, text="synthetic answer")
    if stopped:
        record = await e.inbox.request_publication_stop(record, stop_id="original-stop", guard=e.guard_factory())
        identity = await box.plan_stopped_publication_reply(record, output=output, preview=shown(record, stopped=True, empty=empty), guard=e.guard_factory(), waiting=True)
    else:
        identity = await box.plan_streamed_publication_reply(record, output=output, shown=shown(record), guard=e.guard_factory())
    row = await _primary(e.db, identity)
    assert row.payload["reply_basis"]["version"] == 4
    if not empty:
        assert row.parts[0]["kind"] == "edit" and row.parts[0]["target_message_id"] == 701
    if stopped:
        assert row.parts[-1]["source_ref"] == {"key": "stop.answer", "values": {"written": not empty, "waiting": True}}
    calls = []

    async def read(original, link):
        calls.append((original, link))
        return output

    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.producer import _item

    ready = await ModelSource(read_output=read).read_part(SendingPart(e.link, _item(row), row.parts[0], "process", "c" * 32, e.link.token_revision))
    assert len(calls) == 1
    if empty:
        assert ready is None  # The fixed stop notice is rendered by the adapter.
    else:
        assert ready.plain_text == ("synthetic" if stopped else "synthetic answer")


@pytest.mark.asyncio
async def test_stopped_source_cannot_be_forged_or_replace_a_normal_primary(publication_environment):
    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    box = Outbox(e.sessions)
    with pytest.raises(RuntimeError):
        await box.plan_stopped_publication_reply(record, output=cancelled(record), preview={"text": "untrusted bus hint"}, guard=e.guard_factory())
    record = await e.inbox.request_publication_stop(record, stop_id="original-stop", guard=e.guard_factory())
    normal = await box.plan_verified_publication_reply(record, output=projection(record), guard=e.guard_factory())
    with pytest.raises(ValueError):
        await box.plan_stopped_publication_reply(record, output=cancelled(record), preview=shown(record, stopped=True), guard=e.guard_factory())
    assert (await _primary(e.db, normal)).payload["reply_basis"]["version"] == 2


@pytest.mark.asyncio
async def test_exact_display_readback_needs_no_new_preview_or_delivery_mutation(publication_environment):
    from sqlalchemy import event

    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    box = Outbox(e.sessions)
    output = projection(record, text="synthetic answer")
    identity = await box.plan_streamed_publication_reply(record, output=output, shown=shown(record), guard=e.guard_factory())
    statements = []

    def sql(_connection, _cursor, statement, _parameters, _context, _many):
        statements.append(statement.lstrip().upper())

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", sql)
    try:
        assert await box.read_display_primary(record, output=output, guard=e.guard_factory()) == identity
        # The existing SQLite PublicationGuard reserves its writer with one
        # literal no-op link UPDATE; no delivery/input mutation is admitted.
        writes = [statement for statement in statements if statement.startswith(("INSERT", "UPDATE", "DELETE"))]
        assert all(statement.startswith("UPDATE TELEGRAM_BOT_LINKS SET ID=TELEGRAM_BOT_LINKS.ID ") for statement in writes)
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", sql)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["deleted-part", "bool-version", "arbitrary-anchor", "changed-source", "bool-generation", "conflicting-outcome"])
async def test_saved_display_corruption_or_source_change_never_becomes_cached_text(publication_environment, change):
    from copy import deepcopy
    from dataclasses import replace

    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.producer import DeliveryClaimRetired, _item

    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    box = Outbox(e.sessions)
    output = projection(record, text="synthetic answer")
    identity = await box.plan_streamed_publication_reply(record, output=output, shown=shown(record), guard=e.guard_factory())
    item = _item(await _primary(e.db, identity))
    selected = deepcopy(item.parts[0])
    if change == "deleted-part":
        item = replace(item, parts=[])
    elif change == "bool-version":
        item.payload["presentation_version"] = False
    elif change == "arbitrary-anchor":
        item.parts[0]["target_message_id"] = selected["target_message_id"] = 999
    elif change == "bool-generation":
        assert item.binding_generation == 1
        item = replace(item, binding_generation=True)
    elif change == "conflicting-outcome":
        item.payload["turn_outcome_basis"] = {"version": 1}
    calls = []

    async def read(*args):
        calls.append(args)
        return projection(record, text="changed") if change == "changed-source" else output

    with pytest.raises(DeliveryClaimRetired):
        await ModelSource(read_output=read).read_part(SendingPart(e.link, item, selected, "process", "c" * 32, e.link.token_revision))
    assert len(calls) == (1 if change == "changed-source" else 0)


@pytest.mark.asyncio
async def test_deleted_newest_anchor_replaces_only_unacknowledged_suffix(publication_environment):
    from datetime import UTC, datetime, timedelta

    from telegram.error import BadRequest

    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender

    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    clock = [datetime.now(UTC)]
    box = Outbox(e.sessions, clock=lambda: clock[0])
    text = "A" * 3500 + "tail"
    output = projection(record, text=text)
    segments = tuple(
        StoppedSegment("model", "synthetic-assistant", index + 1, raw, text_digest(raw), "edit", 701 + index, None, index, f"shown-{index}", start, start + len(raw)) for index, (start, raw) in enumerate([(0, "A" * 3500), (3500, "t")])
    )
    capability = _issue_shown_preview(publication_identity=publication_identity(record), run_id=record.run_id, segments=segments, check_current=lambda: True)
    identity = await box.plan_streamed_publication_reply(record, output=output, shown=capability, guard=e.guard_factory())
    from app.telegram_bots.model_source import validate_model_reply_basis
    from app.telegram_bots.producer import _item

    initial = _item(await _primary(e.db, identity))
    validate_model_reply_basis(initial)
    calls = []

    async def read(*args):
        return output

    async def eligible(_link, item, _part):
        # The shared fixture also owns an older welcome notice. This source
        # reader qualifies only the actual display plan under test.
        return DeliveryEligibility.allow() if item.id == identity else DeliveryEligibility.wait()

    async def deliver(**capture):
        private = await box.capture_sending_part(token_revision=e.link.token_revision, **capture)
        validate_model_reply_basis(private.item, private.part)
        ready = await ModelSource(read_output=read).read_part(private)
        calls.append((private.part, ready))
        if private.part["kind"] == "edit":
            raise BadRequest("Message to edit not found")
        return [703]

    sender = OutboxSender(box, run_id="display-test-process", deliver_part=deliver, eligibility=eligible)
    try:
        clock[0] += timedelta(seconds=3)
        assert await sender.tick() == 1
        await sender.drain()
        row = await _primary(e.db, identity)
        assert calls, (row.state, row.payload.get("failed_reason"))
        active = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        assert len(active) == 1 and active[0]["kind"] == "text" and active[0]["visible_text"] == "tail"
        assert any(part["state"] == "sent" and part["message_ids"] == [701] for part in row.parts)
        clock[0] += timedelta(seconds=3)
        assert await sender.tick() == 1
        await sender.drain()
        assert [part["target_message_id"] for part, _ready in calls if part["kind"] == "edit"] == [702]
        assert calls[-1][1].plain_text == "tail"
        assert (await _primary(e.db, identity)).state == "sent"
    finally:
        await sender.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped", [False, True])
async def test_display_primary_replies_to_actual_first_accepted_input(publication_environment, stopped):
    e = publication_environment
    first = e.origins[0]
    later = await e.add_origin(message_id=500, update_id=1800, text="Later accepted, smaller identity")
    e.origins = first, later
    record = await e.reserve()
    assert record.origins[0].message_id == later.message_id
    await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    record = await e.inbox.bind_publication_run(record, run_id="managed-primary-run", guard=e.guard_factory())
    box = Outbox(e.sessions)
    if stopped:
        record = await e.inbox.request_publication_stop(record, stop_id="original-stop", guard=e.guard_factory())
        identity = await box.plan_stopped_publication_reply(record, output=cancelled(record), preview=shown(record, stopped=True, empty=True), guard=e.guard_factory())
    else:
        capability = _issue_shown_preview(publication_identity=publication_identity(record), run_id=record.run_id, segments=(), check_current=lambda: True)
        identity = await box.plan_streamed_publication_reply(record, output=projection(record), shown=capability, guard=e.guard_factory())
    row = await _primary(e.db, identity)
    assert all(part["reply_to_message_id"] == first.message_id for part in row.parts)

"""Closed mixed proof + actual SQL custody; source projections here are explicit seeds."""

from copy import deepcopy
from dataclasses import replace

import httpx
import pytest
from _telegram_human_input_test_support import source_basis
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_publication_authority import attempt_environment as attempt_environment  # noqa: F401
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment  # noqa: F401
from test_telegram_publication_observation import ACK, run_row

from app.telegram_bots.human_input_source import HumanInputSource
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence

PATH = "/mnt/user-data/outputs/report.txt"


def mixed_basis(record, *, payload=None):
    basis = source_basis(record, payload=payload)
    basis["version"] = 2
    document = basis["projection"]["inventory"]["document"]
    document["version"] = 2
    document.pop("scan")
    document["output_view"] = {"kind": "no_admitted_output_view", "scope": {"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"}}
    document["items"].insert(0, {"kind": "file", "path": PATH, "surfaces": ["presentation_reference"], "witnesses": [{"kind": "root_snapshot", "root_epoch": document["root_epoch"], "message_id": "clarification:call"}]})
    evidence = build_event_inventory_evidence(basis["projection"]["output"]["output_evidence"], status="success", inventory_event_seq=9, document=document)
    basis["projection"]["inventory"]["evidence"] = evidence.model_dump(mode="json")
    return basis


async def mixed_environment(e, *, payload=None):
    from app.telegram_bots.callbacks import CallbackRepository

    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    basis = mixed_basis(replace(record, run_id="coordinator-run"), payload=payload)

    async def respond(request):
        if "/artifacts/" in request.url.path:
            assert request.method == "HEAD"
            return httpx.Response(200, headers={"content-length": "4", "content-type": "text/plain"})
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=basis["projection"])
        if request.url.path.endswith("/messages"):
            return httpx.Response(200, headers=ACK, json={"data": basis["messages"], "has_more": False})
        return httpx.Response(200, headers=ACK, json={**run_row(record), "metadata": {"channel_turn_id": record.publication_id}, "status": "success"})

    e.responder = respond
    source = HumanInputSource(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture)
    qualified = await source.read_request(record, e.link)
    assert qualified is not None
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    callbacks = CallbackRepository(e.db.sessions)
    return qualified, attempt.read_guard, callbacks, basis


@pytest.mark.asyncio
async def test_initial_mixed_primary_files_before_question_and_original_input_seal(coordinator_environment):
    from app.telegram_bots.human_input import present_request
    from app.telegram_bots.mixed_source import human_input_parts, prepare_mixed_files, validate_mixed_reply_basis

    e = coordinator_environment
    source, guard, callbacks, basis = await mixed_environment(e)
    record = source.record
    file_plan = await prepare_mixed_files(record, source, http_owner=e.http, guard=guard, capacity=1024, max_bytes=1024)
    view = present_request(source.request, thread_id=record.scope.thread_id, reply_to_message_id=record.origins[0].message_id, locale="en")
    identity = await e.outbox.plan_human_input_reply(record, source, parts=list(view.parts), keyboard=list(view.keyboard), callbacks=callbacks, guard=guard, file_plan=file_plan)
    item = await e.outbox.get(e.link, identity)
    proof = validate_mixed_reply_basis(item)
    assert proof.kind == "human_input_files" and proof.output.model_dump(mode="json") == basis["projection"]
    assert item.parts[0]["kind"] == "document" and "caption_html" not in item.parts[0]
    assert item.parts[0].get("callback_group_id") is None
    assert item.parts[-1]["source_ref"]["key"] == "human_input.presentation" and item.parts[-1]["callback_group_id"]
    assert human_input_parts(item) == tuple(item.parts[1:])
    assert item.payload["reply_basis"]["inputs"]["inputs"]
    assert (await e.outbox.list_human_inputs(e.link, topic=None))[0].id == identity


@pytest.mark.asyncio
@pytest.mark.parametrize("relocate", [False, True])
async def test_deferred_prompt_not_visible_or_expiring_until_exact_question_ack(coordinator_environment, relocate):
    from datetime import UTC, datetime, timedelta

    from app.telegram_bots.callbacks import Prompts
    from app.telegram_bots.mixed_source import prepare_mixed_files
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = coordinator_environment
    source, guard, callbacks, basis = await mixed_environment(e)
    record = source.record
    now = [datetime(2026, 10, 29, tzinfo=UTC)]
    callbacks.clock = e.outbox.clock = lambda: now[0]
    prompts = Prompts(callbacks)
    file_plan = await prepare_mixed_files(record, source, http_owner=e.http, guard=guard, capacity=1024, max_bytes=1024)
    ui = [{"kind": "text", "html": "Enter a field", "visible_text": "Enter a field", "reply_to_message_id": record.origins[0].message_id, "force_reply": True}]
    async with callbacks.writer(e.link, guard) as writer:
        identity = await e.outbox.plan_human_input_reply(record, source, parts=ui, keyboard=[], callbacks=callbacks, guard=guard, file_plan=file_plan, session=writer)
        prompt_id = await prompts.open(
            e.link, "form_field", {"thread_id": record.scope.thread_id, "request_id": "clarification:call"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None, defer_visibility=True, guard=guard, session=writer
        )
        await prompts.attach(e.link, prompt_id, outbox_id=identity, guard=guard, session=writer)
    prompt = await prompts.get(e.link, prompt_id, guard=guard)
    assert prompt.expires_at is None and not prompts.is_visible(prompt)
    now[0] += timedelta(hours=1)
    assert await prompts.current(e.link, e.link.chat_id, None, guard=guard) is None
    await callbacks.sweep(now[0])
    assert (await prompts.get(e.link, prompt_id, guard=guard)).consumed_at is None
    calls = []

    async def deliver(**kwargs):
        assert await prompts.current(e.link, e.link.chat_id, None, guard=guard) is None
        calls.append(kwargs)
        return [2100 + len(calls)]

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(e.outbox, run_id="worker", deliver_part=deliver, eligibility=eligible)

    async def claim():
        async with e.outbox._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"

    await claim()
    if relocate:
        previous = (await prompts.get(e.link, prompt_id, guard=guard)).payload["prompt_visibility"]["part_id"]
        await sender._replace(e.link, await e.outbox.get(e.link, identity), "claim", reason="protection")
        current = (await prompts.get(e.link, prompt_id, guard=guard)).payload["prompt_visibility"]["part_id"]
        assert current != previous
        await claim()
    await sender._run(e.link.id, identity, e.link.binding_generation, "claim")
    assert len(calls) == 1
    assert await callbacks.for_message(e.link.id, 2101, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation) is None
    assert not prompts.is_visible(await prompts.get(e.link, prompt_id, guard=guard))
    now[0] += timedelta(seconds=2)
    ack_time = now[0]
    await claim()
    await sender._run(e.link.id, identity, e.link.binding_generation, "claim")
    visible = await prompts.current(e.link, e.link.chat_id, None, guard=guard)
    assert len(calls) == 2 and visible.message_id == 2102 and prompts.is_visible(visible)
    assert visible.expires_at == ack_time + timedelta(seconds=callbacks.config.prompts.ttl_seconds)
    # A later replay/attachment is not a new visibility clock or file anchor.
    now[0] += timedelta(seconds=100)
    async with callbacks.writer(e.link, guard) as writer:
        await prompts.attach(e.link, prompt_id, outbox_id=identity, guard=guard, session=writer)
    assert (await prompts.get(e.link, prompt_id, guard=guard)).expires_at == visible.expires_at


async def planned_mixed(e):
    from app.telegram_bots.human_input import present_request
    from app.telegram_bots.mixed_source import prepare_mixed_files

    source, guard, callbacks, basis = await mixed_environment(e)
    record = source.record
    plan = await prepare_mixed_files(record, source, http_owner=e.http, guard=guard, capacity=1024, max_bytes=1024)
    view = present_request(source.request, thread_id=record.scope.thread_id, reply_to_message_id=record.origins[0].message_id, locale="en")
    identity = await e.outbox.plan_human_input_reply(record, source, parts=list(view.parts), keyboard=list(view.keyboard), callbacks=callbacks, guard=guard, file_plan=plan)
    return source, guard, callbacks, identity


@pytest.mark.asyncio
@pytest.mark.parametrize("answered", [False, True])
async def test_delivery_only_file_retry_and_question_keep_independent_ack_anchors(coordinator_environment, answered):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from app.telegram_bots.mixed_source import human_input_visible, validate_mixed_reply_basis
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender, SourceUnavailable
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = coordinator_environment
    source, guard, callbacks, identity = await planned_mixed(e)
    now = [datetime(2026, 10, 29, tzinfo=UTC)]
    callbacks.clock = e.outbox.clock = lambda: now[0]
    sent = []

    async def deliver(**kwargs):
        sent.append(kwargs["part_id"])
        return [3100 + len(sent)]

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(e.outbox, run_id="worker", deliver_part=deliver, eligibility=eligible)

    async def claim(*, first_sending=False):
        async with e.outbox._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"
            if first_sending:
                parts = deepcopy(row.parts)
                parts[0]["state"] = "sending"
                row.parts = parts

    await claim(first_sending=True)
    item = await e.outbox.get(e.link, identity)
    await sender._unavailable(e.link.id, identity, e.link.binding_generation, "claim", item.parts[0]["part_id"], SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.txt"}}))
    for _ in range(2):
        now[0] += timedelta(seconds=2)
        await claim()
        await sender._run(e.link.id, identity, e.link.binding_generation, "claim")
    item = await e.outbox.get(e.link, identity)
    assert human_input_visible(item)
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))
        retry = next(row for row in rows if row.kind == "file_retry")
        question = next(row for row in rows if row.kind == "clarify_option")
        assert retry.message_id == 3101 and question.message_id == 3102
        retry_id, question_id, question_group = retry.id, question.id, question.group_id
    if answered:
        await callbacks.consume_group(question_group, reason="answered", link=e.link, guard=guard)
        assert await e.outbox.list_human_inputs(e.link, topic=None) == ()
    metadata = {"path": PATH, "filename": "report.txt", "size": 4, "mime_type": "text/plain", "state": "ready"}
    assert await e.outbox.retry_file_callback(e.link, retry_id, metadata=metadata, callbacks=callbacks, guard=guard)
    now[0] += timedelta(seconds=2)
    await claim()
    await sender._run(e.link.id, identity, e.link.binding_generation, "claim")
    final = await e.outbox.get(e.link, identity)
    assert final.parts[-1]["kind"] == "document" and final.parts[-1]["message_ids"] == [3103]
    assert validate_mixed_reply_basis(final).kind == "human_input_files"
    assert human_input_visible(final)
    assert (await callbacks.get(question_id)).message_id == 3102
    route = await callbacks.for_message(e.link.id, 3102, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route is not None and route.retired_presentation is answered
    assert await callbacks.for_message(e.link.id, 3103, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation) is None


@pytest.mark.asyncio
async def test_stale_unsent_question_retires_only_ui_and_still_sends_requested_file_retry(coordinator_environment):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from app.telegram_bots.mixed_source import is_human_input_part, validate_mixed_reply_basis
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender, SourceUnavailable
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = coordinator_environment
    source, guard, callbacks, identity = await planned_mixed(e)
    now = [datetime(2026, 10, 29, tzinfo=UTC)]
    callbacks.clock = e.outbox.clock = lambda: now[0]

    async def claim(first=False):
        async with e.outbox._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"
            if first:
                parts = deepcopy(row.parts)
                parts[0]["state"] = "sending"
                row.parts = parts

    sent = []

    async def deliver(**kwargs):
        sent.append(kwargs["part_id"])
        return [3200 + len(sent)]

    async def eligible(_link, item, part):
        if part is not None and is_human_input_part(item, part):
            return DeliveryEligibility.skip("superseded")
        return DeliveryEligibility("allow")

    sender = OutboxSender(e.outbox, run_id="worker", deliver_part=deliver, eligibility=eligible)
    await claim(True)
    item = await e.outbox.get(e.link, identity)
    await sender._unavailable(e.link.id, identity, e.link.binding_generation, "claim", item.parts[0]["part_id"], SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.txt"}}))
    # Simulate the warning's actual visible ACK before a new plain intent closes
    # only the question controls; the media retry remains a separate intent.
    async with e.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
        parts = deepcopy(row.parts)
        parts[1].update(state="sent", message_ids=[3301], sent_at=now[0].isoformat())
        row.parts = parts
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))
        retry = next(control for control in controls if control.kind == "file_retry")
        retry.message_id = 3301
        retry_id = retry.id
        for control in controls:
            if control.kind != "file_retry":
                control.consumed_at, control.closed_reason = now[0], "command"
    metadata = {"path": PATH, "filename": "report.txt", "size": 4, "mime_type": "text/plain", "state": "ready"}
    assert await e.outbox.retry_file_callback(e.link, retry_id, metadata=metadata, callbacks=callbacks, guard=guard)
    now[0] += timedelta(seconds=2)
    await claim()
    await sender._run(e.link.id, identity, e.link.binding_generation, "claim")
    final = await e.outbox.get(e.link, identity)
    assert validate_mixed_reply_basis(final).kind == "human_input_files"
    assert final.parts[2]["state"] == "retired" and final.parts[-1]["state"] == "sent"
    assert sent == [final.parts[-1]["part_id"]]


@pytest.mark.asyncio
async def test_mixed_v5_steps_require_original_question_visibility_and_survive_input_gc(coordinator_environment):
    from sqlalchemy import delete

    from app.telegram_bots.human_input_source import validate_human_input_reply_basis
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    e = coordinator_environment
    source, guard, callbacks, identity = await planned_mixed(e)
    record = source.record
    ui = [{"kind": "text", "html": "Next field", "visible_text": "Next field", "reply_to_message_id": record.origins[0].message_id, "force_reply": True}]

    async def step():
        return await e.outbox.plan_human_input_step(record, source, step_id="field:1", parts=ui, keyboard=[], callbacks=callbacks, guard=guard)

    with pytest.raises(ValueError):
        await step()
    async with e.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
        parts = deepcopy(row.parts)
        parts[0].update(state="sent", message_ids=[3401], sent_at=e.outbox.clock().isoformat())
        row.parts = parts
    with pytest.raises(ValueError):
        await step()
    async with e.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
        parts = deepcopy(row.parts)
        parts[-1].update(state="sent", message_ids=[3402], sent_at=e.outbox.clock().isoformat())
        row.parts = parts
        await writer.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    child = await step()
    item = await e.outbox.get(e.link, child)
    assert item.payload["reply_basis"]["version"] == 5 and item.payload["reply_basis"]["outcome"]["request_basis"]["version"] == 2
    assert item.payload["reply_basis"]["step_id"] == "field:1" and all(part["kind"] == "text" for part in item.parts)
    assert validate_human_input_reply_basis(item).record.publication_id == record.publication_id
    assert await step() == child


@pytest.mark.asyncio
async def test_mixed_source_rejects_changed_whole_projection_order_or_keyboard(coordinator_environment):
    from app.telegram_bots.mixed_source import validate_mixed_reply_basis

    e = coordinator_environment
    source, guard, callbacks, identity = await planned_mixed(e)
    item = await e.outbox.get(e.link, identity)
    for change in ("literal_version", "file_filtered", "ui_filtered", "reorder", "keyboard", "source_projection"):
        payload = deepcopy(item.payload)
        basis = payload["reply_basis"]
        if change == "literal_version":
            basis["version"] = True
        elif change == "file_filtered":
            basis["files"] = []
        elif change == "ui_filtered":
            basis["human_input_parts"] = []
        elif change == "reorder":
            basis["parts"].reverse()
        elif change == "keyboard":
            payload["product_keyboard_basis"] = []
        else:
            basis["outcome"]["projection"]["inventory"]["document"]["items"].pop(0)
        with pytest.raises(ValueError, match="human-input-source-conflict"):
            validate_mixed_reply_basis(replace(item, payload=payload))


@pytest.mark.asyncio
async def test_original_mixed_metadata_guard_cannot_be_extended_after_head(coordinator_environment):
    from sqlalchemy import select

    from app.telegram_bots.mixed_source import prepare_mixed_files
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = coordinator_environment
    source, guard, callbacks, basis = await mixed_environment(e)
    active, heads = [True], []
    original_responder = e.responder

    async def respond(incoming):
        if incoming.method == "HEAD":
            heads.append(incoming.url.path)
            active[0] = False
        return await original_responder(incoming)

    e.responder = respond

    class OriginalGuard:
        captured_link = e.link

        async def prepare(self):
            if not active[0]:
                raise ValueError("original read retired")
            await guard.prepare()
            if not active[0]:
                raise ValueError("original read retired")

        async def check(self, writer):
            current = await guard.check(writer)
            if not active[0]:
                raise ValueError("original read retired")
            return current

    with pytest.raises(ValueError, match="publication-guard-unavailable"):
        await prepare_mixed_files(source.record, source, http_owner=e.http, guard=OriginalGuard(), capacity=1024, max_bytes=1024)
    assert len(heads) == 1
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + source.record.turn_id)))


@pytest.mark.asyncio
async def test_mixed_v5_step_relocation_rebinds_only_pending_prompt_and_arms_exact_ack(coordinator_environment):
    from datetime import timedelta

    from app.telegram_bots.callbacks import Prompts
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = coordinator_environment
    source, guard, callbacks, primary = await planned_mixed(e)
    now = [e.outbox.clock()]
    callbacks.clock = e.outbox.clock = lambda: now[0]
    async with e.outbox._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, primary, with_for_update=True)
        parts = deepcopy(row.parts)
        for index, part in enumerate(parts):
            part.update(state="sent", message_ids=[3500 + index], sent_at=now[0].isoformat())
        row.parts, row.state = parts, "sent"
    record = source.record
    ui = [{"kind": "text", "html": "Other answer", "visible_text": "Other answer", "reply_to_message_id": record.origins[0].message_id, "force_reply": True}]
    prompts = Prompts(callbacks)
    async with callbacks.writer(e.link, guard) as writer:
        child = await e.outbox.plan_human_input_step(record, source, step_id="other", parts=ui, keyboard=[], callbacks=callbacks, guard=guard, session=writer)
        prompt_id = await prompts.open(
            e.link, "clarify_other", {"thread_id": record.scope.thread_id, "request_id": "clarification:call"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None, defer_visibility=True, guard=guard, session=writer
        )
        await prompts.attach(e.link, prompt_id, outbox_id=child, guard=guard, session=writer)
    previous = await prompts.get(e.link, prompt_id, guard=guard)

    async def deliver(**kwargs):
        return [3601]

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(e.outbox, run_id="worker", deliver_part=deliver, eligibility=eligible)

    async def claim():
        async with e.outbox._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, child, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"

    await claim()
    await sender._replace(e.link, await e.outbox.get(e.link, child), "claim", reason="protection")
    current = await prompts.get(e.link, prompt_id, guard=guard)
    assert current.payload["prompt_visibility"]["part_id"] != previous.payload["prompt_visibility"]["part_id"]
    assert current.payload["presentation_version"] == 1 and not prompts.is_visible(current)
    now[0] += timedelta(seconds=3)
    await claim()
    await sender._run(e.link.id, child, e.link.binding_generation, "claim")
    actual = await prompts.current(e.link, e.link.chat_id, None, guard=guard)
    assert prompts.is_visible(actual) and actual.message_id == 3601
    assert actual.expires_at == now[0] + timedelta(seconds=callbacks.config.prompts.ttl_seconds)

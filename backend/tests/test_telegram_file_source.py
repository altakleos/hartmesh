"""Closed outbound file source qualification; synthetic proof, no provider/network."""

import hashlib
from copy import deepcopy

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from test_output_inventory import inventory_document
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401
from test_telegram_publication_primary import _output

from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest


def record():
    scope = PublicationScope(
        link_id="l" * 32,
        owner_user_id="owner",
        bot_id=42,
        agent_kind="lead_agent",
        agent_ref="lead_agent",
        assistant_id="lead_agent",
        agent_name=None,
        connection_id="connection",
        telegram_user_id=7,
        chat_id=7,
        message_thread_id=None,
        binding_generation=1,
        thread_id="thread",
        endpoint_id="e" * 64,
    )
    origin = PublicationOrigin(link_id=scope.link_id, message_id=71, update_id=171, chat_id=7, message_thread_id=None, binding_generation=1)
    return PublicationRecord(scope, "t" * 32, hashlib.sha256(("t" * 32 + ":m71").encode()).hexdigest()[:32], "m71", (origin,), 1, "a" * 64, True, "managed-primary-run")


def file_output(record, *, reason=None, paths=("/mnt/user-data/outputs/report.txt",)):
    output = _output(record, reason=reason)
    receipt = output.output_evidence
    doc = inventory_document(version=2, owner_user_id=receipt.owner_user_id, thread_id=receipt.thread_id, run_id=receipt.run_id, input_message_id=receipt.input_message_id, output_receipt_sha256=output_receipt_digest(receipt))
    doc.pop("scan")
    doc["output_view"] = {"kind": "no_admitted_output_view", "scope": {"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"}}
    doc["items"] = [{"kind": "file", "path": path, "surfaces": ["presentation_reference"], "witnesses": [{"kind": "root_snapshot", "root_epoch": doc["root_epoch"], "message_id": "synthetic-assistant"}]} for path in paths]
    evidence = build_event_inventory_evidence(receipt, status=output.run_status, inventory_event_seq=9, document=doc)
    return RunOutputProjectionV2.model_validate({"version": 2, "output": output, "inventory": {"state": "matched", "evidence": evidence, "document": doc, "reason": None}})


def test_complete_file_source_and_explicit_file_only_are_supported():
    from app.telegram_bots.file_source import qualify_file_output

    saved = record()
    assert qualify_file_output(saved, file_output(saved)) == ("/mnt/user-data/outputs/report.txt",)
    assert qualify_file_output(saved, file_output(saved, reason="unsupported_output")) == ("/mnt/user-data/outputs/report.txt",)


@pytest.mark.parametrize("reason", ["unsupported_input", "human_input_required", "ambiguous_source", "missing_message_identity", "failed", "cancelled"])
def test_other_unavailable_reasons_do_not_become_file_only(reason):
    from app.telegram_bots.file_source import qualify_file_output

    saved = record()
    with pytest.raises(ValueError, match="publication-source-conflict"):
        qualify_file_output(saved, file_output(saved, reason=reason))


def test_exact_record_and_complete_inventory_are_required():
    from app.telegram_bots.file_source import qualify_file_output

    saved = record()
    projection = file_output(saved)
    malformed = deepcopy(projection)
    object.__setattr__(malformed.inventory.document, "version", True)
    with pytest.raises(ValueError):
        qualify_file_output(saved, malformed)
    with pytest.raises(ValueError):
        qualify_file_output(saved, file_output(saved, paths=()))


@pytest.mark.asyncio
async def test_file_primary_seals_actual_source_and_caption_failure_preserves_answer(publication_environment):
    from app.telegram_bots.model_source import validate_model_reply_basis
    from app.telegram_bots.producer import Outbox

    e = publication_environment
    from datetime import UTC, datetime

    from telegram import Chat, Message, Update, User

    original, _ = await e.inbox.accept(
        e.link,
        Update(1801, message=Message(801, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Fixture", False), text="Produce report")),
        sanitized_input={"type": "text", "text": "Produce report"},
    )
    original = await e.inbox.route(original, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    saved = await e.reserve((original,))
    assert await e.inbox.mark_dispatch_started(saved, guard=e.guard_factory())
    saved = await e.inbox.bind_publication_run(saved, run_id="managed-primary-run", guard=e.guard_factory())
    output = file_output(saved)
    box = Outbox(e.sessions)
    from app.telegram_bots.file_source import prepare_file_reply

    files = [{"path": "/mnt/user-data/outputs/report.txt", "filename": "report.txt", "size": 4, "mime_type": "text/plain", "state": "ready"}]
    basis, parts = prepare_file_reply(saved, output, files=files, capacity=1024, max_bytes=1024)
    identity = await box.plan_file_publication_reply(saved, basis=basis, parts=parts, guard=e.guard_factory())
    item = await box.get(e.link, identity)
    proof = validate_model_reply_basis(item)
    assert proof.kind == "file_outputs" and proof.output == output
    assert len(item.parts) == 1 and item.parts[0]["caption_text"] == "synthetic answer"
    assert item.payload["reply_basis"]["inputs"]["inputs"]


async def claimed_primary(e, *, paths=("/mnt/user-data/outputs/report.txt",), display=None, conversation=False):
    from datetime import UTC, datetime

    from telegram import Chat, Message, Update, User

    from app.telegram_bots.file_source import prepare_file_reply
    from app.telegram_bots.producer import Outbox
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    origin, _ = await e.inbox.accept(
        e.link,
        Update(1901, message=Message(901, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Fixture", False), text="Prepare files")),
        sanitized_input={"type": "text", "text": "Prepare files"},
    )
    origin = await e.inbox.route(origin, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    saved = await e.reserve((origin,))
    assert await e.inbox.mark_dispatch_started(saved, guard=e.guard_factory())
    saved = await e.inbox.bind_publication_run(saved, run_id="managed-primary-run", guard=e.guard_factory())
    output = file_output(saved, paths=paths)
    box = Outbox(e.sessions)
    files = [{"path": path, "filename": path.rsplit("/", 1)[-1], "size": 4, "mime_type": "image/png" if path.endswith("png") else "text/plain", "state": "ready"} for path in paths]
    basis, parts = prepare_file_reply(
        saved,
        output,
        files=files,
        capacity=1024,
        max_bytes=1024,
        display=display,
        conversation_url="https://hartmesh.example/workspace/" + ("agents/" + saved.scope.agent_ref + "/" if saved.scope.agent_kind == "custom_agent" else "") + "chats/" + saved.scope.thread_id if conversation else None,
    )
    shown = None
    if display is not None:
        from app.telegram_bots.inbox import publication_identity
        from app.telegram_bots.stopped_preview import StoppedSegment, _issue_shown_preview

        shown = _issue_shown_preview(publication_identity=publication_identity(saved), run_id=saved.run_id, segments=tuple(StoppedSegment(**segment) for segment in display["segments"]), check_current=lambda: e.attempt_current[0])
    identity = await box.plan_file_publication_reply(saved, basis=basis, parts=parts, guard=e.guard_factory(), shown=shown)
    async with box._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, identity, with_for_update=True)
        row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"
        values = deepcopy(row.parts)
        # The text answer is already ACKed before this media delivery.
        for value in values:
            value.update(state="sent", message_ids=[1001])
        values[-1].update(state="sending", message_ids=[])
        row.parts = values
    capture = dict(link_id=e.link.id, outbox_id=identity, part_id=(await box.get(e.link, identity)).parts[-1]["part_id"], claim_owner="worker", claim_id="claim", token_revision=e.link.token_revision)
    return box, saved, output, await box.capture_sending_part(**capture), capture


@pytest.mark.asyncio
async def test_hash_is_first_authorized_bytes_and_never_overwritten(publication_environment):
    e = publication_environment
    box, record, output, captured, capture = await claimed_primary(e)
    path = captured.part["files"][0]["path"]
    assert await box.record_file_hashes(captured, {path: "1" * 64})
    current = await box.capture_sending_part(**capture)
    assert not await box.record_file_hashes(current, {path: "2" * 64})
    final = await box.capture_sending_part(**capture)
    assert final.part["source_ref"]["output_files"] == [{"path": path, "sha256": "1" * 64}]
    assert await box.fallback_to_plain(final)
    item = await box.get(e.link, final.item.id)
    assert item.parts[0]["plain_fallback"] is True and item.parts[0]["source_ref"] == final.part["source_ref"]
    from app.telegram_bots.model_source import validate_model_reply_basis

    assert validate_model_reply_basis(item).kind == "file_outputs"


@pytest.mark.asyncio
async def test_claimed_classification_replacement_retains_sources_hashes_and_prior_ack(publication_environment):
    from app.telegram_bots.model_source import validate_model_reply_basis

    e = publication_environment
    box, record, output, captured, capture = await claimed_primary(e, paths=("/mnt/user-data/outputs/a.png", "/mnt/user-data/outputs/b.png"))
    assert captured.part["kind"] == "album"
    assert await box.record_file_hashes(captured, {file["path"]: "3" * 64 for file in captured.part["files"]})
    current = await box.capture_sending_part(**capture)
    templates = [{"kind": "document", "files": [file], "unavailable": None} for file in current.part["files"]]
    assert await box.replace_output_parts(current, templates, reason="photo-classification")
    assert not await box.replace_output_parts(current, templates, reason="photo-classification")
    item = await box.get(e.link, current.item.id)
    assert validate_model_reply_basis(item).kind == "file_outputs"
    assert item.parts[0]["state"] == "sent" and item.parts[0]["message_ids"] == [1001]
    assert item.parts[1]["state"] == "retired"
    assert [part["kind"] for part in item.parts[2:]] == ["document", "document"]
    assert all(part["source_ref"]["output_files"][0]["sha256"] == "3" * 64 for part in item.parts[2:])


@pytest.mark.asyncio
async def test_caption_failure_keeps_full_answer_and_original_source_for_retry(publication_environment):
    from app.telegram_bots.model_source import validate_model_reply_basis
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender, SourceUnavailable

    e = publication_environment
    box, record, output, captured, capture = await claimed_primary(e)

    async def unused(**kwargs):
        raise AssertionError("no transport expected")

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(box, run_id="worker", deliver_part=unused, eligibility=eligible)
    await sender._unavailable(e.link.id, captured.item.id, e.link.binding_generation, "claim", captured.part["part_id"], SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.txt"}}))
    item = await box.get(e.link, captured.item.id)
    assert item.parts[0]["state"] == "unavailable"
    assert item.parts[1]["visible_text"] == "synthetic answer"
    assert item.parts[2]["source_ref"] == {"key": "file.out_failed", "values": {"file": "report.txt"}}
    assert item.parts[2]["callback_group_id"]
    assert validate_model_reply_basis(item).kind == "file_outputs"


@pytest.mark.asyncio
@pytest.mark.parametrize("relocate", [False, True])
async def test_outbound_retry_reuses_saved_thread_delivery_only_after_input_gc(publication_environment, relocate):
    from sqlalchemy import delete, select

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender, SourceUnavailable
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

    e = publication_environment
    box, record, output, captured, capture = await claimed_primary(e)

    async def unused(**kwargs):
        raise AssertionError("no transport expected")

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(box, run_id="worker", deliver_part=unused, eligibility=eligible)
    await sender._unavailable(e.link.id, captured.item.id, e.link.binding_generation, "claim", captured.part["part_id"], SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.txt"}}))
    if relocate:
        async with box._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"
        await sender._replace(e.link, await box.get(e.link, captured.item.id), "claim", reason="topic-missing")
    callbacks = CallbackRepository(e.sessions)
    async with box._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
        parts = deepcopy(row.parts)
        for value in parts:
            if value["state"] == "pending":
                value.update(state="sent", message_ids=[1201])
        row.parts = parts
        row.state = "sent"
        selected = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == row.id))
        callback_id = selected.id
        selected.message_id = 1201
        await writer.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    metadata = {"path": "/mnt/user-data/outputs/report.txt", "filename": "report.txt", "size": 4, "mime_type": "text/plain", "state": "ready"}
    assert await box.retry_file_callback(e.link, callback_id, metadata=metadata, callbacks=callbacks, guard=e.guard_factory())
    assert not await box.retry_file_callback(e.link, callback_id, metadata=metadata, callbacks=callbacks, guard=e.guard_factory())
    item = await box.get(e.link, captured.item.id)
    assert item.parts[-1]["state"] == "pending" and item.parts[-1]["kind"] == "document"
    assert "caption_html" not in item.parts[-1]
    assert item.parts[-1]["source_ref"] == captured.part["source_ref"]
    from app.telegram_bots.model_source import validate_model_reply_basis

    assert validate_model_reply_basis(item).record.scope.thread_id == record.scope.thread_id
    async with e.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id)))
        cleanup = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "file-retry-cleanup:" + callback_id))
        assert cleanup is not None and cleanup.parts[0]["remove_keyboard"] is True and cleanup.parts[0]["target_message_id"] == 1201


def edit_display():
    text = "**synthetic answer**"
    return {
        "kind": "model_text",
        "stop_id": None,
        "waiting": False,
        "segments": [
            {
                "span_id": "span",
                "message_id": "synthetic-assistant",
                "sequence": 1,
                "text": text,
                "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                "mode": "edit",
                "anchor_message_id": 1401,
                "draft_id": None,
                "part_index": 0,
                "part_id": "preview-part",
                "source_start": 0,
                "source_end": len(text),
            }
        ],
    }


def test_caption_display_is_validated_before_it_can_authorize_cleanup():
    from app.telegram_bots.file_source import prepare_file_reply

    saved = record()
    files = [{"path": "/mnt/user-data/outputs/report.txt", "filename": "report.txt", "size": 4, "mime_type": "text/plain", "state": "ready"}]
    output = file_output(saved)
    basis, parts = prepare_file_reply(saved, output, files=files, capacity=1024, max_bytes=1024, display=edit_display())
    assert parts[0]["kind"] == "document" and basis["display"]["segments"][0]["anchor_message_id"] == 1401
    malformed = edit_display()
    malformed["segments"][0]["anchor_message_id"] = True
    with pytest.raises(ValueError, match="publication-source-conflict"):
        prepare_file_reply(saved, output, files=files, capacity=1024, max_bytes=1024, display=malformed)


@pytest.mark.asyncio
async def test_caption_ack_survives_cleanup_retries_and_server_floor(publication_environment):
    from datetime import timedelta

    from app.telegram_bots.producer import DeliveryEligibility, DeliveryRetry, OutboxSender
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = publication_environment
    box, record, output, captured, capture = await claimed_primary(e, display=edit_display())
    sends, cleanup = [], []

    async def deliver(**kwargs):
        sends.append(kwargs)
        return [1501]

    async def eligible(*args):
        return DeliveryEligibility("allow")

    async def clean(**kwargs):
        proof = await box.capture_caption_cleanup(**kwargs, token_revision=e.link.token_revision)
        assert proof.part["state"] == "sent" and proof.part["message_ids"] == [1501]
        assert proof.item.payload["caption_cleanup"]["message_ids"] == [1401]
        cleanup.append(kwargs)
        if len(cleanup) == 1:
            raise DeliveryRetry(30)
        return True

    sender = OutboxSender(box, run_id="worker", deliver_part=deliver, eligibility=eligible, cleanup_caption=clean)
    await sender._run(e.link.id, captured.item.id, e.link.binding_generation, "claim")
    item = await box.get(e.link, captured.item.id)
    assert item.parts[0]["state"] == "sent" and item.payload["caption_cleanup"]["state"] == "pending"

    async def claim_again():
        async with box._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"

    await claim_again()
    began = box.clock()
    await sender._run(e.link.id, item.id, e.link.binding_generation, "claim")
    async with e.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, item.id)
        assert row.not_before.replace(tzinfo=began.tzinfo) >= began + timedelta(seconds=30)
    await claim_again()
    await sender._run(e.link.id, item.id, e.link.binding_generation, "claim")
    final = await box.get(e.link, item.id)
    assert final.parts[0]["state"] == "sent" and final.payload["caption_cleanup"]["state"] == "done"
    assert len(sends) == 1 and len(cleanup) == 2


async def fresh_source(e):
    from datetime import UTC, datetime

    from telegram import Chat, Message, Update, User

    origin, _ = await e.inbox.accept(
        e.link,
        Update(2001, message=Message(1001, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Fixture", False), text="Original task")),
        sanitized_input={"type": "text", "text": "Original task"},
    )
    origin = await e.inbox.route(origin, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    saved = await e.reserve((origin,))
    assert await e.inbox.mark_dispatch_started(saved, guard=e.guard_factory())
    return await e.inbox.bind_publication_run(saved, run_id="managed-primary-run", guard=e.guard_factory())


@pytest.mark.asyncio
@pytest.mark.parametrize("retirement", ["original", "live-preview-owner"])
async def test_file_head_cannot_commit_after_original_or_display_owner_changes(publication_environment, retirement):
    from types import SimpleNamespace

    from sqlalchemy import select

    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.producer import Outbox
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    e = publication_environment
    saved = await fresh_source(e)
    output = file_output(saved)
    active, reads = [False], []

    async def describe(owner, thread, path, *, before_dispatch):
        await before_dispatch()
        reads.append(path)
        if retirement == "original":
            e.attempt_current[0] = False
        else:
            active[0] = True
        return SimpleNamespace(path=path, filename="report.txt", size=4, mime_type="text/plain")

    async def reader(*args):
        return output

    box = Outbox(e.sessions)
    source = ModelSource(read_output=reader, outbox=box, http_owner=SimpleNamespace(describe_artifact=describe), file_output_active=lambda *_: active[0])
    with pytest.raises(ValueError):
        await source.plan_file_reply(saved, e.link, output, guard=e.guard_factory())
    assert len(reads) == 1
    async with e.sessions() as sql:
        assert not list(await sql.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + saved.turn_id)))
        mark = await sql.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 1001))
        assert not mark.route_complete and mark.outbox_id is None
    # Recovery cannot even HEAD while an actual owner can freeze its preview.
    if retirement == "live-preview-owner":
        assert await source.plan_file_reply(saved, e.link, output, guard=e.guard_factory()) is None
        assert len(reads) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("file_only", [False, True])
async def test_failed_metadata_preserves_verified_text_and_explicit_file_only_identity(publication_environment, file_only):
    from types import SimpleNamespace

    from sqlalchemy import select

    from app.telegram_bots.model_source import ModelSource, validate_model_reply_basis
    from app.telegram_bots.producer import Outbox
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = publication_environment
    saved = await fresh_source(e)
    output = file_output(saved, reason="unsupported_output" if file_only else None)

    async def describe(*args, before_dispatch):
        await before_dispatch()
        raise LookupError("synthetic missing artifact")

    async def reader(*args):
        return output

    box = Outbox(e.sessions)
    source = ModelSource(read_output=reader, outbox=box, http_owner=SimpleNamespace(describe_artifact=describe))
    identity = await source.plan_file_reply(saved, e.link, output, guard=e.guard_factory())
    item = await box.get(e.link, identity)
    proof = validate_model_reply_basis(item)
    assert proof.output == output
    assert item.parts[-1]["unavailable"] == "metadata"
    assert [part["kind"] for part in item.parts] == (["document"] if file_only else ["text", "document"])
    if not file_only:
        assert item.parts[0]["visible_text"] == "synthetic answer"
    async with e.sessions() as sql:
        mark = await sql.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 1001))
        assert mark.route_complete and mark.assistant_message_id == (None if file_only else "synthetic-assistant")


@pytest.mark.asyncio
async def test_first_member_hash_survives_later_batch_failure(publication_environment):
    e = publication_environment
    box, saved, output, captured, capture = await claimed_primary(e, paths=("/mnt/user-data/outputs/a.png", "/mnt/user-data/outputs/b.png"))
    first, second = [file["path"] for file in captured.part["files"]]
    assert await box.record_file_hashes(captured, {first: "4" * 64})
    current = await box.capture_sending_part(**capture)
    assert current.part["source_ref"]["output_files"] == [{"path": first, "sha256": "4" * 64}, {"path": second, "sha256": None}]
    assert not await box.record_file_hashes(current, {first: "5" * 64})
    assert not await box.record_file_hashes(current, {"/mnt/user-data/outputs/not-saved.png": "6" * 64})
    assert await box.record_file_hashes(current, {second: "6" * 64})
    final = await box.capture_sending_part(**capture)
    assert [file["sha256"] for file in final.part["source_ref"]["output_files"]] == ["4" * 64, "6" * 64]


@pytest.mark.asyncio
@pytest.mark.parametrize("oversize", [False, True])
async def test_file_notices_offer_original_conversation_and_keep_each_ack_anchor(publication_environment, oversize):
    from sqlalchemy import select

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender, SourceUnavailable
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = publication_environment
    paths = ("/mnt/user-data/outputs/a.png",) if oversize else ("/mnt/user-data/outputs/a.png", "/mnt/user-data/outputs/b.png")
    box, saved, output, captured, capture = await claimed_primary(e, paths=paths, conversation=True)

    async def unused(**kwargs):
        raise AssertionError("no network")

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(box, run_id="worker", deliver_part=unused, eligibility=eligible)
    key = "file.too_large_out" if oversize else "file.out_failed"
    await sender._unavailable(e.link.id, captured.item.id, e.link.binding_generation, "claim", captured.part["part_id"], SourceUnavailable(source_ref={"key": key, "values": {"file": "synthetic"}}))
    item = await box.get(e.link, captured.item.id)
    warnings = [part for part in item.parts if part["source_ref"].get("key") == key]
    assert len(warnings) == len(paths)
    callbacks = CallbackRepository(e.sessions)
    expected_anchors = {}
    for index, warning in enumerate(warnings):
        async with box._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True)
            row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"
            parts = deepcopy(row.parts)
            for value in parts:
                if value["state"] == "pending":
                    value.update(state="sent", message_ids=[1600 + index])
                if value["part_id"] == warning["part_id"]:
                    value.update(state="sending", message_ids=[])
                    break
            row.parts = parts
        claimed = await box.capture_sending_part(**{**capture, "part_id": warning["part_id"]})
        keyboard = await callbacks.read_keyboard(claimed)
        assert keyboard[-1][0].label == "label.open_conversation"
        assert keyboard[-1][0].url == item.payload["reply_basis"]["conversation_url"]
        assert len(keyboard) == (1 if oversize else 2)
        if not oversize:
            assert keyboard[0][0].label == "label.send_again"
        async with box._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True)
            parts = deepcopy(row.parts)
            value = next(part for part in parts if part["part_id"] == warning["part_id"])
            value.update(state="sent", message_ids=[1700 + index])
            row.parts = parts
            expected_anchors[warning["callback_group_id"]] = 1700 + index
            if index == len(warnings) - 1:
                await sender._complete(writer, row)
    async with e.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == item.id)))
        assert len(rows) == (0 if oversize else len(paths))
        assert all(row.message_id == expected_anchors[row.group_id] for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["topic-missing", "protection"])
async def test_v6_relocation_retains_acknowledged_answer_and_exact_media_source(publication_environment, reason):
    from app.telegram_bots.model_source import validate_model_reply_basis
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender

    e = publication_environment
    box, saved, output, captured, capture = await claimed_primary(e, paths=("/mnt/user-data/outputs/a.png", "/mnt/user-data/outputs/b.png"), display=edit_display() if reason == "protection" else None)
    if reason == "protection":
        from dataclasses import replace

        from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

        async with box._writer(e.link.id) as (writer, _):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            parts = deepcopy(row.parts)
            parts[0].update(state="sending", message_ids=[])
            parts[-1]["state"] = "pending"
            row.parts = parts
        captured = replace(captured, item=await box.get(e.link, captured.item.id))

    async def unused(**kwargs):
        raise AssertionError("no network")

    async def eligible(*args):
        return DeliveryEligibility("allow")

    sender = OutboxSender(box, run_id="worker", deliver_part=unused, eligibility=eligible)
    await sender._replace(e.link, captured.item, "claim", reason=reason)
    item = await box.get(e.link, captured.item.id)
    assert validate_model_reply_basis(item).kind == "file_outputs"
    if reason == "topic-missing":
        assert item.parts[0]["state"] == "sent" and item.parts[0]["message_ids"] == [1001]
    active = [part for part in item.parts if part["state"] not in {"sent", "retired", "unavailable"}]
    assert [part["kind"] for part in active] == (["text", "album"] if reason == "protection" else ["album"])
    if reason == "protection":
        assert "target_message_id" not in active[0]
        assert active[0]["source_ref"] == item.parts[0]["source_ref"]
    assert active[-1]["source_ref"] == captured.part["source_ref"]
    assert item.message_thread_id is None


@pytest.mark.asyncio
async def test_permanent_optional_cleanup_cannot_hold_successful_file(publication_environment):
    from app.telegram_bots.producer import DeliveryEligibility, OutboxSender
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = publication_environment
    box, saved, output, captured, capture = await claimed_primary(e, display=edit_display())
    calls = []

    async def deliver(**kwargs):
        calls.append("upload")
        return [1801]

    async def eligible(*args):
        return DeliveryEligibility("allow")

    async def clean(**kwargs):
        proof = await box.capture_caption_cleanup(**kwargs, token_revision=e.link.token_revision)
        assert proof.part["state"] == "sent"
        calls.append("cleanup")
        return "unavailable"

    sender = OutboxSender(box, run_id="worker", deliver_part=deliver, eligibility=eligible, cleanup_caption=clean)
    await sender._run(e.link.id, captured.item.id, e.link.binding_generation, "claim")
    async with box._writer(e.link.id) as (writer, _):
        row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
        row.state, row.claim_owner, row.claim_id = "sending", "worker", "claim"
    await sender._run(e.link.id, captured.item.id, e.link.binding_generation, "claim")
    async with e.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, captured.item.id)
        assert row.state == "sent" and row.payload["caption_cleanup"]["state"] == "unavailable"
        assert row.parts[0]["message_ids"] == [1801]
    assert calls == ["upload", "cleanup"]


def test_mixed_structured_and_file_inventory_is_held_without_dropping_proof():
    from app.telegram_bots.file_source import qualify_file_output

    saved = record()
    projection = file_output(saved)
    payload = projection.model_dump(mode="json")
    doc = payload["inventory"]["document"]
    doc["items"].append({"kind": "structured", "carrier": "message_artifact", "value_sha256": "a" * 64, "witness": {"kind": "message_event", "event_seq": 2, "message_id": "actual-tool"}})
    evidence = build_event_inventory_evidence(projection.output.output_evidence, status="success", inventory_event_seq=9, document=doc)
    payload["inventory"]["evidence"] = evidence.model_dump(mode="json")
    with pytest.raises(ValueError, match="publication-source-conflict"):
        qualify_file_output(saved, RunOutputProjectionV2.model_validate(payload))


@pytest.mark.asyncio
async def test_saved_file_receipt_requires_exact_unique_album_ack_count(publication_environment):
    from dataclasses import replace

    from app.telegram_bots.model_source import validate_model_reply_basis

    e = publication_environment
    box, saved, output, captured, capture = await claimed_primary(e, paths=("/mnt/user-data/outputs/a.png", "/mnt/user-data/outputs/b.png"))
    for ids in ([1901], [1901, 1901], [1901, 1902, 1903]):
        parts = deepcopy(captured.item.parts)
        parts[-1].update(state="sent", message_ids=ids)
        with pytest.raises(ValueError, match="publication-source-conflict"):
            validate_model_reply_basis(replace(captured.item, parts=parts))
    parts = deepcopy(captured.item.parts)
    parts[-1].update(state="sent", message_ids=[1901, 1902])
    assert validate_model_reply_basis(replace(captured.item, parts=parts)).kind == "file_outputs"


@pytest.mark.asyncio
async def test_raw_file_writer_cannot_claim_preview_ids_without_original_capsule(publication_environment):
    from sqlalchemy import select

    from app.telegram_bots.file_source import prepare_file_reply
    from app.telegram_bots.inbox import publication_identity
    from app.telegram_bots.producer import Outbox
    from app.telegram_bots.stopped_preview import StoppedSegment, _issue_shown_preview
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = publication_environment
    saved = await fresh_source(e)
    box = Outbox(e.sessions)
    files = [{"path": "/mnt/user-data/outputs/report.txt", "filename": "report.txt", "size": 4, "mime_type": "text/plain", "state": "ready"}]
    basis, parts = prepare_file_reply(saved, file_output(saved), files=files, capacity=1024, max_bytes=1024, display=edit_display())
    with pytest.raises(ValueError, match="publication-source-conflict"):
        await box.plan_file_publication_reply(saved, basis=basis, parts=parts, guard=e.guard_factory())
    foreign = edit_display()["segments"][0]
    foreign["anchor_message_id"] = 2401
    shown = _issue_shown_preview(publication_identity=publication_identity(saved), run_id=saved.run_id, segments=(StoppedSegment(**foreign),), check_current=lambda: True)
    with pytest.raises(ValueError, match="publication-source-conflict"):
        await box.plan_file_publication_reply(saved, basis=basis, parts=parts, guard=e.guard_factory(), shown=shown)
    async with e.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + saved.turn_id)))

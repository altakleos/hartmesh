"""Actual Work results retain one delivery and source-owned media receipts."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def prepare_delivery(e):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots.files import FileBytesBudget
    from app.telegram_bots.work_result_delivery import WorkReviewPlans
    from app.telegram_bots.work_result_source import WorkResultSources

    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    services = e.channel._services
    e.delivery_clock = ManualClock(services.outbox.clock())
    services.outbox.clock = e.delivery_clock.now
    services.file_bytes_budget = FileBytesBudget(services.config.files.in_memory_max_bytes)
    plans = WorkReviewPlans(outbox=e.plans.outbox, reader=e.reader, callbacks=e.plans.callbacks)
    identity = await plans.plan(e.link, request_id=e.result["human_input_request_id"], dedupe_key="result:" + uuid4().hex)
    services.work_result_sources = WorkResultSources(reader=e.reader, outbox=e.plans.outbox, callbacks=e.plans.callbacks)
    return identity


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_file_upload_precedes_card_and_arms_controls_after_whole_ack(work_exports, monkeypatch):
    from test_telegram_bot_adapter import message

    e = work_exports
    identity = await prepare_delivery(e)
    charges = []
    sdk = e.channel.bot.request
    request = sdk.do_request

    async def uploaded(*args, **kwargs):
        if kwargs.get("request_data") and kwargs["request_data"].multipart_data:
            charges.append(e.channel._services.file_bytes_budget.used)
        return await request(*args, **kwargs)

    monkeypatch.setattr(sdk, "do_request", uploaded)
    message(sdk, "sendDocument", identity=901)
    message(sdk, identity=902)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts[0]["message_ids"] == [901]
        assert row.parts[-1]["state"] == "pending"
        assert all(control.payload["presentation_pending"] for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == ["sendDocument", "sendMessage"]
    assert calls[0].parameters["disable_notification"] is True
    assert "reply_to_message_id" not in calls[0].parameters
    assert charges == [len(b"verified result")]
    assert e.channel._services.file_bytes_budget.used == 0
    assert e.native_reads == ["outputs/result.txt"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent"
        assert [part["message_ids"] for part in row.parts] == [[901], [902]]
        for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])):
            assert control.payload["presentation_pending"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_export_final_provider_snapshot_requires_review_route_ceiling(work_exports, monkeypatch):
    from test_telegram_bot_adapter import message

    import app.telegram_bots.work_file_exports as exports

    e = work_exports
    await prepare_delivery(e)
    original = exports.prepare_attention
    checked = []

    async def prepared(*args, **kwargs):
        admission = await original(*args, **kwargs)

        async def without_write(**options):
            held, target = await admission(**options)
            if options.get("session") is not None:
                checked.append(True)
                held = held - {"agents:write"}
            return held, target

        return without_write

    monkeypatch.setattr(exports, "prepare_attention", prepared)
    message(e.channel.bot.request, "sendDocument", identity=911)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert checked
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]
    assert not e.native_reads


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
async def test_final_album_dispatch_admits_every_referenced_space_operation_guard(work_exports, monkeypatch):
    from test_telegram_outbound_upload import message

    import app.telegram_bots.work_result_upload as upload
    from deerflow.spaces.contract import SpaceConflict

    e = work_exports
    await prepare_delivery(e)
    original = upload.authorize_work_file_batch
    guard = e.files.registry.operation_guard
    at_dispatch, checked = [], []

    async def dispatch(*args, **kwargs):
        at_dispatch.append(True)
        return await original(*args, **kwargs)

    async def pending(writer, rows, requests, operation_id):
        if at_dispatch and e.source_space.id in requests:
            checked.append(set(requests))
            raise SpaceConflict("The second Space has a pending operation")
        if guard is not None:
            await guard(writer, rows, requests, operation_id)

    monkeypatch.setattr(upload, "authorize_work_file_batch", dispatch)
    monkeypatch.setattr(e.files.registry, "operation_guard", pending)
    e.channel.bot.request.enqueue("sendMediaGroup", result=[message(921), message(922)])
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert at_dispatch and checked
    assert checked[-1] == {e.home.id, e.source_space.id}
    assert len(e.native_reads) == 2
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMediaGroup"]
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_card_transport_retry_does_not_resend_acknowledged_result_file(work_exports):
    from test_telegram_bot_adapter import message

    e = work_exports
    identity = await prepare_delivery(e)
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=931)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    sdk.enqueue("sendMessage", status=503, body={"ok": False, "description": "Unavailable", "error_code": 503})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts[0]["message_ids"] == [931]
        assert row.parts[-1]["state"] == "pending"
        assert all(control.payload["presentation_pending"] for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
    message(sdk, identity=932)
    e.delivery_clock.advance(120)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, e.dispatch_errors)
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
    assert len(e.export_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
@pytest.mark.parametrize("failure,key", [("missing", "file.out_failed"), ("large", "file.too_large_out"), ("grant", "file.no_access")])
async def test_file_failure_notice_must_be_acknowledged_before_review_card(work_exports, failure, key):
    from test_telegram_bot_adapter import message

    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Permission

    e = work_exports
    identity = await prepare_delivery(e)
    if failure == "missing":
        await e.files.remove(actor=e.instance.principal, space_id=e.home.id, expected_generation=e.home.generation, operation_id=uuid4().hex, path="outputs/result.txt")
    elif failure == "large":
        services = e.channel._services
        services.config = services.config.model_copy(update={"files": services.config.files.model_copy(update={"max_outbound_bytes": 3})})
    else:
        async with e.db.sessions.begin() as writer:
            (await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id))).permissions = int(Permission.READ)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts[0]["state"] == "unavailable", (row.parts, e.dispatch_errors)
        assert row.parts[1]["result_file_warning"]["key"] == key
        assert row.parts[-1]["state"] == "pending"
    message(e.channel.bot.request, identity=941)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    message(e.channel.bot.request, identity=942)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, e.dispatch_errors)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
@pytest.mark.parametrize("change", ["space", "request", "claim"])
async def test_album_final_provider_snapshot_rechecks_all_spaces_request_and_claim(work_exports, monkeypatch, change):
    from test_telegram_outbound_upload import message

    import app.gateway.authz as authz
    import app.telegram_bots.work_file_exports as exports
    import app.telegram_bots.work_result_upload as upload
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Permission

    e = work_exports
    identity = await prepare_delivery(e)
    batch, prepare, resolve = upload.authorize_work_file_batch, exports.prepare_attention, authz.resolve_route_permissions
    active, changed = {}, []

    async def dispatch(*args, **kwargs):
        active["dispatch"] = True
        try:
            return await batch(*args, **kwargs)
        finally:
            active.clear()

    async def prepared(*args, **kwargs):
        admission = await prepare(*args, **kwargs)

        async def check(**options):
            if active.get("dispatch"):
                active["writer"] = options.get("session")
            return await admission(**options)

        return check

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if active.get("writer") is not None and not changed:
            writer = active["writer"]
            if change == "space":
                (await writer.get(SpaceGrantRow, (e.source_space.id, "human", e.actor.subject_id))).permissions = int(Permission.READ)
            elif change == "request":
                (await writer.get(HumanInputRequestRow, e.result["human_input_request_id"])).revision += 1
            else:
                (await writer.get(TelegramOutboxRow, identity)).claim_id = "retired-claim"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(upload, "authorize_work_file_batch", dispatch)
    monkeypatch.setattr(exports, "prepare_attention", prepared)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    e.channel.bot.request.enqueue("sendMediaGroup", result=[message(951), message(952)])
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and len(e.native_reads) == 2
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMediaGroup"]
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_album"], indirect=True)
@pytest.mark.parametrize("rejected", [False, True])
async def test_duplicate_paths_in_distinct_spaces_send_in_order_with_durable_photo_fallback(work_exports, rejected):
    from test_telegram_outbound_upload import message

    e = work_exports
    identity = await prepare_delivery(e)
    sdk = e.channel.bot.request
    if rejected:
        sdk.enqueue("sendMediaGroup", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: invalid media"})
        sdk.enqueue("sendDocument", result=message(961))
        sdk.enqueue("sendDocument", result=message(962))
    else:
        sdk.enqueue("sendMediaGroup", result=[message(961), message(962)])
    sdk.enqueue("sendMessage", result=message(963))
    for _ in range(4 if rejected else 2):
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.parts, e.dispatch_errors)
        if rejected:
            assert [part["state"] for part in row.parts] == ["retired", "sent", "sent", "sent"]
            assert row.payload["work_result_ledger"]["events"][0]["reason"] == "photo-rejected"
        else:
            assert row.parts[0]["message_ids"] == [961, 962]
    assert [call.endpoint for call in sdk.calls if call.endpoint in {"sendMediaGroup", "sendDocument", "sendMessage"}] == (["sendMediaGroup", "sendDocument", "sendDocument", "sendMessage"] if rejected else ["sendMediaGroup", "sendMessage"])
    assert [call[1] for call in e.export_calls] == [e.home.id, e.source_space.id] * (2 if rejected else 1)
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_retry_cannot_upload_changed_bytes_under_an_existing_result_receipt(work_exports):
    import hashlib

    e = work_exports
    identity = await prepare_delivery(e)
    sdk = e.channel.bot.request
    sdk.enqueue("sendDocument", status=503, body={"ok": False, "error_code": 503, "description": "Unavailable"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    await e.files.write(
        actor=e.instance.principal, space_id=e.home.id, expected_generation=e.home.generation, operation_id=uuid4().hex, path="outputs/result.txt", content=b"changed result", expected_sha256=hashlib.sha256(b"verified result").hexdigest()
    )
    e.delivery_clock.advance(120)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts[0]["state"] == "unavailable", (row.parts, e.dispatch_errors)
        assert row.parts[1]["result_file_warning"]["key"] == "file.out_failed"
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_ordinary_question_recovery_cannot_discard_result_file_ownership(work_exports):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_request_recovery import WorkRequestRecovery

    e = work_exports
    identity = await prepare_delivery(e)
    source = await e.reader.read_request(e.link, e.result["human_input_request_id"])
    settings = await e.plans.settings.get(e.link.owner_user_id)
    with pytest.raises(ProductSourceRetired):
        async with e.plans.outbox._writer(e.link.id) as (writer, _binding):
            row = await writer.get(TelegramOutboxRow, identity)
            await WorkRequestRecovery(e.plans)._replace(writer, e.link, row, source, settings)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0
        assert "result_files" in row.payload["work_request_basis"]
        assert row.parts[0]["kind"] == "document"


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_cancelled_result_sender_retains_lease_until_native_read_drains(work_exports, monkeypatch):
    import asyncio
    import threading

    from deerflow.spaces.filesystem import ConfinedFilesystem

    e = work_exports
    identity = await prepare_delivery(e)
    entered, release = asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), ConfinedFilesystem.read_bytes

    def held(filesystem, *args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10)
        return original(filesystem, *args, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", held)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.claim_owner, row.claim_id = "sending", e.sender.run_id, "owned-cancellation"
    task = asyncio.create_task(e.sender._run(e.link.id, identity, e.link.binding_generation, "owned-cancellation"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert e.channel._services.file_bytes_budget.used == e.channel._services.config.files.max_outbound_bytes
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert e.channel._services.file_bytes_budget.used == 0
        assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_result_plan_replay_cannot_adopt_an_ordinary_text_only_review(work_exports):
    from app.telegram_bots.work_result_delivery import WorkReviewPlans

    e = work_exports
    key = "ordinary:" + uuid4().hex
    identity = await e.plans.plan(e.link, request_id=e.result["human_input_request_id"], dedupe_key=key)
    plans = WorkReviewPlans(outbox=e.plans.outbox, reader=e.reader, callbacks=e.plans.callbacks)
    with pytest.raises(ValueError, match="source-conflict"):
        await plans.plan(e.link, request_id=e.result["human_input_request_id"], dedupe_key=key)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert "result_files" not in row.payload["work_request_basis"]


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
async def test_review_files_and_card_share_one_source_qualified_plan(work_exports):
    from app.telegram_bots.producer import _item
    from app.telegram_bots.work_request_source import frozen
    from app.telegram_bots.work_result_delivery import WorkReviewPlans

    e = work_exports
    plans = WorkReviewPlans(outbox=e.plans.outbox, reader=e.reader, callbacks=e.plans.callbacks)
    assert e.result["status"] == "submitted"
    request_id = e.result["human_input_request_id"]
    key = "result:" + uuid4().hex
    identity = await plans.plan(e.link, request_id=request_id, dedupe_key=key)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert [part["kind"] for part in row.parts] == ["document", "text"]
        assert row.parts[0]["silent"] is True
        assert row.parts[-1]["visible_text"].endswith("Files above:\nresult.txt")
        assert row.parts[0]["callback_group_id"] is None
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.payload["presentation_pending"] for control in controls)
        original = row.parts
        selected = {**row.parts[0], "state": "sending"}
        row.parts = [selected, row.parts[1]]
        basis, source, part = frozen(SimpleNamespace(link=e.link, item=_item(row), part=selected))
        assert basis["result_files"]["version"] == 1
        assert part["files"][0]["source"] == {"kind": "space_file", "space_id": e.home.id, "path": "outputs/result.txt"}
        assert source.seen["outcome_id"] == e.result["outcome_id"]
    assert await plans.plan(e.link, request_id=request_id, dedupe_key=key) == identity
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).parts == original
    assert not e.native_reads

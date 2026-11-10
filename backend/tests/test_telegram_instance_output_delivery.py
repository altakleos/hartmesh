"""SQL/Home/public SDK composition; explicit output seeds are not worker proof."""

import asyncio
import hashlib
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_exports import exports as exports
from test_telegram_instance_output_planning import instance_output
from test_telegram_instance_publication import publication as publication
from test_telegram_outbound_upload import message
from test_telegram_output_planning import png_header

from app.telegram_bots.files import FileBytesBudget
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import OutboxSender
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def delivery(exports, monkeypatch, request):
    from app.gateway import storage_spaces

    e = exports
    count, capacity = getattr(request, "param", (0, 50_000_000))
    e.contents = {"result.txt": b"result"}
    if count:
        from deerflow.agent_instances.contract import AgentPermission

        view = await e.file_service.get(actor=e.actor, instance_id=e.link.agent_ref, permission=AgentPermission.USE)
        home = await e.files.registry.get(actor=view.principal, space_id=e.home.id)
        e.contents = {f"image-{index}.png": png_header(2, 2) for index in range(count)}
        for name, content in e.contents.items():
            await e.files.write(actor=view.principal, space_id=home.id, expected_generation=home.generation, operation_id=uuid4().hex, path="outputs/" + name, content=content, create=True)
    # Publication admission remains real; only resident startup is a fixture.
    # Once the record exists, exports must never consult either dependency.
    execution, residents = storage_spaces.require_execution, e.file_service.residents
    monkeypatch.setattr(storage_spaces, "require_execution", AsyncMock())
    e.file_service.residents = SimpleNamespace(admit=AsyncMock())
    try:
        saved = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    finally:
        monkeypatch.setattr(storage_spaces, "require_execution", execution)
        e.file_service.residents = residents
    guard = (await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)).read_guard
    e.record = await e.inbox.bind_publication_run(saved, run_id="managed-primary-run", guard=guard)
    e.output = instance_output(e.record, paths=tuple("/mnt/user-data/outputs/" + name for name in e.contents))
    services = e.channel._services
    services.authority = e.live.authority
    from app.telegram_bots.callbacks import CallbackRepository

    services.callbacks = CallbackRepository(e.db.sessions, config=services.config)
    services.eligibility = e.live.authority.delivery
    services.file_bytes_budget = FileBytesBudget(capacity)
    e.reservations = []
    reserve = services.file_bytes_budget.reserve_batch

    async def reserved(ceilings, **kwargs):
        assert services.file_bytes_budget.used == 0, "Never reacquire with a partial album"
        e.reservations.append(ceilings)
        return await reserve(ceilings, **kwargs)

    monkeypatch.setattr(services.file_bytes_budget, "reserve_batch", reserved)
    e.legacy = AsyncMock(side_effect=AssertionError("Instance output must never use the legacy artifact door"))
    services.thread_client = SimpleNamespace(describe_artifact=e.legacy, read_artifact_capped=e.legacy)

    async def read_output(record, link):
        assert record == e.record and link.id == e.link.id
        return e.output

    services.model_source = ModelSource(read_output=read_output, outbox=services.outbox, http_owner=services.thread_client, file_capacity=capacity)
    e.output_id = await services.model_source.plan_file_reply(e.record, e.link, e.output, guard=guard)
    assert not e.reservations
    e.sdk = e.channel._requests[0]
    e.uploads = []
    request_call = e.sdk.do_request

    async def request_bytes(url, method, request_data=None, **kwargs):
        if request_data and request_data.multipart_data:
            received = []
            for name, handle, _mime in request_data.multipart_data.values():
                digest = hashlib.sha256()
                handle.seek(0)
                while chunk := handle.read(65_536):
                    digest.update(chunk)
                handle.seek(0)
                received.append((name, digest.hexdigest()))
            e.uploads.append(received)
        return await request_call(url, method, request_data, **kwargs)

    monkeypatch.setattr(e.sdk, "do_request", request_bytes)

    async def deliver(*, link_id, **kwargs):
        assert link_id == e.link.id
        return await e.channel.send_part(**kwargs)

    e.sender = OutboxSender(services.outbox, run_id="export-test", deliver_part=deliver, eligibility=services.eligibility)
    e.failures = []
    failed = e.sender._failure

    async def failure(*args):
        error = args[-1]
        e.failures.append((type(error).__name__, str(error)))
        return await failed(*args)

    monkeypatch.setattr(e.sender, "_failure", failure)
    e.charges = []
    original = e.files.export_file

    async def export(**kwargs):
        e.charges.append(services.file_bytes_budget.used)
        return await original(**kwargs)

    monkeypatch.setattr(e.files, "export_file", export)
    yield e
    await e.sender.stop()


async def claim(e):
    e.channel._services.outbox.clock.__self__.advance(2)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.output_id)
        row.state, row.claim_owner, row.claim_id = "sending", "export-test", "export-claim"


async def send(e):
    await claim(e)
    await e.sender._run(e.link.id, e.output_id, e.link.binding_generation, "export-claim")
    async with e.db.sessions() as reader:
        e.output_state = (await reader.get(TelegramOutboxRow, e.output_id)).state
    return await e.channel._services.outbox.get(e.link, e.output_id)


async def test_claimed_sender_exports_as_human_and_acks_without_rereading(delivery):
    e = delivery
    e.sdk.enqueue("sendDocument", result=message(700))
    item = await send(e)
    assert e.output_state == "sent", (e.failures, item.parts, e.export_calls, e.charges, e.sdk.calls)
    assert item.parts[0]["message_ids"] == [700]
    assert e.charges == [50_000_000]
    assert e.uploads == [[("result.txt", hashlib.sha256(b"result").hexdigest())]]
    assert e.export_calls == [(e.actor, e.home.id, "outputs/result.txt", 50_000_000)]
    assert e.channel._services.file_bytes_budget.used == 0
    e.legacy.assert_not_awaited()
    await e.sender._run(e.link.id, e.output_id, e.link.binding_generation, "export-claim")
    assert e.charges == [50_000_000]


@pytest.mark.parametrize("delivery,groups", [((5, 200_000_000), [4, 1]), ((10, 200_000_000), [4, 4, 2]), ((2, 50_000_000), [1, 1])], indirect=["delivery"])
async def test_unknown_albums_finish_with_one_atomic_reservation_per_group(delivery, groups):
    e = delivery
    e.sdk.enqueue("sendMessage", result=message(700))
    index = 701
    for count in groups:
        e.sdk.enqueue("sendPhoto" if count == 1 else "sendMediaGroup", result=message(index) if count == 1 else [message(index + i) for i in range(count)])
        index += count
    for _ in range(len(groups) + 2):
        item = await send(e)
        if e.output_state == "sent":
            break
    assert e.output_state == "sent", (e.failures, item.parts)
    assert e.reservations == [(50_000_000,) * count for count in groups]
    assert len(e.export_calls) == len(e.contents)
    assert [member for upload in e.uploads for member in upload] == [(name, hashlib.sha256(data).hexdigest()) for name, data in e.contents.items()]
    assert e.channel._services.file_bytes_budget.used == 0
    e.legacy.assert_not_awaited()


@pytest.mark.parametrize("change", ["inspect", "export", "protected", "disabled", "claim"])
async def test_final_sdk_authority_snapshot_refuses_late_changes(delivery, change):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.spaces.contract import Permission

    e = delivery
    limiter, changed = e.channel.bot.rate_limiter, []

    def withdraw(connection, cursor, statement, parameters, context, many):
        if "FROM telegram_bot_links JOIN agent_instances" not in statement or "JOIN storage_space_grants" not in statement or changed:
            return
        changed.append(True)
        if change == "inspect":
            connection.execute(update(AgentInstanceGrantRow).values(permissions=1))
        elif change == "export":
            connection.execute(update(SpaceGrantRow).where(SpaceGrantRow.principal_kind == "human").values(permissions=int(Permission.READ)))
        elif change == "disabled":
            connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))
        else:
            connection.execute(insert(TelegramInstancePolicyRow).values(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))

    async def waited(delay):
        limiter._short_hold_until = 0
        if change == "claim":
            async with e.db.sessions.begin() as writer:
                await writer.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == e.output_id).values(claim_id="replacement"))
            changed.append(True)
        else:
            event.listen(e.db.engine.sync_engine, "before_cursor_execute", withdraw)

    limiter._sleep = waited
    limiter._short_hold_until = limiter._time() + 10
    e.sdk.enqueue("sendDocument", result=message(700))
    try:
        await send(e)
        assert changed == [True]
        assert e.native_reads == ["outputs/result.txt"]
        assert not [call for call in e.sdk.calls if call.endpoint == "sendDocument"]
        assert not e.uploads and e.channel._services.file_bytes_budget.used == 0
    finally:
        if event.contains(e.db.engine.sync_engine, "before_cursor_execute", withdraw):
            event.remove(e.db.engine.sync_engine, "before_cursor_execute", withdraw)


async def test_cancelled_sender_retains_entire_batch_until_native_read_settles(delivery, monkeypatch):
    from deerflow.spaces.filesystem import ConfinedFilesystem

    e = delivery
    entered, release = asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), ConfinedFilesystem.read_bytes

    def read(filesystem, *args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(10)
        return original(filesystem, *args, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", read)
    task = asyncio.create_task(send(e))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and e.channel._services.file_bytes_budget.used == 50_000_000
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert e.channel._services.file_bytes_budget.used == 0
        assert not e.uploads
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_claim_replaced_at_volume_boundary_never_reads_bytes(delivery, monkeypatch):
    e = delivery
    volume, entered = e.files._volume, []

    async def changed(writer, row):
        result = await volume(writer, row)
        entered.append(True)
        await writer.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == e.output_id).values(claim_id="replacement"))
        return result

    monkeypatch.setattr(e.files, "_volume", changed)
    await send(e)
    assert entered and not e.native_reads and not e.uploads
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.parametrize("succeed", [False, True])
async def test_space_conflicts_keep_one_reservation_and_never_publish_a_new_turn(delivery, monkeypatch, succeed):
    from app.telegram_bots import outbound
    from deerflow.spaces.contract import SpaceConflict

    e = delivery
    budget = e.channel._services.file_bytes_budget
    e.channel._services.config = e.channel._services.config.model_copy(deep=True)
    e.channel._services.config.files.retry_max_seconds = 6
    read, retry = e.files.export_file, outbound.retry_export
    elapsed, attempts = [0], []
    initial_inputs = e.bus.inbound_queue.qsize()

    async def conflict(**kwargs):
        attempts.append((elapsed[0], budget.used))
        if not succeed or len(attempts) < 3:
            raise SpaceConflict("File maintenance")
        return await read(**kwargs)

    async def sleep(seconds):
        assert budget.used == 50_000_000 and not e.native_reads
        elapsed[0] += seconds

    async def bounded(read, **kwargs):
        return await retry(read, **kwargs, clock=lambda: elapsed[0], sleep=sleep)

    monkeypatch.setattr(e.files, "export_file", conflict)
    monkeypatch.setattr(outbound, "retry_export", bounded)
    if succeed:
        e.sdk.enqueue("sendDocument", result=message(700))
    else:
        e.sdk.enqueue("sendMessage", result=message(701))
        e.sdk.enqueue("sendMessage", result=message(702))
    for _ in range(4):
        item = await send(e)
        if e.output_state == "sent":
            break
    assert e.output_state == "sent", (e.failures, item.parts)
    assert attempts == [(0, 50_000_000), (3, 50_000_000), (6, 50_000_000)]
    assert e.reservations == [(50_000_000,)] and budget.used == 0
    assert e.native_reads == (["outputs/result.txt"] if succeed else [])
    assert e.bus.inbound_queue.qsize() == initial_inputs


async def test_protected_employee_upload_sets_sdk_content_protection(delivery):
    from datetime import UTC, datetime

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = delivery
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    e.sdk.enqueue("sendDocument", result=message(700))
    item = await send(e)
    assert e.output_state == "sent" and item.parts[0]["protect_content"] is True
    assert [call.parameters["protect_content"] for call in e.sdk.calls if call.endpoint == "sendDocument"] == [True]
    assert not [call for call in e.sdk.calls if call.endpoint == "sendMessageDraft"]


async def test_use_only_recipient_keeps_answer_and_gets_plain_no_access(delivery):
    e = delivery
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=1))
    e.sdk.enqueue("sendMessage", result=message(701))
    e.sdk.enqueue("sendMessage", result=message(702))
    for _ in range(4):
        item = await send(e)
        if e.output_state == "sent":
            break
    assert e.output_state == "sent", (e.failures, item.parts, e.export_calls, e.charges, e.sdk.calls)
    sent = [call for call in e.sdk.calls if call.endpoint == "sendMessage"]
    assert len(sent) == 2 and "synthetic answer" in sent[0].parameters["text"]
    assert "Can see all work" in sent[1].parameters["text"]
    assert not sent[1].parameters.get("reply_markup")
    assert not e.native_reads and not e.export_calls
    assert e.channel._services.file_bytes_budget.used == 0
    e.legacy.assert_not_awaited()


@pytest.mark.parametrize("failure,key", [("oversize", "file.too_large_out"), ("missing", "file.out_failed")])
async def test_file_failure_preserves_caption_and_requires_explicit_retry(delivery, failure, key):
    from deerflow.agent_instances.contract import AgentPermission

    e = delivery
    if failure == "oversize":
        e.channel._services.config = e.channel._services.config.model_copy(deep=True)
        e.channel._services.config.files.max_outbound_bytes = 3
    else:
        view = await e.file_service.get(actor=e.actor, instance_id=e.link.agent_ref, permission=AgentPermission.USE)
        home = await e.files.registry.get(actor=view.principal, space_id=e.home.id)
        await e.files.remove(actor=view.principal, space_id=home.id, expected_generation=home.generation, operation_id=uuid4().hex, path="outputs/result.txt")
    e.sdk.enqueue("sendMessage", result=message(701))
    e.sdk.enqueue("sendMessage", result=message(702))
    for _ in range(4):
        item = await send(e)
        if e.output_state == "sent":
            break
    assert e.output_state == "sent", (e.failures, item.parts)
    warning = [part for part in item.parts if part.get("source_ref", {}).get("key") == key]
    assert len(warning) == 1 and warning[0]["state"] == "sent"
    assert len(e.export_calls) == 1 and not e.uploads
    assert e.channel._services.file_bytes_budget.used == 0
    sent = [call for call in e.sdk.calls if call.endpoint == "sendMessage"]
    assert "synthetic answer" in sent[0].parameters["text"]
    assert bool(warning[0].get("file_retry_source")) is (failure == "missing")

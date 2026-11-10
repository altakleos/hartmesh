"""Incoming instance files save before chat admission and use receipt-only publication."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_attachments import pending_service
from test_telegram_instance_file_delivery import delivery
from test_telegram_instance_publication import publication as publication

from app.telegram_bots.turns import ChatTurns, _Turn
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPendingFileRow

pytestmark = pytest.mark.asyncio


async def input_environment(e, monkeypatch, *, text="", size=4):
    origin, file, request, reads = await delivery(e, monkeypatch, text=text, size=size)
    services = e.channel._services
    services.pending_files = await pending_service(e, services.file_puts)
    services.threads, services.thread_client = e.threads, e.http_client
    services.publication_authority, services.publications = e.host, e.coordinator
    turns = ChatTurns(services)
    services.turns = turns
    started = []

    def start(channel, link, inbound):
        # Keep the actual turn/SQL/publication path; only automatic execution
        # observation is held while the test drives its admitted boundary.
        sources = tuple(turns._origins[(link.id, identity)] for identity in inbound.metadata["telegram_source_message_ids"])
        turn = _Turn(channel, link, inbound, sources)
        turns._turns[(link.id, turn.topic)] = turn
        started.append(turn)

    monkeypatch.setattr(turns, "_start", start)
    return turns, origin, file, request, reads, started


async def test_captionless_input_saves_directly_asks_once_and_starts_nothing(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    await turns._admit_origins(e.channel, e.link, (origin,))
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert act is not None and act.state == "done"
        pending = await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id))
        assert pending.agent_file_path == act.result["path"]
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
        assert len(rows) == 1 and rows[0].parts[0]["source_ref"]["key"] == "file.saved_ask"
        assert rows[0].parts[0]["source_ref"]["values"]["name"] == "Fixture employee"
    assert len(reads) == 1 and not started and not turns._turns and e.bus.inbound_queue.empty()


async def test_captioned_file_is_saved_before_the_turn_can_prepare_a_workspace(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, text="Read the file")
    await turns._admit_origins(e.channel, e.link, (origin,))
    assert len(started) == 1 and started[0].inbound.text == "Read the file"
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert act is not None and act.state == "done"
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
    assert len(reads) == 1 and e.bus.inbound_queue.empty()


async def test_known_oversize_captionless_file_is_refused_without_legacy_preflight_or_download(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, size=20_000_001)

    async def forbidden(*args, **kwargs):
        raise AssertionError("An instance file cannot enter personal-storage admission")

    monkeypatch.setattr(e.channel, "_file_preflight", forbidden)
    await turns._admit_origins(e.channel, e.link, (origin,))
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert act is not None and act.reason == "file-too-large"
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete
    assert not reads and not started


async def publish(e, turns, turn, monkeypatch, tmp_path):
    from types import SimpleNamespace

    from test_telegram_instance_file_publication import manager_endpoint

    from app.telegram_bots.supervisor import TelegramSupervisor

    async def admit(view, *, work):
        # The file writer already finished without any resident capability.
        assert view.id == e.link.agent_ref and work is False

    e.file_service.residents = SimpleNamespace(admit=admit)
    services = e.channel._services

    async def receipts(link, *, current=None):
        return await TelegramSupervisor.instance_file_receipts(services, link, current=current)

    async def preflight(link, record, files, *, execution_context):
        return await TelegramSupervisor.preflight_inbound_files(services, link, record, files, execution_context=execution_context)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Instance input cannot use ordinary storage preflight or upload")

    monkeypatch.setattr(e.channel, "_file_preflight", forbidden)
    monkeypatch.setattr(e.channel, "read_inbound_file", forbidden)
    e.coordinator.instance_file_receipts, e.coordinator.file_preflight = receipts, preflight
    assert await turns._prepare_once(turn)
    sent = e.bus.inbound_queue.get_nowait()
    manager, bodies = manager_endpoint(e, monkeypatch, tmp_path)
    await manager._handle_chat(sent)
    assert len(bodies) == 1
    return sent, bodies[0]


@pytest.mark.parametrize("size", [4, 20_000_001])
async def test_captioned_input_reaches_actual_manager_without_legacy_upload(file_admission, monkeypatch, tmp_path, size):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, text="Read the file", size=size)
    await turns._admit_origins(e.channel, e.link, (origin,))
    sent, body = await publish(e, turns, started[0], monkeypatch, tmp_path)
    assert sent.files == []
    files = body["input"]["messages"][0]["additional_kwargs"].get("files", [])
    if size == 4:
        from dataclasses import replace
        from uuid import uuid4

        context = await turns.file_context(e.channel, file, execution_context=sent.execution_context)
        assert context is not None and e.channel._file_current(sent.execution_context, context)
        forged = replace(sent.execution_context, scope=replace(sent.execution_context.scope, instance_id=uuid4().hex))
        assert not e.channel._file_current(forged, context)
        assert files == [{"filename": "input.txt", "size": 4, "path": "/mnt/user-data/uploads/input.txt", "status": "uploaded"}]
        assert len(reads) == 1
    else:
        assert not files and not reads


async def test_next_text_carries_captionless_receipt_without_redownload_or_extra_notice(file_admission, monkeypatch, tmp_path):
    from test_telegram_instance_file_attachments import next_text

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    await turns._admit_origins(e.channel, e.link, (origin,))
    target = await next_text(e)
    await turns._sequence_input(e.channel, e.link, target, [], False, "Use the file")
    assert len(started) == 1 and started[0].inbound.text == "Use the file"
    sent, body = await publish(e, turns, started[0], monkeypatch, tmp_path)
    assert sent.files == []
    assert body["input"]["messages"][0]["additional_kwargs"]["files"][0]["path"] == "/mnt/user-data/uploads/input.txt"
    assert len(reads) == 1
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, origin.message_id)) is None
        notices = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
        assert len(notices) == 1


async def test_lost_initial_confirmation_gets_an_exact_file_retry_without_reapplying(file_admission, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    original, effects = e.file_service.agent_files.apply, []

    async def lost(**kwargs):
        effects.append(kwargs["operation_id"])
        await original(**kwargs)
        raise RuntimeError("confirmation lost after actual native write")

    monkeypatch.setattr(e.file_service.agent_files, "apply", lost)
    await turns._admit_origins(e.channel, e.link, (origin,))
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert act.state == "uncertain" and act.operation_ids == effects
        button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.target_id == act.id))
        assert button.kind == "file_retry" and button.payload["act_id"] == act.id
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete
    assert len(reads) == len(effects) == 1 and not started


async def test_actual_sdk_input_and_redelivery_use_the_new_file_path(file_admission, monkeypatch):
    from types import SimpleNamespace

    from telegram import Update
    from test_telegram_media_turns import media

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    payload = media(e, 921).to_dict()
    payload["message"]["document"].update(file_id=file["file_id"], file_unique_id=file["file_unique_id"], file_name=file["filename"], file_size=4, mime_type="text/plain")
    incoming = Update.de_json(payload, e.channel.bot)
    # Account admission is qualified by the existing ingress suite. Here the
    # actual SDK input crosses joining, SQL admission and the file owner.
    monkeypatch.setattr(e.channel, "_admission", lambda update: SimpleNamespace(link=e.link))
    monkeypatch.setattr(e.channel, "_classified", lambda *args, **kwargs: None)
    assert await turns.accept(e.channel, incoming)
    assert await turns.accept(e.channel, incoming)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 921, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
        rows = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.message_id == 921)))
        assert len(rows) == 1 and rows[0].state == "done"
    assert len(reads) == 1 and not started


async def test_captionless_album_saves_both_members_and_asks_one_question(file_admission, monkeypatch):
    from test_telegram_instance_file_puts import admitted
    from test_telegram_instance_file_retry_delivery import next_download

    e = file_admission
    turns, _origin, _file, request, reads, started = await input_environment(e, monkeypatch)
    one, file = await admitted(e, message_id=921, album="file-album")
    two, _ = await admitted(e, message_id=922, album="file-album")
    next_download(request, file)
    await turns._admit_origins(e.channel, e.link, (one, two))
    async with e.db.sessions() as reader:
        pending = list(await reader.scalars(select(TelegramPendingFileRow).where(TelegramPendingFileRow.link_id == e.link.id)))
        assert len(pending) == 2
        questions = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
        assert len(questions) == 1 and questions[0].parts[0]["source_ref"] == {"key": "file.saved_ask_many", "values": {"n": 2}}
    assert len(reads) == 2 and not started


async def test_question_writer_rechecks_use_before_closing_the_input(file_admission, monkeypatch):
    from sqlalchemy import update

    from app.telegram_bots.file_puts import _FileReceiptGuard
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch)
    original, changed = _FileReceiptGuard.check, []

    async def revoke_in_question(guard, writer):
        question = await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%")))
        if question and not changed:
            changed.append(True)
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        return await original(guard, writer)

    monkeypatch.setattr(_FileReceiptGuard, "check", revoke_in_question)
    with pytest.raises(AgentDenied):
        await turns._admit_origins(e.channel, e.link, (origin,))
    assert changed == [True]
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert not mark.route_complete
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
    assert len(reads) == 1 and not started


async def test_failed_metadata_recovery_preserves_a_spent_buttons_refusal(file_admission, monkeypatch):
    from datetime import UTC, datetime

    from test_telegram_instance_file_retry_delivery import failed_control

    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = file_admission
    origin, file, request, reads, callback = await failed_control(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, callback.id)
        row.consumed_at = datetime.now(UTC)
    before = len(request.calls)
    with pytest.raises(CallbackRefused):
        await e.channel.execute_callback(callback)
    assert len(request.calls) == before and len(reads) == 1
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.target_id)
        assert act.state == "refused" and act.operation_ids == [act.id]

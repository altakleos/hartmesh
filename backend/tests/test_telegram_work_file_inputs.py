"""A real Telegram file reply saves once and answers without starting a chat."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Chat, Document, Message, Update, User
from test_telegram_instance_file_admission import prepare_file_home
from test_telegram_work_card_inputs import card
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.file_puts import FilePuts, work_input_digest
from app.telegram_bots.files import FileBytesBudget
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramPendingFileRow


async def file_reply(e, monkeypatch, tmp_path, *, caption="", quoted=801, filename="statement.pdf", on_download=None, allow_refusal=False, fail_download=False, before_input=None, before_respond=None, album=None):
    request = await card(e, "information")
    await prepare_file_home(e, tmp_path, monkeypatch)
    install_inputs(e)
    services = e.channel._services
    if before_respond is not None:
        actions = services.work_text_inputs.callbacks.acts
        prepare = actions._work_admission

        async def admission(link, act, **kwargs):
            original = await prepare(link, act, **kwargs)

            async def check(writer):
                await before_respond(e, writer)
                return await original(writer)

            return check

        monkeypatch.setattr(actions, "_work_admission", admission)
    services.turns = SimpleNamespace(accept=AsyncMock(), retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)

    async def ordinary(channel, update, **kwargs):
        channel._classified(update)
        return True

    services.turns.accept.side_effect = ordinary
    services.file_puts = FilePuts(services.callbacks, services.outbox.inbox, config=e.db.config.files)
    services.file_bytes_budget = FileBytesBudget(e.db.config.files.in_memory_max_bytes)
    services.prepare_live_owner = e.live.authority.prepare_live_owner
    services.protect_notice = e.live.authority.protect_notice
    transport = e.channel.bot.request
    transport.enqueue("sendMessageDraft", result=True)
    transport.enqueue("sendMessageDraft", result=True)
    if fail_download:
        transport.enqueue("getFile", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: file not found"})
    else:
        transport.enqueue("getFile", result={"file_id": "work-file", "file_unique_id": "work-unique", "file_size": 4, "file_path": "documents/statement.pdf"})
    reads = []

    async def capped(url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        await before_dispatch()
        assert services.file_bytes_budget.used == 4
        reads.append(True)
        if on_download is not None:
            await on_download(e)
        return b"data"

    monkeypatch.setattr(transport, "read_file_capped", capped, raising=False)
    chat, person = Chat(e.link.chat_id, "private"), User(e.link.telegram_user_id, "Person", False)
    incoming = Update(
        2900,
        message=Message(
            900,
            datetime.now(UTC),
            chat,
            from_user=person,
            caption=caption or None,
            media_group_id=album,
            document=Document("work-file", "work-unique", file_name=filename, mime_type="application/pdf", file_size=4),
            reply_to_message=Message(quoted, datetime.now(UTC), chat, from_user=User(e.link.bot_id, "Bot", True), text="Question") if quoted is not None else None,
        ),
    )
    if before_input is not None:
        await before_input(e, request)
    try:
        assert await e.channel.handle_update(incoming)
    except (ValueError, PermissionError):
        if not allow_refusal:
            raise
    await e.channel._drain_ingress()
    return request, incoming, reads


@pytest.mark.asyncio
@pytest.mark.parametrize("caption", ["", "The requested statement."])
async def test_file_reply_saves_once_and_records_the_actual_saved_name(work_delivery, monkeypatch, tmp_path, caption):
    e = work_delivery
    request, incoming, reads = await file_reply(e, monkeypatch, tmp_path, caption=caption)
    async with e.db.sessions() as reader:
        acts = list(await reader.scalars(select(TelegramActRow)))
        files = [act for act in acts if act.kind == "file_put"]
        assert len(files) == 1 and files[0].state == "done"
        answers = [act for act in acts if act.kind == "respond"]
        assert len(answers) == 1 and answers[0].state == "done"
        assert "uploads/statement.pdf" in answers[0].body["text"]
        assert caption in answers[0].body["text"]
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_input" and mark.sanitized_input["type"] == "media"
        assert await reader.get(TelegramPendingFileRow, (e.link.id, 900)) is None
    assert e.channel._services.turns.accept.await_count == 0 and reads == [True]
    responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(responses) == 1
    await e.channel.handle_update(incoming)
    await e.channel._drain_ingress()
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1
    assert reads == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["home", "journal"])
async def test_committed_file_answer_replay_uses_response_receipt_after_storage_changes(work_delivery, monkeypatch, tmp_path, changed):
    from deerflow.persistence.spaces.model import SpaceRow

    e = work_delivery
    request, incoming, reads = await file_reply(e, monkeypatch, tmp_path)
    async with e.db.sessions.begin() as writer:
        file = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        if changed == "home":
            (await writer.get(SpaceRow, e.home.id)).generation += 1
        else:
            await writer.delete(file)
    assert await e.channel._services.work_text_inputs.files.handle(e.channel, incoming, captured_link=e.link)
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1
    assert reads == [True] and e.channel._services.turns.accept.await_count == 0
    assert sum(call.endpoint == "getFile" for call in e.channel.bot.request.calls) == 1
    if changed == "journal":
        async with e.db.sessions() as reader:
            assert await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put")) is None


@pytest.mark.asyncio
async def test_work_file_journal_retains_its_actual_input_and_does_not_create_chat_context(work_delivery, monkeypatch, tmp_path):
    e = work_delivery
    request, _, _ = await file_reply(e, monkeypatch, tmp_path)
    async with e.db.sessions() as reader:
        file = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert file.continuation["version"] == 4
        assert file.continuation["input"] == {"captionless": True, "topic": None, "message_ids": [900]}
        assert file.continuation["work"] == {
            "version": 1,
            "request_id": request["id"],
            "source_outbox_id": mark.route_basis["work_input"]["source_outbox_id"],
            "operation_id": mark.operation_ids[0],
            "update_id": 2900,
            "input_sha256": work_input_digest(mark),
        }
        assert await reader.get(TelegramPendingFileRow, (e.link.id, 900)) is None


@pytest.mark.asyncio
async def test_retired_work_file_input_cannot_write_after_download(work_delivery, monkeypatch, tmp_path):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    async def retire(e):
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            mark.route_complete = True

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path, on_download=retire, allow_refusal=True)
    async with e.db.sessions() as reader:
        file = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert await reader.get(SpaceFileOperationRow, (e.home.id, file.id)) is None
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
    assert e.channel._services.file_bytes_budget.used == 0


@pytest.mark.asyncio
async def test_file_answer_acknowledgement_names_the_file_and_finishes_only_after_delivery(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path, caption="The requested statement.")
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert not mark.route_complete
        outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == mark.operation_ids[0]))
        assert outcome is not None
        text = "".join(part["visible_text"] for part in outcome.parts)
        assert "the file statement.pdf, now in Fixture employee's files" in text
        assert "The requested statement." in text and "uploads/" not in text
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    e.outcome_identity = outcome.id
    await clear_outcome_cleanup(e)
    for index in range(len(outcome.parts)):
        await dispatch(e, 920 + index)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, outcome.id)).state == "sent", e.dispatch_errors
        assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete


@pytest.mark.asyncio
async def test_actual_file_retry_answers_work_without_starting_chat_or_asking_for_instructions(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_delivery
    request, _, reads = await file_reply(e, monkeypatch, tmp_path, fail_download=True)
    async with e.db.sessions() as reader:
        file = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert file.state == "uncertain"
        notice = await reader.get(TelegramOutboxRow, file.continuation["delivery"]["outbox_id"])
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    e.outcome_identity = notice.id
    await clear_outcome_cleanup(e)
    await dispatch(e, 910)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice.id)
        assert notice.state == "sent", e.dispatch_errors
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "file_retry"))
    transport = e.channel.bot.request
    transport.enqueue("sendMessageDraft", result=True)
    transport.enqueue("sendMessageDraft", result=True)
    transport.enqueue("getFile", result={"file_id": "work-file", "file_unique_id": "work-unique", "file_size": 4, "file_path": "documents/statement.pdf"})
    await press_work(e, retry, message_id=910)
    replies = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(replies) == 1 and "uploads/statement.pdf" in replies[0]["text"]
    assert reads == [True] and e.channel._services.turns.accept.await_count == 0
    async with e.db.sessions() as reader:
        file = await reader.get(TelegramActRow, file.id)
        assert file.continuation["labels"] == ["initial", "retry:2"]
        assert not [part for row in await reader.scalars(select(TelegramOutboxRow)) for part in row.parts if part.get("source_ref", {}).get("key") == "file.saved_ask"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["path", "operation", "original"])
async def test_file_answer_delivery_rechecks_frozen_file_evidence_after_provider_read(work_delivery, monkeypatch, tmp_path, damage):
    from copy import deepcopy

    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    import app.telegram_bots.work_outcome_delivery as module
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == mark.operation_ids[0]))
    e.outcome_identity = outcome.id
    await clear_outcome_cleanup(e)
    original = module.current_attention
    changed = []

    async def withdraw(*args, **kwargs):
        changed.append(True)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            if damage == "original":
                raw = deepcopy(mark.sanitized_input)
                raw["files"][0]["file_unique_id"] = "different-file"
                mark.sanitized_input = raw
            else:
                basis = deepcopy(mark.route_basis)
                if damage == "path":
                    basis["work_file"]["result"]["path"] = "uploads/changed.pdf"
                else:
                    basis["work_file"]["operation_id"] = True
                mark.route_basis = basis
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", withdraw)
    before = len(e.channel.bot.request.calls)
    await dispatch(e, 920)
    assert changed and len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, outcome.id)).state != "sent"


@pytest.mark.asyncio
async def test_file_reply_to_closed_question_is_held_without_download_or_chat(work_delivery, monkeypatch, tmp_path):
    from uuid import uuid4

    from deerflow.agent_instances.work_contract import WorkCommand
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    async def close(e, request):
        work = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=request["work_id"])
        await e.human.work.command(
            actor=e.actor,
            instance_id=e.link.agent_ref,
            work_id=work["id"],
            request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"]),
            can_stop=False,
        )

    e = work_delivery
    _, _, reads = await file_reply(e, monkeypatch, tmp_path, before_input=close)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.operation_ids
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        assert notice is not None
        assert not list(await reader.scalars(select(TelegramActRow)))
    assert not reads and e.channel._services.turns.accept.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_discard"])
async def test_closed_file_choices_keep_original_media_and_explicit_intent(work_delivery, monkeypatch, tmp_path, choice):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_delivery
    await test_file_reply_to_closed_question_is_held_without_download_or_chat(e, monkeypatch, tmp_path)
    e.channel._services.turns.accept_staged = AsyncMock()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        original = mark.sanitized_input
    await dispatch(e, 920)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice.id)
        assert notice.state == "sent", e.dispatch_errors
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == choice))
    await press_work(e, selected, message_id=920)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete
        assert mark.sanitized_input == original
        acts = list(await reader.scalars(select(TelegramActRow)))
        assert len(acts) == int(choice == "prompt_send_new")
        assert all(act.kind == "file_put" and act.state == "done" for act in acts)
    assert e.channel._services.turns.accept_staged.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["operation", "target", "original"])
async def test_file_answer_reader_rejects_malformed_admitted_file_basis(work_delivery, monkeypatch, tmp_path, damage):
    from copy import deepcopy

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        operation = mark.operation_ids[0]
        if damage == "original":
            raw = deepcopy(mark.sanitized_input)
            raw["files"][0]["file_unique_id"] = "different-file"
            mark.sanitized_input = raw
        else:
            basis = deepcopy(mark.route_basis)
            if damage == "operation":
                basis["work_file"]["operation_id"] = True
            else:
                basis["work_file"]["target"]["home_generation"] = True
            mark.route_basis = basis
    with pytest.raises((CallbackRefused, ProductSourceRetired, ValueError)):
        await e.channel._services.work_text_inputs.callbacks.outcomes.reader.read(e.link, operation)


@pytest.mark.asyncio
async def test_file_answer_cannot_admit_response_after_home_changes(work_delivery, monkeypatch, tmp_path):
    from deerflow.persistence.spaces.model import SpaceRow

    entered = []

    async def change(e, writer):
        entered.append(True)
        home = await writer.get(SpaceRow, e.home.id)
        home.generation += 1
        await writer.flush()

    e = work_delivery
    request, _, reads = await file_reply(e, monkeypatch, tmp_path, before_respond=change)
    assert entered and reads == [True]
    assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    async with e.db.sessions() as reader:
        response = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "respond"))
        assert response.message_id == 900 and response.state != "done"


@pytest.mark.asyncio
async def test_unsubmitted_work_file_keeps_native_receipt_through_retention(work_delivery, monkeypatch, tmp_path):
    from datetime import timedelta

    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = work_delivery

    async def hold(e, request):
        monkeypatch.setattr(e.channel._services.work_text_inputs.files.cards, "stage", AsyncMock(side_effect=RuntimeError("hold-response")))

    with pytest.raises(RuntimeError, match="hold-response"):
        await file_reply(e, monkeypatch, tmp_path, before_input=hold)
    async with e.db.sessions.begin() as writer:
        file = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert file.state == "done"
        file.created_at = e.clock() - timedelta(days=ACT_RETENTION_DAYS + 1)
        identity = file.id
    await _sweep_acts(e.db.sessions, e.clock())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is not None
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete = True
    await _sweep_acts(e.db.sessions, e.clock())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, identity) is None


@pytest.mark.asyncio
async def test_restart_after_saved_file_and_closed_question_keeps_work_ownership(work_delivery, monkeypatch, tmp_path):
    from uuid import uuid4

    from app.telegram_bots.inbox import InboxRow
    from deerflow.agent_instances.work_contract import WorkCommand

    e = work_delivery
    saved_stage = []

    async def hold(e, request):
        cards = e.channel._services.work_text_inputs.files.cards
        saved_stage.append(cards.stage)
        monkeypatch.setattr(cards, "stage", AsyncMock(side_effect=RuntimeError("hold-response")))

    with pytest.raises(RuntimeError, match="hold-response"):
        await file_reply(e, monkeypatch, tmp_path, before_input=hold)
    controller = e.channel._services.work_text_inputs.files
    monkeypatch.setattr(controller.cards, "stage", saved_stage[0])
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        original = InboxRow.from_row(mark)
        work_id = mark.route_basis["work_input"]["work_id"]
    work = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=work_id)
    await e.human.work.command(
        actor=e.actor,
        instance_id=e.link.agent_ref,
        work_id=work_id,
        request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"]),
        can_stop=False,
    )
    assert await controller.resume(e.channel, original)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.operation_ids
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
    assert sum(call.endpoint == "getFile" for call in e.channel.bot.request.calls) == 1
    assert e.channel._services.turns.accept.await_count == 0


@pytest.mark.asyncio
async def test_completed_file_answer_retry_handoff_is_receipt_only(work_delivery, monkeypatch, tmp_path):
    from app.telegram_bots.acts import _snapshot

    e = work_delivery
    await test_file_answer_acknowledgement_names_the_file_and_finishes_only_after_delivery(e, monkeypatch, tmp_path)
    async with e.db.sessions() as reader:
        file = _snapshot(await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put")))
    before = len(e.channel.bot.request.calls)
    assert await e.channel._services.work_text_inputs.files.after_retry(e.channel, e.link, file)
    assert len(e.channel.bot.request.calls) == before


@pytest.mark.asyncio
async def test_file_acknowledgement_requires_its_exact_action_origin(work_delivery, monkeypatch, tmp_path):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path)
    async with e.db.sessions.begin() as writer:
        act = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "respond"))
        identity = act.id
        act.message_id += 1
    with pytest.raises(ProductSourceRetired):
        await e.channel._services.work_text_inputs.callbacks.outcomes.reader.read(e.link, identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("at", ["download", "response"])
async def test_file_answer_never_adopts_a_replacement_caption(work_delivery, monkeypatch, tmp_path, at):
    from copy import deepcopy

    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    changed = []

    async def replace_caption(e):
        changed.append(True)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            raw, basis = deepcopy(mark.sanitized_input), deepcopy(mark.route_basis)
            raw["text"] = basis["work_input"]["text"] = "Replacement caption"
            mark.sanitized_input, mark.route_basis = raw, basis

    async def stage_boundary(e, request):
        cards = e.channel._services.work_text_inputs.files.cards
        original = cards.stage

        async def stage(*args, **kwargs):
            await replace_caption(e)
            return await original(*args, **kwargs)

        monkeypatch.setattr(cards, "stage", stage)

    e = work_delivery
    request, _, reads = await file_reply(e, monkeypatch, tmp_path, caption="Original caption", on_download=replace_caption if at == "download" else None, before_input=stage_boundary if at == "response" else None, allow_refusal=True)
    assert changed and reads == [True]
    assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    async with e.db.sessions() as reader:
        file = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        saved = await reader.get(SpaceFileOperationRow, (e.home.id, file.id))
        assert (saved is None) == (at == "download")


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["resume", "retry"])
async def test_file_recovery_drains_its_initial_writer_on_repeated_cancellation(work_delivery, monkeypatch, tmp_path, entry):
    import asyncio
    from contextlib import asynccontextmanager

    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.inbox import InboxRow

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path)
    controller = e.channel._services.work_text_inputs.files
    async with e.db.sessions() as reader:
        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
        receipt = _snapshot(await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put")))
    entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = controller.services.callbacks.writer

    @asynccontextmanager
    async def held(*args, **kwargs):
        async with original(*args, **kwargs) as writer:
            entered.set()
            await release.wait()
            yield writer
        exited.set()

    monkeypatch.setattr(controller.services.callbacks, "writer", held)
    task = asyncio.create_task(controller.resume(e.channel, origin) if entry == "resume" else controller.after_retry(e.channel, e.link, receipt))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        for _ in range(4):
            await asyncio.sleep(0)
        task.cancel()
        for _ in range(4):
            await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        results = await asyncio.gather(task, return_exceptions=True)
    assert exited.is_set() and isinstance(results[0], asyncio.CancelledError)


@pytest.mark.asyncio
async def test_file_retention_does_not_cast_historical_versions_to_postgres_integers(work_delivery, monkeypatch, tmp_path):
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.telegram_bots.retention import _sweep_acts

    e = work_delivery
    await file_reply(e, monkeypatch, tmp_path)
    captured = []
    original = AsyncSession.execute

    async def execute(session, statement, *args, **kwargs):
        sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        if "work_input" in sql and "file_put" in sql:
            captured.append(sql)
        return await original(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", execute)
    await _sweep_acts(e.db.sessions, e.clock())
    assert captured and all("AS INTEGER" not in sql for sql in captured)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["saved", "unsaved", "uncertain"])
@pytest.mark.parametrize("caption", ["Use this file in a new message", ""])
async def test_closed_saved_file_send_new_reaches_actual_chat_publication(work_delivery, monkeypatch, tmp_path, state, caption):
    from uuid import uuid4

    from test_telegram_instance_file_attachments import pending_service
    from test_telegram_instance_file_input import publish
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.turns import ChatTurns, _Turn
    from deerflow.agent_instances.work_contract import WorkCommand
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e, requests = work_delivery, []

    async def remember(e, request):
        requests.append(request)
        if state == "unsaved":
            await close(e)

    async def close(e):
        work = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=requests[0]["work_id"])
        await e.human.work.command(
            actor=e.actor,
            instance_id=e.link.agent_ref,
            work_id=work["id"],
            request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"]),
            can_stop=False,
        )

    request, _, reads = await file_reply(e, monkeypatch, tmp_path, caption=caption, before_input=remember, on_download=close if state == "saved" else None, fail_download=state == "uncertain")
    if state == "uncertain":
        from datetime import timedelta

        from app.telegram_bots.inbox import InboxRow

        await close(e)
        async with e.db.sessions.begin() as writer:
            origin = InboxRow.from_row(await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
            native = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
            failure_id = native.continuation["delivery"]["outbox_id"]
            (await writer.get(TelegramOutboxRow, failure_id)).not_before = e.clock() + timedelta(hours=1)
        assert await e.channel._services.work_text_inputs.files.resume(e.channel, origin)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        assert mark.route_kind == "work_card_hold"
    e.outcome_identity = notice.id
    await clear_outcome_cleanup(e)
    await dispatch(e, 920)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice.id)
        assert notice.state == "sent", e.dispatch_errors
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_send_new"))
    services = e.channel._services
    services.pending_files = await pending_service(e, services.file_puts)
    services.threads, services.thread_client = e.threads, e.http_client
    from app.channels.message_bus import MessageBus
    from app.telegram_bots.publication import PublicationCoordinator
    from app.telegram_bots.publication_authority import PublicationAttemptAuthority

    e.bus = MessageBus(inbound_queue_maxsize=4)
    e.host = PublicationAttemptAuthority(links=e.db.links, authority=e.live.authority, client=e.http_client, channel=lambda identity: e.current[0], create_allowed=lambda: e.active[0], read_allowed=lambda: e.active[0])
    e.coordinator = PublicationCoordinator(inbox=services.outbox.inbox, outbox=services.outbox, bus=e.bus, http_owner=e.http_client, capture_attempt=e.host.capture)
    services.publication_authority, services.publications = e.host, e.coordinator
    turns, started = ChatTurns(services), []
    services.turns = turns

    def start(channel, link, inbound):
        topic = inbound.metadata.get("message_thread_id")
        callback = turns.queue.current_callback(link, topic)
        origins = turns._callback_origins.pop((link.id, callback))[1]
        turn = _Turn(channel, link, inbound, origins)
        turn.callback_id = callback
        turns._turns[(link.id, topic)] = turn
        started.append(turn)

    monkeypatch.setattr(turns, "_start", start)
    prepare = turns._prepare_once

    async def prepare_chat(turn):
        result = await prepare(turn)
        e.thread_id = turn.record.scope.thread_id
        return result

    monkeypatch.setattr(turns, "_prepare_once", prepare_chat)
    try:
        await press_work(e, selected, message_id=920)
        if not caption:
            assert not started and e.bus.inbound_queue.empty()
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
                assert mark.route_complete
                pending = await reader.get(TelegramPendingFileRow, (e.link.id, 900))
                questions = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
                if state == "uncertain":
                    assert pending is None and not questions and not reads
                else:
                    assert pending.agent_file_path == "uploads/statement.pdf" and len(questions) == 1 and reads == [True]
            assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
            return
        assert len(started) == 1
        sent, body = await publish(e, turns, started[0], monkeypatch, tmp_path)
        assert sent.files == []
        files = body["input"]["messages"][0]["additional_kwargs"].get("files", [])
        if state == "uncertain":
            assert files == [] and reads == []
            async with e.db.sessions.begin() as writer:
                native = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
                assert native.continuation["version"] == 3 and native.state == "uncertain"
                (await writer.get(TelegramOutboxRow, failure_id)).not_before = e.clock()
            e.outcome_identity = failure_id
            await clear_outcome_cleanup(e)
            await dispatch(e, 950)
            async with e.db.sessions() as reader:
                failure = await reader.get(TelegramOutboxRow, failure_id)
                assert failure.state == "sent", e.dispatch_errors
                retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == failure.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "file_retry"))
            transport = e.channel.bot.request
            transport.enqueue("sendMessageDraft", result=True)
            transport.enqueue("sendMessageDraft", result=True)
            transport.enqueue("getFile", result={"file_id": "work-file", "file_unique_id": "work-unique", "file_size": 4, "file_path": "documents/statement.pdf"})
            await press_work(e, retry, message_id=950, update_id=2902)
            async with e.db.sessions() as reader:
                assert (await reader.get(TelegramActRow, native.id)).state == "done"
            assert len(started) == 1
        else:
            assert len(files) == 1 and files[0]["path"] == "/mnt/user-data/uploads/statement.pdf"
        assert reads == [True]
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    finally:
        for turn in started:
            turns._forget(turn)
        await turns.stop()
        await e.coordinator.stop()
        e.bus.discard_pending_inbound()


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", [False, True])
async def test_work_file_retry_restart_recovers_metadata_without_another_download(work_delivery, monkeypatch, tmp_path, saved):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.file_recovery import recover_retries
    from app.telegram_bots.inbox import InboxRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_delivery
    request, _, reads = await file_reply(e, monkeypatch, tmp_path, fail_download=True)
    services = e.channel._services
    async with e.db.sessions() as reader:
        file = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        notice = await reader.get(TelegramOutboxRow, file.continuation["delivery"]["outbox_id"])
        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
    e.outcome_identity = notice.id
    await clear_outcome_cleanup(e)
    await dispatch(e, 910)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice.id)
        assert notice.state == "sent", e.dispatch_errors
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "file_retry"))
    transport = e.channel.bot.request
    transport.enqueue("sendMessageDraft", result=True)
    transport.enqueue("sendMessageDraft", result=True)
    if saved:
        transport.enqueue("getFile", result={"file_id": "work-file", "file_unique_id": "work-unique", "file_size": 4, "file_path": "documents/statement.pdf"})
    else:
        transport.enqueue("getFile", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: file not found"})
    receipt = await e.channel._own_instance_file(e.link, origin, file.continuation["file"], callback=retry)
    assert receipt.state == ("done" if saved else "uncertain") and receipt.continuation["delivery"] is None
    before = sum(call.endpoint == "getFile" for call in transport.calls)
    services.turns._turns = {}
    await recover_retries(services, lambda: e.active[0], link_id=e.link.id)
    if saved:
        assert await services.work_text_inputs.files.resume(e.channel, origin)
        responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
        assert len(responses) == 1
    else:
        async with e.db.sessions() as reader:
            recovered = await reader.get(TelegramActRow, receipt.id)
            assert recovered.continuation["delivery"] is not None
            assert recovered.continuation["delivery"]["operation_id"] == receipt.operation_ids[-1]
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    assert sum(call.endpoint == "getFile" for call in transport.calls) == before and len(reads) == int(saved)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["before-upload", "after-receipt"])
async def test_file_new_message_recovery_owns_first_gesture_without_reupload(work_delivery, monkeypatch, tmp_path, boundary):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.inbox import InboxRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_delivery
    await test_file_reply_to_closed_question_is_held_without_download_or_chat(e, monkeypatch, tmp_path)
    services = e.channel._services
    services.turns.accept_staged = AsyncMock()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
    await dispatch(e, 920)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice.id)
        assert notice.state == "sent", e.dispatch_errors
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_send_new"))
    upload = e.channel._own_instance_file

    async def interrupted(*args, **kwargs):
        if boundary == "after-receipt":
            await upload(*args, **kwargs)
        raise RuntimeError("interrupted-handoff")

    monkeypatch.setattr(e.channel, "_own_instance_file", interrupted)
    await press_work(e, selected, message_id=920, update_id=2901)
    monkeypatch.setattr(e.channel, "_own_instance_file", upload)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        origin = InboxRow.from_row(mark)
        assert mark.route_kind == "work_file_new" and not mark.route_complete
        assert mark.route_basis["work_file_handoff"] == {"callback_id": selected.id, "update_id": 2901}
    before = sum(call.endpoint == "getFile" for call in e.channel.bot.request.calls)
    assert await services.work_text_inputs.files.resume(e.channel, origin)
    assert sum(call.endpoint == "getFile" for call in e.channel.bot.request.calls) == before
    assert services.turns.accept_staged.await_count == 0
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete and mark.route_kind == "work_file_new"
        file = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert file.state == ("done" if boundary == "after-receipt" else "uncertain")
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))


@pytest.mark.asyncio
@pytest.mark.parametrize("retired", ["input", "choice"])
async def test_file_new_message_native_write_rechecks_reserved_choice(work_delivery, monkeypatch, tmp_path, retired):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_delivery
    await test_file_reply_to_closed_question_is_held_without_download_or_chat(e, monkeypatch, tmp_path)
    services = e.channel._services
    services.turns.accept_staged = AsyncMock()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
    await dispatch(e, 920)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice.id)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_send_new"))
    entered = []

    async def capped(url, *, before_dispatch, **kwargs):
        await before_dispatch()
        entered.append(True)
        async with e.db.sessions.begin() as writer:
            if retired == "input":
                (await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete = True
            else:
                (await writer.get(TelegramCallbackRow, selected.id)).consumed_at = e.clock()
        return b"data"

    monkeypatch.setattr(e.channel.bot.request, "read_file_capped", capped)
    await press_work(e, selected, message_id=920)
    assert entered and services.turns.accept_staged.await_count == 0
    assert services.file_bytes_budget.used == 0
    async with e.db.sessions() as reader:
        native = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert await reader.get(SpaceFileOperationRow, (e.home.id, native.id)) is None
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))

"""A collected album saves every actual member before one Work response."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Document, Message, Update, User
from test_telegram_instance_file_attachments import pending_service
from test_telegram_work_file_inputs import file_reply
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramPendingFileRow


@asynccontextmanager
async def collected_album(
    e,
    monkeypatch,
    tmp_path,
    *,
    quoted_first=False,
    caption="First caption",
    second_caption="Second caption",
    on_download=None,
    before_respond=None,
    filenames=("statement.pdf", "part-two.pdf"),
    extra_members=(),
    fail_first=False,
    live_submit=False,
):
    owned = []

    async def prepare(e, request):
        services = e.channel._services
        services.pending_files = await pending_service(e, services.file_puts)
        turns = ChatTurns(services)
        services.turns = turns
        owned.append(turns)
        monkeypatch.setattr(turns, "_arm_join", lambda key, joined: None)
        e.channel.bot.request.enqueue("sendChatAction", result=True)
        e.channel.bot.request.enqueue("sendChatAction", result=True)
        e.channel.bot.request.enqueue("sendMessageDraft", result=True)
        e.channel.bot.request.enqueue("sendMessageDraft", result=True)
        e.channel.bot.request.enqueue("getFile", result={"file_id": "second-file", "file_unique_id": "second-unique", "file_size": 4, "file_path": "documents/part-two.pdf"})

    try:
        request, first, reads = await file_reply(
            e, monkeypatch, tmp_path, quoted=801 if quoted_first else None, caption=caption, filename=filenames[0], album="answer-album", before_input=prepare, on_download=on_download, before_respond=before_respond, fail_download=fail_first
        )
        second = Update(
            2901,
            message=Message(
                901,
                datetime.now(UTC),
                first.message.chat,
                from_user=first.message.from_user,
                media_group_id="answer-album",
                caption=second_caption or None,
                document=Document("second-file", "second-unique", file_name=filenames[1], mime_type="application/pdf", file_size=4),
                reply_to_message=None if quoted_first else Message(801, datetime.now(UTC), first.message.chat, from_user=User(e.link.bot_id, "Bot", True), text="Question"),
            ),
        )
        assert await e.channel.handle_update(second)
        await e.channel._drain_ingress()
        for index, (filename, text) in enumerate(extra_members):
            identity = 902 + index
            file_id = "extra-file-" + str(identity)
            e.channel.bot.request.enqueue("getFile", result={"file_id": file_id, "file_unique_id": file_id + "-unique", "file_size": 4, "file_path": "documents/extra.pdf"})
            e.channel.bot.request.enqueue("sendChatAction", result=True)
            e.channel.bot.request.enqueue("sendMessageDraft", result=True)
            extra = Update(
                2902 + index,
                message=Message(
                    identity, datetime.now(UTC), first.message.chat, from_user=first.message.from_user, media_group_id="answer-album", caption=text, document=Document(file_id, file_id + "-unique", file_name=filename, file_size=4)
                ),
            )
            assert await e.channel.handle_update(extra)
            await e.channel._drain_ingress()
        album_owner = e.channel._services.work_text_inputs.files.albums
        submit = album_owner.submit

        async def hold_submission(*args, **kwargs):
            return True

        if not live_submit:
            monkeypatch.setattr(album_owner, "submit", hold_submission)
        for key, joined in list(owned[0]._joins.items()):
            await owned[0]._flush_join(key, joined)
        monkeypatch.setattr(album_owner, "submit", submit)
        async with e.db.sessions() as reader:
            root = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900 if quoted_first else 901, e.link.chat_id)))
        assert root.route_kind == ("work_input" if live_submit else "work_file_album")
        assert reads == ([True, True] if live_submit else [])
        yield request, root, reads
    finally:
        for turns in owned:
            await turns.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted_first", [False, True])
async def test_collected_album_saves_all_members_before_one_work_answer(work_delivery, monkeypatch, tmp_path, quoted_first):
    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, quoted_first=quoted_first) as (request, root, reads):
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            acts = list(await reader.scalars(select(TelegramActRow)))
            files = [act for act in acts if act.kind == "file_put"]
            answers = [act for act in acts if act.kind == "respond"]
            assert len(files) == 2 and all(act.state == "done" for act in files)
            assert len(answers) == 1 and answers[0].state == "done"
            assert answers[0].message_id == root.message_id
            assert all(text in answers[0].body["text"] for text in ("First caption", "Second caption", "uploads/statement.pdf", "uploads/part-two.pdf"))
            assert not list(await reader.scalars(select(TelegramPendingFileRow)))
            marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            assert sum("work_action_id" in mark.route_basis for mark in marks) == 1
        assert reads == [True, True]
        assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1
        assert await e.channel._services.work_text_inputs.files.resume(e.channel, root)
        assert reads == [True, True]
        assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted_first", [False, True])
async def test_album_capture_preserves_actual_inputs_and_allocates_only_one_response(work_delivery, monkeypatch, tmp_path, quoted_first):
    from app.telegram_bots.work_album_inputs import AlbumInputProof

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, quoted_first=quoted_first) as (_request, root, reads):
        files = e.channel._services.work_text_inputs.files
        guard = e.channel._services.commands._guard(e.channel, e.link)
        captured = await files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=guard)
        assert captured.sanitized_input == root.sanitized_input
        assert len(captured.route_basis["work_input"]["files"]) == 1
        async with e.db.sessions() as reader:
            proof = AlbumInputProof(reader.get_bind().dialect.name == "sqlite", captured)
            packets = proof.check(await reader.scalar(select(proof.expression())))
            assert len(packets) == 2
            assert sum(len(packet["operation_ids"]) for packet in packets) == 1
            assert [packet["sanitized_input"].get("reply_to_message_id") for packet in packets] == ([801, None] if quoted_first else [None, 801])
            assert not list(await reader.scalars(select(TelegramActRow)))
        assert reads == []


@pytest.mark.asyncio
async def test_each_album_member_uses_actual_native_file_receipt_without_chat_context(work_delivery, monkeypatch, tmp_path):
    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, reads):
        services = e.channel._services
        captured = await services.work_text_inputs.files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=services.commands._guard(e.channel, e.link))
        for message_id in captured.route_basis["work_album"]["member_ids"]:
            async with e.db.sessions() as reader:
                member = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, message_id, e.link.chat_id)))
            receipt = await e.channel._own_instance_file(e.link, member, member.sanitized_input["files"][0])
            assert receipt.state == "done"
            assert receipt.continuation["version"] == 4 and receipt.continuation["work"]["version"] == 2
            assert receipt.message_id == message_id
            assert receipt.continuation["work"]["album"]["root_message_id"] == root.message_id
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramPendingFileRow)))
        assert reads == [True, True]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["late", "publication", "action"])
async def test_late_album_member_fences_native_write_after_download(work_delivery, monkeypatch, tmp_path, change):
    from telegram import Chat

    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    e, arrived = work_delivery, []

    async def arrive(e):
        if change != "late":
            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
                if change == "publication":
                    mark.outbox_id = e.identity
                else:
                    mark.route_basis = {**mark.route_basis, "work_action_id": mark.route_basis["work_album"]["operation_id"]}
            arrived.append(True)
            return
        update = Update(
            2902,
            message=Message(
                902,
                datetime.now(UTC),
                Chat(e.link.chat_id, "private"),
                from_user=User(e.link.telegram_user_id, "Person", False),
                media_group_id="answer-album",
                document=Document("third-file", "third-unique", file_name="late.pdf", file_size=4),
            ),
        )
        e.channel.bot.request.enqueue("sendChatAction", result=True)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        arrived.append(True)

    async with collected_album(e, monkeypatch, tmp_path, on_download=arrive) as (_request, root, reads):
        services = e.channel._services
        await services.work_text_inputs.files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=services.commands._guard(e.channel, e.link))
        async with e.db.sessions() as reader:
            member = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
        receipt = await e.channel._own_instance_file(e.link, member, member.sanitized_input["files"][0])
        assert arrived == [True] and reads == [True] and receipt.state == "uncertain"
        async with e.db.sessions() as reader:
            assert await reader.get(SpaceFileOperationRow, (e.home.id, receipt.operation_ids[-1])) is None
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))


@pytest.mark.asyncio
async def test_album_acknowledgement_names_every_file_and_settles_every_member(work_delivery, monkeypatch, tmp_path, multipart=False):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    kwargs = {"caption": ("First caption " + "a" * 970)[:970], "second_caption": ("Second caption " + "b" * 970)[:970], "extra_members": (("part-three.pdf", "c" * 970), ("part-four.pdf", "d" * 970))} if multipart else {}
    identities = [900, 901, 902, 903] if multipart else [900, 901]
    async with collected_album(e, monkeypatch, tmp_path, **kwargs) as (_request, root, _reads):
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, root.message_id, e.link.chat_id))
            outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == mark.operation_ids[0]))
            assert outcome is not None
            text = "".join(part["visible_text"] for part in outcome.parts)
            assert all(value in text for value in ("statement.pdf", "part-two.pdf", "First caption", "Second caption"))
            assert "uploads/" not in text
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_(identities))))
            assert all(not member.route_complete and outcome.id in member.delivery_ids for member in members)
        if multipart:
            assert len(outcome.parts) >= 2
        e.outcome_identity = outcome.id
        await clear_outcome_cleanup(e)
        for index in range(len(outcome.parts)):
            await dispatch(e, 920 + index)
            if index < len(outcome.parts) - 1:
                async with e.db.sessions() as reader:
                    members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_(identities))))
                    assert all(not member.route_complete for member in members)
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, outcome.id)).state == "sent", e.dispatch_errors
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_(identities))))
            assert len(members) == len(identities) and all(member.route_complete for member in members)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["operation", "generation", "principal"])
async def test_album_frozen_native_reference_requires_closed_typed_identity(work_delivery, monkeypatch, tmp_path, damage):
    from copy import deepcopy

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, _reads):
        services = e.channel._services
        assert await services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, root.message_id, e.link.chat_id))
            act_id = mark.operation_ids[0]
            data = deepcopy(mark.route_basis)
            file = data["work_file"]["files"][0]
            if damage == "operation":
                file["operation_id"] = "not-an-operation"
            elif damage == "generation":
                file["target"]["home_generation"] = "1"
            else:
                file["target"]["principal_id"] = "foreign-principal"
            mark.route_basis = data
        with pytest.raises((CallbackRefused, ProductSourceRetired, ValueError)):
            await services.work_text_inputs.callbacks.outcomes.reader.read(e.link, act_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["owner_user_id", "link_id", "binding_generation"])
async def test_album_response_rejects_native_receipt_from_wrong_scope(work_delivery, monkeypatch, tmp_path, damage):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, _reads):
        services = e.channel._services
        guard = services.commands._guard(e.channel, e.link)
        captured = await services.work_text_inputs.files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=guard)
        for identity in captured.route_basis["work_album"]["member_ids"]:
            async with e.db.sessions() as reader:
                member = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, identity, e.link.chat_id)))
            await e.channel._own_instance_file(e.link, member, member.sanitized_input["files"][0])
        async with e.db.sessions.begin() as writer:
            file = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put", TelegramActRow.message_id == 900))
            setattr(file, damage, 987654 if damage == "binding_generation" else None if damage == "link_id" else "f" * 32)
        with pytest.raises(CallbackRefused):
            await services.work_text_inputs.files.cards.stage(e.link, captured, guard=guard)
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["owner_user_id", "cancel_requested_at", "attempts"])
async def test_final_album_response_rechecks_complete_native_journal(work_delivery, monkeypatch, tmp_path, damage):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e, changed = work_delivery, []

    async def mutate(e, writer):
        file = await writer.scalar(select(TelegramActRow).where(TelegramActRow.kind == "file_put", TelegramActRow.message_id == 900))
        assert file is not None and file.state == "done"
        setattr(file, damage, "f" * 32 if damage == "owner_user_id" else e.clock() if damage == "cancel_requested_at" else file.attempts + 1)
        await writer.flush()
        changed.append(True)

    async with collected_album(e, monkeypatch, tmp_path, before_respond=mutate) as (request, root, reads):
        try:
            await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        except (CallbackRefused, ProductSourceRetired, ValueError):
            pass
        assert changed == [True] and reads == [True, True]
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["home", "journals"])
async def test_album_response_receipt_replays_without_native_storage(work_delivery, monkeypatch, tmp_path, changed):
    from sqlalchemy import delete

    from deerflow.persistence.spaces.model import SpaceRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        files = e.channel._services.work_text_inputs.files
        assert await files.albums.submit(e.channel, root)
        async with e.db.sessions.begin() as writer:
            if changed == "home":
                (await writer.get(SpaceRow, e.home.id)).generation += 1
            else:
                await writer.execute(delete(TelegramActRow).where(TelegramActRow.kind == "file_put"))
        assert await files.resume(e.channel, root)
        assert reads == [True, True]
        assert sum(call.endpoint == "getFile" for call in e.channel.bot.request.calls) == 2
        assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1


@pytest.mark.asyncio
async def test_album_bounds_collision_name_growth_before_any_native_write(work_delivery, monkeypatch, tmp_path):
    from pydantic import ValidationError

    e = work_delivery
    names = tuple(letter * 36 + ".pdf" for letter in "abcd")
    async with collected_album(e, monkeypatch, tmp_path, caption="a" * 972, second_caption="b" * 972, filenames=names[:2], extra_members=((names[2], "c" * 973), (names[3], "d" * 972))) as (_request, root, reads):
        with pytest.raises(ValidationError):
            await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        assert reads == []
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_album_file_retry_saves_only_failed_member_then_answers_once(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, fail_first=True) as (request, root, reads):
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            files = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put").order_by(TelegramActRow.message_id)))
            assert [file.state for file in files] == ["uncertain", "done"]
            notice = await reader.get(TelegramOutboxRow, files[0].continuation["delivery"]["outbox_id"])
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "respond")))
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
        assert len(replies) == 1 and all(name in replies[0]["text"] for name in ("uploads/statement.pdf", "uploads/part-two.pdf"))
        assert reads == [True, True]
        assert sum(call.endpoint == "getFile" for call in transport.calls) == 3
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramPendingFileRow)))
            assert (await reader.get(TelegramActRow, files[1].id)).attempts == 1


@pytest.mark.asyncio
async def test_album_ack_cannot_settle_changed_manifest_during_sdk_send(work_delivery, monkeypatch, tmp_path):
    import asyncio
    from copy import deepcopy

    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from app.telegram_bots.work_album_files import bundle_text
    from app.telegram_bots.work_album_inputs import inputs_digest
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e, release = work_delivery, asyncio.Event()
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, _reads):
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, root.message_id, e.link.chat_id))
            outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == mark.operation_ids[0]))
        e.outcome_identity = outcome.id
        await clear_outcome_cleanup(e)
        reply = e.channel.bot.request.enqueue("sendMessage", result={"message_id": 920, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saved files"}, release=release)
        try:
            e.clock.advance(10)
            assert await e.sender.tick() == 1
            await asyncio.wait_for(reply.started.wait(), 5)
            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, root.message_id, e.link.chat_id))
                data = deepcopy(mark.route_basis)
                frozen = data["work_file"]
                index = data["work_album"]["member_ids"].index(root.message_id)
                frozen["inputs"] = [frozen["inputs"][index]]
                frozen["files"] = [frozen["files"][index]]
                context = {**data["work_album"], "member_ids": [root.message_id], "inputs_sha256": inputs_digest(frozen["inputs"])}
                data["work_album"] = frozen["album"] = context
                data["album"] = {**data["album"], "message_ids": [root.message_id]}
                frozen["text"] = bundle_text(frozen["inputs"], frozen["files"])
                mark.route_basis = data
            release.set()
            await e.sender.drain()
            async with e.db.sessions() as reader:
                outcome = await reader.get(TelegramOutboxRow, outcome.id)
                assert outcome.state != "sent"
                members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
                assert all(not member.route_complete for member in members)
        finally:
            release.set()
            await e.sender.drain()


@pytest.mark.asyncio
async def test_multipart_album_ack_settles_nothing_before_last_part(work_delivery, monkeypatch, tmp_path):
    await test_album_acknowledgement_names_every_file_and_settles_every_member(work_delivery, monkeypatch, tmp_path, multipart=True)


@pytest.mark.asyncio
async def test_album_recovery_keeps_unstarted_sibling_for_explicit_file_retry(work_delivery, monkeypatch, tmp_path):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        services = e.channel._services
        await services.work_text_inputs.files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=services.commands._guard(e.channel, e.link))
        async with e.db.sessions() as reader:
            first = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
        saved = await e.channel._own_instance_file(e.link, first, first.sanitized_input["files"][0])
        assert saved.state == "done" and reads == [True]
        assert await services.work_text_inputs.files.resume(e.channel, root)
        async with e.db.sessions() as reader:
            native = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put").order_by(TelegramActRow.message_id)))
            assert [file.state for file in native] == ["done", "uncertain"]
            assert native[1].reason == "interrupted"
            retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "file_retry", TelegramCallbackRow.target_id == native[1].id))
            assert retry is not None
        assert reads == [True]
        assert sum(call.endpoint == "getFile" for call in e.channel.bot.request.calls) == 1
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []


@pytest.mark.asyncio
async def test_late_member_blocks_response_after_native_receipts_are_frozen(work_delivery, monkeypatch, tmp_path):
    from telegram import Chat

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e, inserted = work_delivery, []
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        services = e.channel._services
        acts = services.work_text_inputs.callbacks.acts
        prepare = acts._work_admission

        async def before_effect(link, act, **kwargs):
            admission = await prepare(link, act, **kwargs)
            update = Update(
                2902,
                message=Message(
                    902,
                    datetime.now(UTC),
                    Chat(e.link.chat_id, "private"),
                    from_user=User(e.link.telegram_user_id, "Person", False),
                    media_group_id="answer-album",
                    document=Document("late-file", "late-unique", file_name="late.pdf", file_size=4),
                ),
            )
            e.channel.bot.request.enqueue("sendChatAction", result=True)
            assert await e.channel.handle_update(update)
            await e.channel._drain_ingress()
            inserted.append(True)
            return admission

        monkeypatch.setattr(acts, "_work_admission", before_effect)
        try:
            await services.work_text_inputs.files.albums.submit(e.channel, root)
        except (CallbackRefused, ProductSourceRetired, ValueError):
            pass
        assert inserted == [True] and reads == [True, True]
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
        for key, joined in list(services.turns._joins.items()):
            await services.turns._flush_join(key, joined)
        async with e.db.sessions() as reader:
            late = await reader.get(TelegramInboundMarkRow, (e.link.id, 902, e.link.chat_id))
            assert late.route_kind == "work_file_album" and late.route_basis["album"]["state"] == "late"


@pytest.mark.asyncio
async def test_album_submit_drains_repeated_cancellation_and_releases_file_budget(work_delivery, monkeypatch, tmp_path):
    import asyncio

    e, entered, release = work_delivery, asyncio.Event(), asyncio.Event()

    async def pause(_e):
        entered.set()
        await release.wait()

    async with collected_album(e, monkeypatch, tmp_path, on_download=pause) as (request, root, reads):
        services = e.channel._services
        task = asyncio.create_task(services.work_text_inputs.files.albums.submit(e.channel, root))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and services.file_bytes_budget.used == 4
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert reads == [True, True] and services.file_bytes_budget.used == 0
            assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted_first", [False, True])
async def test_actual_album_ingress_reaches_one_complete_work_response(work_delivery, monkeypatch, tmp_path, quoted_first):
    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, quoted_first=quoted_first, live_submit=True) as (request, root, reads):
        assert reads == [True, True]
        responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
        assert len(responses) == 1
        assert all(name in responses[0]["text"] for name in ("uploads/statement.pdf", "uploads/part-two.pdf"))
        async with e.db.sessions() as reader:
            actions = list(await reader.scalars(select(TelegramActRow)))
            assert len(actions) == 3 and all(action.state == "done" for action in actions)


@pytest.mark.asyncio
async def test_unsubmitted_album_pins_every_native_receipt_through_retention(work_delivery, monkeypatch, tmp_path):
    from datetime import timedelta

    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, _reads):
        services = e.channel._services
        captured = await services.work_text_inputs.files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=services.commands._guard(e.channel, e.link))
        for identity in captured.route_basis["work_album"]["member_ids"]:
            async with e.db.sessions() as reader:
                member = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, identity, e.link.chat_id)))
            assert (await e.channel._own_instance_file(e.link, member, member.sanitized_input["files"][0])).state == "done"
        async with e.db.sessions.begin() as writer:
            for file in await writer.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put")):
                file.created_at = e.clock() - timedelta(days=ACT_RETENTION_DAYS + 1)
        assert await _sweep_acts(e.db.sessions, e.clock()) == 0
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put")))) == 2

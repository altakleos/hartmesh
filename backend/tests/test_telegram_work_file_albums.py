"""Actual album ingress collects all members before selecting Work ownership."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Document, Message, Update, User
from test_telegram_instance_file_attachments import pending_service
from test_telegram_work_file_inputs import file_reply
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramPendingFileRow


async def hold_work_submission(*args, **kwargs):
    # These cases qualify collection up to its handoff. Complete native/Work
    # submission is exercised by test_telegram_work_album_answers.
    return True


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted_first", [True, False])
async def test_album_quote_on_later_member_precedes_any_ordinary_file_effect(
    work_delivery, monkeypatch, tmp_path, quoted_first, conflict=False, late=False, restart=False, retention=False, malformed=None, reverse_ids=False, failed_lookup=False
):
    e, owned = work_delivery, []
    second_id = 899 if reverse_ids else 901

    async def prepare(e, request):
        services = e.channel._services
        services.pending_files = await pending_service(e, services.file_puts)
        turns = ChatTurns(services)
        services.turns = turns
        monkeypatch.setattr(services.work_text_inputs.files.albums, "submit", hold_work_submission)
        owned.append(turns)
        # Drive the real joined-input flush after the two actual SDK updates,
        # without a wall-clock race with the ordinary one-second quiet window.
        monkeypatch.setattr(turns, "_arm_join", lambda key, joined: None)
        e.channel.bot.request.enqueue("sendChatAction", result=True)
        e.channel.bot.request.enqueue("sendChatAction", result=True)

    try:
        request, first, reads = await file_reply(e, monkeypatch, tmp_path, quoted=801 if quoted_first else None, album="work-album", before_input=prepare)
        second = Update(
            2901,
            message=Message(
                second_id,
                datetime.now(UTC),
                first.message.chat,
                from_user=first.message.from_user,
                media_group_id="work-album",
                document=Document("second-file", "second-unique", file_name="part-two.pdf", mime_type="application/pdf", file_size=4),
                reply_to_message=Message(778, datetime.now(UTC), first.message.chat, from_user=User(e.link.bot_id, "Bot", True), text="Ordinary quote")
                if conflict
                else None
                if quoted_first
                else Message(801, datetime.now(UTC), first.message.chat, from_user=User(e.link.bot_id, "Bot", True), text="Question"),
            ),
        )
        assert await e.channel.handle_update(second)
        await e.channel._drain_ingress()
        turns = owned[0]
        async with e.db.sessions() as reader:
            queued = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([900, second_id]))))
            assert len(queued) == 2 and all(mark.route_kind == "work_album_pending" for mark in queued)
        if failed_lookup:
            locator = e.channel._services.work_text_inputs.cards
            find = locator.find

            async def unavailable(*args, **kwargs):
                raise RuntimeError("lookup-interrupted")

            monkeypatch.setattr(locator, "find", unavailable)
            for key, joined in list(turns._joins.items()):
                await turns._flush_join(key, joined)
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

            async with e.db.sessions() as reader:
                assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message")))
                marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, second_id]))))
                assert len(marks) == 2 and all(mark.route_kind == "work_album_pending" for mark in marks)
            monkeypatch.setattr(locator, "find", find)
            await turns.recover_link(e.channel)
        if retention:
            from datetime import timedelta

            from app.telegram_bots.retention import sweep_rows
            from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

            await sweep_rows(e.db.sessions, e.clock() + timedelta(days=31), callback_max_age_days=7)
            async with e.db.sessions() as reader:
                assert await reader.get(TelegramOutboxRow, e.identity) is not None
        if restart:
            await turns.retire(e.channel)
            await turns.stop()
            turns = ChatTurns(e.channel._services)
            e.channel._services.turns = turns
            owned.append(turns)
            await turns.recover_link(e.channel)
        else:
            for key, joined in list(turns._joins.items()):
                await turns._flush_join(key, joined)
        assert reads == []
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow)))
            assert not list(await reader.scalars(select(TelegramPendingFileRow)))
            marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([900, second_id]))))
            assert len(marks) == 2 and all(mark.route_kind == "work_file_album" for mark in marks)
            assert all(mark.route_basis["album"]["message_ids"] == [900, second_id] for mark in marks)
            assert all(mark.route_basis["album"]["state"] == ("ambiguous" if conflict else "ready") for mark in marks)
            assert all(mark.route_basis["album"]["root_message_id"] == (None if conflict else 900 if quoted_first else second_id) for mark in marks)
        if malformed is not None:
            from sqlalchemy.orm.attributes import flag_modified

            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
                album = {**mark.route_basis["album"], **malformed}
                mark.route_basis = {**mark.route_basis, "album": album}
                flag_modified(mark, "route_basis")
        if late:
            third = Update(
                2902, message=Message(902, datetime.now(UTC), first.message.chat, from_user=first.message.from_user, media_group_id="work-album", document=Document("third-file", "third-unique", file_name="part-three.pdf", file_size=4))
            )
            e.channel.bot.request.enqueue("sendChatAction", result=True)
            assert await e.channel.handle_update(third)
            await e.channel._drain_ingress()
            for key, joined in list(turns._joins.items()):
                await turns._flush_join(key, joined)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 902, e.link.chat_id))
                if malformed is None:
                    assert mark.route_kind == "work_file_album" and mark.route_basis["album"]["state"] == "late"
                else:
                    assert mark.route_kind == "work_album_pending"
            assert reads == []
        assert await e.channel._services.work_text_inputs.files.handle(e.channel, first, captured_link=e.link)
        assert reads == []
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    finally:
        for turns in owned:
            await turns.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("different_quotes", [False, True])
async def test_non_work_album_preserves_ordinary_reply_boundaries_and_file_receipts(work_delivery, monkeypatch, tmp_path, different_quotes, late_quote=False, restart=False):
    e, owned, partitions = work_delivery, [], []

    async def prepare(e, request):
        services = e.channel._services
        services.pending_files = await pending_service(e, services.file_puts)
        turns = ChatTurns(services)
        services.turns = turns
        monkeypatch.setattr(services.work_text_inputs.files.albums, "submit", hold_work_submission)
        owned.append(turns)
        monkeypatch.setattr(turns, "_arm_join", lambda key, joined: None)
        admit = turns._admit_origins

        async def capture(channel, link, origins, **kwargs):
            partitions.append([origin.message_id for origin in origins])
            return await admit(channel, link, origins, **kwargs)

        monkeypatch.setattr(turns, "_admit_origins", capture)
        transport = e.channel.bot.request
        transport.enqueue("sendChatAction", result=True)
        transport.enqueue("sendChatAction", result=True)
        transport.enqueue("sendMessageDraft", result=True)
        transport.enqueue("sendMessageDraft", result=True)
        transport.enqueue("getFile", result={"file_id": "second-file", "file_unique_id": "second-unique", "file_size": 4, "file_path": "documents/part-two.pdf"})

    try:
        request, first, reads = await file_reply(e, monkeypatch, tmp_path, quoted=777 if different_quotes else None, album="ordinary-album", before_input=prepare)
        second = Update(
            2901,
            message=Message(
                901,
                datetime.now(UTC),
                first.message.chat,
                from_user=first.message.from_user,
                media_group_id="ordinary-album",
                document=Document("second-file", "second-unique", file_name="part-two.pdf", mime_type="application/pdf", file_size=4),
                reply_to_message=Message(778, datetime.now(UTC), first.message.chat, from_user=User(e.link.bot_id, "Bot", True), text="Ordinary reply") if different_quotes else None,
            ),
        )
        assert await e.channel.handle_update(second)
        await e.channel._drain_ingress()
        turns = owned[0]
        if restart:
            await turns.retire(e.channel)
            await turns.stop()
            turns = ChatTurns(e.channel._services)
            e.channel._services.turns = turns
            owned.append(turns)
            await e.channel._services.work_text_inputs.files.albums.recover(lambda: True, link_id=e.link.id)
            page = await e.channel._services.outbox.inbox.prepublication_page(link_id=e.link.id)
            groups = [[origin.message_id for origin in entry.origins] for entry in page.entries]
            assert groups == ([[900], [901]] if different_quotes else [[900, 901]])
            await turns.recover_link(e.channel)
            async with e.db.sessions() as reader:
                acts = list(await reader.scalars(select(TelegramActRow)))
                assert len(acts) == 2 and all(act.state == "uncertain" for act in acts)
            assert reads == [] and partitions == []
            return
        for key, joined in list(turns._joins.items()):
            await turns._flush_join(key, joined)
        assert partitions == ([[900], [901]] if different_quotes else [[900, 901]])
        assert reads == [True, True]
        async with e.db.sessions() as reader:
            acts = list(await reader.scalars(select(TelegramActRow)))
            assert len(acts) == 2 and all(act.kind == "file_put" and act.state == "done" for act in acts)
            assert len(list(await reader.scalars(select(TelegramPendingFileRow)))) == 2
            marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            assert len(marks) == 2 and all(mark.route_complete for mark in marks)
        if late_quote:
            third = Update(
                2902,
                message=Message(
                    902,
                    datetime.now(UTC),
                    first.message.chat,
                    from_user=first.message.from_user,
                    media_group_id="ordinary-album",
                    document=Document("third-file", "third-unique", file_name="part-three.pdf", file_size=4),
                    reply_to_message=Message(801, datetime.now(UTC), first.message.chat, from_user=User(e.link.bot_id, "Bot", True), text="Question"),
                ),
            )
            e.channel.bot.request.enqueue("sendChatAction", result=True)
            assert await e.channel.handle_update(third)
            await e.channel._drain_ingress()
            for key, joined in list(turns._joins.items()):
                await turns._flush_join(key, joined)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 902, e.link.chat_id))
                assert mark.route_kind == "work_file_album"
                assert mark.route_basis["album"]["state"] == "ambiguous"
                assert mark.route_basis["album"]["root_message_id"] is None
            assert reads == [True, True]
        assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
    finally:
        for turns in owned:
            await turns.stop()


@pytest.mark.asyncio
async def test_album_conflicting_explicit_quote_is_retained_without_selecting_a_work_root(work_delivery, monkeypatch, tmp_path):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, True, conflict=True)


@pytest.mark.asyncio
async def test_late_unquoted_member_keeps_durable_work_album_ownership(work_delivery, monkeypatch, tmp_path):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, True, late=True)


@pytest.mark.asyncio
async def test_restart_classifies_whole_durable_album_without_file_or_chat_effects(work_delivery, monkeypatch, tmp_path):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, False, restart=True)


@pytest.mark.asyncio
async def test_first_album_acceptance_and_collection_route_commit_together(work_delivery, monkeypatch, tmp_path):
    e, owned, attempts = work_delivery, [], []

    async def prepare(e, request):
        services = e.channel._services
        turns = ChatTurns(services)
        services.turns = turns
        monkeypatch.setattr(services.work_text_inputs.files.albums, "submit", hold_work_submission)
        owned.append(turns)
        monkeypatch.setattr(turns, "_arm_join", lambda key, joined: None)
        route = services.outbox.inbox.route

        async def interrupt(row, **kwargs):
            if kwargs["kind"] == "work_album_pending" and not attempts:
                attempts.append(True)
                raise ValueError("simulated-stop-before-album-route")
            return await route(row, **kwargs)

        monkeypatch.setattr(services.outbox.inbox, "route", interrupt)
        e.channel.bot.request.enqueue("sendChatAction", result=True)
        e.channel.bot.request.enqueue("sendChatAction", result=True)

    try:
        _, incoming, reads = await file_reply(e, monkeypatch, tmp_path, album="atomic-album", before_input=prepare, allow_refusal=True)
        assert attempts == [True]
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)) is None
        assert await e.channel.handle_update(incoming)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            assert mark.route_kind == "work_album_pending"
        assert reads == []
    finally:
        for turns in owned:
            await turns.stop()


@pytest.mark.asyncio
async def test_unclassified_album_keeps_potential_work_quote_through_retention(work_delivery, monkeypatch, tmp_path):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, False, retention=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [{"version": True}, {"root_message_id": 999}, {"source_ids": []}, {"unexpected": True}], ids=["boolean-version", "foreign-root", "missing-source", "future-shape"])
async def test_malformed_prior_album_cannot_assign_late_member(work_delivery, monkeypatch, tmp_path, malformed):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, True, late=True, malformed=malformed)


@pytest.mark.asyncio
async def test_album_member_identity_is_independent_of_database_order(work_delivery, monkeypatch, tmp_path):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, False, reverse_ids=True)


@pytest.mark.asyncio
async def test_album_lookup_failure_never_offers_ordinary_chat_retry(work_delivery, monkeypatch, tmp_path):
    await test_album_quote_on_later_member_precedes_any_ordinary_file_effect(work_delivery, monkeypatch, tmp_path, False, failed_lookup=True)


@pytest.mark.asyncio
async def test_late_work_quote_cannot_repurpose_an_already_ordinary_album(work_delivery, monkeypatch, tmp_path):
    await test_non_work_album_preserves_ordinary_reply_boundaries_and_file_receipts(work_delivery, monkeypatch, tmp_path, False, late_quote=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("different_quotes", [False, True])
async def test_restart_preserves_ordinary_album_group_without_file_effects(work_delivery, monkeypatch, tmp_path, different_quotes):
    await test_non_work_album_preserves_ordinary_reply_boundaries_and_file_receipts(work_delivery, monkeypatch, tmp_path, different_quotes, restart=True)

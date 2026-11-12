"""Recovery choices retain every actual album member and its original quotes."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_album_answers import collected_album
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


async def cancel_question(e, request):
    record = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=request["work_id"])
    await e.human.work.command(
        actor=e.actor,
        instance_id=e.link.agent_ref,
        work_id=record["id"],
        request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=record["revision"], expected_assignment_revision=record["assignment_revision"]),
        can_stop=False,
    )


@pytest.mark.asyncio
async def test_closed_album_offers_one_complete_group_choice_before_any_download(work_delivery, monkeypatch, tmp_path):
    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        await cancel_question(e, request)
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        assert reads == []
        async with e.db.sessions() as reader:
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901])).order_by(TelegramInboundMarkRow.message_id)))
            assert all(mark.route_kind == "work_album_hold" and not mark.route_complete for mark in members)
            context = members[0].route_basis["work_album_recovery"]
            assert all(mark.route_basis["work_album_recovery"] == context for mark in members)
            assert context["member_ids"] == [900, 901]
            notice = await reader.get(TelegramOutboxRow, context["notice_id"])
            assert notice.payload["product_basis"]["origins"] == [[e.link.id, 900, e.link.chat_id], [e.link.id, 901, e.link.chat_id]]
            assert [button[0]["kind"] for button in notice.payload["product_keyboard_basis"]] == ["prompt_send_new", "prompt_discard"]
            assert not list(await reader.scalars(select(TelegramActRow)))
            assert members[0].sanitized_input.get("reply_to_message_id") is None
            assert members[1].sanitized_input["reply_to_message_id"] == 801


@pytest.mark.asyncio
@pytest.mark.parametrize("shown", [False, True])
async def test_discard_settles_whole_album_without_closing_another_prompt(work_delivery, monkeypatch, tmp_path, shown):
    from test_telegram_work_actions import management_request
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_interaction_delivery import open_view
    from test_telegram_work_interaction_delivery import selected as select_control
    from test_telegram_work_preview_recovery import dispatch
    from test_telegram_work_typing_prompts import clear_old

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        await cancel_question(e, request)
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_discard"))
        _record, another_request, _seen = await management_request(e, "review")
        e.identity = await e.plans.plan(e.link, request_id=another_request["id"], dedupe_key="another-album-review")
        await dispatch(e, 911)
        e.identity = await open_view(e, await select_control(e, "changes_open"))
        await clear_old(e)
        if shown:
            await dispatch(e, 912)
        async with e.db.sessions() as reader:
            later_prompt = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
        await press_work(e, callback, message_id=910)
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramCallbackRow, later_prompt)).consumed_at is None
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            assert all(member.route_complete for member in members)
            assert not list(await reader.scalars(select(TelegramActRow)))
            assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
        assert reads == []


@pytest.mark.asyncio
@pytest.mark.parametrize("caption", ["First caption", ""])
async def test_album_send_new_preserves_all_actual_inputs_in_one_publication(work_delivery, monkeypatch, tmp_path, caption, retry_path=False, expire_retry=False):
    from test_telegram_instance_file_input import publish
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from app.channels.message_bus import MessageBus
    from app.telegram_bots.publication import PublicationCoordinator
    from app.telegram_bots.publication_authority import PublicationAttemptAuthority
    from app.telegram_bots.turns import _Turn
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramPendingFileRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, caption=caption, second_caption="") as (request, root, reads):
        await cancel_question(e, request)
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_send_new"))
        services, started = e.channel._services, []
        services.threads, services.thread_client = e.threads, e.http_client
        e.bus = MessageBus(inbound_queue_maxsize=4)
        e.host = PublicationAttemptAuthority(links=e.db.links, authority=e.live.authority, client=e.http_client, channel=lambda identity: e.current[0], create_allowed=lambda: e.active[0], read_allowed=lambda: e.active[0])
        e.coordinator = PublicationCoordinator(inbox=services.outbox.inbox, outbox=services.outbox, bus=e.bus, http_owner=e.http_client, capture_attempt=e.host.capture)
        services.publication_authority, services.publications = e.host, e.coordinator
        turns = services.turns
        try:

            def start(channel, link, inbound):
                topic = inbound.metadata.get("message_thread_id")
                identity = turns.queue.current_callback(link, topic)
                origins = turns._callback_origins.pop((link.id, identity))[1]
                turn = _Turn(channel, link, inbound, origins)
                turn.callback_id = identity
                turns._turns[(link.id, topic)] = turn
                started.append(turn)

            monkeypatch.setattr(turns, "_start", start)
            prepare = turns._prepare_once

            async def prepare_chat(turn):
                result = await prepare(turn)
                e.thread_id = turn.record.scope.thread_id
                return result

            monkeypatch.setattr(turns, "_prepare_once", prepare_chat)
            await press_work(e, callback, message_id=910)
            assert reads == [True, True]
            if caption:
                # An independently saved ordinary file in the same scope must be
                # collectible; a pending album cannot pin the whole chat.
                from datetime import UTC, datetime, timedelta

                from telegram import Document, Message, Update
                from test_telegram_inbox import incoming

                from app.telegram_bots.retention import _sweep_acts, sweep_rows

                template = incoming(message=999, update_id=2999, account=e.link.chat_id)
                update = Update(2999, message=Message(999, datetime.now(UTC), template.message.chat, from_user=template.message.from_user, document=Document("unrelated", "unrelated-unique", file_name="unrelated.pdf", file_size=4)))
                descriptor = {**e.channel._extract_inbound_files(update.message)[0], "message_id": 999, "telegram_bot_id": e.link.bot_id}
                independent, _ = await services.outbox.inbox.accept(e.link, update, sanitized_input={"type": "media", "text": "An independent file", "files": [descriptor]})
                independent = await services.outbox.inbox.route(independent, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None}}, operation_ids=[])
                e.channel.bot.request.enqueue("getFile", result={"file_id": "unrelated", "file_unique_id": "unrelated-unique", "file_size": 4, "file_path": "documents/unrelated.pdf"})
                assert (await e.channel._own_instance_file(e.link, independent, descriptor, origins=(independent,))).state == "done"
                assert reads == [True, True, True]
                await services.outbox.inbox.finish_route(independent)
                async with e.db.sessions.begin() as writer:
                    for native in await writer.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put")):
                        native.created_at = e.clock() - timedelta(days=401)
                    for mark in await writer.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))):
                        assert mark.state == "done"
                        mark.closed_at = e.clock() - timedelta(days=8)
                swept = await sweep_rows(e.db.sessions, e.clock(), callback_max_age_days=7)
                assert swept["inbound"] == 2 and swept["acts"] == 1
                assert await _sweep_acts(e.db.sessions, e.clock()) == 0
                assert len(started) == 1
                if retry_path:
                    from types import SimpleNamespace

                    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

                    from app.telegram_bots.supervisor import TelegramSupervisor
                    from app.telegram_bots.turns import ChatTurns

                    async def admit(view, *, work):
                        assert view.id == e.link.agent_ref and work is False

                    e.file_service.residents = SimpleNamespace(admit=admit)

                    async def receipts(link, *, current=None):
                        return await TelegramSupervisor.instance_file_receipts(services, link, current=current)

                    async def preflight(link, record, files, *, execution_context):
                        return await TelegramSupervisor.preflight_inbound_files(services, link, record, files, execution_context=execution_context)

                    e.coordinator.instance_file_receipts, e.coordinator.file_preflight = receipts, preflight
                    for index, boundary in enumerate(("before-publication", "restart", "reserved-before-dispatch")):
                        previous = started[-1]
                        if boundary == "restart":
                            turns._forget(previous)
                            await turns.stop()
                            turns = ChatTurns(services)
                            services.turns = turns
                            monkeypatch.setattr(turns, "_start", start)
                            prepare = turns._prepare_once
                            monkeypatch.setattr(turns, "_prepare_once", prepare_chat)
                            await turns._close_left_inputs(lambda: True, link_id=e.link.id)
                        else:
                            if boundary == "reserved-before-dispatch":

                                async def interrupted_dispatch(*args, **kwargs):
                                    raise RuntimeError("Interrupted after reservation, before dispatch")

                                with monkeypatch.context() as interrupted:
                                    interrupted.setattr(e.coordinator, "dispatch_record", interrupted_dispatch)
                                    with pytest.raises(RuntimeError, match="Interrupted after reservation"):
                                        await turns._prepare_once(previous)
                                assert previous.record is not None and not previous.record.dispatch_started
                                assert e.bus.inbound_queue.empty()
                            await turns._close(previous)
                        async with e.db.sessions() as reader:
                            prior_owner = (await reader.get(TelegramCallbackRow, previous.callback_id)).payload["publication_owner"]
                            e.identity = prior_owner["outbox_id"]
                            assert e.identity is not None
                        e.outcome_identity = e.identity
                        await clear_outcome_cleanup(e)
                        await dispatch(e, 920 + index)
                        assert await _sweep_acts(e.db.sessions, e.clock()) == 0
                        async with e.db.sessions() as reader:
                            parent = await reader.get(TelegramOutboxRow, e.identity)
                            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "retry_message"))
                        async with e.db.sessions.begin() as writer:
                            old_owner = await writer.get(TelegramCallbackRow, previous.callback_id)
                            old_owner.created_at = old_owner.consumed_at = e.clock() - timedelta(days=8)
                        if expire_retry:
                            async with e.db.sessions.begin() as writer:
                                (await writer.get(TelegramCallbackRow, callback.id)).expires_at = e.clock() - timedelta(seconds=1)
                            await services.callbacks.sweep(e.clock())
                            async with e.db.sessions() as reader:
                                assert await reader.get(TelegramCallbackRow, previous.callback_id) is None
                            assert await _sweep_acts(e.db.sessions, e.clock()) == 2
                            return
                        await services.callbacks.sweep(e.clock())
                        async with e.db.sessions() as reader:
                            assert await reader.get(TelegramCallbackRow, previous.callback_id) is not None
                            if index:
                                assert await reader.get(TelegramCallbackRow, started[index - 1].callback_id) is None
                        await press_work(e, callback, message_id=920 + index, update_id=3000 + index)
                        assert len(started) == index + 2
                        assert reads == [True, True, True]
                sent, body = await publish(e, turns, started[-1], monkeypatch, tmp_path)
                assert sent.files == []
                files = body["input"]["messages"][0]["additional_kwargs"]["files"]
                assert len(files) == 2 and {file["path"] for file in files} == {"/mnt/user-data/uploads/statement.pdf", "/mnt/user-data/uploads/part-two.pdf"}
                async with e.db.sessions() as reader:
                    selected = await reader.get(TelegramCallbackRow, callback.id)
                    owner = selected.payload["publication_owner"]
                    assert [mark["message_id"] for mark in owner["inputs"]] == [900, 901]
                    assert owner["inputs"][0]["sanitized_input"].get("reply_to_message_id") is None
                    assert owner["inputs"][1]["sanitized_input"]["reply_to_message_id"] == 801
            else:
                assert not started and e.bus.inbound_queue.empty()
                async with e.db.sessions() as reader:
                    pending = list(await reader.scalars(select(TelegramPendingFileRow).where(TelegramPendingFileRow.link_id == e.link.id)))
                    assert len(pending) == 2
                    questions = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:file-received:%"))))
                    assert len(questions) == 1 and questions[0].parts[0]["source_ref"]["values"]["n"] == 2
            assert (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"] == []
        finally:
            for turn in started:
                turns._forget(turn)
            await turns.stop()
            await e.coordinator.stop()
            e.bus.discard_pending_inbound()


@pytest.mark.asyncio
async def test_reopen_album_displays_fresh_question_without_submitting_old_members(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_input_refresh import deliver_refreshed
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        async with e.db.sessions.begin() as writer:
            source = await writer.get(TelegramOutboxRow, root.route_basis["source_outbox_id"])
            for callback in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == source.parts[-1]["callback_group_id"])):
                callback.consumed_at, callback.closed_reason = e.clock(), "command"
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
            assert callback is not None
        await press_work(e, callback, message_id=910)
        async with e.db.sessions() as reader:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == callback.id))
            assert fresh is not None
            e.identity = fresh.id
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            assert all(member.route_complete and fresh.id in member.delivery_ids for member in members)
            assert not list(await reader.scalars(select(TelegramActRow)))
        await deliver_refreshed(e, 911)
        async with e.db.sessions() as reader:
            fresh = await reader.get(TelegramOutboxRow, e.identity)
            assert fresh.state == "sent", e.dispatch_errors
            prompt = await reader.get(TelegramCallbackRow, fresh.payload["work_request_basis"]["prompt_id"])
            assert prompt.consumed_at is None and prompt.message_id == 911
        assert not reads and not (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["prompt_discard", "prompt_send_new"])
async def test_late_member_after_group_recovery_keeps_work_ownership(work_delivery, monkeypatch, tmp_path, disposition):
    from datetime import UTC, datetime

    from telegram import Document, Message, Update
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, caption="", second_caption="") as (request, root, reads):
        await cancel_question(e, request)
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == disposition))
        await press_work(e, callback, message_id=910)
        before = len(reads)
        from test_telegram_inbox import incoming

        template = incoming(message=902, update_id=2902, account=e.link.chat_id)
        late = Update(2902, message=Message(902, datetime.now(UTC), template.message.chat, from_user=template.message.from_user, media_group_id="answer-album", document=Document("late", "late-unique", file_name="late.pdf", file_size=4)))
        assert await e.channel.handle_update(late)
        await e.channel._drain_ingress()
        for key, joined in list(e.channel._services.turns._joins.items()):
            await e.channel._services.turns._flush_join(key, joined)
        async with e.db.sessions() as reader:
            late_mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 902, e.link.chat_id))
            assert late_mark.route_kind == "work_album_hold"
            assert late_mark.route_basis["album"]["state"] == "late"
            assert late_mark.route_basis["work_album_recovery"]["member_ids"] == [902]
        assert len(reads) == before


@pytest.mark.asyncio
async def test_group_offer_rolls_back_member_mutation_during_final_authority(work_delivery, monkeypatch, tmp_path):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.commands import _FixedGuard

    e, changed = work_delivery, []
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        await cancel_question(e, request)
        check = _FixedGuard.check

        async def mutate(self, writer):
            result = await check(self, writer)
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            if "work_album_recovery" in mark.route_basis and not changed:
                changed.append(True)
                mark.sanitized_input = {**mark.sanitized_input, "text": "Replaced during final authority"}
                await writer.flush()
            return result

        monkeypatch.setattr(_FixedGuard, "check", mutate)
        with pytest.raises(CallbackRefused):
            await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        assert changed
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            assert mark.sanitized_input["text"] == "First caption"
            assert "work_album_recovery" not in mark.route_basis


@pytest.mark.asyncio
async def test_group_native_write_refuses_changed_sibling_choice_after_download(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e, changed = work_delivery, []

    async def withdraw(_e):
        if changed:
            return
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            parent = await writer.get(TelegramOutboxRow, mark.route_basis["work_album_recovery"]["notice_id"])
            other = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_discard"))
            other.consumed_at, other.closed_reason = e.clock(), "command"
            changed.append(True)

    async with collected_album(e, monkeypatch, tmp_path, caption="", second_caption="", on_download=withdraw) as (request, root, reads):
        await cancel_question(e, request)
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_send_new"))
        await press_work(e, callback, message_id=910)
        assert changed and reads == [True]
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == e.home.id)))


@pytest.mark.asyncio
async def test_reopen_cannot_redirect_coherently_changed_group_to_another_request(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        async with e.db.sessions.begin() as writer:
            source = await writer.get(TelegramOutboxRow, root.route_basis["source_outbox_id"])
            for callback in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == source.parts[-1]["callback_group_id"])):
                callback.consumed_at, callback.closed_reason = e.clock(), "command"
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        other_work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Different work", success_criteria="Different evidence"))
        other = await e.human.create(
            actor=e.actor,
            instance_id=e.instance.id,
            work_id=other_work["id"],
            request=CreateRequest(
                operation_id=uuid4().hex,
                expected_work_revision=1,
                expected_assignment_revision=1,
                purpose="information",
                question="Other question?",
                reason="Other context",
                expected_response="A different answer",
                recipient_id=e.link.owner_user_id,
            ),
        )
        async with e.db.sessions.begin() as writer:
            notice = await writer.get(TelegramOutboxRow, e.identity)
            callback = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
            for mark in await writer.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))):
                mark.route_basis = {**mark.route_basis, "work_album_recovery": {**mark.route_basis["work_album_recovery"], "request_id": other["id"], "work_id": other_work["id"]}}
        await press_work(e, callback, message_id=910)
        async with e.db.sessions() as reader:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == callback.id))
            assert fresh is None
            assert not (await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete


async def closed_send_choice(e, request, root):
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    await cancel_question(e, request)
    await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
    await dispatch(e, 910)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        return await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_send_new"))


@pytest.mark.asyncio
async def test_interrupted_album_handoff_reconciles_metadata_without_redownloading(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work

    from app.telegram_bots.inbox import InboxRow
    from deerflow.persistence.telegram_bots.model import TelegramPendingFileRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, caption="", second_caption="") as (request, root, reads):
        selected = await closed_send_choice(e, request, root)
        actual = e.channel._own_instance_file
        count = []

        async def interrupt(link, origin, file, **kwargs):
            count.append(origin.message_id)
            if len(count) == 2:
                raise RuntimeError("interrupted before second member")
            return await actual(link, origin, file, **kwargs)

        monkeypatch.setattr(e.channel, "_own_instance_file", interrupt)
        await press_work(e, selected, message_id=910)
        assert reads == [True]
        async with e.db.sessions() as reader:
            retained = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
            assert retained.route_kind == "work_file_new" and not retained.route_complete
        monkeypatch.setattr(e.channel, "_own_instance_file", actual)
        assert await e.channel._services.work_text_inputs.files.resume(e.channel, retained)
        assert reads == [True]
        async with e.db.sessions() as reader:
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            assert all(mark.route_complete for mark in members)
            natives = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "file_put").order_by(TelegramActRow.message_id)))
            assert [native.state for native in natives] == ["done", "uncertain"]
            assert len(list(await reader.scalars(select(TelegramPendingFileRow).where(TelegramPendingFileRow.link_id == e.link.id)))) == 1


@pytest.mark.asyncio
async def test_saved_work_album_transfers_receipts_without_repeating_native_writes(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work

    from app.telegram_bots.inbox import InboxRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, caption="", second_caption="") as (request, root, reads):
        files = e.channel._services.work_text_inputs.files
        captured = await files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=e.channel._services.commands._guard(e.channel, e.link))
        for identity in captured.route_basis["work_album"]["member_ids"]:
            async with e.db.sessions() as reader:
                member = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, identity, e.link.chat_id)))
            assert (await e.channel._own_instance_file(e.link, member, member.sanitized_input["files"][0])).state == "done"
        async with e.db.sessions() as reader:
            before = {native.id: (native.body, native.operation_ids, native.continuation["target"], native.result) for native in await reader.scalars(select(TelegramActRow))}
        selected = await closed_send_choice(e, request, captured)
        await press_work(e, selected, message_id=910)
        assert reads == [True, True]
        async with e.db.sessions() as reader:
            after = list(await reader.scalars(select(TelegramActRow)))
            assert len(after) == 2
            assert all(native.continuation["version"] == 3 and "work" not in native.continuation for native in after)
            assert {native.id: (native.body, native.operation_ids, native.continuation["target"], native.result) for native in after} == before


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_discard", "prompt_send_new", "prompt_reopen"])
async def test_group_choices_preserve_arrival_order_with_reversed_message_ids(work_delivery, monkeypatch, tmp_path, choice):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, caption="", second_caption="", second_identity=899) as (request, root, reads):
        async with e.db.sessions.begin() as writer:
            source = await writer.get(TelegramOutboxRow, root.route_basis["source_outbox_id"])
            for callback in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == source.parts[-1]["callback_group_id"])):
                callback.consumed_at, callback.closed_reason = e.clock(), "command"
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
            assert mark.route_basis["work_album_recovery"]["member_ids"] == [900, 899]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == choice))
        await press_work(e, callback, message_id=910)
        async with e.db.sessions() as reader:
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 899]))))
            assert len(members) == 2 and all(mark.route_complete for mark in members)
        assert reads == ([True, True] if choice == "prompt_send_new" else [])


@pytest.mark.asyncio
async def test_overlong_album_explains_limit_without_partial_upload_or_response(work_delivery, monkeypatch, tmp_path):
    e = work_delivery
    name, text = "x" * 180 + ".pdf", "c" * 900
    async with collected_album(e, monkeypatch, tmp_path, caption=text, second_caption=text, filenames=(name, name), extra_members=((name, text), (name, text))) as (request, root, reads):
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_album_recovery"]["notice_id"])
            assert notice.parts[0]["source_ref"]["key"] == "ask.answer_too_long"
            assert len(mark.route_basis["work_album_recovery"]["member_ids"]) == 4
            assert not list(await reader.scalars(select(TelegramActRow)))
        assert not reads and not (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]


@pytest.mark.asyncio
async def test_ambiguous_album_has_no_arbitrary_reopen_target(work_delivery, monkeypatch, tmp_path):
    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path, quoted_first=True, second_quote_id=999) as (request, root, reads):
        assert root.route_basis["album"]["state"] == "ambiguous"
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            context = mark.route_basis["work_album_recovery"]
            assert context["request_id"] is None and context["source_outbox_id"] is None
            notice = await reader.get(TelegramOutboxRow, context["notice_id"])
            assert [row[0]["kind"] for row in notice.payload["product_keyboard_basis"]] == ["prompt_send_new", "prompt_discard"]
        assert not reads and not (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]


@pytest.mark.asyncio
async def test_orphaned_group_response_marker_cannot_offer_recovery_choices(work_delivery, monkeypatch, tmp_path):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.inbox import InboxRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, reads):
        files = e.channel._services.work_text_inputs.files
        captured = await files.cards.capture(e.link, root, source_outbox_id=root.route_basis["source_outbox_id"], guard=e.channel._services.commands._guard(e.channel, e.link))
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, captured.message_id, e.link.chat_id))
            mark.route_basis = {**mark.route_basis, "work_action_id": mark.operation_ids[0]}
            captured = InboxRow.from_row(mark)
        with pytest.raises(CallbackRefused):
            await files.resume(e.channel, captured)
        async with e.db.sessions() as reader:
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            assert all(not mark.route_complete and "work_album_recovery" not in mark.route_basis for mark in members)
        assert reads == []


@pytest.mark.asyncio
async def test_reopen_album_after_question_closes_keeps_both_members_for_new_choice(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        async with e.db.sessions.begin() as writer:
            source = await writer.get(TelegramOutboxRow, root.route_basis["source_outbox_id"])
            for callback in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == source.parts[-1]["callback_group_id"])):
                callback.consumed_at, callback.closed_reason = e.clock(), "command"
        await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            e.identity = mark.route_basis["work_album_recovery"]["notice_id"]
        await dispatch(e, 910)
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, e.identity)
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
        await cancel_question(e, request)
        await press_work(e, callback, message_id=910)
        async with e.db.sessions() as reader:
            members = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([900, 901]))))
            contexts = [mark.route_basis["work_album_recovery"] for mark in members]
            assert contexts[0] == contexts[1] and contexts[0]["notice_id"] != e.identity
            replacement = await reader.get(TelegramOutboxRow, contexts[0]["notice_id"])
            assert replacement.parts[0]["source_ref"]["key"] == "prompt.unavailable"
            assert [line[0]["kind"] for line in replacement.payload["product_keyboard_basis"]] == ["prompt_send_new", "prompt_discard"]
            assert all(not mark.route_complete for mark in members)
        assert not reads


@pytest.mark.asyncio
async def test_album_metadata_recovery_keeps_send_new_after_inspect_is_removed(work_delivery, monkeypatch, tmp_path):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (_request, root, reads):
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
        assert await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_album_recovery"]["notice_id"])
            assert [line[0]["kind"] for line in notice.payload["product_keyboard_basis"]] == ["prompt_send_new", "prompt_discard"]
        assert not reads


@pytest.mark.asyncio
async def test_group_offer_rechecks_use_with_its_final_input_snapshot(work_delivery, monkeypatch, tmp_path):
    from app.telegram_bots.commands import _FixedGuard
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e, changed = work_delivery, []
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, reads):
        await cancel_question(e, request)
        check = _FixedGuard.check

        async def withdraw(self, writer):
            result = await check(self, writer)
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            if "work_album_recovery" in mark.route_basis and not changed:
                changed.append(True)
                (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 2
                await writer.flush()
            return result

        monkeypatch.setattr(_FixedGuard, "check", withdraw)
        with pytest.raises(AgentDenied):
            await e.channel._services.work_text_inputs.files.albums.submit(e.channel, root)
        assert changed
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            assert "work_album_recovery" not in mark.route_basis and not mark.route_complete


@pytest.mark.asyncio
async def test_album_retry_survives_inbox_collection_and_controller_restart(work_delivery, monkeypatch, tmp_path):
    await test_album_send_new_preserves_all_actual_inputs_in_one_publication(work_delivery, monkeypatch, tmp_path, "First caption", retry_path=True)


@pytest.mark.asyncio
async def test_expired_album_retry_releases_owner_and_native_receipts(work_delivery, monkeypatch, tmp_path):
    await test_album_send_new_preserves_all_actual_inputs_in_one_publication(work_delivery, monkeypatch, tmp_path, "First caption", retry_path=True, expire_retry=True)

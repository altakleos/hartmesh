"""Explicit Work file replies retain the actual media and native write receipt."""

from copy import deepcopy

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import CallbackRefused, normalize_callback_topic
from app.telegram_bots.file_puts import _file_journal, file_act_id, work_input_digest
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_input_capture import _TEXT
from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow
from deerflow.utils.file_io import await_drained


def file_response_text(captured, receipt):
    text = captured.route_basis["work_input"]["text"]
    return _TEXT.validate_python((text + "\n\n" if text else "") + receipt.result["path"])


def validate_file_response(link, captured, expected, actual, instance, home_generation):
    from app.telegram_bots.work_album_files import AlbumFileBundle

    if isinstance(expected, AlbumFileBundle):
        expected.check(actual, instance, home_generation)
        return
    if actual is None:
        raise CallbackRefused()
    file = _file_journal(actual)
    original = captured.route_basis["work_input"]["files"][0]
    if actual.continuation.get("work", {}).get("input_sha256") != work_input_digest(captured) or actual.state != "done" or not same_body(file, original) or actual.id != file_act_id(link, original):
        raise CallbackRefused()
    scope = {
        "link_id": link.id,
        "owner_user_id": link.owner_user_id,
        "bot_id": link.bot_id,
        "binding_generation": link.binding_generation,
        "telegram_user_id": link.telegram_user_id,
        "chat_id": link.chat_id,
        "instance_id": link.agent_ref,
        "message_id": captured.message_id,
    }
    target = actual.continuation["target"]
    if any(getattr(actual, key) != value for key, value in scope.items()) or any(
        target[key] != value
        for key, value in {
            "instance_id": instance.id,
            "principal_kind": "nonhuman",
            "principal_id": instance.principal_id,
            "instance_generation": instance.generation,
            "definition_revision": instance.definition_revision,
            "home_id": instance.home_id,
            "home_generation": home_generation,
        }.items()
    ):
        raise CallbackRefused()
    if _snapshot(actual) != expected[0] or file_response_text(captured, expected[0]) != expected[1]:
        raise CallbackRefused()


async def read_file_response(link, captured, *, session):
    if "work_album" in captured.route_basis:
        from app.telegram_bots.work_album_files import read_album_files

        return await read_album_files(link, captured, session=session)
    file = captured.route_basis["work_input"]["files"][0]
    row = await session.get(TelegramActRow, file_act_id(link, file), with_for_update=True, populate_existing=True)
    if row is None or row.state != "done" or not same_body(_file_journal(row), file) or row.continuation.get("work", {}).get("input_sha256") != work_input_digest(captured):
        raise CallbackRefused()
    receipt = _snapshot(row)
    return receipt, file_response_text(captured, receipt)


def freeze_file_answer(receipt):
    from app.telegram_bots.work_album_files import AlbumFileBundle

    if isinstance(receipt, AlbumFileBundle):
        return receipt.freeze()
    row, text = receipt
    return {"version": 1, "act_id": row.id, "operation_id": row.operation_ids[-1], "target": thaw(row.continuation["target"]), "result": thaw(row.result), "text": text}


def saved_file_text(captured):
    if "work_album" in captured.route_basis:
        from app.telegram_bots.work_album_files import saved_album_text

        return saved_album_text(captured)
    data = captured.route_basis.get("work_file")
    if type(data) is not dict or set(data) != {"version", "act_id", "operation_id", "target", "result", "text"} or type(data["version"]) is not int or data["version"] != 1:
        raise CallbackRefused()
    from types import SimpleNamespace

    from app.telegram_bots.inbox import chat_input_parts

    validate_saved_file_reference(data)
    actual_text, files = chat_input_parts(captured)
    work = captured.route_basis["work_input"]
    if len(files) != 1 or not same_body(files, work["files"]) or data["act_id"] != file_act_id(SimpleNamespace(id=captured.link_id), files[0]) or work["text"] != (_TEXT.validate_python(actual_text) if actual_text.strip() else None):
        raise CallbackRefused()
    caption = work["text"]
    text = _TEXT.validate_python((caption + "\n\n" if caption else "") + data["result"]["path"])
    if data["text"] != text:
        raise CallbackRefused()
    return text


def validate_saved_file_reference(data):
    import re

    from app.telegram_bots.file_puts import _file_result

    _file_result(data["result"])
    target = data["target"]
    if type(target) is not dict or set(target) != {"instance_id", "principal_kind", "principal_id", "instance_generation", "definition_revision", "home_id", "home_generation"}:
        raise CallbackRefused()
    if (
        any(type(value) is not str or re.fullmatch(r"[0-9a-f]{32}", value) is None for value in (data["act_id"], data["operation_id"], target["instance_id"], target["home_id"]))
        or type(target["definition_revision"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", target["definition_revision"]) is None
        or (target["principal_kind"], target["principal_id"]) != ("nonhuman", "agent:" + target["instance_id"])
        or any(type(target[key]) is not int or target[key] < 1 for key in ("instance_generation", "home_generation"))
    ):
        raise CallbackRefused()


class WorkFileInputs:
    def __init__(self, inputs):
        self.inputs, self.services = inputs, inputs.services
        self.cards, self.inbox = inputs.cards.cards, inputs.inbox
        from app.telegram_bots.work_file_albums import WorkFileAlbums

        self.albums = WorkFileAlbums(self)

    async def handle(self, channel, update, *, captured_link):
        async def perform():
            link, message = deepcopy(captured_link), update.message
            if link.agent_kind != "instance" or message is None or not (message.document or message.photo) or not channel._callback_current(link):
                return False
            if message.forward_origin is not None or message.is_automatic_forward is True or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date")):
                return False
            guard = self.services.commands._guard(channel, link)
            if message.media_group_id is not None:
                async with self.services.callbacks.writer(link, guard) as writer:
                    mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
                    if mark is not None and (mark.route_kind == "work_file_album" or "work_album" in mark.route_basis):
                        self.inbox._current(link, mark)
                        return True
                return False  # The shared ingress joins the whole album before classification.
            async with self.services.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
                if mark is not None and mark.route_kind == "work_file_new":
                    return True
                if mark is not None and mark.route_kind is not None:
                    if mark.route_kind == "work_card_hold" and mark.sanitized_input.get("type") == "media":
                        origin, source = InboxRow.from_row(mark), mark.route_basis["source_outbox_id"]
                    elif mark.route_kind != "work_input" or mark.route_basis.get("work_input", {}).get("version") != 4:
                        return False
                    if mark.route_complete:
                        return True
                    if mark.route_kind != "work_card_hold":
                        origin, source = InboxRow.from_row(mark), mark.route_basis["work_input"]["source_outbox_id"]
                else:
                    origin, source = None, None
            if origin is None:
                if message.reply_to_message is None:
                    return False
                source = await self.inputs.cards.find(link, message.reply_to_message.message_id, topic=normalize_callback_topic(message.message_thread_id))
                if source is None:
                    return False
                files = [{**value, "message_id": message.message_id, "telegram_bot_id": link.bot_id} for value in channel._extract_inbound_files(message)]
                origin, _ = await self.inbox.accept(link, update, sanitized_input={"type": "media", "text": message.caption or "", "files": files})
            try:
                return await self._apply(channel, link, origin, source, guard)
            except (CallbackRefused, ProductSourceRetired, AgentConflict):
                return await self._hold(channel, link, origin, source, guard)

        return await await_drained(perform())

    async def _hold(self, channel, link, origin, source, guard):
        async with self.services.callbacks.writer(link, guard) as writer:
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            if "work_album" in mark.route_basis:
                raise CallbackRefused()  # Group choices must retain every member.
            if mark.route_complete:
                return True
            # An admitted/unknown response retains its immutable receipt owner.
            if "work_action_id" in mark.route_basis:
                raise CallbackRefused()
            for identity in mark.operation_ids:
                if await writer.get(TelegramActRow, identity) is not None:
                    raise CallbackRefused()
            if mark.route_kind not in {None, "work_input", "work_card_hold"}:
                raise CallbackRefused()
            mark.route_kind, mark.route_target_id, mark.operation_ids = "work_card_hold", source, []
            mark.route_basis = {**mark.route_basis, "source_outbox_id": source, "input_profile": {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None}}
            await writer.flush()
            captured = InboxRow.from_row(mark)
        await self.inputs.callbacks.closed.offer(link, captured, guard=guard)
        return True

    async def after_retry(self, channel, link, receipt):
        return await await_drained(self._after_retry(channel, link, receipt))

    async def _after_retry(self, channel, link, receipt):
        guard = self.services.commands._guard(channel, link)
        async with self.services.callbacks.writer(link, guard) as writer:
            mark = await writer.get(TelegramInboundMarkRow, (link.id, receipt.message_id, link.chat_id))
            if mark is None:
                raise CallbackRefused()
            if receipt.continuation["work"]["version"] == 2:
                from app.telegram_bots.work_album_inputs import album_context, album_input_digest

                if not same_body(album_context(mark), receipt.continuation["work"]["album"]) or album_input_digest(mark) != receipt.continuation["work"]["input_sha256"]:
                    raise CallbackRefused()
            else:
                WorkFileWriteProjection(link, receipt).check([(mark,)], allow_complete=True)
            if mark.route_complete:
                return True
            captured = InboxRow.from_row(mark)
        return await self.resume(channel, captured)

    async def resume(self, channel, origin):
        return await await_drained(self._resume(channel, origin))

    async def _resume(self, channel, origin):
        link = deepcopy(channel.link)
        guard = self.services.commands._guard(channel, link)
        async with self.services.callbacks.writer(link, guard) as writer:
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            album = mark.route_kind == "work_file_album" or "work_album" in mark.route_basis
            if not album and mark.route_kind != "work_file_new" and (mark.route_kind != "work_input" or mark.route_basis.get("work_input", {}).get("version") != 4):
                return False
            if mark.route_complete:
                return True
            captured = InboxRow.from_row(mark)

        if album:
            from app.telegram_bots.work_album_answers import WorkAlbumAnswers

            return await WorkAlbumAnswers(self).submit(channel, captured, recover=True)

        if captured.route_kind == "work_file_new":
            from app.telegram_bots.work_file_handoff import recover_file_message

            return await recover_file_message(self.inputs.callbacks.closed, channel, link, captured, guard=guard)

        async def perform():
            source = captured.route_basis["work_input"]["source_outbox_id"]
            try:
                return await self._apply(channel, link, captured, source, guard, recover=True)
            except (CallbackRefused, ProductSourceRetired, AgentConflict):
                return await self._hold(channel, link, captured, source, guard)

        return await await_drained(perform())

    async def _apply(self, channel, link, origin, source, guard, *, recover=False):
        captured = await self.cards.capture(link, origin, source_outbox_id=source, guard=guard)
        # A submitted response owns its file intent. Its canonical receipt must
        # survive Home changes and file-journal collection without another put.
        if "work_action_id" not in captured.route_basis:
            for file in captured.route_basis["work_input"]["files"]:
                if recover:
                    receipt = await self.services.file_puts.reconcile_input(link, captured, thaw(file), origins=None, authority=self.services.authority, current=lambda: channel._callback_current(link))
                else:
                    receipt = await channel._own_instance_file(link, captured, thaw(file))
                if receipt.state != "done":
                    from app.telegram_bots.file_outcomes import FileOutcomes

                    await FileOutcomes(self.services).plan(channel, link, captured, thaw(file))
                    return True
        staged = await self.cards.stage(link, captured, guard=guard)
        if staged.attempts == 0:
            await self.inputs.callbacks.acts.check(staged.id, expected={"id": staged.id, "kind": staged.kind, "body": staged.body, "attempts": staged.attempts}, attempt_current=lambda: channel._callback_current(link))
        else:
            await self.inputs.callbacks.acts.reconcile_receipt(staged.id)
        await self.inputs.callbacks._observe(link, staged.id, guard=guard)
        return True


class WorkFileWriteProjection:
    """The native writer rereads the same unsealed media input after providers."""

    def __init__(self, link, receipt):
        self.link, self.receipt = link, receipt

    def extend(self, query):
        from sqlalchemy import and_
        from sqlalchemy.orm import aliased

        mark = aliased(TelegramInboundMarkRow)
        return query.add_columns(mark).outerjoin(mark, and_(mark.link_id == self.link.id, mark.chat_id == self.link.chat_id, mark.message_id == self.receipt.message_id))

    def check(self, rows, *, allow_complete=False):
        mark = rows[0][-1]
        row, link = self.receipt, self.link
        work, context = row.continuation["work"], row.continuation["input"]
        if (
            mark is None
            or (not allow_complete and (mark.route_complete or mark.state == "done"))
            or (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id, mark.update_id, mark.route_kind, mark.route_target_id)
            != (link.id, link.chat_id, link.binding_generation, context["topic"], work["update_id"], "work_input", work["request_id"])
        ):
            raise CallbackRefused()
        data = mark.route_basis.get("work_input", {})
        from app.telegram_bots.inbox import chat_input_parts

        text, files = chat_input_parts(mark, bot_id=link.bot_id)
        if (
            work.get("input_sha256") != work_input_digest(mark)
            or type(data.get("version")) is not int
            or data["version"] != 4
            or data.get("source_outbox_id") != work["source_outbox_id"]
            or data.get("request_id") != work["request_id"]
            or mark.operation_ids != [work["operation_id"]]
            or not same_body(files, [thaw(row.continuation["file"])])
            or not same_body(data.get("files"), files)
            or bool(not text.strip()) is not context["captionless"]
        ):
            raise CallbackRefused()


async def prepare_file_effect(acts, link, act):
    if act.message_id is None:
        return None
    from app.telegram_bots.work_card_inputs import response

    async with acts.session_factory() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (link.id, act.message_id, link.chat_id))
        if mark is None or mark.route_kind != "work_input" or mark.route_basis.get("work_input", {}).get("version") != 4 or mark.route_basis.get("work_action_id") != act.id or mark.operation_ids != [act.id] or mark.route_complete:
            raise CallbackRefused()
        captured = InboxRow.from_row(mark)
        if not same_body(response(captured).model_dump(mode="json", exclude_unset=True), act.body):
            raise CallbackRefused()
        receipt = await read_file_response(link, captured, session=reader)
        if not same_body(freeze_file_answer(receipt), mark.route_basis["work_file"]):
            raise CallbackRefused()
    return captured, receipt

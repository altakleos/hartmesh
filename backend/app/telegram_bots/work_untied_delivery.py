"""Final dispatch proof for text and voice answer-consent presentations."""

from datetime import datetime

from sqlalchemy import JSON, and_, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _aware, _validate_rows, canonical_keyboard
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_source import _RequestProjection
from app.telegram_bots.work_typing import validate_shown_prompt
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def answer_data(mark):
    data = mark.route_basis.get("work_answer")
    fields = {"version", "source_outbox_id", "presentation_version", "group_id", "request_id", "work_id", "seen", "text", "notice_id"}
    if type(data) is not dict or type(data.get("version")) is not int or data["version"] not in {1, 2} or set(data) != fields | ({"original", "locale", "prompt_id", "purpose"} if data["version"] == 2 else set()):
        raise CallbackRefused()
    if type(data["presentation_version"]) is not int or data["presentation_version"] < 0 or type(data["seen"]) is not dict or type(data["text"]) is not str or not data["text"].strip():
        raise CallbackRefused()
    for key in ("source_outbox_id", "group_id", "request_id", "work_id", "notice_id"):
        value = data[key]
        if type(value) is not str or len(value) != 32 or any(char not in "0123456789abcdef" for char in value):
            raise CallbackRefused()
    if data["version"] == 2:
        if type(data["original"]) is not dict or data["original"].get("type") != "voice" or type(data["locale"]) is not str:
            raise CallbackRefused()
        if data["purpose"] not in {"answer", "work_comment", "cannot_provide", "wrong_recipient"} or data["prompt_id"] is not None and (type(data["prompt_id"]) is not str or len(data["prompt_id"]) != 32):
            raise CallbackRefused()
    return data


def answer_keyboard(data, message_id, *, cleared=False):
    voice = data["version"] == 2
    result = [
        list(line)
        for line in canonical_keyboard(
            [
                [
                    {
                        "kind": "voice_save_answer" if voice else "answer_yes",
                        "label": "label.save_answer",
                        "target_kind": "request",
                        "target_id": data["request_id"],
                        "payload": {"request_id": data["request_id"], "request_revision": data["seen"]["request_revision"], "assignment_revision": data["seen"]["assignment_revision"], "transcript" if voice else "text": data["text"]},
                    }
                ],
                [
                    {
                        "kind": "voice_discard" if voice else "answer_no",
                        "label": "label.discard" if voice else "label.new_message",
                        "target_kind": "request",
                        "target_id": data["request_id"],
                        "payload": {} if voice else {"text": data["text"], "message_id": message_id},
                    }
                ],
            ]
        )
    ]
    if cleared:
        for line in result:
            for control in line:
                control["payload"].pop("transcript", None)
    return result


def answer_parts(data, message_id):
    if data["version"] == 1:
        return [{"kind": "text", "source_ref": {"key": "ask.is_this_answer", "values": {}}, "reply_to_message_id": message_id}]
    return [
        {"kind": "text", "source_ref": {"key": "voice.confirm_answer", "values": {"part": index}}, "html": chunk, "visible_text": visible_text(chunk), "reply_to_message_id": message_id}
        for index, chunk in enumerate(split_html(render("voice.confirm_answer", data["locale"], text=data["text"])))
    ]


def answer_input(mark, data):
    raw = {"type": "text", "text": data["text"]}
    if data["version"] == 2:
        original = data["original"]
        if not same_body(mark.route_basis.get("voice_note"), {"version": 1, "original": original}):
            return False
        if "reply_to_message_id" in original:
            raw["reply_to_message_id"] = original["reply_to_message_id"]
    return same_body(mark.sanitized_input, raw)


def answer_source_group(parent, controls, data):
    if data.get("prompt_id") is None:
        _validate_rows(parent, controls)
        return
    if len(controls) != 1 or controls[0].id != data["prompt_id"] or controls[0].group_id != data["group_id"] or controls[0].payload.get("purpose") != data["purpose"]:
        raise CallbackRefused()
    validate_shown_prompt(_item(parent), controls[0])


def answer_source_input(parent, mark, data):
    quoted = data.get("original", {}).get("reply_to_message_id")
    parts = [part for part in parent.parts if part["presentation_version"] == data["presentation_version"]]
    if quoted is not None and (type(quoted) is not int or quoted not in [value for part in parts for value in part["message_ids"]]):
        raise CallbackRefused()
    for part in parts:
        stamp = datetime.fromisoformat(part["dispatched_at" if quoted is not None else "sent_at"])
        if stamp.utcoffset() is None or stamp > _aware(mark.accepted_at) or any(value >= mark.message_id for value in part["message_ids"]):
            raise CallbackRefused()


def notice_recipes(notice, data, message_id):
    parts = answer_parts(data, message_id)
    if len(parts) != len(notice.parts) or any("protect_content" in part and type(part["protect_content"]) is not bool for part in notice.parts):
        raise CallbackRefused()
    return [{**part, **({"stored_keyboard": True} if index == len(parts) - 1 else {}), **({"protect_content": notice.parts[index]["protect_content"]} if "protect_content" in notice.parts[index] else {})} for index, part in enumerate(parts)]


def same_input(mark, expected):
    return InboxRow.from_row(mark) == expected and all(same_body(getattr(mark, name), getattr(expected, name)) for name in ("route_basis", "sanitized_input", "operation_ids", "grouped_message_ids", "delivery_ids"))


def callback_facts(sessions, condition):
    callback = TelegramCallbackRow
    sqlite = sessions.kw["bind"].dialect.name == "sqlite"
    args = []
    for column in callback.__table__.columns:
        args.extend((column.name, func.json(column) if sqlite and isinstance(column.type, JSON) else column))
    aggregate = func.json_group_array(func.json_object(*args)) if sqlite else func.json_agg(func.json_build_object(*args))
    return select(aggregate).where(condition).scalar_subquery()


class WorkAnswerNotice:
    def __init__(self, host):
        self.host = host

    async def read_part(self, captured):
        async def perform():
            await self.authorize(captured, silent=False, now=self.host.plans.outbox.clock())
            if captured.part["source_ref"]["key"] == "voice.confirm_answer":
                return captured.part["html"], captured.part["visible_text"]
            settings = await self.host.services.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            return render("ask.is_this_answer", settings.locale), render_plain("ask.is_this_answer", settings.locale)

        return await await_drained(perform())

    async def authorize(self, captured, *, silent, now):
        async def perform():
            host, link = self.host, captured.link
            channel = host.services.channel(link.id)
            if channel is None or not channel._callback_current(link):
                raise DeliveryClaimRetired()
            origins = captured.item.payload.get("product_basis", {}).get("origins")
            if type(origins) is not list or len(origins) != 1 or len(origins[0]) != 3 or origins[0][0] != link.id or origins[0][2] != link.chat_id:
                raise ProductSourceRetired()
            async with host.plans.outbox.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, tuple(origins[0]))
                if mark is None or "work_answer" not in mark.route_basis:
                    raise ProductSourceRetired()
                origin = InboxRow.from_row(mark)
            admission = await prepare_attention(host.plans.reader, link)
            await admission(projection=_NoticeProjection(host, captured, origin))
            # No provider, session or codec await follows this lifetime fence.
            if not channel._callback_current(link) or not channel._send_allowed():
                raise DeliveryClaimRetired()

        return await await_drained(perform())


class _NoticeProjection:
    def __init__(self, host, captured, origin):
        self.host, self.captured, self.origin = host, captured, origin

    def extend(self, query):
        mark, parent, notice, callback = TelegramInboundMarkRow, aliased(TelegramOutboxRow), TelegramOutboxRow, TelegramCallbackRow
        source_id = self.origin.route_basis["work_answer"]["source_outbox_id"]
        groups = {part.get("callback_group_id") for part in self.captured.item.parts} - {None}
        groups.add(self.origin.route_basis["work_answer"]["group_id"])
        controls = callback_facts(self.host.plans.outbox.sessions, callback.group_id.in_(groups))
        return (
            query.add_columns(mark, parent, notice, controls)
            .join(mark, and_(mark.link_id == self.origin.link_id, mark.chat_id == self.origin.chat_id, mark.message_id == self.origin.message_id))
            .join(parent, parent.id == source_id)
            .join(notice, notice.id == self.captured.item.id)
        )

    def check(self, rows):
        host, captured, link = self.host, self.captured, self.captured.link
        binding, target, permissions, policy = rows[0][:4]
        mark, parent, notice, raw_controls = rows[0][8:12]
        latest = host.plans.outbox._sending(binding, notice, link_id=link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
        if latest != captured or not same_input(mark, self.origin):
            raise DeliveryClaimRetired()
        if target.custody == "company" and policy == "protected" and not captured.part.get("protect_content"):
            raise DeliveryClaimRetired()
        data = answer_data(mark)
        if mark.route_kind != "work_answer" or mark.route_complete or mark.operation_ids or data.get("notice_id") != notice.id or mark.message_thread_id != notice.message_thread_id or mark.message_thread_id != parent.message_thread_id:
            raise ProductSourceRetired()
        if (mark.link_id, mark.chat_id, mark.binding_generation) != (link.id, link.chat_id, link.binding_generation) or not answer_input(mark, data):
            raise ProductSourceRetired()
        basis = notice.payload.get("product_basis", {})
        recipes = answer_parts(data, mark.message_id)
        if not same_body(basis.get("parts"), recipes) or basis.get("finish_route") is not False or basis.get("origins") != [[mark.link_id, mark.message_id, mark.chat_id]]:
            raise ProductSourceRetired()
        from app.telegram_bots.work_outcome_delivery import acknowledged_parts

        acknowledged_parts(notice, notice_recipes(notice, data, mark.message_id), sending_part_id=captured.part["part_id"])
        source = acknowledged_source(link, parent)
        answer_source_input(parent, mark, data)
        if source.request["purpose"] != "information" and permissions & 6 != 6:
            raise ProductSourceRetired()
        if source.request["id"] != data["request_id"] or source.work["id"] != data["work_id"] or not same_body(source.seen, data["seen"]) or parent.payload["presentation_version"] != data["presentation_version"]:
            raise ProductSourceRetired()
        if not same_body(notice.payload.get("product_keyboard_basis"), answer_keyboard(data, mark.message_id)):
            raise ProductSourceRetired()
        all_controls = _RequestProjection.objects(raw_controls)
        original = [row for row in all_controls if row.group_id == data["group_id"]]
        controls = [row for row in all_controls if row.payload.get("anchor_outbox_id") == notice.id]
        answer_source_group(parent, original, data)
        _validate_rows(notice, controls, allow_pending=True)
        if len(controls) != 2 or {row.kind for row in controls} != ({"voice_save_answer", "voice_discard"} if data["version"] == 2 else {"answer_yes", "answer_no"}):
            raise ProductSourceRetired()
        for row in original + controls:
            if row.consumed_at is not None or "work_action_id" in row.payload or "publication_owner" in row.payload:
                raise ProductSourceRetired()
            # Pending presentation controls are qualified by this claim; only
            # the already shown question must pass the ordinary usable check.
            if row in original:
                host.callbacks._usable(row)

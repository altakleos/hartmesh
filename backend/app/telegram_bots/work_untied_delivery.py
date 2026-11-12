"""Final dispatch proof for the fixed untied-answer consent question."""

from sqlalchemy import JSON, and_, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows, canonical_keyboard
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_source import _RequestProjection
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def answer_data(mark):
    data = mark.route_basis.get("work_answer")
    if type(data) is not dict or set(data) != {"version", "source_outbox_id", "presentation_version", "group_id", "request_id", "work_id", "seen", "text", "notice_id"} or type(data["version"]) is not int or data["version"] != 1:
        raise CallbackRefused()
    if type(data["presentation_version"]) is not int or data["presentation_version"] < 0 or type(data["seen"]) is not dict or type(data["text"]) is not str or not data["text"].strip():
        raise CallbackRefused()
    for key in ("source_outbox_id", "group_id", "request_id", "work_id", "notice_id"):
        value = data[key]
        if type(value) is not str or len(value) != 32 or any(char not in "0123456789abcdef" for char in value):
            raise CallbackRefused()
    return data


def answer_keyboard(data, message_id):
    return [
        list(line)
        for line in canonical_keyboard(
            [
                [
                    {
                        "kind": "answer_yes",
                        "label": "label.save_answer",
                        "target_kind": "request",
                        "target_id": data["request_id"],
                        "payload": {"request_id": data["request_id"], "request_revision": data["seen"]["request_revision"], "assignment_revision": data["seen"]["assignment_revision"], "text": data["text"]},
                    }
                ],
                [{"kind": "answer_no", "label": "label.new_message", "target_kind": "request", "target_id": data["request_id"], "payload": {"text": data["text"], "message_id": message_id}}],
            ]
        )
    ]


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
        if (mark.link_id, mark.chat_id, mark.binding_generation) != (link.id, link.chat_id, link.binding_generation) or mark.sanitized_input != {"type": "text", "text": data["text"]}:
            raise ProductSourceRetired()
        basis = notice.payload.get("product_basis", {})
        recipe = {"kind": "text", "source_ref": {"key": "ask.is_this_answer", "values": {}}, "reply_to_message_id": mark.message_id}
        if basis.get("parts") != [recipe] or basis.get("finish_route") is not False or basis.get("origins") != [[mark.link_id, mark.message_id, mark.chat_id]]:
            raise ProductSourceRetired()
        source = acknowledged_source(link, parent)
        if source.request["purpose"] != "information" and permissions & 6 != 6:
            raise ProductSourceRetired()
        if source.request["id"] != data["request_id"] or source.work["id"] != data["work_id"] or not same_body(source.seen, data["seen"]) or parent.payload["presentation_version"] != data["presentation_version"]:
            raise ProductSourceRetired()
        if not same_body(notice.payload.get("product_keyboard_basis"), answer_keyboard(data, mark.message_id)):
            raise ProductSourceRetired()
        all_controls = _RequestProjection.objects(raw_controls)
        original = [row for row in all_controls if row.group_id == data["group_id"]]
        controls = [row for row in all_controls if row.payload.get("anchor_outbox_id") == notice.id]
        _validate_rows(parent, original)
        _validate_rows(notice, controls, allow_pending=True)
        if len(controls) != 2 or {row.kind for row in controls} != {"answer_yes", "answer_no"}:
            raise ProductSourceRetired()
        for row in original + controls:
            if row.consumed_at is not None or "work_action_id" in row.payload or "publication_owner" in row.payload:
                raise ProductSourceRetired()
            # Pending presentation controls are qualified by this claim; only
            # the already shown question must pass the ordinary usable check.
            if row in original:
                host.callbacks._usable(row)

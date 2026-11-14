"""Mixed Stop captures real chat ownership without manufacturing a turn."""

import asyncio
import weakref
from dataclasses import replace
from datetime import datetime, timedelta

from sqlalchemy import select

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY, InboxRow, _identity_record, publication_identity
from app.telegram_bots.producer import ProductSourceRetired, _item, validate_publication_outcome
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_management_edit_records import hex_identity, read_incoming
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def capture_answer(turns, original, link, origin):
    if original is None or original.channel is not turns.services.channel(link.id) or not turns._current(original) or original.topic != origin.message_thread_id:
        return None
    return {
        "version": 1,
        "origins": [origin_facts(row) for row in original.origins],
        "callback_id": original.callback_id,
        "publication": publication_identity(original.record) if original.record is not None else None,
    }


def read_answer(value, *, incoming):
    if value is None:
        return None
    if (
        type(value) is not dict
        or set(value) != {"version", "origins", "callback_id", "publication"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or type(value["origins"]) is not list
        or not 1 <= len(value["origins"]) <= 64
    ):
        raise ProductSourceRetired()
    if value["callback_id"] is not None and not hex_identity(value["callback_id"]):
        raise ProductSourceRetired()
    seen = set()
    for raw in value["origins"]:
        row = read_incoming(raw)
        key = row["link_id"], row["chat_id"], row["message_id"]
        if key in seen or any(row[name] != incoming[name] for name in ("link_id", "chat_id", "binding_generation", "message_thread_id")):
            raise ProductSourceRetired()
        seen.add(key)
    if value["publication"] is not None:
        try:
            record = _identity_record(value["publication"])
            if record.owner_callback_id != value["callback_id"] or {(row.link_id, row.chat_id, row.message_id) for row in record.origins} != seen:
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise ProductSourceRetired() from None
    return value


def selected_answer(basis):
    selection = basis["selection"]
    return basis["answer"] is not None and selection is not None and selection["choice"] in {"answer", "all"}


class AnswerCancelAdmission:
    """Durable admission and exact native-call entry use the original clock."""

    def __init__(self, answers, link, origin, basis, guard, original=None):
        self.answers, self.link, self.origin, self.basis, self.guard, self.original = answers, link, origin, basis, guard, original
        self.identity = digest(basis)
        self.stop_id = f"command:{origin.chat_id}:{origin.message_id}"
        self.entered_locally, self.expired_unentered = False, False

    def current(self):
        self.guard.current()
        if self.original is not None and not self.answers.owner.host.turns._current(self.original):
            raise ProductSourceRetired()

    async def _check(self, writer, *, allow_expired=False, require_no_call=False, prepublication=False, receipt_only=False):
        incoming, accepted_at, record = await self.answers.read(writer, self.link, self.basis, guard=self.guard)
        if self.original is not None:
            turns = self.answers.owner.host.turns
            if not turns._current(self.original) or (record is None) != (self.original.record is None) or record is not None and not same_body(publication_identity(record), publication_identity(self.original.record)):
                raise ProductSourceRetired()
        mark = (await self.answers.owner.outbox.inbox._rows(writer, [incoming]))[0]
        marker = await self.answers.stop_marker(writer, self.basis)
        self.current()
        if receipt_only:
            return mark, record, None
        intent = mark.route_basis.get("stop_answer_admission")
        entered = mark.route_basis.get("stop_answer_dispatch")
        publication = publication_identity(record) if record is not None else None
        if type(intent) is dict and intent.get("phase") == "ready" and intent.get("publication") is None and publication is not None and self.basis["answer"]["publication"] is None and entered is None:
            # read() proved this is the sole first publication of the actual
            # original input/callback; an automatic retry cannot enter here.
            intent = {**intent, "publication": publication}
            mark.route_basis = {**mark.route_basis, "stop_answer_admission": intent}

        def valid_time(value):
            return type(value) is str and datetime.fromisoformat(value).utcoffset() is not None and accepted_at <= datetime.fromisoformat(value) < accepted_at + timedelta(hours=1)

        if intent is not None and (
            type(intent) is not dict
            or set(intent) != {"version", "stop_id", "publication", "phase", "at"}
            or type(intent["version"]) is not int
            or intent["version"] != 1
            or intent["stop_id"] != self.stop_id
            or not same_body(intent["publication"], publication)
            or intent["phase"] not in {"ready", "entered"}
            or not valid_time(intent["at"])
        ):
            raise ProductSourceRetired()
        if entered is not None:
            if (
                intent is None
                or intent["phase"] != "entered"
                or type(entered) is not dict
                or set(entered) != {"version", "stop_id", "publication", "run_id", "at"}
                or type(entered["version"]) is not int
                or entered["version"] != 1
                or entered["stop_id"] != self.stop_id
                or record is None
                or not same_body(entered["publication"], publication)
                or record.run_id not in (None, entered["run_id"])
                or type(entered["run_id"]) is not str
                or not entered["run_id"]
                or not valid_time(entered["at"])
            ):
                raise ProductSourceRetired()
            self.entered_locally = True
        elif self.entered_locally or intent is not None and intent["phase"] == "entered" or intent is None and marker == self.stop_id:
            # Missing half of entered evidence cannot become a new no-call intent.
            raise ProductSourceRetired()
        elif not allow_expired and self.answers.owner.outbox.clock() >= accepted_at + timedelta(hours=1):
            self.expired_unentered = intent is not None
            raise ProductSourceRetired()
        if require_no_call and (entered is not None or intent is None or intent["phase"] != "ready") or prepublication and record is not None:
            raise ProductSourceRetired()
        self.current()
        return mark, record, entered

    async def _with_writer(self, session, **options):
        if session is not None:
            return await self._check(session, **options)
        async with self.answers.owner.callbacks.writer(self.link, self.guard) as writer:
            return await self._check(writer, **options)

    async def __call__(self, session=None):
        await self._with_writer(session)

    async def admit(self):
        async with self.answers.owner.callbacks.writer(self.link, self.guard) as writer:
            mark, record, _entered = await self._check(writer)
            if mark.route_basis.get("stop_answer_admission") is None:
                mark.route_basis = {
                    **mark.route_basis,
                    "stop_answer_admission": {"version": 1, "stop_id": self.stop_id, "publication": publication_identity(record) if record is not None else None, "phase": "ready", "at": self.answers.owner.outbox.clock().isoformat()},
                }
                await writer.flush()
            self.current()

    async def enter(self, run_id):
        async with self.answers.owner.callbacks.writer(self.link, self.guard) as writer:
            mark, record, entered = await self._check(writer)
            if record is None or record.run_id not in (None, run_id) or mark.route_basis.get("stop_answer_admission") is None:
                raise ProductSourceRetired()
            if entered is not None:
                if entered["run_id"] != run_id:
                    raise ProductSourceRetired()
                return
            mark.route_basis = {
                **mark.route_basis,
                "stop_answer_admission": {**mark.route_basis["stop_answer_admission"], "phase": "entered"},
                "stop_answer_dispatch": {"version": 1, "stop_id": self.stop_id, "publication": publication_identity(record), "run_id": run_id, "at": self.answers.owner.outbox.clock().isoformat()},
            }
            await writer.flush()
            self.current()
        self.entered_locally = True

    async def no_call(self, session=None):
        if not self.expired_unentered or self.entered_locally:
            raise ProductSourceRetired()
        await self._with_writer(session, allow_expired=True, require_no_call=True)

    async def prepublication(self, session=None):
        await self._with_writer(session, allow_expired=True, require_no_call=True, prepublication=True)

    async def receipt(self, session=None):
        # Read-only settlement never grants permission for a native call.
        await self._with_writer(session, receipt_only=True)


async def answer_receipt_delivery(reader, link, basis, mark):
    receipt = mark.route_basis.get("stop_answer_receipt")
    if receipt is None:
        return None
    if type(receipt) is not dict or set(receipt) != {"version", "kind", "delivery_id", "payload_signature", "publication"} or type(receipt["version"]) is not int or receipt["version"] != 1 or not hex_identity(receipt["delivery_id"]):
        raise ProductSourceRetired()
    row = await reader.get(TelegramOutboxRow, receipt["delivery_id"], populate_existing=True)
    if (
        row is None
        or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.message_thread_id, row.class_) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, basis["input"]["message_thread_id"], "reply")
        or row.instance_id not in (None, basis["instance_id"])
        or answer_signature(row.payload) != receipt["payload_signature"]
        or not row.parts
    ):
        raise ProductSourceRetired()
    expected = receipt["publication"]
    if basis["answer"]["publication"] is not None and not same_body(expected, basis["answer"]["publication"]):
        raise ProductSourceRetired()
    if receipt["kind"] == "notice":
        source = row.payload.get("command_stop_answer_notice")
        if (
            type(source) is not dict
            or set(source) != {"version", "input", "answer", "reason", "publication"}
            or source["version"] != 1
            or source["reason"] not in {"ended", "expired", "refused"}
            or not same_body(source["input"], basis["input"])
            or not same_body(source["answer"], basis["answer"])
            or not same_body(source["publication"], expected)
            or row.dedupe_key != "notice:command-stop-answer:" + digest(basis["input"])
        ):
            raise ProductSourceRetired()
        product = row.payload.get("product_basis", {})
        key = "stop.nothing" if source["reason"] == "ended" else "stop.failed"
        if product.get("origins") != [[mark.link_id, mark.message_id, mark.chat_id]] or product.get("finish_route") is not False or [part.get("source_ref") for part in product.get("parts", [])] != [{"key": key, "values": {}}]:
            raise ProductSourceRetired()
    elif receipt["kind"] != "chat":
        raise ProductSourceRetired()
    elif expected is not None:
        if "reply_basis" in row.payload:
            from app.telegram_bots.model_source import validate_model_reply_basis

            proof = await asyncio.to_thread(validate_model_reply_basis, _item(row))
        else:
            proof = await asyncio.to_thread(validate_publication_outcome, _item(row))
        if not same_body(publication_identity(proof.record), expected):
            raise ProductSourceRetired()
    else:
        product = row.payload.get("product_basis", {})
        origins = {(value["link_id"], value["message_id"], value["chat_id"]) for value in basis["answer"]["origins"]}
        callback_id = basis["answer"]["callback_id"]
        if (
            {tuple(value) for value in product.get("origins", [])} != (origins if callback_id is None else set())
            or product.get("finish_route") is not (callback_id is None)
            or row.payload.get("input_owner_callback_id") != callback_id
            or [part.get("source_ref", {}) for part in product.get("parts", [])] != [{"key": "busy.cancelled", "values": {}}]
        ):
            raise ProductSourceRetired()
    qualify_answer_delivery(row)
    return row


def answer_signature(payload):
    # Canonical source validators separately verify documented repair history.
    return digest({key: value for key, value in payload.items() if key not in {"presentation_version", "replacement_reason", "file_events"}})


def qualify_answer_delivery(row):
    """Stored recipes and complete SDK receipts determine historical closure."""
    from app.telegram_bots.producer import _PART_IDENTITY, _TRANSPORT_RECEIPTS

    source = row.payload.get("reply_basis", row.payload.get("product_basis", {}))
    recipes = source.get("parts")
    model = "reply_basis" in row.payload
    version = row.payload.get("presentation_version")
    active = [part for part in row.parts if part.get("state") != "retired"] if model else [part for part in row.parts if part.get("presentation_version") == version]
    if type(recipes) is not list or not recipes or not active or not model and len(recipes) != len(active):
        raise ProductSourceRetired()
    if "reply_basis" not in row.payload:
        if (source.get("chat_id"), source.get("message_thread_id")) != (row.chat_id, row.message_thread_id):
            raise ProductSourceRetired()
        if "product_keyboard_basis" in row.payload:
            recipes = [*recipes[:-1], {**recipes[-1], "stored_keyboard": True}]
    receipts = _PART_IDENTITY | _TRANSPORT_RECEIPTS | {"callback_group_id", "protect_content", "sent_protect_content"}
    identities, message_ids = set(), set()
    for index, part in enumerate(active):
        # The canonical model validator above owns repair/relocation history.
        recipe = part if model else recipes[index]
        if (
            not hex_identity(part.get("part_id"))
            or part["part_id"] in identities
            or type(part.get("ordinal")) is not int
            or (part["ordinal"] < 0 if model else part["ordinal"] != index)
            or type(version) is not int
            or not model
            and part.get("presentation_version") != version
            or not same_body({key: value for key, value in part.items() if key not in receipts}, {key: value for key, value in recipe.items() if key not in receipts})
            or any(type(part[key]) is not bool for key in ("protect_content", "sent_protect_content") if key in part)
            or recipe.get("protect_content") is True
            and part.get("protect_content") is not True
        ):
            raise ProductSourceRetired()
        identities.add(part["part_id"])
        if row.state != "sent":
            continue
        ids = part.get("message_ids")
        shown = model and part.get("shown_message_id") is not None
        protection = part.get("protect_content", False if shown else None)
        if (
            part.get("state") != "sent"
            or type(ids) is not list
            or len(ids) != (len(recipe["files"]) if recipe.get("kind") == "album" else 1)
            or any(type(identity) is not int or identity <= 0 or identity in message_ids for identity in ids)
            or type(protection) is not bool
            or shown
            and ids != [part["shown_message_id"]]
            or "sent_protect_content" in part
            and part["sent_protect_content"] != part["protect_content"]
            or type(part.get("sent_at")) is not str
            or datetime.fromisoformat(part["sent_at"]).utcoffset() is None
        ):
            raise ProductSourceRetired()
        message_ids.update(ids)


class CommandStopAnswers:
    def __init__(self, owner):
        self.owner, self.live = owner, weakref.WeakValueDictionary()

    async def recover_before_disposition(self, link, entry, *, guard):
        """An original selected by mixed Stop keeps its existing recovery owner."""
        from app.telegram_bots.work_command_stop_origins import command_basis

        after = -1
        expected = [origin_facts(row) for row in entry.origins]
        while True:
            async with self.owner.callbacks.writer(link, guard) as reader:
                rows = list(
                    await reader.scalars(
                        select(TelegramInboundMarkRow)
                        .where(
                            TelegramInboundMarkRow.link_id == link.id,
                            TelegramInboundMarkRow.chat_id == link.chat_id,
                            TelegramInboundMarkRow.binding_generation == link.binding_generation,
                            TelegramInboundMarkRow.route_kind == "command_stop",
                            TelegramInboundMarkRow.message_id > after,
                        )
                        .order_by(TelegramInboundMarkRow.message_id)
                        .limit(32)
                    )
                )
                captured = []
                for row in rows:
                    try:
                        basis = command_basis(row)
                    except (ProductSourceRetired, ValueError, TypeError, KeyError):
                        continue
                    answer = basis["answer"]
                    if selected_answer(basis) and answer["callback_id"] == entry.input_owner_callback_id:
                        if same_body(answer["origins"], expected):
                            captured.append(InboxRow.from_row(row))
                        elif {(value["link_id"], value["chat_id"], value["message_id"], value["binding_generation"], value["message_thread_id"]) for value in answer["origins"]} == {
                            (value["link_id"], value["chat_id"], value["message_id"], value["binding_generation"], value["message_thread_id"]) for value in expected
                        }:
                            # A damaged owned input must not become a generic retry.
                            return True
                if rows:
                    after = rows[-1].message_id
            if captured:
                for origin in captured:
                    await self.owner.recover(link, origin, guard=guard)
                return True
            if len(rows) < 32:
                return await self.historical_hold(link, entry, guard=guard)

    async def historical_hold(self, link, entry, *, guard):
        """A genuine shown choice can hold damaged custody, never execute it."""
        from types import SimpleNamespace

        from app.telegram_bots.work_news_delivery import frozen_news

        locators = {(row.link_id, row.chat_id, row.message_id, row.binding_generation, row.message_thread_id) for row in entry.origins}
        after = ""
        while True:
            async with self.owner.callbacks.writer(link, guard) as reader:
                rows = list(
                    await reader.scalars(
                        select(TelegramOutboxRow)
                        .where(
                            TelegramOutboxRow.link_id == link.id,
                            TelegramOutboxRow.owner_user_id == link.owner_user_id,
                            TelegramOutboxRow.binding_generation == link.binding_generation,
                            TelegramOutboxRow.chat_id == link.chat_id,
                            TelegramOutboxRow.message_thread_id == entry.origins[0].message_thread_id,
                            TelegramOutboxRow.instance_id == link.agent_ref,
                            TelegramOutboxRow.payload["command_stop_intent"].as_string().is_not(None),
                            TelegramOutboxRow.id > after,
                        )
                        .order_by(TelegramOutboxRow.id)
                        .limit(32)
                    )
                )
                for row in rows:
                    try:
                        _basis, source, _part = frozen_news(SimpleNamespace(link=link, item=_item(row), part=None), acknowledged=True)
                        answer = source.work["basis"]["answer"]
                        if (
                            answer is not None
                            and answer["callback_id"] == entry.input_owner_callback_id
                            and {(value["link_id"], value["chat_id"], value["message_id"], value["binding_generation"], value["message_thread_id"]) for value in answer["origins"]} == locators
                        ):
                            return True
                    except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
                        continue
                if rows:
                    after = rows[-1].id
            if len(rows) < 32:
                return False

    def remember(self, origin, basis, original):
        answer = basis["answer"]
        if (
            original is not None
            and answer is not None
            and same_body([origin_facts(row) for row in original.origins], answer["origins"])
            and original.callback_id == answer["callback_id"]
            and (answer["publication"] is None or original.record is not None and same_body(publication_identity(original.record), answer["publication"]))
        ):
            self.live[origin.link_id, origin.chat_id, origin.message_id] = original

    async def stop_marker(self, reader, basis):
        answer = basis["answer"]
        if answer["callback_id"] is not None:
            row = await reader.get(TelegramCallbackRow, answer["callback_id"], populate_existing=True)
            history = row.payload[CALLBACK_PUBLICATION_KEY].get("publications", [])
        else:
            origin = answer["origins"][0]
            row = await reader.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"]), populate_existing=True)
            history = row.route_basis.get("publications", [])
        return history[-1]["publication_execution"].get("stop_id") if history else None

    async def read(self, reader, link, basis, *, guard):
        from app.telegram_bots.work_command_stop_origins import read_command_choice

        incoming, accepted_at = await read_command_choice(reader, link, basis)
        answer = read_answer(basis["answer"], incoming=basis["input"])
        if answer is None or not selected_answer(basis):
            raise ProductSourceRetired()
        origins = []
        for expected in answer["origins"]:
            row = await reader.get(TelegramInboundMarkRow, (expected["link_id"], expected["message_id"], expected["chat_id"]), populate_existing=True)
            if row is None or not same_body(origin_facts(InboxRow.from_row(row)), expected):
                raise ProductSourceRetired()
            origins.append(row)
        if answer["callback_id"] is not None:
            stage = await self.owner.outbox.inbox.lock_callback_input(link, answer["callback_id"], session=reader)
            record = stage.record
            callback = await reader.get(TelegramCallbackRow, answer["callback_id"], populate_existing=True)
            history = callback.payload[CALLBACK_PUBLICATION_KEY].get("publications", [])
        elif origins[0].turn_id is not None:
            record = self.owner.outbox.inbox._publication_record(origins)
            history = origins[0].route_basis["publications"]
        else:
            record = None
            history = []
        if answer["publication"] is None and len(history) > 1:
            raise ProductSourceRetired()
        if answer["publication"] is not None and (record is None or not same_body(publication_identity(record), answer["publication"])):
            if record is None:
                raise ProductSourceRetired()
            expected = answer["publication"]
            previous = next((value for value in history if value["publication_id"] == expected["publication_id"] and value["event_id"] == expected["event_id"]), None)
            if previous is None:
                raise ProductSourceRetired()
            execution = previous["publication_execution"]
            if not same_body({key: execution[key] for key in ("scope", "origins", "input_version", "input_sha256")}, {key: expected[key] for key in ("scope", "origins", "input_version", "input_sha256")}):
                raise ProductSourceRetired()
            # Interpret this actual stored historical publication; no new
            # turn, origin, run or creating capability is manufactured.
            record = replace(_identity_record(expected), dispatch_started=execution["dispatch_started"], run_id=previous["run_id"])
        if record is not None:
            self.owner.outbox.inbox._publication_guard_matches(guard, link=link, scope=record.scope)
        return incoming, accepted_at, record

    async def run(self, link, origin, basis, *, guard):
        async with self.owner.callbacks.writer(link, guard) as writer:
            incoming, accepted_at, record = await self.read(writer, link, basis, guard=guard)
            mark = (await self.owner.outbox.inbox._rows(writer, [incoming]))[0]
            if mark.route_basis.get("stop_answer_receipt") is not None:
                from app.telegram_bots.work_command_stop_completion import complete_selected

                await complete_selected(self.owner.chat.executions, link, mark, writer, guard=guard)
                return
        turns = self.owner.host.turns
        original = self.live.get((origin.link_id, origin.chat_id, origin.message_id))
        if original is None:
            candidate = turns._turns.get((link.id, origin.message_thread_id))
            if candidate is not None and same_body([origin_facts(row) for row in candidate.origins], basis["answer"]["origins"]) and candidate.callback_id == basis["answer"]["callback_id"]:
                original = candidate
        if original is None or not turns._current(original):
            return await self.recover_original(link, origin, basis, record, accepted_at, guard=guard)
        if not same_body([origin_facts(row) for row in original.origins], basis["answer"]["origins"]) or original.callback_id != basis["answer"]["callback_id"]:
            raise ProductSourceRetired()
        if record is not None and (original.record is None or not same_body(publication_identity(original.record), publication_identity(record))):
            return await self.recover_original(link, origin, basis, record, accepted_at, guard=guard)
        if self.owner.outbox.clock() >= accepted_at + timedelta(hours=1) and not original.stop_requested:
            return await self.notice(link, origin, basis, record, reason="expired", guard=guard)

        admission = AnswerCancelAdmission(self, link, origin, basis, guard, original)
        if all(row.message_id != incoming.message_id for row in original.stop_commands):
            original.stop_commands.append(incoming)
        await turns._stop_turn(original, stop_id=f"command:{origin.chat_id}:{origin.message_id}", admission=admission)

    async def notice(self, link, origin, basis, record, *, reason, guard):
        from app.telegram_bots.work_command_stop_completion import complete_selected

        async with self.owner.callbacks.writer(link, guard) as writer:
            incoming, _accepted_at, fresh = await self.read(writer, link, basis, guard=guard)
            if (fresh is None) != (record is None) or record is not None and not same_body(publication_identity(fresh), publication_identity(record)):
                raise ProductSourceRetired()
            key = "stop.nothing" if reason == "ended" else "stop.failed"
            identity = await self.owner.outbox.plan_product(
                link,
                source_id="command-stop-answer:" + digest(basis["input"]),
                parts=[{"kind": "text", "source_ref": {"key": key, "values": {}}}],
                inbox_rows=[incoming],
                finish_route=False,
                session=writer,
            )
            row = await writer.get(TelegramOutboxRow, identity, populate_existing=True)
            publication = publication_identity(record) if record is not None else None
            source = {"version": 1, "input": basis["input"], "answer": basis["answer"], "reason": reason, "publication": publication}
            if row.payload.get("command_stop_answer_notice") is not None and not same_body(row.payload["command_stop_answer_notice"], source):
                raise ProductSourceRetired()
            row.payload = {**row.payload, "command_stop_answer_notice": source}
            mark = (await self.owner.outbox.inbox._rows(writer, [incoming]))[0]
            mark.route_basis = {**mark.route_basis, "stop_answer_receipt": {"version": 1, "kind": "notice", "delivery_id": row.id, "payload_signature": answer_signature(row.payload), "publication": publication}}
            await complete_selected(self.owner.chat.executions, link, mark, writer, guard=guard)

    async def recover_original(self, link, origin, basis, record, accepted_at, *, guard):
        answer = basis["answer"]
        async with self.owner.callbacks.writer(link, guard) as writer:
            captured, actual_at, fresh = await self.read(writer, link, basis, guard=guard)
            mark = (await self.owner.outbox.inbox._rows(writer, [captured]))[0]
            expired = self.owner.outbox.clock() >= actual_at + timedelta(hours=1) and mark.route_basis.get("stop_answer_dispatch") is None
            admitted = mark.route_basis.get("stop_answer_admission")
            marker = await self.stop_marker(writer, basis)
            if answer["callback_id"] is not None:
                stage = await self.owner.outbox.inbox.lock_callback_input(link, answer["callback_id"], session=writer)
                closed, rows = stage.route_complete, stage.origins
                current_publication = stage.record.publication_id if stage.record is not None else None
            else:
                actual = [await writer.get(TelegramInboundMarkRow, (value["link_id"], value["message_id"], value["chat_id"]), populate_existing=True) for value in answer["origins"]]
                closed, rows = all(row.route_complete for row in actual), tuple(InboxRow.from_row(row) for row in actual)
                current_publication = actual[0].route_basis.get("current_publication_id")
        historical = fresh is not None and current_publication != fresh.publication_id
        if historical:
            from app.telegram_bots.work_command_stop_cold import observe_historical

            return await observe_historical(self, link, origin, basis, fresh, guard=guard)
        if closed:
            return await self.notice(link, origin, basis, fresh, reason="ended", guard=guard)
        if fresh is None:
            from app.telegram_bots.turns import _StopAdmissionGuard

            admission = AnswerCancelAdmission(self, link, origin, basis, guard)
            if expired and admitted is None:
                return await self.notice(link, origin, basis, fresh, reason="expired", guard=guard)
            if admitted is None:
                await admission.admit()
            closure_guard = _StopAdmissionGuard(guard, admission.prepublication)
            # No live driver and a positively unreserved original input: the
            # ordinary input writer seals cancellation before any future reserve.
            await self.owner.outbox.plan_input_outcome(
                link,
                source_id=f"chat-input:{link.chat_id}:{link.binding_generation}:" + (answer["callback_id"] or str(rows[0].message_id)),
                source_ref={"key": "busy.cancelled", "values": {}},
                keyboard=(),
                callbacks=self.owner.callbacks,
                guard=closure_guard,
                inbox_rows=() if answer["callback_id"] else rows,
                input_owner_callback_id=answer["callback_id"],
            )
            return await self.complete_receipt(link, origin, guard=guard)
        if expired and marker is None:
            return await self.notice(link, origin, basis, fresh, reason="expired", guard=guard)
        from app.telegram_bots.work_command_stop_cold import recover_published

        return await recover_published(self, link, origin, basis, fresh, guard=guard)

    async def completed(self, turn, origin, *, guard):
        return await self.complete_receipt(turn.link, origin, guard=guard)

    async def refused(self, turn, origin, *, guard, reason="refused"):
        from app.telegram_bots.work_command_stop_origins import command_basis

        async with self.owner.callbacks.writer(turn.link, guard) as writer:
            mark = (await self.owner.outbox.inbox._rows(writer, [origin]))[0]
            basis = command_basis(mark)
        return await self.notice(turn.link, origin, basis, turn.record, reason=reason, guard=guard)

    async def complete_receipt(self, link, origin, *, guard):
        from app.telegram_bots.work_command_stop_completion import complete_selected
        from app.telegram_bots.work_command_stop_origins import command_basis

        async with self.owner.callbacks.writer(link, guard) as writer:
            mark = (await self.owner.outbox.inbox._rows(writer, [origin]))[0]
            basis = command_basis(mark)
            incoming, _accepted_at, record = await self.read(writer, link, basis, guard=guard)
            if basis["answer"]["callback_id"] is not None:
                callback = await writer.get(TelegramCallbackRow, basis["answer"]["callback_id"], populate_existing=True)
                data = callback.payload[CALLBACK_PUBLICATION_KEY]
                identity = data.get("outbox_id")
                if record is None:
                    key = f"notice:chat-input:{link.chat_id}:{link.binding_generation}:" + callback.id
                    identity = await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
                    if identity is None or identity not in data["delivery_ids"]:
                        raise ProductSourceRetired()
            else:
                rows = [await writer.get(TelegramInboundMarkRow, (value["link_id"], value["message_id"], value["chat_id"]), populate_existing=True) for value in basis["answer"]["origins"]]
                identities = {row.outbox_id for row in rows}
                if len(identities) != 1:
                    raise ProductSourceRetired()
                identity = identities.pop()
                if record is None:
                    key = f"notice:chat-input:{link.chat_id}:{link.binding_generation}:" + str(basis["answer"]["origins"][0]["message_id"])
                    identity = await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
                    if identity is None or any(identity not in row.delivery_ids for row in rows):
                        raise ProductSourceRetired()
            row = await writer.get(TelegramOutboxRow, identity, populate_existing=True) if identity is not None else None
            if row is None:
                raise ProductSourceRetired()
            receipt = {"version": 1, "kind": "chat", "delivery_id": row.id, "payload_signature": answer_signature(row.payload), "publication": publication_identity(record) if record is not None else None}
            saved = mark.route_basis.get("stop_answer_receipt")
            if saved is not None and not same_body(saved, receipt):
                raise ProductSourceRetired()
            mark.route_basis = {**mark.route_basis, "stop_answer_receipt": receipt}
            await answer_receipt_delivery(writer, link, basis, mark)
            await self.owner.outbox.inbox.register_delivery([incoming], outbox_id=row.id, primary=False, finish=False, session=writer)
            await complete_selected(self.owner.chat.executions, link, mark, writer, guard=guard)

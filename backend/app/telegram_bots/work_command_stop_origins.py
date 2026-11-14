"""Typed Stop journals retain the real command and its selected original targets."""

from collections.abc import Mapping
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import String, and_, cast, false, select
from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.callbacks import _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_origins import anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def operation_id(incoming, target):
    return digest(["command-stop", incoming, target])[:32]


def selected_targets(basis):
    selection = basis["selection"]
    if selection is None:
        return []
    choice = selection["choice"]
    if choice == "answer" and basis["answer"] is not None:
        return []
    if choice == "all":
        return basis["targets"]
    targets = [target for target in basis["targets"] if choice == "attempt:" + target["attempt_id"]]
    if not targets:
        raise ProductSourceRetired()
    return targets


def command_basis(mark):
    basis = thaw(mark.route_basis.get("stop_command"))
    if (
        type(basis) is not dict
        or set(basis) != {"version", "input", "owner_user_id", "instance_id", "answer", "targets", "selection"}
        or type(basis["version"]) is not int
        or basis["version"] != 1
        or type(basis["owner_user_id"]) is not str
        or not basis["owner_user_id"]
        or type(basis["instance_id"]) is not str
        or len(basis["instance_id"]) != 32
        or any(ch not in "0123456789abcdef" for ch in basis["instance_id"])
        or mark.route_kind != "command_stop"
        or mark.state == "interrupted"
        or not same_body(basis["input"], origin_facts(InboxRow.from_row(mark) if not isinstance(mark, InboxRow) else mark))
        or mark.sanitized_input != {"command": "stop"}
        or mark.route_target_id != digest(["command-stop", basis["input"]])[:32]
        or type(basis["targets"]) is not list
        or not basis["targets"]
    ):
        raise ProductSourceRetired()
    targets = basis["targets"]
    from app.telegram_bots.work_command_stop_answers import read_answer

    read_answer(basis["answer"], incoming=basis["input"])
    for target in targets:
        if type(target) is not dict or set(target) != {"work_id", "attempt_id"} or any(type(value) is not str or len(value) != 32 or any(ch not in "0123456789abcdef" for ch in value) for value in target.values()):
            raise ProductSourceRetired()
    if len({target["work_id"] for target in targets}) != len(targets) or len({target["attempt_id"] for target in targets}) != len(targets):
        raise ProductSourceRetired()
    selection = basis["selection"]
    if selection is None:
        if mark.operation_ids:
            raise ProductSourceRetired()
        return basis
    if type(selection) is not dict or selection.get("kind") not in {"command", "callback"} or set(selection) != {"kind", "choice"} | ({"origin"} if selection["kind"] == "callback" else set()):
        raise ProductSourceRetired()
    if selection["kind"] == "command" and (basis["answer"] is not None or len(targets) != 1 or selection["choice"] != "attempt:" + targets[0]["attempt_id"]):
        raise ProductSourceRetired()
    if list(mark.operation_ids) != [operation_id(basis["input"], target) for target in selected_targets(basis)]:
        raise ProductSourceRetired()
    return basis


def source_marker(act):
    continuation = act.continuation
    marker = thaw(continuation.get("command_stop_source")) if isinstance(continuation, Mapping) else None
    if marker is not None and (type(marker) is not dict or set(marker) != {"version", "basis", "target"} or type(marker["version"]) is not int or marker["version"] != 1 or act.kind != "stop"):
        raise ProductSourceRetired()
    return marker


def require_command_choice(link, basis, mark, callback=None, parent=None, controls=()):
    if (
        not same_body(command_basis(mark), basis)
        or basis["selection"] is None
        or (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id, basis["owner_user_id"], basis["instance_id"])
        != (link.id, link.chat_id, link.binding_generation, basis["input"]["message_thread_id"], link.owner_user_id, link.agent_ref)
    ):
        raise ProductSourceRetired()
    selection = basis["selection"]
    if selection["kind"] == "callback":
        from app.telegram_bots.work_news_delivery import frozen_news

        origin = selection["origin"]
        if callback is None or parent is None or set(origin) != {"callback_id", "anchor_outbox_id", "presentation_version", "signature"}:
            raise ProductSourceRetired()
        item = _item(parent)
        _frozen, source, _part = frozen_news(SimpleNamespace(link=link, item=item, part=None), acknowledged=True)
        _validate_rows(item, list(controls))
        if (
            source.key != "command.stop"
            or not same_body(source.work["basis"], {**basis, "selection": None})
            or (callback.id, parent.id, parent.payload["presentation_version"], anchor_signature(item)) != (origin["callback_id"], origin["anchor_outbox_id"], origin["presentation_version"], origin["signature"])
            or (callback.kind, callback.target_kind, callback.target_id, callback.payload.get("choice")) != ("stop_which", "link", link.id, selection["choice"])
            or callback.id not in {value.id for value in controls}
            or any(value.consumed_at is None or value.closed_reason != "stop-chosen" for value in controls)
            or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.message_thread_id, parent.instance_id)
            != (link.id, link.owner_user_id, link.chat_id, link.binding_generation, basis["input"]["message_thread_id"], basis["instance_id"])
        ):
            raise ProductSourceRetired()
        return aware(callback.consumed_at)
    else:
        return aware(mark.accepted_at)


async def read_command_choice(reader, link, basis):
    incoming = basis["input"]
    mark = await reader.get(TelegramInboundMarkRow, (incoming["link_id"], incoming["message_id"], incoming["chat_id"]), populate_existing=True)
    if mark is None:
        raise ProductSourceRetired()
    origin = basis["selection"].get("origin")
    callback = await reader.get(TelegramCallbackRow, origin["callback_id"], populate_existing=True) if origin is not None else None
    parent = await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"], populate_existing=True) if origin is not None else None
    controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id).execution_options(populate_existing=True))) if callback is not None else []
    accepted_at = require_command_choice(link, basis, mark, callback, parent, controls)
    return InboxRow.from_row(mark), accepted_at


class CommandStopProof:
    def __init__(self, link, act):
        self.link, self.act, self.marker = link, act, source_marker(act)
        if self.marker is None:
            raise ProductSourceRetired()
        self.incoming = self.marker["basis"]["input"]
        self.topic = self.incoming["message_thread_id"]
        self.origin = {"version": 1, "kind": "command_stop", "input": self.incoming, "signature": digest(self.marker)}

    def require(self, mark, callback=None, parent=None, controls=()):
        basis, target = command_basis(mark), self.marker["target"]
        if (
            not same_body(basis, self.marker["basis"])
            or target not in selected_targets(basis)
            or self.act.id != operation_id(basis["input"], target)
            or (self.act.work_id, self.act.body.get("attempt_id")) != (target["work_id"], target["attempt_id"])
            or (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id) != (self.link.id, self.link.chat_id, self.link.binding_generation, self.topic)
            or basis["selection"] is None
            or basis["instance_id"] != self.act.instance_id
            or basis["owner_user_id"] != self.act.owner_user_id
        ):
            raise ProductSourceRetired()
        self.accepted_at = require_command_choice(self.link, basis, mark, callback, parent, controls)

    async def read(self, reader):
        incoming = self.incoming
        mark = await reader.get(TelegramInboundMarkRow, (incoming["link_id"], incoming["message_id"], incoming["chat_id"]), populate_existing=True)
        if mark is None:
            raise ProductSourceRetired()
        origin = self.marker["basis"]["selection"].get("origin")
        callback = await reader.get(TelegramCallbackRow, origin["callback_id"], populate_existing=True) if origin is not None else None
        parent = await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"], populate_existing=True) if origin is not None else None
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id).execution_options(populate_existing=True))) if callback is not None else []
        self.require(mark, callback, parent, controls)

        return InboxRow.from_row(mark)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        mark, callback, parent, control = aliased(TelegramInboundMarkRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        origin = self.marker["basis"]["selection"].get("origin")
        return (
            query.add_columns(mark, callback, parent, control)
            .outerjoin(mark, and_(mark.link_id == self.incoming["link_id"], mark.chat_id == self.incoming["chat_id"], mark.message_id == self.incoming["message_id"]))
            .outerjoin(callback, callback.id == origin["callback_id"] if origin is not None else false())
            .outerjoin(parent, parent.id == origin["anchor_outbox_id"] if origin is not None else false())
            .outerjoin(control, control.group_id == callback.group_id)
        )

    def check(self, rows):
        mark, callback, parent, _control = rows[0][self.column : self.column + 4]
        if mark is None:
            raise ProductSourceRetired()
        controls = list({row[self.column + 3].id: row[self.column + 3] for row in rows if row[self.column + 3] is not None}.values())
        self.require(mark, callback, parent, controls)


class CommandStopModeProof:
    """Independent reverse ownership prevents a Stop from losing its source mode."""

    def __init__(self, link, root, *, clock):
        self.link, self.root, self.marker = link, root, source_marker(root)
        self.clock = clock
        self.proof = CommandStopProof(link, root) if self.marker is not None else None

    def extend(self, query):
        self.column = len(query.column_descriptions)
        mark, delivery = aliased(TelegramInboundMarkRow), aliased(TelegramOutboxRow)
        # Operation IDs are typed string-array members. Exact JSON quotes avoid
        # matching another identifier; the joined input is re-proved below.
        result = (
            query.add_columns(mark, delivery)
            .outerjoin(
                mark,
                and_(
                    mark.link_id == self.root.link_id,
                    mark.chat_id == self.root.chat_id,
                    mark.binding_generation == self.root.binding_generation,
                    mark.route_kind == "command_stop",
                    cast(mark.operation_ids, String).contains('"' + self.root.id + '"', autoescape=True),
                ),
            )
            .outerjoin(delivery, and_(delivery.payload["work_execution_intent"]["act_id"].as_string() == self.root.id, delivery.payload["work_execution_intent"]["origin"]["kind"].as_string() == "command_stop"))
        )
        return self.proof.extend(result) if self.proof is not None else result

    def check(self, rows, saved):
        if not same_body(source_marker(saved), self.marker):
            raise ProductSourceRetired()
        inputs = {(row[self.column].link_id, row[self.column].message_id, row[self.column].chat_id) for row in rows if row[self.column] is not None}
        notices = {row[self.column + 1].id: row[self.column + 1] for row in rows if row[self.column + 1] is not None}
        if self.proof is None:
            if inputs or notices:
                raise ProductSourceRetired()
            return
        incoming = self.proof.incoming
        if inputs != {(incoming["link_id"], incoming["message_id"], incoming["chat_id"])}:
            raise ProductSourceRetired()
        self.proof.check(rows)
        if saved.state != "done" and not saved.operation_ids and self.clock() >= self.proof.accepted_at + timedelta(hours=1):
            raise ProductSourceRetired()
        for row in notices.values():
            if (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.message_thread_id, row.instance_id, row.work_id, row.dedupe_key) != (
                self.root.link_id,
                self.root.owner_user_id,
                self.root.binding_generation,
                self.root.chat_id,
                self.proof.topic,
                self.root.instance_id,
                self.root.work_id,
                "notice:work-execution:" + self.root.id,
            ) or not same_body(row.payload["work_execution_intent"]["origin"], self.proof.origin):
                raise ProductSourceRetired()


async def stop_mode_proof(acts, link, act):
    from app.telegram_bots.work_stop_retries import parent_of

    parent = parent_of(act)
    root = await acts._work_record(parent["parent_id"]) if parent is not None else act
    return CommandStopModeProof(link, root, clock=acts.outbox.clock)

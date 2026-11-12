"""Immutable actual-member proof shared by a Work album's single response owner."""

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import JSON, and_, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow, chat_input_parts
from app.telegram_bots.work_actions import same_body, thaw
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

_IMMUTABLE = ("link_id", "chat_id", "binding_generation", "message_thread_id", "message_id", "update_id", "sanitized_input")
_PUBLICATION_FIELDS = ("turn_id", "thread_id", "run_id", "outbox_id", "assistant_message_id")
_CAPSULE = {"version", "album_id", "root_message_id", "member_ids", "inputs_sha256", "reply_sha256", "request_id", "source_outbox_id", "operation_id"}


def input_packet(mark):
    return {**{name: deepcopy(getattr(mark, name)) for name in _IMMUTABLE}, "input_profile": deepcopy(mark.route_basis["input_profile"])}


def inputs_digest(packets):
    return hashlib.sha256(json.dumps(packets, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def album_context(mark):
    context = mark.route_basis.get("work_album")
    if type(context) is not dict or set(context) != _CAPSULE or type(context["version"]) is not int or context["version"] != 1:
        raise CallbackRefused()
    ids = context["member_ids"]
    if type(ids) is not list or not 1 <= len(ids) <= 64 or any(type(value) is not int or value <= 0 for value in ids) or len(set(ids)) != len(ids):
        raise CallbackRefused()
    if type(context["root_message_id"]) is not int or context["root_message_id"] not in ids or mark.message_id not in ids:
        raise CallbackRefused()
    if type(context["album_id"]) is not str or not context["album_id"] or context["album_id"] != mark.sanitized_input.get("media_group_id"):
        raise CallbackRefused()
    if any(type(context[key]) is not str or re.fullmatch(r"[0-9a-f]{32}", context[key]) is None for key in ("request_id", "source_outbox_id", "operation_id")):
        raise CallbackRefused()
    if any(type(context[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", context[key]) is None for key in ("inputs_sha256", "reply_sha256")):
        raise CallbackRefused()
    return context


def member_packet(mark):
    return {
        **input_packet(mark),
        "context": deepcopy(mark.route_basis.get("work_album")),
        "work_input": deepcopy(mark.route_basis.get("work_input")),
        "route_kind": mark.route_kind,
        "route_target_id": mark.route_target_id,
        "operation_ids": deepcopy(mark.operation_ids),
        "route_complete": mark.route_complete,
        "state": mark.state,
        "work_action_id": mark.route_basis.get("work_action_id"),
        **{name: getattr(mark, name) for name in _PUBLICATION_FIELDS},
    }


async def outcome_members(session, root):
    context = album_context(root)
    row = TelegramInboundMarkRow
    members = list(await session.scalars(select(row).where(row.link_id == root.link_id, row.chat_id == root.chat_id, row.message_id.in_(context["member_ids"])).with_for_update().execution_options(populate_existing=True)))
    AlbumInputProof(session.get_bind().dialect.name == "sqlite", InboxRow.from_row(root), complete=True).check([member_packet(mark) for mark in members])
    return members


@dataclass(frozen=True)
class AlbumInputProof:
    sqlite: bool
    root: InboxRow
    complete: bool = False

    def expression(self):
        row = aliased(TelegramInboundMarkRow)
        context = album_context(self.root)
        args = []
        for name in _IMMUTABLE:
            column = getattr(row, name)
            args.extend((name, func.json(column) if self.sqlite and isinstance(column.type, JSON) else column))
        for name, column in (
            ("input_profile", row.route_basis["input_profile"]),
            ("context", row.route_basis["work_album"]),
            ("work_input", row.route_basis["work_input"]),
            ("route_kind", row.route_kind),
            ("route_target_id", row.route_target_id),
            ("operation_ids", row.operation_ids),
            ("route_complete", row.route_complete),
            ("state", row.state),
            ("work_action_id", row.route_basis["work_action_id"].as_string()),
            *((name, getattr(row, name)) for name in _PUBLICATION_FIELDS),
        ):
            args.extend((name, func.json(column) if self.sqlite and isinstance(column.type, JSON) else column))
        packet = func.json_object(*args) if self.sqlite else func.json_build_object(*args)
        grouped = func.json_group_array(packet, type_=JSON) if self.sqlite else func.json_agg(packet, type_=JSON)
        scope = and_(
            row.link_id == self.root.link_id,
            row.chat_id == self.root.chat_id,
            row.binding_generation == self.root.binding_generation,
            row.message_thread_id == self.root.message_thread_id,
            row.sanitized_input["media_group_id"].as_string() == context["album_id"],
        )
        if self.complete:
            scope = and_(scope, row.message_id.in_(context["member_ids"]))
        return select(grouped).where(scope).scalar_subquery().label("work_album_inputs")

    def check(self, packets):
        context = album_context(self.root)
        if type(packets) is not list or len(packets) != len(context["member_ids"]):
            raise CallbackRefused()
        indexed = {packet["message_id"]: packet for packet in packets}
        if len(indexed) != len(packets) or set(indexed) != set(context["member_ids"]):
            raise CallbackRefused()
        ordered = [indexed[identity] for identity in context["member_ids"]]
        originals = [{**{name: packet[name] for name in _IMMUTABLE}, "input_profile": packet["input_profile"]} for packet in ordered]
        if inputs_digest(originals) != context["inputs_sha256"]:
            raise CallbackRefused()
        for packet in ordered:
            root = packet["message_id"] == context["root_message_id"]
            if any(packet[name] is not None for name in _PUBLICATION_FIELDS) or packet["work_action_id"] not in ({None, context["operation_id"]} if root else {None}):
                raise CallbackRefused()
            if root and inputs_digest(packet["work_input"]) != context["reply_sha256"] or not root and packet["work_input"] is not None:
                raise CallbackRefused()
            if (not self.complete and (packet["route_complete"] or packet["state"] != "reply_pending")) or not same_body(packet["context"], context):
                raise CallbackRefused()
            if self.complete and packet["route_complete"] != self.root.route_complete:
                raise CallbackRefused()
            if packet["state"] not in ({"reply_pending", "done"} if self.complete and self.root.route_complete else {"reply_pending"}):
                raise CallbackRefused()
            if packet["route_kind"] != ("work_input" if root else "work_album_member") or packet["route_target_id"] != context["request_id"] or packet["operation_ids"] != ([context["operation_id"]] if root else []):
                raise CallbackRefused()
        return ordered


async def captured_members(cards, link, writer, root):
    """Lock the complete ready cohort; no new members or prior cohort can join it."""
    from app.telegram_bots.work_file_albums import album_manifest

    manifest = album_manifest(root, link)
    if manifest["state"] != "ready" or manifest["root_message_id"] != root.message_id:
        raise CallbackRefused()
    row = TelegramInboundMarkRow
    members = list(
        await writer.scalars(
            select(row)
            .where(
                row.link_id == link.id,
                row.chat_id == link.chat_id,
                row.binding_generation == link.binding_generation,
                row.message_thread_id == root.message_thread_id,
                row.sanitized_input["media_group_id"].as_string() == manifest["id"],
            )
            .order_by(row.update_id, row.message_id)
            .limit(65)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if [member.message_id for member in members] != manifest["message_ids"]:
        raise CallbackRefused()
    for member in members:
        cards.inbox._current(link, member)
        if (
            not same_body(album_manifest(member, link), manifest)
            or member.route_complete
            or member.state != "reply_pending"
            or member.operation_ids
            or any(getattr(member, key) is not None for key in ("turn_id", "thread_id", "run_id", "outbox_id"))
        ):
            raise CallbackRefused()
        _text, files = chat_input_parts(member, bot_id=link.bot_id)
        if len(files) != 1 or files[0]["message_id"] != member.message_id:
            raise CallbackRefused()
    return tuple(InboxRow.from_row(member) for member in members), members


def capture_context(members, root, data, operation):
    # Bound the exact visible captions plus worst-case saved path names before
    # any native write. Native conflict renames use bounded generated names.
    from app.telegram_bots.work_input_capture import _TEXT
    from deerflow.spaces.service import MAX_NAME_SUFFIX

    pieces = []
    for member in members:
        text, files = chat_input_parts(member)
        name = max((files[0]["filename"], "telegram-" + str(member.message_id) + "." + "x" * 16), key=len)
        pieces.append((text + "\n\n" if text.strip() else "") + "uploads/" + name + f" ({MAX_NAME_SUFFIX})")
    _TEXT.validate_python("\n\n".join(pieces))
    return {
        "version": 1,
        "album_id": root.sanitized_input["media_group_id"],
        "root_message_id": root.message_id,
        "member_ids": [member.message_id for member in members],
        "inputs_sha256": inputs_digest([input_packet(member) for member in members]),
        "reply_sha256": inputs_digest(data),
        "request_id": data["request_id"],
        "source_outbox_id": data["source_outbox_id"],
        "operation_id": operation,
    }


@dataclass(frozen=True)
class AlbumJournalScope:
    """Frozen native-journal selector; it grants no Work or file authority."""

    link_id: str
    chat_id: int
    binding_generation: int
    message_thread_id: int | None
    message_id: int
    sanitized_input: dict
    route_basis: dict

    @classmethod
    def from_receipt(cls, receipt):
        capsule = thaw(receipt.continuation["work"]["album"])
        return cls(receipt.link_id, receipt.chat_id, receipt.binding_generation, receipt.continuation["input"]["topic"], capsule["root_message_id"], {"media_group_id": capsule["album_id"]}, {"work_album": capsule})


def album_input_digest(mark):
    return inputs_digest({"input": input_packet(mark), "album": album_context(mark)})


class AlbumFileWriteProjection:
    def __init__(self, link, receipt, *, sqlite):
        self.link, self.receipt = link, receipt
        self.proof = AlbumInputProof(sqlite, AlbumJournalScope.from_receipt(receipt))

    def extend(self, query):
        row = aliased(TelegramInboundMarkRow)
        return query.add_columns(row, self.proof.expression()).outerjoin(row, and_(row.link_id == self.link.id, row.chat_id == self.link.chat_id, row.message_id == self.receipt.message_id))

    def check(self, rows):
        mark, packet = rows[0][-2:]
        receipt = self.receipt
        context = receipt.continuation["work"]
        self.proof.check(packet)
        if mark is None or album_input_digest(mark) != context["input_sha256"] or mark.update_id != context["update_id"]:
            raise CallbackRefused()
        text, files = chat_input_parts(mark, bot_id=self.link.bot_id)
        if not same_body(files, [thaw(receipt.continuation["file"])]) or bool(not text.strip()) is not receipt.continuation["input"]["captionless"]:
            raise CallbackRefused()

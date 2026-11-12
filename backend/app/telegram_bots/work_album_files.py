"""Complete native receipt bundle for one actual Work album response."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace

from sqlalchemy import JSON, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import ActRow
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.file_puts import _file_journal, file_act_id
from app.telegram_bots.inbox import InboxRow, chat_input_parts, validate_chat_file
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_album_inputs import AlbumInputProof, album_context, album_input_digest, input_packet, inputs_digest
from app.telegram_bots.work_input_capture import _TEXT
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow

_FILE_FIELDS = tuple(ActRow.__dataclass_fields__)
_DATES = ("first_attempt_at", "cancel_requested_at", "created_at", "updated_at")


def normalized_packet(packet):
    if type(packet) is not dict or set(packet) != set(_FILE_FIELDS):
        raise CallbackRefused()
    packet = deepcopy(packet)
    for name in _DATES:
        value = packet[name]
        if value is not None:
            if isinstance(value, str):
                value = datetime.fromisoformat(value)
            if not isinstance(value, datetime):
                raise CallbackRefused()
            packet[name] = (value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)).isoformat()
    return packet


def file_packet(row):
    return normalized_packet({name: deepcopy(getattr(row, name)) for name in _FILE_FIELDS})


def bundle_text(inputs, files):
    pieces = []
    for packet, file in zip(inputs, files, strict=True):
        caption = packet["sanitized_input"]["text"]
        pieces.append((caption + "\n\n" if caption.strip() else "") + file["result"]["path"])
    return _TEXT.validate_python("\n\n".join(pieces))


@dataclass(frozen=True)
class AlbumFileBundle:
    root: InboxRow
    sqlite: bool
    inputs: tuple
    files: tuple
    packet: dict
    scope: dict

    def expression(self):
        row = aliased(TelegramActRow)
        args = []
        for name in _FILE_FIELDS:
            column = getattr(row, name)
            args.extend((name, func.json(column) if self.sqlite and isinstance(column.type, JSON) else column))
        value = func.json_object(*args) if self.sqlite else func.json_build_object(*args)
        aggregate = func.json_group_array(value, type_=JSON) if self.sqlite else func.json_agg(value, type_=JSON)
        files = select(aggregate).where(row.id.in_([file["id"] for file in self.files])).scalar_subquery()
        inputs = AlbumInputProof(self.sqlite, self.root).expression()
        return (func.json_object("inputs", func.json(inputs), "files", func.json(files), type_=JSON) if self.sqlite else func.json_build_object("inputs", inputs, "files", files, type_=JSON)).label("work_album_files")

    def check(self, packet, instance, home_generation):
        if type(packet) is not dict or set(packet) != {"inputs", "files"} or type(packet["files"]) is not list:
            raise CallbackRefused()
        AlbumInputProof(self.sqlite, self.root).check(packet["inputs"])
        actual_files = [normalized_packet(file) for file in packet["files"]]
        for actual in actual_files:
            _file_journal(SimpleNamespace(**actual))
            if any(actual[key] != value for key, value in self.scope.items()):
                raise CallbackRefused()
        if not same_body(sorted(actual_files, key=lambda file: file["id"]), sorted(self.packet["files"], key=lambda file: file["id"])):
            raise CallbackRefused()
        target = {
            "instance_id": instance.id,
            "principal_kind": "nonhuman",
            "principal_id": instance.principal_id,
            "instance_generation": instance.generation,
            "definition_revision": instance.definition_revision,
            "home_id": instance.home_id,
            "home_generation": home_generation,
        }
        if any(not same_body(file["continuation"]["target"], target) for file in self.files):
            raise CallbackRefused()

    def freeze(self):
        return {
            "version": 2,
            "album": deepcopy(album_context(self.root)),
            "inputs": list(deepcopy(self.inputs)),
            "files": [{"act_id": file["id"], "operation_id": file["operation_ids"][-1], "target": deepcopy(file["continuation"]["target"]), "result": deepcopy(file["result"])} for file in self.files],
            "text": bundle_text(self.inputs, self.files),
        }


async def read_album_files(link, captured, *, session):
    context = album_context(captured)
    proof = AlbumInputProof(session.get_bind().dialect.name == "sqlite", captured)
    packet = proof.check(await session.scalar(select(proof.expression())))
    row = TelegramInboundMarkRow
    marks = list(await session.scalars(select(row).where(row.link_id == link.id, row.chat_id == link.chat_id, row.message_id.in_(context["member_ids"])).with_for_update().execution_options(populate_existing=True)))
    indexed = {mark.message_id: mark for mark in marks}
    scope = {"link_id": link.id, "instance_id": link.agent_ref, **{key: getattr(link, key) for key in ("bot_id", "owner_user_id", "binding_generation", "telegram_user_id", "chat_id")}}
    inputs, files = [], []
    for identity in context["member_ids"]:
        mark = indexed[identity]
        text, descriptors = chat_input_parts(mark, bot_id=link.bot_id)
        if len(descriptors) != 1:
            raise CallbackRefused()
        actual = await session.get(TelegramActRow, file_act_id(link, descriptors[0]), with_for_update=True, populate_existing=True)
        if actual is None or actual.state != "done" or not same_body(_file_journal(actual), descriptors[0]):
            raise CallbackRefused()
        if any(getattr(actual, key) != value for key, value in scope.items()):
            raise CallbackRefused()
        work = actual.continuation.get("work", {})
        if work.get("version") != 2 or not same_body(work.get("album"), context) or work.get("input_sha256") != album_input_digest(mark) or actual.instance_id != link.agent_ref:
            raise CallbackRefused()
        inputs.append(input_packet(mark))
        files.append(file_packet(actual))
    bundle = AlbumFileBundle(captured, proof.sqlite, tuple(inputs), tuple(files), {"inputs": packet, "files": files}, scope)
    bundle_text(bundle.inputs, bundle.files)
    return bundle


def saved_album_text(mark):
    frozen, context = mark.route_basis.get("work_file"), album_context(mark)
    if type(frozen) is not dict or set(frozen) != {"version", "album", "inputs", "files", "text"} or type(frozen["version"]) is not int or frozen["version"] != 2 or not same_body(frozen["album"], context):
        raise CallbackRefused()
    inputs, files = frozen["inputs"], frozen["files"]
    if type(inputs) is not list or type(files) is not list or len(inputs) != len(context["member_ids"]) or len(files) != len(inputs) or inputs_digest(inputs) != context["inputs_sha256"]:
        raise CallbackRefused()
    from app.telegram_bots.work_file_inputs import validate_saved_file_reference

    for identity, packet, file in zip(context["member_ids"], inputs, files, strict=True):
        if packet["message_id"] != identity or type(file) is not dict or set(file) != {"act_id", "operation_id", "target", "result"}:
            raise CallbackRefused()
        raw = packet["sanitized_input"]["files"]
        if type(raw) is not list or len(raw) != 1:
            raise CallbackRefused()
        descriptor = validate_chat_file(raw[0])
        if descriptor["message_id"] != identity or file["act_id"] != file_act_id(SimpleNamespace(id=mark.link_id), descriptor):
            raise CallbackRefused()
        validate_saved_file_reference(file)
        if not same_body(file["target"], files[0]["target"]):
            raise CallbackRefused()
    text = bundle_text(inputs, files)
    if text != frozen["text"]:
        raise CallbackRefused()
    return text

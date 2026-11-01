"""Persist one scoped form without inventing a Telegram human message."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass

from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS, CallbackRefused
from app.telegram_bots.callbacks import freeze_callback_value as _freeze
from app.telegram_bots.callbacks import normalize_callback_topic as _topic
from app.telegram_bots.callbacks import safe_callback_value as _plain
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow
from deerflow.utils.file_io import await_drained

_ID = re.compile(r"[0-9a-f]{32}\Z")
_STATES = {"field", "review", "submitted", "cancelled"}
_KEYS = {"version", "thread_id", "request_id", "request", "fields", "values", "field_index", "revision", "state", "editing", "last_update_id", "last_transition"}


def _request(value):
    from app.telegram_bots.human_input_source import decode_request

    request = decode_request(value)
    if request["source"] != "ask_clarification" or request["input_mode"] != "form":
        raise CallbackRefused() from None
    return request


def _fields(fields):
    return _request({"version": 2, "kind": "human_input_request", "source": "ask_clarification", "request_id": "form-shape", "question": "Form", "input_mode": "form", "fields": list(fields)})["fields"]


def _values(fields, values):
    if not isinstance(values, Mapping) or any(key not in {field["name"] for field in fields} for key in values):
        raise CallbackRefused() from None
    result = {}
    for field in fields:
        name, kind = field["name"], field["type"]
        value = values.get(name, False if kind == "checkbox" else None)
        if kind == "checkbox":
            if type(value) is not bool:
                raise CallbackRefused() from None
        elif kind == "multi_select":
            if value is None:
                continue
            if not isinstance(value, (tuple, list)) or any(type(item) is not str for item in value) or len(set(value)) != len(value) or set(value) - {option["value"] for option in field["options"]}:
                raise CallbackRefused() from None
            value = list(value)
        else:
            if value is None:
                continue
            if type(value) is not str or kind == "select" and value not in {option["value"] for option in field["options"]}:
                raise CallbackRefused() from None
        result[name] = value
    return result


def _empty(value):
    return value is None or type(value) is str and not value.strip() or isinstance(value, (tuple, list)) and not value


def _missing_required(field, values):
    value = values.get(field["name"])
    return value is not True if field["type"] == "checkbox" else _empty(value)


def form_submission_value(fields, values):
    """Match the browser summary/JSON, including implicit false checkboxes."""
    fields = _fields(fields)
    values = _values(fields, values)
    record, lines = {}, []
    for field in fields:
        value = values.get(field["name"])
        if _empty(value):
            continue
        record[field["name"]] = value
        shown = ", ".join(value) if isinstance(value, list) else "yes" if value is True else "no" if value is False else value
        lines.append(f"{field['label']}: {shown}")

    # JSON.stringify enumerates canonical array-index keys before other keys.
    def index(key):
        return key.isascii() and key.isdecimal() and str(int(key)) == key and int(key) < 2**32 - 1

    integers = sorted((key for key in record if index(key)), key=int)
    ordered = {key: record[key] for key in [*integers, *(key for key in record if not index(key))]}
    return "; ".join(lines) + " [values: " + json.dumps(ordered, separators=(",", ":"), ensure_ascii=False) + "]"


@dataclass(frozen=True)
class FormSession:
    id: str
    thread_id: str
    request: Mapping
    fields: tuple[Mapping, ...]
    values: Mapping
    field_index: int
    revision: int
    state: str
    editing: bool
    last_update_id: int | None


@dataclass(frozen=True)
class FormSubmission:
    form: FormSession
    value: str
    response: Mapping


def _decode(row):
    value = _plain(row.payload)
    try:
        if set(value) != _KEYS or type(value["version"]) is not int or value["version"] != 1:
            raise ValueError
        request = _request(value["request"])
        fields = request["fields"]
        if value["fields"] != fields or value["request_id"] != request["request_id"] or type(value["thread_id"]) is not str or not value["thread_id"] or len(value["thread_id"]) > 64:
            raise ValueError
        values = _values(fields, value["values"])
        if values != value["values"] or type(value["field_index"]) is not int or not 0 <= value["field_index"] <= len(fields) or type(value["revision"]) is not int or value["revision"] < 0:
            raise ValueError
        if value["state"] not in _STATES or type(value["editing"]) is not bool or value["state"] == "field" and value["field_index"] == len(fields):
            raise ValueError
        if value["last_update_id"] is not None and (type(value["last_update_id"]) is not int or value["last_update_id"] < 0):
            raise ValueError
        if (value["last_transition"] is None) != (value["last_update_id"] is None):
            raise ValueError
        return value, FormSession(row.id, value["thread_id"], _freeze(request), tuple(_freeze(field) for field in fields), _freeze(values), value["field_index"], value["revision"], value["state"], value["editing"], value["last_update_id"])
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None


class Forms:
    def __init__(self, callbacks):
        self.callbacks = callbacks

    async def _locked(self, writer, link, form_id):
        if type(form_id) is not str or _ID.fullmatch(form_id) is None:
            raise CallbackRefused() from None
        row = await writer.get(TelegramCallbackRow, form_id, with_for_update=True, populate_existing=True)
        if row is None or row.kind != "form" or row.target_kind != "thread" or (row.link_id, row.chat_id, row.binding_generation) != (link.id, link.chat_id, link.binding_generation):
            raise CallbackRefused() from None
        data, snapshot = _decode(row)
        if row.target_id != snapshot.thread_id:
            raise CallbackRefused() from None
        return row, data, snapshot

    async def create(self, link, *, form_id, thread_id, request, guard, session=None, message_thread_id=None):
        request = _request(_plain(request))
        topic = _topic(message_thread_id)
        if type(form_id) is not str or _ID.fullmatch(form_id) is None or type(thread_id) is not str or not thread_id or len(thread_id) > 64:
            raise CallbackRefused() from None

        async def perform():
            async with self.callbacks.writer(link, guard, session=session) as writer:
                existing = await writer.get(TelegramCallbackRow, form_id, with_for_update=True, populate_existing=True)
                if existing is not None:
                    row, data, result = await self._locked(writer, link, form_id)
                    if _plain(result.request) != request or result.thread_id != thread_id or row.message_thread_id != topic:
                        raise CallbackRefused() from None
                    return result
                data = {
                    "version": 1,
                    "thread_id": thread_id,
                    "request_id": request["request_id"],
                    "request": request,
                    "fields": request["fields"],
                    "values": _values(request["fields"], {}),
                    "field_index": 0,
                    "revision": 0,
                    "state": "field",
                    "editing": False,
                    "last_update_id": None,
                    "last_transition": None,
                }
                row = TelegramCallbackRow(
                    id=form_id,
                    group_id=form_id,
                    link_id=link.id,
                    chat_id=link.chat_id,
                    binding_generation=link.binding_generation,
                    message_id=None,
                    message_thread_id=topic,
                    kind="form",
                    target_kind="thread",
                    target_id=thread_id,
                    payload=data,
                    created_at=self.callbacks.clock(),
                )
                writer.add(row)
                await writer.flush()
                return _decode(row)[1]

        return await await_drained(perform())

    async def read(self, link, form_id, *, guard, session=None):
        async def perform():
            async with self.callbacks.writer(link, guard, session=session) as writer:
                return (await self._locked(writer, link, form_id))[2]

        return await await_drained(perform())

    async def transition(self, link, form_id, *, expected_revision, update_id, action, field=None, value=None, guard, session=None):
        if type(expected_revision) is not int or expected_revision < 0 or type(update_id) is not int or update_id < 0 or action not in {"answer", "toggle", "next", "skip", "edit", "review", "submit", "cancel"}:
            raise CallbackRefused() from None
        gesture = _plain({"revision": expected_revision, "action": action, "field": field, "value": value})
        signature = hashlib.sha256(json.dumps(gesture, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()

        async def perform():
            async with self.callbacks.writer(link, guard, session=session) as writer:
                row, data, form = await self._locked(writer, link, form_id)
                if update_id == form.last_update_id:
                    if data["last_transition"] != signature:
                        raise CallbackRefused() from None
                    return form
                # An ordinary input already queued before this form appeared
                # keeps its actual older update ID when it finally ends the
                # form. Only this terminal transition may cross that watermark,
                # and a press on a list, which a tick can overtake on its way in.
                if form.revision != expected_revision or action not in {"cancel", "toggle", "next", "skip"} and form.last_update_id is not None and update_id < form.last_update_id or form.state in {"submitted", "cancelled"}:
                    raise CallbackRefused() from None
                values = deepcopy(data["values"])
                if action in {"answer", "toggle", "next", "skip"}:
                    if form.state != "field" or field != data["fields"][form.field_index]["name"]:
                        raise CallbackRefused() from None
                    definition = data["fields"][form.field_index]
                    kind = definition["type"]
                    advance = True
                    if action == "skip":
                        if definition["required"]:
                            raise CallbackRefused() from None
                        if kind == "checkbox":
                            values[field] = False
                        else:
                            values.pop(field, None)
                    elif action == "toggle":
                        if kind != "multi_select":
                            raise CallbackRefused() from None
                        options = {option["id"]: option["value"] for option in definition["options"]}
                        if value not in options:
                            raise CallbackRefused() from None
                        selected = list(values.get(field, []))
                        selected.remove(options[value]) if options[value] in selected else selected.append(options[value])
                        values[field], advance = selected, False
                    elif action == "next":
                        if kind != "multi_select":
                            raise CallbackRefused() from None
                        if definition["required"] and _empty(values.get(field)):
                            raise CallbackRefused("form.required") from None
                    else:
                        if kind == "multi_select":
                            raise CallbackRefused() from None
                        chosen = value
                        if kind == "select":
                            options = {option["id"]: option["value"] for option in definition["options"]}
                            if value not in options:
                                raise CallbackRefused() from None
                            chosen = options[value]
                        values[field] = chosen
                        values = _values(data["fields"], values)
                        if definition["required"] and _missing_required(definition, values):
                            raise CallbackRefused("form.required") from None
                    if advance:
                        data["field_index"] = len(form.fields) if form.editing else form.field_index + 1
                        if data["field_index"] == len(form.fields):
                            data["state"], data["editing"] = "review", False
                elif action == "edit":
                    names = [item["name"] for item in form.fields]
                    if form.state != "review" or field not in names:
                        raise CallbackRefused() from None
                    data.update(field_index=names.index(field), state="field", editing=True)
                elif action in {"review", "submit"}:
                    if form.state != "review":
                        raise CallbackRefused() from None
                    if any(item["required"] and _missing_required(item, values) for item in form.fields):
                        raise CallbackRefused() from None
                    if action == "submit":
                        data["state"] = "submitted"
                        row.consumed_at, row.closed_reason = self.callbacks.clock(), "answered"
                else:
                    data["state"] = "cancelled"
                    row.consumed_at, row.closed_reason = self.callbacks.clock(), "cancelled"
                data.update(values=values, revision=form.revision + 1, last_update_id=update_id, last_transition=signature)
                row.payload = data
                await writer.flush()
                return _decode(row)[1]

        return await await_drained(perform())

    async def submission(self, link, callback_id, *, guard, session):
        async with self.callbacks.writer(link, guard, session=session) as writer:
            selected = await writer.get(TelegramCallbackRow, callback_id, with_for_update=True, populate_existing=True)
            if selected is None or selected.kind != "form_send" or selected.consumed_at is not None or (selected.link_id, selected.chat_id, selected.binding_generation) != (link.id, link.chat_id, link.binding_generation):
                raise CallbackRefused() from None
            await self.callbacks.lock_current(link, callback_id, chat_id=link.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, guard=guard, session=writer)
            payload = {key: child for key, child in selected.payload.items() if key not in CALLBACK_METADATA_KEYS}
            if set(payload) != {"form_id", "revision"}:
                raise CallbackRefused() from None
            row, data, form = await self._locked(writer, link, payload["form_id"])
            if (
                type(payload["revision"]) is not int
                or payload["revision"] != form.revision
                or form.state != "review"
                or (selected.target_kind, selected.target_id, selected.message_thread_id) != ("thread", form.thread_id, row.message_thread_id)
            ):
                raise CallbackRefused() from None
            if any(field["required"] and _missing_required(field, form.values) for field in form.fields):
                raise CallbackRefused() from None
            value = form_submission_value(form.fields, form.values)
            response = {"version": 1, "kind": "human_input_response", "source": form.request["source"], "request_id": form.request["request_id"], "response_kind": "text", "value": value}
            return FormSubmission(form, value, _freeze(response))

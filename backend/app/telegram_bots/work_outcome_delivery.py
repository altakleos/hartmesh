"""Durable, receipt-owned Work result replies; delivery never retries an action."""

import asyncio
import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import and_, false, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import current_attention, prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows, canonical_keyboard
from app.telegram_bots.inbox import InboxRepository, InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.pages import page_address
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired, keyboard_cleanup_row
from app.telegram_bots.settings import _snapshot
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_input_previews import input_identity
from app.telegram_bots.work_outcome_reads import WorkOutcomeReader, _OutcomeProjection
from app.telegram_bots.work_outcomes import present_outcome
from app.telegram_bots.work_source_spaces import ReferencedSpaces
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def context(read):
    act, source = read.act, read.source
    return {
        **({"file_answer": read.file_answer.shown} if read.file_answer is not None else {}),
        "act": {"id": act.id, "kind": act.kind, "state": act.state, "reason": act.reason, "body": thaw(act.body), "attempts": act.attempts},
        "work_id": act.work_id,
        "request_id": act.request_id,
        "work_revision": source.work["revision"],
        "request_revision": source.request["revision"],
        "purpose": source.request["purpose"],
        "permissions": read.permissions,
        "attempt_status": read.attempt_status,
        "continued": read.continued,
        "protected": read.protected,
        "name": source.request["instance_name"],
        "supervisor": read.supervisor,
        "person": read.recipient,
        "accepted_at": (read.receipt or {}).get("review", {}).get("accepted_at") if act.kind == "accept" and act.state == "done" else None,
    }


def materialize(link, data, *, locale, timezone, product, routing=None, comment=None):
    if comment is not None:
        from app.telegram_bots.work_comment_cards import materialize_comment

        return materialize_comment(link, data, comment, locale=locale)
    if routing is not None:
        from app.telegram_bots.work_routing_followups import materialize_followup

        return materialize_followup(link, data, routing, locale=locale)
    act = SimpleNamespace(**data["act"])
    accepted = data["accepted_at"]
    values = {"name": data["name"], "supervisor": data["supervisor"], "person": data["person"], "product": product}
    if accepted is not None:
        values["time"] = datetime.fromisoformat(accepted).astimezone(ZoneInfo(timezone)).strftime("%Y-%m-%d %H:%M")
    shown = present_outcome(
        act, purpose=data["purpose"], permissions=data["permissions"], attempt_status=data["attempt_status"], continued=data["continued"], locale=locale, values=values, protected=data["protected"], file_answer=data.get("file_answer")
    )
    if shown is None:
        return None
    keyboard = []
    if shown.text.open_assignment:
        address = page_address("web.work", link, work_id=data["work_id"], permissions=data["permissions"])
        if address is not None:
            keyboard.append([{"label": "label.open_assignment", "url": address}])
    if shown.text.retry_action:
        keyboard.append([{"label": "label.retry_action", "kind": "act_check", "target_kind": "act", "target_id": act.id, "payload": {"act_id": act.id}}])
    parts = [{**part, "source_ref": {"key": "work.action.outcome", "values": {"act_id": act.id, "part": index}}, **({"stored_keyboard": True} if keyboard and index == len(shown.parts) - 1 else {})} for index, part in enumerate(shown.parts)]
    return parts, canonical_keyboard(keyboard), shown.text


def phase(act):
    return [act["state"], act["reason"], act["attempts"]]


def delivery_receipt(value):
    if value is None:
        return None
    from collections.abc import Mapping

    if not isinstance(value, Mapping) or set(value) != {"version", "work_outcome"} or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("invalid-work-outcome-receipt")
    receipt = value["work_outcome"]
    if not isinstance(receipt, Mapping) or set(receipt) != {"phase", "outbox_id"}:
        raise ValueError("invalid-work-outcome-receipt")
    state, reason, attempts = receipt["phase"]
    if state not in {"done", "refused", "conflict", "pending", "waiting", "uncertain"} or reason is not None and type(reason) is not str or type(attempts) is not int or attempts < 0:
        raise ValueError("invalid-work-outcome-receipt")
    identity = receipt["outbox_id"]
    if type(identity) is not str or len(identity) != 32 or any(ch not in "0123456789abcdef" for ch in identity):
        raise ValueError("invalid-work-outcome-receipt")
    return receipt


def validate_input(mark, basis, row):
    if (
        mark is None
        or not same_body(input_identity(mark), basis["input"])
        or mark.route_kind != "work_input"
        or (basis.get("refresh", {}).get("input_outbox_id", row.id)) not in mark.delivery_ids
        or "refresh" in basis
        and not mark.route_complete
        or mark.route_basis.get("work_input", {}).get("work_id") != row.work_id
        or mark.message_thread_id != basis["destination"]["message_thread_id"]
    ):
        raise ProductSourceRetired()


def validate_origin(callback, parent, basis, row):
    data, origin, mark = basis["context"], basis["origin"], basis["input"]
    if origin is None:
        if callback is not None or mark is None or mark["operation_id"] != data["act"]["id"]:
            raise ProductSourceRetired()
        return
    if callback is None or parent is None or (callback.id, parent.id) != (origin["callback_id"], origin["anchor_outbox_id"]):
        raise ProductSourceRetired()
    scope = (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id)
    if (callback.link_id, callback.chat_id, callback.binding_generation, callback.message_thread_id) != scope or (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != scope:
        raise ProductSourceRetired()
    if parent.owner_user_id != row.owner_user_id or callback.payload.get("work_action_id") != data["act"]["id"] or callback.group_id not in {part.get("callback_group_id") for part in parent.parts}:
        raise ProductSourceRetired()
    original_input = parent.payload.get("work_request_basis", {}).get("input")
    if callback.kind in {"answer_yes", "voice_save_answer"} and original_input is None and mark is not None:
        fixed = parent.payload.get("product_basis", {})
        if (
            fixed.get("origins") != [[mark["link_id"], mark["message_id"], mark["chat_id"]]]
            or fixed.get("finish_route") is not False
            or not fixed.get("parts")
            or any(part.get("source_ref", {}).get("key") != ("voice.confirm_answer" if callback.kind == "voice_save_answer" else "ask.is_this_answer") for part in fixed["parts"])
        ):
            raise ProductSourceRetired()
        original_input = mark
    if not same_body(original_input, mark):
        raise ProductSourceRetired()


def validate_edit(callback, parent, basis, row):
    """An edit uses only the exact acknowledged source of the routing command."""
    edit = basis.get("edit")
    if edit is None:
        return
    if basis["context"]["act"]["kind"] != "route" or basis["context"]["act"]["state"] != "done" or basis["origin"] is None or parent is None or callback is None:
        raise ProductSourceRetired()
    if type(edit) is not dict or set(edit) != {"presentation_version", "part_id", "message_id", "protect_content"}:
        raise ProductSourceRetired()
    if type(edit["presentation_version"]) is not int or type(edit["message_id"]) is not int or edit["message_id"] <= 0 or type(edit["protect_content"]) is not bool:
        raise ProductSourceRetired()
    part = next((part for part in parent.parts if part["part_id"] == edit["part_id"]), None)
    if (
        len(row.parts) != 1
        or parent.id != basis["origin"]["anchor_outbox_id"]
        or parent.state != "sent"
        or parent.payload["presentation_version"] != edit["presentation_version"]
        or part is None
        or part["presentation_version"] != edit["presentation_version"]
        or part.get("callback_group_id") != callback.group_id
        or callback.message_id != edit["message_id"]
        or part["state"] != "sent"
        or part["message_ids"] != [edit["message_id"]]
        or not part.get("sent_at")
        or bool(part.get("protect_content")) != edit["protect_content"]
        or basis["context"]["protected"]
        and not edit["protect_content"]
    ):
        raise ProductSourceRetired()
    current = row.parts[0]
    if (
        current.get("kind") != "edit"
        or current.get("target_message_id") != edit["message_id"]
        or current.get("target_protect_content") != edit["protect_content"]
        or current["state"] == "sent"
        and current["message_ids"] != [edit["message_id"]]
    ):
        raise ProductSourceRetired()


def _recipe_matches(part, recipe, *, edit=False):
    return all(same_body(part.get(key), value) for key, value in recipe.items() if key != "kind" or not edit)


def acknowledged_parts(row, recipes, *, edit=False, sending_part_id=None):
    """Complete typed transport proof shared by outcome ACK and shown controls."""
    if type(row.payload["presentation_version"]) is not int or row.payload["presentation_version"] != 0 or len(row.parts) != len(recipes) or not row.parts:
        raise ProductSourceRetired()
    receipts = {"part_id", "presentation_version", "ordinal", "state", "message_ids", "callback_group_id", "sent_at", "dispatched_at", "plain_fallback", "invalid_token_verification_pending"} | (
        {"target_message_id", "target_protect_content"} if edit else set()
    )
    identities, message_ids = set(), set()
    sending = next((index for index, part in enumerate(row.parts) if part.get("part_id") == sending_part_id), None)
    if sending_part_id is not None and sending is None:
        raise ProductSourceRetired()
    for index, (part, recipe) in enumerate(zip(row.parts, recipes, strict=True)):
        identity = part["part_id"]
        if type(identity) is not str or len(identity) != 32 or identity in identities or any(ch not in "0123456789abcdef" for ch in identity):
            raise ProductSourceRetired()
        identities.add(identity)
        if (
            type(part["ordinal"]) is not int
            or part["ordinal"] != index
            or type(part["presentation_version"]) is not int
            or part["presentation_version"] != 0
            or set(part) - set(recipe) - receipts
            or bool(part.get("callback_group_id")) != bool(recipe.get("stored_keyboard"))
            or not _recipe_matches(part, recipe, edit=index == 0 and edit)
        ):
            raise ProductSourceRetired()
        if sending is not None and index >= sending:
            if part.get("state") != ("sending" if index == sending else "pending") or part.get("message_ids") != []:
                raise ProductSourceRetired()
            continue
        if part.get("state") != "sent" or type(part.get("message_ids")) is not list or len(part["message_ids"]) != 1 or any(type(value) is not int or value <= 0 or value in message_ids for value in part["message_ids"]):
            raise ProductSourceRetired()
        message_ids.update(part["message_ids"])
        if datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
            raise ProductSourceRetired()


async def acknowledge_outcome(session, row):
    """Preserve complete-delivery evidence after the ordinary outbox expires."""
    basis = row.payload["work_outcome_basis"]
    data = basis["context"]
    act = await session.get(TelegramActRow, data["act"]["id"], with_for_update=True, populate_existing=True)
    if act is None or (act.link_id, act.owner_user_id, act.binding_generation, act.work_id, act.request_id) != (row.link_id, row.owner_user_id, row.binding_generation, row.work_id, row.request_id):
        raise ProductSourceRetired()
    if act.kind != data["act"]["kind"] or not same_body(act.body, data["act"]["body"]):
        raise ProductSourceRetired()
    acknowledged_parts(row, basis["parts"], edit=basis.get("edit") is not None)
    source_callback = await session.get(TelegramCallbackRow, act.id)
    source_parent = await session.get(TelegramOutboxRow, source_callback.payload.get("anchor_outbox_id")) if source_callback else None
    validate_origin(source_callback, source_parent, basis, row)
    validate_edit(source_callback, source_parent, basis, row)
    if "refresh" in basis:
        from app.telegram_bots.work_followup_refresh import validate_refresh

        refresh = basis["refresh"]
        refresh_callback = await session.get(TelegramCallbackRow, refresh["callback_id"])
        refresh_parent = await session.get(TelegramOutboxRow, refresh["anchor_outbox_id"])
        validate_refresh(refresh_callback, refresh_parent, basis, row)
    if data["act"]["state"] == "done" and basis["input"] is not None:
        identity = basis["input"]
        mark = await session.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]), with_for_update=True, populate_existing=True)
        validate_input(mark, basis, row)
        expected_album = data.get("file_answer", {}).get("album")
        if expected_album is not None or "work_album" in mark.route_basis:
            from app.telegram_bots.work_album_inputs import album_context, inputs_digest, outcome_members

            if type(expected_album) is not dict or set(expected_album) != {"version", "context", "file_sha256"} or type(expected_album["version"]) is not int or expected_album["version"] != 1:
                raise ProductSourceRetired()
            if not same_body(expected_album["context"], album_context(mark)) or expected_album["file_sha256"] != inputs_digest(mark.route_basis.get("work_file")):
                raise ProductSourceRetired()
            for member in await outcome_members(session, mark):
                member.route_complete = True
        else:
            mark.route_complete = True
    act.continuation = {"version": 1, "work_outcome": {"phase": phase(data["act"]), "outbox_id": row.id}}


class WorkOutcomePlans:
    def __init__(self, plans, acts, *, product="HartMesh", routing=None):
        self.plans, self.acts, self.product, self.routing = plans, acts, product, routing
        self.outbox, self.callbacks, self.settings = plans.outbox, plans.callbacks, plans.settings
        self.reader = WorkOutcomeReader(plans.reader, acts)
        self.inbox = InboxRepository(self.outbox.sessions)

    async def plan(self, link, act_id, *, guard):
        async def perform():
            read = await self.reader.read(link, act_id)
            settings = await self.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            data = context(read)
            receipt = delivery_receipt(read.act.continuation)
            if receipt is not None and same_body(receipt["phase"], phase(data["act"])):
                return receipt["outbox_id"]
            from app.telegram_bots.work_routing_followups import read_followup

            routing = await read_followup(self.routing, link, read)
            from app.telegram_bots.work_comment_cards import read_comment

            comment = read_comment(link, read)
            shown = await asyncio.to_thread(materialize, link, data, locale=settings.locale, timezone=settings.timezone, product=self.product, routing=routing, comment=comment)
            if shown is None:
                return None
            parts, keyboard, _text = shown
            prepared = await prepare_attention(self.plans.reader, link)
            composed = _OutcomeGuard(guard, prepared, read, routing=_followup_candidates(self, link, data, routing), references=ReferencedSpaces(self.plans.reader, self.outbox.sessions, read.source) if comment is not None else None)
            async with self.callbacks.writer(link, composed) as writer:
                original, origin, topic = await self._origin(writer, link, read.act)
                destination = {"chat_id": link.chat_id, "message_thread_id": topic}
                edit = None
                if read.act.kind == "route" and read.act.state == "done" and original is not None:
                    parent = await writer.get(TelegramOutboxRow, original.payload["anchor_outbox_id"], populate_existing=True)
                    part = next((part for part in parent.parts if part.get("callback_group_id") == original.group_id and part["presentation_version"] == parent.payload["presentation_version"]), None)
                    if part is not None and parent.state == "sent" and part["state"] == "sent" and part["message_ids"] == [original.message_id] and (not read.protected or part.get("protect_content")):
                        edit = {"presentation_version": part["presentation_version"], "part_id": part["part_id"], "message_id": original.message_id, "protect_content": bool(part.get("protect_content"))}
                basis = {
                    "version": 5 if comment is not None else 3 if routing is not None else 2 if edit is not None else 1,
                    **({"comment": comment} if comment is not None else {}),
                    **({"routing": routing} if routing is not None else {}),
                    **({"edit": edit} if edit is not None else {}),
                    "context": data,
                    "locale": settings.locale,
                    "timezone": settings.timezone,
                    "parts": parts,
                    "keyboard": keyboard,
                    "destination": destination,
                    "input": input_identity(origin) if origin else None,
                    "origin": {"callback_id": original.id, "anchor_outbox_id": original.payload["anchor_outbox_id"]} if original else None,
                }
                digest = hashlib.sha256(json.dumps(basis, sort_keys=True).encode()).hexdigest()[:32]
                key = "notice:work-outcome:" + act_id + "." + digest
                previous = list(
                    await writer.scalars(
                        select(TelegramOutboxRow)
                        .where(
                            TelegramOutboxRow.link_id == link.id,
                            TelegramOutboxRow.binding_generation == link.binding_generation,
                            TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act_id,
                        )
                        .order_by(TelegramOutboxRow.id)
                        .with_for_update()
                    )
                )
                if any(old.failed_reason == "work-outcome-refresh-required" and old.state in {"failed", "skipped"} for old in previous):
                    # An observed missing/protection-incompatible target must not
                    # be edited again. Re-read the receipt/source above and build
                    # a fresh delivery while retaining every old ACK.
                    edit = None
                    basis = {**basis, **({"version": 2, "edit": None} if routing is None and comment is None else {})}
                    digest = hashlib.sha256(json.dumps(basis, sort_keys=True).encode()).hexdigest()[:32]
                    key = "notice:work-outcome:" + act_id + "." + digest
                for old in previous:
                    if (
                        old.state != "skipped"
                        and old.failed_reason != "work-outcome-refresh-required"
                        and old.dedupe_key.split("~", 1)[0] == key
                        or old.state == "sent"
                        and same_body(phase(old.payload["work_outcome_basis"]["context"]["act"]), phase(data["act"]))
                    ):
                        return old.id
                for old in previous:
                    await self._retire(writer, link, old)
                if any(old.dedupe_key.split("~", 1)[0] == key for old in previous):
                    key += "~" + str(len(previous))
                identity, _ = await self.outbox._add(
                    link,
                    dedupe_key=key,
                    class_="reply",
                    payload={"parts": parts, "work_outcome_basis": basis, "product_keyboard_basis": keyboard},
                    instance_id=link.agent_ref,
                    work_id=read.act.work_id,
                    request_id=read.act.request_id,
                    message_thread_id=topic,
                    session=writer,
                )
                row = await writer.get(TelegramOutboxRow, identity)
                saved_parts = self.outbox._parts(parts, 0)
                if edit is not None:
                    saved_parts[0].update(kind="edit", target_message_id=edit["message_id"], target_protect_content=edit["protect_content"])
                    row.parts = saved_parts
                    validate_edit(original, parent, basis, row)
                if keyboard:
                    group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=topic, session=writer)
                    saved_parts[-1]["callback_group_id"] = group.group_id
                    for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                        control.payload = {**control.payload, "presentation_pending": True}
                row.parts = saved_parts
                if read.act.state == "done":
                    if original is not None:
                        parent_id = original.payload.get("anchor_outbox_id")
                        parent = await writer.get(TelegramOutboxRow, parent_id, with_for_update=True, populate_existing=True)
                        if parent is not None:
                            await self._retire_controls(writer, link, parent, "work-done", editing=edit["message_id"] if edit else None)
                if origin is not None:
                    members = [origin]
                    if "work_album" in origin.route_basis:
                        from app.telegram_bots.work_album_inputs import outcome_members

                        members = await outcome_members(writer, origin)
                    await self.inbox.register_delivery([InboxRow.from_row(member) for member in members], outbox_id=identity, finish=False, session=writer)
                return identity

        return await await_drained(perform())

    async def _origin(self, writer, link, act):
        original = await writer.get(TelegramCallbackRow, act.id)
        operation, topic = act.id, None
        if original is not None:
            if (original.link_id, original.chat_id, original.binding_generation, original.payload.get("work_action_id")) != (link.id, link.chat_id, link.binding_generation, act.id):
                raise ProductSourceRetired()
            topic = original.message_thread_id
            parent = await writer.get(TelegramOutboxRow, original.payload.get("anchor_outbox_id"))
            if (
                parent is None
                or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != (link.id, link.owner_user_id, link.chat_id, link.binding_generation, topic)
                or original.group_id not in {part.get("callback_group_id") for part in parent.parts}
            ):
                raise ProductSourceRetired()
            basis = parent.payload.get("work_request_basis", {})
            if basis.get("version") == 4:
                operation = basis["input"]["operation_id"]
        mark = TelegramInboundMarkRow
        origin = await writer.scalar(
            select(mark)
            .where(
                mark.link_id == link.id,
                mark.chat_id == link.chat_id,
                mark.binding_generation == link.binding_generation,
                mark.route_kind == "work_input",
                mark.operation_ids[0].as_string() == operation,
            )
            .with_for_update()
        )
        if origin is not None:
            if origin.route_basis.get("work_input", {}).get("work_id") != act.work_id or len(origin.operation_ids) != 1:
                raise ProductSourceRetired()
            topic = origin.message_thread_id
        if original is None and origin is None:
            raise ProductSourceRetired()
        return original, origin, topic

    async def _retire_controls(self, writer, link, row, reason, *, editing=None):
        groups = {part.get("callback_group_id") for part in row.parts} - {None}
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update()))
        cleared = set()
        for control in controls:
            if control.consumed_at is not None and control.closed_reason != "work-done":
                continue
            if (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id) or (
                control.link_id,
                control.chat_id,
                control.binding_generation,
                control.message_thread_id,
                control.payload.get("anchor_outbox_id"),
            ) != (link.id, link.chat_id, link.binding_generation, row.message_thread_id, row.id):
                raise ProductSourceRetired()
            own = next((part for part in row.parts if part.get("callback_group_id") == control.group_id and part["presentation_version"] == control.payload.get("presentation_version")), None)
            if own is None or control.message_id is not None and (own["state"] != "sent" or own["message_ids"] != [control.message_id]):
                raise ProductSourceRetired()
            if control.message_id is not None and control.message_id != editing and control.group_id not in cleared:
                cleared.add(control.group_id)
                key = "notice:work-outcome-cleanup:" + control.group_id
                existing = await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
                if existing is None:
                    writer.add(keyboard_cleanup_row(control, owner_user_id=link.owner_user_id, now=self.outbox.clock(), dedupe_key=key))
            if control.consumed_at is None:
                control.consumed_at, control.closed_reason = self.outbox.clock(), reason
            if reason == "work-done" and control.kind == "voice_save_answer":
                control.payload = {key: value for key, value in control.payload.items() if key != "transcript"}
                keyboard = deepcopy(row.payload["product_keyboard_basis"])
                for line in keyboard:
                    for descriptor in line:
                        if descriptor.get("kind") == "voice_save_answer":
                            descriptor["payload"].pop("transcript", None)
                row.payload = {**row.payload, "product_keyboard_basis": keyboard}

    async def _retire(self, writer, link, row):
        if row.claim_id is not None:
            raise DeliveryRetry(timedelta(seconds=5))
        await self._retire_controls(writer, link, row, "superseded")
        if row.state not in {"sent", "skipped"}:
            row.state, row.skip_reason, row.closed_at = "skipped", "superseded", self.outbox.clock()
            parts = deepcopy(row.parts)
            for part in parts:
                if part["state"] not in {"sent", "retired", "unavailable"}:
                    part["state"] = "retired"
            row.parts = parts


def _followup_candidates(plans, link, data, routing):
    if routing is None:
        return None
    from app.telegram_bots.work_routing_authority import RoutingCandidates
    from app.telegram_bots.work_routing_followups import frozen_followup

    return RoutingCandidates(plans.outbox.sessions, link, frozen_followup(routing, data), routing["routing"])


class _ContextProjection(_OutcomeProjection):
    def __init__(self, read, *, routing=None, references=None):
        super().__init__(read.act, read.source, read.receipt, file_answer=read.file_answer)
        self.read, self.routing, self.references = read, routing, references

    def extend(self, query):
        result = super().extend(query)
        result = self.routing.extend(result) if self.routing is not None else result
        return result.add_columns(self.references.expression()) if self.references is not None else result

    def check(self, rows):
        super().check(rows)
        if (rows[0][2], self.supervisor, self.recipient, self.continued) != (self.read.permissions, self.read.supervisor, self.read.recipient, self.read.continued):
            raise ProductSourceRetired()
        if self.routing is not None:
            self.routing.check(rows)
        if self.references is not None:
            self.references.check(rows[0][-1])


class _OutcomeGuard:
    def __init__(self, base, prepared, read, *, routing=None, references=None):
        self.base, self.prepared, self.read, self.routing, self.references = base, prepared, read, routing, references

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.prepared(session=writer, projection=_ContextProjection(self.read, routing=self.routing, references=self.references))
        self.base.current()
        return current


class WorkOutcomeSources:
    def __init__(self, plans):
        self.plans = plans

    async def _prepare(self, captured):
        basis = captured.item.payload.get("work_outcome_basis")
        try:
            if (
                type(basis) is not dict
                or set(basis)
                != {"version", "context", "locale", "timezone", "parts", "keyboard", "destination", "input", "origin"}
                | ({"edit"} if basis.get("version") == 2 else set())
                | ({"routing"} if basis.get("version") in {3, 4} else set())
                | ({"refresh"} if basis.get("version") == 4 else set())
                | ({"comment"} if basis.get("version") == 5 else set())
                or type(basis["version"]) is not int
                or basis["version"] not in {1, 2, 3, 4, 5}
                or captured.item.class_ != "reply"
            ):
                raise ValueError()
            if basis["version"] == 4:
                from app.telegram_bots.work_followup_refresh import refresh_identity

                refresh_identity(basis)
            destination = basis["destination"]
            if not same_body(destination, {"chat_id": captured.item.chat_id, "message_thread_id": captured.item.message_thread_id}) or captured.item.chat_id != captured.link.chat_id:
                raise ValueError()
            origin = basis["input"]
            if origin is not None:
                if type(origin) is not dict or set(origin) != {"link_id", "chat_id", "message_id", "update_id", "binding_generation", "operation_id"}:
                    raise ValueError()
                if origin["link_id"] != captured.link.id or origin["chat_id"] != captured.item.chat_id or origin["binding_generation"] != captured.link.binding_generation:
                    raise ValueError()
                if any(type(origin[key]) is not int or not 0 <= origin[key] < 2**63 for key in ("chat_id", "message_id", "update_id", "binding_generation")) or type(origin["operation_id"]) is not str:
                    raise ValueError()
            act_id = basis["context"]["act"]["id"]
            callback = basis["origin"]
            if callback is not None and (type(callback) is not dict or set(callback) != {"callback_id", "anchor_outbox_id"} or callback["callback_id"] != act_id or type(callback["anchor_outbox_id"]) is not str):
                raise ValueError()
            if callback is None and (origin is None or origin["operation_id"] != act_id):
                raise ValueError()
            read = await self.plans.reader.read(captured.link, act_id)
            if not same_body(context(read), basis["context"]):
                raise ProductSourceRetired()
            settings = await self.plans.settings.get(captured.link.owner_user_id)
            if settings is None or (settings.locale, settings.timezone) != (basis["locale"], basis["timezone"]):
                raise ProductSourceRetired()
            from app.telegram_bots.work_routing_followups import read_followup, same_followup

            routing = await read_followup(self.plans.routing, captured.link, read)
            if not same_followup(routing, basis.get("routing")):
                raise ProductSourceRetired()
            from app.telegram_bots.work_comment_cards import read_comment, same_comment

            if basis["version"] == 5 and not same_comment(captured.link, basis["context"], read_comment(captured.link, read), basis["comment"]):
                raise ProductSourceRetired()
            result = await asyncio.to_thread(materialize, captured.link, basis["context"], locale=basis["locale"], timezone=basis["timezone"], product=self.plans.product, routing=basis.get("routing"), comment=basis.get("comment"))
            if result is None:
                raise ValueError()
            parts, keyboard, _text = result
            if not same_body(parts, basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, captured.item.payload.get("product_keyboard_basis")):
                raise ValueError()
            if (
                (captured.item.instance_id, captured.item.work_id, captured.item.request_id) != (captured.link.agent_ref, read.act.work_id, read.act.request_id)
                or type(captured.item.payload.get("presentation_version")) is not int
                or captured.item.payload["presentation_version"] != 0
            ):
                raise ValueError()
            if len(captured.item.parts) != len(parts):
                raise ValueError()
            index = captured.part["source_ref"]["values"]["part"]
            if type(index) is not int or not 0 <= index < len(parts) or captured.item.parts[index] != captured.part or captured.part["state"] != "sending":
                raise ValueError()
            identities, message_ids = set(), set()
            receipts = {"part_id", "presentation_version", "ordinal", "state", "message_ids", "callback_group_id", "sent_at", "dispatched_at", "plain_fallback", "invalid_token_verification_pending"} | (
                {"target_message_id", "target_protect_content"} if basis.get("edit") is not None else set()
            )
            for ordinal, (part, recipe) in enumerate(zip(captured.item.parts, parts, strict=True)):
                identity = part["part_id"]
                if type(identity) is not str or len(identity) != 32 or identity in identities or any(ch not in "0123456789abcdef" for ch in identity):
                    raise ValueError()
                identities.add(identity)
                if bool(part.get("callback_group_id")) != bool(recipe.get("stored_keyboard")):
                    raise ValueError()
                if type(part["ordinal"]) is not int or part["ordinal"] != ordinal or type(part["presentation_version"]) is not int or part["presentation_version"] != 0 or set(part) - set(recipe) - receipts:
                    raise ValueError()
                if not _recipe_matches(part, recipe, edit=ordinal == 0 and basis.get("edit") is not None) or ordinal < len(parts) - 1 and part.get("callback_group_id"):
                    raise ValueError()
                if ordinal < index and (part["state"] != "sent" or not part.get("sent_at") or len(part["message_ids"]) != 1 or any(type(value) is not int or value <= 0 or value in message_ids for value in part["message_ids"])):
                    raise ValueError()
                if ordinal < index:
                    if datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
                        raise ValueError()
                    message_ids.update(part["message_ids"])
            return basis, read, settings, parts[index]
        except (AccessOff, CeilingRefused, AgentDenied, AgentConflict):
            raise ProductSourceRetired() from None
        except (KeyError, ValueError, TypeError, IndexError):
            raise PermanentDeliveryError("invalid-work-outcome-source") from None

    async def read_part(self, captured):
        async def perform():
            _basis, _read, _settings, part = await self._prepare(captured)
            return part["html"], part["visible_text"]

        return await await_drained(perform())

    async def authorize(self, captured, *, silent, now):
        async def perform():
            basis, read, settings, _part = await self._prepare(captured)
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            projection = _DeliveryProjection(self.plans, captured, read, settings, zone, silent, now)
            try:
                await current_attention(self.plans.plans.reader, captured.link, projection=projection)
            except (AccessOff, CeilingRefused, AgentDenied, AgentConflict, CallbackRefused):
                raise ProductSourceRetired() from None

        return await await_drained(perform())


class _DeliveryProjection(_ContextProjection):
    def __init__(self, plans, captured, read, settings, zone, silent, now):
        super().__init__(read)
        self.plans, self.captured, self.settings, self.zone, self.silent, self.now = plans, captured, settings, zone, silent, now
        self.controls_part = next((part for part in captured.item.parts if part.get("stored_keyboard")), None)
        basis = captured.item.payload["work_outcome_basis"]
        self.candidates = _followup_candidates(plans, captured.link, basis["context"], basis.get("routing"))
        self.delivery_references = ReferencedSpaces(plans.plans.reader, plans.outbox.sessions, read.source) if basis.get("version") == 5 else None

    def extend(self, query):
        delivery, settings, callback, mark = TelegramOutboxRow, TelegramPersonSettingsRow, TelegramCallbackRow, TelegramInboundMarkRow
        origin = self.captured.item.payload["work_outcome_basis"]["input"]
        source_callback, source_parent = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        refresh_callback, refresh_parent = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        refresh = self.captured.item.payload["work_outcome_basis"].get("refresh")
        result = (
            super()
            .extend(query)
            .add_columns(delivery, settings, callback, mark, source_callback, source_parent, refresh_callback, refresh_parent)
            .join(delivery, delivery.id == self.captured.item.id)
            .join(settings, settings.owner_user_id == delivery.owner_user_id)
            .outerjoin(callback, callback.group_id == self.controls_part["callback_group_id"] if self.controls_part else false())
            .outerjoin(mark, and_(mark.link_id == origin["link_id"], mark.message_id == origin["message_id"], mark.chat_id == origin["chat_id"]) if origin else false())
            .outerjoin(source_callback, source_callback.id == self.read.act.id)
            .outerjoin(source_parent, source_parent.id == source_callback.payload["anchor_outbox_id"].as_string())
            .outerjoin(refresh_callback, refresh_callback.id == refresh["callback_id"] if refresh else false())
            .outerjoin(refresh_parent, refresh_parent.id == refresh["anchor_outbox_id"] if refresh else false())
        )

        result = self.candidates.extend(result) if self.candidates is not None else result
        return result.add_columns(self.delivery_references.expression()) if self.delivery_references is not None else result

    def check(self, rows):
        super().check(rows)
        link = rows[0][0]
        delivery, settings = rows[0][17:19]
        actual = self.plans.outbox._sending(link, delivery, link_id=link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision)
        if actual != self.captured or _snapshot(settings) != self.settings:
            raise DeliveryClaimRetired()
        minute = self.now.astimezone(self.zone).hour * 60 + self.now.astimezone(self.zone).minute
        start, end = self.settings.quiet_start_minute, self.settings.quiet_end_minute
        quiet = self.settings.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        recent = link.last_update_at is not None and self.now - aware(link.last_update_at) <= timedelta(seconds=self.plans.outbox.config.outbox.recent_act_seconds)
        if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(self.silent):
            raise DeliveryClaimRetired()
        validate_origin(rows[0][21], rows[0][22], delivery.payload["work_outcome_basis"], delivery)
        validate_edit(rows[0][21], rows[0][22], delivery.payload["work_outcome_basis"], delivery)
        if "refresh" in delivery.payload["work_outcome_basis"]:
            from app.telegram_bots.work_followup_refresh import validate_refresh

            validate_refresh(rows[0][23], rows[0][24], delivery.payload["work_outcome_basis"], delivery)
        if delivery.payload["work_outcome_basis"]["input"] is not None:
            validate_input(rows[0][20], delivery.payload["work_outcome_basis"], delivery)
        if self.controls_part is not None:
            callbacks = {row[19].id: row[19] for row in rows if row[19] is not None}
            _validate_rows(delivery, list(callbacks.values()), part=self.controls_part, allow_pending=True)
            for control in callbacks.values():
                self.plans.callbacks._usable(control)
        if self.candidates is not None:
            self.candidates.check(rows)
        if self.delivery_references is not None:
            self.delivery_references.check(rows[0][-1])

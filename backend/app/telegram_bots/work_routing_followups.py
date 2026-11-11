"""A canonical wrong-person receipt owns a direct routing follow-up."""

import asyncio
from dataclasses import asdict
from datetime import datetime

from app.telegram_bots.config import load_config
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_request_source import same_displayed_basis
from app.telegram_bots.work_requests import WorkRequestRead, seen_basis
from app.telegram_bots.work_routing import RoutingRead, present_routing


async def read_followup(plans, link, read):
    act = read.act
    if plans is None or act.kind != "respond" or act.state != "done" or act.body.get("disposition") != "wrong_recipient" or read.permissions & 6 != 6:
        return None
    value = await plans.reader.read_routing(link, act.request_id)
    if not same_displayed_basis(value.source, read.source):
        raise ProductSourceRetired()
    latest = max((row for row in value.source.responses if row["disposition"] == "wrong_recipient"), key=lambda row: (row["created_at"], row["id"]), default=None)
    response_id = (read.receipt or {}).get("receipt", {}).get("response_id")
    if not value.after_wrong or latest is None or latest["id"] != response_id:
        return None
    from deerflow.config.app_config import get_app_config

    base = load_config(await asyncio.to_thread(get_app_config)).public_base_url
    source = asdict(value.source)
    source["responses"] = list(source["responses"])
    return {"version": 1, "source": source, "routing": {"recipients": list(value.recipients), "after_wrong": True, "public_base_url": base}, "response_id": response_id}


def frozen_followup(value, data):
    try:
        if type(value) is not dict or set(value) != {"version", "source", "routing", "response_id"} or type(value["version"]) is not int or value["version"] != 1:
            raise ValueError()
        act, routing = data["act"], value["routing"]
        source = WorkRequestRead(**{**value["source"], "responses": tuple(value["source"]["responses"])})
        if (
            act["kind"] != "respond"
            or act["state"] != "done"
            or act["body"].get("disposition") != "wrong_recipient"
            or type(data["permissions"]) is not int
            or data["permissions"] & 6 != 6
            or type(source.protected) is not bool
            or source.request["work_id"] != source.work["id"]
            or source.request["instance_id"] != source.work["instance_id"]
            or type(routing) is not dict
            or set(routing) != {"recipients", "after_wrong", "public_base_url"}
            or routing["after_wrong"] is not True
            or type(routing["recipients"]) is not list
            or routing["public_base_url"] is not None
            and type(routing["public_base_url"]) is not str
        ):
            raise ValueError()
        people = routing["recipients"]
        if any(
            type(person) is not dict
            or set(person) != {"user_id", "name", "permissions"}
            or type(person["user_id"]) is not str
            or not person["user_id"]
            or type(person["name"]) is not str
            or type(person["permissions"]) is not int
            or not 1 <= person["permissions"] <= 7
            for person in people
        ):
            raise ValueError()
        if len({person["user_id"] for person in people}) != len(people):
            raise ValueError()
        if not same_body(source.seen, seen_basis(source.work, source.request, source.responses)):
            raise ValueError()
        if (source.work["id"], source.request["id"], source.work["revision"], source.request["revision"], source.protected, source.request["purpose"]) != (
            data["work_id"],
            data["request_id"],
            data["work_revision"],
            data["request_revision"],
            data["protected"],
            data["purpose"],
        ):
            raise ValueError()
        response = next((row for row in source.responses if row["id"] == value["response_id"]), None)
        latest = max((row for row in source.responses if row["disposition"] == "wrong_recipient"), key=lambda row: (row["created_at"], row["id"]), default=None)
        if response is None or response != latest or response["text"] != act["body"]["text"] or response["request_revision"] != source.request["request_revision"] or response["assignment_revision"] != source.request["assignment_revision"]:
            raise ValueError()
        return source
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None


def same_followup(left, right):
    if left is None or right is None:
        return left is right
    try:
        lsource = WorkRequestRead(**{**left["source"], "responses": tuple(left["source"]["responses"])})
        rsource = WorkRequestRead(**{**right["source"], "responses": tuple(right["source"]["responses"])})
        return same_body({key: value for key, value in left.items() if key != "source"}, {key: value for key, value in right.items() if key != "source"}) and same_displayed_basis(lsource, rsource)
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None


def materialize_followup(link, data, value, *, locale):
    source = frozen_followup(value, data)
    routing = value["routing"]
    shown = present_routing(RoutingRead(source, tuple(routing["recipients"]), True), locale=locale, link=link, public_base_url=routing["public_base_url"])
    keyboard = [list(line) for line in shown.keyboard]
    parts = [
        {**part, "source_ref": {"key": "work.action.outcome", "values": {"act_id": data["act"]["id"], "part": index}}, **({"stored_keyboard": True} if keyboard and index == len(shown.parts) - 1 else {})}
        for index, part in enumerate(shown.parts)
    ]
    return parts, keyboard, None


def acknowledged_followup(link, parent):
    """A shown outcome supplies captured choices, never current action rights."""
    try:
        basis = parent.payload["work_outcome_basis"]
        if type(basis) is not dict or set(basis) != {"version", "context", "locale", "timezone", "parts", "keyboard", "destination", "input", "origin", "routing"} | ({"refresh"} if basis.get("version") == 4 else set()):
            raise ValueError()
        if parent.state != "sent" or parent.class_ != "reply" or basis["version"] not in {3, 4} or type(basis["version"]) is not int or type(basis["locale"]) is not str:
            raise ValueError()
        if basis["version"] == 4:
            from app.telegram_bots.work_followup_refresh import refresh_identity

            refresh_identity(basis)
        source = frozen_followup(basis["routing"], basis["context"])
        if link.agent_kind != "instance" or (parent.instance_id, parent.work_id, parent.request_id) != (link.agent_ref, source.work["id"], source.request["id"]) or source.request["instance_id"] != link.agent_ref:
            raise ValueError()
        response = next(row for row in source.responses if row["id"] == basis["routing"]["response_id"])
        if response["actor_id"] != link.owner_user_id or not same_body(basis["destination"], {"chat_id": link.chat_id, "message_thread_id": None}):
            raise ValueError()
        if (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, None):
            raise ValueError()
        parts, keyboard, _ = materialize_followup(link, basis["context"], basis["routing"], locale=basis["locale"])
        if (
            type(parent.payload["presentation_version"]) is not int
            or parent.payload["presentation_version"] != 0
            or not same_body(parts, basis["parts"])
            or not same_body(keyboard, basis["keyboard"])
            or not same_body(keyboard, parent.payload["product_keyboard_basis"])
        ):
            raise ValueError()
        if len(parts) != len(parent.parts) or not parts:
            raise ValueError()
        identities, messages = set(), set()
        receipts = {"part_id", "presentation_version", "ordinal", "state", "message_ids", "callback_group_id", "sent_at", "dispatched_at", "plain_fallback", "invalid_token_verification_pending"}
        for index, (part, recipe) in enumerate(zip(parent.parts, parts, strict=True)):
            identity = part["part_id"]
            if type(identity) is not str or len(identity) != 32 or identity in identities or any(ch not in "0123456789abcdef" for ch in identity):
                raise ValueError()
            identities.add(identity)
            if (
                part["state"] != "sent"
                or type(part["ordinal"]) is not int
                or part["ordinal"] != index
                or type(part["presentation_version"]) is not int
                or part["presentation_version"] != 0
                or type(part["message_ids"]) is not list
                or len(part["message_ids"]) != 1
                or any(type(value) is not int or value <= 0 or value in messages for value in part["message_ids"])
                or datetime.fromisoformat(part["sent_at"]).utcoffset() is None
                or set(part) - set(recipe) - receipts
                or bool(part.get("callback_group_id")) != bool(recipe.get("stored_keyboard"))
                or any(not same_body(part.get(key), value) for key, value in recipe.items())
            ):
                raise ValueError()
            messages.update(part["message_ids"])
        return source
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None


class FollowupAdmission:
    """Reprove the completed response and its actual shown reply at admission."""

    def __init__(self, link, parent):
        from copy import deepcopy

        acknowledged_followup(link, parent)
        self.link, self.identity = link, parent.id
        self.basis = deepcopy(parent.payload["work_outcome_basis"])

    def extend(self, query):
        from sqlalchemy import and_
        from sqlalchemy.orm import aliased

        from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
        from deerflow.persistence.agent_instances.work import AgentWorkEventRow
        from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow

        act, event, effect, parent = aliased(TelegramActRow), aliased(HumanInputEventRow), aliased(AgentWorkEventRow), aliased(TelegramOutboxRow)
        return (
            query.add_columns(act, event, effect, parent)
            .outerjoin(act, act.id == self.basis["context"]["act"]["id"])
            .outerjoin(event, and_(event.instance_id == act.instance_id, event.operation_id == act.id))
            .outerjoin(effect, and_(effect.instance_id == act.instance_id, effect.operation_id == act.id))
            .outerjoin(parent, parent.id == self.identity)
        )

    def check(self, rows):
        from app.telegram_bots.work_outcome_delivery import delivery_receipt, phase

        link = rows[0][0]
        act, event, effect, parent = rows[0][-4:]
        data = self.basis["context"]
        if act is None or parent is None or event is None or effect is None:
            raise ProductSourceRetired()
        if not same_body(parent.payload.get("work_outcome_basis"), self.basis):
            raise ProductSourceRetired()
        acknowledged_followup(link, parent)
        if (
            (act.link_id, act.owner_user_id, act.instance_id, act.binding_generation, act.work_id, act.request_id) != (link.id, link.owner_user_id, link.agent_ref, link.binding_generation, data["work_id"], data["request_id"])
            or act.kind != "respond"
            or act.state != "done"
            or not same_body(act.body, data["act"]["body"])
            or not same_body(delivery_receipt(act.continuation), {"phase": phase(data["act"]), "outbox_id": parent.id})
            or (event.actor_kind, event.actor_id, event.action, event.request_id) != ("human", link.owner_user_id, "respond", act.request_id)
            or not same_body(event.request, act.body)
            or event.receipt.get("response_id") != self.basis["routing"]["response_id"]
            or (effect.actor_kind, effect.actor_id, effect.action, effect.work_id) != ("human", link.owner_user_id, "human_response", act.work_id)
            or not same_body(effect.request, act.body)
        ):
            raise ProductSourceRetired()

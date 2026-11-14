"""Keep each unresolved request/activation family and its original consent."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_continuation_state import activation_child_id, continuation_state, require_child
from app.telegram_bots.work_outcome_delivery import delivery_receipt
from app.telegram_bots.work_outcome_transfer import transferred
from app.telegram_bots.work_request_continuation_sources import qualify_request_primary
from app.telegram_bots.work_request_continuations import request_activation, request_signature
from app.telegram_bots.work_start_retention import closed_activation
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@dataclass
class RequestDependencies:
    acts: set = field(default_factory=set)
    inputs: set = field(default_factory=set)
    deliveries: set = field(default_factory=set)
    groups: set = field(default_factory=set)
    expired_history: set = field(default_factory=set)


def mapping(value):
    return value if isinstance(value, Mapping) else {}


def scope(parent):
    return SimpleNamespace(
        id=parent.link_id, owner_user_id=parent.owner_user_id, bot_id=parent.bot_id, telegram_user_id=parent.telegram_user_id, binding_generation=parent.binding_generation, chat_id=parent.chat_id, agent_ref=parent.instance_id
    )


def same_scope(row, link):
    return (row.link_id, row.chat_id, row.binding_generation) == (link.id, link.chat_id, link.binding_generation) and (not hasattr(row, "owner_user_id") or row.owner_user_id == link.owner_user_id)


def input_locators(delivery):
    raw = [mapping(delivery.payload.get("work_request_basis")).get("input")]
    origins = mapping(delivery.payload.get("product_basis")).get("origins")
    if isinstance(origins, (list, tuple)):
        raw.extend({"link_id": row[0], "message_id": row[1], "chat_id": row[2]} for row in origins if isinstance(row, (list, tuple)) and len(row) == 3)
    return raw


def original_mark(raw, marks):
    raw = mapping(raw)
    key = (raw.get("link_id"), raw.get("message_id"), raw.get("chat_id"))
    return marks.get(key) if type(key[0]) is str and type(key[1]) is int and type(key[2]) is int else None


def original_topic(source, chosen, deliveries, link):
    origin = mapping(source.get("origin"))
    anchor_id = origin.get("anchor_outbox_id")
    anchor = deliveries.get(anchor_id) if type(anchor_id) is str else None
    try:
        if anchor is not None and same_scope(anchor, link) and request_signature(anchor) == origin.get("signature"):
            return anchor.message_thread_id
    except (KeyError, TypeError, ValueError):
        pass
    # Retained physical ACKs still name the original topic if private metadata
    # is damaged. A callback's mutable topic alone cannot relocate custody.
    anchor_id = chosen.payload.get("anchor_outbox_id") if chosen is not None else None
    anchor = deliveries.get(anchor_id) if type(anchor_id) is str else None
    if anchor is not None and same_scope(anchor, link) and any(part.get("callback_group_id") == chosen.group_id and chosen.message_id in part.get("message_ids", []) for part in anchor.parts):
        return anchor.message_thread_id
    return None


def pin_orphan(result, chosen, deliveries, marks, by_anchor, related):
    """Raw same-scope edges retain evidence without reconstructing a journal."""
    if chosen is None or type(chosen.payload.get("anchor_outbox_id")) is not str:
        return
    anchor = deliveries.get(chosen.payload["anchor_outbox_id"])
    if anchor is None or (anchor.link_id, anchor.chat_id, anchor.binding_generation, anchor.message_thread_id) != (chosen.link_id, chosen.chat_id, chosen.binding_generation, chosen.message_thread_id):
        return
    link = SimpleNamespace(id=anchor.link_id, chat_id=anchor.chat_id, binding_generation=anchor.binding_generation, owner_user_id=anchor.owner_user_id)
    owned = {anchor.id, *[value for value in related if value in deliveries and same_scope(deliveries[value], link) and deliveries[value].message_thread_id == anchor.message_thread_id]}
    for value in list(owned):
        for raw in input_locators(deliveries[value]):
            mark = original_mark(raw, marks)
            if mark is None or not same_scope(mark, link) or mark.message_thread_id != anchor.message_thread_id:
                continue
            result.inputs.add((mark.link_id, mark.message_id, mark.chat_id))
            for data in (mapping(mark.route_basis.get("work_answer")), mapping(mark.route_basis.get("work_input"))):
                source = deliveries.get(data.get("source_outbox_id")) if type(data.get("source_outbox_id")) is str else None
                if source is not None and same_scope(source, link) and source.message_thread_id == anchor.message_thread_id and source.work_id == data.get("work_id"):
                    owned.add(source.id)
    result.deliveries.update(owned)
    result.groups.update(row.group_id for value in owned for row in by_anchor[value] if same_scope(row, link) and row.message_thread_id == deliveries[value].message_thread_id)


async def closed_request(session, parent, child, link, attempt):
    try:
        snapshot = _snapshot(parent)
        value = mapping(parent.continuation)
        receipt_source = replace(snapshot, continuation={key: item for key, item in {**value, "version": 1}.items() if key != "work_outcome_transfer"})
        state = continuation_state(receipt_source)
        actual = request_activation(receipt_source)
        event = await session.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == parent.instance_id, AgentWorkEventRow.operation_id == parent.id))
        response = await session.scalar(select(HumanInputEventRow).where(HumanInputEventRow.instance_id == parent.instance_id, HumanInputEventRow.operation_id == parent.id))
        if parent.state in {"conflict", "refused"}:
            receipt = delivery_receipt(parent.continuation)
            delivered = receipt is not None and list(receipt["phase"]) == [parent.state, parent.reason, parent.attempts]
            return (
                (delivered or transferred(parent))
                and parent.reason not in {None, "invalid", "missing", "cancel-unconfirmed"}
                and state["phase"] in {"awaiting_primary", "refused", "cancelled", "expired"}
                and state["child_body"] is None
                and not parent.operation_ids
                and child is None
                and attempt is None
                and event is None
                and response is None
            )
        if parent.state != "done" or delivery_receipt(parent.continuation) is None:
            return False
        if child is not None:
            require_child(_snapshot(parent), _snapshot(child))
            if parent.operation_ids != [child.id]:
                return False
            if state["phase"] not in {"done", "refused", "cancelled"} or not closed_activation(child, link, attempt, work_id=parent.work_id) or attempt is not None and attempt.status in UNRESOLVED:
                return False
        elif state["child_body"] is not None or state["phase"] not in {"expired", "cancelled", "refused"}:
            return False
        if child is None and state["phase"] == "refused" and "refusal" not in state or child is None and state["phase"] == "cancelled" and parent.cancel_requested_at is None:
            return False
        if (state["work_id"], state["assignment_revision"]) != (parent.work_id, actual["assignment_revision"]):
            return False
        answer_id = mapping(response.receipt).get("response_id") if response is not None else None
        answer = await session.get(HumanInputResponseRow, answer_id) if type(answer_id) is str else None
        qualify_request_primary(_snapshot(parent), actual, event, response, answer)
        return True
    except (CallbackRefused, ProductSourceRetired, ValueError, KeyError, TypeError, AttributeError):
        return False


async def request_dependencies(session, now, *, max_age_days):
    result = RequestDependencies()
    journals = {row.id: row for row in await session.scalars(select(TelegramActRow).execution_options(populate_existing=True))}
    controls = list(await session.scalars(select(TelegramCallbackRow).execution_options(populate_existing=True)))
    candidates = {row.id for row in journals.values() if row.kind in {"decide", "respond"} and "request_activation" in mapping(row.continuation)}
    candidates.update(row.payload["work_action_id"] for row in controls if row.kind in {"decide_confirm_resume", "voice_save_resume"} and type(row.payload.get("work_action_id")) is str)
    candidates.update(
        value["act_id"]
        for row in journals.values()
        if type((value := mapping(mapping(row.continuation).get("activation_parent"))).get("act_id")) is str and value["act_id"] in journals and journals[value["act_id"]].kind in {"decide", "respond"}
    )
    if not candidates:
        return result
    deliveries = {row.id: row for row in await session.scalars(select(TelegramOutboxRow).execution_options(populate_existing=True))}
    marks = {(row.link_id, row.message_id, row.chat_id): row for row in await session.scalars(select(TelegramInboundMarkRow).execution_options(populate_existing=True))}
    by_parent, by_anchor, by_act = (defaultdict(list) for _ in range(3))
    for row in controls:
        if type(row.payload.get("work_action_id")) is str:
            by_parent[row.payload["work_action_id"]].append(row)
        if type(row.payload.get("anchor_outbox_id")) is str:
            by_anchor[row.payload["anchor_outbox_id"]].append(row)
    for row in deliveries.values():
        payload = row.payload
        identity = mapping(mapping(mapping(payload.get("work_outcome_basis")).get("context")).get("act")).get("id")
        if type(identity) is str:
            by_act[identity].append(row.id)
        for key in ("work_execution_intent", "capacity_offer_intent"):
            identity = mapping(payload.get(key)).get("act_id")
            if type(identity) is str:
                by_act[identity].append(row.id)
    attempts = {row.activation_id: row for row in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id.in_([activation_child_id(identity) for identity in sorted(candidates)])))}
    for identity in sorted(candidates):
        parent = journals.get(identity)
        child = journals.get(activation_child_id(identity))
        chosen = next((row for row in by_parent[identity] if row.id == identity), None)
        if parent is None and (child is None or chosen is None or not same_scope(chosen, scope(child))):
            pin_orphan(result, chosen, deliveries, marks, by_anchor, [*by_act[identity], *by_act[activation_child_id(identity)]])
            continue
        custodian = parent if parent is not None else child
        link = scope(custodian)
        source = mapping(mapping(parent.continuation).get("request_activation")) if parent is not None else {}
        members = [*([parent] if parent is not None else []), *([child] if child is not None and same_scope(child, link) and child.instance_id == custodian.instance_id and child.work_id == custodian.work_id else [])]
        member_ids = {value.id for value in members}
        changed = True
        while changed:
            changed = False
            forward = {identity, activation_child_id(identity)} | {value for member in members for value in (member.operation_ids if isinstance(member.operation_ids, list) else []) if type(value) is str}
            for member in journals.values():
                if (
                    member.id in member_ids
                    or not same_scope(member, link)
                    or (member.bot_id, member.telegram_user_id, member.instance_id, member.work_id) != (custodian.bot_id, custodian.telegram_user_id, custodian.instance_id, custodian.work_id)
                ):
                    continue
                references = set(value for value in (member.operation_ids if isinstance(member.operation_ids, list) else []) if type(value) is str)
                reverse = mapping(mapping(member.continuation).get("activation_parent")).get("act_id")
                if member.id in forward or references & member_ids or type(reverse) is str and reverse in member_ids:
                    members.append(member)
                    member_ids.add(member.id)
                    changed = True
        owned = set(by_act[identity]) | set(by_act[activation_child_id(identity)])
        transfer = mapping(mapping(parent.continuation).get("work_outcome_transfer")) if parent is not None else {}
        if type(transfer.get("outbox_id")) is str:
            owned.add(transfer["outbox_id"])
        owned.update(value for value in (mapping(source.get("origin")).get("anchor_outbox_id"), source.get("outcome_id")) if type(value) is str)
        owned.update(row.payload.get("anchor_outbox_id") for row in by_parent[identity] if same_scope(row, link) and type(row.payload.get("anchor_outbox_id")) is str)
        owned = {value for value in owned if value in deliveries and same_scope(deliveries[value], link) and deliveries[value].instance_id in {None, custodian.instance_id} and deliveries[value].work_id in {None, custodian.work_id}}
        topic = original_topic(source, chosen, deliveries, link)
        owned = {value for value in owned if deliveries[value].message_thread_id == topic}
        inputs = set()
        raw_inputs = [source.get("input")]
        for value in list(owned):
            raw_inputs.extend(input_locators(deliveries[value]))
        for raw in raw_inputs:
            mark = original_mark(raw, marks)
            if mark is None or not same_scope(mark, link) or mark.message_thread_id != topic:
                continue
            inputs.add((mark.link_id, mark.message_id, mark.chat_id))
            for data in (mapping(mark.route_basis.get("work_answer")), mapping(mark.route_basis.get("work_input"))):
                value = data.get("source_outbox_id")
                if (
                    type(value) is str
                    and value in deliveries
                    and same_scope(deliveries[value], link)
                    and deliveries[value].message_thread_id == topic
                    and deliveries[value].instance_id == custodian.instance_id
                    and deliveries[value].work_id == custodian.work_id
                ):
                    owned.add(value)
        related = [row for value in owned for row in by_anchor[value] if same_scope(row, link) and row.message_thread_id == deliveries[value].message_thread_id]
        live = any(row.consumed_at is None and aware(now) < aware(row.created_at) + timedelta(days=max_age_days) and (row.expires_at is None or aware(now) < aware(row.expires_at)) for row in related)
        pending = any(deliveries[value].state in {"pending", "sending", "failed"} for value in owned)
        canonical = {identity, *([activation_child_id(identity)] if child is not None else [])}
        closed = parent is not None and member_ids == canonical and await closed_request(session, parent, child, link, attempts.get(activation_child_id(identity)))
        if not closed or live or pending:
            result.acts.update(row.id for row in members)
            result.inputs.update(inputs)
            result.deliveries.update(owned)
            result.groups.update(row.group_id for row in related)
        elif all(aware(row.created_at) < aware(now) - timedelta(days=400) for row in members):
            result.expired_history.update(row.id for row in members)
    result.expired_history.difference_update(result.acts)
    return result

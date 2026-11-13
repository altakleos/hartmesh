"""Original execution consent survives unresolved delivery and cancellation."""

from collections.abc import Mapping

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_execution_sources import qualify_stop_receipt, require_execution_scope
from app.telegram_bots.work_start_retention import closed_activation
from app.telegram_bots.work_stop_retention import closed_scope
from app.telegram_bots.work_stop_retries import parent_of, validate_child
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


def closed_stop(root, journals, attempts, events):
    """A physical follow-up stays held until its exact original settles."""
    try:
        if root.state not in {"done", "refused", "conflict"} or not closed_scope(root):
            return False
        members = [root]
        for ordinal, identity in enumerate(root.operation_ids, 2):
            child = journals[identity]
            validate_child(_snapshot(root), _snapshot(child), ordinal)
            members.append(child)
        if len(members) > 3 or parent_of(_snapshot(root)) is not None:
            return False
        if root.operation_ids:
            if events.get((root.instance_id, root.id)) is not None:
                return False
            if root.state == "done" and (root.result.get("stop_child_id") != members[-1].id or not same_body(root.result.get("receipt"), members[-1].result)):
                return False
        for member in members:
            if member.state not in {"done", "refused", "conflict"} or not closed_scope(member):
                return False
            event = events.get((member.instance_id, member.id))
            if member is root and root.operation_ids:
                continue
            if member.state != "done":
                if event is not None:
                    return False
                continue
            qualify_stop_receipt(event, member)
            scope = member.stop_dispatch
            original = attempts.get(member.body["attempt_id"])
            if (
                event is None
                or (event.work_id, event.action, event.actor_kind, event.actor_id) != (member.work_id, "stop", "human", member.owner_user_id)
                or not same_body(event.request, member.body)
                or original is None
                or original.work_id != member.work_id
                or original.status not in {"succeeded", "failed", "cancelled"}
                or original.settled_at is None
                or original.thread_id != scope["thread_id"]
                or scope["run_id"] is not None
                and original.run_id != scope["run_id"]
            ):
                return False
        return True
    except (ProductSourceRetired, KeyError, TypeError, ValueError, AttributeError):
        return False


async def execution_dependencies(session):
    """Conservative same-binding evidence pins; never renew callback age."""
    deliveries = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_execution_intent"]["act_id"].as_string().is_not(None))))
    if not deliveries:
        return set(), set(), set()
    links = {row.id: row for row in await session.scalars(select(TelegramBotLinkRow))}
    controls = {row.id: row for row in await session.scalars(select(TelegramCallbackRow))}
    identities = {row.payload["work_execution_intent"]["act_id"] for row in deliveries if type(row.payload["work_execution_intent"]["act_id"]) is str}
    journals = {}
    ordered = sorted(identities)
    for start in range(0, len(ordered), 500):
        for journal in await session.scalars(select(TelegramActRow).where(TelegramActRow.id.in_(ordered[start : start + 500]))):
            journals[journal.id] = journal
    children = sorted({identity for journal in journals.values() for identity in (journal.operation_ids if isinstance(journal.operation_ids, list) else []) if type(identity) is str})
    for start in range(0, len(children), 500):
        for journal in await session.scalars(select(TelegramActRow).where(TelegramActRow.id.in_(children[start : start + 500]))):
            journals[journal.id] = journal
    attempts, activation_attempts, events = {}, {}, {}
    if any(journal.kind in {"activate", "stop"} for journal in journals.values()):
        targets = sorted({journal.body.get("attempt_id") for journal in journals.values() if journal.kind == "stop" and isinstance(journal.body, Mapping) and type(journal.body.get("attempt_id")) is str})
        for start in range(0, len(targets), 500):
            for attempt in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.id.in_(targets[start : start + 500]))):
                attempts[attempt.id] = attempt
        for start in range(0, len(ordered), 500):
            for attempt in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id.in_(ordered[start : start + 500]))):
                activation_attempts[(attempt.work_id, attempt.activation_id)] = attempt
        operations = sorted(journals)
        for start in range(0, len(operations), 500):
            for event in await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id.in_(operations[start : start + 500]))):
                events[(event.instance_id, event.operation_id)] = event
    pinned_groups, anchors, held_acts = set(), set(), set()
    for delivery in deliveries:
        link = links.get(delivery.link_id)
        if link is None or (delivery.owner_user_id, delivery.chat_id, delivery.binding_generation, delivery.instance_id) != (link.owner_user_id, link.chat_id, link.binding_generation, link.agent_ref):
            continue
        intent = delivery.payload["work_execution_intent"]
        journal, control = journals.get(intent["act_id"]), controls.get(intent["act_id"])
        parent = await session.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control else None
        closed = False
        if journal is not None:
            try:
                require_execution_scope(link, _snapshot(journal))
                if journal.work_id == delivery.work_id:
                    closed = closed_activation(journal, link, activation_attempts.get((journal.work_id, journal.id)), work_id=delivery.work_id) if journal.kind == "activate" else closed_stop(journal, journals, attempts, events)
            except (ProductSourceRetired, KeyError, TypeError, ValueError, AttributeError):
                pass
        if not closed or delivery.state in {"pending", "sending", "failed"}:
            anchors.add(delivery.id)
            if type(intent.get("act_id")) is str:
                held_acts.add(intent["act_id"])
            if journal is not None and isinstance(journal.operation_ids, list):
                held_acts.update(identity for identity in journal.operation_ids if type(identity) is str)
            if control is not None and (control.link_id, control.binding_generation, control.chat_id) == (link.id, link.binding_generation, link.chat_id):
                pinned_groups.add(control.group_id)
            if parent is not None and (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id) == (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                anchors.add(parent.id)
            for part in delivery.parts:
                if type(part.get("callback_group_id")) is str:
                    pinned_groups.add(part["callback_group_id"])
    return pinned_groups, anchors, held_acts

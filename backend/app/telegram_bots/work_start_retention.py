"""Unresolved Start reservations keep their original callback and full ACK."""

from collections.abc import Mapping

from sqlalchemy import and_, select

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow


def closed_start(control, journal, binding, attempt, *, context=None):
    """The displayed revision and reservation must own the exact activation."""
    from app.telegram_bots.work_actions import request_for

    try:
        if control.kind == "work_activation_origin":
            from app.telegram_bots.work_actions import same_body
            from app.telegram_bots.work_news_carriers import news_witness_id

            return (
                journal is not None
                and control.id == news_witness_id(journal.id)
                and control.payload.get("work_action_id") == journal.id
                and same_body(control.payload.get("news_origin"), (journal.continuation or {}).get("news_origin"))
                and closed_activation(journal, binding, attempt, work_id=control.target_id, context=context)
            )
        if journal is None or journal.id != control.id or control.payload.get("work_action_id") != journal.id or type(control.payload.get("assignment_revision")) is not int:
            return False
        body = request_for("activate", journal.body, identity=journal.id)
        return body.expected_assignment_revision == control.payload["assignment_revision"] and closed_activation(journal, binding, attempt, work_id=control.target_id, context=context)
    except (TypeError, KeyError, ValueError, AttributeError):
        return False


def closed_activation(journal, binding, attempt, *, work_id, context=None):
    """Exact original activation outcome, independent of callback lifetime."""
    from app.telegram_bots.work_actions import request_for, same_body
    from deerflow.agent_instances.work import UNRESOLVED

    try:
        if journal is None or journal.kind not in {"activate", "activate_context"} or journal.state not in {"done", "refused", "conflict"}:
            return False
        if journal.kind == "activate" and (journal.operation_ids or "activation_context" in (journal.continuation or {}) or context is not None and any(value is not None for value in context[:3])):
            return closed_context(journal, binding, context, work_id=work_id)
        body = request_for("activate", journal.body, identity=journal.id)
        if (journal.link_id, journal.owner_user_id, journal.bot_id, journal.binding_generation, journal.telegram_user_id, journal.chat_id, journal.instance_id, journal.work_id, journal.request_id, journal.message_id) != (
            binding.id,
            binding.owner_user_id,
            binding.bot_id,
            binding.binding_generation,
            binding.telegram_user_id,
            binding.chat_id,
            binding.agent_ref,
            work_id,
            None,
            None,
        ) or not same_body(journal.body, body.model_dump(mode="json")):
            return False
        if attempt is not None and (
            (attempt.work_id, attempt.activation_id, attempt.requester_id, attempt.thread_id) != (journal.work_id, journal.id, journal.owner_user_id, body.thread_id) or not same_body(attempt.request, body.model_dump(mode="json"))
        ):
            return False
        if journal.state != "done":
            # An exact replay can be refused while its original runless launch
            # promise remains unresolved. That refusal is no settlement proof.
            return type(journal.reason) is str and bool(journal.reason) and (attempt is None or attempt.status not in UNRESOLVED)
        result = journal.result
        receipt = result.get("activation_receipt") if isinstance(result, Mapping) else None
        return (
            journal.reason is None
            and isinstance(receipt, Mapping)
            and set(receipt) == {"operation_id", "attempt_id"}
            and receipt["operation_id"] == journal.id
            and attempt is not None
            and receipt["attempt_id"] == attempt.id
            and result.get("id") == journal.work_id
            and result.get("instance_id") == journal.instance_id
            and (attempt.run_id is not None or attempt.status not in UNRESOLVED)
        )
    except (TypeError, KeyError, ValueError, AttributeError):
        return False


def closed_context(root, binding, context, *, work_id):
    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.work_actions import same_body
    from app.telegram_bots.work_start_conversations import context_plan, qualify_context_receipt, require_context_child

    try:
        marker = context_plan(_snapshot(root))
        if (root.link_id, root.owner_user_id, root.bot_id, root.binding_generation, root.telegram_user_id, root.chat_id, root.instance_id, root.work_id) != (
            binding.id,
            binding.owner_user_id,
            binding.bot_id,
            binding.binding_generation,
            binding.telegram_user_id,
            binding.chat_id,
            binding.agent_ref,
            work_id,
        ):
            return False
        if context is None:
            return False
        child, attempt, event, original_exists = context
        if original_exists:
            return False
        if child is None:
            return marker["phase"] == "planning" and root.state == "refused" and root.reason == "cancelled" and root.cancel_requested_at is not None and attempt is None and event is None
        require_context_child(_snapshot(root), _snapshot(child))
        if not closed_activation(child, binding, attempt, work_id=work_id):
            return False
        if root.state == "done":
            qualify_context_receipt(_snapshot(root), _snapshot(child), event, attempt, root.result["receipt"])
            return child.state == "done" and root.result.get("activation_context_child_id") == child.id and same_body(root.result["receipt"], child.result)
        return marker["phase"] == "ended" and root.state == "refused" and child.state in {"refused", "conflict"} and attempt is None and event is None
    except (TypeError, KeyError, ValueError, AttributeError):
        return False


async def context_facts(session, root, *, force=False):
    from app.telegram_bots.work_start_conversations import context_child_id, operation_absent
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    if root is None or root.kind != "activate" or not (force or root.operation_ids or "activation_context" in (root.continuation or {})):
        return None
    identity = context_child_id(root.id)
    child = await session.get(TelegramActRow, identity)
    attempts = list(await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == identity)))
    events = list(await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)))
    return child, attempts[0] if len(attempts) == 1 else None, events[0] if len(events) == 1 else None, not await operation_absent(session, root.id) or len(attempts) > 1 or len(events) > 1


async def closed_context_member(session, child, binding):
    """An internal capacity owner closes only through its actual Start family."""
    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.work_execution_sources import require_execution_scope
    from app.telegram_bots.work_start_conversations import require_context_child

    marker = (child.continuation or {}).get("activation_context_parent")
    if child.kind != "activate_context" or not isinstance(marker, Mapping) or type(marker.get("root_id")) is not str:
        return False
    root = await session.get(TelegramActRow, marker["root_id"], populate_existing=True)
    if root is None:
        return False
    require_execution_scope(binding, _snapshot(root))
    require_context_child(_snapshot(root), _snapshot(child))
    return closed_activation(root, binding, None, work_id=child.work_id, context=await context_facts(session, root, force=True))


async def context_owners(session):
    """Pin-only discovery survives loss of optional JSON relation markers."""
    from app.telegram_bots.work_actions import _ACT_SCOPE
    from app.telegram_bots.work_start_conversations import context_child_id
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow

    scope = [getattr(TelegramActRow, key) for key in _ACT_SCOPE if key != "kind"]
    roots = {context_child_id(row[0]): row for row in await session.execute(select(TelegramActRow.id, *scope).where(TelegramActRow.kind == "activate"))}
    identities, result = sorted(roots), set()
    for start in range(0, len(identities), 500):
        batch = identities[start : start + 500]
        children = await session.execute(select(TelegramActRow.id, *scope).where(TelegramActRow.id.in_(batch), TelegramActRow.kind == "activate_context"))
        for child in children:
            root = roots[child.id]
            if tuple(child[1:]) == tuple(root[1:]):
                result.add(root.id)
        event_owners = select(AgentWorkEventRow.operation_id, AgentWorkEventRow.instance_id, AgentWorkEventRow.work_id, AgentWorkEventRow.actor_id).where(
            AgentWorkEventRow.operation_id.in_(batch),
            AgentWorkEventRow.action == "activate",
            AgentWorkEventRow.actor_kind == "human",
        )
        attempt_owners = (
            select(AgentWorkAttemptRow.activation_id, AgentWorkRow.instance_id, AgentWorkAttemptRow.work_id, AgentWorkAttemptRow.requester_id)
            .join(AgentWorkRow, AgentWorkRow.id == AgentWorkAttemptRow.work_id)
            .where(AgentWorkAttemptRow.activation_id.in_(batch))
        )
        for query in (event_owners, attempt_owners):
            for identity, instance, work, owner in await session.execute(query):
                root = roots[identity]
                if (instance, work, owner) == (root.instance_id, root.work_id, root.owner_user_id):
                    result.add(root.id)
    return result


async def reserved_starts(session):
    """Missing/malformed journals stay held; retention never renews consent."""
    callback, act, link = TelegramCallbackRow, TelegramActRow, TelegramBotLinkRow
    rows = (
        await session.execute(
            select(callback, act, link)
            .join(link, and_(link.id == callback.link_id, link.binding_generation == callback.binding_generation, link.chat_id == callback.chat_id))
            .outerjoin(act, act.id == callback.payload["work_action_id"].as_string())
            .where(callback.kind.in_(("work_start", "work_activation_origin")), callback.target_kind == "work", callback.payload["work_action_id"].as_string().is_not(None))
        )
    ).all()
    identities = sorted({journal.id for _control, journal, _binding in rows if journal is not None and journal.kind == "activate"})
    context_roots = await context_owners(session)
    attempts = {}
    for start in range(0, len(identities), 500):
        for attempt in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id.in_(identities[start : start + 500]))):
            attempts[(attempt.work_id, attempt.activation_id)] = attempt
    groups, anchors = set(), set()
    for control, journal, binding in rows:
        attempt = attempts.get((control.target_id, journal.id)) if journal is not None else None
        if not closed_start(control, journal, binding, attempt, context=await context_facts(session, journal, force=journal is not None and journal.id in context_roots)):
            groups.add(control.group_id)
            if control.kind == "work_activation_origin":
                from app.telegram_bots.work_news_carriers import news_witness_dependencies

                own_groups, own_anchors = await news_witness_dependencies(session, control, journal, binding)
                groups.update(own_groups)
                anchors.update(own_anchors)
                continue
            if type(anchor := control.payload.get("anchor_outbox_id")) is str:
                anchors.add(anchor)
    return groups, anchors

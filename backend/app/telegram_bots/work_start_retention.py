"""Unresolved Start reservations keep their original callback and full ACK."""

from collections.abc import Mapping

from sqlalchemy import and_, select

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow


def closed_start(control, journal, binding, attempt):
    """The displayed revision and reservation must own the exact activation."""
    from app.telegram_bots.work_actions import request_for

    try:
        if journal is None or journal.id != control.id or control.payload.get("work_action_id") != journal.id or type(control.payload.get("assignment_revision")) is not int:
            return False
        body = request_for("activate", journal.body, identity=journal.id)
        return body.expected_assignment_revision == control.payload["assignment_revision"] and closed_activation(journal, binding, attempt, work_id=control.target_id)
    except (TypeError, KeyError, ValueError, AttributeError):
        return False


def closed_activation(journal, binding, attempt, *, work_id):
    """Exact original activation outcome, independent of callback lifetime."""
    from app.telegram_bots.work_actions import request_for, same_body
    from deerflow.agent_instances.work import UNRESOLVED

    try:
        if journal is None or journal.kind != "activate" or journal.state not in {"done", "refused", "conflict"}:
            return False
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


async def reserved_starts(session):
    """Missing/malformed journals stay held; retention never renews consent."""
    callback, act, link = TelegramCallbackRow, TelegramActRow, TelegramBotLinkRow
    rows = (
        await session.execute(
            select(callback, act, link)
            .join(link, and_(link.id == callback.link_id, link.binding_generation == callback.binding_generation, link.chat_id == callback.chat_id))
            .outerjoin(act, act.id == callback.payload["work_action_id"].as_string())
            .where(callback.kind == "work_start", callback.target_kind == "work", callback.payload["work_action_id"].as_string().is_not(None))
        )
    ).all()
    identities = sorted({journal.id for _control, journal, _binding in rows if journal is not None and journal.kind == "activate"})
    attempts = {}
    for start in range(0, len(identities), 500):
        for attempt in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id.in_(identities[start : start + 500]))):
            attempts[(attempt.work_id, attempt.activation_id)] = attempt
    groups, anchors = set(), set()
    for control, journal, binding in rows:
        attempt = attempts.get((control.target_id, journal.id)) if journal is not None else None
        if not closed_start(control, journal, binding, attempt):
            groups.add(control.group_id)
            if type(anchor := control.payload.get("anchor_outbox_id")) is str:
                anchors.add(anchor)
    return groups, anchors

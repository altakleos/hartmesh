"""Expire only closed, receipt-consistent Stop journal groups."""

from collections.abc import Mapping
from datetime import datetime

from sqlalchemy import select

from app.telegram_bots.work_actions import request_for, same_body
from app.telegram_bots.work_stop_retries import parent_of, validate_child
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


def closed_scope(row):
    request = request_for("stop", row.body, identity=row.id)
    scope = row.stop_dispatch
    if scope is None:
        # The pre-call no-running result never had a cancellation follow-up.
        # Missing scopes on any attempted command or group remain unknown.
        return row.state == "refused" and row.reason == "no-running-attempt" and row.attempts == 0 and not row.operation_ids and parent_of(row) is None
    if (
        not isinstance(scope, Mapping)
        or set(scope) != {"attempt_id", "thread_id", "run_id", "state", "attempts", "not_before"}
        or scope["attempt_id"] != request.attempt_id
        or scope["state"] not in {"settled", "ended"}
        or type(scope["thread_id"]) is not str
        or not scope["thread_id"]
        or scope["run_id"] is not None
        and (type(scope["run_id"]) is not str or not scope["run_id"])
        or type(scope["attempts"]) is not int
        or scope["attempts"] < 0
    ):
        return False
    if scope["not_before"] is not None:
        if type(scope["not_before"]) is not str or datetime.fromisoformat(scope["not_before"]).utcoffset() is None:
            return False
    if scope["state"] == "settled" and row.state != "done":
        return False
    return True


async def expired_stop_groups(session, *, cutoff, pending_outcome):
    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.work_outcome_delivery import delivery_receipt
    from app.telegram_bots.work_outcome_transfer import transferred

    rows = list(
        await session.scalars(
            select(TelegramActRow)
            .where(
                TelegramActRow.kind == "stop",
                TelegramActRow.created_at < cutoff,
                TelegramActRow.state.in_(("done", "refused", "conflict")),
                ~pending_outcome,
            )
            .with_for_update(skip_locked=True)
        )
    )
    captured, valid = {row.id: row for row in rows}, {}
    # References outside this age/phase batch still retain their child. Healthy
    # writers only append newly created children, never adopt an old journal.
    referenced_by = {}
    for identity, children in await session.execute(select(TelegramActRow.id, TelegramActRow.operation_ids).where(TelegramActRow.kind == "stop")):
        if isinstance(children, list):
            for child_id in children:
                if type(child_id) is str:
                    referenced_by.setdefault(child_id, set()).add(identity)
    for row in rows:
        try:
            if not closed_scope(row):
                continue
            child = parent_of(_snapshot(row))
            if row.continuation is not None and child is None and delivery_receipt(row.continuation) is None and not transferred(row):
                continue
            event = await session.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == row.instance_id, AgentWorkEventRow.operation_id == row.id))
            if row.operation_ids:
                if child is not None or len(row.operation_ids) > 2 or len(set(row.operation_ids)) != len(row.operation_ids) or event is not None:
                    continue
                if row.state == "done" and (not isinstance(row.result, dict) or row.result.get("stop_child_id") != row.operation_ids[-1]):
                    continue
            elif row.state == "done":
                if event is None or (event.work_id, event.action, event.actor_kind, event.actor_id) != (row.work_id, "stop", "human", row.owner_user_id) or not same_body(event.request, row.body):
                    continue
            elif event is not None:
                continue
            valid[row.id] = (row, child)
        except (ValueError, TypeError, KeyError, AttributeError):
            continue

    deletable = set()
    for row, child in valid.values():
        root_id = child["parent_id"] if child is not None else row.id
        root_entry = valid.get(root_id)
        if root_entry is None:
            continue
        root = root_entry[0]
        members = [root.id, *root.operation_ids]
        if referenced_by.get(root.id) or any(identity not in valid for identity in members) or row.id not in members:
            continue
        try:
            for ordinal, identity in enumerate(root.operation_ids, 2):
                if referenced_by.get(identity) != {root.id}:
                    raise ValueError("ambiguous-stop-parent")
                candidate = captured[identity]
                validate_child(_snapshot(root), _snapshot(candidate), ordinal)
                if root.state == "done" and identity == root.operation_ids[-1] and candidate.state != "done":
                    raise ValueError("unsettled-stop-child")
            deletable.update(members)
        except (ValueError, TypeError, KeyError, AttributeError):
            continue
    return sorted(deletable)

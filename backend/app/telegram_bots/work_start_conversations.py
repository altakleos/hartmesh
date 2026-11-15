"""One conversation recovery child retains the original Start consent and CAS."""

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.authority import AuthorityUnavailable, require
from app.telegram_bots.work_actions import _ACT_SCOPE, _SCOPE, same_body, thaw
from app.telegram_bots.work_authority import prepare_work_owner
from app.telegram_bots.work_start_actions import link_matches
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.spaces.contract import PrincipalRef

CONVERSATION_REFUSALS = frozenset({"conversation-changed", "conversation-mismatch", "space-changed"})
ACTIVATION_KINDS = frozenset({"activate", "activate_context"})


def context_child_id(identity):
    return hashlib.sha256((identity + ":conversation").encode()).hexdigest()[:32]


def context_deadline(root):
    deadline = root.first_attempt_at + timedelta(hours=1)
    marker = (root.continuation or {}).get("activation_parent")
    if marker is not None:
        deadline = min(deadline, datetime.fromisoformat(marker["authorized_at"]) + timedelta(hours=1))
    return deadline


def source_signature(root):
    continuation = thaw(root.continuation) or {}
    origin = {key: value for key, value in continuation.items() if key not in {"activation_retry", "start_context_recovery", "activation_context"}}
    return hashlib.sha256(json.dumps([root.id, [getattr(root, key) for key in _ACT_SCOPE], root.first_attempt_at.isoformat(), thaw(root.body), origin], sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def context_plan(root):
    marker = root.continuation.get("activation_context") if isinstance(root.continuation, Mapping) else None
    if (
        root.kind != "activate"
        or not isinstance(marker, Mapping)
        or set(marker) != {"version", "child_id", "reason", "source_signature", "answered_attempt", "authorized_at", "phase"}
        or type(marker["version"]) is not int
        or marker["version"] != 1
        or marker["authorized_at"] != root.first_attempt_at.isoformat()
        or marker["phase"] not in {"planning", "staged", "ended"}
        or marker["child_id"] != context_child_id(root.id)
        or marker["reason"] not in CONVERSATION_REFUSALS
        or type(marker["answered_attempt"]) is not int
        or marker["answered_attempt"] != root.attempts
        or marker["answered_attempt"] < 1
        or marker["source_signature"] != source_signature(root)
        or tuple(root.operation_ids) != (marker["child_id"],)
    ):
        raise ValueError("invalid-activation-context")
    return marker


def require_context_child(root, child):
    from app.telegram_bots.work_actions import request_for

    marker = context_plan(root)
    request_for(child.kind, child.body, identity=child.id)
    expected = {**thaw(root.body), "operation_id": child.id, "thread_id": child.body.get("thread_id")}
    if (
        child.kind != "activate_context"
        or marker["phase"] not in {"staged", "ended"}
        or child.id != marker["child_id"]
        or child.body.get("thread_id") == root.body.get("thread_id")
        or not same_body(expected, child.body)
        or any(getattr(root, key) != getattr(child, key) for key in _ACT_SCOPE if key != "kind")
        or child.first_attempt_at != root.first_attempt_at
        or child.operation_ids
        or not same_body((child.continuation or {}).get("activation_context_parent"), {"version": 1, "root_id": root.id, "source_signature": marker["source_signature"]})
    ):
        raise ValueError("invalid-activation-context-child")


async def operation_absent(session, identity):
    # Global operation ownership: a conflicting foreign Work must also hold it.
    attempt = await session.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == identity).limit(1))
    event = await session.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == identity).limit(1))
    return attempt is None and event is None


async def original_absent(session, root):
    return await operation_absent(session, root.id)


async def fence_context_child(writer, root, *, now):
    """The caller's binding writer fences both generations atomically."""
    if not root.operation_ids and "activation_context" not in (root.continuation or {}):
        return False
    marker = context_plan(_snapshot(root))
    child = await writer.get(TelegramActRow, marker["child_id"], with_for_update=True, populate_existing=True)
    if child is None:
        return False
    require_context_child(_snapshot(root), _snapshot(child))
    reserved = await writer.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == child.id).limit(1))
    if child.cancel_requested_at is None:
        child.cancel_requested_at = child.updated_at = now
    if child.state != "done":
        conclusive = reserved is None and (not child.attempts or child.state in {"waiting", "refused", "conflict"})
        child.state, child.reason = ("refused", "cancelled") if conclusive else ("uncertain", "cancel-unconfirmed")
        if conclusive:
            root.continuation = {**thaw(root.continuation), "activation_context": {**thaw(marker), "phase": "ended"}}
    await writer.flush()
    return reserved is not None or child.state == "uncertain"


async def context_receipt(acts, root):
    from app.telegram_bots.work_actions import request_for
    from deerflow.agent_instances.work_execution import WorkExecution

    marker = context_plan(root)
    async with acts.session_factory() as reader:
        child = await reader.get(TelegramActRow, marker["child_id"])
        if child is None:
            return None
        require_context_child(root, _snapshot(child))
        if not await original_absent(reader, root):
            raise ValueError("conflicting-activation-context")
        child = _snapshot(child)
    receipt = await WorkExecution(acts.work).activation_receipt(actor=PrincipalRef("human", root.owner_user_id), instance_id=root.instance_id, work_id=root.work_id, request=request_for(child.kind, child.body, identity=child.id))
    if receipt is None:
        return None
    async with acts.session_factory() as reader:
        actual = await reader.get(TelegramActRow, child.id, populate_existing=True)
        if actual is None:
            raise ValueError("missing-activation-context-child")
        child = _snapshot(actual)
        events = list(await reader.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == child.id)))
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child.id)))
        if len(events) != 1 or len(attempts) != 1:
            raise ValueError("ambiguous-activation-context-operation")
        event, attempt = events[0], attempts[0]
        receipt = thaw(child.result) if child.state == "done" else receipt
        qualify_context_receipt(root, child, event, attempt, receipt)
    return {"activation_context_child_id": child.id, "receipt": thaw(receipt)}


def qualify_child_operation(child, event, attempt, *, allow_absent=False):
    if allow_absent and event is None and attempt is None:
        return
    if (
        event is None
        or attempt is None
        or (event.instance_id, event.work_id, event.operation_id, event.action, event.actor_kind, event.actor_id) != (child.instance_id, child.work_id, child.id, "activate", "human", child.owner_user_id)
        or not same_body(event.request, child.body)
        or (attempt.work_id, attempt.activation_id, attempt.requester_id, attempt.thread_id) != (child.work_id, child.id, child.owner_user_id, child.body["thread_id"])
        or not same_body(attempt.request, child.body)
    ):
        raise ValueError("invalid-activation-context-operation")


def exact_runless_promise(child, event, attempt, *, deadline):
    from app.telegram_bots.notifications import aware
    from deerflow.agent_instances.work import UNRESOLVED

    if event is None and attempt is None:
        return False
    qualify_child_operation(child, event, attempt)
    return attempt.run_id is None and attempt.status in UNRESOLVED and aware(attempt.created_at) < deadline and aware(event.created_at) < deadline


async def existing_context_promise(acts, root):
    marker = context_plan(root)
    async with acts.session_factory() as reader:
        child = await reader.get(TelegramActRow, marker["child_id"])
        if child is None or not await original_absent(reader, root):
            return False
        child = _snapshot(child)
        require_context_child(root, child)
        events = list(await reader.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == child.id)))
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child.id)))
        if len(events) != 1 or len(attempts) != 1:
            return False
        return exact_runless_promise(child, events[0], attempts[0], deadline=context_deadline(root))


def qualify_context_receipt(root, child, event, attempt, receipt):
    from deerflow.agent_instances.work import UNRESOLVED

    require_context_child(root, child)
    qualify_child_operation(child, event, attempt)
    if (
        attempt.run_id is None
        and attempt.status in UNRESOLVED
        or not isinstance(receipt, Mapping)
        or receipt.get("id") != child.work_id
        or receipt.get("instance_id") != child.instance_id
        or not same_body(receipt.get("activation_receipt"), {"operation_id": child.id, "attempt_id": attempt.id})
    ):
        raise ValueError("invalid-activation-context-receipt")


async def complete_context_child(acts, root, result):
    receipt = await context_receipt(acts, root)
    if (
        receipt is None
        or not isinstance(result, Mapping)
        or result.get("activation_context_child_id") != receipt["activation_context_child_id"]
        or not same_body(result.get("receipt", {}).get("activation_receipt"), receipt["receipt"]["activation_receipt"])
    ):
        raise ValueError("invalid-activation-context-receipt")
    child = await acts._work_record(receipt["activation_context_child_id"])
    settled = await acts._complete_activation(child, receipt["receipt"])
    if settled.state != "done":
        raise ValueError("unsettled-activation-context-child")
    return {"activation_context_child_id": child.id, "receipt": thaw(settled.result)}


async def end_context(acts, root, *, uncertain=False):
    async with acts.outbox._writer(root.link_id) as (writer, _link):
        row = await writer.get(TelegramActRow, root.id, with_for_update=True, populate_existing=True)
        if row is None or not same_body(row.body, root.body) or not same_body(row.continuation, root.continuation) or row.state == "done":
            return root if row is None else _snapshot(row)
        marker = context_plan(_snapshot(row))
        row.continuation = {**thaw(row.continuation), "activation_context": {**thaw(marker), "phase": "ended"}}
        row.updated_at = acts.outbox.clock()
        if uncertain:
            row.state = "uncertain"
        await writer.flush()
        return _snapshot(row)


async def plan_context(acts, root, *, reason, attempt_current):
    """Persist the answered original refusal before preparing any new resource."""
    if root.kind != "activate" or root.operation_ids or "activation_context" in (root.continuation or {}):
        return None
    receipt = await acts.receipt_for(root)
    if receipt is not None:
        return await acts._complete_activation(root, receipt)
    root = await acts._activation_outcome(root, state="refused", reason=reason, answered=True)
    link = await acts.links.get(root.link_id)
    if not link_matches(link, root):
        return root
    owner = await prepare_work_owner(acts.authority, root.owner_user_id)
    async with acts.outbox._writer(root.link_id) as (writer, current):
        row = await writer.get(TelegramActRow, root.id, with_for_update=True, populate_existing=True)
        if (
            row is None
            or any(getattr(row, key) != getattr(root, key) for key in _ACT_SCOPE)
            or not same_body(row.body, root.body)
            or not same_body(row.continuation, root.continuation)
            or row.state != "refused"
            or row.reason != reason
            or row.attempts != root.attempts
            or row.cancel_requested_at is not None
            or current is None
            or any(getattr(current, key) != getattr(link, key) for key in _SCOPE)
            or not await original_absent(writer, root)
        ):
            return root
        row.operation_ids = [context_child_id(root.id)]
        row.continuation = {
            **thaw(row.continuation),
            "activation_context": {
                "version": 1,
                "child_id": row.operation_ids[0],
                "reason": reason,
                "source_signature": source_signature(root),
                "answered_attempt": root.attempts,
                "authorized_at": root.first_attempt_at.isoformat(),
                "phase": "planning",
            },
        }
        row.updated_at = acts.outbox.clock()
        await writer.flush()
        await owner(root.owner_user_id, writer)
        if not attempt_current() or acts.outbox.clock() >= context_deadline(root):
            raise AuthorityUnavailable()
        return _snapshot(row)


async def resume_context(acts, root, *, attempt_current, explicit=False, retry_expected=None):
    """Recover the single deterministic child; never refresh original Work CAS."""
    marker = context_plan(root)
    receipt = await context_receipt(acts, root)
    if receipt is not None:
        return await acts._complete_activation(root, receipt)
    if acts.outbox.clock() >= context_deadline(root) and not (explicit and await existing_context_promise(acts, root)):
        return await end_context(acts, root, uncertain=True)
    if root.state == "done" or root.cancel_requested_at is not None or not attempt_current():
        return root
    link = await acts.links.get(root.link_id)
    if not link_matches(link, root):
        raise AgentDenied("The action's binding is no longer current")
    await acts._work_link(link, kind="activate", writable=True)
    await require(root.owner_user_id, "agents:read", "agents:write", "runs:create", "threads:write")
    async with acts.session_factory() as reader:
        child = await reader.get(TelegramActRow, marker["child_id"])
        if child is not None:
            require_context_child(root, _snapshot(child))
            child = _snapshot(child)
        free = await original_absent(reader, root)
    if not free:
        return await end_context(acts, root, uncertain=True)
    if child is None:
        if marker["phase"] != "planning":
            return await end_context(acts, root, uncertain=True)
        conversation = await acts.authority.app.state.agent_conversations.create(
            actor=PrincipalRef("human", root.owner_user_id),
            instance_id=root.instance_id,
            creation_id=marker["child_id"],
            metadata={"channel_source": {"type": "im_channel", "provider": "telegram", "connection_id": link.connection_id}, "work_id": root.work_id},
        )
        owner = await prepare_work_owner(acts.authority, root.owner_user_id)
        async with acts.outbox._writer(root.link_id) as (writer, current):
            row = await writer.get(TelegramActRow, root.id, with_for_update=True, populate_existing=True)
            if row is None:
                return root
            if not same_body(row.body, root.body) or not same_body(row.continuation, root.continuation) or row.cancel_requested_at is not None or row.state != "refused":
                return _snapshot(row)
            context_plan(_snapshot(row))
            if current is None or any(getattr(current, key) != getattr(link, key) for key in _SCOPE) or current.state != "connected" or current.health != "ok" or not await original_absent(writer, root):
                raise AuthorityUnavailable()
            child_row = await writer.get(TelegramActRow, marker["child_id"], with_for_update=True, populate_existing=True)
            if child_row is None:
                if not await operation_absent(writer, marker["child_id"]):
                    row.continuation = {**thaw(row.continuation), "activation_context": {**thaw(marker), "phase": "ended"}}
                    row.state, row.updated_at = "uncertain", acts.outbox.clock()
                    await writer.flush()
                    return _snapshot(row)
                now = acts.outbox.clock()
                child_row = TelegramActRow(
                    id=marker["child_id"],
                    **{key: getattr(root, key) for key in _ACT_SCOPE if key != "kind"},
                    kind="activate_context",
                    body={**thaw(root.body), "operation_id": marker["child_id"], "thread_id": conversation["thread_id"]},
                    continuation={"version": 1, "activation_context_parent": {"version": 1, "root_id": root.id, "source_signature": marker["source_signature"]}},
                    state="pending",
                    operation_ids=[],
                    attempts=0,
                    first_attempt_at=root.first_attempt_at,
                    created_at=now,
                    updated_at=now,
                )
                writer.add(child_row)
                row.continuation = {**thaw(row.continuation), "activation_context": {**thaw(row.continuation["activation_context"]), "phase": "staged"}}
                await writer.flush()
            require_context_child(_snapshot(row), _snapshot(child_row))
            await owner(root.owner_user_id, writer)
            if not attempt_current() or acts.outbox.clock() >= context_deadline(root):
                raise AuthorityUnavailable()
            child = _snapshot(child_row)
            root = _snapshot(row)
    interval = acts.outbox.config.queue.retry_seconds if child.state == "waiting" and child.reason == "busy" else acts.outbox.config.acts.retry_seconds
    if not explicit and child.attempts and child.state in {"pending", "waiting"} and acts.outbox.clock() - child.updated_at < timedelta(seconds=interval):
        return await acts._work_record(root.id)
    async with acts._work_serial(child.id):
        child = await acts._attempt_activation(child, explicit=explicit, attempt_current=attempt_current, retry_expected=retry_expected)
    receipt = await context_receipt(acts, await acts._work_record(root.id))
    if receipt is not None:
        return await acts._complete_activation(root, receipt)
    if child.state in {"refused", "conflict"} or child.state == "waiting" and child.reason == "work-limit":
        return await end_context(acts, await acts._work_record(root.id))
    return await acts._work_record(root.id)

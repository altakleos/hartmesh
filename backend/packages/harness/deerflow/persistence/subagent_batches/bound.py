"""Transactional ownership of a job that executes inside an instance conversation run."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from deerflow_extension_api.agent_runs import ExecutionFence
from sqlalchemy import case, func, or_, select, text, update

from deerflow.mcp_scope import THREAD_INCARNATION_CONTEXT_KEY
from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.extension_leases.model import ExtensionExecutionLeaseRow
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.subagent_batches.model import SubagentBatchItemRow as Item
from deerflow.persistence.subagent_batches.model import SubagentBatchRow as Batch
from deerflow.persistence.subagent_batches.scope import known_scope_candidate
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.user.model import UserRow
from deerflow.runtime.runs.schemas import GOAL_RUN_HARD_STOP_REASONS
from deerflow.subagents.batch_runtime import BATCH_CONTINUATION_NAMESPACE, batch_continuation_key
from deerflow.subagents.batch_scope import is_ordinary_batch_spec, known_agent_batch_scope
from deerflow.utils.visible_message import is_visible, visible_text

UNFINISHED = ("pending", "queued", "leased", "running")
ACTIVE_RUN = ("pending", "running")
LOST = ("orphan_recovered", "gateway_stopped")


@dataclass(frozen=True)
class BoundBatchExecutionFence(ExecutionFence):
    """Host-only expected identity carried through ordinary SQL run admission."""

    scope: dict
    thread_incarnation: str
    thread_generation: str


def utc(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def carry(batch):
    doc = batch.continuation_json
    if not isinstance(doc, dict):
        return None
    state = doc.get("carry")
    if not isinstance(state, dict) or type(state.get("version")) is not int or state["version"] != 1:
        return None
    if type(state.get("replacements")) is not int or state["replacements"] not in (0, 1):
        return None
    return state


def set_carry(batch, **changes):
    batch.continuation_json = {**batch.continuation_json, "carry": {**carry(batch), **changes}}


def dispatch_key(batch):
    return batch_continuation_key(batch.id) + (":replacement" if carry(batch)["replacements"] else "")


def unsettled_final(session):
    doc = Batch.continuation_json
    if session.get_bind().dialect.name == "sqlite":
        safe_doc = case((func.json_valid(doc) == 1, doc), else_=None)
        version = func.json_extract(safe_doc, "$.carry.version") == 1
        outcome = func.json_type(safe_doc, "$.outcome")
    else:
        version = func.json_extract_path_text(doc, "carry", "version") == "1"
        outcome = func.json_typeof(func.json_extract_path(doc, "outcome"))
    return (Batch.continuation_status == "dispatched") & version & outcome.is_(None)


def pending_job(session):
    return or_(Batch.continuation_status.in_(("pending", "dispatching")), unsettled_final(session))


def open_job(batch):
    return batch.continuation_status in ("pending", "dispatching", "dispatched") and not (isinstance(batch.continuation_json, dict) and batch.continuation_json.get("outcome"))


async def matching_thread(session, batch, *, lock=False):
    doc = batch.continuation_json
    scope = known_agent_batch_scope(batch.execution_spec)
    if scope is None or not isinstance(doc, dict) or not isinstance(doc.get("thread_incarnation"), str) or not doc["thread_incarnation"]:
        return False
    query = (
        select(ThreadMetaRow.thread_id)
        .join(AgentConversationRow, AgentConversationRow.thread_id == ThreadMetaRow.thread_id)
        .where(ThreadMetaRow.thread_id == batch.thread_id, ThreadMetaRow.incarnation == doc["thread_incarnation"], AgentConversationRow.instance_id == scope.instance_id)
    )
    if lock:
        query = query.with_for_update()
    return (await session.scalar(query)) is not None


def matching_grant(batch):
    scope = known_agent_batch_scope(batch.execution_spec)
    doc = batch.continuation_json
    grant = doc.get("grant") if isinstance(doc, dict) else None
    return (
        isinstance(grant, dict)
        and batch.execution_spec.get(THREAD_INCARNATION_CONTEXT_KEY) == doc.get("thread_incarnation")
        and scope is not None
        and grant.get("source") == "session"
        and type(grant.get("token_version")) is int
        and grant["token_version"] == scope.token_version
        and grant.get("principal") == {"oauth_id": scope.oauth_id, "oauth_provider": scope.oauth_provider, "system_role": scope.system_role}
    )


async def matching_account(session, batch):
    scope = known_agent_batch_scope(batch.execution_spec)
    account = await session.get(UserRow, batch.user_id, with_for_update=True)
    return (
        account is not None
        and not account.disabled
        and account.email_released_from is None
        and not account.needs_setup
        and account.token_version == scope.token_version
        and account.system_role == scope.system_role
        and account.oauth_id == scope.oauth_id
        and account.oauth_provider == scope.oauth_provider
    )


def check_submission_replay(existing, *, thread_id, run_id, execution_spec):
    if is_ordinary_batch_spec(existing.execution_spec) and is_ordinary_batch_spec(execution_spec):
        return
    if (
        existing.thread_id != thread_id
        or existing.run_id != run_id
        or known_agent_batch_scope(existing.execution_spec) is None
        or known_agent_batch_scope(existing.execution_spec) != known_agent_batch_scope(execution_spec)
        or existing.execution_spec.get(THREAD_INCARNATION_CONTEXT_KEY) != execution_spec.get(THREAD_INCARNATION_CONTEXT_KEY)
    ):
        raise ValueError("Batch submission belongs to another source")


def source_run(batch, run):
    return (
        run is not None
        and run.user_id == batch.user_id
        and run.thread_id == batch.thread_id
        and run.operation_kind == "run"
        and run.origin_kind is None
        and (run.metadata_json or {}).get("agent_instance_id") == known_agent_batch_scope(batch.execution_spec).instance_id
    )


def admitted_run(batch, run):
    return (
        run is not None
        and run.run_id == batch.continuation_run_id
        and run.thread_id == batch.thread_id
        and run.user_id == batch.user_id
        and run.operation_kind == "run"
        and run.origin_kind == "subagent_batch"
        and carry(batch) is not None
        and run.idempotency_key == dispatch_key(batch)
    )


async def reserve_submission(session, batch):
    """Serialize first submissions on the existing physical conversation, including across owners."""
    if session.get_bind().dialect.name == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))
    if not await matching_thread(session, batch, lock=True) or not matching_grant(batch) or not await matching_account(session, batch):
        raise ValueError("Conversation job authority no longer matches")
    existing = await session.scalar(select(Batch).where(Batch.user_id == batch.user_id, Batch.submission_key == batch.submission_key))
    if existing is not None:
        check_submission_replay(existing, thread_id=batch.thread_id, run_id=batch.run_id, execution_spec=batch.execution_spec)
        return existing
    if not batch.run_id or not 1 <= batch.total_items <= 50 or not isinstance(batch.continuation_json.get("prompt"), str) or not batch.continuation_json["prompt"].strip():
        raise ValueError("A conversation job needs a source run, follow-up and at most 50 items")
    origin = await session.get(RunRow, batch.run_id, with_for_update=True)
    if not source_run(batch, origin) or origin.status not in ACTIVE_RUN or origin.cancel_requested_at:
        raise ValueError("Originating conversation run is no longer active")
    peers = await session.scalars(select(Batch).where(Batch.thread_id == batch.thread_id, known_scope_candidate(session), or_(Batch.status.in_(("queued", "paused", "running")), pending_job(session))))
    if any(known_agent_batch_scope(peer.execution_spec) is not None for peer in peers):
        raise ValueError("Another conversation job is still open")
    batch.continuation_json = {**batch.continuation_json, "carry": {"version": 1, "replacements": 0, "thread_generation": uuid4().hex}}
    return None


async def bind_run_admission(session, fence, *, run_id, thread_id, user_id, key, origin, metadata):
    """The same transaction that inserts the Run also grants it this job. Parent before fence."""
    from deerflow.runtime.runs.manager import ConflictError

    if fence.namespace != BATCH_CONTINUATION_NAMESPACE:
        return
    batch = await session.get(Batch, fence.resource_id, with_for_update=True)
    trusted = isinstance(fence, BoundBatchExecutionFence)
    if not trusted and (batch is None or is_ordinary_batch_spec(batch.execution_spec)):
        return
    if not trusted or batch is None or known_agent_batch_scope(batch.execution_spec) is None or carry(batch) is None:
        raise ConflictError("Unsupported conversation job admission")
    if batch.user_id != user_id or batch.thread_id != thread_id or batch.execution_spec["scope"] != fence.scope or batch.continuation_json.get("thread_incarnation") != fence.thread_incarnation or not matching_grant(batch):
        raise ConflictError("Conversation job identity changed")
    if not isinstance(fence.thread_generation, str) or not fence.thread_generation or fence.thread_generation != carry(batch).get("thread_generation"):
        raise ConflictError("Conversation changed during job admission")
    if await session.scalar(select(RunRow.run_id).where(RunRow.thread_id == batch.thread_id, RunRow.status.in_(ACTIVE_RUN)).limit(1)):
        raise ConflictError("Conversation already has an active operation")
    if origin != "subagent_batch" or (metadata or {}).get("agent_instance_id") != fence.scope["instance_id"] or key != dispatch_key(batch):
        raise ConflictError("Conversation job run identity changed")
    if not await matching_thread(session, batch, lock=True) or not await matching_account(session, batch):
        raise ConflictError("Conversation job binding changed")
    source = await session.get(RunRow, batch.run_id, with_for_update=True)
    if not source_run(batch, source) or source.status != "success" or source.stop_reason in GOAL_RUN_HARD_STOP_REASONS:
        raise ConflictError("Originating conversation run did not succeed")
    if batch.continuation_status != "dispatching" or batch.continuation_run_id is not None:
        raise ConflictError("Conversation job is no longer waiting for admission")
    batch.continuation_run_id = run_id
    batch.continuation_status = "dispatched"
    set_carry(batch, wait_reason=None)


@asynccontextmanager
async def job_writer(factory):
    async with factory.begin() as session:
        if session.get_bind().dialect.name == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        yield session


async def locked_job(session, batch_id):
    row = await session.get(Batch, batch_id, with_for_update=True)
    return row if row is not None and known_agent_batch_scope(row.execution_spec) is not None and isinstance(row.continuation_json, dict) else None


async def live_job(session, batch_id, run_id, user_id, thread_id):
    batch = await locked_job(session, batch_id)
    if batch is None or batch.thread_id != thread_id or batch.user_id != user_id or batch.continuation_status != "dispatched" or not open_job(batch) or batch.continuation_run_id != run_id:
        return None
    run = await session.get(RunRow, run_id, with_for_update=True)
    if (
        not admitted_run(batch, run)
        or run.status not in ACTIVE_RUN
        or run.cancel_requested_at is not None
        or run.cancel_action is not None
        or run.producer_state == "revoked"
        or (run.lease_expires_at is not None and utc(run.lease_expires_at) <= datetime.now(UTC))
    ):
        return None
    return batch


async def advance_thread_generation(session, *, thread_id, admitted_batch=None):
    """Invalidate a checkpoint read if another operation commits for the same chat.

    The run-change clock is held. Lock only open bound batches in this physical
    thread, before locking other run rows. A later run deletion cannot resurrect
    an earlier observation. Replayed and refused admissions roll back all changes.
    """
    query = select(Batch).where(Batch.thread_id == thread_id, known_scope_candidate(session), pending_job(session))
    if admitted_batch is not None:
        query = query.where(Batch.id != admitted_batch)
    for batch in await session.scalars(query.order_by(Batch.id).with_for_update()):
        state = carry(batch)
        if state is None or not state.get("thread_generation") or not open_job(batch) or known_agent_batch_scope(batch.execution_spec) is None:
            continue
        set_carry(batch, thread_generation=uuid4().hex)
        if batch.continuation_status == "dispatching":
            batch.continuation_status = "pending"
            await expire_fence(session, batch.id)


async def expire_fence(session, batch_id):
    await session.execute(
        update(ExtensionExecutionLeaseRow).where(ExtensionExecutionLeaseRow.namespace == BATCH_CONTINUATION_NAMESPACE, ExtensionExecutionLeaseRow.resource_id == batch_id).values(expires_at=0, epoch=ExtensionExecutionLeaseRow.epoch + 1)
    )


async def close_job(session, batch, reason, now):
    remaining = await session.execute(
        update(Item)
        .where(Item.batch_id == batch.id, Item.status.in_(UNFINISHED))
        .values(status="cancelled", error="Conversation job ended before this check finished.", lease_owner=None, lease_expires_at=None, cancel_requested_at=now, completed_at=now, updated_at=now)
    )
    if remaining.rowcount or batch.status in ("running", "queued", "paused"):
        batch.status = "cancelled"
        batch.completed_at = now
    batch.continuation_status = "cancelled"
    await record_outcome(session, batch, reason, now)


async def confirmed_reply(session, batch, run):
    """Only the admitted lead's latest journaled model reply can finish a final turn."""
    if carry(batch).get("final_started") is not True:
        return False
    raw = await session.scalar(
        select(RunEventRow.content)
        .where(
            RunEventRow.thread_id == batch.thread_id,
            RunEventRow.run_id == run.run_id,
            RunEventRow.user_id == batch.user_id,
            RunEventRow.category == "message",
            RunEventRow.event_type == "llm.ai.response",
            RunEventRow.event_metadata["caller"].as_string() == "lead_agent",
        )
        .order_by(RunEventRow.seq.desc())
        .limit(1)
    )
    if not raw:
        return False
    try:
        message = json.loads(raw)
        if not isinstance(message, dict) or message.get("type") != "ai" or not message.get("id"):
            return False
        extra = message.get("additional_kwargs", {})
        if message.get("tool_calls") or message.get("invalid_tool_calls") or extra.get("tool_calls") or extra.get("function_call"):
            return False
        return is_visible(message) and bool(visible_text(message))
    except (ValueError, TypeError, AttributeError):
        # A malformed old message is not confirmation that the reply was delivered.
        return False


async def record_outcome(session, batch, reason, now):
    finished = await session.scalar(select(func.count(Item.id)).where(Item.batch_id == batch.id, Item.status.in_(("succeeded", "failed"))))
    batch.continuation_json = {**batch.continuation_json, "outcome": {"version": 1, "reason": reason, "finished": finished, "total": batch.total_items}}
    batch.updated_at = now
    await expire_fence(session, batch.id)


async def reconcile_unavailable_final(session, batch, now):
    """A disabled worker can still reconcile the final reply on startup."""
    state = carry(batch)
    if state is None or not open_job(batch):
        return False
    run = await session.get(RunRow, batch.continuation_run_id, with_for_update=True) if batch.continuation_run_id else None
    if admitted_run(batch, run):
        if run.status in ACTIVE_RUN and (run.lease_expires_at is None or utc(run.lease_expires_at) > now):
            return False
        if await confirmed_reply(session, batch, run):
            await record_outcome(session, batch, "completed", now)
            return True
    await close_job(session, batch, "interrupted", now)
    return True


class BoundBatchDispatch:
    async def list_bound_jobs(self, *, after=None, limit=32):
        if limit <= 0:
            return []
        async with self._sf() as session:
            query = select(Batch.id).where(known_scope_candidate(session), pending_job(session))
            if after is not None:
                query = query.where(Batch.id > after)
            return list(await session.scalars(query.order_by(Batch.id).limit(limit)))

    async def claim_bound_dispatch(self, batch_id, *, lease_owner, lease_seconds, now, start_wait_seconds=3600):
        if not 0 < start_wait_seconds <= 3600 or lease_seconds <= 0:
            raise ValueError("Invalid conversation job dispatch window")
        async with job_writer(self._sf) as session:
            batch = await locked_job(session, batch_id)
            if batch is None or not open_job(batch):
                return None
            state = carry(batch)
            if state is None or not isinstance(state.get("thread_generation"), str) or not state["thread_generation"]:
                return None
            if batch.continuation_run_id is not None:
                if not await self._prepare_replacement(session, batch, now, start_wait_seconds):
                    return None
                state = carry(batch)
            source = await session.get(RunRow, batch.run_id, with_for_update=True) if batch.run_id else None
            if source_run(batch, source) and source.status in ACTIVE_RUN:
                return None
            if not source_run(batch, source) or source.status != "success" or source.stop_reason in GOAL_RUN_HARD_STOP_REASONS:
                await close_job(session, batch, "source_ended", now)
                return None
            if state["replacements"]:
                until = _time(state.get("replace_by"))
            else:
                until = utc(source.updated_at) + timedelta(seconds=start_wait_seconds)
            if until is None or now >= until:
                await close_job(session, batch, "interrupted" if state["replacements"] else "start_timeout", now)
                return None
            if await session.scalar(select(RunRow.run_id).where(RunRow.thread_id == batch.thread_id, RunRow.status.in_(ACTIVE_RUN)).limit(1)):
                return None
            fence = await session.get(ExtensionExecutionLeaseRow, (BATCH_CONTINUATION_NAMESPACE, batch.id), with_for_update=True)
            if batch.continuation_status == "dispatching" and fence is not None and fence.expires_at > now.timestamp():
                return None
            if fence is None:
                fence = ExtensionExecutionLeaseRow(namespace=BATCH_CONTINUATION_NAMESPACE, resource_id=batch.id, user_id=batch.user_id, epoch=0)
                session.add(fence)
            fence.epoch += 1
            fence.holder_id = lease_owner
            fence.expires_at = min((now + timedelta(seconds=lease_seconds)).timestamp(), until.timestamp())
            batch.continuation_status = "dispatching"
            batch.updated_at = now
            return {
                **self._execution_batch_dict(batch),
                "continuation": batch.continuation_json,
                "idempotency_key": dispatch_key(batch),
                "thread_generation": state["thread_generation"],
                "execution_fence": {"namespace": fence.namespace, "resource_id": batch.id, "holder_id": lease_owner, "epoch": fence.epoch},
            }

    async def _prepare_replacement(self, session, batch, now, start_wait_seconds):
        """Reconcile an earlier admission before allowing its sole replacement."""
        state = carry(batch)
        run = await session.get(RunRow, batch.continuation_run_id, with_for_update=True)
        if not admitted_run(batch, run):
            await close_job(session, batch, "interrupted", now)
            return False
        if run.status in ACTIVE_RUN:
            return False
        counts = await self._counts(session, batch.id)
        unfinished = sum(counts[status] for status in UNFINISHED)
        if not unfinished and await confirmed_reply(session, batch, run):
            await record_outcome(session, batch, "completed", now)
            return False
        lost = not run.cancel_requested_at and not run.cancel_action and (run.stop_reason in LOST or run.status == "error")
        loss = utc(run.lease_expires_at or run.updated_at)
        until = loss + timedelta(seconds=start_wait_seconds)
        if not (lost and unfinished and state["replacements"] == 0 and now < until and state.get("final_started") is None):
            await close_job(session, batch, "interrupted" if lost or run.status == "success" else "cancelled", now)
            return False
        await session.execute(update(Item).where(Item.batch_id == batch.id, Item.status.in_(("leased", "running"))).values(status="queued", lease_owner=None, lease_expires_at=None, started_at=None))
        set_carry(batch, replacements=1, replace_by=until.isoformat())
        batch.continuation_run_id = None
        batch.continuation_status = "pending"
        return True

    async def finish_bound_dispatch(self, batch_id, *, execution_fence, status, reason=None):
        if status not in ("waiting", "ended"):
            raise ValueError("Unsupported conversation job decision")
        from deerflow.persistence.extension_leases.fence import lock_execution_fence
        from deerflow.runtime.runs.manager import ConflictError

        fence = ExecutionFence(**execution_fence) if isinstance(execution_fence, dict) else execution_fence
        if fence.namespace != BATCH_CONTINUATION_NAMESPACE or fence.resource_id != batch_id:
            return False
        async with job_writer(self._sf) as session:
            batch = await locked_job(session, batch_id)
            if batch is None or carry(batch) is None or batch.continuation_status != "dispatching":
                return False
            try:
                await lock_execution_fence(session, fence, batch.user_id)
            except ConflictError:
                return False
            if status == "ended":
                await close_job(session, batch, reason or "cancelled", datetime.now(UTC))
            else:
                set_carry(batch, wait_reason=reason)
                batch.continuation_status = "pending"
                await expire_fence(session, batch.id)
            return True

    async def cancel_bound_batch(self, batch_id, *, user_id, reason="cancelled", only_open=False):
        async with job_writer(self._sf) as session:
            batch = await locked_job(session, batch_id)
            if batch is None or batch.user_id != user_id:
                return None
            if not open_job(batch):
                return None if only_open else await self._with_counts(session, batch)
            state = carry(batch)
            if batch.continuation_status == "dispatched" and state is not None and state.get("final_started") is True:
                run = await session.get(RunRow, batch.continuation_run_id, with_for_update=True)
                if admitted_run(batch, run) and run.status not in ACTIVE_RUN and await confirmed_reply(session, batch, run):
                    await record_outcome(session, batch, "completed", datetime.now(UTC))
                    return None if only_open else await self._with_counts(session, batch)
            await close_job(session, batch, reason, datetime.now(UTC))
            return await self._with_counts(session, batch)


def _time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return utc(parsed)

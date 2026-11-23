"""Bookkeeping for helpers owned exclusively by an admitted conversation run."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update

from deerflow.persistence.subagent_batches.bound import UNFINISHED, Batch, Item, _time, carry, job_writer, live_job, set_carry
from deerflow.subagents.acceptance_checks import validate_acceptance_verdict


def deadline(batch, now, work_seconds):
    state = carry(batch)
    if "items_deadline" not in state:
        end = now + timedelta(seconds=work_seconds)
        set_carry(batch, items_deadline=end.isoformat())
        return end
    # A damaged versioned timer must not grant a fresh execution window.
    return _time(state["items_deadline"]) or now


class BoundBatchItems:
    async def get_bound_run(self, batch_id, *, run_id, thread_id, user_id, work_seconds=900):
        if not 0 < work_seconds <= 900:
            raise ValueError("Invalid conversation job work limit")
        async with job_writer(self._sf) as session:
            batch = await live_job(session, batch_id, run_id, user_id, thread_id)
            if batch is None:
                return None
            end = deadline(batch, datetime.now(UTC), work_seconds)
            counts = await self._counts(session, batch.id)
            return {
                **self._execution_batch_dict(batch),
                "subagent_type": batch.subagent_type,
                "continuation": batch.continuation_json,
                "counts": dict(counts),
                "unfinished": sum(counts[status] for status in UNFINISHED),
                "max_running_items": batch.max_running_items,
                "items_deadline": end,
            }

    async def claim_bound_items(self, batch_id, *, run_id, thread_id, user_id, lease_owner, now, limit):
        if limit <= 0 or not lease_owner:
            return []
        async with job_writer(self._sf) as session:
            batch = await live_job(session, batch_id, run_id, user_id, thread_id)
            if batch is None or batch.status not in ("queued", "running") or carry(batch).get("final_started"):
                return []
            end = deadline(batch, now, 900)
            if now >= end:
                return []
            # We hold the verified parent and its run for the entire transaction.
            await self._fail_blocked_dependencies(session, batch, now=now, scope_predicate=Batch.id == batch.id)
            counts = await self._counts(session, batch.id)
            busy = counts["running"] + counts["leased"]
            take = min(limit, batch.max_running_items - busy, batch.max_live_items - busy)
            if take <= 0:
                return []
            rows = await session.scalars(
                select(Item).where(Item.batch_id == batch.id, Item.status.in_(("pending", "queued")), Item.cancel_requested_at.is_(None), self._dependencies_ready()).order_by(Item.position).limit(take).with_for_update()
            )
            claimed = []
            for item in rows:
                item.status = "running"
                item.attempt += 1
                item.lease_owner = lease_owner
                item.lease_expires_at = end
                item.started_at = item.updated_at = now
                item.error = None
                claimed.append({**self._item_dict(item), "prompt": item.prompt, "batch": {**self._execution_batch_dict(batch), "subagent_type": batch.subagent_type}})
            if claimed:
                batch.status = "running"
                batch.updated_at = now
            return claimed

    async def finish_bound_item(
        self, batch_id, item_id, *, run_id, thread_id, user_id, lease_owner, succeeded, result, result_preview, result_truncated, error, stop_reason, token_usage, model_name, completed_at, acceptance_verdict=None, result_artifact=None
    ):
        async with job_writer(self._sf) as session:
            batch = await live_job(session, batch_id, run_id, user_id, thread_id)
            if batch is None or batch.status not in ("queued", "running"):
                return False
            item = await session.get(Item, item_id, with_for_update=True)
            if item is None or item.batch_id != batch.id or item.status != "running" or item.lease_owner != lease_owner or item.cancel_requested_at is not None:
                return False
            end = _time(carry(batch).get("items_deadline"))
            if end is None or completed_at >= end:
                return False
            item.lease_owner = item.lease_expires_at = None
            item.updated_at = completed_at
            item.model_name = model_name
            item.stop_reason = stop_reason
            item.token_usage = token_usage
            if succeeded:
                item.status = "succeeded"
                item.result = result
                item.result_preview = result_preview
                item.result_truncated = result_truncated
                item.result_artifact = result_artifact
                item.acceptance_verdict = validate_acceptance_verdict(acceptance_verdict)
                item.completed_at = completed_at
                item.error = None
            elif item.attempt < batch.max_attempts:
                item.status = "queued"
                item.error = error
                item.started_at = None
            else:
                item.status = "failed"
                item.completed_at = completed_at
                item.error = error
            await session.flush()
            await self._fail_blocked_dependencies(session, batch, now=completed_at, scope_predicate=Batch.id == batch.id)
            await self._refresh_batch_status(session, batch, now=completed_at)
            return True

    async def timeout_bound_items(self, batch_id, *, run_id, thread_id, user_id, now):
        async with job_writer(self._sf) as session:
            batch = await live_job(session, batch_id, run_id, user_id, thread_id)
            if batch is None:
                return False
            end = _time(carry(batch).get("items_deadline"))
            if end is None or now < end:
                return False
            await session.execute(
                update(Item)
                .where(Item.batch_id == batch.id, Item.status.in_(UNFINISHED))
                .values(status="cancelled", stop_reason="time_limit", error="Not run: time limit.", lease_owner=None, lease_expires_at=None, completed_at=now, updated_at=now)
            )
            await self._refresh_batch_status(session, batch, now=now)
            return True

    async def begin_bound_final(self, batch_id, *, run_id, thread_id, user_id, now):
        async with job_writer(self._sf) as session:
            batch = await live_job(session, batch_id, run_id, user_id, thread_id)
            if batch is None or batch.status not in ("completed", "failed") or carry(batch).get("final_started") is not None:
                return False
            if await session.scalar(select(Item.id).where(Item.batch_id == batch.id, Item.status.in_(UNFINISHED)).limit(1)):
                return False
            set_carry(batch, final_started=True)
            batch.updated_at = now
            return True

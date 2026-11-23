"""Inspect or stop durable native batches in this deployment before a planned change."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from sqlalchemy import and_, func, or_, select

from deerflow.persistence.run import RunRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.subagent_batches import SubagentBatchItemRow, SubagentBatchRepository, SubagentBatchRow
from deerflow.persistence.subagent_batches.bound import admitted_run, carry, open_job
from deerflow.persistence.subagent_batches.scope import known_scope_candidate, ordinary_batch
from deerflow.persistence.subagent_batches.sql import BATCH_ACTIVE_STATUSES
from deerflow.subagents.batch_runtime import batch_continuation_key
from deerflow.subagents.batch_scope import is_ordinary_batch_spec, known_agent_batch_scope


class BatchOperator:
    def __init__(self, session_factory):
        self.sessions = session_factory
        self.batches = SubagentBatchRepository(session_factory)
        self.runs = RunRepository(session_factory)

    @staticmethod
    def _kind(batch):
        if known_agent_batch_scope(batch.execution_spec) is not None:
            return "agent"
        if is_ordinary_batch_spec(batch.execution_spec):
            return "personal"
        return None

    @staticmethod
    def _unfinished(batch, live):
        if not is_ordinary_batch_spec(batch.execution_spec):
            # Includes a lost final whose checks are already terminal.
            return carry(batch) is not None and open_job(batch)
        return batch.status in BATCH_ACTIVE_STATUSES or batch.continuation_status in ("pending", "dispatching") or (batch.continuation_status == "dispatched" and live)

    @staticmethod
    def _run_relation():
        # Both keys are host-owned. They also identify an ordinary launch whose
        # acknowledgement was lost before the batch stored its run ID.
        prefix = batch_continuation_key("")
        return and_(
            RunRow.operation_kind == "run",
            RunRow.user_id == SubagentBatchRow.user_id,
            RunRow.thread_id == SubagentBatchRow.thread_id,
            RunRow.origin_kind == "subagent_batch",
            RunRow.idempotency_key.in_([prefix + SubagentBatchRow.id, prefix + SubagentBatchRow.id + ":replacement"]),
        )

    async def list(self, *, after: str | None, limit: int) -> dict:
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        async with self.sessions() as session:
            live = select(RunRow.run_id).where(self._run_relation(), RunRow.status.in_(("pending", "running"))).exists()
            stmt = select(SubagentBatchRow, live).where(
                or_(ordinary_batch(session), known_scope_candidate(session)), or_(SubagentBatchRow.status.in_(BATCH_ACTIVE_STATUSES), SubagentBatchRow.continuation_status.in_(("pending", "dispatching", "dispatched")))
            )
            if after:
                stmt = stmt.where(SubagentBatchRow.id > after)
            candidates = (await session.execute(stmt.order_by(SubagentBatchRow.id).limit(limit + 1))).all()
            jobs = []
            for row, active in candidates[:limit]:
                kind = self._kind(row)
                if kind and self._unfinished(row, active):
                    jobs.append({"batch_id": row.id, "thread_id": row.thread_id, "kind": kind, "status": row.status, "continuation_status": row.continuation_status})
            return {"jobs": jobs, "next_after": candidates[limit - 1][0].id if len(candidates) > limit else None}

    async def cancel(self, batch_id: str, *, wait_seconds: float) -> dict:
        async with self.sessions() as session:
            batch = await session.get(SubagentBatchRow, batch_id)
            kind = self._kind(batch) if batch else None
            if kind is None or (kind == "agent" and carry(batch) is None):
                raise ValueError("Batch unavailable")
            owner = batch.user_id
        if kind == "agent":
            await self.batches.cancel_bound_batch(batch_id, user_id=owner, reason="cancelled", only_open=True)
        else:
            await self.batches.cancel_batch(batch_id, user_id=owner, only_open=True)

        async with self.sessions() as session:
            batch = await session.get(SubagentBatchRow, batch_id)
            if not batch or batch.user_id != owner or self._kind(batch) != kind:
                raise ValueError("Batch unavailable")
            if batch.status != "cancelled" and batch.continuation_status != "cancelled":
                return {"batch_id": batch_id, "cancelled": False, "run_stop": "already_ended", "unconfirmed_items": 0}
            candidates = await session.scalars(select(RunRow).join(SubagentBatchRow, self._run_relation()).where(SubagentBatchRow.id == batch_id, RunRow.status.in_(("pending", "running"))))
            if kind == "agent":
                run_ids = [run.run_id for run in candidates if admitted_run(batch, run)]
            else:
                run_ids = [run.run_id for run in candidates if run.idempotency_key == batch_continuation_key(batch_id)]
            unconfirmed = await session.scalar(
                select(func.count())
                .select_from(SubagentBatchItemRow)
                .where(SubagentBatchItemRow.batch_id == batch_id, SubagentBatchItemRow.status == "cancelled", SubagentBatchItemRow.started_at.is_not(None), SubagentBatchItemRow.cancel_requested_at.is_not(None))
            )
        for run_id in run_ids:
            await self.runs.request_cancel(run_id, action="interrupt")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_seconds
        while run_ids:
            async with self.sessions() as session:
                run_ids = list(await session.scalars(select(RunRow.run_id).where(RunRow.run_id.in_(run_ids), RunRow.status.in_(("pending", "running")))))
            if not run_ids or loop.time() >= deadline:
                break
            await asyncio.sleep(min(0.25, max(0, deadline - loop.time())))
        return {"batch_id": batch_id, "cancelled": True, "run_stop": "unconfirmed" if run_ids else "confirmed", "unconfirmed_items": unconfirmed}


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from deerflow.config import get_app_config
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config

    db = get_app_config().database
    if db.backend == "memory":
        raise ValueError("This command requires durable SQLite or PostgreSQL storage")
    await init_engine_from_config(db, existing_only=True, read_only=args.command == "list")
    try:
        operator = BatchOperator(get_session_factory())
        if args.command == "list":
            return await operator.list(after=args.after, limit=args.limit)
        if not 0 <= args.wait_seconds <= 120:
            raise ValueError("wait-seconds must be between 0 and 120")
        return await operator.cancel(args.batch_id, wait_seconds=args.wait_seconds)
    finally:
        await close_engine()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List a page of open batches and follow-ups")
    listing.add_argument("--after", help="Value of next_after from the previous page")
    listing.add_argument("--limit", type=int, default=100)
    cancel = commands.add_parser("cancel", help="Stop one selected batch and its follow-up")
    cancel.add_argument("batch_id")
    cancel.add_argument("--wait-seconds", type=float, default=10)
    args = parser.parse_args()
    try:
        result = asyncio.run(_run(args))
    except ValueError as error:
        print(json.dumps({"error": str(error)}))
        return 1
    print(json.dumps(result))
    return 2 if result.get("run_stop") == "unconfirmed" or result.get("unconfirmed_items") else 0


if __name__ == "__main__":
    raise SystemExit(main())

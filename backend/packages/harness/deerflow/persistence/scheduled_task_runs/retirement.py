"""Execution holds for terminal scheduled occurrences, using existing lease fields."""

from sqlalchemy import and_, or_

EXECUTION_RETIREMENT_PENDING = "__execution_retirement_pending__"


def retirement_pending(row):
    return and_(row.status.in_(("success", "failed", "interrupted", "skipped")), row.lease_owner == EXECUTION_RETIREMENT_PENDING)


def holds_occurrence(row):
    return or_(row.status.in_(("queued", "launching", "running")), retirement_pending(row))


def holds_execution(row):
    return or_(row.status.in_(("launching", "running")), retirement_pending(row))


def blocks_thread(row, other, *, created_after=None):
    # A retiring worker blocks every claimant on its thread, including older
    # queued rows or rows whose admitting process clock was behind the owner's.
    active_older = and_(other.status.in_(("queued", "launching", "running")), or_(other.created_at < row.created_at, and_(other.created_at == row.created_at, other.id < row.id)))
    if created_after is not None:
        active_older = and_(active_older, or_(other.status != "queued", other.created_at > created_after))
    return and_(other.thread_id == row.thread_id, or_(retirement_pending(other), active_older))

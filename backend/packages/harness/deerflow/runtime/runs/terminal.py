"""Bounded, read-before-recovery terminal persistence for capable run stores."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.exc import SQLAlchemyError

from .schemas import RunStatus
from .store.base import StatusFinalization

logger = logging.getLogger(__name__)
TERMINAL_WRITE_FAILED = "terminal_write_failed"
UNCONFIRMED_STATE = "run_state_unconfirmed"
RECOVERED_METADATA_REASON = "terminal_metadata_recovered"
UNCONFIRMED_ERROR = "The run stopped, but its final state could not be confirmed. Check its status before retrying; contact your administrator if it remains unresolved."
TERMINAL_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})


def exception_classification(exc: BaseException) -> str:
    """Only fixed classifications; exception text and arbitrary class names are unsafe."""
    if isinstance(exc, (ValueError, TypeError)):
        return "validation_error"
    if isinstance(exc, SQLAlchemyError):
        return "database_error"
    if isinstance(exc, (OSError, TimeoutError)):
        return "transport_error"
    return "unexpected_error"


class TerminalWriteFailure(RuntimeError):
    """The store attests to the phase it reached, without retaining payload text."""

    def __init__(self, outcome: str, exception_class: str):
        super().__init__(TERMINAL_WRITE_FAILED)
        self.outcome = outcome
        self.exception_class = exception_class


@dataclass(frozen=True)
class TerminalRecoveryPolicy:
    # Each attempt includes its read and optional conditional write. SQL session
    # rollback/close must finish before an attempt is considered settled.
    max_attempts: int = 3
    attempt_timeout: float = 5.0
    retry_delay: float = 0.1


def diagnostic(record, status, *, outcome, exception_class="none", attempt=0):
    logger.warning(
        "Terminal persistence run_id=%s transition=%s reason=%s outcome=%s exception_class=%s attempt=%d",
        record.run_id,
        status.value,
        TERMINAL_WRITE_FAILED,
        outcome,
        exception_class,
        attempt,
    )


async def persist_terminal(manager, record, status, *, error=None, stop_reason=None, respect_cancellation=False):
    """Confirm or recover one owned outcome; never replay an ambiguous transaction.

    The SQL CAS uses the original observed change position, owner, source status,
    cancellation action and a current lease. After failure, a fresh read either
    confirms an already committed terminal row, returns cancellation to the
    worker, or permits a separate transition for the actual executed outcome.
    A changed active version is fenced, even when its owner string is unchanged.
    """
    store = manager._store
    policy = manager._terminal_recovery_policy
    async with record.terminal_lock:
        if record.ownership_lost:
            return StatusFinalization(finalized=False)
        expected_sequence = None
        recovering = False
        ownership_rejected = False
        for attempt in range(1, policy.max_attempts + 1):
            try:
                async with asyncio.timeout(policy.attempt_timeout):
                    row = await store.get(record.run_id, user_id=record.user_id)
                    rejection = None
                    if row is None:
                        rejection = "missing_row"
                    elif row.get("owner_worker_id") != record.owner_worker_id or record.owner_worker_id != manager.worker_id:
                        rejection = "stale_owner"
                    if rejection:
                        ownership_rejected = True
                        diagnostic(record, status, outcome=rejection, attempt=attempt)
                        break

                    source = row["status"]
                    action = row.get("cancel_action")
                    # An acknowledged interrupt may still be upgraded to the
                    # worker's rollback error after checkpoint cleanup.
                    rollback_tail = source == "interrupted" and status == RunStatus.error and record.abort_event.is_set() and record.abort_action == "rollback"
                    if source in TERMINAL_STATUSES and not rollback_tail:
                        async with manager._lock:
                            record.status = RunStatus(source)
                            record.error = row.get("error")
                            record.stop_reason = row.get("stop_reason")
                            record.updated_at = row["updated_at"]
                            record.terminal_confirmed = True
                        return StatusFinalization(finalized=True)
                    if manager.heartbeat_enabled:
                        deadline = manager._parse_lease_deadline(row.get("lease_expires_at"))
                        if deadline is None or deadline <= datetime.now(UTC):
                            ownership_rejected = True
                            diagnostic(record, status, outcome="lease_expired", attempt=attempt)
                            break
                    if action is not None and respect_cancellation:
                        record.abort_action = action
                        record.abort_event.set()
                        return StatusFinalization(finalized=False, cancel_action=action)
                    if expected_sequence is None:
                        expected_sequence = row["change_seq"]
                    elif row["change_seq"] != expected_sequence:
                        ownership_rejected = True
                        diagnostic(record, status, outcome="stale_version", attempt=attempt)
                        break
                    ownership_rejected = False

                    # Preserve the real worker outcome. Only invalid metadata
                    # shapes may be repaired; never mutate checkpoint state as
                    # a side effect of persistence reconciliation.
                    saved_error, saved_reason = error, stop_reason
                    if recovering:
                        if error is not None and not isinstance(error, str):
                            saved_error = "The run ended with an error; its original error details could not be saved." if status == RunStatus.error else None
                        if stop_reason is not None and (not isinstance(stop_reason, str) or len(stop_reason) > 50):
                            saved_reason = RECOVERED_METADATA_REASON
                    finalized = await store.finalize_owned(
                        record.run_id,
                        owner_worker_id=record.owner_worker_id,
                        expected_change_seq=expected_sequence,
                        expected_status=source,
                        expected_cancel_action=action,
                        require_lease=manager.heartbeat_enabled,
                        status=status.value,
                        error=saved_error,
                        stop_reason=saved_reason,
                    )
                    if finalized:
                        async with manager._lock:
                            record.status = status
                            record.error = saved_error
                            record.stop_reason = saved_reason
                            record.updated_at = datetime.now(UTC).isoformat()
                            record.terminal_confirmed = True
                        return StatusFinalization(finalized=True)
                    ownership_rejected = True
                    diagnostic(record, status, outcome="conditional_rejection", attempt=attempt)
            except Exception as exc:
                diagnostic(
                    record,
                    status,
                    outcome=exc.outcome if isinstance(exc, TerminalWriteFailure) else "unknown",
                    exception_class=exc.exception_class if isinstance(exc, TerminalWriteFailure) else exception_classification(exc),
                    attempt=attempt,
                )
                recovering = True
            if attempt < policy.max_attempts:
                await asyncio.sleep(policy.retry_delay)

        # This is a process-local fence only. Restoring persistence and restarting
        # the stopped Gateway invokes existing startup orphan recovery; no peer
        # heartbeat is needed in the single-worker profile.
        unconfirmed = not ownership_rejected
        if manager.heartbeat_enabled:
            deadline = manager._parse_lease_deadline(record.lease_expires_at)
            unconfirmed = unconfirmed and deadline is not None and deadline > datetime.now(UTC)
        await manager._mark_ownership_lost(record, reason=UNCONFIRMED_ERROR, require_active=False, terminal_write_unconfirmed=unconfirmed)
        return StatusFinalization(finalized=False)

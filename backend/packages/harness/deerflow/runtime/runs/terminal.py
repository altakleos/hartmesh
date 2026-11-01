"""Bounded, read-before-recovery terminal persistence for capable run stores."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.exc import SQLAlchemyError

from deerflow.utils.file_io import await_drained

from .capture_io import CaptureIOClosed, CaptureIOUnsupported, current_capture_io_owner, run_capture_io
from .output import OUTPUT_CAPTURE_KEY, OUTPUT_EVIDENCE_KEY, OutputEvidenceError, capture_from_kwargs, receipt_from_kwargs, receipt_to_dict
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


_FIXED_WORD = re.compile(r"[a-z][a-z0-9]*(?:[-_][a-z0-9]+)+")


def _fixed_word(error: BaseException) -> str | None:
    """One short slug of an exact builtin type. A dot is no separator: file names and hosts have them."""
    if type(error) not in (ValueError, RuntimeError) or len(error.args) != 1:
        return None
    word = error.args[0]
    return word if type(word) is str and len(word) <= 64 and _FIXED_WORD.fullmatch(word) else None


def failure_chain(error: BaseException | None, *, links: int = 4) -> str:
    """Why something failed, as one token a log line can carry.

    `raise ... from None` hides a cause from tracebacks, but `__context__` still
    holds it where the failure is finally reported. Exception text is unsafe, so
    a message prints only when it is a fixed word. Otherwise a link is its class
    and where it was raised, which opens no file.
    """
    parts = []
    seen = set()
    while error is not None and len(parts) < links and id(error) not in seen:
        seen.add(id(error))
        name = type(error).__name__
        part = [name if name.isidentifier() else "Exception"]
        word = _fixed_word(error)
        if word is not None:
            part.append(word)
        frame = error.__traceback__
        if frame is not None:
            while frame.tb_next is not None:
                frame = frame.tb_next
            part.append(f"{os.path.basename(frame.tb_frame.f_code.co_filename)}:{frame.tb_lineno}")
        parts.append(":".join(part))
        error = error.__cause__ if error.__cause__ is not None else error.__context__
    return ">".join(parts)


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


async def persist_terminal(manager, record, status, *, error=None, stop_reason=None, respect_cancellation=False, output_receipt=None, inventory_evidence=None, _inventory_attempt=None):
    """Confirm or recover one owned outcome; never replay an ambiguous transaction.

    The SQL CAS uses the original observed change position, owner, source status,
    cancellation action and a current lease. After failure, a fresh read either
    confirms an already committed terminal row, returns cancellation to the
    worker, or permits a separate transition for the actual executed outcome.
    A changed active version is fenced, even when its owner string is unchanged.
    """
    store = manager._store
    policy = manager._terminal_recovery_policy
    wanted = None
    if output_receipt is not None:
        from .output import validate_receipt_status

        wanted = validate_receipt_status(output_receipt, status)
    wanted_json = receipt_to_dict(wanted) if wanted is not None else None
    if inventory_evidence is not None and wanted is None:
        raise OutputEvidenceError() from None
    async with record.terminal_lock:
        if record.ownership_lost:
            return StatusFinalization(finalized=False)
        expected_sequence = None
        recovering = False
        ownership_rejected = False
        for attempt in range(1, policy.max_attempts + 1):
            try:
                async with asyncio.timeout(policy.attempt_timeout):
                    read = run_capture_io(lambda: store.get(record.run_id, user_id=record.user_id))
                    row = await await_drained(read) if wanted is not None else await read
                    rejection = None
                    if row is None:
                        rejection = "missing_row"
                    elif row.get("owner_worker_id") != record.owner_worker_id or record.owner_worker_id != manager.worker_id:
                        rejection = "stale_owner"
                    saved_receipt = None
                    saved_inventory = None
                    wants_inventory = False
                    if rejection is None and wanted is not None:
                        if row.get("user_id") != wanted.owner_user_id or row.get("thread_id") != wanted.thread_id or row.get("run_id") != wanted.run_id or row.get("operation_kind", "run") != "run":
                            rejection = "output_scope_rejected"
                        else:
                            try:
                                intent = capture_from_kwargs(row.get("kwargs"))
                                if intent is None:
                                    rejection = "output_capture_missing"
                                wants_inventory = intent is not None and intent.version == 2
                                saved_receipt = receipt_from_kwargs(row.get("kwargs"))
                                if wants_inventory:
                                    from .output_inventory import inventory_evidence_from_kwargs, validate_inventory_pair

                                    saved_inventory = inventory_evidence_from_kwargs(row["kwargs"])
                                    wanted, inventory_evidence = validate_inventory_pair(wanted, inventory_evidence, status=status)
                                    if (saved_receipt is None) != (saved_inventory is None):
                                        raise OutputEvidenceError()
                                    if saved_receipt is not None:
                                        validate_inventory_pair(saved_receipt, saved_inventory, status=row["status"])
                                elif inventory_evidence is not None:
                                    raise OutputEvidenceError()
                                if saved_receipt is not None:
                                    validate_receipt_status(saved_receipt, row["status"])
                            except OutputEvidenceError:
                                rejection = "output_evidence_invalid"
                    elif rejection is None:
                        # The ordinary API historically permits null/opaque
                        # kwargs. Only the new literal opt-in selects this guard;
                        # do not run proof decoding for a legacy status write.
                        values = row.get("kwargs")
                        intent = values.get(OUTPUT_CAPTURE_KEY) if isinstance(values, Mapping) else None
                        if isinstance(intent, Mapping) and type(intent.get("version")) is int and intent["version"] == 2:
                            rejection = "output_inventory_missing"
                    if rejection:
                        ownership_rejected = True
                        diagnostic(record, status, outcome=rejection, attempt=attempt)
                        break

                    source = row["status"]
                    action = row.get("cancel_action")
                    # An acknowledged interrupt may still be upgraded to the
                    # worker's rollback error after checkpoint cleanup.
                    rollback_tail = source == "interrupted" and status == RunStatus.error and record.abort_event.is_set() and record.abort_action == "rollback"
                    if wanted is not None:
                        rollback_tail = rollback_tail and action == "rollback" and wanted.kind == "unavailable" and wanted.reason == "cancelled" and saved_receipt == wanted and saved_inventory == inventory_evidence
                    if source in TERMINAL_STATUSES and not rollback_tail:
                        output_confirmed = wanted is not None and source == status.value and saved_receipt == wanted
                        inventory_confirmed = wants_inventory and output_confirmed and saved_inventory == inventory_evidence
                        async with manager._lock:
                            record.status = RunStatus(source)
                            record.error = row.get("error")
                            record.stop_reason = row.get("stop_reason")
                            record.updated_at = row["updated_at"]
                            record.terminal_confirmed = True
                            if wanted is not None:
                                record.kwargs = deepcopy(row["kwargs"])
                        return StatusFinalization(finalized=True, output_receipt_confirmed=output_confirmed, inventory_evidence_confirmed=inventory_confirmed)
                    if wanted is not None and saved_receipt is not None and (saved_receipt != wanted or source in {"pending", "running"}):
                        ownership_rejected = True
                        diagnostic(record, status, outcome="output_evidence_conflict", attempt=attempt)
                        break
                    if manager.heartbeat_enabled:
                        deadline = manager._parse_lease_deadline(row.get("lease_expires_at"))
                        if deadline is None or deadline <= datetime.now(UTC):
                            ownership_rejected = True
                            diagnostic(record, status, outcome="lease_expired", attempt=attempt)
                            break
                    if wants_inventory and _inventory_attempt is not None and _inventory_attempt._cleanup is not None:
                        cleanup = _inventory_attempt._cleanup
                        decision = _inventory_attempt._decision
                        if (
                            action not in {None, cleanup.action}
                            or _inventory_attempt._action_bound
                            and action != _inventory_attempt._action_basis
                            or decision is not None
                            and (row["owner_worker_id"], row["change_seq"], source) != decision._basis[:3]
                        ):
                            ownership_rejected = True
                            diagnostic(record, status, outcome="inventory_cleanup_action_changed", attempt=attempt)
                            break
                        _inventory_attempt._action_basis = action
                        _inventory_attempt._action_bound = True
                    if action is not None and (respect_cancellation or wanted is not None and wanted.kind == "text"):
                        if wants_inventory and _inventory_attempt is not None:
                            from .inventory_finalization import _record_decision

                            _record_decision(_inventory_attempt, row)
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
                    finalized = await run_capture_io(
                        lambda: store.finalize_owned(
                            record.run_id,
                            owner_worker_id=record.owner_worker_id,
                            expected_change_seq=expected_sequence,
                            expected_status=source,
                            expected_cancel_action=action,
                            require_lease=manager.heartbeat_enabled,
                            status=status.value,
                            error=saved_error,
                            stop_reason=saved_reason,
                            **({"output_receipt": wanted} if wanted is not None else {}),
                            **({"inventory_evidence": inventory_evidence} if wants_inventory else {}),
                        )
                    )
                    if finalized:
                        async with manager._lock:
                            record.status = status
                            record.error = saved_error
                            record.stop_reason = saved_reason
                            record.updated_at = datetime.now(UTC).isoformat()
                            record.terminal_confirmed = True
                            if wanted is not None:
                                record.kwargs = {**record.kwargs, OUTPUT_EVIDENCE_KEY: wanted_json}
                                if wants_inventory:
                                    from .output_inventory import INVENTORY_EVIDENCE_KEY, inventory_evidence_to_dict

                                    record.kwargs[INVENTORY_EVIDENCE_KEY] = inventory_evidence_to_dict(inventory_evidence)
                        return StatusFinalization(finalized=True, output_receipt_confirmed=wanted is not None, inventory_evidence_confirmed=wants_inventory)
                    ownership_rejected = True
                    diagnostic(record, status, outcome="conditional_rejection", attempt=attempt)
            except (CaptureIOClosed, CaptureIOUnsupported):
                raise
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
                owner = current_capture_io_owner()
                if owner is None:
                    await asyncio.sleep(policy.retry_delay)
                else:
                    owner.check_open()
                    try:
                        await asyncio.wait_for(owner.wait_retired(), timeout=max(0.0, policy.retry_delay))
                    except TimeoutError:
                        pass
                    owner.check_open()

        # This is a process-local fence only. Restoring persistence and restarting
        # the stopped Gateway invokes existing startup orphan recovery; no peer
        # heartbeat is needed in the single-worker profile.
        unconfirmed = not ownership_rejected
        if manager.heartbeat_enabled:
            deadline = manager._parse_lease_deadline(record.lease_expires_at)
            unconfirmed = unconfirmed and deadline is not None and deadline > datetime.now(UTC)
        await manager._mark_ownership_lost(record, reason=UNCONFIRMED_ERROR, require_active=False, terminal_write_unconfirmed=unconfirmed)
        return StatusFinalization(finalized=False)

"""Host-only inventory finalization handles; never request or persisted values."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .output import OutputReceipt

_ISSUER = object()
_AUTHORITY: ContextVar[object | None] = ContextVar("inventory_finalization_authority", default=None)


class _AuthorityScope:
    __slots__ = ("value", "active")

    def __init__(self, value):
        self.value, self.active = value, True


class InventoryCleanupOutcome:
    """A trusted worker callback's closed result, rather than a cleanup bool."""

    __slots__ = ("_kind", "_receipt")

    def __init__(self, kind, receipt, *, _issuer=None):
        if _issuer is not _ISSUER:
            raise TypeError("Inventory cleanup outcomes require a host factory") from None
        self._kind, self._receipt = kind, receipt

    @property
    def kind(self):
        return self._kind

    @property
    def receipt(self):
        return self._receipt

    @property
    def action(self):
        return {"interrupt_completed": "interrupt", "rollback_completed": "rollback"}.get(self._kind)

    @classmethod
    def _completed(cls, kind, receipt):
        from .output import OutputEvidenceError, validate_receipt

        value = validate_receipt(receipt)
        if value.kind != "unavailable" or value.reason != "cancelled":
            raise OutputEvidenceError() from None
        return cls(kind, value, _issuer=_ISSUER)

    @classmethod
    def interrupt_completed(cls, receipt: OutputReceipt):
        return cls._completed("interrupt_completed", receipt)

    @classmethod
    def rollback_completed(cls, receipt: OutputReceipt):
        return cls._completed("rollback_completed", receipt)

    @classmethod
    def unconfirmed(cls):
        return cls("unconfirmed", None, _issuer=_ISSUER)


class InventoryCancellationDecision:
    """Read-only decision issued by the existing terminal read protocol."""

    __slots__ = ("_attempt", "_action", "_basis")

    def __init__(self, attempt, action, basis, *, _issuer=None):
        if _issuer is not _ISSUER or action not in {"interrupt", "rollback"}:
            raise TypeError("Inventory cancellation decisions require host authority") from None
        self._attempt, self._action, self._basis = attempt, action, basis

    @property
    def action(self):
        return self._action


class InventoryFinalizationAttempt:
    """Opaque original-record handle retained by the manager's existing lifetime."""

    __slots__ = ("_manager", "_record", "_status", "_output", "_inventory", "_candidate_output", "_candidate_inventory", "_cleanup", "_error", "_stop_reason", "_decision", "_action_bound", "_action_basis", "_running", "_result")

    def __init__(self, manager, record, status, output, inventory, cleanup, error, stop_reason, *, _issuer=None):
        if _issuer is not _ISSUER:
            raise TypeError("Inventory finalization attempts require host authority") from None
        self._manager, self._record, self._status = manager, record, status
        self._output, self._inventory, self._cleanup = output, inventory, cleanup
        self._candidate_output, self._candidate_inventory = output, inventory
        self._error, self._stop_reason = error, stop_reason
        self._decision = self._result = None
        self._action_bound = self._running = False
        self._action_basis = None


def _new_attempt(*args):
    return InventoryFinalizationAttempt(*args, _issuer=_ISSUER)


def _record_decision(attempt, row):
    decision = InventoryCancellationDecision(attempt, row["cancel_action"], (row["owner_worker_id"], row["change_seq"], row["status"], row.get("lease_expires_at")), _issuer=_ISSUER)
    attempt._decision = decision
    return decision


@contextmanager
def _inventory_authority(value):
    scope = _AuthorityScope(value)
    token = _AUTHORITY.set(scope)
    try:
        yield
    finally:
        scope.active = False
        _AUTHORITY.reset(token)


def _current_inventory_authority():
    scope = _AUTHORITY.get()
    return scope.value if scope is not None and scope.active else None

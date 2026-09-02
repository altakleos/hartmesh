"""Abstract interface for thread metadata storage.

Implementations:
- ThreadMetaRepository: SQL-backed (sqlite / postgres via SQLAlchemy)
- MemoryThreadMetaStore: wraps LangGraph BaseStore (memory mode)

All mutating and querying methods accept a ``user_id`` parameter with
three-state semantics (see :mod:`deerflow.runtime.user_context`):

- ``AUTO`` (default): resolve from the request-scoped contextvar.
- Explicit ``str``: use the provided value verbatim.
- Explicit ``None``: bypass owner filtering (migration/CLI only).
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any

from deerflow.runtime.user_context import AUTO, _AutoSentinel

# Cross-component metadata key. Keep in sync with
# ``frontend/src/core/threads/utils.ts`` and
# ``frontend/tests/e2e/utils/mock-api.ts``.
THREAD_PINNED_METADATA_KEY = "deerflow_pinned"


class InvalidMetadataFilterError(ValueError):
    """Raised when all client-supplied metadata filter keys are rejected."""


class ThreadMetaAlreadyExistsError(RuntimeError):
    """Raised when a create would replace an existing thread metadata row."""


@dataclass(frozen=True, slots=True)
class ThreadMetaRunProjection:
    """One authority-bound run projection into mutable thread metadata.

    ``active_state_version`` identifies the execution epoch owned by
    ``owner_worker_id``.  A terminal projection additionally carries the exact
    terminal version plus the prior owner/active epoch captured by that
    owner's successful status transition. The store independently proves that
    full authority tuple and that this is still the latest admitted normal run
    before applying either field.
    """

    run_id: str
    thread_id: str
    owner_worker_id: str
    active_state_version: int
    status: str
    terminal_state_version: int | None = None
    display_name: str | None = None

    @property
    def run_status(self) -> str:
        """Return the exact durable run status this projection represents."""

        return "success" if self.status == "idle" else self.status

    def __post_init__(self) -> None:
        if not self.run_id or not self.thread_id or not self.owner_worker_id:
            raise ValueError("run, thread, and owner identifiers are required")
        if type(self.active_state_version) is not int or self.active_state_version < 0:
            raise ValueError("active_state_version must be a non-negative integer")
        if self.status == "running":
            if self.terminal_state_version is not None:
                raise ValueError("a running projection cannot carry a terminal version")
        elif self.status in {"idle", "error", "timeout", "interrupted"}:
            if type(self.terminal_state_version) is not int or self.terminal_state_version <= self.active_state_version:
                raise ValueError("a terminal projection requires a later terminal version")
        else:
            raise ValueError("unsupported run-derived thread status")


class ThreadMetaStore(abc.ABC):
    @abc.abstractmethod
    async def create(
        self,
        thread_id: str,
        *,
        assistant_id: str | None = None,
        user_id: str | None | _AutoSentinel = AUTO,
        display_name: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """Create a row without replacing an existing thread.

        Raise :class:`ThreadMetaAlreadyExistsError` when ``thread_id`` is
        already present.
        """
        pass

    @abc.abstractmethod
    async def get(self, thread_id: str, *, user_id: str | None | _AutoSentinel = AUTO) -> dict | None:
        pass

    @abc.abstractmethod
    async def search(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> list[dict[str, Any]]:
        """Search threads.

        Results are ordered with pinned threads first
        (``metadata.deerflow_pinned is True``), then by ``updated_at`` and
        ``thread_id`` descending within each group.
        """
        pass

    @abc.abstractmethod
    async def update_display_name(
        self,
        thread_id: str,
        display_name: str,
        *,
        remove_metadata_keys: tuple[str, ...] = (),
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> None:
        pass

    @abc.abstractmethod
    async def update_status(self, thread_id: str, status: str, *, user_id: str | None | _AutoSentinel = AUTO) -> None:
        pass

    async def project_run(
        self,
        projection: ThreadMetaRunProjection,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> bool:
        """Conditionally project one authoritative latest run.

        Return ``True`` only when the title/status mutation was applied.  A
        missing row, failed owner check, stale execution fence, or older run is
        a fail-closed ``False``.  Human/admin mutation methods remain separate.
        """

        del projection, user_id
        return False

    @abc.abstractmethod
    async def update_metadata(self, thread_id: str, metadata: dict, *, touch: bool = True, user_id: str | None | _AutoSentinel = AUTO) -> None:
        """Merge ``metadata`` into the thread's metadata field.

        Existing keys are overwritten by the new values; keys absent from
        ``metadata`` are preserved. No-op if the thread does not exist
        or the owner check fails.

        When ``touch`` is ``True`` (default) the row's ``updated_at`` is
        refreshed so the change bumps recency ordering. Pass ``touch=False``
        for metadata that is not conversation activity (e.g. pin/unpin) so the
        thread keeps its place in ``updated_at``-sorted lists.
        """
        pass

    @abc.abstractmethod
    async def update_owner(self, thread_id: str, owner_user_id: str, *, user_id: str | None | _AutoSentinel = AUTO) -> None:
        """Move a thread metadata row to a new owner.

        Intended for trusted internal repair/migration paths. No-op if the
        row does not exist or the caller fails the owner check.
        """
        pass

    @abc.abstractmethod
    async def claim_unowned(self, thread_id: str, owner_user_id: str) -> bool:
        """Atomically assign a legacy NULL-owned row to ``owner_user_id``.

        Return ``True`` only when this call changed the row. Missing rows and
        rows that already have any owner return ``False``. This operation is
        the only supported adoption path; unlike :meth:`update_owner`, it can
        never transfer an owned thread.
        """
        pass

    @abc.abstractmethod
    async def check_access(self, thread_id: str, user_id: str, *, require_existing: bool = False) -> bool:
        """Check if ``user_id`` has access to ``thread_id``."""
        pass

    @abc.abstractmethod
    async def delete(self, thread_id: str, *, user_id: str | None | _AutoSentinel = AUTO) -> None:
        pass

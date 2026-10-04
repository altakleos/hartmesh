"""Single-use output baseline for the worker's first graph invocation.

This capability lives only in the execution Context, never runtime config,
graph state, checkpoint metadata or public events.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from .types import WorkspaceSnapshot


class _OutputBaseline:
    def __init__(self, snapshot: WorkspaceSnapshot, binding: tuple[str, str, str | None, frozenset[str]]) -> None:
        self._snapshot: WorkspaceSnapshot | None = snapshot
        self._binding = binding
        self._lock = threading.Lock()

    def take(self, binding: tuple[str, str, str | None, frozenset[str]]) -> WorkspaceSnapshot | None:
        with self._lock:
            snapshot, self._snapshot = self._snapshot, None
            return snapshot if self._binding == binding else None

    def close(self) -> None:
        with self._lock:
            self._snapshot = None


_baseline: ContextVar[_OutputBaseline | None] = ContextVar("deerflow_output_baseline", default=None)


@contextmanager
def bind_output_snapshot(
    snapshot: WorkspaceSnapshot | None,
    *,
    thread_id: str,
    run_id: str,
    user_id: str | None,
    extra_excluded_dir_names: frozenset[str] | None,
) -> Iterator[None]:
    """Offer a complete worker baseline only during the first graph stream.

    Closing the shared capability also invalidates any copied child Contexts.
    Even an unavailable baseline shadows an outer run's capability.
    """
    eligible = snapshot is not None and not snapshot.truncated and snapshot.text_cache_dir is None and all(file.root == "outputs" and file.text is None and file.text_path is None for file in snapshot.files.values())
    handoff = _OutputBaseline(snapshot, (thread_id, run_id, user_id, extra_excluded_dir_names or frozenset())) if eligible else None
    token = _baseline.set(handoff)
    try:
        yield
    finally:
        if handoff is not None:
            handoff.close()
        _baseline.reset(token)


def take_output_snapshot(
    *,
    thread_id: str,
    run_id: str,
    user_id: str | None,
    extra_excluded_dir_names: frozenset[str] | None,
) -> WorkspaceSnapshot | None:
    """Claim matching execution evidence once; callers scan on any mismatch."""
    handoff = _baseline.get()
    if handoff is None:
        return None
    return handoff.take((thread_id, run_id, user_id, extra_excluded_dir_names or frozenset()))

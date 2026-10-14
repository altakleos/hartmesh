"""Private provenance for one committed, freshly admitted preparation worker."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deerflow.spaces.attachments import AttachmentPlan

_current = ContextVar("storage_committed_preparation", default=None)


@dataclass(eq=False)
class _Preparation:
    provider: object
    plan: AttachmentPlan
    live: bool = True
    _claimed: bool = False
    _lock: object = field(default_factory=Lock, repr=False)
    _retained: list = field(default_factory=list, repr=False)

    def claim(self):
        """A copied context cannot submit a second primary with this intent."""
        with self._lock:
            if not self.live or self._claimed:
                raise ValueError("Preparation token is expired or already claimed")
            self._claimed = True

    def retain_release(self, release):
        with self._lock:
            if not self.live or not callable(release):
                raise ValueError("Only a live preparation can retain coordination")
            self._retained.append(release)

    def finish_worker(self):
        with self._lock:
            self.live = False

    def close(self):
        self.finish_worker()
        with self._lock:
            retained, self._retained = self._retained, []
        for release in reversed(retained):
            release()


def current_preparation():
    token = _current.get()
    return token if isinstance(token, _Preparation) and token.live else None


@contextmanager
def preparation_scope(provider, plan):
    # Only attachment admission mints this, after durable intent and fresh SQL
    # validation. Its owner must drain the worker before this finally runs.
    proof = _Preparation(provider, plan)
    reset = _current.set(proof)
    try:
        yield proof
    finally:
        try:
            proof.close()
        finally:
            _current.reset(reset)

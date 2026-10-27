"""Trusted host qualification of one run's pinned managed output routing.

These objects are capabilities, never runtime/checkpoint data or file access.
The host owns active-principal and actual routing/adapter verification.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from deerflow.config.paths import Paths, peek_scoped_paths

from .capture_io import CaptureIOClosed, CaptureIOOwner, CaptureIOUnsupported, current_capture_io_owner
from .output import OutputEvidenceError
from .output_inventory import NoAdmittedOutputView


@dataclass(frozen=True)
class OutputViewRequest:
    owner_user_id: str
    thread_id: str
    run_id: str
    paths: Paths
    prepared: object
    io_owner: CaptureIOOwner

    def __post_init__(self):
        if any(type(value) is not str or not value or len(value) > bound for value, bound in ((self.owner_user_id, 128), (self.thread_id, 64), (self.run_id, 128))):
            raise OutputEvidenceError() from None
        if not isinstance(self.paths, Paths) or type(self.io_owner) is not CaptureIOOwner:
            raise OutputEvidenceError() from None


def _check_request(request):
    if type(request) is not OutputViewRequest:
        raise OutputEvidenceError() from None
    request.io_owner.check_open()
    if current_capture_io_owner() is not request.io_owner or peek_scoped_paths() is not request.paths:
        raise OutputEvidenceError() from None


class NoAdmittedOutputViewLease:
    """One original host decision with irreversible retirement and fresh audit."""

    def __init__(self, request: OutputViewRequest, *, refresh: Callable[[], Awaitable[None]]):
        _check_request(request)
        if request.prepared is None or not callable(refresh):
            raise OutputEvidenceError() from None
        self._request = request
        self._refresh = refresh
        self._retired = False

    def _check(self, request):
        _check_request(request)
        if request is not self._request or self._retired:
            raise OutputEvidenceError() from None

    def retire(self) -> None:
        self._retired = True

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    async def confirm(self, request: OutputViewRequest) -> NoAdmittedOutputView:
        self._check(request)

        async def perform():
            self._check(request)
            try:
                operation = self._refresh()
                if not inspect.isawaitable(operation):
                    raise OutputEvidenceError()
                if await operation is not None:
                    raise OutputEvidenceError()
                self._check(request)
                return NoAdmittedOutputView(kind="no_admitted_output_view", scope={"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"})
            except (CaptureIOClosed, CaptureIOUnsupported):
                raise
            except Exception:
                raise OutputEvidenceError() from None

        return await request.io_owner.run(perform)


class OutputViewQualifier:
    """Actual host callback support; its presence is not per-run evidence."""

    def __init__(self, issue: Callable[[OutputViewRequest], Awaitable[NoAdmittedOutputViewLease | None]]):
        if not callable(issue):
            raise OutputEvidenceError() from None
        self._issue = issue

    async def __call__(self, request: OutputViewRequest) -> NoAdmittedOutputViewLease | None:
        _check_request(request)

        async def perform():
            _check_request(request)
            try:
                operation = self._issue(request)
                if not inspect.isawaitable(operation):
                    raise OutputEvidenceError()
                result = await operation
                _check_request(request)
                if result is not None:
                    if type(result) is not NoAdmittedOutputViewLease:
                        raise OutputEvidenceError()
                    result._check(request)
                return result
            except (CaptureIOClosed, CaptureIOUnsupported):
                raise
            except Exception:
                raise OutputEvidenceError() from None

        return await request.io_owner.run(perform)

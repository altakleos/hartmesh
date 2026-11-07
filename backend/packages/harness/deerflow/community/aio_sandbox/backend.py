"""Abstract base class for sandbox provisioning backends."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import math
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse

import httpx
import requests

from .sandbox_info import SandboxInfo

logger = logging.getLogger(__name__)


def sandbox_http_trust_env(sandbox_url: str) -> bool:
    """Whether HTTP clients for *sandbox_url* should inherit proxy settings.

    Local Docker, DooD, and Kubernetes sandbox endpoints are control-plane
    connections, not internet traffic. Sending them through ``HTTP_PROXY`` can
    produce a misleading proxy-generated 502 even though the sandbox container
    is healthy (#3441). External fully-qualified hosts retain normal environment
    proxy behavior.
    """
    try:
        hostname = (urlparse(sandbox_url).hostname or "").rstrip(".").lower()
    except ValueError:
        return True
    if not hostname:
        return True
    if hostname == "localhost" or hostname.endswith(".localhost") or hostname.endswith(".docker.internal") or hostname.endswith(".containers.internal"):
        return False
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return "." in hostname
    return not (address.is_loopback or address.is_private or address.is_link_local)


# The default readiness budget, in seconds, that the local-container provider
# paths (sync and async) enforce before destroying a sandbox that never became
# ready. ``sandbox.ready_timeout`` overrides it per deployment: the released
# Compose profile runs one-CPU gVisor sandboxes whose cold start was measured at
# 80 to 91 seconds, so a fixed 60 destroyed every one of them. Tests that
# validate a shipped image must use the deployment's effective budget: a longer
# one can pass while every real acquisition still fails.
SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT = 60
# The largest budget any configuration may ask for. Finite on purpose: the
# budget is a failure deadline, and a value that cannot elapse is no deadline.
SANDBOX_READY_TIMEOUT_MAX = 3600
# One readiness probe; each probe is further clamped to what is left of the
# budget so no request can outlive the deadline.
_READY_REQUEST_TIMEOUT = 5.0
# The clock the sync poller reads. Monotonic, never wall time: an NTP step or a
# suspended host must neither extend nor shorten the budget. Bound to a name so
# tests can drive it.
_monotonic = time.monotonic


def normalize_ready_timeout(value: object) -> float:
    """Return *value* as a readiness budget in seconds, or refuse it.

    Accepts a finite positive ``int`` or ``float`` no larger than
    ``SANDBOX_READY_TIMEOUT_MAX``. Everything else -- zero, a negative number,
    ``inf``, ``nan``, a bool, a string, ``None`` -- raises ``ValueError`` so no
    value can quietly turn the deadline off or into something that is not a
    number of seconds.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"the sandbox readiness budget must be a number of seconds, not {type(value).__name__}")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("the sandbox readiness budget must be a finite number of seconds greater than 0")
    if seconds > SANDBOX_READY_TIMEOUT_MAX:
        raise ValueError(f"the sandbox readiness budget must not exceed {SANDBOX_READY_TIMEOUT_MAX} seconds")
    return seconds


class SandboxCreationError(RuntimeError):
    """Preserve a rejected creation result for ownership-fenced cleanup."""

    def __init__(self, message: str, *, info: SandboxInfo):
        super().__init__(message)
        self.info = info


def wait_for_sandbox_ready(
    sandbox_url: str,
    timeout: float = 30,
    *,
    headers: Mapping[str, str] | None = None,
    cancelled: threading.Event | None = None,
) -> bool:
    """Poll the sandbox health endpoint until it answers 200 or the budget ends.

    The budget is a hard deadline on a monotonic clock: every probe and every
    sleep is clamped to what is left of it, and a 200 that lands after it has
    passed is not a success -- the caller's next step is to destroy the
    container, and a sandbox accepted late is one the deadline never bounded.

    This wait runs in a worker thread, which no task cancellation reaches, so
    a caller that can be stopped passes its stop event as *cancelled*: the
    pause between probes waits on it, and once it is set no further probe is
    sent. A Stop therefore ends the wait within one probe (at most
    ``_READY_REQUEST_TIMEOUT``), not at the deadline.

    Args:
        sandbox_url: URL of the sandbox (e.g. http://k3s:30001).
        timeout: The budget in seconds; validated by ``normalize_ready_timeout``.
        cancelled: The caller's stop event, if it can be stopped.

    Returns:
        True if the sandbox answered 200 within the budget, False otherwise,
        including when *cancelled* was set first.

    Raises:
        ValueError: ``timeout`` is not a finite positive number of seconds.
    """
    budget = normalize_ready_timeout(timeout)
    deadline = _monotonic() + budget
    with requests.Session() as session:
        session.trust_env = sandbox_http_trust_env(sandbox_url)
        if headers:
            session.headers.update(headers)
        while True:
            remaining = deadline - _monotonic()
            if remaining <= 0 or (cancelled is not None and cancelled.is_set()):
                return False
            try:
                response = session.get(f"{sandbox_url}/v1/sandbox", timeout=min(_READY_REQUEST_TIMEOUT, remaining))
            except requests.exceptions.RequestException:
                response = None
            if response is not None and response.status_code == 200:
                return _monotonic() <= deadline
            remaining = deadline - _monotonic()
            if remaining <= 0:
                return False
            if cancelled is None:
                time.sleep(min(1.0, remaining))
            elif cancelled.wait(min(1.0, remaining)):
                return False


async def _settle(future: asyncio.Future[Any]) -> Any:
    """Await a future that owns a resource, even if this task is cancelled again.

    Cleanup must not be the thing that gets abandoned: the caller remembers its
    own cancellation and re-raises it once the resource is released.
    """
    while True:
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            if future.cancelled():
                raise


async def _open_client(**kwargs: Any) -> httpx.AsyncClient:
    """Build an ``httpx.AsyncClient`` without loading TLS trust on the event loop.

    The constructor resolves the system trust store synchronously, so it has to
    run in a thread. That alone is not enough: a cancellation can arrive while
    the worker is still inside the constructor, and then the await is abandoned
    while the thread goes on to hand back a live client that nobody closes. So
    own the construction as a task and, once cancelled, wait for it and close
    whatever it produced before the cancellation propagates. If either owned
    task fails or is itself cancelled, log it and re-raise the caller's original
    cancellation.
    """
    construction: asyncio.Task[httpx.AsyncClient] = asyncio.create_task(asyncio.to_thread(httpx.AsyncClient, **kwargs))
    try:
        return await asyncio.shield(construction)
    except asyncio.CancelledError:
        try:
            late_client = await _settle(construction)
        except (Exception, asyncio.CancelledError) as error:
            logger.error("Sandbox readiness HTTPX client construction failed after cancellation: %r", error)
        else:
            try:
                await _settle(asyncio.create_task(late_client.aclose()))
            except (Exception, asyncio.CancelledError) as error:
                logger.error("Closing a sandbox readiness HTTPX client built after cancellation failed: %r", error)
        raise


async def wait_for_sandbox_ready_async(
    sandbox_url: str,
    timeout: float = 30,
    poll_interval: float = 1.0,
    *,
    headers: Mapping[str, str] | None = None,
) -> bool:
    """Async variant of sandbox readiness polling.

    Use this from async runtime paths so sandbox startup waits do not block the
    event loop. The synchronous ``wait_for_sandbox_ready`` function remains for
    existing synchronous backend/provider call sites.

    Same deadline semantics as the sync poller, on the loop's monotonic clock.
    httpx timeouts are per phase (connect, read, write, pool), so a reply that
    drips a byte at a time would never trip them; each probe is therefore also
    wrapped in ``asyncio.timeout`` for what is left of the budget. Cancelling
    the awaiting task cancels the in-flight probe and closes the client.

    Raises:
        ValueError: ``timeout`` is not a finite positive number of seconds.
    """
    budget = normalize_ready_timeout(timeout)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget

    client_kwargs: dict[str, object] = {
        "timeout": _READY_REQUEST_TIMEOUT,
        "trust_env": sandbox_http_trust_env(sandbox_url),
    }
    if headers:
        client_kwargs["headers"] = dict(headers)
    # Construction loads TLS trust synchronously, off the loop, and owns the
    # client even when this coroutine is cancelled mid-construction.
    async with await _open_client(**client_kwargs) as client:
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            try:
                async with asyncio.timeout(remaining):
                    response = await client.get(f"{sandbox_url}/v1/sandbox", timeout=min(_READY_REQUEST_TIMEOUT, remaining))
            except (httpx.RequestError, TimeoutError):
                response = None
            if response is not None and response.status_code == 200:
                return loop.time() <= deadline
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(poll_interval, remaining))


class DestroyOutcome(StrEnum):
    """What a backend established about a resource set after trying to destroy it.

    Successful destruction means the owned set is *confirmed absent*, not that
    commands were attempted or a function returned. A failed command is not
    absence, and an observation the backend could not make is not absence
    either; only a positive not-found counts. A backend whose only observation
    is a remote service's acceptance of the deletion answers ``ABSENT`` on that
    acceptance and says so in its own docstring.
    """

    ABSENT = "absent"  # every resource in the set positively established absent
    PARTIAL = "partial"  # some resources confirmed absent, at least one remains
    FAILED = "failed"  # nothing confirmed absent; the set is intact or its commands failed
    UNKNOWN = "unknown"  # the observation was unavailable or ambiguous (timeout, daemon error)

    @property
    def confirmed_absent(self) -> bool:
        return self is DestroyOutcome.ABSENT


class SandboxBackend(ABC):
    """Abstract base for sandbox provisioning backends.

    Two implementations:
    - LocalContainerBackend: starts Docker/Apple Container locally, manages ports
    - RemoteSandboxBackend: connects to a pre-existing URL (K8s service, external)
    """

    @abstractmethod
    def create(
        self,
        thread_id: str | None,
        sandbox_id: str,
        extra_mounts: list[tuple[str, str, bool]] | None = None,
        *,
        user_id: str | None = None,
        provision_lark_cli_runtime: bool = False,
        provision_lark_cli_broker: bool = False,
    ) -> SandboxInfo:
        """Create/provision a new sandbox.

        Args:
            thread_id: Thread ID for which the sandbox is being created. Useful for backends that want to organize sandboxes by thread.
            sandbox_id: Deterministic sandbox identifier.
            extra_mounts: Additional volume mounts as (host_path, container_path, read_only) tuples.
                Ignored by backends that don't manage containers (e.g., remote).
            user_id: User bucket that the sandbox should mount or provision for.
            provision_lark_cli_runtime: Ask the backend to provision the sandbox
                lark-cli runtime via its native mechanism (e.g. the provisioner's
                init container + emptyDir). Backends that can't do this ignore it.
            provision_lark_cli_broker: Ask the backend to provision a lark-cli
                broker sidecar (Pattern B, issue #4338) so credentials stay out of
                the sandbox. Supersedes ``provision_lark_cli_runtime`` when the
                backend supports it; backends that can't do this ignore it.

        Returns:
            SandboxInfo with connection details.
        """
        ...

    @abstractmethod
    def destroy(self, info: SandboxInfo) -> DestroyOutcome | None:
        """Destroy/cleanup a sandbox and release its resources.

        Returns what was *established* about the resource set afterwards, as a
        :class:`DestroyOutcome`. A backend that returns ``None`` predates the
        contract and is trusted as it always was: a normal return means gone,
        an exception means not. Backends that can observe their resources must
        report rather than assume, because a stop or remove command that fails
        can still return normally.

        Args:
            info: The sandbox metadata to destroy.
        """
        ...

    @abstractmethod
    def is_alive(self, info: SandboxInfo) -> bool:
        """Quick check whether a sandbox is still alive.

        This should be a lightweight check (e.g., container inspect)
        rather than a full health check.

        Args:
            info: The sandbox metadata to check.

        Returns:
            True if the sandbox appears to be alive.
        """
        ...

    @abstractmethod
    def discover(self, sandbox_id: str) -> SandboxInfo | None:
        """Try to discover an existing sandbox by its deterministic ID.

        Used for cross-process recovery: when another process started a sandbox,
        this process can discover it by the deterministic container name or URL.

        Args:
            sandbox_id: The deterministic sandbox ID to look for.

        Returns:
            SandboxInfo if found, including ``requires_replacement=True`` when
            the backend can identify an incompatible persisted provisioning
            policy without safely adopting it. Enumeration must not destroy
            resources; the provider owns replacement fencing. None otherwise.
        """
        ...

    def is_absent(self, sandbox_id: str) -> bool:
        """Prove that no runtime exists under this logical ID, even if stopped.

        Unknown/unavailable runtimes must never return True. Backends without
        an authoritative absence check retain their quarantine fences.
        """
        return False

    def complete_absent_teardown(self, sandbox_id: str) -> None:
        """Finish cleanup bookkeeping after authoritative runtime absence.

        The provider must confirm ``is_absent(sandbox_id) is True`` and hold
        both its local teardown reservation and cross-worker teardown lease
        throughout this call. Implementations may release pending resources
        scoped to this absent logical ID. Failure must raise so quarantine is
        retained and cleanup can be retried before creating a replacement.
        Backends without pending cleanup require no completion work.
        """

    def inspect_runtime(self, sandbox_id: str) -> SandboxInfo | None:
        """Inspect the runtime generation independently of endpoint readiness.

        Return None only after confirming no resource has this logical ID,
        including stopped or terminating resources. Unavailable or unsupported
        inspections must raise rather than imply absence.
        """
        raise RuntimeError(f"Runtime inspection is unavailable for sandbox {sandbox_id}")

    def list_running(self) -> list[SandboxInfo]:
        """Enumerate all running sandboxes managed by this backend.

        Used for startup reconciliation: when the process restarts, it needs
        to discover containers started by previous processes so they can be
        adopted into the warm pool or destroyed if idle too long.

        The default implementation returns an empty list, which is correct
        for backends that don't manage local containers (e.g., RemoteSandboxBackend
        delegates lifecycle to the provisioner which handles its own cleanup).
        Enumeration must be read-only. Backends report resources that need
        replacement through ``SandboxInfo.requires_replacement`` so the
        provider can apply ownership and local teardown fencing first.

        Returns:
            A list of SandboxInfo for all currently running sandboxes.
        """
        return []

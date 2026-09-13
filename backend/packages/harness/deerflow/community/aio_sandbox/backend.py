"""Abstract base class for sandbox provisioning backends."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import math
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
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


def wait_for_sandbox_ready(
    sandbox_url: str,
    timeout: float = 30,
    *,
    headers: Mapping[str, str] | None = None,
) -> bool:
    """Poll the sandbox health endpoint until it answers 200 or the budget ends.

    The budget is a hard deadline on a monotonic clock: every probe and every
    sleep is clamped to what is left of it, and a 200 that lands after it has
    passed is not a success -- the caller's next step is to destroy the
    container, and a sandbox accepted late is one the deadline never bounded.

    Args:
        sandbox_url: URL of the sandbox (e.g. http://k3s:30001).
        timeout: The budget in seconds; validated by ``normalize_ready_timeout``.

    Returns:
        True if the sandbox answered 200 within the budget, False otherwise.

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
            if remaining <= 0:
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
            time.sleep(min(1.0, remaining))


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
    async with httpx.AsyncClient(**client_kwargs) as client:
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
        accepted_skills_only: bool = False,
        accepted_skill_binding: object | None = None,
        egress_allowance: object | None = None,
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
            accepted_skill_binding: Optional immutable accepted-skill request.
                Only a backend with a verified materialization contract may use it.
            accepted_skills_only: Exclude every mutable live-skill projection even
                when the accepted set is empty.
            egress_allowance: The accepted Kind's run-bound ``EgressAllowanceV1``.
                Only a backend that renders it into the sandbox's network policy
                and attests the rendered digest may accept it.

        Returns:
            SandboxInfo with connection details.
        """
        ...

    @abstractmethod
    def destroy(self, info: SandboxInfo) -> None:
        """Destroy/cleanup a sandbox and release its resources.

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

    def renew_accepted_attempt(self, info: SandboxInfo) -> bool:
        """Renew a backend-native accepted-material attempt when present.

        Backends without an expiring native attempt keep their existing
        lifecycle semantics. Remote Kubernetes overrides this fail-closed seam.
        """

        del info
        return True

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

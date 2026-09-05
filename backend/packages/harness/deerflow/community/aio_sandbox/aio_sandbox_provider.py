"""AIO Sandbox Provider — orchestrates sandbox lifecycle with pluggable backends.

This provider composes:
- SandboxBackend: how sandboxes are provisioned (local container vs remote/K8s)

The provider itself handles:
- In-process caching for fast repeated access
- Idle timeout management
- Graceful shutdown with signal handling
- Mount computation (thread-specific, skills)
"""

import asyncio
import atexit
import contextlib
import hashlib
import json
import logging
import os
import re
import signal
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]
    import msvcrt

from deerflow.community.warm_pool_lifecycle import (
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_REPLICAS,
    WarmPoolLifecycleMixin,
)
from deerflow.community.warm_pool_lifecycle import (
    IDLE_CHECK_INTERVAL as _SHARED_IDLE_CHECK_INTERVAL,
)
from deerflow.config import get_app_config
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths, join_host_path
from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH
from deerflow.integrations.lark_cli import INTEGRATION_ID as LARK_CLI_INTEGRATION_ID
from deerflow.integrations.lark_cli import LARK_CLI_SANDBOX_CONFIG_DIR, LARK_CLI_SANDBOX_DATA_DIR, LARK_CLI_SANDBOX_LOCKS_DIR, LARK_CLI_SANDBOX_RUNTIME_DIR, ensure_lark_cli_credential_tree, lark_skills_installed
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.sandbox.accepted_material import (
    AcceptedMaterialCapability,
    AcceptedMaterialError,
    AcceptedMaterialExecutionClaimV1,
    AcceptedMaterializerSelection,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxQualificationV1,
    AcceptedSkillExecutionEvidence,
    AcceptedSkillExecutionEvidenceV2,
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.acquire_serialization import AcquireSerializer
from deerflow.sandbox.capabilities import (
    AcceptedMaterialization,
    AcceptedSkillProjection,
    reject_writable_accepted_skill_aliases,
)
from deerflow.sandbox.identity import derive_sandbox_scope_token
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider
from deerflow.skills.types import SkillCategory

from .aio_sandbox import AioSandbox
from .backend import SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT, SandboxBackend, wait_for_sandbox_ready, wait_for_sandbox_ready_async
from .local_backend import LocalContainerBackend
from .ownership import (
    OwnershipBackendError,
    RenewOutcome,
    SandboxOwnershipStore,
    compute_lease_ttl,
    generate_owner_id,
    make_sandbox_ownership_store,
    resolve_ownership_config,
)
from .remote_backend import RemoteSandboxBackend, _normalize_skills_container_path
from .sandbox_info import AcceptedSkillMaterialReceiptV2, SandboxInfo

if TYPE_CHECKING:
    from deerflow.runtime.skill_projection import SkillProjectionClear

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_IMAGE = "enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest"
DEFAULT_PORT = 8080
DEFAULT_CONTAINER_PREFIX = "deer-flow-sandbox"
# Containers that carry digest-bound accepted material. They are never adopted
# into the warm pool: a crashed accepted run must not turn into an ordinary,
# reusable thread sandbox.
ACCEPTED_SANDBOX_ID_SUFFIX = "-accepted"
IDLE_CHECK_INTERVAL = _SHARED_IDLE_CHECK_INTERVAL


class SandboxBeingDestroyedError(RuntimeError):
    """A peer is tearing this container down, so it must not be handed out.

    Raised on the acquire path when the ownership lease is in its teardown state.
    The caller drops the container from tracking and lets the normal
    discover-or-create path provision a fresh one, rather than handing an agent a
    sandbox that is about to stop underneath it.
    """

    def __init__(self, sandbox_id: str) -> None:
        super().__init__(f"sandbox {sandbox_id} is being destroyed by another instance")
        self.sandbox_id = sandbox_id


class SandboxPolicyReplacementDeferredError(RuntimeError):
    """An incompatible sandbox cannot be replaced until it is a true orphan."""

    def __init__(self, sandbox_id: str) -> None:
        super().__init__(f"sandbox {sandbox_id} has an incompatible provisioning policy; replacement is deferred until its current owner releases it")
        self.sandbox_id = sandbox_id


class SandboxIdentityCollisionError(RuntimeError):
    """A deterministic ID is already tracked for a different user/thread."""

    def __init__(
        self,
        sandbox_id: str,
        stored_key: tuple[str, str] | None,
        requested_key: tuple[str, str],
    ) -> None:
        super().__init__(f"sandbox ID collision for {sandbox_id}: tracked identity is {stored_key!r}, requested identity is {requested_key!r}")
        self.sandbox_id = sandbox_id


def _lock_file_exclusive(lock_file) -> None:
    if fcntl is not None:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        return

    lock_file.seek(0)
    msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)


def _unlock_file(lock_file) -> None:
    if fcntl is not None:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        return

    lock_file.seek(0)
    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)


def _open_lock_file(lock_path):
    return open(lock_path, "a", encoding="utf-8")


class AioSandboxProvider(
    WarmPoolLifecycleMixin[SandboxInfo],
    SandboxProvider,
    AcceptedSkillProjection,
    AcceptedMaterialization,
):
    """Sandbox provider that manages containers running the AIO sandbox.

    Architecture:
        This provider composes a SandboxBackend (how to provision), enabling:
        - Local Docker/Apple Container mode (auto-start containers)
        - Remote/K8s mode (connect to pre-existing sandbox URL)

    Configuration options in config.yaml under sandbox:
        use: deerflow.community.aio_sandbox:AioSandboxProvider
        image: <container image>
        port: 8080                      # Base port for local containers
        container_prefix: deer-flow-sandbox
        idle_timeout: 600               # Idle timeout in seconds (0 to disable)
        replicas: 3                     # Max concurrent sandbox containers (LRU eviction when exceeded)
        thread_data_mounts: null        # null = backend auto-detection
        mounts:                         # Volume mounts for local containers
          - host_path: /path/on/host
            container_path: /path/in/container
            read_only: false
        environment:                    # Environment variables for containers
          NODE_ENV: production
          API_KEY: $MY_API_KEY
    """

    supports_agent_skill_isolation = True

    # How long `_held_teardown_lease` waits for its heartbeat thread to exit
    # before deferring the final lease release to that (still-running) thread.
    # The store's socket timeout bounds each operation, but context exit can
    # catch the heartbeat in one final refresh and must then wait for its final
    # release. Keep this above both sequential five-second operation bounds so a
    # normally timing-out refresh + release still finishes synchronously.
    _TEARDOWN_JOIN_TIMEOUT_SECONDS = 12.0

    def __init__(self):
        self._lock = threading.Lock()
        self._sandboxes: dict[str, AioSandbox] = {}  # sandbox_id -> AioSandbox instance
        self._sandbox_infos: dict[str, SandboxInfo] = {}  # sandbox_id -> SandboxInfo (for destroy)
        self._thread_sandboxes: dict[tuple[str, str], str] = {}  # (user_id, thread_id) -> sandbox_id
        self._acquire_serializer: AcquireSerializer[tuple[str, str]] = AcquireSerializer(thread_name_prefix="aio-sandbox-lock-wait")
        self._last_activity: dict[str, float] = {}  # sandbox_id -> last activity timestamp
        # Warm pool: released sandboxes whose containers are still running.
        # Maps sandbox_id -> (SandboxInfo, release_timestamp).
        # Containers here can be reclaimed quickly (no cold-start) or destroyed
        # when replicas capacity is exhausted.
        self._warm_pool: dict[str, tuple[SandboxInfo, float]] = {}
        self._active_sandbox_identity: dict[str, tuple[str, str] | None] = {}
        self._warm_pool_identity: dict[str, tuple[str, str] | None] = {}
        # sandbox_id -> when reconciliation first saw it running with no lease.
        # Gates adoption behind a recovery grace (see _adoptable_after_grace).
        self._unowned_since: dict[str, float] = {}
        # The two halves of same-process exclusion. The ownership store excludes
        # peers and nothing else — `claim()` and `take()` both succeed against
        # our own lease by design — so `del:` says nothing to this process's own
        # threads. See _reserve_local_teardown / _acquire_epoch.
        self._local_teardown: set[str] = set()
        self._acquire_epoch: dict[str, int] = {}
        self._acquire_epoch_counter = 0
        self._acquire_inflight: dict[str, int] = {}
        self._shutdown_called = False
        self._idle_checker_stop = threading.Event()
        self._idle_checker_thread: threading.Thread | None = None
        self._renewal_stop = threading.Event()
        self._renewal_thread: threading.Thread | None = None
        # Per-instance id used for cross-instance sandbox ownership leases (#4206).
        self._owner_id = generate_owner_id()

        self._config = self._load_config()
        self._ownership_config = resolve_ownership_config(self._config.get("ownership"), stream_bridge=self._config.get("stream_bridge"))
        self._ownership: SandboxOwnershipStore = make_sandbox_ownership_store(self._ownership_config, owner_id=self._owner_id)
        if not self._ownership.supports_cross_process:
            # Peers cannot see these leases, so every container looks like an
            # orphan to them. Say so once rather than letting #4206 resurface
            # silently on a multi-worker deployment that never set the config.
            logger.warning(
                "Sandbox ownership store cannot coordinate across processes (sandbox.ownership.type: %s). "
                "Safe for a single gateway instance only — multi-worker / load-balanced gateways sharing a "
                "container backend must set sandbox.ownership.type: redis, or peers will adopt and idle-destroy "
                "each other's live sandboxes (#4206).",
                self._ownership_config.type,
            )
        self._backend: SandboxBackend = self._create_backend()

        # Register shutdown handler
        atexit.register(self.shutdown)
        self._register_signal_handlers()

        # Reconcile orphaned containers from previous process lifecycles
        self._reconcile_orphans()

        # Renewal is independent of idle cleanup: an owner must keep proving it is
        # alive even when the idle reaper is disabled, or peers adopt its live
        # containers once the lease lapses (idle_timeout: 0 is a supported config).
        self._start_lease_renewal()

        # Start idle checker if enabled
        if self._config.get("idle_timeout", DEFAULT_IDLE_TIMEOUT) > 0:
            self._start_idle_checker()

    @property
    def uses_thread_data_mounts(self) -> bool:
        """Whether thread workspace/uploads/outputs are visible via mounts.

        Local container backends bind-mount the thread data directories, so files
        written by the gateway are already visible when the sandbox starts.
        Remote backends may require explicit file sync. Operators can override
        this detection when gateway and remote sandboxes share the same storage.
        """
        override = self._config.get("thread_data_mounts")
        if override is not None:
            return override
        return isinstance(self._backend, LocalContainerBackend)

    # ── Factory methods ──────────────────────────────────────────────────

    def _create_backend(self) -> SandboxBackend:
        """Create the appropriate backend based on configuration.

        Selection logic (checked in order):
        1. ``provisioner_url`` set → RemoteSandboxBackend (provisioner mode)
              Provisioner dynamically creates Pods + Services in k3s.
        2. Default → LocalContainerBackend (local mode)
              Local provider manages container lifecycle directly (start/stop).
        """
        provisioner_url = self._config.get("provisioner_url")
        if provisioner_url:
            if self.sandbox_network_mode() != "open":
                raise RuntimeError("sandbox.network restricted modes are currently supported only by the local Docker AIO backend")
            logger.info(f"Using remote sandbox backend with provisioner at {provisioner_url}")
            api_key = self._config.get("provisioner_api_key", "")
            return RemoteSandboxBackend(
                provisioner_url=provisioner_url,
                api_key=api_key,
                service_account_token_file=self._config.get(
                    "provisioner_service_account_token_file",
                    "",
                ),
            )

        logger.info("Using local container sandbox backend")
        return LocalContainerBackend(
            image=self._config["image"],
            base_port=self._config["port"],
            container_prefix=self._config["container_prefix"],
            config_mounts=self._config["mounts"],
            environment=self._config["environment"],
            network_config=self._config["network"],
        )

    # ── Configuration ────────────────────────────────────────────────────

    def _load_config(self) -> dict:
        """Load sandbox configuration from app config."""
        config = get_app_config()
        sandbox_config = config.sandbox

        idle_timeout = getattr(sandbox_config, "idle_timeout", None)
        replicas = getattr(sandbox_config, "replicas", None)
        configured_skills_path = getattr(
            getattr(config, "skills", None),
            "container_path",
            None,
        )
        if not isinstance(configured_skills_path, str):
            configured_skills_path = DEFAULT_SKILLS_CONTAINER_PATH

        return {
            "image": sandbox_config.image or DEFAULT_IMAGE,
            "port": sandbox_config.port or DEFAULT_PORT,
            "container_prefix": sandbox_config.container_prefix or DEFAULT_CONTAINER_PREFIX,
            "idle_timeout": idle_timeout if idle_timeout is not None else DEFAULT_IDLE_TIMEOUT,
            "replicas": replicas if replicas is not None else DEFAULT_REPLICAS,
            "mounts": sandbox_config.mounts or [],
            "thread_data_mounts": getattr(sandbox_config, "thread_data_mounts", None),
            "environment": self._resolve_env_vars(sandbox_config.environment or {}),
            "network": sandbox_config.network.model_dump(),
            "ownership": getattr(sandbox_config, "ownership", None),
            # A redis stream bridge means the deployment is multi-instance, which
            # is what the ownership store must default to. Read the same source
            # the bridge's own resolver reads, not just its env var.
            "stream_bridge": getattr(config, "stream_bridge", None),
            # provisioner URL for dynamic pod management (e.g. http://provisioner:8002)
            "provisioner_url": getattr(sandbox_config, "provisioner_url", None) or "",
            "provisioner_api_key": getattr(sandbox_config, "provisioner_api_key", None) or "",
            "provisioner_service_account_token_file": getattr(
                sandbox_config,
                "provisioner_service_account_token_file",
                None,
            )
            or "",
            "accepted_skill_projection_profile": getattr(
                sandbox_config,
                "accepted_skill_projection_profile",
                "disabled",
            ),
            "accepted_material_lease_duration_seconds": getattr(
                sandbox_config,
                "accepted_material_lease_duration_seconds",
                300,
            ),
            "accepted_material_qualification_evidence": getattr(
                sandbox_config,
                "accepted_material_qualification_evidence",
                None,
            ),
            "accepted_material_qualification_digest": getattr(
                sandbox_config,
                "accepted_material_qualification_digest",
                None,
            ),
            "accepted_material_qualification_max_age_seconds": getattr(
                sandbox_config,
                "accepted_material_qualification_max_age_seconds",
                30 * 24 * 60 * 60,
            ),
            "skills_container_path": _normalize_skills_container_path(
                configured_skills_path,
            ),
        }

    def sandbox_network_mode(self) -> str:
        return str(self._config.get("network", {}).get("mode", "open"))

    def sandbox_network_temporary_grant_ttl(self) -> int:
        return int(self._config.get("network", {}).get("temporary_grant_ttl", 300))

    def consume_network_policy_events(self, sandbox_id: str) -> list[dict[str, object]]:
        if not isinstance(self._backend, LocalContainerBackend):
            return []
        return self._backend.consume_network_policy_events(sandbox_id)

    def deny_pending_network_policy_events(self, sandbox_id: str) -> bool:
        if not isinstance(self._backend, LocalContainerBackend):
            return True
        return self._backend.deny_pending_network_policy_events(sandbox_id)

    def decide_network_policy_request(self, sandbox_id: str, request_id: str, decision: str) -> bool:
        if not isinstance(self._backend, LocalContainerBackend):
            return False
        return self._backend.decide_network_policy_request(sandbox_id, request_id, decision)

    @staticmethod
    def _resolve_env_vars(env_config: dict[str, str]) -> dict[str, str]:
        """Resolve environment variable references (values starting with $)."""
        resolved = {}
        for key, value in env_config.items():
            if isinstance(value, str) and value.startswith("$"):
                env_name = value[1:]
                resolved[key] = os.environ.get(env_name, "")
            else:
                resolved[key] = str(value)
        return resolved

    # ── Cross-instance ownership leases ───────────────────────────────────

    def _publish_ownership(self, sandbox_id: str) -> None:
        """Take responsibility for *sandbox_id* on the acquire path.

        Takes over from whichever instance served this thread last — the
        container is deterministic per (user, thread), so a turn routing here is
        a legitimate handover. The previous owner's next renewal reports LOST and
        it stops tracking the container without touching it.

        Deliberately **not** fail-open. Swallowing the error and handing the
        sandbox out anyway would leave it unowned while in active use, so a peer
        would see an orphan and reap it — the exact failure this store exists to
        stop. Callers must let this propagate.

        The intent mark is published **before** the round trip, and that ordering
        is the point. ``take()`` makes the takeover durable before it returns —
        on redis the server has committed the SET while the reply is still in
        flight — so bumping the epoch afterwards leaves a window where the store
        already says the container is ours but the guard still reads as though it
        were not. A renewal holding an older ``LOST`` walks straight through it,
        drops the maps, and closes the client this call is about to hand back, so
        acquire returns an id the provider no longer tracks and ``get()`` answers
        ``None``. A guard must become visible no later than the transition it
        guards; the epoch alone cannot, because it can only be written after the
        call that performs the transition returns.

        So the two marks cover the two halves and are both needed: the intent
        mark covers "an acquire is mid-flight", the epoch covers "an acquire
        completed since you decided".

        Raises:
            SandboxBeingDestroyedError: a peer is tearing this container down, so
                it must not be handed to an agent (the destroy → re-acquire race).
            OwnershipBackendError: ownership could not be published.
        """
        with self._lock:
            self._acquire_inflight[sandbox_id] = self._acquire_inflight.get(sandbox_id, 0) + 1
        try:
            if not self._ownership.take(sandbox_id):
                raise SandboxBeingDestroyedError(sandbox_id)
            with self._lock:
                self._acquire_epoch_counter += 1
                self._acquire_epoch[sandbox_id] = self._acquire_epoch_counter
        finally:
            # A count rather than a set: acquires for one id are serialized by
            # the per-thread lock today, so a set would be equivalent — but that
            # is an assumption about a caller two layers up, and if it ever
            # stopped holding, a set would be cleared by the first finisher and
            # silently reopen this window. Counting removes the assumption.
            with self._lock:
                remaining = self._acquire_inflight.get(sandbox_id, 0) - 1
                if remaining > 0:
                    self._acquire_inflight[sandbox_id] = remaining
                else:
                    self._acquire_inflight.pop(sandbox_id, None)

    # ── Same-process exclusion (the half the store does not provide) ───────
    #
    # A lease excludes *peers*: `claim()` succeeds against our own `own:` lease
    # by design (that is what lets a destroy path claim what it already owns),
    # and `take()` succeeds against it too. So between this process's reaper
    # threads — idle checker, renewal, eviction — and its own acquire path, the
    # store provides **no exclusion at all**. Every reaper decides outside
    # `self._lock` (a store round trip must not be held under the lock that
    # guards every acquire), so each one acts on a decision an acquire may
    # already have invalidated. The two helpers below are that missing half, one
    # per direction:
    #
    #   reaping  — we are about to stop/drop it, so nothing may promote it:
    #              reserve it, and make every promote path honour the reservation
    #              exactly as it honours a peer's `del:`.
    #   forgetting — a peer legitimately owns it and must win, so the promote is
    #              the thing to detect: compare the acquire epoch we decided on.

    def _reserve_local_teardown(self, sandbox_id: str, still_reapable: Callable[[], bool]) -> bool:
        """Reserve *sandbox_id* for teardown by this process.

        ``still_reapable`` is evaluated in the **same** critical section as the
        reservation, so no acquire can slip between the last check and the mark.
        That pairing is the whole point: checking first and marking second is the
        window, not a narrower version of it.

        Consequence, and the one rule a new caller has to know: **the predicate
        runs with ``self._lock`` held**, which is a plain ``Lock``, so a predicate
        that touches the lock — directly, or via a provider method that takes it —
        deadlocks. Predicates must be cheap, non-blocking reads of the maps
        (``sandbox_id in self._warm_pool``, a ``_last_activity`` comparison). The
        constraint is stated rather than engineered around on purpose: making the
        lock reentrant to tolerate it would trade a loud hang for a quiet class of
        re-entrancy bugs everywhere else in this provider.
        """
        with self._lock:
            if sandbox_id in self._local_teardown or not still_reapable():
                return False
            self._local_teardown.add(sandbox_id)
            return True

    def _finish_local_teardown(self, sandbox_id: str) -> None:
        with self._lock:
            self._local_teardown.discard(sandbox_id)

    def _being_torn_down_locally(self, sandbox_id: str) -> bool:
        """Whether a reaper thread in *this* process holds *sandbox_id*.

        Callers must already hold ``self._lock``.
        """
        return sandbox_id in self._local_teardown

    def _acquire_epoch_of(self, sandbox_id: str) -> int:
        """Snapshot the acquire generation, so a stale decision can be detected.

        Bumped only by ``_publish_ownership`` — i.e. exactly when an acquire path
        (re)takes the lease on the way to handing the sandbox to an agent.
        Re-establishing a lapsed lease from ``_refresh_ownership`` deliberately
        does not bump it: nothing was handed out, so a reaper's decision about
        that id is still current.
        """
        with self._lock:
            return self._acquire_epoch.get(sandbox_id, 0)

    def _claim_ownership(self, sandbox_id: str, *, for_destroy: bool = False) -> bool:
        """Take (or refresh) ownership of *sandbox_id*.

        A successful claim is what makes acting on the container safe: while we
        hold the lease a peer's claim fails. With ``for_destroy`` the lease is
        additionally marked as a teardown, which a concurrent acquire-side
        ``take()`` refuses — that is what closes the ownership-check → container-
        stop window the deleted per-sandbox flock guard used to cover.

        Fails closed on a backend error: ownership unknown is treated as
        "not ours" so we neither adopt nor destroy the container.
        """
        try:
            return self._ownership.claim(sandbox_id, for_destroy=for_destroy)
        except OwnershipBackendError as e:
            logger.warning("Sandbox ownership claim failed for %s (treating as not owned): %s", sandbox_id, e)
            return False

    def _release_ownership(self, sandbox_id: str) -> None:
        try:
            self._ownership.release(sandbox_id)
        except OwnershipBackendError as e:
            # Best effort: the lease expires on its own, so a failed release
            # delays reuse rather than corrupting ownership.
            logger.warning("Failed to release sandbox ownership for %s: %s", sandbox_id, e)

    def _refresh_ownership(self, sandbox_id: str) -> bool:
        """Keep holding *sandbox_id*'s lease. False when a peer has taken it.

        A **lapsed** lease is re-established rather than treated as lost: nobody
        holds it, so re-claiming is safe, and this is what keeps a Redis restart
        (which drops every key) from evicting every live sandbox fleet-wide. A
        lease a peer actually holds is never re-taken — that is the #4206 kill.
        """
        try:
            outcome = self._ownership.renew(sandbox_id)
        except OwnershipBackendError as e:
            # Unknown, not lost: keep the sandbox and retry next tick. The TTL
            # still bounds how long a genuinely dead owner holds the lease.
            logger.warning("Could not renew sandbox ownership for %s, will retry: %s", sandbox_id, e)
            return True

        if outcome is RenewOutcome.RENEWED:
            return True
        if outcome is RenewOutcome.LAPSED:
            # Free: re-establish. This is the deliberate fail-open renewal path,
            # so it cannot use `_claim_ownership`: that helper turns a backend
            # error into False for adopt/reap callers, which would conflate an
            # outage between these two round trips with a peer takeover and
            # evict a live sandbox. Unknown means keep-and-retry here, exactly as
            # it does when the `renew()` call itself cannot answer above.
            try:
                if self._ownership.claim(sandbox_id):
                    logger.info("Re-established a lapsed ownership lease for %s", sandbox_id)
                    return True
            except OwnershipBackendError as e:
                logger.warning("Could not re-establish lapsed lease for %s, will retry: %s", sandbox_id, e)
                return True
            logger.warning("Lapsed ownership lease for %s was taken by a peer", sandbox_id)
            return False
        return False

    @contextlib.contextmanager
    def _held_teardown_lease(self, sandbox_id: str):
        """Keep *sandbox_id*'s teardown marker alive for as long as its stop runs.

        ``claim(..., for_destroy=True)`` writes the ``del:`` marker with the
        ordinary lease TTL, and normal ``renew()`` extends only ``own:`` while
        deliberately reporting a teardown as ``LOST``. Active and unhealthy
        destroy paths drop the sandbox from the maps ``_renew_owned_leases``
        iterates; the warm path keeps its entry visible until the stop succeeds,
        so ``_forget_lost_sandbox`` separately honours ``_local_teardown`` rather
        than misreading our own marker as a peer takeover. Without this heartbeat,
        a container stop that outlived the TTL let the marker lapse, a peer's
        ``take()`` succeeded against the still-running container, and the stop
        landed on the turn that had just been handed it — the very window the
        ``del:`` state exists to close, reopened by its own expiry.

        This is what the per-sandbox ``flock`` used to cover for free: a held lock
        cannot expire. A lease can, so the exclusion has to be held deliberately
        rather than assumed to outlast the work it guards. Reachable without an
        abnormal backend — the config schema bounds only ``renewal_interval_seconds``
        (> 0) and ``ttl_multiplier`` (>= 2), so a legal setting puts the TTL below a
        normal container stop, and ``LocalContainerBackend._stop_container`` passes
        no ``timeout`` to ``subprocess.run``, so a wedged daemon blocks unbounded
        even at the default 120s.

        The TTL stays finite on purpose: the heartbeat dies with the process, so a
        destroyer that crashes mid-stop still releases the container one TTL later
        instead of marking it undestroyable forever.

        The final release is the heartbeat's own last act, not the caller's. A
        refresh ``claim`` still in flight when the context exits (the socket
        timeout bounds it, but it can be mid-call) would otherwise land *after* a
        caller-side release and rewrite the ``del:`` marker on a container whose
        stop had already completed — stranding a fresh ``take()`` (or rolling back
        a fresh create) until the TTL. Releasing from inside the heartbeat, after
        its loop has stopped, sequences the release strictly after the last
        refresh, so no claim can follow it.
        """
        stop = threading.Event()

        def beat() -> None:
            interval = self._ownership_config.renewal_interval_seconds
            try:
                while not stop.wait(interval):
                    try:
                        if not self._ownership.claim(sandbox_id, for_destroy=True):
                            # Only reachable if the store lost our marker *and* a
                            # peer took it (e.g. a flush mid-stop). The stop is
                            # already in flight and cannot be recalled, so say so
                            # loudly rather than let a peer's container die without
                            # a trace.
                            logger.error(
                                "Lost the teardown exclusion for %s while its container stop was still in flight; a peer may have taken it",
                                sandbox_id,
                            )
                            return
                    except Exception as e:
                        # Broad on purpose: a refresh that raises must not kill the
                        # heartbeat and strand the marker for a stop that can run
                        # unbounded. Unknown, not lost — the marker may still be
                        # live and the TTL bounds a stale one. Retry on the next tick.
                        logger.warning("Could not refresh the teardown lease for %s, will retry: %s", sandbox_id, e)
            finally:
                # Release last, from the heartbeat itself, so an in-flight refresh
                # can never run after the marker is cleared. `release()` drops only
                # our own lease, so this is a safe no-op if a peer took it above.
                self._release_ownership(sandbox_id)

        beater = threading.Thread(target=beat, name="sandbox-teardown-lease", daemon=True)
        beater.start()
        try:
            yield
        finally:
            stop.set()
            beater.join(timeout=self._TEARDOWN_JOIN_TIMEOUT_SECONDS)
            if beater.is_alive():
                # The budget covers a normally timing-out refresh plus the final
                # release. The release is the heartbeat's job and is still
                # pending; clearing the marker here would reopen the exact race
                # this owns, so leave it — the thread will release when it
                # unblocks, or the TTL will reap it.
                logger.warning(
                    "Teardown heartbeat for %s did not exit within %.1fs; its lease release is deferred to that thread",
                    sandbox_id,
                    self._TEARDOWN_JOIN_TIMEOUT_SECONDS,
                )

    # ── Startup reconciliation ────────────────────────────────────────────

    def _adoptable_after_grace(self, sandbox_id: str, now: float) -> bool:
        """Whether *sandbox_id* has looked unowned long enough to be a real orphan.

        An absent lease normally proves the owner died and its TTL ran out. But
        the store can lose every key while every owner is alive and serving — a
        Redis restart without persistence, or eviction under ``maxmemory``
        pressure. ``_refresh_ownership`` already refuses to read that as
        abandonment (``LAPSED`` is re-established, not surrendered). Reading the
        same signal as "orphan, adopt" here would contradict it on the other
        path: whoever reconciles first would adopt every live container in the
        window before its owner's next renewal tick, that owner's renewal would
        then report ``LOST``, and it would drop a sandbox it is actively serving
        for the adopter to idle-destroy — #4206 through the back door.

        Waiting one full lease TTL rebuilds the delay the state loss erased. A
        live owner republishes within one renewal interval, which is shorter than
        the TTL by construction (``ttl_multiplier >= 2``), so only a container
        whose owner is really gone stays unowned across the whole grace.
        """
        if not self._ownership.supports_cross_process:
            # No peer can hold a lease this store would show us, so an unowned
            # container cannot be a live peer's — it is from a dead lifecycle of
            # this process. Single-instance deployments keep instant cleanup, and
            # a grace could not help a multi-worker one on this store anyway:
            # peers are invisible to each other's leases with or without it.
            return True

        try:
            current_owner = self._ownership.owner(sandbox_id)
        except OwnershipBackendError as e:
            # Unknown, not free: fail closed, same as _claim_ownership.
            logger.warning("Could not read sandbox ownership for %s during reconciliation (deferring adoption): %s", sandbox_id, e)
            return False

        if current_owner is not None:
            # Owned — by a peer, or already by us. Either way not an orphan, and
            # a live owner republishing must restart the grace rather than let a
            # stale one expire over its lease.
            self._unowned_since.pop(sandbox_id, None)
            return False

        first_seen = self._unowned_since.setdefault(sandbox_id, now)
        return now - first_seen >= compute_lease_ttl(self._ownership_config)

    def _replace_incompatible_sandbox(self, info: SandboxInfo, now: float) -> bool:
        """Destroy an incompatible sandbox only after both ownership fences.

        Backends report policy mismatches through ``SandboxInfo`` without
        mutating Docker state. That is essential during rolling upgrades: an
        older Gateway may still be serving the container under a live lease.
        Replacement is therefore an orphan-reconciliation operation, not a
        discovery side effect. The recovery grace protects against ownership
        store state loss, the teardown lease excludes peers, and the local
        reservation excludes this provider's own acquire/reaper paths.
        """
        if not info.requires_replacement:
            return False
        if not self._adoptable_after_grace(info.sandbox_id, now):
            return False
        if not self._reserve_local_teardown(
            info.sandbox_id,
            lambda: info.sandbox_id not in self._sandboxes and info.sandbox_id not in self._sandbox_infos and info.sandbox_id not in self._warm_pool,
        ):
            return False

        try:
            if not self._claim_ownership(info.sandbox_id, for_destroy=True):
                return False
            try:
                with self._held_teardown_lease(info.sandbox_id):
                    self._backend.destroy(info)
            except Exception as e:
                logger.warning("Failed to replace sandbox %s with incompatible provisioning policy: %s", info.sandbox_id, e)
                return False
            self._unowned_since.pop(info.sandbox_id, None)
            logger.info("Removed orphaned sandbox %s with incompatible provisioning policy", info.sandbox_id)
            return True
        finally:
            self._finish_local_teardown(info.sandbox_id)

    def _reconcile_orphans(self) -> None:
        """Reconcile orphaned containers left by previous process lifecycles.

        On startup (and periodically from the idle checker), enumerate running
        containers matching our prefix and adopt **true orphans** into the warm
        pool.  A container is only adopted when this instance can claim its
        ownership lease — so multi-instance gateways cannot adopt and later
        idle-destroy a peer's live sandbox (#4206).

        Adopted orphans get a fresh warm-pool timestamp; the idle checker then
        destroys them if nobody re-acquires within ``idle_timeout``.  That still
        cleans containers left by a crashed process once its lease expires.

        An unowned container is not adopted on sight — it must stay unowned for a
        recovery grace first, so a store that lost its state cannot be mistaken
        for a fleet of dead owners (see ``_adoptable_after_grace``).
        """
        try:
            running = self._backend.list_running()
        except Exception as e:
            logger.warning(f"Failed to enumerate running containers during startup reconciliation: {e}")
            return

        # Forget grace timers for containers that no longer exist, so a
        # long-lived instance does not accumulate an entry per destroyed
        # container. Runs before the empty-list return so it also drains.
        running_ids = {info.sandbox_id for info in running}
        self._unowned_since = {sid: seen for sid, seen in self._unowned_since.items() if sid in running_ids}

        if not running:
            return

        current_time = time.time()
        adopted = 0
        destroyed = 0
        replaced = 0
        skipped_live = 0
        deferred = 0

        for info in running:
            age = current_time - info.created_at if info.created_at > 0 else float("inf")
            if info.requires_replacement:
                if self._replace_incompatible_sandbox(info, current_time):
                    replaced += 1
                else:
                    deferred += 1
                    logger.debug(
                        "Deferring replacement of container %s during reconciliation: owned, locally tracked, or not yet past the recovery grace",
                        info.sandbox_id,
                    )
                continue

            if not self._adoptable_after_grace(info.sandbox_id, current_time):
                deferred += 1
                logger.debug("Deferring container %s during reconciliation: owned, or not yet past the recovery grace", info.sandbox_id)
                continue
            if info.sandbox_id.endswith(ACCEPTED_SANDBOX_ID_SUFFIX):
                # Accepted material is digest-bound to the run that admitted it.
                # An orphan of that kind is evidence of a crash, never a warm
                # sandbox: claim it as a teardown and stop it, under the same
                # held marker an explicit destroy uses.
                if not self._claim_ownership(info.sandbox_id, for_destroy=True):
                    skipped_live += 1
                    logger.debug("Skipping accepted container %s during reconciliation: owned by another instance", info.sandbox_id)
                    continue
                try:
                    with self._held_teardown_lease(info.sandbox_id):
                        self._backend.destroy(info)
                except Exception:
                    logger.warning("Failed to destroy orphaned accepted container %s during reconciliation", info.sandbox_id, exc_info=True)
                    continue
                self._unowned_since.pop(info.sandbox_id, None)
                destroyed += 1
                logger.info(f"Destroyed orphaned accepted container {info.sandbox_id} instead of adopting it (age: {age:.0f}s)")
                continue

            # Claim second: a successful claim proves the container is not a
            # peer's and locks peers out. It says nothing about *us* — it
            # succeeds against our own lease by design — so it is not a substitute
            # for the local teardown check below. The grace above is likewise a
            # precondition, not a substitute; only the claim is atomic.
            if not self._claim_ownership(info.sandbox_id):
                skipped_live += 1
                logger.debug("Skipping container %s during reconciliation: owned by another instance", info.sandbox_id)
                continue

            # Single lock acquisition per container: atomic check-and-insert.
            # Avoids a TOCTOU window between the "already tracked?" check and the
            # warm-pool insert.
            with self._lock:
                if info.sandbox_id in self._sandboxes or info.sandbox_id in self._warm_pool:
                    continue
                if self._being_torn_down_locally(info.sandbox_id):
                    # Adoption is a promote, so it needs the same reservation
                    # check as the other three. A container being torn down here
                    # is untracked and still running, which is exactly the shape
                    # this loop adopts — and neither the claim nor the grace
                    # excludes it. On `memory` the grace is skipped outright
                    # (`supports_cross_process = False`), so nothing else stands
                    # in the way there at all; adopting would park a container
                    # into the warm pool moments before its stop lands, leaving a
                    # dead entry for the next reclaim to hand out.
                    deferred += 1
                    logger.debug("Deferring container %s during reconciliation: this instance is tearing it down", info.sandbox_id)
                    continue
                self._warm_pool[info.sandbox_id] = (info, current_time)
                self._warm_pool_identity[info.sandbox_id] = None
            self._unowned_since.pop(info.sandbox_id, None)
            adopted += 1
            logger.info(f"Adopted container {info.sandbox_id} into warm pool (age: {age:.0f}s)")

        logger.info(
            "Startup reconciliation complete: %s adopted into warm pool, %s accepted destroyed, %s incompatible orphan(s) replaced, "
            "%s skipped (live peer ownership), %s deferred (owned, locally tracked, or within recovery grace), %s total found",
            adopted,
            destroyed,
            replaced,
            skipped_live,
            deferred,
            len(running),
        )

    # ── Deterministic ID ─────────────────────────────────────────────────

    @staticmethod
    def _effective_acquire_user_id(user_id: str | None) -> str:
        return user_id or get_effective_user_id()

    @staticmethod
    def _thread_key(thread_id: str, user_id: str) -> tuple[str, str]:
        return (user_id, thread_id)

    @staticmethod
    def _deterministic_sandbox_id(thread_id: str, user_id: str) -> str:
        """Generate a deterministic sandbox ID from user/thread scope.

        Includes user_id so a previously-created default-bucket sandbox cannot be
        reused for an auth/channel run that should mount a user-scoped bucket.

        During a mixed-version rollout, older 8-character containers are not
        reused under the new 16-character identity. They remain eligible for
        normal orphan cleanup while the first new-version acquire cold-starts.
        """
        return derive_sandbox_scope_token(user_id=user_id, thread_id=thread_id)

    @staticmethod
    def _thread_skill_projection_active(thread_id: str, user_id: str) -> bool:
        return get_paths().thread_skills_view_dir(thread_id, user_id=user_id).exists()

    @staticmethod
    def _policy_scoped_sandbox_id(
        thread_id: str,
        user_id: str,
        skills_container_path: str,
    ) -> str:
        """Return a root-aware domain-separated identity for a policy sandbox."""
        normalized_root = _normalize_skills_container_path(skills_container_path)
        seed = b"agent-skills-v2\0" + user_id.encode() + b"\0" + thread_id.encode() + b"\0" + normalized_root.encode()
        return hashlib.sha256(seed).hexdigest()[:16]

    @staticmethod
    def _custom_root_sandbox_id(
        thread_id: str,
        user_id: str,
        skills_container_path: str,
    ) -> str:
        """Return an identity for a shared-view sandbox at a custom root."""
        normalized_root = _normalize_skills_container_path(skills_container_path)
        seed = b"skills-root-v1\0" + user_id.encode() + b"\0" + thread_id.encode() + b"\0" + normalized_root.encode()
        return hashlib.sha256(seed).hexdigest()[:16]

    def _assert_active_identity_available_locked(
        self,
        sandbox_id: str,
        requested_key: tuple[str, str],
    ) -> None:
        """Fail closed if an active truncated ID belongs to another identity."""
        if sandbox_id not in self._sandboxes and sandbox_id not in self._sandbox_infos:
            return

        stored_key = self._active_sandbox_identity.get(sandbox_id)
        if stored_key is None:
            matching_keys = [key for key, mapped_id in self._thread_sandboxes.items() if mapped_id == sandbox_id]
            if len(matching_keys) == 1:
                stored_key = matching_keys[0]
        if stored_key != requested_key:
            raise SandboxIdentityCollisionError(sandbox_id, stored_key, requested_key)

    def _assert_warm_identity_available_locked(
        self,
        sandbox_id: str,
        requested_key: tuple[str, str],
    ) -> None:
        """Fail closed if a warm ID changed tenants during an acquire."""
        if sandbox_id not in self._warm_pool:
            return
        # Startup-adopted entries have unknown identity until their first reclaim.
        stored_key = self._warm_pool_identity.get(sandbox_id)
        if stored_key is not None and stored_key != requested_key:
            raise SandboxIdentityCollisionError(sandbox_id, stored_key, requested_key)

    # ── Mount helpers ────────────────────────────────────────────────────

    def _get_extra_mounts(
        self,
        thread_id: str | None,
        *,
        user_id: str | None = None,
        accepted_skills_only: bool = False,
    ) -> list[tuple[str, str, bool]]:
        """Collect all extra mounts for a sandbox (thread-specific + skills)."""
        mounts: list[tuple[str, str, bool]] = []
        skills_container_path = self._configured_skills_container_path()

        if thread_id:
            mounts.extend(self._get_thread_mounts(thread_id, user_id=user_id))
            if accepted_skills_only:
                paths = get_paths()
                effective_user_id = self._effective_acquire_user_id(user_id)
                active_view = paths.skill_snapshot_active_view_dir(
                    effective_user_id,
                    thread_id,
                )
                active_view.mkdir(parents=True, exist_ok=True)
                try:
                    skills_container_path = get_app_config().skills.container_path
                except FileNotFoundError:
                    from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH

                    skills_container_path = DEFAULT_SKILLS_CONTAINER_PATH
                mounts.append(
                    (
                        paths.host_skill_snapshot_active_view_dir(
                            effective_user_id,
                            thread_id,
                        ),
                        f"{skills_container_path}/.accepted",
                        True,
                    )
                )
            logger.info(f"Adding thread mounts for thread {thread_id}: {mounts}")

        if not accepted_skills_only:
            skills_mounts = self._get_skills_mounts(
                thread_id,
                user_id=user_id,
                skills_container_path=skills_container_path,
            )
            if skills_mounts:
                mounts.extend(skills_mounts)
                logger.info(f"Adding skills mounts: {skills_mounts}")

            effective_user_id = self._effective_acquire_user_id(user_id)
            thread_projection_active = bool(
                thread_id
                and self._thread_skill_projection_active(
                    thread_id,
                    effective_user_id,
                )
            )
            user_skill_mounts = (
                []
                if thread_projection_active
                else self._get_user_skill_mounts(
                    user_id=user_id,
                    skills_container_path=skills_container_path,
                )
            )
            if user_skill_mounts:
                mounts.extend(user_skill_mounts)
                logger.info(f"Adding user skill mounts: {user_skill_mounts}")

        lark_cli_mounts = self._get_lark_cli_runtime_mounts(user_id=user_id)
        if lark_cli_mounts:
            mounts.extend(lark_cli_mounts)
            logger.info(f"Adding Lark CLI runtime mounts: {lark_cli_mounts}")

        return self._dedupe_mounts_by_container_path(mounts)

    def _local_config_mount_exclusion_root(
        self,
        thread_id: str | None,
        *,
        user_id: str,
    ) -> str | None:
        """Return the skills subtree owned by a policy-scoped local sandbox."""
        if not isinstance(self._backend, LocalContainerBackend) or not thread_id:
            return None
        if not self._thread_skill_projection_active(thread_id, user_id):
            return None
        return self._configured_skills_container_path()

    def _configured_skills_container_path(self) -> str:
        """Return the provider-startup skills root used by IDs and mounts."""
        # A few mount-helper callers intentionally construct an uninitialized
        # provider. Production instances always use the startup snapshot, while
        # that narrow compatibility path loads the same validated value lazily.
        config = getattr(self, "_config", None)
        if not isinstance(config, dict):
            config = self._load_config()
        return _normalize_skills_container_path(
            config.get(
                "skills_container_path",
                DEFAULT_SKILLS_CONTAINER_PATH,
            )
        )

    @staticmethod
    def _dedupe_mounts_by_container_path(mounts: list[tuple[str, str, bool]]) -> list[tuple[str, str, bool]]:
        """Keep the first mount for each container path.

        Duplicate container paths are rejected by the provisioner and can also
        fail local Docker creation. The earlier mount wins because mount helpers
        are appended in priority order: thread data, skill roots, integration
        skill roots, then integration runtimes/credentials.
        """
        seen: set[str] = set()
        deduped: list[tuple[str, str, bool]] = []
        for host_path, container_path, read_only in mounts:
            if container_path in seen:
                logger.warning(
                    "Skipping duplicate sandbox mount for container path %s from host %s",
                    container_path,
                    host_path,
                )
                continue
            seen.add(container_path)
            deduped.append((host_path, container_path, read_only))
        return deduped

    @staticmethod
    def _get_thread_mounts(thread_id: str, *, user_id: str | None = None) -> list[tuple[str, str, bool]]:
        """Get volume mounts for a thread's data directories.

        Creates directories if they don't exist (lazy initialization).
        Mount sources use host_base_dir so that when running inside Docker with a
        mounted Docker socket (DooD), the host Docker daemon can resolve the paths.
        """
        paths = get_paths()
        effective_user_id = AioSandboxProvider._effective_acquire_user_id(user_id)
        paths.ensure_thread_dirs(thread_id, user_id=effective_user_id)

        return [
            (paths.host_sandbox_work_dir(thread_id, user_id=effective_user_id), f"{VIRTUAL_PATH_PREFIX}/workspace", False),
            (paths.host_sandbox_uploads_dir(thread_id, user_id=effective_user_id), f"{VIRTUAL_PATH_PREFIX}/uploads", False),
            (paths.host_sandbox_outputs_dir(thread_id, user_id=effective_user_id), f"{VIRTUAL_PATH_PREFIX}/outputs", False),
            # ACP workspace: read-only inside the sandbox (lead agent reads results;
            # the ACP subprocess writes from the host side, not from within the container).
            (paths.host_acp_workspace_dir(thread_id, user_id=effective_user_id), "/mnt/acp-workspace", True),
        ]

    @staticmethod
    def _get_skills_mounts(
        thread_id: str | None = None,
        *,
        user_id: str | None = None,
        skills_container_path: str | None = None,
    ) -> list[tuple[str, str, bool]]:
        """Get skills directory mount configurations for three-way skills layout.

        Mirrors ``LocalSandboxProvider._build_thread_path_mappings`` for AIO
        sandboxes: public, per-user custom, and legacy (pre-migration
        global-custom) skills are mounted to separate container subdirectories so
        that ``Skill.get_container_path()`` category-aware paths resolve
        correctly inside the sandbox.

        Mount sources use ``DEER_FLOW_HOST_BASE_DIR`` when running inside
        Docker (DooD) so the host Docker daemon can resolve the projection
        paths.
        """
        mounts: list[tuple[str, str, bool]] = []
        try:
            config = get_app_config()
            container_path = _normalize_skills_container_path(skills_container_path or config.skills.container_path)
            effective_user_id = AioSandboxProvider._effective_acquire_user_id(user_id)
            paths = get_paths()
            host_base_dir = str(paths.host_base_dir)

            if thread_id and AioSandboxProvider._thread_skill_projection_active(
                thread_id,
                effective_user_id,
            ):
                host_root = paths.host_thread_skills_view_dir(
                    thread_id,
                    user_id=effective_user_id,
                )
                return [
                    (
                        join_host_path(host_root, category.value),
                        f"{container_path}/{category.value}",
                        True,
                    )
                    for category in SkillCategory
                ]

            AioSandboxProvider._ensure_skills_projection(effective_user_id)

            # 1. Public skills: global, read-only — static, shared by all threads
            mounts.append(
                (
                    join_host_path(host_base_dir, "skills_view", "public"),
                    f"{container_path}/public",
                    True,
                )
            )

            # 2. Per-user custom skills: read-only, per-thread/per-user
            host_user_custom = join_host_path(
                host_base_dir,
                "users",
                effective_user_id,
                "skills_view",
                "custom",
            )
            mounts.append(
                (
                    host_user_custom,
                    f"{container_path}/custom",
                    True,
                )
            )

            # 3. Legacy visibility is encoded by projection contents. Keep the
            # mount stable even when the directory is empty so a later state
            # change is visible without recreating the sandbox.
            mounts.append(
                (
                    join_host_path(host_base_dir, "users", effective_user_id, "skills_view", "legacy"),
                    f"{container_path}/legacy",
                    True,
                )
            )
        except Exception as e:
            logger.warning("Could not setup skills mounts: %s", e)

        return mounts

    @staticmethod
    def _ensure_skills_projection(user_id: str):
        """Best-effort: a projection failure must not fail sandbox acquire.

        Called directly (for its side effect) from ``_acquire_internal`` /
        ``_acquire_internal_async`` outside any try/except, as well as from
        within ``_get_skills_mounts``'s own guarded block — swallowing here
        keeps both call sites safe without duplicating the guard.
        """
        from deerflow.skills.projection import ensure_skill_projections
        from deerflow.skills.storage import get_or_new_user_skill_storage

        try:
            storage = get_or_new_user_skill_storage(user_id, app_config=get_app_config())
            return ensure_skill_projections(storage)
        except Exception as exc:
            logger.warning("Could not ensure skills projection for user %s: %s", user_id, exc, exc_info=True)
            return None

    @staticmethod
    def _get_user_skill_mounts(
        *,
        user_id: str | None = None,
        skills_container_path: str | None = None,
    ) -> list[tuple[str, str, bool]]:
        """Mount enabled managed integration skills into AIO sandboxes.

        Per-user custom skills are already mounted by ``_get_skills_mounts``.
        Integration packages are shared, but their enabled state is per-user, so
        this helper mounts the user's projection instead of the raw shared root.
        """
        try:
            config = get_app_config()
            paths = get_paths()
            resolved_skills_container_path = _normalize_skills_container_path(skills_container_path or config.skills.container_path)
            effective_user_id = AioSandboxProvider._effective_acquire_user_id(user_id)
            AioSandboxProvider._ensure_skills_projection(effective_user_id)
            return [
                (
                    join_host_path(
                        str(paths.host_base_dir),
                        "users",
                        effective_user_id,
                        "skills_view",
                        "integrations",
                    ),
                    f"{resolved_skills_container_path}/integrations",
                    True,
                ),
            ]
        except Exception as e:
            logger.warning(f"Could not setup user skill mounts: {e}")
            return []

    @staticmethod
    def _lark_integration_active(user_id: str | None = None) -> bool:
        """Whether the managed Lark skill pack is installed for this user.

        Drives whether a sandbox requests the lark-cli runtime (init container /
        Gateway-download mount). Independent of whether a local ``sandbox-cli``
        dir exists, so remote/K8s can opt in without a Gateway-side download.
        """
        try:
            effective_user_id = AioSandboxProvider._effective_acquire_user_id(user_id)
            return lark_skills_installed(effective_user_id)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not determine Lark integration state: {e}")
            return False

    @staticmethod
    def _lark_broker_active(user_id: str | None = None) -> bool:
        """Whether this user's sandbox should use the lark-cli broker (Pattern B).

        True only when the Lark pack is installed AND the remote provisioner
        reports a configured broker image. When true, the provisioner keeps the
        credentials in a sidecar and the sandbox gets only a shim, so the
        Gateway-side credential-mount overlay must not run either.
        """
        try:
            if not AioSandboxProvider._lark_integration_active(user_id):
                return False
            from deerflow.integrations.lark_cli import sandbox_lark_broker_active

            return sandbox_lark_broker_active()
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not determine Lark broker state: {e}")
            return False

    @staticmethod
    def _get_lark_cli_runtime_mounts(*, user_id: str | None = None) -> list[tuple[str, str, bool]]:
        """Mount the per-user lark-cli config/data dirs used by Settings auth.

        Settings endpoints run ``lark-cli`` on the Gateway with
        ``LARKSUITE_CLI_CONFIG_DIR`` / ``DATA_DIR`` pointing at
        ``users/{user}/integrations/lark-cli``. Agent conversations run
        ``lark-cli`` inside the sandbox, so those same directories must be
        mounted into the container or the CLI sees a separate unauthenticated
        profile.

        The ``config`` dir holds the long-lived Lark ``appSecret`` (written by
        ``lark-cli config init`` on the Gateway, never in-sandbox), so it is
        mounted **read-only**: sandbox processes only need to read it, and a
        read-only bind stops a compromised agent from tampering with or
        replacing the app credentials. Newer ``lark-cli`` versions coordinate
        API calls through ``config/locks``, so that empty subdirectory is
        over-mounted writable without exposing the rest of ``config`` to
        writes. The ``data`` dir holds refreshable OAuth tokens that
        ``lark-cli auth`` updates in-sandbox, so it stays writable.
        This is defense-in-depth only — both dirs remain readable to arbitrary
        sandbox processes until the auth-proxy follow-up (issue #4338) lands.
        See the sandbox trust-boundary note in ``backend/AGENTS.md``.
        """
        try:
            paths = get_paths()
            effective_user_id = AioSandboxProvider._effective_acquire_user_id(user_id)
            ensure_lark_cli_credential_tree(effective_user_id, paths=paths)
            config_dir = paths.host_user_integration_config_dir(effective_user_id, LARK_CLI_INTEGRATION_ID)
            mounts = [
                (config_dir, LARK_CLI_SANDBOX_CONFIG_DIR, True),
                (join_host_path(config_dir, "locks"), LARK_CLI_SANDBOX_LOCKS_DIR, False),
                (paths.host_user_integration_data_dir(effective_user_id, LARK_CLI_INTEGRATION_ID), LARK_CLI_SANDBOX_DATA_DIR, False),
            ]
            runtime_dir = paths.base_dir / "integrations" / LARK_CLI_INTEGRATION_ID / "sandbox-cli"
            if runtime_dir.is_dir():
                mounts.append(
                    (
                        join_host_path(str(paths.host_base_dir), "integrations", LARK_CLI_INTEGRATION_ID, "sandbox-cli"),
                        LARK_CLI_SANDBOX_RUNTIME_DIR,
                        True,
                    )
                )
            return mounts
        except Exception as e:
            logger.warning(f"Could not setup Lark CLI runtime mounts: {e}")
            return []

    # ── Idle timeout management ──────────────────────────────────────────

    def _cleanup_idle_resources(self, idle_timeout: float) -> None:
        """Clean AIO resources idle longer than ``idle_timeout`` seconds."""
        # Pick up containers whose peer leases expired since startup (crash path).
        self._reconcile_orphans()
        self._cleanup_idle_sandboxes(idle_timeout)

    # ── Ownership lease renewal ──────────────────────────────────────────

    def _start_lease_renewal(self) -> None:
        """Start the daemon thread that keeps this instance's leases alive.

        Deliberately not folded into the idle checker: that thread only starts
        when ``idle_timeout > 0``, so renewal riding on it silently stopped for
        ``idle_timeout: 0`` deployments — a supported config ("keep warm VMs
        until shutdown") — letting every lease lapse and reopening #4206 one TTL
        later. Liveness and reaping must not share a switch.
        """
        if self._renewal_thread is not None and self._renewal_thread.is_alive():
            return

        self._renewal_stop.clear()
        self._renewal_thread = threading.Thread(
            target=self._lease_renewal_loop,
            name="sandbox-lease-renewal",
            daemon=True,
        )
        self._renewal_thread.start()
        logger.info(
            "Started sandbox ownership renewal thread (interval: %.1fs, ttl: %.1fs)",
            self._ownership_config.renewal_interval_seconds,
            self._ownership_config.renewal_interval_seconds * self._ownership_config.ttl_multiplier,
        )

    def _stop_lease_renewal(self) -> None:
        self._renewal_stop.set()
        thread = self._renewal_thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=5)

    def _lease_renewal_loop(self) -> None:
        interval = self._ownership_config.renewal_interval_seconds
        while not self._renewal_stop.wait(interval):
            try:
                self._renew_owned_leases()
            except Exception:
                logger.exception("Error in sandbox ownership renewal loop")

    def _renew_owned_leases(self) -> None:
        """Renew every container this instance believes it owns.

        Covers warm entries as well as active ones: a warm container is still
        ours (we hold it for fast reclaim), so letting its lease lapse would let
        a peer adopt a container we are about to hand back to its thread.

        Only a lease a **peer** now holds means the container is no longer ours;
        a lapsed one is re-established (see ``_refresh_ownership``). Conflating
        the two would evict every live sandbox on this instance the first time
        the store lost its state.
        """
        with self._lock:
            owned_ids = list(self._sandboxes.keys()) + list(self._warm_pool.keys())

        for sandbox_id in owned_ids:
            # Snapshot before the round trip: by the time `renew()` answers LOST,
            # an acquire in this process may already have taken the lease back
            # and promoted the id, and the answer is about the lease we held then.
            epoch = self._acquire_epoch_of(sandbox_id)
            if not self._refresh_ownership(sandbox_id):
                logger.warning("Lost sandbox ownership lease for %s; dropping it from this instance", sandbox_id)
                self._forget_lost_sandbox(sandbox_id, expected_epoch=epoch)
                continue

    def _forget_lost_sandbox(self, sandbox_id: str, *, expected_epoch: int | None = None) -> None:
        """Drop a sandbox whose lease we no longer hold, without touching the container.

        The container now belongs to whichever instance holds the lease, so
        stopping it here would be the very cross-instance kill this store exists
        to prevent. Only our host-side handle goes away.

        ``expected_epoch`` guards callers whose "we lost it" decision came from a
        store round trip made outside the lock. An acquire **mid-flight** counts
        too: its ``take()`` can already have made the takeover durable while the
        epoch is still unwritten, so the epoch alone would let a stale decision
        through (see ``_publish_ownership``). An acquire that re-took the lease
        in that window has already handed the sandbox to a turn — and, on the
        reuse path, handed out the *same* tracked client, so no object-identity
        check would notice. Dropping it then closes a client mid-turn and leaves
        the agent holding an id whose tool calls fail until the next turn.
        """
        with self._lock:
            # A warm teardown deliberately keeps its entry visible until the
            # backend stop succeeds. Its own `del:` marker makes `renew()` report
            # LOST, but that is not a peer takeover and must not pop the retained
            # entry — especially when the stop fails and the container remains
            # live for retry/reclaim. The teardown path removes it on success.
            if sandbox_id in self._local_teardown:
                logger.debug("Not dropping sandbox %s: this instance is tearing it down", sandbox_id)
                return
            # The in-flight check is deliberately *not* conditional on
            # `expected_epoch`. Today's epoch-less callers (the two
            # `SandboxBeingDestroyedError` handlers) cannot collide with a
            # publish for the same id — `_publish_ownership` has already cleared
            # the mark by the time they run, and acquires for one id are
            # serialized by the per-thread lock — so this changes no current
            # behaviour. It is here because "no epoch supplied" reading as "no
            # guard at all" is how the next caller of a dangerous primitive gets
            # written; an id being acquired right now must never be dropped.
            if sandbox_id in self._acquire_inflight:
                logger.info("Not dropping sandbox %s: an acquire is publishing ownership for it", sandbox_id)
                return
            if expected_epoch is not None and self._acquire_epoch.get(sandbox_id, 0) != expected_epoch:
                logger.info("Not dropping sandbox %s: this instance re-acquired it after the lease check", sandbox_id)
                return

            sandbox = self._sandboxes.pop(sandbox_id, None)
            self._sandbox_infos.pop(sandbox_id, None)
            self._active_sandbox_identity.pop(sandbox_id, None)
            self._last_activity.pop(sandbox_id, None)
            self._warm_pool.pop(sandbox_id, None)
            self._warm_pool_identity.pop(sandbox_id, None)
            self._acquire_epoch.pop(sandbox_id, None)
            for key, mapped_id in list(self._thread_sandboxes.items()):
                if mapped_id == sandbox_id:
                    del self._thread_sandboxes[key]

        # Close the host-side HTTP client we are dropping (#2872); the container
        # itself stays up for its new owner.
        if sandbox is not None:
            try:
                sandbox.close()
            except Exception as e:
                logger.warning(f"Error closing sandbox {sandbox_id} after losing its lease: {e}")

    def _cleanup_idle_sandboxes(self, idle_timeout: float) -> None:
        from deerflow.runtime.skill_projection import (
            get_skill_projection_coordinator,
        )

        current_time = time.time()
        active_to_destroy = []
        coordinator = get_skill_projection_coordinator()

        def projection_is_owned(sandbox_id: str) -> bool:
            """Check while ``self._lock`` stabilizes the sandbox identity."""
            identity = self._active_sandbox_identity.get(sandbox_id)
            if identity is None:
                identity = next(
                    (key for key, mapped_id in self._thread_sandboxes.items() if mapped_id == sandbox_id),
                    None,
                )
            return bool(
                identity is not None
                and coordinator.is_busy(
                    user_id=identity[0],
                    thread_id=identity[1],
                )
            )

        with self._lock:
            # Active sandboxes: tracked via _last_activity
            for sandbox_id, last_activity in self._last_activity.items():
                idle_duration = current_time - last_activity
                if idle_duration > idle_timeout and not projection_is_owned(sandbox_id):
                    active_to_destroy.append(sandbox_id)
                    logger.info(f"Sandbox {sandbox_id} idle for {idle_duration:.1f}s, marking for destroy")

        # Destroy active sandboxes (re-verify still idle before acting).
        #
        # The re-verify has to happen in the same critical section as the
        # teardown reservation, which is why it is handed to `_destroy_tracked`
        # as a predicate rather than run here. Checking here and destroying
        # afterwards left a window — widened by this PR from a few instructions
        # to a store round trip, since `destroy()` now claims ownership before it
        # untracks — in which a turn re-acquires the sandbox and then has its
        # container stopped underneath it.
        def still_idle(sandbox_id: str) -> bool:
            if projection_is_owned(sandbox_id):
                logger.info(
                    "Sandbox %s has an invocation-owned skill projection; skipping idle destroy",
                    sandbox_id,
                )
                return False
            last_activity = self._last_activity.get(sandbox_id)
            if last_activity is None:
                # Already released or destroyed by another path — skip.
                logger.info(f"Sandbox {sandbox_id} already gone before idle destroy, skipping")
                return False
            if (time.time() - last_activity) < idle_timeout:
                # Re-acquired (activity updated) since the snapshot — skip.
                logger.info(f"Sandbox {sandbox_id} was re-acquired before idle destroy, skipping")
                return False
            return True

        for sandbox_id in active_to_destroy:
            try:
                logger.info(f"Destroying idle sandbox {sandbox_id}")
                self._destroy_tracked(sandbox_id, still_reapable=lambda sid=sandbox_id: still_idle(sid))
            except Exception as e:
                logger.error(f"Failed to destroy idle sandbox {sandbox_id}: {e}")

        self._reap_expired_warm(idle_timeout)

    def _reap_expired_warm(self, idle_timeout: float | None = None) -> None:
        """Destroy warm entries older than ``idle_timeout``, never a peer's live container."""
        timeout = float(self._config.get("idle_timeout", DEFAULT_IDLE_TIMEOUT) if idle_timeout is None else idle_timeout)
        if timeout <= 0:
            return

        now = time.time()
        expired: list[tuple[str, SandboxInfo]] = []
        with self._lock:
            for sandbox_id, (entry, timestamp) in self._warm_pool.items():
                if now - timestamp > timeout:
                    expired.append((sandbox_id, entry))

        # Only drop an entry from the warm pool once we know it is really going
        # away. Popping first would lose the container on a refused or
        # unanswerable claim: still running, no longer tracked by anyone. The
        # deferred pop is why the reservation is needed — the entry stays visible
        # to `_reclaim_warm_pool_sandbox` for the whole stop.
        for sandbox_id, entry in expired:
            self._destroy_warm_entry(sandbox_id, entry, reason="idle_timeout", still_reapable=lambda sid=sandbox_id: sid in self._warm_pool)

    def _evict_oldest_warm(self) -> str | None:
        """Evict the oldest warm entry this instance still owns."""
        with self._lock:
            if not self._warm_pool:
                return None
            # Snapshot oldest-first under the lock; ownership is resolved outside
            # it, since a claim can be a network round trip and the provider lock
            # guards every acquire path.
            candidates = [(sandbox_id, entry) for sandbox_id, (entry, _) in sorted(self._warm_pool.items(), key=lambda item: item[1][1])]

        for sandbox_id, entry in candidates:
            # "Still in the warm pool?" is the reapable check, and it has to run
            # in the same critical section as the reservation — checking it here
            # and reserving afterwards is exactly the window a reclaim slips
            # through. `_destroy_warm_entry` does both under one lock hold.
            if not self._destroy_warm_entry(sandbox_id, entry, reason="replica_enforcement", still_reapable=lambda sid=sandbox_id: sid in self._warm_pool):
                continue
            return sandbox_id

        return None

    # ── Signal handling ──────────────────────────────────────────────────

    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown.

        Handles SIGTERM, SIGINT, and SIGHUP (terminal close) to ensure
        sandbox containers are cleaned up even when the user closes the terminal.
        """
        self._original_sigterm = signal.getsignal(signal.SIGTERM)
        self._original_sigint = signal.getsignal(signal.SIGINT)
        self._original_sighup = signal.getsignal(signal.SIGHUP) if hasattr(signal, "SIGHUP") else None

        def signal_handler(signum, frame):
            self.shutdown()
            if signum == signal.SIGTERM:
                original = self._original_sigterm
            elif hasattr(signal, "SIGHUP") and signum == signal.SIGHUP:
                original = self._original_sighup
            else:
                original = self._original_sigint
            if callable(original):
                original(signum, frame)
            elif original == signal.SIG_DFL:
                signal.signal(signum, signal.SIG_DFL)
                signal.raise_signal(signum)

        try:
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
            if hasattr(signal, "SIGHUP"):
                signal.signal(signal.SIGHUP, signal_handler)
        except ValueError:
            logger.debug("Could not register signal handlers (not main thread)")

    # ── Thread locking (in-process) ──────────────────────────────────────

    def _sandbox_id_for_thread(self, thread_id: str | None, user_id: str | None) -> str:
        """Return deterministic IDs for thread sandboxes and random IDs otherwise."""
        if not thread_id:
            return str(uuid.uuid4())[:8]
        effective_user_id = self._effective_acquire_user_id(user_id)
        skills_container_path = self._configured_skills_container_path()
        if self._thread_skill_projection_active(thread_id, effective_user_id):
            return self._policy_scoped_sandbox_id(
                thread_id,
                effective_user_id,
                skills_container_path,
            )
        # Preserve the historic deterministic ID for the default root while
        # preventing a custom-root Pod/container from being reused after the
        # configured mount destination changes.
        if skills_container_path != DEFAULT_SKILLS_CONTAINER_PATH:
            return self._custom_root_sandbox_id(
                thread_id,
                effective_user_id,
                skills_container_path,
            )
        return self._deterministic_sandbox_id(thread_id, effective_user_id)

    def _reuse_in_process_sandbox(self, thread_id: str | None, *, user_id: str | None = None, post_lock: bool = False) -> str | None:
        """Reuse an active in-process sandbox for a thread if one is still tracked."""
        if thread_id is None:
            return None

        effective_user_id = self._effective_acquire_user_id(user_id)
        key = self._thread_key(thread_id, effective_user_id)
        root_scoped_identity = (
            self._thread_skill_projection_active(
                thread_id,
                effective_user_id,
            )
            or self._configured_skills_container_path() != DEFAULT_SKILLS_CONTAINER_PATH
        )
        expected_id = self._sandbox_id_for_thread(thread_id, effective_user_id)
        stale_id: str | None = None
        with self._lock:
            if key not in self._thread_sandboxes:
                return None

            existing_id = self._thread_sandboxes[key]
            if root_scoped_identity and existing_id != expected_id:
                stale_id = existing_id
            elif self._being_torn_down_locally(existing_id):
                # A reaper thread in this process is stopping this container.
                # Same answer as a peer's `del:` lease: cold-start instead.
                logger.info("Cached sandbox %s is being destroyed by this instance; not reusing it", existing_id)
                return None
            elif existing_id in self._sandboxes:
                info = self._sandbox_infos.get(existing_id)
            else:
                del self._thread_sandboxes[key]
                return None

        if stale_id is not None:
            logger.info(
                "Replacing sandbox %s with expected identity %s for user/thread %s/%s",
                stale_id,
                expected_id,
                effective_user_id,
                thread_id,
            )
            self.destroy(stale_id)
            return None

        alive = self._check_tracked_sandbox_alive(existing_id, info) if info is not None else True
        if alive is False:
            self._drop_unhealthy_sandbox(
                existing_id,
                "in-process cache failed health check",
                expected_info=info,
            )
            return None

        with self._lock:
            if self._thread_sandboxes.get(key) != existing_id:
                return None
            if existing_id not in self._sandboxes:
                self._thread_sandboxes.pop(key, None)
                return None

            suffix = " (post-lock check)" if post_lock else ""
            logger.info(f"Reusing in-process sandbox {existing_id} for user/thread {effective_user_id}/{thread_id}{suffix}")
            self._last_activity[existing_id] = time.time()

        # Fail closed: an OwnershipBackendError propagates rather than handing out
        # a sandbox we could not publish ownership for.
        try:
            self._publish_ownership(existing_id)
        except SandboxBeingDestroyedError:
            # A peer is stopping this container. Drop it and let the caller
            # discover-or-create a fresh one instead of handing over a sandbox
            # that is about to disappear.
            logger.info("Cached sandbox %s is being destroyed by another instance; not reusing it", existing_id)
            self._forget_lost_sandbox(existing_id)
            return None

        with self._lock:
            if self._being_torn_down_locally(existing_id):
                # The first reservation check ran before the backend health
                # check and ownership round trip. A local reaper can win while
                # either is in flight, and it deliberately keeps the entry in
                # `_sandboxes` until its destroy claim succeeds. Membership
                # alone therefore cannot prove this id is still safe to return.
                logger.info("Cached sandbox %s was reserved for teardown while publishing ownership; not reusing it", existing_id)
                return None
            if existing_id not in self._sandboxes:
                # Dropped while we were publishing. The intent mark closes the
                # window *inside* `_publish_ownership`, but not the gap before
                # it: until the mark is set a renewal's `LOST` is both current
                # and correct — the peer really did hold the lease — so the
                # forget legitimately runs and closes this client. Returning the
                # id anyway would hand back a sandbox whose `get()` is `None`.
                # Fall through instead; the caller re-discovers and builds a
                # fresh client, and the lease we just took is already ours.
                logger.info("Cached sandbox %s was dropped while publishing ownership; falling through to discovery", existing_id)
                return None
        return existing_id

    def _reclaim_warm_pool_sandbox(
        self,
        thread_id: str | None,
        sandbox_id: str,
        *,
        user_id: str | None = None,
        post_lock: bool = False,
    ) -> str | None:
        """Promote a warm-pool sandbox back to active tracking if available."""
        if thread_id is None:
            return None

        effective_user_id = self._effective_acquire_user_id(user_id)
        key = self._thread_key(thread_id, effective_user_id)
        with self._lock:
            if sandbox_id not in self._warm_pool:
                return None
            self._assert_warm_identity_available_locked(sandbox_id, key)
            if self._being_torn_down_locally(sandbox_id):
                # The entry deliberately stays in `_warm_pool` for the whole stop
                # (so a refused claim does not lose the container), so pool
                # membership alone does not mean it is reclaimable.
                logger.info("Warm-pool sandbox %s is being destroyed by this instance; not reclaiming it", sandbox_id)
                return None

            info, _ = self._warm_pool[sandbox_id]

        alive = self._check_tracked_sandbox_alive(sandbox_id, info)
        if alive is False:
            self._drop_unhealthy_sandbox(
                sandbox_id,
                "warm-pool cache failed health check",
                expected_info=info,
            )
            return None

        # Publish ownership before the warm → active transition: a raise here must
        # not leave the sandbox tracked as active but unowned (a peer would see an
        # orphan and reap it mid-turn). On failure the entry stays warm and this
        # instance keeps its existing lease.
        try:
            self._publish_ownership(sandbox_id)
        except SandboxBeingDestroyedError:
            logger.info("Warm-pool sandbox %s is being destroyed by another instance; not reclaiming it", sandbox_id)
            self._forget_lost_sandbox(sandbox_id)
            return None

        with self._lock:
            if self._being_torn_down_locally(sandbox_id):
                # Re-checked, because the first check was before the round trip.
                # A reaper can reserve *after* our `take()` — the warm entry is
                # still there, since its pop is deferred until the stop returns —
                # then claim `del:` (which succeeds: the lease is ours, we just
                # took it) and stop the container. Whichever pop lands first
                # decides, and if ours does we install a client for a container
                # that is already stopped.
                logger.info("Warm-pool sandbox %s was claimed for teardown while publishing ownership; not reclaiming it", sandbox_id)
                return None
            self._assert_warm_identity_available_locked(sandbox_id, key)
            warm_item = self._warm_pool.pop(sandbox_id, None)
            if warm_item is None:
                return None
            self._warm_pool_identity.pop(sandbox_id, None)
            info, _ = warm_item
            sandbox = AioSandbox(id=sandbox_id, base_url=info.sandbox_url, request_headers=info.request_headers)
            self._sandboxes[sandbox_id] = sandbox
            self._sandbox_infos[sandbox_id] = info
            self._active_sandbox_identity[sandbox_id] = key
            self._last_activity[sandbox_id] = time.time()
            self._thread_sandboxes[key] = sandbox_id

        suffix = " (post-lock check)" if post_lock else f" at {info.sandbox_url}"
        logger.info(f"Reclaimed warm-pool sandbox {sandbox_id} for user/thread {effective_user_id}/{thread_id}{suffix}")
        return sandbox_id

    def _recheck_cached_sandbox(self, thread_id: str, sandbox_id: str, *, user_id: str) -> str | None:
        """Re-check in-memory caches after acquiring the cross-process file lock."""
        return self._reuse_in_process_sandbox(thread_id, user_id=user_id, post_lock=True) or self._reclaim_warm_pool_sandbox(
            thread_id,
            sandbox_id,
            user_id=user_id,
            post_lock=True,
        )

    def _register_discovered_sandbox(self, thread_id: str, info: SandboxInfo, *, user_id: str) -> str:
        """Track a sandbox discovered through the backend.

        Raises:
            SandboxBeingDestroyedError: discovery found the container still
                running, but a peer is stopping it. Deliberately propagated
                rather than swallowed: falling through to create would collide
                with the not-yet-removed container name, and handing this one to
                an agent is exactly the mid-turn death (#4206) the store exists to
                prevent. The window is a peer's in-flight container stop, so the
                thread's next turn discovers nothing and cold-starts cleanly.
        """
        if info.requires_replacement:
            raise SandboxPolicyReplacementDeferredError(info.sandbox_id)
        key = self._thread_key(thread_id, user_id)
        with self._lock:
            if self._being_torn_down_locally(info.sandbox_id):
                # Discovery is the fall-through once the caches miss, so it is
                # also the path a reaper's own untracking opens up. `take()` would
                # only refuse this once the reaper's `del:` claim has landed;
                # until then it succeeds against our own lease.
                raise SandboxBeingDestroyedError(info.sandbox_id)
            self._assert_active_identity_available_locked(info.sandbox_id, key)
            self._assert_warm_identity_available_locked(info.sandbox_id, key)

        sandbox = AioSandbox(id=info.sandbox_id, base_url=info.sandbox_url, request_headers=info.request_headers)
        # Ownership first, so a failure cannot leave a tracked-but-unowned sandbox.
        # There is no container to roll back (we did not create it), but the
        # host-side HTTP client constructed above is ours and must not leak —
        # same close-on-failure as `_register_created_sandbox`.
        try:
            self._publish_ownership(info.sandbox_id)
            with self._lock:
                if self._being_torn_down_locally(info.sandbox_id):
                    # The pre-publish reservation check is only an early-out: a
                    # local reaper can reserve the id during the store round
                    # trip. Do not install a client for a container that reaper
                    # has already committed to stopping.
                    raise SandboxBeingDestroyedError(info.sandbox_id)
                self._assert_active_identity_available_locked(info.sandbox_id, key)
                self._assert_warm_identity_available_locked(info.sandbox_id, key)
                # Active and warm are exclusive states, and only this insert can
                # violate that: a warm entry for the same id is stale the moment
                # the id becomes active. Leaving it there gives the container two
                # reapers — `_reap_expired_warm` judges it by the warm timestamp
                # and never looks at `_last_activity`, so it stops a container an
                # agent is actively using while `_sandboxes` still hands out its
                # client.
                self._warm_pool.pop(info.sandbox_id, None)
                self._warm_pool_identity.pop(info.sandbox_id, None)
                self._sandboxes[info.sandbox_id] = sandbox
                self._sandbox_infos[info.sandbox_id] = info
                self._active_sandbox_identity[info.sandbox_id] = key
                self._last_activity[info.sandbox_id] = time.time()
                self._thread_sandboxes[key] = info.sandbox_id
        except (
            OwnershipBackendError,
            SandboxBeingDestroyedError,
            SandboxIdentityCollisionError,
        ):
            try:
                sandbox.close()
            except Exception as e:
                logger.warning(f"Error closing sandbox {info.sandbox_id} after failed ownership publish: {e}")
            raise

        logger.info(f"Discovered existing sandbox {info.sandbox_id} for user/thread {user_id}/{thread_id} at {info.sandbox_url}")
        return info.sandbox_id

    def _register_created_sandbox(self, thread_id: str | None, sandbox_id: str, info: SandboxInfo, *, user_id: str | None = None) -> str:
        """Track a newly-created sandbox in the active maps."""
        sandbox = AioSandbox(id=sandbox_id, base_url=info.sandbox_url, request_headers=info.request_headers)
        key = (
            self._thread_key(
                thread_id,
                self._effective_acquire_user_id(user_id),
            )
            if thread_id
            else None
        )
        # Ownership first. Unlike the discover path there IS something to roll
        # back: we just started this container, and an unowned running container
        # is exactly what a peer's reconciliation adopts. Leaking it would hand a
        # peer a container this instance is about to use.
        # SandboxBeingDestroyedError is possible even here: a peer that died
        # mid-stop leaves a teardown marker until its TTL lapses. Roll back on
        # both, or the container we just started is leaked.
        try:
            if key is not None:
                with self._lock:
                    self._assert_active_identity_available_locked(sandbox_id, key)
                    self._assert_warm_identity_available_locked(sandbox_id, key)
            self._publish_ownership(sandbox_id)

            with self._lock:
                if key is not None:
                    self._assert_active_identity_available_locked(sandbox_id, key)
                    self._assert_warm_identity_available_locked(sandbox_id, key)
                # Same exclusivity rule as the discover path.
                self._warm_pool.pop(sandbox_id, None)
                self._warm_pool_identity.pop(sandbox_id, None)
                self._sandboxes[sandbox_id] = sandbox
                self._sandbox_infos[sandbox_id] = info
                self._active_sandbox_identity[sandbox_id] = key
                self._last_activity[sandbox_id] = time.time()
                if key is not None:
                    self._thread_sandboxes[key] = sandbox_id
        except (
            OwnershipBackendError,
            SandboxBeingDestroyedError,
            SandboxIdentityCollisionError,
        ):
            logger.error(
                "Could not register new sandbox %s; attempting ownership-fenced cleanup",
                sandbox_id,
            )
            try:
                sandbox.close()
            except Exception as e:
                logger.warning(f"Error closing sandbox {sandbox_id} during ownership rollback: {e}")
            self._destroy_unready_sandbox(sandbox_id, info)
            raise

        logger.info(f"Created sandbox {sandbox_id} for thread {thread_id} at {info.sandbox_url}")
        return sandbox_id

    def _check_tracked_sandbox_alive(self, sandbox_id: str, info: SandboxInfo) -> bool | None:
        """Return whether a tracked sandbox appears alive, or None if unknown."""
        try:
            return self._backend.is_alive(info)
        except Exception as e:
            logger.warning(f"Failed to check sandbox {sandbox_id} health: {e}")
            return None

    def _remove_tracked_sandbox(
        self,
        sandbox_id: str,
        *,
        expected_info: SandboxInfo | None = None,
    ) -> tuple[Sandbox | None, SandboxInfo | None, bool]:
        """Remove a sandbox from in-process tracking maps.

        When expected_info is provided, removal only happens if the currently
        tracked active or warm-pool entry is the exact info object that was
        checked. This prevents a stale health-check result from deleting a
        freshly recreated sandbox with the same deterministic id.
        """
        thread_keys_to_remove: list[tuple[str, str]] = []

        with self._lock:
            active_info = self._sandbox_infos.get(sandbox_id)
            warm_item = self._warm_pool.get(sandbox_id)
            warm_info = warm_item[0] if warm_item is not None else None
            if expected_info is not None and active_info is not expected_info and warm_info is not expected_info:
                return None, None, False

            sandbox = self._sandboxes.pop(sandbox_id, None)
            info = self._sandbox_infos.pop(sandbox_id, None)
            self._active_sandbox_identity.pop(sandbox_id, None)
            thread_keys_to_remove = [key for key, sid in self._thread_sandboxes.items() if sid == sandbox_id]
            for key in thread_keys_to_remove:
                del self._thread_sandboxes[key]
            self._last_activity.pop(sandbox_id, None)
            self._acquire_epoch.pop(sandbox_id, None)
            if info is None and sandbox_id in self._warm_pool:
                info, _ = self._warm_pool.pop(sandbox_id)
            else:
                self._warm_pool.pop(sandbox_id, None)
            self._warm_pool_identity.pop(sandbox_id, None)

        return sandbox, info, True

    def _drop_unhealthy_sandbox(self, sandbox_id: str, reason: str, *, expected_info: SandboxInfo | None = None) -> None:
        """Remove and destroy a sandbox after a definitive failed health check."""
        # Reserved for the whole path, not just the stop: this one untracks
        # first, so between the untrack and the `del:` claim an acquire misses
        # the caches and falls through to discovery, where `take()` still
        # succeeds against our own lease.
        if not self._reserve_local_teardown(sandbox_id, lambda: True):
            logger.info(f"Skipped dropping sandbox {sandbox_id}: already being torn down by this instance")
            return
        try:
            self._drop_unhealthy_reserved(sandbox_id, reason, expected_info=expected_info)
        finally:
            self._finish_local_teardown(sandbox_id)

    def _drop_unhealthy_reserved(self, sandbox_id: str, reason: str, *, expected_info: SandboxInfo | None = None) -> None:
        sandbox, info, removed = self._remove_tracked_sandbox(sandbox_id, expected_info=expected_info)
        if not removed:
            logger.info(f"Skipped dropping sandbox {sandbox_id}: tracked info changed after health check")
            return

        if sandbox is not None:
            try:
                sandbox.close()
            except Exception as e:
                logger.warning(f"Error closing unhealthy sandbox {sandbox_id}: {e}")

        if info is not None:
            # Gate this like every other reap path. The container failed a
            # definitive health check, but "definitively dead to us" is not proof
            # it is ours: a peer may have replaced the container behind this id,
            # in which case stopping it is the cross-instance kill again.
            if self._claim_ownership(sandbox_id, for_destroy=True):
                try:
                    # Held like the other two stop paths: this one untracks before
                    # claiming, so `_renew_owned_leases` cannot see the id either
                    # and nothing else would refresh the marker. The heartbeat
                    # releases the marker on exit (success or failure), so there is
                    # no caller-side release to race a late refresh.
                    with self._held_teardown_lease(sandbox_id):
                        self._backend.destroy(info)
                except Exception as e:
                    logger.warning(f"Error destroying unhealthy sandbox {sandbox_id}: {e}")
            else:
                logger.info("Not destroying unhealthy sandbox %s: owned by another instance", sandbox_id)

        logger.warning(f"Dropped unhealthy sandbox {sandbox_id}: {reason}")

    def _active_count_locked(self) -> int:
        """Return active AIO sandbox count while ``_lock`` is held."""
        return len(self._sandboxes)

    def _destroy_warm_entry(self, sandbox_id: str, entry: SandboxInfo, *, reason: str, still_reapable: Callable[[], bool]) -> bool:
        """Destroy a warm-pool sandbox using AIO-specific backend logging.

        Claiming for destroy is the exclusion against **peers**: the lease is
        marked as a teardown, so a concurrent acquire on another instance is
        refused and the container cannot be re-acquired between this decision and
        the stop. That pairing is what replaced the per-sandbox flock guard. A
        claim that fails — peer-owned or backend unavailable — fails closed and
        we do not destroy.

        It is *not* an exclusion against this process: `claim()` succeeds against
        our own `own:` lease, so a same-process reclaim that ran before it wins
        the container and this stop lands on a turn already using it. The
        reservation is that half, and it is taken before the claim — after it,
        the entry stays visible in `_warm_pool` for the whole stop, so a reclaim
        would otherwise still find it.

        ``still_reapable`` is required rather than defaulting to unconditional:
        the safe default is the one that makes a new call site think about it,
        and this signature deliberately diverges from ``WarmPoolLifecycleMixin``'s
        hook for that reason. Safe because this provider overrides both mixin
        callers (``_evict_oldest_warm`` / ``_reap_expired_warm``); if those
        overrides were ever dropped, the mixin's call would fail loudly here
        rather than silently reopen the window.

        Returns:
            ``True`` when the container was stopped and the caller should drop
            its warm-pool entry; ``False`` when it is still running.
        """
        if not self._reserve_local_teardown(sandbox_id, still_reapable):
            logger.info("Refusing to destroy warm-pool sandbox %s for %s: reclaimed by this instance", sandbox_id, reason)
            return False

        try:
            if not self._claim_ownership(sandbox_id, for_destroy=True):
                logger.info("Refusing to destroy warm-pool sandbox %s for %s: owned by another instance", sandbox_id, reason)
                return False

            try:
                # The marker must outlast the stop, not the TTL it was written with,
                # and is released by the heartbeat on exit. On a failed stop that
                # release matters just as much — the container is probably still up,
                # so a marker left behind would block its thread from re-acquiring it.
                with self._held_teardown_lease(sandbox_id):
                    self._backend.destroy(entry)
            except Exception as e:
                if reason == "idle_timeout":
                    logger.error(f"Failed to destroy idle warm-pool sandbox {sandbox_id}: {e}")
                elif reason == "replica_enforcement":
                    logger.error(f"Failed to destroy warm-pool sandbox {sandbox_id}: {e}")
                else:
                    logger.error(f"Failed to destroy warm-pool sandbox {sandbox_id} for {reason}: {e}")
                return False

            # Remove the entry here, inside the reservation, rather than leaving
            # it to the caller. Releasing the reservation when the stop returns
            # and popping afterwards leaves a gap in which the container is
            # already stopped, the entry is still in `_warm_pool`, and nothing
            # marks it — so a reclaim picks it up and hands out a dead container.
            # The pop stays deferred relative to the *stop* (a refused or failed
            # stop keeps the entry), just no longer relative to the reservation.
            with self._lock:
                current = self._warm_pool.get(sandbox_id)
                if current is not None and current[0] is entry:
                    self._warm_pool.pop(sandbox_id, None)
                    self._warm_pool_identity.pop(sandbox_id, None)
        finally:
            self._finish_local_teardown(sandbox_id)

        if reason == "idle_timeout":
            logger.info(f"Destroyed idle warm-pool sandbox {sandbox_id}")
        elif reason == "replica_enforcement":
            logger.info(f"Destroyed warm-pool sandbox {sandbox_id}")
        else:
            logger.info(f"Destroyed warm-pool sandbox {sandbox_id} for {reason}")
        return True

    # ── Core: acquire / get / release / shutdown ─────────────────────────

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        """Acquire a sandbox environment and return its ID.

        For the same thread_id, this method will return the same sandbox_id
        across multiple turns, multiple processes, and (with shared storage)
        multiple pods.

        Thread-safe with both in-process and cross-process locking.

        Args:
            thread_id: Optional thread ID for thread-specific configurations.

        Returns:
            The ID of the acquired sandbox environment.
        """
        effective_user_id = self._effective_acquire_user_id(user_id)
        if thread_id:
            with self._acquire_serializer.hold(self._thread_key(thread_id, effective_user_id)):
                return self._acquire_internal(thread_id, user_id=effective_user_id)
        return self._acquire_internal(thread_id, user_id=effective_user_id)

    def provision_accepted_skills(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> str:
        """Create a durable-run sandbox whose only skills mount is ``.accepted``."""
        sandbox_id = self._acquire_accepted_skills_internal(
            thread_id,
            user_id=user_id,
            binding=binding,
        )
        self.bind_accepted_skill_snapshot(
            sandbox_id,
            thread_id=thread_id,
            user_id=user_id,
            binding=binding,
        )
        return sandbox_id

    async def provision_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
        execution_claim: AcceptedMaterialExecutionClaimV1 | None = None,
        resource_scope_ref: str | None = None,
    ) -> str:
        acquire_task = asyncio.create_task(
            asyncio.to_thread(
                self._provision_accepted_skills_with_claim,
                thread_id,
                user_id,
                binding,
                execution_claim,
                resource_scope_ref,
            ),
            name=f"aio-accepted-acquire:{binding.run_id}",
        )
        try:
            return await asyncio.shield(acquire_task)
        except asyncio.CancelledError as cancellation:
            # Executor work cannot be cancelled once it starts. Recover the
            # exact resource identity before propagating cancellation so a
            # lead or batch caller cannot orphan a newly created sandbox.
            while not acquire_task.done():
                try:
                    await asyncio.shield(acquire_task)
                except asyncio.CancelledError:
                    continue
            try:
                sandbox_id = acquire_task.result()
            except Exception:
                logger.warning(
                    "Accepted sandbox acquisition failed after caller cancellation",
                    exc_info=True,
                )
            else:
                destroy_task = asyncio.create_task(
                    asyncio.to_thread(self.destroy, sandbox_id),
                    name=f"aio-accepted-cancel-cleanup:{binding.run_id}",
                )
                while not destroy_task.done():
                    try:
                        await asyncio.shield(destroy_task)
                    except asyncio.CancelledError:
                        continue
                try:
                    destroy_task.result()
                except Exception:
                    logger.error(
                        "Accepted sandbox cleanup failed after caller cancellation",
                        exc_info=True,
                    )
            raise cancellation

    def _provision_accepted_skills_with_claim(
        self,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
        execution_claim: AcceptedMaterialExecutionClaimV1 | None,
        resource_scope_ref: str | None,
    ) -> str:
        sandbox_id = self._acquire_accepted_skills_internal(
            thread_id,
            user_id=user_id,
            binding=binding,
            execution_claim=execution_claim,
            resource_scope_ref=resource_scope_ref,
        )
        try:
            identity_thread_id = self._accepted_resource_thread_id(
                thread_id,
                resource_scope_ref,
            )
            self.bind_accepted_skill_snapshot(
                sandbox_id,
                thread_id=identity_thread_id,
                user_id=user_id,
                binding=binding,
            )
        except BaseException as exc:
            try:
                self.destroy(sandbox_id)
            except BaseException:
                logger.error(
                    "Accepted sandbox cleanup failed after binding failure",
                    exc_info=True,
                )
            raise exc
        return sandbox_id

    @staticmethod
    def _accepted_resource_thread_id(
        thread_id: str,
        resource_scope_ref: str | None,
    ) -> str:
        """Separate attempt ownership identity from the mounted parent thread."""

        if resource_scope_ref is None:
            return thread_id
        if not isinstance(resource_scope_ref, str) or not resource_scope_ref or len(resource_scope_ref.encode("utf-8")) > 512:
            raise AcceptedSkillSandboxBindingError(
                "accepted_material_scope_unavailable",
            )
        digest = hashlib.sha256(
            b"hartmesh.accepted-sandbox-attempt.v1\0" + thread_id.encode("utf-8") + b"\0" + resource_scope_ref.encode("utf-8"),
        ).hexdigest()
        return f"accepted-attempt-{digest[:32]}"

    async def recover_bound_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
        execution_claim: AcceptedMaterialExecutionClaimV1,
    ) -> str:
        if not execution_claim.execution_takeover:
            raise AcceptedSkillSandboxBindingError(
                "accepted_material_execution_takeover_invalid",
            )
        return await self.provision_accepted_skills_async(
            thread_id,
            user_id=user_id,
            binding=binding,
            execution_claim=execution_claim,
        )

    def _acquire_accepted_skills_internal(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
        execution_claim: AcceptedMaterialExecutionClaimV1 | None = None,
        resource_scope_ref: str | None = None,
    ) -> str:
        effective_user_id = self._effective_acquire_user_id(user_id)
        try:
            skills_root = get_app_config().skills.container_path.rstrip("/")
        except FileNotFoundError:
            from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH

            skills_root = DEFAULT_SKILLS_CONTAINER_PATH.rstrip("/")
        configured_host_mounts: list[tuple[str, bool]] = []
        for mount in self._config.get("mounts") or ():
            container_path = getattr(mount, "container_path", None)
            host_path = getattr(mount, "host_path", None)
            read_only = bool(getattr(mount, "read_only", False))
            if container_path is None and isinstance(mount, dict):
                container_path = mount.get("container_path")
                host_path = mount.get("host_path")
                read_only = bool(mount.get("read_only", False))
            if isinstance(container_path, str) and (container_path.rstrip("/") == skills_root or container_path.startswith(f"{skills_root}/")):
                raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_isolation_conflict")
            if isinstance(host_path, str):
                configured_host_mounts.append((host_path, read_only))
        paths = get_paths()
        reject_writable_accepted_skill_aliases(
            paths.host_skill_snapshot_active_view_dir(
                effective_user_id,
                thread_id,
            ),
            configured_host_mounts,
        )
        identity_thread_id = self._accepted_resource_thread_id(
            thread_id,
            resource_scope_ref,
        )
        key = self._thread_key(identity_thread_id, effective_user_id)
        with self._acquire_serializer.hold(key):
            with self._lock:
                existing = self._thread_sandboxes.get(key)
                accepted_ids = getattr(self, "_accepted_only_sandbox_ids", set())
            if existing is not None:
                if existing in accepted_ids:
                    return existing
                raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_isolation_conflict")
            sandbox_id = f"{self._sandbox_id_for_thread(identity_thread_id, effective_user_id)}{ACCEPTED_SANDBOX_ID_SUFFIX}"
            created = self._create_sandbox(
                thread_id,
                sandbox_id,
                user_id=effective_user_id,
                accepted_skills_only=True,
                accepted_skill_binding=binding,
                accepted_execution_claim=execution_claim,
                identity_thread_id=identity_thread_id,
            )
            with self._lock:
                accepted_ids = getattr(self, "_accepted_only_sandbox_ids", None)
                if accepted_ids is None:
                    accepted_ids = self._accepted_only_sandbox_ids = set()
                accepted_ids.add(created)
            return created

    def has_accepted_skill_isolation(self, sandbox_id: str) -> bool:
        with self._lock:
            return sandbox_id in getattr(self, "_accepted_only_sandbox_ids", set())

    def accepted_skill_material_capability(
        self,
        sandbox_id: str,
    ) -> AcceptedMaterialCapability:
        if not self.has_accepted_skill_isolation(sandbox_id):
            return AcceptedMaterialCapability.EMPTY_ONLY
        if isinstance(getattr(self, "_backend", None), RemoteSandboxBackend):
            with self._lock:
                info = self._sandbox_infos.get(sandbox_id)
            if info is None or not isinstance(
                info.accepted_skill_material,
                AcceptedSkillMaterialReceiptV2,
            ):
                return AcceptedMaterialCapability.EMPTY_ONLY
        return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

    def provider_neutral_accepted_materialization_enabled(self) -> bool:
        """Return whether this instance is the qualified remote AIO v2 adapter."""

        return self._config.get("accepted_skill_projection_profile") == "rwx_verified_copy_v2" and isinstance(self._backend, RemoteSandboxBackend)

    @staticmethod
    def accepted_sandbox_capability_profile() -> AcceptedSandboxCapabilityProfileV1:
        """Declare AIO guarantees separately from live qualification.

        AIO's Redis/Kubernetes ownership seam is authoritative for lease
        acquisition, but its ordinary sandbox operations do not carry the
        expected ownership epoch into the provider acceptance step.  The
        stronger atomic-operation flag and exact-two support therefore remain
        false.
        """

        return AcceptedSandboxCapabilityProfileV1.build(
            material_capability=AcceptedMaterialCapability.IMMUTABLE_READ_ONLY,
            atomic_provider_ownership_fencing=True,
            atomic_provider_operation_fencing=False,
            authoritative_shared_expiry=True,
            resolved_immutable_image=True,
            restricted_non_root_isolation=True,
            # The running process can revalidate the protected resource, but
            # a replacement process cannot recover the ephemeral attempt
            # capability. Process loss therefore terminalizes and reconciles.
            recoverable_resource_lookup=False,
            durable_one_replica=True,
            exact_two=False,
        )

    async def _accepted_sandbox_qualification(
        self,
        *,
        profile: AcceptedSandboxCapabilityProfileV1,
        runtime_image_digest: str,
    ) -> AcceptedSandboxQualificationV1:
        """Load one bounded, canonical, current live qualification artifact."""

        from deerflow.qualification_evidence import (
            ACCEPTED_SANDBOX_OPERATION_FENCING_MODE_V1,
            MAX_QUALIFICATION_EVIDENCE_BYTES,
            AcceptedSandboxQualificationArtifactV1,
            AcceptedSandboxRuntimeTopologyV1,
            qualification_evidence_digest,
        )

        try:
            runtime_subjects = await asyncio.to_thread(
                self._backend.accepted_material_runtime_qualification_subjects,
            )
        except (AttributeError, RuntimeError, TypeError, ValueError):
            raise AcceptedMaterialError("sandbox_provider_unqualified") from None
        if not isinstance(runtime_subjects, tuple) or len(runtime_subjects) != 3:
            raise AcceptedMaterialError("sandbox_provider_unqualified")
        sandbox_image_digest, verifier_image_digest, runtime_topology = runtime_subjects
        if any(not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None for digest in (sandbox_image_digest, verifier_image_digest)) or not isinstance(runtime_topology, AcceptedSandboxRuntimeTopologyV1):
            raise AcceptedMaterialError("sandbox_provider_unqualified")
        if sandbox_image_digest != runtime_image_digest:
            raise AcceptedMaterialError("sandbox_image_unresolved")

        gateway_image_digest = os.getenv("DEER_FLOW_IMAGE_DIGEST", "")
        if re.fullmatch(r"sha256:[0-9a-f]{64}", gateway_image_digest) is None:
            raise AcceptedMaterialError("sandbox_provider_unqualified")

        reference = self._config.get("accepted_material_qualification_evidence")
        expected_digest = self._config.get(
            "accepted_material_qualification_digest",
        )
        if not isinstance(reference, str) or not reference or not isinstance(expected_digest, str) or not expected_digest:
            from deerflow.runtime.kubernetes_qualification import (
                accepted_sandbox_qualification_candidate_enabled,
            )

            if not accepted_sandbox_qualification_candidate_enabled():
                raise AcceptedMaterialError("sandbox_provider_unqualified")
            now = datetime.now(UTC)
            candidate_id = os.environ["DEER_FLOW_QUALIFICATION_CANDIDATE_ID"]
            candidate_payload = {
                "version": 1,
                "candidate_id": candidate_id,
                "gateway_image_digest": gateway_image_digest,
                "sandbox_image_digest": sandbox_image_digest,
                "verifier_image_digest": verifier_image_digest,
            }
            candidate_digest = hashlib.sha256(
                json.dumps(
                    candidate_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"),
            ).hexdigest()
            return AcceptedSandboxQualificationV1.build(
                capability_profile_digest=profile.digest,
                qualification_scope=(AcceptedSandboxQualificationArtifactV1.SCOPE),
                artifact_digest=candidate_digest,
                topology_digest=runtime_topology.digest,
                verified_at=now,
                expires_at=now + timedelta(minutes=15),
                status="candidate",
            )

        def _read_bounded_artifact() -> bytes:
            with Path(reference).open("rb") as artifact:
                payload = artifact.read(MAX_QUALIFICATION_EVIDENCE_BYTES + 1)
            if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
                raise ValueError("qualification evidence exceeds bounded size")
            return payload

        try:
            payload = await asyncio.to_thread(_read_bounded_artifact)
            actual_digest = qualification_evidence_digest(payload)
            if actual_digest.removeprefix("sha256:") != expected_digest.removeprefix(
                "sha256:",
            ):
                raise ValueError("qualification artifact digest mismatch")
            evidence = AcceptedSandboxQualificationArtifactV1.from_bytes(payload)
        except (OSError, TypeError, ValueError):
            raise AcceptedMaterialError("sandbox_provider_unqualified") from None
        subordinate = evidence.accepted_skill_evidence
        if subordinate.sandbox_image_digest.removeprefix("sha256:") != runtime_image_digest:
            raise AcceptedMaterialError("sandbox_image_unresolved")
        if (
            subordinate.gateway_image_digest != gateway_image_digest
            or subordinate.provisioner_image_digest.removeprefix("sha256:") != verifier_image_digest
            or subordinate.verifier_image_digest.removeprefix("sha256:") != verifier_image_digest
            or evidence.provider_kind != "aio_kubernetes"
            or evidence.capability_profile_version != profile.version
            or evidence.capability_profile_digest != profile.digest
            or evidence.operation_fencing_mode != ACCEPTED_SANDBOX_OPERATION_FENCING_MODE_V1
            or profile.atomic_provider_operation_fencing
            or evidence.topology_policy_digest != runtime_topology.qualification_policy_digest
        ):
            raise AcceptedMaterialError("sandbox_provider_unqualified")
        max_age_seconds = self._config.get(
            "accepted_material_qualification_max_age_seconds",
            30 * 24 * 60 * 60,
        )
        if type(max_age_seconds) is not int or max_age_seconds < 60 or max_age_seconds > 365 * 24 * 60 * 60:
            raise AcceptedMaterialError("sandbox_provider_unqualified")
        qualification = AcceptedSandboxQualificationV1.build(
            capability_profile_digest=profile.digest,
            qualification_scope=evidence.SCOPE,
            artifact_digest=actual_digest.removeprefix("sha256:"),
            topology_digest=runtime_topology.digest,
            verified_at=evidence.completed_at,
            expires_at=evidence.completed_at + timedelta(seconds=max_age_seconds),
        )
        if not qualification.is_current(datetime.now(UTC)):
            raise AcceptedMaterialError("sandbox_provider_unqualified")
        return qualification

    async def accepted_materializer_selection(
        self,
        *,
        binding: AcceptedSkillSandboxBindingV1,
        thread_id: str,
        user_id: str,
    ) -> AcceptedMaterializerSelection | None:
        """Construct the qualified neutral adapter without exposing AIO to callers."""

        if not self.provider_neutral_accepted_materialization_enabled():
            return None
        from .accepted_materializer import AioAcceptedMaterializer

        runtime_image_digest = await self.accepted_material_runtime_image_digest_async()
        profile = self.accepted_sandbox_capability_profile()
        qualification = await self._accepted_sandbox_qualification(
            profile=profile,
            runtime_image_digest=runtime_image_digest,
        )
        lease_duration = timedelta(
            seconds=self._config["accepted_material_lease_duration_seconds"],
        )
        return AcceptedMaterializerSelection(
            materializer=AioAcceptedMaterializer(
                provider=self,
                binding_resolver=lambda _request: binding,
                scope_resolver=lambda _request: (thread_id, user_id),
                lease_duration=lease_duration,
                qualification=qualification,
            ),
            runtime_image_digest=runtime_image_digest,
            lease_duration=lease_duration,
            capability_profile=profile,
            qualification=qualification,
        )

    def accepted_skill_execution_evidence(
        self,
        sandbox_id: str,
    ) -> AcceptedSkillExecutionEvidence | None:
        with self._lock:
            info = self._sandbox_infos.get(sandbox_id)
        receipt = None if info is None else info.accepted_skill_material
        if not isinstance(receipt, AcceptedSkillMaterialReceiptV2):
            return None
        return AcceptedSkillExecutionEvidenceV2(
            profile=receipt.profile,
            attempt_id=receipt.attempt_id,
            snapshot_id=receipt.snapshot_id,
            run_id=receipt.run_id,
            generation=receipt.generation,
            pod_uid=receipt.pod_uid,
            pod_isolation_digest=receipt.pod_isolation_digest,
            lease_uid=receipt.lease_uid,
            network_policy_uid=receipt.network_policy_uid,
            network_policy_spec_digest=receipt.network_policy_spec_digest,
            evidence_secret_uid=receipt.evidence_secret_uid,
            evidence_secret_digest=receipt.evidence_secret_digest,
            capability_secret_uid=receipt.capability_secret_uid,
            capability_secret_digest=receipt.capability_secret_digest,
            sandbox_image_digest=receipt.sandbox_image_digest,
            accepted_skill_runtime_image_digest=(receipt.accepted_skill_runtime_image_digest),
            runtime_image_ids_digest=receipt.runtime_image_ids_digest,
            verifier_receipt_digest=receipt.verifier_receipt_digest,
            materialization_evidence_digest=(receipt.materialization_evidence_digest),
        )

    def _accepted_execution_info(
        self,
        sandbox_id: str,
        evidence: AcceptedSkillExecutionEvidence,
    ) -> SandboxInfo | None:
        with self._lock:
            info = self._sandbox_infos.get(sandbox_id)
        if info is None or info.accepted_skill_material is None:
            return None
        current = self.accepted_skill_execution_evidence(sandbox_id)
        if current is None or current != evidence:
            return None
        return info

    async def validate_accepted_skill_execution_async(
        self,
        sandbox_id: str,
        evidence: AcceptedSkillExecutionEvidence,
    ) -> bool:
        """Validate the exact remote Pod/Lease/materialization tuple."""

        info = self._accepted_execution_info(sandbox_id, evidence)
        if info is None or not isinstance(self._backend, RemoteSandboxBackend):
            return False
        try:
            return await asyncio.to_thread(self._backend.is_alive, info)
        except Exception:
            logger.warning(
                "Accepted sandbox execution fence unavailable for %s",
                sandbox_id,
            )
            return False

    async def renew_accepted_skill_execution_async(
        self,
        sandbox_id: str,
        evidence: AcceptedSkillExecutionEvidence,
    ) -> bool:
        """Renew only after the owning RunManager renewed its durable lease."""

        info = self._accepted_execution_info(sandbox_id, evidence)
        if info is None or not isinstance(self._backend, RemoteSandboxBackend):
            return False
        return await asyncio.to_thread(self._backend.renew_accepted_attempt, info)

    async def accepted_material_runtime_image_digest_async(self) -> str:
        """Read the pinned runtime digest from authenticated provisioner preflight."""

        if self._config.get("accepted_skill_projection_profile") != "rwx_verified_copy_v2" or not isinstance(self._backend, RemoteSandboxBackend):
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_immutability_unsupported",
            )
        return await asyncio.to_thread(
            self._backend.accepted_material_runtime_image_digest,
        )

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        """Acquire a sandbox environment without blocking the event loop.

        Mirrors ``acquire()`` while keeping blocking backend operations off the
        event loop and using async-native readiness polling for newly created
        sandboxes.
        """
        effective_user_id = self._effective_acquire_user_id(user_id)
        if thread_id:
            async with self._acquire_serializer.hold_async(self._thread_key(thread_id, effective_user_id)):
                return await self._acquire_internal_async(thread_id, user_id=effective_user_id)
        return await self._acquire_internal_async(thread_id, user_id=effective_user_id)

    def _acquire_internal(self, thread_id: str | None, *, user_id: str) -> str:
        """Internal sandbox acquisition with two-layer consistency.

        Layer 1: In-process cache (fastest, covers same-process repeated access)
        Layer 2: Backend discovery (covers containers started by other processes;
                 sandbox_id is deterministic from thread_id so no shared state file
                 is needed — any process can derive the same container name)
        """
        self._ensure_skills_projection(user_id)
        cached_id = self._reuse_in_process_sandbox(thread_id, user_id=user_id)
        if cached_id is not None:
            return cached_id

        # Deterministic ID for thread-specific, random for anonymous
        sandbox_id = self._sandbox_id_for_thread(thread_id, user_id)
        if thread_id:
            key = self._thread_key(thread_id, user_id)
            with self._lock:
                self._assert_active_identity_available_locked(sandbox_id, key)

        # ── Layer 1.5: Warm pool (container still running, no cold-start) ──
        reclaimed_id = self._reclaim_warm_pool_sandbox(thread_id, sandbox_id, user_id=user_id)
        if reclaimed_id is not None:
            return reclaimed_id

        # ── Layer 2: Backend discovery + create (protected by cross-process lock) ──
        # Use a file lock so that two processes racing to create the same sandbox
        # for the same thread_id serialize here: the second process will discover
        # the container started by the first instead of hitting a name-conflict.
        if thread_id:
            return self._discover_or_create_with_lock(thread_id, sandbox_id, user_id=user_id)

        return self._create_sandbox(thread_id, sandbox_id, user_id=user_id)

    async def _acquire_internal_async(self, thread_id: str | None, *, user_id: str) -> str:
        """Async counterpart to ``_acquire_internal``."""
        await asyncio.to_thread(self._ensure_skills_projection, user_id)
        cached_id = await asyncio.to_thread(self._reuse_in_process_sandbox, thread_id, user_id=user_id)
        if cached_id is not None:
            return cached_id

        # Deterministic ID for thread-specific, random for anonymous
        sandbox_id = self._sandbox_id_for_thread(thread_id, user_id)
        if thread_id:
            key = self._thread_key(thread_id, user_id)
            with self._lock:
                self._assert_active_identity_available_locked(sandbox_id, key)

        # ── Layer 1.5: Warm pool (container still running, no cold-start) ──
        reclaimed_id = await asyncio.to_thread(self._reclaim_warm_pool_sandbox, thread_id, sandbox_id, user_id=user_id)
        if reclaimed_id is not None:
            return reclaimed_id

        # ── Layer 2: Backend discovery + create (protected by cross-process lock) ──
        if thread_id:
            return await self._discover_or_create_with_lock_async(thread_id, sandbox_id, user_id=user_id)

        return await self._create_sandbox_async(thread_id, sandbox_id, user_id=user_id)

    def _discover_or_create_with_lock(self, thread_id: str, sandbox_id: str, *, user_id: str | None = None) -> str:
        """Discover an existing sandbox or create a new one under a cross-process file lock.

        The file lock serializes concurrent sandbox creation for the same thread_id
        across multiple processes, preventing container-name conflicts.
        """
        paths = get_paths()
        effective_user_id = self._effective_acquire_user_id(user_id)
        paths.ensure_thread_dirs(thread_id, user_id=effective_user_id)
        lock_path = paths.thread_dir(thread_id, user_id=effective_user_id) / f"{sandbox_id}.lock"

        with open(lock_path, "a", encoding="utf-8") as lock_file:
            locked = False
            try:
                _lock_file_exclusive(lock_file)
                locked = True
                # Re-check in-process caches under the file lock in case another
                # thread in this process won the race while we were waiting.
                cached_id = self._recheck_cached_sandbox(thread_id, sandbox_id, user_id=effective_user_id)
                if cached_id is not None:
                    return cached_id

                # Backend discovery: another process may have created the container.
                discovered = self._backend.discover(sandbox_id)
                if discovered is not None:
                    if discovered.requires_replacement:
                        if not self._replace_incompatible_sandbox(discovered, time.time()):
                            raise SandboxPolicyReplacementDeferredError(sandbox_id)
                    else:
                        return self._register_discovered_sandbox(thread_id, discovered, user_id=effective_user_id)

                return self._create_sandbox(thread_id, sandbox_id, user_id=effective_user_id)
            finally:
                if locked:
                    _unlock_file(lock_file)

    async def _discover_or_create_with_lock_async(self, thread_id: str, sandbox_id: str, *, user_id: str | None = None) -> str:
        """Async counterpart to ``_discover_or_create_with_lock``."""
        paths = get_paths()
        effective_user_id = self._effective_acquire_user_id(user_id)
        await asyncio.to_thread(paths.ensure_thread_dirs, thread_id, user_id=effective_user_id)
        lock_path = paths.thread_dir(thread_id, user_id=effective_user_id) / f"{sandbox_id}.lock"

        lock_file = await asyncio.to_thread(_open_lock_file, lock_path)
        locked = False
        try:
            await asyncio.to_thread(_lock_file_exclusive, lock_file)
            locked = True
            # Re-check in-process caches under the file lock in case another
            # thread in this process won the race while we were waiting.
            cached_id = await asyncio.to_thread(self._recheck_cached_sandbox, thread_id, sandbox_id, user_id=effective_user_id)
            if cached_id is not None:
                return cached_id

            # Backend discovery is sync because local discovery may inspect
            # Docker and perform a health check; keep it off the event loop.
            discovered = await asyncio.to_thread(self._backend.discover, sandbox_id)
            if discovered is not None:
                if discovered.requires_replacement:
                    replaced = await asyncio.to_thread(
                        self._replace_incompatible_sandbox,
                        discovered,
                        time.time(),
                    )
                    if not replaced:
                        raise SandboxPolicyReplacementDeferredError(sandbox_id)
                else:
                    # Registration publishes ownership, which is blocking store
                    # IO (filesystem or network depending on the backend) — same
                    # reason every other step in this coroutine is offloaded.
                    return await asyncio.to_thread(self._register_discovered_sandbox, thread_id, discovered, user_id=effective_user_id)

            return await self._create_sandbox_async(thread_id, sandbox_id, user_id=effective_user_id)
        finally:
            if locked:
                await asyncio.to_thread(_unlock_file, lock_file)
            await asyncio.to_thread(lock_file.close)

    def _destroy_unready_sandbox(self, sandbox_id: str, info: SandboxInfo) -> None:
        """Tear down a freshly-created container whose readiness check failed.

        The container was started by the backend but never reached ready, so it
        never entered ``_register_created_sandbox`` and the ownership store has
        no lease for it yet. For the full readiness timeout (60s) it runs
        unowned, which is exactly the window a peer gateway's startup
        reconciliation is built to adopt across (#4206). Without a claim, a peer
        that adopts the not-yet-ready Pod and then has this instance's stop land
        on it is a cross-instance kill that interrupts an active turn (#4248).

        Claim the teardown lease first so this reap path is gated by the same
        ownership guard as every other destroy (``_destroy_warm_entry``,
        ``_drop_unhealthy_reserved``); fail closed (leave the container for the
        peer to reap via its own reconciliation) if a peer already owns it or
        the ownership store cannot answer.

        The claim alone is only the cross-**instance** half: it succeeds against
        our own lease by design, so it says nothing about this process. The
        same-process half is the local teardown reservation, taken first and
        held across the whole path — between the readiness timeout and the
        claim, ``_reconcile_orphans`` (idle checker, every 60s) can see this
        container running, untracked, and past its recovery grace, and park it
        in ``_warm_pool``; the subsequent claim would still succeed and the
        stop would land on an entry this instance has just adopted, leaving a
        dead warm entry for the next reclaim to hand out. The predicate checks
        the id is absent from both the active and warm maps; the reservation
        makes that check and the teardown mark one critical section, so no
        adopt/acquire can slip between them (same pairing as
        ``_destroy_warm_entry``).
        """
        if not self._reserve_local_teardown(
            sandbox_id,
            lambda: sandbox_id not in self._sandboxes and sandbox_id not in self._sandbox_infos and sandbox_id not in self._warm_pool,
        ):
            logger.warning(
                "Not destroying unready sandbox %s: adopted or being torn down by this instance",
                sandbox_id,
            )
            return
        try:
            if not self._claim_ownership(sandbox_id, for_destroy=True):
                logger.warning(
                    "Not destroying unready sandbox %s: owned by another instance or ownership unavailable",
                    sandbox_id,
                )
                return
            try:
                with self._held_teardown_lease(sandbox_id):
                    self._backend.destroy(info)
            except Exception as e:
                logger.warning(f"Error destroying unready sandbox {sandbox_id}: {e}")
        finally:
            self._finish_local_teardown(sandbox_id)

    def _create_sandbox(
        self,
        thread_id: str | None,
        sandbox_id: str,
        *,
        user_id: str | None = None,
        accepted_skills_only: bool = False,
        accepted_skill_binding: AcceptedSkillSandboxBindingV1 | None = None,
        accepted_execution_claim: AcceptedMaterialExecutionClaimV1 | None = None,
        identity_thread_id: str | None = None,
    ) -> str:
        """Create a new sandbox via the backend.

        Args:
            thread_id: Optional thread ID.
            sandbox_id: The sandbox ID to use.

        Returns:
            The sandbox_id.

        Raises:
            RuntimeError: If sandbox creation or readiness check fails.
        """
        effective_user_id = self._effective_acquire_user_id(user_id)
        if accepted_skills_only:
            extra_mounts = self._get_extra_mounts(
                thread_id,
                user_id=effective_user_id,
                accepted_skills_only=True,
            )
        else:
            extra_mounts = self._get_extra_mounts(thread_id, user_id=effective_user_id)
        provision_lark_cli_runtime = self._lark_integration_active(effective_user_id)
        provision_lark_cli_broker = self._lark_broker_active(effective_user_id)
        config_mount_exclusion_root = self._local_config_mount_exclusion_root(
            thread_id,
            user_id=effective_user_id,
        )

        # Enforce replicas: only warm-pool containers count toward eviction budget.
        # Active sandboxes are in use by live threads and must not be forcibly stopped.
        replicas, total = self._replica_count()
        if total >= replicas:
            evicted = self._evict_oldest_warm()
            self._log_replicas_soft_cap(replicas, sandbox_id, evicted)

        create_kwargs = {}
        if config_mount_exclusion_root is not None and not isinstance(self._backend, RemoteSandboxBackend):
            create_kwargs["config_mount_exclusion_root"] = config_mount_exclusion_root
        if isinstance(self._backend, RemoteSandboxBackend):
            create_kwargs["skills_container_path"] = self._configured_skills_container_path()
            create_kwargs["accepted_skills_only"] = accepted_skills_only
            create_kwargs["accepted_skill_binding"] = accepted_skill_binding
            create_kwargs["accepted_execution_claim"] = accepted_execution_claim
        info = self._backend.create(
            thread_id,
            sandbox_id,
            extra_mounts=extra_mounts or None,
            user_id=effective_user_id,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
            **create_kwargs,
        )

        # Wait for sandbox to be ready
        readiness_kwargs = {"headers": info.request_headers} if info.request_headers else {}
        if not wait_for_sandbox_ready(info.sandbox_url, timeout=SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT, **readiness_kwargs):
            # The container is running but unowned: ownership is published by
            # ``_register_created_sandbox`` after this gate. Claim the teardown
            # lease before stopping it so a peer cannot adopt the not-yet-ready
            # Pod in the meantime (#4248).
            self._destroy_unready_sandbox(sandbox_id, info)
            raise RuntimeError(f"Sandbox {sandbox_id} failed to become ready within timeout at {info.sandbox_url}")

        return self._register_created_sandbox(
            identity_thread_id or thread_id,
            sandbox_id,
            info,
            user_id=effective_user_id,
        )

    async def _create_sandbox_async(self, thread_id: str | None, sandbox_id: str, *, user_id: str | None = None) -> str:
        """Async counterpart to ``_create_sandbox``."""
        effective_user_id = self._effective_acquire_user_id(user_id)
        extra_mounts = await asyncio.to_thread(self._get_extra_mounts, thread_id, user_id=effective_user_id)
        provision_lark_cli_runtime = await asyncio.to_thread(self._lark_integration_active, effective_user_id)
        provision_lark_cli_broker = await asyncio.to_thread(self._lark_broker_active, effective_user_id)
        config_mount_exclusion_root = await asyncio.to_thread(
            self._local_config_mount_exclusion_root,
            thread_id,
            user_id=effective_user_id,
        )

        # Enforce replicas: only warm-pool containers count toward eviction budget.
        # Active sandboxes are in use by live threads and must not be forcibly stopped.
        replicas, total = self._replica_count()
        if total >= replicas:
            evicted = await asyncio.to_thread(self._evict_oldest_warm)
            self._log_replicas_soft_cap(replicas, sandbox_id, evicted)

        create_kwargs = {}
        if config_mount_exclusion_root is not None:
            create_kwargs["config_mount_exclusion_root"] = config_mount_exclusion_root
        if isinstance(self._backend, RemoteSandboxBackend):
            create_kwargs["skills_container_path"] = self._configured_skills_container_path()
        info = await asyncio.to_thread(
            self._backend.create,
            thread_id,
            sandbox_id,
            extra_mounts=extra_mounts or None,
            user_id=effective_user_id,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
            **create_kwargs,
        )

        # Wait for sandbox to be ready without blocking the event loop.
        readiness_kwargs = {"headers": info.request_headers} if info.request_headers else {}
        if not await wait_for_sandbox_ready_async(
            info.sandbox_url,
            timeout=SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT,
            **readiness_kwargs,
        ):
            # The container is running but unowned: ownership is published by
            # ``_register_created_sandbox`` after this gate. Claim the teardown
            # lease before stopping it so a peer cannot adopt the not-yet-ready
            # Pod in the meantime (#4248).
            await asyncio.to_thread(self._destroy_unready_sandbox, sandbox_id, info)
            raise RuntimeError(f"Sandbox {sandbox_id} failed to become ready within timeout at {info.sandbox_url}")

        # Registration publishes ownership (blocking store IO), so it is offloaded
        # like every other blocking step on this path.
        return await asyncio.to_thread(self._register_created_sandbox, thread_id, sandbox_id, info, user_id=effective_user_id)

    def get(self, sandbox_id: str) -> Sandbox | None:
        """Get a sandbox by ID. Updates last activity timestamp.

        Stays a pure in-memory lookup: async tool paths call this directly on the
        event loop (``ensure_sandbox_initialized_async``), so it must not touch
        the ownership store — that is blocking filesystem or network IO depending
        on the backend. Ownership is published off the event loop on
        acquire/reclaim and refreshed by the renewal thread (see
        ``_renew_owned_leases``).

        Args:
            sandbox_id: The ID of the sandbox.

        Returns:
            The sandbox instance if found, None otherwise.
        """
        with self._lock:
            sandbox = self._sandboxes.get(sandbox_id)
            if sandbox is not None:
                self._last_activity[sandbox_id] = time.time()
        return sandbox

    def release(self, sandbox_id: str) -> None:
        """Release a sandbox from active use into the warm pool.

        The container is kept running so it can be reclaimed quickly by the same
        thread on its next turn without a cold-start.  The container will only be
        stopped when the replicas limit forces eviction or during shutdown.

        The host-side HTTP client owned by the cached ``AioSandbox`` instance is
        closed before the instance is dropped (#2872). The warm-pool entry only
        stores ``SandboxInfo``, so a fresh ``AioSandbox`` (and a fresh client)
        is constructed if the container is later reclaimed.

        Args:
            sandbox_id: The ID of the sandbox to release.
        """
        info = None
        sandbox = None
        thread_keys_to_remove: list[tuple[str, str]] = []

        with self._lock:
            sandbox = self._sandboxes.pop(sandbox_id, None)
            info = self._sandbox_infos.pop(sandbox_id, None)
            thread_keys_to_remove = [key for key, sid in self._thread_sandboxes.items() if sid == sandbox_id]
            for key in thread_keys_to_remove:
                del self._thread_sandboxes[key]
            active_identity = self._active_sandbox_identity.pop(sandbox_id, None)
            self._last_activity.pop(sandbox_id, None)
            # Park in warm pool — container keeps running
            if info and sandbox_id not in self._warm_pool:
                self._warm_pool[sandbox_id] = (info, time.time())
                self._warm_pool_identity[sandbox_id] = thread_keys_to_remove[0] if thread_keys_to_remove else active_identity

        if sandbox is not None:
            # Defense-in-depth: close() already swallows its own errors; this
            # guard only protects against a future close() that misbehaves, so
            # host-side client cleanup can never block parking in the warm pool.
            try:
                sandbox.close()
            except Exception as e:
                logger.warning(f"Error closing sandbox {sandbox_id} during release: {e}")

        # Keep the lease while warm so a peer cannot adopt+destroy before we
        # reclaim, re-establishing it if it lapsed during a long turn. Never
        # raises: the turn is already over, so a store problem must not surface
        # through after_agent, and the renewal thread (which covers warm entries)
        # is the actual guarantee — this only narrows the window.
        if info is not None:
            # Same staleness as the renewal thread: the refresh is a store round
            # trip, and the thread's next turn can reclaim this warm entry while
            # it is in flight. Only drop it if nothing re-acquired it since.
            epoch = self._acquire_epoch_of(sandbox_id)
            if not self._refresh_ownership(sandbox_id):
                logger.warning("Sandbox %s is owned by another instance; releasing it from this warm pool", sandbox_id)
                self._forget_lost_sandbox(sandbox_id, expected_epoch=epoch)

        logger.info(f"Released sandbox {sandbox_id} to warm pool (container still running)")

    def _identity_for_sandbox(self, sandbox_id: str) -> tuple[str, str] | None:
        with self._lock:
            identity = self._active_sandbox_identity.get(sandbox_id)
            if identity is not None:
                return identity
            return next(
                (key for key, mapped_id in self._thread_sandboxes.items() if mapped_id == sandbox_id),
                None,
            )

    def bind_accepted_skill_snapshot(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> None:
        if self._identity_for_sandbox(sandbox_id) != (user_id, thread_id):
            raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_sandbox_identity_mismatch")
        if isinstance(getattr(self, "_backend", None), RemoteSandboxBackend):
            with self._lock:
                info = self._sandbox_infos.get(sandbox_id)
            receipt = None if info is None else info.accepted_skill_material
            if receipt is None:
                if binding.snapshot_id is None:
                    return
                raise AcceptedSkillSandboxBindingError(
                    "accepted_skill_snapshot_immutability_unsupported",
                )
            if (
                not isinstance(receipt, AcceptedSkillMaterialReceiptV2)
                or receipt.profile != "rwx_verified_copy_v2"
                or receipt.snapshot_id != binding.snapshot_id
                or receipt.content_digest != binding.snapshot_id
                or receipt.run_id != binding.run_id
                or receipt.generation != binding.generation
            ):
                raise AcceptedSkillSandboxBindingError(
                    "accepted_skill_snapshot_receipt_mismatch",
                )
            return
        from deerflow.runtime.skill_snapshot import bind_skill_snapshot_active_view

        try:
            bind_skill_snapshot_active_view(
                user_id=user_id,
                thread_id=thread_id,
                snapshot_id=binding.snapshot_id,
                run_id=binding.run_id,
                generation=binding.generation,
                evidence=binding.evidence,
            )
        except Exception as exc:
            raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_projection_failed") from exc

    def _clear_bound_accepted_skill_snapshot(self, sandbox_id: str) -> None:
        if isinstance(getattr(self, "_backend", None), RemoteSandboxBackend):
            return
        identity = self._identity_for_sandbox(sandbox_id)
        if identity is None:
            return
        user_id, thread_id = identity
        from deerflow.runtime.skill_snapshot import force_clear_skill_snapshot_active_view

        force_clear_skill_snapshot_active_view(user_id=user_id, thread_id=thread_id)

    def clear_accepted_skill_snapshot(
        self,
        clear: "SkillProjectionClear",
    ) -> bool:
        from deerflow.runtime.skill_projection import SkillProjectionClear
        from deerflow.runtime.skill_snapshot import clear_skill_snapshot_active_view

        if not isinstance(clear, SkillProjectionClear):
            raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_clear_fence_invalid")
        if isinstance(getattr(self, "_backend", None), RemoteSandboxBackend):
            if self._identity_for_sandbox(clear.sandbox_id) != (
                clear.user_id,
                clear.thread_id,
            ):
                return False
            with self._lock:
                info = self._sandbox_infos.get(clear.sandbox_id)
            receipt = None if info is None else info.accepted_skill_material
            if receipt is None:
                return clear.snapshot_id is None
            if receipt.snapshot_id != clear.snapshot_id or receipt.run_id != clear.run_id or receipt.generation != clear.generation:
                return False
            self.destroy(clear.sandbox_id)
            return self.get(clear.sandbox_id) is None
        return clear_skill_snapshot_active_view(
            user_id=clear.user_id,
            thread_id=clear.thread_id,
            run_id=clear.run_id,
            generation=clear.generation,
        )

    def ensure_accepted_skill_snapshot_absent(self, clear: "SkillProjectionClear") -> bool:
        from deerflow.runtime.skill_projection import SkillProjectionClear
        from deerflow.runtime.skill_snapshot import prove_skill_snapshot_active_view_absent

        if not isinstance(clear, SkillProjectionClear):
            return False
        if not self.has_accepted_skill_isolation(clear.sandbox_id):
            return False
        if self._identity_for_sandbox(clear.sandbox_id) != (
            clear.user_id,
            clear.thread_id,
        ):
            return False
        if isinstance(getattr(self, "_backend", None), RemoteSandboxBackend):
            return self.get(clear.sandbox_id) is None
        return prove_skill_snapshot_active_view_absent(
            user_id=clear.user_id,
            thread_id=clear.thread_id,
        )

    def destroy(self, sandbox_id: str) -> None:
        """Destroy a sandbox: stop the container and free all resources.

        Unlike release(), this actually stops the container.  Use this for
        explicit cleanup, capacity-driven eviction, or shutdown.

        The host-side HTTP client owned by the cached ``AioSandbox`` instance is
        closed alongside backend/container destruction so no client/socket
        resources leak (#2872).

        Args:
            sandbox_id: The ID of the sandbox to destroy.
        """
        self._destroy_tracked(sandbox_id, still_reapable=lambda: True)

    def _assert_no_invocation_owned_skill_projections(self) -> None:
        """Refuse provider teardown while accepted material still has an owner."""
        from deerflow.runtime.skill_projection import get_skill_projection_coordinator

        with self._lock:
            identities = set(getattr(self, "_thread_sandboxes", {}))
            identities.update(
                identity
                for identity in getattr(
                    self,
                    "_active_sandbox_identity",
                    {},
                ).values()
                if identity is not None
            )
        coordinator = get_skill_projection_coordinator()
        if any(coordinator.is_busy(user_id=user_id, thread_id=thread_id) for user_id, thread_id in identities):
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_projection_in_use",
            )

    def _destroy_tracked(self, sandbox_id: str, *, still_reapable: Callable[[], bool]) -> None:
        """``destroy()`` with a caller-supplied "is this still reapable" gate.

        Callers that decided to destroy *earlier* (the idle checker) pass their
        own predicate so the decision is re-validated in the same critical
        section that reserves the teardown. ``destroy()`` itself passes a
        constant: an explicit destroy is a decision made now.
        """
        if not self._reserve_local_teardown(sandbox_id, still_reapable):
            logger.info("Skipping destroy of sandbox %s: re-acquired by this instance or already being torn down", sandbox_id)
            return

        try:
            self._destroy_reserved(sandbox_id)
        finally:
            self._finish_local_teardown(sandbox_id)

    def _destroy_reserved(self, sandbox_id: str) -> None:
        # Claim before untracking. The reverse order loses the container on a
        # refused claim: still running, and no longer in any of our maps, so
        # nothing here would ever reap or reclaim it.
        if not self._claim_ownership(sandbox_id, for_destroy=True):
            logger.warning("Refusing to destroy sandbox %s: owned by another instance", sandbox_id)
            return

        try:
            self._clear_bound_accepted_skill_snapshot(sandbox_id)
        except Exception:
            logger.error(
                "Could not clear accepted skill snapshot for sandbox %s during destroy",
                sandbox_id,
                exc_info=True,
            )
        sandbox, info, _ = self._remove_tracked_sandbox(sandbox_id)

        if sandbox is not None:
            # Defense-in-depth: close() already swallows its own errors; this
            # guard only protects against a future close() that misbehaves, so
            # host-side client cleanup can never block container destruction.
            try:
                sandbox.close()
            except Exception as e:
                logger.warning(f"Error closing sandbox {sandbox_id} during destroy: {e}")

        if info:
            # The marker must outlast the stop, not the TTL it was written with,
            # and the heartbeat releases it on exit — on both outcomes. On a
            # failed stop the container is probably still up, so a marker left
            # behind would refuse its own thread's `take()` until the TTL lapses;
            # the error still propagates out of the `with` (`shutdown()` logs per
            # sandbox off it), it is just no longer this method's job to release.
            with self._held_teardown_lease(sandbox_id):
                self._backend.destroy(info)
            logger.info(f"Destroyed sandbox {sandbox_id}")
        else:
            # No container to stop, so no teardown lease was held: clear the
            # marker the claim above wrote, so an untracked id cannot leave a
            # lease stuck in `del:`.
            self._release_ownership(sandbox_id)

    def reset(self) -> None:
        """Destroy tracked sandboxes only after accepted owners have drained."""
        self.shutdown()

    def shutdown(self) -> None:
        """Shutdown all sandboxes. Thread-safe and idempotent."""
        self._assert_no_invocation_owned_skill_projections()
        with self._lock:
            if self._shutdown_called:
                return
            self._shutdown_called = True
            sandbox_ids = list(self._sandboxes.keys())
            warm_items = list(self._warm_pool.items())
            self._warm_pool.clear()
            self._warm_pool_identity.clear()

        self._stop_idle_checker()
        # Stop renewing before destroying: the destroy paths claim ownership
        # themselves, and a renewal racing them only re-publishes leases we are
        # about to drop.
        self._stop_lease_renewal()

        logger.info(f"Shutting down {len(sandbox_ids)} active + {len(warm_items)} warm-pool sandbox(es)")

        for sandbox_id in sandbox_ids:
            try:
                self.destroy(sandbox_id)
            except Exception as e:
                logger.error(f"Failed to destroy sandbox {sandbox_id} during shutdown: {e}")

        for sandbox_id, (info, _) in warm_items:
            # Route through _destroy_warm_entry so the ownership claim and the
            # container stop stay together, as on the idle path. Unconditional
            # here: the entries were removed from `_warm_pool` under the lock
            # above, so the pool-membership predicate the other callers use would
            # refuse every one of them.
            self._destroy_warm_entry(sandbox_id, info, reason="shutdown", still_reapable=lambda: True)

        try:
            self._ownership.close()
        except Exception as e:
            logger.warning(f"Error closing sandbox ownership store during shutdown: {e}")

        self._acquire_serializer.close()

import asyncio
import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from deerflow.config import get_app_config
from deerflow.reflection import resolve_class
from deerflow.sandbox.sandbox import Sandbox

if TYPE_CHECKING:
    from deerflow.skills.projection import SkillProjectionPaths


class SandboxProvider(ABC):
    """Abstract base class for sandbox providers"""

    uses_thread_data_mounts: bool = False
    needs_upload_permission_adjustment: bool = True
    # Capability for enforcing a lead Agent's physical skill view across the
    # provider's current Agent-accessible tool surface. Host-backed providers
    # must return False whenever shell access can bypass managed path mappings.
    supports_agent_skill_isolation: bool = False

    @abstractmethod
    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        """Acquire a sandbox environment and return its ID.

        Returns:
            The ID of the acquired sandbox environment.
        """
        pass

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        """Acquire a sandbox without blocking the event loop.

        Most sandbox providers expose a synchronous lifecycle API because local
        Docker/provisioner operations are blocking. Async runtimes should call
        this method so those blocking operations run in a worker thread instead
        of stalling the event loop.
        """
        return await asyncio.to_thread(self.acquire, thread_id, user_id=user_id)

    def sync_agent_skills(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        projection: "SkillProjectionPaths",
    ) -> None:
        """Synchronize a prepared thread skill projection into a sandbox.

        Bind-mount providers observe the stable projection roots directly and
        use this no-op implementation. Upload-based providers override it.
        """

    async def sync_agent_skills_async(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        projection: "SkillProjectionPaths",
    ) -> None:
        """Async wrapper for upload-based skill synchronization."""
        await asyncio.to_thread(
            self.sync_agent_skills,
            sandbox_id,
            thread_id=thread_id,
            user_id=user_id,
            projection=projection,
        )

    @abstractmethod
    def get(self, sandbox_id: str) -> Sandbox | None:
        """Get a sandbox environment by ID.

        Args:
            sandbox_id: The ID of the sandbox environment to retain.
        """
        pass

    @abstractmethod
    def release(self, sandbox_id: str) -> None:
        """Release a sandbox environment.

        Args:
            sandbox_id: The ID of the sandbox environment to destroy.
        """
        pass

    def reset(self) -> None:
        """Clear cached state that survives provider instance replacement.

        Provider overrides can release resources and make the instance unusable.
        """
        pass

    def capability[CapabilityT](self, protocol: type[CapabilityT]) -> CapabilityT | None:
        """Negotiate an optional capability; ``None`` when this provider lacks it.

        The required provider surface is ``acquire``, its async twin, ``get``
        and ``release``. Everything else HartMesh needs from a provider is a
        contract in :mod:`deerflow.sandbox.capabilities`, offered here. A
        provider offers a contract by inheriting it, in which case this
        default answers the provider itself, or by overriding this method to
        answer a companion object that inherits it. Callers negotiate through
        ``sandbox_capability`` and fail closed on ``None``.
        """
        return self if isinstance(self, protocol) else None

    def sandbox_network_mode(self) -> str:
        """Return the provider's effective outbound network mode."""
        return "open"

    def sandbox_network_temporary_grant_ttl(self) -> int:
        return 300

    def consume_network_policy_events(self, sandbox_id: str) -> list[dict[str, object]]:
        """Claim the oldest unsurfaced trusted-proxy event for a sandbox.

        Providers without a managed network policy use the empty default.
        """
        del sandbox_id
        return []

    async def consume_network_policy_events_async(self, sandbox_id: str) -> list[dict[str, object]]:
        return await asyncio.to_thread(self.consume_network_policy_events, sandbox_id)

    def deny_pending_network_policy_events(self, sandbox_id: str) -> bool:
        """Atomically deny all unsurfaced trusted-proxy events for a sandbox."""
        del sandbox_id
        return False

    async def deny_pending_network_policy_events_async(self, sandbox_id: str) -> bool:
        return await asyncio.to_thread(self.deny_pending_network_policy_events, sandbox_id)

    def decide_network_policy_request(self, sandbox_id: str, request_id: str, decision: str) -> bool:
        """Apply a user decision to one trusted-proxy event."""
        del sandbox_id, request_id, decision
        return False

    async def decide_network_policy_request_async(self, sandbox_id: str, request_id: str, decision: str) -> bool:
        return await asyncio.to_thread(self.decide_network_policy_request, sandbox_id, request_id, decision)


_default_sandbox_provider: SandboxProvider | None = None
# Guards every read and write of `_default_sandbox_provider`. The singleton is
# reachable from more than one OS thread (e.g. the main event loop and the Feishu
# channel thread, which runs its own loop), so a bare check-then-create can double
# initialize the provider, and an unsynchronized reset/shutdown racing a get can
# hand a caller `None` or a torn instance. Every access to the global below takes
# this lock, including the read+return in `get_sandbox_provider()`.
#
# Provider callbacks (`__init__`, `reset()`, `shutdown()`) and the dynamic import
# in `resolve_class()` run *outside* the lock: they are plugin-supplied and may be
# slow or re-enter lifecycle functions. During reset/shutdown, a condition-backed
# transition prevents another thread from installing a replacement until the
# callback succeeds or refuses teardown. Waiters release the non-reentrant lock;
# callback-thread re-entry observes the transitioning provider without deadlock.
_provider_lock = threading.Lock()
_provider_condition = threading.Condition(_provider_lock)
_provider_teardown: SandboxProvider | None = None
_provider_teardown_owner: int | None = None


def _wait_for_provider_teardown_locked() -> SandboxProvider | None:
    """Wait for another thread's teardown; return self-owned transition."""
    current_thread = threading.get_ident()
    while _provider_teardown is not None:
        if _provider_teardown_owner == current_thread:
            return _provider_teardown
        _provider_condition.wait()
    return None


def _as_session_provider(provider: SandboxProvider) -> SandboxProvider:
    """Install every provider behind the session provider, exactly once.

    The session provider is the single resolution point for sandbox handles:
    it dispatches acquire, get and release by the executing session's
    declaration and forwards everything else to the configured provider. One
    wrapper per backing provider keeps ``id(provider)``-keyed registries stable.
    """
    from deerflow.sandbox.session import sandbox_session_provider

    return sandbox_session_provider(provider)


def lifecycle_sandbox_provider(provider: SandboxProvider) -> SandboxProvider:
    """The provider object that owns process-local lifecycle state for ``provider``.

    Lifecycle registries such as the sandbox lease manager are keyed by provider
    identity. Runtime code holds the installed session provider, while tests and
    embedders often hold the backing provider they passed to
    ``set_sandbox_provider``; both must land on the same registry entry, and
    that entry must call through the session provider so declared sessions and
    mount-scope refusals apply to lease-managed acquires as well.

    A partial duck-typed double that cannot stand behind the session provider
    (it lacks acquire, get, or release) keeps its own identity: it can never be
    installed, so nothing else can hold a different registry entry for it.
    """
    try:
        return _as_session_provider(provider)
    except TypeError:
        return provider


def get_initialized_sandbox_provider() -> SandboxProvider | None:
    """Return the provider only when another lifecycle path initialized it."""
    with _provider_lock:
        return _default_sandbox_provider


def get_sandbox_provider(**kwargs) -> SandboxProvider:
    """Get the sandbox provider singleton.

    Returns a cached singleton instance. Use `reset_sandbox_provider()` to clear
    the cache, or `shutdown_sandbox_provider()` to properly shutdown and clear.

    Returns:
        A sandbox provider instance.
    """
    global _default_sandbox_provider
    # Fast path: a single locked read so a concurrent reset/shutdown can't null
    # the global between the check and the return.
    with _provider_condition:
        reentrant_provider = _wait_for_provider_teardown_locked()
        if reentrant_provider is not None:
            return reentrant_provider
        if _default_sandbox_provider is not None:
            return _default_sandbox_provider

    # Cold start. Resolve + construct outside the lock: the import and the
    # provider constructor are plugin code and must not run under a non-reentrant
    # lock. The construction may race another caller; we reconcile under the lock.
    config = get_app_config()
    cls = resolve_class(config.sandbox.use, SandboxProvider)
    provider = _as_session_provider(cls(**kwargs))

    with _provider_condition:
        reentrant_provider = _wait_for_provider_teardown_locked()
        if reentrant_provider is not None:
            winner = reentrant_provider
        elif _default_sandbox_provider is None:
            _default_sandbox_provider = provider
            return provider
        else:
            # We lost the install race: another thread got there first.
            # ``winner`` is read under the same lock, so it is live.
            winner = _default_sandbox_provider

    # Discard the instance we just built (outside the lock). For providers with
    # side-effectful constructors (e.g. AioSandboxProvider starts an idle-checker
    # thread), this tears down the orphan so it does not leak — issue #3721.
    if hasattr(provider, "shutdown"):
        provider.shutdown()
    return winner


def reset_sandbox_provider() -> None:
    """Reset the sandbox provider singleton.

    This clears the cached instance without calling shutdown directly.
    The next call to `get_sandbox_provider()` will create a new instance.
    Useful for testing or when switching configurations.

    Providers can override `reset()` to clear any module-level state they keep
    alive across instances (for example, `LocalSandboxProvider`'s cached
    `LocalSandbox` singleton). Without it, config/mount changes would not take
    effect on the next acquire().

    A provider override can release active sandboxes during reset.
    Otherwise, active sandboxes become orphaned.
    Concurrent callers wait until reset either completes or restores a provider
    that refused teardown.
    Use `shutdown_sandbox_provider()` for proper cleanup.
    """
    global _default_sandbox_provider, _provider_teardown, _provider_teardown_owner
    # Detach the reference under the lock, then run the provider's `reset()`
    # callback outside it (see the `_provider_lock` note).
    with _provider_condition:
        if _wait_for_provider_teardown_locked() is not None:
            return
        provider = _default_sandbox_provider
        _default_sandbox_provider = None
        if provider is not None:
            _provider_teardown = provider
            _provider_teardown_owner = threading.get_ident()
    if provider is not None:
        from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingError
        from deerflow.sandbox.lease import discard_sandbox_lease_manager

        try:
            provider.reset()
        except AcceptedSkillSandboxBindingError as exc:
            with _provider_condition:
                if exc.code == "accepted_skill_snapshot_projection_in_use":
                    # The provider stays installed, and so does its lease
                    # manager: the refused teardown keeps every binding live.
                    if _default_sandbox_provider is None:
                        _default_sandbox_provider = provider
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
            raise
        except BaseException:
            with _provider_condition:
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
            discard_sandbox_lease_manager(provider)
            raise
        else:
            with _provider_condition:
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
            discard_sandbox_lease_manager(provider)


def shutdown_sandbox_provider() -> None:
    """Shutdown and reset the sandbox provider.

    This properly shuts down the provider (releasing all sandboxes)
    before clearing the singleton. Call this when the application
    is shutting down or when you need to completely reset the sandbox system.
    """
    global _default_sandbox_provider, _provider_teardown, _provider_teardown_owner
    # Detach the reference under the lock, then run the (potentially slow)
    # `shutdown()` callback outside it (see the `_provider_lock` note).
    with _provider_condition:
        if _wait_for_provider_teardown_locked() is not None:
            return
        provider = _default_sandbox_provider
        _default_sandbox_provider = None
        if provider is not None:
            _provider_teardown = provider
            _provider_teardown_owner = threading.get_ident()
    if provider is not None and hasattr(provider, "shutdown"):
        from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingError
        from deerflow.sandbox.lease import discard_sandbox_lease_manager

        try:
            provider.shutdown()
        except AcceptedSkillSandboxBindingError as exc:
            with _provider_condition:
                if exc.code == "accepted_skill_snapshot_projection_in_use":
                    if _default_sandbox_provider is None:
                        _default_sandbox_provider = provider
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
            raise
        except BaseException:
            with _provider_condition:
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
            discard_sandbox_lease_manager(provider)
            raise
        else:
            with _provider_condition:
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
            discard_sandbox_lease_manager(provider)
    elif provider is not None:
        from deerflow.sandbox.lease import discard_sandbox_lease_manager

        with _provider_condition:
            _provider_teardown = None
            _provider_teardown_owner = None
            _provider_condition.notify_all()
        discard_sandbox_lease_manager(provider)


def set_sandbox_provider(provider: SandboxProvider) -> None:
    """Set a custom sandbox provider instance.

    This allows injecting a custom or mock provider for testing purposes.

    Note: any previously installed provider is replaced but not shut down; the
    caller owns the lifecycle of the instance it is overwriting.

    Args:
        provider: The SandboxProvider instance to use.
    """
    global _default_sandbox_provider
    with _provider_condition:
        if _wait_for_provider_teardown_locked() is not None:
            raise RuntimeError("sandbox provider cannot be replaced during its teardown callback")
        previous = _default_sandbox_provider
        installed = _as_session_provider(provider)
        _default_sandbox_provider = installed
    if previous is not None and previous is not installed:
        from deerflow.sandbox.lease import discard_sandbox_lease_manager

        discard_sandbox_lease_manager(previous)

import asyncio
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from deerflow.config import get_app_config
from deerflow.reflection import resolve_class
from deerflow.sandbox.accepted_material import (
    AcceptedMaterialCapability,
    AcceptedSkillExecutionEvidence,
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.accepted_material import (
    AcceptedSkillExecutionEvidenceV1 as AcceptedSkillExecutionEvidenceV1,
)
from deerflow.sandbox.accepted_material import (
    AcceptedSkillExecutionEvidenceV2 as AcceptedSkillExecutionEvidenceV2,
)
from deerflow.sandbox.sandbox import Sandbox

if TYPE_CHECKING:
    from deerflow.runtime.skill_projection import SkillProjectionClear
    from deerflow.skills.projection import SkillProjectionPaths


AcceptedSkillMaterialCapability = AcceptedMaterialCapability
"""Compatibility name for the provider-neutral accepted-material capability."""


def reject_writable_accepted_skill_aliases(
    accepted_root: str | Path,
    mounts: list[tuple[str, bool]],
) -> None:
    """Reject writable host mounts that overlap accepted material.

    Container-path isolation alone is insufficient: the same host directory
    can be mounted again at an unrelated virtual path. Either ancestor or
    descendant overlap gives that alias write access to accepted bytes.
    """
    try:
        accepted = Path(accepted_root).resolve()
        for host_path, read_only in mounts:
            if read_only:
                continue
            mounted = Path(host_path).resolve()
            if mounted == accepted or mounted in accepted.parents or accepted in mounted.parents:
                raise AcceptedSkillSandboxBindingError(
                    "accepted_skill_snapshot_writable_alias",
                )
    except AcceptedSkillSandboxBindingError:
        raise
    except OSError as exc:
        raise AcceptedSkillSandboxBindingError(
            "accepted_skill_snapshot_writable_alias",
        ) from exc


def accepted_skill_binding_from_runtime(runtime: object) -> AcceptedSkillSandboxBindingV1 | None:
    """Return the coordinator-issued binding, never caller dictionaries."""
    context = getattr(runtime, "context", None)
    if not isinstance(context, dict):
        return None
    from deerflow.runtime.skill_projection import SKILL_PROJECTION_TOKEN_CONTEXT_KEY

    token = context.get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
    if token is None:
        return None
    return AcceptedSkillSandboxBindingV1.from_consumer_token(token)


def accepted_skill_material_binding_from_runtime(
    runtime: object,
    *,
    user_id: str,
) -> AcceptedSkillSandboxBindingV1 | None:
    """Return the committed pre-acquisition material request for this run."""

    context = getattr(runtime, "context", None)
    if not isinstance(context, dict):
        return None
    snapshot_id = accepted_skill_snapshot_id_from_runtime(runtime)
    if snapshot_id is _NO_BINDING:
        return None
    thread_id = context.get("thread_id")
    run_id = context.get("run_id")
    if not isinstance(thread_id, str) or not isinstance(run_id, str):
        raise AcceptedSkillSandboxBindingError(
            "accepted_skill_snapshot_runtime_identity_missing",
        )
    from deerflow.runtime.skill_projection import (
        SkillProjectionBusyError,
        get_skill_projection_coordinator,
    )

    try:
        generation, committed_snapshot_id, evidence = get_skill_projection_coordinator().binding_for_committed_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id=run_id,
        )
    except SkillProjectionBusyError as exc:
        from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
        from deerflow.runtime.agent_revision import (
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
        )
        from deerflow.runtime.skill_projection import SkillProjectionEvidence

        material = context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
        if not isinstance(material, ResolvedAgentMaterialV1):
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_binding_conflict",
            ) from exc
        fallback_evidence = SkillProjectionEvidence.from_snapshot(
            material.skill_snapshot,
        )
        try:
            coordinator = get_skill_projection_coordinator()
            coordinator.claim_committed_run(
                user_id=user_id,
                thread_id=thread_id,
                run_id=run_id,
                snapshot_id=snapshot_id,
                evidence=fallback_evidence,
            )
            generation, committed_snapshot_id, evidence = coordinator.binding_for_committed_run(
                user_id=user_id,
                thread_id=thread_id,
                run_id=run_id,
            )
        except Exception as fallback_exc:
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_binding_conflict",
            ) from fallback_exc
    if committed_snapshot_id != snapshot_id:
        raise AcceptedSkillSandboxBindingError(
            "accepted_skill_snapshot_binding_conflict",
        )
    return AcceptedSkillSandboxBindingV1(
        snapshot_id=committed_snapshot_id,
        run_id=run_id,
        generation=generation,
        evidence=evidence,
    )


def accepted_skill_snapshot_id_from_runtime(runtime: object) -> str | None | object:
    """Return accepted snapshot ID, explicit ``None``, or ``_NO_BINDING``."""
    context = getattr(runtime, "context", None)
    if not isinstance(context, dict):
        return _NO_BINDING
    from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
    from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY

    material = context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
    if not isinstance(material, ResolvedAgentMaterialV1):
        return _NO_BINDING
    snapshot = material.skill_snapshot
    return None if snapshot is None else snapshot.snapshot_id


def accepted_skill_access_from_runtime(runtime: object) -> tuple[bool, str | None]:
    """Return whether durable accepted material is present and its snapshot ID.

    The boolean distinguishes an accepted empty skill set from legacy execution,
    for which no immutable skill-access contract exists.
    """
    snapshot_id = accepted_skill_snapshot_id_from_runtime(runtime)
    if snapshot_id is _NO_BINDING:
        return False, None
    return True, snapshot_id


_NO_BINDING = object()


def require_runtime_accepted_skill_isolation(
    provider: "SandboxProvider",
    runtime: object,
    *,
    sandbox_id: str,
) -> None:
    """Fail before binding unless accepted acquisition proves live-path isolation."""
    accepted, _snapshot_id = accepted_skill_access_from_runtime(runtime)
    if accepted and not provider.has_accepted_skill_isolation(sandbox_id):
        raise AcceptedSkillSandboxBindingError(
            "accepted_skill_snapshot_isolation_unverified",
        )
    if accepted and _snapshot_id is not None:
        capability = provider.accepted_skill_material_capability(sandbox_id)
        if capability is not AcceptedSkillMaterialCapability.IMMUTABLE_READ_ONLY:
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_immutability_unsupported",
            )


def ensure_accepted_skill_binding(
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
) -> tuple[AcceptedSkillSandboxBindingV1 | None, object | None, bool]:
    """Activate the lead/child projection token before provider binding."""
    context = getattr(runtime, "context", None)
    if not isinstance(context, dict):
        return None, None, False
    snapshot_id = accepted_skill_snapshot_id_from_runtime(runtime)
    if snapshot_id is _NO_BINDING:
        return None, None, False
    thread_id = context.get("thread_id")
    run_id = context.get("run_id")
    if not isinstance(thread_id, str) or not isinstance(run_id, str):
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_runtime_identity_missing")
    from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
    from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
    from deerflow.runtime.skill_projection import (
        SKILL_PROJECTION_TOKEN_CONTEXT_KEY,
        SkillProjectionConsumerToken,
        SkillProjectionEvidence,
        get_skill_projection_coordinator,
    )

    material = context.get(RESOLVED_AGENT_MATERIAL_CONTEXT_KEY)
    if not isinstance(material, ResolvedAgentMaterialV1):
        return None, None, False

    coordinator = get_skill_projection_coordinator()
    existing = context.get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
    if isinstance(existing, SkillProjectionConsumerToken):
        if existing.user_id != user_id or existing.thread_id != thread_id or existing.run_id != run_id or existing.sandbox_id != sandbox_id or existing.snapshot_id != snapshot_id:
            context.pop(SKILL_PROJECTION_TOKEN_CONTEXT_KEY, None)
            raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_binding_conflict")
        if coordinator.owns(existing):
            return AcceptedSkillSandboxBindingV1.from_consumer_token(existing), existing, False
        context.pop(SKILL_PROJECTION_TOKEN_CONTEXT_KEY, None)

    evidence = SkillProjectionEvidence.from_snapshot(material.skill_snapshot)
    try:
        coordinator.claim_committed_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id=run_id,
            snapshot_id=snapshot_id,
            evidence=evidence,
        )
        token = coordinator.activate(
            user_id=user_id,
            thread_id=thread_id,
            sandbox_id=sandbox_id,
            run_id=run_id,
            snapshot_id=snapshot_id,
            consumer_id=f"run:{run_id}:lead",
        )
    except Exception as exc:
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_binding_conflict") from exc
    context[SKILL_PROJECTION_TOKEN_CONTEXT_KEY] = token
    return AcceptedSkillSandboxBindingV1.from_consumer_token(token), token, True


def invalidate_runtime_skill_projection_token(runtime: object, token: object) -> bool:
    """Remove only the failed binding token retained by this runtime context."""
    context = getattr(runtime, "context", None)
    if not isinstance(context, dict):
        return False
    from deerflow.runtime.skill_projection import SKILL_PROJECTION_TOKEN_CONTEXT_KEY

    if context.get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY) != token:
        return False
    context.pop(SKILL_PROJECTION_TOKEN_CONTEXT_KEY, None)
    return True


def release_accepted_skill_consumer(token: object) -> bool:
    """Release one consumer, retaining ownership through provider cleanup."""
    from deerflow.runtime.skill_projection import (
        SkillProjectionConsumerToken,
        get_skill_projection_coordinator,
    )

    if not isinstance(token, SkillProjectionConsumerToken):
        return False
    coordinator = get_skill_projection_coordinator()
    clear = coordinator.release(token)
    if clear is None:
        return False
    provider = get_sandbox_provider()
    cleared = provider.clear_accepted_skill_snapshot(clear)
    if not cleared:
        cleared = provider.ensure_accepted_skill_snapshot_absent(clear)
    if not cleared:
        return False
    try:
        provider.release(clear.sandbox_id)
    finally:
        # A successful compare-and-clear is the material-isolation boundary.
        # Resource parking/teardown may fail, but it cannot make the removed
        # accepted bytes reachable again, so stale ownership must not strand
        # the thread indefinitely.
        finalized = coordinator.finalize_release(clear)
    return finalized


def bind_runtime_accepted_skill_projection(
    provider: "SandboxProvider",
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
) -> bool:
    """Idempotently bind accepted material for middleware and cached tools."""
    require_runtime_accepted_skill_isolation(
        provider,
        runtime,
        sandbox_id=sandbox_id,
    )
    binding, token, _created = ensure_accepted_skill_binding(
        runtime,
        sandbox_id=sandbox_id,
        user_id=user_id,
    )
    if binding is None:
        return False
    context = getattr(runtime, "context", None)
    thread_id = context.get("thread_id") if isinstance(context, dict) else None
    if not isinstance(thread_id, str):
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_runtime_identity_missing")
    try:
        provider.bind_accepted_skill_snapshot(
            sandbox_id,
            thread_id=thread_id,
            user_id=user_id,
            binding=binding,
        )
    except Exception:
        invalidate_runtime_skill_projection_token(runtime, token)
        if token is not None:
            release_accepted_skill_consumer(token)
        raise
    return True


async def bind_runtime_accepted_skill_projection_async(
    provider: "SandboxProvider",
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
) -> bool:
    """Async counterpart that keeps provider I/O off the event loop."""
    require_runtime_accepted_skill_isolation(
        provider,
        runtime,
        sandbox_id=sandbox_id,
    )
    binding, token, _created = ensure_accepted_skill_binding(
        runtime,
        sandbox_id=sandbox_id,
        user_id=user_id,
    )
    if binding is None:
        return False
    context = getattr(runtime, "context", None)
    thread_id = context.get("thread_id") if isinstance(context, dict) else None
    if not isinstance(thread_id, str):
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_runtime_identity_missing")
    try:
        await provider.bind_accepted_skill_snapshot_async(
            sandbox_id,
            thread_id=thread_id,
            user_id=user_id,
            binding=binding,
        )
    except Exception:
        invalidate_runtime_skill_projection_token(runtime, token)
        if token is not None:
            await asyncio.to_thread(release_accepted_skill_consumer, token)
        raise
    return True


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

    def acquire_accepted_skills(self, thread_id: str, *, user_id: str) -> str:
        """Acquire a sandbox created without mutable live skill projections."""
        del thread_id, user_id
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_projection_unsupported")

    async def acquire_accepted_skills_async(self, thread_id: str, *, user_id: str) -> str:
        return await asyncio.to_thread(
            self.acquire_accepted_skills,
            thread_id,
            user_id=user_id,
        )

    def accepted_skill_execution_evidence(
        self,
        sandbox_id: str,
    ) -> AcceptedSkillExecutionEvidence | None:
        """Return bounded execution evidence when the backend has a native attempt."""

        del sandbox_id
        return None

    async def validate_accepted_skill_execution_async(
        self,
        sandbox_id: str,
        evidence: AcceptedSkillExecutionEvidence,
    ) -> bool:
        """Revalidate the exact materialized attempt before executable work."""

        del sandbox_id, evidence
        return False

    async def renew_accepted_skill_execution_async(
        self,
        sandbox_id: str,
        evidence: AcceptedSkillExecutionEvidence,
    ) -> bool:
        """Renew the exact attempt after authoritative RunRow renewal."""

        del sandbox_id, evidence
        return False

    def acquire_bound_accepted_skills(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> str:
        """Acquire and materialize accepted bytes before returning a sandbox."""

        del binding
        return self.acquire_accepted_skills(thread_id, user_id=user_id)

    async def acquire_bound_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> str:
        # Preserve providers that already implement the accepted-only async seam.
        # The default contract does not consume the binding during acquisition;
        # it is still validated by bind_accepted_skill_snapshot_async below.
        del binding
        return await self.acquire_accepted_skills_async(
            thread_id,
            user_id=user_id,
        )

    def has_accepted_skill_isolation(self, sandbox_id: str) -> bool:
        """Return whether ``sandbox_id`` was created without live skill paths."""
        del sandbox_id
        return False

    def accepted_skill_material_capability(
        self,
        sandbox_id: str,
    ) -> AcceptedSkillMaterialCapability:
        """Return the immutable access profile available to accepted material.

        The conservative default permits only the explicit empty accepted set.
        Providers that advertise immutable read-only material must prove projected
        files immutable to commands run inside that sandbox.
        """
        del sandbox_id
        return AcceptedSkillMaterialCapability.EMPTY_ONLY

    def bind_accepted_skill_snapshot(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> None:
        """Expose exactly one accepted snapshot, or fail before execution.

        Custom providers retain their legacy behavior until an accepted
        invocation supplies this binding.  They must implement the same exact
        projection contract before durable skill material can execute.
        """
        del sandbox_id, thread_id, user_id, binding
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_projection_unsupported")

    async def bind_accepted_skill_snapshot_async(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> None:
        await asyncio.to_thread(
            self.bind_accepted_skill_snapshot,
            sandbox_id,
            thread_id=thread_id,
            user_id=user_id,
            binding=binding,
        )

    def clear_accepted_skill_snapshot(
        self,
        clear: "SkillProjectionClear",
    ) -> bool:
        """Compare-and-clear only an exact last-consumer ownership proof."""
        del clear
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_projection_unsupported")

    def ensure_accepted_skill_snapshot_absent(self, clear: "SkillProjectionClear") -> bool:
        """Prove an exact failed/unpublished projection cannot be reached.

        This is intentionally separate from compare-and-clear: providers may
        use it only when no exact binding receipt exists. Custom providers fail
        closed until they can prove an empty namespace or quarantine/destroy
        the exact accepted-only sandbox.
        """
        del clear
        return False

    async def clear_accepted_skill_snapshot_async(
        self,
        clear: "SkillProjectionClear",
    ) -> bool:
        """Run exact accepted-snapshot cleanup without blocking the event loop."""
        return await asyncio.to_thread(
            self.clear_accepted_skill_snapshot,
            clear,
        )

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
    provider = cls(**kwargs)

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
        try:
            provider.reset()
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
            raise
        else:
            with _provider_condition:
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()


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
            raise
        else:
            with _provider_condition:
                _provider_teardown = None
                _provider_teardown_owner = None
                _provider_condition.notify_all()
    elif provider is not None:
        with _provider_condition:
            _provider_teardown = None
            _provider_teardown_owner = None
            _provider_condition.notify_all()


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
        _default_sandbox_provider = provider

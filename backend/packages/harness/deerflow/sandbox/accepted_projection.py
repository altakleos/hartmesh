"""The accepted-skills projection: a thread workspace plus one bound accepted snapshot.

This is the Material of the accepted-skills projection, the session a durable
invocation runs in when its material is a committed skill snapshot rather
than the thread's live skill roots. It has two halves:

* the provider half, :class:`~deerflow.sandbox.capabilities.AcceptedSkillProjection`,
  negotiated from the installed provider (:func:`require_accepted_skill_projection`).
  It provisions the isolated sandbox, binds exactly one snapshot, and proves
  isolation and immutability before execution; and
* the consumer half, the consumer-token coordinator in
  :mod:`deerflow.runtime.skill_projection`. Its membership (every lead and
  child consumer of one projection) differs from the execution lease's, so it
  stays a second refcount inside this Material: the last consumer's exact
  compare-and-clear is what parks the sandbox, and the execution lease only
  ever borrows it.

Provisioning precedes binding. :func:`provision_runtime_accepted_skill_projection`
provisions through the capability, activates the run's consumer token and
binds the coordinator-issued snapshot, unwinding the token and the sandbox if
any step fails; :func:`bind_runtime_accepted_skill_projection` does the same
for a sandbox the run already holds. Both resolve the projection from the
run's committed material in the runtime context, never from caller-supplied
dictionaries.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable

from deerflow.sandbox.accepted_material import (
    AcceptedMaterialCapability,
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.capabilities import AcceptedSkillProjection, sandbox_capability

logger = logging.getLogger(__name__)

_NO_BINDING = object()


def accepted_skill_projection(provider: object) -> AcceptedSkillProjection | None:
    """The provider's accepted-skill projection capability, if it offers one."""
    return sandbox_capability(provider, AcceptedSkillProjection)


def require_accepted_skill_projection(provider: object) -> AcceptedSkillProjection:
    """The projection capability, or the typed refusal accepted material needs."""
    projection = accepted_skill_projection(provider)
    if projection is None:
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_projection_unsupported")
    return projection


def has_accepted_skill_isolation(provider: object, sandbox_id: str) -> bool:
    """Whether ``sandbox_id`` is an accepted-only sandbox of ``provider``.

    A provider without the capability has no accepted-only sandboxes.
    """
    projection = accepted_skill_projection(provider)
    return projection is not None and projection.has_accepted_skill_isolation(sandbox_id)


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


def require_runtime_accepted_skill_isolation(
    provider: object,
    runtime: object,
    *,
    sandbox_id: str,
) -> None:
    """Fail before binding unless accepted acquisition proves live-path isolation."""
    accepted, snapshot_id = accepted_skill_access_from_runtime(runtime)
    if not accepted:
        return
    projection = accepted_skill_projection(provider)
    if projection is None or not projection.has_accepted_skill_isolation(sandbox_id):
        raise AcceptedSkillSandboxBindingError(
            "accepted_skill_snapshot_isolation_unverified",
        )
    if snapshot_id is not None:
        capability = projection.accepted_skill_material_capability(sandbox_id)
        if capability is not AcceptedMaterialCapability.IMMUTABLE_READ_ONLY:
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_immutability_unsupported",
            )


def _projection_consumer_id(context: dict, run_id: str) -> str:
    """The execution a projection consumer belongs to: the lead, or one delegated task.

    A delegated task that reaches the sandbox before its lead holds no parent
    token to retain, so it activates its own, under the same identity as its
    sandbox lease. Sharing the lead's would let the first task to finish clear
    the projection under its siblings and the lead.
    """
    if context.get("is_subagent") is not True:
        return f"run:{run_id}:lead"
    from deerflow.sandbox.lease import sandbox_lease_owner

    owner_id = sandbox_lease_owner(context)
    if owner_id is None:
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_runtime_identity_missing")
    return owner_id


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
    consumer_id = _projection_consumer_id(context, run_id)
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
            consumer_id=consumer_id,
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
    released = _drive_clear(clear)
    if not released:
        # Said once, here: only the worker's interrupted-predecessor wait
        # inspects this bool, every other caller discards it, so the refusal
        # is otherwise invisible until a tenant reports a chat that stopped
        # answering.
        _warn_release_unfinished(token)
    return released


def complete_pending_projection_clear(*, user_id: str, thread_id: str) -> bool:
    """Finish the clear a thread is fenced under, for a caller holding no token.

    ``release_accepted_skill_consumer`` retries a refused release only for a
    caller that still has the exact consumer token, and nothing holds that
    token once the owning worker has finished. A clear whose provider work
    never confirmed therefore holds the thread forever: admission,
    replacement fencing and the worker's claim all refuse a clearing state,
    so every later turn on that chat is rejected until the process restarts.

    Callers about to fail for exactly that reason ask here first. The thread's
    own ``clearing`` proof is the authority for the retry, so this can never
    disturb a thread a consumer still owns, and a provider that still cannot
    confirm absence leaves the thread fenced -- an unproven clear must not free
    it. Provider failures are logged rather than raised: every caller is
    already on its failure path. The answer is read back from the coordinator
    rather than inferred from the failure, because parking the sandbox is the
    one step that runs after the thread is already free -- it can raise over a
    fence that is genuinely gone.
    """
    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

    coordinator = get_skill_projection_coordinator()
    clear = coordinator.pending_clear(user_id=user_id, thread_id=thread_id)
    if clear is None:
        return False
    try:
        return _drive_clear(clear)
    except Exception:
        fenced = coordinator.pending_clear(user_id=user_id, thread_id=thread_id) is not None
        logger.warning(
            "The accepted-skill clear on thread %s failed (thread still fenced: %s)",
            thread_id,
            fenced,
            exc_info=True,
        )
        return not fenced


_drive_locks_guard = threading.Lock()
_drive_locks: dict[tuple[str, str], threading.Lock] = {}


def _drive_lock_for(key: tuple[str, str]) -> threading.Lock:
    with _drive_locks_guard:
        return _drive_locks.setdefault(key, threading.Lock())


def _forget_drive_lock(key: tuple[str, str], lock: threading.Lock) -> None:
    """Drop a per-thread lock nobody is waiting on, so the table stays bounded.

    A driver that takes the entry between the check and the pop is harmless:
    by then the clear is finalized, so whichever lock object it waits on, it
    finds the thread no longer clearing and touches nothing.
    """
    with _drive_locks_guard:
        if _drive_locks.get(key) is lock and not lock.locked():
            _drive_locks.pop(key, None)


def _drive_clear(clear: object) -> bool:
    """Empty the view ``clear`` fences, then finalize the thread's ownership.

    Serialized per thread. Two drivers of one clear were already possible --
    the agent loop's release and the worker's terminal cleanup both hold the
    token -- and the retry adds a third on a different clock. Only the last
    step, parking the sandbox, is unfenced: ``finalize_release`` frees the
    thread, a new turn may reclaim the same warm sandbox at once, and a second
    driver arriving there would park a container that run is executing in. So
    the whole tail runs under one lock, and a driver that finds the clear
    already gone reports the completion it was asking for without touching the
    provider.
    """
    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

    coordinator = get_skill_projection_coordinator()
    key = (clear.user_id, clear.thread_id)
    lock = _drive_lock_for(key)
    try:
        return _drive_clear_locked(clear, coordinator, lock)
    finally:
        _forget_drive_lock(key, lock)


def _drive_clear_locked(clear: object, coordinator: object, lock: threading.Lock) -> bool:
    # Resolved at call time so a replaced or test-installed provider is the
    # one that clears; the coordinator's ownership outlives any one instance.
    from deerflow.sandbox.sandbox_provider import get_sandbox_provider

    with lock:
        if not coordinator.is_clearing(clear):
            # Another driver of this exact clear finished it. Nothing here is
            # this thread's to empty or park any more.
            return True
        provider = get_sandbox_provider()
        projection = require_accepted_skill_projection(provider)
        cleared = projection.clear_accepted_skill_snapshot(clear)
        if not cleared:
            cleared = projection.ensure_accepted_skill_snapshot_absent(clear)
        if not cleared:
            return False
        try:
            provider.release(clear.sandbox_id)
        finally:
            # A successful compare-and-clear is the material-isolation
            # boundary: it releases the exact binding, and the retained bytes
            # behind it can only be used again by a bind that re-verifies
            # them. Where there was no exact record to compare, the provider
            # emptied the view under this same fence, the stronger form of the
            # same boundary. Resource parking/teardown may fail after that,
            # but it cannot make anything reachable that the next bind would
            # not have to prove, so stale ownership must not strand the thread
            # indefinitely.
            finalized = coordinator.finalize_release(clear)
        return finalized


def _runtime_thread_id(runtime: object) -> str:
    context = getattr(runtime, "context", None)
    thread_id = context.get("thread_id") if isinstance(context, dict) else None
    if not isinstance(thread_id, str):
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_runtime_identity_missing")
    return thread_id


def _unwind_failed_binding(
    runtime: object,
    token: object | None,
    *,
    release_unbound: Callable[[], None] | None,
) -> None:
    """Undo a failed bind: drop the token, and the sandbox if nothing owns it.

    The consumer release is the coordinator's compare-and-clear, which parks
    the sandbox itself when it succeeds. Only a sandbox that never got a token
    is nobody's to park, so ``release_unbound`` runs for that case alone.
    """
    invalidate_runtime_skill_projection_token(runtime, token)
    if token is not None:
        try:
            release_accepted_skill_consumer(token)
        except Exception:
            logger.warning("Failed to clear a rejected accepted-skill projection", exc_info=True)
        return
    if release_unbound is not None:
        release_unbound()


def _warn_release_unfinished(token: object) -> None:
    """Say so when a release could not finish: the thread stays fenced until one does."""
    logger.warning(
        "Run %s on thread %s could not release its accepted-skill projection; the thread stays fenced until a later turn on it finishes that clear",
        getattr(token, "run_id", None),
        getattr(token, "thread_id", None),
    )


def _bind_runtime(
    provider: object,
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
    release_unbound: Callable[[], None] | None,
) -> bool:
    token = None
    try:
        require_runtime_accepted_skill_isolation(provider, runtime, sandbox_id=sandbox_id)
        binding, token, _created = ensure_accepted_skill_binding(runtime, sandbox_id=sandbox_id, user_id=user_id)
        if binding is None:
            return False
        require_accepted_skill_projection(provider).bind_accepted_skill_snapshot(
            sandbox_id,
            thread_id=_runtime_thread_id(runtime),
            user_id=user_id,
            binding=binding,
        )
    except Exception:
        _unwind_failed_binding(runtime, token, release_unbound=release_unbound)
        raise
    return True


async def _bind_runtime_async(
    provider: object,
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
    release_unbound: Callable[[], None] | None,
) -> bool:
    token = None
    try:
        require_runtime_accepted_skill_isolation(provider, runtime, sandbox_id=sandbox_id)
        binding, token, _created = ensure_accepted_skill_binding(runtime, sandbox_id=sandbox_id, user_id=user_id)
        if binding is None:
            return False
        await require_accepted_skill_projection(provider).bind_accepted_skill_snapshot_async(
            sandbox_id,
            thread_id=_runtime_thread_id(runtime),
            user_id=user_id,
            binding=binding,
        )
    except Exception:
        await asyncio.to_thread(_unwind_failed_binding, runtime, token, release_unbound=release_unbound)
        raise
    return True


def bind_runtime_accepted_skill_projection(
    provider: object,
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
) -> bool:
    """Idempotently bind accepted material into a sandbox the run already holds.

    Returns ``False`` when the run carries no accepted material. A failed bind
    releases the run's consumer token; the sandbox itself stays with whoever
    held it before.
    """
    return _bind_runtime(provider, runtime, sandbox_id=sandbox_id, user_id=user_id, release_unbound=None)


async def bind_runtime_accepted_skill_projection_async(
    provider: object,
    runtime: object,
    *,
    sandbox_id: str,
    user_id: str,
) -> bool:
    """Async counterpart that keeps provider I/O off the event loop."""
    return await _bind_runtime_async(provider, runtime, sandbox_id=sandbox_id, user_id=user_id, release_unbound=None)


def _material_binding(runtime: object, *, user_id: str) -> AcceptedSkillSandboxBindingV1:
    binding = accepted_skill_material_binding_from_runtime(runtime, user_id=user_id)
    if binding is None:
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_runtime_identity_missing")
    return binding


def provision_runtime_accepted_skill_projection(
    provider: object,
    runtime: object,
    *,
    thread_id: str,
    user_id: str,
) -> str:
    """Provision the run's accepted-skill projection and bind it; return the sandbox id.

    Provisioning goes through the provider's projection capability with the
    run's committed material binding, then the coordinator's consumer token is
    activated and its snapshot bound. A failure after provisioning unwinds the
    token, and releases the sandbox only when no token ever owned it.
    """
    binding = _material_binding(runtime, user_id=user_id)
    projection = require_accepted_skill_projection(provider)
    sandbox_id = projection.provision_accepted_skills(thread_id, user_id=user_id, binding=binding)
    release = getattr(provider, "release")
    bound = _bind_runtime(provider, runtime, sandbox_id=sandbox_id, user_id=user_id, release_unbound=lambda: release(sandbox_id))
    if not bound:
        release(sandbox_id)
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_binding_missing")
    return sandbox_id


async def provision_runtime_accepted_skill_projection_async(
    provider: object,
    runtime: object,
    *,
    thread_id: str,
    user_id: str,
) -> str:
    """Async counterpart of :func:`provision_runtime_accepted_skill_projection`.

    On the tenant profile this is where an accepted turn's sandbox is acquired
    (its first sandbox-backed tool call), so it records the same two phases the
    worker recorded when it did this before the model: ``skill_projection``
    and ``skill_snapshot_bind``.
    """
    from deerflow.runtime.turn_phases import TurnPhase, phase_span

    binding = _material_binding(runtime, user_id=user_id)
    projection = require_accepted_skill_projection(provider)
    with phase_span(TurnPhase.SKILL_PROJECTION):
        sandbox_id = await projection.provision_accepted_skills_async(thread_id, user_id=user_id, binding=binding)
    release = getattr(provider, "release")
    with phase_span(TurnPhase.SKILL_SNAPSHOT_BIND):
        bound = await _bind_runtime_async(provider, runtime, sandbox_id=sandbox_id, user_id=user_id, release_unbound=lambda: release(sandbox_id))
    if not bound:
        await asyncio.to_thread(release, sandbox_id)
        raise AcceptedSkillSandboxBindingError("accepted_skill_snapshot_binding_missing")
    return sandbox_id


__all__ = [
    "accepted_skill_access_from_runtime",
    "accepted_skill_binding_from_runtime",
    "accepted_skill_material_binding_from_runtime",
    "accepted_skill_projection",
    "accepted_skill_snapshot_id_from_runtime",
    "bind_runtime_accepted_skill_projection",
    "bind_runtime_accepted_skill_projection_async",
    "ensure_accepted_skill_binding",
    "has_accepted_skill_isolation",
    "invalidate_runtime_skill_projection_token",
    "provision_runtime_accepted_skill_projection",
    "provision_runtime_accepted_skill_projection_async",
    "complete_pending_projection_clear",
    "release_accepted_skill_consumer",
    "require_accepted_skill_projection",
    "require_runtime_accepted_skill_isolation",
]

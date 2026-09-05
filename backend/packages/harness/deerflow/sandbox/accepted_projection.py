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
    # Resolved at call time so a replaced or test-installed provider is the
    # one that clears; the coordinator's ownership outlives any one instance.
    from deerflow.sandbox.sandbox_provider import get_sandbox_provider

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
        # A successful compare-and-clear is the material-isolation boundary.
        # Resource parking/teardown may fail, but it cannot make the removed
        # accepted bytes reachable again, so stale ownership must not strand
        # the thread indefinitely.
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
    """Async counterpart of :func:`provision_runtime_accepted_skill_projection`."""
    binding = _material_binding(runtime, user_id=user_id)
    projection = require_accepted_skill_projection(provider)
    sandbox_id = await projection.provision_accepted_skills_async(thread_id, user_id=user_id, binding=binding)
    release = getattr(provider, "release")
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
    "release_accepted_skill_consumer",
    "require_accepted_skill_projection",
    "require_runtime_accepted_skill_isolation",
]

"""Optional sandbox provider capabilities, negotiated at runtime.

A sandbox provider is required to implement four verbs: ``acquire``, its async
twin, ``get`` and ``release`` (see
:class:`~deerflow.sandbox.sandbox_provider.SandboxProvider`). Everything
HartMesh's durable execution needs beyond that is a capability: a contract
declared here, offered by a provider through
``SandboxProvider.capability(protocol)`` and discovered by callers through
:func:`sandbox_capability`. A provider offers a capability by inheriting its
contract, in which case the base negotiation answers the provider itself, or
by answering a companion object that inherits it. A provider that offers
nothing answers ``None``, and every caller fails closed with a typed error, so
an accepted invocation can never run on a provider that only half-implements
the material it was admitted for.

Contracts:

* :class:`AcceptedSkillProjection`: sandboxes whose only skills mount is one
  accepted snapshot. This is the provider half of the accepted-skills
  projection Material (:mod:`deerflow.sandbox.accepted_projection`).
  ``provision_accepted_skills`` is its one provisioning verb: it acquires the
  isolated sandbox and exposes exactly the requested snapshot, which is what
  the four acquire-shaped provider methods used to spell out between them.
  Every member fails closed until the provider implements it.
* :class:`AcceptedMaterialization`: a qualified provider-neutral accepted
  material adapter (:class:`~deerflow.sandbox.accepted_material.AcceptedMaterializer`),
  selected per run. Only a provider with current external qualification
  evidence answers a selection.

Upstream's network policy hooks and skill projection sync stay on the base
class because upstream's middleware calls them there; they are the next
candidates for this negotiation once upstream adopts it.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from deerflow.sandbox.accepted_material import (
    AcceptedMaterialCapability,
    AcceptedSkillExecutionEvidence,
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)

if TYPE_CHECKING:
    from deerflow.runtime.skill_projection import SkillProjectionClear
    from deerflow.sandbox.accepted_material import AcceptedMaterializerSelection

_PROJECTION_UNSUPPORTED = "accepted_skill_snapshot_projection_unsupported"


def sandbox_capability[CapabilityT](provider: object, protocol: type[CapabilityT]) -> CapabilityT | None:
    """The object through which ``provider`` offers ``protocol``, or ``None``.

    A provider negotiates through its ``capability`` method when it has one;
    a duck-typed double without it offers exactly the contracts it inherits.
    Whatever is answered must itself inherit the contract: a capability is a
    declaration, never an accident of attribute names.
    """
    negotiate = getattr(provider, "capability", None)
    if callable(negotiate):
        found = negotiate(protocol)
    else:
        found = provider if isinstance(provider, protocol) else None
    if found is not None and not isinstance(found, protocol):
        raise TypeError(f"{type(provider).__name__}.capability answered an object that does not implement {protocol.__name__}")
    return found


class AcceptedSkillProjection:
    """Sandboxes whose only skills mount is one accepted snapshot.

    A provider that inherits this contract declares that it can create a
    sandbox without any live skill projection, bind exactly one accepted
    snapshot into it, prove that isolation before execution, and clear the
    snapshot under the coordinator's exact ownership proof. Every member
    fails closed, so a partial implementation refuses accepted material
    rather than executing it against live skill roots.
    """

    def provision_accepted_skills(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> str:
        """Acquire a sandbox isolated to accepted material for ``binding``.

        Returns the sandbox id. The returned sandbox's only skills mount is
        the ``.accepted`` tree; a thread sandbox created with live projections
        must never be returned here, and reusing the thread's existing
        accepted sandbox is the provider's business. The projection Material
        binds the coordinator-issued snapshot afterwards through
        ``bind_accepted_skill_snapshot``; a provider that must materialize at
        creation (a remote receipt) consumes ``binding`` here and treats that
        later bind as its idempotent receipt check.
        """
        del thread_id, user_id, binding
        raise AcceptedSkillSandboxBindingError(_PROJECTION_UNSUPPORTED)

    async def provision_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> str:
        return await asyncio.to_thread(
            self.provision_accepted_skills,
            thread_id,
            user_id=user_id,
            binding=binding,
        )

    def has_accepted_skill_isolation(self, sandbox_id: str) -> bool:
        """Whether ``sandbox_id`` was created without live skill paths."""
        del sandbox_id
        return False

    def accepted_skill_material_capability(self, sandbox_id: str) -> AcceptedMaterialCapability:
        """The immutable access profile available to accepted material.

        The conservative default permits only the explicit empty accepted set.
        A provider that advertises immutable read-only material must prove the
        projected files immutable to commands run inside that sandbox.
        """
        del sandbox_id
        return AcceptedMaterialCapability.EMPTY_ONLY

    def bind_accepted_skill_snapshot(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> None:
        """Expose exactly one accepted snapshot, or fail before execution."""
        del sandbox_id, thread_id, user_id, binding
        raise AcceptedSkillSandboxBindingError(_PROJECTION_UNSUPPORTED)

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

    def clear_accepted_skill_snapshot(self, clear: SkillProjectionClear) -> bool:
        """Compare-and-clear only an exact last-consumer ownership proof."""
        del clear
        raise AcceptedSkillSandboxBindingError(_PROJECTION_UNSUPPORTED)

    def ensure_accepted_skill_snapshot_absent(self, clear: SkillProjectionClear) -> bool:
        """Prove an exact failed or unpublished projection cannot be reached.

        Deliberately separate from compare-and-clear: it may be used only when
        no exact binding receipt exists, and it fails closed until the provider
        can prove an empty namespace or quarantine the exact sandbox.
        """
        del clear
        return False

    def accepted_skill_execution_evidence(self, sandbox_id: str) -> AcceptedSkillExecutionEvidence | None:
        """Bounded execution evidence when the provider has a native attempt."""
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
        """Renew the exact attempt after the authoritative run lease renewed."""
        del sandbox_id, evidence
        return False


class AcceptedMaterialization:
    """A qualified provider-neutral accepted material adapter, selected per run.

    ``resolve_accepted_materializer`` negotiates this contract and validates
    the answered selection's qualification and capability profile; the worker
    never imports a concrete adapter. The default answers no selection, which
    admits only the explicit empty accepted set.
    """

    async def accepted_materializer_selection(
        self,
        *,
        binding: AcceptedSkillSandboxBindingV1,
        thread_id: str,
        user_id: str,
    ) -> AcceptedMaterializerSelection | None:
        del binding, thread_id, user_id
        return None


def reject_writable_accepted_skill_aliases(
    accepted_root: str | Path,
    mounts: list[tuple[str, bool]],
) -> None:
    """Reject writable host mounts that overlap accepted material.

    An implementer's helper: container-path isolation alone is insufficient,
    because the same host directory can be mounted again at an unrelated
    virtual path, and either ancestor or descendant overlap gives that alias
    write access to accepted bytes.
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


__all__ = [
    "AcceptedMaterialization",
    "AcceptedSkillProjection",
    "reject_writable_accepted_skill_aliases",
    "sandbox_capability",
]

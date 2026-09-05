"""Capability negotiation on the sandbox provider surface."""

from __future__ import annotations

import pytest

from deerflow.sandbox.capabilities import sandbox_capability
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider


class SkillProjection:
    """A contract a provider may offer: sync a skills projection into a sandbox."""

    def project_skills(self, sandbox_id: str) -> bool:
        return False


class Warmable:
    """Another contract, unrelated to the first."""

    def prewarm(self, count: int) -> int:
        return 0


class _Minimal(SandboxProvider):
    def acquire(self, thread_id=None, *, user_id=None):
        return "sandbox"

    async def acquire_async(self, thread_id=None, *, user_id=None):
        return "sandbox"

    def get(self, sandbox_id):
        return None

    def release(self, sandbox_id):
        return None


class _Projecting(_Minimal, SkillProjection):
    pass


class _Companion(Warmable):
    pass


class _Delegating(_Minimal):
    def __init__(self) -> None:
        self.companion = _Companion()

    def capability(self, protocol):
        if protocol is Warmable:
            return self.companion
        return super().capability(protocol)


class _Lying(_Minimal):
    def capability(self, protocol):
        return object()


def test_the_required_surface_offers_no_capability() -> None:
    provider = _Minimal()

    assert provider.capability(SkillProjection) is None
    assert sandbox_capability(provider, SkillProjection) is None


def test_a_provider_offers_a_capability_by_inheriting_its_contract() -> None:
    provider = _Projecting()

    assert provider.capability(SkillProjection) is provider
    assert sandbox_capability(provider, SkillProjection) is provider
    assert sandbox_capability(provider, Warmable) is None


def test_a_provider_may_answer_a_companion_object() -> None:
    provider = _Delegating()

    assert sandbox_capability(provider, Warmable) is provider.companion
    assert sandbox_capability(provider, SkillProjection) is None


def test_an_answer_must_declare_the_contract_it_answers_for() -> None:
    with pytest.raises(TypeError, match="does not implement SkillProjection"):
        sandbox_capability(_Lying(), SkillProjection)


def test_duck_typed_doubles_negotiate_structurally_or_not_at_all() -> None:
    class _Double(SkillProjection):
        pass

    assert sandbox_capability(_Double(), SkillProjection) is not None
    assert sandbox_capability(object(), SkillProjection) is None


def test_the_sandbox_handle_type_is_not_a_capability() -> None:
    assert sandbox_capability(_Minimal(), Sandbox) is None

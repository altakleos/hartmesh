"""Provider capability negotiation and the accepted-skill projection contract."""

from __future__ import annotations

import inspect

import pytest

from deerflow.sandbox.accepted_material import (
    AcceptedMaterialCapability,
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.accepted_projection import (
    accepted_skill_projection,
    has_accepted_skill_isolation,
    require_accepted_skill_projection,
)
from deerflow.sandbox.capabilities import (
    AcceptedMaterialization,
    AcceptedSkillProjection,
    sandbox_capability,
)
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider
from deerflow.sandbox.session import sandbox_session_provider

# The HartMesh extras that used to sit on every provider. None of them may
# come back to the required surface: a provider author reading the abstract
# class alone must see acquire, its async twin, get, release and negotiation.
_RETIRED_PROVIDER_EXTRAS = frozenset(
    {
        "acquire_accepted_skills",
        "acquire_accepted_skills_async",
        "acquire_bound_accepted_skills",
        "acquire_bound_accepted_skills_async",
        "accepted_skill_execution_evidence",
        "validate_accepted_skill_execution_async",
        "renew_accepted_skill_execution_async",
        "has_accepted_skill_isolation",
        "accepted_skill_material_capability",
        "bind_accepted_skill_snapshot",
        "bind_accepted_skill_snapshot_async",
        "clear_accepted_skill_snapshot",
        "ensure_accepted_skill_snapshot_absent",
        "clear_accepted_skill_snapshot_async",
        "accepted_materializer_selection",
    }
)


class _Ordinary(SandboxProvider):
    def acquire(self, thread_id=None, *, user_id=None):
        return "ordinary"

    def get(self, sandbox_id):
        return None

    def release(self, sandbox_id):
        return None


class _Projecting(_Ordinary, AcceptedSkillProjection):
    def __init__(self) -> None:
        self.provisioned: list[tuple[str, str, AcceptedSkillSandboxBindingV1]] = []

    def provision_accepted_skills(self, thread_id, *, user_id, binding):
        self.provisioned.append((thread_id, user_id, binding))
        return f"{thread_id}-accepted"

    def has_accepted_skill_isolation(self, sandbox_id):
        return sandbox_id.endswith("-accepted")


class _Companion(AcceptedMaterialization):
    async def accepted_materializer_selection(self, *, binding, thread_id, user_id):
        return None


class _Delegating(_Ordinary):
    def __init__(self) -> None:
        self.companion = _Companion()

    def capability(self, protocol):
        if protocol is AcceptedMaterialization:
            return self.companion
        return super().capability(protocol)


def test_required_provider_surface_carries_no_accepted_extras() -> None:
    public = {name for name in dir(SandboxProvider) if not name.startswith("_")}
    assert public.isdisjoint(_RETIRED_PROVIDER_EXTRAS)
    abstract = set(SandboxProvider.__abstractmethods__)
    assert abstract == {"acquire", "get", "release"}
    assert inspect.isfunction(inspect.getattr_static(SandboxProvider, "capability"))


def test_provider_offers_a_capability_by_implementing_its_contract() -> None:
    provider = _Projecting()
    assert provider.capability(AcceptedSkillProjection) is provider
    assert provider.capability(AcceptedMaterialization) is None
    assert sandbox_capability(provider, AcceptedSkillProjection) is provider
    assert sandbox_capability(_Ordinary(), AcceptedSkillProjection) is None


def test_provider_may_answer_a_companion_object() -> None:
    provider = _Delegating()
    assert sandbox_capability(provider, AcceptedMaterialization) is provider.companion
    assert sandbox_capability(provider, AcceptedSkillProjection) is None


def test_a_companion_must_declare_the_contract_it_answers_for() -> None:
    class _Undeclared:
        async def accepted_materializer_selection(self, *, binding, thread_id, user_id):
            return None

    class _Lying(_Ordinary):
        def capability(self, protocol):
            return _Undeclared()

    with pytest.raises(TypeError, match="does not implement AcceptedMaterialization"):
        sandbox_capability(_Lying(), AcceptedMaterialization)


def test_duck_typed_doubles_negotiate_structurally_or_not_at_all() -> None:
    class _Bare(AcceptedSkillProjection):
        pass

    assert sandbox_capability(_Bare(), AcceptedSkillProjection).__class__ is _Bare
    assert sandbox_capability(object(), AcceptedSkillProjection) is None
    with pytest.raises(TypeError):
        sandbox_capability(_Projecting(), object())


def test_session_provider_answers_itself_for_self_implemented_capabilities() -> None:
    backing = _Projecting()
    wrapper = sandbox_session_provider(backing)
    projection = wrapper.capability(AcceptedSkillProjection)
    assert projection is wrapper
    assert projection.provision_accepted_skills("thread-1", user_id="user-1", binding=AcceptedSkillSandboxBindingV1(snapshot_id=None)) == "thread-1-accepted"
    assert backing.provisioned[0][:2] == ("thread-1", "user-1")
    assert wrapper.capability(AcceptedMaterialization) is None
    delegating = sandbox_session_provider(_Delegating())
    assert delegating.capability(AcceptedMaterialization) is delegating.backing.companion


def test_accepted_skill_projection_defaults_fail_closed() -> None:
    class _Bare(AcceptedSkillProjection):
        pass

    projection = _Bare()
    binding = AcceptedSkillSandboxBindingV1(snapshot_id=None)
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_skill_snapshot_projection_unsupported"):
        projection.provision_accepted_skills("thread-1", user_id="user-1", binding=binding)
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_skill_snapshot_projection_unsupported"):
        projection.bind_accepted_skill_snapshot("sandbox", thread_id="thread-1", user_id="user-1", binding=binding)
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_skill_snapshot_projection_unsupported"):
        projection.clear_accepted_skill_snapshot(object())
    assert projection.has_accepted_skill_isolation("sandbox") is False
    assert projection.accepted_skill_material_capability("sandbox") is AcceptedMaterialCapability.EMPTY_ONLY
    assert projection.ensure_accepted_skill_snapshot_absent(object()) is False
    assert projection.accepted_skill_execution_evidence("sandbox") is None


@pytest.mark.anyio
async def test_accepted_skill_projection_async_defaults_offload_the_sync_members() -> None:
    class _Sync(AcceptedSkillProjection):
        def __init__(self) -> None:
            self.calls: list[str] = []

        def provision_accepted_skills(self, thread_id, *, user_id, binding):
            self.calls.append("provision")
            return "provisioned"

        def bind_accepted_skill_snapshot(self, sandbox_id, *, thread_id, user_id, binding):
            self.calls.append("bind")

    projection = _Sync()
    binding = AcceptedSkillSandboxBindingV1(snapshot_id=None)
    assert await projection.provision_accepted_skills_async("thread-1", user_id="user-1", binding=binding) == "provisioned"
    await projection.bind_accepted_skill_snapshot_async("provisioned", thread_id="thread-1", user_id="user-1", binding=binding)
    assert projection.calls == ["provision", "bind"]
    assert await projection.validate_accepted_skill_execution_async("provisioned", object()) is False
    assert await projection.renew_accepted_skill_execution_async("provisioned", object()) is False


def test_require_accepted_skill_projection_fails_typed_without_the_capability() -> None:
    assert accepted_skill_projection(_Ordinary()) is None
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_skill_snapshot_projection_unsupported"):
        require_accepted_skill_projection(_Ordinary())
    provider = _Projecting()
    assert require_accepted_skill_projection(provider) is provider
    assert has_accepted_skill_isolation(provider, "thread-accepted") is True
    assert has_accepted_skill_isolation(provider, "thread") is False
    assert has_accepted_skill_isolation(_Ordinary(), "thread-accepted") is False


def test_accepted_materialization_default_offers_no_selection() -> None:
    class _Bare(AcceptedMaterialization):
        pass

    import asyncio

    selection = asyncio.run(_Bare().accepted_materializer_selection(binding=AcceptedSkillSandboxBindingV1(snapshot_id=None), thread_id="thread-1", user_id="user-1"))
    assert selection is None


def test_sandbox_handle_type_is_not_a_capability() -> None:
    assert sandbox_capability(_Projecting(), Sandbox) is None

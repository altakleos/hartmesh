"""Authoritative, bounded evidence for the graph an accepted run assembled."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from dataclasses import replace

import pytest
from deerflow_extension_api import AgentAssemblyDescriptor, MiddlewareDescriptor, ToolDescriptor

from deerflow.runtime.assembly_evidence import (
    AcceptedAssemblyAnchors,
    AssemblyEvidenceError,
    AssemblyEvidenceV1,
    assert_descriptor_projection_complete,
    build_assembly_evidence,
    canonical_durable_policy_digest,
    canonical_skillset_digest,
    verify_bound_assembly,
)


def _policies() -> dict[str, object]:
    return {
        "bootstrap": False,
        "non_interactive": True,
        "plan_mode": False,
        "recursion_limit": 1000,
        "subagents": {
            "enabled": True,
            "max_concurrent": 2,
            "max_total": 5,
            "type_allowlist": ["general-purpose"],
            "runtime_limits": {"general-purpose": {"max_turns": 20, "timeout_seconds": 300}},
        },
        "deferred_tools": {"enabled": True, "catalog_hash": "8" * 64},
        "deferred_skills": True,
        "prompt_template_id": "deerflow-lead-agent-v1",
        "skill_catalog_hash": "7" * 64,
    }


def _descriptor() -> AgentAssemblyDescriptor:
    return AgentAssemblyDescriptor(
        namespace="deerflow",
        agent_name="lead-agent",
        requested_model="default",
        effective_model="gpt-5",
        model_parameters={"temperature": 0.2, "provider": "openai"},
        thinking_enabled=True,
        reasoning_effort="high",
        base_prompt_hash="1" * 64,
        tools=(
            ToolDescriptor(
                name="bash",
                description_hash="2" * 64,
                schema_hash="3" * 64,
                source="builtin",
            ),
        ),
        middlewares=(
            MiddlewareDescriptor(
                name="LimitMiddleware",
                module="deerflow.test",
                policy_parameters={"limit": 5},
            ),
        ),
        deferred_tool_names=("browser",),
        enabled_skills=("research",),
        effective_policies=_policies(),
        build={"git_commit": "not-authoritative"},
    )


def _anchors(descriptor: AgentAssemblyDescriptor | None = None) -> AcceptedAssemblyAnchors:
    descriptor = descriptor or _descriptor()
    return AcceptedAssemblyAnchors(
        run_id="run-123",
        expected_namespace="deerflow",
        expected_agent_name="lead-agent",
        expected_effective_model="gpt-5",
        expected_skillset_digest=canonical_skillset_digest(
            descriptor.enabled_skills,
            catalog_digest="7" * 64,
        ),
        expected_policy_digest=canonical_durable_policy_digest(descriptor.effective_policies),
        agent_revision_digest="4" * 64,
        extension_generation=9,
    )


def _assert_code(expected: str, callable_) -> None:
    with pytest.raises(AssemblyEvidenceError) as exc_info:
        callable_()
    assert exc_info.value.code == expected
    assert str(exc_info.value) == expected


def test_evidence_is_stable_across_mapping_order_and_process_restart():
    descriptor = _descriptor()
    reordered = replace(
        descriptor,
        model_parameters={"provider": "openai", "temperature": 0.2},
        effective_policies=dict(reversed(list(descriptor.effective_policies.items()))),
    )
    expected = build_assembly_evidence(descriptor, anchors=_anchors(descriptor))
    assert expected.fingerprint == descriptor.fingerprint
    assert build_assembly_evidence(reordered, anchors=_anchors(reordered)) == expected

    script = textwrap.dedent(
        f"""
        from deerflow_extension_api import AgentAssemblyDescriptor, MiddlewareDescriptor, ToolDescriptor
        from deerflow.runtime.assembly_evidence import (
            AcceptedAssemblyAnchors, build_assembly_evidence,
            canonical_durable_policy_digest, canonical_skillset_digest,
        )
        policies = {_policies()!r}
        descriptor = AgentAssemblyDescriptor(
            namespace='deerflow', agent_name='lead-agent', requested_model='default',
            effective_model='gpt-5', model_parameters={{'provider': 'openai', 'temperature': 0.2}},
            thinking_enabled=True, reasoning_effort='high', base_prompt_hash={"1" * 64!r},
            tools=(ToolDescriptor(name='bash', description_hash={"2" * 64!r}, schema_hash={"3" * 64!r}, source='builtin'),),
            middlewares=(MiddlewareDescriptor(name='LimitMiddleware', module='deerflow.test', policy_parameters={{'limit': 5}}),),
            deferred_tool_names=('browser',), enabled_skills=('research',),
            effective_policies=policies, build={{'git_commit': 'not-authoritative'}},
        )
        anchors = AcceptedAssemblyAnchors(
            run_id='run-123', expected_namespace='deerflow', expected_agent_name='lead-agent',
            expected_effective_model='gpt-5',
            expected_skillset_digest=canonical_skillset_digest(descriptor.enabled_skills, catalog_digest={"7" * 64!r}),
            expected_policy_digest=canonical_durable_policy_digest(policies),
            agent_revision_digest={"4" * 64!r}, extension_generation=9,
        )
        print(build_assembly_evidence(descriptor, anchors=anchors).fingerprint)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == expected.fingerprint


def test_every_descriptor_field_has_an_explicit_evidence_classification():
    assert_descriptor_projection_complete()


@pytest.mark.parametrize(
    ("classification_name", "removed_field", "descriptor_name"),
    [
        ("_FINGERPRINTED_TOOL_FIELDS", "source", "ToolDescriptor"),
        (
            "_FINGERPRINTED_MIDDLEWARE_FIELDS",
            "module",
            "MiddlewareDescriptor",
        ),
    ],
)
def test_nested_descriptor_fields_require_explicit_classification(
    monkeypatch,
    classification_name: str,
    removed_field: str,
    descriptor_name: str,
) -> None:
    from deerflow.runtime import assembly_evidence

    classified = getattr(assembly_evidence, classification_name)
    monkeypatch.setattr(
        assembly_evidence,
        classification_name,
        classified - {removed_field},
    )

    with pytest.raises(AssertionError, match=descriptor_name):
        assert_descriptor_projection_complete()


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda value: replace(value, namespace="other"), "assembly_agent_mismatch"),
        (lambda value: replace(value, agent_name="other"), "assembly_agent_mismatch"),
        (lambda value: replace(value, effective_model="other"), "assembly_model_mismatch"),
        (lambda value: replace(value, enabled_skills=("other",)), "assembly_skill_mismatch"),
        (
            lambda value: replace(value, effective_policies={**value.effective_policies, "recursion_limit": 999}),
            "assembly_policy_mismatch",
        ),
    ],
)
def test_accepted_anchor_mismatches_have_stable_safe_codes(mutate, code):
    descriptor = mutate(_descriptor())
    _assert_code(code, lambda: build_assembly_evidence(descriptor, anchors=_anchors()))


@pytest.mark.parametrize(
    "descriptor",
    [
        replace(_descriptor(), base_prompt_hash="A" * 64),
        replace(_descriptor(), agent_name="x" * 129),
        replace(_descriptor(), tools=_descriptor().tools * 2),
        replace(_descriptor(), enabled_skills=("research", "research")),
        replace(_descriptor(), model_parameters={"api_key": "must-not-survive"}),
        replace(_descriptor(), effective_policies={**_policies(), "opaque_secret": "must-not-survive"}),
    ],
)
def test_malformed_duplicate_unbounded_and_secret_shaped_descriptors_are_rejected(descriptor):
    _assert_code(
        "assembly_descriptor_invalid",
        lambda: build_assembly_evidence(descriptor, anchors=_anchors(descriptor)),
    )


def test_persisted_evidence_is_strict_bounded_and_round_trips():
    evidence = build_assembly_evidence(_descriptor(), anchors=_anchors())
    payload = evidence.to_persisted_json()
    assert AssemblyEvidenceV1.from_persisted_json(payload) == evidence

    _assert_code(
        "assembly_descriptor_invalid",
        lambda: AssemblyEvidenceV1.from_persisted_json({**payload, "raw_prompt": "do not store me"}),
    )
    _assert_code(
        "assembly_descriptor_invalid",
        lambda: AssemblyEvidenceV1.from_persisted_json({**payload, "effective_model": "é" * 65}),
    )
    _assert_code(
        "assembly_descriptor_invalid",
        lambda: AssemblyEvidenceV1.from_persisted_json({**payload, "fingerprint": "A" * 64}),
    )


def test_bound_evidence_requires_byte_equivalent_v1_evidence():
    persisted = build_assembly_evidence(_descriptor(), anchors=_anchors())
    verify_bound_assembly(persisted, persisted=persisted)

    changed = replace(persisted, toolset_digest="f" * 64)
    _assert_code(
        "assembly_evidence_mismatch",
        lambda: verify_bound_assembly(changed, persisted=persisted),
    )

"""Run-bound egress: the accepted Kind's allowance is declared at admission."""

from __future__ import annotations

import ipaddress

import pytest
from deerflow_extension_api import TenantReferenceV1
from pydantic import ValidationError

from deerflow.config.execution_policy_config import AcceptedEgressConfig, ExecutionPolicyConfig
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.sandbox.egress import (
    EGRESS_ALLOWANCE_VERSION,
    MAX_EGRESS_RULES,
    NEVER_ALLOWED_NETWORKS,
    EgressAllowanceV1,
    EgressPolicyError,
    EgressRuleV1,
    resolve_egress_allowance,
)


def _rule(cidr: str, *, protocol: str = "TCP", port: int | None = 443) -> EgressRuleV1:
    return EgressRuleV1.build(cidr=cidr, protocol=protocol, port=port)


def test_rules_are_canonical_and_public_only() -> None:
    rule = EgressRuleV1.build(cidr="140.82.112.0/20", protocol="tcp", port=443)
    assert rule.protocol == "TCP"
    assert rule.cidr == "140.82.112.0/20"
    assert EgressRuleV1.build(cidr="2606:4700::/32", protocol="udp", port=None).port is None

    with pytest.raises(EgressPolicyError, match="egress_rule_cidr_invalid"):
        EgressRuleV1.build(cidr="140.82.112.1/20", protocol="TCP", port=443)
    with pytest.raises(EgressPolicyError, match="egress_rule_cidr_invalid"):
        EgressRuleV1.build(cidr="github.com", protocol="TCP", port=443)
    for never in ("10.1.0.0/16", "127.0.0.1/32", "169.254.169.254/32", "172.20.0.0/16", "192.168.1.0/24", "100.64.0.0/10", "224.0.0.0/8", "fe80::/64", "fc00::/8", "::1/128", "::ffff:10.0.0.0/104"):
        with pytest.raises(EgressPolicyError, match="egress_rule_not_public"):
            EgressRuleV1.build(cidr=never, protocol="TCP", port=443)
    with pytest.raises(EgressPolicyError, match="egress_rule_protocol_invalid"):
        EgressRuleV1.build(cidr="1.1.1.0/24", protocol="ICMP", port=None)
    for port in (0, 65536, "443", True):
        with pytest.raises(EgressPolicyError, match="egress_rule_port_invalid"):
            EgressRuleV1.build(cidr="1.1.1.0/24", protocol="TCP", port=port)  # type: ignore[arg-type]


def test_never_allowed_networks_cover_private_loopback_link_local_and_metadata() -> None:
    networks = [ipaddress.ip_network(value) for value in NEVER_ALLOWED_NETWORKS]
    for address in ("10.42.0.7", "172.17.0.1", "192.168.0.1", "127.0.0.1", "169.254.169.254", "100.64.1.1", "224.0.0.1", "0.0.0.0", "255.255.255.255", "::1", "fe80::1", "fd00::1", "ff02::1"):
        assert any(ipaddress.ip_address(address) in network for network in networks), address
    for address in ("1.1.1.1", "140.82.112.3", "2606:4700::1111"):
        assert not any(ipaddress.ip_address(address) in network for network in networks), address


def test_allowance_is_canonical_digest_bound_and_bounded() -> None:
    allowance = EgressAllowanceV1.build(
        profile="accepted-egress-v1",
        dns=True,
        rules=(_rule("140.82.112.0/20"), _rule("1.1.1.0/24"), _rule("140.82.112.0/20")),
    )
    assert [rule.cidr for rule in allowance.rules] == ["1.1.1.0/24", "140.82.112.0/20"]
    encoded = allowance.to_json()
    assert set(encoded) == {"version", "profile", "dns", "rules", "digest"}
    assert encoded["version"] == EGRESS_ALLOWANCE_VERSION
    assert encoded["rules"][0] == {"cidr": "1.1.1.0/24", "protocol": "TCP", "port": 443}
    assert EgressAllowanceV1.from_json(encoded) == allowance
    assert EgressAllowanceV1.from_json(encoded).digest == allowance.digest

    forged = {**encoded, "rules": [{"cidr": "1.1.1.0/24", "protocol": "TCP", "port": 80}, encoded["rules"][1]]}
    with pytest.raises(EgressPolicyError, match="egress_allowance_digest_invalid"):
        EgressAllowanceV1.from_json(forged)
    with pytest.raises(EgressPolicyError, match="egress_allowance_version_unsupported"):
        EgressAllowanceV1.from_json({**encoded, "extra": 1})
    with pytest.raises(EgressPolicyError, match="egress_allowance_not_canonical"):
        EgressAllowanceV1.from_json({**encoded, "rules": list(reversed(encoded["rules"])), "digest": "0" * 64})
    with pytest.raises(EgressPolicyError, match="egress_allowance_profile_invalid"):
        EgressAllowanceV1.build(profile="bad profile", dns=False, rules=())
    with pytest.raises(EgressPolicyError, match="egress_allowance_rule_limit"):
        EgressAllowanceV1.build(profile="p", dns=False, rules=tuple(_rule(f"1.{index}.0.0/16") for index in range(MAX_EGRESS_RULES + 1)))

    deny_all = EgressAllowanceV1.deny_all()
    assert deny_all.rules == ()
    assert deny_all.dns is False
    assert deny_all.digest == EgressAllowanceV1.deny_all().digest
    assert deny_all.digest != allowance.digest


def test_callers_can_only_narrow_the_ceiling() -> None:
    ceiling = EgressAllowanceV1.build(
        profile="accepted-egress-v1",
        dns=True,
        rules=(_rule("140.82.112.0/20"), _rule("1.1.1.0/24", protocol="UDP", port=None)),
    )
    narrowed = ceiling.narrow({"dns": False, "rules": [{"cidr": "140.82.113.0/24", "port": 443}, {"cidr": "1.1.1.8/29", "protocol": "udp", "port": 53}]})
    assert narrowed.dns is False
    assert [(rule.cidr, rule.protocol, rule.port) for rule in narrowed.rules] == [("1.1.1.8/29", "UDP", 53), ("140.82.113.0/24", "TCP", 443)]
    assert narrowed.profile == ceiling.profile
    assert ceiling.narrow({}) == ceiling
    assert ceiling.narrow({"rules": []}).rules == ()

    for requested in (
        {"rules": [{"cidr": "140.82.0.0/16", "port": 443}]},
        {"rules": [{"cidr": "140.82.113.0/24", "port": 80}]},
        {"rules": [{"cidr": "140.82.113.0/24", "protocol": "UDP", "port": 443}]},
        {"rules": [{"cidr": "8.8.8.0/24", "port": 443}]},
    ):
        with pytest.raises(EgressPolicyError, match="egress_allowance_broadening_forbidden"):
            ceiling.narrow(requested)
    with pytest.raises(EgressPolicyError, match="egress_allowance_broadening_forbidden"):
        EgressAllowanceV1.build(profile="p", dns=False, rules=(_rule("1.1.1.0/24"),)).narrow({"dns": True})
    with pytest.raises(EgressPolicyError, match="egress_allowance_field_forbidden"):
        ceiling.narrow({"profile": "other"})
    for invalid in ({"rules": "all"}, {"rules": [1]}, {"dns": "yes"}, {"rules": [{"cidr": "10.0.0.0/8"}]}):
        with pytest.raises(EgressPolicyError):
            ceiling.narrow(invalid)


def test_config_ceiling_resolves_and_rejects_private_ranges() -> None:
    default = ExecutionPolicyConfig()
    assert default.accepted_egress == AcceptedEgressConfig()
    assert resolve_egress_allowance(default.accepted_egress) == EgressAllowanceV1.build(profile="accepted-egress-v1", dns=False, rules=())

    config = ExecutionPolicyConfig.model_validate(
        {
            "accepted_egress": {
                "profile": "team-egress-v2",
                "dns": True,
                "allow": [{"cidr": "140.82.112.0/20", "port": 443}, {"cidr": "1.1.1.0/24", "protocol": "udp"}],
            }
        }
    )
    ceiling = resolve_egress_allowance(config.accepted_egress)
    assert ceiling.profile == "team-egress-v2"
    assert ceiling.dns is True
    assert [(rule.cidr, rule.protocol, rule.port) for rule in ceiling.rules] == [("1.1.1.0/24", "UDP", None), ("140.82.112.0/20", "TCP", 443)]
    narrowed = resolve_egress_allowance(config.accepted_egress, requested={"dns": False, "rules": [{"cidr": "140.82.112.0/24", "port": 443}]})
    assert narrowed.dns is False and len(narrowed.rules) == 1

    with pytest.raises(ValidationError, match="egress_rule_not_public"):
        ExecutionPolicyConfig.model_validate({"accepted_egress": {"allow": [{"cidr": "10.0.0.0/8"}]}})
    with pytest.raises(ValidationError, match="egress_rule_cidr_invalid"):
        ExecutionPolicyConfig.model_validate({"accepted_egress": {"allow": [{"cidr": "example.com"}]}})
    with pytest.raises(ValidationError):
        ExecutionPolicyConfig.model_validate({"accepted_egress": {"allow": [{"cidr": "1.1.1.0/24", "port": 70000}]}})
    with pytest.raises(EgressPolicyError, match="egress_allowance_request_invalid"):
        resolve_egress_allowance(config.accepted_egress, requested="all")  # type: ignore[arg-type]


def _material() -> ResolvedAgentMaterialV1:
    return ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="file",
        storage_version="v1",
        agent_config=None,
        soul="test",
        model_profile={"name": "default", "model": "test"},
        tool_groups=(),
        tools=(),
        skills=(),
        runtime_defaults={},
    )


def test_accepted_invocation_binds_and_validates_the_egress_allowance() -> None:
    allowance = EgressAllowanceV1.build(profile="accepted-egress-v1", dns=False, rules=(_rule("140.82.112.0/20"),))
    seal_arguments = {
        "principal": PrincipalProjection(user_id="user-1"),
        "origin": InvocationOrigin(source_kind="http"),
        "tenant": TenantReferenceV1(version=1, public_ref="tenant-aaaaaaaaaaaaaaaa", digest="a" * 64),
        "thread_id": "thread-egress",
        "context_references": {},
        "agent_revision": ResolvedAgentRevision.from_material(_material()),
        "normalized_input": {},
        "execution_options": {},
        "extension_generation": 0,
        "contributor_execution_digest": canonical_digest({"version": 1, "execution": []}),
    }
    accepted = AcceptedInvocation.seal(**seal_arguments, egress_allowance=allowance)
    unbound = AcceptedInvocation.seal(**seal_arguments)

    assert accepted.egress_allowance == allowance
    assert unbound.egress_allowance is None
    assert accepted.runtime_identity_digest != unbound.runtime_identity_digest
    assert accepted.decision_evidence["egress_allowance"]["digest"] == allowance.digest
    assert accepted.to_persisted()["decision_evidence_json"]["egress_allowance"] == allowance.to_json()

    restored = AcceptedInvocation.from_persisted({**accepted.to_persisted(), "thread_id": accepted.thread_id, "kwargs": {}})
    assert restored.egress_allowance == allowance

    forged = accepted.to_persisted()
    forged["decision_evidence_json"]["egress_allowance"]["rules"][0]["port"] = 80
    with pytest.raises(ValueError, match="egress allowance"):
        AcceptedInvocation.from_persisted({**forged, "thread_id": accepted.thread_id, "kwargs": {}})
    with pytest.raises(TypeError):
        AcceptedInvocation.seal(**seal_arguments, egress_allowance=allowance.to_json())  # type: ignore[arg-type]

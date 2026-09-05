"""The provisioner renders the accepted Kind's run-bound egress and attests it."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from deerflow.runtime.skill_projection import SkillProjectionEvidence, SkillSnapshotProjection
from deerflow.sandbox.egress import NEVER_ALLOWED_NETWORKS, EgressAllowanceV1, EgressRuleV1, egress_allowance_digest


def _allowance(*, dns: bool = True) -> EgressAllowanceV1:
    return EgressAllowanceV1.build(
        profile="accepted-egress-v1",
        dns=dns,
        rules=(
            EgressRuleV1.build(cidr="0.0.0.0/0", protocol="TCP", port=443),
            EgressRuleV1.build(cidr="140.82.112.0/20", protocol="UDP", port=None),
            EgressRuleV1.build(cidr="::/0", protocol="TCP", port=443),
        ),
    )


def test_provisioner_shares_the_never_allowed_networks_and_the_digest(provisioner_module) -> None:
    assert tuple(provisioner_module.EGRESS_NEVER_ALLOWED_NETWORKS) == NEVER_ALLOWED_NETWORKS
    allowance = _allowance()
    parsed = provisioner_module.EgressAllowanceV1.model_validate(allowance.to_json())
    assert parsed.digest == allowance.digest
    projection = {key: value for key, value in allowance.to_json().items() if key != "digest"}
    assert provisioner_module._egress_allowance_digest(projection) == egress_allowance_digest(projection)
    assert provisioner_module.EgressAllowanceV1.model_validate(EgressAllowanceV1.deny_all().to_json()).rules == []

    for forged in (
        {**allowance.to_json(), "digest": "0" * 64},
        {**allowance.to_json(), "dns": not allowance.dns},
        {**allowance.to_json(), "rules": list(reversed(allowance.to_json()["rules"]))},
        {**allowance.to_json(), "rules": [{"cidr": "10.0.0.0/8", "protocol": "TCP", "port": 443}]},
        {**allowance.to_json(), "rules": [{"cidr": "140.82.112.1/20", "protocol": "TCP", "port": 443}]},
        {**allowance.to_json(), "extra": True},
    ):
        with pytest.raises(ValidationError):
            provisioner_module.EgressAllowanceV1.model_validate(forged)

    with pytest.raises(ValidationError, match="egress_allowance requires accepted material"):
        provisioner_module.CreateSandboxRequest(sandbox_id="sandbox-1", egress_allowance=allowance.to_json())


def test_accepted_policy_renders_the_allowance_and_denies_everything_else(provisioner_module) -> None:
    owner = provisioner_module.k8s_client.V1OwnerReference(api_version="coordination.k8s.io/v1", kind="Lease", name="sandbox-sandbox-1-accepted-attempt", uid="lease-uid-1")

    legacy = provisioner_module._build_accepted_network_policy("sandbox-1", accepted_attempt_owner=owner)
    assert legacy.spec.policy_types == ["Ingress"]
    assert legacy.spec.egress is None

    deny_all = provisioner_module._build_accepted_network_policy("sandbox-1", accepted_attempt_owner=owner, egress_allowance=provisioner_module.EgressAllowanceV1.model_validate(EgressAllowanceV1.deny_all().to_json()))
    assert deny_all.spec.policy_types == ["Ingress", "Egress"]
    assert deny_all.spec.egress is None
    assert deny_all.spec.ingress == legacy.spec.ingress
    assert provisioner_module._resource_spec_digest(deny_all) != provisioner_module._resource_spec_digest(legacy)

    rendered = provisioner_module._build_accepted_network_policy("sandbox-1", accepted_attempt_owner=owner, egress_allowance=provisioner_module.EgressAllowanceV1.model_validate(_allowance().to_json()))
    assert rendered.spec.policy_types == ["Ingress", "Egress"]
    assert rendered.metadata.annotations == {"hartmesh.io/accepted-egress-allowance-digest": _allowance().digest}
    dns_rule, *ip_rules = rendered.spec.egress
    assert dns_rule.to[0].namespace_selector.match_labels == {"kubernetes.io/metadata.name": "kube-system"}
    assert dns_rule.to[0].pod_selector.match_labels == {"k8s-app": "kube-dns"}
    assert [(port.protocol, port.port) for port in dns_rule.ports] == [("UDP", 53), ("TCP", 53)]
    assert [(rule.to[0].ip_block.cidr, rule.ports[0].protocol, rule.ports[0].port) for rule in ip_rules] == [("0.0.0.0/0", "TCP", 443), ("140.82.112.0/20", "UDP", None), ("::/0", "TCP", 443)]
    ipv4_excepts = ip_rules[0].to[0].ip_block._except
    assert "10.0.0.0/8" in ipv4_excepts and "169.254.0.0/16" in ipv4_excepts and "224.0.0.0/4" in ipv4_excepts
    assert all(":" not in value for value in ipv4_excepts)
    assert ip_rules[1].to[0].ip_block._except is None
    assert "fe80::/10" in ip_rules[2].to[0].ip_block._except and all(":" in value for value in ip_rules[2].to[0].ip_block._except)
    assert provisioner_module._resource_spec_digest(rendered) != provisioner_module._resource_spec_digest(deny_all)
    without_dns = provisioner_module._build_accepted_network_policy("sandbox-1", accepted_attempt_owner=owner, egress_allowance=provisioner_module.EgressAllowanceV1.model_validate(_allowance(dns=False).to_json()))
    assert len(without_dns.spec.egress) == 3


def _projection_evidence() -> SkillProjectionEvidence:
    content = b"# accepted\n"
    header = json.dumps(["public", "demo", "SKILL.md", "regular"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    tree = hashlib.sha256()
    tree.update(len(header).to_bytes(4, "big"))
    tree.update(header)
    tree.update(len(content).to_bytes(8, "big"))
    tree.update(content)
    projection = SkillSnapshotProjection(name="demo", category="public", relative_path="demo", manifest_digest=hashlib.sha256(content).hexdigest(), content_digest=tree.hexdigest(), file_count=1, total_bytes=len(content))
    snapshot_id = hashlib.sha256(json.dumps({"version": 1, "skills": [projection.to_json()]}, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()
    return SkillProjectionEvidence(snapshot_id=snapshot_id, content_digest=snapshot_id, projections=(projection,), file_count=1, total_bytes=len(content))


def _projection(provisioner_module):
    evidence = _projection_evidence()
    return provisioner_module.AcceptedSkillProjectionV2(
        profile="rwx_verified_copy_v2",
        snapshot_id=evidence.snapshot_id,
        content_digest=evidence.content_digest,
        run_id="run-1",
        generation=7,
        projections=[item.to_json() for item in evidence.projections],
        file_count=evidence.file_count,
        total_bytes=evidence.total_bytes,
    ), evidence


def test_attempt_lease_identity_includes_the_allowance(provisioner_module) -> None:
    projection, _evidence = _projection(provisioner_module)
    allowance = provisioner_module.EgressAllowanceV1.model_validate(_allowance().to_json())
    other = provisioner_module.EgressAllowanceV1.model_validate(EgressAllowanceV1.deny_all().to_json())
    lease = provisioner_module._build_accepted_attempt_lease("sandbox-1", projection, "A" * 43, egress_allowance=allowance)
    annotations = lease.metadata.annotations
    assert annotations["hartmesh.io/accepted-egress-allowance-digest"] == allowance.digest
    assert json.loads(annotations["hartmesh.io/accepted-egress-allowance"]) == _allowance().to_json()
    assert provisioner_module._lease_matches_attempt(lease, projection, "A" * 43, egress_allowance=allowance)
    assert not provisioner_module._lease_matches_attempt(lease, projection, "A" * 43, egress_allowance=other)
    assert not provisioner_module._lease_matches_attempt(lease, projection, "A" * 43)
    legacy = provisioner_module._build_accepted_attempt_lease("sandbox-1", projection, "A" * 43)
    assert "hartmesh.io/accepted-egress-allowance-digest" not in legacy.metadata.annotations
    assert provisioner_module._lease_matches_attempt(legacy, projection, "A" * 43)
    assert not provisioner_module._lease_matches_attempt(legacy, projection, "A" * 43, egress_allowance=allowance)

    created: list[object] = []

    class Coordination:
        def create_namespaced_lease(self, _namespace: str, candidate):
            if created:
                raise provisioner_module.ApiException(status=409)
            candidate.metadata.uid = "lease-uid-1"
            created.append(candidate)
            return candidate

        def read_namespaced_lease(self, _name: str, _namespace: str):
            return created[0]

    provisioner_module.coordination_v1 = Coordination()
    now = datetime(2026, 9, 5, tzinfo=UTC)
    provisioner_module._claim_accepted_attempt("sandbox-1", projection, "A" * 43, now=now, egress_allowance=allowance)
    replayed = provisioner_module._claim_accepted_attempt("sandbox-1", projection, "A" * 43, now=now, egress_allowance=allowance)
    assert replayed.metadata.uid == "lease-uid-1"
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._claim_accepted_attempt("sandbox-1", projection, "A" * 43, now=now, egress_allowance=other)
    assert rejected.value.detail == "accepted_attempt_identity_conflict"


def _ready_attempt(provisioner_module, allowance):
    projection, evidence = _projection(provisioner_module)
    capability = "A" * 43
    provisioner_module.ACCEPTED_SKILL_PROJECTION_PROFILE = "rwx_verified_copy_v2"
    provisioner_module.USERDATA_PVC_NAME = "shared-rwx"
    provisioner_module.ACCEPTED_SKILL_RUNTIME_IMAGE = "registry.example/provisioner@sha256:" + ("a" * 64)
    provisioner_module.SANDBOX_IMAGE = "registry.example/aio@sha256:" + ("b" * 64)
    lease = provisioner_module._build_accepted_attempt_lease("sandbox-1", projection, capability, egress_allowance=allowance)
    lease.metadata.uid = "lease-uid-1"
    owner = provisioner_module._accepted_attempt_owner_reference(lease)
    pod = provisioner_module._build_pod("sandbox-1", "thread-1", user_id="owner-1", accepted_skill_projection=projection, attempt_capability=capability, accepted_attempt_owner=owner, egress_allowance=allowance)
    lease.metadata.annotations["hartmesh.io/accepted-isolation-digest"] = pod.metadata.annotations["hartmesh.io/accepted-isolation-digest"]
    lease.metadata.annotations["hartmesh.io/accepted-attempt-state"] = "pod_creation_started"
    lease.metadata.annotations["hartmesh.io/accepted-pod-uid"] = "pod-uid-1"
    pod.metadata.uid = "pod-uid-1"
    image = "containerd://registry.example/provisioner@sha256:" + ("a" * 64)
    pod.status = SimpleNamespace(
        phase="Running",
        pod_ip="10.0.0.8",
        container_statuses=[SimpleNamespace(name="sandbox", image_id="containerd://registry.example/aio@sha256:" + ("b" * 64), ready=True), SimpleNamespace(name="accepted-skill-gate", image_id=image, ready=True)],
        init_container_statuses=[SimpleNamespace(name="accepted-skill-verifier", image_id=image, state=SimpleNamespace(terminated=SimpleNamespace(exit_code=0)))],
    )
    secrets: dict[str, object] = {}
    policies: dict[str, object] = {}

    class Core:
        def create_namespaced_secret(self, _namespace: str, secret):
            secret.metadata.uid = f"{secret.metadata.name}-uid"
            secrets[secret.metadata.name] = secret
            return secret

        def read_namespaced_secret(self, name: str, _namespace: str):
            if name not in secrets:
                raise provisioner_module.ApiException(status=404)
            return secrets[name]

    class Networking:
        def create_namespaced_network_policy(self, _namespace: str, policy):
            policy.metadata.uid = f"{policy.metadata.name}-uid"
            policies[policy.metadata.name] = policy
            return policy

        def read_namespaced_network_policy(self, name: str, _namespace: str):
            if name not in policies:
                raise provisioner_module.ApiException(status=404)
            return policies[name]

    class Coordination:
        def replace_namespaced_lease(self, _name: str, _namespace: str, candidate):
            return candidate

    provisioner_module.core_v1 = Core()
    provisioner_module.networking_v1 = Networking()
    provisioner_module.coordination_v1 = Coordination()
    provisioner_module._create_accepted_secrets("sandbox-1", projection, capability, accepted_attempt_owner=owner)
    provisioner_module._create_accepted_network_policy_exact("sandbox-1", accepted_attempt_owner=owner, egress_allowance=allowance)
    verifier_receipt = {"version": 2, "profile": "rwx_verified_copy_v2", "snapshot_id": evidence.snapshot_id, "content_digest": evidence.content_digest, "file_count": evidence.file_count, "total_bytes": evidence.total_bytes}
    return projection, capability, lease, pod, verifier_receipt, policies


def test_accepted_pod_response_attests_the_rendered_allowance(provisioner_module) -> None:
    allowance = provisioner_module.EgressAllowanceV1.model_validate(_allowance().to_json())
    projection, capability, lease, pod, verifier_receipt, policies = _ready_attempt(provisioner_module, allowance)
    assert pod.metadata.annotations["hartmesh.io/accepted-egress-allowance-digest"] == allowance.digest
    policy = policies["sandbox-sandbox-1-accepted-gate"]
    assert policy.spec.policy_types == ["Ingress", "Egress"] and len(policy.spec.egress) == 4

    response = provisioner_module._accepted_pod_response(
        "sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt, expected_egress_allowance_digest=allowance.digest
    )
    assert response is not None
    assert response.egress_allowance_digest == allowance.digest
    receipt = response.accepted_skill_material
    assert receipt is not None
    assert receipt["network_policy_spec_digest"] == provisioner_module._resource_spec_digest(policy)

    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response(
            "sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt, expected_egress_allowance_digest="0" * 64
        )
    assert rejected.value.status_code == 409 and rejected.value.detail == "accepted_attempt_egress_allowance_conflict"

    # A policy rendered from a different allowance never passes the fence.
    other = provisioner_module.EgressAllowanceV1.model_validate(EgressAllowanceV1.deny_all().to_json())
    policies["sandbox-sandbox-1-accepted-gate"] = provisioner_module._build_accepted_network_policy("sandbox-1", accepted_attempt_owner=provisioner_module._accepted_attempt_owner_reference(lease), egress_allowance=other)
    policies["sandbox-sandbox-1-accepted-gate"].metadata.uid = policy.metadata.uid
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response("sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt)
    assert rejected.value.detail == "accepted_attempt_network_policy_conflict"
    policies["sandbox-sandbox-1-accepted-gate"] = policy

    # The Lease is the authority for what was admitted; a Pod that disagrees is a conflict.
    pod.metadata.annotations["hartmesh.io/accepted-egress-allowance-digest"] = other.digest
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response("sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt)
    assert rejected.value.detail == "accepted_attempt_egress_allowance_conflict"
    pod.metadata.annotations["hartmesh.io/accepted-egress-allowance-digest"] = allowance.digest

    lease.metadata.annotations["hartmesh.io/accepted-egress-allowance"] = "not json"
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response("sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt)
    assert rejected.value.detail == "accepted_attempt_egress_allowance_invalid"


def test_accepted_pod_response_without_an_allowance_keeps_the_legacy_policy(provisioner_module) -> None:
    projection, capability, lease, pod, verifier_receipt, policies = _ready_attempt(provisioner_module, None)
    assert "hartmesh.io/accepted-egress-allowance-digest" not in pod.metadata.annotations
    assert policies["sandbox-sandbox-1-accepted-gate"].spec.policy_types == ["Ingress"]
    response = provisioner_module._accepted_pod_response("sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt)
    assert response is not None and response.egress_allowance_digest is None
    with pytest.raises(provisioner_module.HTTPException) as rejected:
        provisioner_module._accepted_pod_response(
            "sandbox-1", expected=projection, expected_capability=capability, expected_lease_uid="lease-uid-1", attempt_lease=lease, pod=pod, verifier_receipt=verifier_receipt, expected_egress_allowance_digest=_allowance().digest
        )
    assert rejected.value.detail == "accepted_attempt_egress_allowance_conflict"

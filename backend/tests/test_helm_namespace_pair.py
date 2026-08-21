"""Helm contracts for release and sandbox namespace separation."""

from __future__ import annotations

from typing import Any

import pytest
from support.helm import container_env, find_rendered_object, render_chart


def test_split_namespace_routes_sandboxes_without_moving_gateway_identity() -> None:
    documents = render_chart(
        "--set",
        "sandboxNamespace=acme-sbx",
        namespace="acme",
    )

    provisioner = find_rendered_object(documents, "Deployment", component="provisioner")
    environment = container_env(provisioner)
    role = find_rendered_object(documents, "Role", component="provisioner")
    role_binding = find_rendered_object(
        documents,
        "RoleBinding",
        component="provisioner",
    )

    assert provisioner["metadata"]["namespace"] == "acme"
    assert environment["K8S_NAMESPACE"] == "acme-sbx"
    assert environment["PROVISIONER_GATEWAY_NAMESPACE"] == "acme"
    assert role["metadata"]["namespace"] == "acme-sbx"
    assert role_binding["metadata"]["namespace"] == "acme-sbx"
    assert role_binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "deer-flow-deer-flow-provisioner",
            "namespace": "acme",
        }
    ]
    assert all(document.get("metadata", {}).get("namespace") != "deer-flow" for document in documents)


def test_provisioner_rbac_matches_audited_api_calls() -> None:
    documents = render_chart(
        "--set",
        "sandboxNamespace=acme-sbx",
        namespace="acme",
    )

    role = find_rendered_object(documents, "Role", component="provisioner")
    cluster_role = find_rendered_object(
        documents,
        "ClusterRole",
        component="provisioner",
    )

    assert cluster_role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["namespaces"],
            "resourceNames": ["acme-sbx"],
            "verbs": ["get"],
        },
        {
            "apiGroups": ["authentication.k8s.io"],
            "resources": ["tokenreviews"],
            "verbs": ["create"],
        },
    ]
    assert role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get", "create", "delete"],
        },
        {
            "apiGroups": [""],
            "resources": ["services"],
            "verbs": ["get", "list", "create", "delete"],
        },
        {
            "apiGroups": [""],
            "resources": ["secrets"],
            "verbs": ["get", "create", "delete"],
        },
        {
            "apiGroups": [""],
            "resources": ["persistentvolumeclaims"],
            "verbs": ["get"],
        },
        {
            "apiGroups": ["networking.k8s.io"],
            "resources": ["networkpolicies"],
            "verbs": ["get", "create", "delete"],
        },
        {
            "apiGroups": ["coordination.k8s.io"],
            "resources": ["leases"],
            "verbs": ["get", "list", "create", "delete", "update"],
        },
    ]


def test_cluster_scoped_rbac_names_are_unique_per_release_namespace() -> None:
    acme_documents = render_chart(
        "--set",
        "sandboxNamespace=acme-sbx",
        namespace="acme",
    )
    beta_documents = render_chart(
        "--set",
        "sandboxNamespace=beta-sbx",
        namespace="beta",
    )

    acme_role = find_rendered_object(
        acme_documents,
        "ClusterRole",
        component="provisioner",
    )
    beta_role = find_rendered_object(
        beta_documents,
        "ClusterRole",
        component="provisioner",
    )
    acme_binding = find_rendered_object(
        acme_documents,
        "ClusterRoleBinding",
        component="provisioner",
    )
    beta_binding = find_rendered_object(
        beta_documents,
        "ClusterRoleBinding",
        component="provisioner",
    )

    assert acme_role["metadata"]["name"] != beta_role["metadata"]["name"]
    assert acme_binding["metadata"]["name"] != beta_binding["metadata"]["name"]
    assert acme_binding["roleRef"]["name"] == acme_role["metadata"]["name"]
    assert beta_binding["roleRef"]["name"] == beta_role["metadata"]["name"]


@pytest.mark.parametrize(
    ("extra_args", "expected_namespace"),
    [
        ((), "foo"),
        (("--set", "namespace=explicit"), "explicit"),
    ],
    ids=["helm-release-namespace", "explicit-override"],
)
def test_release_namespace_resolution(
    extra_args: tuple[str, ...],
    expected_namespace: str,
) -> None:
    documents = render_chart(*extra_args, namespace="foo")

    rendered_namespaces = {document.get("metadata", {}).get("namespace") for document in documents if document.get("metadata", {}).get("namespace") is not None}
    provisioner = find_rendered_object(
        documents,
        "Deployment",
        component="provisioner",
    )
    environment = container_env(provisioner)

    assert rendered_namespaces == {expected_namespace}
    assert environment["K8S_NAMESPACE"] == expected_namespace
    assert environment["PROVISIONER_GATEWAY_NAMESPACE"] == expected_namespace


def test_release_namespace_default_preserves_rendered_objects_except_qualified_rbac_names() -> None:
    release_namespace_documents = render_chart(namespace="foo")
    legacy_namespace_documents = render_chart(
        "--set",
        "namespace=deer-flow",
        namespace="foo",
    )

    def identities(documents: list[dict[str, Any]]) -> set[tuple[str, str]]:
        return {
            (
                document["kind"],
                ("<namespace-qualified-provisioner-rbac>" if document["kind"] in {"ClusterRole", "ClusterRoleBinding"} else document["metadata"]["name"]),
            )
            for document in documents
        }

    assert identities(release_namespace_documents) == identities(
        legacy_namespace_documents,
    )


def test_home_existing_claim_drives_gateway_and_provisioner_without_creating_pvc() -> None:
    documents = render_chart(
        "--set",
        "persistence.home.existingClaim=acme-home",
        namespace="acme",
    )

    gateway = find_rendered_object(documents, "Deployment", component="gateway")
    provisioner = find_rendered_object(
        documents,
        "Deployment",
        component="provisioner",
    )
    home_volume = next(volume for volume in gateway["spec"]["template"]["spec"]["volumes"] if volume["name"] == "home")

    assert not any(document.get("kind") == "PersistentVolumeClaim" for document in documents)
    assert home_volume["persistentVolumeClaim"]["claimName"] == "acme-home"
    assert container_env(provisioner)["USERDATA_PVC_NAME"] == "acme-home"

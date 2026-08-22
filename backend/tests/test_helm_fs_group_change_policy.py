"""Helm contracts for avoiding recursive persistent-volume ownership walks."""

from __future__ import annotations

from typing import Any

from support.helm import find_rendered_object, render_pvc_chart


def _pod_spec(workload: dict[str, Any]) -> dict[str, Any]:
    return workload["spec"]["template"]["spec"]


def _mounts_persistent_volume(workload: dict[str, Any]) -> bool:
    pod_spec = _pod_spec(workload)
    return bool(workload["spec"].get("volumeClaimTemplates")) or any("persistentVolumeClaim" in volume for volume in pod_spec.get("volumes", []))


def test_every_pvc_workload_with_fs_group_avoids_repeat_recursive_chown() -> None:
    documents = render_pvc_chart(
        "--set",
        "postgresql.enabled=true",
        "--set",
        "redis.enabled=true",
    )
    workloads = [document for document in documents if document.get("kind") in {"Deployment", "StatefulSet"}]
    persistent_workloads = {workload["metadata"]["labels"]["app.kubernetes.io/component"]: workload for workload in workloads if _mounts_persistent_volume(workload) and "fsGroup" in _pod_spec(workload).get("securityContext", {})}

    assert set(persistent_workloads) == {"gateway", "postgres", "redis"}
    for component, workload in persistent_workloads.items():
        assert _pod_spec(workload)["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch", component


def test_gateway_and_frontend_use_uniform_fs_group_change_policy() -> None:
    documents = render_pvc_chart()

    for component in ("gateway", "frontend"):
        workload = find_rendered_object(documents, "Deployment", component=component)
        assert _pod_spec(workload)["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch"

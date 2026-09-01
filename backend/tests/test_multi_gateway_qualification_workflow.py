"""Static contract for the exact-profile live qualification workflow."""

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW = _ROOT / ".github/workflows/multi-gateway-qualification.yml"
_SCOPE = "durable_two_gateway_v1_postgres_redis_aio_rwx"


def test_multi_gateway_workflow_pins_exact_inputs_and_only_live_entrypoint() -> None:
    document = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    dispatch = document[True]["workflow_dispatch"]
    inputs = dispatch["inputs"]
    required = {
        "qualification_subjects_json",
        "kube_context",
        "namespace",
        "qualification_id",
    }
    assert required <= set(inputs)
    assert all(inputs[name]["required"] is True for name in required)
    assert len(inputs) <= 10

    job = document["jobs"]["qualify-exact-two-gateway-profile"]
    assert job["env"]["DEERFLOW_TEST_KUBERNETES_SCOPE"] == _SCOPE
    assert job["env"]["DEERFLOW_TEST_KUBERNETES_RUNTIME"] == "1"
    assert job["env"]["DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION"] == "1"
    commands = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "tests/kubernetes/test_multi_gateway_qualification.py" in commands
    assert "pytest -m kubernetes_contract" in commands
    assert "incompatible Gateway digest must differ" in commands
    for environment_name in (
        "DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST",
        "DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_DIGEST",
        "DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_DIGEST",
        "DEERFLOW_TEST_FRONTEND_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_FRONTEND_IMAGE_DIGEST",
        "DEERFLOW_TEST_NGINX_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_NGINX_IMAGE_DIGEST",
        "DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST",
        "DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST",
        "DEERFLOW_TEST_POSTGRES_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_POSTGRES_IMAGE_DIGEST",
        "DEERFLOW_TEST_REDIS_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_REDIS_IMAGE_DIGEST",
        "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS",
        "DEERFLOW_TEST_EXTENSION_ARTIFACT_DIGEST",
        "DEERFLOW_TEST_EXTENSION_CONFIGURATION_DIGEST",
        "DEERFLOW_TEST_CAPABILITY_MANIFEST_DIGEST",
        "DEERFLOW_TEST_DATABASE_SCHEMA_REF",
    ):
        assert environment_name in commands
    upload = next(step for step in job["steps"] if step.get("uses") == "actions/upload-artifact@v4.6.2")
    assert upload["if"] == "always()"
    assert "multi-gateway-qualification.json" in upload["with"]["path"]
    assert "failure-artifacts" in upload["with"]["path"]

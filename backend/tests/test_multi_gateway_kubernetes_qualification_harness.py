"""Offline contracts for the exact two-Gateway Kubernetes harness."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import re
import urllib.error
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from support.kubernetes_qualification import (
    QualificationCommandError,
    QualificationPrerequisiteError,
    validate_kubernetes_prerequisites,
)
from support.multi_gateway_qualification import (
    KubernetesMultiGatewayQualificationConfigV1,
    KubernetesMultiGatewayQualificationDriverV1,
    KubernetesMultiGatewayQualificationRunnerV1,
)

from deerflow.deployment.topology import (
    MULTI_GATEWAY_PROFILE,
    MULTI_GATEWAY_QUALIFICATION_SCOPE,
    ReplicaRegistrationV1,
    TopologyFingerprintV1,
)
from deerflow.multi_gateway_qualification import (
    MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
    MultiGatewayQualificationSubjectsV1,
)
from deerflow.persistence.bootstrap import get_expected_migration_head
from deerflow.runtime import kubernetes_qualification
from deerflow.runtime.runs import worker as run_worker
from deerflow.runtime.tenant_identity import TenantIdentityV1, TenantSubsystem


def _load_multi_gateway_live_entrypoint():
    path = Path(__file__).parent / "kubernetes" / "test_multi_gateway_qualification.py"
    spec = importlib.util.spec_from_file_location(
        "multi_gateway_live_entrypoint_under_test",
        path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _environment(tmp_path: Path) -> dict[str, str]:
    def digest(character: str) -> str:
        return "sha256:" + (character * 64)

    return {
        "DEERFLOW_TEST_KUBERNETES": "1",
        "DEERFLOW_TEST_KUBERNETES_SCOPE": MULTI_GATEWAY_QUALIFICATION_SCOPE,
        "KUBECONFIG": str((tmp_path / "kubeconfig").resolve()),
        "DEERFLOW_TEST_KUBERNETES_CONTEXT": "qualification-context",
        "DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT": "qualification-context",
        "DEERFLOW_TEST_KUBERNETES_NAMESPACE": ("hartmesh-qualification-two-gateway"),
        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID": "qualification-09",
        "DEERFLOW_TEST_KUBERNETES_EVIDENCE": str((tmp_path / "evidence.json").resolve()),
        "DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY": "registry.example/gateway",
        "DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST": digest("a"),
        "DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_REPOSITORY": ("registry.example/gateway-predecessor"),
        "DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_DIGEST": digest("8"),
        "DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_REPOSITORY": ("registry.example/gateway-incompatible"),
        "DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_DIGEST": digest("9"),
        "DEERFLOW_TEST_FRONTEND_IMAGE_REPOSITORY": "registry.example/frontend",
        "DEERFLOW_TEST_FRONTEND_IMAGE_DIGEST": digest("b"),
        "DEERFLOW_TEST_NGINX_IMAGE_REPOSITORY": "registry.example/nginx",
        "DEERFLOW_TEST_NGINX_IMAGE_DIGEST": digest("c"),
        "DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY": ("registry.example/provisioner"),
        "DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST": digest("d"),
        "DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY": "registry.example/sandbox",
        "DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST": digest("e"),
        "DEERFLOW_TEST_POSTGRES_IMAGE_REPOSITORY": "registry.example/postgres",
        "DEERFLOW_TEST_POSTGRES_IMAGE_DIGEST": digest("1"),
        "DEERFLOW_TEST_REDIS_IMAGE_REPOSITORY": "registry.example/redis",
        "DEERFLOW_TEST_REDIS_IMAGE_DIGEST": digest("2"),
        "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS": "rwx-storage",
        "DEERFLOW_TEST_EXTENSION_ARTIFACT_DIGEST": digest("3"),
        "DEERFLOW_TEST_EXTENSION_CONFIGURATION_DIGEST": digest("4"),
        "DEERFLOW_TEST_CAPABILITY_MANIFEST_DIGEST": digest("5"),
        "DEERFLOW_TEST_DATABASE_SCHEMA_REF": "schema:" + digest("6"),
    }


@pytest.mark.parametrize(
    ("scope", "error"),
    [
        (None, "exact qualification scope"),
        ("unsupported-qualification-scope", "unsupported scope"),
    ],
    ids=["missing", "wrong"],
)
def test_exact_two_live_entrypoint_fails_closed_without_its_exact_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    scope: str | None,
    error: str,
) -> None:
    entrypoint = _load_multi_gateway_live_entrypoint()
    tools = tmp_path / "bin"
    tools.mkdir()
    for name in ("helm", "kubectl"):
        executable = tools / name
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o755)
    environment = _environment(tmp_path)
    environment["PATH"] = str(tools)
    if scope is None:
        environment.pop("DEERFLOW_TEST_KUBERNETES_SCOPE")
    else:
        environment["DEERFLOW_TEST_KUBERNETES_SCOPE"] = scope
    monkeypatch.setattr(entrypoint.os, "environ", environment)

    with pytest.raises(QualificationPrerequisiteError, match=error):
        entrypoint.test_exact_two_gateway_kubernetes_qualification()


def test_multi_gateway_prerequisites_require_every_exact_subject(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path)
    environment.pop("DEERFLOW_TEST_NGINX_IMAGE_DIGEST")
    environment.pop("DEERFLOW_TEST_DATABASE_SCHEMA_REF")

    with pytest.raises(QualificationPrerequisiteError) as captured:
        validate_kubernetes_prerequisites(
            environment,
            executable_lookup=lambda name: f"/usr/bin/{name}",
        )

    assert "DEERFLOW_TEST_NGINX_IMAGE_DIGEST" in str(captured.value)
    assert "DEERFLOW_TEST_DATABASE_SCHEMA_REF" in str(captured.value)


def test_multi_gateway_requires_distinct_predecessor_and_negative_control(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path)
    environment["DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_DIGEST"] = environment["DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST"]

    with pytest.raises(
        ValueError,
        match="predecessor Gateway image digest must differ",
    ):
        KubernetesMultiGatewayQualificationConfigV1.from_environment(environment)

    environment = _environment(tmp_path)
    environment["DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_DIGEST"] = environment["DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_DIGEST"]
    with pytest.raises(
        ValueError,
        match="predecessor and incompatible Gateway digests must differ",
    ):
        KubernetesMultiGatewayQualificationConfigV1.from_environment(environment)


def test_multi_gateway_live_values_are_candidate_only_and_exact(
    tmp_path: Path,
) -> None:
    config = KubernetesMultiGatewayQualificationConfigV1.from_environment(_environment(tmp_path))
    driver = KubernetesMultiGatewayQualificationDriverV1(config)

    values = driver.values()
    app_config = yaml.safe_load(values["config"])
    extensions = json.loads(values["extensionsConfig"])

    assert values["namespace"] == "hartmesh-qualification-two-gateway"
    assert values["deployment"]["mode"] == "durable_two_gateway_v1"
    assert values["deployment"]["qualificationEvidence"] == []
    assert values["deployment"]["qualificationCandidate"] == {
        "enabled": True,
        "id": "qualification-09",
    }
    assert values["gateway"]["replicas"] == 2
    assert values["postgresql"] == {
        "enabled": False,
        "image": {
            "repository": "registry.example/postgres",
            "digest": "sha256:" + ("1" * 64),
        },
        "external": {"existingSecret": driver.store_secret},
    }
    assert values["redis"] == {
        "enabled": False,
        "image": {
            "repository": "registry.example/redis",
            "digest": "sha256:" + ("2" * 64),
        },
        "external": {"existingSecret": driver.store_secret},
    }
    assert app_config["deployment"]["profile"] == ("durable_two_gateway_v1")
    assert app_config["scheduler"] == {
        "enabled": True,
        "multi_instance": True,
        "poll_interval_seconds": 1,
        "lease_seconds": 10,
        "max_concurrent_runs": 2,
        "queue_timeout_seconds": 60,
        "min_once_delay_seconds": 1,
        "recursion_limit": 1000,
    }
    assert app_config["plugins"] == [
        {
            "name": "governance",
            "package": "hartmesh-governance-extension",
            "use": "hartmesh_governance_extension:install",
            "enabled": True,
            "required": True,
            "config": {
                "audit_adapter": "stateless_qualification",
                "fail_closed_startup": True,
            },
        },
    ]
    assert app_config["required_capabilities"] == [
        "invocation_constraints.v2",
        "mcp_interceptor:hartmesh.governance.mcp",
    ]
    toolset = extensions["mcpServers"]["qualification-tasks"]["task_toolsets"][0]
    assert toolset == {
        "name": "qualification",
        "submit_tool": "submit_task",
        "status_tool": "task_status",
        "cancel_tool": "cancel_task",
    }


def test_live_candidate_uses_a_disposable_secret_backed_replay_keyring(
    tmp_path: Path,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )

    manifest = driver.replay_keyring_secret_manifest()
    string_data = manifest["stringData"]
    keys = json.loads(string_data["MCP_TASK_REPLAY_HMAC_KEYS"])
    active_key_id = string_data["MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID"]
    policy_keys = json.loads(string_data["EXECUTION_POLICY_HMAC_KEYS"])
    policy_active_key_id = string_data["EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID"]
    rendered_values = json.dumps(driver.values(), sort_keys=True)

    assert manifest["kind"] == "Secret"
    assert manifest["metadata"] == {"name": driver.replay_keyring_secret}
    assert set(string_data) == {
        "MCP_TASK_REPLAY_HMAC_KEYS",
        "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID",
        "EXECUTION_POLICY_HMAC_KEYS",
        "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID",
    }
    assert len(keys) == 2
    assert active_key_id in keys
    assert len(policy_keys) == 1
    assert policy_active_key_id in policy_keys
    assert driver.values()["gateway"]["extraEnvFrom"] == [
        {"configMapRef": {"name": driver.runtime_config_map}},
        {"secretRef": {"name": driver.replay_keyring_secret}},
    ]
    assert all(encoded_key not in rendered_values for encoded_key in keys.values())
    assert all(encoded_key not in rendered_values for encoded_key in policy_keys.values())
    assert re.fullmatch(
        r"sha256:[0-9a-f]{64}",
        driver.replay_keyring_confirmation.digest,
    )
    assert re.fullmatch(
        r"sha256:[0-9a-f]{64}",
        driver.execution_policy_keyring_confirmation.digest,
    )


def test_live_driver_has_one_named_handler_for_every_required_scenario(
    tmp_path: Path,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(KubernetesMultiGatewayQualificationConfigV1.from_environment(_environment(tmp_path)))

    assert tuple(driver.scenario_handlers) == MULTI_GATEWAY_QUALIFICATION_SCENARIOS
    assert len(set(driver.scenario_handlers.values())) == len(MULTI_GATEWAY_QUALIFICATION_SCENARIOS)
    assert not hasattr(driver, "_not_started")


def test_owner_sigkill_dispatch_gap_is_a_selected_fail_closed_window() -> None:
    assert kubernetes_qualification._point_matches(
        "post_dispatch_marker_before_graph",
        "owner_sigkill",
    )

    source = inspect.getsource(
        KubernetesMultiGatewayQualificationDriverV1._owner_sigkill,
    )
    assert '"post_dispatch_marker_before_graph"' in source
    assert "expected_takeover_resumes=0" in source
    assert "expected_graph_starts=0" in source
    assert "expected_model_starts=0" in source
    assert '"fail_closed_window_count": 1' in source
    assert '"fail_closed_stop_reason"' in source
    assert '"fail_closed_takeover_resumes"' in source
    assert '"fail_closed_graph_starts"' in source
    assert '"fail_closed_model_starts"' in source
    assert '"recovered_window_count": len(observations) - 1' in source


def test_dispatch_gap_barrier_is_after_marker_and_before_execution() -> None:
    source = inspect.getsource(run_worker.run_agent)

    marker = source.index("RUN_EXECUTION_STARTED_EVENT.event_type")
    barrier = source.index('"post_dispatch_marker_before_graph"')
    takeover_resume = source.index('"execution_takeover_resumes"')
    graph_start = source.index('"graph_starts"')
    graph_execution = source.index("agent.astream(", graph_start)

    assert marker < barrier < takeover_resume < graph_start < graph_execution


def test_unsupported_live_scenario_includes_two_startup_gates() -> None:
    source = inspect.getsource(
        KubernetesMultiGatewayQualificationDriverV1._unsupported_surfaces,
    )

    assert "github_webhook_ingress" in source
    assert "qualification_candidate_missing" in source
    assert "topology_channel_not_replica_safe" in source
    assert "topology_qualification_missing" in source


def test_live_runner_uses_atomic_passing_and_failure_artifact_paths(
    tmp_path: Path,
) -> None:
    config = KubernetesMultiGatewayQualificationConfigV1.from_environment(_environment(tmp_path))
    runner = KubernetesMultiGatewayQualificationRunnerV1(config)

    assert runner.passing_path == tmp_path / "evidence.json.passing"
    assert runner.failure_artifacts_path == (tmp_path / "evidence.failure-artifacts")


def test_qualification_mcp_manifest_uses_the_exact_gateway_image(
    tmp_path: Path,
) -> None:
    config = KubernetesMultiGatewayQualificationConfigV1.from_environment(_environment(tmp_path))
    driver = KubernetesMultiGatewayQualificationDriverV1(config)

    deployment, service = driver.qualification_mcp_manifests()
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    assert container["image"] == ("registry.example/gateway@" + ("sha256:" + ("a" * 64)))
    assert container["command"] == ["sh", "-c"]
    assert "python -m deerflow.qualification_mcp_server" in container["args"][0]
    assert service["spec"]["ports"][0]["port"] == 8090


def test_live_epoch_probe_uses_the_authoritative_run_primary_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )
    queries: list[str] = []

    def postgres(query: str) -> str:
        queries.append(query)
        return "7"

    monkeypatch.setattr(driver, "_postgres", postgres)

    assert driver._run_state_version("run-1") == 7
    assert queries == [
        "SELECT state_version FROM runs WHERE run_id='run-1'",
    ]


def test_maintenance_gate_counts_queued_and_input_required_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )
    queries: list[str] = []

    def postgres(query: str) -> str:
        queries.append(query)
        return "0"

    monkeypatch.setattr(driver, "_postgres", postgres)

    assert driver._active_ownership_counts() == (0, 0, 0)
    assert "'queued'" in queries[1]
    assert "'input_required'" in queries[2]
    assert "notification_status" in queries[2]


def test_redis_outage_counter_requires_observed_retryable_sse_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("support.multi_gateway_qualification")
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )

    monkeypatch.setattr(
        driver,
        "_gateway_pods",
        lambda **_kwargs: (
            {"metadata": {"name": "gateway-0"}},
            {"metadata": {"name": "gateway-1"}},
        ),
    )

    class Forward:
        def __init__(self, *_args, **_kwargs) -> None:
            self.port = 8001

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

    class Session:
        def __init__(self, _base_url: str) -> None:
            self.base_url = _base_url
            self._opener = self

        def open(self, request, *, timeout: float):
            assert request.full_url.endswith("/api/threads/thread-1/runs/run-1/stream")
            assert request.get_header("Last-event-id") == "1-0"
            assert timeout == 10
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                {"Retry-After": "1"},
                None,
            )

    monkeypatch.setattr(module, "_PortForward", Forward)
    monkeypatch.setattr(module, "_RuntimeHttpSession", Session)

    monkeypatch.setattr(driver, "_login", lambda _client: None)

    assert driver._retryable_sse_transport_failure(
        thread_id="thread-1",
        run_id="run-1",
        last_event_id="1-0",
    ) == (503, 503)


def test_cross_release_provisioner_probe_uses_bearer_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Response:
        status = 401

        @staticmethod
        def read() -> bytes:
            return b"Unauthorized"

    def open_request(request, *, timeout: float):
        observed["authorization"] = request.get_header("Authorization")
        observed["method"] = request.method
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", open_request)

    status = KubernetesMultiGatewayQualificationDriverV1._bearer_request_status(
        "http://127.0.0.1:8002",
        "/api/sandboxes/sandbox-1",
        token="projected-token",
        method="GET",
    )

    assert status == 401
    assert observed == {
        "authorization": "Bearer projected-token",
        "method": "GET",
        "timeout": 10,
    }


def test_gateway_signal_targets_and_verifies_uvicorn_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )
    commands: list[tuple[str, ...]] = []

    def kubectl(*arguments: str, **_kwargs) -> str:
        commands.append(arguments)
        script = arguments[-1]
        assert "/proc/{name}/cmdline" in script
        assert "app.gateway.app:create_app" in script
        assert "kill -STOP 1" not in script
        return "pid=42:state=T" if "SIGSTOP" in script else "pid=42:state=running"

    monkeypatch.setattr(driver, "_kubectl", kubectl)

    assert driver._signal_gateway_process("gateway-0", "STOP") == 42
    assert driver._signal_gateway_process("gateway-0", "CONT") == 42
    assert all(command[3:5] == ("python", "-c") for command in commands)


def test_postgres_fixture_uses_an_explicit_restricted_gateway_role(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )
    calls: list[tuple[str, bool]] = []

    def postgres(query: str, *, redact_diagnostics: bool = False) -> str:
        calls.append((query, redact_diagnostics))
        return ""

    monkeypatch.setattr(driver, "_postgres", postgres)

    driver._initialize_primary_database_role()

    assert len(calls) == 1
    query, redacted = calls[0]
    assert redacted is True
    assert "qualification_primary" in query
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION" in query
    assert "REVOKE CONNECT ON DATABASE deerflow FROM PUBLIC" in query
    assert "GRANT CONNECT,TEMPORARY ON DATABASE deerflow TO qualification_primary" in query


def test_postgres_partition_targets_only_one_gateway_and_excludes_postgres(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("support.multi_gateway_qualification")
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )
    manifests: list[dict[str, object]] = []
    commands: list[tuple[str, ...]] = []

    def kubectl(*arguments: str, **kwargs) -> str:
        commands.append(arguments)
        if arguments[:3] == ("apply", "-f", "-"):
            manifests.append(yaml.safe_load(kwargs["input_text"]))
        return ""

    monkeypatch.setattr(driver, "_kubectl", kubectl)
    monkeypatch.setattr(
        driver,
        "_gateway_pods",
        lambda **_kwargs: (
            {"metadata": {"name": "gateway-owner"}},
            {"metadata": {"name": "gateway-peer"}},
        ),
    )
    monkeypatch.setattr(
        driver,
        "_gateway_http_ready",
        lambda pod_name: pod_name == "gateway-peer",
    )

    def wait_once(predicate, **_kwargs) -> None:
        assert predicate() is True

    monkeypatch.setattr(module, "wait_until", wait_once)

    policy_name = driver._partition_gateway_from_postgres("gateway-owner")

    assert policy_name.startswith("mgq-pg-")
    assert len(manifests) == 1
    policy = manifests[0]
    assert policy["kind"] == "NetworkPolicy"
    assert policy["spec"]["podSelector"]["matchLabels"] == {
        "hartmesh.io/qualification-pg-partition": "true",
    }
    rendered_policy = yaml.safe_dump(policy)
    assert driver.redis_name in rendered_policy
    assert driver.postgres_name not in rendered_policy
    assert any(command[:3] == ("label", "pod", "gateway-owner") for command in commands)


def test_runner_never_deletes_a_namespace_it_did_not_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesMultiGatewayQualificationRunnerV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        )
    )
    called = False

    def unexpected(*_args, **_kwargs) -> str:
        nonlocal called
        called = True
        return ""

    monkeypatch.setattr(runner.driver, "_kubectl", unexpected)

    runner._delete_namespace()

    assert called is False


def test_runner_deletes_only_exact_uid_and_owner_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesMultiGatewayQualificationRunnerV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        )
    )
    runner.driver._owned_namespace_uid = "namespace-uid"
    calls: list[tuple[str, ...]] = []

    def kubectl(*arguments: str, **_kwargs) -> str:
        calls.append(arguments)
        if arguments[0] == "get":
            return json.dumps(
                {
                    "metadata": {
                        "uid": "namespace-uid",
                        "labels": {"hartmesh.io/qualification-owner": (runner.driver._namespace_owner)},
                    }
                }
            )
        return ""

    monkeypatch.setattr(runner.driver, "_kubectl", kubectl)

    runner._delete_namespace()

    assert [call[0] for call in calls] == ["get", "delete"]
    assert runner.driver._owned_namespace_uid is None


def test_redis_fixture_keeps_acl_secrets_out_of_pod_spec(
    tmp_path: Path,
) -> None:
    driver = KubernetesMultiGatewayQualificationDriverV1(
        KubernetesMultiGatewayQualificationConfigV1.from_environment(
            _environment(tmp_path),
        ),
    )
    manifests = driver._store_manifests()
    deployment = next(item for item in manifests if item.get("kind") == "Deployment" and item.get("metadata", {}).get("name") == driver.redis_name)
    pod_spec = json.dumps(deployment, sort_keys=True)
    configuration = driver._redis_configuration()

    assert driver._redis_password not in pod_spec
    assert driver._redis_admin_password not in pod_spec
    assert "nopass" not in configuration
    assert "user default off" in configuration
    assert "/etc/redis-secret/redis.conf" in pod_spec


def test_live_runner_verifier_expectation_is_independent_of_artifact_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubernetesMultiGatewayQualificationConfigV1.from_environment(
        _environment(tmp_path),
    )
    runner = KubernetesMultiGatewayQualificationRunnerV1(config)
    replay_keyring_confirmation = runner.driver.replay_keyring_confirmation
    execution_policy_keyring_confirmation = runner.driver.execution_policy_keyring_confirmation
    tenant = TenantIdentityV1.from_canonical_id("qualification")
    redis_namespace_digest = "sha256:" + tenant.namespace(TenantSubsystem.REDIS).digest
    fingerprint = TopologyFingerprintV1.create(
        profile=MULTI_GATEWAY_PROFILE,
        tenant_digest=tenant.digest,
        image_digests=config.image_digests,
        config_digest="sha256:" + ("7" * 64),
        database_schema_ref=config.database_schema_ref,
        redis_namespace_digest=redis_namespace_digest,
        extension_artifact_digest=config.extension_artifact_digest,
        extension_configuration_digest=config.extension_configuration_digest,
        capability_manifest_digest=(config.capability_manifest_digest.removeprefix("sha256:")),
        mcp_task_replay_keyring_confirmation_version=(replay_keyring_confirmation.version),
        mcp_task_replay_keyring_confirmation_digest=(replay_keyring_confirmation.digest),
        execution_policy_keyring_confirmation_version=(execution_policy_keyring_confirmation.version),
        execution_policy_keyring_confirmation_digest=(execution_policy_keyring_confirmation.digest),
        migration_head=get_expected_migration_head(),
        accepted_materialization_profile="rwx_verified_copy_v2",
    )
    now = datetime(2026, 9, 1, tzinfo=UTC)
    registrations = tuple(
        ReplicaRegistrationV1(
            replica_id=f"gateway-{index}",
            topology_fingerprint=fingerprint,
            started_at=now,
            heartbeat_at=now,
        )
        for index in range(2)
    )
    subjects = MultiGatewayQualificationSubjectsV1(
        git_revision="a" * 40,
        chart_version="2.1.0+hartmesh.4",
        chart_digest="sha256:" + ("8" * 64),
        image_digests=config.image_digests,
        configuration_digest=fingerprint.config_digest,
        migration_head=fingerprint.migration_head,
        tenant_public_ref=tenant.public_ref,
        tenant_digest=tenant.digest,
        namespace=config.namespace,
        kubernetes_refs={
            "gateway_service_uid": "service-uid",
            "gateway_pod_0_uid": "pod-0-uid",
            "gateway_pod_1_uid": "pod-1-uid",
            "provisioner_pod_uid": "provisioner-uid",
            "sandbox_pvc_uid": "pvc-uid",
        },
        database_schema_ref=config.database_schema_ref,
        redis_namespace_digest=redis_namespace_digest,
        redis_acl_proof_digest="sha256:" + ("9" * 64),
        extension_artifact_digest=config.extension_artifact_digest,
        extension_configuration_digest=config.extension_configuration_digest,
        capability_manifest_digest=config.capability_manifest_digest,
        topology_registrations=registrations,
    )
    monkeypatch.setattr(runner.driver, "_git_revision", lambda: subjects.git_revision)
    monkeypatch.setattr(
        runner.driver,
        "_chart_version",
        lambda: subjects.chart_version,
    )
    monkeypatch.setattr(
        runner.driver,
        "_chart_digest",
        lambda: subjects.chart_digest,
    )

    expected = runner._expectation(subjects)
    assert expected.image_digests == config.image_digests
    assert expected.topology_digest == fingerprint.digest
    assert expected.kubernetes_refs == subjects.kubernetes_refs
    assert expected.redis_acl_proof_digest == subjects.redis_acl_proof_digest

    tampered_subjects = replace(
        subjects,
        image_digests={
            **config.image_digests,
            "gateway": "sha256:" + ("f" * 64),
        },
    )
    with pytest.raises(
        QualificationCommandError,
        match="independent qualification inputs",
    ):
        runner._expectation(tampered_subjects)

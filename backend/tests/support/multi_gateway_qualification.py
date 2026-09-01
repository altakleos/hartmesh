"""Real-cluster driver for the exact two-Gateway qualification scope.

The driver is deliberately test-only.  It may render the profile as an
unqualified candidate solely in a disposable ``hartmesh-qualification-*``
namespace, runs every named scenario through a separate method, and publishes
no passing artifact until the package-level orchestrator has validated every
counter.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import os
import re
import secrets
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from deerflow.deployment.topology import (
    MULTI_GATEWAY_QUALIFICATION_SCOPE,
    ReplicaRegistrationV1,
    TopologyFingerprintV1,
)
from deerflow.multi_gateway_qualification import (
    MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
    KubernetesMultiGatewayQualificationEvidenceV1,
    MultiGatewayQualificationExpectationV1,
    MultiGatewayQualificationSubjectsV1,
    MultiGatewayScenarioObservationV1,
    run_multi_gateway_qualification,
    verify_multi_gateway_qualification_evidence,
)
from deerflow.persistence.bootstrap import get_expected_migration_head
from deerflow.qualification_evidence import qualification_evidence_digest
from deerflow.runtime.tenant_identity import (
    RedisTenantComponent,
    TenantIdentityV1,
    TenantSubsystem,
    redis_component_key_prefix,
)
from support.kubernetes_qualification import (
    KubernetesQualificationConfig,
    QualificationCommandError,
    QualificationPrerequisiteError,
    QualificationTimeout,
    _PortForward,
    _RuntimeHttpSession,
    run_bounded,
    validate_kubernetes_prerequisites,
    wait_until,
)

_IMAGE_REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,511}\Z")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SCHEMA_REF = re.compile(r"schema:sha256:[0-9a-f]{64}\Z")
_STORAGE_CLASS = re.compile(r"[a-z0-9](?:[-.a-z0-9]{0,251}[a-z0-9])?\Z")
_SAFE_RESOURCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_TERMINAL_RUN_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})
_TERMINAL_SCHEDULE_STATUSES = frozenset({"success", "failed", "skipped", "interrupted"})
_QUALIFICATION_SKILL_NAME = "qualification-skill"
_QUALIFICATION_SKILL_FILES = {
    "SKILL.md": (b"---\nname: qualification-skill\ndescription: Deterministic multi-Gateway accepted-skill fixture.\nallowed-tools:\n  - read_file\n---\nRead resources/proof.txt from the immutable accepted snapshot.\n"),
    "resources/proof.txt": b"hartmesh multi-gateway qualification v1\n",
}
_ACCEPTED_ATTEMPT_LEASE_SECONDS = 30
_QUALIFICATION_OWNER_LABEL = "hartmesh.io/qualification-owner"


def _required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value:
        raise QualificationPrerequisiteError(f"enabled multi-Gateway qualification is missing {name}")
    return value


def _image_subject(
    environment: Mapping[str, str],
    *,
    repository_name: str,
    digest_name: str,
) -> tuple[str, str]:
    repository = _required(environment, repository_name)
    digest = _required(environment, digest_name)
    if _IMAGE_REPOSITORY.fullmatch(repository) is None:
        raise QualificationPrerequisiteError(f"{repository_name} is not an OCI repository")
    if _IMAGE_DIGEST.fullmatch(digest) is None:
        raise QualificationPrerequisiteError(f"{digest_name} is not an immutable sha256 digest")
    return repository, digest


@dataclass(frozen=True, kw_only=True)
class KubernetesMultiGatewayQualificationConfigV1(KubernetesQualificationConfig):
    """Explicit immutable inputs for the exact live topology."""

    frontend_image_repository: str
    frontend_image_digest: str
    predecessor_gateway_image_repository: str
    predecessor_gateway_image_digest: str
    incompatible_gateway_image_repository: str
    incompatible_gateway_image_digest: str
    nginx_image_repository: str
    nginx_image_digest: str
    provisioner_image_repository: str
    provisioner_image_digest: str
    sandbox_image_repository: str
    sandbox_image_digest: str
    postgres_image_repository: str
    postgres_image_digest: str
    redis_image_repository: str
    redis_image_digest: str
    rwx_storage_class: str
    extension_artifact_digest: str
    extension_configuration_digest: str
    capability_manifest_digest: str
    database_schema_ref: str

    def __post_init__(self) -> None:
        super().__post_init__()
        for repository in (
            self.predecessor_gateway_image_repository,
            self.incompatible_gateway_image_repository,
            self.frontend_image_repository,
            self.nginx_image_repository,
            self.provisioner_image_repository,
            self.sandbox_image_repository,
            self.postgres_image_repository,
            self.redis_image_repository,
        ):
            if _IMAGE_REPOSITORY.fullmatch(repository) is None:
                raise ValueError("multi-Gateway image repository is invalid")
        for digest in (
            self.predecessor_gateway_image_digest,
            self.incompatible_gateway_image_digest,
            self.frontend_image_digest,
            self.nginx_image_digest,
            self.provisioner_image_digest,
            self.sandbox_image_digest,
            self.postgres_image_digest,
            self.redis_image_digest,
            self.extension_artifact_digest,
            self.extension_configuration_digest,
            self.capability_manifest_digest,
        ):
            if _IMAGE_DIGEST.fullmatch(digest) is None:
                raise ValueError("multi-Gateway digest is invalid")
        if _SCHEMA_REF.fullmatch(self.database_schema_ref) is None:
            raise ValueError("multi-Gateway database schema reference is invalid")
        if _STORAGE_CLASS.fullmatch(self.rwx_storage_class) is None:
            raise ValueError("multi-Gateway RWX StorageClass is invalid")
        if self.incompatible_gateway_image_digest == self.image_digest:
            raise ValueError("incompatible Gateway image digest must differ from the qualified Gateway digest")
        if self.predecessor_gateway_image_digest == self.image_digest:
            raise ValueError(
                "predecessor Gateway image digest must differ from the qualified Gateway digest",
            )
        if self.predecessor_gateway_image_digest == self.incompatible_gateway_image_digest:
            raise ValueError(
                "predecessor and incompatible Gateway digests must differ",
            )

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str],
    ) -> KubernetesMultiGatewayQualificationConfigV1:
        validate_kubernetes_prerequisites(environment)
        if environment.get("DEERFLOW_TEST_KUBERNETES_SCOPE") != (MULTI_GATEWAY_QUALIFICATION_SCOPE):
            raise QualificationPrerequisiteError("multi-Gateway runner requires its exact qualification scope")
        context = _required(
            environment,
            "DEERFLOW_TEST_KUBERNETES_CONTEXT",
        )
        if environment.get("DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT") != context:
            raise QualificationPrerequisiteError("confirmed Kubernetes context does not match")
        gateway = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST",
        )
        incompatible_gateway = _image_subject(
            environment,
            repository_name=("DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_REPOSITORY"),
            digest_name="DEERFLOW_TEST_INCOMPATIBLE_GATEWAY_IMAGE_DIGEST",
        )
        predecessor_gateway = _image_subject(
            environment,
            repository_name=("DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_REPOSITORY"),
            digest_name="DEERFLOW_TEST_PREDECESSOR_GATEWAY_IMAGE_DIGEST",
        )
        frontend = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_FRONTEND_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_FRONTEND_IMAGE_DIGEST",
        )
        nginx = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_NGINX_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_NGINX_IMAGE_DIGEST",
        )
        provisioner = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST",
        )
        sandbox = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST",
        )
        postgres = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_POSTGRES_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_POSTGRES_IMAGE_DIGEST",
        )
        redis = _image_subject(
            environment,
            repository_name="DEERFLOW_TEST_REDIS_IMAGE_REPOSITORY",
            digest_name="DEERFLOW_TEST_REDIS_IMAGE_DIGEST",
        )
        return cls(
            kubeconfig=Path(_required(environment, "KUBECONFIG")).expanduser().resolve(),
            context=context,
            namespace=_required(
                environment,
                "DEERFLOW_TEST_KUBERNETES_NAMESPACE",
            ),
            image_repository=gateway[0],
            image_digest=gateway[1],
            evidence_path=Path(
                _required(
                    environment,
                    "DEERFLOW_TEST_KUBERNETES_EVIDENCE",
                )
            )
            .expanduser()
            .resolve(),
            qualification_id=_required(
                environment,
                "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID",
            ),
            predecessor_gateway_image_repository=predecessor_gateway[0],
            predecessor_gateway_image_digest=predecessor_gateway[1],
            incompatible_gateway_image_repository=incompatible_gateway[0],
            incompatible_gateway_image_digest=incompatible_gateway[1],
            frontend_image_repository=frontend[0],
            frontend_image_digest=frontend[1],
            nginx_image_repository=nginx[0],
            nginx_image_digest=nginx[1],
            provisioner_image_repository=provisioner[0],
            provisioner_image_digest=provisioner[1],
            sandbox_image_repository=sandbox[0],
            sandbox_image_digest=sandbox[1],
            postgres_image_repository=postgres[0],
            postgres_image_digest=postgres[1],
            redis_image_repository=redis[0],
            redis_image_digest=redis[1],
            rwx_storage_class=_required(
                environment,
                "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS",
            ),
            extension_artifact_digest=_required(
                environment,
                "DEERFLOW_TEST_EXTENSION_ARTIFACT_DIGEST",
            ),
            extension_configuration_digest=_required(
                environment,
                "DEERFLOW_TEST_EXTENSION_CONFIGURATION_DIGEST",
            ),
            capability_manifest_digest=_required(
                environment,
                "DEERFLOW_TEST_CAPABILITY_MANIFEST_DIGEST",
            ),
            database_schema_ref=_required(
                environment,
                "DEERFLOW_TEST_DATABASE_SCHEMA_REF",
            ),
        )

    @property
    def image_digests(self) -> dict[str, str]:
        return {
            "gateway": self.image_digest,
            "frontend": self.frontend_image_digest,
            "nginx": self.nginx_image_digest,
            "provisioner": self.provisioner_image_digest,
            "sandbox": self.sandbox_image_digest,
            "postgres": self.postgres_image_digest,
            "redis": self.redis_image_digest,
        }


class KubernetesMultiGatewayQualificationDriverV1:
    """Kubernetes CLI implementation of the package-level driver protocol."""

    def __init__(
        self,
        config: KubernetesMultiGatewayQualificationConfigV1,
        *,
        repository_root: Path | None = None,
    ) -> None:
        self.config = config
        self.repository_root = repository_root.resolve() if repository_root is not None else Path(__file__).resolve().parents[3]
        self.chart_path = self.repository_root / "deploy/helm/deer-flow"
        self.fullname = f"{config.release_name}-deer-flow"
        self.gateway_service = f"{self.fullname}-gateway"
        self.gateway_deployment = f"{self.fullname}-gateway"
        self.store_secret = "hartmesh-multi-gateway-stores"
        self.runtime_config_map = "hartmesh-multi-gateway-runtime"
        self.home_claim = "hartmesh-multi-gateway-home"
        self.skills_claim = "hartmesh-multi-gateway-skills"
        self.postgres_name = "hartmesh-multi-gateway-postgres"
        self.redis_name = "hartmesh-multi-gateway-redis"
        self.mcp_name = "hartmesh-multi-gateway-mcp"
        self.skill_fixture_config_map = "hartmesh-multi-gateway-skill-fixture"
        self.skill_fixture_job = "hartmesh-multi-gateway-skill-fixture"
        self._postgres_password = secrets.token_urlsafe(32)
        self._postgres_tenant_password = secrets.token_urlsafe(32)
        self._redis_password = secrets.token_urlsafe(32)
        self._redis_admin_password = secrets.token_urlsafe(32)
        self._admin_password = secrets.token_urlsafe(24) + "Aa1!"
        self._owned_namespace_uid: str | None = None
        self._namespace_owner = hashlib.sha256(
            f"{config.namespace}:{config.qualification_id}".encode(),
        ).hexdigest()[:32]
        self._subjects: MultiGatewayQualificationSubjectsV1 | None = None
        self._scenario_run_ids: dict[str, str] = {}
        self._completed_scenarios: list[str] = []
        self._candidate_values_path: Path | None = None
        self._mixed_binary_rejection_verified = False
        secondary_stem = config.namespace[:53].rstrip("-")
        self._secondary_namespace = f"{secondary_stem}-tenant-b"
        self.scenario_handlers: dict[str, Callable[[], MultiGatewayScenarioObservationV1]] = {
            "topology_identity": self._topology_identity,
            "concurrent_admission": self._concurrent_admission,
            "execution_ownership": self._execution_ownership,
            "owner_sigkill": self._owner_sigkill,
            "sse_reconnect": self._sse_reconnect,
            "scheduler_occurrence": self._scheduler_occurrence,
            "scheduler_owner_loss": self._scheduler_owner_loss,
            "sandbox_recovery": self._sandbox_recovery,
            "mcp_task_notification": self._mcp_task_notification,
            "cancellation_finalization": self._cancellation_finalization,
            "redis_outage_recovery": self._redis_outage_recovery,
            "postgresql_interruption": self._postgresql_interruption,
            "config_artifact_skew": self._config_artifact_skew,
            "tenant_separation": self._tenant_separation,
            "unsupported_surfaces": self._unsupported_surfaces,
            "upgrade_truthfulness": self._upgrade_truthfulness,
        }
        if tuple(self.scenario_handlers) != MULTI_GATEWAY_QUALIFICATION_SCENARIOS:
            raise AssertionError("live scenario dispatch is incomplete or reordered")

    @property
    def subjects(self) -> MultiGatewayQualificationSubjectsV1:
        """Return the pre-scenario live subject snapshot, never artifact bytes."""

        if self._subjects is None:
            raise QualificationCommandError(
                "multi-Gateway qualification subjects are not available",
            )
        return self._subjects

    def _application_config(self) -> str:
        config = {
            "config_version": 47,
            "log_level": "info",
            "models": [
                {
                    "name": "kubernetes-qualification",
                    "display_name": "Kubernetes qualification double",
                    "description": "deterministic no-network qualification model",
                    "use": "deerflow.runtime.kubernetes_qualification:KubernetesQualificationChatModel",
                    "model": "kubernetes-qualification",
                }
            ],
            "sandbox": {
                "use": "deerflow.community.aio_sandbox:AioSandboxProvider",
                "provisioner_url": f"http://{self.fullname}-provisioner:8002",
                "image": (f"{self.config.sandbox_image_repository}@{self.config.sandbox_image_digest}"),
                "ownership": {"type": "redis"},
                "replicas": 1,
                "idle_timeout": 0,
                "accepted_skill_projection_profile": ("rwx_verified_copy_v2"),
                "accepted_materialization_profile": "disabled",
            },
            "database": {
                "backend": "postgres",
                "postgres_url": "$DATABASE_URL",
                "command_timeout": 30,
                "checkpoint_cache": {"type": "redis"},
            },
            "checkpointer": {
                "type": "postgres",
                "connection_string": "$DATABASE_URL",
            },
            "run_events": {"backend": "db"},
            "agent_storage": {"backend": "db"},
            "dedupe_storage": {"backend": "postgres"},
            "stream_bridge": {"type": "redis"},
            "run_ownership": {
                "heartbeat_enabled": True,
                "lease_seconds": 9,
                "grace_seconds": 1,
            },
            "scheduler": {
                "enabled": True,
                "multi_instance": True,
                "poll_interval_seconds": 1,
                "lease_seconds": 10,
                "max_concurrent_runs": 2,
                "queue_timeout_seconds": 60,
                "min_once_delay_seconds": 1,
                "recursion_limit": 1000,
            },
            "mcp_tasks": {
                "enabled": True,
                "poll_interval_seconds": 1,
                "lease_seconds": 10,
                "max_concurrent_polls": 2,
                "max_poll_backoff_seconds": 5,
                "input_required_poll_interval_seconds": 5,
            },
            "deployment": {
                "profile": "durable_two_gateway_v1",
                "readiness": {
                    "capability_cache_seconds": 1.0,
                    "admission_health_max_age_seconds": 2.0,
                    "required_health_stale_seconds": 6.0,
                    "capability_probe_timeout_seconds": 2.0,
                    "overall_timeout_seconds": 5.0,
                    "required_failure_threshold": 1,
                },
                "shutdown": {
                    "admission_seconds": 2.0,
                    "channel_seconds": 1.0,
                    "scheduler_seconds": 2.0,
                    "run_seconds": 8.0,
                    "dependencies_seconds": 2.0,
                },
            },
            "memory": {
                "enabled": False,
                "shutdown_flush_timeout_seconds": 1.0,
            },
            "channel_connections": {"enabled": False},
            "plugins": [
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
                }
            ],
            "required_capabilities": [
                "invocation_constraints.v2",
                "mcp_interceptor:hartmesh.governance.mcp",
            ],
            "subagents": {"enabled": False},
            "title": {"enabled": False},
            "tool_groups": [{"name": "qualification"}],
            "tools": [
                {
                    "name": "qualification_sandbox_operation",
                    "group": "qualification",
                    "use": ("deerflow.runtime.kubernetes_qualification:qualification_sandbox_operation"),
                }
            ],
        }
        return yaml.safe_dump(config, sort_keys=False)

    def values(self) -> dict[str, object]:
        """Render the exact candidate without any inline credential."""

        sandbox_ref = f"{self.config.sandbox_image_repository}@{self.config.sandbox_image_digest}"
        extensions = {
            "mcpServers": {
                "qualification-tasks": {
                    "enabled": True,
                    "type": "http",
                    "url": f"http://{self.mcp_name}:8090/mcp",
                    "credential_binding_id": "qualification-v1",
                    "task_toolsets": [
                        {
                            "name": "qualification",
                            "submit_tool": "submit_task",
                            "status_tool": "task_status",
                            "cancel_tool": "cancel_task",
                        }
                    ],
                }
            },
            "skills": {},
        }
        return {
            "namespace": self.config.namespace,
            "tenant": {"id": "qualification"},
            "deployment": {
                "mode": "durable_two_gateway_v1",
                "persistenceTier": "shared_durable",
                "topology": {
                    "databaseSchemaRef": self.config.database_schema_ref,
                },
                "provenance": {"sourceRevision": self._git_revision()},
                "qualificationEvidence": [],
                "qualificationCandidate": {
                    "enabled": True,
                    "id": self.config.qualification_id,
                },
            },
            "gateway": {
                "replicas": 2,
                "image": {
                    "repository": self.config.image_repository,
                    "digest": self.config.image_digest,
                },
                "extraEnvFrom": [{"configMapRef": {"name": self.runtime_config_map}}],
            },
            "frontend": {
                "replicas": 0,
                "image": {
                    "repository": self.config.frontend_image_repository,
                    "digest": self.config.frontend_image_digest,
                },
            },
            "nginx": {
                "replicas": 0,
                "image": {
                    "repository": self.config.nginx_image_repository,
                    "digest": self.config.nginx_image_digest,
                },
            },
            "provisioner": {
                "enabled": True,
                "image": {
                    "repository": self.config.provisioner_image_repository,
                    "digest": self.config.provisioner_image_digest,
                },
                "sandboxImage": sandbox_ref,
                "sandboxServiceType": "ClusterIP",
                "acceptedSkillProjectionProfile": "rwx_verified_copy_v2",
                "acceptedAttempt": {
                    "leaseSeconds": _ACCEPTED_ATTEMPT_LEASE_SECONDS,
                    "reconcileIntervalSeconds": 5,
                    "reconcileLimit": 100,
                },
            },
            "sandbox": {"volumeMode": "pvc"},
            "postgresql": {
                "enabled": False,
                "image": {
                    "repository": self.config.postgres_image_repository,
                    "digest": self.config.postgres_image_digest,
                },
                "external": {"existingSecret": self.store_secret},
            },
            "redis": {
                "enabled": False,
                "image": {
                    "repository": self.config.redis_image_repository,
                    "digest": self.config.redis_image_digest,
                },
                "external": {"existingSecret": self.store_secret},
            },
            "persistence": {
                "home": {
                    "enabled": True,
                    "existingClaim": self.home_claim,
                    "accessMode": "ReadWriteMany",
                }
            },
            "skills": {
                "enabled": True,
                "existingClaim": self.skills_claim,
                "accessMode": "ReadWriteMany",
            },
            "extensions": {
                "artifactManifestDigest": self.config.extension_artifact_digest,
                "configurationDigest": (self.config.extension_configuration_digest),
            },
            "ingress": {"enabled": False},
            "config": self._application_config(),
            "extensionsConfig": json.dumps(
                extensions,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }

    def _git_revision(self) -> str:
        result = run_bounded(
            ("git", "rev-parse", "HEAD"),
            timeout_seconds=10,
            runner=lambda *args, **kwargs: subprocess.run(
                *args,
                cwd=self.repository_root,
                **kwargs,
            ),
        ).stdout.strip()
        if re.fullmatch(r"[0-9a-f]{40}", result) is None:
            raise QualificationPrerequisiteError("qualification checkout has no exact Git revision")
        return result

    def qualification_mcp_manifests(
        self,
    ) -> tuple[dict[str, object], dict[str, object]]:
        labels = {"app": self.mcp_name}
        deployment: dict[str, object] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": self.mcp_name},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "mcp",
                                "image": (f"{self.config.image_repository}@{self.config.image_digest}"),
                                "command": ["sh", "-c"],
                                "args": ["cd /app/backend && PYTHONPATH=. uv run --no-sync python -m deerflow.qualification_mcp_server"],
                                "env": [
                                    {
                                        "name": "DEERFLOW_QUALIFICATION_MCP",
                                        "value": "1",
                                    }
                                ],
                                "ports": [{"containerPort": 8090}],
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                            }
                        ],
                    },
                },
            },
        }
        service: dict[str, object] = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": self.mcp_name},
            "spec": {
                "selector": labels,
                "ports": [{"name": "http", "port": 8090, "targetPort": 8090}],
            },
        }
        return deployment, service

    def _run(
        self,
        command: tuple[str, ...],
        *,
        timeout_seconds: float = 60,
        input_text: str | None = None,
        redact_diagnostics: bool = False,
    ) -> str:
        return run_bounded(
            command,
            timeout_seconds=timeout_seconds,
            input_text=input_text,
            redact_diagnostics=redact_diagnostics,
        ).stdout.strip()

    def _kubectl(
        self,
        *arguments: str,
        namespaced: bool = True,
        timeout_seconds: float = 60,
        input_text: str | None = None,
        redact_diagnostics: bool = False,
    ) -> str:
        return self._run(
            self.config.kubectl(*arguments, namespaced=namespaced),
            timeout_seconds=timeout_seconds,
            input_text=input_text,
            redact_diagnostics=redact_diagnostics,
        )

    def _kubectl_for(
        self,
        config: KubernetesQualificationConfig,
        *arguments: str,
        namespaced: bool = True,
        timeout_seconds: float = 60,
        input_text: str | None = None,
        redact_diagnostics: bool = False,
    ) -> str:
        return self._run(
            config.kubectl(*arguments, namespaced=namespaced),
            timeout_seconds=timeout_seconds,
            input_text=input_text,
            redact_diagnostics=redact_diagnostics,
        )

    def _helm(self, *arguments: str, timeout_seconds: float = 600) -> str:
        return self._run(
            self.config.helm(*arguments),
            timeout_seconds=timeout_seconds,
        )

    def _helm_for(
        self,
        config: KubernetesQualificationConfig,
        *arguments: str,
        timeout_seconds: float = 600,
    ) -> str:
        return self._run(
            config.helm(*arguments),
            timeout_seconds=timeout_seconds,
        )

    def _apply(self, manifest: Mapping[str, object]) -> None:
        self._kubectl(
            "apply",
            "-f",
            "-",
            input_text=json.dumps(manifest),
            redact_diagnostics=True,
        )

    def _apply_for(
        self,
        config: KubernetesQualificationConfig,
        manifest: Mapping[str, object],
    ) -> None:
        self._kubectl_for(
            config,
            "apply",
            "-f",
            "-",
            input_text=json.dumps(manifest),
            redact_diagnostics=True,
        )

    def _confirm_context(self) -> None:
        actual = self._kubectl(
            "config",
            "current-context",
            namespaced=False,
            timeout_seconds=10,
        )
        if actual != self.config.context:
            raise QualificationPrerequisiteError("KUBECONFIG current-context changed after explicit confirmation")
        self._kubectl(
            "version",
            "--output=json",
            namespaced=False,
            timeout_seconds=20,
        )

    @staticmethod
    def _pvc(
        name: str,
        *,
        storage_class: str,
        access_mode: str,
        size: str,
    ) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name},
            "spec": {
                "accessModes": [access_mode],
                "storageClassName": storage_class,
                "resources": {"requests": {"storage": size}},
            },
        }

    def _skill_fixture_manifests(
        self,
    ) -> tuple[dict[str, object], dict[str, object]]:
        """Populate the RWX source with one immutable nonempty public skill."""

        fixture = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": self.skill_fixture_config_map},
            "data": {
                "skill-md": _QUALIFICATION_SKILL_FILES["SKILL.md"].decode(),
                "proof": _QUALIFICATION_SKILL_FILES["resources/proof.txt"].decode(),
            },
        }
        script = "\n".join(
            (
                "from pathlib import Path",
                "root = Path('/skills/public/qualification-skill')",
                "(root / 'resources').mkdir(parents=True, exist_ok=True)",
                "(root / 'SKILL.md').write_bytes(Path('/fixture/skill-md').read_bytes())",
                "(root / 'resources/proof.txt').write_bytes(Path('/fixture/proof').read_bytes())",
                "for item in (root, root / 'resources'):",
                "    item.chmod(0o755)",
                "for item in (root / 'SKILL.md', root / 'resources/proof.txt'):",
                "    item.chmod(0o444)",
            )
        )
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": self.skill_fixture_job},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": {"app.kubernetes.io/component": ("qualification-skill-fixture")}},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "writer",
                                "image": (f"{self.config.provisioner_image_repository}@{self.config.provisioner_image_digest}"),
                                "command": ["python", "-c", script],
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "volumeMounts": [
                                    {
                                        "name": "skills",
                                        "mountPath": "/skills",
                                    },
                                    {
                                        "name": "fixture",
                                        "mountPath": "/fixture",
                                        "readOnly": True,
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "skills",
                                "persistentVolumeClaim": {"claimName": self.skills_claim},
                            },
                            {
                                "name": "fixture",
                                "configMap": {"name": self.skill_fixture_config_map},
                            },
                        ],
                    },
                },
            },
        }
        return fixture, job

    def _store_manifests(self) -> tuple[dict[str, object], ...]:
        postgres_labels = {"app": self.postgres_name}
        redis_labels = {"app": self.redis_name}
        return (
            self._pvc(
                self.home_claim,
                storage_class=self.config.rwx_storage_class,
                access_mode="ReadWriteMany",
                size="2Gi",
            ),
            self._pvc(
                self.skills_claim,
                storage_class=self.config.rwx_storage_class,
                access_mode="ReadWriteMany",
                size="256Mi",
            ),
            self._pvc(
                f"{self.postgres_name}-data",
                storage_class=self.config.rwx_storage_class,
                access_mode="ReadWriteOnce",
                size="2Gi",
            ),
            self._pvc(
                f"{self.redis_name}-data",
                storage_class=self.config.rwx_storage_class,
                access_mode="ReadWriteOnce",
                size="1Gi",
            ),
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": self.postgres_name},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": postgres_labels},
                    "template": {
                        "metadata": {"labels": postgres_labels},
                        "spec": {
                            "containers": [
                                {
                                    "name": "postgres",
                                    "image": (f"{self.config.postgres_image_repository}@{self.config.postgres_image_digest}"),
                                    "env": [
                                        {"name": "POSTGRES_USER", "value": "deerflow"},
                                        {"name": "POSTGRES_DB", "value": "deerflow"},
                                        {
                                            "name": "POSTGRES_PASSWORD",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": self.store_secret,
                                                    "key": "postgres-password",
                                                }
                                            },
                                        },
                                        {
                                            "name": "PGDATA",
                                            "value": "/var/lib/postgresql/data/pgdata",
                                        },
                                    ],
                                    "ports": [{"containerPort": 5432}],
                                    "readinessProbe": {
                                        "exec": {
                                            "command": [
                                                "pg_isready",
                                                "-U",
                                                "deerflow",
                                                "-d",
                                                "deerflow",
                                            ]
                                        },
                                        "periodSeconds": 2,
                                        "failureThreshold": 30,
                                    },
                                    "volumeMounts": [
                                        {
                                            "name": "data",
                                            "mountPath": "/var/lib/postgresql/data",
                                        }
                                    ],
                                }
                            ],
                            "volumes": [
                                {
                                    "name": "data",
                                    "persistentVolumeClaim": {"claimName": f"{self.postgres_name}-data"},
                                }
                            ],
                        },
                    },
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": self.postgres_name},
                "spec": {
                    "selector": postgres_labels,
                    "ports": [{"port": 5432, "targetPort": 5432}],
                },
            },
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": self.redis_name},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": redis_labels},
                    "template": {
                        "metadata": {"labels": redis_labels},
                        "spec": {
                            "containers": [
                                {
                                    "name": "redis",
                                    "image": (f"{self.config.redis_image_repository}@{self.config.redis_image_digest}"),
                                    "args": [
                                        "/etc/redis-secret/redis.conf",
                                    ],
                                    "env": [
                                        {
                                            "name": "REDISCLI_AUTH",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": self.store_secret,
                                                    "key": "redis-password",
                                                }
                                            },
                                        }
                                    ],
                                    "ports": [{"containerPort": 6379}],
                                    "readinessProbe": {
                                        "exec": {
                                            "command": [
                                                "redis-cli",
                                                "--user",
                                                "qualification",
                                                "ping",
                                            ]
                                        },
                                        "periodSeconds": 2,
                                        "failureThreshold": 30,
                                    },
                                    "volumeMounts": [
                                        {
                                            "name": "data",
                                            "mountPath": "/data",
                                        },
                                        {
                                            "name": "redis-secret",
                                            "mountPath": "/etc/redis-secret",
                                            "readOnly": True,
                                        },
                                    ],
                                }
                            ],
                            "volumes": [
                                {
                                    "name": "data",
                                    "persistentVolumeClaim": {"claimName": f"{self.redis_name}-data"},
                                },
                                {
                                    "name": "redis-secret",
                                    "secret": {
                                        "secretName": self.store_secret,
                                        "items": [
                                            {
                                                "key": "redis.conf",
                                                "path": "redis.conf",
                                            },
                                            {
                                                "key": "redis-password",
                                                "path": "redis-password",
                                            },
                                            {
                                                "key": "redis-admin-password",
                                                "path": "redis-admin-password",
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": self.redis_name},
                "spec": {
                    "selector": redis_labels,
                    "ports": [{"port": 6379, "targetPort": 6379}],
                },
            },
        )

    def _redis_configuration(self) -> str:
        redis_prefix = (
            TenantIdentityV1.from_canonical_id(
                "qualification",
            )
            .namespace(TenantSubsystem.REDIS)
            .key_prefix
        )
        return "\n".join(
            (
                "appendonly yes",
                "appendfsync always",
                "user default off",
                (f"user qualification on >{self._redis_password} ~{redis_prefix}:* &{redis_prefix}:* +@all"),
                (f"user qualification-admin on >{self._redis_admin_password} ~* &* +@all"),
                "",
            )
        )

    def _create_cluster_candidate(self) -> None:
        self._kubectl(
            "create",
            "-f",
            "-",
            namespaced=False,
            input_text=yaml.safe_dump(
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": self.config.namespace,
                        "labels": {
                            _QUALIFICATION_OWNER_LABEL: self._namespace_owner,
                        },
                    },
                },
                sort_keys=True,
            ),
        )
        namespace = json.loads(
            self._kubectl(
                "get",
                "namespace",
                self.config.namespace,
                "-o",
                "json",
                namespaced=False,
            )
        )
        metadata = namespace.get("metadata") if isinstance(namespace, dict) else None
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        uid = metadata.get("uid") if isinstance(metadata, dict) else None
        if not isinstance(uid, str) or not uid or not isinstance(labels, dict) or labels.get(_QUALIFICATION_OWNER_LABEL) != self._namespace_owner:
            raise QualificationCommandError(
                "created qualification namespace ownership was not verifiable",
            )
        self._owned_namespace_uid = uid
        database_url = f"postgresql://qualification_primary:{self._postgres_tenant_password}@{self.postgres_name}:5432/deerflow"
        redis_url = f"redis://qualification:{self._redis_password}@{self.redis_name}:6379/0"
        redis_configuration = self._redis_configuration()
        self._apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": self.store_secret},
                "type": "Opaque",
                "stringData": {
                    "database-url": database_url,
                    "postgres-password": self._postgres_password,
                    "postgres-tenant-password": self._postgres_tenant_password,
                    "redis-url": redis_url,
                    "redis-password": self._redis_password,
                    "redis-admin-password": self._redis_admin_password,
                    "redis.conf": redis_configuration,
                },
            }
        )
        self._apply(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": self.runtime_config_map},
                "data": {
                    "DEERFLOW_TEST_KUBERNETES_RUNTIME": "1",
                    "DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION": "1",
                    "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID": (self.config.qualification_id),
                    "DEERFLOW_TEST_KUBERNETES_BARRIER_TIMEOUT_SECONDS": "180",
                    "DEERFLOW_TEST_KUBERNETES_MODEL_DELAY_SECONDS": "5",
                },
            }
        )
        for manifest in self._store_manifests():
            self._apply(manifest)
        for manifest in self._skill_fixture_manifests():
            self._apply(manifest)
        for manifest in self.qualification_mcp_manifests():
            self._apply(manifest)
        self._kubectl(
            "wait",
            "--for=condition=complete",
            f"job/{self.skill_fixture_job}",
            "--timeout=4m",
            timeout_seconds=250,
        )
        for deployment in (
            self.postgres_name,
            self.redis_name,
            self.mcp_name,
        ):
            self._kubectl(
                "rollout",
                "status",
                f"deployment/{deployment}",
                "--timeout=4m",
                timeout_seconds=250,
            )
        self._initialize_primary_database_role()
        values_path = self.config.evidence_path.parent / (f".{self.config.qualification_id}.candidate.values.json")
        values_path.parent.mkdir(parents=True, exist_ok=True)
        values_path.write_text(
            json.dumps(self.values(), sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        self._candidate_values_path = values_path
        self._helm(
            "upgrade",
            "--install",
            self.config.release_name,
            str(self.chart_path),
            "--values",
            str(values_path),
            "--wait",
            "--timeout",
            "10m",
            timeout_seconds=630,
        )

    def _pod_document(self, selector: str) -> dict[str, object]:
        value = json.loads(self._kubectl("get", "pods", "-l", selector, "-o", "json"))
        if not isinstance(value, dict):
            raise QualificationCommandError("kubectl returned invalid pod JSON")
        return value

    def _single_pod(self, selector: str) -> dict[str, object]:
        items = self._pod_document(selector).get("items")
        if not isinstance(items, list) or len(items) != 1:
            raise QualificationCommandError("qualification component does not have exactly one pod")
        item = items[0]
        if not isinstance(item, dict):
            raise QualificationCommandError("qualification pod JSON is invalid")
        return item

    @staticmethod
    def _metadata_value(document: Mapping[str, object], name: str) -> str:
        metadata = document.get("metadata")
        value = metadata.get(name) if isinstance(metadata, dict) else None
        if not isinstance(value, str) or not value:
            raise QualificationCommandError(f"Kubernetes resource is missing metadata.{name}")
        return value

    def _gateway_pods(self, *, require_ready: bool = True) -> tuple[dict[str, object], ...]:
        selector = f"app.kubernetes.io/instance={self.config.release_name},app.kubernetes.io/component=gateway"
        items = self._pod_document(selector).get("items")
        if not isinstance(items, list):
            raise QualificationCommandError("Gateway pod list is invalid")
        pods = tuple(
            sorted(
                (item for item in items if isinstance(item, dict)),
                key=lambda item: self._metadata_value(item, "name"),
            )
        )
        if len(pods) != 2:
            raise QualificationCommandError("qualification requires exactly two Gateway pods")
        if require_ready:
            for pod in pods:
                status = pod.get("status")
                conditions = status.get("conditions", []) if isinstance(status, dict) else []
                if not any(isinstance(condition, dict) and condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions):
                    raise QualificationCommandError("both Gateway pods must be Ready")
        return pods

    def _component_pod_name(self, component: str) -> str:
        pod = self._single_pod(f"app={component}")
        return self._metadata_value(pod, "name")

    def _postgres(
        self,
        sql: str,
        *,
        redact_diagnostics: bool = False,
    ) -> str:
        return self._kubectl(
            "exec",
            self._component_pod_name(self.postgres_name),
            "--",
            "psql",
            "-U",
            "deerflow",
            "-d",
            "deerflow",
            "-Atc",
            sql,
            redact_diagnostics=redact_diagnostics,
        )

    def _initialize_primary_database_role(self) -> None:
        """Create the non-superuser role used by the primary Gateway."""

        password = self._postgres_tenant_password
        self._postgres(
            "DO $$ BEGIN "
            "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='qualification_primary') THEN "
            "CREATE ROLE qualification_primary LOGIN NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION "
            f"PASSWORD '{password}'; "
            "ELSE "
            "ALTER ROLE qualification_primary LOGIN NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION "
            f"PASSWORD '{password}'; "
            "END IF; END $$; "
            "REVOKE CONNECT ON DATABASE deerflow FROM PUBLIC; "
            "GRANT CONNECT,TEMPORARY ON DATABASE deerflow TO qualification_primary; "
            "GRANT USAGE,CREATE ON SCHEMA public TO qualification_primary",
            redact_diagnostics=True,
        )

    def _require_postgres_connect_denial(
        self,
        *,
        role: str,
        password: str,
        database: str,
    ) -> str:
        if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", database) is None or not password:
            raise QualificationCommandError(
                "PostgreSQL tenant denial probe is unsafe",
            )
        try:
            self._kubectl(
                "exec",
                self._component_pod_name(self.postgres_name),
                "--",
                "env",
                f"PGPASSWORD={password}",
                "psql",
                "-h",
                "127.0.0.1",
                "-U",
                role,
                "-d",
                database,
                "-Atc",
                "SELECT 1",
                redact_diagnostics=True,
            )
        except QualificationCommandError:
            return "denied"
        raise QualificationCommandError(
            "PostgreSQL tenant credentials crossed a database boundary",
        )

    def _redis(self, *arguments: str) -> str:
        """Run an ACL-scoped command as the tenant-bound Gateway user."""

        return self._kubectl(
            "exec",
            self._component_pod_name(self.redis_name),
            "--",
            "sh",
            "-c",
            ('REDISCLI_AUTH=$(cat /etc/redis-secret/redis-password) exec redis-cli --raw --no-auth-warning --user qualification "$@"'),
            "redis-qualification",
            *arguments,
            redact_diagnostics=True,
        )

    def _redis_admin(self, *arguments: str) -> str:
        """Use the Secret-backed fixture administrator for ACL changes."""

        return self._kubectl(
            "exec",
            self._component_pod_name(self.redis_name),
            "--",
            "sh",
            "-c",
            ('REDISCLI_AUTH=$(cat /etc/redis-secret/redis-admin-password) exec redis-cli --raw --no-auth-warning --user qualification-admin "$@"'),
            "redis-admin",
            *arguments,
            redact_diagnostics=True,
        )

    def _require_redis_unauthenticated_denial(self) -> str:
        """Prove the default Redis user cannot execute even ``PING``."""

        result = self._kubectl(
            "exec",
            self._component_pod_name(self.redis_name),
            "--",
            "sh",
            "-c",
            ("response=$(redis-cli --raw PING 2>&1 || true); case \"$response\" in NOAUTH*|DENIED*) printf 'denied' ;; *) exit 41 ;; esac"),
            redact_diagnostics=True,
        )
        if result != "denied":
            raise QualificationCommandError(
                "Redis unauthenticated negative control was not denied",
            )
        return "denied"

    def _require_redis_acl_denial(self, *arguments: str) -> str:
        """Distinguish a foreign-prefix ACL denial from a Redis outage."""

        try:
            result = self._redis(*arguments)
        except QualificationCommandError:
            if self._redis("PING") != "PONG":
                raise QualificationCommandError("Redis became unavailable during the ACL negative control") from None
            return "command_denied"
        if not result.startswith(("NOPERM", "ERR this user has no permissions")):
            raise QualificationCommandError("tenant Redis credentials accessed a foreign prefix")
        if self._redis("PING") != "PONG":
            raise QualificationCommandError("Redis became unavailable during the ACL negative control")
        return "noperm"

    def _initialize_admin(self) -> None:
        with _PortForward(self.config, self.gateway_service) as forwarded:
            client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            status, payload = client.request(
                "POST",
                "/api/v1/auth/initialize",
                payload={
                    "email": f"{self.config.qualification_id}@qualification.invalid",
                    "password": self._admin_password,
                    "remember_me": False,
                },
            )
            if status != 201:
                raise QualificationCommandError(f"qualification admin initialization failed with HTTP {status}: {sorted(payload)}")

    def _pod_report(self, pod_name: str) -> dict[str, object]:
        with _PortForward(
            self.config,
            pod_name,
            resource_kind="pod",
        ) as forwarded:
            client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            status, payload = client.request(
                "POST",
                "/api/v1/auth/login/local",
                form_payload={
                    "username": (f"{self.config.qualification_id}@qualification.invalid"),
                    "password": self._admin_password,
                },
            )
            if status != 200:
                raise QualificationCommandError(f"qualification admin login failed with HTTP {status}: {sorted(payload)}")
            status, report = client.request(
                "GET",
                "/api/runtime/v1/deployment",
            )
            if status != 200:
                raise QualificationCommandError("Gateway deployment report was unavailable")
            return report

    def _topology_registrations(self) -> tuple[ReplicaRegistrationV1, ...]:
        rows = self._postgres(
            "SELECT COALESCE(json_agg(json_build_object("
            "'version',1,'replica_id',replica_id,"
            "'topology_fingerprint',fingerprint_json,"
            "'started_at',started_at,'heartbeat_at',heartbeat_at) "
            "ORDER BY replica_id),'[]'::json) "
            "FROM hartmesh_topology_replicas "
            "WHERE profile='durable_two_gateway_v1' "
            "AND heartbeat_at >= CURRENT_TIMESTAMP - INTERVAL '35 seconds'"
        )
        try:
            payload = json.loads(rows)
            registrations = tuple(ReplicaRegistrationV1.from_dict(item) for item in payload)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise QualificationCommandError("topology registration evidence is malformed") from exc
        if len(registrations) != 2:
            raise QualificationCommandError("topology registration evidence is not exact-two")
        return registrations

    def _resource_uid(self, kind: str, name: str) -> str:
        value = self._kubectl(
            "get",
            kind,
            name,
            "-o",
            "jsonpath={.metadata.uid}",
        )
        if not value or len(value) > 128:
            raise QualificationCommandError("Kubernetes UID is unavailable")
        return value

    def _chart_version(self) -> str:
        chart = yaml.safe_load((self.chart_path / "Chart.yaml").read_text(encoding="utf-8"))
        version = chart.get("version") if isinstance(chart, dict) else None
        if not isinstance(version, str) or not version:
            raise QualificationCommandError("chart version is unavailable")
        return version

    def _chart_digest(self) -> str:
        digest = hashlib.sha256()
        for path in sorted(item for item in self.chart_path.rglob("*") if item.is_file()):
            digest.update(path.relative_to(self.chart_path).as_posix().encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return "sha256:" + digest.hexdigest()

    @staticmethod
    def _safe_digest(value: object, *, name: str, prefixed: bool = True) -> str:
        pattern = _IMAGE_DIGEST if prefixed else re.compile(r"[0-9a-f]{64}\Z")
        if not isinstance(value, str) or pattern.fullmatch(value) is None:
            raise QualificationCommandError(f"{name} is not a safe digest")
        return value

    def _collect_subjects(self) -> MultiGatewayQualificationSubjectsV1:
        pods = self._gateway_pods()
        reports = tuple(self._pod_report(self._metadata_value(pod, "name")) for pod in pods)
        topologies = tuple(report.get("topology") for report in reports)
        if any(not isinstance(item, dict) for item in topologies):
            raise QualificationCommandError("Gateway topology report is unavailable")
        replica_ids = tuple(item.get("replica_id") for item in topologies)
        topology_digests = tuple(item.get("topology_digest") for item in topologies)
        if len(set(replica_ids)) != 2 or len(set(topology_digests)) != 1 or not all(item.get("qualification_ready") is True for item in topologies) or not all(item.get("live_compatible_replicas") == 2 for item in topologies):
            raise QualificationCommandError("Gateway topology reports do not prove exact-two identity")
        registrations = self._topology_registrations()
        fingerprint = registrations[0].topology_fingerprint
        if any(registration.topology_fingerprint.digest != fingerprint.digest for registration in registrations):
            raise QualificationCommandError("Gateway registration fingerprints differ")
        if set(fingerprint.image_digests.items()) != set(self.config.image_digests.items()):
            raise QualificationCommandError("running topology image tuple differs from qualification inputs")
        expected_digests = {
            "extension artifact": (
                fingerprint.extension_artifact_digest,
                self.config.extension_artifact_digest,
            ),
            "extension configuration": (
                fingerprint.extension_configuration_digest,
                self.config.extension_configuration_digest,
            ),
            "capability manifest": (
                "sha256:" + fingerprint.capability_manifest_digest,
                self.config.capability_manifest_digest,
            ),
        }
        if any(actual != expected for actual, expected in expected_digests.values()):
            raise QualificationCommandError("running extension/capability tuple differs from qualification inputs")
        tenant = TenantIdentityV1.from_canonical_id("qualification")
        if tenant.digest != fingerprint.tenant_digest:
            raise QualificationCommandError("running topology tenant digest differs from qualification tenant")
        redis_key = (
            redis_component_key_prefix(
                tenant.namespace(TenantSubsystem.REDIS),
                RedisTenantComponent.QUALIFICATION,
            )
            + f":{self.config.qualification_id}:acl-proof"
        )
        if self._redis("SET", redis_key, "1", "EX", "30") != "OK":
            raise QualificationCommandError("Redis qualification write failed")
        if self._redis("GET", redis_key) != "1":
            raise QualificationCommandError("Redis qualification read failed")
        self._redis("DEL", redis_key)
        foreign_prefix = TenantIdentityV1.from_canonical_id("topology-acl-negative-control").namespace(TenantSubsystem.REDIS).key_prefix
        foreign_key_denial = self._require_redis_acl_denial(
            "SET",
            f"{foreign_prefix}:qualification:key",
            "1",
        )
        foreign_channel_denial = self._require_redis_acl_denial(
            "PUBLISH",
            f"{foreign_prefix}:qualification:channel",
            "1",
        )
        unauthenticated_denial = self._require_redis_unauthenticated_denial()
        acl_proof = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    {
                        "version": 1,
                        "redis_namespace_digest": fingerprint.redis_namespace_digest,
                        "operations": ["delete", "publish", "read", "write"],
                        "foreign_key_denial": foreign_key_denial,
                        "foreign_channel_denial": foreign_channel_denial,
                        "unauthenticated_denial": unauthenticated_denial,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        )
        provisioner = self._single_pod(f"app.kubernetes.io/instance={self.config.release_name},app.kubernetes.io/component=provisioner")
        return MultiGatewayQualificationSubjectsV1(
            git_revision=self._git_revision(),
            chart_version=self._chart_version(),
            chart_digest=self._chart_digest(),
            image_digests=self.config.image_digests,
            configuration_digest=fingerprint.config_digest,
            migration_head=fingerprint.migration_head,
            tenant_public_ref=tenant.public_ref,
            tenant_digest=tenant.digest,
            namespace=self.config.namespace,
            kubernetes_refs={
                "gateway_service_uid": self._resource_uid(
                    "service",
                    self.gateway_service,
                ),
                "gateway_pod_0_uid": self._metadata_value(pods[0], "uid"),
                "gateway_pod_1_uid": self._metadata_value(pods[1], "uid"),
                "provisioner_pod_uid": self._metadata_value(
                    provisioner,
                    "uid",
                ),
                "sandbox_pvc_uid": self._resource_uid(
                    "pvc",
                    self.home_claim,
                ),
            },
            database_schema_ref=fingerprint.database_schema_ref,
            redis_namespace_digest=fingerprint.redis_namespace_digest,
            redis_acl_proof_digest=acl_proof,
            extension_artifact_digest=fingerprint.extension_artifact_digest,
            extension_configuration_digest=(fingerprint.extension_configuration_digest),
            capability_manifest_digest=("sha256:" + fingerprint.capability_manifest_digest),
            topology_registrations=registrations,
        )

    async def prepare(self) -> MultiGatewayQualificationSubjectsV1:
        """Install the isolated candidate and collect exact live subjects."""

        validate_kubernetes_prerequisites(os.environ)
        self._confirm_context()
        self._create_cluster_candidate()
        self._initialize_admin()
        self._subjects = self._collect_subjects()
        return self._subjects

    async def run_scenario(
        self,
        scenario_id: str,
    ) -> MultiGatewayScenarioObservationV1:
        handler = self.scenario_handlers.get(scenario_id)
        if handler is None:
            raise QualificationCommandError("unknown multi-Gateway scenario")
        observation = await asyncio.to_thread(handler)
        self._completed_scenarios.append(scenario_id)
        return observation

    async def close(self) -> None:
        """Cluster cleanup is implemented by the publication phase."""

    @staticmethod
    def _ensure_payload(
        scenario_id: str,
        *,
        delivery_id: str = "delivery-1",
    ) -> dict[str, object]:
        if _SAFE_RESOURCE.fullmatch(delivery_id) is None:
            raise QualificationCommandError("qualification delivery id is unsafe")
        return {
            "api_version": "deerflow.runtime/v1",
            "kind": "invocation.ensure",
            "external_key": f"k8s-qual-v1:{scenario_id}:{delivery_id}",
            "thread_id": f"multi-gateway-{scenario_id}-{delivery_id}",
            "agent_hint": None,
            "input": {
                "api_version": "deerflow.runtime/v1",
                "kind": "invocation.input.graph",
                "value": {
                    "messages": [
                        {
                            "role": "user",
                            "content": "deterministic multi-Gateway qualification",
                        }
                    ]
                },
            },
            "options": {
                "api_version": "deerflow.runtime/v1",
                "kind": "invocation.options",
                "model_name": "kubernetes-qualification",
                "thinking_enabled": False,
                "multitask_strategy": "reject",
                "checkpoint_id": None,
                "interrupt_before": None,
                "interrupt_after": None,
            },
        }

    def _qualification_prefix(self, scenario_id: str) -> str:
        tenant = TenantIdentityV1.from_canonical_id("qualification")
        return (
            redis_component_key_prefix(
                tenant.namespace(TenantSubsystem.REDIS),
                RedisTenantComponent.QUALIFICATION,
            )
            + f":{self.config.qualification_id}:{scenario_id}"
        )

    def _wait_for_barrier(self, scenario_id: str) -> str:
        run_id = ""

        def reached() -> bool:
            nonlocal run_id
            run_id = self._redis(
                "GET",
                f"{self._qualification_prefix(scenario_id)}:reached",
            )
            return bool(run_id)

        wait_until(
            reached,
            description=f"multi-Gateway barrier {scenario_id}",
            timeout_seconds=150,
            interval_seconds=1,
        )
        return run_id

    def _arm_invocation_barrier(
        self,
        scenario_id: str,
        point: str,
    ) -> int:
        if _SAFE_RESOURCE.fullmatch(point) is None:
            raise QualificationCommandError("qualification barrier point is unsafe")
        prefix = self._qualification_prefix(scenario_id)
        baseline = self._counter(scenario_id, "barrier_hits")
        self._redis(
            "DEL",
            f"{prefix}:reached",
            f"{prefix}:reached_point",
            f"{prefix}:release",
            f"{prefix}:owner_replica_id",
        )
        if (
            self._redis(
                "SET",
                f"{prefix}:selected_point",
                point,
                "EX",
                "300",
            )
            != "OK"
        ):
            raise QualificationCommandError("qualification barrier selection failed")
        if (
            self._redis(
                "SET",
                f"{prefix}:arm",
                "1",
                "EX",
                "300",
            )
            != "OK"
        ):
            raise QualificationCommandError("qualification barrier arm failed")
        return baseline

    def _release_barrier(self, scenario_id: str) -> None:
        if (
            self._redis(
                "SET",
                f"{self._qualification_prefix(scenario_id)}:release",
                "1",
                "EX",
                "300",
            )
            != "OK"
        ):
            raise QualificationCommandError("qualification barrier release failed")

    def _counter(self, scenario_id: str, name: str) -> int:
        value = self._redis(
            "GET",
            f"{self._qualification_prefix(scenario_id)}:{name}",
        )
        try:
            return int(value or "0")
        except ValueError as exc:
            raise QualificationCommandError("qualification counter is malformed") from exc

    def _login(self, client: _RuntimeHttpSession) -> None:
        status, payload = client.request(
            "POST",
            "/api/v1/auth/login/local",
            form_payload={
                "username": (f"{self.config.qualification_id}@qualification.invalid"),
                "password": self._admin_password,
            },
        )
        if status != 200:
            raise QualificationCommandError(f"qualification login failed with HTTP {status}: {sorted(payload)}")

    @staticmethod
    def _bearer_request_status(
        base_url: str,
        path: str,
        *,
        token: str,
        method: str,
        payload: Mapping[str, object] | None = None,
    ) -> int:
        """Make one bounded provisioner request without exposing its token."""

        if not base_url.startswith("http://127.0.0.1:") or not path.startswith("/api/") or method not in {"GET", "POST"} or not token or len(token.encode("utf-8")) > 16 * 1024:
            raise QualificationCommandError(
                "cross-release provisioner probe is invalid",
            )
        body = (
            None
            if payload is None
            else json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            base_url.rstrip("/") + path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as exc:
            response = exc
        try:
            response.read()
            return int(response.status)
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()

    def _observe_until_terminal(
        self,
        client: _RuntimeHttpSession,
        run_id: str,
    ) -> dict[str, object]:
        observation: dict[str, object] = {}

        def terminal() -> bool:
            nonlocal observation
            status, payload = client.request(
                "GET",
                f"/api/runtime/v1/invocations/{run_id}?limit=500",
            )
            if status != 200:
                return False
            observation = payload
            return payload.get("status") in {
                "success",
                "error",
                "timeout",
                "interrupted",
            }

        wait_until(
            terminal,
            description=f"terminal invocation {run_id}",
            timeout_seconds=180,
            interval_seconds=1,
        )
        return observation

    def _run_state_version(self, run_id: str) -> int:
        if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id) is None:
            raise QualificationCommandError("qualification run id is unsafe")
        value = self._postgres(
            f"SELECT state_version FROM runs WHERE run_id='{run_id}'",
        )
        try:
            epoch = int(value)
        except ValueError as exc:
            raise QualificationCommandError("qualification run lease epoch is unavailable") from exc
        if epoch < 0:
            raise QualificationCommandError("qualification run lease epoch is invalid")
        return epoch

    def _run_status_version(self, run_id: str) -> tuple[str, int]:
        if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id) is None:
            raise QualificationCommandError("qualification run id is unsafe")
        payload = json.loads(
            self._postgres(
                f"SELECT json_build_object('status',status,'epoch',state_version)::text FROM runs WHERE run_id='{run_id}'",
            )
        )
        status = payload.get("status") if isinstance(payload, dict) else None
        epoch = payload.get("epoch") if isinstance(payload, dict) else None
        if not isinstance(status, str) or type(epoch) is not int or epoch < 0:
            raise QualificationCommandError(
                "qualification run status/version is unavailable",
            )
        return status, epoch

    def _wait_for_gateway_replacement(
        self,
        old_uid: str,
    ) -> tuple[dict[str, object], ...]:
        result: tuple[dict[str, object], ...] | None = None

        def replaced() -> bool:
            nonlocal result
            try:
                pods = self._gateway_pods()
            except QualificationCommandError:
                return False
            if old_uid in {self._metadata_value(pod, "uid") for pod in pods}:
                return False
            result = pods
            return True

        wait_until(
            replaced,
            description="replacement Gateway after SIGKILL",
            timeout_seconds=240,
            interval_seconds=2,
        )
        if result is None:
            raise QualificationTimeout("replacement Gateway result was unavailable")
        return result

    def _signal_gateway_process(self, pod_name: str, signal: str) -> int:
        if _SAFE_RESOURCE.fullmatch(pod_name) is None or signal not in {
            "STOP",
            "CONT",
        }:
            raise QualificationCommandError(
                "Gateway process signal target is unsafe",
            )
        target_state = "T" if signal == "STOP" else "running"
        script = "\n".join(
            (
                "import os, signal, time",
                "matches = []",
                "for name in os.listdir('/proc'):",
                "    if not name.isdigit() or int(name) in {os.getpid(), os.getppid()}:",
                "        continue",
                "    try:",
                "        raw = open(f'/proc/{name}/cmdline', 'rb').read()",
                "    except OSError:",
                "        continue",
                "    args = [item.decode(errors='replace') for item in raw.split(b'\\0') if item]",
                "    if any(item.rsplit('/', 1)[-1] == 'uvicorn' for item in args) and 'app.gateway.app:create_app' in args:",
                "        matches.append(int(name))",
                "if len(matches) != 1:",
                "    raise SystemExit(f'gateway-worker-match-count:{len(matches)}')",
                "pid = matches[0]",
                f"os.kill(pid, signal.SIG{signal})",
                "deadline = time.monotonic() + 5",
                "while True:",
                "    state = next(line.split()[1] for line in open(f'/proc/{pid}/status') if line.startswith('State:'))",
                ("    if state in {'T', 't'}: break" if signal == "STOP" else "    if state not in {'T', 't'}: break"),
                "    if time.monotonic() >= deadline: raise SystemExit('gateway-worker-signal-not-observed')",
                "    time.sleep(0.05)",
                f"print(f'pid={{pid}}:state={target_state}')",
            )
        )
        result = self._kubectl(
            "exec",
            pod_name,
            "--",
            "python",
            "-c",
            script,
            timeout_seconds=20,
        )
        match = re.fullmatch(
            rf"pid=([1-9][0-9]*):state={target_state}",
            result,
        )
        if match is None:
            raise QualificationCommandError(
                "Gateway worker signal verification was malformed",
            )
        return int(match.group(1))

    def _gateway_http_ready(self, pod_name: str) -> bool:
        try:
            with _PortForward(
                self.config,
                pod_name,
                resource_kind="pod",
            ) as forwarded:
                client = _RuntimeHttpSession(
                    f"http://127.0.0.1:{forwarded.port}",
                )
                status, payload = client.request("GET", "/ready")
        except (
            QualificationCommandError,
            urllib.error.URLError,
            TimeoutError,
        ):
            return False
        return status == 200 and payload.get("status") == "ready"

    def _partition_gateway_from_postgres(self, pod_name: str) -> str:
        """Block only one Gateway's PostgreSQL egress with NetworkPolicy."""

        if _SAFE_RESOURCE.fullmatch(pod_name) is None:
            raise QualificationCommandError(
                "Gateway partition target is unsafe",
            )
        policy_name = "mgq-pg-" + hashlib.sha256(f"{self.config.qualification_id}:{pod_name}".encode()).hexdigest()[:16]
        partition_label = "hartmesh.io/qualification-pg-partition"
        policy = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": policy_name},
            "spec": {
                "podSelector": {
                    "matchLabels": {partition_label: "true"},
                },
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "podSelector": {
                                    "matchLabels": {
                                        "app": self.redis_name,
                                    }
                                }
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 6379}],
                    },
                    {
                        "to": [
                            {
                                "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": ("kube-system")}},
                                "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                            }
                        ],
                        "ports": [
                            {"protocol": "UDP", "port": 53},
                            {"protocol": "TCP", "port": 53},
                        ],
                    },
                ],
            },
        }
        self._kubectl(
            "apply",
            "-f",
            "-",
            input_text=yaml.safe_dump(policy, sort_keys=True),
        )
        self._kubectl(
            "label",
            "pod",
            pod_name,
            f"{partition_label}=true",
            "--overwrite",
        )
        peer_name = next(self._metadata_value(pod, "name") for pod in self._gateway_pods(require_ready=False) if self._metadata_value(pod, "name") != pod_name)
        wait_until(
            lambda: not self._gateway_http_ready(pod_name) and self._gateway_http_ready(peer_name),
            description="owner-only PostgreSQL network partition",
            timeout_seconds=90,
            interval_seconds=2,
        )
        return policy_name

    def _restore_gateway_postgres(
        self,
        pod_name: str,
        policy_name: str,
    ) -> None:
        partition_label = "hartmesh.io/qualification-pg-partition"
        self._kubectl(
            "label",
            "pod",
            pod_name,
            f"{partition_label}-",
            "--overwrite",
        )
        self._kubectl(
            "delete",
            "networkpolicy",
            policy_name,
            "--ignore-not-found",
        )

    def _stale_control_rejected(
        self,
        client: _RuntimeHttpSession,
        run_id: str,
        state_version: int,
    ) -> bool:
        stale_version = max(0, state_version - 1)
        status, _payload = client.request(
            "POST",
            f"/api/runtime/v1/invocations/{run_id}/control",
            payload={
                "api_version": "deerflow.runtime/v1",
                "kind": "invocation.cancel",
                "run_id": run_id,
                "expected_state_version": stale_version,
            },
        )
        return status == 409

    @staticmethod
    def _event_counts(observation: Mapping[str, object]) -> tuple[int, int]:
        events = observation.get("events")
        if not isinstance(events, list):
            raise QualificationCommandError("qualification observation omitted lifecycle events")
        lifecycle = [item.get("lifecycle_type") for item in events if isinstance(item, dict)]
        terminal = {
            "cancelled",
            "succeeded",
            "failed",
            "timed_out",
            "interrupted",
        }
        authoritative = int(lifecycle.count("accepted") == 1) * int(sum(item in terminal for item in lifecycle) == 1)
        duplicates = max(0, lifecycle.count("accepted") - 1) + max(
            0,
            sum(item in terminal for item in lifecycle) - 1,
        )
        return authoritative, duplicates

    def _assert_terminal_cleanup(
        self,
        run_id: str,
        *,
        expected_status: str,
    ) -> None:
        if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id) is None:
            raise QualificationCommandError("qualification run id is unsafe")
        facts = json.loads(
            self._postgres(
                "SELECT json_build_object("
                "'status',target.status,"
                "'terminal_events',(SELECT count(*) FROM run_lifecycle_events "
                "WHERE run_id=target.run_id AND lifecycle_type IN "
                "('cancelled','succeeded','failed','timed_out','interrupted')),"
                "'active_auxiliary',(SELECT count(*) FROM runs auxiliary "
                "WHERE auxiliary.thread_id=target.thread_id "
                "AND auxiliary.operation_kind<>'run' "
                "AND auxiliary.status IN ('pending','running'))"
                ",'owner_lease_released',CASE WHEN target.owner_worker_id IS NULL "
                "AND target.lease_expires_at IS NULL THEN 1 ELSE 0 END)::text "
                f"FROM runs target WHERE target.run_id='{run_id}'",
            )
        )
        if not isinstance(facts, dict) or facts.get("status") != expected_status or facts.get("terminal_events") != 1 or facts.get("active_auxiliary") != 0 or facts.get("owner_lease_released") != 1:
            raise QualificationCommandError(
                "cross-pod finalization did not converge with complete cleanup",
            )
        self._assert_accepted_attempt_renewal_stopped(run_id)

    def _accepted_attempt_renewal_marker(
        self,
        run_id: str,
    ) -> tuple[str, str] | None:
        document = json.loads(self._kubectl("get", "leases", "-o", "json"))
        items = document.get("items") if isinstance(document, dict) else None
        for item in items if isinstance(items, list) else []:
            metadata = item.get("metadata") if isinstance(item, dict) else None
            annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
            if (
                not isinstance(annotations, dict)
                or annotations.get(
                    "hartmesh.io/accepted-skill-run",
                )
                != run_id
            ):
                continue
            uid = metadata.get("uid")
            renew_time = item.get("spec", {}).get("renewTime")
            if not isinstance(uid, str) or not isinstance(renew_time, str):
                raise QualificationCommandError(
                    "accepted-material cleanup marker is malformed",
                )
            return uid, renew_time
        return None

    def _assert_accepted_attempt_renewal_stopped(self, run_id: str) -> None:
        """Prove terminal cleanup releases accepted Lease and sandbox Pod."""

        def released() -> bool:
            lease_absent = self._accepted_attempt_renewal_marker(run_id) is None
            pods = json.loads(self._kubectl("get", "pods", "-o", "json"))
            items = pods.get("items") if isinstance(pods, dict) else None
            sandbox_absent = not any(
                isinstance(item, dict)
                and isinstance(item.get("metadata"), dict)
                and isinstance(item["metadata"].get("annotations"), dict)
                and item["metadata"]["annotations"].get(
                    "hartmesh.io/accepted-skill-run",
                )
                == run_id
                for item in items
                if isinstance(items, list)
            )
            return lease_absent and sandbox_absent

        wait_until(
            released,
            description=f"accepted material cleanup for {run_id}",
            timeout_seconds=90,
            interval_seconds=2,
        )

    @staticmethod
    def _resource_for_run(
        document: object,
        *,
        run_id: str,
        annotation: str,
        kind: str,
    ) -> dict[str, object]:
        items = document.get("items") if isinstance(document, dict) else None
        matches: list[dict[str, object]] = []
        for item in items if isinstance(items, list) else []:
            metadata = item.get("metadata") if isinstance(item, dict) else None
            annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
            if isinstance(annotations, dict) and annotations.get(annotation) == run_id:
                matches.append(item)
        if len(matches) != 1:
            raise QualificationCommandError(f"qualification requires exactly one {kind} for the run")
        return matches[0]

    def _accepted_skill_attempt_facts(
        self,
        run_id: str,
    ) -> dict[str, str | int]:
        """Verify live RWX material, Lease identity, images, and ledger proof."""

        pod = self._resource_for_run(
            json.loads(
                self._kubectl(
                    "get",
                    "pods",
                    "-l",
                    "app.kubernetes.io/component=sandbox",
                    "-o",
                    "json",
                )
            ),
            run_id=run_id,
            annotation="hartmesh.io/accepted-skill-run",
            kind="accepted-skill sandbox pod",
        )
        lease = self._resource_for_run(
            json.loads(self._kubectl("get", "leases", "-o", "json")),
            run_id=run_id,
            annotation="hartmesh.io/accepted-skill-run",
            kind="accepted-skill Lease",
        )
        pod_metadata = pod.get("metadata")
        lease_metadata = lease.get("metadata")
        if not isinstance(pod_metadata, dict) or not isinstance(
            lease_metadata,
            dict,
        ):
            raise QualificationCommandError("accepted-skill resource metadata is malformed")
        annotations = lease_metadata.get("annotations")
        labels = pod_metadata.get("labels")
        if not isinstance(annotations, dict) or not isinstance(labels, dict):
            raise QualificationCommandError("accepted-skill evidence labels are unavailable")
        pod_name = pod_metadata.get("name")
        pod_uid = pod_metadata.get("uid")
        lease_uid = lease_metadata.get("uid")
        sandbox_id = labels.get("sandbox-id")
        if not all(isinstance(value, str) and _SAFE_RESOURCE.fullmatch(value) for value in (pod_name, pod_uid, lease_uid, sandbox_id)):
            raise QualificationCommandError("accepted-skill live identities are invalid")
        if labels.get("hartmesh.io/accepted-skill-profile") != "rwx_verified_copy_v2" or annotations.get("hartmesh.io/accepted-attempt-state") != "materialized":
            raise QualificationCommandError("accepted-skill attempt was not materialized under v2")
        receipt = json.loads(
            self._kubectl(
                "exec",
                str(pod_name),
                "-c",
                "accepted-skill-gate",
                "--",
                "cat",
                "/var/run/hartmesh/accepted-receipt/receipt.json",
            )
        )
        if not isinstance(receipt, dict) or receipt.get("version") != 2 or receipt.get("profile") != "rwx_verified_copy_v2" or receipt.get("snapshot_id") != receipt.get("content_digest"):
            raise QualificationCommandError("accepted-skill verifier receipt is incomplete")
        snapshot_id = receipt.get("snapshot_id")
        if not isinstance(snapshot_id, str) or re.fullmatch(r"[0-9a-f]{64}", snapshot_id) is None:
            raise QualificationCommandError("accepted-skill snapshot identity is invalid")
        for relative, expected in _QUALIFICATION_SKILL_FILES.items():
            path = f"/mnt/skills/.accepted/{snapshot_id}/public/{_QUALIFICATION_SKILL_NAME}/{relative}"
            encoded = self._kubectl(
                "exec",
                str(pod_name),
                "-c",
                "sandbox",
                "--",
                "python",
                "-c",
                (f"import base64,pathlib;print(base64.b64encode(pathlib.Path({path!r}).read_bytes()).decode('ascii'))"),
            )
            if base64.b64decode(encoded) != expected:
                raise QualificationCommandError("accepted-skill sandbox bytes differ from the fixture")
        if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run_id) is None:
            raise QualificationCommandError("qualification run id is unsafe")
        row = self._postgres(f"SELECT json_build_object('evidence',execution_evidence_json,'digest',execution_evidence_digest)::text FROM runs WHERE run_id='{run_id}'")
        ledger = json.loads(row)
        evidence = ledger.get("evidence") if isinstance(ledger, dict) else None
        evidence_digest = ledger.get("digest") if isinstance(ledger, dict) else None
        epoch = annotations.get("hartmesh.io/accepted-skill-generation")
        if (
            not isinstance(evidence, dict)
            or not isinstance(evidence_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", evidence_digest) is None
            or evidence.get("run_id") != run_id
            or evidence.get("provider_kind") != "aio_kubernetes"
            or evidence.get("provider_instance_ref") != sandbox_id
            or evidence.get("skill_snapshot_digest") != snapshot_id
            or str(evidence.get("ownership_epoch")) != str(epoch)
        ):
            raise QualificationCommandError("accepted-skill durable ledger differs from live material")
        return {
            "sandbox_id": str(sandbox_id),
            "sandbox_pod_uid": str(pod_uid),
            "accepted_lease_uid": str(lease_uid),
            "snapshot_digest": snapshot_id,
            "execution_evidence_digest": evidence_digest,
            "ownership_epoch": int(str(epoch)),
        }

    def _barrier_invocation(
        self,
        scenario_id: str,
        *,
        kill_owner: bool = False,
        cancel: bool = False,
        require_conflict: bool = False,
        barrier_probe: Callable[[str], None] | None = None,
        barrier_fault: Callable[[str], None] | None = None,
        terminal_probe: Callable[[str, Mapping[str, object]], None] | None = None,
        require_stale_rejection: bool = False,
        dependency_interruption_count: int = 0,
        barrier_point: str = "during_model_execution",
        delivery_id: str = "delivery-1",
        simultaneous_admission: bool = False,
        partition_owner: bool = False,
        stale_counter_names: tuple[str, ...] = (),
        arm_stale_external_renewal: bool = False,
    ) -> MultiGatewayScenarioObservationV1:
        started = time.monotonic()
        barrier_hits_before = self._arm_invocation_barrier(
            scenario_id,
            barrier_point,
        )
        pods = self._gateway_pods()
        names = tuple(self._metadata_value(pod, "name") for pod in pods)
        payload = self._ensure_payload(
            scenario_id,
            delivery_id=delivery_id,
        )
        request_results: dict[int, tuple[int, dict[str, object]]] = {}
        request_errors: dict[int, str] = {}
        deleted_uid: str | None = None
        epoch_before = 1
        epoch_after = 1
        stale_rejections = 0
        takeover_count = 0
        paused_owner_name: str | None = None
        paused_owner_pid: int | None = None
        partition_policy_name: str | None = None
        stale_counter_baselines = {name: self._counter(scenario_id, name) for name in stale_counter_names}
        with ExitStack() as stack:
            forwards = tuple(
                stack.enter_context(
                    _PortForward(
                        self.config,
                        name,
                        resource_kind="pod",
                    )
                )
                for name in names
            )
            clients = tuple(_RuntimeHttpSession(f"http://127.0.0.1:{forward.port}") for forward in forwards)
            for client in clients:
                self._login(client)

            concurrent_admission = scenario_id == "concurrent_admission" or simultaneous_admission
            start_gate = threading.Barrier(2) if concurrent_admission else None

            def ensure(client_index: int) -> None:
                try:
                    if start_gate is not None:
                        start_gate.wait(timeout=10)
                    request_results[client_index] = clients[client_index].request(
                        "POST",
                        "/api/runtime/v1/invocations/ensure",
                        payload=payload,
                        timeout_seconds=210,
                    )
                except Exception as exc:  # serving pod may be killed
                    request_errors[client_index] = type(exc).__name__

            initial_request_count = 2 if concurrent_admission else 1
            with ThreadPoolExecutor(max_workers=initial_request_count) as executor:
                futures = tuple(executor.submit(ensure, index) for index in range(initial_request_count))
                run_id = self._wait_for_barrier(scenario_id)
                if (
                    self._redis(
                        "GET",
                        f"{self._qualification_prefix(scenario_id)}:reached_point",
                    )
                    != barrier_point
                ):
                    raise QualificationCommandError(
                        "qualification reached an unselected owner-loss window",
                    )
                self._scenario_run_ids[scenario_id] = run_id
                epoch_before = self._run_state_version(run_id)
                if barrier_probe is not None:
                    barrier_probe(run_id)
                replay_status, replay = clients[1].request(
                    "POST",
                    "/api/runtime/v1/invocations/ensure",
                    payload=payload,
                )
                if replay_status != 200 or replay.get("run_id") != run_id or replay.get("disposition") != "known":
                    raise QualificationCommandError("cross-pod idempotent admission did not converge")
                if require_conflict:
                    conflicting = json.loads(json.dumps(payload))
                    conflicting["thread_id"] = f"multi-gateway-{scenario_id}-conflict"
                    conflict_statuses = tuple(
                        client.request(
                            "POST",
                            "/api/runtime/v1/invocations/ensure",
                            payload=conflicting,
                        )[0]
                        for client in clients
                    )
                    if conflict_statuses != (409, 409):
                        raise QualificationCommandError("conflicting admission was not stable across pods")
                if cancel:
                    query_status, current = clients[1].request(
                        "GET",
                        f"/api/runtime/v1/invocations/{run_id}",
                    )
                    current_version = current.get("state_version")
                    if query_status != 200 or not isinstance(current_version, int):
                        raise QualificationCommandError("cancellation race could not read state version")
                    cancel_status, _receipt = clients[1].request(
                        "POST",
                        f"/api/runtime/v1/invocations/{run_id}/control",
                        payload={
                            "api_version": "deerflow.runtime/v1",
                            "kind": "invocation.cancel",
                            "run_id": run_id,
                            "expected_state_version": current_version,
                        },
                    )
                    if cancel_status != 200:
                        raise QualificationCommandError("cross-pod cancellation was not accepted")
                if kill_owner or partition_owner:
                    owner = self._redis(
                        "GET",
                        f"{self._qualification_prefix(scenario_id)}:owner_replica_id",
                    )
                    owner_pod = next(
                        (pod for pod in pods if self._metadata_value(pod, "name") == owner),
                        None,
                    )
                    if owner_pod is None:
                        raise QualificationCommandError("qualification barrier did not identify the owning pod")
                    if kill_owner:
                        deleted_uid = self._metadata_value(owner_pod, "uid")
                        self._kubectl(
                            "delete",
                            "pod",
                            owner,
                            "--grace-period=0",
                            "--force",
                            "--wait=false",
                            timeout_seconds=30,
                        )
                        self._wait_for_gateway_replacement(deleted_uid)
                    else:
                        partition_policy_name = self._partition_gateway_from_postgres(owner)
                        stack.callback(
                            self._restore_gateway_postgres,
                            owner,
                            partition_policy_name,
                        )

                if barrier_fault is not None:
                    barrier_fault(run_id)

                if kill_owner or partition_owner:
                    prefix = self._qualification_prefix(scenario_id)
                    if self._redis("SET", f"{prefix}:arm", "1", "EX", "300") != "OK":
                        raise QualificationCommandError(
                            "takeover barrier could not be re-armed",
                        )
                    if (
                        arm_stale_external_renewal
                        and self._redis(
                            "SET",
                            f"{prefix}:stale_external_renewal_arm",
                            "1",
                            "EX",
                            "300",
                        )
                        != "OK"
                    ):
                        raise QualificationCommandError(
                            "stale external renewal probe could not be armed",
                        )

                    def takeover_reached() -> bool:
                        return (
                            self._counter(
                                scenario_id,
                                "barrier_hits",
                            )
                            >= barrier_hits_before + 2
                        )

                    wait_until(
                        takeover_reached,
                        description=f"takeover barrier {scenario_id}",
                        timeout_seconds=120,
                        interval_seconds=1,
                    )
                    takeover_count = 1
                    if partition_owner:
                        paused_owner_name = owner
                        paused_owner_pid = self._signal_gateway_process(
                            owner,
                            "STOP",
                        )
                        stack.callback(
                            self._signal_gateway_process,
                            owner,
                            "CONT",
                        )
                self._release_barrier(scenario_id)

                if partition_owner:

                    def peer_terminalized() -> bool:
                        status, state_version = self._run_status_version(run_id)
                        return status in {"success", "error", "timeout", "interrupted"} and state_version > epoch_before

                    wait_until(
                        peer_terminalized,
                        description=f"peer terminal takeover {scenario_id}",
                        timeout_seconds=120,
                        interval_seconds=1,
                    )
                    assert paused_owner_name is not None
                    assert partition_policy_name is not None
                    self._restore_gateway_postgres(
                        paused_owner_name,
                        partition_policy_name,
                    )
                    resumed_owner_pid = self._signal_gateway_process(
                        paused_owner_name,
                        "CONT",
                    )
                    if resumed_owner_pid != paused_owner_pid:
                        raise QualificationCommandError(
                            "Gateway worker identity changed across stale return",
                        )
                    for counter_name, baseline in stale_counter_baselines.items():
                        wait_until(
                            lambda name=counter_name, before=baseline: self._counter(scenario_id, name) > before,
                            description=(f"stale {counter_name} rejection {scenario_id}"),
                            timeout_seconds=60,
                            interval_seconds=1,
                        )
                for future in futures:
                    try:
                        future.result(timeout=220)
                    except Exception:
                        if not (kill_owner or partition_owner):
                            raise

                if concurrent_admission:
                    if request_errors or set(request_results) != {0, 1}:
                        raise QualificationCommandError(
                            "simultaneous cross-pod admission did not complete",
                        )
                    initial_receipts = tuple(request_results[index] for index in range(2))
                    if any(status != 200 or receipt.get("run_id") != run_id for status, receipt in initial_receipts) or {receipt.get("disposition") for _status, receipt in initial_receipts} != {"created", "known"}:
                        raise QualificationCommandError(
                            "simultaneous cross-pod admission did not elect one creator",
                        )
                elif not (kill_owner or partition_owner):
                    response = request_results.get(0)
                    if request_errors or response is None or response[0] != 200 or response[1].get("run_id") != run_id:
                        raise QualificationCommandError(
                            "qualification invocation did not return its durable run",
                        )

        current_pods = self._gateway_pods()
        with _PortForward(
            self.config,
            self._metadata_value(current_pods[0], "name"),
            resource_kind="pod",
        ) as forwarded:
            client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            self._login(client)
            observation = self._observe_until_terminal(client, run_id)
            state_version = observation.get("state_version")
            if not isinstance(state_version, int):
                raise QualificationCommandError("terminal observation omitted state version")
            epoch_after = self._run_state_version(run_id)
            if kill_owner:
                if epoch_after <= epoch_before:
                    raise QualificationCommandError("owner loss did not advance the lease epoch")
            if kill_owner or require_stale_rejection:
                if not self._stale_control_rejected(
                    client,
                    run_id,
                    state_version,
                ):
                    raise QualificationCommandError("stale state-version write was not rejected")
                stale_rejections = 1
            stale_counter_rejections = sum(self._counter(scenario_id, name) - baseline for name, baseline in stale_counter_baselines.items())
            stale_rejections += stale_counter_rejections
            final_status, final_replay = client.request(
                "POST",
                "/api/runtime/v1/invocations/ensure",
                payload=payload,
            )
            if final_status != 200 or final_replay.get("run_id") != run_id or final_replay.get("disposition") != "known":
                raise QualificationCommandError("terminal admission replay was not stable")
            if terminal_probe is not None:
                terminal_probe(run_id, observation)
        authoritative, duplicates = self._event_counts(observation)
        if authoritative != 1 or duplicates != 0:
            raise QualificationCommandError("invocation lifecycle is not single-authority")
        if scenario_id == "execution_ownership" and (self._counter(scenario_id, "model_starts") != 1 or self._counter(scenario_id, "graph_starts") != 1):
            raise QualificationCommandError("execution was performed by more than one worker")
        return MultiGatewayScenarioObservationV1(
            scenario_id=scenario_id,
            input_facts={
                "delivery": delivery_id,
                "barrier_point": barrier_point,
                "direct_pod_count": 2,
                "simultaneous_requests": initial_request_count,
                "fault": ("owner_sigkill" if kill_owner else "owner_postgresql_network_partition" if partition_owner else "cancel_race" if cancel else "none"),
            },
            evidence_facts={
                "run_id": run_id,
                "terminal_status": str(observation.get("status")),
                "topology_digest": (self._subjects.topology_registrations[0].topology_fingerprint.digest if self._subjects is not None else "unavailable"),
                "model_starts": self._counter(scenario_id, "model_starts"),
                "stale_counter_rejections": stale_counter_rejections,
                "owner_process_stop_verified": bool(partition_owner and paused_owner_pid is not None),
            },
            authoritative_count=1,
            duplicate_count=0,
            stale_write_rejections=stale_rejections,
            takeover_count=takeover_count,
            pod_deletion_count=int(deleted_uid is not None),
            pod_restart_count=int(deleted_uid is not None),
            lease_epoch_before=epoch_before,
            lease_epoch_after=epoch_after,
            dependency_interruption_count=dependency_interruption_count,
            duration_millis=max(1, round((time.monotonic() - started) * 1000)),
        )

    @staticmethod
    def _observation(
        scenario_id: str,
        *,
        started: float,
        input_facts: Mapping[str, str | int | bool],
        evidence_facts: Mapping[str, str | int | bool],
        authoritative_count: int = 1,
        duplicate_count: int = 0,
        stale_write_rejections: int = 0,
        takeover_count: int = 0,
        pod_deletion_count: int = 0,
        pod_restart_count: int = 0,
        lease_epoch_before: int = 0,
        lease_epoch_after: int = 0,
        dependency_interruption_count: int = 0,
        verified_case_count: int = 1,
        cleanup_count: int = 0,
        retryable_failure_count: int = 0,
    ) -> MultiGatewayScenarioObservationV1:
        return MultiGatewayScenarioObservationV1(
            scenario_id=scenario_id,
            input_facts=input_facts,
            evidence_facts=evidence_facts,
            authoritative_count=authoritative_count,
            duplicate_count=duplicate_count,
            stale_write_rejections=stale_write_rejections,
            takeover_count=takeover_count,
            pod_deletion_count=pod_deletion_count,
            pod_restart_count=pod_restart_count,
            lease_epoch_before=lease_epoch_before,
            lease_epoch_after=lease_epoch_after,
            dependency_interruption_count=dependency_interruption_count,
            duration_millis=max(
                1,
                round((time.monotonic() - started) * 1000),
            ),
            verified_case_count=verified_case_count,
            cleanup_count=cleanup_count,
            retryable_failure_count=retryable_failure_count,
        )

    def _scale_deployment(self, name: str, replicas: int) -> None:
        self._kubectl(
            "scale",
            f"deployment/{name}",
            f"--replicas={replicas}",
            timeout_seconds=60,
        )
        if replicas:
            self._kubectl(
                "rollout",
                "status",
                f"deployment/{name}",
                "--timeout=4m",
                timeout_seconds=250,
            )

    def _wait_gateway_http_readiness(
        self,
        *,
        expected_ready: bool,
        expected_ready_count: int | None = None,
    ) -> None:
        def expected() -> bool:
            try:
                pods = self._gateway_pods(require_ready=False)
            except QualificationCommandError:
                return False
            observed: list[bool] = []
            for pod in pods:
                name = self._metadata_value(pod, "name")
                try:
                    with _PortForward(
                        self.config,
                        name,
                        resource_kind="pod",
                    ) as forwarded:
                        client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                        status, payload = client.request("GET", "/ready")
                    observed.append(status == 200 and payload.get("status") == "ready")
                except QualificationCommandError:
                    observed.append(False)
            if expected_ready_count is not None:
                return len(observed) == 2 and sum(observed) == expected_ready_count
            return bool(observed) and all(value is expected_ready for value in observed)

        wait_until(
            expected,
            description=(
                f"exactly {expected_ready_count} Gateway readiness probes ready" if expected_ready_count is not None else "both Gateway readiness probes to become ready" if expected_ready else "both Gateway readiness probes to fail closed"
            ),
            timeout_seconds=180,
            interval_seconds=2,
        )

    def _retryable_sse_transport_failure(
        self,
        *,
        thread_id: str,
        run_id: str,
        last_event_id: str,
    ) -> tuple[int, ...]:
        """Require an exact retryable response from the Redis-backed SSE API."""

        if _SAFE_RESOURCE.fullmatch(thread_id) is None or _SAFE_RESOURCE.fullmatch(run_id) is None:
            raise QualificationCommandError(
                "Redis outage SSE target is unsafe",
            )
        self._redis_stream_id(last_event_id)
        path = f"/api/threads/{thread_id}/runs/{run_id}/stream"

        statuses: list[int] = []
        for pod in self._gateway_pods(require_ready=False):
            name = self._metadata_value(pod, "name")
            with _PortForward(
                self.config,
                name,
                resource_kind="pod",
            ) as forwarded:
                client = _RuntimeHttpSession(
                    f"http://127.0.0.1:{forwarded.port}",
                )
                self._login(client)
                request = urllib.request.Request(
                    client.base_url + path,
                    headers={
                        "Accept": "text/event-stream",
                        "Last-Event-ID": last_event_id,
                    },
                    method="GET",
                )
                try:
                    response = client._opener.open(request, timeout=10)
                except urllib.error.HTTPError as exc:
                    status = exc.code
                    retry_after = exc.headers.get("Retry-After")
                    exc.close()
                except (urllib.error.URLError, TimeoutError) as exc:
                    raise QualificationCommandError(
                        "Redis outage SSE request lacked an HTTP retry boundary",
                    ) from exc
                else:
                    response.close()
                    raise QualificationCommandError(
                        "Redis outage SSE request unexpectedly opened a stream",
                    )
            if status != 503 or retry_after != "1":
                raise QualificationCommandError(
                    "Redis outage did not return the exact retryable SSE failure",
                )
            statuses.append(status)
        if len(statuses) != 2:
            raise QualificationCommandError(
                "Redis outage did not exercise both direct Gateway SSE routes",
            )
        return tuple(statuses)

    @staticmethod
    def _read_sse_frame(response: Any) -> dict[str, str]:
        fields: dict[str, str] = {}
        while True:
            raw = response.readline()
            if not raw:
                raise QualificationCommandError("SSE connection ended before a complete frame")
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line:
                if fields:
                    return fields
                continue
            if line.startswith(":"):
                continue
            name, separator, value = line.partition(":")
            if separator:
                fields[name] = value.lstrip()

    @staticmethod
    def _open_sse(
        client: _RuntimeHttpSession,
        path: str,
        *,
        last_event_id: str | None = None,
        timeout_seconds: float = 180,
    ) -> Any:
        headers = {"Accept": "text/event-stream"}
        if last_event_id is not None:
            headers["Last-Event-ID"] = last_event_id
        request = urllib.request.Request(
            client.base_url + path,
            headers=headers,
            method="GET",
        )
        try:
            response = client._opener.open(request, timeout=timeout_seconds)
        except urllib.error.HTTPError as exc:
            raise QualificationCommandError(f"SSE endpoint returned HTTP {exc.code}") from exc
        if response.status != 200 or not str(response.headers.get("Content-Type", "")).startswith("text/event-stream"):
            response.close()
            raise QualificationCommandError("Gateway did not open an SSE event stream")
        return response

    @staticmethod
    def _redis_stream_id(value: str) -> tuple[int, int]:
        if re.fullmatch(r"[0-9]+-[0-9]+", value) is None:
            raise QualificationCommandError("SSE event ID is not a Redis ID")
        milliseconds, sequence = value.split("-", 1)
        return int(milliseconds), int(sequence)

    def _topology_identity(self) -> MultiGatewayScenarioObservationV1:
        started = time.monotonic()
        if self._subjects is None:
            raise QualificationPrerequisiteError("topology identity requires prepared cluster subjects")
        registrations = self._subjects.topology_registrations
        digest = registrations[0].topology_fingerprint.digest
        return MultiGatewayScenarioObservationV1(
            scenario_id="topology_identity",
            input_facts={"expected_replicas": 2, "route_modes": "direct,service"},
            evidence_facts={
                "replica_0": registrations[0].replica_id,
                "replica_1": registrations[1].replica_id,
                "topology_digest": digest,
            },
            authoritative_count=2,
            duplicate_count=0,
            stale_write_rejections=0,
            takeover_count=0,
            pod_deletion_count=0,
            pod_restart_count=0,
            lease_epoch_before=0,
            lease_epoch_after=0,
            dependency_interruption_count=0,
            duration_millis=max(1, round((time.monotonic() - started) * 1000)),
            verified_case_count=2,
        )

    def _concurrent_admission(self) -> MultiGatewayScenarioObservationV1:
        return self._barrier_invocation(
            "concurrent_admission",
            require_conflict=True,
        )

    def _execution_ownership(self) -> MultiGatewayScenarioObservationV1:
        return self._barrier_invocation("execution_ownership")

    def _owner_sigkill(self) -> MultiGatewayScenarioObservationV1:
        started = time.monotonic()
        windows = (
            ("accepted_before_materialization", "before-materialization"),
            (
                "post_materialization_before_checkpoint",
                "after-materialization",
            ),
            ("post_checkpoint_before_graph", "after-checkpoint"),
            ("during_model_execution", "during-model"),
            ("during_tool_execution", "during-tool"),
            ("terminal_before_lifecycle_commit", "before-terminal"),
        )
        observations = tuple(
            self._barrier_invocation(
                "owner_sigkill",
                kill_owner=True,
                barrier_point=point,
                delivery_id=delivery_id,
            )
            for point, delivery_id in windows
        )
        run_ids = tuple(str(observation.evidence_facts["run_id"]) for observation in observations)
        tool_starts = self._counter("owner_sigkill", "tool_starts")
        tool_completions = self._counter("owner_sigkill", "tool_completions")
        if tool_starts < 2 or tool_completions != 1:
            raise QualificationCommandError(
                "tool-window recovery lacked one completed fenced attempt",
            )
        return self._observation(
            "owner_sigkill",
            started=started,
            input_facts={
                "controlled_window_count": len(windows),
                "controlled_windows": ",".join(point for point, _delivery_id in windows),
                "fault": "owner_sigkill",
            },
            evidence_facts={
                "run_id_digest": "sha256:" + hashlib.sha256("\n".join(run_ids).encode()).hexdigest(),
                "recovered_window_count": len(observations),
                "stale_owner_probe_count": sum(observation.stale_write_rejections for observation in observations),
                "terminal_states": ",".join(str(observation.evidence_facts["terminal_status"]) for observation in observations),
                "tool_starts": tool_starts,
                "tool_completions": tool_completions,
            },
            authoritative_count=len(observations),
            stale_write_rejections=sum(observation.stale_write_rejections for observation in observations),
            takeover_count=sum(observation.takeover_count for observation in observations),
            pod_deletion_count=sum(observation.pod_deletion_count for observation in observations),
            pod_restart_count=sum(observation.pod_restart_count for observation in observations),
            lease_epoch_before=observations[0].lease_epoch_before,
            lease_epoch_after=max(observation.lease_epoch_after for observation in observations),
            verified_case_count=len(observations),
        )

    def _sse_reconnect(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "sse_reconnect"
        started = time.monotonic()
        pods = self._gateway_pods()
        names = tuple(self._metadata_value(pod, "name") for pod in pods)
        deleted_uid = self._metadata_value(pods[0], "uid")
        payload = self._ensure_payload(scenario_id)
        first_event_id = ""
        run_id = ""
        with ExitStack() as stack:
            forwards = tuple(
                stack.enter_context(
                    _PortForward(
                        self.config,
                        name,
                        resource_kind="pod",
                    )
                )
                for name in names
            )
            clients = tuple(_RuntimeHttpSession(f"http://127.0.0.1:{item.port}") for item in forwards)
            for client in clients:
                self._login(client)

            def ensure() -> None:
                clients[0].request(
                    "POST",
                    "/api/runtime/v1/invocations/ensure",
                    payload=payload,
                    timeout_seconds=210,
                )

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ensure)
                run_id = self._wait_for_barrier(scenario_id)
                self._scenario_run_ids[scenario_id] = run_id
                path = f"/api/threads/{payload['thread_id']}/runs/{run_id}/stream"
                response = self._open_sse(clients[0], path)
                try:
                    for _ in range(32):
                        frame = self._read_sse_frame(response)
                        candidate = frame.get("id")
                        if candidate:
                            first_event_id = candidate
                            break
                    if not first_event_id:
                        raise QualificationCommandError("pod A SSE stream emitted no resumable event ID")
                    self._kubectl(
                        "delete",
                        "pod",
                        names[0],
                        "--grace-period=0",
                        "--force",
                        "--wait=false",
                        timeout_seconds=30,
                    )
                    self._wait_for_gateway_replacement(deleted_uid)
                finally:
                    response.close()
                self._release_barrier(scenario_id)
                try:
                    future.result(timeout=30)
                except Exception:
                    pass

        replacement = self._gateway_pods()
        resume_pod = next(pod for pod in replacement if self._metadata_value(pod, "name") != names[0])
        with _PortForward(
            self.config,
            self._metadata_value(resume_pod, "name"),
            resource_kind="pod",
        ) as forwarded:
            client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            self._login(client)
            observation = self._observe_until_terminal(client, run_id)
            response = self._open_sse(
                client,
                (f"/api/threads/{payload['thread_id']}/runs/{run_id}/stream"),
                last_event_id=first_event_id,
            )
            resumed_ids: list[str] = []
            saw_end = False
            try:
                for _ in range(256):
                    frame = self._read_sse_frame(response)
                    if event_id := frame.get("id"):
                        resumed_ids.append(event_id)
                    if frame.get("event") == "gap":
                        raise QualificationCommandError("cross-pod SSE reconnect reported a replay gap")
                    if frame.get("event") == "end":
                        saw_end = True
                        break
            finally:
                response.close()
        if not saw_end or not resumed_ids:
            raise QualificationCommandError("pod B SSE reconnect did not reach the retained end marker")
        ordered = [self._redis_stream_id(item) for item in resumed_ids]
        if len(set(resumed_ids)) != len(resumed_ids) or ordered != sorted(ordered) or any(item <= self._redis_stream_id(first_event_id) for item in ordered):
            raise QualificationCommandError("cross-pod SSE cursor replay was duplicated or out of order")
        authoritative, duplicates = self._event_counts(observation)
        if authoritative != 1 or duplicates:
            raise QualificationCommandError("SSE recovery durable lifecycle is not single-authority")
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "direct_pod_count": 2,
                "reconnect_cursor": True,
                "serving_pod_deleted": True,
            },
            evidence_facts={
                "run_id": run_id,
                "first_event_id": first_event_id,
                "resumed_event_count": len(resumed_ids),
                "terminal_status": str(observation.get("status")),
            },
            pod_deletion_count=1,
            pod_restart_count=1,
        )

    def _create_once_task(
        self,
        client: _RuntimeHttpSession,
        *,
        title: str,
        run_at: datetime,
    ) -> str:
        status, payload = client.request(
            "POST",
            "/api/scheduled-tasks",
            payload={
                "context_mode": "fresh_thread_per_run",
                "title": title,
                "prompt": "deterministic scheduled qualification",
                "schedule_type": "once",
                "schedule_spec": {"run_at": run_at.isoformat()},
                "timezone": "UTC",
            },
        )
        task_id = payload.get("id")
        if status != 200 or not isinstance(task_id, str) or re.fullmatch(r"task-[0-9a-f]{32}", task_id) is None:
            raise QualificationCommandError("scheduled qualification task creation failed")
        return task_id

    def _scheduled_counts(self, task_ids: tuple[str, ...]) -> dict[str, int]:
        if not task_ids or any(re.fullmatch(r"task-[0-9a-f]{32}", task_id) is None for task_id in task_ids):
            raise QualificationCommandError("scheduled qualification task IDs are invalid")
        quoted = ",".join(f"'{item}'" for item in task_ids)
        raw = self._postgres(
            "SELECT json_build_object("
            "'total',count(*),'distinct_occurrences',count(DISTINCT id),"
            "'distinct_runs',count(DISTINCT run_id),"
            "'active',count(*) FILTER (WHERE status IN "
            "('launching','running')),'terminal',count(*) FILTER "
            f"(WHERE status IN ('success','failed','skipped','interrupted'))) "
            "FROM scheduled_task_runs WHERE task_id IN "
            f"({quoted})"
        )
        value = json.loads(raw)
        if not isinstance(value, dict) or any(
            not isinstance(value.get(name), int)
            for name in (
                "total",
                "distinct_occurrences",
                "distinct_runs",
                "active",
                "terminal",
            )
        ):
            raise QualificationCommandError("scheduled qualification counters are malformed")
        return {name: int(item) for name, item in value.items()}

    def _scheduler_occurrence(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "scheduler_occurrence"
        started = time.monotonic()
        pods = self._gateway_pods()
        due_at = datetime.now(UTC) + timedelta(seconds=8)
        task_ids: list[str] = []
        with ExitStack() as stack:
            clients: list[_RuntimeHttpSession] = []
            for pod in pods:
                forwarded = stack.enter_context(
                    _PortForward(
                        self.config,
                        self._metadata_value(pod, "name"),
                        resource_kind="pod",
                    )
                )
                client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                self._login(client)
                clients.append(client)
            for index in range(4):
                task_ids.append(
                    self._create_once_task(
                        clients[index % 2],
                        title=f"multi-gateway-occurrence-{index}",
                        run_at=due_at,
                    )
                )
        selected = tuple(task_ids)
        max_active = 0
        final: dict[str, int] = {}

        def completed() -> bool:
            nonlocal max_active, final
            final = self._scheduled_counts(selected)
            max_active = max(max_active, final["active"])
            if final["active"] > 2:
                raise QualificationCommandError("scheduler exceeded its global two-run capacity")
            return final["total"] == 4 and final["terminal"] == 4

        wait_until(
            completed,
            description="four coordinated scheduled occurrences",
            timeout_seconds=240,
            interval_seconds=0.25,
        )
        quoted = ",".join(f"'{item}'" for item in selected)
        run_counts = int(self._postgres(f"SELECT COALESCE(sum(run_count),0) FROM scheduled_tasks WHERE id IN ({quoted})"))
        deterministic_ids = int(self._postgres(f"SELECT count(*) FROM scheduled_task_runs WHERE task_id IN ({quoted}) AND id ~ '^task-run-[0-9a-f]{{48}}$'"))
        if max_active != 2 or final["distinct_occurrences"] != 4 or final["distinct_runs"] != 4 or run_counts != 4 or deterministic_ids != 4:
            raise QualificationCommandError("scheduler occurrence identity or global-cap evidence failed")
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "scheduler_replicas": 2,
                "scheduled_occurrences": 4,
                "global_capacity": 2,
            },
            evidence_facts={
                "durable_occurrences": final["total"],
                "distinct_runs": final["distinct_runs"],
                "maximum_active": max_active,
                "deterministic_identities": deterministic_ids,
            },
        )

    def _scheduler_owner_loss(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "scheduler_owner_loss"
        started = time.monotonic()
        task_ids: list[str] = []
        deleted_uids: list[str] = []
        epoch_before = 0
        points = ("claimed_before_launch", "launched_before_record")
        for index, point in enumerate(points):
            prefix = f"{self._qualification_prefix(scenario_id)}:{point}"
            if self._redis("SET", f"{prefix}:arm", "1", "EX", "180") != "OK":
                raise QualificationCommandError("scheduler qualification barrier could not be armed")
            pod = self._gateway_pods()[index % 2]
            with _PortForward(
                self.config,
                self._metadata_value(pod, "name"),
                resource_kind="pod",
            ) as forwarded:
                client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                self._login(client)
                task_ids.append(
                    self._create_once_task(
                        client,
                        title=f"scheduler-owner-loss-{point}",
                        run_at=datetime.now(UTC) + timedelta(seconds=5),
                    )
                )
            task_run_id = ""

            def reached() -> bool:
                nonlocal task_run_id
                task_run_id = self._redis("GET", f"{prefix}:reached")
                return bool(task_run_id)

            wait_until(
                reached,
                description=f"scheduler launch window {point}",
                timeout_seconds=120,
                interval_seconds=0.5,
            )
            owner = self._redis("GET", f"{prefix}:owner_replica_id")
            owner_pod = next(
                (item for item in self._gateway_pods() if self._metadata_value(item, "name") == owner),
                None,
            )
            if owner_pod is None:
                raise QualificationCommandError("scheduler barrier did not identify its lease owner")
            deleted_uid = self._metadata_value(owner_pod, "uid")
            deleted_uids.append(deleted_uid)
            epoch_before += int(self._postgres(f"SELECT attempt_count FROM scheduled_task_runs WHERE id='{task_run_id}'"))
            self._kubectl(
                "delete",
                "pod",
                owner,
                "--grace-period=0",
                "--force",
                "--wait=false",
                timeout_seconds=30,
            )
            self._wait_for_gateway_replacement(deleted_uid)

            selected = (task_ids[-1],)

            def terminal() -> bool:
                counts = self._scheduled_counts(selected)
                return counts["total"] == 1 and counts["terminal"] == 1

            wait_until(
                terminal,
                description=f"scheduler recovery after {point}",
                timeout_seconds=180,
                interval_seconds=1,
            )
        selected = tuple(task_ids)
        counts = self._scheduled_counts(selected)
        quoted = ",".join(f"'{item}'" for item in selected)
        epoch_after = int(self._postgres(f"SELECT COALESCE(sum(attempt_count),0) FROM scheduled_task_runs WHERE task_id IN ({quoted})"))
        admitted = int(self._postgres(f"SELECT count(DISTINCT run_id) FROM scheduled_task_runs WHERE task_id IN ({quoted})"))
        if counts["total"] != 2 or counts["distinct_occurrences"] != 2 or admitted != 2 or epoch_after <= epoch_before:
            raise QualificationCommandError("scheduler owner-loss recovery was not single-admission")
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "launch_windows": len(points),
                "fault": "owner_sigkill",
            },
            evidence_facts={
                "durable_occurrences": counts["total"],
                "distinct_runs": admitted,
                "recovered_windows": len(points),
            },
            takeover_count=len(points),
            pod_deletion_count=len(deleted_uids),
            pod_restart_count=len(deleted_uids),
            lease_epoch_before=epoch_before,
            lease_epoch_after=epoch_after,
            authoritative_count=len(points),
            verified_case_count=len(points),
        )

    def _sandbox_recovery(self) -> MultiGatewayScenarioObservationV1:
        before: dict[str, str | int] = {}

        def inspect_material(run_id: str) -> None:
            before.update(self._accepted_skill_attempt_facts(run_id))

        def inspect_ledger(
            run_id: str,
            _observation: Mapping[str, object],
        ) -> None:
            row = json.loads(self._postgres(f"SELECT json_build_object('evidence',execution_evidence_json,'digest',execution_evidence_digest)::text FROM runs WHERE run_id='{run_id}'"))
            evidence = row.get("evidence") if isinstance(row, dict) else None
            if (
                not isinstance(evidence, dict)
                or row.get("digest") != before.get("execution_evidence_digest")
                or evidence.get("skill_snapshot_digest") != before.get("snapshot_digest")
                or evidence.get("provider_instance_ref") != before.get("sandbox_id")
            ):
                raise QualificationCommandError("sandbox takeover changed accepted immutable evidence")

        observation = self._barrier_invocation(
            "sandbox_recovery",
            kill_owner=True,
            barrier_probe=inspect_material,
            terminal_probe=inspect_ledger,
            barrier_point="during_tool_execution",
            delivery_id="during-tool",
        )
        materialization_starts = self._counter(
            "sandbox_recovery",
            "materialization_starts",
        )
        materialization_validations = self._counter(
            "sandbox_recovery",
            "materialization_validations",
        )
        graph_starts = self._counter("sandbox_recovery", "graph_starts")
        model_starts = self._counter("sandbox_recovery", "model_starts")
        tool_starts = self._counter("sandbox_recovery", "tool_starts")
        tool_completions = self._counter(
            "sandbox_recovery",
            "tool_completions",
        )
        run_id = self._scenario_run_ids["sandbox_recovery"]
        receipt_counts = json.loads(
            self._postgres(
                "SELECT json_build_object("
                "'starts',count(*) FILTER (WHERE event_type='tool_receipt.started.v1'),"
                "'outcomes',count(*) FILTER (WHERE event_type='tool_receipt.outcome.v1'),"
                "'rows',count(*),'distinct_keys',count(DISTINCT idempotency_key))::text "
                f"FROM run_events WHERE run_id='{run_id}' "
                "AND event_type IN ('tool_receipt.started.v1','tool_receipt.outcome.v1')",
            )
        )
        if (
            materialization_starts != 2
            or materialization_validations != 2
            or graph_starts != 2
            or model_starts != 1
            or tool_starts < 2
            or tool_completions != 1
            or not isinstance(receipt_counts, dict)
            or receipt_counts.get("starts", 0) < 1
            or receipt_counts.get("outcomes") != 1
            or receipt_counts.get("rows") != receipt_counts.get("distinct_keys")
        ):
            raise QualificationCommandError(
                "sandbox recovery duplicated execution or skipped accepted-material revalidation",
            )
        return replace(
            observation,
            input_facts={
                "nonempty_skill": _QUALIFICATION_SKILL_NAME,
                "materialization_profile": "rwx_verified_copy_v2",
                "fault": "owning_gateway_sigkill",
                "fault_window": "during_long_sandbox_operation",
                "external_side_effect_contract": "at_least_once_indeterminate",
            },
            evidence_facts={
                "run_id": run_id,
                "snapshot_digest": str(before["snapshot_digest"]),
                "execution_evidence_digest": str(before["execution_evidence_digest"]),
                "ownership_epoch": int(before["ownership_epoch"]),
                "materialization_starts": materialization_starts,
                "materialization_validations": materialization_validations,
                "graph_starts": graph_starts,
                "model_starts": model_starts,
                "tool_starts": tool_starts,
                "tool_completions": tool_completions,
                "durable_tool_attempts": int(receipt_counts["starts"]),
                "durable_tool_outcomes": int(receipt_counts["outcomes"]),
            },
        )

    def _mcp_task_notification(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "mcp_task_notification"
        started = time.monotonic()
        point = "polled_before_apply"
        prefix = f"{self._qualification_prefix(scenario_id)}:{point}"
        if self._redis("SET", f"{prefix}:arm", "1", "EX", "180") != "OK":
            raise QualificationCommandError("MCP poller qualification barrier could not be armed")
        thread_id = "multi-gateway-execution_ownership"
        token = f"result-{self.config.qualification_id}"
        body = {
            "server_name": "qualification-tasks",
            "task_name": "qualification",
            "arguments": {
                "qualification_task_id": self.config.qualification_id,
                "result_token": token,
                "polls_before_complete": 4,
            },
            "idempotency_key": f"mcp-{self.config.qualification_id}",
        }
        pods = self._gateway_pods()
        with ExitStack() as stack:
            clients: list[_RuntimeHttpSession] = []
            for pod in pods:
                forwarded = stack.enter_context(
                    _PortForward(
                        self.config,
                        self._metadata_value(pod, "name"),
                        resource_kind="pod",
                    )
                )
                client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                self._login(client)
                clients.append(client)
            status, created = clients[0].request(
                "POST",
                f"/api/threads/{thread_id}/mcp-tasks",
                payload=body,
                timeout_seconds=60,
            )
            task_id = created.get("task_id")
            if status != 201 or not isinstance(task_id, str):
                raise QualificationCommandError("deterministic MCP task submission failed")
            duplicate_status, duplicate = clients[1].request(
                "POST",
                f"/api/threads/{thread_id}/mcp-tasks",
                payload=body,
                timeout_seconds=60,
            )
            if duplicate_status != 201 or duplicate.get("task_id") != task_id:
                raise QualificationCommandError("cross-pod MCP idempotency did not converge")
        reached_task = ""

        def reached() -> bool:
            nonlocal reached_task
            reached_task = self._redis("GET", f"{prefix}:reached")
            return bool(reached_task)

        wait_until(
            reached,
            description="MCP poll result before fenced apply",
            timeout_seconds=120,
            interval_seconds=0.5,
        )
        if reached_task != task_id:
            raise QualificationCommandError("MCP qualification barrier identified another task")
        owner = self._redis("GET", f"{prefix}:owner_replica_id")
        owner_pod = next(
            (item for item in self._gateway_pods() if self._metadata_value(item, "name") == owner),
            None,
        )
        if owner_pod is None:
            raise QualificationCommandError("MCP qualification barrier omitted its poller pod")
        deleted_uid = self._metadata_value(owner_pod, "uid")
        epoch_before = int(self._postgres(f"SELECT poll_attempt_count FROM mcp_tasks WHERE id='{task_id}'"))
        self._kubectl(
            "delete",
            "pod",
            owner,
            "--grace-period=0",
            "--force",
            "--wait=false",
            timeout_seconds=30,
        )
        self._wait_for_gateway_replacement(deleted_uid)
        current = self._gateway_pods()[0]
        detail: dict[str, object] = {}
        with _PortForward(
            self.config,
            self._metadata_value(current, "name"),
            resource_kind="pod",
        ) as forwarded:
            client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            self._login(client)

            def delivered() -> bool:
                nonlocal detail
                status, value = client.request(
                    "GET",
                    f"/api/threads/{thread_id}/mcp-tasks/{task_id}",
                )
                detail = value
                return status == 200 and value.get("status") == "completed" and value.get("notification_status") == "delivered"

            wait_until(
                delivered,
                description="MCP terminal result and notification delivery",
                timeout_seconds=240,
                interval_seconds=1,
            )
            links = detail.get("links")
            notification_run_id = links.get("notification_run_id") if isinstance(links, dict) else None
            if not isinstance(notification_run_id, str):
                raise QualificationCommandError("MCP notification lineage omitted its durable run")
            notification = self._observe_until_terminal(
                client,
                notification_run_id,
            )
        row = json.loads(
            self._postgres(
                "SELECT json_build_object("
                "'rows',count(*),'poll_attempts',max(poll_attempt_count),"
                "'event_version',max(event_version),'notified_version',"
                "max(notified_version),'notification_runs',"
                "count(DISTINCT notification_run_id),'lineages',"
                "count(DISTINCT lineage_digest)) FROM mcp_tasks "
                f"WHERE id='{task_id}'"
            )
        )
        epoch_after = int(row.get("poll_attempts", 0))
        result = detail.get("result")
        lineage = detail.get("lineage")
        authoritative, duplicates = self._event_counts(notification)
        if (
            row.get("rows") != 1
            or row.get("notification_runs") != 1
            or row.get("lineages") != 1
            or row.get("event_version") != row.get("notified_version")
            or epoch_after <= epoch_before
            or not isinstance(result, dict)
            or result.get("token") != token
            or not isinstance(lineage, dict)
            or lineage.get("status") != "verified"
            or authoritative != 1
            or duplicates != 0
        ):
            raise QualificationCommandError("MCP takeover result or notification lineage was duplicated")
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "duplicate_submissions": 2,
                "fault_point": point,
                "poller_sigkill": True,
            },
            evidence_facts={
                "task_id": task_id,
                "notification_run_id": notification_run_id,
                "terminal_status": str(detail.get("status")),
                "lineage_digest": str(lineage.get("digest")),
            },
            takeover_count=1,
            pod_deletion_count=1,
            pod_restart_count=1,
            lease_epoch_before=epoch_before,
            lease_epoch_after=epoch_after,
        )

    def _cancellation_finalization(self) -> MultiGatewayScenarioObservationV1:
        started = time.monotonic()
        cases = (
            ("cancel", True, "interrupted"),
            ("fail", False, "error"),
            ("succeed", False, "success"),
        )
        observations: list[MultiGatewayScenarioObservationV1] = []
        terminal_statuses: list[str] = []
        run_ids: list[str] = []
        for delivery_id, cancel, expected_status in cases:
            terminal_takeover = not cancel
            observation = self._barrier_invocation(
                "cancellation_finalization",
                cancel=cancel,
                barrier_point=("during_model_execution" if cancel else "terminal_before_lifecycle_commit"),
                delivery_id=delivery_id,
                simultaneous_admission=True,
                partition_owner=terminal_takeover,
                stale_counter_names=(("terminal_stale_rejections",) if terminal_takeover else ()),
            )
            run_id = str(observation.evidence_facts["run_id"])
            actual_status = str(observation.evidence_facts["terminal_status"])
            self._assert_terminal_cleanup(
                run_id,
                expected_status=expected_status,
            )
            if actual_status != expected_status:
                raise QualificationCommandError(
                    f"{delivery_id} finalization produced {actual_status}",
                )
            observations.append(observation)
            terminal_statuses.append(actual_status)
            run_ids.append(run_id)
        return self._observation(
            "cancellation_finalization",
            started=started,
            input_facts={
                "cross_pod_race_cases": "cancel,fail,succeed",
                "race_case_count": len(cases),
            },
            evidence_facts={
                "terminal_states": ",".join(terminal_statuses),
                "terminal_count": len(terminal_statuses),
                "cleanup_count": len(observations),
                "run_id_digest": "sha256:" + hashlib.sha256("\n".join(run_ids).encode()).hexdigest(),
            },
            authoritative_count=len(observations),
            stale_write_rejections=sum(observation.stale_write_rejections for observation in observations),
            takeover_count=sum(observation.takeover_count for observation in observations),
            lease_epoch_before=min(observation.lease_epoch_before for observation in observations),
            lease_epoch_after=max(observation.lease_epoch_after for observation in observations),
            verified_case_count=len(observations),
            cleanup_count=len(observations),
        )

    def _redis_outage_recovery(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "redis_outage_recovery"
        started = time.monotonic()
        baseline = self._barrier_invocation(
            scenario_id,
            delivery_id="transport",
        )
        run_id = str(baseline.evidence_facts["run_id"])
        thread_id = str(
            self._ensure_payload(
                scenario_id,
                delivery_id="transport",
            )["thread_id"]
        )
        stream_path = f"/api/threads/{thread_id}/runs/{run_id}/stream"
        pod = self._gateway_pods()[0]
        first_event_id = ""
        with _PortForward(
            self.config,
            self._metadata_value(pod, "name"),
            resource_kind="pod",
        ) as forwarded:
            before_client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            self._login(before_client)
            status, before = before_client.request(
                "GET",
                f"/api/runtime/v1/invocations/{run_id}?limit=500",
            )
            response = self._open_sse(before_client, stream_path)
            try:
                for _ in range(64):
                    frame = self._read_sse_frame(response)
                    if frame.get("event") == "gap":
                        raise QualificationCommandError(
                            "Redis outage baseline stream already had a gap",
                        )
                    candidate = frame.get("id")
                    if candidate:
                        self._redis_stream_id(candidate)
                        first_event_id = candidate
                        break
            finally:
                response.close()
        if status != 200 or before.get("status") not in _TERMINAL_RUN_STATUSES:
            raise QualificationCommandError("durable history baseline was unavailable before Redis outage")
        if not first_event_id:
            raise QualificationCommandError(
                "Redis outage baseline emitted no resumable SSE cursor",
            )
        before_events = before.get("events")
        if not isinstance(before_events, list):
            raise QualificationCommandError("durable history baseline omitted lifecycle events")
        self._scale_deployment(self.redis_name, 0)
        try:
            self._wait_gateway_http_readiness(expected_ready=False)
            retryable_statuses = self._retryable_sse_transport_failure(
                thread_id=thread_id,
                run_id=run_id,
                last_event_id=first_event_id,
            )
        finally:
            self._scale_deployment(self.redis_name, 1)
        self._wait_gateway_http_readiness(expected_ready=True)
        current = self._gateway_pods()[1]
        with _PortForward(
            self.config,
            self._metadata_value(current, "name"),
            resource_kind="pod",
        ) as forwarded:
            after_client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
            self._login(after_client)
            status, after = after_client.request(
                "GET",
                f"/api/runtime/v1/invocations/{run_id}?limit=500",
            )
            response = self._open_sse(
                after_client,
                stream_path,
                last_event_id=first_event_id,
            )
            resumed_ids: list[str] = []
            saw_end = False
            try:
                for _ in range(512):
                    frame = self._read_sse_frame(response)
                    if frame.get("event") == "gap":
                        raise QualificationCommandError(
                            "Redis recovery reconnect reported a replay gap",
                        )
                    if event_id := frame.get("id"):
                        resumed_ids.append(event_id)
                    if frame.get("event") == "end":
                        saw_end = True
                        break
            finally:
                response.close()
        after_events = after.get("events")
        if status != 200 or after.get("status") != before.get("status") or after_events != before_events:
            raise QualificationCommandError("durable lifecycle could not be reconstructed after Redis recovery")
        ordered_ids = [self._redis_stream_id(item) for item in resumed_ids]
        if not saw_end or not resumed_ids or len(set(resumed_ids)) != len(resumed_ids) or ordered_ids != sorted(ordered_ids) or any(item <= self._redis_stream_id(first_event_id) for item in ordered_ids):
            raise QualificationCommandError(
                "Redis recovery did not resume an ordered retained SSE suffix",
            )
        foreign_prefix = TenantIdentityV1.from_canonical_id("redis-outage-foreign").namespace(TenantSubsystem.REDIS).key_prefix
        denial = self._require_redis_acl_denial(
            "GET",
            f"{foreign_prefix}:qualification:key",
        )
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "redis_interruptions": 1,
                "transport_readiness_expected": "not_ready",
                "reconnect_pod": 1,
                "transport_endpoint": "run_sse_reconnect",
            },
            evidence_facts={
                "run_id": run_id,
                "durable_event_count": len(after_events),
                "terminal_status": str(after.get("status")),
                "foreign_acl_denial": denial,
                "first_event_id": first_event_id,
                "resumed_event_count": len(resumed_ids),
                "retryable_http_statuses": ",".join(str(status) for status in retryable_statuses),
            },
            authoritative_count=baseline.authoritative_count,
            duplicate_count=baseline.duplicate_count,
            lease_epoch_before=baseline.lease_epoch_before,
            lease_epoch_after=baseline.lease_epoch_after,
            dependency_interruption_count=1,
            retryable_failure_count=int(retryable_statuses == (503, 503)),
        )

    def _postgresql_interruption(self) -> MultiGatewayScenarioObservationV1:
        started = time.monotonic()
        cases = (
            (
                "post_checkpoint_before_graph",
                "checkpoint-fence",
                ("checkpoint_stale_rejections",),
                False,
            ),
            (
                "during_tool_execution",
                "during-tool",
                (
                    "receipt_stale_rejections",
                    "sandbox_stale_renewal_rejections",
                ),
                True,
            ),
            (
                "terminal_before_lifecycle_commit",
                "terminal-fence",
                ("terminal_stale_rejections",),
                False,
            ),
        )
        observations = tuple(
            self._barrier_invocation(
                "postgresql_interruption",
                require_stale_rejection=True,
                dependency_interruption_count=1 if index == 0 else 0,
                barrier_point=point,
                delivery_id=delivery_id,
                partition_owner=True,
                stale_counter_names=counters,
                arm_stale_external_renewal=arm_external,
            )
            for index, (point, delivery_id, counters, arm_external) in enumerate(cases)
        )
        counter_names = tuple(name for _point, _delivery, names, _arm_external in cases for name in names)
        return self._observation(
            "postgresql_interruption",
            started=started,
            input_facts={
                "postgresql_partition_campaigns": 1,
                "barriers": ",".join(point for point, *_rest in cases),
                "both_gateways_probed": True,
                "owner_only_network_partitions": len(cases),
                "stale_process_returns": len(cases),
            },
            evidence_facts={
                "run_id_digest": "sha256:" + hashlib.sha256("\n".join(str(observation.evidence_facts["run_id"]) for observation in observations).encode()).hexdigest(),
                "lease_epoch_before_partition": min(observation.lease_epoch_before for observation in observations),
                "lease_epoch_after_takeover": max(observation.lease_epoch_after for observation in observations),
                "partitioned_owner_not_ready_count": len(observations),
                "peer_ready_during_partition_count": len(observations),
                "stale_rejection_kinds": len(counter_names),
                "stale_rejection_count": sum(self._counter("postgresql_interruption", name) for name in counter_names),
            },
            authoritative_count=len(observations),
            stale_write_rejections=sum(observation.stale_write_rejections for observation in observations),
            takeover_count=sum(observation.takeover_count for observation in observations),
            lease_epoch_before=observations[0].lease_epoch_before,
            lease_epoch_after=max(observation.lease_epoch_after for observation in observations),
            dependency_interruption_count=1,
            verified_case_count=len(observations),
        )

    def _config_artifact_skew(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "config_artifact_skew"
        started = time.monotonic()
        deployment = json.loads(
            self._kubectl(
                "get",
                "deployment",
                self.gateway_deployment,
                "-o",
                "json",
            )
        )
        base_template = deployment.get("spec", {}).get("template")
        if not isinstance(base_template, dict) or not isinstance(base_template.get("spec"), dict):
            raise QualificationCommandError("Gateway pod template is unavailable for skew injection")
        qualified_registrations = self._topology_registrations()
        if len(qualified_registrations) != 2:
            raise QualificationCommandError("skew injection requires two compatible Gateway registrations")

        def environment_entry(
            environment: list[object],
            name: str,
        ) -> dict[str, object]:
            entry = next(
                (item for item in environment if isinstance(item, dict) and item.get("name") == name),
                None,
            )
            if not isinstance(entry, dict):
                raise QualificationCommandError(
                    f"Gateway topology environment omitted {name}",
                )
            return entry

        phases: dict[str, str] = {}

        def assert_rejected(
            *,
            case: str,
            mutate: Callable[[dict[str, object], list[object]], None],
        ) -> None:
            template = copy.deepcopy(base_template)
            metadata = template.setdefault("metadata", {})
            if not isinstance(metadata, dict):
                raise QualificationCommandError("Gateway pod metadata is malformed")
            metadata.pop("annotations", None)
            skew_name = f"hartmesh-multi-gateway-skew-{case}"
            metadata["name"] = skew_name
            metadata["labels"] = {
                "app.kubernetes.io/name": "deer-flow-skew",
                "app.kubernetes.io/instance": "qualification-skew",
                "app.kubernetes.io/component": "gateway-skew",
            }
            spec = template.get("spec")
            if not isinstance(spec, dict):
                raise QualificationCommandError("Gateway pod spec is malformed")
            spec["restartPolicy"] = "Never"
            containers = spec.get("containers")
            if not isinstance(containers, list) or len(containers) != 1 or not isinstance(containers[0], dict):
                raise QualificationCommandError("Gateway pod template has an unexpected container set")
            environment = containers[0].get("env")
            if not isinstance(environment, list):
                raise QualificationCommandError("Gateway topology environment is unavailable")
            mutate(containers[0], environment)
            phase = ""
            self._apply(
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": metadata,
                    "spec": spec,
                }
            )
            try:

                def rejected() -> bool:
                    nonlocal phase
                    pod = json.loads(
                        self._kubectl("get", "pod", skew_name, "-o", "json"),
                    )
                    phase = str(pod.get("status", {}).get("phase", ""))
                    conditions = pod.get("status", {}).get("conditions", [])
                    ready = any(isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True" for item in conditions)
                    return phase in {"Failed", "Succeeded"} or (phase == "Running" and not ready)

                wait_until(
                    rejected,
                    description=f"mismatched Gateway {case} rejection",
                    timeout_seconds=180,
                    interval_seconds=2,
                )
                time.sleep(3)
                pod = json.loads(
                    self._kubectl("get", "pod", skew_name, "-o", "json"),
                )
                conditions = pod.get("status", {}).get("conditions", [])
                if any(isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True" for item in conditions):
                    raise QualificationCommandError(
                        f"mismatched Gateway {case} pod became ready",
                    )
                if len(self._topology_registrations()) != 2:
                    raise QualificationCommandError(
                        f"mismatched Gateway {case} pod entered topology authority",
                    )
                logs = self._kubectl(
                    "logs",
                    skew_name,
                    "--tail=200",
                    timeout_seconds=30,
                )
                if "topology_fingerprint_mismatch" not in logs:
                    raise QualificationCommandError(
                        f"mismatched Gateway {case} pod lacked the stable rejection code",
                    )
                phases[case] = phase
            finally:
                self._kubectl(
                    "delete",
                    "pod",
                    skew_name,
                    "--wait=true",
                    "--ignore-not-found=true",
                    timeout_seconds=60,
                )

        def mutate_config(
            _container: dict[str, object],
            environment: list[object],
        ) -> None:
            entry = environment_entry(
                environment,
                "DEER_FLOW_TOPOLOGY_CONFIG_DIGEST",
            )
            entry.pop("valueFrom", None)
            entry["value"] = "sha256:" + ("0" * 64)

        def mutate_binary(
            container: dict[str, object],
            environment: list[object],
        ) -> None:
            container["image"] = f"{self.config.incompatible_gateway_image_repository}@{self.config.incompatible_gateway_image_digest}"
            entry = environment_entry(
                environment,
                "DEER_FLOW_TOPOLOGY_IMAGE_DIGESTS",
            )
            raw = entry.get("value")
            try:
                images = json.loads(raw) if isinstance(raw, str) else None
            except json.JSONDecodeError as exc:
                raise QualificationCommandError(
                    "Gateway topology image digest set is malformed",
                ) from exc
            if not isinstance(images, dict):
                raise QualificationCommandError(
                    "Gateway topology image digest set is unavailable",
                )
            images["gateway"] = self.config.incompatible_gateway_image_digest
            entry["value"] = json.dumps(
                images,
                sort_keys=True,
                separators=(",", ":"),
            )

        assert_rejected(case="config", mutate=mutate_config)
        assert_rejected(case="binary", mutate=mutate_binary)
        self._mixed_binary_rejection_verified = True
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "skew_kinds": "configuration_digest,gateway_binary_digest",
                "expected_replicas": 2,
            },
            evidence_facts={
                "config_skew_pod_phase": phases["config"],
                "binary_skew_pod_phase": phases["binary"],
                "live_compatible_replicas": len(qualified_registrations),
                "rejection_codes": "topology_fingerprint_mismatch,topology_fingerprint_mismatch",
                "incompatible_gateway_digest": self.config.incompatible_gateway_image_digest,
            },
        )

    def _tenant_separation(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "tenant_separation"
        started = time.monotonic()
        secondary_id = self.config.qualification_id[:110] + "-tenant-b"
        secondary = replace(
            self.config,
            namespace=self._secondary_namespace,
            qualification_id=secondary_id,
        )
        database_name = "deerflow_tenant_b"
        database_role = "qualification_secondary"
        database_password = secrets.token_urlsafe(32)
        tenant_id = "qualification-secondary"
        tenant = TenantIdentityV1.from_canonical_id(tenant_id)
        redis_user = "qualification-secondary"
        redis_password = secrets.token_urlsafe(32)
        namespace_created = False
        database_created = False
        database_role_created = False
        redis_user_created = False
        secondary_run_id = ""
        primary_run_id = self._scenario_run_ids.get("execution_ownership")
        if primary_run_id is None:
            raise QualificationCommandError("tenant isolation requires a primary durable run")
        sandbox_run_id = self._scenario_run_ids.get("sandbox_recovery")
        if sandbox_run_id is None:
            raise QualificationCommandError(
                "tenant isolation requires a primary accepted sandbox",
            )
        sandbox_evidence = json.loads(
            self._postgres(
                f"SELECT execution_evidence_json::text FROM runs WHERE run_id='{sandbox_run_id}'",
            )
        )
        sandbox_id = sandbox_evidence.get("provider_instance_ref") if isinstance(sandbox_evidence, dict) else None
        if (
            not isinstance(sandbox_id, str)
            or _SAFE_RESOURCE.fullmatch(
                sandbox_id,
            )
            is None
        ):
            raise QualificationCommandError(
                "tenant isolation primary sandbox identity is unavailable",
            )
        try:
            self._postgres(
                f"CREATE ROLE {database_role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '{database_password}'",
                redact_diagnostics=True,
            )
            database_role_created = True
            self._postgres(
                f"CREATE DATABASE {database_name} OWNER {database_role}",
            )
            database_created = True
            self._postgres(
                f"REVOKE CONNECT ON DATABASE {database_name} FROM PUBLIC; GRANT CONNECT,TEMPORARY ON DATABASE {database_name} TO {database_role}",
            )
            redis_prefix = tenant.namespace(TenantSubsystem.REDIS).key_prefix
            if (
                self._redis_admin(
                    "ACL",
                    "SETUSER",
                    redis_user,
                    "on",
                    f">{redis_password}",
                    f"~{redis_prefix}:*",
                    f"&{redis_prefix}:*",
                    "+@all",
                )
                != "OK"
            ):
                raise QualificationCommandError("secondary tenant Redis ACL creation failed")
            redis_user_created = True
            self._kubectl_for(
                secondary,
                "create",
                "namespace",
                secondary.namespace,
                namespaced=False,
            )
            namespace_created = True
            database_url = f"postgresql://{database_role}:{database_password}@{self.postgres_name}.{self.config.namespace}.svc.cluster.local:5432/{database_name}"
            redis_url = f"redis://{redis_user}:{redis_password}@{self.redis_name}.{self.config.namespace}.svc.cluster.local:6379/0"
            self._apply_for(
                secondary,
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": self.store_secret},
                    "type": "Opaque",
                    "stringData": {
                        "database-url": database_url,
                        "postgres-tenant-password": database_password,
                        "redis-url": redis_url,
                    },
                },
            )
            self._apply_for(
                secondary,
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": self.runtime_config_map},
                    "data": {
                        "DEERFLOW_TEST_KUBERNETES_RUNTIME": "1",
                        "DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION": "1",
                        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID": (secondary_id),
                        "DEERFLOW_TEST_KUBERNETES_BARRIER_TIMEOUT_SECONDS": ("180"),
                        "DEERFLOW_TEST_KUBERNETES_MODEL_DELAY_SECONDS": "1",
                    },
                },
            )
            for claim, size in (
                (self.home_claim, "2Gi"),
                (self.skills_claim, "256Mi"),
            ):
                self._apply_for(
                    secondary,
                    self._pvc(
                        claim,
                        storage_class=self.config.rwx_storage_class,
                        access_mode="ReadWriteMany",
                        size=size,
                    ),
                )
            for manifest in self.qualification_mcp_manifests():
                self._apply_for(secondary, manifest)
            self._kubectl_for(
                secondary,
                "rollout",
                "status",
                f"deployment/{self.mcp_name}",
                "--timeout=4m",
                timeout_seconds=250,
            )
            values = copy.deepcopy(self.values())
            values["namespace"] = secondary.namespace
            values["tenant"] = {"id": tenant_id}
            deployment = values["deployment"]
            deployment["qualificationCandidate"] = {
                "enabled": True,
                "id": secondary_id,
            }
            deployment["topology"]["databaseSchemaRef"] = "schema:sha256:" + hashlib.sha256(database_name.encode()).hexdigest()
            secondary_values = self.config.evidence_path.parent / (f".{secondary_id}.values.json")
            secondary_values.write_text(
                json.dumps(values, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            self._helm_for(
                secondary,
                "upgrade",
                "--install",
                secondary.release_name,
                str(self.chart_path),
                "--values",
                str(secondary_values),
                "--wait",
                "--timeout",
                "10m",
                timeout_seconds=630,
            )
            with _PortForward(
                secondary,
                self.gateway_service,
            ) as forwarded:
                client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                status, _created = client.request(
                    "POST",
                    "/api/v1/auth/initialize",
                    payload={
                        "email": (f"{self.config.qualification_id}@qualification.invalid"),
                        "password": self._admin_password,
                        "remember_me": False,
                    },
                )
                if status != 201:
                    raise QualificationCommandError("secondary tenant could not initialize the same user key")
                self._login(client)
                invisible_status, _invisible = client.request(
                    "GET",
                    f"/api/runtime/v1/invocations/{primary_run_id}",
                )
                if invisible_status != 404:
                    raise QualificationCommandError("secondary tenant observed the primary run")
                secondary_prefix = (
                    redis_component_key_prefix(
                        tenant.namespace(TenantSubsystem.REDIS),
                        RedisTenantComponent.QUALIFICATION,
                    )
                    + f":{secondary_id}:execution_ownership"
                )
                self._redis_admin(
                    "SET",
                    f"{secondary_prefix}:release",
                    "1",
                    "EX",
                    "180",
                )
                status, admitted = client.request(
                    "POST",
                    "/api/runtime/v1/invocations/ensure",
                    payload=self._ensure_payload("execution_ownership"),
                    timeout_seconds=120,
                )
                secondary_run_id = str(admitted.get("run_id", ""))
                if status not in {200, 201} or not secondary_run_id:
                    raise QualificationCommandError("secondary tenant could not independently reuse run keys")
                self._observe_until_terminal(client, secondary_run_id)
            database_counts = self._kubectl(
                "exec",
                self._component_pod_name(self.postgres_name),
                "--",
                "psql",
                "-U",
                "deerflow",
                "-d",
                database_name,
                "-Atc",
                f"SELECT json_build_object('primary',count(*) FILTER (WHERE run_id='{primary_run_id}'),'secondary',count(*) FILTER (WHERE run_id='{secondary_run_id}')) FROM runs",
            )
            counts = json.loads(database_counts)
            if counts != {"primary": 0, "secondary": 1}:
                raise QualificationCommandError("tenant databases did not remain physically separated")
            secondary_redis_key = f"{redis_prefix}:qualification:proof"
            secondary_result = self._kubectl(
                "exec",
                self._component_pod_name(self.redis_name),
                "--",
                "redis-cli",
                "--raw",
                "--no-auth-warning",
                "--user",
                redis_user,
                "--pass",
                redis_password,
                "SET",
                secondary_redis_key,
                "1",
                "EX",
                "30",
                redact_diagnostics=True,
            )
            if secondary_result != "OK":
                raise QualificationCommandError("secondary tenant Redis namespace was unavailable")
            primary_prefix = TenantIdentityV1.from_canonical_id("qualification").namespace(TenantSubsystem.REDIS).key_prefix
            denial = self._kubectl(
                "exec",
                self._component_pod_name(self.redis_name),
                "--",
                "redis-cli",
                "--raw",
                "--no-auth-warning",
                "--user",
                redis_user,
                "--pass",
                redis_password,
                "GET",
                f"{primary_prefix}:qualification:proof",
                redact_diagnostics=True,
            )
            if not denial.startswith(("NOPERM", "ERR this user has no permissions")):
                raise QualificationCommandError("secondary Redis credentials crossed the tenant prefix")
            primary_denial = self._require_redis_acl_denial(
                "GET",
                secondary_redis_key,
            )
            secondary_database_denial = self._require_postgres_connect_denial(
                role=database_role,
                password=database_password,
                database="deerflow",
            )
            primary_database_denial = self._require_postgres_connect_denial(
                role="qualification_primary",
                password=self._postgres_tenant_password,
                database=database_name,
            )
            can_cross_namespace = self._kubectl_for(
                secondary,
                "auth",
                "can-i",
                "get",
                "pods",
                "--namespace",
                self.config.namespace,
                "--as",
                (f"system:serviceaccount:{secondary.namespace}:{self.fullname}-gateway"),
                namespaced=False,
                timeout_seconds=30,
            )
            if can_cross_namespace != "no":
                raise QualificationCommandError("secondary Gateway identity crossed sandbox namespaces")
            secondary_token = self._kubectl_for(
                secondary,
                "create",
                "token",
                f"{self.fullname}-gateway",
                "--audience=hartmesh-provisioner",
                "--duration=5m",
                timeout_seconds=30,
                redact_diagnostics=True,
            )
            if not secondary_token:
                raise QualificationCommandError(
                    "secondary Gateway projected token was unavailable",
                )
            with _PortForward(
                self.config,
                f"{self.fullname}-provisioner",
                remote_port=8002,
            ) as provisioner_forward:
                provisioner_url = f"http://127.0.0.1:{provisioner_forward.port}"
                provisioner_statuses = (
                    self._bearer_request_status(
                        provisioner_url,
                        f"/api/sandboxes/{sandbox_id}",
                        token=secondary_token,
                        method="GET",
                    ),
                    self._bearer_request_status(
                        provisioner_url,
                        "/api/sandboxes",
                        token=secondary_token,
                        method="POST",
                        payload={
                            "sandbox_id": sandbox_id,
                            "thread_id": "multi-gateway-tenant-separation",
                            "user_id": self.config.qualification_id,
                        },
                    ),
                    self._bearer_request_status(
                        provisioner_url,
                        f"/api/sandboxes/{sandbox_id}/accepted-attempt/renew",
                        token=secondary_token,
                        method="POST",
                        payload={},
                    ),
                )
            if provisioner_statuses != (401, 401, 401):
                raise QualificationCommandError(
                    "secondary Gateway identity crossed the primary provisioner boundary",
                )
            return self._observation(
                scenario_id,
                started=started,
                input_facts={
                    "release_count": 2,
                    "same_user_key": True,
                    "same_external_key": True,
                },
                evidence_facts={
                    "primary_run_in_secondary_database": 0,
                    "secondary_run_count": 1,
                    "redis_cross_prefix": primary_denial,
                    "secondary_to_primary_database": (secondary_database_denial),
                    "primary_to_secondary_database": (primary_database_denial),
                    "sandbox_cross_namespace": can_cross_namespace,
                    "provisioner_cross_release_statuses": ",".join(str(status) for status in provisioner_statuses),
                    "provisioner_cross_release_denials": len(
                        provisioner_statuses,
                    ),
                },
            )
        finally:
            if namespace_created:
                self._kubectl_for(
                    secondary,
                    "delete",
                    "namespace",
                    secondary.namespace,
                    "--wait=true",
                    "--ignore-not-found=true",
                    namespaced=False,
                    timeout_seconds=240,
                )
            if redis_user_created:
                self._redis_admin("ACL", "DELUSER", redis_user)
            if database_created:
                self._postgres(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{database_name}' AND pid <> pg_backend_pid()")
                self._postgres(f"DROP DATABASE {database_name}")
            if database_role_created:
                self._postgres(f"DROP ROLE {database_role}")

    def _unsupported_surfaces(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "unsupported_surfaces"
        started = time.monotonic()

        def config_mutation(
            values: dict[str, object],
            mutate: Callable[[dict[str, object]], None],
        ) -> None:
            config = yaml.safe_load(str(values["config"]))
            if not isinstance(config, dict):
                raise QualificationCommandError("candidate application config is malformed")
            mutate(config)
            values["config"] = yaml.safe_dump(config, sort_keys=False)

        def startup_rejection(
            *,
            case: str,
            expected_code: str,
            config_mutator: Callable[[dict[str, object]], None] | None = None,
            disable_candidate: bool = False,
        ) -> None:
            deployment = json.loads(
                self._kubectl(
                    "get",
                    "deployment",
                    self.gateway_deployment,
                    "-o",
                    "json",
                )
            )
            template = copy.deepcopy(
                deployment.get("spec", {}).get("template"),
            )
            if not isinstance(template, dict):
                raise QualificationCommandError(
                    "Gateway pod template is unavailable for startup rejection",
                )
            metadata = template.get("metadata")
            spec = template.get("spec")
            if not isinstance(metadata, dict) or not isinstance(spec, dict):
                raise QualificationCommandError(
                    "Gateway startup rejection template is malformed",
                )
            pod_name = f"hartmesh-multi-gateway-reject-{case}"
            config_map_name = f"{pod_name}-config"
            metadata.clear()
            metadata.update(
                {
                    "name": pod_name,
                    "labels": {
                        "app.kubernetes.io/name": "deer-flow-rejection",
                        "app.kubernetes.io/instance": "qualification-rejection",
                        "app.kubernetes.io/component": "gateway-rejection",
                    },
                }
            )
            spec["restartPolicy"] = "Never"
            containers = spec.get("containers")
            if not isinstance(containers, list) or len(containers) != 1 or not isinstance(containers[0], dict):
                raise QualificationCommandError(
                    "Gateway startup rejection container is unavailable",
                )
            environment = containers[0].get("env")
            if not isinstance(environment, list):
                raise QualificationCommandError(
                    "Gateway startup rejection environment is unavailable",
                )
            if disable_candidate:
                candidate = next(
                    (item for item in environment if isinstance(item, dict) and item.get("name") == "DEER_FLOW_QUALIFICATION_CANDIDATE"),
                    None,
                )
                if not isinstance(candidate, dict):
                    raise QualificationCommandError(
                        "Gateway candidate startup gate is unavailable",
                    )
                candidate.pop("valueFrom", None)
                candidate["value"] = "0"
            if config_mutator is not None:
                application_config = yaml.safe_load(self._application_config())
                if not isinstance(application_config, dict):
                    raise QualificationCommandError(
                        "Gateway startup rejection config is malformed",
                    )
                config_mutator(application_config)
                self._apply(
                    {
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": config_map_name},
                        "data": {
                            "config.yaml": yaml.safe_dump(
                                application_config,
                                sort_keys=False,
                            )
                        },
                    }
                )
                volumes = spec.get("volumes")
                config_volume = (
                    next(
                        (item for item in volumes if isinstance(item, dict) and item.get("name") == "config"),
                        None,
                    )
                    if isinstance(volumes, list)
                    else None
                )
                if not isinstance(config_volume, dict):
                    raise QualificationCommandError(
                        "Gateway startup rejection config volume is unavailable",
                    )
                config_volume["configMap"] = {"name": config_map_name}
            self._apply(
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": metadata,
                    "spec": spec,
                }
            )
            logs = ""
            try:

                def rejected() -> bool:
                    nonlocal logs
                    try:
                        logs = self._kubectl(
                            "logs",
                            pod_name,
                            "--tail=200",
                            timeout_seconds=15,
                        )
                    except QualificationCommandError:
                        return False
                    return expected_code in logs

                wait_until(
                    rejected,
                    description=f"Gateway startup rejection {case}",
                    timeout_seconds=180,
                    interval_seconds=2,
                )
                pod = json.loads(
                    self._kubectl("get", "pod", pod_name, "-o", "json"),
                )
                conditions = pod.get("status", {}).get("conditions", [])
                if any(isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True" for item in conditions):
                    raise QualificationCommandError(
                        f"unsupported startup case {case} became ready",
                    )
                if len(self._topology_registrations()) != 2:
                    raise QualificationCommandError(
                        f"unsupported startup case {case} entered topology authority",
                    )
            finally:
                self._kubectl(
                    "delete",
                    "pod",
                    pod_name,
                    "--wait=true",
                    "--ignore-not-found=true",
                    timeout_seconds=60,
                )
                if config_mutator is not None:
                    self._kubectl(
                        "delete",
                        "configmap",
                        config_map_name,
                        "--wait=true",
                        "--ignore-not-found=true",
                        timeout_seconds=60,
                    )

        cases: tuple[tuple[str, Callable[[dict[str, object]], None]], ...] = (
            (
                "three_replicas",
                lambda values: values["gateway"].update({"replicas": 3}),
            ),
            (
                "autoscaling",
                lambda values: values["gateway"].update({"autoscaling": {"enabled": True}}),
            ),
            (
                "channel_connector",
                lambda values: config_mutation(
                    values,
                    lambda config: config["channel_connections"].update({"enabled": True}),
                ),
            ),
            (
                "local_store",
                lambda values: config_mutation(
                    values,
                    lambda config: config["database"].update({"backend": "sqlite"}),
                ),
            ),
            (
                "unqualified_provider",
                lambda values: config_mutation(
                    values,
                    lambda config: config["sandbox"].update({"use": ("deerflow.community.opensandbox:OpenSandboxProvider")}),
                ),
            ),
            (
                "unclassified_extension",
                lambda values: config_mutation(
                    values,
                    lambda config: config.update(
                        {
                            "plugins": [
                                {
                                    "name": "unsafe",
                                    "package": "unsafe-extension",
                                    "use": "unsafe:install",
                                }
                            ]
                        }
                    ),
                ),
            ),
        )
        rejected: list[str] = []
        for case, mutate in cases:
            values = copy.deepcopy(self.values())
            mutate(values)
            path = self.config.evidence_path.parent / (f".{self.config.qualification_id}.{case}.json")
            path.write_text(
                json.dumps(values, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            try:
                self._helm(
                    "template",
                    f"reject-{case}",
                    str(self.chart_path),
                    "--values",
                    str(path),
                    timeout_seconds=60,
                )
            except QualificationCommandError:
                rejected.append(case)
            else:
                raise QualificationCommandError(f"unsupported surface rendered successfully: {case}")
            finally:
                path.unlink(missing_ok=True)
        if len(rejected) != len(cases):
            raise QualificationCommandError("unsupported surface rejection coverage is incomplete")
        startup_rejection(
            case="github",
            expected_code="topology_channel_not_replica_safe",
            config_mutator=lambda config: config.update(
                {"channels": {"github": {"enabled": True}}},
            ),
        )
        rejected.append("github_webhook_ingress")
        startup_rejection(
            case="candidate",
            expected_code="topology_qualification_missing",
            disable_candidate=True,
        )
        rejected.append("qualification_candidate_missing")
        if len(rejected) != 8:
            raise QualificationCommandError(
                "unsupported chart/startup rejection coverage is incomplete",
            )
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "unsupported_case_count": len(rejected),
                "render_and_startup_contract": "fail_closed",
            },
            evidence_facts={
                "rejected_count": len(rejected),
                "rejected_cases": ",".join(rejected),
            },
            verified_case_count=len(rejected),
        )

    def _upgrade_truthfulness(self) -> MultiGatewayScenarioObservationV1:
        scenario_id = "upgrade_truthfulness"
        started = time.monotonic()
        if self._candidate_values_path is None:
            raise QualificationCommandError("maintenance upgrade lacks its exact candidate values")
        if not self._mixed_binary_rejection_verified:
            raise QualificationCommandError(
                "maintenance upgrade lacks a live mixed-binary rejection proof",
            )
        run_id = self._scenario_run_ids.get("execution_ownership")
        if run_id is None:
            raise QualificationCommandError("maintenance upgrade lacks durable baseline data")
        active_runs, active_schedule_rows, active_mcp_rows = self._active_ownership_counts()
        if active_runs or active_schedule_rows or active_mcp_rows:
            raise QualificationCommandError("maintenance upgrade refused active ownership")
        pods = self._gateway_pods()
        target_before_uids = {self._metadata_value(pod, "uid") for pod in pods}
        before_row = self._postgres(
            f"SELECT json_build_object('status',status,'state_version',state_version)::text FROM runs WHERE run_id='{run_id}'",
        )

        def stopped() -> bool:
            selector = f"app.kubernetes.io/instance={self.config.release_name},app.kubernetes.io/component=gateway"
            items = self._pod_document(selector).get("items")
            return isinstance(items, list) and not items

        def stop_gateways(description: str) -> None:
            self._scale_deployment(self.gateway_deployment, 0)
            wait_until(
                stopped,
                description=description,
                timeout_seconds=180,
                interval_seconds=2,
            )

        predecessor_values = copy.deepcopy(self.values())
        predecessor_values["gateway"]["image"] = {
            "repository": self.config.predecessor_gateway_image_repository,
            "digest": self.config.predecessor_gateway_image_digest,
        }
        predecessor_values_path = self.config.evidence_path.parent / (f".{self.config.qualification_id}.predecessor.values.json")
        predecessor_values_path.write_text(
            json.dumps(
                predecessor_values,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )

        stop_gateways("target Gateway maintenance stop")
        self._helm(
            "upgrade",
            self.config.release_name,
            str(self.chart_path),
            "--values",
            str(predecessor_values_path),
            "--wait",
            "--timeout",
            "10m",
            timeout_seconds=630,
        )
        predecessor_pods = self._gateway_pods()
        predecessor_uids = {self._metadata_value(pod, "uid") for pod in predecessor_pods}
        predecessor_registrations = self._topology_registrations()
        predecessor_digests = {registration.topology_fingerprint.image_digests["gateway"] for registration in predecessor_registrations}
        if predecessor_digests != {self.config.predecessor_gateway_image_digest} or target_before_uids & predecessor_uids:
            raise QualificationCommandError(
                "maintenance predecessor did not run as a distinct pinned build",
            )
        with _PortForward(self.config, self.gateway_service) as forwarded:
            predecessor_client = _RuntimeHttpSession(
                f"http://127.0.0.1:{forwarded.port}",
            )
            self._login(predecessor_client)
            predecessor_status, predecessor_receipt = predecessor_client.request(
                "POST",
                "/api/runtime/v1/invocations/ensure",
                payload=self._ensure_payload(
                    scenario_id,
                    delivery_id="predecessor-baseline",
                ),
                timeout_seconds=120,
            )
            predecessor_run_id = predecessor_receipt.get("run_id")
            if predecessor_status not in {200, 201} or not isinstance(
                predecessor_run_id,
                str,
            ):
                raise QualificationCommandError(
                    "compatible predecessor could not create durable baseline data",
                )
            predecessor_observation = self._observe_until_terminal(
                predecessor_client,
                predecessor_run_id,
            )
        if predecessor_observation.get("status") != "success":
            raise QualificationCommandError(
                "compatible predecessor baseline did not succeed durably",
            )
        predecessor_row = self._postgres(
            f"SELECT json_build_object('status',status,'state_version',state_version)::text FROM runs WHERE run_id='{predecessor_run_id}'",
        )
        active_runs, active_schedule_rows, active_mcp_rows = self._active_ownership_counts()
        if active_runs or active_schedule_rows or active_mcp_rows:
            raise QualificationCommandError(
                "target upgrade refused active predecessor ownership",
            )
        stop_gateways("compatible predecessor maintenance stop")
        self._helm(
            "upgrade",
            self.config.release_name,
            str(self.chart_path),
            "--values",
            str(self._candidate_values_path),
            "--wait",
            "--timeout",
            "10m",
            timeout_seconds=630,
        )
        replacement = self._gateway_pods()
        target_after_uids = {self._metadata_value(pod, "uid") for pod in replacement}
        if predecessor_uids & target_after_uids or len(target_after_uids) != 2:
            raise QualificationCommandError("maintenance upgrade did not replace exactly two Gateways")
        after_rows = self._postgres(
            "SELECT json_build_object("
            f"'{run_id}',(SELECT json_build_object('status',status,'state_version',state_version) FROM runs WHERE run_id='{run_id}'),"
            f"'{predecessor_run_id}',(SELECT json_build_object('status',status,'state_version',state_version) FROM runs WHERE run_id='{predecessor_run_id}'))::text",
        )
        expected_after_rows = json.dumps(
            {
                run_id: json.loads(before_row),
                predecessor_run_id: json.loads(predecessor_row),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        parsed_after_rows = json.dumps(
            json.loads(after_rows),
            sort_keys=True,
            separators=(",", ":"),
        )
        if expected_after_rows != parsed_after_rows:
            raise QualificationCommandError("maintenance upgrade corrupted durable run state")
        registrations = self._topology_registrations()
        target_digests = {registration.topology_fingerprint.image_digests["gateway"] for registration in registrations}
        deployment = json.loads(
            self._kubectl(
                "get",
                "deployment",
                self.gateway_deployment,
                "-o",
                "json",
            )
        )
        strategy = deployment.get("spec", {}).get("strategy", {}).get("type")
        if strategy != "Recreate" or len(registrations) != 2 or target_digests != {self.config.image_digest}:
            raise QualificationCommandError("upgrade topology made an unsupported rolling claim")
        predecessor_values_path.unlink(missing_ok=True)
        return self._observation(
            scenario_id,
            started=started,
            input_facts={
                "mixed_binary_rejected": True,
                "incompatible_gateway_digest": (self.config.incompatible_gateway_image_digest),
                "predecessor_gateway_digest": (self.config.predecessor_gateway_image_digest),
                "target_gateway_digest": self.config.image_digest,
                "maintenance_window": True,
                "zero_downtime_claimed": False,
            },
            evidence_facts={
                "deployment_strategy": str(strategy),
                "durable_run_preserved": True,
                "predecessor_run_preserved": True,
                "predecessor_run_id": predecessor_run_id,
                "live_compatible_replicas": len(registrations),
            },
            pod_deletion_count=(len(target_before_uids) + len(predecessor_uids)),
            pod_restart_count=(len(predecessor_uids) + len(target_after_uids)),
            verified_case_count=2,
        )

    def _active_ownership_counts(self) -> tuple[int, int, int]:
        """Count every ownership-bearing row that blocks maintenance."""

        return (
            int(
                self._postgres(
                    "SELECT count(*) FROM runs WHERE status IN ('pending','running')",
                )
            ),
            int(
                self._postgres(
                    "SELECT count(*) FROM scheduled_task_runs WHERE status IN ('queued','launching','running')",
                )
            ),
            int(
                self._postgres(
                    "SELECT count(*) FROM mcp_tasks WHERE status IN ('submitted','working','input_required') OR notification_status IN ('pending','claimed','retry','dispatched')",
                )
            ),
        )


class KubernetesMultiGatewayQualificationRunnerV1:
    """Fail-closed publication wrapper around the exact live driver."""

    def __init__(
        self,
        config: KubernetesMultiGatewayQualificationConfigV1,
        *,
        repository_root: Path | None = None,
    ) -> None:
        self.config = config
        self.driver = KubernetesMultiGatewayQualificationDriverV1(
            config,
            repository_root=repository_root,
        )

    @property
    def passing_path(self) -> Path:
        return Path(str(self.config.evidence_path) + ".passing")

    @property
    def failure_artifacts_path(self) -> Path:
        stem = self.config.evidence_path.with_suffix("")
        return stem.with_name(stem.name + ".failure-artifacts")

    def _expectation(
        self,
        subjects: MultiGatewayQualificationSubjectsV1,
    ) -> MultiGatewayQualificationExpectationV1:
        """Build verifier inputs from operator and pre-scenario live facts."""

        tenant = TenantIdentityV1.from_canonical_id("qualification")
        redis_namespace_digest = (
            "sha256:"
            + tenant.namespace(
                TenantSubsystem.REDIS,
            ).digest
        )
        migration_head = get_expected_migration_head()
        expected_fingerprint = TopologyFingerprintV1.create(
            profile="durable_two_gateway_v1",
            tenant_digest=tenant.digest,
            image_digests=self.config.image_digests,
            config_digest=subjects.configuration_digest,
            database_schema_ref=self.config.database_schema_ref,
            redis_namespace_digest=redis_namespace_digest,
            extension_artifact_digest=self.config.extension_artifact_digest,
            extension_configuration_digest=(self.config.extension_configuration_digest),
            capability_manifest_digest=(self.config.capability_manifest_digest.removeprefix("sha256:")),
            migration_head=migration_head,
            accepted_materialization_profile="rwx_verified_copy_v2",
        )
        registrations = subjects.topology_registrations
        exact_subjects = (
            subjects.git_revision == self.driver._git_revision()
            and subjects.chart_version == self.driver._chart_version()
            and subjects.chart_digest == self.driver._chart_digest()
            and dict(subjects.image_digests) == self.config.image_digests
            and subjects.migration_head == migration_head
            and subjects.tenant_public_ref == tenant.public_ref
            and subjects.tenant_digest == tenant.digest
            and subjects.namespace == self.config.namespace
            and set(subjects.kubernetes_refs)
            == {
                "gateway_service_uid",
                "gateway_pod_0_uid",
                "gateway_pod_1_uid",
                "provisioner_pod_uid",
                "sandbox_pvc_uid",
            }
            and subjects.database_schema_ref == self.config.database_schema_ref
            and subjects.redis_namespace_digest == redis_namespace_digest
            and re.fullmatch(
                r"sha256:[0-9a-f]{64}",
                subjects.redis_acl_proof_digest,
            )
            is not None
            and subjects.extension_artifact_digest == self.config.extension_artifact_digest
            and subjects.extension_configuration_digest == self.config.extension_configuration_digest
            and subjects.capability_manifest_digest == self.config.capability_manifest_digest
            and len(registrations) == 2
            and all(registration.topology_fingerprint.digest == expected_fingerprint.digest for registration in registrations)
        )
        if not exact_subjects:
            raise QualificationCommandError(
                "pre-scenario subjects differ from independent qualification inputs",
            )
        return MultiGatewayQualificationExpectationV1(
            qualification_id=self.config.qualification_id,
            git_revision=subjects.git_revision,
            chart_version=subjects.chart_version,
            chart_digest=subjects.chart_digest,
            image_digests=self.config.image_digests,
            configuration_digest=subjects.configuration_digest,
            migration_head=migration_head,
            tenant_public_ref=tenant.public_ref,
            tenant_digest=tenant.digest,
            namespace=self.config.namespace,
            kubernetes_refs=subjects.kubernetes_refs,
            database_schema_ref=self.config.database_schema_ref,
            redis_namespace_digest=redis_namespace_digest,
            redis_acl_proof_digest=subjects.redis_acl_proof_digest,
            extension_artifact_digest=self.config.extension_artifact_digest,
            extension_configuration_digest=(self.config.extension_configuration_digest),
            capability_manifest_digest=self.config.capability_manifest_digest,
            topology_digest=expected_fingerprint.digest,
            scope=MULTI_GATEWAY_QUALIFICATION_SCOPE,
            required_scenarios=MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
            max_age_seconds=86_400,
        )

    def _assert_profile_remains_unavailable(
        self,
        evidence: KubernetesMultiGatewayQualificationEvidenceV1,
        *,
        evidence_digest: str,
    ) -> None:
        """Prove operator-declared evidence cannot turn this checkout into production."""

        values = copy.deepcopy(self.driver.values())
        deployment = values["deployment"]
        if not isinstance(deployment, dict):
            raise QualificationCommandError("qualified deployment values are malformed")
        deployment["qualificationCandidate"] = {"enabled": False, "id": ""}
        deployment["qualificationEvidence"] = [
            {
                "qualificationId": evidence.qualification_id,
                "artifactDigest": evidence_digest,
                "completedAt": evidence.completed_at.isoformat().replace(
                    "+00:00",
                    "Z",
                ),
                "scope": evidence.SCOPE,
                "status": "passed",
            }
        ]
        values_path = self.config.evidence_path.parent / (f".{self.config.qualification_id}.qualified.values.json")
        values_path.write_text(
            json.dumps(values, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        try:
            self.driver._helm(
                "template",
                self.config.release_name,
                str(self.driver.chart_path),
                "--values",
                str(values_path),
                timeout_seconds=60,
            )
        except QualificationCommandError as exc:
            if "topology_qualification_missing" not in str(exc):
                raise QualificationCommandError(
                    "production render failed for a reason other than the qualification gate",
                ) from exc
        else:
            raise QualificationCommandError(
                "operator-declared evidence unexpectedly unlocked the production profile",
            )
        finally:
            values_path.unlink(missing_ok=True)

    def _collect_failure_artifacts(self) -> None:
        """Capture only bounded non-secret cluster state and Gateway tails."""

        destination = self.failure_artifacts_path
        destination.mkdir(parents=True, exist_ok=True)
        commands = {
            "pods.txt": (
                "get",
                "pods",
                "-o",
                "wide",
            ),
            "deployments.txt": (
                "get",
                "deployments",
                "-o",
                "wide",
            ),
            "events.txt": (
                "get",
                "events",
                "--sort-by=.metadata.creationTimestamp",
            ),
        }
        for filename, arguments in commands.items():
            try:
                output = self.driver._kubectl(
                    *arguments,
                    timeout_seconds=30,
                    redact_diagnostics=True,
                )
            except Exception as exc:
                output = f"unavailable:{type(exc).__name__}"
            (destination / filename).write_text(
                output[-64_000:] + "\n",
                encoding="utf-8",
            )
        try:
            pods = self.driver._gateway_pods(require_ready=False)
        except Exception:
            pods = ()
        for index, pod in enumerate(pods):
            try:
                output = self.driver._kubectl(
                    "logs",
                    self.driver._metadata_value(pod, "name"),
                    "--tail=500",
                    timeout_seconds=30,
                    redact_diagnostics=True,
                )
            except Exception as exc:
                output = f"unavailable:{type(exc).__name__}"
            (destination / f"gateway-{index}.log").write_text(
                output[-128_000:] + "\n",
                encoding="utf-8",
            )

    def _delete_namespace(self) -> None:
        owned_uid = self.driver._owned_namespace_uid
        if owned_uid is None:
            return
        try:
            namespace = json.loads(
                self.driver._kubectl(
                    "get",
                    "namespace",
                    self.config.namespace,
                    "-o",
                    "json",
                    namespaced=False,
                    timeout_seconds=30,
                )
            )
        except QualificationCommandError:
            return
        metadata = namespace.get("metadata") if isinstance(namespace, dict) else None
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        if not isinstance(metadata, dict) or metadata.get("uid") != owned_uid or not isinstance(labels, dict) or labels.get(_QUALIFICATION_OWNER_LABEL) != self.driver._namespace_owner:
            raise QualificationCommandError(
                "refused to delete a qualification namespace not owned by this run",
            )
        self.driver._kubectl(
            "delete",
            "namespace",
            self.config.namespace,
            "--wait=true",
            "--ignore-not-found=true",
            namespaced=False,
            timeout_seconds=240,
        )
        self.driver._owned_namespace_uid = None

    def _write_failure(self, exc: BaseException) -> None:
        failure = {
            "api_version": ("deerflow.kubernetes-multi-gateway-qualification/v1"),
            "kind": "kubernetes.qualification.failure",
            "status": "failed",
            "scope": MULTI_GATEWAY_QUALIFICATION_SCOPE,
            "qualification_id": self.config.qualification_id,
            "namespace": self.config.namespace,
            "completed_scenarios": list(self.driver._completed_scenarios),
            "failure_code": type(exc).__name__,
            "completed_at": datetime.now(UTC)
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            ),
        }
        payload = (json.dumps(failure, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(payload) > 16 * 1024:
            raise QualificationCommandError("bounded multi-Gateway failure artifact is too large")
        temporary = Path(str(self.config.evidence_path) + ".failed.tmp")
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_bytes(payload)
        os.replace(temporary, self.config.evidence_path)

    def qualify(self) -> KubernetesMultiGatewayQualificationEvidenceV1:
        """Run, independently verify, report, and atomically publish evidence."""

        validate_kubernetes_prerequisites(os.environ)
        self.passing_path.unlink(missing_ok=True)
        try:
            evidence = asyncio.run(
                run_multi_gateway_qualification(
                    self.driver,
                    qualification_id=self.config.qualification_id,
                )
            )
            payload = evidence.canonical_bytes()
            self.passing_path.parent.mkdir(parents=True, exist_ok=True)
            self.passing_path.write_bytes(payload)
            declared_digest = qualification_evidence_digest(payload)
            verification = verify_multi_gateway_qualification_evidence(
                self.passing_path.read_bytes(),
                declared_digest=declared_digest,
                expected=self._expectation(self.driver.subjects),
                now=datetime.now(UTC),
            )
            if verification.artifact_digest != declared_digest:
                raise QualificationCommandError("offline verifier returned another artifact digest")
            self._assert_profile_remains_unavailable(
                evidence,
                evidence_digest=declared_digest,
            )
            self._delete_namespace()
            os.replace(self.passing_path, self.config.evidence_path)
            return evidence
        except Exception as exc:
            self.passing_path.unlink(missing_ok=True)
            self._collect_failure_artifacts()
            try:
                self._delete_namespace()
            finally:
                self._write_failure(exc)
            raise


__all__ = [
    "KubernetesMultiGatewayQualificationConfigV1",
    "KubernetesMultiGatewayQualificationDriverV1",
    "KubernetesMultiGatewayQualificationRunnerV1",
]

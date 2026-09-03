"""Distribution-neutral command construction for Kubernetes qualification."""

from __future__ import annotations

import base64
import hashlib
import http.cookiejar
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from deerflow.community.aio_sandbox.aio_sandbox_provider import (
    AioSandboxProvider,
)
from deerflow.deployment.topology import MULTI_GATEWAY_QUALIFICATION_SCOPE
from deerflow.qualification_evidence import (
    ACCEPTED_SKILL_QUALIFICATION_SCENARIOS_V2,
    ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
    AcceptedSkillMaterialEvidenceV2,
    AcceptedSkillQualificationEnvironmentV2,
    AcceptedSkillQualificationExpectationV2,
    AcceptedSkillScenarioEvidenceV2,
    KubernetesAcceptedSkillQualificationEvidenceV2,
    KubernetesQualificationEvidence,
    KubernetesQualificationFailureEvidence,
    QualificationEvidenceExpectation,
    ScenarioEvidence,
    StoreContinuityEvidence,
    qualification_evidence_digest,
    verify_qualification_evidence,
)
from deerflow.runtime.tenant_identity import (
    RedisTenantComponent,
    TenantIdentityV1,
    TenantSubsystem,
    redis_component_key_prefix,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV2,
    accepted_sandbox_resource_commitment,
    decode_accepted_execution_evidence,
)

_SAFE_CONTEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,252}\Z")
_SAFE_NAMESPACE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_IMAGE_REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,511}\Z")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
_SAFE_STORAGE_CLASS = re.compile(r"[a-z0-9](?:[-.a-z0-9]{0,251}[a-z0-9])?\Z")
_RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)
KUBERNETES_OPT_IN_MESSAGE = "Kubernetes qualification is opt-in; set DEERFLOW_TEST_KUBERNETES=1 with an explicit KUBECONFIG and qualification context"

_QUALIFICATION_SKILL_NAME = "qualification-skill"
_QUALIFICATION_ACCEPTED_ATTEMPT_LEASE_SECONDS = 120
_QUALIFICATION_REDIS_PREFIX = redis_component_key_prefix(
    TenantIdentityV1.from_canonical_id("qualification").namespace(TenantSubsystem.REDIS),
    RedisTenantComponent.QUALIFICATION,
)
_QUALIFICATION_SKILL_FILES = {
    "SKILL.md": (b"---\nname: qualification-skill\ndescription: Deterministic accepted-skill qualification fixture.\nallowed-tools:\n  - read_file\n---\nRead resources/proof.txt only from the accepted immutable snapshot.\n"),
    "resources/proof.txt": b"hartmesh accepted skill qualification v2\n",
}


def _expected_worker_attachments(scenario: str) -> int:
    return 0 if scenario == "accepted_before_worker_start" else 1


def _execution_counts_are_valid(
    scenario: str,
    graph_starts: int,
    model_starts: int,
) -> bool:
    if scenario == "accepted_before_worker_start":
        return graph_starts == 0 and model_starts == 0
    if scenario == "accepted_before_client_response":
        return graph_starts <= 1 and model_starts <= 1
    return graph_starts == 1 and model_starts == 1


def _expected_termination_mode(
    scenario: str,
) -> Literal["abrupt", "graceful", "forced_deadline"]:
    if scenario == "graceful_rollout_termination":
        return "graceful"
    if scenario == "forced_kill_after_graceful_deadline":
        return "forced_deadline"
    return "abrupt"


class QualificationPrerequisiteError(RuntimeError):
    """An explicitly enabled qualification cannot safely start."""


class QualificationCommandError(RuntimeError):
    """A bounded external command failed or exceeded its deadline."""


class QualificationTimeout(RuntimeError):
    """A bounded Kubernetes state wait did not converge."""


def kubernetes_qualification_enabled(environment: Mapping[str, str]) -> bool:
    """Return true only for the exact documented opt-in value."""

    return environment.get("DEERFLOW_TEST_KUBERNETES") == "1"


def optional_cluster_driver(environment: Mapping[str, str]) -> str | None:
    """Normalize an omitted workflow input to unknown rather than invalid empty."""

    return environment.get("DEERFLOW_TEST_KUBERNETES_DRIVER") or None


def validate_kubernetes_prerequisites(
    environment: Mapping[str, str],
    *,
    executable_lookup: Callable[[str], str | None] = shutil.which,
) -> None:
    """Fail an enabled session when any external prerequisite is absent."""

    if not kubernetes_qualification_enabled(environment):
        return
    missing: list[str] = []
    required_environment = (
        "KUBECONFIG",
        "DEERFLOW_TEST_KUBERNETES_CONTEXT",
        "DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT",
        "DEERFLOW_TEST_KUBERNETES_NAMESPACE",
        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID",
        "DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST",
        "DEERFLOW_TEST_KUBERNETES_EVIDENCE",
    )
    scope = environment.get(
        "DEERFLOW_TEST_KUBERNETES_SCOPE",
        KubernetesQualificationEvidence.SCOPE,
    )
    if scope not in {
        KubernetesQualificationEvidence.SCOPE,
        ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
        MULTI_GATEWAY_QUALIFICATION_SCOPE,
    }:
        raise QualificationPrerequisiteError(
            "enabled Kubernetes qualification has an unsupported scope",
        )
    if scope == ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2:
        required_environment += (
            "DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY",
            "DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST",
            "DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY",
            "DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST",
            "DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY",
            "DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST",
            "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS",
        )
    if scope == MULTI_GATEWAY_QUALIFICATION_SCOPE:
        required_environment += (
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
        )
    missing.extend(name for name in required_environment if not environment.get(name))
    missing.extend(name for name in ("kubectl", "helm") if executable_lookup(name) is None)
    if missing:
        raise QualificationPrerequisiteError("enabled Kubernetes qualification is missing prerequisites: " + ", ".join(sorted(missing)))


def run_bounded(
    command: tuple[str, ...],
    *,
    timeout_seconds: float,
    input_text: str | None = None,
    redact_diagnostics: bool = False,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> subprocess.CompletedProcess[str]:
    """Run one CLI command with captured output and a hard wall-clock bound."""

    if timeout_seconds <= 0:
        raise ValueError("command timeout must be positive")
    try:
        result = runner(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            input=input_text,
        )
    except subprocess.TimeoutExpired as exc:
        raise QualificationCommandError(f"{command[0]} {command[-1]} timed out after {timeout_seconds:.1f}s") from exc
    if result.returncode != 0:
        if redact_diagnostics:
            raise QualificationCommandError(f"{command[0]} command failed with exit {result.returncode}; diagnostic redacted")
        diagnostic = (result.stderr or result.stdout or "command failed").strip()
        if len(diagnostic) > 4096:
            diagnostic = diagnostic[-4096:]
        raise QualificationCommandError(f"{command[0]} command failed with exit {result.returncode}: {diagnostic}")
    return result


def wait_until(
    predicate: Callable[[], bool],
    *,
    description: str,
    timeout_seconds: float,
    interval_seconds: float = 0.5,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    """Poll a bounded state predicate without extending its absolute deadline."""

    if timeout_seconds <= 0 or interval_seconds < 0:
        raise ValueError("wait timeout must be positive and interval non-negative")
    deadline = monotonic() + timeout_seconds
    while True:
        if predicate():
            return
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise QualificationTimeout(f"timed out waiting for {description} after {timeout_seconds:.1f}s")
        sleeper(min(interval_seconds, remaining))


def evidence_sha256(path: Path) -> str:
    """Return the digest used by the bounded administrative evidence link."""

    return qualification_evidence_digest(path.read_bytes())


@dataclass(frozen=True)
class KubernetesQualificationConfig:
    """Explicit cluster identity and immutable artifact under qualification."""

    kubeconfig: Path
    context: str
    namespace: str
    image_repository: str
    image_digest: str
    evidence_path: Path
    qualification_id: str = "durable-pod-recovery"
    release_name: str = "hartmesh-qualification"

    def __post_init__(self) -> None:
        if not self.kubeconfig.is_absolute():
            raise ValueError("KUBECONFIG must be an absolute path")
        if _SAFE_CONTEXT.fullmatch(self.context) is None:
            raise ValueError("qualification context is invalid")
        if _SAFE_NAMESPACE.fullmatch(self.namespace) is None:
            raise ValueError("qualification namespace is invalid")
        if not self.namespace.startswith("hartmesh-qualification-"):
            raise ValueError("qualification namespace must begin with hartmesh-qualification-")
        if _IMAGE_REPOSITORY.fullmatch(self.image_repository) is None:
            raise ValueError("qualification image repository is invalid")
        if _IMAGE_DIGEST.fullmatch(self.image_digest) is None:
            raise ValueError("qualification image digest is invalid")
        if not self.evidence_path.is_absolute():
            raise ValueError("qualification evidence path must be absolute")
        if _SAFE_ID.fullmatch(self.qualification_id) is None:
            raise ValueError("qualification id is invalid")
        if _SAFE_NAMESPACE.fullmatch(self.release_name) is None:
            raise ValueError("qualification release name is invalid")

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str],
    ) -> KubernetesQualificationConfig:
        """Build the explicit immutable cluster/artifact selection."""

        validate_kubernetes_prerequisites(environment)
        context = environment["DEERFLOW_TEST_KUBERNETES_CONTEXT"]
        confirmation = environment.get("DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT")
        if confirmation != context:
            raise QualificationPrerequisiteError("DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT must exactly match DEERFLOW_TEST_KUBERNETES_CONTEXT")
        return cls(
            kubeconfig=Path(environment["KUBECONFIG"]).expanduser().resolve(),
            context=context,
            namespace=environment["DEERFLOW_TEST_KUBERNETES_NAMESPACE"],
            image_repository=environment["DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY"],
            image_digest=environment["DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST"],
            evidence_path=Path(environment["DEERFLOW_TEST_KUBERNETES_EVIDENCE"]).expanduser().resolve(),
            qualification_id=environment["DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID"],
            release_name=environment.get(
                "DEERFLOW_TEST_KUBERNETES_RELEASE",
                "hartmesh-qualification",
            ),
        )

    def kubectl(self, *arguments: str, namespaced: bool = True) -> tuple[str, ...]:
        if namespaced and any(argument in {"--all-namespaces", "-A", "--namespace", "-n"} for argument in arguments):
            raise ValueError("qualification commands cannot supply a namespace override")
        if not namespaced and arguments:
            mutation = arguments[0] in {"apply", "create", "delete", "patch", "replace", "scale"}
            own_namespace_mutation = arguments[:3] in {
                ("delete", "namespace", self.namespace),
                ("create", "namespace", self.namespace),
            }
            if mutation and not own_namespace_mutation:
                raise ValueError("cluster-scoped mutation is outside the qualification namespace")
        command = [
            "kubectl",
            "--kubeconfig",
            str(self.kubeconfig),
            "--context",
            self.context,
        ]
        if namespaced:
            command.extend(("--namespace", self.namespace))
        command.extend(arguments)
        return tuple(command)

    def helm(self, *arguments: str) -> tuple[str, ...]:
        """Build a Helm command pinned to the same explicit cluster identity."""

        if any(argument in {"--kubeconfig", "--kube-context", "--namespace", "-n"} for argument in arguments):
            raise ValueError("qualification Helm commands cannot override cluster identity")
        return (
            "helm",
            "--kubeconfig",
            str(self.kubeconfig),
            "--kube-context",
            self.context,
            "--namespace",
            self.namespace,
            *arguments,
        )

    def token_review(self) -> tuple[str, ...]:
        """Build the one permitted non-persistent cluster authentication call."""

        return (
            "kubectl",
            "--kubeconfig",
            str(self.kubeconfig),
            "--context",
            self.context,
            "create",
            "--raw",
            "/apis/authentication.k8s.io/v1/tokenreviews",
            "-f",
            "-",
        )


@dataclass(frozen=True, kw_only=True)
class KubernetesAcceptedSkillQualificationConfigV2(KubernetesQualificationConfig):
    """Exact artifact and RWX inputs for live nonempty skill qualification."""

    provisioner_image_repository: str
    provisioner_image_digest: str
    verifier_image_repository: str
    verifier_image_digest: str
    sandbox_image_repository: str
    sandbox_image_digest: str
    rwx_storage_class: str

    def __post_init__(self) -> None:
        super().__post_init__()
        for name in (
            "provisioner_image_repository",
            "verifier_image_repository",
            "sandbox_image_repository",
        ):
            if _IMAGE_REPOSITORY.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} is invalid")
        for name in (
            "provisioner_image_digest",
            "verifier_image_digest",
            "sandbox_image_digest",
        ):
            if _IMAGE_DIGEST.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} is invalid")
        if self.verifier_image_repository != self.provisioner_image_repository or self.verifier_image_digest != self.provisioner_image_digest:
            raise QualificationPrerequisiteError(
                "verifier image must exactly match the deployed provisioner image",
            )
        if _SAFE_STORAGE_CLASS.fullmatch(self.rwx_storage_class) is None:
            raise ValueError("qualification RWX storage class is invalid")

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str],
    ) -> KubernetesAcceptedSkillQualificationConfigV2:
        """Build a fail-closed v2 selection from explicit operator inputs."""

        validate_kubernetes_prerequisites(environment)
        if environment.get("DEERFLOW_TEST_KUBERNETES_SCOPE") != (ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2):
            raise QualificationPrerequisiteError(
                "accepted-skill qualification requires its explicit v2 scope",
            )
        context = environment["DEERFLOW_TEST_KUBERNETES_CONTEXT"]
        if environment.get("DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT") != context:
            raise QualificationPrerequisiteError(
                "DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT must exactly match DEERFLOW_TEST_KUBERNETES_CONTEXT",
            )
        return cls(
            kubeconfig=Path(environment["KUBECONFIG"]).expanduser().resolve(),
            context=context,
            namespace=environment["DEERFLOW_TEST_KUBERNETES_NAMESPACE"],
            image_repository=environment["DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY"],
            image_digest=environment["DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST"],
            evidence_path=Path(
                environment["DEERFLOW_TEST_KUBERNETES_EVIDENCE"],
            )
            .expanduser()
            .resolve(),
            qualification_id=environment["DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID"],
            release_name=environment.get(
                "DEERFLOW_TEST_KUBERNETES_RELEASE",
                "hartmesh-qualification",
            ),
            provisioner_image_repository=environment["DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY"],
            provisioner_image_digest=environment["DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST"],
            verifier_image_repository=environment["DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY"],
            verifier_image_digest=environment["DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST"],
            sandbox_image_repository=environment["DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY"],
            sandbox_image_digest=environment["DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST"],
            rwx_storage_class=environment["DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS"],
        )

    @property
    def provisioner_image_reference(self) -> str:
        return f"{self.provisioner_image_repository}@{self.provisioner_image_digest}"

    @property
    def verifier_image_reference(self) -> str:
        return f"{self.verifier_image_repository}@{self.verifier_image_digest}"

    @property
    def sandbox_image_reference(self) -> str:
        return f"{self.sandbox_image_repository}@{self.sandbox_image_digest}"


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


class _RuntimeHttpSession:
    """Small cookie-aware JSON client for the real Gateway port-forward."""

    def __init__(self, base_url: str) -> None:
        self._cookies = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self._cookies))
        self._base_url = base_url.rstrip("/")

    def set_base_url(self, base_url: str) -> None:
        """Retarget the retained authenticated session to a replacement pod."""

        self._base_url = base_url.rstrip("/")

    @property
    def base_url(self) -> str:
        return self._base_url

    def _csrf_token(self) -> str | None:
        return next(
            (cookie.value for cookie in self._cookies if cookie.name == "csrf_token"),
            None,
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        form_payload: Mapping[str, str] | None = None,
        timeout_seconds: float = 30,
    ) -> tuple[int, dict[str, object]]:
        if payload is not None and form_payload is not None:
            raise ValueError("HTTP request accepts JSON or form data, not both")
        body = None if payload is None else _canonical_json(payload)
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        elif form_payload is not None:
            body = urllib.parse.urlencode(form_payload).encode("ascii")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        csrf_token = self._csrf_token()
        if csrf_token and method not in {"GET", "HEAD", "OPTIONS"}:
            headers["X-CSRF-Token"] = csrf_token
        request = urllib.request.Request(
            self._base_url + path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            response = self._opener.open(request, timeout=timeout_seconds)
        except urllib.error.HTTPError as exc:
            response = exc
        response_body = response.read()
        try:
            decoded = json.loads(response_body) if response_body else {}
        except json.JSONDecodeError as exc:
            raise QualificationCommandError(f"Gateway returned non-JSON HTTP {response.status} for {path}") from exc
        if not isinstance(decoded, dict):
            raise QualificationCommandError(f"Gateway returned a non-object HTTP payload for {path}")
        return response.status, decoded


class _PortForward(AbstractContextManager["_PortForward"]):
    """Bounded service port-forward pinned to the qualification context."""

    def __init__(
        self,
        config: KubernetesQualificationConfig,
        service_name: str,
        *,
        resource_kind: Literal["service", "pod"] = "service",
        remote_port: int = 8001,
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._resource_kind = resource_kind
        self._remote_port = remote_port
        self._process: subprocess.Popen[str] | None = None
        self.port = 0

    def __enter__(self) -> _PortForward:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        command = self._config.kubectl(
            "port-forward",
            f"{self._resource_kind}/{self._service_name}",
            f"{self.port}:{self._remote_port}",
        )
        self._process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def listening() -> bool:
            if self._process is None or self._process.poll() is not None:
                output = "" if self._process is None or self._process.stdout is None else self._process.stdout.read()
                raise QualificationCommandError("Gateway port-forward exited before listening: " + output[-2048:])
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.25):
                    return True
            except OSError:
                return False

        wait_until(
            listening,
            description="Gateway port-forward",
            timeout_seconds=20,
            interval_seconds=0.2,
        )
        return self

    def __exit__(self, *exc_info: object) -> None:
        del exc_info
        self.close()

    def close(self) -> None:
        """Stop the forwarding subprocess; repeated cleanup is harmless."""

        if self._process is None or self._process.poll() is not None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)


class KubernetesQualificationRunner:
    """Distribution-neutral real-pod qualification orchestrator.

    Every mutation is pinned to the confirmed context and one disposable
    namespace. PostgreSQL and Redis remain running while only the Gateway pod
    is replaced. The runner deliberately uses Helm and kubectl CLIs instead of
    adding a Kubernetes client dependency to the backend test environment.
    """

    _TERMINAL_LIFECYCLE_TYPES = frozenset({"cancelled", "succeeded", "failed", "timed_out", "interrupted"})
    _SHUTDOWN_PHASES = (
        ("admission_seconds", 1.0),
        ("channel_seconds", 1.0),
        ("scheduler_seconds", 1.0),
        ("run_seconds", 4.0),
        ("dependencies_seconds", 1.0),
    )
    _MEMORY_SHUTDOWN_BUDGET_SECONDS = 1.0
    _PRESTOP_SECONDS = 1.0
    _SHUTDOWN_SCHEDULING_HEADROOM_SECONDS = 2.0
    _APPLICATION_SHUTDOWN_BUDGET_SECONDS = sum(value for _name, value in _SHUTDOWN_PHASES) + _MEMORY_SHUTDOWN_BUDGET_SECONDS
    _GRACEFUL_DEADLINE_SECONDS = _PRESTOP_SECONDS + _APPLICATION_SHUTDOWN_BUDGET_SECONDS
    _TERMINATION_GRACE_SECONDS = _GRACEFUL_DEADLINE_SECONDS + _SHUTDOWN_SCHEDULING_HEADROOM_SECONDS

    def __init__(
        self,
        config: KubernetesQualificationConfig,
        *,
        repository_root: Path | None = None,
    ) -> None:
        self.config = config
        self.repository_root = repository_root.resolve() if repository_root is not None else Path(__file__).resolve().parents[3]
        self.chart_path = self.repository_root / "deploy/helm/deer-flow"
        self.fullname = f"{config.release_name}-deer-flow"
        self.gateway_service = f"{self.fullname}-gateway"
        self.gateway_deployment = f"{self.fullname}-gateway"
        self.store_secret = "hartmesh-qualification-stores"
        self.runtime_config_map = "hartmesh-qualification-runtime"
        self._postgres_password = secrets.token_urlsafe(32)
        self._admin_password = secrets.token_urlsafe(24) + "Aa1!"
        self._admin_email = f"{config.qualification_id}@qualification.invalid"
        self._nonowner_password = secrets.token_urlsafe(24) + "Bb2!"
        self._nonowner_email = f"nonowner-{config.qualification_id}@qualification.invalid"

    def values(self) -> dict[str, object]:
        """Return deterministic Helm values without embedding credentials."""

        shutdown = dict(self._SHUTDOWN_PHASES)
        app_config = "\n".join(
            (
                "config_version: 40",
                "log_level: info",
                "models:",
                "  - name: kubernetes-qualification",
                "    display_name: Kubernetes qualification double",
                "    description: deterministic no-network qualification model",
                "    use: deerflow.runtime.kubernetes_qualification:KubernetesQualificationChatModel",
                "    model: kubernetes-qualification",
                "sandbox:",
                "  use: deerflow.sandbox.local:LocalSandboxProvider",
                "database:",
                "  backend: postgres",
                "  postgres_url: $DATABASE_URL",
                "  command_timeout: 30",
                "deployment:",
                "  profile: durable_production",
                "  readiness:",
                "    capability_cache_seconds: 2.0",
                "    admission_health_max_age_seconds: 2.0",
                "    required_health_stale_seconds: 6.0",
                "    capability_probe_timeout_seconds: 2.0",
                "    overall_timeout_seconds: 5.0",
                "    required_failure_threshold: 1",
                "  shutdown:",
                *(f"    {name}: {value}" for name, value in shutdown.items()),
                "checkpointer:",
                "  type: postgres",
                "  connection_string: $DATABASE_URL",
                "stream_bridge:",
                "  type: redis",
                "run_events:",
                "  backend: db",
                "memory:",
                "  enabled: false",
                f"  shutdown_flush_timeout_seconds: {self._MEMORY_SHUTDOWN_BUDGET_SECONDS}",
                "scheduler:",
                "  enabled: false",
                "channel_connections:",
                "  enabled: false",
                "subagents:",
                "  enabled: false",
                "title:",
                "  enabled: false",
                "tool_groups: []",
                "tools: []",
                "",
            )
        )
        return {
            "namespace": self.config.namespace,
            "tenant": {"id": "qualification"},
            "deployment": {
                "mode": "durable_one_replica",
                "persistenceTier": "shared_durable",
            },
            "gateway": {
                "image": {
                    "repository": self.config.image_repository,
                    "digest": self.config.image_digest,
                },
                "replicas": 1,
                "preStopSleepSeconds": self._PRESTOP_SECONDS,
                "shutdownSchedulingHeadroomSeconds": self._SHUTDOWN_SCHEDULING_HEADROOM_SECONDS,
                "readinessProbe": {
                    "initialDelaySeconds": 1,
                    "periodSeconds": 6,
                    "timeoutSeconds": 6,
                    "failureThreshold": 3,
                },
                "livenessProbe": {
                    "initialDelaySeconds": 10,
                    "periodSeconds": 5,
                    "timeoutSeconds": 2,
                    "failureThreshold": 6,
                },
                "extraEnvFrom": [{"configMapRef": {"name": self.runtime_config_map}}],
            },
            "frontend": {"replicas": 0},
            "nginx": {"replicas": 0},
            "provisioner": {"enabled": False},
            "ingress": {"enabled": False},
            "serviceAccount": {
                "create": True,
                "automountServiceAccountToken": False,
            },
            "postgresql": {
                "enabled": True,
                "existingSecret": self.store_secret,
                "primary": {"persistence": {"enabled": True, "size": "2Gi"}},
            },
            "redis": {
                "enabled": True,
                "existingSecret": self.store_secret,
                "primary": {"persistence": {"enabled": True, "size": "1Gi"}},
            },
            "persistence": {"home": {"enabled": False}},
            "config": app_config,
            "extensionsConfig": '{"mcpServers":{},"skills":{}}',
        }

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

    def _helm(self, *arguments: str, timeout_seconds: float = 180) -> str:
        return self._run(
            self.config.helm(*arguments),
            timeout_seconds=timeout_seconds,
        )

    def _write_values(self, values: Mapping[str, object], suffix: str) -> Path:
        target = self.config.evidence_path.parent / f".{self.config.qualification_id}-{suffix}.values.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_canonical_json(values) + b"\n")
        return target

    def _confirm_context(self) -> None:
        actual = self._kubectl(
            "config",
            "current-context",
            namespaced=False,
            timeout_seconds=10,
        )
        if actual != self.config.context:
            raise QualificationPrerequisiteError("KUBECONFIG current-context does not match the explicitly confirmed qualification context")
        self._kubectl(
            "version",
            "--output=json",
            namespaced=False,
            timeout_seconds=20,
        )

    def _apply_json(self, manifest: Mapping[str, object]) -> None:
        self._kubectl(
            "apply",
            "-f",
            "-",
            input_text=json.dumps(manifest),
            redact_diagnostics=True,
        )

    def _create_namespace_and_configuration(self) -> None:
        self._kubectl(
            "create",
            "namespace",
            self.config.namespace,
            namespaced=False,
        )
        database_url = f"postgresql://deerflow:{self._postgres_password}@{self.fullname}-postgres:5432/deerflow"
        redis_url = f"redis://{self.fullname}-redis:6379/0"
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": self.store_secret},
                "type": "Opaque",
                "stringData": {
                    "database-url": database_url,
                    "postgres-password": self._postgres_password,
                    "redis-url": redis_url,
                },
            }
        )
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": self.runtime_config_map},
                "data": {
                    "DEERFLOW_TEST_KUBERNETES_RUNTIME": "1",
                    "DEERFLOW_TEST_KUBERNETES_FAULT_INJECTION": "1",
                    "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID": self.config.qualification_id,
                    "DEERFLOW_TEST_KUBERNETES_REDIS_URL": redis_url,
                    "DEERFLOW_TEST_KUBERNETES_BARRIER_TIMEOUT_SECONDS": "180",
                },
            }
        )

    def _install(self, values: Mapping[str, object]) -> None:
        values_path = self._write_values(values, "initial")
        self._helm(
            "upgrade",
            "--install",
            self.config.release_name,
            str(self.chart_path),
            "--values",
            str(values_path),
            "--wait",
            "--timeout",
            "8m",
            timeout_seconds=510,
        )

    def _pod_json(self, component: str) -> dict[str, object]:
        raw = self._kubectl(
            "get",
            "pods",
            "-l",
            f"app.kubernetes.io/instance={self.config.release_name},app.kubernetes.io/component={component}",
            "-o",
            "json",
        )
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise QualificationCommandError("kubectl returned invalid pod JSON")
        return value

    def _ready_gateway(self, *, previous_uid: str | None = None) -> tuple[str, str]:
        result: tuple[str, str] | None = None

        def ready() -> bool:
            nonlocal result
            items = self._pod_json("gateway").get("items")
            if not isinstance(items, list):
                return False
            for item in items:
                if not isinstance(item, dict):
                    continue
                metadata = item.get("metadata", {})
                status = item.get("status", {})
                uid = metadata.get("uid") if isinstance(metadata, dict) else None
                name = metadata.get("name") if isinstance(metadata, dict) else None
                conditions = status.get("conditions", []) if isinstance(status, dict) else []
                is_ready = any(isinstance(condition, dict) and condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions)
                if isinstance(uid, str) and isinstance(name, str) and uid != previous_uid and is_ready:
                    result = (name, uid)
                    return True
            return False

        wait_until(
            ready,
            description="a replacement Ready Gateway pod",
            timeout_seconds=240,
            interval_seconds=2,
        )
        if result is None:  # pragma: no cover - wait contract
            raise QualificationTimeout("Gateway readiness result was unavailable")
        return result

    def _component_pod_name(self, component: str) -> str:
        items = self._pod_json(component).get("items")
        if not isinstance(items, list) or len(items) != 1:
            raise QualificationCommandError(f"qualification requires exactly one {component} pod")
        metadata = items[0].get("metadata") if isinstance(items[0], dict) else None
        name = metadata.get("name") if isinstance(metadata, dict) else None
        if not isinstance(name, str):
            raise QualificationCommandError(f"{component} pod has no name")
        return name

    def _redis(self, *arguments: str) -> str:
        return self._kubectl(
            "exec",
            self._component_pod_name("redis"),
            "--",
            "redis-cli",
            "--raw",
            *arguments,
        )

    def _barrier_key(self, scenario: str, suffix: str) -> str:
        return f"{_QUALIFICATION_REDIS_PREFIX}:{self.config.qualification_id}:{scenario}:{suffix}"

    def _wait_for_barrier(self, scenario: str) -> str:
        run_id = ""

        def reached() -> bool:
            nonlocal run_id
            run_id = self._redis("GET", self._barrier_key(scenario, "reached"))
            return bool(run_id)

        wait_until(
            reached,
            description=f"real-process barrier {scenario}",
            timeout_seconds=120,
            interval_seconds=1,
        )
        return run_id

    def _arm_barrier(self, scenario: str) -> None:
        self._redis(
            "DEL",
            self._barrier_key(scenario, "reached"),
            self._barrier_key(scenario, "release"),
            self._barrier_key(scenario, "owner_replica_id"),
        )
        if (
            self._redis(
                "SET",
                self._barrier_key(scenario, "arm"),
                "1",
                "EX",
                "300",
            )
            != "OK"
        ):
            raise QualificationCommandError("qualification barrier arm failed")

    def _counter(self, scenario: str, name: str) -> int:
        value = self._redis("GET", self._barrier_key(scenario, name))
        return int(value or "0")

    @staticmethod
    def _ensure_payload(scenario: str) -> dict[str, object]:
        return {
            "api_version": "deerflow.runtime/v1",
            "kind": "invocation.ensure",
            "external_key": f"k8s-qual-v1:{scenario}:delivery-1",
            "thread_id": f"k8s-qualification-{scenario}",
            "agent_hint": None,
            "input": {
                "api_version": "deerflow.runtime/v1",
                "kind": "invocation.input.graph",
                "value": {"messages": [{"role": "user", "content": "deterministic qualification"}]},
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

    def _initialize_admin(self, client: _RuntimeHttpSession) -> None:
        status, payload = client.request(
            "POST",
            "/api/v1/auth/initialize",
            payload={
                "email": self._admin_email,
                "password": self._admin_password,
                "remember_me": False,
            },
        )
        if status != 201:
            raise QualificationCommandError(f"qualification admin initialization failed with HTTP {status}: {sorted(payload)}")

    def _login_admin(self, client: _RuntimeHttpSession) -> None:
        status, payload = client.request(
            "POST",
            "/api/v1/auth/login/local",
            form_payload={
                "username": self._admin_email,
                "password": self._admin_password,
            },
        )
        if status != 200:
            raise QualificationCommandError(f"qualification admin login failed with HTTP {status}: {sorted(payload)}")

    def _register_nonowner(self, client: _RuntimeHttpSession) -> None:
        status, payload = client.request(
            "POST",
            "/api/v1/auth/register",
            payload={
                "email": self._nonowner_email,
                "password": self._nonowner_password,
                "remember_me": False,
            },
        )
        if status != 201:
            raise QualificationCommandError(f"qualification non-owner registration failed with HTTP {status}: {sorted(payload)}")

    def _assert_visibility_and_control_isolation(
        self,
        owner: _RuntimeHttpSession,
        nonowner: _RuntimeHttpSession,
        run_id: str,
    ) -> dict[str, object]:
        owner_status, observation = owner.request(
            "GET",
            f"/api/runtime/v1/invocations/{run_id}?limit=100",
        )
        state_version = observation.get("state_version")
        if owner_status != 200 or not isinstance(state_version, int):
            raise QualificationCommandError("owning principal could not observe the retained invocation")
        for label, caller, expected_statuses in (
            ("authenticated non-owner", nonowner, {404}),
            ("unauthenticated caller", _RuntimeHttpSession(owner.base_url), {401, 403, 404}),
        ):
            observe_status, _ = caller.request(
                "GET",
                f"/api/runtime/v1/invocations/{run_id}",
            )
            if observe_status not in expected_statuses:
                raise QualificationCommandError(f"{label} could observe another principal's invocation")
            control_status, _ = caller.request(
                "POST",
                f"/api/runtime/v1/invocations/{run_id}/control",
                payload={
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.cancel",
                    "run_id": run_id,
                    "expected_state_version": state_version,
                },
            )
            if control_status not in expected_statuses:
                raise QualificationCommandError(f"{label} could control another principal's invocation")
        return observation

    def _delete_gateway(self, pod_name: str, *, graceful: bool) -> None:
        arguments = ["delete", "pod", pod_name, "--wait=false"]
        if not graceful:
            arguments.extend(("--grace-period=0", "--force"))
        self._kubectl(*arguments, timeout_seconds=30)

    def _restart_gateway_deployment(self) -> None:
        """Exercise the chart's real Recreate rollout path."""

        self._kubectl(
            "rollout",
            "restart",
            f"deployment/{self.gateway_deployment}",
            timeout_seconds=30,
        )

    def _wait_for_recreate_handoff(
        self,
        pod_uid: str,
        *,
        started: float,
    ) -> float:
        """Require the old Gateway to disappear before a replacement exists."""

        def old_uid_is_gone_without_overlap() -> bool:
            items = self._pod_json("gateway").get("items")
            if not isinstance(items, list):
                return False
            observed_uids = {metadata.get("uid") for item in items if isinstance(item, dict) and isinstance((metadata := item.get("metadata")), dict) and isinstance(metadata.get("uid"), str)}
            if pod_uid in observed_uids and observed_uids - {pod_uid}:
                raise QualificationCommandError("Gateway Recreate rollout overlapped old and replacement pods")
            return pod_uid not in observed_uids

        wait_until(
            old_uid_is_gone_without_overlap,
            description=f"Recreate handoff for old Gateway pod UID {pod_uid}",
            timeout_seconds=60,
            interval_seconds=0.25,
        )
        return time.monotonic() - started

    def _wait_for_old_gateway_termination(
        self,
        pod_name: str,
        pod_uid: str,
        *,
        started: float,
    ) -> float:
        def old_uid_is_gone() -> bool:
            raw = self._kubectl(
                "get",
                "pod",
                pod_name,
                "--ignore-not-found=true",
                "-o",
                "json",
                timeout_seconds=15,
            )
            if not raw:
                return True
            value = json.loads(raw)
            metadata = value.get("metadata") if isinstance(value, dict) else None
            return not isinstance(metadata, dict) or metadata.get("uid") != pod_uid

        wait_until(
            old_uid_is_gone,
            description=f"old Gateway pod UID {pod_uid} termination",
            timeout_seconds=60,
            interval_seconds=0.25,
        )
        return time.monotonic() - started

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
                f"/api/runtime/v1/invocations/{run_id}?limit=100",
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
            description=f"terminal lifecycle for {run_id}",
            timeout_seconds=120,
            interval_seconds=1,
        )
        return observation

    def _run_scenario(
        self,
        scenario: str,
        client: _RuntimeHttpSession,
        owner_observer: _RuntimeHttpSession,
        nonowner: _RuntimeHttpSession,
        gateway_pod: tuple[str, str],
        port_forward: _PortForward,
        barrier_probe: Callable[[str, str], None] | None = None,
    ) -> tuple[ScenarioEvidence, tuple[str, str]]:
        payload = self._ensure_payload(scenario)
        request_result: dict[str, object] = {}
        self._arm_barrier(scenario)

        def ensure_request() -> None:
            try:
                request_result["response"] = client.request(
                    "POST",
                    "/api/runtime/v1/invocations/ensure",
                    payload=payload,
                    timeout_seconds=30,
                )
            except Exception as exc:  # expected when the serving pod is killed
                request_result["error_class"] = type(exc).__name__

        with ThreadPoolExecutor(max_workers=1) as executor:
            future: Future[None] = executor.submit(ensure_request)
            run_id = self._wait_for_barrier(scenario)
            if barrier_probe is not None:
                barrier_probe(scenario, run_id)
            if scenario == "accepted_before_client_response" and future.done():
                raise QualificationCommandError("client response completed before the response-loss barrier")
            self._assert_visibility_and_control_isolation(
                owner_observer,
                nonowner,
                run_id,
            )
            pod_name, pod_uid = gateway_pod
            # Drop the caller connection at the reached commit boundary, then
            # kill the actual serving pod. This prevents a stale port-forward
            # subprocess from extending the bounded client wait.
            port_forward.close()
            started = time.monotonic()
            if scenario == "graceful_rollout_termination":
                self._restart_gateway_deployment()
                termination_elapsed = self._wait_for_recreate_handoff(
                    pod_uid,
                    started=started,
                )
            else:
                graceful = scenario == "forced_kill_after_graceful_deadline"
                self._delete_gateway(pod_name, graceful=graceful)
                termination_elapsed = self._wait_for_old_gateway_termination(
                    pod_name,
                    pod_uid,
                    started=started,
                )
            replacement = self._ready_gateway(previous_uid=pod_uid)
            if scenario == "graceful_rollout_termination":
                if self._counter(scenario, "cancellation_observed") < 1:
                    raise QualificationCommandError("graceful termination did not reach the run cancellation fence")
                if termination_elapsed >= self._TERMINATION_GRACE_SECONDS:
                    raise QualificationCommandError("graceful termination reached the Kubernetes force deadline")
            if scenario == "forced_kill_after_graceful_deadline":
                if self._counter(scenario, "cancellation_observed") < 1:
                    raise QualificationCommandError("forced termination did not first attempt graceful cancellation")
                if termination_elapsed + 0.25 < self._TERMINATION_GRACE_SECONDS:
                    raise QualificationCommandError("forced termination completed before the Kubernetes termination deadline")
            try:
                future.result(timeout=35)
            except TimeoutError as exc:
                raise QualificationTimeout(f"client request did not end after pod replacement for {scenario}") from exc

        # The Service target changed, so use a fresh bounded port-forward while
        # retaining the authenticated cookie jar in the caller-owned client.
        with _PortForward(self.config, self.gateway_service) as forwarded:
            base_url = f"http://127.0.0.1:{forwarded.port}"
            client.set_base_url(base_url)
            owner_observer.set_base_url(base_url)
            nonowner.set_base_url(base_url)
            status, replay = client.request(
                "POST",
                "/api/runtime/v1/invocations/ensure",
                payload=payload,
            )
            if status != 200 or replay.get("disposition") != "known" or replay.get("run_id") != run_id:
                raise QualificationCommandError(f"same-key replay did not retain {run_id} for {scenario}")
            observation = self._observe_until_terminal(client, run_id)
            state_version = observation.get("state_version")
            if not isinstance(state_version, int):
                raise QualificationCommandError("terminal observation omitted state_version")
            self._assert_visibility_and_control_isolation(
                owner_observer,
                nonowner,
                run_id,
            )
            control_status, control = client.request(
                "POST",
                f"/api/runtime/v1/invocations/{run_id}/control",
                payload={
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.cancel",
                    "run_id": run_id,
                    "expected_state_version": state_version,
                },
            )
            if control_status != 200 or control.get("disposition") != "already_terminal":
                raise QualificationCommandError("terminal cancellation did not preserve authoritative receipt ordering")
            graph_starts = self._counter(scenario, "graph_starts")
            model_starts = self._counter(scenario, "model_starts")
            worker_attachments = self._counter(scenario, "worker_attachments")
            second_status, second_replay = client.request(
                "POST",
                "/api/runtime/v1/invocations/ensure",
                payload=payload,
            )
            retained_fields = ("disposition", "run_id", "thread_id", "status", "state_version")
            if second_status != 200 or second_replay.get("run_id") != run_id or any(second_replay.get(field) != replay.get(field) for field in retained_fields):
                raise QualificationCommandError("stable replay failed after terminal observation")
            if graph_starts != self._counter(scenario, "graph_starts") or model_starts != self._counter(scenario, "model_starts"):
                raise QualificationCommandError("terminal replay started duplicate graph/model work")
            events = observation.get("events")
            if not isinstance(events, list):
                raise QualificationCommandError("observation omitted lifecycle events")
            lifecycle_types = [event.get("lifecycle_type") for event in events if isinstance(event, dict)]
            if lifecycle_types.count("accepted") != 1:
                raise QualificationCommandError("lifecycle did not contain exactly one acceptance")
            if lifecycle_types.count("started") > 1:
                raise QualificationCommandError("lifecycle contained duplicate started evidence")
            terminal_events = [kind for kind in lifecycle_types if kind in self._TERMINAL_LIFECYCLE_TYPES]
            if len(terminal_events) != 1:
                raise QualificationCommandError("lifecycle did not contain exactly one terminal event")
            if worker_attachments != _expected_worker_attachments(scenario):
                raise QualificationCommandError(f"unexpected worker attachment count for {scenario}: {worker_attachments}")
            if not _execution_counts_are_valid(
                scenario,
                graph_starts,
                model_starts,
            ):
                raise QualificationCommandError(f"unexpected execution count for {scenario}: graph={graph_starts}, model={model_starts}")
            return (
                ScenarioEvidence(
                    name=scenario,
                    run_id=run_id,
                    worker_attachments=worker_attachments,
                    graph_starts=graph_starts,
                    model_starts=model_starts,
                    terminal_status=str(observation["status"]),
                    termination_mode=_expected_termination_mode(scenario),
                    old_pod_termination_millis=round(termination_elapsed * 1000),
                ),
                replacement,
            )

    def _chart_version(self) -> str:
        chart = (self.chart_path / "Chart.yaml").read_text(encoding="utf-8")
        match = re.search(r"^version:\s*[\"']?([^\s\"']+)", chart, re.MULTILINE)
        if match is None:
            raise QualificationCommandError("chart version is unavailable")
        return match.group(1)

    def _chart_digest(self) -> str:
        digest = hashlib.sha256()
        for path in sorted(item for item in self.chart_path.rglob("*") if item.is_file()):
            digest.update(path.relative_to(self.chart_path).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return "sha256:" + digest.hexdigest()

    def _store_continuity_evidence(self, component: Literal["postgres", "redis"]) -> StoreContinuityEvidence:
        pod_document = self._pod_json(component)
        items = pod_document.get("items")
        if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
            raise QualificationCommandError(f"qualification requires exactly one {component} store pod")
        metadata = items[0].get("metadata")
        status = items[0].get("status")
        pod_uid = metadata.get("uid") if isinstance(metadata, dict) else None
        statuses = status.get("containerStatuses") if isinstance(status, dict) else None
        container = next(
            (item for item in statuses or [] if isinstance(item, dict) and item.get("name") == component),
            None,
        )
        image_id = container.get("imageID") if isinstance(container, dict) else None
        volumes = json.loads(
            self._kubectl(
                "get",
                "persistentvolumeclaims",
                "-l",
                f"app.kubernetes.io/instance={self.config.release_name},app.kubernetes.io/component={component}",
                "-o",
                "json",
            )
        )
        volume_items = volumes.get("items") if isinstance(volumes, dict) else None
        if not isinstance(volume_items, list) or len(volume_items) != 1 or not isinstance(volume_items[0], dict):
            raise QualificationCommandError(f"qualification requires exactly one {component} persistent volume claim")
        volume_metadata = volume_items[0].get("metadata")
        volume_uid = volume_metadata.get("uid") if isinstance(volume_metadata, dict) else None
        if not all(isinstance(value, str) and value for value in (pod_uid, image_id, volume_uid)):
            raise QualificationCommandError(f"{component} store identity is incomplete")
        pod_name = self._component_pod_name(component)
        if component == "postgres":
            version = self._kubectl("exec", pod_name, "--", "postgres", "--version")
        else:
            version = self._kubectl("exec", pod_name, "--", "redis-server", "--version")
        return StoreContinuityEvidence(
            component=component,
            pod_uid=pod_uid,
            volume_uid=volume_uid,
            image_id=image_id,
            version=version,
        )

    def _shared_store_evidence(self) -> tuple[StoreContinuityEvidence, ...]:
        return (
            self._store_continuity_evidence("postgres"),
            self._store_continuity_evidence("redis"),
        )

    def _environment_facts(self, gateway_pod_name: str) -> dict[str, str]:
        postgres_pod = self._component_pod_name("postgres")
        migration_head = self._kubectl(
            "exec",
            postgres_pod,
            "--",
            "psql",
            "-U",
            "deerflow",
            "-d",
            "deerflow",
            "-Atc",
            "select version_num from alembic_version",
        )
        kubernetes = json.loads(
            self._kubectl(
                "version",
                "--output=json",
                namespaced=False,
            )
        )
        server_version = kubernetes.get("serverVersion", {}).get("gitVersion")
        if not isinstance(server_version, str):
            raise QualificationCommandError("Kubernetes server version is unavailable")
        pod = json.loads(self._kubectl("get", "pod", gateway_pod_name, "-o", "json"))
        statuses = pod.get("status", {}).get("containerStatuses", [])
        gateway_status = next(
            (item for item in statuses if item.get("name") == "gateway"),
            None,
        )
        image_id = gateway_status.get("imageID") if isinstance(gateway_status, dict) else None
        if not isinstance(image_id, str) or self.config.image_digest not in image_id:
            raise QualificationCommandError("running Gateway imageID does not match the qualified digest")
        return {
            "migration_head": migration_head,
            "kubernetes_server_version": server_version,
        }

    def _publish_qualification(
        self,
        values: dict[str, object],
        evidence: KubernetesQualificationEvidence,
        client: _RuntimeHttpSession,
    ) -> Path:
        passing_path = self.config.evidence_path.with_suffix(
            self.config.evidence_path.suffix + ".passing",
        )
        evidence.write(passing_path)
        evidence_digest = evidence_sha256(passing_path)
        verification = verify_qualification_evidence(
            passing_path.read_bytes(),
            declared_digest=evidence_digest,
            expected=QualificationEvidenceExpectation(
                qualification_id=evidence.qualification_id,
                image_digest=self.config.image_digest,
                chart_version=self._chart_version(),
                chart_digest=self._chart_digest(),
                configuration_digest=_sha256_bytes(_canonical_json(values)),
                migration_head=evidence.migration_head,
                scope=evidence.SCOPE,
                namespace=self.config.namespace,
                required_scenarios=evidence.REQUIRED_SCENARIOS,
            ),
        )
        if verification.artifact_digest != evidence_digest:
            raise QualificationCommandError("offline qualification verification returned the wrong digest")
        completed_at = evidence.completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        published = json.loads(json.dumps(values))
        published["deployment"]["qualificationEvidence"] = [
            {
                "qualificationId": evidence.qualification_id,
                "artifactDigest": evidence_digest,
                "completedAt": completed_at,
                "scope": evidence.SCOPE,
                "status": "passed",
            }
        ]
        values_path = self._write_values(published, "qualified")
        self._helm(
            "upgrade",
            self.config.release_name,
            str(self.chart_path),
            "--values",
            str(values_path),
            "--wait",
            "--timeout",
            "8m",
            timeout_seconds=510,
        )
        self._ready_gateway()
        with _PortForward(self.config, self.gateway_service) as forwarded:
            client.set_base_url(f"http://127.0.0.1:{forwarded.port}")
            status, report = client.request("GET", "/api/runtime/v1/deployment")
        if status != 200:
            raise QualificationCommandError("qualified administrative deployment report was unavailable")
        qualification = report.get("qualification")
        entries = qualification.get("evidence") if isinstance(qualification, dict) else None
        if (
            not isinstance(qualification, dict)
            or qualification.get("trust") != "operator_asserted"
            or not isinstance(entries, list)
            or not any(
                isinstance(entry, dict) and entry.get("qualification_id") == evidence.qualification_id and entry.get("artifact_digest") == evidence_digest and entry.get("scope") == evidence.SCOPE and entry.get("status") == "passed"
                for entry in entries
            )
        ):
            raise QualificationCommandError("administrative report did not expose bounded passing qualification evidence")
        return passing_path

    def _collect_failure_artifacts(self) -> None:
        target = self.config.evidence_path.with_suffix(".failure-artifacts")
        target.mkdir(parents=True, exist_ok=True)
        commands = {
            "resources.txt": ("get", "all", "-o", "wide"),
            "events.txt": ("get", "events", "--sort-by=.metadata.creationTimestamp"),
            "gateway-logs.txt": (
                "logs",
                "-l",
                f"app.kubernetes.io/instance={self.config.release_name},app.kubernetes.io/component=gateway",
                "--all-containers=true",
                "--prefix=true",
                "--tail=2000",
            ),
        }
        for filename, arguments in commands.items():
            try:
                output = self._kubectl(*arguments, timeout_seconds=30)
            except Exception as exc:
                output = f"collection_failed:{type(exc).__name__}"
            output = output[-1024 * 1024 :]
            (target / filename).write_text(output, encoding="utf-8")

    def qualify(self) -> KubernetesQualificationEvidence:
        """Run all required real-pod scenarios and publish passing evidence."""

        validate_kubernetes_prerequisites(os.environ)
        self._confirm_context()
        values = self.values()
        scenarios: list[ScenarioEvidence] = []
        passing_path: Path | None = None
        try:
            self._create_namespace_and_configuration()
            self._install(values)
            gateway_pod = self._ready_gateway()
            stores = self._shared_store_evidence()
            with _PortForward(self.config, self.gateway_service) as forwarded:
                client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                self._initialize_admin(client)
                owner_observer = _RuntimeHttpSession(client.base_url)
                self._login_admin(owner_observer)
                nonowner = _RuntimeHttpSession(client.base_url)
                self._register_nonowner(nonowner)
            for scenario in KubernetesQualificationEvidence.REQUIRED_SCENARIOS:
                with _PortForward(self.config, self.gateway_service) as forwarded:
                    base_url = f"http://127.0.0.1:{forwarded.port}"
                    client.set_base_url(base_url)
                    owner_observer.set_base_url(base_url)
                    nonowner.set_base_url(base_url)
                    scenario_evidence, gateway_pod = self._run_scenario(
                        scenario,
                        client,
                        owner_observer,
                        nonowner,
                        gateway_pod,
                        forwarded,
                    )
                scenarios.append(scenario_evidence)
                if self._shared_store_evidence() != stores:
                    raise QualificationCommandError("shared PostgreSQL or Redis identity changed during Gateway faults")
            facts = self._environment_facts(gateway_pod[0])
            evidence = KubernetesQualificationEvidence(
                qualification_id=self.config.qualification_id,
                image_reference=(f"{self.config.image_repository}@{self.config.image_digest}"),
                image_digest=self.config.image_digest,
                chart_version=self._chart_version(),
                chart_digest=self._chart_digest(),
                configuration_digest=_sha256_bytes(_canonical_json(values)),
                migration_head=facts["migration_head"],
                stores=stores,
                kubernetes_server_version=facts["kubernetes_server_version"],
                cluster_context=self.config.context,
                cluster_driver=optional_cluster_driver(os.environ),
                namespace=self.config.namespace,
                completed_at=datetime.now(UTC),
                scenarios=tuple(scenarios),
            )
            passing_path = self.config.evidence_path.with_suffix(
                self.config.evidence_path.suffix + ".passing",
            )
            if self._publish_qualification(values, evidence, client) != passing_path:
                raise QualificationCommandError("passing evidence staging path changed unexpectedly")
            if self._shared_store_evidence() != stores:
                raise QualificationCommandError("shared PostgreSQL or Redis identity changed while publishing evidence")
            self._kubectl(
                "delete",
                "namespace",
                self.config.namespace,
                "--wait=true",
                "--ignore-not-found=true",
                namespaced=False,
                timeout_seconds=180,
            )
            os.replace(passing_path, self.config.evidence_path)
            return evidence
        except Exception as exc:
            if passing_path is not None:
                passing_path.unlink(missing_ok=True)
            self._collect_failure_artifacts()
            try:
                KubernetesQualificationFailureEvidence(
                    qualification_id=self.config.qualification_id,
                    image_digest=self.config.image_digest,
                    chart_version=self._chart_version(),
                    chart_digest=self._chart_digest(),
                    configuration_digest=_sha256_bytes(_canonical_json(values)),
                    cluster_context=self.config.context,
                    namespace=self.config.namespace,
                    completed_at=datetime.now(UTC),
                    completed_scenarios=tuple(item.name for item in scenarios),
                    failure_code=type(exc).__name__,
                ).write(self.config.evidence_path)
            except Exception as evidence_exc:
                raise QualificationCommandError("qualification failed and bounded failure evidence could not be written") from evidence_exc
            raise


@dataclass(frozen=True)
class _AcceptedSkillAttemptObservation:
    scenario: str
    run_id: str
    sandbox_id: str
    pod_name: str
    pod_uid: str
    gateway_node: str
    pod_node: str
    lease_name: str
    lease_uid: str
    receipt: Mapping[str, object]
    materialization_digest: str
    verifier_receipt_digest: str
    token_review_authenticated: bool
    lease_renewals: int
    session_validation_passes: int = 0
    raced_provider_calls: int = 0
    post_loss_rejections: int = 0
    stale_terminal_rejected: bool = False

    def result_digest(
        self,
        *,
        evidence_scenario: str,
        cleanup_outcome: str,
        gateway_replacement_uid: str | None = None,
    ) -> str:
        return _sha256_bytes(
            _canonical_json(
                {
                    "fault_scenario": self.scenario,
                    "evidence_scenario": evidence_scenario,
                    "run_id": self.run_id,
                    "sandbox_id": self.sandbox_id,
                    "pod_uid": self.pod_uid,
                    "gateway_node": self.gateway_node,
                    "pod_node": self.pod_node,
                    "lease_uid": self.lease_uid,
                    "receipt": dict(self.receipt),
                    "materialization_digest": self.materialization_digest,
                    "verifier_receipt_digest": self.verifier_receipt_digest,
                    "session_validation_passes": (self.session_validation_passes),
                    "raced_provider_calls": self.raced_provider_calls,
                    "post_loss_rejections": self.post_loss_rejections,
                    "stale_terminal_rejected": self.stale_terminal_rejected,
                    "cleanup_outcome": cleanup_outcome,
                    "gateway_replacement_uid": gateway_replacement_uid,
                }
            )
        )


class KubernetesAcceptedSkillQualificationRunnerV2(KubernetesQualificationRunner):
    """Live runner for the separate cross-node nonempty-skill evidence scope."""

    skill_source_claim = "hartmesh-qualification-skill-source"
    skill_fixture_config_map = "hartmesh-qualification-skill-fixture"
    skill_fixture_job = "hartmesh-qualification-skill-fixture"
    accepted_qualification_mount = "/var/run/hartmesh/qualification"

    def __init__(
        self,
        config: KubernetesAcceptedSkillQualificationConfigV2,
        *,
        repository_root: Path | None = None,
    ) -> None:
        super().__init__(config, repository_root=repository_root)
        self.config = config

    def values(self) -> dict[str, object]:
        """Select the exact pinned remote-AIO and RWX projection profile."""

        values = super().values()
        local_sandbox = "sandbox:\n  use: deerflow.sandbox.local:LocalSandboxProvider"
        remote_sandbox = "\n".join(
            (
                "sandbox:",
                "  use: deerflow.community.aio_sandbox:AioSandboxProvider",
                f"  provisioner_url: http://{self.fullname}-provisioner:8002",
                "  accepted_skill_projection_profile: rwx_verified_copy_v2",
                "  replicas: 1",
                "  idle_timeout: 0",
            )
        )
        config_text = str(values["config"])
        if local_sandbox not in config_text:
            raise QualificationCommandError(
                "qualification base sandbox configuration changed",
            )
        values["config"] = config_text.replace(
            local_sandbox,
            remote_sandbox,
            1,
        )
        empty_tools = "tool_groups: []\ntools: []"
        qualification_tools = "\n".join(
            (
                "tool_groups:",
                "  - name: qualification",
                "tools:",
                "  - name: qualification_sandbox_operation",
                "    group: qualification",
                "    use: deerflow.runtime.kubernetes_qualification:qualification_sandbox_operation",
            )
        )
        if empty_tools not in values["config"]:
            raise QualificationCommandError(
                "qualification base tool configuration changed",
            )
        values["config"] = str(values["config"]).replace(
            empty_tools,
            qualification_tools,
            1,
        )
        values["provisioner"] = {
            "enabled": True,
            "image": {
                "repository": self.config.provisioner_image_repository,
                "digest": self.config.provisioner_image_digest,
            },
            "sandboxImage": self.config.sandbox_image_reference,
            "sandboxServiceType": "ClusterIP",
            "acceptedSkillProjectionProfile": "rwx_verified_copy_v2",
            "acceptedAttempt": {
                "leaseSeconds": _QUALIFICATION_ACCEPTED_ATTEMPT_LEASE_SECONDS,
                "reconcileIntervalSeconds": 30,
                "reconcileLimit": 100,
            },
        }
        values["persistence"] = {
            "home": {
                "enabled": True,
                "storageClass": self.config.rwx_storage_class,
                "accessMode": "ReadWriteMany",
                "size": "2Gi",
            }
        }
        values["skills"] = {
            "enabled": True,
            "existingClaim": self.skill_source_claim,
            "configMap": "",
        }
        values["deployment"]["qualificationCandidate"] = {
            "enabled": True,
            "id": self.config.qualification_id,
        }
        return values

    @staticmethod
    def _ensure_payload(scenario: str) -> dict[str, object]:
        payload = KubernetesQualificationRunner._ensure_payload(scenario)
        if scenario == "terminal_before_lifecycle_commit":
            payload["external_key"] = f"{payload['external_key']}:during-tool"
        return payload

    def _create_namespace_and_configuration(self) -> None:
        super()._create_namespace_and_configuration()
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": self.skill_source_claim},
                "spec": {
                    "accessModes": ["ReadWriteMany"],
                    "storageClassName": self.config.rwx_storage_class,
                    "resources": {"requests": {"storage": "64Mi"}},
                },
            }
        )
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": self.skill_fixture_config_map},
                "data": {
                    "skill-md": _QUALIFICATION_SKILL_FILES["SKILL.md"].decode("utf-8"),
                    "proof": _QUALIFICATION_SKILL_FILES["resources/proof.txt"].decode("utf-8"),
                },
            }
        )
        fixture_script = "\n".join(
            (
                "from pathlib import Path",
                "root = Path('/skills/public/qualification-skill')",
                "(root / 'resources').mkdir(parents=True, exist_ok=True)",
                "(root / 'SKILL.md').write_bytes(Path('/fixture/skill-md').read_bytes())",
                "(root / 'resources/proof.txt').write_bytes(Path('/fixture/proof').read_bytes())",
                "for path in (root, root / 'resources'):",
                "    path.chmod(0o755)",
                "for path in (root / 'SKILL.md', root / 'resources/proof.txt'):",
                "    path.chmod(0o444)",
            )
        )
        self._apply_json(
            {
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
                                    "image": self.config.verifier_image_reference,
                                    "command": ["python", "-c", fixture_script],
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
                                    "persistentVolumeClaim": {"claimName": self.skill_source_claim},
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
        )

        def fixture_ready() -> bool:
            raw = self._kubectl(
                "get",
                "job",
                self.skill_fixture_job,
                "-o",
                "json",
            )
            value = json.loads(raw)
            return value.get("status", {}).get("succeeded") == 1

        wait_until(
            fixture_ready,
            description="deterministic accepted-skill fixture population",
            timeout_seconds=180,
            interval_seconds=2,
        )

    def _schedulable_nodes(self) -> tuple[str, ...]:
        value = json.loads(self._kubectl("get", "nodes", "-o", "json", namespaced=False))
        items = value.get("items") if isinstance(value, dict) else None
        nodes: list[str] = []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            spec = item.get("spec")
            metadata = item.get("metadata")
            conditions = item.get("status", {}).get("conditions", [])
            ready = any(isinstance(condition, dict) and condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions)
            if isinstance(spec, dict) and not spec.get("unschedulable", False) and isinstance(metadata, dict) and isinstance(metadata.get("name"), str) and ready:
                nodes.append(metadata["name"])
        result = tuple(sorted(nodes))
        if len(result) < 2:
            raise QualificationPrerequisiteError(
                "accepted-skill qualification requires at least two schedulable Ready nodes",
            )
        return result

    @staticmethod
    def _resource_for_run(
        document: object,
        *,
        run_id: str,
        annotation: str,
        kind: str,
    ) -> dict[str, object]:
        items = document.get("items") if isinstance(document, dict) else None
        matches = []
        for item in items if isinstance(items, list) else []:
            metadata = item.get("metadata") if isinstance(item, dict) else None
            annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
            if isinstance(annotations, dict) and annotations.get(annotation) == run_id:
                matches.append(item)
        if len(matches) != 1:
            raise QualificationCommandError(
                f"qualification requires exactly one {kind} for the accepted run",
            )
        return matches[0]

    def _accepted_attempt(
        self,
        scenario: str,
        run_id: str,
        *,
        gateway_node: str,
    ) -> _AcceptedSkillAttemptObservation:
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
        pod_metadata = pod.get("metadata", {})
        pod_spec = pod.get("spec", {})
        lease_metadata = lease.get("metadata", {})
        annotations = lease_metadata.get("annotations", {})
        labels = pod_metadata.get("labels", {})
        pod_name = pod_metadata.get("name")
        pod_uid = pod_metadata.get("uid")
        pod_node = pod_spec.get("nodeName")
        sandbox_id = labels.get("sandbox-id")
        lease_name = lease_metadata.get("name")
        lease_uid = lease_metadata.get("uid")
        bounded_strings = (
            pod_name,
            pod_uid,
            pod_node,
            sandbox_id,
            lease_name,
            lease_uid,
        )
        if not all(isinstance(value, str) and value for value in bounded_strings):
            raise QualificationCommandError(
                "accepted-skill Pod or Lease identity is incomplete",
            )
        if labels.get("hartmesh.io/accepted-skill-profile") != ("rwx_verified_copy_v2") or annotations.get("hartmesh.io/accepted-attempt-state") != "materialized":
            raise QualificationCommandError(
                "accepted-skill attempt was not materialized under v2",
            )
        receipt_raw = self._kubectl(
            "exec",
            pod_name,
            "-c",
            "accepted-skill-gate",
            "--",
            "cat",
            "/var/run/hartmesh/accepted-receipt/receipt.json",
        )
        receipt = json.loads(receipt_raw)
        required_receipt = {
            "version",
            "profile",
            "snapshot_id",
            "content_digest",
            "file_count",
            "total_bytes",
        }
        if not isinstance(receipt, dict) or set(receipt) != required_receipt or receipt.get("version") != 2 or receipt.get("profile") != "rwx_verified_copy_v2" or receipt.get("snapshot_id") != receipt.get("content_digest"):
            raise QualificationCommandError(
                "accepted-skill verifier receipt is incomplete",
            )
        snapshot_id = receipt["snapshot_id"]
        if not isinstance(snapshot_id, str) or re.fullmatch(r"[0-9a-f]{64}", snapshot_id) is None:
            raise QualificationCommandError(
                "accepted-skill snapshot identity is invalid",
            )
        for relative, expected in _QUALIFICATION_SKILL_FILES.items():
            observed = base64.b64decode(
                self._kubectl(
                    "exec",
                    pod_name,
                    "-c",
                    "sandbox",
                    "--",
                    "python",
                    "-c",
                    ("import base64,pathlib;print(base64.b64encode(pathlib.Path(" + repr(f"/mnt/skills/.accepted/{snapshot_id}/public/{_QUALIFICATION_SKILL_NAME}/{relative}") + ").read_bytes()).decode('ascii'))"),
                )
            )
            if observed != expected:
                raise QualificationCommandError(
                    "accepted-skill sandbox bytes differ from the deterministic fixture",
                )
        expected_images = {
            "sandbox": self.config.sandbox_image_digest,
            "accepted-skill-gate": self.config.verifier_image_digest,
            "accepted-skill-verifier": self.config.verifier_image_digest,
        }
        statuses = {item.get("name"): item.get("imageID") for item in ((pod.get("status", {}).get("containerStatuses") or []) + (pod.get("status", {}).get("initContainerStatuses") or [])) if isinstance(item, dict)}
        if any(not isinstance(statuses.get(name), str) or digest not in statuses[name] for name, digest in expected_images.items()):
            raise QualificationCommandError(
                "accepted-skill runtime image identity is not exact",
            )
        materialization_digest = annotations.get("hartmesh.io/accepted-materialization-digest")
        verifier_receipt_digest = annotations.get("hartmesh.io/accepted-verifier-receipt-digest")
        if any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in (materialization_digest, verifier_receipt_digest)):
            raise QualificationCommandError(
                "accepted-skill materialization digests are invalid",
            )
        try:
            execution_evidence = decode_accepted_execution_evidence(
                self._run_execution_evidence(run_id),
            )
            ownership_epoch = int(
                annotations["hartmesh.io/accepted-skill-generation"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise QualificationCommandError(
                "accepted-skill ledger evidence is malformed",
            ) from exc
        expected_profile = AioSandboxProvider.accepted_sandbox_capability_profile()
        if (
            execution_evidence.run_id != run_id
            or execution_evidence.provider_kind != "aio_kubernetes"
            or not isinstance(execution_evidence, AcceptedExecutionEvidenceV2)
            or execution_evidence.provider_resource_commitment
            != accepted_sandbox_resource_commitment(
                tenant_digest=execution_evidence.tenant.digest,
                provider_kind="aio_kubernetes",
                provider_instance_ref=sandbox_id,
            )
            or execution_evidence.ownership_epoch != ownership_epoch
            or execution_evidence.runtime_image_digest != self.config.sandbox_image_digest.removeprefix("sha256:")
            or execution_evidence.skill_snapshot_digest != snapshot_id
            or execution_evidence.materialization_digest != materialization_digest
            or execution_evidence.verifier_image_digest != self.config.verifier_image_digest.removeprefix("sha256:")
            or execution_evidence.verifier_contract_version != "rwx_verified_copy_v2:accepted_execution_claim_v2"
            or execution_evidence.qualification_scope != ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2
            or execution_evidence.capability_profile_digest != expected_profile.digest
            or not execution_evidence.isolation.restricted_non_root
            or not execution_evidence.isolation.read_only_accepted_material
            or not execution_evidence.isolation.privilege_escalation_disabled
        ):
            raise QualificationCommandError(
                "accepted-skill ledger evidence does not match the live attempt",
            )
        return _AcceptedSkillAttemptObservation(
            scenario=scenario,
            run_id=run_id,
            sandbox_id=sandbox_id,
            pod_name=pod_name,
            pod_uid=pod_uid,
            gateway_node=gateway_node,
            pod_node=pod_node,
            lease_name=lease_name,
            lease_uid=lease_uid,
            receipt=receipt,
            materialization_digest=materialization_digest,
            verifier_receipt_digest=verifier_receipt_digest,
            token_review_authenticated=False,
            lease_renewals=0,
        )

    def _run_execution_evidence(self, run_id: str) -> dict[str, object]:
        if re.fullmatch(r"[A-Za-z0-9-]{1,64}", run_id) is None:
            raise QualificationCommandError("qualification run identity is invalid")
        postgres_pod = self._component_pod_name("postgres")
        raw = self._kubectl(
            "exec",
            postgres_pod,
            "--",
            "psql",
            "-U",
            "deerflow",
            "-d",
            "deerflow",
            "-Atc",
            (f"select execution_evidence_json::text from runs where run_id = '{run_id}'"),
        )
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise QualificationCommandError(
                "accepted-skill ledger evidence is unavailable",
            ) from exc
        if not isinstance(value, dict):
            raise QualificationCommandError(
                "accepted-skill ledger evidence is malformed",
            )
        return value

    def _wait_for_lease_renewal(
        self,
        attempt: _AcceptedSkillAttemptObservation,
    ) -> _AcceptedSkillAttemptObservation:
        first = json.loads(
            self._kubectl(
                "get",
                "lease",
                attempt.lease_name,
                "-o",
                "json",
            )
        )
        first_identity, first_holder, first_duration, first_renew_time = self._lease_renewal_facts(first, attempt)

        def renewed() -> bool:
            value = json.loads(
                self._kubectl(
                    "get",
                    "lease",
                    attempt.lease_name,
                    "-o",
                    "json",
                )
            )
            identity, holder, duration, renew_time = self._lease_renewal_facts(
                value,
                attempt,
            )
            if identity != first_identity or holder != first_holder:
                raise QualificationCommandError(
                    "accepted-skill Lease holder identity changed during renewal",
                )
            if duration != first_duration:
                raise QualificationCommandError(
                    "accepted-skill Lease duration changed during renewal",
                )
            return renew_time > first_renew_time

        wait_until(
            renewed,
            description="accepted-skill Lease renewal",
            timeout_seconds=60,
            interval_seconds=2,
        )
        return replace(attempt, lease_renewals=attempt.lease_renewals + 1)

    @staticmethod
    def _lease_renewal_facts(
        value: object,
        attempt: _AcceptedSkillAttemptObservation,
    ) -> tuple[str, str, int, datetime]:
        if not isinstance(value, dict):
            raise QualificationCommandError(
                "accepted-skill Lease renewal evidence is malformed",
            )
        metadata = value.get("metadata")
        spec = value.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            raise QualificationCommandError(
                "accepted-skill Lease renewal evidence is malformed",
            )
        if metadata.get("uid") != attempt.lease_uid:
            raise QualificationCommandError(
                "accepted-skill Lease was replaced during renewal",
            )
        annotations = metadata.get("annotations")
        identity = annotations.get("hartmesh.io/accepted-attempt-identity") if isinstance(annotations, dict) else None
        if not isinstance(identity, str) or re.fullmatch(r"[0-9a-f]{64}", identity) is None:
            raise QualificationCommandError(
                "accepted-skill Lease holder identity changed during renewal",
            )
        holder = spec.get("holderIdentity")
        if holder != f"accepted:{identity}":
            raise QualificationCommandError(
                "accepted-skill Lease holder identity changed during renewal",
            )
        duration = spec.get("leaseDurationSeconds")
        if not isinstance(duration, int) or isinstance(duration, bool) or duration != _QUALIFICATION_ACCEPTED_ATTEMPT_LEASE_SECONDS:
            raise QualificationCommandError(
                "accepted-skill Lease duration changed during renewal",
            )
        renew_time_raw = spec.get("renewTime")
        if not isinstance(renew_time_raw, str) or len(renew_time_raw) > 64 or _RFC3339.fullmatch(renew_time_raw) is None:
            raise QualificationCommandError(
                "accepted-skill Lease renewTime is invalid",
            )
        try:
            renew_time = datetime.fromisoformat(
                renew_time_raw.replace("Z", "+00:00"),
            )
        except ValueError:
            raise QualificationCommandError(
                "accepted-skill Lease renewTime is invalid",
            ) from None
        if renew_time.tzinfo is None or renew_time.utcoffset() is None:
            raise QualificationCommandError(
                "accepted-skill Lease renewTime is invalid",
            )
        return identity, holder, duration, renew_time.astimezone(UTC)

    def _verify_gateway_token_review(
        self,
        attempt: _AcceptedSkillAttemptObservation,
    ) -> _AcceptedSkillAttemptObservation:
        gateway_pod = self._component_pod_name("gateway")
        token = self._kubectl(
            "exec",
            gateway_pod,
            "-c",
            "gateway",
            "--",
            "cat",
            "/var/run/secrets/hartmesh-provisioner/token",
            redact_diagnostics=True,
        )
        if not 1 <= len(token.encode("utf-8")) <= 16 * 1024:
            raise QualificationCommandError(
                "projected Gateway token is unavailable for TokenReview",
            )
        request = {
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "spec": {
                "token": token,
                "audiences": ["hartmesh-provisioner"],
            },
        }
        raw = self._run(
            self.config.token_review(),
            timeout_seconds=20,
            input_text=json.dumps(request),
            redact_diagnostics=True,
        )
        request["spec"]["token"] = ""
        token = ""  # Drop the only retained bearer reference before parsing.
        value = json.loads(raw)
        status = value.get("status") if isinstance(value, dict) else None
        user = status.get("user") if isinstance(status, dict) else None
        expected_username = f"system:serviceaccount:{self.config.namespace}:{self.fullname}-gateway"
        if not isinstance(status, dict) or status.get("authenticated") is not True or not isinstance(user, dict) or user.get("username") != expected_username or "hartmesh-provisioner" not in (status.get("audiences") or []):
            raise QualificationCommandError(
                "Gateway projected identity did not pass exact TokenReview",
            )
        return replace(attempt, token_review_authenticated=True)

    def _delete_attempt_lease(
        self,
        attempt: _AcceptedSkillAttemptObservation,
    ) -> None:
        current = json.loads(
            self._kubectl(
                "get",
                "lease",
                attempt.lease_name,
                "-o",
                "json",
            )
        )
        if current.get("metadata", {}).get("uid") != attempt.lease_uid:
            raise QualificationCommandError(
                "accepted-skill Lease changed before sandbox owner-loss fault",
            )
        self._kubectl(
            "delete",
            "lease",
            attempt.lease_name,
            "--wait=false",
        )

    def _prove_accepted_session_race(
        self,
        attempt: _AcceptedSkillAttemptObservation,
    ) -> _AcceptedSkillAttemptObservation:
        """Release the exact post-validation race and require narrow guarantees."""

        self._delete_attempt_lease(attempt)
        if (
            self._redis(
                "SET",
                self._barrier_key(attempt.scenario, "release"),
                "1",
                "EX",
                "300",
            )
            != "OK"
        ):
            raise QualificationCommandError(
                "accepted-sandbox race barrier release failed",
            )

        observed: dict[str, int] = {}

        def completed() -> bool:
            observed.update(
                {
                    "session_validation_passes": self._counter(
                        attempt.scenario,
                        "accepted_sandbox_validations",
                    ),
                    "raced_provider_calls": self._counter(
                        attempt.scenario,
                        "accepted_sandbox_raced_provider_calls",
                    ),
                    "post_loss_rejections": self._counter(
                        attempt.scenario,
                        "accepted_sandbox_post_loss_rejections",
                    ),
                }
            )
            return all(value == 1 for value in observed.values())

        wait_until(
            completed,
            description="accepted-sandbox validation/takeover/delegation race",
            timeout_seconds=60,
            interval_seconds=1,
        )
        if any(value != 1 for value in observed.values()):
            raise QualificationCommandError(
                "accepted-sandbox race evidence is incomplete",
            )
        return replace(attempt, **observed)

    def _wait_for_attempt_cleanup(
        self,
        attempt: _AcceptedSkillAttemptObservation,
    ) -> None:
        def removed() -> bool:
            documents = (
                json.loads(self._kubectl("get", "pods", "-o", "json")),
                json.loads(self._kubectl("get", "leases", "-o", "json")),
                json.loads(self._kubectl("get", "secrets", "-o", "json")),
                json.loads(self._kubectl("get", "networkpolicies", "-o", "json")),
            )
            for document in documents:
                for item in document.get("items", []):
                    if not isinstance(item, dict):
                        continue
                    metadata = item.get("metadata", {})
                    if metadata.get("uid") in {
                        attempt.pod_uid,
                        attempt.lease_uid,
                    }:
                        return False
                    for owner in metadata.get("ownerReferences", []):
                        if isinstance(owner, dict) and owner.get("uid") == attempt.lease_uid:
                            return False
            return True

        wait_until(
            removed,
            description=f"accepted-skill attempt cleanup for {attempt.run_id}",
            timeout_seconds=180,
            interval_seconds=2,
        )

    @staticmethod
    def _fixture_material(
        attempt: _AcceptedSkillAttemptObservation,
    ) -> AcceptedSkillMaterialEvidenceV2:
        files = sorted(_QUALIFICATION_SKILL_FILES.items())
        tree = hashlib.sha256()
        for relative, data in files:
            header = json.dumps(
                ["public", _QUALIFICATION_SKILL_NAME, relative, "regular"],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            tree.update(len(header).to_bytes(4, "big"))
            tree.update(header)
            tree.update(len(data).to_bytes(8, "big"))
            tree.update(data)
        tree_digest = tree.hexdigest()
        total_bytes = sum(len(data) for _relative, data in files)
        projection = {
            "name": _QUALIFICATION_SKILL_NAME,
            "category": "public",
            "relative_path": _QUALIFICATION_SKILL_NAME,
            "manifest_digest": hashlib.sha256(_QUALIFICATION_SKILL_FILES["SKILL.md"]).hexdigest(),
            "content_digest": tree_digest,
            "file_count": len(files),
            "total_bytes": total_bytes,
        }
        snapshot_digest = hashlib.sha256(
            json.dumps(
                {"version": 1, "skills": [projection]},
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        receipt = attempt.receipt
        if receipt.get("snapshot_id") != snapshot_digest or receipt.get("file_count") != len(files) or receipt.get("total_bytes") != total_bytes:
            raise QualificationCommandError(
                "accepted-skill verifier receipt does not match fixture bytes",
            )
        policy_digest = _sha256_bytes(
            _canonical_json(
                {
                    "version": 1,
                    "skill": _QUALIFICATION_SKILL_NAME,
                    "allowed_tools": ["read_file"],
                }
            )
        )
        return AcceptedSkillMaterialEvidenceV2(
            skill_name=_QUALIFICATION_SKILL_NAME,
            snapshot_digest="sha256:" + snapshot_digest,
            skill_tree_digest="sha256:" + tree_digest,
            allowed_tool_policy_digest=policy_digest,
            file_count=len(files),
            total_bytes=total_bytes,
            materialization_digest="sha256:" + attempt.materialization_digest,
            receipt_digest="sha256:" + attempt.verifier_receipt_digest,
        )

    def _gateway_node(self) -> str:
        pod_name = self._component_pod_name("gateway")
        value = json.loads(self._kubectl("get", "pod", pod_name, "-o", "json"))
        node = value.get("spec", {}).get("nodeName")
        if not isinstance(node, str) or not node:
            raise QualificationCommandError("Gateway scheduling node is unavailable")
        return node

    def _rwx_volume_identity(self) -> str:
        value = json.loads(
            self._kubectl(
                "get",
                "persistentvolumeclaim",
                f"{self.fullname}-home",
                "-o",
                "json",
            )
        )
        metadata = value.get("metadata", {})
        spec = value.get("spec", {})
        uid = metadata.get("uid")
        if spec.get("storageClassName") != self.config.rwx_storage_class or spec.get("accessModes") != ["ReadWriteMany"] or not isinstance(uid, str) or not uid:
            raise QualificationCommandError(
                "qualified home volume is not the exact RWX claim",
            )
        return uid

    def _assert_provisioner_image(self) -> None:
        value = self._pod_json("provisioner")
        items = value.get("items")
        if not isinstance(items, list) or len(items) != 1:
            raise QualificationCommandError(
                "qualification requires exactly one provisioner pod",
            )
        statuses = items[0].get("status", {}).get("containerStatuses", [])
        provisioner = next(
            (item for item in statuses if isinstance(item, dict) and item.get("name") == "provisioner"),
            None,
        )
        image_id = provisioner.get("imageID") if isinstance(provisioner, dict) else None
        if not isinstance(image_id, str) or self.config.provisioner_image_digest not in image_id:
            raise QualificationCommandError(
                "running provisioner imageID does not match the qualified digest",
            )

    def _accepted_qualification_config_map(self) -> str:
        candidate = f"{self.fullname}-accepted-sandbox-qualification"
        if len(candidate) <= 63:
            return candidate
        suffix = _sha256_bytes(candidate.encode("utf-8"))[:8]
        return f"{candidate[:54].rstrip('-')}-{suffix}"

    def _published_accepted_skill_values(
        self,
        values: dict[str, object],
        *,
        evidence_digest: str,
    ) -> dict[str, object]:
        """Mount the exact passing bytes used by runtime qualification."""

        if _IMAGE_DIGEST.fullmatch(evidence_digest) is None:
            raise QualificationCommandError(
                "accepted-skill evidence digest is invalid",
            )
        published = json.loads(json.dumps(values))
        published["deployment"]["qualificationCandidate"] = {
            "enabled": False,
            "id": "",
        }
        profile_line = "  accepted_skill_projection_profile: rwx_verified_copy_v2"
        rendered_config = published.get("config")
        if not isinstance(rendered_config, str) or rendered_config.count(profile_line) != 1:
            raise QualificationCommandError(
                "accepted-skill runtime config cannot be pinned",
            )
        evidence_path = f"{self.accepted_qualification_mount}/evidence.json"
        published["config"] = rendered_config.replace(
            profile_line,
            "\n".join(
                (
                    profile_line,
                    "  accepted_material_qualification_evidence: " + evidence_path,
                    "  accepted_material_qualification_digest: " + evidence_digest,
                    "  accepted_material_qualification_max_age_seconds: 2592000",
                )
            ),
            1,
        )
        gateway = published.get("gateway")
        if not isinstance(gateway, dict):
            raise QualificationCommandError(
                "accepted-skill Gateway values are invalid",
            )
        volume_name = "accepted-sandbox-qualification"
        config_map_name = self._accepted_qualification_config_map()
        gateway["extraVolumes"] = [
            *gateway.get("extraVolumes", []),
            {
                "name": volume_name,
                "configMap": {
                    "name": config_map_name,
                    "items": [
                        {"key": "evidence.json", "path": "evidence.json"},
                    ],
                },
            },
        ]
        gateway["extraVolumeMounts"] = [
            *gateway.get("extraVolumeMounts", []),
            {
                "name": volume_name,
                "mountPath": self.accepted_qualification_mount,
                "readOnly": True,
            },
        ]
        return published

    def _publish_accepted_skill_qualification(
        self,
        values: dict[str, object],
        evidence: KubernetesAcceptedSkillQualificationEvidenceV2,
        client: _RuntimeHttpSession,
    ) -> Path:
        passing_path = self.config.evidence_path.with_suffix(
            self.config.evidence_path.suffix + ".passing",
        )
        evidence.write(passing_path)
        evidence_digest = evidence_sha256(passing_path)
        verification = verify_qualification_evidence(
            passing_path.read_bytes(),
            declared_digest=evidence_digest,
            expected=AcceptedSkillQualificationExpectationV2(
                qualification_id=evidence.qualification_id,
                gateway_image_digest=self.config.image_digest,
                provisioner_image_digest=(self.config.provisioner_image_digest),
                verifier_image_digest=self.config.verifier_image_digest,
                sandbox_image_digest=self.config.sandbox_image_digest,
                chart_version=self._chart_version(),
                chart_digest=self._chart_digest(),
                configuration_digest=_sha256_bytes(_canonical_json(values)),
                migration_head=evidence.migration_head,
                scope=evidence.SCOPE,
                namespace=self.config.namespace,
                required_scenarios=evidence.REQUIRED_SCENARIOS,
            ),
        )
        if verification.artifact_digest != evidence_digest:
            raise QualificationCommandError(
                "offline accepted-skill verification returned the wrong digest",
            )
        completed_at = (
            evidence.completed_at.astimezone(UTC)
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )
        published = self._published_accepted_skill_values(
            values,
            evidence_digest=evidence_digest,
        )
        published["deployment"]["qualificationEvidence"] = [
            {
                "qualificationId": evidence.qualification_id,
                "artifactDigest": evidence_digest,
                "completedAt": completed_at,
                "scope": evidence.SCOPE,
                "status": "passed",
            }
        ]
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": self._accepted_qualification_config_map(),
                },
                "data": {
                    "evidence.json": passing_path.read_text(encoding="utf-8"),
                },
            }
        )
        values_path = self._write_values(published, "qualified-skill-v2")
        self._helm(
            "upgrade",
            self.config.release_name,
            str(self.chart_path),
            "--values",
            str(values_path),
            "--wait",
            "--timeout",
            "8m",
            timeout_seconds=510,
        )
        self._ready_gateway()
        with _PortForward(self.config, self.gateway_service) as forwarded:
            client.set_base_url(f"http://127.0.0.1:{forwarded.port}")
            status, report = client.request(
                "GET",
                "/api/runtime/v1/deployment",
            )
        qualification = report.get("qualification")
        entries = qualification.get("evidence") if isinstance(qualification, dict) else None
        if (
            status != 200
            or not isinstance(qualification, dict)
            or qualification.get("trust") != "operator_asserted"
            or not isinstance(entries, list)
            or not any(
                isinstance(entry, dict) and entry.get("qualification_id") == evidence.qualification_id and entry.get("artifact_digest") == evidence_digest and entry.get("scope") == evidence.SCOPE and entry.get("status") == "passed"
                for entry in entries
            )
        ):
            raise QualificationCommandError(
                "administrative report did not expose accepted-skill evidence",
            )
        return passing_path

    def qualify(self) -> KubernetesAcceptedSkillQualificationEvidenceV2:
        """Run real faults, verify nonempty material, and publish strict v2."""

        validate_kubernetes_prerequisites(os.environ)
        self._confirm_context()
        values = self.values()
        attempts: dict[str, _AcceptedSkillAttemptObservation] = {}
        cleanup: dict[str, str] = {}
        gateway_replacements: dict[str, str] = {}
        passing_path: Path | None = None
        try:
            self._create_namespace_and_configuration()
            self._install(values)
            gateway_pod = self._ready_gateway()
            self._assert_provisioner_image()
            stores = self._shared_store_evidence()
            schedulable_nodes = self._schedulable_nodes()
            rwx_volume_uid = self._rwx_volume_identity()
            with _PortForward(self.config, self.gateway_service) as forwarded:
                client = _RuntimeHttpSession(f"http://127.0.0.1:{forwarded.port}")
                self._initialize_admin(client)
                owner_observer = _RuntimeHttpSession(client.base_url)
                self._login_admin(owner_observer)
                nonowner = _RuntimeHttpSession(client.base_url)
                self._register_nonowner(nonowner)

            observed_faults = {
                "active_execution",
                "terminal_before_lifecycle_commit",
                "graceful_rollout_termination",
                "forced_kill_after_graceful_deadline",
            }

            def probe(scenario: str, run_id: str) -> None:
                if scenario not in observed_faults:
                    return
                gateway_node = self._gateway_node()
                attempt = self._accepted_attempt(
                    scenario,
                    run_id,
                    gateway_node=gateway_node,
                )
                if gateway_node not in schedulable_nodes or attempt.pod_node not in schedulable_nodes or gateway_node == attempt.pod_node:
                    raise QualificationCommandError(
                        "accepted-skill qualification did not execute cross-node",
                    )
                if scenario == "active_execution":
                    attempt = self._verify_gateway_token_review(attempt)
                    attempt = self._wait_for_lease_renewal(attempt)
                if scenario == "terminal_before_lifecycle_commit":
                    attempt = self._prove_accepted_session_race(attempt)
                attempts[scenario] = attempt

            for scenario in KubernetesQualificationEvidence.REQUIRED_SCENARIOS:
                with _PortForward(self.config, self.gateway_service) as forwarded:
                    base_url = f"http://127.0.0.1:{forwarded.port}"
                    client.set_base_url(base_url)
                    owner_observer.set_base_url(base_url)
                    nonowner.set_base_url(base_url)
                    scenario_evidence, gateway_pod = self._run_scenario(
                        scenario,
                        client,
                        owner_observer,
                        nonowner,
                        gateway_pod,
                        forwarded,
                        barrier_probe=probe,
                    )
                if scenario == "terminal_before_lifecycle_commit":
                    attempt = attempts.get(scenario)
                    if attempt is None or scenario_evidence.terminal_status != "error":
                        raise QualificationCommandError(
                            "stale accepted-sandbox success was not refused",
                        )
                    attempts[scenario] = replace(
                        attempt,
                        stale_terminal_rejected=True,
                    )
                if scenario in {
                    "graceful_rollout_termination",
                    "forced_kill_after_graceful_deadline",
                }:
                    gateway_replacements[scenario] = gateway_pod[1]
                attempt = attempts.get(scenario)
                if attempt is not None:
                    self._wait_for_attempt_cleanup(attempt)
                    cleanup[scenario] = "deleted"
                if self._shared_store_evidence() != stores:
                    raise QualificationCommandError(
                        "shared stores changed during accepted-skill faults",
                    )
            if set(attempts) != observed_faults:
                raise QualificationCommandError(
                    "accepted-skill fault coverage is incomplete",
                )
            active = attempts["active_execution"]
            accepted_session_race = attempts["terminal_before_lifecycle_commit"]
            if accepted_session_race.session_validation_passes != 1 or accepted_session_race.raced_provider_calls != 1 or accepted_session_race.post_loss_rejections != 1 or not accepted_session_race.stale_terminal_rejected:
                raise QualificationCommandError(
                    "accepted-sandbox session race evidence is incomplete",
                )
            facts = self._environment_facts(gateway_pod[0])
            evidence_scenarios = (
                ("nonempty_material_execution", active),
                ("token_review_and_lease_renewal", active),
                (
                    "gateway_replacement_cleanup",
                    attempts["graceful_rollout_termination"],
                ),
                ("sandbox_owner_loss_cleanup", attempts["terminal_before_lifecycle_commit"]),
                (
                    "process_loss_cleanup",
                    attempts["forced_kill_after_graceful_deadline"],
                ),
            )
            scenario_items: list[AcceptedSkillScenarioEvidenceV2] = []
            for name, attempt in evidence_scenarios:
                gateway_replacement_uid = (
                    gateway_replacements["graceful_rollout_termination"] if name == "gateway_replacement_cleanup" else gateway_replacements["forced_kill_after_graceful_deadline"] if name == "process_loss_cleanup" else None
                )
                scenario_items.append(
                    AcceptedSkillScenarioEvidenceV2(
                        name=name,
                        run_id=attempt.run_id,
                        result_digest=attempt.result_digest(
                            evidence_scenario=name,
                            cleanup_outcome=cleanup[attempt.scenario],
                            gateway_replacement_uid=gateway_replacement_uid,
                        ),
                        replacement_observed=name
                        in {
                            "gateway_replacement_cleanup",
                            "process_loss_cleanup",
                        },
                        cleanup_outcome="deleted",
                    )
                )
            scenarios = tuple(scenario_items)
            if tuple(item.name for item in scenarios) != (ACCEPTED_SKILL_QUALIFICATION_SCENARIOS_V2):
                raise QualificationCommandError(
                    "accepted-skill evidence scenario order changed",
                )
            evidence = KubernetesAcceptedSkillQualificationEvidenceV2(
                qualification_id=self.config.qualification_id,
                gateway_image_reference=(f"{self.config.image_repository}@{self.config.image_digest}"),
                gateway_image_digest=self.config.image_digest,
                provisioner_image_reference=(self.config.provisioner_image_reference),
                provisioner_image_digest=(self.config.provisioner_image_digest),
                verifier_image_reference=self.config.verifier_image_reference,
                verifier_image_digest=self.config.verifier_image_digest,
                sandbox_image_reference=self.config.sandbox_image_reference,
                sandbox_image_digest=self.config.sandbox_image_digest,
                chart_version=self._chart_version(),
                chart_digest=self._chart_digest(),
                configuration_digest=_sha256_bytes(_canonical_json(values)),
                migration_head=facts["migration_head"],
                environment=AcceptedSkillQualificationEnvironmentV2(
                    kubernetes_server_version=facts["kubernetes_server_version"],
                    cluster_context=self.config.context,
                    cluster_driver=optional_cluster_driver(os.environ),
                    namespace=self.config.namespace,
                    schedulable_nodes=schedulable_nodes,
                    gateway_node=active.gateway_node,
                    sandbox_node=active.pod_node,
                    rwx_storage_class=self.config.rwx_storage_class,
                    rwx_volume_uid=rwx_volume_uid,
                    token_review_authenticated=(active.token_review_authenticated),
                    gateway_service_account=f"{self.fullname}-gateway",
                    lease_uid=active.lease_uid,
                    lease_renewals=active.lease_renewals,
                ),
                material=self._fixture_material(active),
                scenarios=scenarios,
                completed_at=datetime.now(UTC),
            )
            passing_path = self._publish_accepted_skill_qualification(
                values,
                evidence,
                client,
            )
            self._kubectl(
                "delete",
                "namespace",
                self.config.namespace,
                "--wait=true",
                "--ignore-not-found=true",
                namespaced=False,
                timeout_seconds=180,
            )
            os.replace(passing_path, self.config.evidence_path)
            return evidence
        except Exception as exc:
            if passing_path is not None:
                passing_path.unlink(missing_ok=True)
            self._collect_failure_artifacts()
            failure = {
                "api_version": ("deerflow.kubernetes-accepted-skill-qualification/v2"),
                "kind": "kubernetes.qualification.evidence",
                "status": "failed",
                "scope": ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
                "qualification_id": self.config.qualification_id,
                "namespace": self.config.namespace,
                "failure_code": type(exc).__name__,
                "completed_scenarios": sorted(cleanup),
                "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
            payload = _canonical_json(failure) + b"\n"
            self.config.evidence_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.config.evidence_path.with_suffix(self.config.evidence_path.suffix + ".failed.tmp")
            temporary.write_bytes(payload)
            os.replace(temporary, self.config.evidence_path)
            raise

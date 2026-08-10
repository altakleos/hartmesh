"""Distribution-neutral command construction for Kubernetes qualification."""

from __future__ import annotations

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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from deerflow.qualification_evidence import (
    KubernetesQualificationEvidence,
    KubernetesQualificationFailureEvidence,
    QualificationEvidenceExpectation,
    ScenarioEvidence,
    StoreContinuityEvidence,
    qualification_evidence_digest,
    verify_qualification_evidence,
)

_SAFE_CONTEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,252}\Z")
_SAFE_NAMESPACE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_IMAGE_REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,511}\Z")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
KUBERNETES_OPT_IN_MESSAGE = "Kubernetes qualification is opt-in; set DEERFLOW_TEST_KUBERNETES=1 with an explicit KUBECONFIG and qualification context"


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
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._process: subprocess.Popen[str] | None = None
        self.port = 0

    def __enter__(self) -> _PortForward:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        command = self._config.kubectl(
            "port-forward",
            f"service/{self._service_name}",
            f"{self.port}:8001",
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
                "config_version: 39",
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
        return f"deerflow:kubernetes-qualification:{self.config.qualification_id}:{scenario}:{suffix}"

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
    ) -> tuple[ScenarioEvidence, tuple[str, str]]:
        payload = self._ensure_payload(scenario)
        request_result: dict[str, object] = {}

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

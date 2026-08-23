"""DeerFlow Sandbox Provisioner Service.

Dynamically creates and manages per-sandbox Pods in Kubernetes.
Each ``sandbox_id`` gets its own Pod + Service.  The backend accesses sandboxes
through NodePort or Kubernetes service DNS, depending on configuration.

The provisioner connects to the host machine's Kubernetes cluster via a
mounted kubeconfig (``~/.kube/config``) or in-cluster config.  Sandbox Pods
run in K8s and are accessed by the backend via the configured Service mode.

Endpoints:
    POST   /api/sandboxes              — Create a sandbox Pod + Service
    DELETE /api/sandboxes/{sandbox_id} — Destroy a sandbox Pod + Service
    GET    /api/sandboxes/{sandbox_id} — Get sandbox status & URL
    GET    /api/sandboxes              — List all sandboxes
    GET    /health                     — Provisioner health check

Architecture (docker-compose-dev):
    ┌────────────┐  HTTP  ┌─────────────┐  K8s API  ┌──────────────┐
    │ remote     │ ─────▸ │ provisioner │ ────────▸ │  host K8s    │
    │ _backend   │        │ :8002       │           │  API server  │
    └────────────┘        └─────────────┘           └──────┬───────┘
                                                           │ creates
                          ┌─────────────┐           ┌──────▼───────┐
                          │   backend   │ ────────▸ │   sandbox    │
                          │             │ direct/DNS│   Pod(s)     │
                          └─────────────┘           └──────────────┘
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import posixpath
import re
import secrets
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal, NamedTuple

import urllib3
from fastapi import FastAPI, HTTPException, Request, Response
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException
from pydantic import BaseModel, ConfigDict, Field, model_validator

# Suppress only the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ── Configuration (all tuneable via environment variables) ───────────────

K8S_NAMESPACE = os.environ.get("K8S_NAMESPACE", "deer-flow")


def _provisioner_create_namespace_from_env() -> bool:
    return os.environ.get(
        "PROVISIONER_CREATE_NAMESPACE",
        "false",
    ).strip().lower() == "true"


PROVISIONER_CREATE_NAMESPACE = _provisioner_create_namespace_from_env()
SANDBOX_IMAGE = os.environ.get(
    "SANDBOX_IMAGE",
    "enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest",
)


def _sandbox_runtime_class_from_env() -> str:
    """Resolve the optional RuntimeClass without ever returning an empty value."""

    return os.environ.get("SANDBOX_RUNTIME_CLASS", "").strip()


SANDBOX_RUNTIME_CLASS = _sandbox_runtime_class_from_env()


def _sandbox_runtime_label() -> str:
    return SANDBOX_RUNTIME_CLASS or "default runtime"


# Optional "lark-cli init" image (Pattern A). When set, sandbox Pods get an init
# container + shared emptyDir that provisions the lark-cli runtime binary, instead
# of a hostPath/PVC runtime mount fed by a Gateway-side GitHub download. Empty ⇒
# feature off (legacy behavior).
LARK_CLI_INIT_IMAGE = os.environ.get("LARK_CLI_INIT_IMAGE", "")
LARK_CLI_RUNTIME_CONTAINER_PATH = "/mnt/integrations/lark-cli/runtime"
LARK_CLI_RUNTIME_VOLUME_NAME = "lark-cli-runtime"
# Optional "lark-cli broker" image (Pattern B, issue #4338). When set, sandbox
# Pods requesting the broker get an init container that stages a shim + a
# long-running broker sidecar that holds the credentials, instead of mounting the
# plaintext config/locks/data credential dirs into the sandbox container. Empty ⇒
# broker off (Pattern A / legacy behavior). Broker supersedes Pattern A when both
# are set.
LARK_CLI_BROKER_IMAGE = os.environ.get("LARK_CLI_BROKER_IMAGE", "")
# Optional comma-separated lark-cli subcommand denylist forwarded to the broker
# sidecar (issue #4338 hardening). Empty ⇒ no subcommand is blocked. See the
# broker README's "subcommand denylist" section.
LARK_CLI_BROKER_DENY_SUBCOMMANDS = os.environ.get("DEERFLOW_LARK_BROKER_DENY_SUBCOMMANDS", "")
LARK_CLI_CONFIG_CONTAINER_PATH = "/mnt/integrations/lark-cli/config"
LARK_CLI_LOCKS_CONTAINER_PATH = f"{LARK_CLI_CONFIG_CONTAINER_PATH}/locks"
LARK_CLI_DATA_CONTAINER_PATH = "/mnt/integrations/lark-cli/data"
# Where the broker sidecar reads the per-user credentials (sidecar-only paths).
LARK_BROKER_SIDECAR_CONFIG_PATH = "/var/lark/config"
LARK_BROKER_SIDECAR_LOCKS_PATH = f"{LARK_BROKER_SIDECAR_CONFIG_PATH}/locks"
LARK_BROKER_SIDECAR_DATA_PATH = "/var/lark/data"
LARK_BROKER_CONFIG_VOLUME_NAME = "lark-cli-config"
LARK_BROKER_LOCKS_VOLUME_NAME = "lark-cli-locks"
LARK_BROKER_DATA_VOLUME_NAME = "lark-cli-data"
LARK_BROKER_URL = "http://127.0.0.1:8788"
THREADS_HOST_PATH = os.environ.get("THREADS_HOST_PATH", "/.deer-flow/threads")
DEER_FLOW_HOST_BASE_DIR = os.environ.get("DEER_FLOW_HOST_BASE_DIR", "/.deer-flow")
SKILLS_PVC_NAME = os.environ.get("SKILLS_PVC_NAME", "")
USERDATA_PVC_NAME = os.environ.get("USERDATA_PVC_NAME", "")


class SandboxVolumeConfig(NamedTuple):
    """One startup-resolved sandbox volume mode and its selection reason."""

    mode: Literal["pvc", "hostpath"]
    reason: Literal["explicit", "inferred"]


def resolve_sandbox_volume_mode(
    explicit_mode: str | None,
    *,
    userdata_pvc_name: str,
    skills_pvc_name: str,
) -> SandboxVolumeConfig:
    """Resolve the sandbox volume mode, rejecting incomplete PVC settings."""

    requested_mode = (explicit_mode or "").strip()
    if requested_mode and requested_mode not in {"pvc", "hostpath"}:
        raise RuntimeError(
            f"Invalid SANDBOX_VOLUME_MODE={requested_mode!r}; expected 'pvc' or 'hostpath'",
        )
    if requested_mode == "hostpath":
        return SandboxVolumeConfig(mode="hostpath", reason="explicit")

    missing_names = [
        name
        for name, value in (
            ("USERDATA_PVC_NAME", userdata_pvc_name),
            ("SKILLS_PVC_NAME", skills_pvc_name),
        )
        if not value
    ]
    if requested_mode == "pvc":
        if missing_names:
            raise RuntimeError(
                "Invalid SANDBOX_VOLUME_MODE=pvc: missing required " + ", ".join(missing_names),
            )
        return SandboxVolumeConfig(mode="pvc", reason="explicit")

    if not missing_names:
        return SandboxVolumeConfig(mode="pvc", reason="inferred")
    if len(missing_names) == 2:
        return SandboxVolumeConfig(mode="hostpath", reason="inferred")
    raise RuntimeError(
        "Invalid inferred SANDBOX_VOLUME_MODE=pvc: missing required " + missing_names[0],
    )


def _sandbox_volume_config_from_env() -> SandboxVolumeConfig:
    return resolve_sandbox_volume_mode(
        os.environ.get("SANDBOX_VOLUME_MODE"),
        userdata_pvc_name=os.environ.get("USERDATA_PVC_NAME", ""),
        skills_pvc_name=os.environ.get("SKILLS_PVC_NAME", ""),
    )


SANDBOX_VOLUME_CONFIG = _sandbox_volume_config_from_env()
SKILLS_PVC_SUBPATH_TEMPLATE = os.environ.get("SKILLS_PVC_SUBPATH_TEMPLATE", "")
ACCEPTED_SKILL_PROJECTION_PROFILE = os.environ.get(
    "ACCEPTED_SKILL_PROJECTION_PROFILE",
    "",
)
ACCEPTED_SKILL_RUNTIME_IMAGE = os.environ.get(
    "ACCEPTED_SKILL_RUNTIME_IMAGE",
    "",
)
ACCEPTED_SKILL_GATE_PORT = 8081
ACCEPTED_SKILL_SOURCE_MOUNT = "/accepted-source"
ACCEPTED_SKILL_DESTINATION_MOUNT = "/accepted-destination"
ACCEPTED_SKILL_SANDBOX_MOUNT = "/mnt/skills/.accepted"
ACCEPTED_SKILL_EVIDENCE_MOUNT = "/var/run/hartmesh/accepted-evidence"
ACCEPTED_SKILL_CAPABILITY_MOUNT = "/var/run/hartmesh/accepted-capability"
ACCEPTED_SKILL_RECEIPT_MOUNT = "/var/run/hartmesh/accepted-receipt"
ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V1 = "rwx_verified_copy_v1"
ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2 = "rwx_verified_copy_v2"


def _bounded_int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid {name}; expected an integer") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(
            f"Invalid {name}; expected a value in [{minimum}, {maximum}]",
        )
    return value


SANDBOX_STARTUP_PROBE_INITIAL_DELAY_SECONDS = _bounded_int_env(
    "SANDBOX_STARTUP_PROBE_INITIAL_DELAY_SECONDS",
    0,
    minimum=0,
    maximum=300,
)
SANDBOX_STARTUP_PROBE_PERIOD_SECONDS = _bounded_int_env(
    "SANDBOX_STARTUP_PROBE_PERIOD_SECONDS",
    10,
    minimum=1,
    maximum=300,
)
SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS = _bounded_int_env(
    "SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS",
    3,
    minimum=1,
    maximum=300,
)
SANDBOX_STARTUP_PROBE_FAILURE_THRESHOLD = _bounded_int_env(
    "SANDBOX_STARTUP_PROBE_FAILURE_THRESHOLD",
    20,
    minimum=1,
    maximum=60,
)
if SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS > SANDBOX_STARTUP_PROBE_PERIOD_SECONDS:
    raise RuntimeError(
        "Invalid sandbox startup probe: SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS must not exceed SANDBOX_STARTUP_PROBE_PERIOD_SECONDS",
    )

SANDBOX_LIVENESS_PROBE_INITIAL_DELAY_SECONDS = _bounded_int_env(
    "SANDBOX_LIVENESS_PROBE_INITIAL_DELAY_SECONDS",
    10,
    minimum=0,
    maximum=300,
)
SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS = _bounded_int_env(
    "SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS",
    10,
    minimum=1,
    maximum=300,
)
SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS = _bounded_int_env(
    "SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS",
    10,
    minimum=1,
    maximum=300,
)
SANDBOX_LIVENESS_PROBE_FAILURE_THRESHOLD = _bounded_int_env(
    "SANDBOX_LIVENESS_PROBE_FAILURE_THRESHOLD",
    3,
    minimum=1,
    maximum=60,
)
if SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS > SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS:
    raise RuntimeError(
        "Invalid sandbox liveness probe: SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS must not exceed SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS",
    )


ACCEPTED_ATTEMPT_LEASE_SECONDS = _bounded_int_env(
    "ACCEPTED_ATTEMPT_LEASE_SECONDS",
    120,
    minimum=30,
    maximum=900,
)
ACCEPTED_ATTEMPT_RECONCILE_INTERVAL_SECONDS = _bounded_int_env(
    "ACCEPTED_ATTEMPT_RECONCILE_INTERVAL_SECONDS",
    30,
    minimum=5,
    maximum=300,
)
ACCEPTED_ATTEMPT_RECONCILE_LIMIT = _bounded_int_env(
    "ACCEPTED_ATTEMPT_RECONCILE_LIMIT",
    100,
    minimum=1,
    maximum=500,
)
_accepted_reconcile_continue: str | None = None
SANDBOX_CONTAINER_PORT_RAW = os.environ.get("SANDBOX_CONTAINER_PORT", "8080")
SANDBOX_SERVICE_TYPE = os.environ.get("SANDBOX_SERVICE_TYPE", "NodePort")
try:
    SANDBOX_CONTAINER_PORT = int(SANDBOX_CONTAINER_PORT_RAW)
except ValueError as exc:
    raise RuntimeError(f"Invalid SANDBOX_CONTAINER_PORT={SANDBOX_CONTAINER_PORT_RAW!r}; expected an integer TCP port") from exc
if not (1 <= SANDBOX_CONTAINER_PORT <= 65535):
    raise RuntimeError(f"Invalid SANDBOX_CONTAINER_PORT={SANDBOX_CONTAINER_PORT}; expected a value in [1, 65535]")
if SANDBOX_SERVICE_TYPE not in {"NodePort", "ClusterIP"}:
    raise RuntimeError(f"Invalid SANDBOX_SERVICE_TYPE={SANDBOX_SERVICE_TYPE!r}; expected 'NodePort' or 'ClusterIP'")
SAFE_THREAD_ID_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"
SAFE_USER_ID_PATTERN = r"^[A-Za-z0-9_\-]+$"
DEFAULT_USER_ID = "default"
MAX_EXTRA_MOUNTS = 10
ALLOWED_EXTRA_MOUNT_PATHS = {
    "/mnt/acp-workspace",
    "/mnt/skills/custom",
    "/mnt/skills/integrations",
    "/mnt/integrations/lark-cli/config",
    "/mnt/integrations/lark-cli/config/locks",
    "/mnt/integrations/lark-cli/data",
    "/mnt/integrations/lark-cli/runtime",
}

# Path to the kubeconfig *inside* the provisioner container.
# Typically the host's ~/.kube/config is mounted here.
KUBECONFIG_PATH = os.environ.get("KUBECONFIG_PATH", "/root/.kube/config")
PROVISIONER_API_KEY = os.environ.get("PROVISIONER_API_KEY", "")
PROVISIONER_AUTH_AUDIENCE = os.environ.get("PROVISIONER_AUTH_AUDIENCE", "")
PROVISIONER_GATEWAY_NAMESPACE = os.environ.get(
    "PROVISIONER_GATEWAY_NAMESPACE",
    "",
)
PROVISIONER_GATEWAY_SERVICE_ACCOUNT = os.environ.get(
    "PROVISIONER_GATEWAY_SERVICE_ACCOUNT",
    "",
)

# The hostname / IP that the backend uses to reach NodePort services. On Docker
# Desktop for macOS this is ``host.docker.internal``; on Linux it may be the
# host's LAN IP. Ignored when SANDBOX_SERVICE_TYPE=ClusterIP.
NODE_HOST = os.environ.get("NODE_HOST", "host.docker.internal")


def join_host_path(base: str, *parts: str) -> str:
    """Join host filesystem path segments while preserving native style."""
    if not parts:
        return base

    if re.match(r"^[A-Za-z]:[\\/]", base) or base.startswith("\\\\") or "\\" in base:
        from pathlib import PureWindowsPath

        result = PureWindowsPath(base)
        for part in parts:
            result /= part
        return str(result)

    from pathlib import Path

    result = Path(base)
    for part in parts:
        result /= part
    return str(result)


def _host_base_dir_for_extra_mounts() -> str:
    """Return the host-visible DeerFlow state root used for controlled mounts."""
    if DEER_FLOW_HOST_BASE_DIR:
        return os.path.normpath(DEER_FLOW_HOST_BASE_DIR)

    normalized_threads = os.path.normpath(THREADS_HOST_PATH)
    if os.path.basename(normalized_threads) == "threads":
        return os.path.dirname(normalized_threads)
    return ""


def _is_path_under_base(path: str, base: str) -> bool:
    """Return whether *path* is inside *base* after normalization."""
    if not base:
        return False
    try:
        return os.path.commonpath([os.path.normpath(path), os.path.normpath(base)]) == os.path.normpath(base)
    except ValueError:
        return False


def _normalize_extra_mount_container_path(container_path: str) -> str:
    normalized = posixpath.normpath(container_path)
    if not normalized.startswith("/"):
        raise HTTPException(status_code=400, detail=f"Extra mount path must be absolute: {container_path}")
    if normalized not in ALLOWED_EXTRA_MOUNT_PATHS:
        raise HTTPException(status_code=400, detail=f"Unsupported extra mount path: {container_path}")
    return normalized


def _validated_extra_mounts(extra_mounts: list[ExtraMount] | None) -> list[ExtraMount]:
    """Validate extra mounts before converting them into K8s hostPath/PVC mounts."""
    if not extra_mounts:
        return []
    if len(extra_mounts) > MAX_EXTRA_MOUNTS:
        raise HTTPException(status_code=400, detail=f"Too many extra mounts; max is {MAX_EXTRA_MOUNTS}")

    host_base_dir = _host_base_dir_for_extra_mounts()
    seen_container_paths: set[str] = set()
    validated: list[ExtraMount] = []
    for mount in extra_mounts:
        host_path = os.path.normpath(mount.host_path)
        if not os.path.isabs(host_path):
            raise HTTPException(status_code=400, detail=f"Extra mount host path must be absolute: {mount.host_path}")
        if not _is_path_under_base(host_path, host_base_dir):
            raise HTTPException(status_code=400, detail=f"Extra mount host path is outside DeerFlow state: {mount.host_path}")

        container_path = _normalize_extra_mount_container_path(mount.container_path)
        if container_path in seen_container_paths:
            raise HTTPException(status_code=400, detail=f"Duplicate extra mount path: {container_path}")
        seen_container_paths.add(container_path)

        validated.append(
            ExtraMount(
                host_path=host_path,
                container_path=container_path,
                read_only=mount.read_only,
            )
        )
    return validated


def _extra_mount_volume_name(index: int) -> str:
    return f"extra-{index}"


def _lark_cli_runtime_enabled(provision_lark_cli_runtime: bool) -> bool:
    """Whether to provision the lark-cli runtime via init container + emptyDir."""
    return bool(LARK_CLI_INIT_IMAGE) and provision_lark_cli_runtime


def _lark_cli_broker_enabled(provision_lark_cli_broker: bool) -> bool:
    """Whether to provision the lark-cli broker sidecar (Pattern B)."""
    return bool(LARK_CLI_BROKER_IMAGE) and provision_lark_cli_broker


def _runtime_provided_extra_mounts(
    extra_mounts: list[ExtraMount] | None,
    *,
    provision_lark_cli_runtime: bool,
    provision_lark_cli_broker: bool = False,
) -> list[ExtraMount]:
    """Drop lark-cli extra mounts the init container / broker sidecar supersede.

    Pattern A (init container + emptyDir) provides
    ``/mnt/integrations/lark-cli/runtime``, so a hostPath/PVC mount at the same
    path would collide — it is dropped, leaving the per-user ``config`` /
    ``config/locks`` / ``data`` mounts intact. The nested locks mount is writable
    so lark-cli can coordinate API calls while the config root remains read-only.

    Pattern B (broker sidecar) additionally moves all three mounts off the
    *sandbox* container and into the sidecar, so those are dropped here too —
    the sandbox never sees plaintext credentials.
    """
    dropped: set[str] = set()
    if _lark_cli_broker_enabled(provision_lark_cli_broker):
        dropped = {
            LARK_CLI_RUNTIME_CONTAINER_PATH,
            LARK_CLI_CONFIG_CONTAINER_PATH,
            LARK_CLI_LOCKS_CONTAINER_PATH,
            LARK_CLI_DATA_CONTAINER_PATH,
        }
    elif _lark_cli_runtime_enabled(provision_lark_cli_runtime):
        dropped = {LARK_CLI_RUNTIME_CONTAINER_PATH}
    if not extra_mounts or not dropped:
        return list(extra_mounts or [])
    return [mount for mount in extra_mounts if posixpath.normpath(mount.container_path) not in dropped]


def _lark_broker_credential_mounts(extra_mounts: list[ExtraMount] | None) -> dict[str, ExtraMount]:
    """Extract the config/locks/data mounts the broker sidecar needs.

    Keyed by container path so the caller can wire each into the sidecar's fixed
    ``/var/lark/{config,config/locks,data}`` paths.
    """
    result: dict[str, ExtraMount] = {}
    for mount in _validated_extra_mounts(extra_mounts):
        normalized = posixpath.normpath(mount.container_path)
        if normalized in (
            LARK_CLI_CONFIG_CONTAINER_PATH,
            LARK_CLI_LOCKS_CONTAINER_PATH,
            LARK_CLI_DATA_CONTAINER_PATH,
        ):
            result[normalized] = mount
    return result


def _extra_mount_pvc_sub_path(host_path: str) -> str:
    host_base_dir = _host_base_dir_for_extra_mounts()
    if not _is_path_under_base(host_path, host_base_dir):
        raise HTTPException(status_code=400, detail=f"Extra mount host path is outside DeerFlow state: {host_path}")

    rel_path = os.path.relpath(os.path.normpath(host_path), host_base_dir)
    rel_parts = [part for part in rel_path.replace(os.sep, "/").split("/") if part and part != "."]
    if not rel_parts or any(part == ".." for part in rel_parts):
        raise HTTPException(status_code=400, detail=f"Invalid extra mount host path: {host_path}")
    return posixpath.join("deer-flow", *rel_parts)


def _reject_accepted_skill_source_aliases(
    extra_mounts: list[ExtraMount] | None,
) -> None:
    """Keep every sandbox-visible mount disjoint from accepted snapshot storage."""

    source = "deer-flow/runtime/skill-snapshots"
    for mount in _validated_extra_mounts(extra_mounts):
        candidate = posixpath.normpath(
            _extra_mount_pvc_sub_path(mount.host_path),
        )
        if candidate == source or candidate.startswith(f"{source}/") or source.startswith(f"{candidate}/"):
            raise HTTPException(
                status_code=400,
                detail="accepted skill source alias is forbidden",
            )


# ── K8s client setup ────────────────────────────────────────────────────

core_v1: k8s_client.CoreV1Api | None = None
networking_v1: k8s_client.NetworkingV1Api | None = None
coordination_v1: k8s_client.CoordinationV1Api | None = None
authentication_v1: k8s_client.AuthenticationV1Api | None = None


def _init_k8s_client() -> k8s_client.CoreV1Api:
    """Load kubeconfig from the mounted host config and return a CoreV1Api.

    Tries the mounted kubeconfig first, then falls back to in-cluster
    config (useful if the provisioner itself runs inside K8s).
    """
    if os.path.exists(KUBECONFIG_PATH):
        if os.path.isdir(KUBECONFIG_PATH):
            raise RuntimeError(f"KUBECONFIG_PATH points to a directory, expected a file: {KUBECONFIG_PATH}")
        try:
            k8s_config.load_kube_config(config_file=KUBECONFIG_PATH)
            logger.info(f"Loaded kubeconfig from {KUBECONFIG_PATH}")
        except Exception as exc:
            raise RuntimeError(f"Failed to load kubeconfig from {KUBECONFIG_PATH}: {exc}") from exc
    else:
        logger.warning(f"Kubeconfig not found at {KUBECONFIG_PATH}; trying in-cluster config")
        try:
            k8s_config.load_incluster_config()
        except Exception as exc:
            raise RuntimeError(f"Failed to initialize Kubernetes client. No kubeconfig at {KUBECONFIG_PATH}, and in-cluster config is unavailable: {exc}") from exc

    # When connecting from inside Docker to the host's K8s API, the
    # kubeconfig may reference ``localhost`` or ``127.0.0.1``.  We
    # optionally rewrite the server address so it reaches the host.
    k8s_api_server = os.environ.get("K8S_API_SERVER")
    if k8s_api_server:
        configuration = k8s_client.Configuration.get_default_copy()
        configuration.host = k8s_api_server
        # Self-signed certs are common for local clusters
        configuration.verify_ssl = False
        api_client = k8s_client.ApiClient(configuration)
        return k8s_client.CoreV1Api(api_client)

    return k8s_client.CoreV1Api()


def _wait_for_kubeconfig(timeout: int = 30) -> None:
    """Wait for kubeconfig file if configured, then continue with fallback support."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(KUBECONFIG_PATH):
            if os.path.isfile(KUBECONFIG_PATH):
                logger.info(f"Found kubeconfig file at {KUBECONFIG_PATH}")
                return
            if os.path.isdir(KUBECONFIG_PATH):
                raise RuntimeError(f"Kubeconfig path is a directory. Please mount a kubeconfig file at {KUBECONFIG_PATH}.")
            raise RuntimeError(f"Kubeconfig path exists but is not a regular file: {KUBECONFIG_PATH}")
        logger.info(f"Waiting for kubeconfig at {KUBECONFIG_PATH} …")
        time.sleep(2)
    logger.warning(f"Kubeconfig not found at {KUBECONFIG_PATH} after {timeout}s; will attempt in-cluster Kubernetes config")


def _ensure_namespace() -> None:
    """Require the sandbox namespace, creating it only by explicit opt-in."""
    try:
        core_v1.read_namespace(K8S_NAMESPACE)
        logger.info(f"Namespace '{K8S_NAMESPACE}' already exists")
    except ApiException as exc:
        if exc.status == 404:
            if not PROVISIONER_CREATE_NAMESPACE:
                raise RuntimeError(
                    f"sandbox namespace {K8S_NAMESPACE!r} does not exist. Pre-create it (Helm: the namespace named by "
                    "sandboxNamespace, or the release namespace), or set PROVISIONER_CREATE_NAMESPACE=true for "
                    "single-namespace local/Compose installs.",
                ) from None
            ns = k8s_client.V1Namespace(
                metadata=k8s_client.V1ObjectMeta(
                    name=K8S_NAMESPACE,
                    labels={
                        "app.kubernetes.io/name": "deer-flow",
                        "app.kubernetes.io/component": "sandbox",
                    },
                )
            )
            core_v1.create_namespace(ns)
            logger.info(f"Created namespace '{K8S_NAMESPACE}'")
        else:
            raise


# ── FastAPI lifespan ─────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global authentication_v1, coordination_v1, core_v1, networking_v1
    logger.info(
        "Sandbox volume mode: %s (%s)",
        SANDBOX_VOLUME_CONFIG.mode,
        SANDBOX_VOLUME_CONFIG.reason,
    )
    logger.info(
        "Sandbox runtime class: %s",
        _sandbox_runtime_label(),
    )
    logger.info(
        "Sandbox namespace mode: %s (K8S_NAMESPACE=%s)",
        (
            "create-if-missing"
            if PROVISIONER_CREATE_NAMESPACE
            else "pre-created-required"
        ),
        K8S_NAMESPACE,
    )
    _wait_for_kubeconfig()
    core_v1 = _init_k8s_client()
    networking_v1 = k8s_client.NetworkingV1Api(core_v1.api_client)
    coordination_v1 = k8s_client.CoordinationV1Api(core_v1.api_client)
    authentication_v1 = k8s_client.AuthenticationV1Api(core_v1.api_client)
    _ensure_namespace()
    await asyncio.to_thread(_reconcile_expired_accepted_attempts)
    reconciliation = asyncio.create_task(
        _accepted_attempt_reconcile_loop(),
        name="accepted-sandbox-attempt-reconciler",
    )
    logger.info("Provisioner is ready (using host Kubernetes)")
    try:
        yield
    finally:
        reconciliation.cancel()
        try:
            await reconciliation
        except asyncio.CancelledError:
            pass


app = FastAPI(title="DeerFlow Sandbox Provisioner", lifespan=lifespan)


@app.middleware("http")
async def verify_api_key(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        key = request.headers.get("X-API-Key", "")
        static_authenticated = bool(PROVISIONER_API_KEY) and secrets.compare_digest(
            key,
            PROVISIONER_API_KEY,
        )
        bearer = request.headers.get("Authorization", "")
        token_authenticated = False
        if not static_authenticated and bearer.startswith("Bearer "):
            token = bearer.removeprefix("Bearer ")
            tokenreview_configured = all(
                (
                    PROVISIONER_AUTH_AUDIENCE,
                    PROVISIONER_GATEWAY_NAMESPACE,
                    PROVISIONER_GATEWAY_SERVICE_ACCOUNT,
                )
            )
            if tokenreview_configured and authentication_v1 is not None and 1 <= len(token.encode("utf-8")) <= 16 * 1024:
                try:
                    review = await asyncio.to_thread(
                        authentication_v1.create_token_review,
                        k8s_client.V1TokenReview(
                            spec=k8s_client.V1TokenReviewSpec(
                                token=token,
                                audiences=[PROVISIONER_AUTH_AUDIENCE],
                            ),
                        ),
                        _request_timeout=2,
                    )
                    status = getattr(review, "status", None)
                    username = getattr(
                        getattr(status, "user", None),
                        "username",
                        None,
                    )
                    expected_username = f"system:serviceaccount:{PROVISIONER_GATEWAY_NAMESPACE}:{PROVISIONER_GATEWAY_SERVICE_ACCOUNT}"
                    token_authenticated = getattr(status, "authenticated", None) is True and username == expected_username and PROVISIONER_AUTH_AUDIENCE in (getattr(status, "audiences", None) or [])
                except Exception:
                    logger.warning(
                        "provisioner token review unavailable: %s %s",
                        request.method,
                        request.url.path,
                    )
        if not static_authenticated and not token_authenticated:
            logger.warning("provisioner auth rejected: %s %s", request.method, request.url.path)
            return Response(status_code=401, content="Unauthorized")
    return await call_next(request)


# ── Request / Response models ───────────────────────────────────────────


class ExtraMount(BaseModel):
    host_path: str
    container_path: str
    read_only: bool = False


class AcceptedSkillProjectionItemV1(BaseModel):
    """One bounded skill-tree digest in the accepted projection."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    category: str = Field(pattern=r"^(public|custom|integrations|legacy)$")
    relative_path: str = Field(min_length=1, max_length=512)
    manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_count: int = Field(ge=1, le=256)
    total_bytes: int = Field(ge=1, le=8 * 1024 * 1024)


class AcceptedSkillProjectionV1(BaseModel):
    """Strict request to verify one accepted snapshot into a private Pod copy."""

    model_config = ConfigDict(extra="forbid")

    profile: str = Field(pattern=r"^rwx_verified_copy_v1$")
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    run_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=0)
    projections: list[AcceptedSkillProjectionItemV1] = Field(min_length=1, max_length=64)
    file_count: int = Field(ge=1, le=2_048)
    total_bytes: int = Field(ge=1, le=32 * 1024 * 1024)

    @model_validator(mode="after")
    def validate_projection(self):
        if self.content_digest != self.snapshot_id:
            raise ValueError("accepted skill content_digest must equal snapshot_id")
        identities = [(item.category, item.relative_path, item.name) for item in self.projections]
        if identities != sorted(identities) or len(set(identities)) != len(identities):
            raise ValueError("accepted skill projections must be unique and ordered")
        if self.file_count != sum(item.file_count for item in self.projections):
            raise ValueError("accepted skill file_count mismatch")
        if self.total_bytes != sum(item.total_bytes for item in self.projections):
            raise ValueError("accepted skill total_bytes mismatch")
        return self

    def evidence_wire(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "content_digest": self.content_digest,
            "projections": [item.model_dump(mode="json") for item in self.projections],
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
        }


class AcceptedSkillProjectionV2(AcceptedSkillProjectionV1):
    """Projection requiring the complete v2 Kubernetes isolation receipt."""

    profile: str = Field(pattern=r"^rwx_verified_copy_v2$")


AcceptedSkillProjection = AcceptedSkillProjectionV1 | AcceptedSkillProjectionV2


class CreateSandboxRequest(BaseModel):
    sandbox_id: str
    thread_id: str | None = Field(default=None, pattern=SAFE_THREAD_ID_PATTERN)
    user_id: str = Field(default=DEFAULT_USER_ID, pattern=SAFE_USER_ID_PATTERN)
    extra_mounts: list[ExtraMount] = Field(default_factory=list)
    include_legacy_skills: bool = False
    # When true (and LARK_CLI_INIT_IMAGE is configured), provision the sandbox
    # lark-cli runtime via an init container + emptyDir instead of a runtime
    # hostPath/PVC extra mount.
    provision_lark_cli_runtime: bool = False
    # When true (and LARK_CLI_BROKER_IMAGE is configured), provision a lark-cli
    # broker sidecar (Pattern B, issue #4338): a shim in the sandbox forwards to
    # the sidecar, which holds the credentials — so the plaintext config/data are
    # mounted into the sidecar only, never the sandbox. Supersedes the runtime
    # binary + credential mounts when enabled.
    provision_lark_cli_broker: bool = False
    accepted_skills_only: bool = False
    accepted_skill_projection: AcceptedSkillProjection | None = None
    attempt_capability: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_-]{43,128}$",
    )

    @model_validator(mode="after")
    def validate_accepted_projection_pair(self):
        if (self.accepted_skill_projection is None) != (self.attempt_capability is None):
            raise ValueError(
                "accepted_skill_projection and attempt_capability must be supplied together",
            )
        if self.accepted_skill_projection is not None and not self.accepted_skills_only:
            raise ValueError(
                "accepted_skill_projection requires accepted_skills_only",
            )
        return self


class SandboxResponse(BaseModel):
    sandbox_id: str
    sandbox_url: str
    status: str
    accepted_skill_material: dict[str, object] | None = None


class RenewAcceptedAttemptRequest(BaseModel):
    """Exact process-local attempt identity used for bounded Lease renewal."""

    model_config = ConfigDict(extra="forbid")

    pod_uid: str = Field(min_length=1, max_length=128)
    lease_uid: str = Field(min_length=1, max_length=128)
    materialization_evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


# ── K8s resource helpers ─────────────────────────────────────────────────


def _accepted_attempt_lease_name(sandbox_id: str) -> str:
    return f"sandbox-{sandbox_id}-accepted-attempt"


def _accepted_attempt_identity(
    projection: AcceptedSkillProjection,
) -> str:
    payload = json.dumps(
        {
            "profile": projection.profile,
            "snapshot_id": projection.snapshot_id,
            "run_id": projection.run_id,
            "generation": projection.generation,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _capability_digest(capability: str) -> str:
    return hashlib.sha256(capability.encode("utf-8")).hexdigest()


def _build_accepted_attempt_lease(
    sandbox_id: str,
    projection: AcceptedSkillProjection,
    capability: str,
    *,
    isolation_digest: str = "0" * 64,
    now: datetime | None = None,
) -> k8s_client.V1Lease:
    """Build the single owner root for one immutable sandbox attempt."""

    observed_at = now or datetime.now(UTC)
    identity = _accepted_attempt_identity(projection)
    return k8s_client.V1Lease(
        metadata=k8s_client.V1ObjectMeta(
            name=_accepted_attempt_lease_name(sandbox_id),
            namespace=K8S_NAMESPACE,
            labels={
                "app": "deer-flow-sandbox",
                "sandbox-id": sandbox_id,
                "hartmesh.io/accepted-skill-attempt": "true",
            },
            annotations={
                "hartmesh.io/accepted-attempt-identity": identity,
                "hartmesh.io/accepted-capability-digest": _capability_digest(
                    capability,
                ),
                "hartmesh.io/accepted-skill-digest": projection.content_digest,
                "hartmesh.io/accepted-skill-run": projection.run_id,
                "hartmesh.io/accepted-skill-generation": str(
                    projection.generation,
                ),
                "hartmesh.io/accepted-isolation-digest": isolation_digest,
                "hartmesh.io/accepted-attempt-state": "claimed",
            },
        ),
        spec=k8s_client.V1LeaseSpec(
            acquire_time=observed_at,
            renew_time=observed_at,
            holder_identity=f"accepted:{identity}",
            lease_duration_seconds=ACCEPTED_ATTEMPT_LEASE_SECONDS,
        ),
    )


def _accepted_lease_expired(
    lease: object,
    *,
    now: datetime | None = None,
) -> bool:
    observed_at = now or datetime.now(UTC)
    spec = getattr(lease, "spec", None)
    renewed_at = getattr(spec, "renew_time", None) or getattr(
        spec,
        "acquire_time",
        None,
    )
    duration = getattr(spec, "lease_duration_seconds", None)
    if not isinstance(renewed_at, datetime) or type(duration) is not int:
        return True
    if renewed_at.tzinfo is None:
        renewed_at = renewed_at.replace(tzinfo=UTC)
    return renewed_at + timedelta(seconds=duration) <= observed_at


def _accepted_attempt_owner_reference(
    lease: k8s_client.V1Lease,
) -> k8s_client.V1OwnerReference:
    uid = getattr(getattr(lease, "metadata", None), "uid", None)
    if not isinstance(uid, str) or not uid:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_lease_identity_unavailable",
        )
    return k8s_client.V1OwnerReference(
        api_version="coordination.k8s.io/v1",
        kind="Lease",
        name=lease.metadata.name,
        uid=uid,
        controller=True,
        block_owner_deletion=False,
    )


def _lease_matches_attempt(
    lease: object,
    projection: AcceptedSkillProjection,
    capability: str,
    *,
    isolation_digest: str = "0" * 64,
) -> bool:
    annotations = getattr(getattr(lease, "metadata", None), "annotations", None)
    expected = {
        "hartmesh.io/accepted-attempt-identity": _accepted_attempt_identity(
            projection,
        ),
        "hartmesh.io/accepted-capability-digest": _capability_digest(capability),
        "hartmesh.io/accepted-skill-digest": projection.content_digest,
        "hartmesh.io/accepted-skill-run": projection.run_id,
        "hartmesh.io/accepted-skill-generation": str(projection.generation),
        "hartmesh.io/accepted-isolation-digest": isolation_digest,
    }
    return isinstance(annotations, dict) and all(annotations.get(key) == value for key, value in expected.items()) and annotations.get("hartmesh.io/accepted-attempt-state") in {"claimed", "pod_creation_started", "materialized"}


def _claim_accepted_attempt(
    sandbox_id: str,
    projection: AcceptedSkillProjection,
    capability: str,
    *,
    isolation_digest: str = "0" * 64,
    now: datetime | None = None,
) -> k8s_client.V1Lease:
    """Create or replay one exact live attempt; never adopt another identity."""

    if coordination_v1 is None:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_coordination_unavailable",
        )
    candidate = _build_accepted_attempt_lease(
        sandbox_id,
        projection,
        capability,
        isolation_digest=isolation_digest,
        now=now,
    )
    try:
        return coordination_v1.create_namespaced_lease(
            K8S_NAMESPACE,
            candidate,
        )
    except ApiException as exc:
        if exc.status != 409:
            raise HTTPException(
                status_code=503,
                detail="accepted_attempt_lease_unavailable",
            ) from exc
    try:
        existing = coordination_v1.read_namespaced_lease(
            candidate.metadata.name,
            K8S_NAMESPACE,
        )
    except ApiException as exc:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_lease_unavailable",
        ) from exc
    if _accepted_lease_expired(existing, now=now) or not _lease_matches_attempt(
        existing,
        projection,
        capability,
        isolation_digest=isolation_digest,
    ):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_identity_conflict",
        )
    return existing


def _replace_attempt_lease(
    lease: object,
    *,
    annotations: dict[str, str],
) -> object:
    if coordination_v1 is None:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_coordination_unavailable",
        )
    candidate = copy.deepcopy(lease)
    candidate.metadata.annotations = annotations
    try:
        return coordination_v1.replace_namespaced_lease(
            candidate.metadata.name,
            K8S_NAMESPACE,
            candidate,
        )
    except ApiException as exc:
        raise HTTPException(
            status_code=(409 if exc.status == 409 else 503),
            detail=("accepted_attempt_identity_conflict" if exc.status == 409 else "accepted_attempt_lease_unavailable"),
        ) from None


def _prepare_accepted_pod_creation(lease: object) -> tuple[object, bool]:
    """Irreversibly record that one Pod creation may have been attempted."""

    annotations = dict(getattr(lease.metadata, "annotations", None) or {})
    state = annotations.get("hartmesh.io/accepted-attempt-state")
    if state == "claimed":
        annotations["hartmesh.io/accepted-attempt-state"] = "pod_creation_started"
        return _replace_attempt_lease(lease, annotations=annotations), True
    if state in {"pod_creation_started", "materialized"}:
        return lease, False
    raise HTTPException(
        status_code=409,
        detail="accepted_attempt_identity_conflict",
    )


def _bind_accepted_attempt_pod_uid(lease: object, pod_uid: str) -> object:
    annotations = dict(getattr(lease.metadata, "annotations", None) or {})
    existing = annotations.get("hartmesh.io/accepted-pod-uid")
    if existing is not None:
        if existing != pod_uid:
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_pod_replaced",
            )
        return lease
    if annotations.get("hartmesh.io/accepted-attempt-state") != ("pod_creation_started"):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_identity_conflict",
        )
    annotations["hartmesh.io/accepted-pod-uid"] = pod_uid
    return _replace_attempt_lease(lease, annotations=annotations)


def _bind_accepted_attempt_materialization(
    lease: object,
    receipt: dict[str, object],
) -> object:
    annotations = dict(getattr(lease.metadata, "annotations", None) or {})
    pod_uid = receipt.get("pod_uid")
    if annotations.get("hartmesh.io/accepted-pod-uid") != pod_uid:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_pod_replaced",
        )
    fields = {
        "hartmesh.io/accepted-pod-isolation-digest": receipt.get(
            "pod_isolation_digest",
        ),
        "hartmesh.io/accepted-network-policy-uid": receipt.get(
            "network_policy_uid",
        ),
        "hartmesh.io/accepted-network-policy-spec-digest": receipt.get(
            "network_policy_spec_digest",
        ),
        "hartmesh.io/accepted-evidence-secret-uid": receipt.get(
            "evidence_secret_uid",
        ),
        "hartmesh.io/accepted-evidence-secret-digest": receipt.get(
            "evidence_secret_digest",
        ),
        "hartmesh.io/accepted-capability-secret-uid": receipt.get(
            "capability_secret_uid",
        ),
        "hartmesh.io/accepted-capability-secret-digest": receipt.get(
            "capability_secret_digest",
        ),
        "hartmesh.io/accepted-sandbox-image-digest": receipt.get(
            "sandbox_image_digest",
        ),
        "hartmesh.io/accepted-skill-runtime-image-digest": receipt.get(
            "accepted_skill_runtime_image_digest",
        ),
        "hartmesh.io/accepted-verifier-receipt-digest": receipt.get(
            "verifier_receipt_digest",
        ),
        "hartmesh.io/accepted-runtime-images-digest": receipt.get(
            "runtime_image_ids_digest",
        ),
        "hartmesh.io/accepted-materialization-digest": receipt.get(
            "materialization_evidence_digest",
        ),
    }
    identity_fields = {
        "hartmesh.io/accepted-network-policy-uid",
        "hartmesh.io/accepted-evidence-secret-uid",
        "hartmesh.io/accepted-capability-secret-uid",
    }
    if any(
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 128
        or any(ord(character) < 32 for character in value)
        if key in identity_fields
        else not isinstance(value, str)
        or re.fullmatch(r"[0-9a-f]{64}", value) is None
        for key, value in fields.items()
    ):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_materialization_invalid",
        )
    if annotations.get("hartmesh.io/accepted-attempt-state") == "materialized":
        if not all(annotations.get(key) == value for key, value in fields.items()):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_materialization_mismatch",
            )
        return lease
    if annotations.get("hartmesh.io/accepted-attempt-state") != ("pod_creation_started"):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_identity_conflict",
        )
    annotations.update(fields)
    annotations["hartmesh.io/accepted-attempt-state"] = "materialized"
    return _replace_attempt_lease(lease, annotations=annotations)


def _delete_lease_by_exact_uid(name: str, uid: str) -> None:
    if coordination_v1 is None:
        return
    coordination_v1.delete_namespaced_lease(
        name,
        K8S_NAMESPACE,
        body=k8s_client.V1DeleteOptions(
            propagation_policy="Background",
            preconditions=k8s_client.V1Preconditions(uid=uid),
        ),
    )


def _reconcile_expired_accepted_attempts(
    *,
    now: datetime | None = None,
) -> int:
    """Delete at most one bounded page of expired attempt owner roots."""

    global _accepted_reconcile_continue

    if coordination_v1 is None:
        return 0
    observed_at = now or datetime.now(UTC)
    try:
        page = coordination_v1.list_namespaced_lease(
            K8S_NAMESPACE,
            label_selector="hartmesh.io/accepted-skill-attempt=true",
            limit=ACCEPTED_ATTEMPT_RECONCILE_LIMIT,
            _continue=_accepted_reconcile_continue,
        )
    except ApiException as exc:
        if exc.status == 410:
            _accepted_reconcile_continue = None
        logger.warning("accepted attempt reconciliation could not list leases")
        return 0
    next_cursor = getattr(getattr(page, "metadata", None), "_continue", None)
    _accepted_reconcile_continue = next_cursor if isinstance(next_cursor, str) and next_cursor else None
    removed = 0
    for lease in list(getattr(page, "items", ()) or ()):
        if not _accepted_lease_expired(lease, now=observed_at):
            continue
        metadata = getattr(lease, "metadata", None)
        name = getattr(metadata, "name", None)
        uid = getattr(metadata, "uid", None)
        if not isinstance(name, str) or not isinstance(uid, str):
            continue
        try:
            _delete_lease_by_exact_uid(name, uid)
        except ApiException as exc:
            if exc.status not in {404, 409}:
                logger.warning(
                    "accepted attempt reconciliation failed: lease=%s status=%s",
                    name,
                    exc.status,
                )
            continue
        removed += 1
    return removed


async def _accepted_attempt_reconcile_loop() -> None:
    while True:
        await asyncio.sleep(ACCEPTED_ATTEMPT_RECONCILE_INTERVAL_SECONDS)
        await asyncio.to_thread(_reconcile_expired_accepted_attempts)


def _pod_name(sandbox_id: str) -> str:
    return f"sandbox-{sandbox_id}"


def _svc_name(sandbox_id: str) -> str:
    return f"sandbox-{sandbox_id}-svc"


def _accepted_evidence_secret_name(sandbox_id: str) -> str:
    return f"sandbox-{sandbox_id}-accepted-evidence"


def _accepted_capability_secret_name(sandbox_id: str) -> str:
    return f"sandbox-{sandbox_id}-accepted-capability"


def _accepted_subject_scope(user_id: str) -> str:
    return "subject-" + hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:32]


def _accepted_snapshot_sub_path(
    user_id: str,
    projection: AcceptedSkillProjection,
) -> str:
    return posixpath.join(
        "deer-flow",
        "runtime",
        "skill-snapshots",
        _accepted_subject_scope(user_id),
        projection.snapshot_id,
    )


def _require_accepted_projection_runtime() -> None:
    if ACCEPTED_SKILL_PROJECTION_PROFILE != ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2:
        raise HTTPException(
            status_code=503,
            detail="rwx_verified_copy_v2 accepted skill projection is not enabled",
        )
    if not USERDATA_PVC_NAME:
        raise HTTPException(
            status_code=503,
            detail="rwx_verified_copy_v2 requires an RWX home PVC",
        )
    if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", ACCEPTED_SKILL_RUNTIME_IMAGE) is None:
        raise HTTPException(
            status_code=503,
            detail="rwx_verified_copy_v2 requires a digest-pinned accepted skill runtime image",
        )
    if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", SANDBOX_IMAGE) is None:
        raise HTTPException(
            status_code=503,
            detail="rwx_verified_copy_v2 requires a digest-pinned sandbox image",
        )


def _restricted_container_security_context(
    *,
    read_only_root_filesystem: bool | None = None,
) -> k8s_client.V1SecurityContext:
    """Return the baseline security profile for every sandbox Pod container."""

    return k8s_client.V1SecurityContext(
        privileged=False,
        allow_privilege_escalation=False,
        read_only_root_filesystem=read_only_root_filesystem,
        capabilities=k8s_client.V1Capabilities(drop=["ALL"]),
        seccomp_profile=k8s_client.V1SeccompProfile(type="RuntimeDefault"),
    )


def _accepted_skill_volumes(
    sandbox_id: str,
    thread_id: str,
    user_id: str,
    *,
    projection: AcceptedSkillProjection | None,
    extra_mounts: list[ExtraMount] | None,
    provision_lark_cli_runtime: bool,
    provision_lark_cli_broker: bool,
) -> list[k8s_client.V1Volume]:
    """Build an accepted-only volume set with no live skill source alias."""

    ordinary = _build_volumes(
        thread_id,
        user_id=user_id,
        extra_mounts=extra_mounts,
        provision_lark_cli_runtime=provision_lark_cli_runtime,
        provision_lark_cli_broker=provision_lark_cli_broker,
    )
    volumes = [volume for volume in ordinary if not volume.name.startswith("skills")]
    volumes.append(
        k8s_client.V1Volume(
            name="accepted-skill-material",
            empty_dir=k8s_client.V1EmptyDirVolumeSource(),
        ),
    )
    volumes.append(
        k8s_client.V1Volume(
            name="accepted-skill-receipt",
            empty_dir=k8s_client.V1EmptyDirVolumeSource(),
        ),
    )
    if projection is not None:
        volumes.extend(
            [
                k8s_client.V1Volume(
                    name="accepted-skill-source",
                    persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=USERDATA_PVC_NAME,
                        read_only=True,
                    ),
                ),
                k8s_client.V1Volume(
                    name="accepted-skill-evidence",
                    secret=k8s_client.V1SecretVolumeSource(
                        secret_name=_accepted_evidence_secret_name(sandbox_id),
                        default_mode=0o400,
                    ),
                ),
                k8s_client.V1Volume(
                    name="accepted-skill-capability",
                    secret=k8s_client.V1SecretVolumeSource(
                        secret_name=_accepted_capability_secret_name(sandbox_id),
                        default_mode=0o400,
                    ),
                ),
            ]
        )
    return volumes


def _accepted_sandbox_mounts(
    thread_id: str,
    user_id: str,
    *,
    extra_mounts: list[ExtraMount] | None,
    provision_lark_cli_runtime: bool,
    provision_lark_cli_broker: bool,
) -> list[k8s_client.V1VolumeMount]:
    ordinary = _build_volume_mounts(
        thread_id,
        user_id=user_id,
        extra_mounts=extra_mounts,
        provision_lark_cli_runtime=provision_lark_cli_runtime,
        provision_lark_cli_broker=provision_lark_cli_broker,
    )
    mounts = [mount for mount in ordinary if mount.mount_path != "/mnt/skills" and not mount.mount_path.startswith("/mnt/skills/")]
    mounts.append(
        k8s_client.V1VolumeMount(
            name="accepted-skill-material",
            mount_path=ACCEPTED_SKILL_SANDBOX_MOUNT,
            read_only=True,
        )
    )
    return mounts


def _accepted_verifier_container(
    user_id: str,
    projection: AcceptedSkillProjection,
) -> k8s_client.V1Container:
    return k8s_client.V1Container(
        name="accepted-skill-verifier",
        image=ACCEPTED_SKILL_RUNTIME_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["python", "/app/accepted_skills.py"],
        args=[
            "materialize",
            "--source",
            ACCEPTED_SKILL_SOURCE_MOUNT,
            "--destination",
            ACCEPTED_SKILL_DESTINATION_MOUNT,
            "--evidence-file",
            f"{ACCEPTED_SKILL_EVIDENCE_MOUNT}/evidence.json",
            "--receipt-file",
            f"{ACCEPTED_SKILL_RECEIPT_MOUNT}/receipt.json",
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                name="accepted-skill-source",
                mount_path=ACCEPTED_SKILL_SOURCE_MOUNT,
                sub_path=_accepted_snapshot_sub_path(user_id, projection),
                read_only=True,
            ),
            k8s_client.V1VolumeMount(
                name="accepted-skill-material",
                mount_path=ACCEPTED_SKILL_DESTINATION_MOUNT,
                read_only=False,
            ),
            k8s_client.V1VolumeMount(
                name="accepted-skill-evidence",
                mount_path=ACCEPTED_SKILL_EVIDENCE_MOUNT,
                read_only=True,
            ),
            k8s_client.V1VolumeMount(
                name="accepted-skill-receipt",
                mount_path=ACCEPTED_SKILL_RECEIPT_MOUNT,
                read_only=False,
            ),
        ],
        security_context=_restricted_container_security_context(
            read_only_root_filesystem=True,
        ),
    )


def _accepted_gate_container() -> k8s_client.V1Container:
    return k8s_client.V1Container(
        name="accepted-skill-gate",
        image=ACCEPTED_SKILL_RUNTIME_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["python", "/app/accepted_skills.py"],
        args=[
            "gate",
            "--listen-port",
            str(ACCEPTED_SKILL_GATE_PORT),
            "--upstream",
            f"http://127.0.0.1:{SANDBOX_CONTAINER_PORT}",
            "--capability-file",
            f"{ACCEPTED_SKILL_CAPABILITY_MOUNT}/capability",
            "--receipt-file",
            f"{ACCEPTED_SKILL_RECEIPT_MOUNT}/receipt.json",
        ],
        ports=[
            k8s_client.V1ContainerPort(
                name="accepted-http",
                container_port=ACCEPTED_SKILL_GATE_PORT,
                protocol="TCP",
            )
        ],
        readiness_probe=k8s_client.V1Probe(
            tcp_socket=k8s_client.V1TCPSocketAction(
                port=ACCEPTED_SKILL_GATE_PORT,
            ),
            initial_delay_seconds=1,
            period_seconds=2,
            timeout_seconds=1,
            failure_threshold=10,
        ),
        volume_mounts=[
            k8s_client.V1VolumeMount(
                name="accepted-skill-capability",
                mount_path=ACCEPTED_SKILL_CAPABILITY_MOUNT,
                read_only=True,
            ),
            k8s_client.V1VolumeMount(
                name="accepted-skill-receipt",
                mount_path=ACCEPTED_SKILL_RECEIPT_MOUNT,
                read_only=True,
            ),
        ],
        security_context=_restricted_container_security_context(
            read_only_root_filesystem=True,
        ),
    )


def _sandbox_url(sandbox_id: str, node_port: int | None = None) -> str:
    """Build the sandbox access URL for the configured Service mode."""
    if SANDBOX_SERVICE_TYPE == "ClusterIP":
        return f"http://{_svc_name(sandbox_id)}.{K8S_NAMESPACE}.svc.cluster.local:{SANDBOX_CONTAINER_PORT}"
    if node_port is None:
        raise RuntimeError("node_port is required when SANDBOX_SERVICE_TYPE=NodePort")
    return f"http://{NODE_HOST}:{node_port}"


def _build_extra_volumes(extra_mounts: list[ExtraMount] | None = None) -> list[k8s_client.V1Volume]:
    volumes: list[k8s_client.V1Volume] = []
    for index, mount in enumerate(_validated_extra_mounts(extra_mounts)):
        if SANDBOX_VOLUME_CONFIG.mode == "pvc":
            volumes.append(
                k8s_client.V1Volume(
                    name=_extra_mount_volume_name(index),
                    persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=USERDATA_PVC_NAME,
                    ),
                )
            )
            continue

        volumes.append(
            k8s_client.V1Volume(
                name=_extra_mount_volume_name(index),
                host_path=k8s_client.V1HostPathVolumeSource(
                    path=mount.host_path,
                    type="Directory" if mount.read_only else "DirectoryOrCreate",
                ),
            )
        )
    return volumes


def _build_extra_volume_mounts(extra_mounts: list[ExtraMount] | None = None) -> list[k8s_client.V1VolumeMount]:
    mounts: list[k8s_client.V1VolumeMount] = []
    for index, mount in enumerate(_validated_extra_mounts(extra_mounts)):
        volume_mount = k8s_client.V1VolumeMount(
            name=_extra_mount_volume_name(index),
            mount_path=mount.container_path,
            read_only=mount.read_only,
        )
        if SANDBOX_VOLUME_CONFIG.mode == "pvc":
            volume_mount.sub_path = _extra_mount_pvc_sub_path(mount.host_path)
        mounts.append(volume_mount)
    return mounts


def _build_volumes(
    thread_id: str,
    user_id: str = DEFAULT_USER_ID,
    *,
    include_legacy_skills: bool = False,
    extra_mounts: list[ExtraMount] | None = None,
    provision_lark_cli_runtime: bool = False,
    provision_lark_cli_broker: bool = False,
) -> list[k8s_client.V1Volume]:
    """Build the volume list for the startup-resolved PVC or hostPath mode.

    Skills are split into public, per-user custom, and legacy (global-custom)
    volumes so that ``/mnt/skills/{public,custom,legacy}/`` paths resolve
    correctly inside the sandbox — matching the hostPath layout produced by
    ``LocalSandboxProvider`` and ``AioSandboxProvider``.
    """
    volumes: list[k8s_client.V1Volume] = []
    del include_legacy_skills  # retained for request compatibility

    # ── Skills volumes ────────────────────────────────────────────────

    if SANDBOX_VOLUME_CONFIG.mode == "pvc":
        # PVC mode: three-way subPath not yet supported; fall back to
        # single-volume mount for backward compatibility.
        logger.warning("SKILLS_PVC_NAME is set — three-way skills layout is not supported in PVC mode yet; falling back to single /mnt/skills mount")
        volumes.append(
            k8s_client.V1Volume(
                name="skills",
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=SKILLS_PVC_NAME,
                    read_only=True,
                ),
            )
        )
    else:
        # hostPath mode: three-way layout
        public_path = join_host_path(DEER_FLOW_HOST_BASE_DIR, "skills_view", "public")
        volumes.append(
            k8s_client.V1Volume(
                name="skills-public",
                host_path=k8s_client.V1HostPathVolumeSource(
                    path=public_path,
                    type="Directory",
                ),
            )
        )

        user_custom_path = join_host_path(
            DEER_FLOW_HOST_BASE_DIR,
            "users",
            user_id,
            "skills_view",
            "custom",
        )
        volumes.append(
            k8s_client.V1Volume(
                name="skills-custom",
                host_path=k8s_client.V1HostPathVolumeSource(
                    path=user_custom_path,
                    type="Directory",
                ),
            )
        )

        legacy_path = join_host_path(DEER_FLOW_HOST_BASE_DIR, "users", user_id, "skills_view", "legacy")
        volumes.append(
            k8s_client.V1Volume(
                name="skills-legacy",
                host_path=k8s_client.V1HostPathVolumeSource(
                    path=legacy_path,
                    type="Directory",
                ),
            )
        )

    # ── User-data volume ──────────────────────────────────────────────

    if SANDBOX_VOLUME_CONFIG.mode == "pvc":
        userdata_vol = k8s_client.V1Volume(
            name="user-data",
            persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                claim_name=USERDATA_PVC_NAME,
            ),
        )
    else:
        userdata_vol = k8s_client.V1Volume(
            name="user-data",
            host_path=k8s_client.V1HostPathVolumeSource(
                path=join_host_path(THREADS_HOST_PATH, thread_id, "user-data"),
                type="DirectoryOrCreate",
            ),
        )

    volumes.append(userdata_vol)
    volumes.extend(
        _build_extra_volumes(
            _runtime_provided_extra_mounts(
                extra_mounts,
                provision_lark_cli_runtime=provision_lark_cli_runtime,
                provision_lark_cli_broker=provision_lark_cli_broker,
            )
        )
    )
    # The runtime emptyDir is shared by the init container (writer) and the
    # sandbox container (reader) in both Pattern A and Pattern B (shim).
    if _lark_cli_runtime_enabled(provision_lark_cli_runtime) or _lark_cli_broker_enabled(provision_lark_cli_broker):
        volumes.append(
            k8s_client.V1Volume(
                name=LARK_CLI_RUNTIME_VOLUME_NAME,
                empty_dir=k8s_client.V1EmptyDirVolumeSource(),
            )
        )
    # Pattern B: config/locks/data volumes go to the broker sidecar only.
    if _lark_cli_broker_enabled(provision_lark_cli_broker):
        credential_mounts = _lark_broker_credential_mounts(extra_mounts)
        for container_path, volume_name in (
            (LARK_CLI_CONFIG_CONTAINER_PATH, LARK_BROKER_CONFIG_VOLUME_NAME),
            (LARK_CLI_LOCKS_CONTAINER_PATH, LARK_BROKER_LOCKS_VOLUME_NAME),
            (LARK_CLI_DATA_CONTAINER_PATH, LARK_BROKER_DATA_VOLUME_NAME),
        ):
            mount = credential_mounts.get(container_path)
            if mount is None:
                continue
            if SANDBOX_VOLUME_CONFIG.mode == "pvc":
                volumes.append(
                    k8s_client.V1Volume(
                        name=volume_name,
                        persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                            claim_name=USERDATA_PVC_NAME,
                        ),
                    )
                )
            else:
                volumes.append(
                    k8s_client.V1Volume(
                        name=volume_name,
                        host_path=k8s_client.V1HostPathVolumeSource(
                            path=mount.host_path,
                            type="Directory" if mount.read_only else "DirectoryOrCreate",
                        ),
                    )
                )
    return volumes


def _build_volume_mounts(
    thread_id: str,
    user_id: str = DEFAULT_USER_ID,
    *,
    include_legacy_skills: bool = False,
    extra_mounts: list[ExtraMount] | None = None,
    provision_lark_cli_runtime: bool = False,
    provision_lark_cli_broker: bool = False,
) -> list[k8s_client.V1VolumeMount]:
    """Build volume mount list, mirroring three-way skills layout.

    Skills are mounted to ``/mnt/skills/{public,custom,legacy}/`` so that
    category-aware ``Skill.get_container_path()`` paths resolve correctly.
    PVC mode falls back to a single ``/mnt/skills`` mount and can optionally
    scope that mount with ``SKILLS_PVC_SUBPATH_TEMPLATE``.
    """
    mounts: list[k8s_client.V1VolumeMount] = []
    del include_legacy_skills  # retained for request compatibility

    if SANDBOX_VOLUME_CONFIG.mode == "pvc":
        skills_mount = k8s_client.V1VolumeMount(
            name="skills",
            mount_path="/mnt/skills",
            read_only=True,
        )
        if SKILLS_PVC_SUBPATH_TEMPLATE:
            skills_mount.sub_path = SKILLS_PVC_SUBPATH_TEMPLATE.format(
                user_id=user_id,
                thread_id=thread_id,
            )
        mounts.append(skills_mount)
    else:
        mounts.extend(
            [
                k8s_client.V1VolumeMount(
                    name="skills-public",
                    mount_path="/mnt/skills/public",
                    read_only=True,
                ),
                k8s_client.V1VolumeMount(
                    name="skills-custom",
                    mount_path="/mnt/skills/custom",
                    read_only=True,
                ),
                k8s_client.V1VolumeMount(
                    name="skills-legacy",
                    mount_path="/mnt/skills/legacy",
                    read_only=True,
                ),
            ]
        )

    userdata_mount = k8s_client.V1VolumeMount(
        name="user-data",
        mount_path="/mnt/user-data",
        read_only=False,
    )
    if SANDBOX_VOLUME_CONFIG.mode == "pvc":
        userdata_mount.sub_path = f"deer-flow/users/{user_id}/threads/{thread_id}/user-data"
    mounts.append(userdata_mount)
    mounts.extend(
        _build_extra_volume_mounts(
            _runtime_provided_extra_mounts(
                extra_mounts,
                provision_lark_cli_runtime=provision_lark_cli_runtime,
                provision_lark_cli_broker=provision_lark_cli_broker,
            )
        )
    )
    # Sandbox reads the runtime dir (real binary in Pattern A, shim in Pattern B).
    if _lark_cli_runtime_enabled(provision_lark_cli_runtime) or _lark_cli_broker_enabled(provision_lark_cli_broker):
        mounts.append(
            k8s_client.V1VolumeMount(
                name=LARK_CLI_RUNTIME_VOLUME_NAME,
                mount_path=LARK_CLI_RUNTIME_CONTAINER_PATH,
                read_only=True,
            )
        )

    return mounts


def _build_lark_cli_init_containers(
    provision_lark_cli_runtime: bool,
    provision_lark_cli_broker: bool = False,
) -> list[k8s_client.V1Container]:
    """Init container that stages the lark-cli runtime into the shared emptyDir.

    Pattern B (broker) supersedes Pattern A: the broker image's ``install-shim``
    mode writes the forwarding shim; Pattern A's init image copies the real
    binary layout.
    """
    runtime_mount = k8s_client.V1VolumeMount(
        name=LARK_CLI_RUNTIME_VOLUME_NAME,
        mount_path=LARK_CLI_RUNTIME_CONTAINER_PATH,
        read_only=False,
    )
    secure = _restricted_container_security_context()
    if _lark_cli_broker_enabled(provision_lark_cli_broker):
        return [
            k8s_client.V1Container(
                name="lark-cli-shim-init",
                image=LARK_CLI_BROKER_IMAGE,
                image_pull_policy="IfNotPresent",
                args=["install-shim", LARK_CLI_RUNTIME_CONTAINER_PATH],
                env=[k8s_client.V1EnvVar(name="LARK_CLI_RUNTIME_DEST", value=LARK_CLI_RUNTIME_CONTAINER_PATH)],
                volume_mounts=[runtime_mount],
                security_context=secure,
            )
        ]
    if not _lark_cli_runtime_enabled(provision_lark_cli_runtime):
        return []
    return [
        k8s_client.V1Container(
            name="lark-cli-init",
            image=LARK_CLI_INIT_IMAGE,
            image_pull_policy="IfNotPresent",
            env=[
                k8s_client.V1EnvVar(
                    name="LARK_CLI_RUNTIME_DEST",
                    value=LARK_CLI_RUNTIME_CONTAINER_PATH,
                )
            ],
            volume_mounts=[runtime_mount],
            security_context=secure,
        )
    ]


def _build_lark_cli_broker_sidecars(
    provision_lark_cli_broker: bool,
    extra_mounts: list[ExtraMount] | None,
) -> list[k8s_client.V1Container]:
    """Broker sidecar that holds lark-cli + the per-user credentials (Pattern B).

    The config/locks/data dirs are mounted **only** here (never on the sandbox
    container), so the plaintext app secret / OAuth tokens stay out of the
    sandbox filesystem. The config root is read-only and its nested locks mount
    is writable. The broker serves the command surface on loopback.
    """
    if not _lark_cli_broker_enabled(provision_lark_cli_broker):
        return []
    credential_mounts = _lark_broker_credential_mounts(extra_mounts)
    volume_mounts: list[k8s_client.V1VolumeMount] = []
    for container_path, volume_name, sidecar_path in (
        (LARK_CLI_CONFIG_CONTAINER_PATH, LARK_BROKER_CONFIG_VOLUME_NAME, LARK_BROKER_SIDECAR_CONFIG_PATH),
        (LARK_CLI_LOCKS_CONTAINER_PATH, LARK_BROKER_LOCKS_VOLUME_NAME, LARK_BROKER_SIDECAR_LOCKS_PATH),
        (LARK_CLI_DATA_CONTAINER_PATH, LARK_BROKER_DATA_VOLUME_NAME, LARK_BROKER_SIDECAR_DATA_PATH),
    ):
        mount = credential_mounts.get(container_path)
        if mount is None:
            continue
        sidecar_mount = k8s_client.V1VolumeMount(
            name=volume_name,
            mount_path=sidecar_path,
            read_only=mount.read_only,
        )
        if SANDBOX_VOLUME_CONFIG.mode == "pvc":
            sidecar_mount.sub_path = _extra_mount_pvc_sub_path(mount.host_path)
        volume_mounts.append(sidecar_mount)
    broker_env = [
        k8s_client.V1EnvVar(name="LARKSUITE_CLI_CONFIG_DIR", value=LARK_BROKER_SIDECAR_CONFIG_PATH),
        k8s_client.V1EnvVar(name="LARKSUITE_CLI_DATA_DIR", value=LARK_BROKER_SIDECAR_DATA_PATH),
    ]
    # Forward the optional subcommand denylist so the broker refuses secret-dump
    # subcommands (issue #4338 hardening); omitted when unset ⇒ nothing blocked.
    if LARK_CLI_BROKER_DENY_SUBCOMMANDS:
        broker_env.append(
            k8s_client.V1EnvVar(
                name="DEERFLOW_LARK_BROKER_DENY_SUBCOMMANDS",
                value=LARK_CLI_BROKER_DENY_SUBCOMMANDS,
            )
        )
    return [
        k8s_client.V1Container(
            name="lark-cli-broker",
            image=LARK_CLI_BROKER_IMAGE,
            image_pull_policy="IfNotPresent",
            args=["serve"],
            env=broker_env,
            volume_mounts=volume_mounts,
            security_context=_restricted_container_security_context(),
        )
    ]


def _accepted_mount_projection(mount: object) -> dict[str, object]:
    return {
        "name": getattr(mount, "name", None),
        "mount_path": getattr(mount, "mount_path", None),
        "sub_path": getattr(mount, "sub_path", None),
        "read_only": bool(getattr(mount, "read_only", False)),
    }


def _canonical_k8s_value(value: object) -> object:
    """Return a deterministic JSON value for a bounded host-built K8s field."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {
            str(key): _canonical_k8s_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_k8s_value(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _canonical_k8s_value(to_dict())
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, dict):
        return _canonical_k8s_value(
            {
                key: item
                for key, item in attributes.items()
                if not key.startswith("_")
            }
        )
    raise HTTPException(
        status_code=409,
        detail="accepted_attempt_resource_spec_invalid",
    )


def _accepted_security_projection(context: object | None) -> object:
    return _canonical_k8s_value(context)


def _accepted_container_projection(container: object) -> dict[str, object]:
    return {
        "name": getattr(container, "name", None),
        "image": getattr(container, "image", None),
        "image_pull_policy": getattr(container, "image_pull_policy", None),
        "command": list(getattr(container, "command", None) or []),
        "args": list(getattr(container, "args", None) or []),
        "env": _canonical_k8s_value(getattr(container, "env", None) or []),
        "env_from": _canonical_k8s_value(
            getattr(container, "env_from", None) or [],
        ),
        "working_dir": getattr(container, "working_dir", None),
        "ports": sorted(
            (
                getattr(port, "name", None),
                getattr(port, "container_port", None),
                getattr(port, "protocol", None),
            )
            for port in (getattr(container, "ports", None) or [])
        ),
        "mounts": sorted(
            (_accepted_mount_projection(mount) for mount in (getattr(container, "volume_mounts", None) or [])),
            key=lambda item: (
                str(item["mount_path"]),
                str(item["name"]),
            ),
        ),
        "security": _accepted_security_projection(
            getattr(container, "security_context", None),
        ),
        "resources": _canonical_k8s_value(
            getattr(container, "resources", None),
        ),
        "readiness_probe": _canonical_k8s_value(
            getattr(container, "readiness_probe", None),
        ),
        "liveness_probe": _canonical_k8s_value(
            getattr(container, "liveness_probe", None),
        ),
        "startup_probe": _canonical_k8s_value(
            getattr(container, "startup_probe", None),
        ),
        "lifecycle": _canonical_k8s_value(
            getattr(container, "lifecycle", None),
        ),
        "stdin": bool(getattr(container, "stdin", False)),
        "tty": bool(getattr(container, "tty", False)),
    }


def _accepted_volume_projection(volume: object) -> dict[str, object]:
    pvc = getattr(volume, "persistent_volume_claim", None)
    secret = getattr(volume, "secret", None)
    host_path = getattr(volume, "host_path", None)
    config_map = getattr(volume, "config_map", None)
    projected = getattr(volume, "projected", None)
    return {
        "name": getattr(volume, "name", None),
        "empty_dir": _canonical_k8s_value(
            getattr(volume, "empty_dir", None),
        ),
        "pvc": (
            None
            if pvc is None
            else {
                "claim_name": getattr(pvc, "claim_name", None),
                "read_only": bool(getattr(pvc, "read_only", False)),
            }
        ),
        "secret": (
            None
            if secret is None
            else {
                "secret_name": getattr(secret, "secret_name", None),
                "default_mode": getattr(secret, "default_mode", None),
            }
        ),
        "host_path": (
            None
            if host_path is None
            else {
                "path": getattr(host_path, "path", None),
                "type": getattr(host_path, "type", None),
            }
        ),
        "config_map": (
            None
            if config_map is None
            else {
                "name": getattr(config_map, "name", None),
                "default_mode": getattr(config_map, "default_mode", None),
            }
        ),
        "projected": _canonical_k8s_value(projected),
        "csi": _canonical_k8s_value(getattr(volume, "csi", None)),
        "downward_api": _canonical_k8s_value(
            getattr(volume, "downward_api", None),
        ),
        "ephemeral": _canonical_k8s_value(
            getattr(volume, "ephemeral", None),
        ),
    }


def _accepted_pod_isolation_digest(pod: object) -> str:
    """Digest the admitted Pod fields that enforce accepted-skill isolation."""

    spec = getattr(pod, "spec", None)
    if spec is None:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_pod_spec_mismatch",
        )
    projection = {
        "version": 2,
        "namespace": getattr(getattr(pod, "metadata", None), "namespace", None),
        "labels": _canonical_k8s_value(
            getattr(getattr(pod, "metadata", None), "labels", None) or {},
        ),
        "host_network": bool(getattr(spec, "host_network", False)),
        "host_pid": bool(getattr(spec, "host_pid", False)),
        "host_ipc": bool(getattr(spec, "host_ipc", False)),
        "share_process_namespace": bool(
            getattr(spec, "share_process_namespace", False),
        ),
        "service_account_name": getattr(spec, "service_account_name", None),
        "automount_service_account_token": getattr(
            spec,
            "automount_service_account_token",
            None,
        ),
        "runtime_class_name": getattr(spec, "runtime_class_name", None),
        "dns_policy": getattr(spec, "dns_policy", None),
        "dns_config": _canonical_k8s_value(getattr(spec, "dns_config", None)),
        "restart_policy": getattr(spec, "restart_policy", None),
        "security_context": _accepted_security_projection(
            getattr(spec, "security_context", None),
        ),
        "image_pull_secrets": _canonical_k8s_value(
            getattr(spec, "image_pull_secrets", None) or [],
        ),
        "affinity": _canonical_k8s_value(getattr(spec, "affinity", None)),
        "containers": [_accepted_container_projection(container) for container in (getattr(spec, "containers", None) or [])],
        "init_containers": [_accepted_container_projection(container) for container in (getattr(spec, "init_containers", None) or [])],
        "ephemeral_containers": [_accepted_container_projection(container) for container in (getattr(spec, "ephemeral_containers", None) or [])],
        "volumes": sorted(
            (_accepted_volume_projection(volume) for volume in (getattr(spec, "volumes", None) or [])),
            key=lambda item: str(item["name"]),
        ),
    }
    return hashlib.sha256(
        json.dumps(
            projection,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ).hexdigest()


def _build_pod(
    sandbox_id: str,
    thread_id: str,
    user_id: str = DEFAULT_USER_ID,
    *,
    include_legacy_skills: bool = False,
    extra_mounts: list[ExtraMount] | None = None,
    provision_lark_cli_runtime: bool = False,
    provision_lark_cli_broker: bool = False,
    accepted_skills_only: bool = False,
    accepted_skill_projection: AcceptedSkillProjection | None = None,
    attempt_capability: str | None = None,
    accepted_attempt_owner: k8s_client.V1OwnerReference | None = None,
) -> k8s_client.V1Pod:
    """Construct a Pod manifest for a single sandbox."""
    if (accepted_skill_projection is None) != (attempt_capability is None):
        raise HTTPException(
            status_code=400,
            detail="accepted skill projection and capability must be supplied together",
        )
    accepted_material = accepted_skill_projection is not None
    accepted_skills_only = accepted_skills_only or accepted_material
    if accepted_skills_only:
        _reject_accepted_skill_source_aliases(extra_mounts)
    if accepted_material:
        _require_accepted_projection_runtime()
        assert accepted_skill_projection is not None
        if not isinstance(accepted_skill_projection, AcceptedSkillProjectionV2):
            raise HTTPException(
                status_code=409,
                detail="accepted_skill_projection_version_unsupported",
            )
        init_container_items = [
            _accepted_verifier_container(user_id, accepted_skill_projection),
            *_build_lark_cli_init_containers(
                provision_lark_cli_runtime,
                provision_lark_cli_broker,
            ),
        ]
        volumes = _accepted_skill_volumes(
            sandbox_id,
            thread_id,
            user_id,
            projection=accepted_skill_projection,
            extra_mounts=extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
        sandbox_mounts = _accepted_sandbox_mounts(
            thread_id,
            user_id,
            extra_mounts=extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
    elif accepted_skills_only:
        init_container_items = _build_lark_cli_init_containers(
            provision_lark_cli_runtime,
            provision_lark_cli_broker,
        )
        volumes = _accepted_skill_volumes(
            sandbox_id,
            thread_id,
            user_id,
            projection=None,
            extra_mounts=extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
        sandbox_mounts = _accepted_sandbox_mounts(
            thread_id,
            user_id,
            extra_mounts=extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
    else:
        init_container_items = _build_lark_cli_init_containers(
            provision_lark_cli_runtime,
            provision_lark_cli_broker,
        )
        volumes = _build_volumes(
            thread_id,
            user_id=user_id,
            include_legacy_skills=include_legacy_skills,
            extra_mounts=extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
        sandbox_mounts = _build_volume_mounts(
            thread_id,
            user_id=user_id,
            include_legacy_skills=include_legacy_skills,
            extra_mounts=extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
    labels = {
        "app": "deer-flow-sandbox",
        "sandbox-id": sandbox_id,
        "app.kubernetes.io/name": "deer-flow",
        "app.kubernetes.io/component": "sandbox",
    }
    annotations: dict[str, str] | None = None
    if accepted_skill_projection is not None:
        labels["hartmesh.io/accepted-skill-profile"] = accepted_skill_projection.profile
        annotations = {
            "hartmesh.io/accepted-skill-digest": accepted_skill_projection.content_digest,
            "hartmesh.io/accepted-skill-run": accepted_skill_projection.run_id,
            "hartmesh.io/accepted-skill-generation": str(
                accepted_skill_projection.generation,
            ),
            "hartmesh.io/accepted-capability-digest": _capability_digest(
                attempt_capability or "",
            ),
        }
    elif accepted_skills_only:
        labels["hartmesh.io/accepted-skill-profile"] = "empty_only"
    pod = k8s_client.V1Pod(
        metadata=k8s_client.V1ObjectMeta(
            name=_pod_name(sandbox_id),
            namespace=K8S_NAMESPACE,
            labels=labels,
            annotations=annotations,
            owner_references=([accepted_attempt_owner] if accepted_attempt_owner is not None else None),
        ),
        spec=k8s_client.V1PodSpec(
            containers=[
                k8s_client.V1Container(
                    name="sandbox",
                    image=SANDBOX_IMAGE,
                    image_pull_policy="IfNotPresent",
                    env=([k8s_client.V1EnvVar(name="DEERFLOW_LARK_BROKER_URL", value=LARK_BROKER_URL)] if _lark_cli_broker_enabled(provision_lark_cli_broker) else None),
                    ports=[
                        k8s_client.V1ContainerPort(
                            name="http",
                            container_port=SANDBOX_CONTAINER_PORT,
                            protocol="TCP",
                        )
                    ],
                    readiness_probe=k8s_client.V1Probe(
                        http_get=k8s_client.V1HTTPGetAction(
                            path="/v1/sandbox",
                            port=SANDBOX_CONTAINER_PORT,
                        ),
                        initial_delay_seconds=5,
                        period_seconds=5,
                        timeout_seconds=3,
                        failure_threshold=3,
                    ),
                    startup_probe=k8s_client.V1Probe(
                        http_get=k8s_client.V1HTTPGetAction(
                            path="/v1/sandbox",
                            port=SANDBOX_CONTAINER_PORT,
                        ),
                        initial_delay_seconds=SANDBOX_STARTUP_PROBE_INITIAL_DELAY_SECONDS,
                        period_seconds=SANDBOX_STARTUP_PROBE_PERIOD_SECONDS,
                        timeout_seconds=SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS,
                        failure_threshold=SANDBOX_STARTUP_PROBE_FAILURE_THRESHOLD,
                    ),
                    liveness_probe=k8s_client.V1Probe(
                        http_get=k8s_client.V1HTTPGetAction(
                            path="/v1/sandbox",
                            port=SANDBOX_CONTAINER_PORT,
                        ),
                        initial_delay_seconds=SANDBOX_LIVENESS_PROBE_INITIAL_DELAY_SECONDS,
                        period_seconds=SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS,
                        timeout_seconds=SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS,
                        failure_threshold=SANDBOX_LIVENESS_PROBE_FAILURE_THRESHOLD,
                    ),
                    resources=k8s_client.V1ResourceRequirements(
                        requests={
                            "cpu": "100m",
                            "memory": "256Mi",
                            "ephemeral-storage": "500Mi",
                        },
                        limits={
                            "cpu": "1000m",
                            "memory": "1Gi",
                            "ephemeral-storage": "500Mi",
                        },
                    ),
                    volume_mounts=sandbox_mounts,
                    security_context=_restricted_container_security_context(),
                ),
                *([_accepted_gate_container()] if accepted_material else []),
                *_build_lark_cli_broker_sidecars(provision_lark_cli_broker, extra_mounts),
            ],
            init_containers=init_container_items or None,
            volumes=volumes,
            security_context=k8s_client.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=1000,
                run_as_group=1000,
                fs_group=1000,
                fs_group_change_policy="OnRootMismatch",
            ),
            affinity=(
                k8s_client.V1Affinity(
                    pod_anti_affinity=k8s_client.V1PodAntiAffinity(
                        preferred_during_scheduling_ignored_during_execution=[
                            k8s_client.V1WeightedPodAffinityTerm(
                                weight=100,
                                pod_affinity_term=k8s_client.V1PodAffinityTerm(
                                    topology_key="kubernetes.io/hostname",
                                    label_selector=k8s_client.V1LabelSelector(
                                        match_labels={
                                            "app.kubernetes.io/component": ("gateway"),
                                        },
                                    ),
                                ),
                            )
                        ],
                    ),
                )
                if accepted_material
                else None
            ),
            host_network=False,
            host_pid=False,
            host_ipc=False,
            share_process_namespace=False,
            service_account_name="default",
            automount_service_account_token=False,
            dns_policy="ClusterFirst",
            restart_policy="Always",
            runtime_class_name=SANDBOX_RUNTIME_CLASS or None,
        ),
    )
    if accepted_material:
        assert pod.metadata.annotations is not None
        pod.metadata.annotations["hartmesh.io/accepted-isolation-digest"] = _accepted_pod_isolation_digest(pod)
    return pod


def _build_service(sandbox_id: str) -> k8s_client.V1Service:
    """Construct a Service manifest for the configured access mode."""
    return k8s_client.V1Service(
        metadata=k8s_client.V1ObjectMeta(
            name=_svc_name(sandbox_id),
            namespace=K8S_NAMESPACE,
            labels={
                "app": "deer-flow-sandbox",
                "sandbox-id": sandbox_id,
                "app.kubernetes.io/name": "deer-flow",
                "app.kubernetes.io/component": "sandbox",
            },
        ),
        spec=k8s_client.V1ServiceSpec(
            type=SANDBOX_SERVICE_TYPE,
            ports=[
                k8s_client.V1ServicePort(
                    name="http",
                    port=SANDBOX_CONTAINER_PORT,
                    target_port=SANDBOX_CONTAINER_PORT,
                    protocol="TCP",
                )
            ],
            selector={
                "sandbox-id": sandbox_id,
            },
        ),
    )


def _build_accepted_network_policy(
    sandbox_id: str,
    *,
    accepted_attempt_owner: k8s_client.V1OwnerReference | None = None,
) -> k8s_client.V1NetworkPolicy:
    """Expose the capability gate only to Gateway control-plane Pods."""

    gateway_namespace_selector = k8s_client.V1LabelSelector(
        match_labels={
            "kubernetes.io/metadata.name": PROVISIONER_GATEWAY_NAMESPACE,
        },
    )

    return k8s_client.V1NetworkPolicy(
        metadata=k8s_client.V1ObjectMeta(
            name=f"sandbox-{sandbox_id}-accepted-gate",
            namespace=K8S_NAMESPACE,
            labels={
                "app": "deer-flow-sandbox",
                "sandbox-id": sandbox_id,
            },
            owner_references=([accepted_attempt_owner] if accepted_attempt_owner is not None else None),
        ),
        spec=k8s_client.V1NetworkPolicySpec(
            pod_selector=k8s_client.V1LabelSelector(
                match_labels={"sandbox-id": sandbox_id},
            ),
            policy_types=["Ingress"],
            ingress=[
                k8s_client.V1NetworkPolicyIngressRule(
                    _from=[
                        k8s_client.V1NetworkPolicyPeer(
                            namespace_selector=gateway_namespace_selector,
                            pod_selector=k8s_client.V1LabelSelector(
                                match_labels={
                                    "app.kubernetes.io/component": "gateway",
                                }
                            )
                        ),
                        k8s_client.V1NetworkPolicyPeer(
                            namespace_selector=gateway_namespace_selector,
                            pod_selector=k8s_client.V1LabelSelector(
                                match_labels={
                                    "app.kubernetes.io/component": ("provisioner"),
                                },
                            ),
                        ),
                    ],
                    ports=[
                        k8s_client.V1NetworkPolicyPort(
                            port=ACCEPTED_SKILL_GATE_PORT,
                            protocol="TCP",
                        )
                    ],
                )
            ],
        ),
    )


def _url_from_service(svc, sandbox_id: str) -> str | None:
    """Build the backend-facing sandbox URL from an already-fetched Service."""
    if SANDBOX_SERVICE_TYPE == "ClusterIP":
        return _sandbox_url(sandbox_id)

    for port in svc.spec.ports or []:
        if port.name == "http" and port.node_port:
            return _sandbox_url(sandbox_id, node_port=port.node_port)
    return None


def _sandbox_access_url(sandbox_id: str, *, tolerate_read_errors: bool = False) -> str | None:
    """Read the sandbox Service and return its backend-facing URL when ready."""
    try:
        svc = core_v1.read_namespaced_service(_svc_name(sandbox_id), K8S_NAMESPACE)
    except ApiException as exc:
        if exc.status == 404:
            return None
        if tolerate_read_errors and exc.status not in {401, 403}:
            logger.warning(
                "Transient error reading Service %s: status=%s reason=%s",
                _svc_name(sandbox_id),
                exc.status,
                exc.reason,
            )
            return None
        raise

    return _url_from_service(svc, sandbox_id)


def _get_pod_phase(sandbox_id: str) -> str:
    """Return the Pod phase (Pending / Running / Succeeded / Failed / Unknown)."""
    try:
        pod = core_v1.read_namespaced_pod(_pod_name(sandbox_id), K8S_NAMESPACE)
        return pod.status.phase or "Unknown"
    except ApiException:
        return "NotFound"


def _accepted_pod_url(pod_ip: str) -> str:
    try:
        parsed = ipaddress.ip_address(pod_ip)
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_pod_identity_invalid",
        ) from exc
    host = f"[{parsed.compressed}]" if parsed.version == 6 else parsed.compressed
    return f"http://{host}:{ACCEPTED_SKILL_GATE_PORT}"


def _fetch_verifier_receipt(
    pod_ip: str,
    capability: str,
    expected: AcceptedSkillProjection,
) -> dict[str, object]:
    """Read the verifier-authored receipt through the per-attempt gate."""

    response = None
    try:
        response = urllib3.PoolManager(retries=False).request(
            "GET",
            _accepted_pod_url(pod_ip) + "/__hartmesh/accepted-material/v2",
            headers={"Authorization": f"Bearer {capability}"},
            timeout=urllib3.Timeout(connect=2.0, read=2.0),
            preload_content=False,
            retries=False,
        )
        payload = response.read(4 * 1024 + 1)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_verifier_receipt_unavailable",
        ) from None
    finally:
        if response is not None:
            response.release_conn()
    if response.status != 200 or not payload or len(payload) > 4 * 1024:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_verifier_receipt_unavailable",
        )
    try:
        receipt = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_verifier_receipt_invalid",
        ) from None
    expected_receipt = {
        "version": 2,
        "profile": ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2,
        "snapshot_id": expected.snapshot_id,
        "content_digest": expected.content_digest,
        "file_count": expected.file_count,
        "total_bytes": expected.total_bytes,
    }
    if receipt != expected_receipt:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_verifier_receipt_mismatch",
        )
    return receipt


def _accepted_pod_response(
    sandbox_id: str,
    *,
    expected: AcceptedSkillProjection | None = None,
    expected_capability: str | None = None,
    expected_lease_uid: str | None = None,
    attempt_lease: object | None = None,
    verifier_receipt: dict[str, object] | None = None,
    pod: object | None = None,
) -> SandboxResponse | None:
    """Return the exact accepted Pod identity, never a replacement by name."""

    if pod is None:
        try:
            pod = core_v1.read_namespaced_pod(
                _pod_name(sandbox_id),
                K8S_NAMESPACE,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
    if not hasattr(pod, "metadata"):
        return None
    labels = pod.metadata.labels or {}
    if labels.get("hartmesh.io/accepted-skill-profile") != ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2:
        return None
    if expected is not None and not isinstance(expected, AcceptedSkillProjectionV2):
        raise HTTPException(
            status_code=409,
            detail="accepted_skill_projection_version_unsupported",
        )
    annotations = pod.metadata.annotations or {}
    if expected is not None and (
        annotations.get("hartmesh.io/accepted-skill-digest") != expected.content_digest
        or annotations.get("hartmesh.io/accepted-skill-run") != expected.run_id
        or annotations.get("hartmesh.io/accepted-skill-generation") != str(expected.generation)
        or (expected_capability is not None and annotations.get("hartmesh.io/accepted-capability-digest") != _capability_digest(expected_capability))
    ):
        raise HTTPException(
            status_code=409,
            detail="accepted skill sandbox identity conflict",
        )
    owners = getattr(pod.metadata, "owner_references", None) or []
    lease_owners = [owner for owner in owners if getattr(owner, "kind", None) == "Lease" and isinstance(getattr(owner, "uid", None), str)]
    if len(lease_owners) != 1:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_owner_conflict",
        )
    lease_owner = lease_owners[0]
    if expected_lease_uid is not None and lease_owner.uid != expected_lease_uid:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_owner_conflict",
        )
    pod_ip = getattr(pod.status, "pod_ip", None)
    pod_uid = getattr(pod.metadata, "uid", None)
    if not isinstance(pod_ip, str) or not pod_ip or not isinstance(pod_uid, str) or not pod_uid:
        return None
    if attempt_lease is None:
        if coordination_v1 is None:
            return None
        try:
            attempt_lease = coordination_v1.read_namespaced_lease(
                lease_owner.name,
                K8S_NAMESPACE,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
    lease_metadata = getattr(attempt_lease, "metadata", None)
    if getattr(lease_metadata, "uid", None) != lease_owner.uid or _accepted_lease_expired(attempt_lease):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_lease_invalid",
        )
    lease_annotations = dict(
        getattr(lease_metadata, "annotations", None) or {},
    )
    bound_pod_uid = lease_annotations.get("hartmesh.io/accepted-pod-uid")
    if bound_pod_uid is not None and bound_pod_uid != pod_uid:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_pod_replaced",
        )
    isolation_digest = annotations.get(
        "hartmesh.io/accepted-isolation-digest",
    )
    if not isinstance(isolation_digest, str) or isolation_digest != lease_annotations.get("hartmesh.io/accepted-isolation-digest") or _accepted_pod_isolation_digest(pod) != isolation_digest:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_pod_spec_mismatch",
        )
    if getattr(pod.status, "phase", None) != "Running":
        return None
    container_statuses = {getattr(status, "name", ""): status for status in (getattr(pod.status, "container_statuses", None) or [])}
    init_statuses = {getattr(status, "name", ""): status for status in (getattr(pod.status, "init_container_statuses", None) or [])}
    expected_images = {
        "sandbox": SANDBOX_IMAGE.rsplit("@", 1)[-1],
        "accepted-skill-gate": ACCEPTED_SKILL_RUNTIME_IMAGE.rsplit("@", 1)[-1],
        "accepted-skill-verifier": ACCEPTED_SKILL_RUNTIME_IMAGE.rsplit("@", 1)[-1],
    }
    image_ids: dict[str, str] = {}
    for name, expected_digest in expected_images.items():
        status = init_statuses.get(name) if name == "accepted-skill-verifier" else container_statuses.get(name)
        image_id = getattr(status, "image_id", None)
        if not isinstance(image_id, str) or not image_id:
            return None
        if not image_id.endswith(expected_digest):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_image_identity_mismatch",
            )
        image_ids[name] = image_id
        if name == "accepted-skill-verifier":
            terminated = getattr(getattr(status, "state", None), "terminated", None)
            if getattr(terminated, "exit_code", None) != 0:
                return None
        elif getattr(status, "ready", None) is not True:
            return None
    runtime_image_ids_digest = hashlib.sha256(
        json.dumps(
            image_ids,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ).hexdigest()
    digest = annotations.get("hartmesh.io/accepted-skill-digest")
    run_id = annotations.get("hartmesh.io/accepted-skill-run")
    generation_text = annotations.get("hartmesh.io/accepted-skill-generation")
    try:
        generation = int(generation_text)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=500,
            detail="accepted skill sandbox evidence is malformed",
        ) from None
    if not isinstance(digest, str) or not isinstance(run_id, str):
        raise HTTPException(
            status_code=500,
            detail="accepted skill sandbox evidence is malformed",
        )
    if lease_annotations.get("hartmesh.io/accepted-attempt-state") == ("materialized"):
        verifier_receipt_digest = lease_annotations.get(
            "hartmesh.io/accepted-verifier-receipt-digest",
        )
    else:
        if expected is None or expected_capability is None:
            return None
        verifier_receipt = verifier_receipt or _fetch_verifier_receipt(
            pod_ip,
            expected_capability,
            expected,
        )
        verifier_receipt_digest = hashlib.sha256(
            json.dumps(
                verifier_receipt,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        ).hexdigest()
    if not isinstance(verifier_receipt_digest, str) or re.fullmatch(r"[0-9a-f]{64}", verifier_receipt_digest) is None:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_verifier_receipt_invalid",
        )
    capability_digest = lease_annotations.get(
        "hartmesh.io/accepted-capability-digest",
    )
    if not isinstance(capability_digest, str) or re.fullmatch(r"[0-9a-f]{64}", capability_digest) is None:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_capability_invalid",
        )
    resources = _accepted_supporting_resource_evidence(
        sandbox_id,
        lease_uid=lease_owner.uid,
        projection=(expected if isinstance(expected, AcceptedSkillProjectionV2) else None),
        capability=expected_capability,
        capability_digest=capability_digest,
    )
    sandbox_image_digest = SANDBOX_IMAGE.rsplit("@sha256:", 1)[-1]
    accepted_skill_runtime_image_digest = ACCEPTED_SKILL_RUNTIME_IMAGE.rsplit(
        "@sha256:",
        1,
    )[-1]
    if any(
        re.fullmatch(r"[0-9a-f]{64}", value) is None
        for value in (
            sandbox_image_digest,
            accepted_skill_runtime_image_digest,
        )
    ):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_image_identity_mismatch",
        )
    materialization_evidence = {
        "version": 2,
        "profile": ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2,
        "attempt_id": lease_owner.name,
        "snapshot_id": digest,
        "content_digest": digest,
        "run_id": run_id,
        "generation": generation,
        "pod_uid": pod_uid,
        "pod_isolation_digest": isolation_digest,
        "lease_uid": lease_owner.uid,
        **resources,
        "sandbox_image_digest": sandbox_image_digest,
        "accepted_skill_runtime_image_digest": (
            accepted_skill_runtime_image_digest
        ),
        "runtime_image_ids_digest": runtime_image_ids_digest,
        "verifier_receipt_digest": verifier_receipt_digest,
    }
    materialization_digest = hashlib.sha256(
        json.dumps(
            materialization_evidence,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ).hexdigest()
    if lease_annotations.get("hartmesh.io/accepted-attempt-state") == ("materialized"):
        bound_fields = {
            "hartmesh.io/accepted-pod-isolation-digest": isolation_digest,
            "hartmesh.io/accepted-network-policy-uid": resources[
                "network_policy_uid"
            ],
            "hartmesh.io/accepted-network-policy-spec-digest": resources[
                "network_policy_spec_digest"
            ],
            "hartmesh.io/accepted-evidence-secret-uid": resources[
                "evidence_secret_uid"
            ],
            "hartmesh.io/accepted-evidence-secret-digest": resources[
                "evidence_secret_digest"
            ],
            "hartmesh.io/accepted-capability-secret-uid": resources[
                "capability_secret_uid"
            ],
            "hartmesh.io/accepted-capability-secret-digest": resources[
                "capability_secret_digest"
            ],
            "hartmesh.io/accepted-sandbox-image-digest": sandbox_image_digest,
            "hartmesh.io/accepted-skill-runtime-image-digest": (
                accepted_skill_runtime_image_digest
            ),
            "hartmesh.io/accepted-runtime-images-digest": runtime_image_ids_digest,
            "hartmesh.io/accepted-materialization-digest": materialization_digest,
        }
        if any(
            lease_annotations.get(key) != value
            for key, value in bound_fields.items()
        ):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_materialization_mismatch",
            )
    return SandboxResponse(
        sandbox_id=sandbox_id,
        sandbox_url=_accepted_pod_url(pod_ip),
        status=pod.status.phase or "Unknown",
        accepted_skill_material={
            **materialization_evidence,
            "materialization_evidence_digest": materialization_digest,
        },
    )


def _resource_has_owner_uid(resource: object, uid: str) -> bool:
    owners = getattr(getattr(resource, "metadata", None), "owner_references", None)
    return any(getattr(owner, "kind", None) == "Lease" and getattr(owner, "uid", None) == uid for owner in (owners or []))


def _resource_uid(resource: object, *, code: str) -> str:
    uid = getattr(getattr(resource, "metadata", None), "uid", None)
    if not isinstance(uid, str) or not uid or len(uid.encode("utf-8")) > 128:
        raise HTTPException(status_code=409, detail=code)
    return uid


def _resource_spec_digest(resource: object) -> str:
    metadata = getattr(resource, "metadata", None)
    payload = {
        "labels": _canonical_k8s_value(
            getattr(metadata, "labels", None) or {},
        ),
        "spec": _canonical_k8s_value(getattr(resource, "spec", None)),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8",
        )
    ).hexdigest()


def _secret_payload(secret: object, key: str) -> bytes:
    string_data = getattr(secret, "string_data", None) or {}
    if isinstance(string_data, dict) and isinstance(string_data.get(key), str):
        return string_data[key].encode("utf-8")
    data = getattr(secret, "data", None) or {}
    encoded = data.get(key) if isinstance(data, dict) else None
    if not isinstance(encoded, str):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_secret_conflict",
        )
    try:
        return base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_secret_conflict",
        ) from None


def _secret_matches_exact(existing: object, candidate: object, *, owner_uid: str) -> bool:
    if not _resource_has_owner_uid(existing, owner_uid):
        return False
    for attribute in ("immutable", "type"):
        if getattr(existing, attribute, None) != getattr(candidate, attribute, None):
            return False
    existing_metadata = getattr(existing, "metadata", None)
    candidate_metadata = getattr(candidate, "metadata", None)
    if (getattr(existing_metadata, "labels", None) or {}) != (getattr(candidate_metadata, "labels", None) or {}):
        return False
    if (getattr(existing_metadata, "annotations", None) or {}) != (getattr(candidate_metadata, "annotations", None) or {}):
        return False
    candidate_string_data = getattr(candidate, "string_data", None) or {}
    if not isinstance(candidate_string_data, dict) or len(candidate_string_data) != 1:
        return False
    key, expected_value = next(iter(candidate_string_data.items()))
    if not isinstance(key, str) or not isinstance(expected_value, str):
        return False
    try:
        return hmac.compare_digest(
            _secret_payload(existing, key),
            expected_value.encode("utf-8"),
        )
    except HTTPException:
        return False


def _create_secret_exact(
    secret: k8s_client.V1Secret,
    *,
    owner_uid: str,
) -> None:
    try:
        core_v1.create_namespaced_secret(K8S_NAMESPACE, secret)
    except ApiException as exc:
        if exc.status != 409:
            raise
        try:
            existing = core_v1.read_namespaced_secret(
                secret.metadata.name,
                K8S_NAMESPACE,
            )
        except ApiException as read_exc:
            raise HTTPException(
                status_code=503,
                detail="accepted_attempt_secret_unavailable",
            ) from read_exc
        if not _secret_matches_exact(existing, secret, owner_uid=owner_uid):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_secret_conflict",
            ) from exc


def _delete_accepted_secrets(
    sandbox_id: str,
    *,
    expected_owner_uid: str | None = None,
) -> None:
    if not hasattr(core_v1, "delete_namespaced_secret"):
        return
    for name in (
        _accepted_evidence_secret_name(sandbox_id),
        _accepted_capability_secret_name(sandbox_id),
    ):
        try:
            if expected_owner_uid is None:
                core_v1.delete_namespaced_secret(name, K8S_NAMESPACE)
                continue
            secret = core_v1.read_namespaced_secret(name, K8S_NAMESPACE)
            if not _resource_has_owner_uid(secret, expected_owner_uid):
                continue
            secret_uid = getattr(secret.metadata, "uid", None)
            if not isinstance(secret_uid, str) or not secret_uid:
                continue
            core_v1.delete_namespaced_secret(
                name,
                K8S_NAMESPACE,
                body=k8s_client.V1DeleteOptions(
                    preconditions=k8s_client.V1Preconditions(uid=secret_uid),
                ),
            )
        except ApiException as exc:
            if exc.status != 404:
                logger.warning(
                    "Failed to delete accepted sandbox Secret %s: %s",
                    name,
                    exc.reason,
                )


def _create_accepted_secrets(
    sandbox_id: str,
    projection: AcceptedSkillProjection,
    capability: str,
    *,
    accepted_attempt_owner: k8s_client.V1OwnerReference,
) -> None:
    evidence_json = json.dumps(
        projection.evidence_wire(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    _create_secret_exact(
        k8s_client.V1Secret(
            metadata=k8s_client.V1ObjectMeta(
                name=_accepted_evidence_secret_name(sandbox_id),
                namespace=K8S_NAMESPACE,
                labels={
                    "app": "deer-flow-sandbox",
                    "sandbox-id": sandbox_id,
                    "hartmesh.io/accepted-skill-profile": projection.profile,
                },
                annotations={
                    "hartmesh.io/accepted-evidence-digest": hashlib.sha256(
                        evidence_json.encode("utf-8"),
                    ).hexdigest(),
                },
                owner_references=[accepted_attempt_owner],
            ),
            immutable=True,
            string_data={"evidence.json": evidence_json},
            type="Opaque",
        ),
        owner_uid=accepted_attempt_owner.uid,
    )
    try:
        _create_secret_exact(
            k8s_client.V1Secret(
                metadata=k8s_client.V1ObjectMeta(
                    name=_accepted_capability_secret_name(sandbox_id),
                    namespace=K8S_NAMESPACE,
                    labels={
                        "app": "deer-flow-sandbox",
                        "sandbox-id": sandbox_id,
                    },
                    annotations={
                        "hartmesh.io/accepted-capability-digest": (_capability_digest(capability)),
                    },
                    owner_references=[accepted_attempt_owner],
                ),
                immutable=True,
                string_data={"capability": capability},
                type="Opaque",
            ),
            owner_uid=accepted_attempt_owner.uid,
        )
    except Exception:
        _delete_accepted_secrets(
            sandbox_id,
            expected_owner_uid=accepted_attempt_owner.uid,
        )
        raise


def _create_accepted_network_policy_exact(
    sandbox_id: str,
    *,
    accepted_attempt_owner: k8s_client.V1OwnerReference,
) -> None:
    policy = _build_accepted_network_policy(
        sandbox_id,
        accepted_attempt_owner=accepted_attempt_owner,
    )
    try:
        networking_v1.create_namespaced_network_policy(
            K8S_NAMESPACE,
            policy,
        )
    except ApiException as exc:
        if exc.status != 409:
            raise HTTPException(
                status_code=503,
                detail="accepted_attempt_network_policy_unavailable",
            ) from exc
        try:
            existing = networking_v1.read_namespaced_network_policy(
                policy.metadata.name,
                K8S_NAMESPACE,
            )
        except ApiException as read_exc:
            raise HTTPException(
                status_code=503,
                detail="accepted_attempt_network_policy_unavailable",
            ) from read_exc
        if not _resource_has_owner_uid(existing, accepted_attempt_owner.uid) or _resource_spec_digest(existing) != _resource_spec_digest(policy):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_network_policy_conflict",
            ) from exc


def _accepted_supporting_resource_evidence(
    sandbox_id: str,
    *,
    lease_uid: str,
    projection: AcceptedSkillProjectionV2 | None,
    capability: str | None,
    capability_digest: str,
) -> dict[str, str]:
    """Re-read and prove the exact NetworkPolicy and immutable Secrets."""

    if networking_v1 is None or core_v1 is None:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_supporting_resources_unavailable",
        )
    owner = k8s_client.V1OwnerReference(
        api_version="coordination.k8s.io/v1",
        kind="Lease",
        name=_accepted_attempt_lease_name(sandbox_id),
        uid=lease_uid,
    )
    expected_policy = _build_accepted_network_policy(
        sandbox_id,
        accepted_attempt_owner=owner,
    )
    try:
        policy = networking_v1.read_namespaced_network_policy(
            expected_policy.metadata.name,
            K8S_NAMESPACE,
        )
    except ApiException as exc:
        raise HTTPException(
            status_code=(409 if exc.status == 404 else 503),
            detail=("accepted_attempt_network_policy_missing" if exc.status == 404 else "accepted_attempt_network_policy_unavailable"),
        ) from None
    expected_policy_digest = _resource_spec_digest(expected_policy)
    if not _resource_has_owner_uid(policy, lease_uid) or _resource_spec_digest(policy) != expected_policy_digest:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_network_policy_conflict",
        )

    result = {
        "network_policy_uid": _resource_uid(
            policy,
            code="accepted_attempt_network_policy_identity_invalid",
        ),
        "network_policy_spec_digest": expected_policy_digest,
    }
    for kind in ("evidence", "capability"):
        name = (
            _accepted_evidence_secret_name(sandbox_id)
            if kind == "evidence"
            else _accepted_capability_secret_name(sandbox_id)
        )
        try:
            secret = core_v1.read_namespaced_secret(name, K8S_NAMESPACE)
        except ApiException as exc:
            raise HTTPException(
                status_code=(409 if exc.status == 404 else 503),
                detail=("accepted_attempt_secret_missing" if exc.status == 404 else "accepted_attempt_secret_unavailable"),
            ) from None
        payload_key = "evidence.json" if kind == "evidence" else "capability"
        payload = _secret_payload(secret, payload_key)
        payload_digest = hashlib.sha256(payload).hexdigest()
        metadata = getattr(secret, "metadata", None)
        expected_labels = {
            "app": "deer-flow-sandbox",
            "sandbox-id": sandbox_id,
        }
        expected_annotation_key = "hartmesh.io/accepted-capability-digest"
        expected_payload_digest = capability_digest
        if kind == "evidence":
            expected_labels["hartmesh.io/accepted-skill-profile"] = (
                ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2
            )
            expected_annotation_key = "hartmesh.io/accepted-evidence-digest"
            expected_payload_digest = payload_digest
            if projection is not None:
                expected_payload = json.dumps(
                    projection.evidence_wire(),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                if not hmac.compare_digest(payload, expected_payload):
                    raise HTTPException(
                        status_code=409,
                        detail="accepted_attempt_secret_conflict",
                    )
                expected_payload_digest = hashlib.sha256(expected_payload).hexdigest()
        elif capability is not None and not hmac.compare_digest(
            payload,
            capability.encode("utf-8"),
        ):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_secret_conflict",
            )
        if (
            not _resource_has_owner_uid(secret, lease_uid)
            or getattr(secret, "immutable", None) is not True
            or getattr(secret, "type", None) != "Opaque"
            or (getattr(metadata, "labels", None) or {}) != expected_labels
            or (getattr(metadata, "annotations", None) or {})
            != {expected_annotation_key: expected_payload_digest}
            or payload_digest != expected_payload_digest
        ):
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_secret_conflict",
            )
        result[f"{kind}_secret_uid"] = _resource_uid(
            secret,
            code="accepted_attempt_secret_identity_invalid",
        )
        result[f"{kind}_secret_digest"] = payload_digest
    return result


def _delete_accepted_network_policy(
    sandbox_id: str,
    *,
    expected_owner_uid: str | None = None,
) -> None:
    if networking_v1 is None:
        return
    name = f"sandbox-{sandbox_id}-accepted-gate"
    if expected_owner_uid is None:
        networking_v1.delete_namespaced_network_policy(
            name,
            K8S_NAMESPACE,
        )
        return
    policy = networking_v1.read_namespaced_network_policy(
        name,
        K8S_NAMESPACE,
    )
    if not _resource_has_owner_uid(policy, expected_owner_uid):
        return
    policy_uid = getattr(policy.metadata, "uid", None)
    if not isinstance(policy_uid, str) or not policy_uid:
        return
    networking_v1.delete_namespaced_network_policy(
        name,
        K8S_NAMESPACE,
        body=k8s_client.V1DeleteOptions(
            preconditions=k8s_client.V1Preconditions(uid=policy_uid),
        ),
    )


def _renew_accepted_attempt(
    sandbox_id: str,
    request: RenewAcceptedAttemptRequest,
    *,
    now: datetime | None = None,
) -> None:
    """Renew only the exact live Pod/Lease/materialization tuple."""

    if coordination_v1 is None:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_coordination_unavailable",
        )
    response = _accepted_pod_response(
        sandbox_id,
        expected_lease_uid=request.lease_uid,
    )
    if response is None or response.accepted_skill_material is None:
        raise HTTPException(status_code=404, detail="accepted_attempt_not_found")
    receipt = response.accepted_skill_material
    if receipt.get("pod_uid") != request.pod_uid or receipt.get("lease_uid") != request.lease_uid or receipt.get("materialization_evidence_digest") != request.materialization_evidence_digest:
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_fence_mismatch",
        )
    try:
        lease = coordination_v1.read_namespaced_lease(
            str(receipt["attempt_id"]),
            K8S_NAMESPACE,
        )
    except ApiException as exc:
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_lease_unavailable",
        ) from exc
    if getattr(lease.metadata, "uid", None) != request.lease_uid or _accepted_lease_expired(
        lease,
        now=now,
    ):
        raise HTTPException(
            status_code=409,
            detail="accepted_attempt_fence_mismatch",
        )
    lease.spec.renew_time = now or datetime.now(UTC)
    lease.spec.lease_duration_seconds = ACCEPTED_ATTEMPT_LEASE_SECONDS
    try:
        coordination_v1.replace_namespaced_lease(
            lease.metadata.name,
            K8S_NAMESPACE,
            lease,
        )
    except ApiException as exc:
        if exc.status == 409:
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_fence_mismatch",
            ) from exc
        raise HTTPException(
            status_code=503,
            detail="accepted_attempt_lease_unavailable",
        ) from exc


# ── API endpoints ────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Provisioner health check."""
    return {"status": "ok"}


@app.get("/ready")
def readiness():
    """Prove bounded provisioner prerequisites without conflating liveness."""

    if core_v1 is None or networking_v1 is None or coordination_v1 is None:
        raise HTTPException(status_code=503, detail="kubernetes_api_unavailable")
    tokenreview_configured = all(
        (
            PROVISIONER_AUTH_AUDIENCE,
            PROVISIONER_GATEWAY_NAMESPACE,
            PROVISIONER_GATEWAY_SERVICE_ACCOUNT,
        )
    )
    if not PROVISIONER_API_KEY and not tokenreview_configured:
        raise HTTPException(
            status_code=503,
            detail="provisioner_management_auth_unavailable",
        )
    if tokenreview_configured and authentication_v1 is None:
        raise HTTPException(
            status_code=503,
            detail="provisioner_token_review_unavailable",
        )
    if ACCEPTED_SKILL_PROJECTION_PROFILE not in {
        "",
        "disabled",
        ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V1,
        ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2,
    }:
        raise HTTPException(
            status_code=503,
            detail="accepted_skill_profile_invalid",
        )
    if ACCEPTED_SKILL_PROJECTION_PROFILE == ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V1:
        raise HTTPException(
            status_code=503,
            detail="accepted_skill_profile_v1_compatibility_only",
        )
    if ACCEPTED_SKILL_PROJECTION_PROFILE == ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2:
        _require_accepted_projection_runtime()
        try:
            claim = core_v1.read_namespaced_persistent_volume_claim(
                USERDATA_PVC_NAME,
                K8S_NAMESPACE,
            )
        except ApiException as exc:
            logger.warning(
                "accepted skill readiness could not read configured PVC: status=%s",
                exc.status,
            )
            raise HTTPException(
                status_code=503,
                detail="accepted_skill_pvc_unavailable",
            ) from None
        access_modes = getattr(getattr(claim, "spec", None), "access_modes", None)
        if not isinstance(access_modes, list) or "ReadWriteMany" not in access_modes:
            raise HTTPException(
                status_code=503,
                detail="accepted_skill_pvc_not_rwx",
            )
        if getattr(getattr(claim, "status", None), "phase", None) != "Bound":
            raise HTTPException(
                status_code=503,
                detail="accepted_skill_pvc_not_bound",
            )
    return {"status": "ready"}


@app.get("/api/capabilities")
async def capabilities():
    """Report provisioner-side capabilities the Gateway cannot infer statically.

    ``lark_cli_init_image`` / ``lark_cli_broker_image`` reflect whether a lark-cli
    init image (Pattern A) / broker image (Pattern B) is configured, which the
    Gateway surfaces as the Lark integration sandbox-runtime readiness signal so a
    green UI can't hide a chat-time ``command not found``.
    """
    return {
        "lark_cli_init_image": bool(LARK_CLI_INIT_IMAGE),
        "lark_cli_broker_image": bool(LARK_CLI_BROKER_IMAGE),
        "accepted_skill_projection_profiles": (
            [ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2]
            if ACCEPTED_SKILL_PROJECTION_PROFILE == ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2
            and bool(USERDATA_PVC_NAME)
            and re.fullmatch(
                r"[^\s@]+@sha256:[0-9a-f]{64}",
                ACCEPTED_SKILL_RUNTIME_IMAGE,
            )
            and re.fullmatch(
                r"[^\s@]+@sha256:[0-9a-f]{64}",
                SANDBOX_IMAGE,
            )
            else []
        ),
    }


@app.post("/api/sandboxes", response_model=SandboxResponse)
def create_sandbox(req: CreateSandboxRequest):
    """Create a sandbox Pod + Service for *sandbox_id*.

    If the sandbox already exists, returns the existing information
    (idempotent).
    """
    sandbox_id = req.sandbox_id
    thread_id = req.thread_id or sandbox_id
    user_id = req.user_id
    include_legacy_skills = req.include_legacy_skills
    provision_lark_cli_runtime = req.provision_lark_cli_runtime
    provision_lark_cli_broker = req.provision_lark_cli_broker
    accepted_projection = req.accepted_skill_projection
    accepted = accepted_projection is not None

    logger.info(
        "Received request to create sandbox '%s' for thread '%s' user '%s' include_legacy_skills=%s provision_lark_cli_runtime=%s provision_lark_cli_broker=%s runtime_class=%s",
        sandbox_id,
        thread_id,
        user_id,
        include_legacy_skills,
        _lark_cli_runtime_enabled(provision_lark_cli_runtime),
        _lark_cli_broker_enabled(provision_lark_cli_broker),
        _sandbox_runtime_label(),
    )

    # ── Fast path: sandbox already exists ────────────────────────────
    if accepted:
        assert accepted_projection is not None
        _require_accepted_projection_runtime()
        assert req.attempt_capability is not None
        accepted_pod = _build_pod(
            sandbox_id,
            thread_id,
            user_id=user_id,
            include_legacy_skills=include_legacy_skills,
            extra_mounts=req.extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
            accepted_skills_only=req.accepted_skills_only,
            accepted_skill_projection=accepted_projection,
            attempt_capability=req.attempt_capability,
        )
        isolation_digest = accepted_pod.metadata.annotations["hartmesh.io/accepted-isolation-digest"]
        attempt_lease = _claim_accepted_attempt(
            sandbox_id,
            accepted_projection,
            req.attempt_capability,
            isolation_digest=isolation_digest,
        )
        attempt_owner = _accepted_attempt_owner_reference(attempt_lease)
        existing_accepted = _accepted_pod_response(
            sandbox_id,
            expected=accepted_projection,
            expected_capability=req.attempt_capability,
            expected_lease_uid=attempt_owner.uid,
            attempt_lease=attempt_lease,
        )
        if existing_accepted is not None:
            receipt = existing_accepted.accepted_skill_material
            assert receipt is not None
            attempt_lease = _bind_accepted_attempt_pod_uid(
                attempt_lease,
                str(receipt["pod_uid"]),
            )
            _bind_accepted_attempt_materialization(
                attempt_lease,
                receipt,
            )
            return existing_accepted
        _create_accepted_secrets(
            sandbox_id,
            accepted_projection,
            req.attempt_capability,
            accepted_attempt_owner=attempt_owner,
        )
        _create_accepted_network_policy_exact(
            sandbox_id,
            accepted_attempt_owner=attempt_owner,
        )
        attempt_lease, create_accepted_pod = _prepare_accepted_pod_creation(attempt_lease)
        accepted_pod.metadata.owner_references = [attempt_owner]
    else:
        existing_url = _sandbox_access_url(
            sandbox_id,
            tolerate_read_errors=True,
        )
        if existing_url:
            if req.accepted_skills_only:
                existing_pod = core_v1.read_namespaced_pod(
                    _pod_name(sandbox_id),
                    K8S_NAMESPACE,
                )
                labels = getattr(existing_pod.metadata, "labels", None) or {}
                if labels.get("hartmesh.io/accepted-skill-profile") != "empty_only":
                    raise HTTPException(
                        status_code=409,
                        detail="accepted-empty sandbox identity conflict",
                    )
            return SandboxResponse(
                sandbox_id=sandbox_id,
                sandbox_url=existing_url,
                status=_get_pod_phase(sandbox_id),
            )

    # ── Create Pod ───────────────────────────────────────────────────
    try:
        if not accepted or create_accepted_pod:
            core_v1.create_namespaced_pod(
                K8S_NAMESPACE,
                (
                    accepted_pod
                    if accepted
                    else _build_pod(
                        sandbox_id,
                        thread_id,
                        user_id=user_id,
                        include_legacy_skills=include_legacy_skills,
                        extra_mounts=req.extra_mounts,
                        provision_lark_cli_runtime=provision_lark_cli_runtime,
                        provision_lark_cli_broker=provision_lark_cli_broker,
                        accepted_skills_only=req.accepted_skills_only,
                        accepted_skill_projection=accepted_projection,
                        attempt_capability=req.attempt_capability,
                        accepted_attempt_owner=(attempt_owner if accepted else None),
                    )
                ),
            )
            logger.info(f"Created Pod {_pod_name(sandbox_id)}")
        elif accepted:
            try:
                core_v1.read_namespaced_pod(
                    _pod_name(sandbox_id),
                    K8S_NAMESPACE,
                )
            except ApiException as exc:
                if exc.status == 404:
                    raise HTTPException(
                        status_code=409,
                        detail="accepted_attempt_pod_unavailable",
                    ) from None
                raise
    except ApiException as exc:
        if exc.status != 409:  # 409 = AlreadyExists
            if accepted:
                raise HTTPException(
                    status_code=503,
                    detail="accepted_attempt_pod_unavailable",
                ) from exc
            raise HTTPException(status_code=500, detail=f"Pod creation failed: {exc.reason}")

    if accepted:
        accepted_response: SandboxResponse | None = None
        for _ in range(20):
            try:
                observed_pod = core_v1.read_namespaced_pod(
                    _pod_name(sandbox_id),
                    K8S_NAMESPACE,
                )
            except ApiException as exc:
                if exc.status == 404:
                    raise HTTPException(
                        status_code=409,
                        detail="accepted_attempt_pod_unavailable",
                    ) from None
                raise
            observed_uid = getattr(
                getattr(observed_pod, "metadata", None),
                "uid",
                None,
            )
            if isinstance(observed_uid, str) and observed_uid:
                attempt_lease = _bind_accepted_attempt_pod_uid(
                    attempt_lease,
                    observed_uid,
                )
            accepted_response = _accepted_pod_response(
                sandbox_id,
                expected=accepted_projection,
                expected_capability=req.attempt_capability,
                expected_lease_uid=attempt_owner.uid,
                attempt_lease=attempt_lease,
                pod=observed_pod,
            )
            if accepted_response is not None:
                break
            time.sleep(0.5)
        if accepted_response is None:
            raise HTTPException(
                status_code=503,
                detail="accepted_attempt_pod_identity_unavailable",
            )
        receipt = accepted_response.accepted_skill_material
        assert receipt is not None
        _bind_accepted_attempt_materialization(
            attempt_lease,
            receipt,
        )
        return accepted_response

    # ── Create Service ───────────────────────────────────────────────
    try:
        core_v1.create_namespaced_service(K8S_NAMESPACE, _build_service(sandbox_id))
        logger.info(f"Created Service {_svc_name(sandbox_id)}")
    except ApiException as exc:
        if exc.status != 409:
            # Roll back the Pod on failure
            try:
                core_v1.delete_namespaced_pod(_pod_name(sandbox_id), K8S_NAMESPACE)
            except ApiException:
                pass
            raise HTTPException(status_code=500, detail=f"Service creation failed: {exc.reason}")

    # ── Wait until the Service has a usable access URL ───────────────
    sandbox_url: str | None = None
    for _ in range(20):
        sandbox_url = _sandbox_access_url(sandbox_id, tolerate_read_errors=True)
        if sandbox_url:
            break
        time.sleep(0.5)

    if not sandbox_url:
        raise HTTPException(status_code=500, detail="Service access URL was not available in time")

    return SandboxResponse(
        sandbox_id=sandbox_id,
        sandbox_url=sandbox_url,
        status=_get_pod_phase(sandbox_id),
    )


@app.delete("/api/sandboxes/{sandbox_id}")
def destroy_sandbox(
    sandbox_id: str,
    pod_uid: str | None = None,
    lease_uid: str | None = None,
):
    """Destroy a sandbox Pod + Service."""
    errors: list[str] = []

    try:
        current_pod = core_v1.read_namespaced_pod(
            _pod_name(sandbox_id),
            K8S_NAMESPACE,
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
        current_pod = None
    current_labels = getattr(getattr(current_pod, "metadata", None), "labels", None) or {}
    is_accepted_attempt = current_labels.get("hartmesh.io/accepted-skill-profile") in {
        ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V1,
        ACCEPTED_SKILL_PROFILE_RWX_VERIFIED_COPY_V2,
    }
    accepted = _accepted_pod_response(sandbox_id, pod=current_pod) if is_accepted_attempt else None
    if is_accepted_attempt:
        owners = (
            getattr(
                getattr(current_pod, "metadata", None),
                "owner_references",
                None,
            )
            or []
        )
        lease_owners = [owner for owner in owners if getattr(owner, "kind", None) == "Lease" and isinstance(getattr(owner, "uid", None), str) and isinstance(getattr(owner, "name", None), str)]
        if len(lease_owners) != 1:
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_owner_conflict",
            )
        receipt = (
            accepted.accepted_skill_material or {}
            if accepted is not None
            else {
                "attempt_id": lease_owners[0].name,
                "pod_uid": getattr(current_pod.metadata, "uid", None),
                "lease_uid": lease_owners[0].uid,
            }
        )
        if pod_uid is None or lease_uid is None or receipt.get("pod_uid") != pod_uid or receipt.get("lease_uid") != lease_uid:
            raise HTTPException(
                status_code=409,
                detail="accepted_attempt_fence_mismatch",
            )
        try:
            core_v1.delete_namespaced_pod(
                _pod_name(sandbox_id),
                K8S_NAMESPACE,
                body=k8s_client.V1DeleteOptions(
                    preconditions=k8s_client.V1Preconditions(uid=pod_uid),
                ),
            )
        except ApiException as exc:
            if exc.status != 404:
                errors.append(f"pod:{exc.status}")
        _delete_accepted_secrets(
            sandbox_id,
            expected_owner_uid=lease_uid,
        )
        try:
            _delete_accepted_network_policy(
                sandbox_id,
                expected_owner_uid=lease_uid,
            )
        except ApiException as exc:
            if exc.status != 404:
                errors.append(f"network-policy:{exc.status}")
        try:
            _delete_lease_by_exact_uid(
                str(receipt["attempt_id"]),
                lease_uid,
            )
        except ApiException as exc:
            if exc.status != 404:
                errors.append(f"lease:{exc.status}")
        if errors:
            raise HTTPException(
                status_code=503,
                detail="accepted_attempt_cleanup_incomplete",
            )
        return {"ok": True, "sandbox_id": sandbox_id}

    # Delete Service
    try:
        core_v1.delete_namespaced_service(_svc_name(sandbox_id), K8S_NAMESPACE)
        logger.info(f"Deleted Service {_svc_name(sandbox_id)}")
    except ApiException as exc:
        if exc.status != 404:
            errors.append(f"service: {exc.reason}")

    # Delete Pod
    try:
        core_v1.delete_namespaced_pod(_pod_name(sandbox_id), K8S_NAMESPACE)
        logger.info(f"Deleted Pod {_pod_name(sandbox_id)}")
    except ApiException as exc:
        if exc.status != 404:
            errors.append(f"pod: {exc.reason}")

    _delete_accepted_secrets(sandbox_id)
    try:
        _delete_accepted_network_policy(sandbox_id)
    except ApiException as exc:
        if exc.status != 404:
            errors.append(f"network-policy: {exc.reason}")

    if errors:
        raise HTTPException(status_code=500, detail=f"Partial cleanup: {', '.join(errors)}")

    return {"ok": True, "sandbox_id": sandbox_id}


@app.post("/api/sandboxes/{sandbox_id}/accepted-attempt/renew")
def renew_accepted_attempt(
    sandbox_id: str,
    request: RenewAcceptedAttemptRequest,
):
    """Renew one exact accepted attempt after the Gateway ownership fence."""

    _renew_accepted_attempt(sandbox_id, request)
    return {"ok": True, "sandbox_id": sandbox_id}


@app.get("/api/sandboxes/{sandbox_id}", response_model=SandboxResponse)
def get_sandbox(sandbox_id: str):
    """Return current status and URL for a sandbox."""
    accepted = _accepted_pod_response(sandbox_id)
    if accepted is not None:
        return accepted
    sandbox_url = _sandbox_access_url(sandbox_id)
    if not sandbox_url:
        raise HTTPException(status_code=404, detail=f"Sandbox '{sandbox_id}' not found")

    return SandboxResponse(
        sandbox_id=sandbox_id,
        sandbox_url=sandbox_url,
        status=_get_pod_phase(sandbox_id),
    )


@app.get("/api/sandboxes")
def list_sandboxes():
    """List every sandbox currently managed in the namespace."""
    try:
        services = core_v1.list_namespaced_service(
            K8S_NAMESPACE,
            label_selector="app=deer-flow-sandbox",
        )
    except ApiException as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list services: {exc.reason}")

    sandboxes: list[SandboxResponse] = []
    for svc in services.items:
        sid = (svc.metadata.labels or {}).get("sandbox-id")
        if not sid:
            continue
        sandbox_url = _url_from_service(svc, sid)
        if not sandbox_url:
            continue
        sandboxes.append(
            SandboxResponse(
                sandbox_id=sid,
                sandbox_url=sandbox_url,
                status=_get_pod_phase(sid),
            )
        )

    return {"sandboxes": sandboxes, "count": len(sandboxes)}

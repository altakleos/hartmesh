"""Remote sandbox backend — delegates Pod lifecycle to the provisioner service.

The provisioner dynamically creates per-sandbox-id Pods + NodePort Services
in k3s.  The backend accesses sandbox pods directly via ``k3s:{NodePort}``.

Architecture:
    ┌────────────┐  HTTP   ┌─────────────┐  K8s API  ┌──────────┐
    │ this file  │ ──────▸ │ provisioner │ ────────▸ │   k3s    │
    │ (backend)  │         │ :8002       │           │ :6443    │
    └────────────┘         └─────────────┘           └─────┬────┘
                                                           │ creates
                           ┌─────────────┐           ┌─────▼──────┐
                           │   backend   │ ────────▸ │  sandbox   │
                           │             │  direct   │  Pod(s)    │
                           └─────────────┘ k3s:NPort └────────────┘
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import threading
from pathlib import Path

import requests

from deerflow.runtime.user_context import get_effective_user_id
from deerflow.skills.storage import user_should_see_legacy_skills

from .backend import SandboxBackend
from .sandbox_info import AcceptedSkillMaterialReceiptV1, SandboxInfo

logger = logging.getLogger(__name__)

_PROVISIONER_EXTRA_MOUNT_PATHS = {
    "/mnt/acp-workspace",
    "/mnt/skills/custom",
    "/mnt/skills/integrations",
    "/mnt/integrations/lark-cli/config",
    "/mnt/integrations/lark-cli/data",
    "/mnt/integrations/lark-cli/runtime",
}

_LARK_CLI_RUNTIME_CONTAINER_PATH = "/mnt/integrations/lark-cli/runtime"
_LARK_CLI_CONFIG_CONTAINER_PATH = "/mnt/integrations/lark-cli/config"
_LARK_CLI_DATA_CONTAINER_PATH = "/mnt/integrations/lark-cli/data"


def _provisioner_extra_mounts_payload(
    extra_mounts: list[tuple[str, str, bool]] | None,
    *,
    provision_lark_cli_runtime: bool = False,
    provision_lark_cli_broker: bool = False,
) -> list[dict[str, object]]:
    """Return only extra mounts the provisioner knows how to recreate safely.

    When ``provision_lark_cli_runtime`` is set, the provisioner supplies the
    lark-cli runtime via an init container + emptyDir, so the runtime extra mount
    is dropped here to avoid a colliding hostPath/PVC mount at the same path. The
    per-user config/data credential mounts are still forwarded (they are mounted
    into the sandbox in Pattern A).

    When ``provision_lark_cli_broker`` is set (Pattern B, issue #4338), the
    provisioner runs a broker sidecar that holds the credentials, so the
    config/data mounts are **forwarded** (the provisioner wires them into the
    sidecar, not the sandbox) while the runtime mount is dropped. Nothing changes
    in this payload beyond keeping config/data available for the provisioner to
    place; the runtime entry is dropped in both modes.
    """
    if not extra_mounts:
        return []

    drop_runtime = provision_lark_cli_runtime or provision_lark_cli_broker

    payload: list[dict[str, object]] = []
    for host_path, container_path, read_only in extra_mounts:
        if container_path not in _PROVISIONER_EXTRA_MOUNT_PATHS:
            continue
        if drop_runtime and container_path == _LARK_CLI_RUNTIME_CONTAINER_PATH:
            continue
        payload.append(
            {
                "host_path": host_path,
                "container_path": container_path,
                "read_only": read_only,
            }
        )
    return payload


class RemoteSandboxBackend(SandboxBackend):
    """Backend that delegates sandbox lifecycle to the provisioner service.

    All Pod creation, destruction, and discovery are handled by the
    provisioner.  This backend is a thin HTTP client.

    Typical config.yaml::

        sandbox:
          use: deerflow.community.aio_sandbox:AioSandboxProvider
          provisioner_url: http://provisioner:8002
          provisioner_api_key: $PROVISIONER_API_KEY
    """

    def __init__(
        self,
        provisioner_url: str,
        api_key: str = "",
        service_account_token_file: str = "",
    ):
        """Initialize with the provisioner service URL and optional API key.

        Args:
            provisioner_url: URL of the provisioner service
                             (e.g., ``http://provisioner:8002``).
            api_key: Value sent as ``X-API-Key`` header on every request.
                     Leave empty to send no authentication header.
        """
        if api_key and service_account_token_file:
            raise ValueError(
                "provisioner API key and ServiceAccount token are mutually exclusive",
            )
        self._provisioner_url = provisioner_url.rstrip("/")
        self._api_key = api_key
        self._service_account_token_file = service_account_token_file
        self._attempt_capabilities: dict[str, str] = {}
        self._attempt_capabilities_lock = threading.Lock()

    @property
    def provisioner_url(self) -> str:
        return self._provisioner_url

    def _auth_headers(self) -> dict[str, str]:
        if self._api_key:
            return {"X-API-Key": self._api_key}
        if not self._service_account_token_file:
            return {}
        try:
            token = (
                Path(self._service_account_token_file)
                .read_text(
                    encoding="utf-8",
                )
                .strip()
            )
        except (OSError, UnicodeError) as exc:
            raise RuntimeError("provisioner_service_account_token_unavailable") from exc
        if not token or len(token.encode("utf-8")) > 16 * 1024 or any(ord(character) < 32 for character in token):
            raise RuntimeError("provisioner_service_account_token_invalid")
        return {"Authorization": f"Bearer {token}"}

    def accepted_skill_projection_ready(self) -> bool:
        """Authenticate and require exact provisioner profile advertisement."""

        try:
            readiness = requests.get(
                f"{self._provisioner_url}/ready",
                headers=self._auth_headers(),
                timeout=5,
            )
            readiness.raise_for_status()
            if readiness.json() != {"status": "ready"}:
                return False
            response = requests.get(
                f"{self._provisioner_url}/api/capabilities",
                headers=self._auth_headers(),
                timeout=5,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, RuntimeError, ValueError):
            return False
        return isinstance(payload, dict) and payload.get("accepted_skill_projection_profiles") == ["rwx_verified_copy_v1"]

    # ── SandboxBackend interface ──────────────────────────────────────────

    def create(
        self,
        thread_id: str | None,
        sandbox_id: str,
        extra_mounts: list[tuple[str, str, bool]] | None = None,
        *,
        user_id: str | None = None,
        provision_lark_cli_runtime: bool = False,
        provision_lark_cli_broker: bool = False,
        accepted_skills_only: bool = False,
        accepted_skill_binding: object | None = None,
    ) -> SandboxInfo:
        """Create a sandbox Pod + Service via the provisioner.

        Calls ``POST /api/sandboxes`` which creates a dedicated Pod +
        NodePort Service in k3s.
        """
        kwargs = {
            "user_id": user_id,
            "provision_lark_cli_runtime": provision_lark_cli_runtime,
            "provision_lark_cli_broker": provision_lark_cli_broker,
        }
        if accepted_skills_only:
            kwargs["accepted_skills_only"] = True
        if accepted_skill_binding is not None:
            kwargs["accepted_skill_binding"] = accepted_skill_binding
        return self._provisioner_create(
            thread_id,
            sandbox_id,
            extra_mounts,
            **kwargs,
        )

    def destroy(self, info: SandboxInfo) -> None:
        """Destroy a sandbox Pod + Service via the provisioner."""
        if info.accepted_skill_material is None:
            self._provisioner_destroy(info.sandbox_id)
        else:
            self._provisioner_destroy(
                info.sandbox_id,
                info.accepted_skill_material,
            )

    def is_alive(self, info: SandboxInfo) -> bool:
        """Check whether the sandbox Pod is running."""
        if info.accepted_skill_material is None:
            return self._provisioner_is_alive(info.sandbox_id)
        return self._provisioner_is_alive(info.sandbox_id, info.accepted_skill_material)

    def discover(self, sandbox_id: str) -> SandboxInfo | None:
        """Discover an existing sandbox via the provisioner.

        Calls ``GET /api/sandboxes/{sandbox_id}`` and returns info if
        the Pod exists.
        """
        return self._provisioner_discover(sandbox_id)

    def list_running(self) -> list[SandboxInfo]:
        """Return all sandboxes currently managed by the provisioner.

        Calls ``GET /api/sandboxes`` so that ``AioSandboxProvider._reconcile_orphans()``
        can adopt pods that were created by a previous process and were never
        explicitly destroyed.
        Without this, a process restart silently orphans all existing k8s Pods —
        they stay running forever because the idle checker only
        tracks in-process state.
        """
        return self._provisioner_list()

    # ── Provisioner API calls ─────────────────────────────────────────────

    def _provisioner_list(self) -> list[SandboxInfo]:
        """GET /api/sandboxes → list all running sandboxes."""
        try:
            resp = requests.get(f"{self._provisioner_url}/api/sandboxes", headers=self._auth_headers(), timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                logger.warning("Provisioner list_running returned non-dict payload: %r", type(data))
                return []

            sandboxes = data.get("sandboxes", [])
            if not isinstance(sandboxes, list):
                logger.warning("Provisioner list_running returned non-list sandboxes: %r", type(sandboxes))
                return []

            infos: list[SandboxInfo] = []
            for sandbox in sandboxes:
                if not isinstance(sandbox, dict):
                    logger.warning("Provisioner list_running entry is not a dict: %r", type(sandbox))
                    continue

                sandbox_id = sandbox.get("sandbox_id")
                sandbox_url = sandbox.get("sandbox_url")
                if isinstance(sandbox_id, str) and sandbox_id and isinstance(sandbox_url, str) and sandbox_url:
                    infos.append(SandboxInfo(sandbox_id=sandbox_id, sandbox_url=sandbox_url))

            logger.info("Provisioner list_running: %d sandbox(es) found", len(infos))
            return infos
        except requests.RequestException as exc:
            logger.warning("Provisioner list_running failed: %s", exc)
            return []

    def _provisioner_create(
        self,
        thread_id: str | None,
        sandbox_id: str,
        extra_mounts: list[tuple[str, str, bool]] | None = None,
        *,
        user_id: str | None = None,
        provision_lark_cli_runtime: bool = False,
        provision_lark_cli_broker: bool = False,
        accepted_skills_only: bool = False,
        accepted_skill_binding: object | None = None,
    ) -> SandboxInfo:
        """POST /api/sandboxes → create Pod + Service."""
        accepted_skills_only = accepted_skills_only or accepted_skill_binding is not None
        effective_user_id = user_id or get_effective_user_id()
        include_legacy_skills = user_should_see_legacy_skills(effective_user_id)
        payload = {
            "sandbox_id": sandbox_id,
            "thread_id": thread_id,
            "user_id": effective_user_id,
            "include_legacy_skills": include_legacy_skills,
            "provision_lark_cli_runtime": provision_lark_cli_runtime,
            "provision_lark_cli_broker": provision_lark_cli_broker,
        }
        if accepted_skills_only:
            payload["accepted_skills_only"] = True
        attempt_capability: str | None = None
        if accepted_skill_binding is not None:
            from deerflow.runtime.skill_projection import SkillProjectionEvidence
            from deerflow.sandbox.sandbox_provider import AcceptedSkillSandboxBindingV1

            if not isinstance(accepted_skill_binding, AcceptedSkillSandboxBindingV1):
                raise RuntimeError("accepted_skill_snapshot_binding_invalid")
            evidence = accepted_skill_binding.evidence
            if not isinstance(evidence, SkillProjectionEvidence) or evidence.snapshot_id != accepted_skill_binding.snapshot_id:
                raise RuntimeError("accepted_skill_snapshot_evidence_invalid")
            if evidence.snapshot_id is not None:
                with self._attempt_capabilities_lock:
                    attempt_capability = self._attempt_capabilities.setdefault(
                        sandbox_id,
                        secrets.token_urlsafe(32),
                    )
                payload["attempt_capability"] = attempt_capability
                payload["accepted_skill_projection"] = {
                    "profile": "rwx_verified_copy_v1",
                    "snapshot_id": evidence.snapshot_id,
                    "content_digest": evidence.content_digest,
                    "run_id": accepted_skill_binding.run_id,
                    "generation": accepted_skill_binding.generation,
                    "projections": [item.to_json() for item in evidence.projections],
                    "file_count": evidence.file_count,
                    "total_bytes": evidence.total_bytes,
                }
        provisioner_extra_mounts = _provisioner_extra_mounts_payload(
            extra_mounts,
            provision_lark_cli_runtime=provision_lark_cli_runtime,
            provision_lark_cli_broker=provision_lark_cli_broker,
        )
        if provisioner_extra_mounts:
            payload["extra_mounts"] = provisioner_extra_mounts
        try:
            resp = None
            for attempt in range(2):
                try:
                    resp = requests.post(
                        f"{self._provisioner_url}/api/sandboxes",
                        json=payload,
                        headers=self._auth_headers(),
                        timeout=30,
                    )
                    break
                except requests.RequestException:
                    if attempt:
                        raise
                    logger.warning(
                        "Provisioner create response was unavailable for %s; retrying the same fenced attempt",
                        sandbox_id,
                    )
            assert resp is not None
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"Provisioner created sandbox {sandbox_id}: sandbox_url={data['sandbox_url']}")
            receipt = None
            if attempt_capability is not None:
                raw_receipt = data.get("accepted_skill_material")
                if not isinstance(raw_receipt, dict):
                    raise RuntimeError("accepted_skill_snapshot_receipt_missing")
                receipt_fields = {
                    "version",
                    "profile",
                    "attempt_id",
                    "snapshot_id",
                    "content_digest",
                    "run_id",
                    "generation",
                    "pod_uid",
                    "lease_uid",
                    "runtime_image_ids_digest",
                    "verifier_receipt_digest",
                    "materialization_evidence_digest",
                }
                if set(raw_receipt) != receipt_fields or raw_receipt.get("version") != 1:
                    raise RuntimeError("accepted_skill_snapshot_receipt_invalid")
                materialization_wire = {key: raw_receipt[key] for key in receipt_fields if key != "materialization_evidence_digest"}
                materialization_digest = hashlib.sha256(
                    json.dumps(
                        materialization_wire,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                ).hexdigest()
                if materialization_digest != raw_receipt["materialization_evidence_digest"]:
                    raise RuntimeError("accepted_skill_snapshot_receipt_invalid")
                try:
                    receipt = AcceptedSkillMaterialReceiptV1(
                        profile=raw_receipt["profile"],
                        attempt_id=raw_receipt["attempt_id"],
                        snapshot_id=raw_receipt["snapshot_id"],
                        content_digest=raw_receipt["content_digest"],
                        run_id=raw_receipt["run_id"],
                        generation=raw_receipt["generation"],
                        pod_uid=raw_receipt["pod_uid"],
                        lease_uid=raw_receipt["lease_uid"],
                        runtime_image_ids_digest=raw_receipt["runtime_image_ids_digest"],
                        verifier_receipt_digest=raw_receipt["verifier_receipt_digest"],
                        materialization_evidence_digest=raw_receipt["materialization_evidence_digest"],
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise RuntimeError("accepted_skill_snapshot_receipt_invalid") from exc
                if (
                    receipt.profile != "rwx_verified_copy_v1"
                    or receipt.snapshot_id != accepted_skill_binding.snapshot_id
                    or receipt.content_digest != accepted_skill_binding.snapshot_id
                    or receipt.run_id != accepted_skill_binding.run_id
                    or receipt.generation != accepted_skill_binding.generation
                    or not receipt.pod_uid
                    or not receipt.attempt_id
                    or not receipt.lease_uid
                    or len(receipt.runtime_image_ids_digest) != 64
                    or len(receipt.verifier_receipt_digest) != 64
                    or len(receipt.materialization_evidence_digest) != 64
                ):
                    raise RuntimeError("accepted_skill_snapshot_receipt_mismatch")
            return SandboxInfo(
                sandbox_id=sandbox_id,
                sandbox_url=data["sandbox_url"],
                request_headers=({"Authorization": f"Bearer {attempt_capability}"} if attempt_capability is not None else {}),
                accepted_skill_material=receipt,
            )
        except requests.RequestException as exc:
            if attempt_capability is not None:
                with self._attempt_capabilities_lock:
                    self._attempt_capabilities.pop(sandbox_id, None)
            logger.error(
                "Provisioner create failed for %s: %s",
                sandbox_id,
                type(exc).__name__,
            )
            raise RuntimeError("Provisioner create failed") from exc

    def _provisioner_destroy(
        self,
        sandbox_id: str,
        receipt: AcceptedSkillMaterialReceiptV1 | None = None,
    ) -> None:
        """DELETE /api/sandboxes/{sandbox_id} → destroy Pod + Service."""
        request_kwargs: dict[str, object] = {
            "headers": self._auth_headers(),
            "timeout": 15,
        }
        if receipt is not None:
            request_kwargs["params"] = {
                "pod_uid": receipt.pod_uid,
                "lease_uid": receipt.lease_uid,
            }
        try:
            resp = requests.delete(
                f"{self._provisioner_url}/api/sandboxes/{sandbox_id}",
                **request_kwargs,
            )
            if resp.ok:
                with self._attempt_capabilities_lock:
                    self._attempt_capabilities.pop(sandbox_id, None)
                logger.info(f"Provisioner destroyed sandbox {sandbox_id}")
            else:
                logger.warning(f"Provisioner destroy returned {resp.status_code}: {resp.text}")
        except requests.RequestException as exc:
            logger.warning(f"Provisioner destroy failed for {sandbox_id}: {exc}")

    def _provisioner_is_alive(
        self,
        sandbox_id: str,
        receipt: AcceptedSkillMaterialReceiptV1 | None = None,
    ) -> bool:
        """GET /api/sandboxes/{sandbox_id} → check Pod phase."""
        try:
            resp = requests.get(
                f"{self._provisioner_url}/api/sandboxes/{sandbox_id}",
                headers=self._auth_headers(),
                timeout=10,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Provisioner health check failed for {sandbox_id}: {exc}") from exc

        if resp.status_code == 404:
            return False
        if not resp.ok:
            raise RuntimeError(f"Provisioner health check failed for {sandbox_id}: HTTP {resp.status_code} {resp.text}")

        data = resp.json()
        if receipt is not None:
            current = data.get("accepted_skill_material")
            if not isinstance(current, dict) or any(
                current.get(key) != expected
                for key, expected in (
                    ("profile", receipt.profile),
                    ("attempt_id", receipt.attempt_id),
                    ("snapshot_id", receipt.snapshot_id),
                    ("run_id", receipt.run_id),
                    ("generation", receipt.generation),
                    ("pod_uid", receipt.pod_uid),
                    ("lease_uid", receipt.lease_uid),
                    (
                        "runtime_image_ids_digest",
                        receipt.runtime_image_ids_digest,
                    ),
                    (
                        "verifier_receipt_digest",
                        receipt.verifier_receipt_digest,
                    ),
                    (
                        "materialization_evidence_digest",
                        receipt.materialization_evidence_digest,
                    ),
                )
            ):
                raise RuntimeError("accepted_skill_snapshot_pod_replaced")
        return data.get("status") == "Running"

    def renew_accepted_attempt(self, info: SandboxInfo) -> bool:
        """Renew the exact provisioner Lease for a still-owned sandbox."""

        receipt = info.accepted_skill_material
        if receipt is None:
            return True
        try:
            response = requests.post(
                f"{self._provisioner_url}/api/sandboxes/{info.sandbox_id}/accepted-attempt/renew",
                headers=self._auth_headers(),
                json={
                    "pod_uid": receipt.pod_uid,
                    "lease_uid": receipt.lease_uid,
                    "materialization_evidence_digest": (receipt.materialization_evidence_digest),
                },
                timeout=10,
            )
        except requests.RequestException:
            logger.warning(
                "accepted sandbox attempt renewal unavailable: sandbox=%s",
                info.sandbox_id,
            )
            return False
        if not response.ok:
            logger.warning(
                "accepted sandbox attempt renewal rejected: sandbox=%s status=%s",
                info.sandbox_id,
                response.status_code,
            )
            return False
        return True

    def _provisioner_discover(self, sandbox_id: str) -> SandboxInfo | None:
        """GET /api/sandboxes/{sandbox_id} → discover existing sandbox."""
        try:
            resp = requests.get(
                f"{self._provisioner_url}/api/sandboxes/{sandbox_id}",
                headers=self._auth_headers(),
                timeout=10,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            if data.get("accepted_skill_material") is not None:
                # A process restart intentionally cannot recover the ephemeral
                # per-attempt capability. Lost workers terminalize; they never
                # adopt a Pod with unprovable data-plane identity.
                return None
            return SandboxInfo(
                sandbox_id=sandbox_id,
                sandbox_url=data["sandbox_url"],
            )
        except requests.RequestException as exc:
            logger.debug(f"Provisioner discover failed for {sandbox_id}: {exc}")
            return None

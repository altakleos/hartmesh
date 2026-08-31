"""OpenSandbox control-plane port and bounded SDK boundary.

The pinned OpenSandbox API can create, discover, renew, and destroy remotes, but
it cannot atomically claim ownership. Its candidate read-only-volume and
per-command-identity surfaces are not a qualified trusted-setup boundary. The
SDK adapter exposes those gaps as stable errors instead of emulating either
guarantee in process memory.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import threading
import unicodedata
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Protocol

_OCI_DIGEST_PATTERN = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$", re.ASCII)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_MAX_LABELS = 32
_MAX_LABEL_BYTES = 128
_MAX_REMOTE_REF_BYTES = 256
_MAX_FILE_BYTES = 2 * 1024 * 1024


def _timestamp(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _bounded_text(value: object, field_name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{field_name} is invalid")
    return value


def _labels(value: Mapping[str, str]) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or len(value) > _MAX_LABELS:
        raise ValueError("OpenSandbox labels are invalid")
    copied: dict[str, str] = {}
    for key, item in value.items():
        checked_key = _bounded_text(key, "OpenSandbox label key", _MAX_LABEL_BYTES)
        checked_value = _bounded_text(item, "OpenSandbox label value", _MAX_LABEL_BYTES)
        copied[checked_key] = checked_value
    return MappingProxyType(dict(sorted(copied.items())))


def _sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _validate_upload(path: object, content: object, mode: object) -> tuple[str, bytes, int]:
    checked_path = _bounded_text(path, "OpenSandbox upload path", 512)
    parsed_path = PurePosixPath(checked_path)
    if (
        "\\" in checked_path
        or unicodedata.normalize("NFC", checked_path) != checked_path
        or parsed_path.is_absolute()
        or parsed_path.as_posix() != checked_path
        or len(parsed_path.parts) > 32
        or any(part in {"", ".", ".."} for part in parsed_path.parts)
    ):
        raise ValueError("OpenSandbox upload path is invalid")
    if not isinstance(content, bytes) or len(content) > _MAX_FILE_BYTES:
        raise ValueError("OpenSandbox upload content is invalid")
    if type(mode) is not int or mode < 0 or mode > 0o7777 or mode & 0o7222:
        raise ValueError("OpenSandbox upload mode is invalid")
    return checked_path, content, mode


def _is_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    return status == 404 or getattr(response, "status_code", None) == 404


class OpenSandboxControlPlaneError(RuntimeError):
    """A bounded, secret-free provider error with a support correlation ID."""

    def __init__(self, code: str, *, correlation_id: str | None = None) -> None:
        self.code = _bounded_text(code, "OpenSandbox error code", 96)
        self.correlation_id = _bounded_text(
            correlation_id or uuid.uuid4().hex[:16],
            "OpenSandbox correlation ID",
            64,
        )
        super().__init__(f"{self.code} (correlation_id={self.correlation_id})")


@dataclass(frozen=True, slots=True)
class RemoteSandboxSpec:
    """Secret-free create request passed to an OpenSandbox control plane."""

    image_digest: str
    labels: Mapping[str, str]
    expires_at: datetime
    environment: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.image_digest, str)
            or _OCI_DIGEST_PATTERN.fullmatch(
                self.image_digest,
            )
            is None
        ):
            raise ValueError("opensandbox_image_unpinned")
        object.__setattr__(self, "labels", _labels(self.labels))
        object.__setattr__(self, "expires_at", _timestamp(self.expires_at, "expires_at"))
        if not isinstance(self.environment, Mapping) or len(self.environment) > 64:
            raise ValueError("OpenSandbox environment is invalid")
        environment: dict[str, str] = {}
        for key, value in self.environment.items():
            environment[_bounded_text(key, "environment key", 128)] = _bounded_text(
                value,
                "environment value",
                4096,
            )
        object.__setattr__(self, "environment", MappingProxyType(environment))


@dataclass(frozen=True, slots=True)
class RemoteSandbox:
    """Validated provider response; ``reported_image_ref`` is not a resolved digest proof."""

    remote_id: str
    reported_image_ref: str
    labels: Mapping[str, str]
    expires_at: datetime | None

    def __post_init__(self) -> None:
        _bounded_text(self.remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        _bounded_text(self.reported_image_ref, "OpenSandbox reported image", 512)
        object.__setattr__(self, "labels", _labels(self.labels))
        if self.expires_at is not None:
            object.__setattr__(
                self,
                "expires_at",
                _timestamp(self.expires_at, "expires_at"),
            )


@dataclass(frozen=True, slots=True)
class ClaimResult:
    """Result of one compare-and-set ownership claim."""

    claimed: bool
    ownership_epoch: int

    def __post_init__(self) -> None:
        if type(self.claimed) is not bool:
            raise TypeError("claimed must be a boolean")
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 0:
            raise ValueError("ownership_epoch must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class SetupRequest:
    """Bounded digests passed to a trusted materialization setup action."""

    verifier_digest: str
    manifest_digest: str

    def __post_init__(self) -> None:
        _sha256(self.verifier_digest, "verifier_digest")
        _sha256(self.manifest_digest, "manifest_digest")


@dataclass(frozen=True, slots=True)
class SetupResult:
    """Canonical proof digests returned by trusted materialization setup."""

    succeeded: bool
    materialization_digest: str
    read_only_proof_digest: str

    def __post_init__(self) -> None:
        if type(self.succeeded) is not bool:
            raise TypeError("succeeded must be a boolean")
        _sha256(self.materialization_digest, "materialization_digest")
        _sha256(self.read_only_proof_digest, "read_only_proof_digest")


class OpenSandboxControlPlane(Protocol):
    """Secret-free asynchronous port for accepted OpenSandbox operations."""

    async def create(self, spec: RemoteSandboxSpec) -> RemoteSandbox: ...

    async def get(self, remote_id: str) -> RemoteSandbox | None: ...

    async def list_by_labels(
        self,
        labels: Mapping[str, str],
    ) -> Sequence[RemoteSandbox]: ...

    async def claim(
        self,
        remote_id: str,
        expected_epoch: int | None,
        owner: str,
        expires_at: datetime,
    ) -> ClaimResult: ...

    async def renew(
        self,
        remote_id: str,
        epoch: int,
        owner: str,
        expires_at: datetime,
    ) -> bool: ...

    async def destroy(self, remote_id: str, epoch: int | None = None) -> None: ...

    async def upload_file(
        self,
        remote_id: str,
        path: str,
        content: bytes,
        mode: int,
    ) -> None: ...

    async def exec_setup(
        self,
        remote_id: str,
        request: SetupRequest,
    ) -> SetupResult: ...


class _SdkDriver(Protocol):
    """Synchronous implementation detail kept behind async offload."""

    def create(self, spec: RemoteSandboxSpec) -> RemoteSandbox: ...

    def get(self, remote_id: str) -> RemoteSandbox | None: ...

    def list_by_labels(self, labels: Mapping[str, str]) -> Sequence[RemoteSandbox]: ...

    def destroy(self, remote_id: str) -> None: ...

    def upload_file(self, remote_id: str, path: str, content: bytes, mode: int) -> None: ...


class _OpenSandboxSdkDriver:
    """Exact OpenSandbox 0.1.15 sync SDK integration."""

    def __init__(
        self,
        *,
        api_key: str | None,
        domain: str | None,
        protocol: str,
        request_timeout_seconds: float,
        ready_timeout_seconds: float,
        use_server_proxy: bool,
    ) -> None:
        self._api_key = api_key
        self._domain = domain
        self._protocol = protocol
        self._request_timeout = timedelta(seconds=request_timeout_seconds)
        self._ready_timeout = timedelta(seconds=ready_timeout_seconds)
        self._use_server_proxy = use_server_proxy

    def _config(self) -> Any:
        try:
            from opensandbox.config.connection_sync import ConnectionConfigSync
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("opensandbox_sdk_unavailable") from exc
        return ConnectionConfigSync(
            api_key=self._api_key,
            domain=self._domain,
            protocol=self._protocol,
            request_timeout=self._request_timeout,
            use_server_proxy=self._use_server_proxy,
        )

    @staticmethod
    def _remote(info: Any) -> RemoteSandbox:
        image = getattr(getattr(info, "image", None), "image", None)
        return RemoteSandbox(
            remote_id=getattr(info, "id", None),
            reported_image_ref=image,
            labels=getattr(info, "metadata", None) or {},
            expires_at=getattr(info, "expires_at", None),
        )

    def _service(self, config: Any) -> Any:
        from opensandbox.sync.adapters.factory import AdapterFactorySync

        return AdapterFactorySync(config.with_transport_if_missing()).create_sandbox_service()

    def create(self, spec: RemoteSandboxSpec) -> RemoteSandbox:
        from opensandbox import SandboxSync

        config = self._config()
        sandbox = None
        try:
            lifetime = max(spec.expires_at - datetime.now(UTC), timedelta(seconds=1))
            sandbox = SandboxSync.create(
                spec.image_digest,
                timeout=lifetime,
                ready_timeout=self._ready_timeout,
                env=dict(spec.environment) or None,
                metadata=dict(spec.labels),
                connection_config=config,
            )
            try:
                remote = self._remote(sandbox.get_info())
                if remote.reported_image_ref != spec.image_digest or any(remote.labels.get(key) != value for key, value in spec.labels.items()):
                    raise RuntimeError("opensandbox_create_response_mismatch")
                return remote
            except Exception:
                try:
                    sandbox.destroy()
                except Exception:
                    pass
                raise
        finally:
            if sandbox is not None:
                sandbox.close()
            else:
                config.close_transport_if_owned()

    def get(self, remote_id: str) -> RemoteSandbox | None:
        config = self._config()
        try:
            return self._remote(self._service(config).get_sandbox_info(remote_id))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        finally:
            config.close_transport_if_owned()

    def list_by_labels(self, labels: Mapping[str, str]) -> Sequence[RemoteSandbox]:
        from opensandbox.models.sandboxes import SandboxFilter

        config = self._config()
        try:
            service = self._service(config)
            page = 1
            found: list[RemoteSandbox] = []
            while True:
                result = service.list_sandboxes(
                    SandboxFilter(metadata=dict(labels), page=page, page_size=100),
                )
                found.extend(self._remote(info) for info in result.sandbox_infos)
                if not result.pagination.has_next_page:
                    break
                page += 1
                if page > 100:
                    raise RuntimeError("opensandbox_list_pagination_unbounded")
            return tuple(remote for remote in found if all(remote.labels.get(key) == value for key, value in labels.items()))
        finally:
            config.close_transport_if_owned()

    def destroy(self, remote_id: str) -> None:
        config = self._config()
        try:
            try:
                self._service(config).kill_sandbox(remote_id)
            except Exception as exc:
                if not _is_not_found(exc):
                    raise
        finally:
            config.close_transport_if_owned()

    def upload_file(self, remote_id: str, path: str, content: bytes, mode: int) -> None:
        from opensandbox import SandboxSync

        config = self._config()
        sandbox = None
        try:
            sandbox = SandboxSync.connect(
                remote_id,
                connection_config=config,
                connect_timeout=self._ready_timeout,
            )
            sandbox.files.write_file(path, content, mode=mode)
        finally:
            if sandbox is not None:
                sandbox.close()
            else:
                config.close_transport_if_owned()


class OpenSandboxSdkControlPlane:
    """Async, bounded facade for the pinned synchronous SDK.

    Atomic claim/epoch renewal and trusted setup deliberately remain
    unsupported.  Ordinary create/get/list/destroy/upload calls are useful for
    the feasibility probe and future upstream qualification work only.
    """

    def __init__(
        self,
        *,
        driver: _SdkDriver | Any | None = None,
        api_key: str | None = None,
        domain: str | None = None,
        protocol: str = "http",
        request_timeout_seconds: float = 30,
        ready_timeout_seconds: float = 30,
        use_server_proxy: bool = False,
        call_timeout_seconds: float = 30,
        max_attempts: int = 2,
    ) -> None:
        if call_timeout_seconds <= 0 or call_timeout_seconds > 300:
            raise ValueError("call_timeout_seconds must be between zero and 300")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 3:
            raise ValueError("max_attempts must be between one and three")
        self._driver = driver or _OpenSandboxSdkDriver(
            api_key=api_key,
            domain=domain,
            protocol=protocol,
            request_timeout_seconds=request_timeout_seconds,
            ready_timeout_seconds=ready_timeout_seconds,
            use_server_proxy=use_server_proxy,
        )
        self._call_timeout_seconds = call_timeout_seconds
        self._max_attempts = max_attempts

    async def _invoke(
        self,
        method: str,
        *args: object,
        retry_safe: bool,
    ) -> Any:
        correlation_id = uuid.uuid4().hex[:16]
        attempts = self._max_attempts if retry_safe else 1
        operation = getattr(self._driver, method, None)
        if not callable(operation):
            raise OpenSandboxControlPlaneError(
                "opensandbox_control_plane_unavailable",
                correlation_id=correlation_id,
            )
        for attempt in range(attempts):
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(operation, *args),
                    timeout=self._call_timeout_seconds,
                )
            except TimeoutError:
                if attempt + 1 == attempts:
                    raise OpenSandboxControlPlaneError(
                        "opensandbox_control_plane_timeout",
                        correlation_id=correlation_id,
                    ) from None
            except Exception:
                if attempt + 1 == attempts:
                    raise OpenSandboxControlPlaneError(
                        "opensandbox_control_plane_unavailable",
                        correlation_id=correlation_id,
                    ) from None
        raise AssertionError("unreachable")

    async def create(self, spec: RemoteSandboxSpec) -> RemoteSandbox:
        if not isinstance(spec, RemoteSandboxSpec):
            raise TypeError("spec must be RemoteSandboxSpec")
        remote = await self._invoke("create", spec, retry_safe=False)
        if not isinstance(remote, RemoteSandbox):
            raise OpenSandboxControlPlaneError("opensandbox_response_invalid")
        if remote.reported_image_ref != spec.image_digest:
            try:
                await self._invoke("destroy", remote.remote_id, retry_safe=True)
            except OpenSandboxControlPlaneError:
                pass
            raise OpenSandboxControlPlaneError("opensandbox_image_digest_mismatch")
        if any(remote.labels.get(key) != value for key, value in spec.labels.items()):
            try:
                await self._invoke("destroy", remote.remote_id, retry_safe=True)
            except OpenSandboxControlPlaneError:
                pass
            raise OpenSandboxControlPlaneError("opensandbox_response_invalid")
        return remote

    async def get(self, remote_id: str) -> RemoteSandbox | None:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        remote = await self._invoke("get", remote_id, retry_safe=True)
        if remote is not None and not isinstance(remote, RemoteSandbox):
            raise OpenSandboxControlPlaneError("opensandbox_response_invalid")
        return remote

    async def list_by_labels(
        self,
        labels: Mapping[str, str],
    ) -> Sequence[RemoteSandbox]:
        checked = _labels(labels)
        remotes = await self._invoke("list_by_labels", checked, retry_safe=True)
        if not isinstance(remotes, Sequence) or isinstance(remotes, (str, bytes, bytearray)) or any(not isinstance(remote, RemoteSandbox) for remote in remotes):
            raise OpenSandboxControlPlaneError("opensandbox_response_invalid")
        exact = tuple(remote for remote in remotes if all(remote.labels.get(key) == value for key, value in checked.items()))
        return tuple(sorted(exact, key=lambda remote: remote.remote_id))

    async def claim(
        self,
        remote_id: str,
        expected_epoch: int | None,
        owner: str,
        expires_at: datetime,
    ) -> ClaimResult:
        del remote_id, expected_epoch, owner, expires_at
        raise OpenSandboxControlPlaneError(
            "opensandbox_accepted_claim_cas_unsupported",
        )

    async def renew(
        self,
        remote_id: str,
        epoch: int,
        owner: str,
        expires_at: datetime,
    ) -> bool:
        del remote_id, epoch, owner, expires_at
        raise OpenSandboxControlPlaneError(
            "opensandbox_accepted_claim_cas_unsupported",
        )

    async def destroy(self, remote_id: str, epoch: int | None = None) -> None:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        if epoch is not None:
            raise OpenSandboxControlPlaneError(
                "opensandbox_accepted_claim_cas_unsupported",
            )
        await self._invoke("destroy", remote_id, retry_safe=True)

    async def upload_file(
        self,
        remote_id: str,
        path: str,
        content: bytes,
        mode: int,
    ) -> None:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        checked_path, checked_content, checked_mode = _validate_upload(
            path,
            content,
            mode,
        )
        await self._invoke(
            "upload_file",
            remote_id,
            checked_path,
            checked_content,
            checked_mode,
            retry_safe=False,
        )

    async def exec_setup(
        self,
        remote_id: str,
        request: SetupRequest,
    ) -> SetupResult:
        del remote_id, request
        raise OpenSandboxControlPlaneError("opensandbox_trusted_setup_unsupported")


@dataclass(slots=True)
class _FakeRecord:
    remote: RemoteSandbox
    epoch: int = 0
    owner: str | None = None
    files: dict[str, tuple[bytes, int]] = field(default_factory=dict)


class StatefulOpenSandboxControlPlane:
    """Stateful contract fake that models CAS; never production evidence."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, _FakeRecord] = {}
        self._next_id = 1

    async def create(self, spec: RemoteSandboxSpec) -> RemoteSandbox:
        if not isinstance(spec, RemoteSandboxSpec):
            raise TypeError("spec must be RemoteSandboxSpec")
        with self._lock:
            remote_id = f"fake-opensandbox-{self._next_id}"
            self._next_id += 1
            remote = RemoteSandbox(
                remote_id=remote_id,
                reported_image_ref=spec.image_digest,
                labels=spec.labels,
                expires_at=spec.expires_at,
            )
            self._records[remote_id] = _FakeRecord(remote=remote)
            return remote

    async def get(self, remote_id: str) -> RemoteSandbox | None:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        with self._lock:
            record = self._records.get(remote_id)
            return None if record is None else record.remote

    async def list_by_labels(
        self,
        labels: Mapping[str, str],
    ) -> Sequence[RemoteSandbox]:
        checked = _labels(labels)
        with self._lock:
            return tuple(record.remote for _, record in sorted(self._records.items()) if all(record.remote.labels.get(key) == value for key, value in checked.items()))

    async def claim(
        self,
        remote_id: str,
        expected_epoch: int | None,
        owner: str,
        expires_at: datetime,
    ) -> ClaimResult:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        _bounded_text(owner, "owner", 256)
        _timestamp(expires_at, "expires_at")
        with self._lock:
            record = self._records.get(remote_id)
            if record is None:
                raise OpenSandboxControlPlaneError("opensandbox_remote_missing")
            if expected_epoch is None:
                if record.epoch != 0 or record.owner is not None:
                    return ClaimResult(
                        claimed=False,
                        ownership_epoch=record.epoch,
                    )
                expected_epoch = 0
            if type(expected_epoch) is not int or expected_epoch < 0:
                raise ValueError("expected_epoch must be a non-negative integer")
            if record.epoch != expected_epoch:
                return ClaimResult(claimed=False, ownership_epoch=record.epoch)
            record.epoch += 1
            record.owner = owner
            record.remote = RemoteSandbox(
                remote_id=record.remote.remote_id,
                reported_image_ref=record.remote.reported_image_ref,
                labels=record.remote.labels,
                expires_at=expires_at,
            )
            return ClaimResult(claimed=True, ownership_epoch=record.epoch)

    async def renew(
        self,
        remote_id: str,
        epoch: int,
        owner: str,
        expires_at: datetime,
    ) -> bool:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        if type(epoch) is not int or epoch < 0:
            raise ValueError("epoch must be a non-negative integer")
        _bounded_text(owner, "owner", 256)
        _timestamp(expires_at, "expires_at")
        with self._lock:
            record = self._records.get(remote_id)
            if record is None or record.epoch != epoch or record.owner != owner:
                return False
            record.remote = RemoteSandbox(
                remote_id=record.remote.remote_id,
                reported_image_ref=record.remote.reported_image_ref,
                labels=record.remote.labels,
                expires_at=expires_at,
            )
            return True

    async def destroy(self, remote_id: str, epoch: int | None = None) -> None:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        if epoch is not None and (type(epoch) is not int or epoch < 0):
            raise ValueError("epoch must be a non-negative integer")
        with self._lock:
            record = self._records.get(remote_id)
            if record is None:
                return
            if epoch is not None and epoch != record.epoch:
                return
            self._records.pop(remote_id, None)

    async def upload_file(
        self,
        remote_id: str,
        path: str,
        content: bytes,
        mode: int,
    ) -> None:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        checked_path, checked_content, checked_mode = _validate_upload(
            path,
            content,
            mode,
        )
        with self._lock:
            record = self._records.get(remote_id)
            if record is None:
                raise OpenSandboxControlPlaneError("opensandbox_remote_missing")
            record.files[checked_path] = (bytes(checked_content), checked_mode)

    async def exec_setup(
        self,
        remote_id: str,
        request: SetupRequest,
    ) -> SetupResult:
        _bounded_text(remote_id, "OpenSandbox remote_id", _MAX_REMOTE_REF_BYTES)
        if not isinstance(request, SetupRequest):
            raise TypeError("request must be SetupRequest")
        with self._lock:
            record = self._records.get(remote_id)
            if record is None:
                raise OpenSandboxControlPlaneError("opensandbox_remote_missing")
            materialization = hashlib.sha256(
                b"".join(path.encode("utf-8") + content + mode.to_bytes(2, "big") for path, (content, mode) in sorted(record.files.items())) + request.manifest_digest.encode("ascii"),
            ).hexdigest()
            read_only = hashlib.sha256(
                (remote_id + str(record.epoch) + request.verifier_digest + materialization).encode("utf-8"),
            ).hexdigest()
            return SetupResult(
                succeeded=True,
                materialization_digest=materialization,
                read_only_proof_digest=read_only,
            )


__all__ = [
    "ClaimResult",
    "OpenSandboxControlPlane",
    "OpenSandboxControlPlaneError",
    "OpenSandboxSdkControlPlane",
    "RemoteSandbox",
    "RemoteSandboxSpec",
    "SetupRequest",
    "SetupResult",
    "StatefulOpenSandboxControlPlane",
]

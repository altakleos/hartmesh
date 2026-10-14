"""Private owning-loop startup coordination; Gateway installation is separate."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable
from deerflow.utils.file_io import await_drained

STARTUP_BUDGET_SECONDS = 120.0


@dataclass(frozen=True, eq=False)
class NativeAccountingProof:
    """Retain the host collector's exact census and producer-drain validation.

    Capture performs no census and grants no completeness by itself. The trusted
    collector must supply a memory-only validator of its complete retained
    census and drained producer identities. It runs under the provider lock;
    it must neither acquire that lock again nor do SQL, IO or a loop bridge.
    """

    provider: object
    permit: object
    owner: object
    bridge: object
    evidence: object
    evidence_revision: int
    validate_locked: Callable[[], None]
    resident_revision: int

    @classmethod
    def capture(cls, provider, permit, *, validate_locked):
        if not callable(validate_locked):
            raise TypeError("Native accounting requires its retained census validator")
        result = None

        def capture():
            nonlocal result
            provider._require_bootstrap_accounting_locked(permit)
            owner, bridge = provider._pool_runtime_owner, provider._native_creation
            if bridge.evidence is not owner.native_create_evidence:
                raise StorageRuntimeUnavailable("Native accounting evidence belongs to another owner")
            result = cls(provider, permit, owner, bridge, bridge.evidence, bridge.evidence.teardown_revision, validate_locked, bridge.evidence.resident_revision)

        provider.external_changed(capture)
        return result

    def require_current_locked(self, provider, permit):
        if (
            self.provider is not provider
            or self.permit is not permit
            or self.owner is not provider._pool_runtime_owner
            or self.bridge is not provider._native_creation
            or self.evidence is not self.bridge.evidence
            or self.evidence is not self.owner.native_create_evidence
            or self.evidence_revision != self.evidence.teardown_revision
            or self.resident_revision != self.evidence.resident_revision
        ):
            raise StorageRuntimeUnavailable("Native accounting lost its exact retained evidence")
        provider._require_bootstrap_accounting_locked(permit)
        if self.validate_locked() is not None:
            raise StorageRuntimeUnavailable("Native census validation must prove completeness or raise")


@dataclass(frozen=True)
class BootstrapResult:
    workspace_qualified: bool


class ResidentBootstrapCoordinator:
    """Own one initializer, one populated registry and one final drain task.

    Callbacks are private trusted host adapters, never client/plugin input.
    Reconciliation must inventory all durable runs without granting lease
    takeover authority. Its boolean reports workspace qualification only;
    complete capacity accounting requires the separate exact native proof.
    No application installs this foundation automatically.
    """

    def __init__(
        self,
        *,
        registry: ResidentRegistry,
        admission: ProviderAdmission,
        entered: float,
        drain_producers: Callable[[], Awaitable[None]],
        account_native: Callable[[object], Awaitable[NativeAccountingProof]],
        reconcile: Callable[[], Awaitable[bool]],
        drain_workers: Callable[[], Awaitable[None]],
        clock: Callable[[], float] = time.monotonic,
    ):
        if not isinstance(registry, ResidentRegistry) or not isinstance(admission, ProviderAdmission) or not math.isfinite(entered):
            raise ValueError("Bootstrap requires an exact registry, lifecycle and startup time")
        self.registry, self.admission, self.clock = registry, admission, clock
        self.provider, self.attachments = registry.provider, registry.attachments
        self._identity = registry, self.provider, self.attachments, admission
        self.loop = asyncio.get_running_loop()
        if registry.loop is not self.loop:
            raise RuntimeError("Bootstrap requires the registry's owning loop")
        self.deadline = entered + STARTUP_BUDGET_SECONDS
        self.drain_producers, self.account_native, self.reconcile, self.drain_workers = drain_producers, account_native, reconcile, drain_workers
        self.task: asyncio.Task | None = None
        self.result: BootstrapResult | None = None
        self.last_error: BaseException | None = None
        self._closing = False
        self._close_task: asyncio.Task | None = None
        self.provider.external_changed(self._require_bound_closed_locked)

    def _owning_loop(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("Bootstrap decisions require their owning loop")

    def _require_bound_locked(self):
        registry, provider, attachments, admission = self._identity
        if (
            self.registry is not registry
            or self.provider is not provider
            or self.attachments is not attachments
            or self.admission is not admission
            or registry.provider is not provider
            or registry.attachments is not attachments
            or registry.loop is not self.loop
            or getattr(provider, "_runtime_admission", None) is not admission
            or getattr(provider, "_shutdown_called", False)
        ):
            raise SandboxRuntimeAdmissionError("Bootstrap runtime identity changed")

    def _require_bound_closed_locked(self, *, revision=None):
        self._require_bound_locked()
        if self._closing:
            raise SandboxRuntimeAdmissionError("Bootstrap runtime is closing")
        return self.admission.require_closed(self.provider, revision=revision)

    @staticmethod
    def _observe(task):
        if not task.cancelled():
            task.exception()

    async def _initialize(self):
        revision = None

        def capture():
            nonlocal revision
            revision = self._require_bound_closed_locked()

        self.provider.external_changed(capture)
        restore = self.registry.begin_restore()
        permit = None
        try:
            await self.drain_producers()
            self.provider.external_changed(lambda: self._require_bound_closed_locked(revision=revision))
            claims = await self.attachments.retained_claims()
            # This method owns the provider lock itself; never nest it under
            # external_changed. Admission stays closed during restoration.
            self.registry.finish_restore(restore, claims)
            restore = None
            permit = await run_sync_lifecycle_operation(self.provider._begin_bootstrap_accounting, self.admission)
            proof = await self.account_native(permit)
            if not isinstance(proof, NativeAccountingProof):
                raise TypeError("Bootstrap requires retained native accounting evidence")
            qualified = await self.reconcile()
            if type(qualified) is not bool:
                raise TypeError("Workspace qualification requires an explicit result")

            def publish():
                self._require_bound_closed_locked(revision=revision)
                if not self.registry.restoration_complete:
                    raise StorageRuntimeUnavailable("Resident restoration is incomplete")
                proof.require_current_locked(self.provider, permit)
                # Validate the registry's memory count before installing either
                # callback. Physical and SQL effects have already drained.
                self.registry.external_slots()
                self.provider.external_slots = self.registry.external_slots
                self.provider.external_native_claims = self.registry.external_native_claims
                self.provider.evict_external = self.registry.evict_external
                self.admission.open(self.provider)

            self.provider.external_changed(publish)
            return BootstrapResult(qualified)
        except BaseException:
            self._close_admission()
            raise
        finally:
            try:
                if restore is not None:
                    self._identity[0].abort_restore(restore)
            finally:
                if permit is not None:
                    self._identity[1]._end_bootstrap_accounting(permit)

    async def poll(self, *, timeout: float | None = None) -> BootstrapResult | None:
        """Wait within the original startup deadline, retaining unfinished work.

        An explicit timeout belongs to a later host retry pass. Default waiting
        never renews startup's absolute budget. Zero still starts one owned
        initializer for the host's subsequent maintenance pass to consume.
        """
        self._owning_loop()
        if self._closing:
            return None
        if self.result is not None:

            def ready():
                self._require_bound_locked()
                self.admission.require(self.provider)

            self.provider.external_changed(ready)
            return self.result
        if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
            raise ValueError("Bootstrap polling requires a bounded nonnegative timeout")
        if self.task is None:
            self.task = asyncio.create_task(self._initialize(), name="resident-bootstrap")
            self.task.add_done_callback(self._observe)
        task = self.task
        remaining = max(0, self.deadline - self.clock()) if timeout is None else timeout
        done, _pending = await asyncio.wait({task}, timeout=remaining)
        if not done:
            return None
        # Concurrent waiters consume one outcome. None of them can replace an
        # initializer whose result another waiter has already harvested.
        if self.task is not task:
            return self.result
        self.task = None
        try:
            result = task.result()
        except BaseException as exc:
            self.last_error = exc
            if isinstance(exc, asyncio.CancelledError):
                raise
            if not isinstance(exc, Exception):
                raise
            return None
        self.last_error = None
        if not self._closing:
            self.result = result
        return self.result

    def invalidate(self):
        """Close before an explicit recheck; retain callbacks and all charges."""
        self._owning_loop()
        if self.task is not None:
            raise RuntimeError("Bootstrap result is still owned and must be consumed first")
        self._close_admission()
        self.result = None

    def _close_admission(self, *, seal=False):
        _registry, provider, _attachments, admission = self._identity
        provider.external_changed(admission.seal if seal else admission.close)

    async def _drain(self):
        errors = []
        try:
            await self.drain_workers()
        except BaseException as exc:
            errors.append(exc)
        task = self.task
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
            # Initializer failure after sealing is an expected refusal. The
            # observer consumes it; its retained accounting is never removed.
            if self.task is task:
                self.task = None
        try:
            await self._identity[0].close()
        except BaseException as exc:
            errors.append(exc)
        if errors:
            raise BaseExceptionGroup("Resident bootstrap drainage failed", errors)

    async def close(self):
        """Seal first and share drainage across cancellation/repeated callers.

        Provider, native owner, storage and SQL remain usable until this returns;
        their closure belongs to the installing host after this boundary.
        """
        self._owning_loop()
        if self._close_task is None:
            self._closing = True
            self.result = None
            self._close_admission(seal=True)
            self._close_task = asyncio.create_task(self._drain(), name="resident-bootstrap-close")
            self._close_task.add_done_callback(self._observe)

        async def join():
            return await asyncio.shield(self._close_task)

        await await_drained(join())

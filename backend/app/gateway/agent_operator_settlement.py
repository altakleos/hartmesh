"""Exact host settlement; no Gateway workers, arbitrary plugin load or bulk pass."""

import os
from pathlib import PurePosixPath

from app.gateway.agent_operator_contract import CommandRefused
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.service import SpaceOperationPending
from deerflow.utils.file_io import await_drained, run_file_io


def _describe(files, candidate, binding):
    from app.gateway.agent_operator_services import inspect_operator_volume
    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX, USER_FILES_VIRTUAL_PREFIX
    from deerflow.spaces.snapshots import MAX_ARCHIVE_ENTRIES
    from deerflow.spaces.workflows import WorkflowReceipt, probe

    volume, _used, _empty = inspect_operator_volume(files, binding)
    probe(volume)
    request = candidate.request
    if not isinstance(request, dict):
        raise ValueError("The pending file operation request is invalid")
    namespace = request.get("namespace")
    folder = {"hm.shared": "shared", "hm.my-files": "files", "hm.projects": "projects"}.get(namespace, "")
    receipt = None
    with volume.filesystem() as filesystem:
        observation = WorkflowReceipt(volume, candidate.operation_id, request, control_fd=filesystem._control_fd)
        try:
            observation.load()
            receipt = observation.value
        except FileNotFoundError:
            pass
        except (OSError, ValueError):
            pass  # Retain malformed evidence; discard needs no receipt assertion.
        path = request.get("path")
        if namespace == "hm.shared" and receipt is not None:
            facts = receipt.get("facts", {})
            fact = facts.get("publication") or facts.get("removal")
            path = fact.get("path") if isinstance(fact, dict) else None
        if isinstance(path, str):
            for prefix, relative in ((SHARED_VIRTUAL_PREFIX, "shared"), (USER_FILES_VIRTUAL_PREFIX, "files")):
                if path.startswith(prefix + "/"):
                    path = relative + path[len(prefix) :]
                    break
            folder = PurePosixPath(path).parent.as_posix()
            folder = "" if folder == "." else folder
        names = []
        try:
            descriptor = filesystem._open(folder, os.O_RDONLY | os.O_DIRECTORY, allow_root=True, follow_symlinks=False)
        except FileNotFoundError:
            descriptor = None
        try:
            if descriptor is not None:
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        if len(names) >= MAX_ARCHIVE_ENTRIES:
                            raise ValueError("Target folder exceeds the supported entry count")
                        names.append(entry.name)
        finally:
            if descriptor is not None:
                os.close(descriptor)
    return receipt, sorted(names), getattr(volume.spec, "kind", None)


def _settlers(files):
    """Use first-party host recovery callbacks without starting plugin services."""
    from deerflow.features.plugins import FeatureService
    from deerflow.spaces.facade import HostStorageProvider

    provider = HostStorageProvider(lambda: files)
    for namespace in ("hm.my-files", "hm.projects", "hm.shared"):
        service = FeatureService(namespace, namespace, None, host_written=namespace == "hm.shared")
        service.provider, service.healthy = provider, True
        files.workflow_settlers[namespace] = service.settle_workflow


class OperatorSettlement:
    async def _settle(self, *, space_id, operation_id, discard=False):
        from deerflow.spaces.maintenance import settle_file_operation

        adapter, host = None, None
        if discard:
            await self._require_daemon()
            adapter, host = self.adapter, self.adapter.host_id
        async with self.files.registry._sf() as session:
            candidate = await session.get(SpaceFileOperationRow, (space_id, operation_id))
            if candidate is None:
                raise CommandRefused(f"no file operation {operation_id} on space {space_id}")
            if candidate.phase != "pending":
                raise CommandRefused(f"file operation {operation_id} is already {candidate.phase}")
            space = await session.get(SpaceRow, space_id)
            binding = await session.get(SpaceBackingRow, space.backing_handle) if space else None
        try:
            if binding is None or binding.space_id != space_id:
                raise BackingUnavailable("The resource has no qualified filesystem binding")
            receipt, names, kind = await run_file_io(_describe, self.files, candidate, binding)
        except SpaceOperationPending:
            raise CommandRefused(f"another workflow holds the lock of space {space_id}; run the command again in a moment") from None
        except BackingUnavailable as exc:
            raise CommandRefused(f"space {space_id} is unverified: {exc}; see storage-init verify") from None
        request = candidate.request
        fields = dict(space_id=space_id, operation_id=operation_id, kind=kind, namespace=request.get("namespace"), action=request.get("action"), on_behalf_of=request.get("on_behalf_of"), request=request, receipt=receipt, files=names)
        _settlers(self.files)

        async def perform():
            detail = "the exact pending workflow could not be confirmed"

            async def host_current():
                if self.adapter is not adapter or adapter.host_id != host:
                    raise RuntimeError("The operator's Docker host changed before discard")
                await run_file_io(adapter._same_host)

            try:
                await settle_file_operation(self.files, candidate, discard=discard, raise_errors=True, operator_host_id=host, discard_admission=host_current if discard else None)
            except SpaceOperationPending:
                raise CommandRefused(f"another workflow holds the lock of space {space_id}; run the command again in a moment") from None
            except Exception as exc:
                detail = str(exc)
            async with self.files.registry._sf() as session:
                current = await session.get(SpaceFileOperationRow, (space_id, operation_id))
            if current is None or current.phase == "pending":
                return self._answer(
                    "settle", f"publication {operation_id} cannot be settled: {detail}; inspect receipt and files, then run with --discard to record it as failed", returncode=2, verdict="unsettled", resolution=None, **fields
                )
            resolution = current.result.get("resolution") if isinstance(current.result, dict) else None
            verdict = "discarded" if resolution == "host-discarded" else "settled-complete" if current.phase == "complete" else "settled-failed"
            return self._answer("settle", f"file operation {operation_id} on the {kind} Space {verdict} ({resolution})", verdict=verdict, resolution=resolution, **fields)

        return await await_drained(perform())

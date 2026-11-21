"""Exact host settlement; no Gateway workers, arbitrary plugin load or bulk pass."""

import os
from pathlib import PurePosixPath

from app.gateway.agent_operator_contract import CommandRefused
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.filesystem import FilesystemUnavailable, UnsafeSpacePath
from deerflow.spaces.service import SpaceOperationPending
from deerflow.utils.file_io import await_drained, run_file_io


def _describe(files, candidate, binding, *, managed_home):
    from app.gateway.agent_operator_services import inspect_operator_volume
    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX, USER_FILES_VIRTUAL_PREFIX
    from deerflow.spaces.snapshots import MAX_ARCHIVE_ENTRIES
    from deerflow.spaces.workflows import WorkflowReceipt, probe

    volume, _used, _empty = inspect_operator_volume(files, binding, managed_home=managed_home)
    probe(volume)
    request = candidate.request
    if not isinstance(request, dict):
        raise ValueError("The pending file operation request is invalid")
    namespace = request.get("namespace")
    shared_target = None
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
        if namespace == "hm.shared":
            from deerflow.features.publications import publication_destination
            from deerflow.files.store import StoreError, normalize_relative_path

            facts = receipt["facts"] if receipt else {}
            fact = facts.get("publication") or facts.get("removal")
            path = None
            try:
                if request.get("operation") == "publish":
                    selected, requested = publication_destination(request)
                else:
                    requested_path = request["parameters"]["path"]
                    if not isinstance(requested_path, str):
                        raise ValueError("Invalid removal path")
                    selected, _, requested = normalize_relative_path(requested_path).rpartition("/")
                if isinstance(fact, dict):
                    if not isinstance(fact.get("path"), str):
                        raise ValueError("Invalid receipt path")
                    selected = normalize_relative_path(fact["path"]).rpartition("/")[0]
                shared_target = volume, selected, requested
                folder = "shared/" + selected if selected else "shared"
            except (KeyError, TypeError, StoreError, ValueError):
                pass  # The provider can still inspect or discard a malformed intent.
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
            try:
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        if len(names) >= MAX_ARCHIVE_ENTRIES:
                            names = None
                            break
                        names.append(entry.name)
            finally:
                os.close(descriptor)
        except FileNotFoundError:
            names = []
        except (OSError, FilesystemUnavailable, UnsafeSpacePath):
            # This optional directory listing is not evidence for settlement.
            names = None
    return receipt, sorted(names) if names is not None else None, getattr(volume.spec, "kind", None), shared_target


async def _publication_matches(files, space_id, target, names):
    from deerflow.features.qualified_publications import is_publication_name, shared_file_holding
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.files.store import StoreError
    from deerflow.persistence.shared_publications.sql import SharedPublicationRepository

    volume, folder, requested = target
    matches = {}
    async with files.registry._sf() as session, session.begin():
        link = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
        if link is None or link.space_id != space_id:
            return None
        repo = SharedPublicationRepository(files.registry._sf, session=session)
        for name in names:
            if not is_publication_name(requested, name):
                continue
            relative = folder + "/" + name if folder else name
            record = await repo.live_publication(relative)
            if record is None:
                matches[name] = False
                continue
            try:
                held = await run_file_io(shared_file_holding, volume, relative, record["sha256"], settling=True)
                matches[name] = held is not None and held.size == record["size"]
            except (OSError, FilesystemUnavailable, UnsafeSpacePath, StoreError):
                matches[name] = None
    return matches


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
                resolution = candidate.result.get("resolution") if isinstance(candidate.result, dict) else None
                note = f"file operation {operation_id} is already {candidate.phase}"
                if resolution is not None and resolution not in ("host-settled", "host-reconciled", "host-discarded"):
                    raise CommandRefused(f"{note}; its file outcome was not confirmed ({resolution})", verdict="settled-unconfirmed", resolution=resolution, note="Inspect the target before starting another operation")
                raise CommandRefused(note)
            space = await session.get(SpaceRow, space_id)
            binding = await session.get(SpaceBackingRow, space.backing_handle) if space else None
            managed_home = binding is not None and space_id in await self.files._untyped_homes(session, [binding])
        try:
            if binding is None or binding.space_id != space_id:
                raise BackingUnavailable("The resource has no qualified filesystem binding")
            receipt, names, kind, shared_target = await run_file_io(_describe, self.files, candidate, binding, managed_home=managed_home)
        except SpaceOperationPending:
            raise CommandRefused(f"another workflow holds the lock of space {space_id}; run the command again in a moment") from None
        except BackingUnavailable as exc:
            raise CommandRefused(f"space {space_id} is unverified: {exc}; see storage-init verify") from None
        request = candidate.request
        fields = dict(space_id=space_id, operation_id=operation_id, kind=kind, namespace=request.get("namespace"), action=request.get("action"), on_behalf_of=request.get("on_behalf_of"), request=request, receipt=receipt, files=names)
        if shared_target is not None:
            fields["shared_folder"] = shared_target[1]
            from sqlalchemy.exc import SQLAlchemyError

            try:
                fields["publication_matches"] = await _publication_matches(self.files, space_id, shared_target, names) if names is not None else None
            except SQLAlchemyError:
                fields["publication_matches"] = None  # Optional evidence must not prevent operator recovery.
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
                note = f"publication {operation_id} cannot be settled: {detail}; inspect receipt and files, then run with --discard to record it as failed"
                if names is None:
                    note += "; the target folder could not be listed"
                return self._answer("settle", note, returncode=2, verdict="unsettled", resolution=None, **fields)
            resolution = current.result.get("resolution") if isinstance(current.result, dict) else None
            if resolution == "host-discarded":
                verdict = "discarded"
            elif resolution in ("host-settled", "host-reconciled"):
                verdict = "settled-complete" if current.phase == "complete" else "settled-failed"
            else:
                verdict = "settled-unconfirmed"
            note = f"file operation {operation_id} on the {kind} Space {verdict} ({resolution})"
            if names is None:
                note += "; the target folder could not be listed"
            return self._answer("settle", note, verdict=verdict, resolution=resolution, **fields)

        return await await_drained(perform())

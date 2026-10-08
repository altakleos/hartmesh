"""Host-bound neutral storage for existing extension/action dependencies.

StorageActor is a projection, never an authentication proof. Only trusted host
boundaries enter storage_actor_scope; JSON identities and missing context cannot
select an actor. Every call checks its current binding and fresh resource grants.
"""

import hashlib
import re
from contextlib import contextmanager
from contextvars import ContextVar

from deerflow_extension_api.storage import ResourceReference, StorageActor, StorageCapabilities, StorageResource, StorageUnsupported

from deerflow.runtime.context_keys import STORAGE_PROVIDER_CONTEXT_KEY as STORAGE_PROVIDER_CONTEXT_KEY
from deerflow.spaces.contract import Custody, FeatureBinding, InvalidPrincipal, MutationMode, Permission, PrincipalRef, SpaceDenied
from deerflow.spaces.filesystem import _path
from deerflow.spaces.principals import current_human_principal
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.service import SpaceFiles

_actor = ContextVar("host_storage_actor", default=None)
_credential_allowed = ContextVar("host_storage_credential_allowed", default=True)


@contextmanager
def storage_credential_scope(allowed: bool):
    """Host credential admission, inherited by asynchronous run/service work."""
    if type(allowed) is not bool:
        raise InvalidPrincipal("Credential admission requires a host boolean")
    token = _credential_allowed.set(allowed)
    try:
        yield
    finally:
        _credential_allowed.reset(token)


@contextmanager
def storage_actor_scope(reference: PrincipalRef):
    """Trusted embedding/authentication adapter only; no HTTP actor registrar."""
    if not isinstance(reference, PrincipalRef):
        raise InvalidPrincipal("Host actor binding requires a typed principal")
    token = _actor.set(reference)
    try:
        yield
    finally:
        _actor.reset(token)


def _current_actor():
    if _credential_allowed.get() is not True:
        raise SpaceDenied("This credential has no supported storage resource scopes")
    return _actor.get() or current_human_principal()


def _resource(space):
    custody = space.custody.principal
    return StorageResource(space.id, space.name, space.custody.kind, StorageActor(custody.kind, custody.subject_id) if custody else None, space.mode.value, space.generation, space.status, int(space.permissions))


class HostStorageProvider:
    def __init__(self, get_files, *, namespace=None, controller=None, controller_admitted=None):
        self._get_files = get_files
        self._namespace, self._controller, self._controller_admitted = namespace, controller, controller_admitted

    def for_plugin(self, source, plugin, get_extensions):
        """Bind controller attribution to the installed host snapshot, not a payload."""

        async def admitted():
            import asyncio

            from deerflow.extensions.plugin_tools import plugin_settings

            current = get_extensions()
            if not any(owner == source and entry is plugin for owner, entry in current.plugins):
                return False
            return (await asyncio.to_thread(plugin_settings, source, plugin))["enabled"] is True

        return HostStorageProvider(self._get_files, namespace=plugin.namespace, controller=plugin.storage_controller, controller_admitted=admitted)

    @property
    def capabilities(self):
        files = self._get_files()
        if not isinstance(files, SpaceFiles):
            return StorageCapabilities()
        kinds = ("human", "nonhuman") if getattr(files.registry._resolver, "_nonhuman", None) is not None else ("human",)
        return StorageCapabilities(
            available=True,
            actor_kinds=kinds,
            files=True,
            provision=True,
            grants=True,
            mediated_mutations=self._controller is not None and self._controller_admitted is not None,
            # Attachments remain a host-only consumer API until neutral typed
            # attachment requests have been qualified for this facade.
            native_attachments=False,
            quiesced_recovery=True,
        )

    async def current(self):
        reference = _current_actor()
        files = self._get_files()
        if not isinstance(files, SpaceFiles):
            raise StorageUnsupported("This host has no qualified resource storage")
        await files.registry._actor(reference)
        return HostBoundStorage(self, reference)


class HostBoundStorage:
    def __init__(self, provider, reference):
        self._provider, self._reference = provider, reference
        self.actor = StorageActor(reference.kind, reference.subject_id)

    @property
    def capabilities(self):
        return self._provider.capabilities

    def _files(self):
        if _current_actor() != self._reference:
            raise InvalidPrincipal("Storage caller binding changed or is unavailable")
        files = self._provider._get_files()
        if not isinstance(files, SpaceFiles):
            raise StorageUnsupported("The host resource adapter is unavailable")
        return files

    async def list(self, *, limit=100, offset=0):
        return [_resource(space) for space in await self._files().registry.list(actor=self._reference, limit=limit, offset=offset)]

    async def get(self, *, space_id):
        return _resource(await self._files().registry.get(actor=self._reference, space_id=space_id))

    async def provision(self, *, name, custody, mode="native"):
        if custody not in ("personal", "company"):
            raise ValueError("Resource custody must be personal or company")
        files = self._files()
        mutation_mode = MutationMode(mode)
        feature = None
        if mutation_mode == MutationMode.MEDIATED:
            provider = self._provider
            if provider._controller is None or provider._controller_admitted is None or await provider._controller_admitted() is not True:
                raise SpaceDenied("Required installed storage controller is unavailable")
            feature = FeatureBinding(provider._namespace, provider._controller.name, provider._controller.metadata_version)
        space = await files.create(actor=self._reference, name=name, custody=Custody.personal(self._reference) if custody == "personal" else Custody.company(), mode=mutation_mode, feature=feature)
        return _resource(space)

    async def read(self, *, space_id, path, max_bytes):
        return await self._files().read(actor=self._reference, space_id=space_id, path=path, max_bytes=max_bytes)

    async def export_file(self, *, space_id, path, max_bytes):
        return await self._files().export_file(actor=self._reference, space_id=space_id, path=path, max_bytes=max_bytes)

    async def quota(self, *, space_id):
        return await self._files().quota(actor=self._reference, space_id=space_id)

    async def list_directory(self, *, space_id, path="", limit=200):
        return await self._files().list_directory(actor=self._reference, space_id=space_id, path=path, limit=limit)

    async def write(self, *, space_id, generation, operation_id, path, content, expected_sha256=None, create=False):
        return await self._files().write(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id, path=path, content=content, expected_sha256=expected_sha256, create=create)

    async def mkdir(self, *, space_id, generation, operation_id, path):
        return await self._files().mkdir(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id, path=path)

    async def rename(self, *, space_id, generation, operation_id, path, destination):
        return await self._files().rename(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id, path=path, destination=destination)

    async def remove(self, *, space_id, generation, operation_id, path):
        return await self._files().remove(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id, path=path)

    async def grant(self, *, space_id, generation, subject, permissions, acknowledge_existing_data=False):
        if not isinstance(subject, StorageActor) or type(permissions) is not int or not 0 <= permissions <= 31:
            raise ValueError("Grant targets and permissions require typed valid values")
        return _resource(
            await self._files().registry.set_grant(
                actor=self._reference, space_id=space_id, expected_generation=generation, subject=PrincipalRef(subject.kind, subject.subject_id), permissions=Permission(permissions), acknowledge_existing_data=acknowledge_existing_data
            )
        )

    async def copy(self, *, source, destination, source_generation, destination_generation, operation_id, acknowledge_disclosure=False):
        if not isinstance(source, ResourceReference) or not isinstance(destination, ResourceReference) or source.revision is not None or destination.revision is not None:
            raise ValueError("Transfer requires live typed resource references")
        return await self._files().copy(
            actor=self._reference,
            source_id=source.space_id,
            destination_id=destination.space_id,
            source_generation=source_generation,
            destination_generation=destination_generation,
            source_path=source.path,
            destination_path=destination.path,
            operation_id=operation_id,
            acknowledge_disclosure=acknowledge_disclosure,
        )

    async def backup(self, *, space_id, generation, operation_id):
        return await SpaceRecovery(self._files()).backup(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id)

    async def restore(self, *, space_id, generation, operation_id, backup_id):
        return _resource(await SpaceRecovery(self._files()).restore(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id, backup_id=backup_id))

    async def archive(self, *, space_id, generation, operation_id):
        return _resource(await SpaceRecovery(self._files()).archive(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id))

    async def delete(self, *, space_id, generation, operation_id):
        return _resource(await SpaceRecovery(self._files()).delete(actor=self._reference, space_id=space_id, expected_generation=generation, operation_id=operation_id))

    async def recovery_status(self, *, space_id):
        return await SpaceRecovery(self._files()).status(actor=self._reference, space_id=space_id)

    def _controller(self, space_id):
        provider = self._provider
        declaration = provider._controller
        if declaration is None or provider._namespace is None or provider._controller_admitted is None:
            raise SpaceDenied("Required installed storage controller is unavailable")

        async def admitted(_session, rows):
            row, _grant = rows[space_id]
            if row.mode != "mediated" or (row.feature_namespace, row.feature_controller, row.feature_metadata_version) != (provider._namespace, declaration.name, declaration.metadata_version):
                raise SpaceDenied("The installed controller is not bound to this resource")
            if await provider._controller_admitted() is not True:
                raise SpaceDenied("The bound storage controller is disabled or unavailable")

        return declaration, admitted

    async def mediated_write(self, *, space_id, generation, operation_id, path, content, expected_sha256=None, create=False):
        from deerflow.spaces.service import MAX_TRANSFER_BYTES

        _path(path)
        if not isinstance(content, bytes) or len(content) > MAX_TRANSFER_BYTES or type(create) is not bool:
            raise ValueError("Controlled writes require bounded bytes and an explicit create flag")
        if (create and expected_sha256 is not None) or (not create and (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256))):
            raise ValueError("Controlled edits require the captured file revision")
        files = self._files()
        declaration, guard = self._controller(space_id)
        request = {"action": "write", "path": path, "sha256": hashlib.sha256(content).hexdigest(), "expected_sha256": expected_sha256, "create": create, "controller_namespace": self._provider._namespace, "controller": declaration.name}
        return await files._mutate(
            actor=self._reference,
            requests={space_id: (Permission.OPERATE, generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=request,
            controller_admission=guard,
            call=lambda volumes: files._io(volumes[space_id], lambda fs: fs.write_atomic(path, content, expected_sha256=expected_sha256, create=create)),
        )

    async def mediated_remove(self, *, space_id, generation, operation_id, path):
        _path(path)
        files = self._files()
        declaration, guard = self._controller(space_id)
        request = {"action": "remove", "path": path, "controller_namespace": self._provider._namespace, "controller": declaration.name}
        return await files._mutate(
            actor=self._reference,
            requests={space_id: (Permission.OPERATE, generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=request,
            controller_admission=guard,
            call=lambda volumes: files._io(volumes[space_id], lambda fs: fs.remove(path)),
        )

    async def mediated_mkdir(self, *, space_id, generation, operation_id, path):
        _path(path)
        files = self._files()
        declaration, guard = self._controller(space_id)
        request = {"action": "mkdir", "path": path, "controller_namespace": self._provider._namespace, "controller": declaration.name}
        return await files._mutate(
            actor=self._reference,
            requests={space_id: (Permission.OPERATE, generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=request,
            controller_admission=guard,
            call=lambda volumes: files._io(volumes[space_id], lambda fs: fs.mkdir(path)),
        )

    async def mediated_rename(self, *, space_id, generation, operation_id, path, destination):
        _path(path)
        _path(destination)
        files = self._files()
        declaration, guard = self._controller(space_id)
        request = {"action": "rename", "path": path, "destination": destination, "controller_namespace": self._provider._namespace, "controller": declaration.name}
        return await files._mutate(
            actor=self._reference,
            requests={space_id: (Permission.OPERATE, generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=request,
            controller_admission=guard,
            call=lambda volumes: files._io(volumes[space_id], lambda fs: fs.rename(path, destination)),
        )

    async def mediated_copy(self, *, source, destination, source_generation, destination_generation, operation_id, acknowledge_disclosure=False):
        from deerflow.spaces.service import MAX_TRANSFER_BYTES

        if not isinstance(source, ResourceReference) or not isinstance(destination, ResourceReference) or source.revision is not None or destination.revision is not None or source.space_id == destination.space_id:
            raise ValueError("Transfer requires distinct live typed resource references")
        _path(source.path)
        _path(destination.path)
        files = self._files()
        declaration, guard = self._controller(destination.space_id)
        request = {
            "action": "copy",
            "source_id": source.space_id,
            "source_generation": source_generation,
            "source_path": source.path,
            "destination_id": destination.space_id,
            "destination_path": destination.path,
            "acknowledge_disclosure": acknowledge_disclosure is True,
            "controller_namespace": self._provider._namespace,
            "controller": declaration.name,
        }

        def transfer(volumes):
            content = files._io(volumes[source.space_id], lambda fs: fs.read_bytes(source.path, max_bytes=MAX_TRANSFER_BYTES))
            return files._io(volumes[destination.space_id], lambda fs: fs.write_atomic(destination.path, content, expected_sha256=None, create=True))

        return await files._mutate(
            actor=self._reference,
            requests={source.space_id: (Permission.READ | Permission.EXPORT, source_generation), destination.space_id: (Permission.OPERATE, destination_generation)},
            destination_id=destination.space_id,
            operation_id=operation_id,
            request=request,
            controller_admission=guard,
            call=transfer,
        )

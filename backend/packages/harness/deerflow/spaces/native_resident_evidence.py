"""Private actorless original resident graph; durable facts confer no authority."""

from dataclasses import asdict, dataclass, fields
from pathlib import PurePosixPath
from uuid import UUID

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeTeardownResource, _hex, _name, _text, _timestamp

_ROLES = {"network-proxy": "container", "network": "network", "egress-network": "network"}


def _path(value):
    return isinstance(value, str) and len(value) <= 4096 and _text(value, 4096) and value.startswith("/") and str(PurePosixPath(value)) == value and ".." not in PurePosixPath(value).parts


@dataclass(frozen=True)
class NativeResidentRoot:
    slot_id: str
    source: str
    destination: str
    writable: bool
    backing_handle: str
    filesystem_uuid: str
    device: int
    root_inode: int
    control_inode: int
    source_inode: int
    space_id: str | None
    generation: int | None
    revision: str | None

    @classmethod
    def from_view(cls, view, volume):
        """Detach facts from the host's freshly qualified bound volume."""
        root = cls(
            volume.spec.slot_id,
            str(volume.data_path),
            "/mnt/spaces/" + view.alias,
            view.writable,
            volume.backing_handle,
            volume.spec.filesystem_uuid,
            volume.device,
            volume.root_inode,
            volume.control_inode,
            volume.root_inode,
            view.space_id,
            view.generation,
            None,
        )
        return cls.parse(asdict(root))

    @classmethod
    def from_readonly(cls, projection):
        projection.validate()
        volume = projection.volume
        root = cls(
            projection.slot_id,
            projection.source,
            projection.destination,
            False,
            getattr(volume, "backing_handle", None),
            volume.spec.filesystem_uuid,
            volume.device,
            volume.root_inode,
            volume.control_inode,
            projection.identity[1],
            None,
            None,
            projection.revision,
        )
        return cls.parse(asdict(root))

    @classmethod
    def parse(cls, document):
        if not isinstance(document, dict) or set(document) != {field.name for field in fields(cls)}:
            raise StorageHostUnqualified("Resident root evidence has unknown fields")
        scoped = document["space_id"] is not None
        if (
            not _hex(document["slot_id"], 32)
            or not _path(document["source"])
            or not _path(document["destination"])
            or type(document["writable"]) is not bool
            or not _hex(document["backing_handle"], 32)
            or not isinstance(document["filesystem_uuid"], str)
            or type(document["device"]) is not int
            or document["device"] < 0
            or any(type(document[key]) is not int or document[key] <= 0 for key in ("root_inode", "control_inode", "source_inode"))
            or (scoped and (not _hex(document["space_id"], 32) or type(document["generation"]) is not int or not 1 <= document["generation"] <= 2**31 - 1 or document["revision"] is not None))
            or (not scoped and (document["writable"] is not False or document["generation"] is not None or not _hex(document["revision"], 64)))
        ):
            raise StorageHostUnqualified("Resident root evidence is malformed")
        try:
            if str(UUID(document["filesystem_uuid"])) != document["filesystem_uuid"]:
                raise ValueError("Noncanonical filesystem UUID")
        except ValueError as exc:
            raise StorageHostUnqualified("Resident root evidence lacks its qualified filesystem identity") from exc
        return cls(**document)


@dataclass(frozen=True)
class NativeResidentRecord:
    id: str
    daemon_id: str
    attachment_id: str
    incarnation: str
    container_id: str
    name: str
    created_at: str
    phase: str
    network_mode: str
    policy_digest: str | None
    roots: tuple[NativeResidentRoot, ...]
    configured_mounts: tuple[tuple[str, str, bool], ...]
    resources: tuple[NativeTeardownResource, ...]
    network_target: str = "bridge"
    shared_network: NativeTeardownResource | None = None

    @property
    def slots(self):
        return tuple(sorted({root.slot_id for root in self.roots}))

    @property
    def mounts(self):
        return tuple(sorted((*((root.source, root.destination, root.writable) for root in self.roots), *self.configured_mounts)))

    @property
    def correlation(self):
        """Detached accounting identity, without actor or execution authority."""
        return (
            self.attachment_id,
            self.container_id,
            self.daemon_id,
            self.incarnation,
            tuple(sorted((root.space_id, root.generation, root.destination.removeprefix("/mnt/spaces/"), root.writable) for root in self.roots if root.space_id is not None)),
        )

    def document(self):
        return {
            "schema_version": 5,
            "record_kind": "resident-resources",
            **asdict(self),
            "roots": [asdict(root) for root in self.roots],
            "configured_mounts": [list(mount) for mount in self.configured_mounts],
            "resources": [asdict(resource) for resource in self.resources],
        }

    @classmethod
    def parse(cls, document, identifier):
        if (
            not isinstance(document, dict)
            or set(document) != {"schema_version", "record_kind", *(field.name for field in fields(cls))}
            or type(document.get("schema_version")) is not int
            or document["schema_version"] != 5
            or document["record_kind"] != "resident-resources"
        ):
            raise StorageHostUnqualified("Resident evidence schema is unknown")
        if (
            document["id"] != identifier
            or not _hex(identifier, 32)
            or not _hex(document["attachment_id"], 32)
            or not _hex(document["incarnation"], 32)
            or not _text(document["daemon_id"], 128)
            or not _hex(document["container_id"], 64)
            or document["name"] != "hartmesh-space-" + document["attachment_id"]
            or not _timestamp(document["created_at"])
            or document["phase"] not in ("prepared", "retiring")
            or document["network_mode"] not in ("open", "blocked", "allowlist")
            or not _name(document["network_target"])
            or not isinstance(document["roots"], list)
            or not 1 <= len(document["roots"]) <= 128
            or not isinstance(document["configured_mounts"], list)
            or len(document["configured_mounts"]) > 128
            or not isinstance(document["resources"], list)
            or len(document["resources"]) > 3
        ):
            raise StorageHostUnqualified("Resident evidence correlation is malformed")
        roots = tuple(NativeResidentRoot.parse(root) for root in document["roots"])
        configured = []
        for mount in document["configured_mounts"]:
            if not isinstance(mount, list) or len(mount) != 3 or not _path(mount[0]) or not _path(mount[1]) or mount[2] is not False:
                raise StorageHostUnqualified("Resident configured projection is not exactly read-only")
            configured.append(tuple(mount))
        destinations = [PurePosixPath(root.destination) for root in roots] + [PurePosixPath(mount[1]) for mount in configured]
        if len({root.space_id for root in roots if root.space_id is not None}) != sum(root.space_id is not None for root in roots) or any(
            a.is_relative_to(b) or b.is_relative_to(a) for index, a in enumerate(destinations) for b in destinations[index + 1 :]
        ):
            raise StorageHostUnqualified("Resident evidence contains overlapping resource projections")
        resources = []
        for resource in document["resources"]:
            if (
                not isinstance(resource, dict)
                or set(resource) != {field.name for field in fields(NativeTeardownResource)}
                or not isinstance(resource["role"], str)
                or resource["role"] not in _ROLES
                or resource["kind"] != _ROLES[resource["role"]]
                or not _hex(resource["identity"], 64)
                or not _name(resource["name"])
                or not _timestamp(resource["created_at"])
            ):
                raise StorageHostUnqualified("Resident paired evidence is malformed")
            resources.append(NativeTeardownResource(**resource))
        mode, digest = document["network_mode"], document["policy_digest"]
        shared = document["shared_network"]
        if shared is not None:
            if (
                not isinstance(shared, dict)
                or set(shared) != {field.name for field in fields(NativeTeardownResource)}
                or shared["kind"] != "network"
                or shared["role"] != "shared-network"
                or not _hex(shared["identity"], 64)
                or not _name(shared["name"])
                or not _timestamp(shared["created_at"])
            ):
                raise StorageHostUnqualified("Resident shared network correlation is malformed")
            shared = NativeTeardownResource(**shared)
        if mode == "open":
            valid_network = shared is None if document["network_target"] == "none" else shared is not None and document["network_target"] in {shared.name, shared.identity, "default" if shared.name == "bridge" else shared.name}
        else:
            internal = next((member for member in resources if member.role == "network"), None)
            valid_network = shared is None and internal is not None and document["network_target"] in (internal.name, internal.identity)
        if not valid_network:
            raise StorageHostUnqualified("Resident evidence has no exact original network target")
        complete = not resources and digest is None if mode == "open" else len(resources) == 3 and {resource.role for resource in resources} == set(_ROLES) and _hex(digest, 64)
        identities = [document["container_id"], *(resource.identity for resource in resources)]
        names = [("container", document["name"]), *((resource.kind, resource.name) for resource in resources)]
        if not complete or len(set(identities)) != len(identities) or len(set(names)) != len(names):
            raise StorageHostUnqualified("Resident evidence has an incomplete or ambiguous original graph")
        return cls(
            **{
                key: roots if key == "roots" else tuple(configured) if key == "configured_mounts" else tuple(resources) if key == "resources" else shared if key == "shared_network" else value
                for key, value in document.items()
                if key not in ("schema_version", "record_kind")
            }
        )

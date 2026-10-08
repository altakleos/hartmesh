"""Default feature contributions use the one operator-controlled extension loader."""

from dataclasses import dataclass, field

from deerflow_extension_api.plugins import BackendAction, PluginContribution
from deerflow_extension_api.storage import StorageController, StorageUnsupported

from deerflow.extensions.loader import ExtensionSpec
from deerflow.features.resources import FeatureResources
from deerflow.spaces.contract import Custody, FeatureBinding, MutationMode
from deerflow.spaces.facade import _current_actor

DEFAULT_FEATURES = (
    "deerflow.features.plugins:install_my_files",
    "deerflow.features.plugins:install_shared",
    "deerflow.features.plugins:install_projects",
)


def with_default_features(specs):
    """An explicit ordinary loader entry (including disabled) overrides a default."""
    present = {spec.use for spec in specs}
    return [*specs, *(ExtensionSpec(use=use) for use in DEFAULT_FEATURES if use not in present)]


@dataclass
class FeatureServices:
    services: dict = field(default_factory=dict)


class FeatureService:
    def __init__(self, namespace, title, behavior, *, mediated=False):
        self.namespace, self.title, self.mediated = namespace, title, mediated
        self.behavior = behavior
        self.provider = None
        self.healthy = False
        self.store = None
        self.contribution = None

    async def start(self, deps):
        if deps.app_store is None or deps.session_factory is None or deps.storage is None:
            raise StorageUnsupported("Storage features require durable host storage")
        self.provider, self.store = deps.storage, deps.app_store
        if hasattr(self.behavior, "bind"):
            self.behavior.bind(deps.session_factory)
        self.store.get_or_init(FeatureServices, FeatureServices).services[self.namespace] = self
        self.healthy = True

    async def stop(self):
        self.healthy = False
        if self.store is not None:
            state = self.store.get(FeatureServices)
            if state and state.services.get(self.namespace) is self:
                del state.services[self.namespace]
        self.provider = None

    def resources(self):
        if not self.healthy or self.provider is None:
            raise StorageUnsupported("The required storage feature is unavailable")
        files = self.provider._get_files()
        if files is None:
            raise StorageUnsupported("This host has no qualified resource storage")
        return FeatureResources(files, self.namespace)

    async def resource(self, *, provision=True):
        actor = _current_actor()
        if actor.kind != "human":
            raise StorageUnsupported("This convenience feature supports human owners; nonhuman homes use generic storage")
        resources = self.resources()
        key = "company" if self.mediated else f"{actor.kind}:{actor.subject_id}"
        if not provision:
            return await resources.get(actor=actor, key=key)
        return await resources.ensure(
            actor=actor,
            key=key,
            name=self.title,
            custody=Custody.company() if self.mediated else Custody.personal(actor),
            mode=MutationMode.MEDIATED if self.mediated else MutationMode.NATIVE,
            feature=FeatureBinding(self.namespace, "publication", 1) if self.mediated else None,
        )

    def admitted_behavior(self):
        from deerflow.config.paths import get_paths
        from deerflow.features.paths import FeaturePaths
        from deerflow.spaces.contract import SpaceDenied
        from deerflow.spaces.workflows import _owner

        actor = _current_actor()
        files = self.resources().files
        owner = _owner.get()
        paths = get_paths()
        if actor.kind != "human" or owner is None or owner[0] is not files or not isinstance(paths, FeaturePaths) or paths._feature_user != actor.subject_id or self.namespace not in paths._feature_volumes:
            raise SpaceDenied("Feature workflows require their current admitted host resource view")
        return self.behavior, actor

    async def execute(self, operation, **values):
        from deerflow.files.store import StoreError
        from deerflow.spaces.contract import SpaceConflict, SpaceDenied
        from deerflow.spaces.workflows import WorkflowRejected, _effect

        behavior, actor = self.admitted_behavior()
        try:
            return await behavior.execute(operation, user_id=actor.subject_id, **values)
        except (SpaceConflict, SpaceDenied, StoreError, FileNotFoundError, ValueError) as exc:
            effect = _effect.get()
            if effect is not None and effect[0] is False:
                raise WorkflowRejected(exc) from None
            raise

    async def recover(self, payload, ctx):
        from deerflow.config.paths import get_paths, paths_scope
        from deerflow.features.paths import FeaturePaths
        from deerflow.features.publications import reconcile_publication
        from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
        from deerflow.persistence.spaces.files import SpaceFileOperationRow
        from deerflow.spaces.contract import Permission, SpaceDenied
        from deerflow.spaces.workflows import _receipt

        if not self.mediated or ctx.storage is None or set(payload) != {"operation_id", "generation"}:
            raise StorageUnsupported("This controller requires an exact pending operation and generation")
        actor = _current_actor()
        files = self.resources().files
        space = await self.resource(provision=False)
        files._operation_id(payload["operation_id"])
        if type(payload["generation"]) is not int or payload["generation"] != space.generation:
            raise SpaceDenied("Controller recovery requires the current resource generation")
        _declaration, guard = ctx.storage._controller(space.id)
        await files.registry.get(actor=actor, space_id=space.id, permission=Permission.ADMIN | Permission.OPERATE)
        async with files.registry._sf() as session:
            intent = await session.get(SpaceFileOperationRow, (space.id, payload["operation_id"]))
            if intent is None or intent.phase != "pending" or intent.request.get("namespace") != self.namespace:
                raise SpaceDenied("No pending operation belongs to this controller")
            request = dict(intent.request)

        async def reconcile(volumes):
            with paths_scope(FeaturePaths(get_paths(), actor.subject_id, {self.namespace: volumes[space.id]})):
                return await reconcile_publication(SharedPublicationRepository(files.registry._sf), _receipt.get(), intent)

        return await files.run_workflow(
            actor=actor,
            requests={space.id: (Permission.ADMIN | Permission.OPERATE, space.generation)},
            destination_id=space.id,
            operation_id=payload["operation_id"],
            request=request,
            controller_admission=guard,
            call=reconcile,
            reconcile=True,
        )


def _install(registry, config, namespace, title, behavior, *, mediated=False):
    enabled = config.get("enabled", True)
    if type(enabled) is not bool or set(config) - {"enabled"}:
        raise ValueError("Storage feature configuration accepts only boolean enabled")
    service = FeatureService(namespace, title, behavior, mediated=mediated)

    async def resource(_payload, ctx):
        if ctx.storage is None or ctx.actor is None:
            raise StorageUnsupported("A host-bound storage actor is required")
        space = await service.resource()
        # DTO contains no host paths or backing identities.
        return {"id": space.id, "name": space.name, "mode": space.mode.value, "generation": space.generation, "permissions": int(space.permissions)}

    async def recover(payload, ctx):
        return await service.recover(payload, ctx)

    contribution = PluginContribution(
        namespace=namespace,
        title=title,
        enabled=enabled,
        api_version=4,
        storage_api_version=1,
        storage_controller=StorageController("publication", 1) if mediated else None,
        backend=(BackendAction("resource", resource), *((BackendAction("recover", recover),) if mediated else ())),
    )
    service.contribution = contribution
    if registry.plugin(contribution) is not True:
        raise StorageUnsupported("The host lacks storage feature contributions")
    registry.service(service)


def install_my_files(registry, config):
    from deerflow.features.behaviors import MyFilesBehavior

    _install(registry, config, "hm.my-files", "My Files", MyFilesBehavior())


def install_shared(registry, config):
    from deerflow.features.behaviors import SharedBehavior

    _install(registry, config, "hm.shared", "Shared", SharedBehavior(), mediated=True)


def install_projects(registry, config):
    from deerflow.features.behaviors import ProjectsBehavior

    _install(registry, config, "hm.projects", "Projects", ProjectsBehavior())

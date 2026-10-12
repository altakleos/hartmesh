"""Default feature contributions use the one operator-controlled extension loader."""

from dataclasses import dataclass, field

from deerflow_extension_api.plugins import BackendAction, PluginContribution
from deerflow_extension_api.storage import StorageUnsupported

from deerflow.extensions.loader import ExtensionSpec
from deerflow.features.resources import FeatureResources
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
    def __init__(self, namespace, title, behavior, *, host_written=False):
        self.namespace, self.title, self.host_written = namespace, title, host_written
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
        files = self.provider._get_files()
        if files is not None:
            files.workflow_settlers[self.namespace] = self.settle_workflow

    async def stop(self):
        files = self.provider._get_files() if self.provider is not None else None
        if files is not None and files.workflow_settlers.get(self.namespace) == self.settle_workflow:
            del files.workflow_settlers[self.namespace]
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
        return FeatureResources(files, "hm.company-space" if self.namespace == "hm.shared" else "hm.user-space")

    async def resource(self):
        actor = _current_actor()
        if actor.kind != "human":
            raise StorageUnsupported("This convenience feature supports human owners; nonhuman homes use generic storage")
        return await self.resources().get(actor=actor, key="company" if self.namespace == "hm.shared" else f"human:{actor.subject_id}")

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
        from deerflow.spaces.backings import BackingUnavailable
        from deerflow.spaces.contract import SpaceConflict, SpaceDenied
        from deerflow.spaces.workflows import WorkflowRejected, _effect

        behavior, actor = self.admitted_behavior()
        try:
            return await behavior.execute(operation, user_id=actor.subject_id, **values)
        except (BackingUnavailable, SpaceConflict, SpaceDenied, StoreError, FileNotFoundError, ValueError) as exc:
            effect = _effect.get()
            if effect is not None and effect[0] is False:
                raise WorkflowRejected(exc) from None
            raise

    async def settle_workflow(self, intent, *, session):
        """Trusted generic recovery callback; domain reconciliation stays here."""
        from deerflow.spaces.contract import SpaceDenied
        from deerflow.spaces.workflows import _owner, _read_only, _receipt

        files = self.resources().files
        owner, receipt = _owner.get(), _receipt.get()
        if (
            owner is None
            or owner[0] is not files
            or intent.space_id not in owner[1]
            or _read_only.get()
            or receipt is None
            or receipt.operation_id != intent.operation_id
            or receipt.value["request"] != intent.request
            or intent.request.get("namespace") != self.namespace
        ):
            raise SpaceDenied("Workflow settlement requires exact owned evidence")
        if self.host_written:
            await self.settle_publication(intent, session=session)
            return "host-reconciled"
        return "host-settled"

    async def settle_publication(self, intent, *, session):
        """Reconcile one exact host workflow inside its caller's transaction.

        The caller completes the operation in that transaction and removes the
        receipt only after confirmed commit. This method has no human actor.
        """
        from copy import deepcopy

        from deerflow.config.paths import get_paths, paths_scope
        from deerflow.features.paths import FeaturePaths
        from deerflow.features.publications import reconcile_publication
        from deerflow.features.resources import StorageFeatureLinkRow
        from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
        from deerflow.persistence.spaces.files import SpaceFileOperationRow
        from deerflow.persistence.spaces.model import SpaceRow
        from deerflow.spaces.attachments import SpaceAttachments
        from deerflow.spaces.contract import SpaceDenied
        from deerflow.spaces.workflows import _owner, _read_only, _receipt
        from deerflow.utils.file_io import await_drained, run_file_io

        # Refresh may return and overwrite the caller's same ORM instance.
        expected = (intent.space_id, intent.operation_id, intent.actor_kind, intent.actor_id, intent.generation, deepcopy(intent.request))

        async def perform():
            files = self.resources().files
            owner, receipt = _owner.get(), _receipt.get()
            if not self.host_written or self.namespace != "hm.shared" or owner is None or owner[0] is not files or expected[0] not in owner[1] or _read_only.get():
                raise SpaceDenied("Publication settlement requires the owned host workflow")
            if not session.in_transaction() or not session.is_active:
                raise ValueError("Publication settlement requires an active transaction")
            current = await session.get(SpaceFileOperationRow, expected[:2], populate_existing=True)
            if current is None or current.phase != "pending" or current.actor_kind != "human" or (current.space_id, current.operation_id, current.actor_kind, current.actor_id, current.generation, current.request) != expected:
                raise SpaceDenied("The exact pending publication is unavailable")
            if not isinstance(current.request, dict) or current.request.get("action") != "feature-workflow" or current.request.get("namespace") != self.namespace or current.request.get("by") != "host":
                raise SpaceDenied("The pending operation is not a host publication")
            link = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
            row = await session.get(SpaceRow, current.space_id)
            if (
                link is None
                or link.space_id != current.space_id
                or row is None
                or (row.status, row.mode, row.custody_kind, row.feature_namespace, row.feature_controller, row.feature_metadata_version) != ("active", "native", "company", None, None, None)
            ):
                raise SpaceDenied("Publication settlement requires the assigned company Space")
            if await SpaceAttachments._current(session, [current.space_id], lock=True):
                raise SpaceDenied("Publication settlement requires fenced attachments")
            volume = await files._volume(session, row)
            if receipt is None or receipt.root != volume.control_path or receipt.operation_id != current.operation_id or receipt.value.get("request") != current.request:
                raise SpaceDenied("The exact publication receipt is unavailable")
            await run_file_io(receipt.load)
            with paths_scope(FeaturePaths(get_paths(), current.actor_id, {self.namespace: volume})):
                from functools import partial

                from deerflow.features.qualified_publications import shared_file_holding as qualified_holding

                holding = partial(qualified_holding, volume, expected_identity=receipt.value["facts"].get("publication_identity"))
                return await reconcile_publication(SharedPublicationRepository(files.registry._sf, session=session), receipt, current, holding=holding)

        return await await_drained(perform())


def _install(registry, config, namespace, title, behavior, *, host_written=False):
    enabled = config.get("enabled", True)
    if type(enabled) is not bool or set(config) - {"enabled"}:
        raise ValueError("Storage feature configuration accepts only boolean enabled")
    service = FeatureService(namespace, title, behavior, host_written=host_written)

    async def resource(_payload, ctx):
        if ctx.storage is None or ctx.actor is None:
            raise StorageUnsupported("A host-bound storage actor is required")
        space = await service.resource()
        # DTO contains no host paths or backing identities.
        return {"id": space.id, "name": space.name, "mode": space.mode.value, "generation": space.generation, "permissions": int(space.permissions)}

    contribution = PluginContribution(
        namespace=namespace,
        title=title,
        enabled=enabled,
        api_version=4,
        storage_api_version=1,
        storage_controller=None,
        backend=(BackendAction("resource", resource),),
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

    _install(registry, config, "hm.shared", "Shared", SharedBehavior(), host_written=True)


def install_projects(registry, config):
    from deerflow.features.behaviors import ProjectsBehavior

    _install(registry, config, "hm.projects", "Projects", ProjectsBehavior())

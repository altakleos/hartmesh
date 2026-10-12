"""Host-only fresh personal/company chat storage admission."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from app.gateway.storage_spaces import storage_state
from deerflow.config.paths import _validate_thread_id, _validate_user_id, get_paths
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_paths import ChatStoragePaths, ThreadMounts
from deerflow.spaces.contract import InvalidPrincipal, Permission, PrincipalRef, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.facade import _bound_principal, _credential_allowed
from deerflow.spaces.incarnations import verify_bound
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained, run_file_io


@dataclass(frozen=True)
class BindingFacts:
    backing_handle: str
    space_id: str
    slot_id: str
    filesystem_uuid: str
    max_bytes: int
    max_inodes: int
    root_inode: int
    control_inode: int


@dataclass(frozen=True)
class PreparedChatStorage:
    paths: ChatStoragePaths
    mounts: ThreadMounts
    reason: str | None = None
    shared_reason: str | None = None
    protected_roots: tuple[Path, ...] = ()


async def _snapshot_in_session(session, files, actor, namespace, key, *, required=None):
    link = await session.get(StorageFeatureLinkRow, (namespace, key))
    if link is None:
        return None
    row = await session.get(SpaceRow, link.space_id)
    if row is None or (row.status, row.mode, row.feature_namespace, row.feature_controller, row.feature_metadata_version) != ("active", "native", None, None, None):
        raise BackingUnavailable("The assigned role is unavailable")
    personal = namespace == "hm.user-space"
    expected = ("personal", "human", actor.subject_id) if personal else ("company", None, None)
    if (row.custody_kind, row.custodian_kind, row.custodian_id) != expected:
        raise BackingUnavailable("The assigned role has different custody")
    grant = await session.get(SpaceGrantRow, (row.id, actor.kind, actor.subject_id))
    if required is None:
        required = Permission.READ | Permission.WRITE if personal else Permission.READ | Permission.EXPORT
    view = files.registry._view(row, grant.permissions) if grant is not None else None
    if view is None or view.permissions & required != required:
        raise BackingUnavailable("The assigned role is not currently granted")
    binding = await session.get(SpaceBackingRow, row.backing_handle)
    if binding is None or binding.space_id != row.id:
        raise BackingUnavailable("The assigned role has no current binding")
    fields = BindingFacts.__dataclass_fields__
    facts = BindingFacts(**{name: getattr(binding, name) for name in fields})
    identity = (row.id, row.generation, grant.permissions, facts)
    # No filesystem operation belongs inside this SQL session.
    return identity, facts


async def _snapshot(files, actor, namespace, key, *, required=None):
    await files.registry._actor(actor)
    async with files.registry._sf() as session:
        return await _snapshot_in_session(session, files, actor, namespace, key, required=required)


async def _admit_verified(files, actor, user_original, company_original=None):
    requests = {user_original[0][0]: (Permission.READ | Permission.WRITE, user_original[0][1])}
    if company_original is not None:
        requests[company_original[0][0]] = (Permission.READ | Permission.EXPORT, company_original[0][1])
    # These narrow permissions keep the existing pending-operation/attachment
    # guards, without requesting writer exclusion or filesystem work under SQL.
    async with files.registry.admitted(actor=actor, requests=requests) as (session, _rows):
        if await _snapshot_in_session(session, files, actor, "hm.user-space", "human:" + actor.subject_id) != user_original:
            raise BackingUnavailable("Personal storage changed after qualification")
        if company_original is not None and await _snapshot_in_session(session, files, actor, "hm.company-space", "company") != company_original:
            raise BackingUnavailable("Shared storage changed after qualification")


async def _verified_role(files, actor, namespace, key, *, required=None):
    claims = files._claim_snapshot()
    original = await _snapshot(files, actor, namespace, key, required=required)
    if original is None:
        return None, "space-not-assigned", None
    _identity, binding = original
    spec = files._binding_spec(binding)
    expected_kind = "user" if namespace == "hm.user-space" else "company"
    if spec.kind != expected_kind:
        raise BackingUnavailable("The assigned role has the wrong slot kind")
    volume = await await_drained(run_file_io(files.catalog.verify, binding.slot_id, previous=files._verified.get(binding.slot_id)))
    files._check_incarnation(binding, volume)
    bound = await await_drained(run_file_io(verify_bound, volume, binding.backing_handle))
    # Recheck current human eligibility and exact role/grant/binding after I/O.
    if await _snapshot(files, actor, namespace, key, required=required) != original:
        raise BackingUnavailable("The assigned role changed during verification")
    files._cache_bound(binding.slot_id, files._binding_claim(claims, binding.slot_id), binding.backing_handle, volume)
    return bound, None, original


async def prepare_chat_storage(app, user_id, thread_id):
    if getattr(app.state, "storage_spaces_enabled", False) is not True:
        return None
    _validate_user_id(user_id)
    _validate_thread_id(thread_id)
    base = get_paths()

    def projected(user=None, company=None, reason=None, shared_reason=None, user_reference=None, company_reference=None):
        catalog = getattr(getattr(app.state, "storage_spaces", None), "catalog", None)
        roots = tuple(path for spec in getattr(catalog, "volumes", {}).values() for name in ("mount_path", "image_path") if isinstance((path := getattr(spec, name, None)), Path))
        paths = ChatStoragePaths(base, user_id=user_id, user=user, company=company, user_reference=user_reference, company_reference=company_reference, protected_roots=roots)
        return PreparedChatStorage(paths, paths.mounts(thread_id, user_id=user_id), reason, shared_reason, roots)

    actor = _bound_principal()
    if actor.kind != "human" or actor.subject_id != user_id:
        raise InvalidPrincipal("Chat storage requires its current authenticated human owner")
    files = getattr(app.state, "storage_spaces", None)
    if isinstance(files, SpaceFiles):
        await files.registry._actor(actor)
    if _credential_allowed.get() is not True:
        # Unsupported credentials retain text chat, never resource scopes;
        # they still require the actual current, active human identity.
        return projected(reason="credential-unsupported")
    state = await storage_state(app)
    if not isinstance(files, SpaceFiles) or state.state == "unavailable":
        return projected(reason=state.reason or "admission-failed")
    try:
        user, reason, user_original = await _verified_role(files, actor, "hm.user-space", "human:" + user_id)
    except InvalidPrincipal:
        raise
    except (BackingUnavailable, OSError, ValueError):
        return projected(reason="space-unverified")
    if user is None:
        return projected(reason=reason)
    try:
        company, shared_reason, company_original = await _verified_role(files, actor, "hm.company-space", "company")
    except InvalidPrincipal:
        raise
    except (BackingUnavailable, OSError, ValueError):
        company, shared_reason, company_original = None, "space-unverified", None
    # Final publication checks the exact verified identities, not just existence.
    # A failing Company guard may omit Shared; it cannot bypass the User guard.
    try:
        await _admit_verified(files, actor, user_original, company_original)
    except InvalidPrincipal:
        raise
    except (BackingUnavailable, SpaceConflict, SpaceDenied, SpaceNotFound, OSError, ValueError):
        company, shared_reason = None, "space-unverified"
        try:
            await _admit_verified(files, actor, user_original)
        except InvalidPrincipal:
            raise
        except (BackingUnavailable, SpaceConflict, SpaceDenied, SpaceNotFound, OSError, ValueError):
            return projected(reason="space-unverified")
    return projected(user, company, shared_reason=shared_reason, user_reference=user_original[0][:2], company_reference=company_original[0][:2] if company is not None else None)


async def prepare_chat_request(request, thread_id):
    """Authenticated HTTP boundary; permission/ownership checks precede it."""
    from deerflow.spaces.chat_bridge import get_chat_storage_host

    if request is None:
        host = get_chat_storage_host()
        if host is None:
            if isinstance(get_paths(), ChatStoragePaths):
                raise InvalidPrincipal("Native chat storage requires its owning Gateway host")
            return None
        from deerflow.runtime.user_context import get_effective_user_id

        return await host.prepare_async(get_effective_user_id(), thread_id)
    scope = getattr(request, "scope", None)
    app = scope.get("app") if isinstance(scope, dict) else getattr(request, "app", None)
    if app is None:
        if get_chat_storage_host() is not None or isinstance(get_paths(), ChatStoragePaths):
            raise InvalidPrincipal("A storage request requires its owning Gateway")
        return None  # Minimal feature-off fixtures have no ASGI application.
    if getattr(app.state, "storage_spaces_enabled", False) is not True:
        return None
    from app.gateway.deps import get_current_user_from_request
    from app.gateway.internal_auth import get_trusted_internal_owner_user_id
    from app.gateway.routers.spaces import _actor, _http_error
    from app.gateway.storage_spaces import supports_storage_credentials
    from deerflow.spaces.facade import storage_actor_scope, storage_credential_scope

    if not supports_storage_credentials(request):
        user = await get_current_user_from_request(request)
        owner = get_trusted_internal_owner_user_id(request) or str(user.id)
        with storage_actor_scope(PrincipalRef("human", owner)), storage_credential_scope(False):
            try:
                return await prepare_chat_storage(app, owner, thread_id)
            except InvalidPrincipal as exc:
                raise _http_error(exc) from None
    actor = await _actor(request)
    with storage_actor_scope(actor):
        try:
            return await prepare_chat_storage(app, actor.subject_id, thread_id)
        except InvalidPrincipal as exc:
            raise _http_error(exc) from None


async def check_chat_thread_capacity(prepared, thread_id):
    """Map changed backing facts at the HTTP capacity boundary, preserving 507."""
    from fastapi import HTTPException

    from deerflow.spaces.filesystem import FilesystemUnavailable

    try:
        await run_file_io(prepared.paths.check_thread_capacity, thread_id, user_id=prepared.mounts.user_id)
    except (FilesystemUnavailable, OSError):
        raise HTTPException(503, {"code": "storage-unavailable", "reason": "space-unverified", "message": "Your files are not available right now. Chats run without them."}) from None


def chat_storage_scope_factory(app):
    """Host-owned worker scope; request context and credential denial are inherited."""
    from deerflow.config.paths import paths_scope

    @asynccontextmanager
    async def scope(*, user_id, thread_id):
        prepared = await prepare_chat_storage(app, user_id, thread_id)
        if prepared is None:
            yield None
            return
        await await_drained(run_file_io(prepared.paths.check_thread_capacity, thread_id, user_id=user_id))
        with paths_scope(prepared.paths):
            yield prepared

    return scope


async def close_chat_storage_host(app):
    """Drain speculative builds and owning-loop admission before SQL teardown."""
    from deerflow.sandbox.lease import run_sync_lifecycle_operation
    from deerflow.sandbox.sandbox_provider import get_initialized_sandbox_provider, shutdown_sandbox_provider
    from deerflow.spaces.chat_bridge import remove_chat_storage_host

    host = getattr(app.state, "chat_storage_host", None)
    if host is None:
        return
    try:
        provider = get_initialized_sandbox_provider()
        if provider is not None and getattr(provider, "_chat_storage_host", None) is host:
            await await_drained(run_sync_lifecycle_operation(shutdown_sandbox_provider, expected_provider=provider))
    finally:
        try:
            await await_drained(host.close())
        finally:
            remove_chat_storage_host(host)
            if getattr(app.state, "chat_storage_host", None) is host:
                app.state.chat_storage_host = None
                app.state.chat_storage_scope = None


@asynccontextmanager
async def chat_storage_lifecycle(app, *, enabled):
    """Own the callback through startup failures and the runtime's final drain."""
    from deerflow.spaces.chat_bridge import ChatStorageHost, install_chat_storage_host

    app.state.chat_storage_host = None
    app.state.chat_storage_scope = None
    if enabled:
        from app.gateway.native_screenshots import publish_screenshot

        host = ChatStorageHost(asyncio.get_running_loop(), partial(prepare_chat_storage, app), publish=partial(publish_screenshot, app), pool_runtime_owner=getattr(app.state, "pool_runtime_owner", None))
        install_chat_storage_host(host)
        app.state.chat_storage_host = host
        app.state.chat_storage_scope = chat_storage_scope_factory(app)
    try:
        yield
    finally:
        await await_drained(close_chat_storage_host(app))

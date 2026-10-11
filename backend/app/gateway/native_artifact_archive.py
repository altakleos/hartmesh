"""Bounded ZIP downloads from a freshly qualified human User Space."""

import re
from contextlib import AsyncExitStack

from fastapi import HTTPException
from starlette.responses import StreamingResponse

from app.gateway.agent_conversations import reject_legacy_binding
from app.gateway.artifact_archive import MAX_FILES, ArtifactArchiveError, _member_entry, reserved_dir_names
from app.gateway.chat_files import files_unavailable
from app.gateway.deps import get_run_manager
from app.gateway.native_file_sources import NativeFileSources
from app.gateway.routers.spaces import _ERRORS, _actor, _http_error
from deerflow.authz.sandbox_authz import safe_app_config_async
from deerflow.config.paths import get_paths
from deerflow.runtime import ConflictError, ThreadOperationKind
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_gate import conversation_gate
from deerflow.spaces.chat_paths import ChatStoragePaths
from deerflow.spaces.facade import storage_actor_scope
from deerflow.utils.file_io import await_drained, run_file_io


class NativeArchiveResponse(StreamingResponse):
    """Refresh authority before response headers; own cleanup on every exit."""

    def __init__(self, result, sources, run_id):
        self.result, self.sources = result, sources

        async def chunks():
            while chunk := await await_drained(run_file_io(result.file.read, 1 << 20)):
                yield chunk

        safe_run_id = re.sub(r"[^A-Za-z0-9_-]", "", run_id)[:32] or "run"
        super().__init__(
            chunks(),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="artifacts-{safe_run_id}.zip"',
                "Content-Length": str(result.size),
                "Cache-Control": "private, no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def __call__(self, scope, receive, send):
        try:
            with storage_actor_scope(self.sources.actor):
                try:
                    await self.sources.refresh()
                except BackingUnavailable:
                    raise files_unavailable("space-unverified") from None
                except _ERRORS as exc:
                    raise _http_error(exc) from None
                await super().__call__(scope, receive, send)
        finally:
            await await_drained(run_file_io(self.result.file.close))


async def native_run_archive(request, thread_id, run_id):
    from app.gateway.routers.thread_runs import _archive_presented_paths, _build_archive_without_abandoning_worker, _require_run_visible_to_scope

    actor = await _actor(request)
    result = None
    try:
        with storage_actor_scope(actor):
            await reject_legacy_binding(request, thread_id)
            sources = await NativeFileSources.prepare(request, actor)
            await _require_run_visible_to_scope(run_id, thread_id, request)
            virtual_paths = list(dict.fromkeys(await _archive_presented_paths(thread_id, run_id, request)))
            if len(virtual_paths) > MAX_FILES:
                raise ArtifactArchiveError(f"An artifact archive can contain at most {MAX_FILES} files", 413)
            config = await safe_app_config_async()
            custom = getattr(getattr(config, "tool_output", None), "storage_subdir", None)
            extra = {custom} if isinstance(custom, str) else set()
            entries = {virtual: _member_entry(virtual, reserved_dir_names(extra)) for virtual in virtual_paths}
            paths = ChatStoragePaths(get_paths(), user_id=actor.subject_id, user=sources.user, user_reference=sources.snapshot[0][:2])
            async with get_run_manager(request).reserve_thread_operation(thread_id, kind=ThreadOperationKind.artifact_archive, user_id=actor.subject_id), AsyncExitStack() as stack:
                await stack.enter_async_context(conversation_gate(paths, thread_id, user_id=actor.subject_id))
                await sources.refresh()
                descriptors = {}
                for virtual, entry in entries.items():
                    relative = f"conversations/{thread_id}/user-data/outputs/{entry}"
                    descriptors[virtual] = await stack.enter_async_context(sources.open_file(relative))
                result = await _build_archive_without_abandoning_worker(None, None, virtual_paths, extra_reserved_dir_names=extra, descriptor_sources=descriptors)
                await sources.refresh()
            response = NativeArchiveResponse(result, sources, run_id)
            result = None  # The response owns the private export artifact.
            return response
    except ConflictError:
        raise HTTPException(409, "Artifacts are currently being modified; try again shortly") from None
    except ArtifactArchiveError as exc:
        raise HTTPException(exc.status_code, exc.detail) from None
    except BackingUnavailable:
        raise files_unavailable("space-unverified") from None
    except _ERRORS as exc:
        raise _http_error(exc) from None
    finally:
        if result is not None:
            await await_drained(run_file_io(result.file.close))

"""Bind the personal chat's file tools to explicit references and live Gateway admission."""

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import PurePosixPath

from fastapi import HTTPException, Request
from pydantic import ValidationError

from app.gateway.auth.mode import account_refusal
from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_SESSION, get_auth_disabled_user, is_auth_disabled
from app.gateway.authz import AuthContext, resolve_route_permissions
from app.gateway.conversation_access import conversation_output_limit
from app.gateway.conversation_file_import import ConversationSource, _SourceRole, admit_presented_source, capture_source, conversation_scope, read_presented, receipt_paths, require_presented_file
from app.gateway.conversation_file_upload import import_into_chat
from app.gateway.deps import get_local_provider, get_run_store, get_thread_store
from app.gateway.routers.thread_runs import _parse_run_page_created_at
from deerflow.runtime.runs.capture_io import current_capture_io_owner, run_capture_io
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.base import format_run_cursor_created_at
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.tools.conversation_import import ConversationFileAccess, ImportReceipt, file_tool_json
from deerflow.utils.file_io import run_file_io

logger = logging.getLogger(__name__)
_PERMISSIONS = {"threads:read", "threads:write", "runs:read"}
_PAGE_FILES = 8
_PAGE_CHARS = 8000
_FINISHED = (RunStatus.success, RunStatus.error, RunStatus.timeout, RunStatus.interrupted)


def _unavailable():
    return HTTPException(404, "Referenced conversation or file is unavailable")


def _incarnation(record):
    return (record.get("incarnation") or record.get("created_at")) if record else None


async def _file_page(request, thread_id, scope, *, before_created_at, before_run_id, offset, limit):
    """Scan at most three answers and stat at most eight presented files per page."""
    if (before_created_at is None) != (before_run_id is None) or type(offset) is not int or offset < 0 or (offset and before_run_id is None):
        raise _unavailable()
    before = _parse_run_page_created_at(before_created_at) if before_created_at is not None else None
    owner = scope[1] if scope[0] == "ordinary" else None
    store = get_run_store(request)
    rows = await store.list_by_thread(thread_id, user_id=owner, limit=4, before_created_at=before, before_run_id=before_run_id)
    if offset:
        anchor = await store.get(before_run_id, user_id=owner)
        if not anchor or anchor.get("thread_id") != thread_id or format_run_cursor_created_at(anchor["created_at"]) != format_run_cursor_created_at(before):
            raise _unavailable()
        rows = [anchor, *rows]
    result = []
    last_page = None
    too_small = {"files": [], "has_more": False, "status": "output_budget_too_small"}

    def page(anchor=None, index=None):
        value = {
            "files": list(result),
            "has_more": anchor is not None,
            "next_before_created_at": format_run_cursor_created_at(anchor["created_at"]) if anchor else None,
            "next_before_run_id": anchor["run_id"] if anchor else None,
            "next_offset": index,
        }

        return value if len(file_tool_json(value)) <= limit else None

    for index, record in enumerate(rows[:3]):
        start = offset if index == 0 else 0
        if record["thread_id"] != thread_id or (owner is not None and record["user_id"] != owner) or record.get("operation_kind", "run") != "run" or record["status"] not in _FINISHED:
            if start:
                raise _unavailable()
            continue
        run_id = record["run_id"]
        paths = await receipt_paths(request, thread_id, run_id, scope)
        if start > len(paths):
            raise _unavailable()
        for pos in range(start, len(paths)):
            path = paths[pos]
            entry = {"run_id": run_id, "path": path, "filename": PurePosixPath(path).name}

            if len(result) >= _PAGE_FILES:
                return last_page
            try:
                size, _ = await read_presented(request, thread_id, path)
            except HTTPException as exc:
                if exc.status_code != 404:
                    raise
                size = None
            result.append({**entry, "available": size is not None, "size": size})
            # Reserve this row's actual continuation. Never truncate an identifier.
            candidate = page(record, pos + 1)
            if candidate is None:
                return last_page or too_small
            last_page = candidate
    complete = page(rows[2], 0) if len(rows) > 3 else page()
    return complete or last_page or too_small


async def bind_conversation_files(*, request, source_ids, reader, destination, run, config):
    """Keep no HTTP credentials. Bind the created thread's actual incarnation before its run starts."""
    owner_id = run.user_id
    source = getattr(request.state, "auth_source", None)
    auth = getattr(request.state, "auth", None)
    user = getattr(request.state, "user", None)
    destination_incarnation = _incarnation(destination)
    version = getattr(user, "token_version", None)
    if (
        type(version) is not int
        or version < 0
        or not destination_incarnation
        or not owner_id
        or destination.get("user_id") != owner_id
        or destination.get("thread_id") != run.thread_id
        or source not in (AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED)
        or auth is None
        or not auth.is_authenticated
        or str(getattr(user, "id", None)) != owner_id
        or str(auth.user.id) != owner_id
        or not _PERMISSIONS.issubset(auth.permissions)
    ):
        return reader
    destination_id = destination["thread_id"]
    app = request.app
    expected = ("ordinary", owner_id, destination_incarnation)
    loop = asyncio.get_running_loop()
    run_lifetime = current_capture_io_owner()
    limit = min(_PAGE_CHARS, conversation_output_limit(config, "list_conversation_files") or _PAGE_CHARS)

    def active():
        return (run_lifetime is None or not run_lifetime.retired) and run.status in (RunStatus.pending, RunStatus.running) and not run.abort_event.is_set()

    async def current():
        if not active():
            raise _unavailable()
        if source == AUTH_SOURCE_AUTH_DISABLED:
            actor = get_auth_disabled_user() if owner_id == AUTH_DISABLED_USER_ID and is_auth_disabled() else None
        else:
            actor = await get_local_provider().get_user(owner_id)
        if (
            actor is None
            or str(actor.id) != owner_id
            or actor.token_version != version
            or actor.needs_setup
            or (source == AUTH_SOURCE_SESSION and await run_file_io(account_refusal, actor) is not None)
            or not _PERMISSIONS.issubset(await resolve_route_permissions(actor, is_internal=False, strict=True))
        ):
            raise _unavailable()
        return actor

    @asynccontextmanager
    async def operation():
        actor = await current()
        ctx = Request({"type": "http", "app": app, "state": {}, "method": "POST", "path": "/api/threads", "headers": [], "query_string": b""})
        ctx.state.user = actor
        ctx.state.auth_source = source
        ctx.state.auth = AuthContext(actor, sorted(_PERMISSIONS))
        token = set_current_user(actor)
        try:
            yield ctx
        finally:
            reset_current_user(token)

    # The ordinary transcript reader's ownership ceiling also applies to files.
    # Capture the source object here so a deleted, re-created reference cannot inherit the grant.
    try:
        bound = {}
        async with operation() as ctx:
            if await conversation_scope(ctx, destination_id) != expected:
                return reader
            for thread_id in source_ids:
                if thread_id == destination_id:
                    continue
                row = await get_thread_store(ctx).get(thread_id, user_id=owner_id)
                if row and row.get("user_id") == owner_id and _incarnation(row):
                    try:
                        bound[thread_id] = (_incarnation(row), await conversation_scope(ctx, thread_id))
                    except HTTPException:
                        continue
    except Exception:
        logger.warning("Cannot bind referenced file access", exc_info=True)
        return reader

    async def authorized(ctx, thread_id):
        await current()
        if await conversation_scope(ctx, destination_id) != expected:
            raise _unavailable()
        incarnation, scope = bound[thread_id]
        row = await get_thread_store(ctx).get(thread_id, user_id=owner_id)
        if not row or row.get("user_id") != owner_id or _incarnation(row) != incarnation or await conversation_scope(ctx, thread_id) != scope:
            raise _unavailable()
        return row, scope

    async def invoke(thread_id, fn):
        if not isinstance(thread_id, str) or thread_id not in bound or not active() or asyncio.get_running_loop() is not loop:
            return None

        async def perform():
            async with operation() as ctx:
                return await fn(ctx)

        try:
            return await run_capture_io(perform)
        except (HTTPException, ValidationError):
            return None
        except Exception:
            logger.warning("Referenced file access unavailable", exc_info=True)
            return None

    async def list_files(*, thread_id, before_created_at=None, before_run_id=None, offset=0):
        async def listing(ctx):
            _, scope = await authorized(ctx, thread_id)
            await admit_presented_source(ctx, thread_id, scope)
            role = await _SourceRole.capture(ctx, shared=False) if scope[0] == "ordinary" else None
            page = await _file_page(ctx, thread_id, scope, before_created_at=before_created_at, before_run_id=before_run_id, offset=offset, limit=limit)
            await authorized(ctx, thread_id)
            if role:
                await role.recheck()
            await admit_presented_source(ctx, thread_id, scope)
            return page

        return await invoke(thread_id, listing)

    async def import_file(*, thread_id, run_id, path):
        async def copy(ctx):
            await authorized(ctx, thread_id)
            selection = ConversationSource(thread_id=thread_id, run_id=run_id, path=path)
            captured = None

            async def recheck():
                row, scope = await authorized(ctx, thread_id)
                await require_presented_file(ctx, selection, scope)
                if captured is None:
                    raise _unavailable()
                await captured.recheck()
                return row

            async def capture(req, selected, *, maximum_bytes):
                nonlocal captured
                captured = await capture_source(req, selected, maximum_bytes=maximum_bytes)
                return replace(captured, recheck=recheck)

            result = await import_into_chat(request=ctx, thread_id=destination_id, source=selection, config=config, capture=capture, expected_scope=expected)
            # The destination owns a committed copy. Its attribution still requires live access.
            row = await recheck()
            return ImportReceipt(kind="conversation", source_title=row.get("display_name"), **{key: result[key] for key in ("filename", "virtual_path", "size", "sha256")})

        return await invoke(thread_id, copy)

    return ConversationFileAccess(read=reader, list_files=list_files, import_file=import_file)

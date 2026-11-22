"""Reuse completed ordinary attachments by content without recording an import history."""

import asyncio
import hashlib
import os
import re
from contextlib import asynccontextmanager

from fastapi import HTTPException, Request

from app.gateway.authz import require_permission
from app.gateway.conversation_file_import import FileSource, capture_source, conversation_scope, require_scope
from app.gateway.import_rollback import ImportRollback
from app.gateway.routers import uploads
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.threads import ThreadCreateRequest, _create_thread_locked
from app.gateway.routers.uploads import _get_upload_limits
from app.gateway.upload_ingestion import ThreadUploadIngestionService
from deerflow.files.store import SafeFileAccessUnavailable, StoreError, open_directory_source, open_regular_source
from deerflow.runtime.goal import goal_thread_lock
from deerflow.uploads.companions import resolve_companion
from deerflow.uploads.manager import numbered_upload_name
from deerflow.utils.file_io import await_drained, run_file_io


@asynccontextmanager
async def _import_gate(session):
    if session._chat_upload is not None:
        # Native ingestion already holds the stable per-owner conversation gate.
        yield
        return
    try:
        import fcntl
    except ImportError:
        raise HTTPException(501, "Safe concurrent attachment import is unavailable") from None
    fd = None

    def open_dir():
        nonlocal fd
        fd = open_directory_source(session._uploads_dir)

    try:
        await await_drained(run_file_io(open_dir))
        while True:
            try:
                await await_drained(run_file_io(fcntl.flock, fd, fcntl.LOCK_EX | fcntl.LOCK_NB))
                break
            except BlockingIOError:
                await asyncio.sleep(0.02)
        session.import_rollback = ImportRollback(fd)
        try:
            yield
        finally:
            await await_drained(run_file_io(session.import_rollback.close))
            session.import_rollback = None
    finally:
        if fd is not None:
            await await_drained(run_file_io(os.close, fd))


def _variant_order(original, name):
    if name.casefold() == original.casefold():
        return 0
    for counter in re.findall(r"_([1-9][0-9]{0,8})(?=\.|$)", name):
        if numbered_upload_name(original, int(counter)).casefold() == name.casefold():
            return int(counter)
    return None


def _companion_for_retry(original, *, synchronize, maximum_bytes):
    path = resolve_companion(original)
    if path is None or not synchronize:
        return path, None
    try:
        with os.fdopen(open_regular_source(path), "rb") as file:
            stat = os.fstat(file.fileno())
            if stat.st_size > maximum_bytes:
                return None, None
            content = file.read(stat.st_size + 1)
            current = path.stat(follow_symlinks=False)
            if len(content) != stat.st_size or (current.st_dev, current.st_ino, current.st_ctime_ns) != (stat.st_dev, stat.st_ino, stat.st_ctime_ns) or resolve_companion(original) != path:
                return None, None
            return path, content
    except (FileNotFoundError, StoreError):
        return None, None


async def _reuse(session, captured):
    native = session._chat_upload
    if native is not None:
        listing = await native.access.list_uploads(session._request)
    else:
        listing = await run_file_io(uploads.list_files_in_dir, session._uploads_dir)
    names = {item["filename"] for item in listing["files"]}
    session._seen_filenames.update(names)
    original = uploads.normalize_filename(captured.name)
    candidates = sorted((index, name) for name in names if (index := _variant_order(original, name)) is not None)
    for _, name in candidates:
        virtual = uploads.upload_virtual_path(name)

        def matches(file):
            before = os.fstat(file.fileno())
            if before.st_size != len(captured.content):
                return False
            digest = hashlib.sha256()
            remaining = len(captured.content)
            while remaining:
                chunk = file.read(min(remaining, 1 << 20))
                if not chunk:
                    return False
                remaining -= len(chunk)
                digest.update(chunk)
            after = os.fstat(file.fileno())
            return not file.read(1) and (before.st_ctime_ns, before.st_mtime_ns) == (after.st_ctime_ns, after.st_mtime_ns) and digest.hexdigest() == captured.sha256

        try:
            if native is not None:
                same = await native.access.read_with(session._request, virtual, matches)
            else:

                def legacy_match():
                    with os.fdopen(open_regular_source(session._uploads_dir / name), "rb") as file:
                        return matches(file)

                same = await await_drained(run_file_io(legacy_match))
        except SafeFileAccessUnavailable:
            raise HTTPException(501, "Safe file reads are unavailable") from None
        except (StoreError, FileNotFoundError):
            continue
        except HTTPException as exc:
            if exc.status_code == 404:
                continue
            raise
        if same:
            info = {"filename": name, "size": len(captured.content), "virtual_path": virtual, "artifact_url": uploads.upload_artifact_url(session._thread_id, name)}
            if name != original:
                info["original_filename"] = original
            if native is None:
                synchronize = session._sync_to_sandbox and session._sandbox is not None
                companion, markdown = await run_file_io(_companion_for_retry, session._uploads_dir / name, synchronize=synchronize, maximum_bytes=session.limits.max_file_size)
                if companion is not None:
                    info.update(markdown_file=companion.name, markdown_virtual_path=uploads.upload_virtual_path(companion.name), markdown_artifact_url=uploads.upload_artifact_url(session._thread_id, companion.name))
                if synchronize:
                    await session.before_publish()
                    await await_drained(run_file_io(session._sandbox.update_file, virtual, captured.content))
                    if markdown is not None:
                        await session.before_publish()
                        await await_drained(run_file_io(session._sandbox.update_file, info["markdown_virtual_path"], markdown))
            return info
    return None


async def attach_captured(request, thread_id, owner, config, captured, *, recheck):
    async with ThreadUploadIngestionService(request=request, thread_id=thread_id, user_id=owner, app_config=config, before_publish=recheck) as session:
        async with _import_gate(session):
            await recheck()
            committed = False

            async def finalize():
                nonlocal committed
                await session.finalize()
                committed = True

            try:
                info = await _reuse(session, captured)
                if info is None:

                    async def chunks():
                        for offset in range(0, len(captured.content), 1 << 20):
                            yield captured.content[offset : offset + (1 << 20)]

                    info = await session.ingest_chunks(chunks(), display_name=captured.name, expected_size=len(captured.content))
                await recheck()
                await await_drained(finalize())
            except BaseException:
                if not committed:
                    await session.cleanup_written()
                raise
    return {key: value for key, value in info.items() if key in {"filename", "size", "virtual_path", "artifact_url", "original_filename", "markdown_file", "markdown_virtual_path", "markdown_artifact_url"}}


@require_permission("threads", "write", owner_check=True)
async def import_into_chat(request: Request, thread_id: str, source: FileSource, config, *, create_destination=False, capture=None, expected_scope=None):
    owner = acting_user_id(request)
    scope = await conversation_scope(request, thread_id, allow_new=create_destination)
    if expected_scope is not None and scope != expected_scope:
        raise HTTPException(404, "Thread not found")
    if scope is not None and scope[0] != "ordinary":
        raise HTTPException(501, "Use the AI agent's Home for attachments")
    limits = _get_upload_limits(config)
    read_source = capture_source if capture is None else capture
    captured = await read_source(request, source, maximum_bytes=min(limits.max_file_size, limits.max_total_size))
    async with goal_thread_lock(thread_id):
        await captured.recheck()
        if scope is None:
            record = await _create_thread_locked(ThreadCreateRequest(thread_id=thread_id), request, thread_id, exact_owner=owner)
            scope = ("ordinary", owner, record.get("incarnation") or record["created_at"])

        async def recheck():
            await captured.recheck()
            await require_scope(request, thread_id, scope)

        await recheck()
        info = await attach_captured(request, thread_id, owner, config, captured, recheck=recheck)
        # The destination owns the finished bytes. A changed audience may not receive the receipt.
        await require_scope(request, thread_id, scope)
    return {**info, "sha256": captured.sha256, "source": source.model_dump()}

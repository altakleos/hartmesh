"""Publish and remove independently recoverable Shared files for one snapshot."""

import asyncio
import hashlib
import hmac
import json
import os
import tempfile
import time
from typing import Literal

from fastapi import HTTPException, Request, Response

from app.gateway.agent_snapshot_source import AgentSnapshotSource
from app.gateway.auth.config import get_auth_config
from app.gateway.authz import require_permission
from app.gateway.conversation_file_import import conversation_scope, read_presented, receipt_paths, require_scope
from app.gateway.conversation_snapshots import _manifest_document, _open_shared, read_snapshot
from app.gateway.deps import get_run_manager, get_run_store, get_shared_publications_repo, get_thread_store
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.shared import PublishRequest, _publisher_names, publish_snapshot_file, remove_shared
from app.gateway.snapshot_manifest import FILE_LIMIT, MANIFEST_SOURCE, TOTAL_LIMIT, TRANSCRIPT_SOURCE, Digest, SnapshotMember, SnapshotModel, digest_of_bytes, freeze_manifest, generated_name
from app.gateway.storage_features import storage_feature
from app.gateway.transcript import read_conversation
from deerflow.files.store import DescriptorSource
from deerflow.runtime import ThreadOperationKind
from deerflow.runtime.runs.manager import ConflictError
from deerflow.spaces.workflows import try_storage_locks_once
from deerflow.utils.file_io import await_drained, run_file_io
from deerflow.utils.thread_id import ThreadId


class AudienceAcknowledgement(SnapshotModel):
    snapshot: Digest
    audience: Literal["company"]


class ShareRequest(SnapshotModel):
    thread_id: ThreadId
    snapshot: Digest
    acknowledge: AudienceAcknowledgement | None = None


def _refuse(code, status=409, **details):
    return HTTPException(status, {"code": code, **details})


async def share_scope(request, thread_id):
    agent_source = await AgentSnapshotSource.for_thread(request, thread_id)
    if agent_source is not None:
        return agent_source.scope, agent_source
    scope = await conversation_scope(request, thread_id)
    if scope[0] != "ordinary":
        raise _refuse("conversation-unavailable", 404)
    return scope, None


async def require_share_scope(request, thread_id, expected, agent_source):
    if agent_source is None:
        await require_scope(request, thread_id, expected)
    else:
        await agent_source.recheck()


@require_permission("threads", "write")
@storage_feature("hm.shared", export=True)
async def inspect_destination(request: Request, plans: list[str], *, preflight: bool = False):
    repo = get_shared_publications_repo(request)
    records = await repo.live_publications(paths=plans)

    def check():
        for path in plans:
            try:
                fd = _open_shared(path)
            except FileNotFoundError:
                continue
            os.close(fd)
            if not records.get(path):
                raise _refuse("unrecorded-file", path=path)

    if preflight:
        await run_file_io(check)
    return records


async def _read_output(request, thread_id, path, limit, *, changed=False, agent_source=None):
    try:
        if agent_source is not None:
            data, _ = await agent_source.read(path, limit)
        else:
            data, _ = await read_presented(request, thread_id, path, maximum_bytes=limit)
    except HTTPException as exc:
        if exc.status_code in {404, 409} or (changed and exc.status_code == 413):
            raise _refuse("snapshot-file-changed") from None
        raise
    return data


async def prepare(request, thread_id, scope, agent_source, *, authors=None):
    owner = None if agent_source else scope[1]
    record = await get_thread_store(request).get(thread_id, user_id=acting_user_id(request))
    if record is None:
        raise _refuse("conversation-unavailable", 404)
    runs = set()
    human_runs = set()
    conversation = await read_conversation(request, thread_id, record, user_id=owner, visible_runs=runs, **({"visible_human_runs": human_runs} if authors is not None else {}))
    if conversation is None:
        raise _refuse("conversation-empty")
    paths = set()
    for run in sorted(runs):
        row = await get_run_store(request).get(run, user_id=owner)
        if not row or row.get("thread_id") != thread_id or (owner is not None and row.get("user_id") != owner) or row.get("operation_kind", "run") != "run" or row.get("status") not in {"success", "error", "timeout", "interrupted"}:
            continue
        if authors is not None and run in human_runs and (sender := row.get("user_id")):
            authors.add(sender)
        paths.update(await receipt_paths(request, thread_id, run, scope))
        if len(paths) > 64:
            raise _refuse("snapshot-file-count", 413)
    files = []
    total = 0
    for path in sorted(paths):
        content = await _read_output(request, thread_id, path, min(FILE_LIMIT, TOTAL_LIMIT - total), agent_source=agent_source)
        total += len(content)
        files.append(SnapshotMember(path="files/" + path.removeprefix("/mnt/user-data/outputs/"), size=len(content), sha256=await run_file_io(digest_of_bytes, content)))
    await require_share_scope(request, thread_id, scope, agent_source)
    try:
        snapshot = await run_file_io(freeze_manifest, conversation.title, conversation.messages, files, with_agent=agent_source.agent if agent_source else None)
    except ValueError:
        raise _refuse("snapshot-limit", 413) from None
    if any(len((snapshot.folder + "/" + entry.path).rsplit("/", 1)[0]) > 1024 for entry in snapshot.manifest.files):
        raise _refuse("snapshot-path-limit", 413)
    return snapshot


def _stopping(request):
    flag = getattr(request.app.state, "snapshot_stopping", None)
    return flag is not None and flag.is_set()


class SharedWait:
    def __init__(self, request):
        self.request = request
        self.remaining = 60.0

    async def call(self, operation, **values):
        while not _stopping(self.request):
            started = time.monotonic()
            try:
                with try_storage_locks_once():
                    return await operation(**values)
            except HTTPException as exc:
                if not isinstance(exc.detail, dict) or exc.status_code != 409 or exc.detail.get("code") != "storage-busy":
                    raise
            remaining = self.remaining - (time.monotonic() - started)
            if remaining > 0:
                await asyncio.sleep(min(remaining, 0.25))
            self.remaining -= time.monotonic() - started
            if self.remaining <= 0:
                raise _refuse("shared-busy")
        raise _refuse("gateway-stopping", 503)


async def draft(request, thread_id):
    scope, agent_source = await share_scope(request, thread_id)
    # Apply current Shared publishing permission even when there are no files.
    await inspect_destination(request=request, plans=[])
    authors = set() if agent_source else None
    frozen = await prepare(request, thread_id, scope, agent_source, authors=authors)
    path = frozen.folder + "/conversation.json"
    existing = (await inspect_destination(request=request, plans=[path])).get(path)
    shared = existing and existing["from_path"] == MANIFEST_SOURCE and existing["sha256"] == frozen.digest and await exact_manifest(request=request, row=existing)
    audience = await agent_source.audience(authors) if agent_source else None
    await require_share_scope(request, thread_id, scope, agent_source)
    return {
        **({"with": agent_source.agent, "audience": audience} if agent_source else {}),
        "title": frozen.manifest.title,
        "message_count": len(frozen.manifest.messages),
        "snapshot": frozen.digest,
        "files": frozen.manifest.files,
        "already_shared": bool(shared),
        "publication_id": existing["publication_id"] if shared else None,
    }


@require_permission("threads", "write")
@storage_feature("hm.shared", export=True)
async def exact_manifest(request: Request, row: dict):
    current = await get_shared_publications_repo(request).live_publication(row["path"])
    return bool(current and current["publication_id"] == row["publication_id"] and await run_file_io(_manifest_document, current))


def _undo_mac(key, person, manifest_id, ids):
    content = json.dumps(["conversation-snapshot-undo-v1", person, manifest_id, ids], separators=(",", ":")).encode()
    return hmac.new(key.encode(), content, hashlib.sha256).hexdigest()


async def publish_snapshot(request, body):
    scope, agent_source = await share_scope(request, body.thread_id)
    if agent_source is not None and (body.acknowledge is None or body.acknowledge.snapshot != body.snapshot):
        raise _refuse("snapshot-acknowledgement-required")
    wait = SharedWait(request)
    await wait.call(inspect_destination, request=request, plans=[])
    try:
        async with get_run_manager(request).reserve_thread_operation(body.thread_id, kind=ThreadOperationKind.branch, user_id=acting_user_id(request)):
            await require_share_scope(request, body.thread_id, scope, agent_source)
            return await _share(request, body, scope, agent_source, wait)
    except ConflictError:
        raise _refuse("finish-current-turn") from None


async def _share(request, body, scope, agent_source, wait):
    frozen = await prepare(request, body.thread_id, scope, agent_source)
    if frozen.digest != body.snapshot:
        raise _refuse("snapshot-changed")
    docs = frozen.manifest
    files = [(f, "/mnt/user-data/outputs/" + f.path.removeprefix("files/"), None) for f in docs.files]
    files += [(docs.transcript, TRANSCRIPT_SOURCE, frozen.transcript), (SnapshotMember(path="conversation.json", size=len(frozen.document), sha256=frozen.digest), MANIFEST_SOURCE, frozen.document)]
    absolute = [frozen.folder + "/" + file.path for file, _, _ in files]
    manifest = (await wait.call(inspect_destination, request=request, plans=absolute[-1:])).get(absolute[-1])
    if manifest and manifest["sha256"] == frozen.digest and await wait.call(exact_manifest, request=request, row=manifest):
        await require_share_scope(request, body.thread_id, scope, agent_source)
        return {"publication_id": manifest["publication_id"], "created_publication_ids": [], "undo_receipt": None, "already_shared": True}
    await wait.call(inspect_destination, request=request, plans=absolute, preflight=True)
    repo = get_shared_publications_repo(request)
    added = []
    current_id = None
    try:
        for (member, source, content), path in zip(files, absolute, strict=True):
            await require_share_scope(request, body.thread_id, scope, agent_source)
            write = PublishRequest(path=source, folder=path.rsplit("/", 1)[0], thread_id=body.thread_id)
            response = Response(status_code=201)
            generated = content is not None
            if not generated:
                content = await _read_output(request, body.thread_id, source, member.size, changed=True, agent_source=agent_source)
                if len(content) != member.size or await run_file_io(digest_of_bytes, content) != member.sha256:
                    raise _refuse("snapshot-file-changed")

            # A name in a workspace can be replaced between reads. Only these
            # confirmed bytes may be visible, even if the Gateway stops mid-copy.
            def stage():
                fd = tempfile.TemporaryFile()
                try:
                    fd.write(content)
                    fd.flush()
                    return fd
                except BaseException:
                    fd.close()
                    raise

            with await await_drained(run_file_io(stage)) as fd:
                descriptor = DescriptorSource(fd.fileno(), generated_name(source) if generated else source.rsplit("/", 1)[-1])
                item = await wait.call(publish_snapshot_file, body=write, request=request, response=response, generated=descriptor, **({"agent_source": agent_source} if agent_source else {}))
            current_id = item.publication_id
            if response.status_code == 201:
                added.append((item.path, current_id))
            live = await repo.live_publication(item.path)
            if not live or live["publication_id"] != current_id or live["sha256"] != member.sha256 or live["size"] != member.size:
                raise _refuse("snapshot-file-changed")
            if item.path != path or (generated and live["from_path"] != source):
                holder = await repo.live_publication(path) or live
                names = await _publisher_names([holder])
                raise _refuse("snapshot-file-conflict", path=path, publisher=names.get(holder["published_by"]))
    except HTTPException as exc:
        # A busy or stopping host leaves its recorded members for the next
        # press. Roll back only identities newly admitted by this request.
        code = exc.detail.get("code") if isinstance(exc.detail, dict) else None
        if code in {"snapshot-file-conflict", "snapshot-file-changed"}:
            for path, identifier in reversed(added):
                await wait.call(remove_shared, request=request, path=path, expected_publication_id=identifier)
        raise
    await require_share_scope(request, body.thread_id, scope, agent_source)
    ids = [identifier for _, identifier in added]
    signing = await run_file_io(get_auth_config)
    return {"publication_id": current_id, "created_publication_ids": ids, "already_shared": not ids, "undo_receipt": _undo_mac(signing.jwt_secret, acting_user_id(request), current_id, ids) if ids else None}


async def remove_snapshot(request, publication_id, *, undo=None):
    wait = SharedWait(request)
    snapshot = await wait.call(read_snapshot, request=request, publication_id=publication_id)
    if not snapshot.can_remove:
        raise _refuse("snapshot-remove-forbidden", 403)
    repo = get_shared_publications_repo(request)
    manifest = await repo.publication(publication_id)
    if not manifest or manifest["removed_at"] is not None:
        raise _refuse("snapshot-unavailable", 404)
    allowed = None
    if undo is not None:
        signing = await run_file_io(get_auth_config)
        expected = _undo_mac(signing.jwt_secret, acting_user_id(request), publication_id, undo.publication_ids)
        if not hmac.compare_digest(expected, undo.undo_receipt):
            raise _refuse("invalid-undo", 409)
        allowed = set(undo.publication_ids)
    removed = []
    retained = []
    for entry in [snapshot.transcript, *snapshot.files, None]:
        identifier = publication_id if entry is None else entry.publication_id
        path = manifest["path"] if entry is None else entry.shared_path
        if not identifier or (allowed is not None and identifier not in allowed):
            continue
        active = await repo.live_publication(manifest["path"])
        if not active or active["publication_id"] != publication_id:
            raise _refuse("snapshot-unavailable", 404)
        row = await repo.live_publication(path)
        if not row or row["publication_id"] != identifier:
            continue
        if row["published_by"] != manifest["published_by"]:
            names = await _publisher_names([row])
            retained.append({"path": path, "publisher": names.get(row["published_by"])})
            continue
        await wait.call(remove_shared, request=request, path=path, expected_publication_id=identifier)
        removed.append(identifier)
    return {"removed_publication_ids": removed, "retained": retained}

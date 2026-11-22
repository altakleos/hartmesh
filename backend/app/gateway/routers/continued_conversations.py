"""Create a private, independently owned conversation from a published snapshot."""

import json
import logging
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from langchain_core.messages import AIMessage, HumanMessage
from pydantic import BaseModel, ConfigDict

from app.gateway.authz import require_permission
from app.gateway.conversation_file_import import SharedPublicationSource, conversation_scope, require_scope
from app.gateway.conversation_file_upload import import_into_chat
from app.gateway.conversation_snapshots import Answerer, SharedSnapshot, capture_snapshot_member, read_snapshot
from app.gateway.deps import get_config, get_run_event_store, get_thread_store
from app.gateway.history_seed import reserve_history_seed, write_history_seed
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.conversation_files import ImportedAttachment
from app.gateway.routers.threads import _CHECKPOINT_MODE_ERRORS, ThreadCreateRequest, _checkpoint_mode_http_error, _create_thread_locked
from app.gateway.services import build_checkpoint_state_mutation_accessor
from deerflow.config.app_config import AppConfig
from deerflow.runtime.goal import goal_thread_lock
from deerflow.runtime.journal import build_shared_history_seed_events
from deerflow.runtime.runs.manager import ConflictError
from deerflow.utils.messages import SHARED_HISTORY_KEY
from deerflow.utils.thread_id import ThreadId

logger = logging.getLogger(__name__)
router = APIRouter(tags=["shared conversations"])


class RequestedAnswerer(Answerer):
    kind: Literal["assistant"]


class ContinueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    answerer: RequestedAnswerer


class CopiedFile(BaseModel):
    path: str
    status: Literal["copied"] = "copied"
    attachment: ImportedAttachment


class SkippedFile(BaseModel):
    path: str
    status: Literal["not_copied"] = "not_copied"
    reason: Literal["no_longer_shared", "access_denied", "changed", "too_large", "storage_full", "unavailable"]


class ContinuedConversation(BaseModel):
    thread_id: str
    answerer: Answerer
    files: list[CopiedFile | SkippedFile]


class SharedHistory(BaseModel):
    publication_id: str
    message_ids: list[str]


def _stable_id(*parts: str | int) -> str:
    return str(uuid5(NAMESPACE_URL, json.dumps(parts, ensure_ascii=True, separators=(",", ":"))))


async def _read_seed(request: Request, thread_id: str) -> tuple[str | None, list[str], bool]:
    """Return the verified prefix and whether another turn follows it."""
    store = get_run_event_store(request)
    ids = []
    publication = None
    cursor = 0
    while True:
        limit = 128 if ids else 1
        page = await store.list_messages(thread_id, after_seq=cursor, limit=limit, user_id=None)
        for row in page:
            metadata = row.get("metadata")
            mark = metadata.get(SHARED_HISTORY_KEY) if isinstance(metadata, dict) else None
            content = row.get("content")
            if not isinstance(mark, str) or not mark or publication not in (None, mark):
                return publication, ids, True
            if not row["run_id"].startswith(f"branch-seed-{thread_id}-") or row["event_type"] not in ("llm.human.input", "llm.ai.response"):
                return None, [], True
            message_id = _stable_id("shared-turn", thread_id, mark, len(ids))
            kwargs = content.get("additional_kwargs") if isinstance(content, dict) else None
            if not isinstance(kwargs, dict) or kwargs.get(SHARED_HISTORY_KEY) != {"publication_id": mark} or content.get("id") != message_id:
                return None, [], True
            publication = mark
            ids.append(message_id)
            cursor = row["seq"]
        if len(page) < limit:
            return publication, ids, False


@router.get("/api/threads/{thread_id}/shared-history", response_model=SharedHistory | None)
@require_permission("runs", "read")
@require_permission("threads", "read", owner_check=True, require_existing=True)
async def shared_history(request: Request, thread_id: ThreadId, response: Response):
    response.headers["Cache-Control"] = "private, no-store"
    scope = await conversation_scope(request, thread_id)
    publication, ids, _ = await _read_seed(request, thread_id)
    await require_scope(request, thread_id, scope)
    return {"publication_id": publication, "message_ids": ids} if ids else None


async def _seed(request: Request, thread_id: str, publication_id: str, snapshot: SharedSnapshot, record, reservation):
    copied = []
    for index, row in enumerate(snapshot.messages):
        cls = {"human": HumanMessage, "ai": AIMessage}[row.type]
        copied.append(cls(content=row.content, id=_stable_id("shared-turn", thread_id, publication_id, index), additional_kwargs={SHARED_HISTORY_KEY: {"publication_id": publication_id}}))
    prior, ids, personal_turns = await _read_seed(request, thread_id)
    expected = [message.id for message in copied]
    if prior == publication_id and ids == expected:
        return
    if not copied and (personal_turns or record.get("display_name")):
        return
    if personal_turns or (ids and (prior != publication_id or ids != expected[: len(ids)])):
        raise HTTPException(409, detail={"code": "conversation_changed"})

    accessor, config = build_checkpoint_state_mutation_accessor(request, thread_id=thread_id, as_node="branch")
    state = await accessor.aget(config)
    existing = (getattr(state, "values", {}) or {}).get("messages", [])
    if existing and not copied:
        return
    if existing and existing != copied:
        raise HTTPException(409, detail={"code": "conversation_changed"})
    if not existing:
        await accessor.aupdate(config, {"messages": copied, "title": snapshot.title}, as_node="branch")
    await get_thread_store(request).update_display_name(thread_id, snapshot.title)
    events = build_shared_history_seed_events(copied, thread_id=thread_id, publication_id=publication_id)
    await write_history_seed(request, events[len(ids) :], reservation=reservation, seed_kind="branch", thread=record)


@router.post("/api/shared-conversations/{publication_id}/continue", response_model=ContinuedConversation, response_model_exclude_none=True)
@require_permission("threads", "write")
@require_permission("threads", "read")
async def continue_conversation(request: Request, publication_id: str, body: ContinueRequest, config: AppConfig = Depends(get_config)):
    owner = acting_user_id(request)
    thread_id = _stable_id("continue-shared-conversation", owner, publication_id, str(body.request_id))
    try:
        async with goal_thread_lock(thread_id):
            async with reserve_history_seed(request, thread_id) as reservation:
                snapshot = await read_snapshot(request, publication_id)
                record = await _create_thread_locked(ThreadCreateRequest(thread_id=thread_id), request, thread_id, exact_owner=owner)
                scope = await conversation_scope(request, thread_id)
                if scope[0] != "ordinary":
                    raise HTTPException(404, "Thread not found")
                await _seed(request, thread_id, publication_id, snapshot, record, reservation)
    except _CHECKPOINT_MODE_ERRORS as exc:
        raise _checkpoint_mode_http_error(exc, thread_id) from exc
    except ConflictError:
        raise HTTPException(409, detail={"code": "conversation_busy"}) from None
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to seed continued conversation %s", thread_id)
        raise HTTPException(503, detail={"code": "history_copy_failed"}) from None

    files = []
    for member in snapshot.files:
        if not member.available or not member.publication_id:
            files.append({"path": member.path, "status": "not_copied", "reason": "no_longer_shared"})
            continue

        async def capture(request, source, *, maximum_bytes):
            return await capture_snapshot_member(request, publication_id, member, maximum_bytes=maximum_bytes)

        source = SharedPublicationSource(publication_id=member.publication_id, sha256=member.sha256)
        try:
            attachment = await import_into_chat(request, thread_id, source, config, capture=capture, expected_scope=scope)
        except HTTPException as exc:
            reason = {403: "access_denied", 404: "no_longer_shared", 409: "changed", 410: "no_longer_shared", 413: "too_large", 507: "storage_full"}.get(exc.status_code, "unavailable")
            if exc.status_code == 409 and isinstance(exc.detail, dict) and exc.detail.get("code") in ("snapshot-member-unavailable", "no-longer-shared"):
                reason = "no_longer_shared"
            files.append({"path": member.path, "status": "not_copied", "reason": reason})
        else:
            files.append({"path": member.path, "status": "copied", "attachment": attachment})
    await require_scope(request, thread_id, scope)
    return {"thread_id": thread_id, "answerer": body.answerer, "files": files}

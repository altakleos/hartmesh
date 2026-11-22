"""SQL evidence for the durable, permission-scoped search reader."""

import hashlib
import json
from collections import defaultdict
from contextlib import nullcontext

from sqlalchemy import String, any_, bindparam, func, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import aliased

from app.gateway.agent_conversations import _authority
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.persistence.user.model import UserRow
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.utils.time import coerce_iso


def shared_journal(thread_store, event_store):
    return type(thread_store) is ThreadMetaRepository and type(event_store) is DbRunEventStore and thread_store._sf is event_store._sf


def can_list_sql(corpus):
    authority = _authority(corpus.request) if corpus.request is not None else None
    return type(corpus.thread_store) is ThreadMetaRepository and authority is corpus.thread_store.instance_authority


async def sql_threads(corpus, *, check_actor):
    """Rank downstream using one scoped SQL read and just the fields the UI needs."""
    store = corpus.thread_store
    tm = ThreadMetaRow
    stmt = select(tm.thread_id, tm.incarnation, tm.created_at, tm.updated_at, tm.assistant_id, tm.display_name, tm.project_id, tm.metadata_json)
    authority = store.instance_authority if check_actor else None
    if authority is not None:
        bound = aliased(AgentConversationRow)
        stmt = stmt.add_columns(bound.instance_id).outerjoin(bound, bound.thread_id == tm.thread_id)
    stmt = stmt.where(await store._scope(corpus.user_id))
    async with store._sf() as session:
        rows = (await session.execute(stmt)).mappings().all()
    if authority is not None and any(row["instance_id"] is not None for row in rows):
        from app.gateway.routers.spaces import _actor

        if (await _actor(corpus.request)).subject_id != corpus.user_id:
            return
    for row in rows:
        metadata = row["metadata_json"] or {}
        if metadata.get("deerflow_sidecar") is True:
            continue
        if row["project_id"] is not None:
            metadata = {**metadata, "deerflow_project_id": row["project_id"]}
        yield corpus.project_thread({**row, "created_at": coerce_iso(row["created_at"]), "metadata": metadata})


async def index_heads(corpus, threads, *, session=None):
    """Fingerprint the live projection separately from its append position."""
    if not threads or not shared_journal(corpus.thread_store, corpus.event_store):
        return {}
    expected = {thread.thread_id: thread.incarnation for thread in threads}
    store = corpus.thread_store
    if session is None:
        allowed = await store._scope(corpus.user_id)
    else:
        # The writer needs to re-read the current head while it already holds
        # custody. Do every read on that connection, including the grant proof.
        expected = {thread.thread_id: thread.incarnation for thread in threads if await admit_index_write(corpus, session, thread)}
        if not expected:
            return {}
        allowed = True
    # Migrated journals have counters. Fall back to the unique event index if
    # an import populated events before creating its first allocator row.
    last_event = select(func.max(RunEventRow.seq)).where(RunEventRow.thread_id == ThreadMetaRow.thread_id).correlate(ThreadMetaRow).scalar_subquery()
    binding = aliased(AgentConversationRow)
    stmt = (
        select(ThreadMetaRow.thread_id, ThreadMetaRow.incarnation, ThreadMetaRow.created_at, ThreadMetaRow.metadata_json, binding.instance_id, func.coalesce(RunEventThreadSeqRow.seq, last_event, 0))
        .outerjoin(binding, binding.thread_id == ThreadMetaRow.thread_id)
        .outerjoin(RunEventThreadSeqRow, RunEventThreadSeqRow.thread_id == ThreadMetaRow.thread_id)
        .where(allowed)
    )
    source = RunRow.metadata_json["regenerate_from_run_id"].as_string()
    replay = select(RunRow.thread_id, RunRow.user_id, RunRow.run_id, source, RunRow.metadata_json["replay_kind"].as_string(), RunRow.status).where(source.is_not(None), source != "").order_by(RunRow.created_at, RunRow.run_id)
    persisted = defaultdict(list)
    rows = []
    async with nullcontext(session) if session is not None else store._sf() as reader:
        ids = list(expected)
        postgres = reader.get_bind().dialect.name == "postgresql"
        batch_size = len(ids) if postgres else 500
        for start in range(0, len(ids), batch_size):
            batch = ids[start : start + batch_size]
            if postgres:
                values = any_(bindparam("search_threads", batch, type_=ARRAY(String)))
                metadata_scope = ThreadMetaRow.thread_id == values
                replay_scope = RunRow.thread_id == values
            else:
                metadata_scope = ThreadMetaRow.thread_id.in_(batch)
                replay_scope = RunRow.thread_id.in_(batch)
            rows.extend(await reader.execute(stmt.where(metadata_scope)))
            for thread_id, *fact in await reader.execute(replay.where(replay_scope)):
                persisted[thread_id].append(fact)
    live = await corpus.run_manager.live_search_replays(expected)
    heads = {}
    for thread_id, incarnation, created_at, metadata, instance, high_water in rows:
        incarnation = incarnation or coerce_iso(created_at)
        if incarnation != expected[thread_id] or (metadata or {}).get("deerflow_sidecar") is True:
            continue
        user_id = None if instance is not None else corpus.user_id

        # The canonical feed combines durable replay attempts with this
        # process's ordered status overrides. Fingerprint both inputs so a
        # visibility change without a journal append triggers a full read.
        def visible_facts(facts):
            return [fact[1:] for fact in facts if user_id is None or fact[0] == user_id]

        scope = json.dumps([incarnation, instance, user_id, visible_facts(persisted[thread_id]), visible_facts(live.get(thread_id, ()))])
        heads[thread_id] = (hashlib.sha256(scope.encode()).hexdigest(), high_water)
    return heads


async def admit_index_write(corpus, session, thread):
    """Serialize with seat release and agent grant changes through index commit."""
    store = corpus.thread_store
    if not shared_journal(store, corpus.event_store) or session.bind is not store._sf.kw["bind"]:
        return False
    binding = await session.get(AgentConversationRow, thread.thread_id)
    if binding is not None:
        # Seat release and grant changes also lock the assistant/employee first.
        from app.gateway.routers.spaces import _actor

        authority = _authority(corpus.request) if corpus.request is not None else None
        if authority is None or store.instance_authority is not authority or (await _actor(corpus.request)).subject_id != corpus.user_id:
            return False
        if not await authority.locked_permission(session, thread.thread_id, corpus.user_id, permission=AgentPermission.INSPECT):
            return False
    seat = await session.get(UserRow, corpus.user_id, with_for_update={"read": True})
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from app.gateway.storage_spaces import human_lookup
    from deerflow.spaces.contract import PrincipalRef

    if seat is not None and (seat.released_at is not None or seat.disabled):
        return False
    if await human_lookup(SQLiteUserRepository(store._sf))(PrincipalRef("human", corpus.user_id), session=session) is None:
        return False
    stmt = select(ThreadMetaRow).where(ThreadMetaRow.thread_id == thread.thread_id)
    if binding is None:
        stmt = stmt.where(ThreadMetaRow.user_id == corpus.user_id)
    row = (await session.execute(stmt.with_for_update(read=True))).scalar_one_or_none()
    return row is not None and (row.metadata_json or {}).get("deerflow_sidecar") is not True and corpus.project_thread(store._row_to_dict(row)).incarnation == thread.incarnation

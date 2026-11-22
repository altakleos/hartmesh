"""Index the host's visible projection in bounded pages and return candidate identities."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
from uuid import uuid4

from deerflow_extension_api.search import SearchMessage, SearchPage
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from . import schema

logger = logging.getLogger(__name__)
_MAX_ACTIVE_READERS = 2

_MARKERS = text("""
    SELECT thread_id,incarnation,revision,last_seq FROM hm_message_search_threads
    WHERE format=:format AND complete AND thread_id=ANY(CAST(:threads AS text[]))
""")
_STORE_WORDS = text("""
    WITH eligible AS MATERIALIZED (
      SELECT m.seq, m.message_id, m.normalized
      FROM unnest(CAST(:seqs AS integer[]),CAST(:ids AS text[]),CAST(:words AS text[])) m(seq,message_id,normalized)
      JOIN run_events e ON e.thread_id=:thread_id AND e.seq=m.seq FOR KEY SHARE OF e
    ), newest AS (
      SELECT DISTINCT ON(message_id) seq,message_id,normalized FROM eligible ORDER BY message_id,seq DESC
    ), retired AS (
      DELETE FROM hm_message_search_words w USING newest n
      WHERE w.thread_id=:thread_id AND w.format=:format AND w.message_id=n.message_id AND w.seq<n.seq
    ), stored AS (
      INSERT INTO hm_message_search_words(thread_id,seq,format,message_id,normalized)
      SELECT :thread_id,n.seq,:format,n.message_id,n.normalized FROM newest n
      WHERE NOT EXISTS (SELECT FROM hm_message_search_words w WHERE w.thread_id=:thread_id AND w.format=:format
                        AND w.message_id=n.message_id AND w.seq>n.seq)
    ON CONFLICT(thread_id,seq) DO UPDATE SET format=excluded.format,message_id=excluded.message_id,normalized=excluded.normalized
      WHERE (hm_message_search_words.format,hm_message_search_words.message_id,hm_message_search_words.normalized)
            IS DISTINCT FROM (excluded.format,excluded.message_id,excluded.normalized)
    ) SELECT count(*) FROM eligible
""")
_RESERVE = text("""
    INSERT INTO hm_message_search_threads(thread_id,format,incarnation,revision,last_seq,target_seq,generation,complete)
    VALUES (:thread_id,:format,:incarnation,:revision,:last_seq,:target_seq,:generation,false) ON CONFLICT(thread_id) DO UPDATE
    SET format=excluded.format,incarnation=excluded.incarnation,revision=excluded.revision,last_seq=excluded.last_seq,
        target_seq=excluded.target_seq,generation=excluded.generation,complete=false
""")
# Ordering the admitted conversations first lets PostgreSQL stop as soon as a
# common query fills its page. Sparse queries still inspect all eligible words.
_MATCHES = """
    SELECT t.id AS thread_id,w.seq,w.message_id,t.incarnation,t.updated
    FROM (
        SELECT t.id,t.incarnation,t.updated
        FROM unnest(CAST(:threads AS text[]),CAST(:incarnations AS text[]),CAST(:updates AS text[])) t(id,incarnation,updated)
        JOIN hm_message_search_threads c ON c.thread_id=t.id AND c.incarnation=t.incarnation AND c.format=:format AND c.complete
        {thread_cursor}
        ORDER BY t.updated DESC,t.id DESC
    ) t CROSS JOIN LATERAL (
        SELECT w.seq,w.message_id FROM hm_message_search_words w
        WHERE w.thread_id=t.id AND w.format=:format AND strpos(w.normalized,:query)>0
        {message_cursor}
        ORDER BY w.seq DESC LIMIT :limit
    ) w ORDER BY t.updated DESC,t.id DESC,w.seq DESC LIMIT :limit
"""


def _normalized(value):
    # PostgreSQL text rejects NUL; the host always compares the exact original before disclosure.
    return value.casefold().replace("\x00", "\ufffd")


def _position(cursor, scope):
    try:
        saved_scope, updated, thread_id, seq = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
        if saved_scope != scope or not isinstance(updated, str) or not isinstance(thread_id, str) or type(seq) is not int or seq < 0 or seq > 2**31 - 1:
            raise ValueError
        return {"updated": updated, "last_thread": thread_id, "last_seq": seq}
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("Invalid search cursor") from exc


class MessageSearch:
    requires_relational_history = True

    def __init__(self):
        self.ready = False
        self._sf = None
        self._jobs = {}

    async def start(self, deps):
        self._sf = deps.session_factory
        if self._sf is not None:
            try:
                self.ready = await schema.prepare(self._sf)
            except SQLAlchemyError:
                logger.warning("The message search index is unavailable", exc_info=True)
                self.ready = False

    async def stop(self):
        self.ready = False
        tasks = tuple(self._jobs.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._jobs.clear()
        self._sf = None

    async def _check_ties(self, session, *, writing=False):
        if writing:
            # ALTER/DROP of either tie cannot slip between catalogue inspection and commit.
            await session.execute(text("LOCK TABLE hm_message_search_words,hm_message_search_threads IN ROW EXCLUSIVE MODE"))
        if await schema.installed_objects(session) != schema.REQUIRED_OBJECTS:
            self.ready = False
            raise RuntimeError("Message search storage is unavailable")

    async def _reserve(self, corpus, thread, head):
        async with self._sf.begin() as session:
            if not self.ready or not await corpus.admit_index_write(session, thread):
                return None
            await self._check_ties(session, writing=True)
            # A missing marker has no row lock yet. Serialize only this short reservation.
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtext('hm_message_search_threads'),hashtext(:id))"), {"id": thread.thread_id})
            if (await corpus.index_heads((thread,), session=session)).get(thread.thread_id) != head:
                return None
            marker = (await session.execute(text("SELECT * FROM hm_message_search_threads WHERE thread_id=:id FOR UPDATE"), {"id": thread.thread_id})).one_or_none()
            same_view = bool(marker and marker.format == schema.FORMAT and marker.incarnation == thread.incarnation and marker.revision == head[0] and marker.last_seq <= head[1])
            if same_view and marker.target_seq == head[1]:
                return None if marker.complete else (marker.generation, marker.last_seq)
            last_seq = marker.last_seq if same_view else 0
            if not same_view:
                # Readers cannot see a completed marker for a partial rebuild.
                await session.execute(text("DELETE FROM hm_message_search_words WHERE thread_id=:id"), {"id": thread.thread_id})
            generation = uuid4()
            await session.execute(_RESERVE, {"thread_id": thread.thread_id, "format": schema.FORMAT, "incarnation": thread.incarnation, "revision": head[0], "last_seq": last_seq, "target_seq": head[1], "generation": generation})
            return generation, last_seq

    async def _write(self, corpus, thread, generation, messages, *, finished):
        async with self._sf.begin() as session:
            if not self.ready or not await corpus.admit_index_write(session, thread):
                return False
            await self._check_ties(session, writing=True)
            # A reset, deletion, or a later completed build fences outstanding readers.
            guard = {"id": thread.thread_id, "generation": generation}
            if not await session.scalar(text("SELECT 1 FROM hm_message_search_threads WHERE thread_id=:id AND generation=:generation AND NOT complete FOR UPDATE"), guard):
                return False
            if messages:
                present = await session.scalar(
                    _STORE_WORDS,
                    {
                        "format": schema.FORMAT,
                        "thread_id": thread.thread_id,
                        "seqs": [m.seq for m in messages],
                        "ids": [m.message_id for m in messages],
                        "words": [_normalized(m.text) for m in messages],
                    },
                )
                if present != len(messages):
                    return False
            if finished:
                await session.execute(text("UPDATE hm_message_search_threads SET last_seq=target_seq,complete=true WHERE thread_id=:id AND generation=:generation"), guard)
        return True

    async def _refresh(self, corpus, missing):
        for thread, head in missing:
            reservation = await self._reserve(corpus, thread, head)
            if reservation is None:
                continue
            generation, last_seq = reservation
            before = None
            while self.ready:
                page = await corpus.messages(thread, before_seq=before)
                # A complete older marker, not an uncommitted/restarted scan's row, proves coverage.
                messages = [m for m in page.items if m.thread_id == thread.thread_id and m.incarnation == thread.incarnation and last_seq < m.seq <= head[1]]
                finished = page.before_seq is None or any(m.seq <= last_seq for m in page.items)
                if finished and (await corpus.index_heads((thread,))).get(thread.thread_id) != head:
                    break
                if not await self._write(corpus, thread, generation, messages, finished=finished) or finished:
                    break
                if before is not None and page.before_seq >= before:
                    raise RuntimeError("The visible message page did not advance")
                before = page.before_seq

    def _done(self, task):
        if not task.cancelled() and task.exception() is not None:
            # A later poll can retry after a transient database failure.
            logger.warning("Message search catch-up failed (%s)", type(task.exception()).__name__)

    async def search(self, corpus, *, query, threads, limit, cursor=None):
        if not self.ready:
            raise RuntimeError("Message search storage is unavailable")
        scope = hashlib.sha256(json.dumps([corpus.user_id, query.casefold()]).encode()).hexdigest()
        resume = _position(cursor, scope) if cursor is not None else None
        if not threads:
            return SearchPage()
        for reader, task in tuple(self._jobs.items()):
            if task.done():
                del self._jobs[reader]
                if reader == corpus.user_id and not task.cancelled() and task.exception() is not None:
                    raise RuntimeError("Message search catch-up failed") from None
        if corpus.user_id in self._jobs:
            return SearchPage(indexing=True)
        heads = await corpus.index_heads(threads)
        selected = [t for t in threads if t.thread_id in heads]
        if not selected:
            return SearchPage()
        async with self._sf() as session:
            await self._check_ties(session)
            rows = (await session.execute(_MARKERS, {"format": schema.FORMAT, "threads": [t.thread_id for t in selected]})).all()
            covered = {row.thread_id: (row.incarnation, row.revision, row.last_seq) for row in rows}
            missing = []
            for thread in selected:
                revision, seq = heads[thread.thread_id]
                old = covered.get(thread.thread_id)
                if old != (thread.incarnation, revision, seq):
                    missing.append((thread, (revision, seq)))
            if missing:
                if corpus.user_id not in self._jobs and len(self._jobs) < _MAX_ACTIVE_READERS and self.ready:
                    task = asyncio.create_task(self._refresh(corpus, missing))
                    self._jobs[corpus.user_id] = task
                    task.add_done_callback(self._done)
                return SearchPage(indexing=True)
            params = {"format": schema.FORMAT, "threads": [t.thread_id for t in selected], "incarnations": [t.incarnation for t in selected], "updates": [t.updated_at for t in selected], "query": _normalized(query), "limit": limit + 1}
            thread_cursor, message_cursor = "", ""
            if resume:
                thread_cursor = "WHERE (t.updated,t.id)<=(:updated,:last_thread)"
                message_cursor = "AND (t.updated,t.id,w.seq)<(:updated,:last_thread,:last_seq)"
                params.update(resume)
            statement = _MATCHES.format(thread_cursor=thread_cursor, message_cursor=message_cursor)
            rows = (await session.execute(text(statement), params)).all()
        more = len(rows) > limit
        rows = rows[:limit]
        after = None
        if more and rows:
            last = rows[-1]
            after = base64.urlsafe_b64encode(json.dumps([scope, last.updated, last.thread_id, last.seq]).encode()).decode()
        # The host re-reads each candidate and supplies all display fields itself.
        return SearchPage(tuple(SearchMessage(r.thread_id, r.incarnation, r.message_id, "", r.seq, "", "", "") for r in rows), after)

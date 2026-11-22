"""Create the disposable projection only when PostgreSQL can enforce both lifetimes."""

import asyncio
import logging
from time import monotonic

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

logger = logging.getLogger(__name__)

WORDS = "hm_message_search_words"
THREADS = "hm_message_search_threads"
FORMAT = 1

# Only current, validated, ordered column pairs with a real ON DELETE CASCADE count.
# Resolve the relations through this connection's search path, as all reads and writes do.
_OBJECTS = text("""
    SELECT c.conname FROM pg_constraint c
    JOIN (VALUES ('hm_message_search_event_fk','hm_message_search_words','run_events',ARRAY['thread_id','seq']),
                 ('hm_message_search_thread_fk','hm_message_search_threads','threads_meta',ARRAY['thread_id']))
      AS wanted(name,child,parent,columns) ON c.conname=wanted.name
    WHERE c.contype='f' AND c.convalidated AND c.confdeltype='c'
      AND c.conrelid=to_regclass(wanted.child) AND c.confrelid=to_regclass(wanted.parent)
      AND ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(num,ord)
                JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num ORDER BY k.ord)=wanted.columns
      AND ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY k(num,ord)
                JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num ORDER BY k.ord)=wanted.columns
    UNION ALL
    SELECT t.tgname FROM pg_trigger t JOIN pg_proc f ON f.oid=t.tgfoid
    WHERE t.tgname='hm_message_search_source_removed' AND NOT t.tgisinternal
      AND t.tgrelid=to_regclass('hm_message_search_words') AND t.tgenabled IN ('O','A') AND t.tgtype=8 AND t.tgoldtable='retired_search_words'
      AND t.tgnargs=3
      AND t.tgargs=convert_to(t.tgrelid::text,'UTF8') || decode('00','hex')
                  || convert_to(to_regclass('hm_message_search_threads')::oid::text,'UTF8') || decode('00','hex')
                  || convert_to(to_regclass('run_events')::oid::text,'UTF8') || decode('00','hex')
      AND f.proname='hm_message_search_forget_coverage' AND f.pronargs=0 AND f.prosecdef
      AND f.pronamespace=(SELECT relnamespace FROM pg_class WHERE oid=t.tgrelid)
""")

_TIES = (
    (
        WORDS,
        "hm_message_search_event_fk",
        "thread_id varchar(64) NOT NULL, seq integer NOT NULL, format smallint NOT NULL, message_id text NOT NULL, normalized text NOT NULL, PRIMARY KEY(thread_id,seq)",
        "(thread_id,seq) REFERENCES run_events(thread_id,seq)",
    ),
    (
        THREADS,
        "hm_message_search_thread_fk",
        "thread_id varchar(64) PRIMARY KEY, format smallint NOT NULL, incarnation text NOT NULL, revision text NOT NULL, last_seq bigint NOT NULL, target_seq bigint NOT NULL, generation uuid NOT NULL, complete boolean NOT NULL",
        "(thread_id) REFERENCES threads_meta(thread_id)",
    ),
)


_INVALIDATOR = "hm_message_search_forget_coverage"
_TRIGGER = "hm_message_search_source_removed"
REQUIRED_OBJECTS = {tie[1] for tie in _TIES} | {_TRIGGER}


async def installed_objects(session):
    return set(await session.scalars(_OBJECTS))


async def _prepare_invalidation(factory):
    async with factory() as session:
        if await installed_objects(session) == REQUIRED_OBJECTS:
            return True
    # No parent DDL. Serialize with index writers and other starters.
    try:
        async with asyncio.timeout(1.90), factory.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout = '1750ms'"))
            await session.execute(text(f"LOCK TABLE {WORDS},{THREADS} IN SHARE ROW EXCLUSIVE MODE"))
            objects = await installed_objects(session)
            if objects == REQUIRED_OBJECTS:
                return True
            if REQUIRED_OBJECTS - objects != {_TRIGGER}:
                return False
            # Even a history-only role must be able to remove a source event. Limit
            # the definer to this exact trigger relation and its fixed sibling.
            relation, marker_relation, events_relation, namespace = (
                await session.execute(
                    text(f"""
                SELECT c.oid, to_regclass('{THREADS}')::oid,to_regclass('run_events')::oid,quote_ident(n.nspname)
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('{WORDS}')
            """)
                )
            ).one()
            function = f"{namespace}.{_INVALIDATOR}"
            await session.execute(
                text(f"""
                CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
                SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $body$
                DECLARE marker_name regclass; source_name regclass;
                BEGIN
                  IF TG_RELID={int(relation)} AND TG_OP='DELETE' AND TG_WHEN='AFTER'
                     AND EXISTS (SELECT FROM retired_search_words) THEN
                    SELECT m.oid,e.oid INTO marker_name,source_name FROM pg_class m,pg_class e
                      WHERE m.oid={int(marker_relation)} AND m.relname='{THREADS}'
                        AND e.oid={int(events_relation)} AND e.relname='run_events';
                    IF FOUND THEN
                      EXECUTE format('DELETE FROM %s m WHERE m.tableoid=$1 AND m.thread_id IN (
                        SELECT r.thread_id FROM retired_search_words r WHERE NOT EXISTS (
                          SELECT FROM %s e WHERE e.tableoid=$2 AND e.thread_id=r.thread_id AND e.seq=r.seq))',
                        marker_name,source_name) USING marker_name::oid,source_name::oid;
                    END IF;
                  END IF;
                  RETURN NULL;
                END
                $body$
            """)
            )
            await session.execute(text(f"REVOKE ALL ON FUNCTION {function}() FROM PUBLIC"))
            # Persist the bound OIDs in the trigger metadata so a restored/replaced
            # relation forces reinstallation before any of its markers can be trusted.
            # PostgreSQL groups old rows from a cascading DELETE in a transition table.
            # A provider retiring superseded words still has their original journal rows.
            await session.execute(
                text(f"""
                CREATE OR REPLACE TRIGGER {_TRIGGER} AFTER DELETE ON {WORDS}
                REFERENCING OLD TABLE AS retired_search_words FOR EACH STATEMENT
                EXECUTE FUNCTION {function}('{int(relation)}','{int(marker_relation)}','{int(events_relation)}')
            """)
            )
            await session.execute(text(f"ALTER TABLE {WORDS} ENABLE TRIGGER {_TRIGGER}"))
            # Deletes during an absent/disabled trigger cannot be reconstructed from the allocator.
            await session.execute(text(f"DELETE FROM {THREADS}"))
            return await installed_objects(session) == REQUIRED_OBJECTS
    except TimeoutError:
        return False


async def prepare(factory):
    async with factory() as session:
        if session.get_bind().dialect.name != "postgresql":
            return False
        existing = await installed_objects(session)
    for table, name, columns, reference in _TIES:
        if name in existing:
            continue
        started = monotonic()
        try:
            async with asyncio.timeout(1.90):
                async with factory.begin() as session:
                    await session.execute(text("SET LOCAL lock_timeout = '1750ms'"))
                    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(current_schema()::text),hashtext(:name))"), {"name": name})
                    if name in await installed_objects(session):
                        continue
                    await session.execute(text(f"CREATE TABLE IF NOT EXISTS {table} ({columns})"))
                    # A simultaneous starter may have installed this tie.
                    await session.execute(text(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"))
                    if name not in await installed_objects(session):
                        await session.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY {reference} ON DELETE CASCADE"))
        except TimeoutError:
            logger.warning("Message search constraint %s outcome=timeout elapsed_seconds=%.6f reason=attempt_deadline", name, monotonic() - started)
            return False
        except DBAPIError as exc:
            outcome = "timeout" if getattr(exc.orig, "sqlstate", None) == "55P03" else "failed"
            logger.warning("Message search constraint %s outcome=%s elapsed_seconds=%.6f reason=%s", name, outcome, monotonic() - started, "lock_timeout" if outcome == "timeout" else "database_error")
            raise
        logger.info("Message search constraint %s outcome=installed elapsed_seconds=%.6f", name, monotonic() - started)
    return await _prepare_invalidation(factory)

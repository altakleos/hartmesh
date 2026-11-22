"""SQL predicates for the ordinary batch worker and the startup reconciliation reader."""

from contextlib import asynccontextmanager

from sqlalchemy import and_, case, func, text

from deerflow.persistence.subagent_batches.model import SubagentBatchRow
from deerflow.utils.file_io import await_drained


def _sqlite_document():
    # A manually damaged SQLite text value must not abort a scan of its neighbours.
    spec = SubagentBatchRow.execution_spec
    return case((func.json_valid(spec) == 1, spec), else_=None)


def ordinary_batch(session):
    """Test JSON key presence. A present JSON null is different from an absent key."""
    spec = SubagentBatchRow.execution_spec
    if session.get_bind().dialect.name == "sqlite":
        document = _sqlite_document()
        return and_(func.json_type(document) == "object", func.json_type(document, "$.scope").is_(None))
    return and_(func.json_typeof(spec) == "object", func.json_extract_path(spec, "scope").is_(None))


def known_scope_candidate(session):
    """Cheap discriminator. The reconciler validates the complete locked document."""
    if session.get_bind().dialect.name == "sqlite":
        document = _sqlite_document()
        return and_(
            func.json_extract(document, "$.scope.kind") == "agent_conversation",
            func.json_type(document, "$.scope.version") == "integer",
            func.json_extract(document, "$.scope.version") == 1,
        )
    spec = SubagentBatchRow.execution_spec
    return and_(
        func.json_extract_path_text(spec, "scope", "kind") == "agent_conversation",
        func.json_typeof(func.json_extract_path(spec, "scope", "version")) == "number",
        func.json_extract_path_text(spec, "scope", "version") == "1",
    )


@asynccontextmanager
async def startup_lock_budget(session, *, write=False):
    """Bound DB lock waits and restore the pooled connection's original policy."""
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
        yield
        return
    connection = await session.connection()
    previous = (await connection.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
    try:
        await connection.exec_driver_sql("PRAGMA busy_timeout=250")
        if write:
            # No upgrade from a stale SQLite read snapshot. PostgreSQL locks the parent.
            await connection.exec_driver_sql("BEGIN IMMEDIATE")
        yield
    finally:
        # Cancelled aiosqlite statements may invalidate and discard their connection.
        if not connection.invalidated:
            await await_drained(connection.exec_driver_sql(f"PRAGMA busy_timeout={int(previous)}"))

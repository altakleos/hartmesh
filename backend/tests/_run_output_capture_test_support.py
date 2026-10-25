"""Shared real-SQL run/event authority; no worker, model or remote proof is faked."""

from types import SimpleNamespace

import pytest_asyncio
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import capture_run as capture_run
from _run_output_store_test_support import output_store as output_store  # noqa: F401
from sqlalchemy import text

from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.user_context import reset_current_user, set_current_user


@pytest_asyncio.fixture
async def capture_environment(output_store):
    async with output_store.engine.begin() as connection:
        if output_store.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda conn: RunEventRow.metadata.create_all(conn, tables=[RunEventRow.__table__, ThreadMetaRow.__table__]))
    yield SimpleNamespace(**vars(output_store), event_store=DbRunEventStore(output_store.sf), thread_store=ThreadMetaRepository(output_store.sf), manager=RunManager(output_store.repository))


async def put_messages(environment, rows, *, owner=RAW_OWNER, thread_id="thread", run_id="run"):
    token = set_current_user(SimpleNamespace(id=owner))
    try:
        return await environment.event_store.put_batch(
            [{"thread_id": thread_id, "run_id": run_id, "event_type": "llm.human.input" if row["type"] == "human" else "llm.ai.response", "category": "message", "content": row, "metadata": {"caller": "lead_agent"}} for row in rows]
        )
    finally:
        reset_current_user(token)

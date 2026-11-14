"""Real source preparation and materialization with one owned SQL connection."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_telegram_composite_execution_outcomes import db as db
from test_telegram_composite_execution_outcomes import uncertain_child
from test_telegram_composite_execution_outcomes import work_delivery as work_delivery
from test_telegram_composite_execution_outcomes import work_pages as work_pages


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["source-read", "materialize"])
async def test_projection_preparation_uses_one_owned_connection_at_a_time(work_delivery, monkeypatch, entry):
    e = work_delivery
    plans, _parent_id, child_id = await uncertain_child(e, monkeypatch)
    materialize = plans.materialize

    async def defer(*args, **kwargs):
        return True

    # Preserve the production interrupted-before-materialize checkpoint.
    monkeypatch.setattr(plans, "materialize", defer)
    identity = await plans.observe(e.link, child_id)
    monkeypatch.setattr(plans, "materialize", materialize)
    item = await e.sender.outbox.get(e.link, identity)
    assert item.parts == []
    assert (await plans.reader.inspect(e.link, item)).source is not None
    from app.gateway.auth.mode import account_refusal
    from deerflow.spaces.contract import ResolvedPrincipal

    async def borrowed_human(reference, session):
        user = await e.live.users.get_user_by_id(reference.subject_id, session=session)
        return ResolvedPrincipal(reference) if user is not None and account_refusal(user, sign_on_only_mode=False) is None else None

    monkeypatch.setattr(e.human.agents.directory.human, "in_session", borrowed_human, raising=False)
    original_engine = e.db.engine
    args = {}
    if e.db.dialect == "postgres":
        async with original_engine.connect() as connection:
            schema = await connection.scalar(text("select current_schema()"))
        args = {"ssl": False, "server_settings": {"search_path": schema}}
    bounded = create_async_engine(original_engine.url, pool_size=1, max_overflow=0, pool_timeout=0.25, connect_args=args)
    e.db.sessions.configure(bind=bounded)
    try:
        if entry == "source-read":
            assert (await plans.reader.inspect(e.link, item)).source is not None
        else:
            assert await materialize(e.link, identity)
    finally:
        e.db.sessions.configure(bind=original_engine)
        await bounded.dispose()

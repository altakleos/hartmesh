"""Exact group cleanup joins the new field UI transaction."""

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment


@pytest.mark.asyncio
async def test_exact_group_cleanup_rolls_back_with_required_next_field(callbacks_environment):
    e = callbacks_environment
    first = await e.plan(message_id=902)
    second = await e.plan(message_id=903, source="another-group")
    with pytest.raises(RuntimeError, match="next field failed"):
        async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
            await e.callbacks.consume_group(first.group.group_id, link=e.link, guard=e.guard_factory(), session=writer)
            assert all([(await e.callbacks.get(identity)).consumed_at is None for identity in second.group.callback_ids])
            raise RuntimeError("next field failed")
    assert all([(await e.callbacks.get(identity)).consumed_at is None for identity in [*first.group.callback_ids, *second.group.callback_ids]])
    async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
        await e.callbacks.consume_group(first.group.group_id, link=e.link, guard=e.guard_factory(), session=writer)
    assert all([(await e.callbacks.get(identity)).closed_reason == "done" for identity in first.group.callback_ids])
    assert all([(await e.callbacks.get(identity)).consumed_at is None for identity in second.group.callback_ids])


@pytest.mark.asyncio
async def test_exact_group_cleanup_refuses_another_real_owners_group(callbacks_environment, monkeypatch):
    e = callbacks_environment
    plan = await e.plan(message_id=902)
    other = await install_live_owner(e.db, monkeypatch, email="other-group@example.com")
    foreign = await bind_link(e.db, await add_link(e.db, owner=other.id, bot=9000000002), account=7000000002, message=111, update=211)
    checker = await other.authority.prepare_live_owner(other.id)
    guard = e.links.publication_guard(foreign, check_live_owner=checker, check_attempt_current=lambda: True)
    with pytest.raises(ValueError):
        async with e.callbacks.writer(foreign, guard) as writer:
            await e.callbacks.consume_group(plan.group.group_id, link=foreign, guard=guard, session=writer)
    assert all([(await e.callbacks.get(identity)).consumed_at is None for identity in plan.group.callback_ids])

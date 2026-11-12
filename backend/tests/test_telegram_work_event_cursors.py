"""A binding owns its event baseline; restart never discards later Work."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import actions as actions

from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramWorkCursorRow


@pytest.mark.asyncio
async def test_initial_baseline_is_atomic_and_restart_keeps_its_revision(actions):
    from app.telegram_bots.work_event_cursors import WorkEventCursors

    e = actions
    cursors = WorkEventCursors(e.db.links)
    with pytest.raises(RuntimeError, match="rollback baseline"):
        async with e.db.links._transaction() as writer:
            link = await writer.get(TelegramBotLinkRow, e.link.id, with_for_update=True)
            assert await cursors.seed_bound(writer, link)
            raise RuntimeError("rollback baseline")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramBotLinkRow, e.link.id)).work_baseline_generation is None
        assert not list(await reader.scalars(select(TelegramWorkCursorRow)))
    assert await cursors.initialize() == 1
    async with e.db.sessions() as reader:
        cursor = await reader.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))
        before = cursor.revision
        assert before == (await reader.get(AgentWorkRow, e.work["id"])).revision
        assert cursor.binding_generation == e.link.binding_generation
    await e.acts.respond(e.link, operation_id=uuid4().hex, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Read the verified guide.")
    later = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Later assignment", success_criteria="Keep events"))
    assert await WorkEventCursors(e.db.links).initialize() == 0
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))).revision == before
        assert await reader.get(TelegramWorkCursorRow, (e.link.id, later["id"])) is None
        assert (await reader.get(AgentWorkRow, e.work["id"])).revision > before


@pytest.mark.asyncio
async def test_uninstalled_baseline_does_not_change_existing_bindings(actions):
    e = actions
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramBotLinkRow, e.link.id)).work_baseline_generation is None
        assert not list(await reader.scalars(select(TelegramWorkCursorRow)))


async def peer_binding(e, *, fail=False):
    from _telegram_repository_test_support import code_of, welcome

    owner, account = "another-person", 7_000_000_002
    await e.db.settings.ensure(owner, timezone="UTC", locale="en-US")
    peer = await e.db.links.add(owner_user_id=owner, agent_kind="instance", agent_ref=e.instance.id, bot_id=9_000_000_003, bot_username="peer_fixture_bot", encrypted_token="wrapped-fixture", actor_id=owner)
    code = code_of(await e.db.links.mint_code(peer))
    peer = await e.db.links.get(peer.id)
    options = dict(telegram_user_id=account, chat_id=account, first_name="Peer", username=None, code=code, start_identity={"update_id": 3001, "message_id": 1001, "chat_id": account}, hello_parts=welcome("hello.employee"))
    if fail:
        original = e.db.links.work_baseline_seeder

        async def interrupted(writer, link):
            await original(writer, link)
            raise RuntimeError("interrupted binding")

        e.db.links.work_baseline_seeder = interrupted
        try:
            with pytest.raises(RuntimeError, match="interrupted binding"):
                await e.db.links.bind(peer, **options)
        finally:
            e.db.links.work_baseline_seeder = original
        from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramOutboxRow

        async with e.db.sessions() as reader:
            saved = await reader.get(TelegramBotLinkRow, peer.id)
            assert saved.state == "waiting_connect" and saved.binding_generation == peer.binding_generation
            assert saved.connection_id is None and saved.work_baseline_generation is None
            assert await reader.get(TelegramAccountRow, account) is None
            assert not list(await reader.scalars(select(TelegramWorkCursorRow).where(TelegramWorkCursorRow.link_id == peer.id)))
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == peer.id)))
    bound = await e.db.links.bind(peer, **options)
    return bound, options


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_new_colleague_binding_never_resets_an_existing_cursor(actions, fail):
    from app.telegram_bots.work_event_cursors import WorkEventCursors

    e = actions
    cursors = WorkEventCursors(e.db.links)
    cursors.install_binding_hook()
    assert await cursors.initialize() == 1
    async with e.db.sessions() as reader:
        earlier = (await reader.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))).revision
    await e.acts.respond(e.link, operation_id=uuid4().hex, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="A later answer.")
    peer, options = await peer_binding(e, fail=fail)
    assert peer.work_baseline_generation == peer.binding_generation
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))).revision == earlier
        saved = await reader.get(TelegramWorkCursorRow, (peer.id, e.work["id"]))
        assert saved.revision > earlier and saved.binding_generation == peer.binding_generation
        assert saved.revision == (await reader.get(AgentWorkRow, e.work["id"])).revision
    # Replaying Start uses its committed receipt, not a new history boundary.
    assert (await e.db.links.bind(peer, **options)).work_baseline_generation == peer.binding_generation
    assert await WorkEventCursors(e.db.links).initialize() == 0


@pytest.mark.asyncio
async def test_rebinding_seeds_only_the_new_generation(actions):
    from _telegram_repository_test_support import code_of, welcome

    from app.telegram_bots.work_event_cursors import WorkEventCursors

    e = actions
    cursors = WorkEventCursors(e.db.links)
    cursors.install_binding_hook()
    await cursors.initialize()
    await e.acts.respond(e.link, operation_id=uuid4().hex, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Before the new binding.")
    waiting = await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
    code = code_of(await e.db.links.mint_code(waiting))
    bound = await e.db.links.bind(
        await e.db.links.get(waiting.id),
        telegram_user_id=e.link.telegram_user_id,
        chat_id=e.link.chat_id,
        first_name="Person",
        username=None,
        code=code,
        start_identity={"update_id": 3101, "message_id": 1101, "chat_id": e.link.chat_id},
        hello_parts=welcome("hello.employee"),
    )
    async with e.db.sessions() as reader:
        cursor = await reader.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))
        assert cursor.binding_generation == bound.binding_generation > e.link.binding_generation
        assert cursor.revision == (await reader.get(AgentWorkRow, e.work["id"])).revision
        assert bound.work_baseline_generation == bound.binding_generation


@pytest.mark.asyncio
async def test_restored_binding_establishes_its_new_baseline_before_return(actions):
    from app.telegram_bots.work_event_cursors import WorkEventCursors

    e = actions
    cursors = WorkEventCursors(e.db.links)
    cursors.install_binding_hook()
    await cursors.initialize()
    held = await e.db.links.hold_if_current(e.link.id, token_revision=e.link.token_revision, binding_generation=e.link.binding_generation)
    later = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Created before restoration", success_criteria="Do not replay old history"))
    assert await cursors.initialize() == 0
    assert await e.db.connections.restore_held_connection(held.connection_id, owner_user_id=held.owner_user_id) == "restored"
    restored = await e.db.links.release_if_current(held.id, token_revision=held.token_revision, binding_generation=held.binding_generation)
    assert restored.work_baseline_generation == restored.binding_generation
    async with e.db.sessions() as reader:
        cursor = await reader.get(TelegramWorkCursorRow, (e.link.id, later["id"]))
        assert cursor.binding_generation == restored.binding_generation and cursor.revision == later["revision"]


@pytest.mark.asyncio
async def test_concurrent_initializers_seed_a_generation_once(actions):
    import asyncio

    from app.telegram_bots.work_event_cursors import WorkEventCursors

    e = actions
    results = await asyncio.gather(*(WorkEventCursors(e.db.links).initialize() for _ in range(2)))
    assert sorted(results) == [0, 1]
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramWorkCursorRow)))
        assert len(rows) == 1 and rows[0].link_id == e.link.id

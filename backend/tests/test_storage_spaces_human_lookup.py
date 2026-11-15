"""Fresh sign-on policy stays off-loop while account reads borrow real SQL."""

import threading

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import db as db
from sqlalchemy import event

from app.gateway.auth.mode import ACCOUNT_INERT, owner_is_refused
from app.gateway.storage_spaces import human_lookup
from deerflow.config import app_config
from deerflow.spaces.contract import PrincipalRef


@pytest.mark.asyncio
@pytest.mark.parametrize("borrowed", [False, True])
@pytest.mark.parametrize("provider,local_enabled", [(False, True), (False, False), (True, False)])
async def test_human_lookup_reads_live_mode_off_loop_and_keeps_the_borrowed_account_snapshot(db, monkeypatch, borrowed, provider, local_enabled):
    live = await install_live_owner(db, monkeypatch, provider=provider, local_enabled=local_enabled)
    loop_thread = threading.get_ident()
    reads = []

    def current_config():
        reads.append(threading.get_ident())
        assert reads[-1] != loop_thread, "sign-on policy read on the Gateway loop"
        return live.config

    monkeypatch.setattr(app_config, "get_app_config", current_config)
    lookup = human_lookup(live.users)
    actor = PrincipalRef("human", live.id)
    active, peak = [0], [0]

    def checkout(*args):
        active[0] += 1
        peak[0] = max(peak[0], active[0])

    def checkin(*args):
        active[0] -= 1

    event.listen(db.engine.sync_engine, "checkout", checkout)
    event.listen(db.engine.sync_engine, "checkin", checkin)
    try:
        if borrowed:
            async with db.sessions.begin() as writer:
                resolved = await lookup.in_session(actor, writer)
                assert writer.in_transaction()
        else:
            resolved = await lookup(actor)
        assert (resolved is not None) is (local_enabled or provider)
        assert reads and peak[0] == 1 and active[0] == 0
    finally:
        event.remove(db.engine.sync_engine, "checkout", checkout)
        event.remove(db.engine.sync_engine, "checkin", checkin)


@pytest.mark.asyncio
@pytest.mark.parametrize("provider,local_enabled", [(False, True), (False, False), (True, False)])
async def test_internal_owner_reads_current_sign_on_policy_off_loop(db, monkeypatch, provider, local_enabled):
    live = await install_live_owner(db, monkeypatch, provider=provider, local_enabled=local_enabled)
    loop_thread = threading.get_ident()

    def current_config():
        assert threading.get_ident() != loop_thread, "internal owner policy read on the Gateway loop"
        return live.config

    monkeypatch.setattr(app_config, "get_app_config", current_config)
    assert await owner_is_refused(live.id) == (None if local_enabled or provider else ACCOUNT_INERT)

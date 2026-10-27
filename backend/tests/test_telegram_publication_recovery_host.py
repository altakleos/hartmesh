"""Host-only restart provenance and current fixed authority over actual SQL."""

import asyncio
from dataclasses import replace

import pytest
import pytest_asyncio
from _telegram_recovery_test_support import issue_restart_epoch
from _telegram_repository_test_support import db as db
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build

from app.telegram_bots import authority as live_authority

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def restart_environment(publication_environment, monkeypatch):
    """Real account/SQL custody; custom configuration availability controlled."""
    e = publication_environment

    async def available(link):
        return "ok"

    monkeypatch.setattr(e.live.authority, "_supported_target", available)
    return e


async def test_fixed_capture_uses_no_private_or_execution_ceiling(restart_environment, monkeypatch):
    e = restart_environment
    record = await e.reserve()
    issued = issue_restart_epoch(e, monkeypatch, [record])
    calls = []
    original = live_authority.require

    async def require(owner, *needed, **kwargs):
        calls.append(needed)
        assert not needed
        return await original(owner, **kwargs)

    monkeypatch.setattr(live_authority, "require", require)
    attempt = await issued.epoch.capture(record, e.link)
    await attempt.fixed_guard.prepare()
    async with e.sessions.begin() as writer:
        assert (await attempt.fixed_guard.check(writer)).id == e.link.id
    assert calls and all(value == () for value in calls)
    issued.epoch.retire()
    async with e.sessions.begin() as writer:
        with pytest.raises(ValueError):
            await attempt.fixed_guard.check(writer)


@pytest.mark.parametrize("change", ["adapter", "token", "endpoint", "phase", "send_release", "stop", "deadline"])
async def test_original_startup_ticket_cannot_upgrade_after_await(restart_environment, monkeypatch, change):
    e = restart_environment
    record = await e.reserve()
    issued = issue_restart_epoch(e, monkeypatch, [record])
    attempt = await issued.epoch.capture(record, e.link)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.live.authority.prepare_live_owner

    async def paused(owner):
        entered.set()
        await release.wait()
        return await original(owner)

    monkeypatch.setattr(e.live.authority, "prepare_live_owner", paused)
    pending = asyncio.create_task(attempt.fixed_guard.prepare())
    await entered.wait()
    if change == "adapter":
        issued.host._channels[e.link.id] = object()
    elif change == "token":
        issued.channel.transport_revision += 1
    elif change == "endpoint":
        issued.host.thread_client.endpoint_id = "f" * 64
    elif change == "phase":
        issued.host._recovery_phase = object()
    elif change == "send_release":
        issued.host._send_ready = True
    elif change == "stop":
        issued.host._stopping = True
    else:
        # A controlled clock stalls no native SQL; the original D still wins.
        monkeypatch.setattr(issued.epoch, "_clock", lambda: issued.epoch.deadline)
    release.set()
    with pytest.raises(ValueError):
        await pending


async def test_membership_and_ticket_identity_are_not_serializable_authority(restart_environment, monkeypatch):
    from app.telegram_bots.recovery_authority import RestartRecoveryTicket, check_restart_ticket

    e = restart_environment
    record = await e.reserve()
    issued = issue_restart_epoch(e, monkeypatch, [record])
    attempt = await issued.epoch.capture(record, e.link)
    check_restart_ticket(attempt.ticket, replace(record, dispatch_started=True, run_id="actual-observation"))
    for fake in [True, {"startup_id": attempt.ticket.startup_id}, object.__new__(RestartRecoveryTicket)]:
        with pytest.raises(ValueError):
            check_restart_ticket(fake, record)
    with pytest.raises(ValueError):
        await issued.epoch.capture(replace(record, input_sha256="f" * 64), e.link)
    issued.epoch.retire()
    with pytest.raises(ValueError):
        check_restart_ticket(attempt.ticket, record)


async def test_ticket_refuses_another_loop_before_any_authority_or_sql(restart_environment, monkeypatch):
    from app.telegram_bots.recovery_authority import check_restart_ticket

    e = restart_environment
    record = await e.reserve()
    issued = issue_restart_epoch(e, monkeypatch, [record])
    try:
        attempt = await issued.epoch.capture(record, e.link)

        async def forbidden(*args, **kwargs):
            pytest.fail("Foreign-loop ticket reached an authority/SQL collaborator")

        monkeypatch.setattr(e.live.authority, "prepare_live_owner", forbidden)

        async def foreign():
            with pytest.raises(ValueError):
                check_restart_ticket(attempt.ticket, record)
            with pytest.raises(ValueError):
                await attempt.fixed_guard.prepare()

        await asyncio.to_thread(lambda: asyncio.run(foreign()))
    finally:
        await issued.aclose()


async def test_fresh_receipt_read_never_renews_a_retired_write_ticket(restart_environment, monkeypatch):
    from app.telegram_bots.recovery_authority import check_restart_ticket

    e = restart_environment
    record = await e.reserve()
    issued = issue_restart_epoch(e, monkeypatch, [record])
    try:
        # A cohort entry alone supplies no already-issued original read attempt.
        with pytest.raises(ValueError):
            await issued.epoch.prepare_receipt_read(record)
        attempt = await issued.epoch.capture(record, e.link)

        async def unavailable(link):
            return "agent_unavailable"

        async def available(link):
            return "ok"

        monkeypatch.setattr(e.live.authority, "_supported_target", unavailable)
        with pytest.raises(ValueError):
            await attempt.fixed_guard.prepare()
        monkeypatch.setattr(e.live.authority, "_supported_target", available)
        read = await issued.epoch.prepare_receipt_read(record)
        assert read.captured_link == e.link and read.check_current() is True
        async with e.sessions.begin() as reader:
            await read.check_live_owner(e.link.owner_user_id, reader)
        with pytest.raises(ValueError):
            check_restart_ticket(attempt.ticket, record)
        with pytest.raises(ValueError):
            await issued.epoch.capture(record, e.link)
        issued.epoch.retire()
        with pytest.raises(ValueError):
            read.check_current()
    finally:
        await issued.aclose()


async def test_fresh_receipt_read_requires_current_live_account(restart_environment, monkeypatch):
    from datetime import UTC, datetime

    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = restart_environment
    record = await e.reserve()
    issued = issue_restart_epoch(e, monkeypatch, [record])
    try:
        await issued.epoch.capture(record, e.link)
        read = await issued.epoch.prepare_receipt_read(record)
        async with e.sessions.begin() as writer:
            writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.live.id, disabled_at=datetime.now(UTC)))
        async with e.sessions.begin() as reader:
            with pytest.raises(live_authority.AccessOff):
                await read.check_live_owner(e.live.id, reader)
        with pytest.raises(ValueError):
            await issued.epoch.prepare_receipt_read(record)
    finally:
        await issued.aclose()


async def test_actual_startup_calls_recovery_before_opening_admission(db, monkeypatch):
    # A behavior baseline: unchanged startup ignores this controlled collaborator.
    boot = build(db, monkeypatch)
    host, observations = boot.supervisor, []

    async def recover():
        observations.append((host._copy_decision, host._send_ready, host.send_allowed()))

    monkeypatch.setattr(host, "_recover_startup_publications", recover, raising=False)
    try:
        await host.check()
        await host.start()
        assert observations == [(False, False, False)]
        assert host.send_allowed()
    finally:
        await host.stop()


async def test_actual_final_stop_drains_recovery_before_coordinator_and_http(db, monkeypatch):
    from types import SimpleNamespace

    boot = build(db, monkeypatch)
    host, observations = boot.supervisor, []
    try:
        await host.check()
        await host.start()

        def begin():
            observations.append("recovery_begin")

        async def stop():
            assert not host.thread_client.closed and host._subscribed
            observations.append("recovery_stop")

        host.recovery = SimpleNamespace(begin_shutdown=begin, stop=stop)
        original = host.publications.stop

        async def coordinator_stop():
            assert "recovery_stop" in observations
            observations.append("coordinator_stop")
            await original()

        monkeypatch.setattr(host.publications, "stop", coordinator_stop)
        await host.stop()
        assert observations.index("recovery_stop") < observations.index("coordinator_stop")
        assert host.thread_client.closed
    finally:
        host.recovery = None
        if not host._stopped:
            # Restore only the test double before a failed-baseline cleanup.
            if host.publications is not None:
                monkeypatch.setattr(host.publications, "stop", original)
            await host.stop()


async def test_actual_startup_retires_epoch_before_any_held_release(db, monkeypatch):
    from test_telegram_copy_guard_supervisor import environment, stop_environment
    from test_telegram_supervisor import credential

    from app.telegram_bots.polling import PollTask
    from app.telegram_bots.recovery import PublicationRecoveryService

    events = []
    original_recover = PublicationRecoveryService.recover_startup
    original_release = PollTask.release_held

    async def recover(service, epoch):
        assert not boot.supervisor.send_allowed()
        assert epoch.records == ()
        events.append("epoch_captured")
        original_retire = epoch.retire

        def retire():
            original_retire()
            events.append("epoch_retired")

        monkeypatch.setattr(epoch, "retire", retire)
        return await original_recover(service, epoch)

    async def release(poll):
        assert events[-1] == "epoch_retired"
        assert not boot.supervisor.send_allowed()
        events.append("held_release")
        return await original_release(poll)

    monkeypatch.setattr(PublicationRecoveryService, "recover_startup", recover)
    monkeypatch.setattr(PollTask, "release_held", release)

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    boot = environment(db, monkeypatch, script=script)
    await credential(db, boot.env)
    try:
        await boot.supervisor.check()
        await boot.supervisor.start()
        assert "held_release" in events and boot.supervisor.send_allowed()
    finally:
        await stop_environment(boot)


async def test_recovery_cleanup_failure_retains_coordinator_http_and_listener(db, monkeypatch):
    boot = build(db, monkeypatch)
    host = boot.supervisor
    try:
        await host.check()
        await host.start()
        original = host.recovery.stop
        attempts, coordinator_calls = [], []
        coordinator_stop = host.publications.stop

        async def fail_once():
            attempts.append(True)
            if len(attempts) == 1:
                raise RuntimeError("controlled recovery cleanup failure")
            await original()

        async def record_coordinator():
            coordinator_calls.append(True)
            await coordinator_stop()

        monkeypatch.setattr(host.recovery, "stop", fail_once)
        monkeypatch.setattr(host.publications, "stop", record_coordinator)
        with pytest.raises(RuntimeError, match="^telegram-publication-cleanup-failed$"):
            await host.stop()
        assert not coordinator_calls and not host.thread_client.closed and host._subscribed
        await host.stop()
        assert len(attempts) == 2 and coordinator_calls == [True]
        assert host.thread_client.closed and not host._subscribed
    finally:
        await host.stop()


async def test_failed_shared_client_cleanup_preserves_listener_for_retry(db, monkeypatch):
    """A controlled public cleanup failure, not a real response-close claim."""
    boot = build(db, monkeypatch)
    host = boot.supervisor
    try:
        await host.check()
        await host.start()
        original = host.thread_client.stop
        attempts = []

        async def fail_once():
            attempts.append(True)
            if len(attempts) == 1:
                raise RuntimeError("controlled client cleanup failure")
            await original()

        monkeypatch.setattr(host.thread_client, "stop", fail_once)
        with pytest.raises(RuntimeError, match="^telegram-thread-client-cleanup-failed$"):
            await host.stop()
        assert not host.thread_client.closed
        assert host._subscribed, "Client cleanup failed but the original listener was released"
        await host.stop()
        assert host.thread_client.closed and not host._subscribed
        assert attempts == [True, True]
    finally:
        await host.stop()

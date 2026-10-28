"""Real SQL/bus owner transitions; remote run receipts are protocol fixtures."""

from dataclasses import replace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_publication_observation import run_row, serve

from app.telegram_bots.inbox import publication_identity

pytestmark = pytest.mark.asyncio


async def test_default_owner_does_not_change_existing_canonical_identity(publication_environment):
    e = publication_environment
    record = await e.reserve()
    assert set(publication_identity(record)) == {"scope", "turn_id", "publication_id", "event_id", "origins", "input_version", "input_sha256"}
    assert "owner_callback_id" not in publication_identity(record)


async def test_explicit_callback_owner_is_an_immutable_identity_member(publication_environment):
    e = publication_environment
    record = await e.reserve()
    owned = replace(record, owner_callback_id="c" * 32)
    assert publication_identity(owned)["owner_callback_id"] == "c" * 32
    assert owned.origins == record.origins and owned.input_sha256 == record.input_sha256
    with pytest.raises(ValueError):
        replace(record, owner_callback_id=True)
    with pytest.raises(ValueError):
        replace(record, run_id="not-dispatched")


async def test_guarded_input_reader_returns_actual_saved_input_and_rechecks_custody(publication_environment):
    e = publication_environment
    record = await e.reserve()
    fresh, origins = await e.inbox.read_publication_input(record, guard=e.guard_factory())
    assert publication_identity(fresh) == publication_identity(record)
    assert [row.sanitized_input for row in origins] == [row.sanitized_input for row in e.origins]
    e.attempt_current[0] = False
    with pytest.raises(ValueError):
        await e.inbox.read_publication_input(record, guard=e.guard_factory())


async def test_locate_reuses_exact_scan_and_durable_bind_without_reading_output(coordinator_environment):
    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    row = {**run_row(record), "metadata": {"channel_turn_id": record.publication_id}}
    await serve(e, record, run=row)
    result = await e.coordinator.locate_run(record, e.link, max_pages=1)
    assert result.state == "matched" and result.record.run_id == row["run_id"]
    assert result.run.row["metadata"]["channel_turn_id"] == record.publication_id
    assert not any(path.endswith("/output") for _, path, _ in e.requests)
    assert not any(method == "POST" for method, _, _ in e.requests)


async def test_exact_stop_intent_blocks_dispatch_and_only_same_intent_can_clear(publication_environment):
    e = publication_environment
    record = await e.reserve()
    first = await e.inbox.request_publication_stop(record, stop_id="command:1701:stop", guard=e.guard_factory())
    assert await e.inbox.request_publication_stop(record, stop_id="command:1701:stop", guard=e.guard_factory()) == first
    with pytest.raises(ValueError):
        await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    with pytest.raises(ValueError):
        await e.inbox.clear_publication_stop(record, stop_id="another-stop", guard=e.guard_factory())
    await e.inbox.clear_publication_stop(record, stop_id="command:1701:stop", guard=e.guard_factory())
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory()) is True


@pytest.mark.parametrize("marker", [None, "f" * 32])
async def test_locator_holds_conflicting_or_missing_persisted_marker(coordinator_environment, marker):
    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    row = {**run_row(record), "metadata": {} if marker is None else {"channel_turn_id": marker}}
    await serve(e, record, run=row)
    result = await e.coordinator.locate_run(record, e.link, max_pages=1)
    assert result.state == "held" and result.record.run_id is None


async def test_prethread_group_is_durable_complete_and_reused_by_first_reservation(coordinator_environment):
    e = coordinator_environment
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: e.creating[0])
    grouped = await e.inbox.group_prepublication(tuple(reversed(e.origins)), guard=guard)
    assert [row.message_id for row in grouped] == [701, 702]
    assert [row.grouped_message_ids for row in grouped] == [[702], [701]]
    assert all(row.turn_id is None and row.thread_id is None for row in grouped)
    with pytest.raises(ValueError):
        await e.inbox.group_prepublication((grouped[0],), guard=guard)
    record = await e.inbox.reserve_publication(e.link, grouped, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=guard)
    assert [origin.message_id for origin in record.origins] == [701, 702]


async def test_nonmatched_observation_cannot_carry_an_unvalidated_run(publication_environment):
    from app.telegram_bots.publication import PublicationRunObservation

    e = publication_environment
    record = await e.reserve()
    with pytest.raises(RuntimeError):
        PublicationRunObservation("pending", record, run={"run_id": "not-validated"}, reason="awaiting_run")


async def test_exact_stop_that_wins_marker_can_seal_fixed_primary_without_fake_run(publication_environment):
    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.producer import Outbox

    e = publication_environment
    record = await e.reserve()
    record = await e.inbox.request_publication_stop(record, stop_id="actual-command:stop", guard=e.guard_factory())
    outbox = Outbox(e.sessions)
    callbacks = CallbackRepository(e.sessions, config=e.db.config)
    identity = await outbox.plan_cancelled_before_dispatch(record, stop_id="actual-command:stop", guard=e.guard_factory(), callbacks=callbacks)
    assert await outbox.plan_cancelled_before_dispatch(record, stop_id="actual-command:stop", guard=e.guard_factory(), callbacks=callbacks) == identity
    with pytest.raises(ValueError):
        await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    fresh, origins = await e.inbox.read_publication_input(record, guard=e.guard_factory())
    assert fresh.run_id is None and fresh.dispatch_started is False
    assert all(row.route_complete and row.outbox_id == identity and row.run_id is None for row in origins)

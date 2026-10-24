"""Notice budgets survive observation eviction and transport replacement."""

import importlib
import math

import pytest


@pytest.fixture
def ledger():
    return importlib.import_module("app.telegram_bots.strangers").StrangerLedger()


def test_touch_only_lru_does_not_spend_or_replenish_charges(ledger):
    for sender in range(1, 4097):
        ledger.touch(1, sender, 0)
    ledger.touch(1, 1, 1)
    ledger.touch(1, 4097, 1)
    assert len(ledger._recent) == 4096
    assert (1, 1) in ledger._recent and (1, 2) not in ledger._recent
    assert ledger.seen(1, 1, 1)
    ledger.touch(1, 1, 100)
    assert not ledger.seen(1, 1, 100)


def test_pair_charge_survives_eviction_forget_and_same_bot_readd(ledger):
    assert ledger.seen(1, 1, 0)
    ledger.touch(1, 1, 0)
    for sender in range(2, 4098):
        ledger.touch(1, sender, 1)
    assert (1, 1) not in ledger._recent
    ledger.forget_bot(1)
    assert not ledger._recent
    assert not ledger.seen(1, 1, 86399.999)
    assert ledger.seen(1, 1, 86400)


def test_bot_hour_is_rolling_and_seen_charges_both_limits_atomically(ledger):
    for sender in range(1, 31):
        assert ledger.seen(1, sender, 10)
    assert not ledger.seen(1, 31, 3609.999)
    assert (1, 31) not in ledger._pairs
    ledger.forget_bot(1)
    assert not ledger.seen(1, 31, 3609.999)
    assert ledger.seen(1, 31, 3610)
    assert not ledger.seen(1, 1, 3610)
    assert ledger.seen(2, 1, 3610)


def test_bad_code_budget_is_global_per_sender_and_survives_forget(ledger):
    for bot in range(1, 6):
        assert ledger.bad_code(bot, 77, 20)
        ledger.forget_bot(bot)
    assert not ledger.bad_code(6, 77, 3619.999)
    assert ledger.bad_code(6, 78, 3619.999)
    assert ledger.bad_code(6, 77, 3620)
    assert ledger.seen(6, 77, 3620)


def test_full_charge_maps_refuse_unknown_without_evicting_live_allowances(ledger):
    for identity in range(1, 4097):
        assert ledger.seen(identity, identity, 0)
        assert ledger.bad_code(identity, identity, 0)
    assert not ledger.seen(5000, 5000, 1)
    assert not ledger.bad_code(5000, 5000, 1)
    assert 5000 not in ledger._bots
    assert ledger.bad_code(5000, 1, 1)
    assert all(len(store) <= 4096 for store in (ledger._recent, ledger._pairs, ledger._bots, ledger._bad_codes))
    assert not ledger.seen(1, 1, 3600)
    assert not ledger.seen(5000, 5000, 3600)
    assert ledger.bad_code(5000, 5000, 3601)
    assert ledger.seen(5000, 5000, 86400)


def test_no_refund_and_process_restart_starts_empty(ledger):
    assert ledger.seen(1, 1, 0)
    # A failed/cancelled disposable attempt has no refund operation.
    assert not ledger.seen(1, 1, 1)
    assert type(ledger)().seen(1, 1, 1)


@pytest.mark.parametrize("bot,sender,now", [(True, 1, 0), (1, False, 0), (0, 1, 0), (1, -1, 0), (1, 1, math.inf), (1, 1, math.nan)])
def test_invalid_inputs_allocate_nothing(ledger, bot, sender, now):
    for method in (ledger.touch, ledger.seen, ledger.bad_code):
        with pytest.raises(ValueError):
            method(bot, sender, now)
    assert not any((ledger._recent, ledger._pairs, ledger._bots, ledger._bad_codes))


def test_regressed_charge_clock_cannot_expire_or_reorder_live_charges(ledger):
    assert ledger.bad_code(1, 1, 10)
    with pytest.raises(ValueError):
        ledger.bad_code(1, 1, 9)
    assert list(ledger._bad_codes[1]) == [10]

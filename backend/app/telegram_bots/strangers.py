"""Bounded process-local observation and disposable-notice allowances.

Only the Gateway loop calls this await-free object. Observation eviction and
transport retirement never replenish an unexpired reply allowance.
"""

import math
from collections import OrderedDict, deque

STRANGER_MAP_MAX = 4096
STRANGER_REPLY_EVERY_SECONDS = 86400
STRANGER_REPLIES_PER_BOT_PER_HOUR = 30
BAD_CODE_REPLIES_PER_SENDER_PER_HOUR = 5
_HOUR = 3600


class StrangerLedger:
    def __init__(self) -> None:
        self._recent: OrderedDict[tuple[int, int], None] = OrderedDict()
        self._pairs: OrderedDict[tuple[int, int], float] = OrderedDict()
        self._bots: OrderedDict[int, deque[float]] = OrderedDict()
        self._bad_codes: OrderedDict[int, deque[float]] = OrderedDict()
        self._charge_time = -math.inf

    @staticmethod
    def _validate(bot_id: int, telegram_user_id: int, now: float) -> None:
        if any(type(value) is not int or value <= 0 for value in (bot_id, telegram_user_id)) or type(now) not in (int, float) or not math.isfinite(now):
            raise ValueError("invalid-stranger-observation")

    def touch(self, bot_id: int, telegram_user_id: int, now: float) -> None:
        self._validate(bot_id, telegram_user_id, now)
        key = bot_id, telegram_user_id
        self._recent[key] = None
        self._recent.move_to_end(key)
        if len(self._recent) > STRANGER_MAP_MAX:
            self._recent.popitem(last=False)

    def _prune(self, now: float) -> None:
        if now < self._charge_time:
            raise ValueError("regressed-stranger-clock")
        self._charge_time = now
        while self._pairs and next(iter(self._pairs.values())) <= now:
            self._pairs.popitem(last=False)
        for store in (self._bots, self._bad_codes):
            # Successful charges move their key to the end, so last charge
            # times are ordered without an auxiliary, ever-growing heap.
            while store and next(iter(store.values()))[-1] <= now - _HOUR:
                store.popitem(last=False)

    @staticmethod
    def _window(store: OrderedDict[int, deque[float]], key: int, now: float) -> deque[float] | None:
        window = store.get(key)
        if window is not None:
            while window and window[0] <= now - _HOUR:
                window.popleft()
        return window

    def seen(self, bot_id: int, telegram_user_id: int, now: float) -> bool:
        """Atomically reserve one pair-day and bot-hour stranger attempt."""
        self._validate(bot_id, telegram_user_id, now)
        self._prune(now)
        key = bot_id, telegram_user_id
        window = self._window(self._bots, bot_id, now)
        if key in self._pairs or len(self._pairs) >= STRANGER_MAP_MAX:
            return False
        if window is None and len(self._bots) >= STRANGER_MAP_MAX or window is not None and len(window) >= STRANGER_REPLIES_PER_BOT_PER_HOUR:
            return False
        if window is None:
            window = self._bots[bot_id] = deque()
        self._pairs[key] = now + STRANGER_REPLY_EVERY_SECONDS
        window.append(now)
        self._bots.move_to_end(bot_id)
        return True

    def bad_code(self, bot_id: int, telegram_user_id: int, now: float) -> bool:
        """Reserve one attempt from the sender's shared all-bot hour."""
        self._validate(bot_id, telegram_user_id, now)
        self._prune(now)
        window = self._window(self._bad_codes, telegram_user_id, now)
        if window is None and len(self._bad_codes) >= STRANGER_MAP_MAX or window is not None and len(window) >= BAD_CODE_REPLIES_PER_SENDER_PER_HOUR:
            return False
        if window is None:
            window = self._bad_codes[telegram_user_id] = deque()
        window.append(now)
        self._bad_codes.move_to_end(telegram_user_id)
        return True

    def forget_bot(self, bot_id: int) -> None:
        if type(bot_id) is not int or bot_id <= 0:
            raise ValueError("invalid-stranger-observation")
        for key in tuple(self._recent):
            if key[0] == bot_id:
                del self._recent[key]

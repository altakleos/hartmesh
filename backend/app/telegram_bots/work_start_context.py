"""Claim one fresh execution read without changing an admitted Start body."""

from datetime import timedelta

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.work_actions import _ACT_SCOPE, _SCOPE, same_body, thaw
from app.telegram_bots.work_authority import prepare_work_owner
from app.telegram_bots.work_start_actions import answered_calls, link_matches
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def execution_refresh(acts, act, *, attempt_current):
    deadline = act.first_attempt_at + timedelta(hours=1)
    continuation = thaw(act.continuation) or {}
    if "start_context_recovery" in continuation:
        return None
    link = await acts.links.get(act.link_id)
    if not link_matches(link, act) or not attempt_current() or acts.outbox.clock() >= deadline:
        return None
    owner = await prepare_work_owner(acts.authority, link.owner_user_id)
    async with acts.outbox._writer(link.id) as (writer, current):
        saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
        if (
            current is None
            or any(getattr(current, key) != getattr(link, key) for key in _SCOPE)
            or current.state != "connected"
            or current.health != "ok"
            or saved is None
            or saved.state != "pending"
            or saved.cancel_requested_at is not None
            or saved.attempts != act.attempts
            or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE)
            or not same_body(saved.body, act.body)
            or not same_body(saved.continuation, act.continuation)
            or not attempt_current()
        ):
            return None
        saved.continuation = {
            **continuation,
            "version": 1,
            "start_context_recovery": {"version": 1, "execution_attempt": act.attempts},
            "activation_retry": {"answered": min(act.attempts, answered_calls(act) + 1)},
        }
        saved.updated_at = acts.outbox.clock()
        await writer.flush()
        await owner(link.owner_user_id, writer)
        if not attempt_current() or acts.outbox.clock() >= deadline:
            raise AuthorityUnavailable()
        return _snapshot(saved)

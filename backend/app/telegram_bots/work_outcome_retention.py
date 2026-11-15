"""Retain actual primary journals needed by native outcome controls."""

from collections.abc import Mapping
from datetime import timedelta

from sqlalchemy import select

from app.telegram_bots.notifications import aware
from app.telegram_bots.work_news_carriers import news_witness_id
from app.telegram_bots.work_outcome_proof import outcome_key
from app.telegram_bots.work_start_retention import closed_start, reserved_starts
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


async def outcome_primary_acts(session, now, *, max_age_days, required_activations=()):
    from app.telegram_bots.work_start_refresh_retention import refresh_dependencies

    refresh = await refresh_dependencies(session, now, max_age_days=max_age_days)
    _groups, anchors = await reserved_starts(session)
    controls = await session.execute(
        select(TelegramCallbackRow, TelegramOutboxRow, TelegramBotLinkRow)
        .join(TelegramOutboxRow, TelegramOutboxRow.id == TelegramCallbackRow.payload["anchor_outbox_id"].as_string())
        .join(TelegramBotLinkRow, TelegramBotLinkRow.id == TelegramOutboxRow.link_id)
        .where(TelegramCallbackRow.kind.in_(("work_start", "work_change", "work_activation_origin")), TelegramOutboxRow.dedupe_key.like("notice:work-outcome:%"))
    )
    retained = set()
    # An independently owned refresh still needs its actual primary journal
    # after the public source control is lost. This only pins existing rows.
    ordered = sorted(refresh.deliveries)
    for start in range(0, len(ordered), 500):
        for parent in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_(ordered[start : start + 500]), TelegramOutboxRow.dedupe_key.like("notice:work-outcome:%"))):
            tail = outcome_key(parent.dedupe_key)
            binding = await session.get(TelegramBotLinkRow, parent.link_id)
            primary = await session.get(TelegramActRow, tail[0]) if tail is not None else None
            if binding is None or primary is None or primary.kind not in {"respond", "decide", "changes_requested"}:
                continue
            if (primary.link_id, primary.owner_user_id, primary.binding_generation, primary.chat_id, primary.instance_id, primary.work_id, primary.request_id) != (
                parent.link_id,
                parent.owner_user_id,
                parent.binding_generation,
                parent.chat_id,
                parent.instance_id,
                parent.work_id,
                parent.request_id,
            ):
                continue
            captured = parent.payload
            for key in ("work_outcome_basis", "context", "origin_proof", "act"):
                captured = captured.get(key, {}) if isinstance(captured, Mapping) else {}
            if isinstance(captured, Mapping) and any(
                getattr(primary, key) != captured[key] for key in ("id", "link_id", "owner_user_id", "bot_id", "binding_generation", "telegram_user_id", "chat_id", "instance_id", "work_id", "request_id") if key in captured
            ):
                continue
            retained.add(primary.id)
    for control, parent, binding in controls:
        # The host-owned row name survives damaged optional source metadata.
        # These facts only retain an already scoped journal; they admit no Act.
        tail = outcome_key(parent.dedupe_key)
        if tail is None:
            continue
        primary = await session.get(TelegramActRow, tail[0])
        if primary is None or primary.kind not in {"respond", "decide", "changes_requested"}:
            continue
        expected = (binding.id, binding.owner_user_id, binding.bot_id, binding.binding_generation, binding.telegram_user_id, binding.chat_id, binding.agent_ref, parent.work_id)
        if (
            (primary.link_id, primary.owner_user_id, primary.bot_id, primary.binding_generation, primary.telegram_user_id, primary.chat_id, primary.instance_id, primary.work_id) != expected
            or primary.request_id != parent.request_id
            or (parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id) != (binding.owner_user_id, binding.binding_generation, binding.chat_id, binding.agent_ref)
            or (control.link_id, control.binding_generation, control.chat_id, control.message_thread_id, control.target_kind, control.target_id)
            != (binding.id, binding.binding_generation, binding.chat_id, parent.message_thread_id, "work", parent.work_id)
        ):
            continue
        witness = control.kind == "work_activation_origin"
        action_id = control.payload.get("work_action_id")
        if witness:
            if type(action_id) is not str or len(action_id) != 32 or control.id != news_witness_id(action_id) or control.group_id != control.id or parent.id not in anchors:
                continue
            live = False
        else:
            if type(control.message_id) is not int or control.message_id <= 0:
                continue
            live = control.consumed_at is None and aware(control.created_at) > now - timedelta(days=max_age_days) and (control.expires_at is None or aware(control.expires_at) > now)
        admitted = witness or control.kind == "work_start" and action_id == control.id
        child = await session.get(TelegramActRow, action_id) if admitted else None
        own_child = child is not None and child.kind == "activate" and (child.link_id, child.owner_user_id, child.bot_id, child.binding_generation, child.telegram_user_id, child.chat_id, child.instance_id, child.work_id) == expected
        attempt = None
        if own_child:
            attempt = await session.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == child.work_id, AgentWorkAttemptRow.activation_id == child.id))
        if admitted:
            # Unknown journals remain held by their own admitted control. A
            # foreign or damaged child is never adopted as new permission.
            live = live or not closed_start(control, child, binding, attempt) or own_child and child.id in required_activations
        if live or parent.id in refresh.deliveries:
            retained.add(primary.id)
    return retained

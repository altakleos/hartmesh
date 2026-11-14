"""Pin original answer consent and explicit Retry ancestry, never authorize it."""

from collections.abc import Mapping
from datetime import timedelta

from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY, callback_publication_pending
from app.telegram_bots.notifications import aware
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def answer_dependencies(session, controls, links, now, *, max_age_days):
    groups, anchors = set(), set()
    by_id = {row.id: row for row in controls}
    parents = {}

    async def scoped_parent(identity, scope):
        if type(identity) is not str:
            return None
        if identity not in parents:
            parents[identity] = await session.get(TelegramOutboxRow, identity)
        parent = parents[identity]
        return parent if parent is not None and (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) == scope else None

    for start in controls:
        payload = start.payload if isinstance(start.payload, Mapping) else {}
        pending_owner = CALLBACK_PUBLICATION_KEY in payload and callback_publication_pending(payload)
        link = links.get(start.link_id)
        live_retry = (
            link is not None
            and (start.chat_id, start.binding_generation) == (link.chat_id, link.binding_generation)
            and start.kind == "retry_message"
            and start.consumed_at is None
            and now < aware(start.created_at) + timedelta(days=max_age_days)
            and (start.expires_at is None or now < aware(start.expires_at))
        )
        if not (pending_owner or live_retry):
            continue
        scope = (start.link_id, start.chat_id, start.binding_generation, start.message_thread_id)
        row, seen, chain_groups, chain_anchors, answer = start, set(), set(), set(), False
        while row is not None and row.id not in seen:
            if (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id) != scope:
                break
            seen.add(row.id)
            chain_groups.add(row.group_id)
            payload = row.payload if isinstance(row.payload, Mapping) else {}
            owner = payload.get(CALLBACK_PUBLICATION_KEY)
            document = owner.get("submission") if isinstance(owner, Mapping) else None
            # Presence is unknown custody even if its decoder/version is
            # damaged. These raw locators can only prevent deletion.
            if row.kind == "assign_answer":
                answer = True
            anchor = payload.get("anchor_outbox_id")
            if await scoped_parent(anchor, scope) is not None:
                chain_anchors.add(anchor)
            if isinstance(document, Mapping) and {"proposal_basis", "draft_id"}.intersection(document):
                answer = True
                anchor = document.get("source_outbox_id")
                if await scoped_parent(anchor, scope) is not None:
                    chain_anchors.add(anchor)
            source_id = owner.get("source_outbox_id") if isinstance(owner, Mapping) else None
            if source_id is None and row.kind in {"assign_answer", "retry_message"}:
                source_id = payload.get("anchor_outbox_id")
            if type(source_id) is not str:
                break
            parent = await scoped_parent(source_id, scope)
            if parent is None:
                break
            chain_anchors.add(parent.id)
            parent_payload = parent.payload if isinstance(parent.payload, Mapping) else {}
            basis = parent_payload.get("reply_basis", parent_payload.get("turn_outcome_basis", {}))
            publication = basis.get("publication", {}) if isinstance(basis, Mapping) else {}
            identity = parent_payload.get("input_owner_callback_id") or (publication.get("owner_callback_id") if isinstance(publication, Mapping) else None)
            row = by_id.get(identity) if type(identity) is str else None
        if answer:
            groups.update(chain_groups)
            anchors.update(chain_anchors)
    return groups, anchors

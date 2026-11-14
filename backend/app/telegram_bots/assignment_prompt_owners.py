"""Locate actual assignment prompt ACKs without authorizing an input."""

from sqlalchemy import or_, select

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def quoted_assignment_sources(outbox, link, message_id, topic, *, intents=("assignment_prompt_intent",)):
    after, found = "", []
    while True:
        async with outbox.sessions() as reader:
            page = list(
                await reader.scalars(
                    select(TelegramOutboxRow)
                    .where(
                        TelegramOutboxRow.id > after,
                        TelegramOutboxRow.link_id == link.id,
                        TelegramOutboxRow.owner_user_id == link.owner_user_id,
                        TelegramOutboxRow.binding_generation == link.binding_generation,
                        TelegramOutboxRow.chat_id == link.chat_id,
                        TelegramOutboxRow.message_thread_id == topic,
                        TelegramOutboxRow.instance_id == link.agent_ref,
                        or_(*(TelegramOutboxRow.payload[key]["prompt_id"].as_string().is_not(None) for key in intents)),
                    )
                    .order_by(TelegramOutboxRow.id)
                    .limit(100)
                )
            )
            found.extend(row.id for row in page if type(row.message_id) is int and row.message_id == message_id or any(type(value) is int and value == message_id for part in row.parts for value in part.get("message_ids", [])))
            if len(page) < 100:
                return found
            after = page[-1].id

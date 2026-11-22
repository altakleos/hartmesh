"""Generate deterministic, visible conversation history in the isolated acceptance database."""

import asyncio
import hashlib
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID

# Equal turns per conversation avoids giving a selective query a different history size.
THREADS = 4000
SENTENCES = (
    "The delivery schedule lists the expected date and the responsible colleague.",
    "Our weekly review covers the updated budget and the outstanding decisions.",
    "There are two follow-up questions for the next meeting with the local team.",
    "Please compare the current estimate with the previous planning document.",
    "We recorded the change in the project notes and will confirm it on Friday.",
    "The field team has checked the quantities against the original purchase order.",
    "This summary includes the outstanding items, their costs and the due dates.",
    "We will update the register after the remaining participants send their notes.",
)


def owner_for(index):
    return 0 if index < 2000 else 1 + (index - 2000) % 19


def message_count(index):
    return 200


def message(index, seq):
    key = index * 1009 + seq * 817
    length = 1 + (key % 4 if seq % 2 else key % 11)
    content = f"Planning item {index + 1}/{seq}. " + " ".join(
        SENTENCES[(key + n * 5) % len(SENTENCES)] for n in range(length)
    )
    if index == 0 and seq == 1:
        content += " saffron ledger witness"
    if index == 1999 and seq == 200:
        content += " violet schedule witness"
    if index == 3999 and seq == 200:
        content += " foreign meeting witness"
    return {"id": f"note-{index}-{seq}", "type": "human" if seq % 2 else "ai", "content": content}


async def populate(conn, indices, ids, *, controls=False):
    from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
    from deerflow.persistence.thread_meta.model import ThreadMetaRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    started = datetime(2024, 1, 1, tzinfo=UTC)
    batch = []
    total_bytes = rows = 0
    for index in indices:
        thread = f"sample-{index:04d}"
        owner = ids[0] if controls else ids[owner_for(index)]
        size = 500 if controls else message_count(index)
        stamp = started + timedelta(seconds=index)
        title = "Quarterly archive" if index == 0 else f"Planning notes {index:04d}"
        await conn.copy_records_to_table(
            ThreadMetaRow.__tablename__,
            columns=(
                "thread_id",
                "incarnation",
                "user_id",
                "assistant_id",
                "display_name",
                "created_at",
                "updated_at",
                "metadata_json",
                "status",
            ),
            records=[(thread, f"{index + 1:032x}", owner, "lead_agent", title, stamp, stamp, "{}", "idle")],
        )
        for seq in range(1, size + 1):
            msg = message(index, seq)
            content, metadata = DbRunEventStore._content_to_db(msg, None)
            event = "llm.human.input" if msg["type"] == "human" else "llm.ai.response"
            batch.append(
                (
                    thread,
                    f"turn-{index}-{(seq + 1) // 2}",
                    owner,
                    event,
                    "message",
                    content,
                    hashlib.sha256(content.encode()).hexdigest(),
                    json.dumps(metadata),
                    seq,
                    stamp + timedelta(milliseconds=seq),
                )
            )
            rows += 1
            total_bytes += len(msg["content"].encode("utf-8"))
        await conn.copy_records_to_table(
            RunEventThreadSeqRow.__tablename__, columns=("thread_id", "seq"), records=[(thread, size)]
        )
        if len(batch) >= 2000 or index == indices[-1]:
            await conn.copy_records_to_table(
                RunEventRow.__tablename__,
                columns=(
                    "thread_id",
                    "run_id",
                    "user_id",
                    "event_type",
                    "category",
                    "content",
                    "content_sha256",
                    "event_metadata",
                    "seq",
                    "created_at",
                ),
                records=batch,
            )
            batch.clear()
    return {"rows": rows, "visible_bytes": total_bytes, "threads": len(indices)}


async def main(mode, ids):
    import asyncpg

    conn = await asyncpg.connect(os.environ["ACCEPTANCE_DATABASE_URL"])
    try:
        if await conn.fetchval("select current_database()") != "acceptance":
            raise RuntimeError("Requires an isolated acceptance database")
        ids = [str(UUID(user)) for user in ids]
        async with conn.transaction():
            if mode == "corpus" and len(set(ids)) == 20:
                result = await populate(conn, range(THREADS), ids)
            elif mode in {"baseline", "indexed"} and len(ids) == 1:
                begin = 5000 if mode == "baseline" else 6000
                result = await populate(conn, range(begin, begin + 7), ids, controls=True)
            else:
                raise ValueError("Unexpected corpus mode or number of distinct owners")
        print(json.dumps(result))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2:]))

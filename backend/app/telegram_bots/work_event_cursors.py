"""Per-binding Work baselines; installation belongs to notification startup."""

from datetime import UTC, datetime

from sqlalchemy import DateTime, Integer, String, delete, insert, literal, or_, select
from sqlalchemy import inspect as sa_inspect

from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramWorkCursorRow
from deerflow.utils.file_io import await_drained


class WorkEventCursors:
    def __init__(self, links):
        self.links = links

    def install_binding_hook(self):
        """Install before routes admit binding; this does not seed old links."""
        self.links.work_baseline_seeder = self.seed_bound

    async def seed_bound(self, writer, link):
        """Borrow the binding's writer: marker and every cursor commit together."""
        engine = self.links.session_factory.kw["bind"]
        if not writer.in_transaction() or not writer.is_active or writer.get_bind() is not engine.sync_engine or sa_inspect(link).session is not writer.sync_session:
            raise ValueError("work-baseline-writer-required")
        if link.agent_kind != "instance" or link.state not in {"connected", "blocked"} or link.work_baseline_generation == link.binding_generation:
            return False
        if type(link.binding_generation) is not int or link.binding_generation < 1 or not link.agent_ref or link.connection_id is None or link.chat_id is None or link.telegram_user_id is None:
            raise ValueError("work-baseline-binding-required")
        # The caller owns the link lock. One INSERT SELECT defines the history
        # boundary; events committed afterward remain above these revisions.
        # No provider read, content projection or network operation happens here.
        await writer.flush()
        await writer.execute(delete(TelegramWorkCursorRow).where(TelegramWorkCursorRow.link_id == link.id))
        await writer.execute(
            insert(TelegramWorkCursorRow).from_select(
                ["link_id", "work_id", "instance_id", "binding_generation", "revision", "updated_at"],
                select(
                    literal(link.id, String),
                    AgentWorkRow.id,
                    AgentWorkRow.instance_id,
                    literal(link.binding_generation, Integer),
                    AgentWorkRow.revision,
                    literal(datetime.now(UTC), DateTime(timezone=True)),
                ).where(AgentWorkRow.instance_id == link.agent_ref),
            )
        )
        link.work_baseline_generation = link.binding_generation
        await writer.flush()
        return True

    async def initialize(self):
        """Seed only uninitialized bound generations, before notifications start."""

        async def perform():
            last, seeded = "", 0
            row = TelegramBotLinkRow
            while True:
                async with self.links.session_factory() as reader:
                    identities = list(
                        await reader.scalars(
                            select(row.id)
                            .where(row.id > last, row.agent_kind == "instance", row.state.in_(("connected", "blocked")), or_(row.work_baseline_generation.is_(None), row.work_baseline_generation != row.binding_generation))
                            .order_by(row.id)
                            .limit(100)
                        )
                    )
                if not identities:
                    return seeded
                for identity in identities:
                    async with self.links._transaction() as writer:
                        link = await writer.get(row, identity, with_for_update=True, populate_existing=True)
                        if link is not None:
                            seeded += bool(await self.seed_bound(writer, link))
                last = identities[-1]

        return await await_drained(perform())

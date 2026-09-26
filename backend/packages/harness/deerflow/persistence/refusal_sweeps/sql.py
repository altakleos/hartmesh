"""The record through which live Gateway processes confirm a refusal reached what they hold."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.refusal_sweeps.model import GatewayProcessRow, RefusalCheckRow, SurfaceEndingRow
from deerflow.persistence.sql_clock import coerce_database_wall_clock, database_wall_clock_expression
from deerflow.runtime.owner_holdings import Ended


@dataclass(frozen=True)
class LiveProcess:
    process_id: str
    checked_through: int
    #: The surfaces this process has no way to end.
    unreached: tuple[str, ...] = ()


@dataclass(frozen=True)
class SurfaceEnding:
    process_id: str
    user_id: str
    surface: str
    count: int
    ended_at: datetime
    #: How many the process tried to end and could not confirm ended.
    failed: int = 0
    #: The check the process was acting on.
    check_id: int = 0


class RefusalSweepRepository:
    """Checks, live processes and their endings.

    A check's id orders it after the refusal its requester committed first,
    on either dialect: the id is taken after that commit, so any process that
    reads an id at least as large reads the refusal too. Heartbeats are
    stamped on the database's clock and read against it, so no two hosts'
    clocks have to agree.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    @staticmethod
    async def _now(session: AsyncSession):
        dialect = (await session.connection()).dialect.name
        return database_wall_clock_expression(dialect)

    async def request_check(self) -> int:
        async with self._sf() as session:
            now = await self._now(session)
            check = (await session.execute(insert(RefusalCheckRow).values(requested_at=now).returning(RefusalCheckRow.id))).scalar_one()
            await session.commit()
            return int(check)

    async def latest_check(self) -> int:
        async with self._sf() as session:
            return int(await session.scalar(select(func.coalesce(func.max(RefusalCheckRow.id), 0))) or 0)

    async def register(self, process_id: str, *, unreached: tuple[str, ...] = ()) -> int:
        """Start beating at the latest check, and return it: a process that started after it holds nothing from before.

        The process takes the returned value as the check it has acted on,
        so its own count and its row never disagree.
        """
        async with self._sf() as session:
            now = await self._now(session)
            latest = int(await session.scalar(select(func.coalesce(func.max(RefusalCheckRow.id), 0))) or 0)
            await session.execute(delete(GatewayProcessRow).where(GatewayProcessRow.process_id == process_id))
            await session.execute(insert(GatewayProcessRow).values(process_id=process_id, started_at=now, heartbeat_at=now, checked_through=latest, unreached=_unreached_json(unreached)))
            await session.commit()
            return latest

    async def beat(self, process_id: str, *, checked_through: int | None = None, unreached: tuple[str, ...] = ()) -> None:
        """Stamp the heartbeat; ``unreached`` is what the row says again if it was pruned."""
        async with self._sf() as session:
            now = await self._now(session)
            values: dict[str, object] = {"heartbeat_at": now}
            if checked_through is not None:
                values["checked_through"] = checked_through
            result = await session.execute(update(GatewayProcessRow).where(GatewayProcessRow.process_id == process_id).values(**values))
            if not result.rowcount:
                # Pruned while this process was stalled: it is live again.
                await session.execute(insert(GatewayProcessRow).values(process_id=process_id, started_at=now, heartbeat_at=now, checked_through=checked_through or 0, unreached=_unreached_json(unreached)))
            await session.commit()

    async def unregister(self, process_id: str) -> None:
        async with self._sf() as session:
            await session.execute(delete(GatewayProcessRow).where(GatewayProcessRow.process_id == process_id))
            await session.commit()

    async def live_processes(self, *, window_seconds: float) -> list[LiveProcess]:
        """Every process whose last heartbeat is within ``window_seconds`` of the database's now."""
        async with self._sf() as session:
            now = coerce_database_wall_clock(await session.scalar(select(await self._now(session))))
            rows = (await session.execute(select(GatewayProcessRow).order_by(GatewayProcessRow.process_id))).scalars().all()
            cutoff = now - timedelta(seconds=window_seconds)
            return [LiveProcess(row.process_id, int(row.checked_through), tuple(json.loads(row.unreached)) if row.unreached else ()) for row in rows if coerce_database_wall_clock(row.heartbeat_at) >= cutoff]

    async def record_endings(self, process_id: str, check_id: int, endings: dict[str, dict[str, Ended | int]]) -> None:
        """Record what ``process_id`` ended for whom acting on ``check_id``; a bare count is all confirmed ended."""
        rows = []
        for user_id, surfaces in endings.items():
            for surface, outcome in surfaces.items():
                ended = outcome if isinstance(outcome, Ended) else Ended(outcome)
                if ended.count or ended.failed:
                    rows.append({"process_id": process_id, "user_id": user_id, "surface": surface, "count": ended.count, "failed": ended.failed, "check_id": check_id})
        if not rows:
            return
        async with self._sf() as session:
            now = coerce_database_wall_clock(await session.scalar(select(await self._now(session))))
            await session.execute(insert(SurfaceEndingRow), [{**row, "ended_at": now} for row in rows])
            await session.commit()

    async def endings_for(self, user_ids: list[str], *, since_check: int) -> list[SurfaceEnding]:
        if not user_ids:
            return []
        stmt = select(SurfaceEndingRow).where(SurfaceEndingRow.user_id.in_(user_ids), SurfaceEndingRow.check_id >= since_check).order_by(SurfaceEndingRow.id)
        async with self._sf() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return [SurfaceEnding(row.process_id, row.user_id, row.surface, int(row.count), coerce_database_wall_clock(row.ended_at), int(row.failed or 0), int(row.check_id)) for row in rows]

    async def prune(self, *, older_than_seconds: float) -> None:
        """Forget endings and superseded checks nobody will read again, and processes long gone."""
        async with self._sf() as session:
            now = coerce_database_wall_clock(await session.scalar(select(await self._now(session))))
            cutoff = now - timedelta(seconds=older_than_seconds)
            await session.execute(delete(SurfaceEndingRow).where(SurfaceEndingRow.ended_at < cutoff))
            await session.execute(delete(RefusalCheckRow).where(RefusalCheckRow.requested_at < cutoff, RefusalCheckRow.id < select(func.max(RefusalCheckRow.id)).scalar_subquery()))
            await session.execute(delete(GatewayProcessRow).where(GatewayProcessRow.heartbeat_at < cutoff))
            await session.commit()


def _unreached_json(unreached: tuple[str, ...]) -> str | None:
    return json.dumps(sorted(unreached)) if unreached else None

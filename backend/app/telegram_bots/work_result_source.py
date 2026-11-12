"""Current Work review source composed with every exported file at dispatch."""

import asyncio
from zoneinfo import ZoneInfo

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_request_source import WorkRequestSources, _RequestProjection
from app.telegram_bots.work_result_identity import result_identity
from app.telegram_bots.work_result_navigation import NavigationOrigin, require_navigation
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.utils.file_io import await_drained


class WorkResultSources(WorkRequestSources):
    async def _prepare(self, captured):
        basis, source, part = await super()._prepare(captured)
        if "result_files" not in basis or basis["version"] not in {1, 2}:
            raise ProductSourceRetired()
        return basis, source, part

    async def read_part(self, captured):
        async def read():
            _basis, _source, part = await self._prepare(captured)
            if part["kind"] in {"document", "photo", "album"} or "result_file_warning" in part:
                return "", ""
            return part["html"], part["visible_text"]

        return await await_drained(read())

    async def file_projection(self, captured, *, now):
        basis, source, _part = await self._prepare(captured)
        settings = await self.settings.get(captured.link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
        projection = _ResultProjection(self, captured, basis, source, settings, zone, True, now)
        return await self.qualify_navigation(captured, basis, source, projection)


class _ResultProjection(_RequestProjection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.navigation_origin = NavigationOrigin() if "work_result_navigation" in self.captured.item.payload else None

    def extend(self, query):
        result = super().extend(query)
        self.navigation_column = None
        navigation = self.captured.item.payload.get("work_result_navigation")
        if navigation is not None:
            attempt = aliased(AgentWorkAttemptRow)
            self.navigation_column = len(result.column_descriptions)
            result = result.add_columns(attempt).outerjoin(attempt, and_(attempt.id == navigation["attempt_id"], attempt.work_id == AgentWorkRow.id))
        return result

    def check(self, rows):
        if result_identity(rows[0][self.column]) != self.captured.item.payload.get("work_result_identity"):
            raise ProductSourceRetired()
        if self.navigation_column is not None:
            self.navigation_origin.check(self.captured.item.payload["work_result_navigation"])
            try:
                require_navigation(self.captured.item.payload["work_result_navigation"], rows[0][self.navigation_column], self.source.work["id"])
            except ValueError:
                raise ProductSourceRetired() from None
        super().check(rows)


WorkResultSources.projection_type = _ResultProjection

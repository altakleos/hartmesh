"""Current Work review source composed with every exported file at dispatch."""

import asyncio
from zoneinfo import ZoneInfo

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_request_source import WorkRequestSources, _RequestProjection
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
        return _RequestProjection(self, captured, basis, source, settings, zone, True, now)

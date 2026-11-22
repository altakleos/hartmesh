"""Read a presented Home output as its human exporter, under current inspection rights."""

from fastapi import HTTPException

from app.gateway.agent_conversations import inspection_for_read
from app.gateway.routers.spaces import _ERRORS, _http_error
from deerflow.agent_instances.contract import AgentDenied
from deerflow.spaces.contract import Permission, SpaceDenied, SpaceNotFound
from deerflow.spaces.filesystem import FileConflict, FileTooLarge, UnsafeSpacePath
from deerflow.spaces.service import MAX_TRANSFER_BYTES


async def admit_agent_presented_source(request, thread_id):
    inspection = await inspection_for_read(request, thread_id)
    if inspection is None:
        raise HTTPException(404, "Conversation or file is unavailable")
    home = inspection.instance.home_id
    files = inspection.authority.instances.files
    try:
        async with files.registry.admitted(actor=inspection.requester, requests={home: (Permission.READ | Permission.EXPORT, inspection.home_generation)}) as (session, rows):
            await inspection.authority.admit_file_export(session, inspection, rows[home][0].generation)
    except (AgentDenied, SpaceDenied, SpaceNotFound):
        raise HTTPException(403, "The agent's files require export access") from None
    except _ERRORS as exc:
        raise HTTPException(_http_error(exc).status_code, "The agent’s file storage is unavailable") from None


async def read_agent_presented_file(request, thread_id, path, *, maximum_bytes=None):
    inspection = await inspection_for_read(request, thread_id)
    if inspection is None:
        raise HTTPException(404, "Conversation or file is unavailable")
    home = inspection.instance.home_id

    async def admit(session, rows):
        await inspection.authority.admit_file_export(session, inspection, rows[home][0].generation)

    return await export_presented_output(inspection, path, maximum_bytes, admit)


async def export_presented_output(inspection, path, maximum_bytes, admit):
    prefix = "/mnt/user-data/outputs/"
    if not path.startswith(prefix):
        raise HTTPException(404, "Conversation or file is unavailable")
    files = inspection.authority.instances.files
    home = inspection.instance.home_id
    if maximum_bytes is not None:
        maximum_bytes = min(maximum_bytes, MAX_TRANSFER_BYTES)
    try:
        return await files.export_regular(actor=inspection.requester, space_id=home, path="outputs/" + path.removeprefix(prefix), maximum_bytes=maximum_bytes, controller_admission=admit)
    except (AgentDenied, SpaceDenied, SpaceNotFound):
        raise HTTPException(403, "The agent's files require export access") from None
    except (FileNotFoundError, PermissionError, UnsafeSpacePath):
        raise HTTPException(404, "Conversation or file is unavailable") from None
    except FileTooLarge:
        raise HTTPException(413, "File exceeds the attachment limit") from None
    except FileConflict:
        raise HTTPException(409, "The file changed during the read") from None
    except _ERRORS as exc:
        raise HTTPException(_http_error(exc).status_code, "The agent’s file storage is unavailable") from None

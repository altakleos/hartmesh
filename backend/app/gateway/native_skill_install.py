"""Export a qualified archive to private host processing before installation."""

import os

from fastapi import HTTPException

from app.gateway.chat_files import _revision, files_unavailable
from app.gateway.native_file_sources import prepare_feature_source
from app.gateway.routers.spaces import _ERRORS, _actor, _http_error
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.facade import storage_actor_scope
from deerflow.utils.file_io import await_drained, run_file_io


async def install_native_skill(request, body, config):
    from app.gateway.routers.skills import _copy_uploaded_skill_archive, _install_skill_archive, _skill_archive_upload_limit_message, _SkillArchiveUploadTooLargeError

    if not body.path.lower().endswith(".skill"):
        raise HTTPException(400, "Skill archive filename must end with .skill")
    actor = await _actor(request)
    temporary = None

    def copy(source):
        nonlocal temporary
        with os.fdopen(source.open(), "rb") as reader:
            before = os.fstat(reader.fileno())
            temporary = _copy_uploaded_skill_archive(reader)
            if _revision(os.fstat(reader.fileno())) != _revision(before):
                raise HTTPException(409, "The skill archive changed while copying")

    try:
        with storage_actor_scope(actor):
            async with prepare_feature_source(request, actor, "install_skill", {"body": body}) as (sources, source):
                await sources.refresh()
                await await_drained(run_file_io(copy, source))
                await sources.refresh()
                return await _install_skill_archive(temporary, config, allow_baseline_override=body.allow_baseline_override, request=request)
    except _SkillArchiveUploadTooLargeError:
        raise HTTPException(413, _skill_archive_upload_limit_message()) from None
    except BackingUnavailable:
        raise files_unavailable("space-unverified") from None
    except _ERRORS as exc:
        raise _http_error(exc) from None
    finally:
        if temporary is not None:
            await await_drained(run_file_io(temporary.unlink, missing_ok=True))
